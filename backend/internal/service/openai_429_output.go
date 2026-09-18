package service

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
)

// Preserve the original bytes and the caller's response-size/error handling.
// Only a recovery probe needs to inspect SSE while a unary response buffers.
func openAI429RecoveryResponseReader(ctx context.Context, resp *http.Response, account *Account, maxBytes int64) io.Reader {
	attempt := openAI429AttemptFromContext(ctx, account)
	if !attempt.Probe() || !isEventStreamResponse(resp.Header) {
		return resp.Body
	}
	return &openAI429RecoverySSEReader{
		reader: bufio.NewReader(io.LimitReader(resp.Body, maxBytes+1)),
		ctx:    ctx, account: account, attempt: attempt,
	}
}

type openAI429RecoverySSEReader struct {
	reader  *bufio.Reader
	parser  openAICompatSSEFrameParser
	ctx     context.Context
	account *Account
	attempt *OpenAI429Attempt
	pending string
	readErr error
}

func (r *openAI429RecoverySSEReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.pending == "" && r.readErr == nil {
		r.attempt.lifecycleMu.Lock()
		settled := r.attempt.settled
		r.attempt.lifecycleMu.Unlock()
		if settled {
			r.parser = openAICompatSSEFrameParser{}
			return r.reader.Read(p)
		}
		r.pending, r.readErr = r.reader.ReadString('\n')
		line := strings.TrimSuffix(strings.TrimSuffix(r.pending, "\n"), "\r")
		if frame, ok := r.parser.AddLine(line); ok {
			observeOpenAI429RecoveryOutput(r.ctx, r.account, []byte(frame.Data), frame.EventType)
		}
		if r.readErr == io.EOF {
			if frame, ok := r.parser.Finish(); ok {
				observeOpenAI429RecoveryOutput(r.ctx, r.account, []byte(frame.Data), frame.EventType)
			}
		}
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	if r.pending == "" {
		return n, r.readErr
	}
	return n, nil
}

func acceptOpenAI429Recovery(ctx context.Context, account *Account) {
	acceptOpenAI429Attempt(ctx, openAI429AttemptFromContext(ctx, account))
}

func acceptOpenAI429Attempt(ctx context.Context, attempt *OpenAI429Attempt) {
	if !attempt.Probe() {
		return
	}
	if _, err := attempt.Accepted(ctx); err != nil {
		slog.Warn("openai_429_recovery_accept_failed", "account_id", attempt.accountID, "error", err)
	}
}

// Header acceptance and lifecycle preambles are not proof that a rate-limited
// account can generate again. Recover as soon as actual output is observed.
func observeOpenAI429RecoveryOutput(ctx context.Context, account *Account, data []byte, eventType string) {
	attempt := openAI429AttemptFromContext(ctx, account)
	if !attempt.Probe() {
		return
	}
	attempt.lifecycleMu.Lock()
	settled := attempt.settled
	attempt.lifecycleMu.Unlock()
	if settled || !gjson.ValidBytes(data) {
		return
	}
	root := gjson.ParseBytes(data)
	for _, path := range []string{"error", "response.error"} {
		if value := root.Get(path); value.Exists() && value.Type != gjson.Null {
			return
		}
	}
	if eventType == "" {
		eventType = strings.TrimSpace(root.Get("type").String())
	}
	if strings.HasPrefix(eventType, "response.") {
		if strings.HasSuffix(eventType, ".delta") {
			switch eventType {
			case "response.output_text.delta", "response.reasoning_summary_text.delta", "response.reasoning_text.delta",
				"response.function_call_arguments.delta", "response.custom_tool_call_input.delta",
				"response.audio.delta", "response.output_audio.delta", "response.audio_transcript.delta", "response.output_audio_transcript.delta":
			default:
				return
			}
		}
		if openAIStreamDataStartsVisibleOutput(string(data), eventType) {
			acceptOpenAI429Attempt(ctx, attempt)
		} else if (eventType == "response.completed" || eventType == "response.done") &&
			(root.Get("response.output.#").Int() > 0 || root.Get("response.usage.output_tokens").Int() > 0) {
			acceptOpenAI429Attempt(ctx, attempt)
		} else if eventType == "response.output_item.done" || eventType == "response.output_item.added" {
			item := root.Get("item")
			if isResponsesCompactionItemType(item.Get("type").String()) && item.Get("encrypted_content").String() != "" {
				acceptOpenAI429Attempt(ctx, attempt)
			}
		}
		return
	}
	if eventType != "" {
		return
	}
	if root.Get("status").String() == "completed" &&
		(root.Get("output.#").Int() > 0 || root.Get("usage.output_tokens").Int() > 0) {
		acceptOpenAI429Attempt(ctx, attempt)
		return
	}
	if root.Get("status").String() == "" {
		for _, item := range root.Get("output").Array() {
			if isResponsesCompactionItemType(item.Get("type").String()) && item.Get("encrypted_content").String() != "" {
				acceptOpenAI429Attempt(ctx, attempt)
				return
			}
		}
	}
	for _, choice := range root.Get("choices").Array() {
		content := choice.Get("delta")
		if !content.Exists() {
			content = choice.Get("message")
		}
		for _, path := range []string{"content", "reasoning_content", "reasoning", "refusal", "function_call.arguments", "audio.data", "audio.transcript"} {
			if value := content.Get(path); value.Type == gjson.String && value.String() != "" {
				acceptOpenAI429Attempt(ctx, attempt)
				return
			}
		}
		for _, call := range content.Get("tool_calls").Array() {
			if call.Get("function.arguments").String() != "" || call.Get("function.name").String() != "" {
				acceptOpenAI429Attempt(ctx, attempt)
				return
			}
		}
	}
}
