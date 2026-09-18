//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type openAI429OutputCache struct {
	OpenAI429RecoveryCache
	accepted int
	err      error
}

func (s *openAI429OutputCache) AcceptOpenAI429Attempt(context.Context, int64, string, string, string, string) (bool, error) {
	s.accepted++
	return s.err == nil, s.err
}

func newOpenAI429OutputAccount(cache *openAI429OutputCache) *Account {
	return &Account{
		ID:       27,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		OpenAI429Attempt: &OpenAI429Attempt{
			cache: cache, accountID: 27, probe: true, ctx: context.Background(), done: make(chan struct{}),
		},
	}
}

func TestOpenAI429RecoveryOutputRequiresActualGeneration(t *testing.T) {
	for _, tc := range []struct {
		name      string
		payload   string
		eventType string
		accepted  bool
	}{
		{name: "created", payload: `{"type":"response.created","response":{"id":"resp_1","status":"in_progress"}}`},
		{name: "heartbeat", payload: `{"type":"ping"}`},
		{name: "done sentinel", payload: `[DONE]`},
		{name: "empty delta", payload: `{"type":"response.output_text.delta","delta":""}`},
		{name: "unknown delta", payload: `{"type":"response.error.delta","delta":"rate limit"}`},
		{name: "failed", payload: `{"type":"response.failed","response":{"error":{"code":"rate_limit_exceeded"}}}`},
		{name: "error with spurious content", payload: `{"error":{"code":"rate_limit_exceeded"},"choices":[{"delta":{"content":"bad"}}]}`},
		{name: "role preamble", payload: `{"choices":[{"delta":{"role":"assistant"}}]}`},
		{name: "usage only", payload: `{"usage":{"output_tokens":1},"choices":[]}`},
		{name: "empty completed", payload: `{"type":"response.completed","response":{"output":[]}}`},
		{name: "incomplete", payload: `{"type":"response.incomplete","response":{"output":[{"type":"message","content":[{"text":"partial"}]}]}}`},
		{name: "text delta", payload: `{"type":"response.output_text.delta","delta":"Hello"}`, accepted: true},
		{name: "event line type", payload: `{"delta":"Hello"}`, eventType: "response.output_text.delta", accepted: true},
		{name: "reasoning delta", payload: `{"type":"response.reasoning_summary_text.delta","delta":"Reasoning"}`, accepted: true},
		{name: "partial image", payload: `{"type":"response.image_generation_call.partial_image","partial_image_b64":"aW1hZ2U="}`, accepted: true},
		{name: "completed compaction item", payload: `{"type":"response.output_item.done","item":{"type":"compaction","encrypted_content":"opaque"}}`, accepted: true},
		{name: "completed with billed output", payload: `{"type":"response.completed","response":{"output":[],"usage":{"output_tokens":1}}}`, accepted: true},
		{name: "chat content", payload: `{"choices":[{"delta":{"content":"Hello"}}]}`, accepted: true},
		{name: "chat reasoning", payload: `{"choices":[{"delta":{"reasoning_content":"Reasoning"}}]}`, accepted: true},
		{name: "chat tool arguments", payload: `{"choices":[{"delta":{"tool_calls":[{"function":{"arguments":"{}"}}]}}]}`, accepted: true},
		{name: "nonstream chat", payload: `{"choices":[{"message":{"role":"assistant","content":"Hello"}}]}`, accepted: true},
		{name: "nonstream completed", payload: `{"id":"resp_1","status":"completed","output":[{"type":"compaction","encrypted_content":"opaque"}]}`, accepted: true},
		{name: "native compact response", payload: `{"id":"resp_1","output":[{"type":"compaction","encrypted_content":"opaque"}]}`, accepted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := &openAI429OutputCache{}
			account := newOpenAI429OutputAccount(cache)
			observeOpenAI429RecoveryOutput(context.Background(), account, []byte(tc.payload), tc.eventType)
			observeOpenAI429RecoveryOutput(context.Background(), account, []byte(tc.payload), tc.eventType)
			want := 0
			if tc.accepted {
				want = 1
			}
			require.Equal(t, want, cache.accepted, "recovery must only be recorded once per admitted attempt")
		})
	}
}

func TestOpenAI429RecoveryOutputDoesNotRepeatFailedRecoveryWrites(t *testing.T) {
	cache := &openAI429OutputCache{err: errors.New("redis unavailable")}
	account := newOpenAI429OutputAccount(cache)
	payload := []byte(`{"type":"response.output_text.delta","delta":"Hello"}`)
	observeOpenAI429RecoveryOutput(context.Background(), account, payload, "")
	observeOpenAI429RecoveryOutput(context.Background(), account, payload, "")
	require.Equal(t, 1, cache.accepted)
	require.NoError(t, account.OpenAI429Attempt.Context().Err(), "already accepted generation must continue if recovery persistence fails")
}

type openAI429OutputCheckedReader struct {
	reader     io.Reader
	beforeRead func()
}

func (r *openAI429OutputCheckedReader) Read(p []byte) (int, error) {
	r.beforeRead()
	return r.reader.Read(p)
}

func TestOpenAI429PassthroughRecoversBeforeStreamCompletion(t *testing.T) {
	cache := &openAI429OutputCache{}
	account := newOpenAI429OutputAccount(cache)
	first := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"Hello\"}\n\n"
	terminal := "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
	body := io.MultiReader(strings.NewReader(first), &openAI429OutputCheckedReader{
		reader: strings.NewReader(terminal),
		beforeRead: func() {
			require.Equal(t, 1, cache.accepted, "the probe must be accepted before waiting for the rest of a long stream")
		},
	})
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(body)}

	result, err := (&OpenAIGatewayService{}).handleStreamingResponsePassthrough(c.Request.Context(), resp, c, account, time.Now(), "gpt-5", "gpt-5")

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, cache.accepted)
	require.Contains(t, recorder.Body.String(), "Hello")
}

func TestOpenAI429BufferedSSERecoversEarlyAndPreservesRawBytes(t *testing.T) {
	cache := &openAI429OutputCache{}
	account := newOpenAI429OutputAccount(cache)
	first := "event: response.output_text.delta\r\ndata: {\"delta\":\r\ndata: \"Hello\"}\r\n\r\n"
	terminal := "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\r\n\r\n"
	body := io.MultiReader(strings.NewReader(first), &openAI429OutputCheckedReader{
		reader: strings.NewReader(terminal),
		beforeRead: func() {
			require.Equal(t, 1, cache.accepted, "buffering must not hold a recovered account until the end of generation")
		},
	})
	resp := &http.Response{Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(body)}
	reader := openAI429RecoveryResponseReader(context.Background(), resp, account, 1024)

	got, err := readUpstreamResponseBodyLimited(reader, 1024)

	require.NoError(t, err)
	require.Equal(t, first+terminal, string(got), "SSE observation must not normalize or reconstruct the buffered body")
	require.Equal(t, 1, cache.accepted)
}

func TestOpenAI429BufferedSSEPreservesSizeLimitAndReadErrors(t *testing.T) {
	account := newOpenAI429OutputAccount(&openAI429OutputCache{})
	resp := &http.Response{Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(strings.Repeat("x", 65)))}
	reader := openAI429RecoveryResponseReader(context.Background(), resp, account, 64)
	_, err := readUpstreamResponseBodyLimited(reader, 64)
	require.ErrorIs(t, err, ErrUpstreamResponseBodyTooLarge)

	resp.Body = cancelReadCloser{}
	reader = openAI429RecoveryResponseReader(context.Background(), resp, account, 64)
	_, err = readUpstreamResponseBodyLimited(reader, 64)
	require.ErrorIs(t, err, context.Canceled)
}
