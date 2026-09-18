//go:build unit

package handler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type openAICompatFailoverReadError struct{ err error }

func (r openAICompatFailoverReadError) Read([]byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	return 0, io.ErrUnexpectedEOF
}

type openAICompatFailoverWriter struct {
	gin.ResponseWriter
	written chan struct{}
	once    sync.Once
}

func (w *openAICompatFailoverWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	w.once.Do(func() { close(w.written) })
	return n, err
}

type openAICompatFailoverUpstream struct {
	service.HTTPUpstream
	accountIDs []int64
	failure    string
	written    <-chan struct{}
}

func (u *openAICompatFailoverUpstream) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.accountIDs = append(u.accountIDs, accountID)
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
	}
	if accountID == 2 {
		response.Body = io.NopCloser(strings.NewReader(
			"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_winner\",\"model\":\"gpt-5.1\",\"status\":\"in_progress\"}}\n\n" +
				"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"winning-text\"}\n\n" +
				"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_winner\",\"model\":\"gpt-5.1\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"winning-text\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n",
		))
		return response, nil
	}
	response.Body = io.NopCloser(openAICompatFailoverReadError{})
	switch u.failure {
	case "eof":
		response.Body = io.NopCloser(strings.NewReader(""))
	case "deadline":
		response.Body = io.NopCloser(openAICompatFailoverReadError{err: context.DeadlineExceeded})
	case "line_limit":
		response.Body = io.NopCloser(strings.NewReader("data: " + strings.Repeat("x", 1024*1024) + "\n\n"))
	case "after_text":
		response.Body = io.NopCloser(io.MultiReader(
			strings.NewReader("data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"partial-text\"}\n\n"), openAICompatFailoverReadError{},
		))
	case "after_tool":
		response.Body = io.NopCloser(io.MultiReader(
			strings.NewReader("data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"test_tool\",\"arguments\":\"{}\"}}\n\n"), openAICompatFailoverReadError{},
		))
	case "heartbeat_read_error", "idle_timeout":
		reader, writer := io.Pipe()
		response.Body = reader
		go func() {
			defer writer.Close()
			if u.failure == "heartbeat_read_error" {
				select {
				case <-u.written:
					_ = writer.CloseWithError(io.ErrUnexpectedEOF)
					return
				case <-req.Context().Done():
					return
				case <-time.After(4 * time.Second):
					_ = writer.CloseWithError(context.DeadlineExceeded)
					return
				}
			}
			<-req.Context().Done()
		}()
	}
	return response, nil
}

func TestOpenAIChatMessagesAttemptFailureSwitchesAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat", "messages"} {
		for _, stream := range []bool{false, true} {
			for _, failure := range []string{"deadline", "line_limit"} {
				t.Run(fmt.Sprintf("%s/stream_%t/%s", endpoint, stream, failure), func(t *testing.T) {
					upstream := &openAICompatFailoverUpstream{failure: failure}
					handler := newOpenAIResponsesFailoverTestHandler(t, upstream)
					handler.cfg.Gateway.MaxLineSize = 1024 * 1024
					c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
					c.Request.Body = io.NopCloser(strings.NewReader(fmt.Sprintf(`{"model":"gpt-5.1","stream":%t,"max_tokens":32,"messages":[{"role":"user","content":"hello"}]}`, stream)))
					if endpoint == "messages" {
						c.Request.URL.Path = "/v1/messages"
						apiKey, ok := middleware.GetAPIKeyFromContext(c)
						require.True(t, ok)
						apiKey.Group.AllowMessagesDispatch = true
						handler.Messages(c)
					} else {
						c.Request.URL.Path = "/v1/chat/completions"
						handler.ChatCompletions(c)
					}
					require.NoError(t, c.Request.Context().Err())
					require.Equal(t, []int64{1, 2}, upstream.accountIDs, rec.Body.String())
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
					require.Contains(t, rec.Body.String(), "winning-text")
					require.NotContains(t, rec.Body.String(), "upstream_error")
					require.NotContains(t, rec.Body.String(), strings.Repeat("x", 100))
				})
			}
		}
	}
}

func TestOpenAIChatMessagesStreamFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat", "messages"} {
		for _, failure := range []string{"read_error", "eof", "heartbeat_read_error", "idle_timeout", "after_text", "after_tool"} {
			t.Run(endpoint+"/"+failure, func(t *testing.T) {
				upstream := &openAICompatFailoverUpstream{failure: failure}
				handler := newOpenAIResponsesFailoverTestHandler(t, upstream)
				c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
				writer := &openAICompatFailoverWriter{ResponseWriter: c.Writer, written: make(chan struct{})}
				c.Writer = writer
				upstream.written = writer.written
				if failure == "heartbeat_read_error" {
					handler.cfg.Gateway.StreamKeepaliveInterval = 1
				}
				if failure == "idle_timeout" {
					handler.cfg.Gateway.StreamDataIntervalTimeout = 1
				}
				c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"max_tokens":32,"messages":[{"role":"user","content":"hello"}]}`))
				if endpoint == "messages" {
					c.Request.URL.Path = "/v1/messages"
					apiKey, ok := middleware.GetAPIKeyFromContext(c)
					require.True(t, ok)
					apiKey.Group.AllowMessagesDispatch = true
					handler.Messages(c)
				} else {
					c.Request.URL.Path = "/v1/chat/completions"
					handler.ChatCompletions(c)
				}
				if strings.HasPrefix(failure, "after_") {
					require.Equal(t, []int64{1}, upstream.accountIDs)
					require.NotContains(t, rec.Body.String(), "winning-text")
					return
				}
				require.Equal(t, []int64{1, 2}, upstream.accountIDs, rec.Body.String())
				require.Equal(t, http.StatusOK, rec.Code)
				require.Contains(t, rec.Body.String(), "winning-text")
				require.NotContains(t, rec.Body.String(), "upstream_error")
				require.NotContains(t, rec.Body.String(), "partial-text")
				if endpoint == "messages" {
					require.Equal(t, 1, strings.Count(rec.Body.String(), "event: message_start"))
					require.Equal(t, 1, strings.Count(rec.Body.String(), "event: message_stop"))
				} else {
					require.Equal(t, 1, strings.Count(rec.Body.String(), "data: [DONE]"))
				}
			})
		}
	}
}
