//go:build unit

package handler

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recoveryToggleUpstream struct {
	service.HTTPUpstream
	release <-chan struct{}
	calls   atomic.Int32
}

func (u *recoveryToggleUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.calls.Add(1)
	reader, writer := io.Pipe()
	go func() {
		defer writer.Close()
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_upstream\",\"status\":\"in_progress\"}}\n\n"+
			"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_upstream\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n"+
			"data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"item_id\":\"msg_upstream\",\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n"+
			"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"item_id\":\"msg_upstream\",\"delta\":\"first-text\"}\n\n")
		select {
		case <-u.release:
		case <-req.Context().Done():
			return
		}
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_upstream\",\"status\":\"completed\",\"output\":[{\"id\":\"msg_upstream\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"first-text\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n")
	}()
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: reader}, nil
}

type recoveryToggleTimingWriter struct {
	gin.ResponseWriter
	first chan time.Time
	once  sync.Once
}

func (w *recoveryToggleTimingWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte("first-text")) {
		w.once.Do(func() { w.first <- time.Now() })
	}
	return w.ResponseWriter.Write(p)
}

func (w *recoveryToggleTimingWriter) WriteString(s string) (int, error) {
	if strings.Contains(s, "first-text") {
		w.once.Do(func() { w.first <- time.Now() })
	}
	return w.ResponseWriter.WriteString(s)
}

func TestOpenAIRecoveryToggleStreamsBeforeUpstreamTerminal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, managed := range []bool{false, true} {
		name := "ordinary"
		if managed {
			name = "managed"
		}
		t.Run(name, func(t *testing.T) {
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			upstream := &recoveryToggleUpstream{release: release}
			h := newOpenAIResponsesFailoverTestHandler(t, upstream)
			ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(1))
			ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(2))
			ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "newapi")
			c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
			c.Request.URL.Path = "/v1/chat/completions"
			c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"max_tokens":32,"messages":[{"role":"user","content":"hello"}]}`))
			if managed {
				c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
			}
			timing := &recoveryToggleTimingWriter{ResponseWriter: c.Writer, first: make(chan time.Time, 1)}
			c.Writer = timing
			done := make(chan time.Time, 1)
			start := time.Now()
			go func() {
				h.ChatCompletions(c)
				done <- time.Now()
			}()
			var first time.Time
			select {
			case first = <-timing.first:
			case <-time.After(3 * time.Second):
				unblock()
				<-done
				t.Fatalf("first delta did not reach client before upstream terminal: calls=%d status=%d body=%q", upstream.calls.Load(), rec.Code, rec.Body.String())
			}
			select {
			case <-done:
				t.Fatal("handler completed before upstream terminal")
			default:
			}
			time.Sleep(25 * time.Millisecond)
			unblock()
			select {
			case completed := <-done:
				t.Logf("managed=%t first_delta=%s completion=%s", managed, first.Sub(start), completed.Sub(start))
			case <-time.After(3 * time.Second):
				t.Fatal("handler did not complete after upstream terminal")
			}
			require.Contains(t, rec.Body.String(), "first-text")
			assert.Equal(t, 1, strings.Count(rec.Body.String(), "data: [DONE]"))
			if managed {
				assert.Equal(t, "1", rec.Header().Get(openAIRecoveryProtocolHeader))
			} else {
				assert.Empty(t, rec.Header().Get(openAIRecoveryProtocolHeader))
			}
			allocs := testing.AllocsPerRun(3, func() {
				c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
				c.Request.URL.Path = "/v1/chat/completions"
				c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"max_tokens":32,"messages":[{"role":"user","content":"hello"}]}`))
				if managed {
					c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
				}
				h.ChatCompletions(c)
				if rec.Code != http.StatusOK {
					t.Fatalf("controlled upstream request failed: status=%d body=%q", rec.Code, rec.Body.String())
				}
			})
			t.Logf("managed=%t completed_request_allocs=%.0f", managed, allocs)
		})
	}
}
