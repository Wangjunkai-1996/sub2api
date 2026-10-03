package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const openAIChatTTFTCreated = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_ttft\",\"model\":\"gpt-5.5\"}}\n\n"

const openAIChatTTFTOutput = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n" +
	"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ttft\",\"status\":\"completed\",\"usage\":{\"input_tokens\":11,\"output_tokens\":5,\"total_tokens\":16}}}\n\n"

type openAIChatTTFTGate struct {
	blocked chan struct{}
	release chan struct{}
}

func (r *openAIChatTTFTGate) Read([]byte) (int, error) {
	close(r.blocked)
	<-r.release
	return 0, io.EOF
}

func TestOpenAIChatTTFTCommentDeferredUntilClientOutput(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Request.Header.Set("X-Sub2-TTFT", "1")
	gate := &openAIChatTTFTGate{blocked: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate.release) }) }
	t.Cleanup(release)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body: io.NopCloser(io.MultiReader(
			strings.NewReader(openAIChatTTFTCreated), gate, strings.NewReader(openAIChatTTFTOutput),
		)),
	}
	type outcome struct {
		result *OpenAIForwardResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		svc := &OpenAIGatewayService{cfg: &config.Config{}}
		result, err := svc.handleChatStreamingResponse(resp, c,
			&Account{ID: 1, Platform: PlatformOpenAI}, "gpt-5.5", "gpt-5.5", "gpt-5.5",
			time.Now().Add(-1180*time.Millisecond), openAISilentRefusalMinRequestBodyBytes)
		done <- outcome{result, err}
	}()

	select {
	case <-gate.blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream did not reach the pre-output gate")
	}
	require.False(t, c.Writer.Written(), "telemetry must not commit buffered response headers")
	require.Empty(t, rec.Body.String(), "telemetry must stay buffered with the initial role chunk")
	release()
	select {
	case out := <-done:
		require.NoError(t, out.err)
		require.NotNil(t, out.result)
		require.NotNil(t, out.result.FirstTokenMs)
		comment := fmt.Sprintf(": sub2-ttft-ms=%d\n\n", *out.result.FirstTokenMs)
		require.True(t, strings.HasPrefix(rec.Body.String(), comment))
		require.Equal(t, 1, strings.Count(rec.Body.String(), ": sub2-ttft-ms="))
		require.Contains(t, rec.Body.String(), `"content":"hello"`)
		require.Contains(t, rec.Body.String(), "data: [DONE]\n\n")
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not complete after releasing upstream")
	}
}

func TestOpenAIChatTTFTCommentNegotiation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var baseline []map[string]any
	for _, header := range []string{"", "0", "1"} {
		t.Run("header="+header, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			c.Request.Header.Set("X-Sub2-TTFT", header)
			result, err := forwardOpenAIChatTTFTTestStream(c, openAIChatTTFTCreated+openAIChatTTFTOutput, 0, 1180*time.Millisecond)
			require.NoError(t, err)
			require.NotNil(t, result.FirstTokenMs)
			if header == "1" {
				require.Contains(t, rec.Body.String(), fmt.Sprintf(": sub2-ttft-ms=%d\n\n", *result.FirstTokenMs))
			} else {
				require.NotContains(t, rec.Body.String(), ": sub2-ttft-ms=")
			}
			var chunks []map[string]any
			for _, line := range strings.Split(rec.Body.String(), "\n") {
				if !strings.HasPrefix(line, "data: {") {
					continue
				}
				var chunk map[string]any
				require.NoError(t, json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &chunk))
				delete(chunk, "created")
				chunks = append(chunks, chunk)
			}
			require.Len(t, chunks, 4)
			if header == "" {
				baseline = chunks
			} else {
				require.Equal(t, baseline, chunks, "negotiation must not alter Chat Completions data")
			}
		})
	}
}

func TestOpenAIChatTTFTCommentRetryIsolation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for name, terminal := range map[string]string{
		"rate_limit":     "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"rate_limit_exceeded\",\"message\":\"rate limit exceeded\"}}}\n\n",
		"silent_refusal": "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ttft\",\"status\":\"completed\",\"output\":[]}}\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			c.Request.Header.Set("X-Sub2-TTFT", "1")
			result, err := forwardOpenAIChatTTFTTestStream(c, openAIChatTTFTCreated+terminal, openAISilentRefusalMinRequestBodyBytes, 5*time.Second)
			var failoverErr *UpstreamFailoverError
			require.ErrorAs(t, err, &failoverErr)
			require.Nil(t, result)
			require.False(t, c.Writer.Written())
			require.Empty(t, rec.Body.String(), "discarded attempts must not leak telemetry")

			result, err = forwardOpenAIChatTTFTTestStream(c, openAIChatTTFTCreated+openAIChatTTFTOutput, openAISilentRefusalMinRequestBodyBytes, 1180*time.Millisecond)
			require.NoError(t, err)
			require.NotNil(t, result.FirstTokenMs)
			require.Equal(t, 1, strings.Count(rec.Body.String(), ": sub2-ttft-ms="))
			require.Contains(t, rec.Body.String(), fmt.Sprintf(": sub2-ttft-ms=%d\n\n", *result.FirstTokenMs))
		})
	}
}

func TestOpenAIChatTTFTCommentNonRetryableError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	c.Request.Header.Set("X-Sub2-TTFT", "1")
	body := openAIChatTTFTCreated + "data: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"upstream_error\",\"message\":\"input exceeds the context window\"}}}\n\n"
	_, err := forwardOpenAIChatTTFTTestStream(c, body, openAISilentRefusalMinRequestBodyBytes, time.Second)
	require.Error(t, err)
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
	require.JSONEq(t, `{"error":{"type":"upstream_error","message":"input exceeds the context window"}}`, rec.Body.String())
}

func forwardOpenAIChatTTFTTestStream(c *gin.Context, body string, requestBodyLen int, elapsed time.Duration) (*OpenAIForwardResult, error) {
	svc := &OpenAIGatewayService{cfg: &config.Config{}}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
	return svc.handleChatStreamingResponse(resp, c, &Account{ID: 1, Platform: PlatformOpenAI},
		"gpt-5.5", "gpt-5.5", "gpt-5.5", time.Now().Add(-elapsed), requestBodyLen)
}
