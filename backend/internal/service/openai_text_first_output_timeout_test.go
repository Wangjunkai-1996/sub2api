package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAINativeTextFirstOutputCapsEveryEffort(t *testing.T) {
	for _, effort := range []string{"", "low", "high", "xhigh", "max"} {
		t.Run(effort, func(t *testing.T) {
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
				OpenAIFirstOutputTimeoutSeconds:           120,
				OpenAIHighEffortFirstOutputTimeoutSeconds: 600,
			}}}
			c, recorder := gin.CreateTestContext(httptest.NewRecorder())
			_ = recorder
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			body := &firstOutputCloseTrackingBody{ReadCloser: io.NopCloser(strings.NewReader("data: {\"type\":\"response.output_text.delta\",\"delta\":\"too late\"}\n\n")), closed: make(chan struct{})}
			resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}

			_, err := svc.handleStreamingResponseWithReasoning(context.Background(), resp, c, &Account{ID: 1, Platform: PlatformOpenAI}, time.Now().Add(-121*time.Second), "model", "model", effort, false)

			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			assert.True(t, failover.FirstOutputTimeout)
			assert.True(t, failover.SafeToFailoverAfterWrite)
			assert.False(t, c.Writer.Written(), "an already-expired attempt must not release queued output")
			select {
			case <-body.closed:
			default:
				t.Fatal("expired attempt did not close its body")
			}
		})
	}
}

func TestOpenAINativeTextFirstOutputExpiredAtEOF(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{OpenAIFirstOutputTimeoutSeconds: 120}}}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}

	_, err := svc.handleStreamingResponseWithReasoning(c.Request.Context(), resp, c, &Account{ID: 1, Platform: PlatformOpenAI}, time.Now().Add(-121*time.Second), "model", "model", "high", false)

	var failover *UpstreamFailoverError
	require.ErrorAs(t, err, &failover)
	assert.True(t, failover.FirstOutputTimeout)
	assert.Empty(t, recorder.Body.String())
}

func TestOpenAINativeTextFirstOutputTimeoutPreservesReplayBoundary(t *testing.T) {
	for _, tc := range []struct {
		name, event string
		committed   bool
	}{
		{"private_reasoning", privateReasoningEvent, false},
		{"opaque_progress", `{"type":"response.output_item.added","item":{"type":"computer_call","id":"call_1","action":{"type":"click"}}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
				OpenAIFirstOutputTimeoutSeconds:           1,
				OpenAIHighEffortFirstOutputTimeoutSeconds: 600,
			}}}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			reader, writer := io.Pipe()
			t.Cleanup(func() { _ = writer.Close() })
			body := &firstOutputCloseTrackingBody{ReadCloser: reader, closed: make(chan struct{})}
			written := make(chan error, 1)
			go func() {
				_, err := io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_wait\",\"usage\":{\"input_tokens\":13,\"output_tokens\":2}}}\n\ndata: "+tc.event+"\n\n")
				written <- err
			}()
			resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: body}

			result, err := svc.handleStreamingResponseWithReasoning(c.Request.Context(), resp, c, &Account{ID: 1, Platform: PlatformOpenAI}, time.Now(), "model", "model", "high", false)

			require.Error(t, err)
			require.NoError(t, <-written)
			var failover *UpstreamFailoverError
			assert.Equal(t, !tc.committed, errors.As(err, &failover))
			require.NotNil(t, result)
			assert.Equal(t, 13, result.usage.InputTokens)
			assert.Equal(t, 2, result.usage.OutputTokens)
			if tc.committed {
				assert.Contains(t, recorder.Body.String(), tc.event)
				assert.Contains(t, recorder.Body.String(), `"code":"first_output_timeout"`)
				assert.Equal(t, 1, strings.Count(recorder.Body.String(), `"type":"response.failed"`))
				assert.Contains(t, recorder.Body.String(), "event: response.failed\n")
				assert.NotContains(t, recorder.Body.String(), "event: error\n")
				assert.NotContains(t, recorder.Body.String(), "response.completed")
				assert.True(t, IsResponseCommitted(c))
			} else {
				assert.Empty(t, recorder.Body.String())
				assert.True(t, failover.SafeToFailoverAfterWrite)
			}
			select {
			case <-body.closed:
			default:
				t.Fatal("timed-out upstream body remained open")
			}
		})
	}
}

func TestOpenAINativeTextFirstOutputLeavesImageDeadlineUnchanged(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		OpenAIFirstOutputTimeoutSeconds:           120,
		OpenAIHighEffortFirstOutputTimeoutSeconds: 600,
	}}}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	stream := "data: {\"type\":\"response.output_item.done\",\"item\":{\"id\":\"image_1\",\"type\":\"image_generation_call\",\"result\":\"dGVzdA==\"}}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_image\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
	resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(stream))}

	result, err := svc.handleStreamingResponseWithReasoning(c.Request.Context(), resp, c, &Account{ID: 1, Platform: PlatformOpenAI}, time.Now().Add(-121*time.Second), "gpt-image-2", "gpt-image-2", "high", true)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Contains(t, recorder.Body.String(), `"type":"response.completed"`)
	assert.NotContains(t, recorder.Body.String(), "first_output_timeout")
}

func TestOpenAIForwardTextTimeoutAfterCommitPreservesUsage(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	written := make(chan error, 1)
	go func() {
		_, err := io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_partial\",\"usage\":{\"input_tokens\":13,\"output_tokens\":2}}}\n\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"computer_call\",\"id\":\"call_1\",\"action\":{\"type\":\"click\"}}}\n\n")
		written <- err
	}()
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: reader}}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		OpenAIFirstOutputTimeoutSeconds:           1,
		OpenAIHighEffortFirstOutputTimeoutSeconds: 600,
	}}, httpUpstream: upstream}
	body := []byte(`{"model":"gpt-6.1-sol","stream":true,"reasoning":{"effort":"high"},"input":"hello"}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-key", "base_url": "https://upstream.example.test"}}

	result, err := svc.Forward(c.Request.Context(), c, account, body)

	require.Error(t, err)
	require.NoError(t, <-written)
	var failover *UpstreamFailoverError
	assert.NotErrorAs(t, err, &failover)
	require.NotNil(t, result)
	assert.Equal(t, 13, result.Usage.InputTokens)
	assert.Equal(t, 2, result.Usage.OutputTokens)
	assert.Contains(t, recorder.Body.String(), `"code":"first_output_timeout"`)
}

func TestOpenAIForwardNonStreamingTextTimeoutSwitchesBeforeCommit(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	body := &firstOutputCloseTrackingBody{ReadCloser: reader, closed: make(chan struct{})}
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: body,
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		OpenAIFirstOutputTimeoutSeconds:           1,
		OpenAIHighEffortFirstOutputTimeoutSeconds: 600,
	}}, httpUpstream: upstream}
	requestBody := []byte(`{"model":"gpt-6.1-sol","stream":false,"reasoning":{"effort":"high"},"input":"hello","tools":[{"type":"namespace","name":"image_gen","tools":[{"type":"function","name":"imagegen"}]}],"tool_choice":"auto"}`)
	require.True(t, IsImageGenerationIntent(openAIResponsesEndpoint, "gpt-6.1-sol", requestBody))
	require.False(t, IsExplicitImageGenerationIntent(openAIResponsesEndpoint, "gpt-6.1-sol", requestBody))
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(requestBody))
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-key", "base_url": "https://upstream.example.test"}}

	result, err := svc.Forward(c.Request.Context(), c, account, requestBody)

	var failover *UpstreamFailoverError
	require.ErrorAs(t, err, &failover)
	assert.True(t, failover.FirstOutputTimeout)
	assert.True(t, failover.SafeToFailoverAfterWrite)
	assert.Nil(t, result)
	assert.Empty(t, recorder.Body.String())
	select {
	case <-body.closed:
	default:
		t.Fatal("timed-out non-streaming body remained open")
	}
}

func TestOpenAINonStreamingTextRejectsResponseAfterDeadline(t *testing.T) {
	for _, tc := range []struct {
		name, contentType, body string
	}{
		{"json", "application/json", `{"id":"resp_late","usage":{"input_tokens":1,"output_tokens":1},"output":[]}`},
		{"sse", "text/event-stream", "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_late\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1},\"output\":[]}}\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &OpenAIGatewayService{cfg: &config.Config{}}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			resp := &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {tc.contentType}}, Body: io.NopCloser(strings.NewReader(tc.body))}
			_, guard := newOpenAITextFirstOutputGuard(context.Background(), time.Now().Add(-time.Second))
			defer guard.close()

			result, err := svc.handleNonStreamingResponseWithGuard(c.Request.Context(), resp, c, &Account{ID: 1, Platform: PlatformOpenAI}, "model", "model", guard)

			require.ErrorIs(t, err, context.DeadlineExceeded)
			assert.Nil(t, result)
			assert.Empty(t, recorder.Body.String())
		})
	}
}
