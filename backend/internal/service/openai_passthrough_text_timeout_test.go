package service

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIPassthroughTextDeadlineCoversPassiveImageNamespaceAndErrorBody(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
	}{
		{"passive_image_namespace", http.StatusOK},
		{"stalled_error_body", http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader, writer := io.Pipe()
			t.Cleanup(func() { _ = writer.Close() })
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: tc.status, Header: make(http.Header), Body: reader,
			}}
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
				OpenAIFirstOutputTimeoutSeconds: 1,
			}}, httpUpstream: upstream}
			body := []byte(`{"model":"gpt-5.5","stream":false,"input":"hello","tools":[{"type":"namespace","name":"image_gen","tools":[{"type":"function","name":"imagegen"}]}],"tool_choice":"auto"}`)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
			account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
				Credentials: map[string]any{"api_key": "test-key", "base_url": "https://upstream.example.test"},
				Extra:       map[string]any{"openai_passthrough": true},
			}

			result, err := svc.Forward(c.Request.Context(), c, account, body)

			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.True(t, failover.FirstOutputTimeout)
			require.Nil(t, result)
			require.Empty(t, recorder.Body.String())
		})
	}
}

func TestOpenAIPassthroughTextDeadlineIgnoresPrivateReasoning(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		OpenAIFirstOutputTimeoutSeconds: 1,
		StreamKeepaliveInterval:         0,
	}}}
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	written := make(chan error, 1)
	go func() {
		_, err := io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_wait\"}}\n\n"+
			"data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"reasoning\",\"id\":\"reason_1\",\"summary\":[],\"encrypted_content\":\"opaque\"}}\n\n")
		written <- err
	}()
	resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: reader}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}

	_, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "gpt-6.1-sol", "")

	require.NoError(t, <-written)
	var failover *UpstreamFailoverError
	require.ErrorAs(t, err, &failover)
	require.True(t, failover.FirstOutputTimeout)
	require.True(t, failover.SafeToFailoverAfterWrite)
	require.NotContains(t, recorder.Body.String(), "opaque")
}

func TestOpenAIPassthroughTextDeadlineDoesNotReplayCommittedStructure(t *testing.T) {
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		OpenAIFirstOutputTimeoutSeconds: 1,
		StreamKeepaliveInterval:         0,
	}}}
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	written := make(chan error, 1)
	go func() {
		_, err := io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_wait\"}}\n\n"+
			"data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"computer_call\",\"id\":\"call_1\",\"action\":{\"type\":\"click\"}}}\n\n")
		written <- err
	}()
	resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: reader}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}

	_, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "gpt-6.1-sol", "")

	require.NoError(t, <-written)
	require.Error(t, err)
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover))
	require.Contains(t, recorder.Body.String(), "computer_call")
	require.Contains(t, recorder.Body.String(), "response.failed")
	require.True(t, IsResponseCommitted(c))
}

func TestOpenAIPassthroughVisibleOutputClassifierIncludesRefusal(t *testing.T) {
	require.True(t, openAIStreamDataStartsVisibleOutput(`{"type":"response.refusal.done","refusal":"cannot comply"}`, ""))
	require.True(t, openAIStreamDataStartsVisibleOutput(`{"type":"response.output_item.done","item":{"type":"message","content":[{"type":"refusal","refusal":"cannot comply"}]}}`, ""))
}
