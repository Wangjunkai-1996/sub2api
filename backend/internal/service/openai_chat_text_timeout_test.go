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

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type openAIChatExpireAfterPrefix struct {
	prefix    string
	expire    func()
	delivered bool
}

func (r *openAIChatExpireAfterPrefix) Read(p []byte) (int, error) {
	if !r.delivered {
		r.delivered = true
		return copy(p, r.prefix), nil
	}
	r.expire()
	return 0, io.EOF
}

func (r *openAIChatExpireAfterPrefix) Close() error { return nil }

func TestOpenAIChatTextDeadlineKeepsPreamblePrivateForFailover(t *testing.T) {
	for _, tc := range []struct {
		name, prefix string
		raw          bool
	}{
		{"responses_bridge", "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n", false},
		{"raw_chat", "data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"}}]}\n\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			svc := &OpenAIGatewayService{cfg: &config.Config{}}
			_, guard := newOpenAITextFirstOutputGuard(context.Background(), time.Now().Add(time.Hour))
			defer guard.close()
			body := &openAIChatExpireAfterPrefix{prefix: tc.prefix, expire: guard.expire}
			guard.setBody(body)
			resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}
			account := &Account{ID: 1, Platform: PlatformOpenAI}

			var result *OpenAIForwardResult
			var err error
			if tc.raw {
				result, err = svc.streamRawChatCompletions(c, resp, account, "gpt-5.5", "gpt-5.5", "gpt-5.5", nil, nil, time.Now(), 0, guard)
			} else {
				result, err = svc.handleChatStreamingResponse(resp, c, account, "gpt-5.5", "gpt-5.5", "gpt-5.5", time.Now(), 0, guard)
			}

			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			assert.True(t, failover.FirstOutputTimeout)
			assert.Nil(t, result)
			assert.Empty(t, recorder.Body.String())
		})
	}
}

func TestOpenAIChatTextDeadlineDoesNotReplayCommittedToolCall(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	svc := &OpenAIGatewayService{cfg: &config.Config{}}
	_, guard := newOpenAITextFirstOutputGuard(context.Background(), time.Now().Add(time.Hour))
	defer guard.close()
	body := &openAIChatExpireAfterPrefix{
		prefix: "data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"\"}}]}}],\"usage\":{\"prompt_tokens\":13,\"completion_tokens\":2,\"total_tokens\":15}}\n\n",
		expire: guard.expire,
	}
	guard.setBody(body)
	resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}

	result, err := svc.streamRawChatCompletions(c, resp, &Account{ID: 1, Platform: PlatformOpenAI}, "gpt-5.5", "gpt-5.5", "gpt-5.5", nil, nil, time.Now(), 0, guard)

	require.Error(t, err)
	var failover *UpstreamFailoverError
	assert.False(t, errors.As(err, &failover))
	require.NotNil(t, result)
	assert.Equal(t, 13, result.Usage.InputTokens)
	assert.Contains(t, recorder.Body.String(), "call_1")
	assert.False(t, strings.Contains(recorder.Body.String(), "[DONE]"))
}

func TestOpenAIChatTextDeadlineDoesNotReplayCommittedConvertedToolCall(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	svc := &OpenAIGatewayService{cfg: &config.Config{}}
	_, guard := newOpenAITextFirstOutputGuard(context.Background(), time.Now().Add(time.Hour))
	defer guard.close()
	body := &openAIChatExpireAfterPrefix{
		prefix: "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\"}}\n\n" +
			"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"lookup\"}}\n\n",
		expire: guard.expire,
	}
	guard.setBody(body)
	resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}

	result, err := svc.handleChatStreamingResponse(resp, c, &Account{ID: 1, Platform: PlatformOpenAI}, "gpt-5.5", "gpt-5.5", "gpt-5.5", time.Now(), 0, guard)

	require.Error(t, err)
	var failover *UpstreamFailoverError
	assert.False(t, errors.As(err, &failover))
	require.NotNil(t, result)
	assert.Contains(t, recorder.Body.String(), "call_1")
	assert.False(t, strings.Contains(recorder.Body.String(), "[DONE]"))
}

func TestOpenAIChatTextDeadlineStopsBufferedBodyRead(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	svc := &OpenAIGatewayService{cfg: &config.Config{}}
	_, guard := newOpenAITextFirstOutputGuard(context.Background(), time.Now().Add(time.Hour))
	defer guard.close()
	body := &openAIChatExpireAfterPrefix{prefix: `{"choices":[`, expire: guard.expire}
	guard.setBody(body)
	resp := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: body}

	result, err := svc.bufferRawChatCompletions(c, resp, &Account{ID: 1, Platform: PlatformOpenAI}, "gpt-5.5", "gpt-5.5", "gpt-5.5", nil, nil, time.Now(), guard)

	var failover *UpstreamFailoverError
	require.ErrorAs(t, err, &failover)
	assert.True(t, failover.FirstOutputTimeout)
	assert.Nil(t, result)
	assert.Empty(t, recorder.Body.String())
}
