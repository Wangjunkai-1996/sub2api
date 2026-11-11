//go:build unit

package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAISilentRefusalDetectorExplicitRefusalIsVisible(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
	}{
		{"chat raw", `{"choices":[{"delta":{"refusal":"Cannot help"}}]}`},
		{"responses delta", `{"type":"response.refusal.delta","delta":"Cannot help"}`},
		{"responses done", `{"type":"response.refusal.done","refusal":"Cannot help"}`},
		{"responses part added", `{"type":"response.content_part.added","part":{"type":"refusal","refusal":"Cannot help"}}`},
		{"responses part done", `{"type":"response.content_part.done","part":{"type":"refusal","refusal":"Cannot help"}}`},
		{"responses item added", `{"type":"response.output_item.added","item":{"type":"message","content":[{"type":"refusal","refusal":"Cannot help"}]}}`},
		{"responses item done", `{"type":"response.output_item.done","item":{"type":"message","content":[{"type":"refusal","refusal":"Cannot help"}]}}`},
		{"responses completed", `{"type":"response.completed","response":{"status":"completed","output":[{"type":"message","content":[{"type":"refusal","refusal":"Cannot help"}]}]}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newOpenAIChatSilentRefusalDetector(openAISilentRefusalMinRequestBodyBytes)
			d.ObservePayload([]byte(tc.payload))
			assert.True(t, d.ShouldReleaseClientOutput(), "an explicit refusal must not wait for usage")
			d.ObservePayload([]byte(`{"choices":[{"delta":{},"finish_reason":"stop"}]}`))
			assert.False(t, d.IsSilentRefusal(), "a visible refusal is a completed result, not a retryable empty response")
		})
	}
}

func TestOpenAISilentRefusalDetectorTypedRefusalAndEmptyBoundary(t *testing.T) {
	stop := "stop"
	for _, refusal := range []string{"Cannot help", ""} {
		t.Run(refusal, func(t *testing.T) {
			d := newOpenAIChatSilentRefusalDetector(openAISilentRefusalMinRequestBodyBytes)
			d.ObserveChatChunk(apicompat.ChatCompletionsChunk{Choices: []apicompat.ChatChunkChoice{{Delta: apicompat.ChatDelta{Refusal: &refusal}, FinishReason: &stop}}})
			assert.Equal(t, refusal != "", d.ShouldReleaseClientOutput())
			assert.Equal(t, refusal == "", d.IsSilentRefusal())
		})
	}
	for _, payload := range []string{
		`{"choices":[{"delta":{"refusal":""},"finish_reason":"stop"}]}`,
		`{"choices":[{"delta":{"refusal":null},"finish_reason":"stop"}]}`,
		`{"choices":[{"delta":{"refusal":123},"finish_reason":"stop"}]}`,
		`{"type":"response.refusal.delta","delta":""}`,
		`{"type":"response.refusal.done","refusal":null}`,
		`{"type":"response.content_part.done","part":{"type":"refusal","refusal":""}}`,
		`{"type":"response.completed","response":{"output":[{"type":"message","content":[{"type":"refusal","refusal":""}]}]}}`,
	} {
		d := newOpenAIChatSilentRefusalDetector(openAISilentRefusalMinRequestBodyBytes)
		d.ObservePayload([]byte(payload))
		d.ObservePayload([]byte(`{"type":"response.completed","response":{"status":"completed","output":[]}}`))
		assert.False(t, d.ShouldReleaseClientOutput(), payload)
		assert.True(t, d.IsSilentRefusal(), payload)
	}
}

func TestOpenAIExplicitRefusalWithoutUsageDoesNotFailOver(t *testing.T) {
	for _, raw := range []bool{true, false} {
		t.Run(map[bool]string{true: "raw_chat", false: "responses_to_chat"}[raw], func(t *testing.T) {
			body := largeRawChatCompletionsBody()
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
			stream := "data: {\"choices\":[{\"index\":0,\"delta\":{\"refusal\":\"Cannot help\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"
			if !raw {
				stream = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_refusal\",\"model\":\"gpt-5.5\"}}\n\ndata: {\"type\":\"response.refusal.delta\",\"delta\":\"Cannot help\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_refusal\",\"status\":\"completed\"}}\n\n"
			}
			resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(stream))}
			svc := &OpenAIGatewayService{cfg: rawChatCompletionsTestConfig(), httpUpstream: &httpUpstreamRecorder{resp: resp}}
			var result *OpenAIForwardResult
			var err error
			if raw {
				result, err = svc.forwardAsRawChatCompletions(context.Background(), c, rawChatCompletionsTestAccount(), body, "")
			} else {
				result, err = svc.handleChatStreamingResponse(resp, c, rawChatCompletionsTestAccount(), "gpt-5.5", "gpt-5.5", "gpt-5.5", time.Now(), len(body))
			}
			require.NoError(t, err)
			require.NotNil(t, result)
			assert.Contains(t, recorder.Body.String(), `"refusal":"Cannot help"`)
			assert.Contains(t, recorder.Body.String(), `"finish_reason":"stop"`)
			assert.Contains(t, recorder.Body.String(), "data: [DONE]")
			assert.NotContains(t, recorder.Body.String(), "Regenerating")
			assert.NotContains(t, recorder.Body.String(), `"content":"Cannot help"`)
		})
	}
}
