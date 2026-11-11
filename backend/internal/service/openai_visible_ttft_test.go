package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIVisibleOutputClassification(t *testing.T) {
	tests := []struct {
		name      string
		data      string
		eventType string
		want      bool
	}{
		{name: "keepalive", data: `{"type":"keepalive"}`, want: false},
		{name: "created", data: `{"type":"response.created"}`, want: false},
		{name: "empty output item", data: `{"type":"response.output_item.added","item":{"id":"item_test","type":"reasoning","summary":[]}}`, want: false},
		{name: "empty delta", data: `{"type":"response.output_text.delta","delta":""}`, want: false},
		{name: "text delta", data: `{"type":"response.output_text.delta","delta":"test output"}`, want: true},
		{name: "tool arguments", data: `{"type":"response.function_call_arguments.delta","delta":"{}"}`, want: true},
		{name: "partial image", data: `{"type":"response.image_generation_call.partial_image","partial_image_b64":"dGVzdA=="}`, want: true},
		{name: "completed image item", data: `{"type":"response.output_item.done","item":{"id":"item_test","type":"image_generation_call","result":"dGVzdA=="}}`, want: true},
		{name: "empty completed", data: `{"type":"response.completed","response":{"id":"resp_test","output":[]}}`, want: false},
		{name: "completed with output usage only", data: `{"type":"response.completed","response":{"id":"resp_test","usage":{"input_tokens":1,"output_tokens":2}}}`, want: false},
		{name: "completed with text", data: `{"type":"response.completed","response":{"id":"resp_test","output":[{"type":"message","content":[{"type":"output_text","text":"test output"}]}]}}`, want: true},
		{name: "done marker", data: `[DONE]`, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, openAIStreamDataStartsVisibleOutput(tt.data, tt.eventType))
		})
	}
}

func TestOpenAIClientOutputClassificationPreservesReplayBoundary(t *testing.T) {
	for _, tc := range []struct {
		name      string
		data      string
		eventType string
		want      bool
	}{
		{"empty text delta", `{"delta":""}`, "response.output_text.delta", false},
		{"empty reasoning delta", `{"delta":""}`, "response.reasoning_text.delta", false},
		{"null delta", `{"delta":null}`, "response.output_text.delta", true},
		{"missing delta", `{}`, "response.output_text.delta", true},
		{"object delta", `{"delta":{}}`, "response.output_text.delta", true},
		{"malformed delta", `{"delta":`, "response.output_text.delta", true},
		{"unknown empty delta", `{"delta":""}`, "response.unknown.delta", true},
		{"empty text done", `{"text":""}`, "response.output_text.done", false},
		{"null text done", `{"text":null}`, "response.output_text.done", true},
		{"empty argument done", `{"arguments":""}`, "response.function_call_arguments.done", true},
		{"empty custom input done", `{"input":""}`, "response.custom_tool_call_input.done", true},
		{"unknown part done", `{"part":{"type":"opaque","value":"content"}}`, "response.content_part.done", true},
		{"missing item done", `{}`, "response.output_item.done", true},
		{"unknown item done", `{"item":{"type":"computer_call","action":{"type":"click"}}}`, "response.output_item.done", true},
		{"empty message done", `{"item":{"type":"message","content":[]}}`, "response.output_item.done", false},
		{"invalid message done", `{"item":{"type":"message","content":null}}`, "response.output_item.done", true},
		{"empty reasoning done", `{"item":{"type":"reasoning","summary":[]}}`, "response.output_item.done", false},
		{"empty reasoning content done", `{"item":{"type":"reasoning","summary":[],"content":[]}}`, "response.output_item.done", false},
		{"reasoning content done", `{"item":{"type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":"real"}]}}`, "response.output_item.done", true},
		{"reasoning content added", `{"item":{"type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":"real"}]}}`, "response.output_item.added", true},
		{"null reasoning content done", `{"item":{"type":"reasoning","summary":[],"content":null}}`, "response.output_item.done", true},
		{"encrypted reasoning done", `{"item":{"type":"reasoning","summary":[],"encrypted_content":"opaque"}}`, "response.output_item.done", true},
		{"refusal done", `{"item":{"type":"message","content":[{"type":"refusal","refusal":"blocked"}]}}`, "response.output_item.done", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, openAIStreamDataStartsClientOutput(tc.data, tc.eventType))
		})
	}
	const encrypted = `{"type":"response.output_item.done","item":{"type":"reasoning","summary":[],"encrypted_content":"opaque"}}`
	require.True(t, openAIStreamDataStartsClientOutput(encrypted, ""))
	require.False(t, openAIStreamDataStartsVisibleOutput(encrypted, ""), "opaque output must not change visible TTFT")
}

func TestOpenAIHeartbeatReplayClassification(t *testing.T) {
	for _, tc := range []struct {
		name, data, eventType string
		output                bool
	}{
		{"keepalive", `{"type":"keepalive"}`, "", false},
		{"ping", `{"type":"ping"}`, "", false},
		{"named heartbeat", `{}`, "keepalive", false},
		{"metadata", `{"type":"keepalive","sequence_number":2,"timestamp":1789610000}`, "", false},
		{"string timestamp", `{"type":"ping","timestamp":"2026-09-17T01:36:31Z"}`, "", false},
		{"whitespace", ` {"type":" keepalive "} `, " keepalive ", false},
		{"malformed", `{"type":"keepalive"`, "keepalive", true},
		{"array", `[]`, "keepalive", true},
		{"null", `null`, "keepalive", true},
		{"non string type", `{"type":null}`, "keepalive", true},
		{"conflicting type", `{"type":"response.output_text.delta","delta":"answer"}`, "keepalive", true},
		{"duplicate conflicting type", `{"type":"keepalive","type":"response.completed"}`, "", true},
		{"reverse duplicate type", `{"type":"response.output_text.delta","delta":"answer","type":"keepalive"}`, "", true},
		{"invalid sequence", `{"type":"keepalive","sequence_number":{"text":"answer"}}`, "", true},
		{"invalid timestamp", `{"type":"ping","timestamp":{"text":"answer"}}`, "", true},
		{"text", `{"type":"keepalive","delta":"answer"}`, "", true},
		{"tool", `{"type":"keepalive","arguments":"{}"}`, "", true},
		{"encrypted reasoning", `{"type":"keepalive","item":{"type":"reasoning","encrypted_content":"opaque"}}`, "", true},
		{"unknown metadata", `{"type":"keepalive","payload":"opaque"}`, "", true},
		{"unknown event", `{"type":"heartbeat"}`, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.output, openAIStreamDataStartsClientOutput(tc.data, tc.eventType))
			require.Equal(t, tc.output, openAIStreamDataStartsSemanticTTFT(tc.data, tc.eventType))
		})
	}
}

func TestOpenAIResponsesTTFTStartsAtVisibleOutput(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		name := "native"
		if passthrough {
			name = "passthrough"
		}
		t.Run(name, func(t *testing.T) {
			result := runSyntheticVisibleTTFTStream(t, passthrough, 120*time.Millisecond, 0, OpenAITTFTModeVisible,
				`{"type":"response.output_text.delta","delta":"test output"}`)
			require.NotNil(t, result.firstTokenMs)
			require.GreaterOrEqual(t, *result.firstTokenMs, 100)
		})
	}
}

func TestOpenAIResponsesTTFTStartsAtCompletedImage(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		name := "native"
		if passthrough {
			name = "passthrough"
		}
		t.Run(name, func(t *testing.T) {
			result := runSyntheticVisibleTTFTStream(t, passthrough, 120*time.Millisecond, 0, OpenAITTFTModeVisible,
				`{"type":"response.output_item.done","item":{"id":"item_test","type":"image_generation_call","result":"dGVzdA=="}}`)
			require.NotNil(t, result.firstTokenMs)
			require.GreaterOrEqual(t, *result.firstTokenMs, 100)
		})
	}
}

func TestOpenAINativeMetadataAndKeepaliveDoNotDisarmFirstOutputTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		MaxLineSize:                     defaultMaxLineSize,
		OpenAIFirstOutputTimeoutSeconds: 1,
	}}}
	reader, writer := io.Pipe()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		defer func() { _ = writer.Close() }()
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_test\"}}\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"item_test\",\"type\":\"reasoning\",\"summary\":[]}}\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"keepalive\"}\n\n")
		_, _ = io.WriteString(writer, "event: ping\ndata: {}\n\n")
		time.Sleep(1200 * time.Millisecond)
	}()

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: reader}
	account := &Account{ID: 1, Name: "account_test", Platform: PlatformOpenAI}

	_, err := svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "test-model", "test-model")
	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.True(t, failoverErr.SafeToFailoverAfterWrite)
	require.Empty(t, recorder.Body.String())
	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("synthetic upstream writer did not exit")
	}
}

func TestOpenAIResponsesTTFTDefaultsToSemanticOutput(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		name := "native"
		if passthrough {
			name = "passthrough"
		}
		t.Run(name, func(t *testing.T) {
			result := runSyntheticVisibleTTFTStream(t, passthrough, 120*time.Millisecond, 0, "",
				`{"type":"response.output_text.delta","delta":"test output"}`)
			require.NotNil(t, result.firstTokenMs)
			require.Less(t, *result.firstTokenMs, 100)
		})
	}
}

func runSyntheticVisibleTTFTStream(t *testing.T, passthrough bool, visibleDelay time.Duration, timeoutSeconds int, ttftMode string, visibleEvent string) *openaiStreamingResult {
	t.Helper()
	gin.SetMode(gin.TestMode)
	mode := ttftMode
	if mode == "" {
		mode = OpenAITTFTModeSemantic
	}
	gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: mode, expiresAt: time.Now().Add(time.Minute).UnixNano()})
	t.Cleanup(func() {
		gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: OpenAITTFTModeSemantic, expiresAt: time.Now().Add(time.Minute).UnixNano()})
	})
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{
		MaxLineSize:                     defaultMaxLineSize,
		OpenAIFirstOutputTimeoutSeconds: timeoutSeconds,
	}}}
	reader, writer := io.Pipe()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		defer func() { _ = writer.Close() }()
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_test\"}}\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"item_test\",\"type\":\"reasoning\",\"summary\":[]}}\n\n")
		time.Sleep(visibleDelay)
		_, _ = io.WriteString(writer, "data: "+visibleEvent+"\n\n")
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_test\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n")
	}()

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: reader}
	account := &Account{ID: 1, Name: "account_test", Platform: PlatformOpenAI}
	started := time.Now()

	var result *openaiStreamingResult
	var err error
	if passthrough {
		var passthroughResult *openaiStreamingResultPassthrough
		passthroughResult, err = svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, started, "test-model", "test-model")
		if passthroughResult != nil {
			result = &openaiStreamingResult{firstTokenMs: passthroughResult.firstTokenMs}
		}
	} else {
		result, err = svc.handleStreamingResponse(context.Background(), resp, c, account, started, "test-model", "test-model")
	}
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Contains(t, recorder.Body.String(), `"type":"response.output_item.added"`)
	require.Contains(t, recorder.Body.String(), visibleEvent)
	select {
	case <-writerDone:
	case <-time.After(time.Second):
		t.Fatal("synthetic upstream writer did not exit")
	}
	return result
}
