package service

import (
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIResponsesLineageOutputRequiresStrictCompleteTerminal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(nil)
	completePayload := []byte(`{"type":"response.completed","response":{"id":"resp_ok","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}}`)

	result := &OpenAIForwardResult{ResponseID: "resp_ok", UpstreamTerminalEvent: "response.completed"}
	result.CaptureOpenAIResponsesLineageOutput(c, completePayload)
	require.False(t, result.CompletedForLineage(), "out-of-scope requests must not retain output")

	EnableOpenAIStrictLineageCapture(c)
	result.CaptureOpenAIResponsesLineageOutput(c, completePayload)
	require.True(t, result.CompletedForLineage())
	output, ok := result.OpenAIResponsesLineageOutput()
	require.True(t, ok)
	require.JSONEq(t, `[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]`, string(output))
	output[0] = '{'
	again, ok := result.OpenAIResponsesLineageOutput()
	require.True(t, ok)
	require.NotEqual(t, output, again, "accessor must return an immutable copy")
}

func TestOpenAIResponsesLineageOutputRejectsIncompleteAmbiguousAndOversized(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(nil)
	EnableOpenAIStrictLineageCapture(c)
	tests := []struct {
		name, responseID, event string
		payload                 []byte
	}{
		{name: "missing status", responseID: "resp_1", event: "response.completed", payload: []byte(`{"type":"response.completed","response":{"id":"resp_1","output":[]}}`)},
		{name: "incomplete status", responseID: "resp_1", event: "response.completed", payload: []byte(`{"type":"response.completed","response":{"id":"resp_1","status":"incomplete","output":[]}}`)},
		{name: "id conflict", responseID: "resp_expected", event: "response.completed", payload: []byte(`{"type":"response.completed","response":{"id":"resp_other","status":"completed","output":[]}}`)},
		{name: "missing output", responseID: "resp_1", event: "response.completed", payload: []byte(`{"type":"response.completed","response":{"id":"resp_1","status":"completed"}}`)},
		{name: "event missing response envelope", responseID: "resp_1", event: "response.completed", payload: []byte(`{"type":"response.completed","id":"resp_1","status":"completed","output":[]}`)},
		{name: "duplicate terminal response id", responseID: "resp_1", event: "response.completed", payload: []byte(`{"type":"response.completed","response":{"id":"resp_1","id":"resp_other","status":"completed","output":[]}}`)},
		{name: "duplicate terminal output", responseID: "resp_1", event: "response.completed", payload: []byte(`{"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[],"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hidden"}]}]}}`)},
		{name: "failed terminal", responseID: "resp_1", event: "response.failed", payload: []byte(`{"type":"response.failed","response":{"id":"resp_1","status":"failed","output":[]}}`)},
		{name: "oversized", responseID: "resp_1", event: "response.completed", payload: []byte(`{"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"` + strings.Repeat("x", openAIResponsesLineageOutputMaxBytes) + `"}]}]}}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &OpenAIForwardResult{ResponseID: tt.responseID, UpstreamTerminalEvent: tt.event}
			result.CaptureOpenAIResponsesLineageOutput(c, tt.payload)
			require.False(t, result.CompletedForLineage())
			_, ok := result.OpenAIResponsesLineageOutput()
			require.False(t, ok)
		})
	}
}

func TestExtractSingleOpenAIResponsesSuccessTerminalRejectsMultipleTerminals(t *testing.T) {
	body := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_1","status":"completed","output":[]}}`,
		"",
		`data: {"type":"response.done","response":{"id":"resp_1","status":"done","output":[]}}`,
		"",
	}, "\n")
	_, ok := extractSingleOpenAIResponsesSuccessTerminal(body, "resp_1")
	require.False(t, ok)
}
