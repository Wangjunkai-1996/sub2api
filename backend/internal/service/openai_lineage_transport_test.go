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

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type openAIStrictLineageCommitObservation struct {
	turn       int
	responseID string
	output     []byte
	complete   bool
}

func openAIWSReconstructedLineageEvents(responseID string) [][]byte {
	return [][]byte{
		[]byte(`{"type":"response.created","response":{"id":"` + responseID + `","status":"in_progress"}}`),
		[]byte(`{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"SAFE_"}`),
		[]byte(`{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"OK"}`),
		[]byte(`{"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"SAFE_OK"}]}}`),
		[]byte(`{"type":"response.output_item.done","output_index":1,"item":{"id":"tool_1","type":"custom_tool_call","call_id":"call_1","name":"exec","input":"audited command","status":"completed"}}`),
		[]byte(`{"type":"response.completed","response":{"id":"` + responseID + `","status":"completed","output":[],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`),
	}
}

func openAIWSReconstructedLineageSSE(responseID string) string {
	return openAIWSReconstructedLineageSSEWithDone(responseID, true)
}

func openAIWSReconstructedLineageSSEWithDone(responseID string, includeDone bool) string {
	var builder strings.Builder
	for _, event := range openAIWSReconstructedLineageEvents(responseID) {
		builder.WriteString("data: ")
		builder.Write(event)
		builder.WriteString("\n\n")
	}
	if includeDone {
		builder.WriteString("data: [DONE]\n\n")
	}
	return builder.String()
}

func openAIWSDuplicateTerminalLineageSSE(responseID string) string {
	var builder strings.Builder
	for _, event := range openAIWSReconstructedLineageEvents(responseID) {
		builder.WriteString("data: ")
		builder.Write(event)
		builder.WriteString("\n\n")
	}
	builder.WriteString(`data: {"type":"response.done","response":{"id":"` + responseID + `","status":"done","output":[]}}`)
	builder.WriteString("\n\ndata: [DONE]\n\n")
	return builder.String()
}

func openAIWSOrdinaryTailAfterTerminalLineageSSE(responseID string) string {
	var builder strings.Builder
	for _, event := range openAIWSReconstructedLineageEvents(responseID) {
		builder.WriteString("data: ")
		builder.Write(event)
		builder.WriteString("\n\n")
	}
	builder.WriteString(`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"AMBIGUOUS_TAIL"}`)
	builder.WriteString("\n\ndata: [DONE]\n\n")
	return builder.String()
}

func observeFailingOpenAIStrictLineageCommit(
	observed chan<- openAIStrictLineageCommitObservation,
	cause error,
) OpenAIStrictLineageCommitter {
	return func(turn int, result *OpenAIForwardResult) error {
		output, complete := result.OpenAIResponsesLineageOutput()
		observed <- openAIStrictLineageCommitObservation{
			turn:       turn,
			responseID: result.ResponseID,
			output:     output,
			complete:   complete,
		}
		return cause
	}
}

func requireReconstructedOpenAIWSLineage(t *testing.T, observed openAIStrictLineageCommitObservation, responseID string, turn int) {
	t.Helper()
	require.Equal(t, turn, observed.turn)
	require.Equal(t, responseID, observed.responseID)
	require.True(t, observed.complete)
	require.Equal(t, "SAFE_OK", gjson.GetBytes(observed.output, "0.content.0.text").String())
	require.Equal(t, "audited command", gjson.GetBytes(observed.output, "1.input").String())
	document := auditinput.ParseResponsesOutput(observed.output)
	require.True(t, document.Complete, "%+v", document.Issues)
}

func openAIResponsesNonStreamingLineageBody(responseID string) string {
	return `{"id":"` + responseID + `","object":"response","model":"gpt-5.4","status":"completed","output":[{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"SAFE_OK"}]},{"id":"tool_1","type":"custom_tool_call","call_id":"call_1","name":"exec","input":"audited command","status":"completed"}],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}`
}

func openAISSEEventTypes(body string) []string {
	eventTypes := make([]string, 0, 8)
	forEachOpenAISSEDataPayload(body, func(payload []byte) {
		eventTypes = append(eventTypes, strings.TrimSpace(gjson.GetBytes(payload, "type").String()))
	})
	return eventTypes
}

func countOpenAISSEEventType(eventTypes []string, target string) int {
	count := 0
	for _, eventType := range eventTypes {
		if eventType == target {
			count++
		}
	}
	return count
}

func indexOpenAISSEEventType(eventTypes []string, target string) int {
	for index, eventType := range eventTypes {
		if eventType == target {
			return index
		}
	}
	return -1
}

func mismatchedOpenAIResponsesSSE() string {
	return strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_created","status":"in_progress"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_completed","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"unsafe lineage mismatch"}]}],"usage":{"input_tokens":1,"output_tokens":1}}}`,
		"",
	}, "\n")
}

func reconstructedOpenAIResponsesSSE() string {
	return strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_rebuilt","status":"in_progress"}}`,
		"",
		`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"SAFE_"}`,
		"",
		`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"OK"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_rebuilt","object":"response","model":"gpt-5.6-terra","status":"completed","output":[],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`,
		"",
	}, "\n")
}

func partialMixedOpenAIResponsesSSE() string {
	return strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_mixed","status":"in_progress"}}`,
		"",
		`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"SAFE"}`,
		"",
		`data: {"type":"response.output_item.done","output_index":1,"item":{"id":"tool_1","type":"custom_tool_call","call_id":"call_1","name":"exec","input":"dangerous command","status":"completed"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_mixed","status":"completed","output":[]}}`,
		"",
	}, "\n")
}

func malformedContributingOpenAIResponsesSSE() string {
	return strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_malformed","status":"in_progress"}}`,
		"",
		`data: {"type":"response.output_item.added","output_index":0,"item":{"id":"tool_1","type":"custom_tool_call","call_id":"call_1","name":"exec"}}`,
		"",
		`data: {"type":"response.custom_tool_call_input.delta","output_index":0,"delta":"dangerous command"`,
		"",
		`data: {"type":"response.output_text.delta","output_index":1,"content_index":0,"delta":"SAFE"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_malformed","status":"completed","output":[]}}`,
		"",
	}, "\n")
}

func TestOpenAIStreamingTransportsRejectMismatchedCreatedAndTerminalLineageIDs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name string
		run  func(*OpenAIGatewayService, *gin.Context, *http.Response) (string, bool, error)
	}{
		{
			name: "native",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) (string, bool, error) {
				result, err := svc.handleStreamingResponse(context.Background(), resp, c, &Account{ID: 1}, time.Now(), "gpt-5.4", "gpt-5.4")
				if result == nil {
					return "", false, err
				}
				return result.responseID, result.lineageComplete, err
			},
		},
		{
			name: "passthrough",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) (string, bool, error) {
				result, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, &Account{ID: 1}, time.Now(), "gpt-5.4", "gpt-5.4")
				if result == nil {
					return "", false, err
				}
				return result.responseID, result.lineageComplete, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			EnableOpenAIStrictLineageCapture(c)
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(mismatchedOpenAIResponsesSSE())),
			}
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}

			responseID, lineageComplete, err := tt.run(svc, c, resp)
			require.NoError(t, err)
			require.Equal(t, "resp_created", responseID)
			require.False(t, lineageComplete, "terminal output from another response must never become lineage evidence")
		})
	}
}

func TestOpenAISSEToJSONTransportsRejectMismatchedCreatedAndTerminalLineageIDs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := []byte(mismatchedOpenAIResponsesSSE())

	t.Run("native", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		EnableOpenAIStrictLineageCapture(c)
		svc := &OpenAIGatewayService{cfg: &config.Config{}}
		resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}}

		result, err := svc.handleSSEToJSON(resp, c, body, "gpt-5.4", "gpt-5.4")
		require.NoError(t, err)
		require.NotNil(t, result)
		require.False(t, result.lineageComplete)
	})

	t.Run("passthrough", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		EnableOpenAIStrictLineageCapture(c)
		svc := &OpenAIGatewayService{cfg: &config.Config{}}
		resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}}

		result, err := svc.handlePassthroughSSEToJSON(resp, c, body, "gpt-5.4", "gpt-5.4")
		require.NoError(t, err)
		require.NotNil(t, result)
		require.False(t, result.lineageComplete)
	})
}

func TestOpenAISSEToJSONTransportsCaptureReconstructedLineageOutput(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := []byte(reconstructedOpenAIResponsesSSE())

	tests := []struct {
		name string
		run  func(*OpenAIGatewayService, *gin.Context, *http.Response) ([]byte, bool, error)
	}{
		{
			name: "native",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) ([]byte, bool, error) {
				result, err := svc.handleSSEToJSON(resp, c, body, "gpt-5.6-terra", "gpt-5.6-terra")
				if result == nil {
					return nil, false, err
				}
				return result.lineageOutput, result.lineageComplete, err
			},
		},
		{
			name: "passthrough",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) ([]byte, bool, error) {
				result, err := svc.handlePassthroughSSEToJSON(resp, c, body, "gpt-5.6-terra", "gpt-5.6-terra")
				if result == nil {
					return nil, false, err
				}
				return result.lineageOutput, result.lineageComplete, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			EnableOpenAIStrictLineageCapture(c)
			svc := &OpenAIGatewayService{cfg: &config.Config{}}
			resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}}

			output, complete, err := tt.run(svc, c, resp)
			require.NoError(t, err)
			require.True(t, complete)
			require.Equal(t, "SAFE_OK", gjson.GetBytes(output, "0.content.0.text").String())
			document := auditinput.ParseResponsesOutput(output)
			require.True(t, document.Complete, "%+v", document.Issues)
			require.Equal(t, "SAFE_OK", document.NormalizedText)
			require.Equal(t, "SAFE_OK", gjson.Get(recorder.Body.String(), "output.0.content.0.text").String())
		})
	}
}

func TestOpenAIStreamingTransportsCaptureReconstructedLineageOutput(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name string
		run  func(*OpenAIGatewayService, *gin.Context, *http.Response) ([]byte, bool, error)
	}{
		{
			name: "native",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) ([]byte, bool, error) {
				result, err := svc.handleStreamingResponse(context.Background(), resp, c, &Account{ID: 1}, time.Now(), "gpt-5.6-terra", "gpt-5.6-terra")
				if result == nil {
					return nil, false, err
				}
				return result.lineageOutput, result.lineageComplete, err
			},
		},
		{
			name: "passthrough",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) ([]byte, bool, error) {
				result, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, &Account{ID: 1}, time.Now(), "gpt-5.6-terra", "gpt-5.6-terra")
				if result == nil {
					return nil, false, err
				}
				return result.lineageOutput, result.lineageComplete, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			EnableOpenAIStrictLineageCapture(c)
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(reconstructedOpenAIResponsesSSE())),
			}
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}

			output, complete, err := tt.run(svc, c, resp)
			require.NoError(t, err)
			require.True(t, complete)
			require.Equal(t, "SAFE_OK", gjson.GetBytes(output, "0.content.0.text").String())
			document := auditinput.ParseResponsesOutput(output)
			require.True(t, document.Complete, "%+v", document.Issues)
			require.Equal(t, "SAFE_OK", document.NormalizedText)
			if tt.name == "passthrough" {
				require.Equal(t, reconstructedOpenAIResponsesSSE(), recorder.Body.String())
			}
		})
	}
}

func TestOpenAIStreamingTransportsRejectPartialMixedLineageReconstruction(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name string
		run  func(*OpenAIGatewayService, *gin.Context, *http.Response) ([]byte, bool, error)
	}{
		{
			name: "native",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) ([]byte, bool, error) {
				result, err := svc.handleStreamingResponse(context.Background(), resp, c, &Account{ID: 1}, time.Now(), "gpt-5.6-terra", "gpt-5.6-terra")
				if result == nil {
					return nil, false, err
				}
				return result.lineageOutput, result.lineageComplete, err
			},
		},
		{
			name: "passthrough",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) ([]byte, bool, error) {
				result, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, &Account{ID: 1}, time.Now(), "gpt-5.6-terra", "gpt-5.6-terra")
				if result == nil {
					return nil, false, err
				}
				return result.lineageOutput, result.lineageComplete, err
			},
		},
	}

	inputs := []struct {
		name string
		body string
	}{
		{name: "missing_done_item", body: partialMixedOpenAIResponsesSSE()},
		{name: "malformed_contributing_frame", body: malformedContributingOpenAIResponsesSSE()},
	}
	for _, input := range inputs {
		for _, tt := range tests {
			t.Run(input.name+"/"+tt.name, func(t *testing.T) {
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				EnableOpenAIStrictLineageCapture(c)
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
					Body:       io.NopCloser(strings.NewReader(input.body)),
				}
				svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}

				output, complete, err := tt.run(svc, c, resp)
				require.NoError(t, err)
				require.False(t, complete)
				require.Empty(t, output)
			})
		}
	}
}

func TestOpenAISSEToJSONTransportsRejectMalformedContributingLineage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := []byte(malformedContributingOpenAIResponsesSSE())
	tests := []struct {
		name string
		run  func(*OpenAIGatewayService, *gin.Context, *http.Response) ([]byte, bool, error)
	}{
		{
			name: "native",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) ([]byte, bool, error) {
				result, err := svc.handleSSEToJSON(resp, c, body, "gpt-5.6-terra", "gpt-5.6-terra")
				if result == nil {
					return nil, false, err
				}
				return result.lineageOutput, result.lineageComplete, err
			},
		},
		{
			name: "passthrough",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) ([]byte, bool, error) {
				result, err := svc.handlePassthroughSSEToJSON(resp, c, body, "gpt-5.6-terra", "gpt-5.6-terra")
				if result == nil {
					return nil, false, err
				}
				return result.lineageOutput, result.lineageComplete, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			EnableOpenAIStrictLineageCapture(c)
			svc := &OpenAIGatewayService{cfg: &config.Config{}}
			resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}}

			output, complete, err := tt.run(svc, c, resp)
			require.NoError(t, err)
			require.False(t, complete)
			require.Empty(t, output)
		})
	}
}

func TestReconstructResponseLineageOutputFromSSEPreservesAllDoneItems(t *testing.T) {
	body := strings.Join([]string{
		`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"SAFE"}`,
		`data: {"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"SAFE"}]}}`,
		`data: {"type":"response.output_item.done","output_index":1,"item":{"id":"tool_1","type":"custom_tool_call","call_id":"call_1","name":"exec","input":"dangerous command","status":"completed"}}`,
		`data: {"type":"response.completed","response":{"id":"resp_mixed","status":"completed","output":[]}}`,
	}, "\n")

	output, reconstructed, complete := reconstructResponseLineageOutputFromSSE(body)
	require.True(t, complete)
	require.True(t, reconstructed)
	items := gjson.ParseBytes(output).Array()
	require.Len(t, items, 2)
	require.Equal(t, "SAFE", items[0].Get("content.0.text").String())
	require.Equal(t, "dangerous command", items[1].Get("input").String())

	_, _, complete = reconstructResponseLineageOutputFromSSE(partialMixedOpenAIResponsesSSE())
	require.False(t, complete)
}

func TestOpenAIHTTPNonStreamingTransportsWithholdSuccessWhenLineageCommitFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name string
		run  func(*OpenAIGatewayService, *gin.Context, *http.Response) (string, bool, bool, error)
	}{
		{
			name: "native",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) (string, bool, bool, error) {
				result, err := svc.handleNonStreamingResponse(context.Background(), resp, c, &Account{ID: 1, Type: AccountTypeAPIKey}, "gpt-5.4", "gpt-5.4")
				if result == nil {
					return "", false, false, err
				}
				return result.responseID, result.lineageComplete, result.usage != nil, err
			},
		},
		{
			name: "passthrough",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) (string, bool, bool, error) {
				result, err := svc.handleNonStreamingResponsePassthrough(context.Background(), resp, c, "gpt-5.4", "gpt-5.4")
				if result == nil {
					return "", false, false, err
				}
				return result.responseID, result.lineageComplete, result.usage != nil, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			responseID := "resp_http_nonstream_" + tt.name
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			EnableOpenAIStrictLineageCapture(c)
			commitCause := errors.New("lineage store unavailable")
			observed := make(chan openAIStrictLineageCommitObservation, 1)
			SetOpenAIStrictLineageCommitter(c, observeFailingOpenAIStrictLineageCommit(observed, commitCause))
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(openAIResponsesNonStreamingLineageBody(responseID))),
			}
			svc := &OpenAIGatewayService{cfg: &config.Config{}}

			gotResponseID, complete, hasUsage, err := tt.run(svc, c, resp)
			require.Error(t, err)
			require.ErrorIs(t, err, commitCause)
			var commitErr *OpenAIStrictLineageCommitError
			require.ErrorAs(t, err, &commitErr)
			require.Equal(t, responseID, gotResponseID)
			require.True(t, complete)
			require.True(t, hasUsage, "commit failure must return partial usage for billing")
			requireReconstructedOpenAIWSLineage(t, <-observed, responseID, 1)
			require.False(t, c.Writer.Written(), "success status and body must remain uncommitted")
			require.Empty(t, recorder.Body.String())
		})
	}
}

func TestOpenAIHTTPStreamingTransportsWithholdSuccessTerminalWhenLineageCommitFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name string
		run  func(*OpenAIGatewayService, *gin.Context, *http.Response) (string, bool, bool, error)
	}{
		{
			name: "native",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) (string, bool, bool, error) {
				result, err := svc.handleStreamingResponse(context.Background(), resp, c, &Account{ID: 1, Platform: PlatformOpenAI}, time.Now(), "gpt-5.4", "gpt-5.4")
				if result == nil {
					return "", false, false, err
				}
				return result.responseID, result.lineageComplete, result.usage != nil, err
			},
		},
		{
			name: "passthrough",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) (string, bool, bool, error) {
				result, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, &Account{ID: 1}, time.Now(), "gpt-5.4", "gpt-5.4")
				if result == nil {
					return "", false, false, err
				}
				return result.responseID, result.lineageComplete, result.usage != nil, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			responseID := "resp_http_stream_" + tt.name
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			EnableOpenAIStrictLineageCapture(c)
			commitCause := errors.New("lineage store unavailable")
			observed := make(chan openAIStrictLineageCommitObservation, 1)
			SetOpenAIStrictLineageCommitter(c, observeFailingOpenAIStrictLineageCommit(observed, commitCause))
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(openAIWSReconstructedLineageSSE(responseID))),
			}
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}

			gotResponseID, complete, hasUsage, err := tt.run(svc, c, resp)
			require.Error(t, err)
			require.ErrorIs(t, err, commitCause)
			var commitErr *OpenAIStrictLineageCommitError
			require.ErrorAs(t, err, &commitErr)
			require.Equal(t, responseID, gotResponseID)
			require.True(t, complete)
			require.True(t, hasUsage, "commit failure must return partial usage for billing")
			requireReconstructedOpenAIWSLineage(t, <-observed, responseID, 1)
			eventTypes := openAISSEEventTypes(recorder.Body.String())
			require.Contains(t, eventTypes, "response.output_text.delta")
			require.NotContains(t, eventTypes, "response.completed")
			require.NotContains(t, eventTypes, "response.done")
			require.NotContains(t, recorder.Body.String(), "data: [DONE]")
		})
	}
}

func TestOpenAIHTTPStreamingTransportsCommitOnceBeforeSuccessTerminal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	transports := []struct {
		name string
		run  func(*OpenAIGatewayService, *gin.Context, *http.Response) (string, bool, error)
	}{
		{
			name: "native",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) (string, bool, error) {
				result, err := svc.handleStreamingResponse(context.Background(), resp, c, &Account{ID: 1, Platform: PlatformOpenAI}, time.Now(), "gpt-5.4", "gpt-5.4")
				if result == nil {
					return "", false, err
				}
				return result.responseID, result.lineageComplete, err
			},
		},
		{
			name: "passthrough",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) (string, bool, error) {
				result, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, &Account{ID: 1}, time.Now(), "gpt-5.4", "gpt-5.4")
				if result == nil {
					return "", false, err
				}
				return result.responseID, result.lineageComplete, err
			},
		},
	}
	terminations := []struct {
		name        string
		includeDone bool
	}{
		{name: "done_sentinel", includeDone: true},
		{name: "clean_eof", includeDone: false},
	}

	for _, termination := range terminations {
		for _, transport := range transports {
			t.Run(termination.name+"/"+transport.name, func(t *testing.T) {
				responseID := "resp_http_success_" + termination.name + "_" + transport.name
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				EnableOpenAIStrictLineageCapture(c)
				observed := make([]openAIStrictLineageCommitObservation, 0, 1)
				terminalVisibleAtCommit := false
				SetOpenAIStrictLineageCommitter(c, func(turn int, result *OpenAIForwardResult) error {
					terminalVisibleAtCommit = strings.Contains(recorder.Body.String(), `"type":"response.completed"`) ||
						strings.Contains(recorder.Body.String(), `"type":"response.done"`)
					output, complete := result.OpenAIResponsesLineageOutput()
					observed = append(observed, openAIStrictLineageCommitObservation{
						turn: turn, responseID: result.ResponseID, output: output, complete: complete,
					})
					return nil
				})
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
					Body:       io.NopCloser(strings.NewReader(openAIWSReconstructedLineageSSEWithDone(responseID, termination.includeDone))),
				}
				cfg := &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}
				if transport.name == "native" {
					cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 5
				}
				svc := &OpenAIGatewayService{cfg: cfg}

				gotResponseID, complete, err := transport.run(svc, c, resp)
				require.NoError(t, err)
				require.Equal(t, responseID, gotResponseID)
				require.True(t, complete)
				require.Len(t, observed, 1)
				requireReconstructedOpenAIWSLineage(t, observed[0], responseID, 1)
				require.False(t, terminalVisibleAtCommit, "success terminal must be written only after lineage persistence")
				eventTypes := openAISSEEventTypes(recorder.Body.String())
				require.Equal(t, 1, countOpenAISSEEventType(eventTypes, "response.completed"))
				require.NotContains(t, eventTypes, "response.done")
				createdIndex := indexOpenAISSEEventType(eventTypes, "response.created")
				deltaIndex := indexOpenAISSEEventType(eventTypes, "response.output_text.delta")
				completedIndex := indexOpenAISSEEventType(eventTypes, "response.completed")
				require.Equal(t, 0, createdIndex, "guarded response.created preamble must not be lost")
				require.Greater(t, deltaIndex, createdIndex)
				require.Greater(t, completedIndex, deltaIndex)
				terminalOffset := strings.Index(recorder.Body.String(), `"type":"response.completed"`)
				require.NotEqual(t, -1, terminalOffset)
				doneOffset := strings.Index(recorder.Body.String(), "data: [DONE]")
				if termination.includeDone {
					require.Greater(t, doneOffset, terminalOffset, "[DONE] must follow the committed success terminal")
				} else {
					require.Equal(t, -1, doneOffset, "clean EOF must not synthesize an upstream [DONE] sentinel")
				}
			})
		}
	}
}

func TestOpenAIHTTPStreamingTransportsDrainTailBeforeLineageCommit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name string
		run  func(*OpenAIGatewayService, *gin.Context, *http.Response) (bool, error)
	}{
		{
			name: "native",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) (bool, error) {
				result, err := svc.handleStreamingResponse(context.Background(), resp, c, &Account{ID: 1, Platform: PlatformOpenAI}, time.Now(), "gpt-5.4", "gpt-5.4")
				return result != nil, err
			},
		},
		{
			name: "passthrough",
			run: func(svc *OpenAIGatewayService, c *gin.Context, resp *http.Response) (bool, error) {
				result, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, &Account{ID: 1}, time.Now(), "gpt-5.4", "gpt-5.4")
				return result != nil, err
			},
		},
	}
	tails := []struct {
		name string
		body func(string) string
	}{
		{name: "duplicate_terminal", body: openAIWSDuplicateTerminalLineageSSE},
		{name: "ordinary_data", body: openAIWSOrdinaryTailAfterTerminalLineageSSE},
	}

	for _, tail := range tails {
		for _, tt := range tests {
			t.Run(tail.name+"/"+tt.name, func(t *testing.T) {
				responseID := "resp_http_tail_" + tail.name + "_" + tt.name
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				EnableOpenAIStrictLineageCapture(c)
				commitCause := errors.New("ambiguous lineage output")
				observed := make([]openAIStrictLineageCommitObservation, 0, 1)
				SetOpenAIStrictLineageCommitter(c, func(turn int, result *OpenAIForwardResult) error {
					output, complete := result.OpenAIResponsesLineageOutput()
					observed = append(observed, openAIStrictLineageCommitObservation{
						turn: turn, responseID: result.ResponseID, output: output, complete: complete,
					})
					if !complete {
						return commitCause
					}
					return nil
				})
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
					Body:       io.NopCloser(strings.NewReader(tail.body(responseID))),
				}
				svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}

				hasPartialResult, err := tt.run(svc, c, resp)
				require.Error(t, err)
				require.ErrorIs(t, err, commitCause)
				var commitErr *OpenAIStrictLineageCommitError
				require.ErrorAs(t, err, &commitErr)
				require.True(t, hasPartialResult)
				require.Len(t, observed, 1, "lineage must be committed only after the stream tail is fully drained")
				require.Equal(t, responseID, observed[0].responseID)
				require.False(t, observed[0].complete, "data after a success terminal makes lineage ambiguous")
				eventTypes := openAISSEEventTypes(recorder.Body.String())
				require.Contains(t, eventTypes, "response.output_text.delta")
				require.NotContains(t, eventTypes, "response.completed")
				require.NotContains(t, eventTypes, "response.done")
				require.NotContains(t, recorder.Body.String(), "data: [DONE]")
			})
		}
	}
}

func TestOpenAIResponsesChatFallbackWithholdsNonStreamingSuccessWhenLineageCommitFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	EnableOpenAIStrictLineageCapture(c)
	commitCause := errors.New("lineage store unavailable")
	observed := make(chan openAIStrictLineageCommitObservation, 1)
	SetOpenAIStrictLineageCommitter(c, observeFailingOpenAIStrictLineageCommit(observed, commitCause))
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
			"x-request-id": []string{"rid_chat_fallback_commit"},
		},
		Body: io.NopCloser(strings.NewReader(
			`{"id":"chatcmpl_commit","object":"chat.completion","model":"gpt-5.4","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`,
		)),
	}
	svc := &OpenAIGatewayService{cfg: &config.Config{}}

	result, err := svc.bufferChatCompletionsAsResponses(
		c, resp, "gpt-5.4", nil, false, nil, "gpt-5.4", "gpt-5.4", nil, nil, time.Now(),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, commitCause)
	var commitErr *OpenAIStrictLineageCommitError
	require.ErrorAs(t, err, &commitErr)
	require.NotNil(t, result)
	require.Equal(t, "chatcmpl_commit", result.ResponseID)
	require.Equal(t, 3, result.Usage.InputTokens)
	require.Equal(t, 2, result.Usage.OutputTokens)
	commit := <-observed
	require.Equal(t, 1, commit.turn)
	require.Equal(t, "chatcmpl_commit", commit.responseID)
	require.False(t, commit.complete, "chat fallback cannot create strict continuation lineage")
	require.Empty(t, commit.output)
	require.False(t, c.Writer.Written(), "synthesized success must remain uncommitted")
	require.Empty(t, recorder.Body.String())
}

func TestOpenAIResponsesChatFallbackWithholdsStreamingSuccessTerminalWhenLineageCommitFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	EnableOpenAIStrictLineageCapture(c)
	commitCause := errors.New("lineage store unavailable")
	observed := make(chan openAIStrictLineageCommitObservation, 1)
	SetOpenAIStrictLineageCommitter(c, observeFailingOpenAIStrictLineageCommit(observed, commitCause))
	upstreamBody := strings.Join([]string{
		`data: {"id":"chatcmpl_stream_commit","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_stream_commit","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"content":"SAFE_OK"},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_stream_commit","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		"",
		`data: {"id":"chatcmpl_stream_commit","object":"chat.completion.chunk","model":"gpt-5.4","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": []string{"text/event-stream"},
			"x-request-id": []string{"rid_chat_fallback_stream_commit"},
		},
		Body: io.NopCloser(strings.NewReader(upstreamBody)),
	}
	svc := &OpenAIGatewayService{cfg: &config.Config{}}

	result, err := svc.streamChatCompletionsAsResponses(
		c, resp, "gpt-5.4", nil, false, nil, "gpt-5.4", "gpt-5.4", nil, nil, time.Now(),
	)
	require.Error(t, err)
	require.ErrorIs(t, err, commitCause)
	var commitErr *OpenAIStrictLineageCommitError
	require.ErrorAs(t, err, &commitErr)
	require.NotNil(t, result)
	require.Equal(t, "chatcmpl_stream_commit", result.ResponseID)
	require.Equal(t, 3, result.Usage.InputTokens)
	require.Equal(t, 2, result.Usage.OutputTokens)
	commit := <-observed
	require.Equal(t, 1, commit.turn)
	require.Equal(t, "chatcmpl_stream_commit", commit.responseID)
	require.False(t, commit.complete, "chat fallback cannot create strict continuation lineage")
	require.Empty(t, commit.output)
	require.Contains(t, openAISSEEventTypes(recorder.Body.String()), "response.output_text.delta")
	require.NotContains(t, recorder.Body.String(), "event: response.completed")
	require.NotContains(t, recorder.Body.String(), "data: [DONE]")
}

func TestOpenAIWSHTTPBridgeRejectsMismatchedCreatedAndTerminalLineageIDs(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(mismatchedOpenAIResponsesSSE())),
	}}
	svc := &OpenAIGatewayService{
		cfg:          &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream: upstream,
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	EnableOpenAIStrictLineageCapture(c)
	payload := []byte(`{"type":"response.create","model":"gpt-5.4","stream":true,"input":"hello"}`)

	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(
		context.Background(), c,
		&Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1},
		"sk-test", payload, len(payload), "gpt-5.4", "", "", "", "", 1,
		func([]byte) error { return nil },
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "resp_created", result.ResponseID)
	_, complete := result.OpenAIResponsesLineageOutput()
	require.False(t, complete)
	require.False(t, result.CompletedForLineage())
}

func TestOpenAIWSHTTPBridgeRejectsTerminalTailAfterDoneLineage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_duplicate","status":"in_progress"}}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_duplicate","status":"completed","output":[]}}`,
		"",
		`data: [DONE]`,
		"",
		`data: {"type":"response.done","response":{"id":"resp_duplicate","status":"done","output":[]}}`,
		"",
	}, "\n")
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}}
	svc := &OpenAIGatewayService{
		cfg:          &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream: upstream,
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	EnableOpenAIStrictLineageCapture(c)
	payload := []byte(`{"type":"response.create","model":"gpt-5.4","stream":true,"input":"hello"}`)

	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(
		context.Background(), c,
		&Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1},
		"sk-test", payload, len(payload), "gpt-5.4", "", "", "", "", 1,
		func([]byte) error { return nil },
	)
	require.NoError(t, err)
	require.NotNil(t, result)
	_, complete := result.OpenAIResponsesLineageOutput()
	require.False(t, complete)
	require.False(t, result.CompletedForLineage())
}

func TestOpenAIWSV2RejectsConcatenatedTerminalTailLineage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
	c.Request.Header.Set("User-Agent", "codex_cli_rs/0.98.0")
	EnableOpenAIStrictLineageCapture(c)

	cfg := newOpenAIWSV2TestConfig()
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	firstTerminal := `{"type":"response.completed","response":{"id":"resp_ws_tail","status":"completed","model":"gpt-5.4","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"first terminal"}]}],"usage":{"input_tokens":1,"output_tokens":1}}}`
	tailTerminal := `{"type":"response.done","response":{"id":"resp_ws_tail","status":"done","model":"gpt-5.4","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ambiguous tail"}]}]}}`
	captureConn := &openAIWSCaptureConn{events: [][]byte{[]byte(firstTerminal + tailTerminal)}}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(&openAIWSCaptureDialer{conn: captureConn})
	defer pool.Close()
	svc := &OpenAIGatewayService{
		cfg: cfg, httpUpstream: &httpUpstreamRecorder{}, cache: &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), toolCorrector: NewCodexToolCorrector(), openaiWSPool: pool,
	}
	account := &Account{
		ID: 203, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test"},
		Extra:       map[string]any{"responses_websockets_v2_enabled": true},
	}

	result, err := svc.Forward(context.Background(), c, account, []byte(`{"model":"gpt-5.4","stream":false,"input":"hello"}`))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "resp_ws_tail", result.ResponseID)
	_, complete := result.OpenAIResponsesLineageOutput()
	require.False(t, complete)
	require.False(t, result.CompletedForLineage())
}

func TestOpenAIWSV2ReconstructsLineageAndWithholdsSuccessTerminalWhenCommitFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
	c.Request.Header.Set("User-Agent", "codex_cli_rs/0.98.0")
	EnableOpenAIStrictLineageCapture(c)
	commitCause := errors.New("lineage store unavailable")
	observed := make(chan openAIStrictLineageCommitObservation, 1)
	SetOpenAIStrictLineageCommitter(c, observeFailingOpenAIStrictLineageCommit(observed, commitCause))

	cfg := newOpenAIWSV2TestConfig()
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	captureConn := &openAIWSCaptureConn{events: openAIWSReconstructedLineageEvents("resp_ws_lineage")}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(&openAIWSCaptureDialer{conn: captureConn})
	defer pool.Close()
	svc := &OpenAIGatewayService{
		cfg: cfg, httpUpstream: &httpUpstreamRecorder{}, cache: &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), toolCorrector: NewCodexToolCorrector(), openaiWSPool: pool,
	}
	account := &Account{
		ID: 204, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test"},
		Extra:       map[string]any{"responses_websockets_v2_enabled": true},
	}

	result, err := svc.Forward(context.Background(), c, account, []byte(`{"model":"gpt-5.4","stream":true,"input":"hello"}`))
	require.Error(t, err)
	require.ErrorIs(t, err, commitCause)
	var commitErr *OpenAIStrictLineageCommitError
	require.ErrorAs(t, err, &commitErr)
	require.NotNil(t, result)
	requireReconstructedOpenAIWSLineage(t, <-observed, "resp_ws_lineage", 1)
	require.Contains(t, recorder.Body.String(), `"type":"response.output_text.delta"`)
	require.NotContains(t, recorder.Body.String(), `"type":"response.completed"`)
}

func TestOpenAIWSHTTPBridgeReconstructsLineageAndWithholdsSuccessTerminalWhenCommitFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &httpUpstreamRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(openAIWSReconstructedLineageSSE("resp_bridge_lineage"))),
	}}
	svc := &OpenAIGatewayService{
		cfg:          &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream: upstream,
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	EnableOpenAIStrictLineageCapture(c)
	commitCause := errors.New("lineage store unavailable")
	observed := make(chan openAIStrictLineageCommitObservation, 1)
	SetOpenAIStrictLineageCommitter(c, observeFailingOpenAIStrictLineageCommit(observed, commitCause))
	payload := []byte(`{"type":"response.create","model":"gpt-5.4","stream":true,"input":"hello"}`)
	var downstream [][]byte

	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(
		context.Background(), c,
		&Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Concurrency: 1},
		"sk-test", payload, len(payload), "gpt-5.4", "", "", "", "", 2,
		func(message []byte) error {
			downstream = append(downstream, append([]byte(nil), message...))
			return nil
		},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, commitCause)
	var commitErr *OpenAIStrictLineageCommitError
	require.ErrorAs(t, err, &commitErr)
	require.NotNil(t, result)
	requireReconstructedOpenAIWSLineage(t, <-observed, "resp_bridge_lineage", 2)
	var sawDelta bool
	for _, message := range downstream {
		eventType := gjson.GetBytes(message, "type").String()
		sawDelta = sawDelta || eventType == "response.output_text.delta"
		require.NotEqual(t, "response.completed", eventType)
		require.NotEqual(t, "response.done", eventType)
	}
	require.True(t, sawDelta)
}

func TestOpenAIWSIngressReconstructsLineageAndWithholdsSuccessTerminalWhenCommitFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := newOpenAIWSV2TestConfig()
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	captureConn := &openAIWSCaptureConn{events: openAIWSReconstructedLineageEvents("resp_ingress_lineage")}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(&openAIWSCaptureDialer{conn: captureConn})
	defer pool.Close()
	svc := &OpenAIGatewayService{
		cfg: cfg, httpUpstream: &httpUpstreamRecorder{}, cache: &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), toolCorrector: NewCodexToolCorrector(), openaiWSPool: pool,
	}
	account := &Account{
		ID: 205, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test"},
		Extra:       map[string]any{"responses_websockets_v2_enabled": true},
	}
	commitCause := errors.New("lineage store unavailable")
	observed := make(chan openAIStrictLineageCommitObservation, 1)
	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, acceptErr := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if acceptErr != nil {
			serverErr <- acceptErr
			return
		}
		defer func() { _ = conn.CloseNow() }()
		readCtx, cancelRead := context.WithTimeout(r.Context(), 3*time.Second)
		msgType, firstMessage, readErr := conn.Read(readCtx)
		cancelRead()
		if readErr != nil {
			serverErr <- readErr
			return
		}
		if msgType != coderws.MessageText {
			serverErr <- errors.New("first message was not text")
			return
		}
		ginRecorder := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(ginRecorder)
		ginCtx.Request = r.Clone(r.Context())
		ginCtx.Request.Header = r.Header.Clone()
		ginCtx.Request.Header.Set("User-Agent", "unit-test-agent/1.0")
		EnableOpenAIStrictLineageCapture(ginCtx)
		SetOpenAIStrictLineageCommitter(ginCtx, observeFailingOpenAIStrictLineageCommit(observed, commitCause))
		serverErr <- svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, nil)
	}))
	defer server.Close()
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	cancelDial()
	require.NoError(t, err)
	defer func() { _ = clientConn.CloseNow() }()
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.4","stream":true,"input":"hello"}`))
	cancelWrite()
	require.NoError(t, err)

	var eventTypes []string
	for {
		readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
		_, message, readErr := clientConn.Read(readCtx)
		cancelRead()
		if readErr != nil {
			break
		}
		eventTypes = append(eventTypes, gjson.GetBytes(message, "type").String())
	}
	select {
	case err := <-serverErr:
		require.ErrorIs(t, err, commitCause)
		var commitErr *OpenAIStrictLineageCommitError
		require.ErrorAs(t, err, &commitErr)
	case <-time.After(3 * time.Second):
		t.Fatal("ingress lineage commit failure did not terminate relay")
	}
	requireReconstructedOpenAIWSLineage(t, <-observed, "resp_ingress_lineage", 1)
	require.Contains(t, eventTypes, "response.output_text.delta")
	require.NotContains(t, eventTypes, "response.completed")
	require.NotContains(t, eventTypes, "response.done")
}

func TestOpenAIWSPassthroughReconstructsLineageAndWithholdsSuccessTerminalWhenCommitFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controlCtx, cancelControl := context.WithCancelCause(context.Background())
	defer cancelControl(context.Canceled)
	upstream := newStagedPassthroughConn()
	go func() {
		for _, event := range openAIWSReconstructedLineageEvents("resp_passthrough_lineage") {
			upstream.Send(string(event))
		}
	}()
	commitCause := errors.New("lineage store unavailable")
	observed := make(chan openAIStrictLineageCommitObservation, 1)
	serverErr := make(chan error, 1)
	svc := newPassthroughLifecycleService(passthroughLifecycleConfig(), upstream)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, acceptErr := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if acceptErr != nil {
			serverErr <- acceptErr
			return
		}
		defer func() { _ = conn.CloseNow() }()
		msgType, firstMessage, readErr := ReadOpenAIWSClientMessage(
			controlCtx, conn, 3*time.Second, coderws.StatusPolicyViolation, "missing first response.create message",
		)
		if readErr != nil {
			serverErr <- readErr
			return
		}
		if msgType != coderws.MessageText {
			serverErr <- errors.New("first message was not text")
			return
		}
		ginRecorder := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(ginRecorder)
		ginCtx.Request = r.Clone(controlCtx)
		EnableOpenAIStrictLineageCapture(ginCtx)
		SetOpenAIStrictLineageCommitter(ginCtx, observeFailingOpenAIStrictLineageCommit(observed, commitCause))
		serverErr <- svc.ProxyResponsesWebSocketFromClient(controlCtx, ginCtx, conn, passthroughLifecycleAccount(), "sk-test", firstMessage, nil)
	}))
	defer server.Close()
	clientConn := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = clientConn.CloseNow() }()

	var eventTypes []string
	for {
		message, readErr := readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
		if readErr != nil {
			break
		}
		eventTypes = append(eventTypes, gjson.GetBytes(message, "type").String())
	}
	select {
	case err := <-serverErr:
		require.ErrorIs(t, err, commitCause)
		var commitErr *OpenAIStrictLineageCommitError
		require.ErrorAs(t, err, &commitErr)
	case <-time.After(3 * time.Second):
		t.Fatal("passthrough lineage commit failure did not terminate relay")
	}
	requireReconstructedOpenAIWSLineage(t, <-observed, "resp_passthrough_lineage", 1)
	require.Contains(t, eventTypes, "response.output_text.delta")
	require.NotContains(t, eventTypes, "response.completed")
	require.NotContains(t, eventTypes, "response.done")
}
