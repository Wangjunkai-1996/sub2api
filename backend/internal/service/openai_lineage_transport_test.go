package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

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
