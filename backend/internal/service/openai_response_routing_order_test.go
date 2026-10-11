package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type openAIResponseRoutingOrderWriter struct {
	gin.ResponseWriter
	store              *streamingResponseBindingOrderStore
	responseID         string
	wroteBeforeBinding bool
}

func (w *openAIResponseRoutingOrderWriter) Write(data []byte) (int, error) {
	if bytes.Contains(data, []byte(w.responseID)) && !w.store.routingPairBound() {
		w.wroteBeforeBinding = true
	}
	return w.ResponseWriter.Write(data)
}

func (w *openAIResponseRoutingOrderWriter) WriteString(data string) (int, error) {
	return w.Write([]byte(data))
}

func newOpenAIResponseRoutingOrderContext(t *testing.T, store *streamingResponseBindingOrderStore, responseID string) (*gin.Context, *openAIResponseRoutingOrderWriter) {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
	groupID := int64(66101)
	c.Set("api_key", &APIKey{GroupID: &groupID})
	writer := &openAIResponseRoutingOrderWriter{
		ResponseWriter: c.Writer,
		store:          store,
		responseID:     responseID,
	}
	c.Writer = writer
	return c, writer
}

func newOpenAIResponseRoutingOrderAccount() *Account {
	const accountID int64 = 66102
	return &Account{
		ID:          accountID,
		Name:        "response-routing-order",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test"},
		SelectedEgress: &ResolvedAccountEgress{
			BindingID: StableAccountEgressBindingID(accountID, 91),
			RouteID:   91,
		},
	}
}

func TestOpenAIHTTPNonStreamingBindsRoutingBeforeResponseIDWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const responseID = "resp_nonstream_binding_order"
	jsonBody := `{"id":"` + responseID + `","object":"response","status":"completed","model":"gpt-5","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`
	sseBody := strings.Join([]string{
		`event: response.completed`,
		`data: {"type":"response.completed","response":{"id":"` + responseID + `","object":"response","status":"completed","model":"gpt-5","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
		``,
	}, "\n")

	tests := []struct {
		name        string
		contentType string
		body        string
		run         func(*OpenAIGatewayService, context.Context, *http.Response, *gin.Context, *Account) error
	}{
		{
			name:        "standard_json",
			contentType: "application/json",
			body:        jsonBody,
			run: func(s *OpenAIGatewayService, ctx context.Context, resp *http.Response, c *gin.Context, account *Account) error {
				_, err := s.handleNonStreamingResponse(ctx, resp, c, account, "gpt-5", "gpt-5")
				return err
			},
		},
		{
			name:        "standard_sse_to_json",
			contentType: "text/event-stream",
			body:        sseBody,
			run: func(s *OpenAIGatewayService, ctx context.Context, resp *http.Response, c *gin.Context, account *Account) error {
				_, err := s.handleNonStreamingResponse(ctx, resp, c, account, "gpt-5", "gpt-5")
				return err
			},
		},
		{
			name:        "passthrough_json",
			contentType: "application/json",
			body:        jsonBody,
			run: func(s *OpenAIGatewayService, ctx context.Context, resp *http.Response, c *gin.Context, account *Account) error {
				_, err := s.handleNonStreamingResponsePassthrough(ctx, resp, c, account, "gpt-5", "gpt-5")
				return err
			},
		},
		{
			name:        "passthrough_sse_to_json",
			contentType: "text/event-stream",
			body:        sseBody,
			run: func(s *OpenAIGatewayService, ctx context.Context, resp *http.Response, c *gin.Context, account *Account) error {
				_, err := s.handleNonStreamingResponsePassthrough(ctx, resp, c, account, "gpt-5", "gpt-5")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newStreamingResponseBindingOrderStore()
			c, writer := newOpenAIResponseRoutingOrderContext(t, store, responseID)
			svc := &OpenAIGatewayService{
				cfg:                &config.Config{},
				openaiWSStateStore: store,
				toolCorrector:      NewCodexToolCorrector(),
			}
			account := newOpenAIResponseRoutingOrderAccount()
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{tt.contentType}},
				Body:       io.NopCloser(strings.NewReader(tt.body)),
			}

			err := tt.run(svc, c.Request.Context(), resp, c, account)

			require.NoError(t, err)
			require.False(t, writer.wroteBeforeBinding)
			accountBinds, egressBinds := store.bindCounts()
			require.Equal(t, 1, accountBinds)
			require.Equal(t, 1, egressBinds)
		})
	}
}

func TestOpenAIHTTPOverWebSocketBindsRoutingBeforeResponseIDWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const responseID = "resp_http_over_ws_binding_order"

	for _, stream := range []bool{false, true} {
		t.Run("stream_"+strconv.FormatBool(stream), func(t *testing.T) {
			store := newStreamingResponseBindingOrderStore()
			c, writer := newOpenAIResponseRoutingOrderContext(t, store, responseID)
			cfg := newOpenAIWSV2TestConfig()
			cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
			captureConn := &openAIWSCaptureConn{events: [][]byte{
				[]byte(`{"type":"response.created","response":{"id":"` + responseID + `","model":"gpt-5.1","status":"in_progress"}}`),
				[]byte(`{"type":"response.output_text.delta","response_id":"` + responseID + `","delta":"ok"}`),
				[]byte(`{"type":"response.completed","response":{"id":"` + responseID + `","model":"gpt-5.1","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`),
			}}
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(&openAIWSCaptureDialer{conn: captureConn})
			defer pool.Close()
			svc := &OpenAIGatewayService{
				cfg:                cfg,
				httpUpstream:       &httpUpstreamRecorder{},
				cache:              &stubGatewayCache{},
				openaiWSResolver:   NewOpenAIWSProtocolResolver(cfg),
				toolCorrector:      NewCodexToolCorrector(),
				openaiWSPool:       pool,
				openaiWSStateStore: store,
			}
			account := newOpenAIResponseRoutingOrderAccount()
			account.Extra = map[string]any{"responses_websockets_v2_enabled": true}
			body := []byte(`{"model":"gpt-5.1","stream":` + strconv.FormatBool(stream) + `,"input":"hi"}`)

			result, err := svc.Forward(context.Background(), c, account, body)

			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, responseID, result.RequestID)
			require.False(t, writer.wroteBeforeBinding)
			accountBinds, egressBinds := store.bindCounts()
			require.Equal(t, 1, accountBinds)
			require.Equal(t, 1, egressBinds)
			_, connBound := store.GetResponseConn(responseID)
			require.True(t, connBound)
		})
	}
}

func TestOpenAIWSHTTPBridgeBindsRoutingBeforeResponseIDWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const responseID = "resp_http_bridge_binding_order"
	sse := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"` + responseID + `","status":"in_progress"}}`,
		``,
		`data: {"type":"response.output_text.delta","response_id":"` + responseID + `","delta":"ok"}`,
		``,
		`data: {"type":"response.completed","response":{"id":"` + responseID + `","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`,
		``,
	}, "\n")
	store := newStreamingResponseBindingOrderStore()
	c, _ := newOpenAIResponseRoutingOrderContext(t, store, responseID)
	svc := &OpenAIGatewayService{
		cfg:                &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}},
		httpUpstream:       &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(sse))}},
		openaiWSStateStore: store,
		toolCorrector:      NewCodexToolCorrector(),
	}
	account := newOpenAIResponseRoutingOrderAccount()
	wroteBeforeBinding := false

	result, err := svc.proxyOpenAIWSHTTPBridgeTurn(
		context.Background(), c, account, "sk-test",
		[]byte(`{"type":"response.create","model":"gpt-5","stream":true,"input":"hi"}`),
		75, "gpt-5", "", "", "", "", 1,
		func(message []byte) error {
			if bytes.Contains(message, []byte(responseID)) && !store.routingPairBound() {
				wroteBeforeBinding = true
			}
			return nil
		},
	)

	require.NoError(t, err)
	require.NotNil(t, result)
	require.False(t, wroteBeforeBinding)
	accountBinds, egressBinds := store.bindCounts()
	require.Equal(t, 1, accountBinds)
	require.Equal(t, 1, egressBinds)
	require.Less(t, result.Duration, 5*time.Second)
}
