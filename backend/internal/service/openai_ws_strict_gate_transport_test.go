package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type strictPassthroughCountingDialer struct {
	calls atomic.Int32
}

func (d *strictPassthroughCountingDialer) Dial(context.Context, string, http.Header, string) (openAIWSClientConn, int, http.Header, error) {
	d.calls.Add(1)
	return nil, 0, nil, errors.New("strict passthrough must not dial upstream")
}

func strictGateReplaySignals(payload []byte) (assistantText, functionArguments bool) {
	for _, item := range gjson.GetBytes(payload, "input").Array() {
		switch item.Get("type").String() {
		case "message":
			for _, part := range item.Get("content").Array() {
				if strings.Contains(part.Get("text").String(), "dangerous assistant context") {
					assistantText = true
				}
			}
		case "function_call":
			if strings.Contains(item.Get("arguments").String(), "dangerous function arguments") {
				functionArguments = true
			}
		}
	}
	return assistantText, functionArguments
}

func strictGateFirstTurnTerminal(responseID string) string {
	return `data: {"type":"response.completed","response":{"id":"` + responseID + `","status":"completed","model":"gpt-5.4","output":[{"type":"function_call","id":"fc_strict_gate","call_id":"call_strict_gate","name":"shell","arguments":"{\"cmd\":\"dangerous function arguments\"}"}],"usage":{"input_tokens":1,"output_tokens":1}}}` + "\n\n"
}

func strictGateSecondTurnPayload(previousResponseID string) string {
	return `{"type":"response.create","model":"gpt-5.4","stream":false,"store":false,"previous_response_id":"` + previousResponseID + `","input":[{"type":"message","role":"assistant","content":[{"type":"input_text","text":"dangerous assistant context"}]},{"type":"function_call","id":"fc_strict_gate_client","call_id":"call_strict_gate_client","name":"shell","arguments":"{\"cmd\":\"dangerous function arguments\"}"},{"type":"function_call_output","call_id":"call_strict_gate","output":"ok"}]}`
}

func strictGateMixedResponseCreatePayload(previousResponseID string) string {
	return `{"type":"response.create","model":"gpt-5.4","stream":false,"store":false,"previous_response_id":"` + previousResponseID + `","input":"danger","response":{"input":"safe"}}`
}

func runStrictGateTwoTurnSession(
	t *testing.T,
	svc *OpenAIGatewayService,
	account *Account,
	hooks *OpenAIWSIngressHooks,
	secondPayload string,
) ([]byte, error) {
	t.Helper()
	serverErrCh := make(chan error, 1)
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		readCtx, cancelRead := context.WithTimeout(r.Context(), 3*time.Second)
		_, firstMessage, err := conn.Read(readCtx)
		cancelRead()
		if err != nil {
			serverErrCh <- err
			return
		}
		recorder := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(recorder)
		ginCtx.Request = r.Clone(r.Context())
		serverErrCh <- svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, hooks)
	}))
	defer wsServer.Close()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(wsServer.URL, "http"), nil)
	cancelDial()
	require.NoError(t, err)
	defer func() { _ = clientConn.CloseNow() }()

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.4","stream":false,"store":false,"input":"hello"}`))
	cancelWrite()
	require.NoError(t, err)
	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, firstEvent, err := clientConn.Read(readCtx)
	cancelRead()
	require.NoError(t, err)

	writeCtx, cancelWrite = context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(secondPayload))
	cancelWrite()
	require.NoError(t, err)

	select {
	case proxyErr := <-serverErrCh:
		return firstEvent, proxyErr
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for strict audit rejection")
		return nil, nil
	}
}

func TestOpenAIWSPassthroughRejectsStrictAuditBeforeDial(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := passthroughLifecycleConfig()
	dialer := &strictPassthroughCountingDialer{}
	svc := &OpenAIGatewayService{
		cfg:                       cfg,
		httpUpstream:              &httpUpstreamRecorder{},
		cache:                     &stubGatewayCache{},
		openaiWSResolver:          NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:             NewCodexToolCorrector(),
		openaiWSPassthroughDialer: dialer,
	}
	account := passthroughLifecycleAccount()
	serverErrCh := make(chan error, 1)
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		readCtx, cancelRead := context.WithTimeout(r.Context(), 3*time.Second)
		_, firstMessage, err := conn.Read(readCtx)
		cancelRead()
		if err != nil {
			serverErrCh <- err
			return
		}
		recorder := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(recorder)
		ginCtx.Request = r.Clone(r.Context())
		serverErrCh <- svc.ProxyResponsesWebSocketFromClient(
			r.Context(),
			ginCtx,
			conn,
			account,
			"sk-test",
			firstMessage,
			&OpenAIWSIngressHooks{StrictAudit: true},
		)
	}))
	defer wsServer.Close()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(wsServer.URL, "http"), nil)
	cancelDial()
	require.NoError(t, err)
	defer func() { _ = clientConn.CloseNow() }()

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.4","input":"safe"}`))
	cancelWrite()
	require.NoError(t, err)

	select {
	case proxyErr := <-serverErrCh:
		require.ErrorIs(t, proxyErr, ErrOpenAIWSStrictAuditPassthroughUnsupported)
		var closeErr *OpenAIWSClientCloseError
		require.ErrorAs(t, proxyErr, &closeErr)
		require.Equal(t, coderws.StatusTryAgainLater, closeErr.StatusCode())
	case <-time.After(3 * time.Second):
		t.Fatal("strict passthrough rejection did not return")
	}
	require.Zero(t, dialer.calls.Load(), "strict passthrough must fail before any upstream dial")
}

func TestOpenAIWSHTTPBridgeStrictGateBlocksReplayBeforeTurnAndSecondUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	auditBlockErr := errors.New("strict bridge audit blocked")
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(strictGateFirstTurnTerminal("resp_bridge_strict_gate"))),
		},
		{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(strictGateFirstTurnTerminal("resp_bridge_must_not_dispatch"))),
		},
	}}
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
	cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = true
	cfg.Gateway.OpenAIWS.HTTPBridgeThresholdBytes = 1
	cfg.Gateway.OpenAIWS.ClientFirstMessageTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds = 3
	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     upstream,
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
	}
	account := &Account{
		ID: 201, Name: "strict-http-bridge", Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test"},
		Extra:       map[string]any{"openai_apikey_responses_websockets_v2_mode": OpenAIWSIngressModeHTTPBridge},
	}

	var hookMu sync.Mutex
	auditTurn2Calls := 0
	beforeTurn2Calls := 0
	sawAssistant := false
	sawFunctionArguments := false
	hooks := &OpenAIWSIngressHooks{
		StrictAudit: true,
		BeforeRequest: func(turn int, payload []byte, _ string) error {
			if turn != 2 {
				return nil
			}
			assistant, arguments := strictGateReplaySignals(payload)
			hookMu.Lock()
			auditTurn2Calls++
			sawAssistant = assistant
			sawFunctionArguments = arguments
			hookMu.Unlock()
			return auditBlockErr
		},
		BeforeTurn: func(turn int) error {
			if turn == 2 {
				hookMu.Lock()
				beforeTurn2Calls++
				hookMu.Unlock()
			}
			return nil
		},
	}

	serverErrCh := make(chan error, 1)
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		readCtx, cancelRead := context.WithTimeout(r.Context(), 3*time.Second)
		_, firstMessage, err := conn.Read(readCtx)
		cancelRead()
		if err != nil {
			serverErrCh <- err
			return
		}
		recorder := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(recorder)
		ginCtx.Request = r.Clone(r.Context())
		serverErrCh <- svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, hooks)
	}))
	defer wsServer.Close()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(wsServer.URL, "http"), nil)
	cancelDial()
	require.NoError(t, err)
	defer func() { _ = clientConn.CloseNow() }()

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.4","stream":false,"store":false,"input":"hello"}`))
	cancelWrite()
	require.NoError(t, err)
	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, firstEvent, err := clientConn.Read(readCtx)
	cancelRead()
	require.NoError(t, err)
	require.Equal(t, "resp_bridge_strict_gate", gjson.GetBytes(firstEvent, "response.id").String())

	writeCtx, cancelWrite = context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(strictGateSecondTurnPayload("resp_bridge_strict_gate")))
	cancelWrite()
	require.NoError(t, err)

	select {
	case proxyErr := <-serverErrCh:
		require.ErrorIs(t, proxyErr, auditBlockErr)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for strict HTTP bridge audit block")
	}
	hookMu.Lock()
	require.Equal(t, 1, auditTurn2Calls)
	require.Zero(t, beforeTurn2Calls, "blocked replay must not acquire turn concurrency or billing")
	require.True(t, sawAssistant)
	require.True(t, sawFunctionArguments)
	hookMu.Unlock()
	require.Len(t, upstream.requests, 1, "blocked replay must not dispatch a second HTTP upstream request")
}

func TestOpenAIWSIngressStrictGateBlocksSecondTurnBeforeTurnAndUpstreamWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3

	captureConn := &openAIWSCaptureConn{events: [][]byte{
		[]byte(strings.TrimSpace(strings.TrimPrefix(strictGateFirstTurnTerminal("resp_ingress_strict_gate"), "data: "))),
		[]byte(`{"type":"response.completed","response":{"id":"resp_ingress_must_not_dispatch","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`),
	}}
	dialer := &openAIWSCaptureDialer{conn: captureConn}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)
	defer pool.Close()
	svc := &OpenAIGatewayService{
		cfg: cfg, httpUpstream: &httpUpstreamRecorder{}, cache: &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), toolCorrector: NewCodexToolCorrector(), openaiWSPool: pool,
	}
	account := &Account{
		ID: 202, Name: "strict-ingress", Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test"},
		Extra:       map[string]any{"responses_websockets_v2_enabled": true},
	}

	auditBlockErr := errors.New("strict ingress audit blocked")
	var hookMu sync.Mutex
	auditTurn2Calls := 0
	beforeTurn2Calls := 0
	sawAssistant := false
	sawFunctionArguments := false
	hooks := &OpenAIWSIngressHooks{
		StrictAudit: true,
		BeforeRequest: func(turn int, payload []byte, _ string) error {
			if turn != 2 {
				return nil
			}
			assistant, arguments := strictGateReplaySignals(payload)
			hookMu.Lock()
			auditTurn2Calls++
			sawAssistant = assistant
			sawFunctionArguments = arguments
			hookMu.Unlock()
			return auditBlockErr
		},
		BeforeTurn: func(turn int) error {
			if turn == 2 {
				hookMu.Lock()
				beforeTurn2Calls++
				hookMu.Unlock()
			}
			return nil
		},
	}

	serverErrCh := make(chan error, 1)
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		readCtx, cancelRead := context.WithTimeout(r.Context(), 3*time.Second)
		_, firstMessage, err := conn.Read(readCtx)
		cancelRead()
		if err != nil {
			serverErrCh <- err
			return
		}
		recorder := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(recorder)
		ginCtx.Request = r.Clone(r.Context())
		serverErrCh <- svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, hooks)
	}))
	defer wsServer.Close()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(wsServer.URL, "http"), nil)
	cancelDial()
	require.NoError(t, err)
	defer func() { _ = clientConn.CloseNow() }()

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.4","stream":false,"store":false,"input":"hello"}`))
	cancelWrite()
	require.NoError(t, err)
	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, firstEvent, err := clientConn.Read(readCtx)
	cancelRead()
	require.NoError(t, err)
	require.Equal(t, "resp_ingress_strict_gate", gjson.GetBytes(firstEvent, "response.id").String())

	writeCtx, cancelWrite = context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(strictGateSecondTurnPayload("resp_ingress_strict_gate")))
	cancelWrite()
	require.NoError(t, err)

	select {
	case proxyErr := <-serverErrCh:
		require.ErrorIs(t, proxyErr, auditBlockErr)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for strict ingress audit block")
	}
	hookMu.Lock()
	require.Equal(t, 1, auditTurn2Calls)
	require.Zero(t, beforeTurn2Calls, "blocked turn must not acquire per-turn concurrency or billing")
	require.True(t, sawAssistant)
	require.True(t, sawFunctionArguments)
	hookMu.Unlock()
	captureConn.mu.Lock()
	writeCount := len(captureConn.writes)
	captureConn.mu.Unlock()
	require.Equal(t, 1, writeCount, "blocked turn must not be written to the upstream websocket")
	require.Equal(t, 1, dialer.DialCount(), "blocked turn must not dial a replacement upstream websocket")
}

func TestOpenAIWSHTTPBridgeRejectsMixedResponseCreateBeforeTurnAndDispatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	contextIncompleteErr := errors.New("policy_context_incomplete")
	unexpectedParserResultErr := errors.New("mixed response.create was not rejected as incomplete")
	upstream := &httpUpstreamRecorder{responses: []*http.Response{{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(strictGateFirstTurnTerminal("resp_bridge_mixed_gate"))),
	}}}
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
	cfg.Gateway.OpenAIWS.HTTPBridgeEnabled = true
	cfg.Gateway.OpenAIWS.HTTPBridgeThresholdBytes = 1
	cfg.Gateway.OpenAIWS.ClientFirstMessageTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds = 3
	svc := &OpenAIGatewayService{
		cfg: cfg, httpUpstream: upstream, cache: &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), toolCorrector: NewCodexToolCorrector(),
	}
	account := &Account{
		ID: 204, Name: "strict-http-bridge-mixed", Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test"},
		Extra:       map[string]any{"openai_apikey_responses_websockets_v2_mode": OpenAIWSIngressModeHTTPBridge},
	}

	var hookMu sync.Mutex
	auditTurn2Calls := 0
	beforeTurn2Calls := 0
	sawIncomplete := false
	hooks := &OpenAIWSIngressHooks{
		StrictAudit: true,
		BeforeRequest: func(turn int, payload []byte, _ string) error {
			if turn != 2 {
				return nil
			}
			doc := auditinput.Parse(auditinput.ProtocolOpenAIResponses, payload)
			hookMu.Lock()
			auditTurn2Calls++
			sawIncomplete = !doc.Complete && doc.HasIssue(auditinput.IssueInvalidShape)
			hookMu.Unlock()
			if !doc.Complete && doc.HasIssue(auditinput.IssueInvalidShape) {
				return contextIncompleteErr
			}
			return unexpectedParserResultErr
		},
		BeforeTurn: func(turn int) error {
			if turn == 2 {
				hookMu.Lock()
				beforeTurn2Calls++
				hookMu.Unlock()
			}
			return nil
		},
	}

	firstEvent, proxyErr := runStrictGateTwoTurnSession(
		t, svc, account, hooks, strictGateMixedResponseCreatePayload("resp_bridge_mixed_gate"),
	)
	require.Equal(t, "resp_bridge_mixed_gate", gjson.GetBytes(firstEvent, "response.id").String())
	require.ErrorIs(t, proxyErr, contextIncompleteErr)
	hookMu.Lock()
	require.Equal(t, 1, auditTurn2Calls)
	require.Zero(t, beforeTurn2Calls, "incomplete context must be rejected before turn concurrency and billing")
	require.True(t, sawIncomplete)
	hookMu.Unlock()
	require.Len(t, upstream.requests, 1, "incomplete context must not dispatch a second HTTP request")
}

func TestOpenAIWSIngressRejectsMixedResponseCreateBeforeTurnAndUpstreamWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	contextIncompleteErr := errors.New("policy_context_incomplete")
	unexpectedParserResultErr := errors.New("mixed response.create was not rejected as incomplete")
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3

	captureConn := &openAIWSCaptureConn{events: [][]byte{
		[]byte(strings.TrimSpace(strings.TrimPrefix(strictGateFirstTurnTerminal("resp_ingress_mixed_gate"), "data: "))),
	}}
	dialer := &openAIWSCaptureDialer{conn: captureConn}
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)
	defer pool.Close()
	svc := &OpenAIGatewayService{
		cfg: cfg, httpUpstream: &httpUpstreamRecorder{}, cache: &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), toolCorrector: NewCodexToolCorrector(), openaiWSPool: pool,
	}
	account := &Account{
		ID: 205, Name: "strict-ingress-mixed", Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test"},
		Extra:       map[string]any{"responses_websockets_v2_enabled": true},
	}

	var hookMu sync.Mutex
	auditTurn2Calls := 0
	beforeTurn2Calls := 0
	sawIncomplete := false
	hooks := &OpenAIWSIngressHooks{
		StrictAudit: true,
		BeforeRequest: func(turn int, payload []byte, _ string) error {
			if turn != 2 {
				return nil
			}
			doc := auditinput.Parse(auditinput.ProtocolOpenAIResponses, payload)
			hookMu.Lock()
			auditTurn2Calls++
			sawIncomplete = !doc.Complete && doc.HasIssue(auditinput.IssueInvalidShape)
			hookMu.Unlock()
			if !doc.Complete && doc.HasIssue(auditinput.IssueInvalidShape) {
				return contextIncompleteErr
			}
			return unexpectedParserResultErr
		},
		BeforeTurn: func(turn int) error {
			if turn == 2 {
				hookMu.Lock()
				beforeTurn2Calls++
				hookMu.Unlock()
			}
			return nil
		},
	}

	firstEvent, proxyErr := runStrictGateTwoTurnSession(
		t, svc, account, hooks, strictGateMixedResponseCreatePayload("resp_ingress_mixed_gate"),
	)
	require.Equal(t, "resp_ingress_mixed_gate", gjson.GetBytes(firstEvent, "response.id").String())
	require.ErrorIs(t, proxyErr, contextIncompleteErr)
	hookMu.Lock()
	require.Equal(t, 1, auditTurn2Calls)
	require.Zero(t, beforeTurn2Calls, "incomplete context must be rejected before turn concurrency and billing")
	require.True(t, sawIncomplete)
	hookMu.Unlock()
	captureConn.mu.Lock()
	writeCount := len(captureConn.writes)
	captureConn.mu.Unlock()
	require.Equal(t, 1, writeCount, "incomplete context must not be written to the upstream websocket")
	require.Equal(t, 1, dialer.DialCount(), "incomplete context must not dial a replacement upstream websocket")
}
