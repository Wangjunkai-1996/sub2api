package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type openAIWSStagedDial struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newOpenAIWSStagedDial() *openAIWSStagedDial {
	return &openAIWSStagedDial{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (s *openAIWSStagedDial) signalEntered() {
	if s != nil {
		s.enteredOnce.Do(func() { close(s.entered) })
	}
}

func (s *openAIWSStagedDial) allow() {
	if s != nil {
		s.releaseOnce.Do(func() { close(s.release) })
	}
}

type openAIWSStagedQueueDialer struct {
	mu        sync.Mutex
	conns     []openAIWSClientConn
	stages    map[int]*openAIWSStagedDial
	dialCount int
}

func (d *openAIWSStagedQueueDialer) Dial(
	ctx context.Context,
	_ string,
	_ http.Header,
	_ string,
) (openAIWSClientConn, int, http.Header, error) {
	d.mu.Lock()
	d.dialCount++
	dialNo := d.dialCount
	if len(d.conns) == 0 {
		d.mu.Unlock()
		return nil, http.StatusServiceUnavailable, nil, errors.New("no staged websocket connection")
	}
	conn := d.conns[0]
	d.conns = d.conns[1:]
	stage := d.stages[dialNo]
	d.mu.Unlock()

	if stage != nil {
		stage.signalEntered()
		select {
		case <-ctx.Done():
			return nil, 0, nil, ctx.Err()
		case <-stage.release:
		}
	}
	return conn, 0, nil, nil
}

func (d *openAIWSStagedQueueDialer) DialCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dialCount
}

type openAIWSFailSecondWriteConn struct {
	mu         sync.Mutex
	events     [][]byte
	writeCount int
}

func (c *openAIWSFailSecondWriteConn) WriteJSON(context.Context, any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeCount++
	if c.writeCount == 2 {
		return errors.New("stale websocket write failed")
	}
	return nil
}

func (c *openAIWSFailSecondWriteConn) ReadMessage(context.Context) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.events) == 0 {
		return nil, errors.New("no staged websocket event")
	}
	event := c.events[0]
	c.events = c.events[1:]
	return event, nil
}

func (c *openAIWSFailSecondWriteConn) Ping(context.Context) error { return nil }
func (c *openAIWSFailSecondWriteConn) Close() error               { return nil }

func (c *openAIWSFailSecondWriteConn) WriteCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writeCount
}

func startOpenAIWSBeforeDispatchTestSession(
	t *testing.T,
	dialer openAIWSClientDialer,
	hooks *OpenAIWSIngressHooks,
) (*coderws.Conn, <-chan error) {
	t.Helper()

	cfg := &config.Config{}
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3

	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(dialer)
	t.Cleanup(pool.Close)

	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     &httpUpstreamRecorder{},
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector:    NewCodexToolCorrector(),
		openaiWSPool:     pool,
	}
	account := &Account{
		ID:          191,
		Name:        "openai-before-dispatch-staged",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test"},
		Extra:       map[string]any{"responses_websockets_v2_enabled": true},
	}

	serverErrCh := make(chan error, 1)
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			serverErrCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()

		rec := httptest.NewRecorder()
		ginCtx, _ := gin.CreateTestContext(rec)
		req := r.Clone(r.Context())
		req.Header = req.Header.Clone()
		req.Header.Set("User-Agent", "unit-test-agent/1.0")
		ginCtx.Request = req

		readCtx, cancelRead := context.WithTimeout(r.Context(), 3*time.Second)
		msgType, firstMessage, readErr := conn.Read(readCtx)
		cancelRead()
		if readErr != nil {
			serverErrCh <- readErr
			return
		}
		if msgType != coderws.MessageText && msgType != coderws.MessageBinary {
			serverErrCh <- errors.New("unsupported websocket client message type")
			return
		}
		serverErrCh <- svc.ProxyResponsesWebSocketFromClient(
			r.Context(), ginCtx, conn, account, "sk-test", firstMessage, hooks,
		)
	}))
	t.Cleanup(wsServer.Close)

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(wsServer.URL, "http"), nil)
	cancelDial()
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.CloseNow() })
	return clientConn, serverErrCh
}

func writeOpenAIWSBeforeDispatchTestMessage(t *testing.T, conn *coderws.Conn, payload string) {
	t.Helper()
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelWrite()
	require.NoError(t, conn.Write(writeCtx, coderws.MessageText, []byte(payload)))
}

func readOpenAIWSBeforeDispatchTestMessage(t *testing.T, conn *coderws.Conn) []byte {
	t.Helper()
	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelRead()
	msgType, payload, err := conn.Read(readCtx)
	require.NoError(t, err)
	require.Equal(t, coderws.MessageText, msgType)
	return payload
}

func requireOpenAIWSBeforeDispatchSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func requireOpenAIWSBeforeDispatchError(t *testing.T, serverErrCh <-chan error, target error) {
	t.Helper()
	select {
	case err := <-serverErrCh:
		require.ErrorIs(t, err, target)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for staged websocket failure")
	}
}

func openAIWSCaptureConnWriteCount(conn *openAIWSCaptureConn) int {
	if conn == nil {
		return 0
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	return len(conn.writes)
}

func TestOpenAIGatewayService_BeforeDispatchRejectsDriftAfterFirstTurnAcquire(t *testing.T) {
	gin.SetMode(gin.TestMode)
	driftErr := errors.New("credential drift after first-turn admission")
	dialStage := newOpenAIWSStagedDial()
	upstreamConn := &openAIWSCaptureConn{}
	dialer := &openAIWSStagedQueueDialer{
		conns:  []openAIWSClientConn{upstreamConn},
		stages: map[int]*openAIWSStagedDial{1: dialStage},
	}

	var drifted atomic.Bool
	var beforeTurnCalls atomic.Int32
	var beforeDispatchCalls atomic.Int32
	beforeTurnDone := make(chan struct{})
	var beforeTurnDoneOnce sync.Once
	hooks := &OpenAIWSIngressHooks{
		BeforeTurn: func(turn int) error {
			if turn == 1 {
				beforeTurnCalls.Add(1)
				beforeTurnDoneOnce.Do(func() { close(beforeTurnDone) })
			}
			return nil
		},
		BeforeDispatch: func(turn int) error {
			if turn == 1 {
				beforeDispatchCalls.Add(1)
			}
			if drifted.Load() {
				return driftErr
			}
			return nil
		},
	}
	clientConn, serverErrCh := startOpenAIWSBeforeDispatchTestSession(t, dialer, hooks)

	writeOpenAIWSBeforeDispatchTestMessage(t, clientConn,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"input":"first-turn-staged-drift"}`)
	requireOpenAIWSBeforeDispatchSignal(t, beforeTurnDone, "BeforeTurn(1)")
	requireOpenAIWSBeforeDispatchSignal(t, dialStage.entered, "first acquire")
	drifted.Store(true)
	dialStage.allow()
	requireOpenAIWSBeforeDispatchError(t, serverErrCh, driftErr)

	require.Equal(t, int32(1), beforeTurnCalls.Load())
	require.Equal(t, int32(1), beforeDispatchCalls.Load())
	require.Equal(t, 1, dialer.DialCount())
	require.Zero(t, openAIWSCaptureConnWriteCount(upstreamConn),
		"credential drift during acquire must stop the first business write")
}

func TestOpenAIGatewayService_BeforeDispatchRejectsDriftAfterFollowUpPreflightReconnect(t *testing.T) {
	gin.SetMode(gin.TestMode)
	previousPreflightPingIdle := openAIWSIngressPreflightPingIdle
	openAIWSIngressPreflightPingIdle = 0
	t.Cleanup(func() { openAIWSIngressPreflightPingIdle = previousPreflightPingIdle })

	driftErr := errors.New("credential drift during follow-up reconnect")
	firstConn := &openAIWSPreflightFailConn{events: [][]byte{
		[]byte(`{"type":"response.completed","response":{"id":"resp_before_dispatch_follow_up_1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
	}}
	secondConn := &openAIWSCaptureConn{}
	dialStage := newOpenAIWSStagedDial()
	dialer := &openAIWSStagedQueueDialer{
		conns:  []openAIWSClientConn{firstConn, secondConn},
		stages: map[int]*openAIWSStagedDial{2: dialStage},
	}

	var drifted atomic.Bool
	var beforeTurn2Calls atomic.Int32
	var beforeRetry2Calls atomic.Int32
	var beforeDispatch2Calls atomic.Int32
	beforeTurn2Done := make(chan struct{})
	var beforeTurn2DoneOnce sync.Once
	hooks := &OpenAIWSIngressHooks{
		BeforeTurn: func(turn int) error {
			if turn == 2 {
				beforeTurn2Calls.Add(1)
				beforeTurn2DoneOnce.Do(func() { close(beforeTurn2Done) })
			}
			return nil
		},
		BeforeRetry: func(turn int) error {
			if turn == 2 {
				beforeRetry2Calls.Add(1)
			}
			return nil
		},
		BeforeDispatch: func(turn int) error {
			if turn == 2 {
				beforeDispatch2Calls.Add(1)
			}
			if drifted.Load() {
				return driftErr
			}
			return nil
		},
	}
	clientConn, serverErrCh := startOpenAIWSBeforeDispatchTestSession(t, dialer, hooks)

	writeOpenAIWSBeforeDispatchTestMessage(t, clientConn,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"input":"follow-up-first"}`)
	firstResponse := readOpenAIWSBeforeDispatchTestMessage(t, clientConn)
	require.Equal(t, "resp_before_dispatch_follow_up_1", gjson.GetBytes(firstResponse, "response.id").String())

	writeOpenAIWSBeforeDispatchTestMessage(t, clientConn,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"previous_response_id":"resp_before_dispatch_follow_up_1","input":"follow-up-staged-drift"}`)
	requireOpenAIWSBeforeDispatchSignal(t, beforeTurn2Done, "BeforeTurn(2)")
	requireOpenAIWSBeforeDispatchSignal(t, dialStage.entered, "follow-up reconnect acquire")
	drifted.Store(true)
	dialStage.allow()
	requireOpenAIWSBeforeDispatchError(t, serverErrCh, driftErr)

	require.Equal(t, int32(1), beforeTurn2Calls.Load())
	require.Zero(t, beforeRetry2Calls.Load())
	require.Equal(t, int32(1), beforeDispatch2Calls.Load())
	require.Equal(t, 2, dialer.DialCount())
	require.Equal(t, 1, firstConn.WriteCount())
	require.GreaterOrEqual(t, firstConn.PingCount(), 1)
	require.Zero(t, openAIWSCaptureConnWriteCount(secondConn),
		"credential drift during reconnect must stop the follow-up business write")
}

func TestOpenAIGatewayService_BeforeDispatchRejectsDriftAfterRetryReconnect(t *testing.T) {
	gin.SetMode(gin.TestMode)
	driftErr := errors.New("credential drift during retry reconnect")
	firstConn := &openAIWSFailSecondWriteConn{events: [][]byte{
		[]byte(`{"type":"response.completed","response":{"id":"resp_before_dispatch_retry_1","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
	}}
	secondConn := &openAIWSCaptureConn{}
	dialStage := newOpenAIWSStagedDial()
	dialer := &openAIWSStagedQueueDialer{
		conns:  []openAIWSClientConn{firstConn, secondConn},
		stages: map[int]*openAIWSStagedDial{2: dialStage},
	}

	var drifted atomic.Bool
	var beforeTurn2Calls atomic.Int32
	var beforeRetry2Calls atomic.Int32
	var beforeDispatch2Calls atomic.Int32
	beforeTurn2Done := make(chan struct{})
	beforeRetry2Done := make(chan struct{})
	var beforeTurn2DoneOnce sync.Once
	var beforeRetry2DoneOnce sync.Once
	hooks := &OpenAIWSIngressHooks{
		BeforeTurn: func(turn int) error {
			if turn == 2 {
				beforeTurn2Calls.Add(1)
				beforeTurn2DoneOnce.Do(func() { close(beforeTurn2Done) })
			}
			return nil
		},
		BeforeRetry: func(turn int) error {
			if turn == 2 {
				beforeRetry2Calls.Add(1)
				beforeRetry2DoneOnce.Do(func() { close(beforeRetry2Done) })
			}
			return nil
		},
		BeforeDispatch: func(turn int) error {
			if turn == 2 {
				beforeDispatch2Calls.Add(1)
			}
			if drifted.Load() {
				return driftErr
			}
			return nil
		},
	}
	clientConn, serverErrCh := startOpenAIWSBeforeDispatchTestSession(t, dialer, hooks)

	writeOpenAIWSBeforeDispatchTestMessage(t, clientConn,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"input":"retry-first"}`)
	firstResponse := readOpenAIWSBeforeDispatchTestMessage(t, clientConn)
	require.Equal(t, "resp_before_dispatch_retry_1", gjson.GetBytes(firstResponse, "response.id").String())

	writeOpenAIWSBeforeDispatchTestMessage(t, clientConn,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"previous_response_id":"resp_before_dispatch_retry_1","input":"retry-staged-drift"}`)
	requireOpenAIWSBeforeDispatchSignal(t, beforeTurn2Done, "BeforeTurn(2)")
	requireOpenAIWSBeforeDispatchSignal(t, beforeRetry2Done, "BeforeRetry(2)")
	requireOpenAIWSBeforeDispatchSignal(t, dialStage.entered, "retry reconnect acquire")
	drifted.Store(true)
	dialStage.allow()
	requireOpenAIWSBeforeDispatchError(t, serverErrCh, driftErr)

	require.Equal(t, int32(1), beforeTurn2Calls.Load())
	require.Equal(t, int32(1), beforeRetry2Calls.Load())
	require.Equal(t, int32(2), beforeDispatch2Calls.Load())
	require.Equal(t, 2, dialer.DialCount())
	require.Equal(t, 2, firstConn.WriteCount(), "the stale connection should receive only the failed first attempt")
	require.Zero(t, openAIWSCaptureConnWriteCount(secondConn),
		"credential drift during retry acquire must stop the replacement business write")
}
