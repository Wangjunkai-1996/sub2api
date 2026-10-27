package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/testutil"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type openAIWSPassthroughHandlerHarness struct {
	handler        *OpenAIGatewayHandler
	clientConn     *coderws.Conn
	handlerDone    <-chan struct{}
	moderationRepo *contentModerationHandlerTestRepo
	gatewayCache   service.GatewayCache
	apiKey         *service.APIKey
}

func withNewAPIAttributionTestIdentity(req *http.Request) *http.Request {
	ctx := context.WithValue(req.Context(), ctxkey.NewAPIUserID, int64(42))
	ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(77))
	ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "https://newapi.internal")
	return req.WithContext(ctx)
}

func newOpenAIWSPassthroughHandlerHarness(t *testing.T, upstreamURL, sessionID string, settings ...map[string]string) *openAIWSPassthroughHandlerHarness {
	t.Helper()
	gatewayCache := testutil.NewRedisGatewayCache(t)

	settingRepo := &contentModerationHandlerSettingRepo{values: map[string]string{
		service.SettingKeyRiskControlEnabled:          "true",
		service.SettingKeyCyberSessionBlockEnabled:    "true",
		service.SettingKeyCyberSessionBlockTTLSeconds: "60",
	}}
	for _, overrides := range settings {
		for key, value := range overrides {
			settingRepo.values[key] = value
		}
	}
	moderationRepo := &contentModerationHandlerTestRepo{}
	moderationSvc := service.NewContentModerationService(settingRepo, moderationRepo, nil, nil, nil, nil, nil, nil)
	settingSvc := service.NewSettingService(settingRepo, nil)

	groupID := int64(4301)
	account := service.Account{
		ID:          9951,
		Name:        "openai-ws-passthrough-cyber",
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeAPIKey,
		Status:      service.StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Credentials: map[string]any{"api_key": "sk-test", "base_url": upstreamURL},
		Extra: map[string]any{
			"openai_apikey_responses_websockets_v2_enabled": true,
			"openai_apikey_responses_websockets_v2_mode":    service.OpenAIWSIngressModePassthrough,
		},
	}
	cfg := &config.Config{}
	cfg.RunMode = config.RunModeSimple
	cfg.Default.RateMultiplier = 1
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds = 3

	accountRepo := &openAIWSUsageHandlerAccountRepoStub{account: account}
	usageRepo := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 2)}
	billingCacheSvc := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	gatewaySvc := service.NewOpenAIGatewayService(
		accountRepo, usageRepo, nil, nil, nil, nil, gatewayCache, cfg, nil, nil,
		service.NewBillingService(cfg, nil), nil, billingCacheSvc, nil, &service.DeferredService{},
		nil, nil, nil, nil, nil, settingSvc, nil,
	)
	concurrencyCache := &concurrencyCacheMock{
		acquireUserSlotFn:    func(context.Context, int64, int, string) (bool, error) { return true, nil },
		acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil },
	}
	h := &OpenAIGatewayHandler{
		gatewayService:           gatewaySvc,
		billingCacheService:      billingCacheSvc,
		apiKeyService:            &service.APIKeyService{},
		contentModerationService: moderationSvc,
		concurrencyHelper:        NewConcurrencyHelper(service.NewConcurrencyService(concurrencyCache), SSEPingFormatNone, time.Second),
	}

	apiKey := &service.APIKey{
		ID:      1851,
		UserID:  1751,
		Name:    "ws-cyber-key",
		Key:     "sk-handler-cyber-test",
		GroupID: &groupID,
		User:    &service.User{ID: 1751, Status: service.StatusActive},
	}
	handlerDone := make(chan struct{})
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = withNewAPIAttributionTestIdentity(c.Request)
		c.Set(string(middleware.ContextKeyAPIKey), apiKey)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: apiKey.User.ID, Concurrency: 1})
		c.Next()
	})
	router.GET("/openai/v1/responses", func(c *gin.Context) {
		h.ResponsesWebSocket(c)
		close(handlerDone)
	})
	handlerServer := httptest.NewServer(router)
	t.Cleanup(handlerServer.Close)

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	dialOptions := &coderws.DialOptions{}
	if sessionID != "" {
		dialOptions.HTTPHeader = http.Header{"X-Session-Id": []string{sessionID}}
	}
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(handlerServer.URL, "http")+"/openai/v1/responses", dialOptions)
	cancelDial()
	require.NoError(t, err)
	t.Cleanup(func() { _ = clientConn.CloseNow() })

	return &openAIWSPassthroughHandlerHarness{
		handler:        h,
		clientConn:     clientConn,
		handlerDone:    handlerDone,
		moderationRepo: moderationRepo,
		gatewayCache:   gatewayCache,
		apiKey:         apiKey,
	}
}

func TestOpenAIResponsesWebSocketV2PassthroughCyberMarkIsConsumedAfterTurn(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstreamDone := make(chan struct{})
	secondUpstreamFrame := make(chan []byte, 1)
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(upstreamDone)
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		require.NoError(t, err)
		defer func() { _ = conn.CloseNow() }()

		readCtx, cancelRead := context.WithTimeout(r.Context(), 3*time.Second)
		_, _, err = conn.Read(readCtx)
		cancelRead()
		require.NoError(t, err)

		failed := []byte(`{"type":"response.failed","response":{"id":"resp_cyber_handler","model":"gpt-5.1","error":{"code":"cyber_policy","message":"blocked by upstream policy"},"usage":{"input_tokens":11,"output_tokens":3}}}`)
		writeCtx, cancelWrite := context.WithTimeout(r.Context(), 3*time.Second)
		err = conn.Write(writeCtx, coderws.MessageText, failed)
		cancelWrite()
		require.NoError(t, err)

		readCtx, cancelRead = context.WithTimeout(r.Context(), 3*time.Second)
		_, second, err := conn.Read(readCtx)
		cancelRead()
		if err != nil {
			return
		}
		secondUpstreamFrame <- append([]byte(nil), second...)

		completed := []byte(`{"type":"response.completed","response":{"id":"resp_cyber_handler_turn_2","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`)
		writeCtx, cancelWrite = context.WithTimeout(r.Context(), 3*time.Second)
		err = conn.Write(writeCtx, coderws.MessageText, completed)
		cancelWrite()
		require.NoError(t, err)
	}))
	defer upstreamServer.Close()
	harness := newOpenAIWSPassthroughHandlerHarness(t, upstreamServer.URL, "cyber-session-1")

	requestPayload := `{"type":"response.create","model":"gpt-5.1","prompt_cache_key":"cyber-session-1","input":"test"}`
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err := harness.clientConn.Write(writeCtx, coderws.MessageText, []byte(requestPayload))
	cancelWrite()
	require.NoError(t, err)

	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, event, err := harness.clientConn.Read(readCtx)
	cancelRead()
	require.NoError(t, err)
	require.Equal(t, "response.failed", gjson.GetBytes(event, "type").String())

	require.Eventually(t, func() bool {
		logs := harness.moderationRepo.logSnapshot()
		return len(logs) == 1 && logs[0].Action == service.ContentModerationActionCyberPolicy &&
			logs[0].Mode == service.ContentModerationModeCyberLogOnly &&
			strings.Contains(logs[0].Error, "upstream_usage=in:11,out:3")
	}, 3*time.Second, 10*time.Millisecond, "handler AfterTurn must call recordCyberPolicyIfMarked and write the risk-control event")

	keyCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	keyCtx.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", strings.NewReader(requestPayload))
	keyCtx.Request.Header.Set("X-Session-Id", "cyber-session-1")
	keyCtx.Request = withNewAPIAttributionTestIdentity(keyCtx.Request)
	blockKey := service.CyberSessionExplicitBlockKey(harness.apiKey.ID, keyCtx, []byte(requestPayload))
	require.NotEmpty(t, blockKey)
	store, ok := harness.gatewayCache.(service.CyberSessionBlockStore)
	require.True(t, ok)
	require.Eventually(t, func() bool {
		matched, findErr := store.FindCyberSessionBlocked(context.Background(), blockKey)
		return findErr == nil && matched
	}, 3*time.Second, 10*time.Millisecond, "handler AfterTurn must write the cyber session block table")

	writeCtx, cancelWrite = context.WithTimeout(context.Background(), 3*time.Second)
	err = harness.clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","prompt_cache_key":"cyber-session-1","input":"follow-up"}`))
	cancelWrite()
	require.NoError(t, err)

	readCtx, cancelRead = context.WithTimeout(context.Background(), 3*time.Second)
	_, _, err = harness.clientConn.Read(readCtx)
	cancelRead()
	var closeErr coderws.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, coderws.StatusPolicyViolation, closeErr.Code)
	require.Equal(t, cyberSessionBlockedClientMsg, closeErr.Reason)
	select {
	case <-harness.handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("websocket handler did not exit")
	}
	select {
	case <-upstreamDone:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream websocket did not exit")
	}
	select {
	case second := <-secondUpstreamFrame:
		t.Fatalf("blocked follow-up reached upstream: %s", second)
	default:
	}
}

func TestOpenAIResponsesWebSocketV2CyberWithoutConversationDoesNotBlockNextTurn(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		for _, response := range []string{
			`{"type":"response.failed","response":{"id":"resp_cyber","model":"gpt-5.1","error":{"code":"cyber_policy","message":"blocked by upstream"}}}`,
			`{"type":"response.completed","response":{"id":"resp_next","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`,
		} {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
			if err := conn.Write(ctx, coderws.MessageText, []byte(response)); err != nil {
				return
			}
		}
		_, _, _ = conn.Read(ctx)
	}))
	defer upstream.Close()
	harness := newOpenAIWSPassthroughHandlerHarness(t, upstream.URL, "")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first := []byte(`{"type":"response.create","model":"gpt-5.1","prompt_cache_key":"shared-template","input":"first"}`)
	require.NoError(t, harness.clientConn.Write(ctx, coderws.MessageText, first))
	_, event, err := harness.clientConn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "cyber_policy", gjson.GetBytes(event, "response.error.code").String())

	second := []byte(`{"type":"response.create","model":"gpt-5.1","prompt_cache_key":"shared-template","input":"different conversation"}`)
	require.NoError(t, harness.clientConn.Write(ctx, coderws.MessageText, second))
	_, event, err = harness.clientConn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String())
	require.NoError(t, harness.clientConn.Close(coderws.StatusNormalClosure, "done"))
	select {
	case <-harness.handlerDone:
	case <-ctx.Done():
		t.Fatal("handler did not exit")
	}
}

func TestOpenAIResponsesWebSocketV2CyberWithSessionBlockDisabledAllowsNextTurn(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		for _, response := range []string{
			`{"type":"response.failed","response":{"id":"resp_cyber","model":"gpt-5.1","error":{"code":"cyber_policy","message":"blocked by upstream"}}}`,
			`{"type":"response.completed","response":{"id":"resp_next","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`,
		} {
			if _, _, err := conn.Read(ctx); err != nil {
				return
			}
			if err := conn.Write(ctx, coderws.MessageText, []byte(response)); err != nil {
				return
			}
		}
		_, _, _ = conn.Read(ctx)
	}))
	defer upstream.Close()
	harness := newOpenAIWSPassthroughHandlerHarness(t, upstream.URL, "explicit-session", map[string]string{
		service.SettingKeyCyberSessionBlockEnabled: "false",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first := []byte(`{"type":"response.create","model":"gpt-5.1","input":"first"}`)
	require.NoError(t, harness.clientConn.Write(ctx, coderws.MessageText, first))
	_, event, err := harness.clientConn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "cyber_policy", gjson.GetBytes(event, "response.error.code").String())
	require.Eventually(t, func() bool { return len(harness.moderationRepo.logSnapshot()) == 1 }, time.Second, time.Millisecond)

	second := []byte(`{"type":"response.create","model":"gpt-5.1","input":"second"}`)
	require.NoError(t, harness.clientConn.Write(ctx, coderws.MessageText, second))
	_, event, err = harness.clientConn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String())
	require.NoError(t, harness.clientConn.Close(coderws.StatusNormalClosure, "done"))
	select {
	case <-harness.handlerDone:
	case <-ctx.Done():
		t.Fatal("handler did not exit")
	}
}

func TestOpenAIResponsesWebSocketV2ChecksSessionBlockBeforeLaterTurn(t *testing.T) {
	secondUpstreamFrame := make(chan []byte, 1)
	upstreamDone := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(upstreamDone)
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.CloseNow() }()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if _, _, err := conn.Read(ctx); err != nil {
			return
		}
		if err := conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_first","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`)); err != nil {
			return
		}
		if _, payload, err := conn.Read(ctx); err == nil {
			secondUpstreamFrame <- payload
		}
	}))
	defer upstream.Close()
	harness := newOpenAIWSPassthroughHandlerHarness(t, upstream.URL, "blocked-between-turns")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first := []byte(`{"type":"response.create","model":"gpt-5.1","input":"first"}`)
	require.NoError(t, harness.clientConn.Write(ctx, coderws.MessageText, first))
	_, event, err := harness.clientConn.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String())

	keyCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	keyCtx.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)
	keyCtx.Request.Header.Set("X-Session-Id", "blocked-between-turns")
	keyCtx.Request = withNewAPIAttributionTestIdentity(keyCtx.Request)
	key := service.CyberSessionExplicitBlockKey(harness.apiKey.ID, keyCtx, nil)
	require.NotEmpty(t, key)
	store, ok := harness.gatewayCache.(service.CyberSessionBlockStore)
	require.True(t, ok)
	require.NoError(t, store.SetCyberSessionBlocked(ctx, key, time.Minute))

	second := []byte(`{"type":"response.create","model":"gpt-5.1","input":"second"}`)
	require.NoError(t, harness.clientConn.Write(ctx, coderws.MessageText, second))
	_, _, err = harness.clientConn.Read(ctx)
	var closeErr coderws.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, coderws.StatusPolicyViolation, closeErr.Code)
	require.Equal(t, cyberSessionBlockedClientMsg, closeErr.Reason)
	select {
	case <-upstreamDone:
	case <-ctx.Done():
		t.Fatal("upstream did not close after session block")
	}
	select {
	case payload := <-secondUpstreamFrame:
		t.Fatalf("blocked turn reached upstream: %s", payload)
	default:
	}
}

func TestOpenAIResponsesWebSocketV2PassthroughNonCyberTurnAllowsFollowup(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstreamDone := make(chan struct{})
	secondUpstreamFrame := make(chan []byte, 1)
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(upstreamDone)
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		require.NoError(t, err)
		defer func() { _ = conn.CloseNow() }()

		readCtx, cancelRead := context.WithTimeout(r.Context(), 3*time.Second)
		_, _, err = conn.Read(readCtx)
		cancelRead()
		require.NoError(t, err)

		firstCompleted := []byte(`{"type":"response.completed","response":{"id":"resp_non_cyber_handler_turn_1","model":"gpt-5.1","usage":{"input_tokens":2,"output_tokens":1}}}`)
		writeCtx, cancelWrite := context.WithTimeout(r.Context(), 3*time.Second)
		err = conn.Write(writeCtx, coderws.MessageText, firstCompleted)
		cancelWrite()
		require.NoError(t, err)

		readCtx, cancelRead = context.WithTimeout(r.Context(), 3*time.Second)
		_, second, err := conn.Read(readCtx)
		cancelRead()
		require.NoError(t, err)
		secondUpstreamFrame <- append([]byte(nil), second...)

		secondCompleted := []byte(`{"type":"response.completed","response":{"id":"resp_non_cyber_handler_turn_2","model":"gpt-5.1","usage":{"input_tokens":3,"output_tokens":1}}}`)
		writeCtx, cancelWrite = context.WithTimeout(r.Context(), 3*time.Second)
		err = conn.Write(writeCtx, coderws.MessageText, secondCompleted)
		cancelWrite()
		require.NoError(t, err)

		readCtx, cancelRead = context.WithTimeout(r.Context(), 3*time.Second)
		_, _, _ = conn.Read(readCtx)
		cancelRead()
	}))
	defer upstreamServer.Close()
	harness := newOpenAIWSPassthroughHandlerHarness(t, upstreamServer.URL, "cyber-session-1")

	firstPayload := `{"type":"response.create","model":"gpt-5.1","prompt_cache_key":"non-cyber-session-1","input":"first"}`
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err := harness.clientConn.Write(writeCtx, coderws.MessageText, []byte(firstPayload))
	cancelWrite()
	require.NoError(t, err)

	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, firstEvent, err := harness.clientConn.Read(readCtx)
	cancelRead()
	require.NoError(t, err)
	require.Equal(t, "resp_non_cyber_handler_turn_1", gjson.GetBytes(firstEvent, "response.id").String())

	secondPayload := `{"type":"response.create","model":"gpt-5.1","prompt_cache_key":"non-cyber-session-1","input":"follow-up"}`
	writeCtx, cancelWrite = context.WithTimeout(context.Background(), 3*time.Second)
	err = harness.clientConn.Write(writeCtx, coderws.MessageText, []byte(secondPayload))
	cancelWrite()
	require.NoError(t, err)

	readCtx, cancelRead = context.WithTimeout(context.Background(), 3*time.Second)
	_, secondEvent, err := harness.clientConn.Read(readCtx)
	cancelRead()
	require.NoError(t, err)
	require.Equal(t, "resp_non_cyber_handler_turn_2", gjson.GetBytes(secondEvent, "response.id").String())
	require.Empty(t, harness.moderationRepo.logSnapshot())

	keyCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	keyCtx.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", strings.NewReader(firstPayload))
	keyCtx.Request.Header.Set("X-Session-Id", "cyber-session-1")
	keyCtx.Request = withNewAPIAttributionTestIdentity(keyCtx.Request)
	blockKey := service.CyberSessionExplicitBlockKey(harness.apiKey.ID, keyCtx, []byte(firstPayload))
	require.NotEmpty(t, blockKey)
	store, ok := harness.gatewayCache.(service.CyberSessionBlockStore)
	require.True(t, ok)
	matched, findErr := store.FindCyberSessionBlocked(context.Background(), blockKey)
	require.NoError(t, findErr)
	require.False(t, matched)

	require.NoError(t, harness.clientConn.Close(coderws.StatusNormalClosure, "done"))
	select {
	case <-harness.handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("non-cyber websocket handler did not exit")
	}
	select {
	case <-upstreamDone:
	case <-time.After(3 * time.Second):
		t.Fatal("non-cyber upstream websocket did not exit")
	}
	select {
	case second := <-secondUpstreamFrame:
		require.JSONEq(t, secondPayload, string(second))
	default:
		t.Fatal("non-cyber follow-up did not reach upstream")
	}
}
