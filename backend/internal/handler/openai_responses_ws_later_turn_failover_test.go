package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIResponsesWebSocket_CtxPoolLaterTurnFailoverReplaysOnlyCurrentTurn(t *testing.T) {
	runOpenAIWSCtxPoolLaterTurnFailoverHandler(t, false)
}

func TestOpenAIResponsesWebSocket_CtxPoolUnsafeLaterTurnStopsWithReason(t *testing.T) {
	runOpenAIWSCtxPoolLaterTurnFailoverHandler(t, true)
}

func runOpenAIWSCtxPoolLaterTurnFailoverHandler(t *testing.T, unsafeContinuation bool) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	firstPayloadCh := make(chan []byte, 2)
	secondPayloadCh := make(chan []byte, 1)
	upstreamErrCh := make(chan error, 2)
	firstUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			upstreamErrCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		read := func() ([]byte, error) {
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			_, payload, readErr := conn.Read(ctx)
			return payload, readErr
		}
		write := func(payload string) error {
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			return conn.Write(ctx, coderws.MessageText, []byte(payload))
		}
		first, err := read()
		if err != nil {
			upstreamErrCh <- err
			return
		}
		firstPayloadCh <- first
		if err := write(`{"type":"response.completed","response":{"id":"resp_a_first","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`); err != nil {
			upstreamErrCh <- err
			return
		}
		second, err := read()
		if err != nil {
			upstreamErrCh <- err
			return
		}
		firstPayloadCh <- second
		if err := write(`{"type":"response.failed","response":{"id":"resp_a_second","status":"failed","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}`); err != nil {
			upstreamErrCh <- err
			return
		}
		upstreamErrCh <- nil
	}))
	defer firstUpstream.Close()

	secondUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			upstreamErrCh <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		_, payload, err := conn.Read(ctx)
		if err != nil {
			upstreamErrCh <- err
			return
		}
		secondPayloadCh <- payload
		writeCtx, cancelWrite := context.WithTimeout(r.Context(), 5*time.Second)
		err = conn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.completed","response":{"id":"resp_b_second","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`))
		cancelWrite()
		upstreamErrCh <- err
	}))
	defer secondUpstream.Close()

	groupID := int64(4310)
	accounts := []service.Account{
		{
			ID: 9931, Name: "ctx-pool-later-a", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 1, Concurrency: 1,
			Credentials: map[string]any{"api_key": "sk-a", "base_url": firstUpstream.URL},
			Extra: map[string]any{
				"openai_apikey_responses_websockets_v2_enabled": true,
				"openai_apikey_responses_websockets_v2_mode":    service.OpenAIWSIngressModeCtxPool,
			},
		},
		{
			ID: 9932, Name: "ctx-pool-later-b", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 2, Concurrency: 1,
			Credentials: map[string]any{"api_key": "sk-b", "base_url": secondUpstream.URL},
			Extra: map[string]any{
				"openai_apikey_responses_websockets_v2_enabled": true,
				"openai_apikey_responses_websockets_v2_mode":    service.OpenAIWSIngressModeCtxPool,
			},
		},
	}

	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Default.RateMultiplier = 1
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.MaxAccountSwitches = 2
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.IngressModeDefault = service.OpenAIWSIngressModeCtxPool
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds = 5

	accountRepo := &openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}
	rateLimitSvc := service.NewRateLimitService(accountRepo, nil, cfg, nil, nil)
	billingCacheSvc := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billingCacheSvc.Stop)
	gatewaySvc := service.NewOpenAIGatewayService(
		accountRepo, nil, nil, nil, nil, nil, nil, cfg, nil, nil,
		service.NewBillingService(cfg, nil), rateLimitSvc, billingCacheSvc,
		nil, &service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil,
	)
	t.Cleanup(gatewaySvc.CloseOpenAIWSPool)
	cache := &concurrencyCacheMock{
		acquireUserSlotFn:    func(context.Context, int64, int, string) (bool, error) { return true, nil },
		acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil },
	}
	h := &OpenAIGatewayHandler{
		gatewayService:      gatewaySvc,
		billingCacheService: billingCacheSvc,
		apiKeyService:       &service.APIKeyService{},
		concurrencyHelper:   NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatNone, time.Second),
		maxAccountSwitches:  2,
	}
	apiKey := &service.APIKey{
		ID: 1810, GroupID: &groupID,
		User:  &service.User{ID: 1710, Status: service.StatusActive},
		Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive},
	}
	handlerDone := make(chan struct{})
	opsCh := make(chan []*service.OpsUpstreamErrorEvent, 1)
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware.ContextKeyAPIKey), apiKey)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: apiKey.User.ID, Concurrency: 1})
		c.Next()
	})
	router.GET("/openai/v1/responses", func(c *gin.Context) {
		h.ResponsesWebSocket(c)
		raw, _ := c.Get(service.OpsUpstreamErrorsKey)
		events, _ := raw.([]*service.OpsUpstreamErrorEvent)
		opsCh <- events
		close(handlerDone)
	})
	handlerServer := httptest.NewServer(router)
	defer handlerServer.Close()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(handlerServer.URL, "http")+"/openai/v1/responses", &coderws.DialOptions{CompressionMode: coderws.CompressionContextTakeover})
	cancelDial()
	require.NoError(t, err)
	defer func() { _ = clientConn.CloseNow() }()
	write := func(payload string) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require.NoError(t, clientConn.Write(ctx, coderws.MessageText, []byte(payload)))
	}
	read := func() []byte {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		messageType, payload, readErr := clientConn.Read(ctx)
		require.NoError(t, readErr)
		require.Equal(t, coderws.MessageText, messageType)
		return payload
	}

	write(`{"type":"response.create","model":"gpt-5.1","store":true,"input":[{"role":"user","content":"first"}]}`)
	firstCompleted := read()
	require.Equal(t, "response.completed", gjson.GetBytes(firstCompleted, "type").String())
	require.Equal(t, "resp_a_first", gjson.GetBytes(firstCompleted, "response.id").String())
	if unsafeContinuation {
		write(`{"type":"response.create","model":"gpt-5.1","store":true,"previous_response_id":"opaque","input":[{"role":"user","content":"second"}]}`)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _, readErr := clientConn.Read(ctx)
		cancel()
		require.Equal(t, coderws.StatusTryAgainLater, coderws.CloseStatus(readErr))
		require.ErrorContains(t, readErr, "cannot be safely replayed; please restart the conversation")
	} else {
		write(`{"type":"response.create","model":"gpt-5.1","store":true,"input":[{"role":"user","content":"second"}]}`)
		secondCompleted := read()
		require.Equal(t, "response.completed", gjson.GetBytes(secondCompleted, "type").String())
		require.Equal(t, "resp_b_second", gjson.GetBytes(secondCompleted, "response.id").String())
		require.NoError(t, clientConn.Close(coderws.StatusNormalClosure, "done"))
	}

	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("websocket handler did not finish")
	}
	first := <-firstPayloadCh
	secondA := <-firstPayloadCh
	require.Equal(t, "first", gjson.GetBytes(first, "input.0.content").String())
	require.Equal(t, "second", gjson.GetBytes(secondA, "input.0.content").String())
	upstreamCount := 2
	if unsafeContinuation {
		upstreamCount = 1
		require.Empty(t, secondPayloadCh, "unsafe wrapper must not fall back to the first turn")
		events := <-opsCh
		require.NotEmpty(t, events)
		require.Equal(t, "current_turn_replay_blocked", events[len(events)-1].FinalOutcome)
		require.Equal(t, http.StatusServiceUnavailable, events[len(events)-1].FinalClientStatusCode)
	} else {
		secondB := <-secondPayloadCh
		require.Equal(t, "second", gjson.GetBytes(secondB, "input.0.content").String())
		require.NotContains(t, string(secondB), "first")
	}
	for i := 0; i < upstreamCount; i++ {
		select {
		case upstreamErr := <-upstreamErrCh:
			require.NoError(t, upstreamErr)
		case <-time.After(5 * time.Second):
			t.Fatal("upstream websocket did not finish")
		}
	}
}
