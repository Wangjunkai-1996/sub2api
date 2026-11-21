package handler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type continuationTestUpstream struct {
	service.HTTPUpstream
	block bool
	calls atomic.Int32
}

func (u *continuationTestUpstream) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	n := u.calls.Add(1)
	if u.block {
		return &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{"error":{"code":"cyber_policy","message":"Session blocked by cyber-security policy. Start a new session."}}`))}, nil
	}
	binding, ok := service.HTTPUpstreamEgressFromContext(req.Context())
	if !ok || binding.BindingID != service.StableAccountEgressBindingID(accountID, 41) {
		return nil, fmt.Errorf("turn %d lost its selected egress", n)
	}
	body := fmt.Sprintf(`data: {"type":"response.completed","response":{"id":"resp_lease_%d","model":"gpt-5.1","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`+"\n\n", n)
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
}

func continuationTestGateway(t *testing.T, account service.Account, upstream *continuationTestUpstream, cache *redis.Client) (*OpenAIGatewayHandler, *gin.Engine) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Default.RateMultiplier = 1
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	settings := service.NewSettingService(&contentModerationHandlerSettingRepo{values: map[string]string{
		service.SettingKeyAccountEgressPoolRolloutMode: string(service.AccountEgressPoolRolloutEnforce),
	}}, cfg)
	concurrency := service.NewConcurrencyService(repository.NewConcurrencyCache(cache, 15, 120))
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	repo := &openAIWSUsageHandlerAccountRepoStub{account: account}
	logs := &openAIWSUsageHandlerUsageLogRepoStub{created: make(chan *service.UsageLog, 4)}
	gateway := service.NewOpenAIGatewayService(repo, logs, nil, nil, nil, nil, repository.NewGatewayCache(cache),
		cfg, nil, concurrency, service.NewBillingService(cfg, nil), nil, billing, upstream, &service.DeferredService{},
		nil, nil, nil, nil, nil, settings, nil)
	handler := NewOpenAIGatewayHandler(gateway, concurrency, billing, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
	router := gin.New()
	group := int64(42)
	key := &service.APIKey{ID: 101, UserID: 1, GroupID: &group, Group: &service.Group{ID: group, Status: service.StatusActive, Platform: service.PlatformOpenAI}, User: &service.User{ID: 1, Status: service.StatusActive}}
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware.ContextKeyAPIKey), key)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1, Concurrency: 1})
		c.Next()
	})
	router.GET("/v1/responses", handler.ResponsesWebSocket)
	router.POST("/v1/responses", handler.Responses)
	return handler, router
}

func TestOpenAIResponses_RejectsPreviouslyBlockedContinuationAcrossInstances(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	account := service.Account{ID: 83, Concurrency: 1, Status: service.StatusActive, Schedulable: true, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
		Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}}
	upstream := &continuationTestUpstream{block: true}
	first, router := continuationTestGateway(t, account, upstream, rdb)
	require.NoError(t, first.gatewayService.BindOpenAIHTTPResponseOwner(context.Background(), 42, "resp_blocked", 1, 101))
	store := service.NewOpenAIWSStateStore(repository.NewGatewayCache(rdb))
	require.NoError(t, store.BindResponseAccount(context.Background(), 42, "resp_blocked", account.ID, time.Minute))
	for attempt := 0; attempt < 2; attempt++ {
		if attempt == 1 {
			_, router = continuationTestGateway(t, account, upstream, rdb)
		}
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.1","previous_response_id":"resp_blocked","input":"hello"}`))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusConflict, rec.Code, "attempt %d upstream calls %d body %q", attempt, upstream.calls.Load(), rec.Body.String())
		require.Contains(t, rec.Body.String(), "conversation_session_expired")
		require.EqualValues(t, 1, upstream.calls.Load())
	}
}

func TestOpenAIResponsesWebSocket_PoolLeaseSurvivesTwoBridgeTurns(t *testing.T) {
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	now, scope := time.Now(), "local-test"
	account := service.Account{ID: 84, Status: service.StatusActive, Schedulable: true, Concurrency: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Credentials: map[string]any{"access_token": "test", "chatgpt_account_id": "chatgpt-test"},
		Extra:       map[string]any{"openai_oauth_responses_websockets_v2_enabled": true, "openai_oauth_responses_websockets_v2_mode": service.OpenAIWSIngressModeHTTPBridge},
		EgressMode:  service.EgressModePool, EgressRevision: 1}
	account.EgressBindings = []service.AccountEgressBinding{{AccountID: account.ID, BindingID: service.StableAccountEgressBindingID(account.ID, 41), RouteID: 41, IsPrimary: true, Status: service.AccountEgressBindingStatusActive,
		Route: &service.EgressRoute{ID: 41, Kind: service.EgressRouteKindDirect, RuntimeScope: &scope, State: service.EgressRouteStateActive, Revision: 1, VerifiedAt: &now,
			ExpectedIdentity: &service.EgressIdentity{ID: 301, Status: service.EgressIdentityStatusActive}}}}
	upstream := &continuationTestUpstream{}
	_, router := continuationTestGateway(t, account, upstream, rdb)
	host := httptest.NewServer(router)
	defer host.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(host.URL, "http")+"/v1/responses", nil)
	require.NoError(t, err)
	defer conn.CloseNow()
	leaseKeys := func() []string {
		var leases []string
		for _, k := range server.Keys() {
			if strings.Contains(k, ":lease:") {
				leases = append(leases, k)
			}
		}
		return leases
	}
	var firstLease []string
	for turn := 1; turn <= 2; turn++ {
		require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","input":"hello"}`)))
		_, frame, err := conn.Read(ctx)
		require.NoError(t, err)
		require.Equal(t, "response.completed", gjson.GetBytes(frame, "type").String(), string(frame))
		require.EqualValues(t, turn, upstream.calls.Load())
		keys := leaseKeys()
		require.Len(t, keys, 1)
		if turn == 1 {
			firstLease = keys
		} else {
			require.Equal(t, firstLease, keys)
		}
	}
	require.NoError(t, conn.Close(coderws.StatusNormalClosure, "done"))
	require.Eventually(t, func() bool { return len(leaseKeys()) == 0 }, time.Second, 5*time.Millisecond)
}
