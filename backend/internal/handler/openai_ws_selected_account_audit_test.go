//go:build unit

package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const (
	openAIWSAuditSafeOutput   = "Safety: Safe\nCategories: None"
	openAIWSAuditUnsafeOutput = "Safety: Unsafe\nCategories: Jailbreak"
)

type openAIWSAuditScannerDecision func(call int, text string) (status int, output string)

func newOpenAIWSAuditPromptEngine(
	t *testing.T,
	decide openAIWSAuditScannerDecision,
) (securityaudit.PromptEngine, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var scannerText []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Messages []struct {
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid scanner request", http.StatusBadRequest)
			return
		}
		if r.URL.Path != "/v1/chat/completions" || len(request.Messages) != 1 {
			http.Error(w, "unexpected scanner request", http.StatusBadRequest)
			return
		}

		text := request.Messages[0].Content
		mu.Lock()
		scannerText = append(scannerText, text)
		call := len(scannerText)
		mu.Unlock()
		status, output := decide(call, text)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]string{"content": output}}},
		})
	}))
	t.Cleanup(server.Close)

	cfg := securityaudit.ActiveConfig{
		RiskControlEnabled: true,
		Enabled:            true,
		BlockingEnabled:    true,
		AllGroups:          true,
		Scanners:           append([]string(nil), securityaudit.AllScannerIDs...),
		Endpoints: []securityaudit.ActiveEndpoint{{
			ID: "openai-ws-selected-account-audit-test", BaseURL: server.URL,
			Model: securityaudit.DefaultGuardModel, TimeoutMS: 1000,
			InputLimit: securityaudit.MaxInputLimit, Enabled: true,
		}},
	}
	engine := &selectedAccountAuditPromptEngine{
		cfg:       cfg,
		evaluator: securityaudit.NewGuardEvaluator(securityaudit.NewOpenAICompatibleScanner(), nil, securityaudit.NewAtomicMetrics()),
	}
	return engine, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), scannerText...)
	}
}

type openAIWSAuditUpstream struct {
	server      *httptest.Server
	connections atomic.Int32
	writes      atomic.Int32
	onConnect   func()

	mu             sync.Mutex
	authorizations []string
	payloads       [][]byte
	errors         []error
	onWrite        func(connNo int, writeNo int, conn *coderws.Conn, payload []byte) bool
}

func newOpenAIWSAuditUpstream(
	t *testing.T,
	onWrite func(connNo int, writeNo int, conn *coderws.Conn, payload []byte) bool,
) *openAIWSAuditUpstream {
	t.Helper()
	upstream := &openAIWSAuditUpstream{onWrite: onWrite}
	upstream.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if upstream.onConnect != nil {
			upstream.onConnect()
		}
		conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
		if err != nil {
			upstream.recordError(fmt.Errorf("accept upstream websocket: %w", err))
			return
		}
		defer func() { _ = conn.CloseNow() }()
		upstream.mu.Lock()
		upstream.authorizations = append(upstream.authorizations, r.Header.Get("Authorization"))
		upstream.mu.Unlock()
		connNo := int(upstream.connections.Add(1))
		for {
			readCtx, cancelRead := context.WithTimeout(r.Context(), 5*time.Second)
			_, payload, readErr := conn.Read(readCtx)
			cancelRead()
			if readErr != nil {
				return
			}
			writeNo := int(upstream.writes.Add(1))
			upstream.mu.Lock()
			upstream.payloads = append(upstream.payloads, append([]byte(nil), payload...))
			upstream.mu.Unlock()
			if upstream.onWrite != nil && !upstream.onWrite(connNo, writeNo, conn, payload) {
				return
			}
		}
	}))
	t.Cleanup(upstream.server.Close)
	return upstream
}

func (u *openAIWSAuditUpstream) recordError(err error) {
	if u == nil || err == nil {
		return
	}
	u.mu.Lock()
	u.errors = append(u.errors, err)
	u.mu.Unlock()
}

func (u *openAIWSAuditUpstream) writeCompleted(conn *coderws.Conn, responseID string) bool {
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelWrite()
	payload := fmt.Sprintf(
		`{"type":"response.completed","response":{"id":%q,"model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`,
		responseID,
	)
	if err := conn.Write(writeCtx, coderws.MessageText, []byte(payload)); err != nil {
		u.recordError(fmt.Errorf("write upstream response %s: %w", responseID, err))
		return false
	}
	return true
}

func (u *openAIWSAuditUpstream) snapshot() (connections int32, writes int32, payloads [][]byte, errs []error) {
	if u == nil {
		return 0, 0, nil, nil
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	cloned := make([][]byte, 0, len(u.payloads))
	for _, payload := range u.payloads {
		cloned = append(cloned, append([]byte(nil), payload...))
	}
	return u.connections.Load(), u.writes.Load(), cloned, append([]error(nil), u.errors...)
}

func (u *openAIWSAuditUpstream) authorizationSnapshot() []string {
	if u == nil {
		return nil
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.authorizations...)
}

type openAIWSAuditRedirectTransport struct {
	target *url.URL
	base   http.RoundTripper
}

func (t *openAIWSAuditRedirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil ||
		(!strings.EqualFold(req.URL.Hostname(), "chatgpt.com") && !strings.EqualFold(req.URL.Hostname(), "api.openai.com")) {
		return t.base.RoundTrip(req)
	}
	cloned := req.Clone(req.Context())
	rewritten := *req.URL
	rewritten.Scheme = t.target.Scheme
	rewritten.Host = t.target.Host
	cloned.URL = &rewritten
	cloned.Host = t.target.Host
	return t.base.RoundTrip(cloned)
}

func installOpenAIWSAuditUpstreamRedirect(t *testing.T, targetURL string) {
	t.Helper()
	target, err := url.Parse(targetURL)
	require.NoError(t, err)
	base, ok := http.DefaultTransport.(*http.Transport)
	require.True(t, ok)
	transport := base.Clone()
	previousClient := http.DefaultClient
	http.DefaultClient = &http.Client{Transport: &openAIWSAuditRedirectTransport{target: target, base: transport}}
	t.Cleanup(func() {
		http.DefaultClient = previousClient
		transport.CloseIdleConnections()
	})
}

type openAIWSFrameAdmissionAccountRepo struct {
	service.AccountRepository

	mu      sync.RWMutex
	account service.Account
}

func cloneOpenAIWSFrameAdmissionAccount(account service.Account) service.Account {
	cloned := account
	if account.Credentials != nil {
		cloned.Credentials = make(map[string]any, len(account.Credentials))
		for key, value := range account.Credentials {
			cloned.Credentials[key] = value
		}
	}
	if account.Extra != nil {
		cloned.Extra = make(map[string]any, len(account.Extra))
		for key, value := range account.Extra {
			cloned.Extra[key] = value
		}
	}
	return cloned
}

func (r *openAIWSFrameAdmissionAccountRepo) ListSchedulableByPlatform(_ context.Context, platform string) ([]service.Account, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.account.Platform != platform || !r.account.IsSchedulable() {
		return nil, nil
	}
	return []service.Account{cloneOpenAIWSFrameAdmissionAccount(r.account)}, nil
}

func (r *openAIWSFrameAdmissionAccountRepo) ListSchedulableByGroupIDAndPlatform(ctx context.Context, _ int64, platform string) ([]service.Account, error) {
	return r.ListSchedulableByPlatform(ctx, platform)
}

func (r *openAIWSFrameAdmissionAccountRepo) ListSchedulableUngroupedByPlatform(ctx context.Context, platform string) ([]service.Account, error) {
	return r.ListSchedulableByPlatform(ctx, platform)
}

func (r *openAIWSFrameAdmissionAccountRepo) GetByID(_ context.Context, id int64) (*service.Account, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.account.ID != id {
		return nil, nil
	}
	account := cloneOpenAIWSFrameAdmissionAccount(r.account)
	return &account, nil
}

func (r *openAIWSFrameAdmissionAccountRepo) setPlan(plan string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.account = cloneOpenAIWSFrameAdmissionAccount(r.account)
	r.account.Credentials["plan_type"] = plan
}

func (r *openAIWSFrameAdmissionAccountRepo) setAccessToken(token string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.account = cloneOpenAIWSFrameAdmissionAccount(r.account)
	r.account.Credentials["access_token"] = token
}

func newOpenAIWSSelectedAccountAuditHandler(
	t *testing.T,
	engine securityaudit.PromptEngine,
	account service.Account,
) (*OpenAIGatewayHandler, *openAIWSFrameAdmissionAccountRepo, *concurrencyCacheMock) {
	t.Helper()
	repo := &openAIWSFrameAdmissionAccountRepo{account: cloneOpenAIWSFrameAdmissionAccount(account)}
	handler, cache := newOpenAIWSSelectedAccountAuditHandlerWithRepo(t, engine, repo)
	return handler, repo, cache
}

func newOpenAIWSSelectedAccountsAuditHandler(
	t *testing.T,
	engine securityaudit.PromptEngine,
	accounts []service.Account,
) (*OpenAIGatewayHandler, *concurrencyCacheMock) {
	t.Helper()
	return newOpenAIWSSelectedAccountAuditHandlerWithRepo(
		t,
		engine,
		openAIImagesFailoverAccountRepo{accounts: accounts},
	)
}

func newOpenAIWSSelectedAccountAuditHandlerWithRepo(
	t *testing.T,
	engine securityaudit.PromptEngine,
	repo service.AccountRepository,
	channelServices ...*service.ChannelService,
) (*OpenAIGatewayHandler, *concurrencyCacheMock) {
	t.Helper()
	var channelService *service.ChannelService
	if len(channelServices) > 0 {
		channelService = channelServices[0]
	}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Default.RateMultiplier = 1
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.IngressModeDefault = service.OpenAIWSIngressModeCtxPool
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
	cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.IngressInterTurnIdleTimeoutSeconds = 3
	cfg.Gateway.MaxAccountSwitches = 1

	cache := &concurrencyCacheMock{
		acquireUserSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil },
		acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) {
			return true, nil
		},
	}
	concurrencyService := service.NewConcurrencyService(cache)
	rateLimitService := service.NewRateLimitService(repo, nil, cfg, nil, nil)
	billingCacheService := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billingCacheService.Stop)
	gatewayService := service.NewOpenAIGatewayService(
		repo, nil, nil, nil, nil, nil, nil, cfg, nil, concurrencyService,
		service.NewBillingService(cfg, nil), rateLimitService, billingCacheService,
		nil, &service.DeferredService{}, nil, nil, nil, channelService, nil, nil, nil,
	)
	handler := &OpenAIGatewayHandler{
		gatewayService:           gatewayService,
		billingCacheService:      billingCacheService,
		apiKeyService:            &service.APIKeyService{},
		concurrencyHelper:        NewConcurrencyHelper(concurrencyService, SSEPingFormatNone, time.Second),
		maxAccountSwitches:       1,
		cfg:                      cfg,
		securityAuditCoordinator: securityaudit.NewCoordinator(nil, engine),
	}
	return handler, cache
}

func newOpenAIWSSelectedProAuditHandler(
	t *testing.T,
	engine securityaudit.PromptEngine,
) (*OpenAIGatewayHandler, *concurrencyCacheMock) {
	t.Helper()
	account := service.Account{
		ID: 7801, Name: "openai-ws-selected-pro", Platform: service.PlatformOpenAI,
		Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
		Concurrency: 1, Priority: 1,
		Credentials: map[string]any{"access_token": "openai-ws-pro-token", "plan_type": "pro"},
		Extra: map[string]any{
			"openai_oauth_responses_websockets_v2_enabled": true,
			"openai_oauth_responses_websockets_v2_mode":    service.OpenAIWSIngressModeCtxPool,
		},
	}
	handler, _, cache := newOpenAIWSSelectedAccountAuditHandler(t, engine, account)
	return handler, cache
}

func newOpenAIWSAuditSelectionAccount(
	id int64,
	name string,
	accountType string,
	plan string,
	credential string,
	priority int,
) service.Account {
	account := newOpenAIWSNonTurnAdmissionAccount(accountType, plan)
	account.ID = id
	account.Name = name
	account.Priority = priority
	if accountType == service.AccountTypeAPIKey {
		account.Credentials["api_key"] = credential
	} else {
		account.Credentials["access_token"] = credential
	}
	return account
}

func newOpenAIWSSelectedProAuditServer(t *testing.T, handler *OpenAIGatewayHandler) (*httptest.Server, <-chan struct{}) {
	t.Helper()
	groupID := int64(7802)
	apiKey := &service.APIKey{
		ID: 7803, GroupID: &groupID, Status: service.StatusActive,
		User: &service.User{ID: 7804, Status: service.StatusActive},
		Group: &service.Group{
			ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive,
		},
	}
	done := make(chan struct{})
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware2.ContextKeyAPIKey), apiKey)
		c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: apiKey.User.ID, Concurrency: 1})
		c.Next()
	})
	router.GET("/openai/v1/responses", func(c *gin.Context) {
		defer close(done)
		handler.ResponsesWebSocket(c)
	})
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	return server, done
}

func dialOpenAIWSSelectedProAuditClient(t *testing.T, serverURL string) *coderws.Conn {
	t.Helper()
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelDial()
	conn, _, err := coderws.Dial(
		dialCtx,
		"ws"+strings.TrimPrefix(serverURL, "http")+"/openai/v1/responses",
		&coderws.DialOptions{CompressionMode: coderws.CompressionContextTakeover},
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

func writeOpenAIWSAuditRequest(t *testing.T, conn *coderws.Conn, payload string) {
	t.Helper()
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelWrite()
	require.NoError(t, conn.Write(writeCtx, coderws.MessageText, []byte(payload)))
}

func readOpenAIWSAuditCompleted(t *testing.T, conn *coderws.Conn) []byte {
	t.Helper()
	readCtx, cancelRead := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRead()
	for {
		_, event, err := conn.Read(readCtx)
		require.NoError(t, err)
		if gjson.GetBytes(event, "type").String() == "response.completed" {
			return event
		}
	}
}

func requireOpenAIWSAuditClose(t *testing.T, conn *coderws.Conn, want coderws.StatusCode) error {
	t.Helper()
	readCtx, cancelRead := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRead()
	_, _, err := conn.Read(readCtx)
	require.Error(t, err)
	require.Equal(t, want, coderws.CloseStatus(err), err)
	return err
}

func requireOpenAIWSAuditHandlerDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("websocket audit handler did not finish")
	}
}

func TestOpenAIResponsesWebSocket_SelectedProFirstTurnBlockingScannerStopsBeforeUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := newOpenAIWSAuditUpstream(t, nil)
	installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
	engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
		return http.StatusOK, openAIWSAuditUnsafeOutput
	})
	handler, cache := newOpenAIWSSelectedProAuditHandler(t, engine)
	server, done := newOpenAIWSSelectedProAuditServer(t, handler)
	client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"input":[{"type":"additional_tools","role":"developer","tools":[{"type":"custom","name":"exec","description":"ws-additional-tools-canary"}]},{"type":"message","role":"user","content":"ws-first-block-canary"}]}`)
	requireOpenAIWSAuditClose(t, client, coderws.StatusPolicyViolation)
	requireOpenAIWSAuditHandlerDone(t, done)

	calls := scannerCalls()
	require.Len(t, calls, 1)
	require.Contains(t, calls[0], "ws-first-block-canary")
	require.Contains(t, calls[0], "ws-additional-tools-canary")
	connections, writes, _, upstreamErrs := upstream.snapshot()
	require.Zero(t, connections, "blocking scanner must run before upstream WebSocket dispatch")
	require.Zero(t, writes, "blocked first turn must never be written upstream")
	require.Empty(t, upstreamErrs)
	require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseAccountCalled))
	require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
}

func TestOpenAIResponsesWebSocket_SelectedProFirstTurnAuditUnavailableFallsBackOnlyToVerifiedNonPro(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		name        string
		accountType string
		plan        string
		credential  string
	}{
		{name: "OAuth Plus", accountType: service.AccountTypeOAuth, plan: "plus", credential: "ws-fallback-plus-token"},
		{name: "OAuth Team", accountType: service.AccountTypeOAuth, plan: "team", credential: "ws-fallback-team-token"},
		{name: "API key", accountType: service.AccountTypeAPIKey, credential: "ws-fallback-api-key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			upstream := newOpenAIWSAuditUpstream(t, nil)
			upstream.onWrite = func(_ int, _ int, conn *coderws.Conn, _ []byte) bool {
				return upstream.writeCompleted(conn, "resp_ws_audit_fallback_ok")
			}
			installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
			engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
				return http.StatusServiceUnavailable, ""
			})
			accounts := []service.Account{
				newOpenAIWSAuditSelectionAccount(7811, "ws-fallback-pro", service.AccountTypeOAuth, "pro", "ws-fallback-pro-token", 0),
				newOpenAIWSAuditSelectionAccount(7812, "ws-fallback-unknown", service.AccountTypeOAuth, "future-plan", "ws-fallback-unknown-token", 1),
				newOpenAIWSAuditSelectionAccount(7813, "ws-fallback-second-pro", service.AccountTypeOAuth, "pro", "ws-fallback-second-pro-token", 2),
				newOpenAIWSAuditSelectionAccount(7814, "ws-fallback-verified", test.accountType, test.plan, test.credential, 3),
			}
			handler, cache := newOpenAIWSSelectedAccountsAuditHandler(t, engine, accounts)
			server, done := newOpenAIWSSelectedProAuditServer(t, handler)
			client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

			writeOpenAIWSAuditRequest(t, client,
				`{"type":"response.create","model":"gpt-5.1","stream":false,"input":"ws-audit-unavailable-fallback-canary"}`)
			completed := readOpenAIWSAuditCompleted(t, client)
			require.Equal(t, "resp_ws_audit_fallback_ok", gjson.GetBytes(completed, "response.id").String())
			require.NoError(t, client.Close(coderws.StatusNormalClosure, "done"))
			requireOpenAIWSAuditHandlerDone(t, done)

			calls := scannerCalls()
			require.Len(t, calls, 1, "scanner-unavailable fallback must be attempted at most once")
			require.Contains(t, calls[0], "ws-audit-unavailable-fallback-canary")
			connections, writes, payloads, upstreamErrs := upstream.snapshot()
			require.Equal(t, int32(1), connections, "only the verified fallback may open an upstream connection")
			require.Equal(t, int32(1), writes)
			require.Len(t, payloads, 1)
			require.Contains(t, string(payloads[0]), "ws-audit-unavailable-fallback-canary")
			require.Equal(t, []string{"Bearer " + test.credential}, upstream.authorizationSnapshot(),
				"unknown and Pro candidates must not satisfy the audit-exempt fallback")
			require.Empty(t, upstreamErrs)
			require.Equal(t, int32(2), atomic.LoadInt32(&cache.releaseAccountCalled))
			require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
		})
	}
}

func TestOpenAIResponsesWebSocket_SelectedProFirstTurnAuditUnavailableWithoutVerifiedFallbackCloses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := newOpenAIWSAuditUpstream(t, nil)
	installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
	engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
		return http.StatusServiceUnavailable, ""
	})
	accounts := []service.Account{
		newOpenAIWSAuditSelectionAccount(7821, "ws-no-fallback-pro", service.AccountTypeOAuth, "pro", "ws-no-fallback-pro-token", 0),
		newOpenAIWSAuditSelectionAccount(7822, "ws-no-fallback-unknown", service.AccountTypeOAuth, "future-plan", "ws-no-fallback-unknown-token", 1),
		newOpenAIWSAuditSelectionAccount(7823, "ws-no-fallback-second-pro", service.AccountTypeOAuth, "pro", "ws-no-fallback-second-pro-token", 2),
	}
	handler, cache := newOpenAIWSSelectedAccountsAuditHandler(t, engine, accounts)
	server, done := newOpenAIWSSelectedProAuditServer(t, handler)
	client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"input":"ws-audit-unavailable-no-fallback-canary"}`)
	closeErr := requireOpenAIWSAuditClose(t, client, coderws.StatusTryAgainLater)
	require.Contains(t, closeErr.Error(), "no available account")
	requireOpenAIWSAuditHandlerDone(t, done)

	calls := scannerCalls()
	require.Len(t, calls, 1, "scanner-unavailable fallback must be attempted at most once")
	require.Contains(t, calls[0], "ws-audit-unavailable-no-fallback-canary")
	connections, writes, payloads, upstreamErrs := upstream.snapshot()
	require.Zero(t, connections, "no candidate may open an upstream connection")
	require.Zero(t, writes)
	require.Empty(t, payloads)
	require.Empty(t, upstream.authorizationSnapshot())
	require.Empty(t, upstreamErrs)
	require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseAccountCalled))
	require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
}

func TestOpenAIResponsesWebSocket_UninspectableHostedToolRequiresVerifiedNonPro(t *testing.T) {
	gin.SetMode(gin.TestMode)
	payload := `{"type":"response.create","model":"gpt-5.1","stream":false,"input":[{"type":"additional_tools","role":"developer","tools":[{"type":"web_search","name":"search"}]},{"type":"message","role":"user","content":"ws-hosted-search-canary"}]}`

	t.Run("verified Team is selected", func(t *testing.T) {
		upstream := newOpenAIWSAuditUpstream(t, nil)
		upstream.onWrite = func(_ int, _ int, conn *coderws.Conn, _ []byte) bool {
			return upstream.writeCompleted(conn, "resp_ws_hosted_search_ok")
		}
		installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
		engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
			return http.StatusOK, openAIWSAuditSafeOutput
		})
		accounts := []service.Account{
			newOpenAIWSAuditSelectionAccount(7831, "ws-hosted-pro", service.AccountTypeOAuth, "pro", "ws-hosted-pro-token", 0),
			newOpenAIWSAuditSelectionAccount(7832, "ws-hosted-unknown", service.AccountTypeOAuth, "future-plan", "ws-hosted-unknown-token", 1),
			newOpenAIWSAuditSelectionAccount(7833, "ws-hosted-team", service.AccountTypeOAuth, "team", "ws-hosted-team-token", 2),
		}
		handler, cache := newOpenAIWSSelectedAccountsAuditHandler(t, engine, accounts)
		server, done := newOpenAIWSSelectedProAuditServer(t, handler)
		client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

		writeOpenAIWSAuditRequest(t, client, payload)
		completed := readOpenAIWSAuditCompleted(t, client)
		require.Equal(t, "resp_ws_hosted_search_ok", gjson.GetBytes(completed, "response.id").String())
		require.NoError(t, client.Close(coderws.StatusNormalClosure, "done"))
		requireOpenAIWSAuditHandlerDone(t, done)

		require.Empty(t, scannerCalls(), "uninspectable hosted content must never enter the Pro scanner")
		connections, writes, payloads, upstreamErrs := upstream.snapshot()
		require.Equal(t, int32(1), connections)
		require.Equal(t, int32(1), writes)
		require.Len(t, payloads, 1)
		require.Equal(t, []string{"Bearer ws-hosted-team-token"}, upstream.authorizationSnapshot())
		require.Empty(t, upstreamErrs)
		require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseAccountCalled))
		require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
	})

	t.Run("no verified non-Pro closes before upstream", func(t *testing.T) {
		upstream := newOpenAIWSAuditUpstream(t, nil)
		installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
		engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
			return http.StatusOK, openAIWSAuditSafeOutput
		})
		accounts := []service.Account{
			newOpenAIWSAuditSelectionAccount(7841, "ws-hosted-only-pro", service.AccountTypeOAuth, "pro", "ws-hosted-only-pro-token", 0),
			newOpenAIWSAuditSelectionAccount(7842, "ws-hosted-only-unknown", service.AccountTypeOAuth, "future-plan", "ws-hosted-only-unknown-token", 1),
		}
		handler, cache := newOpenAIWSSelectedAccountsAuditHandler(t, engine, accounts)
		server, done := newOpenAIWSSelectedProAuditServer(t, handler)
		client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

		writeOpenAIWSAuditRequest(t, client, payload)
		closeErr := requireOpenAIWSAuditClose(t, client, coderws.StatusTryAgainLater)
		require.Contains(t, closeErr.Error(), "no available account")
		requireOpenAIWSAuditHandlerDone(t, done)

		require.Empty(t, scannerCalls())
		connections, writes, payloads, upstreamErrs := upstream.snapshot()
		require.Zero(t, connections)
		require.Zero(t, writes)
		require.Empty(t, payloads)
		require.Empty(t, upstream.authorizationSnapshot())
		require.Empty(t, upstreamErrs)
		require.Equal(t, int32(0), atomic.LoadInt32(&cache.releaseAccountCalled))
		require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
	})
}

func TestOpenAIResponsesWebSocket_EffectiveSearchModelMappingsRequireVerifiedNonPro(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const (
		groupID     = int64(7802)
		clientModel = "public-alias"
		searchModel = "gpt-5-search-api"
	)
	verifiedAccounts := []struct {
		name        string
		accountType string
		plan        string
		credential  string
		upstream    string
	}{
		{name: "Plus", accountType: service.AccountTypeOAuth, plan: "plus", credential: "ws-mapped-plus-token", upstream: "gpt-5.4"},
		{name: "Team", accountType: service.AccountTypeOAuth, plan: "team", credential: "ws-mapped-team-token", upstream: "gpt-5.4"},
		{name: "APIKey", accountType: service.AccountTypeAPIKey, credential: "ws-mapped-api-key", upstream: searchModel},
	}
	mappingKinds := []struct {
		name    string
		prepare func([]service.Account) ([]service.Account, *service.ChannelService)
	}{
		{
			name: "channel alias",
			prepare: func(accounts []service.Account) ([]service.Account, *service.ChannelService) {
				return accounts, selectedAccountAuditChannelService(groupID, clientModel, searchModel)
			},
		},
		{
			name: "account alias",
			prepare: func(accounts []service.Account) ([]service.Account, *service.ChannelService) {
				return selectedAccountAuditWithModelMapping(accounts, clientModel, searchModel), nil
			},
		},
	}

	for _, mappingKind := range mappingKinds {
		for _, verified := range verifiedAccounts {
			t.Run(mappingKind.name+"/"+verified.name, func(t *testing.T) {
				upstream := newOpenAIWSAuditUpstream(t, nil)
				upstream.onWrite = func(_ int, _ int, conn *coderws.Conn, _ []byte) bool {
					return upstream.writeCompleted(conn, "resp_ws_mapped_search_ok")
				}
				installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
				engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
					return http.StatusOK, openAIWSAuditSafeOutput
				})
				accounts := []service.Account{
					newOpenAIWSAuditSelectionAccount(7851, "ws-mapped-pro", service.AccountTypeOAuth, "pro", "ws-mapped-pro-token", 0),
					newOpenAIWSAuditSelectionAccount(7852, "ws-mapped-unknown", service.AccountTypeOAuth, "future-plan", "ws-mapped-unknown-token", 1),
					newOpenAIWSAuditSelectionAccount(7853, "ws-mapped-verified", verified.accountType, verified.plan, verified.credential, 2),
				}
				accounts, channelService := mappingKind.prepare(accounts)
				handler, _ := newOpenAIWSSelectedAccountAuditHandlerWithRepo(
					t, engine, openAIImagesFailoverAccountRepo{accounts: accounts}, channelService,
				)
				server, done := newOpenAIWSSelectedProAuditServer(t, handler)
				client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

				writeOpenAIWSAuditRequest(t, client,
					`{"type":"response.create","model":"`+clientModel+`","stream":false,"input":"ws-mapped-search-canary"}`)
				completed := readOpenAIWSAuditCompleted(t, client)
				require.Equal(t, "resp_ws_mapped_search_ok", gjson.GetBytes(completed, "response.id").String())
				require.NoError(t, client.Close(coderws.StatusNormalClosure, "done"))
				requireOpenAIWSAuditHandlerDone(t, done)

				require.Empty(t, scannerCalls(), "effective remote-search models cannot be converted into a Pro scan proof")
				connections, writes, payloads, upstreamErrs := upstream.snapshot()
				require.Equal(t, int32(1), connections)
				require.Equal(t, int32(1), writes)
				require.Len(t, payloads, 1)
				require.Equal(t, verified.upstream, gjson.GetBytes(payloads[0], "model").String())
				require.Equal(t, []string{"Bearer " + verified.credential}, upstream.authorizationSnapshot())
				require.Empty(t, upstreamErrs)
			})
		}
	}
}

func TestOpenAIResponsesWebSocket_EffectiveSearchModelMappingsWithoutVerifiedNonProCloses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const (
		groupID     = int64(7802)
		clientModel = "public-alias"
		searchModel = "gpt-5-search-api"
	)
	unsafeAccounts := []service.Account{
		newOpenAIWSAuditSelectionAccount(7861, "ws-mapped-only-pro", service.AccountTypeOAuth, "pro", "ws-mapped-only-pro-token", 0),
		newOpenAIWSAuditSelectionAccount(7862, "ws-mapped-only-unknown", service.AccountTypeOAuth, "future-plan", "ws-mapped-only-unknown-token", 1),
	}
	tests := []struct {
		name           string
		accounts       []service.Account
		channelService *service.ChannelService
	}{
		{
			name:           "channel alias",
			accounts:       unsafeAccounts,
			channelService: selectedAccountAuditChannelService(groupID, clientModel, searchModel),
		},
		{
			name:     "account alias",
			accounts: selectedAccountAuditWithModelMapping(unsafeAccounts, clientModel, searchModel),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := newOpenAIWSAuditUpstream(t, nil)
			installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
			engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
				return http.StatusOK, openAIWSAuditSafeOutput
			})
			handler, _ := newOpenAIWSSelectedAccountAuditHandlerWithRepo(
				t, engine, openAIImagesFailoverAccountRepo{accounts: test.accounts}, test.channelService,
			)
			server, done := newOpenAIWSSelectedProAuditServer(t, handler)
			client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

			writeOpenAIWSAuditRequest(t, client,
				`{"type":"response.create","model":"`+clientModel+`","stream":false,"input":"ws-mapped-search-no-fallback"}`)
			closeErr := requireOpenAIWSAuditClose(t, client, coderws.StatusTryAgainLater)
			require.Contains(t, closeErr.Error(), "no available account")
			requireOpenAIWSAuditHandlerDone(t, done)

			require.Empty(t, scannerCalls())
			connections, writes, payloads, upstreamErrs := upstream.snapshot()
			require.Zero(t, connections)
			require.Zero(t, writes)
			require.Empty(t, payloads)
			require.Empty(t, upstream.authorizationSnapshot())
			require.Empty(t, upstreamErrs)
		})
	}
}

func TestOpenAIResponsesWebSocket_LaterTurnEffectiveSearchModelMappingsRecheckBoundAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const (
		groupID     = int64(7802)
		clientModel = "public-alias"
		searchModel = "gpt-5-search-api"
	)
	mappingKinds := []struct {
		name    string
		prepare func(service.Account) (service.Account, *service.ChannelService)
	}{
		{
			name: "channel alias",
			prepare: func(account service.Account) (service.Account, *service.ChannelService) {
				return account, selectedAccountAuditChannelService(groupID, clientModel, searchModel)
			},
		},
		{
			name: "account alias",
			prepare: func(account service.Account) (service.Account, *service.ChannelService) {
				account = selectedAccountAuditWithModelMapping([]service.Account{account}, clientModel, searchModel)[0]
				account.Credentials["model_mapping"] = map[string]any{
					"gpt-5.1":   "gpt-5.1",
					clientModel: searchModel,
				}
				return account, nil
			},
		},
	}
	accountClasses := []struct {
		name              string
		plan              string
		wantSecondAllowed bool
	}{
		{name: "Pro rejected", plan: "pro"},
		{name: "unknown rejected", plan: "future-plan"},
		{name: "Team allowed", plan: "team", wantSecondAllowed: true},
	}

	for _, mappingKind := range mappingKinds {
		for _, accountClass := range accountClasses {
			t.Run(mappingKind.name+"/"+accountClass.name, func(t *testing.T) {
				upstream := newOpenAIWSAuditUpstream(t, nil)
				upstream.onWrite = func(_ int, writeNo int, conn *coderws.Conn, _ []byte) bool {
					return upstream.writeCompleted(conn, fmt.Sprintf("resp_ws_mapping_turn_%d", writeNo))
				}
				installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
				engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
					return http.StatusOK, openAIWSAuditSafeOutput
				})
				account := newOpenAIWSAuditSelectionAccount(
					7871, "ws-mapped-turn-account", service.AccountTypeOAuth, accountClass.plan, "ws-mapped-turn-token", 0,
				)
				account, channelService := mappingKind.prepare(account)
				handler, _ := newOpenAIWSSelectedAccountAuditHandlerWithRepo(
					t, engine, &openAIWSFrameAdmissionAccountRepo{account: account}, channelService,
				)
				server, done := newOpenAIWSSelectedProAuditServer(t, handler)
				client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

				writeOpenAIWSAuditRequest(t, client,
					`{"type":"response.create","model":"gpt-5.1","stream":false,"input":"ws-mapping-first-turn"}`)
				first := readOpenAIWSAuditCompleted(t, client)
				require.Equal(t, "resp_ws_mapping_turn_1", gjson.GetBytes(first, "response.id").String())
				writeOpenAIWSAuditRequest(t, client,
					`{"type":"response.create","model":"`+clientModel+`","stream":false,"previous_response_id":"resp_ws_mapping_turn_1","input":"ws-mapping-second-turn"}`)

				if accountClass.wantSecondAllowed {
					second := readOpenAIWSAuditCompleted(t, client)
					require.Equal(t, "resp_ws_mapping_turn_2", gjson.GetBytes(second, "response.id").String())
					require.NoError(t, client.Close(coderws.StatusNormalClosure, "done"))
				} else {
					requireOpenAIWSAuditClose(t, client, coderws.StatusTryAgainLater)
				}
				requireOpenAIWSAuditHandlerDone(t, done)

				connections, writes, payloads, upstreamErrs := upstream.snapshot()
				require.Equal(t, int32(1), connections)
				if accountClass.wantSecondAllowed {
					require.Equal(t, int32(2), writes)
					require.Len(t, payloads, 2)
					require.Equal(t, "gpt-5.4", gjson.GetBytes(payloads[1], "model").String())
					require.Empty(t, scannerCalls(), "verified non-Pro turns must not enter the Pro scanner")
				} else {
					require.Equal(t, int32(1), writes, "mapped remote-search turn must be rejected before upstream write")
					require.Len(t, payloads, 1)
					for _, call := range scannerCalls() {
						require.NotContains(t, call, "ws-mapping-second-turn")
					}
				}
				require.Empty(t, upstreamErrs)
			})
		}
	}
}

func TestOpenAIResponsesWebSocket_SelectedProAuditsEachAllowedTurn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := newOpenAIWSAuditUpstream(t, nil)
	upstream.onWrite = func(_ int, writeNo int, conn *coderws.Conn, _ []byte) bool {
		return upstream.writeCompleted(conn, fmt.Sprintf("resp_ws_audit_allow_%d", writeNo))
	}
	installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
	engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
		return http.StatusOK, openAIWSAuditSafeOutput
	})
	handler, _ := newOpenAIWSSelectedProAuditHandler(t, engine)
	server, done := newOpenAIWSSelectedProAuditServer(t, handler)
	client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"input":"ws-first-allow-canary"}`)
	first := readOpenAIWSAuditCompleted(t, client)
	require.Equal(t, "resp_ws_audit_allow_1", gjson.GetBytes(first, "response.id").String())
	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"previous_response_id":"resp_ws_audit_allow_1","input":"ws-second-allow-canary"}`)
	second := readOpenAIWSAuditCompleted(t, client)
	require.Equal(t, "resp_ws_audit_allow_2", gjson.GetBytes(second, "response.id").String())
	require.NoError(t, client.Close(coderws.StatusNormalClosure, "done"))
	requireOpenAIWSAuditHandlerDone(t, done)

	calls := scannerCalls()
	require.Len(t, calls, 2, "each response.create turn on Pro must reach the blocking scanner once")
	require.Contains(t, calls[0], "ws-first-allow-canary")
	require.Contains(t, calls[1], "ws-second-allow-canary")
	connections, writes, payloads, upstreamErrs := upstream.snapshot()
	require.Equal(t, int32(1), connections)
	require.Equal(t, int32(2), writes)
	require.Len(t, payloads, 2)
	require.Contains(t, string(payloads[0]), "ws-first-allow-canary")
	require.Contains(t, string(payloads[1]), "ws-second-allow-canary")
	require.Empty(t, upstreamErrs)
}

func TestOpenAIResponsesWebSocket_SelectedProSecondTurnBlockStopsBeforeUpstreamWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := newOpenAIWSAuditUpstream(t, nil)
	upstream.onWrite = func(_ int, writeNo int, conn *coderws.Conn, _ []byte) bool {
		return upstream.writeCompleted(conn, fmt.Sprintf("resp_ws_audit_block_%d", writeNo))
	}
	installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
	engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(call int, text string) (int, string) {
		if call == 2 && strings.Contains(text, "ws-second-early-block-canary") {
			return http.StatusOK, openAIWSAuditUnsafeOutput
		}
		return http.StatusOK, openAIWSAuditSafeOutput
	})
	handler, _ := newOpenAIWSSelectedProAuditHandler(t, engine)
	server, done := newOpenAIWSSelectedProAuditServer(t, handler)
	client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"input":"ws-before-second-block-canary"}`)
	first := readOpenAIWSAuditCompleted(t, client)
	require.Equal(t, "resp_ws_audit_block_1", gjson.GetBytes(first, "response.id").String())
	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"previous_response_id":"resp_ws_audit_block_1","input":[{"role":"user","content":"ws-second-early-block-canary"},{"role":"user","content":"ws-second-later-safe-canary"}]}`)
	requireOpenAIWSAuditClose(t, client, coderws.StatusPolicyViolation)
	requireOpenAIWSAuditHandlerDone(t, done)

	calls := scannerCalls()
	require.Len(t, calls, 2)
	require.Contains(t, calls[0], "ws-before-second-block-canary")
	require.Contains(t, calls[1], "ws-second-early-block-canary")
	require.Contains(t, calls[1], "ws-second-later-safe-canary")
	connections, writes, payloads, upstreamErrs := upstream.snapshot()
	require.Equal(t, int32(1), connections)
	require.Equal(t, int32(1), writes, "blocked second turn must stop before a second upstream write")
	require.Len(t, payloads, 1)
	require.NotContains(t, string(payloads[0]), "ws-second-early-block-canary")
	require.Empty(t, upstreamErrs)
}

func TestOpenAIResponsesWebSocket_SelectedProTransportRetryReusesTurnAudit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := newOpenAIWSAuditUpstream(t, nil)
	upstream.onWrite = func(connNo int, _ int, conn *coderws.Conn, _ []byte) bool {
		if connNo == 1 {
			_ = conn.CloseNow()
			return false
		}
		return upstream.writeCompleted(conn, "resp_ws_audit_retry_ok")
	}
	installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
	engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
		return http.StatusOK, openAIWSAuditSafeOutput
	})
	handler, _ := newOpenAIWSSelectedProAuditHandler(t, engine)
	server, done := newOpenAIWSSelectedProAuditServer(t, handler)
	client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"input":"ws-transport-retry-canary"}`)
	completed := readOpenAIWSAuditCompleted(t, client)
	require.Equal(t, "resp_ws_audit_retry_ok", gjson.GetBytes(completed, "response.id").String())
	require.NoError(t, client.Close(coderws.StatusNormalClosure, "done"))
	requireOpenAIWSAuditHandlerDone(t, done)

	calls := scannerCalls()
	require.Len(t, calls, 1, "same-turn transport retry must reuse the allowed scanner decision")
	require.Contains(t, calls[0], "ws-transport-retry-canary")
	connections, writes, payloads, upstreamErrs := upstream.snapshot()
	require.Equal(t, int32(2), connections, "retry must establish one replacement upstream connection")
	require.Equal(t, int32(2), writes, "the same audited turn should be attempted once per transport")
	require.Len(t, payloads, 2)
	require.Contains(t, string(payloads[0]), "ws-transport-retry-canary")
	require.Contains(t, string(payloads[1]), "ws-transport-retry-canary")
	require.Empty(t, upstreamErrs)
}

func newOpenAIWSNonTurnAdmissionAccount(accountType string, plan string) service.Account {
	credentials := map[string]any{"access_token": "openai-ws-non-turn-token"}
	extraKey := "openai_oauth_responses_websockets_v2_mode"
	if accountType == service.AccountTypeAPIKey {
		credentials = map[string]any{"api_key": "openai-ws-non-turn-api-key"}
		extraKey = "openai_apikey_responses_websockets_v2_mode"
	} else if plan != "" {
		credentials["plan_type"] = plan
	}
	return service.Account{
		ID: 7901, Name: "openai-ws-non-turn-admission", Platform: service.PlatformOpenAI,
		Type: accountType, Status: service.StatusActive, Schedulable: true,
		Concurrency: 1, Priority: 1,
		Credentials: credentials,
		Extra: map[string]any{
			extraKey: service.OpenAIWSIngressModePassthrough,
		},
	}
}

func startOpenAIWSNonTurnAdmissionSession(
	t *testing.T,
	account service.Account,
) (*coderws.Conn, <-chan struct{}, *openAIWSAuditUpstream, func() []string, *openAIWSFrameAdmissionAccountRepo) {
	t.Helper()
	upstream := newOpenAIWSAuditUpstream(t, nil)
	upstream.onWrite = func(_ int, writeNo int, conn *coderws.Conn, _ []byte) bool {
		if writeNo == 1 {
			return upstream.writeCompleted(conn, "resp_ws_non_turn_first")
		}
		return true
	}
	installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
	engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
		return http.StatusOK, openAIWSAuditSafeOutput
	})
	handler, repo, _ := newOpenAIWSSelectedAccountAuditHandler(t, engine, account)
	server, done := newOpenAIWSSelectedProAuditServer(t, handler)
	client := dialOpenAIWSSelectedProAuditClient(t, server.URL)
	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"input":"ws-non-turn-first-canary"}`)
	completed := readOpenAIWSAuditCompleted(t, client)
	require.Equal(t, "resp_ws_non_turn_first", gjson.GetBytes(completed, "response.id").String())
	return client, done, upstream, scannerCalls, repo
}

func requireOpenAIWSAuditUpstreamWrites(t *testing.T, upstream *openAIWSAuditUpstream, want int32) {
	t.Helper()
	require.Eventually(t, func() bool {
		_, writes, _, _ := upstream.snapshot()
		return writes == want
	}, 3*time.Second, 10*time.Millisecond)
}

func TestOpenAIResponsesWebSocket_SelectedProRejectsUninspectableNonTurnFramesBeforeUpstreamWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		name    string
		payload string
		canary  string
	}{
		{
			name:    "session update",
			payload: `{"type":"session.update","session":{"instructions":"ws-session-update-canary"}}`,
			canary:  "ws-session-update-canary",
		},
		{
			name:    "conversation item create",
			payload: `{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"ws-item-create-canary"}]}}`,
			canary:  "ws-item-create-canary",
		},
		{
			name:    "unknown frame",
			payload: `{"type":"future.model_visible.update","text":"ws-unknown-frame-canary"}`,
			canary:  "ws-unknown-frame-canary",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			account := newOpenAIWSNonTurnAdmissionAccount(service.AccountTypeOAuth, "pro")
			client, done, upstream, scannerCalls, _ := startOpenAIWSNonTurnAdmissionSession(t, account)

			writeOpenAIWSAuditRequest(t, client, test.payload)
			requireOpenAIWSAuditClose(t, client, coderws.StatusTryAgainLater)
			requireOpenAIWSAuditHandlerDone(t, done)

			calls := scannerCalls()
			require.Len(t, calls, 1, "non-turn frame must not bypass or spuriously re-enter the turn scanner")
			require.Contains(t, calls[0], "ws-non-turn-first-canary")
			require.NotContains(t, calls[0], test.canary)
			connections, writes, payloads, upstreamErrs := upstream.snapshot()
			require.Equal(t, int32(1), connections)
			require.Equal(t, int32(1), writes, "rejected non-turn frame must stop before upstream write")
			require.Len(t, payloads, 1)
			require.NotContains(t, string(payloads[0]), test.canary)
			require.Empty(t, upstreamErrs)
		})
	}
}

func TestOpenAIResponsesWebSocket_VerifiedNonProForwardsUninspectableNonTurnFrameWithStableProof(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct {
		name        string
		accountType string
		plan        string
	}{
		{name: "oauth plus", accountType: service.AccountTypeOAuth, plan: "plus"},
		{name: "oauth team", accountType: service.AccountTypeOAuth, plan: "team"},
		{name: "api key", accountType: service.AccountTypeAPIKey},
	} {
		t.Run(test.name, func(t *testing.T) {
			account := newOpenAIWSNonTurnAdmissionAccount(test.accountType, test.plan)
			client, done, upstream, scannerCalls, _ := startOpenAIWSNonTurnAdmissionSession(t, account)
			payload := `{"type":"session.update","session":{"instructions":"ws-verified-non-pro-canary"}}`

			writeOpenAIWSAuditRequest(t, client, payload)
			requireOpenAIWSAuditUpstreamWrites(t, upstream, 2)
			require.NoError(t, client.Close(coderws.StatusNormalClosure, "done"))
			requireOpenAIWSAuditHandlerDone(t, done)

			require.Empty(t, scannerCalls(), "audit-exempt account must not call the Pro scanner")
			connections, writes, payloads, upstreamErrs := upstream.snapshot()
			require.Equal(t, int32(1), connections)
			require.Equal(t, int32(2), writes)
			require.Len(t, payloads, 2)
			require.Equal(t, payload, string(payloads[1]), "admitted non-turn frame must be forwarded verbatim")
			require.Empty(t, upstreamErrs)
		})
	}
}

func TestOpenAIResponsesWebSocket_UnknownAccountRejectsUninspectableNonTurnFrame(t *testing.T) {
	gin.SetMode(gin.TestMode)
	account := newOpenAIWSNonTurnAdmissionAccount(service.AccountTypeOAuth, "future_entitlement")
	client, done, upstream, scannerCalls, _ := startOpenAIWSNonTurnAdmissionSession(t, account)

	writeOpenAIWSAuditRequest(t, client,
		`{"type":"session.update","session":{"instructions":"ws-unknown-account-canary"}}`)
	requireOpenAIWSAuditClose(t, client, coderws.StatusTryAgainLater)
	requireOpenAIWSAuditHandlerDone(t, done)

	calls := scannerCalls()
	require.Len(t, calls, 1, "unknown OAuth plan is conservatively scanned for the auditable first turn")
	require.Contains(t, calls[0], "ws-non-turn-first-canary")
	require.NotContains(t, calls[0], "ws-unknown-account-canary")
	_, writes, payloads, upstreamErrs := upstream.snapshot()
	require.Equal(t, int32(1), writes, "unknown account must not carry an uninspectable frame")
	require.Len(t, payloads, 1)
	require.NotContains(t, string(payloads[0]), "ws-unknown-account-canary")
	require.Empty(t, upstreamErrs)
}

func TestOpenAIResponsesWebSocket_PlusToProDriftRejectsNonTurnFrameBeforeUpstreamWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	account := newOpenAIWSNonTurnAdmissionAccount(service.AccountTypeOAuth, "plus")
	client, done, upstream, scannerCalls, repo := startOpenAIWSNonTurnAdmissionSession(t, account)
	repo.setPlan("pro")

	writeOpenAIWSAuditRequest(t, client,
		`{"type":"session.update","session":{"instructions":"ws-plus-to-pro-drift-canary"}}`)
	requireOpenAIWSAuditClose(t, client, coderws.StatusTryAgainLater)
	requireOpenAIWSAuditHandlerDone(t, done)

	require.Empty(t, scannerCalls())
	_, writes, payloads, upstreamErrs := upstream.snapshot()
	require.Equal(t, int32(1), writes, "fresh Pro classification must stop the frame before upstream write")
	require.Len(t, payloads, 1)
	require.NotContains(t, string(payloads[0]), "ws-plus-to-pro-drift-canary")
	require.Empty(t, upstreamErrs)
}

func TestOpenAIResponsesWebSocket_FirstTurnTokenDriftDuringDialStopsBeforeUpstreamWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := newOpenAIWSAuditUpstream(t, nil)
	installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
	engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
		return http.StatusOK, openAIWSAuditSafeOutput
	})
	account := newOpenAIWSNonTurnAdmissionAccount(service.AccountTypeOAuth, "plus")
	repo := &openAIWSFrameAdmissionAccountRepo{account: cloneOpenAIWSFrameAdmissionAccount(account)}
	var driftOnce sync.Once
	upstream.onConnect = func() {
		driftOnce.Do(func() { repo.setAccessToken("openai-ws-rotated-before-first-write") })
	}
	handler, _ := newOpenAIWSSelectedAccountAuditHandlerWithRepo(t, engine, repo)
	server, done := newOpenAIWSSelectedProAuditServer(t, handler)
	client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"input":"ws-first-turn-token-drift-canary"}`)
	requireOpenAIWSAuditClose(t, client, coderws.StatusTryAgainLater)
	requireOpenAIWSAuditHandlerDone(t, done)

	require.Empty(t, scannerCalls(), "verified non-Pro first turn must not enter the Pro scanner")
	connections, writes, payloads, upstreamErrs := upstream.snapshot()
	require.Equal(t, int32(1), connections, "the credential may change after handshake establishment")
	require.Zero(t, writes, "fresh proof validation must stop the first business frame")
	require.Empty(t, payloads)
	require.Empty(t, upstreamErrs)
}

func TestOpenAIResponsesWebSocket_PooledFirstTurnTokenDriftDuringStagedAcquireStopsBeforeUpstreamWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := newOpenAIWSAuditUpstream(t, nil)
	connectEntered := make(chan struct{})
	allowConnect := make(chan struct{})
	var connectEnteredOnce sync.Once
	var allowConnectOnce sync.Once
	upstream.onConnect = func() {
		connectEnteredOnce.Do(func() { close(connectEntered) })
		<-allowConnect
	}
	t.Cleanup(func() { allowConnectOnce.Do(func() { close(allowConnect) }) })
	installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
	engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
		return http.StatusOK, openAIWSAuditSafeOutput
	})
	account := newOpenAIWSNonTurnAdmissionAccount(service.AccountTypeOAuth, "plus")
	account.Extra["openai_oauth_responses_websockets_v2_enabled"] = true
	account.Extra["openai_oauth_responses_websockets_v2_mode"] = service.OpenAIWSIngressModeCtxPool
	repo := &openAIWSFrameAdmissionAccountRepo{account: cloneOpenAIWSFrameAdmissionAccount(account)}
	handler, _ := newOpenAIWSSelectedAccountAuditHandlerWithRepo(t, engine, repo)
	server, done := newOpenAIWSSelectedProAuditServer(t, handler)
	client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"input":"ws-pooled-first-turn-staged-drift"}`)
	select {
	case <-connectEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for pooled upstream acquire")
	}
	repo.setAccessToken("openai-ws-rotated-during-pooled-acquire")
	allowConnectOnce.Do(func() { close(allowConnect) })

	requireOpenAIWSAuditClose(t, client, coderws.StatusTryAgainLater)
	requireOpenAIWSAuditHandlerDone(t, done)

	require.Empty(t, scannerCalls(), "verified non-Pro first turn must not enter the Pro scanner")
	connections, writes, payloads, upstreamErrs := upstream.snapshot()
	require.Equal(t, int32(1), connections, "the pooled handshake may complete before the final dispatch check")
	require.Zero(t, writes, "credential drift during pooled acquire must stop the first business frame")
	require.Empty(t, payloads)
	require.Empty(t, upstreamErrs)
}

func TestOpenAIResponsesWebSocket_TokenDriftRejectsKnownControlFrameBeforeUpstreamWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	account := newOpenAIWSNonTurnAdmissionAccount(service.AccountTypeOAuth, "plus")
	client, done, upstream, scannerCalls, repo := startOpenAIWSNonTurnAdmissionSession(t, account)
	repo.setAccessToken("openai-ws-rotated-before-control")

	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.cancel","response_id":"resp_ws_non_turn_first"}`)
	requireOpenAIWSAuditClose(t, client, coderws.StatusTryAgainLater)
	requireOpenAIWSAuditHandlerDone(t, done)

	require.Empty(t, scannerCalls())
	_, writes, payloads, upstreamErrs := upstream.snapshot()
	require.Equal(t, int32(1), writes, "credential drift must stop the control frame before its upstream write")
	require.Len(t, payloads, 1)
	require.NotContains(t, string(payloads[0]), "response.cancel")
	require.Empty(t, upstreamErrs)
}

func TestOpenAIResponsesWebSocket_PlusToProDriftRejectsSecondTurnBeforeAuditAndUpstreamWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	account := newOpenAIWSNonTurnAdmissionAccount(service.AccountTypeOAuth, "plus")
	client, done, upstream, scannerCalls, repo := startOpenAIWSNonTurnAdmissionSession(t, account)
	repo.setPlan("pro")

	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"previous_response_id":"resp_ws_non_turn_first","input":"ws-plus-to-pro-second-turn-canary"}`)
	requireOpenAIWSAuditClose(t, client, coderws.StatusTryAgainLater)
	requireOpenAIWSAuditHandlerDone(t, done)

	require.Empty(t, scannerCalls(), "credential drift must be rejected before a newly-Pro turn can enter the scanner")
	connections, writes, payloads, upstreamErrs := upstream.snapshot()
	require.Equal(t, int32(1), connections)
	require.Equal(t, int32(1), writes, "credential drift must stop the second turn before upstream write")
	require.Len(t, payloads, 1)
	require.NotContains(t, string(payloads[0]), "ws-plus-to-pro-second-turn-canary")
	require.Empty(t, upstreamErrs)
}

func TestOpenAIResponsesWebSocket_PlusToProDriftRejectsTransportRetryBeforeSecondWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := newOpenAIWSAuditUpstream(t, nil)
	installOpenAIWSAuditUpstreamRedirect(t, upstream.server.URL)
	engine, scannerCalls := newOpenAIWSAuditPromptEngine(t, func(int, string) (int, string) {
		return http.StatusOK, openAIWSAuditSafeOutput
	})
	account := newOpenAIWSNonTurnAdmissionAccount(service.AccountTypeOAuth, "plus")
	account.Extra["openai_oauth_responses_websockets_v2_enabled"] = true
	account.Extra["openai_oauth_responses_websockets_v2_mode"] = service.OpenAIWSIngressModeCtxPool
	repo := &openAIWSFrameAdmissionAccountRepo{account: cloneOpenAIWSFrameAdmissionAccount(account)}
	upstream.onWrite = func(_ int, writeNo int, conn *coderws.Conn, _ []byte) bool {
		if writeNo == 1 {
			repo.setPlan("pro")
			_ = conn.CloseNow()
			return false
		}
		return upstream.writeCompleted(conn, "resp_ws_retry_drift_unexpected")
	}
	handler, _ := newOpenAIWSSelectedAccountAuditHandlerWithRepo(t, engine, repo)
	server, done := newOpenAIWSSelectedProAuditServer(t, handler)
	client := dialOpenAIWSSelectedProAuditClient(t, server.URL)

	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"input":"ws-plus-to-pro-retry-canary"}`)
	requireOpenAIWSAuditClose(t, client, coderws.StatusTryAgainLater)
	requireOpenAIWSAuditHandlerDone(t, done)

	require.Empty(t, scannerCalls(), "same-turn retry must not rescan or admit the refreshed Pro credential")
	_, writes, payloads, upstreamErrs := upstream.snapshot()
	require.Equal(t, int32(1), writes, "credential proof must be rechecked before retrying the upstream write")
	require.Len(t, payloads, 1)
	require.Contains(t, string(payloads[0]), "ws-plus-to-pro-retry-canary")
	require.Empty(t, upstreamErrs)
}

func TestOpenAIResponsesWebSocket_SelectedProAllowsKnownControlFrame(t *testing.T) {
	gin.SetMode(gin.TestMode)
	account := newOpenAIWSNonTurnAdmissionAccount(service.AccountTypeOAuth, "pro")
	client, done, upstream, scannerCalls, _ := startOpenAIWSNonTurnAdmissionSession(t, account)
	payload := `{"type":"response.cancel","response_id":"resp_ws_non_turn_first"}`

	writeOpenAIWSAuditRequest(t, client, payload)
	requireOpenAIWSAuditUpstreamWrites(t, upstream, 2)
	writeOpenAIWSAuditRequest(t, client,
		`{"type":"response.create","model":"gpt-5.1","stream":false,"previous_response_id":"resp_ws_non_turn_first","input":"ws-after-control-canary"}`)
	requireOpenAIWSAuditUpstreamWrites(t, upstream, 3)
	require.NoError(t, client.Close(coderws.StatusNormalClosure, "done"))
	requireOpenAIWSAuditHandlerDone(t, done)

	calls := scannerCalls()
	require.Len(t, calls, 2, "known control frame must not enter the blocking scanner or consume a turn admission")
	require.Contains(t, calls[1], "ws-after-control-canary")
	_, writes, payloads, upstreamErrs := upstream.snapshot()
	require.Equal(t, int32(3), writes)
	require.Len(t, payloads, 3)
	require.Equal(t, payload, string(payloads[1]))
	require.Contains(t, string(payloads[2]), "ws-after-control-canary")
	require.Empty(t, upstreamErrs)
}

func TestOpenAIResponsesWebSocket_SelectedProRejectsOversizeSecondTurnWithLateImageIntentBeforeAuditAndUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	account := newOpenAIWSNonTurnAdmissionAccount(service.AccountTypeOAuth, "pro")
	client, done, upstream, scannerCalls, _ := startOpenAIWSNonTurnAdmissionSession(t, account)

	prefix := `{"type":"response.create","model":"gpt-5.1","stream":false,"previous_response_id":"resp_ws_non_turn_first","input":"`
	lateImage := `","tools":[{"type":"image_generation"}]}`
	payload := prefix + strings.Repeat("x", securityadmission.CurrentLimits().BodyCapBytes) + lateImage
	require.Greater(t, len(payload), securityadmission.CurrentLimits().BodyCapBytes)
	require.Greater(t, strings.Index(payload, "image_generation"), securityadmission.RoutingEnvelopeWindowBytes)

	writeOpenAIWSAuditRequest(t, client, payload)
	requireOpenAIWSAuditClose(t, client, coderws.StatusTryAgainLater)
	requireOpenAIWSAuditHandlerDone(t, done)

	calls := scannerCalls()
	require.Len(t, calls, 1, "oversize second turn must close before another blocking scan")
	require.Contains(t, calls[0], "ws-non-turn-first-canary")
	_, writes, payloads, upstreamErrs := upstream.snapshot()
	require.Equal(t, int32(1), writes, "oversize second turn must close before upstream dispatch")
	require.Len(t, payloads, 1)
	require.Empty(t, upstreamErrs)
}
