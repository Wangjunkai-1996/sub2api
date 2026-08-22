//go:build unit

package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type selectedAccountAuditResponsesUpstream struct {
	service.HTTPUpstream
	mu         sync.Mutex
	accountIDs []int64
	failFirst  bool
}

func (u *selectedAccountAuditResponsesUpstream) Do(_ *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	u.accountIDs = append(u.accountIDs, accountID)
	call := len(u.accountIDs)
	u.mu.Unlock()

	if u.failFirst && call == 1 {
		return &http.Response{
			StatusCode: 520,
			Header:     http.Header{"Content-Type": []string{"text/html"}},
			Body:       io.NopCloser(bytes.NewBufferString("<html>520: unknown error</html>")),
		}, nil
	}

	body := strings.Join([]string{
		`data: {"type":"response.output_text.delta","delta":"ok"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_selected_audit_ok","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}`,
		"",
	}, "\n")
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func (u *selectedAccountAuditResponsesUpstream) calls() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int64(nil), u.accountIDs...)
}

func selectedAccountAuditProAccounts() []service.Account {
	return []service.Account{
		{
			ID: 1, Name: "selected-audit-pro-1", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 0,
			Credentials: map[string]any{"access_token": "token-1", "plan_type": "pro"},
		},
		{
			ID: 2, Name: "selected-audit-pro-2", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 1,
			Credentials: map[string]any{"access_token": "token-2", "plan_type": "pro"},
		},
	}
}

func selectedAccountAuditFallbackAccounts(accountType string, credentials map[string]any) []service.Account {
	return []service.Account{
		{
			ID: 1, Name: "selected-audit-pro", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 0,
			Credentials: map[string]any{"access_token": "pro-token", "plan_type": "pro"},
		},
		{
			ID: 2, Name: "selected-audit-unknown", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 1,
			Credentials: map[string]any{"access_token": "unknown-token", "plan_type": "future-plan"},
		},
		{
			ID: 3, Name: "selected-audit-second-pro", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 2,
			Credentials: map[string]any{"access_token": "second-pro-token", "plan_type": "pro"},
		},
		{
			ID: 4, Name: "selected-audit-verified", Platform: service.PlatformOpenAI,
			Type: accountType, Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 3, Credentials: credentials,
		},
	}
}

type selectedAccountAuditPromptEngine struct {
	cfg       securityaudit.ActiveConfig
	evaluator *securityaudit.GuardEvaluator
}

func (*selectedAccountAuditPromptEngine) EffectiveMode() securityaudit.Mode {
	return securityaudit.ModeBlocking
}
func (*selectedAccountAuditPromptEngine) Enqueue(context.Context, securityaudit.Request) error {
	return nil
}
func (e *selectedAccountAuditPromptEngine) Evaluate(ctx context.Context, req securityaudit.Request) (*securityaudit.PromptDecision, error) {
	snapshot, err := securityaudit.ExtractBlockingPromptSnapshot(req, e.cfg.BlockingLatestTurnOnly)
	if err != nil {
		return nil, err
	}
	return e.evaluator.Evaluate(ctx, e.cfg, snapshot)
}

func newSelectedAccountAuditPromptEngine(
	t *testing.T,
	guardStatus int,
	guardOutput string,
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
			t.Errorf("decode scanner request: %v", err)
			http.Error(w, "invalid scanner request", http.StatusBadRequest)
			return
		}
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected scanner path: %s", r.URL.Path)
		}
		if len(request.Messages) != 1 {
			t.Errorf("scanner received %d messages, want 1", len(request.Messages))
		} else {
			mu.Lock()
			scannerText = append(scannerText, request.Messages[0].Content)
			mu.Unlock()
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(guardStatus)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"choices": []map[string]any{{"message": map[string]string{"content": guardOutput}}},
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
			ID: "selected-account-audit-test", BaseURL: server.URL, Model: securityaudit.DefaultGuardModel,
			TimeoutMS: 1000, InputLimit: securityaudit.MaxInputLimit, Enabled: true,
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

func newSelectedAccountAuditHandler(
	t *testing.T,
	upstream service.HTTPUpstream,
	engine securityaudit.PromptEngine,
	accounts []service.Account,
) (*OpenAIGatewayHandler, *concurrencyCacheMock) {
	return newSelectedAccountAuditHandlerWithDependencies(
		t,
		upstream,
		engine,
		openAIImagesFailoverAccountRepo{accounts: accounts},
		nil,
	)
}

func newSelectedAccountAuditHandlerWithDependencies(
	t *testing.T,
	upstream service.HTTPUpstream,
	engine securityaudit.PromptEngine,
	accountRepo service.AccountRepository,
	tokenProvider *service.OpenAITokenProvider,
	channelServices ...*service.ChannelService,
) (*OpenAIGatewayHandler, *concurrencyCacheMock) {
	t.Helper()
	var channelService *service.ChannelService
	if len(channelServices) > 0 {
		channelService = channelServices[0]
	}
	cache := &concurrencyCacheMock{
		acquireUserSlotFn: func(context.Context, int64, int, string) (bool, error) {
			return true, nil
		},
		acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) {
			return true, nil
		},
	}
	concurrencyService := service.NewConcurrencyService(cache)
	cfg := &config.Config{RunMode: config.RunModeSimple}
	gatewayService := service.NewOpenAIGatewayService(
		accountRepo,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		cfg,
		nil,
		concurrencyService,
		nil,
		nil,
		nil,
		upstream,
		nil,
		tokenProvider,
		nil,
		nil,
		channelService,
		nil,
		nil,
		nil,
	)
	billingService := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billingService.Stop)
	handler := NewOpenAIGatewayHandler(
		gatewayService,
		concurrencyService,
		billingService,
		service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg),
		nil,
		nil,
		nil,
		nil,
		cfg,
	)
	handler.maxAccountSwitches = 10
	handler.securityAuditCoordinator = securityaudit.NewCoordinator(nil, engine)
	return handler, cache
}

func selectedAccountAuditWithModelMapping(accounts []service.Account, source, target string) []service.Account {
	mapped := make([]service.Account, len(accounts))
	for i := range accounts {
		mapped[i] = cloneSelectedAccountAuditAccount(accounts[i])
		if mapped[i].Credentials == nil {
			mapped[i].Credentials = make(map[string]any)
		}
		mapped[i].Credentials["model_mapping"] = map[string]any{source: target}
	}
	return mapped
}

func selectedAccountAuditChannelService(groupID int64, source, target string) *service.ChannelService {
	return service.NewChannelService(&openAIWSUsageHandlerChannelRepoStub{
		channels: []service.Channel{{
			ID:       8801,
			Name:     "selected-account-audit-channel",
			Status:   service.StatusActive,
			GroupIDs: []int64{groupID},
			ModelMapping: map[string]map[string]string{
				service.PlatformOpenAI: {source: target},
			},
		}},
		groupPlatforms: map[int64]string{groupID: service.PlatformOpenAI},
	}, nil, nil, nil)
}

type selectedAccountAuditMutableRepo struct {
	service.AccountRepository
	mu       sync.Mutex
	accounts []service.Account
}

func newSelectedAccountAuditMutableRepo(accounts []service.Account) *selectedAccountAuditMutableRepo {
	repo := &selectedAccountAuditMutableRepo{accounts: make([]service.Account, len(accounts))}
	for i := range accounts {
		repo.accounts[i] = cloneSelectedAccountAuditAccount(accounts[i])
	}
	return repo
}

func cloneSelectedAccountAuditAccount(account service.Account) service.Account {
	if account.Credentials != nil {
		credentials := make(map[string]any, len(account.Credentials))
		for key, value := range account.Credentials {
			credentials[key] = value
		}
		account.Credentials = credentials
	}
	return account
}

func (r *selectedAccountAuditMutableRepo) GetByID(_ context.Context, id int64) (*service.Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.accounts {
		if r.accounts[i].ID == id {
			account := cloneSelectedAccountAuditAccount(r.accounts[i])
			return &account, nil
		}
	}
	return nil, service.ErrNoAvailableAccounts
}

func (r *selectedAccountAuditMutableRepo) Update(_ context.Context, account *service.Account) error {
	if account == nil {
		return service.ErrNoAvailableAccounts
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.accounts {
		if r.accounts[i].ID == account.ID {
			r.accounts[i] = cloneSelectedAccountAuditAccount(*account)
			return nil
		}
	}
	return service.ErrNoAvailableAccounts
}

func (r *selectedAccountAuditMutableRepo) ListSchedulableByGroupIDAndPlatform(_ context.Context, _ int64, platform string) ([]service.Account, error) {
	return r.accountsForPlatform(platform), nil
}

func (r *selectedAccountAuditMutableRepo) ListSchedulableByPlatform(_ context.Context, platform string) ([]service.Account, error) {
	return r.accountsForPlatform(platform), nil
}

func (r *selectedAccountAuditMutableRepo) ListSchedulableUngroupedByPlatform(_ context.Context, platform string) ([]service.Account, error) {
	return r.accountsForPlatform(platform), nil
}

func (r *selectedAccountAuditMutableRepo) accountsForPlatform(platform string) []service.Account {
	r.mu.Lock()
	defer r.mu.Unlock()
	accounts := make([]service.Account, 0, len(r.accounts))
	for i := range r.accounts {
		if r.accounts[i].Platform == platform {
			accounts = append(accounts, cloneSelectedAccountAuditAccount(r.accounts[i]))
		}
	}
	return accounts
}

type selectedAccountAuditPlusToProRefresher struct {
	refreshes atomic.Int32
}

func (*selectedAccountAuditPlusToProRefresher) CacheKey(account *service.Account) string {
	if account == nil {
		return "selected-account-audit:nil"
	}
	return "selected-account-audit:" + account.Name
}

func (*selectedAccountAuditPlusToProRefresher) CanRefresh(account *service.Account) bool {
	return account != nil && account.ID == 1
}

func (*selectedAccountAuditPlusToProRefresher) NeedsRefresh(account *service.Account, _ time.Duration) bool {
	return account != nil && account.ID == 1 && account.GetCredential("plan_type") == "plus"
}

func (r *selectedAccountAuditPlusToProRefresher) Refresh(_ context.Context, _ *service.Account) (map[string]any, error) {
	r.refreshes.Add(1)
	return map[string]any{
		"access_token":  "rotated-pro-token",
		"refresh_token": "rotated-refresh-token",
		"expires_at":    time.Now().Add(time.Hour).UTC().Format(time.RFC3339),
		"plan_type":     "pro",
	}, nil
}

func selectedAccountAuditHTTPContext(t *testing.T, path string, body string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	c, recorder := newOpenAIResponsesFailoverTestContext(t, nil)
	c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	require.True(t, ok)
	apiKey.Group.AllowMessagesDispatch = true
	c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100, Concurrency: 1})
	return c, recorder
}

func TestOpenAIGatewayHandler_SelectedProAuditBlockReleasesConcurrencyBeforeUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name   string
		path   string
		body   string
		invoke func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{
			name: "Responses",
			path: "/v1/responses",
			body: `{"model":"gpt-5.1","stream":false,"instructions":"responses-instructions-canary","input":[{"type":"additional_tools","role":"developer","tools":[{"type":"custom","name":"exec","description":"responses-tool-schema-canary"}]},{"type":"function_call","call_id":"call-1","name":"lookup","arguments":"responses-arguments-canary"},{"type":"function_call_output","call_id":"call-1","output":"responses-output-canary"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.Responses(c)
			},
		},
		{
			name: "Messages",
			path: "/v1/messages",
			body: `{"model":"gpt-5.1","stream":false,"max_tokens":16,"system":"messages-system-canary","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"tool-1","name":"lookup","input":{"query":"messages-input-canary"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":"messages-result-canary"}]}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.Messages(c)
			},
		},
		{
			name: "Chat Completions",
			path: "/v1/chat/completions",
			body: `{"model":"gpt-5.1","stream":false,"messages":[{"role":"user","content":"old-user-canary"},{"role":"assistant","tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"chat-arguments-canary"}}]},{"role":"tool","tool_call_id":"call-1","content":"chat-tool-canary"},{"role":"function","name":"lookup","content":"chat-function-canary"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.ChatCompletions(c)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &selectedAccountAuditResponsesUpstream{}
			engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusOK, "Safety: Unsafe\nCategories: Jailbreak")
			accounts := append(selectedAccountAuditProAccounts(), service.Account{
				ID: 3, Name: "selected-audit-plus", Platform: service.PlatformOpenAI,
				Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
				Concurrency: 1, Priority: 2,
				Credentials: map[string]any{"access_token": "token-3", "plan_type": "plus"},
			})
			handler, cache := newSelectedAccountAuditHandler(t, upstream, engine, accounts)
			c, recorder := selectedAccountAuditHTTPContext(t, tt.path, tt.body)

			tt.invoke(handler, c)

			require.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())
			require.Empty(t, upstream.calls(), "a blocked selected-account audit must stop before upstream forwarding")
			require.Len(t, scannerCalls(), 1, "the selected Pro request must reach the blocking scanner exactly once")
			for _, canary := range []string{"arguments-canary", "output-canary", "result-canary", "input-canary", "tool-canary", "function-canary", "instructions-canary", "system-canary", "tool-schema-canary"} {
				if strings.Contains(tt.body, canary) {
					require.Contains(t, scannerCalls()[0], canary)
				}
			}
			require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseAccountCalled))
			require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
		})
	}
}

func TestOpenAIGatewayHandlerResponses_FormerAuditSeparatorStillReachesBlockingScanner(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const separator = "\x00SUB2API_PROMPT_AUDIT_PRIORITY_END\x00"

	for _, input := range []string{separator, "before" + separator + "after"} {
		t.Run(strings.Trim(input, "\x00"), func(t *testing.T) {
			body, err := json.Marshal(map[string]any{
				"model": "gpt-5.1", "stream": false, "input": input,
			})
			require.NoError(t, err)
			upstream := &selectedAccountAuditResponsesUpstream{}
			engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusOK, "Safety: Unsafe\nCategories: Jailbreak")
			handler, _ := newSelectedAccountAuditHandler(t, upstream, engine, selectedAccountAuditProAccounts())
			c, recorder := selectedAccountAuditHTTPContext(t, "/v1/responses", string(body))

			handler.Responses(c)

			require.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())
			require.Empty(t, upstream.calls(), "scanner block must stop before upstream dispatch")
			require.Equal(t, []string{input}, scannerCalls(), "client text must reach the scanner exactly once and unchanged")
			state := openAISecurityAdmissionFromContext(c)
			require.NotNil(t, state)
			require.Equal(t, securityadmission.RequestAuditableText, state.admission.Class())
		})
	}
}

func TestOpenAIGatewayHandlerResponses_SelectedProAuditUnavailableFallsBackOnlyToVerifiedNonPro(t *testing.T) {
	verifiedAccounts := []struct {
		name        string
		accountType string
		credentials map[string]any
	}{
		{name: "OAuth Plus", accountType: service.AccountTypeOAuth, credentials: map[string]any{"access_token": "verified-token", "plan_type": "plus"}},
		{name: "OAuth Team", accountType: service.AccountTypeOAuth, credentials: map[string]any{"access_token": "verified-token", "plan_type": "team"}},
		{name: "API key", accountType: service.AccountTypeAPIKey, credentials: map[string]any{"api_key": "verified-key"}},
	}

	for _, tt := range verifiedAccounts {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &selectedAccountAuditResponsesUpstream{}
			engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusServiceUnavailable, "")
			accounts := selectedAccountAuditFallbackAccounts(tt.accountType, tt.credentials)
			handler, cache := newSelectedAccountAuditHandler(t, upstream, engine, accounts)
			c, recorder := selectedAccountAuditHTTPContext(t, "/v1/responses", `{"model":"gpt-5.1","stream":true,"input":"unavailable-fallback-canary"}`)

			handler.Responses(c)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Equal(t, []int64{4}, upstream.calls(), "unknown and Pro accounts must not satisfy the audit-exempt fallback")
			require.Len(t, scannerCalls(), 1, "audit fallback is attempted at most once")
			require.Contains(t, scannerCalls()[0], "unavailable-fallback-canary")
			require.Equal(t, int32(2), atomic.LoadInt32(&cache.releaseAccountCalled))
			require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
		})
	}
}

func TestOpenAIGatewayHandler_SelectedProAuditUnavailableFallbackCoversChatAndMessages(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		body   string
		canary string
		invoke func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{
			name: "Chat Completions", path: "/v1/chat/completions", canary: "chat-fallback-canary",
			body: `{"model":"gpt-5.1","stream":true,"messages":[{"role":"user","content":"chat-fallback-canary"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.ChatCompletions(c)
			},
		},
		{
			name: "Messages", path: "/v1/messages", canary: "messages-fallback-canary",
			body: `{"model":"gpt-5.1","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"messages-fallback-canary"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.Messages(c)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &selectedAccountAuditResponsesUpstream{}
			engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusServiceUnavailable, "")
			accounts := selectedAccountAuditFallbackAccounts(service.AccountTypeOAuth, map[string]any{
				"access_token": "verified-token", "plan_type": "plus",
			})
			handler, cache := newSelectedAccountAuditHandler(t, upstream, engine, accounts)
			c, recorder := selectedAccountAuditHTTPContext(t, tt.path, tt.body)

			tt.invoke(handler, c)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Equal(t, []int64{4}, upstream.calls(), "unknown and Pro accounts must not satisfy the audit-exempt fallback")
			require.Len(t, scannerCalls(), 1, "audit fallback is attempted at most once")
			require.Contains(t, scannerCalls()[0], tt.canary)
			require.Equal(t, int32(2), atomic.LoadInt32(&cache.releaseAccountCalled))
			require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
		})
	}
}

func TestOpenAIGatewayHandler_SelectedProAuditUnavailableWithoutVerifiedFallbackReturns503(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		body     string
		protocol string
		invoke   func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{
			name: "Responses", path: "/v1/responses", protocol: "responses",
			body: `{"model":"gpt-5.1","stream":true,"input":"responses-no-fallback-canary"}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.Responses(c)
			},
		},
		{
			name: "Chat Completions", path: "/v1/chat/completions", protocol: "chat",
			body: `{"model":"gpt-5.1","stream":true,"messages":[{"role":"user","content":"chat-no-fallback-canary"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.ChatCompletions(c)
			},
		},
		{
			name: "Messages", path: "/v1/messages", protocol: "messages",
			body: `{"model":"gpt-5.1","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"messages-no-fallback-canary"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.Messages(c)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &selectedAccountAuditResponsesUpstream{}
			engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusServiceUnavailable, "")
			accounts := append(selectedAccountAuditProAccounts(), service.Account{
				ID: 3, Name: "selected-audit-unknown", Platform: service.PlatformOpenAI,
				Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
				Concurrency: 1, Priority: 2,
				Credentials: map[string]any{"access_token": "unknown-token", "plan_type": "future-plan"},
			})
			handler, cache := newSelectedAccountAuditHandler(t, upstream, engine, accounts)
			c, recorder := selectedAccountAuditHTTPContext(t, tt.path, tt.body)

			tt.invoke(handler, c)

			require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
			require.Empty(t, upstream.calls())
			require.Len(t, scannerCalls(), 1, "audit fallback is attempted at most once")
			require.Contains(t, scannerCalls()[0], tt.protocol+"-no-fallback-canary")
			require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseAccountCalled))
			require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
		})
	}
}

func TestOpenAIGatewayHandlerResponses_SelectedProAuditAllowIsReusedAcrossFailover(t *testing.T) {
	upstream := &selectedAccountAuditResponsesUpstream{failFirst: true}
	engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusOK, "Safety: Safe\nCategories: None")
	handler, cache := newSelectedAccountAuditHandler(t, upstream, engine, selectedAccountAuditProAccounts())
	c, recorder := selectedAccountAuditHTTPContext(t, "/v1/responses", `{"model":"gpt-5.1","stream":true,"input":"failover-scan-canary"}`)

	handler.Responses(c)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, []int64{1, 2}, upstream.calls())
	require.Len(t, scannerCalls(), 1, "an allowed HTTP audit must be reused after account failover")
	require.Contains(t, scannerCalls()[0], "failover-scan-canary")
	require.Equal(t, int32(2), atomic.LoadInt32(&cache.releaseAccountCalled))
	require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
}

func TestOpenAIGatewayHandler_SelectedProAuditAllowReuseCoversChatAndMessagesFailover(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		body   string
		canary string
		invoke func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{
			name: "Chat Completions", path: "/v1/chat/completions", canary: "chat-failover-scan-canary",
			body: `{"model":"gpt-5.1","stream":true,"messages":[{"role":"user","content":"chat-failover-scan-canary"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.ChatCompletions(c)
			},
		},
		{
			name: "Messages", path: "/v1/messages", canary: "messages-failover-scan-canary",
			body: `{"model":"gpt-5.1","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"messages-failover-scan-canary"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.Messages(c)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &selectedAccountAuditResponsesUpstream{failFirst: true}
			engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusOK, "Safety: Safe\nCategories: None")
			handler, cache := newSelectedAccountAuditHandler(t, upstream, engine, selectedAccountAuditProAccounts())
			c, recorder := selectedAccountAuditHTTPContext(t, tt.path, tt.body)

			tt.invoke(handler, c)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Equal(t, []int64{1, 2}, upstream.calls())
			require.Len(t, scannerCalls(), 1, "an allowed HTTP audit must be reused after account failover")
			require.Contains(t, scannerCalls()[0], tt.canary)
			require.Equal(t, int32(2), atomic.LoadInt32(&cache.releaseAccountCalled))
			require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
		})
	}
}

func TestOpenAIGatewayHandler_UninspectablePlusRefreshesToProBeforeDispatchAndReselectsVerifiedNonPro(t *testing.T) {
	accounts := []service.Account{
		{
			ID: 1, Name: "refreshing-plus", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 0,
			Credentials: map[string]any{
				"access_token":  "stale-plus-token",
				"refresh_token": "stale-refresh-token",
				"expires_at":    time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
				"plan_type":     "plus",
			},
		},
		{
			ID: 2, Name: "verified-api-key", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 1,
			Credentials: map[string]any{"api_key": "verified-fallback-key"},
		},
	}
	repo := newSelectedAccountAuditMutableRepo(accounts)
	refresher := &selectedAccountAuditPlusToProRefresher{}
	tokenProvider := service.NewOpenAITokenProvider(repo, nil, nil)
	tokenProvider.SetRefreshAPI(service.NewOAuthRefreshAPI(repo, nil), refresher)
	upstream := &selectedAccountAuditResponsesUpstream{}
	engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusOK, "Safety: Safe\nCategories: None")
	handler, cache := newSelectedAccountAuditHandlerWithDependencies(
		t,
		upstream,
		engine,
		repo,
		tokenProvider,
	)
	// The unknown item type is deliberately uninspectable, so every retry must
	// retain RequireAuditExemptAccount even after the first account refreshes.
	c, recorder := selectedAccountAuditHTTPContext(t, "/v1/responses",
		`{"model":"gpt-5.1","stream":true,"input":[{"type":"future_item","text":"refresh-drift-canary"}]}`)

	handler.Responses(c)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, int32(1), refresher.refreshes.Load(), "the selected Plus credential must refresh exactly once")
	refreshed, err := repo.GetByID(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, "pro", refreshed.GetCredential("plan_type"))
	require.Equal(t, []int64{2}, upstream.calls(), "the refreshed Pro token must never reach an upstream dispatch")
	require.Empty(t, scannerCalls(), "uninspectable traffic cannot be converted into a Pro scan-and-forward path")
	require.Equal(t, int32(2), atomic.LoadInt32(&cache.releaseAccountCalled), "both the rejected and fallback account slots must be released")
	require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
}

func TestOpenAIGatewayHandler_RemoteContentRoutesOnlyToVerifiedNonProWithoutScanning(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		body   string
		invoke func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{
			name: "Responses hosted file search", path: "/v1/responses",
			body:   `{"model":"gpt-5.1","stream":true,"input":"summarize the attached knowledge","tools":[{"type":"file_search","vector_store_ids":["vs_123"]}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) { h.Responses(c) },
		},
		{
			name: "Responses Lite hosted search", path: "/v1/responses",
			body:   `{"model":"gpt-5.1","stream":true,"input":[{"type":"additional_tools","role":"developer","tools":[{"type":"web_search","name":"search"}]},{"type":"message","role":"user","content":"research this"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) { h.Responses(c) },
		},
		{
			name: "Chat web search options", path: "/v1/chat/completions",
			body:   `{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"research this"}],"web_search_options":{"search_context_size":"low"}}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) { h.ChatCompletions(c) },
		},
		{
			name: "Chat hosted search model", path: "/v1/chat/completions",
			body:   `{"model":"gpt-5-search-api","stream":true,"messages":[{"role":"user","content":"research this"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) { h.ChatCompletions(c) },
		},
		{
			name: "Messages emulated name-only search", path: "/v1/messages",
			body:   `{"model":"claude-sonnet-4","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"research this"}],"tools":[{"name":"web_search","input_schema":{"type":"object"}}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) { h.Messages(c) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &selectedAccountAuditResponsesUpstream{}
			engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusOK, "Safety: Safe\nCategories: None")
			handler, _ := newSelectedAccountAuditHandler(t, upstream, engine,
				selectedAccountAuditFallbackAccounts(service.AccountTypeOAuth, map[string]any{
					"access_token": "verified-team-token", "plan_type": "team",
				}))
			c, recorder := selectedAccountAuditHTTPContext(t, tt.path, tt.body)

			tt.invoke(handler, c)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Equal(t, []int64{4}, upstream.calls(), "remote content must exclude Pro and unknown accounts")
			require.Empty(t, scannerCalls(), "uninspectable remote content cannot be converted into a Pro scan proof")
			state := openAISecurityAdmissionFromContext(c)
			require.NotNil(t, state)
			require.Equal(t, securityadmission.RequestUninspectable, state.admission.Class())
			require.Equal(t, securityadmission.ReasonRemoteContent, state.admission.Reason())
			require.Equal(t, securityadmission.AccountRequirementAuditExempt, state.admission.Requirement())
		})
	}
}

func TestOpenAIGatewayHandler_EffectiveSearchModelMappingsRequireVerifiedNonPro(t *testing.T) {
	const (
		groupID     = int64(3131)
		clientModel = "public-alias"
		searchModel = "gpt-5-search-api"
	)
	endpoints := []struct {
		name   string
		path   string
		body   string
		invoke func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{
			name: "Chat Completions", path: "/v1/chat/completions",
			body:   `{"model":"` + clientModel + `","stream":true,"messages":[{"role":"user","content":"mapped-search-canary"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) { h.ChatCompletions(c) },
		},
		{
			name: "Responses", path: "/v1/responses",
			body:   `{"model":"` + clientModel + `","stream":true,"input":"mapped-search-canary"}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) { h.Responses(c) },
		},
		{
			name: "Messages", path: "/v1/messages",
			body:   `{"model":"` + clientModel + `","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"mapped-search-canary"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) { h.Messages(c) },
		},
	}
	verifiedAccounts := []struct {
		name        string
		accountType string
		credentials map[string]any
	}{
		{name: "Plus", accountType: service.AccountTypeOAuth, credentials: map[string]any{"access_token": "plus-token", "plan_type": "plus"}},
		{name: "Team", accountType: service.AccountTypeOAuth, credentials: map[string]any{"access_token": "team-token", "plan_type": "team"}},
		{name: "APIKey", accountType: service.AccountTypeAPIKey, credentials: map[string]any{"api_key": "verified-api-key"}},
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

	for _, endpoint := range endpoints {
		for _, mappingKind := range mappingKinds {
			for _, verified := range verifiedAccounts {
				t.Run(endpoint.name+"/"+mappingKind.name+"/"+verified.name, func(t *testing.T) {
					accounts := selectedAccountAuditFallbackAccounts(verified.accountType, verified.credentials)
					accounts, channelService := mappingKind.prepare(accounts)
					upstream := &selectedAccountAuditResponsesUpstream{}
					engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusOK, "Safety: Safe\nCategories: None")
					handler, _ := newSelectedAccountAuditHandlerWithDependencies(
						t,
						upstream,
						engine,
						openAIImagesFailoverAccountRepo{accounts: accounts},
						nil,
						channelService,
					)
					c, recorder := selectedAccountAuditHTTPContext(t, endpoint.path, endpoint.body)

					endpoint.invoke(handler, c)

					require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
					require.Equal(t, []int64{4}, upstream.calls(), "Pro and unknown accounts must not receive an effective search-model dispatch")
					require.Empty(t, scannerCalls(), "remote search cannot be converted into a Pro scanner proof")
					require.Equal(t, securityadmission.AccountRequirementAuditExempt,
						service.OpenAIAccountRequirementFromContext(c.Request.Context()))
				})
			}
		}
	}
}

func TestOpenAIGatewayHandler_EffectiveSearchModelMappingsWithoutVerifiedNonProReturn503(t *testing.T) {
	const (
		groupID     = int64(3131)
		clientModel = "public-alias"
		searchModel = "gpt-5-search-api"
	)
	endpoints := []struct {
		name   string
		path   string
		body   string
		invoke func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{
			name: "Chat Completions", path: "/v1/chat/completions",
			body:   `{"model":"` + clientModel + `","stream":true,"messages":[{"role":"user","content":"mapped-search-no-fallback"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) { h.ChatCompletions(c) },
		},
		{
			name: "Responses", path: "/v1/responses",
			body:   `{"model":"` + clientModel + `","stream":true,"input":"mapped-search-no-fallback"}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) { h.Responses(c) },
		},
		{
			name: "Messages", path: "/v1/messages",
			body:   `{"model":"` + clientModel + `","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"mapped-search-no-fallback"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) { h.Messages(c) },
		},
	}
	unsafeAccounts := selectedAccountAuditFallbackAccounts(service.AccountTypeAPIKey, map[string]any{"api_key": "unused"})[:3]
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

	for _, endpoint := range endpoints {
		for _, test := range tests {
			t.Run(endpoint.name+"/"+test.name, func(t *testing.T) {
				upstream := &selectedAccountAuditResponsesUpstream{}
				engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusOK, "Safety: Safe\nCategories: None")
				handler, _ := newSelectedAccountAuditHandlerWithDependencies(
					t,
					upstream,
					engine,
					openAIImagesFailoverAccountRepo{accounts: test.accounts},
					nil,
					test.channelService,
				)
				c, recorder := selectedAccountAuditHTTPContext(t, endpoint.path, endpoint.body)

				endpoint.invoke(handler, c)

				require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
				require.Empty(t, upstream.calls())
				require.Empty(t, scannerCalls())
				require.Equal(t, securityadmission.AccountRequirementAuditExempt,
					service.OpenAIAccountRequirementFromContext(c.Request.Context()))
			})
		}
	}
}

func TestOpenAIGatewayHandler_OversizeChatAndMessagesRouteOnlyToVerifiedNonProWithoutScanning(t *testing.T) {
	oversize := strings.Repeat("x", securityadmission.CurrentLimits().BodyCapBytes+1)
	tests := []struct {
		name   string
		path   string
		body   string
		invoke func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{
			name: "Chat Completions", path: "/v1/chat/completions",
			body: `{"model":"gpt-5.1","stream":true,"messages":[{"role":"user","content":"` + oversize + `"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.ChatCompletions(c)
			},
		},
		{
			name: "Messages", path: "/v1/messages",
			body: `{"model":"gpt-5.1","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"` + oversize + `"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.Messages(c)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &selectedAccountAuditResponsesUpstream{}
			engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusOK, "Safety: Safe\nCategories: None")
			handler, _ := newSelectedAccountAuditHandler(t, upstream, engine,
				selectedAccountAuditFallbackAccounts(service.AccountTypeOAuth, map[string]any{
					"access_token": "verified-plus-token", "plan_type": "plus",
				}))
			c, recorder := selectedAccountAuditHTTPContext(t, tt.path, tt.body)

			tt.invoke(handler, c)

			require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
			require.Equal(t, []int64{4}, upstream.calls(), "oversize must exclude Pro and unknown accounts")
			require.Empty(t, scannerCalls(), "oversize classification must not materialize content or invoke the prompt scanner")
			state := openAISecurityAdmissionFromContext(c)
			require.NotNil(t, state)
			require.Equal(t, securityadmission.RequestUninspectable, state.admission.Class())
			require.Equal(t, securityadmission.ReasonLargeBody, state.admission.Reason())
			require.Equal(t, securityadmission.AccountRequirementAuditExempt, state.admission.Requirement())
		})
	}
}

func TestOpenAIGatewayHandler_ChatAndMessagesRejectInvalidCompleteRoutingEnvelope(t *testing.T) {
	oversize := strings.Repeat("x", securityadmission.CurrentLimits().BodyCapBytes+1)
	payloads := []struct {
		name    string
		content string
	}{
		{name: "normal", content: "hello"},
		{name: "oversized", content: oversize},
	}
	endpoints := []struct {
		name      string
		path      string
		prefix    string
		bodyClose string
		invoke    func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{
			name: "Chat Completions", path: "/v1/chat/completions",
			prefix:    `{"model":"gpt-5.1","stream":false,"messages":[{"role":"user","content":"`,
			bodyClose: `"}]`,
			invoke:    func(h *OpenAIGatewayHandler, c *gin.Context) { h.ChatCompletions(c) },
		},
		{
			name: "Messages", path: "/v1/messages",
			prefix:    `{"model":"gpt-5.1","stream":false,"max_tokens":16,"messages":[{"role":"user","content":"`,
			bodyClose: `"}]`,
			invoke:    func(h *OpenAIGatewayHandler, c *gin.Context) { h.Messages(c) },
		},
	}
	cases := []struct {
		name string
		tail string
	}{
		{name: "escaped duplicate model", tail: `,"mo\u0064el":"gpt-5.2"}`},
		{name: "duplicate stream", tail: `,"stream":true}`},
		{name: "case-folded model alias", tail: `,"Model":"gpt-5.2"}`},
		{name: "case-folded stream alias", tail: `,"Stream":true}`},
		{name: "malformed tail", tail: ""},
	}

	for _, endpoint := range endpoints {
		for _, payload := range payloads {
			for _, test := range cases {
				t.Run(endpoint.name+"/"+payload.name+"/"+test.name, func(t *testing.T) {
					upstream := &selectedAccountAuditResponsesUpstream{}
					engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusOK, "Safety: Safe\nCategories: None")
					handler, _ := newSelectedAccountAuditHandler(t, upstream, engine,
						selectedAccountAuditFallbackAccounts(service.AccountTypeOAuth, map[string]any{
							"access_token": "verified-plus-token", "plan_type": "plus",
						}))
					body := endpoint.prefix + payload.content + endpoint.bodyClose + test.tail
					c, recorder := selectedAccountAuditHTTPContext(t, endpoint.path, body)

					endpoint.invoke(handler, c)

					require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
					require.Contains(t, recorder.Body.String(), "invalid_request_error")
					require.Empty(t, upstream.calls(), "invalid routing metadata must be rejected before dispatch")
					require.Empty(t, scannerCalls(), "invalid routing metadata must not invoke the prompt scanner")
				})
			}
		}
	}
}

func TestOpenAIGatewayHandler_OversizeChatAndMessagesKeepUnavailableRoutingEnvelopeAs503(t *testing.T) {
	padding := strings.Repeat("x", securityadmission.CurrentLimits().BodyCapBytes+1)
	tests := []struct {
		name   string
		path   string
		body   string
		invoke func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{
			name: "Chat Completions", path: "/v1/chat/completions",
			body:   `{"padding":"` + padding + `","model":"gpt-5.1","stream":false,"messages":[{"role":"user","content":"hello"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) { h.ChatCompletions(c) },
		},
		{
			name: "Messages", path: "/v1/messages",
			body:   `{"padding":"` + padding + `","model":"gpt-5.1","stream":false,"max_tokens":16,"messages":[{"role":"user","content":"hello"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) { h.Messages(c) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upstream := &selectedAccountAuditResponsesUpstream{}
			engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusOK, "Safety: Safe\nCategories: None")
			handler, _ := newSelectedAccountAuditHandler(t, upstream, engine,
				selectedAccountAuditFallbackAccounts(service.AccountTypeOAuth, map[string]any{
					"access_token": "verified-plus-token", "plan_type": "plus",
				}))
			c, recorder := selectedAccountAuditHTTPContext(t, test.path, test.body)

			test.invoke(handler, c)

			require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), "routing metadata is unavailable")
			require.Empty(t, upstream.calls())
			require.Empty(t, scannerCalls())
		})
	}
}

func TestOpenAIGatewayHandler_OversizeResponsesRejectsOpaqueImageIntentBeforeDispatch(t *testing.T) {
	padding := strings.Repeat("x", securityadmission.CurrentLimits().BodyCapBytes+1)
	body := `{"model":"gpt-5.1","stream":true,"input":"` + padding +
		`","tools":[{"type":"image_generation"}]}`
	upstream := &selectedAccountAuditResponsesUpstream{}
	engine, scannerCalls := newSelectedAccountAuditPromptEngine(t, http.StatusOK, "Safety: Safe\nCategories: None")
	handler, _ := newSelectedAccountAuditHandler(t, upstream, engine,
		selectedAccountAuditFallbackAccounts(service.AccountTypeOAuth, map[string]any{
			"access_token": "verified-plus-token", "plan_type": "plus",
		}))
	c, recorder := selectedAccountAuditHTTPContext(t, "/v1/responses", body)
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	require.True(t, ok)
	apiKey.Group.AllowImageGeneration = false

	handler.Responses(c)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	require.Empty(t, upstream.calls(), "opaque image intent must be rejected before upstream dispatch")
	require.Empty(t, scannerCalls(), "oversize classification must not scan the body")
	state := openAISecurityAdmissionFromContext(c)
	require.NotNil(t, state)
	require.Equal(t, securityadmission.RequestUninspectable, state.admission.Class())
	require.Equal(t, securityadmission.ReasonLargeBody, state.admission.Reason())
}
