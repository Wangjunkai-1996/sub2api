//go:build unit

package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// openAIResponsesFailoverCancelUpstream 固定返回 HTTP 520，可在首次上游调用时
// 触发回调（用于模拟“上游在途期间客户端断开”）。
type openAIResponsesFailoverCancelUpstream struct {
	service.HTTPUpstream
	mu         sync.Mutex
	accountIDs []int64
	onFirstDo  func()
}

type openAIResponsesCapacityFailoverUpstream struct {
	service.HTTPUpstream
	mu         sync.Mutex
	accountIDs []int64
}

func (u *openAIResponsesCapacityFailoverUpstream) Do(_ *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	u.accountIDs = append(u.accountIDs, accountID)
	u.mu.Unlock()

	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_capacity","instructions":"` + strings.Repeat("p", 8*1024) + `"}}`,
		"",
		`data: {"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"overloaded"}}`,
		"",
		`data: {"type":"response.failed","response":{"id":"resp_capacity","status":"failed","error":{"code":"server_is_overloaded","message":"overloaded"}}}`,
		"",
	}, "\n")
	if accountID == 3 {
		body = strings.Join([]string{
			`data: {"type":"response.output_text.delta","delta":"ok"}`,
			"",
			`data: {"type":"response.completed","response":{"id":"resp_capacity_ok","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}`,
			"",
		}, "\n")
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func (u *openAIResponsesCapacityFailoverUpstream) calls() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int64(nil), u.accountIDs...)
}

type openAIResponsesStrictStickyCache struct {
	service.GatewayCache
	mu                   sync.Mutex
	sessionBindings      map[string]int64
	accountID            int64
	responseGetErr       error
	responseGetErrAfter  int32
	responseGetCalls     atomic.Int32
	responseBindErr      error
	responseBindAttempts atomic.Int32
}

func (c *openAIResponsesStrictStickyCache) GetSessionAccountID(_ context.Context, groupID int64, key string) (int64, error) {
	call := c.responseGetCalls.Add(1)
	if c.responseGetErr != nil && call > c.responseGetErrAfter {
		return 0, c.responseGetErr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if accountID, ok := c.sessionBindings[fmt.Sprintf("%d:%s", groupID, key)]; ok {
		return accountID, nil
	}
	return c.accountID, nil
}

func (c *openAIResponsesStrictStickyCache) SetSessionAccountID(_ context.Context, groupID int64, key string, accountID int64, _ time.Duration) error {
	if strings.HasPrefix(key, "openai:response:") {
		c.responseBindAttempts.Add(1)
		if c.responseBindErr != nil {
			return c.responseBindErr
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessionBindings == nil {
		c.sessionBindings = make(map[string]int64)
	}
	c.sessionBindings[fmt.Sprintf("%d:%s", groupID, key)] = accountID
	return nil
}

func (c *openAIResponsesStrictStickyCache) RefreshSessionTTL(context.Context, int64, string, time.Duration) error {
	return nil
}

func (u *openAIResponsesFailoverCancelUpstream) Do(_ *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	u.accountIDs = append(u.accountIDs, accountID)
	first := len(u.accountIDs) == 1
	u.mu.Unlock()
	if first && u.onFirstDo != nil {
		u.onFirstDo()
	}
	return &http.Response{
		StatusCode: 520,
		Header:     http.Header{"Content-Type": []string{"text/html"}},
		Body:       io.NopCloser(bytes.NewBufferString("<html>520: unknown error</html>")),
	}, nil
}

func (u *openAIResponsesFailoverCancelUpstream) calls() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int64(nil), u.accountIDs...)
}

type openAIResponsesStrictSuccessUpstream struct {
	service.HTTPUpstream
	stream bool
	calls  atomic.Int32
}

func (u *openAIResponsesStrictSuccessUpstream) Do(_ *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.calls.Add(1)
	contentType := "application/json"
	body := `{"id":"resp_strict_sticky","object":"response","status":"completed","model":"gpt-5.1","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"safe answer"}]}],"usage":{"input_tokens":1,"output_tokens":1}}`
	if u.stream {
		contentType = "text/event-stream"
		body = "data: " + body[:1] + `"type":"response.completed","response":` + body + "}\n\n" + "data: [DONE]\n\n"
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{contentType}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

type openAIResponsesStrictLineageStore struct {
	binds atomic.Int32
}

type openAIHandlerStrictLegacySpy struct {
	blockingCalls atomic.Int32
	checkCalls    atomic.Int32
}

func (s *openAIHandlerStrictLegacySpy) BlockingApplies(context.Context, securityaudit.Request) (bool, error) {
	s.blockingCalls.Add(1)
	return true, nil
}

func (s *openAIHandlerStrictLegacySpy) Check(context.Context, securityaudit.Request) (*securityaudit.LegacyDecision, error) {
	s.checkCalls.Add(1)
	return &securityaudit.LegacyDecision{Blocked: true, Flagged: true}, nil
}

type openAIHandlerStrictTurnAuditSpy struct {
	blockingCalls atomic.Int32
	checkCalls    atomic.Int32
}

func (s *openAIHandlerStrictTurnAuditSpy) BlockingApplies(context.Context, securityaudit.Request) (bool, error) {
	s.blockingCalls.Add(1)
	return true, nil
}

func (s *openAIHandlerStrictTurnAuditSpy) Check(_ context.Context, req securityaudit.Request) (*securityaudit.LegacyDecision, error) {
	s.checkCalls.Add(1)
	if req.Document != nil && strings.Contains(req.Document.NormalizedText, "synthetic cyber policy marker") {
		return &securityaudit.LegacyDecision{Blocked: true, Flagged: true}, nil
	}
	return &securityaudit.LegacyDecision{Allowed: true}, nil
}

func (s *openAIResponsesStrictLineageStore) Load(context.Context, securityaudit.LineageLookup) (*securityaudit.AuditSummary, error) {
	return nil, securityaudit.ErrLineageNotFound
}

func (s *openAIResponsesStrictLineageStore) BindAllowedResponse(context.Context, securityaudit.AuditSummary, string) error {
	s.binds.Add(1)
	return nil
}

func newOpenAIResponsesFailoverTestHandler(t *testing.T, upstream service.HTTPUpstream) *OpenAIGatewayHandler {
	t.Helper()
	accounts := []service.Account{
		{
			ID:          1,
			Name:        "responses-account-1",
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeOAuth,
			Status:      service.StatusActive,
			Schedulable: true,
			Concurrency: 0,
			Priority:    0,
			Credentials: map[string]any{"access_token": "token-1"},
		},
		{
			ID:          2,
			Name:        "responses-account-2",
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeOAuth,
			Status:      service.StatusActive,
			Schedulable: true,
			Concurrency: 0,
			Priority:    1,
			Credentials: map[string]any{"access_token": "token-2"},
		},
	}
	return newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts, 0)
}

func newOpenAIFailoverTestHandlerWithAccounts(
	t *testing.T,
	upstream service.HTTPUpstream,
	accounts []service.Account,
	auditGroupID int64,
) *OpenAIGatewayHandler {
	t.Helper()
	accountRepo := openAIImagesFailoverAccountRepo{accounts: accounts}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	var settingService *service.SettingService
	if auditGroupID > 0 {
		settingService = service.NewSettingService(&cyberHandlerSettingRepo{values: map[string]string{
			service.SettingKeyOpenAIAccountAuditGroupIDs:              fmt.Sprintf("[%d]", auditGroupID),
			service.SettingKeyOpenAIAccountAuditLongTextRuneThreshold: "12000",
			service.SettingKeyOpenAIAccountAuditPreferAPIKeyEnabled:   "true",
		}}, cfg)
	}
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
		nil,
		nil,
		nil,
		nil,
		upstream,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		settingService,
		nil,
	)
	billingService := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billingService.Stop)
	concurrencyService := service.NewConcurrencyService(nil)
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
	handler.securityAuditCoordinator = securityaudit.NewCoordinator(nil, nil)
	handler.maxAccountSwitches = 10
	return handler
}

func openAIAccountAuditFailoverTestAccount(id int64, priority int, accountType string, groupID int64) service.Account {
	account := service.Account{
		ID: id, Name: fmt.Sprintf("audit-failover-account-%d", id),
		Platform: service.PlatformOpenAI, Type: accountType,
		Status: service.StatusActive, Schedulable: true, Concurrency: 0, Priority: priority,
		GroupIDs: []int64{groupID},
	}
	if accountType == service.AccountTypeOAuth {
		account.Credentials = map[string]any{"access_token": fmt.Sprintf("oauth-token-%d", id), "plan_type": "pro"}
	} else {
		account.Credentials = map[string]any{"api_key": fmt.Sprintf("sk-test-%d", id)}
	}
	return account
}

func newOpenAIResponsesStrictFailoverTestHandler(t *testing.T, upstream service.HTTPUpstream, groupID int64) *OpenAIGatewayHandler {
	return newOpenAIResponsesStrictFailoverTestHandlerWithState(t, upstream, groupID, nil, nil)
}

func newOpenAIResponsesStrictFailoverTestHandlerWithState(
	t *testing.T,
	upstream service.HTTPUpstream,
	groupID int64,
	cache *openAIResponsesStrictStickyCache,
	lineage securityaudit.LineageStore,
	accountBaseURLs ...string,
) *OpenAIGatewayHandler {
	return newOpenAIResponsesContinuationTestHandlerWithState(
		t, upstream, groupID, cache, lineage, service.AccountTypeOAuth, accountBaseURLs...,
	)
}

func newOpenAIResponsesAPIKeyContinuationTestHandlerWithState(
	t *testing.T,
	upstream service.HTTPUpstream,
	groupID int64,
	cache *openAIResponsesStrictStickyCache,
) *OpenAIGatewayHandler {
	return newOpenAIResponsesContinuationTestHandlerWithState(
		t, upstream, groupID, cache, nil, service.AccountTypeAPIKey,
	)
}

func newOpenAIResponsesContinuationTestHandlerWithState(
	t *testing.T,
	upstream service.HTTPUpstream,
	groupID int64,
	cache *openAIResponsesStrictStickyCache,
	lineage securityaudit.LineageStore,
	accountType string,
	accountBaseURLs ...string,
) *OpenAIGatewayHandler {
	t.Helper()
	if cache == nil {
		cache = &openAIResponsesStrictStickyCache{accountID: 1}
	}
	if lineage == nil {
		lineage = &handlerAllowLineageStore{summary: securityaudit.AuditSummary{
			Verdict: securityaudit.AuditVerdictAllow, ContextComplete: true,
			APIKeyID: 99, GroupID: &groupID, PromptHash: "parent-hash", RedactedContext: "parent context",
		}}
	}
	producingBaseURL := "https://api.example.test"
	if len(accountBaseURLs) > 0 && strings.TrimSpace(accountBaseURLs[0]) != "" {
		producingBaseURL = strings.TrimSpace(accountBaseURLs[0])
	}
	producingCredentials := map[string]any{
		"base_url":                     producingBaseURL,
		"pool_mode":                    true,
		"pool_mode_retry_count":        float64(1),
		"pool_mode_retry_status_codes": []any{float64(520)},
	}
	backupCredentials := map[string]any{"base_url": "https://api.example.test"}
	producingExtra := map[string]any{"openai_responses_supported": true}
	backupExtra := map[string]any{"openai_responses_supported": true}
	if accountType == service.AccountTypeOAuth {
		producingCredentials["access_token"] = "token-producing"
		producingCredentials["plan_type"] = "pro"
		backupCredentials["access_token"] = "token-backup"
		backupCredentials["plan_type"] = "pro"
		producingExtra["openai_oauth_responses_websockets_v2_enabled"] = true
		backupExtra["openai_oauth_responses_websockets_v2_enabled"] = true
	} else {
		producingCredentials["api_key"] = "sk-producing"
		backupCredentials["api_key"] = "sk-backup"
		producingExtra["openai_passthrough"] = true
		backupExtra["openai_passthrough"] = true
		producingExtra["openai_apikey_responses_websockets_v2_enabled"] = true
		backupExtra["openai_apikey_responses_websockets_v2_enabled"] = true
	}
	accounts := []service.Account{
		{
			ID: 1, Name: "strict-producing-account", Platform: service.PlatformOpenAI,
			Type: accountType, Status: service.StatusActive, Schedulable: true,
			Concurrency: 0, Priority: 0, GroupIDs: []int64{groupID},
			Credentials: producingCredentials,
			Extra:       producingExtra,
		},
		{
			ID: 2, Name: "strict-backup-account", Platform: service.PlatformOpenAI,
			Type: accountType, Status: service.StatusActive, Schedulable: true,
			Concurrency: 0, Priority: 1, GroupIDs: []int64{groupID},
			Credentials: backupCredentials,
			Extra:       backupExtra,
		},
	}
	accountRepo := &openAIWSFailoverHandlerAccountRepoStub{accounts: accounts}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Default.RateMultiplier = 1
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.MaxAccountSwitches = 3
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.APIKeyEnabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	settingService := service.NewSettingService(&cyberHandlerSettingRepo{values: map[string]string{
		service.SettingKeyOpenAIAccountAuditGroupIDs:              fmt.Sprintf("[%d]", groupID),
		service.SettingKeyOpenAIAccountAuditLongTextRuneThreshold: "12000",
		service.SettingKeyOpenAIAccountAuditPreferAPIKeyEnabled:   "true",
	}}, cfg)
	billingService := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billingService.Stop)
	gatewayService := service.NewOpenAIGatewayService(
		accountRepo, nil, nil, nil, nil, nil,
		cache, cfg, nil, nil,
		service.NewBillingService(cfg, nil), nil, billingService, upstream,
		&service.DeferredService{}, nil, nil, nil, nil, nil, settingService, nil,
	)
	handler := NewOpenAIGatewayHandler(
		gatewayService,
		service.NewConcurrencyService(nil),
		billingService,
		service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg),
		nil,
		nil,
		nil,
		nil,
		cfg,
	)
	handler.securityAuditCoordinator = securityaudit.NewCoordinator(
		&openAIHandlerStrictTurnAuditSpy{},
		&handlerPromptEngine{mode: securityaudit.ModeOff},
	).SetLineageStore(lineage)
	handler.maxAccountSwitches = 3
	return handler
}

func newOpenAIResponsesFailoverTestContext(t *testing.T, ctx context.Context) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	groupID := int64(3131)
	body := []byte(`{"model":"gpt-5.1","stream":false,"input":"hello"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req
	c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
		ID:      99,
		GroupID: &groupID,
		Group: &service.Group{
			ID:       groupID,
			Platform: service.PlatformOpenAI,
		},
		User: &service.User{ID: 100},
	})
	c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100, Concurrency: 0})
	return c, rec
}

// TestOpenAIGatewayHandlerResponses_FailoverAbortsWhenClientDisconnected 复现
// #4257：客户端在上游请求在途期间断开，上游随后返回可 failover 的 520。
// 期望：不再用已取消的 context 重新选号（不触达账号 2）、不把取消误报成
// 502 账号耗尽、请求按 499 归类。
func TestOpenAIGatewayHandlerResponses_FailoverAbortsWhenClientDisconnected(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upstream := &openAIResponsesFailoverCancelUpstream{onFirstDo: cancel}
	handler := newOpenAIResponsesFailoverTestHandler(t, upstream)
	c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)

	handler.Responses(c)

	require.Equal(t, []int64{1}, upstream.calls(), "客户端断开后不应再切换到账号 2")
	require.Equal(t, statusClientClosedRequest, c.Writer.Status(), "应按 499 归类")
	require.Zero(t, rec.Body.Len(), "不应写入 502 错误响应体")

	_, hasFinalUpstreamErr := c.Get(service.OpsUpstreamStatusCodeKey)
	require.False(t, hasFinalUpstreamErr, "不应记录 failover 耗尽的上游错误终态")

	// 真实发生过的 520 应保留 failover 事件（service 层在返回 failover 错误前记录）
	rawEvents, ok := c.Get(service.OpsUpstreamErrorsKey)
	require.True(t, ok)
	events, ok := rawEvents.([]*service.OpsUpstreamErrorEvent)
	require.True(t, ok)
	require.Len(t, events, 1)
	require.Equal(t, "failover", events[0].Kind)
	require.Equal(t, 520, events[0].UpstreamStatusCode)
}

// TestOpenAIGatewayHandlerResponses_FailoverContinuesForConnectedClient 回归
// 守卫：客户端在线时 failover 行为不变——切换到账号 2，两个账号都 520 后按
// 耗尽返回 502。
func TestOpenAIGatewayHandlerResponses_FailoverContinuesForConnectedClient(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := &openAIResponsesFailoverCancelUpstream{}
	handler := newOpenAIResponsesFailoverTestHandler(t, upstream)
	c, rec := newOpenAIResponsesFailoverTestContext(t, nil)

	handler.Responses(c)

	require.Equal(t, []int64{1, 2}, upstream.calls(), "在线客户端应正常切换账号")
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Equal(t, "upstream_error", gjson.GetBytes(rec.Body.Bytes(), "error.type").String())
}

func TestOpenAIGatewayHandlerResponses_CapacityRetryThenUsesNormalAccountSwitchBudget(t *testing.T) {
	gin.SetMode(gin.TestMode)
	newCapacityAccount := func(id int64, priority, retryCount int) service.Account {
		return service.Account{
			ID: id, Name: fmt.Sprintf("capacity-account-%d", id),
			Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
			Status: service.StatusActive, Schedulable: true, Priority: priority,
			Credentials: map[string]any{
				"api_key": "sk-test", "base_url": "https://api.example.test",
				"pool_mode": true, "pool_mode_retry_count": float64(retryCount),
			},
			Extra: map[string]any{"openai_passthrough": false},
		}
	}
	accounts := []service.Account{
		newCapacityAccount(1, 0, 1),
		newCapacityAccount(2, 1, 0),
		newCapacityAccount(3, 2, 0),
	}

	upstream := &openAIResponsesCapacityFailoverUpstream{}
	handler := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts, 0)
	handler.cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 30
	c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
		`{"model":"gpt-5.1","stream":true,"input":"hello"}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")

	handler.Responses(c)

	require.Equal(t, []int64{1, 1, 2, 3}, upstream.calls(), "status=%d body=%s", rec.Code, rec.Body.String())
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"delta":"ok"`)
}

func TestOpenAIChatCompletions_LongSecurityContextTriesAPIKeyPoolBeforeAuditedOAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(3131)
	accounts := []service.Account{
		openAIAccountAuditFailoverTestAccount(1, 10, service.AccountTypeAPIKey, groupID),
		openAIAccountAuditFailoverTestAccount(2, 20, service.AccountTypeAPIKey, groupID),
		openAIAccountAuditFailoverTestAccount(3, 0, service.AccountTypeOAuth, groupID),
	}
	upstream := &openAIResponsesFailoverCancelUpstream{}
	handler := newOpenAIFailoverTestHandlerWithAccounts(
		t,
		upstream,
		accounts,
		groupID,
	)
	legacy := &countingAccountAuditLegacyEngine{decision: &securityaudit.LegacyDecision{Allowed: true}}
	handler.securityAuditCoordinator = securityaudit.NewCoordinator(legacy, nil)
	c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
	longSystemPrompt := strings.Repeat("s", service.DefaultOpenAIAccountAuditLongTextRuneThreshold+1)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(
		fmt.Sprintf(`{"model":"gpt-5.1","stream":false,"messages":[{"role":"system","content":%q},{"role":"user","content":"safe"}]}`, longSystemPrompt),
	))
	c.Request.Header.Set("Content-Type", "application/json")

	handler.ChatCompletions(c)

	require.Equal(t, []int64{1, 2, 3}, upstream.calls())
	require.Equal(t, int32(1), legacy.calls.Load(), "the first OAuth Pro attempt must audit exactly once")
	require.Greater(t, legacy.lastDocument.Load().AuditTextRunes, service.DefaultOpenAIAccountAuditLongTextRuneThreshold)
	require.Contains(t, legacy.lastDocument.Load().NormalizedText, "safe")
	require.Equal(t, http.StatusBadGateway, rec.Code)
}

func TestOpenAIChatCompletions_AuditFailureDoesNotFailOverToAPIKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tt := range []struct {
		name       string
		legacy     *countingAccountAuditLegacyEngine
		wantStatus int
		wantCode   string
	}{
		{
			name: "unavailable",
			legacy: &countingAccountAuditLegacyEngine{
				err: errors.New("audit unavailable"),
			},
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   securityaudit.ErrorCodeAuditUnavailable,
		},
		{
			name: "blocked",
			legacy: &countingAccountAuditLegacyEngine{
				decision: &securityaudit.LegacyDecision{Blocked: true, Flagged: true},
			},
			wantStatus: http.StatusForbidden,
			wantCode:   securityaudit.ErrorCodePolicyBlocked,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			groupID := int64(3131)
			accounts := []service.Account{
				openAIAccountAuditFailoverTestAccount(1, 0, service.AccountTypeOAuth, groupID),
				openAIAccountAuditFailoverTestAccount(2, 1, service.AccountTypeAPIKey, groupID),
			}
			upstream := &openAIResponsesFailoverCancelUpstream{}
			handler := newOpenAIFailoverTestHandlerWithAccounts(
				t,
				upstream,
				accounts,
				groupID,
			)
			handler.securityAuditCoordinator = securityaudit.NewCoordinator(tt.legacy, nil)
			c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(
				`{"model":"gpt-5.1","stream":false,"messages":[{"role":"user","content":"safe"}]}`,
			))
			c.Request.Header.Set("Content-Type", "application/json")

			handler.ChatCompletions(c)

			require.Equal(t, int32(1), tt.legacy.calls.Load())
			require.Empty(t, upstream.calls(), "audit failure must stop before OAuth or backup APIKey forwarding")
			require.Equal(t, tt.wantStatus, rec.Code)
			require.Contains(t, rec.Body.String(), tt.wantCode)
		})
	}
}

func TestOpenAIGatewayHandlerResponses_APIKeyContinuationStaysHTTPAndSkipsAudit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(3131)
	upstream := &openAIResponsesFailoverCancelUpstream{}
	handler := newOpenAIResponsesAPIKeyContinuationTestHandlerWithState(
		t,
		upstream,
		groupID,
		&openAIResponsesStrictStickyCache{accountID: 1},
	)
	legacy := &openAIHandlerStrictLegacySpy{}
	prompt := &handlerPromptEngine{
		mode: securityaudit.ModeBlocking, strict: true,
		decision: &securityaudit.PromptDecision{Kind: securityaudit.DecisionBlock, AllowNextStage: false},
	}
	handler.securityAuditCoordinator = securityaudit.NewCoordinator(legacy, prompt)
	c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
	longInstructions := strings.Repeat("i", service.DefaultOpenAIAccountAuditLongTextRuneThreshold+1)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
		fmt.Sprintf(`{"model":"gpt-5.1","stream":false,"previous_response_id":"resp_api_key_parent","instructions":%q,"input":"continue"}`, longInstructions),
	))
	c.Request.Header.Set("Content-Type", "application/json")

	handler.Responses(c)

	require.Equal(t, []int64{1}, upstream.calls(), "hard binding must keep the producing APIKey account on the HTTP path")
	require.Zero(t, legacy.blockingCalls.Load())
	require.Zero(t, legacy.checkCalls.Load())
	evaluated, enqueued, requests := prompt.snapshot()
	require.Zero(t, evaluated)
	require.Zero(t, enqueued)
	require.Empty(t, requests)
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.NotContains(t, rec.Body.String(), securityaudit.ErrorCodeAuditUnavailable)
	require.NotContains(t, rec.Body.String(), securityaudit.ErrorCodeLineageIncompatible)
}

func TestOpenAIGatewayHandlerResponses_ContinuationBindingMissIsNotAuditError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(3131)
	upstream := &openAIResponsesFailoverCancelUpstream{}
	handler := newOpenAIResponsesAPIKeyContinuationTestHandlerWithState(
		t,
		upstream,
		groupID,
		&openAIResponsesStrictStickyCache{},
	)
	legacy := &openAIHandlerStrictLegacySpy{}
	handler.securityAuditCoordinator = securityaudit.NewCoordinator(legacy, &handlerPromptEngine{mode: securityaudit.ModeOff})
	c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
		`{"model":"gpt-5.1","stream":false,"previous_response_id":"resp_missing","input":"continue"}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")

	handler.Responses(c)

	require.Empty(t, upstream.calls())
	require.Zero(t, legacy.blockingCalls.Load())
	require.Zero(t, legacy.checkCalls.Load())
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "invalid_request_error", gjson.GetBytes(rec.Body.Bytes(), "error.type").String())
	require.NotContains(t, rec.Body.String(), securityaudit.ErrorCodeAuditUnavailable)
	require.NotContains(t, rec.Body.String(), securityaudit.ErrorCodeLineageIncompatible)
}

func TestOpenAIResponsesWebSocket_AuditDependsOnFinalAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tt := range []struct {
		name                string
		accountType         string
		wantModerationCalls int32
		wantUpstreamCalls   int32
		wantPolicyClose     bool
	}{
		{
			name: "oauth pro blocks after selection", accountType: service.AccountTypeOAuth,
			wantModerationCalls: 1, wantPolicyClose: true,
		},
		{
			name: "api key bypasses audit", accountType: service.AccountTypeAPIKey,
			wantUpstreamCalls: 1,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var moderationCalls atomic.Int32
			var moderationPayload atomic.Value
			moderationServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				moderationCalls.Add(1)
				require.Equal(t, "/v1/moderations", r.URL.Path)
				payload, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				moderationPayload.Store(string(payload))
				resultCount := int(gjson.GetBytes(payload, "input.#").Int())
				if resultCount == 0 {
					resultCount = 1
				}
				result := json.RawMessage(`{"flagged":true,"category_scores":{"harassment":0.01,"harassment/threatening":0.01,"hate":0.01,"hate/threatening":0.01,"illicit":0.01,"illicit/violent":0.01,"self-harm":0.01,"self-harm/intent":0.01,"self-harm/instructions":0.01,"sexual":0.9,"sexual/minors":0.01,"violence":0.01,"violence/graphic":0.01}}`)
				results := make([]json.RawMessage, resultCount)
				for index := range results {
					results[index] = result
				}
				require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"results": results}))
			}))
			defer moderationServer.Close()

			var upstreamCalls atomic.Int32
			upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				upstreamCalls.Add(1)
				conn, err := coderws.Accept(w, r, nil)
				require.NoError(t, err)
				defer func() { _ = conn.CloseNow() }()
				ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
				defer cancel()
				_, _, err = conn.Read(ctx)
				require.NoError(t, err)
				require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(
					`{"type":"response.completed","response":{"id":"resp_api_key_ws","status":"completed","model":"gpt-5.1","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`,
				)))
			}))
			defer upstreamServer.Close()

			groupID := int64(3131)
			handler := newOpenAIResponsesContinuationTestHandlerWithState(
				t,
				&openAIResponsesFailoverCancelUpstream{},
				groupID,
				&openAIResponsesStrictStickyCache{accountID: 1},
				nil,
				tt.accountType,
				upstreamServer.URL,
			)
			moderationCfg := service.ContentModerationConfig{
				Enabled: true, Mode: service.ContentModerationModePreBlock,
				BaseURL: moderationServer.URL, Model: "omni-moderation-latest",
				APIKeys: []string{"sk-test"}, MaxRPM: 100000, SampleRate: 100,
				AllGroups: true, BlockMessage: "blocked by account-aware audit",
			}
			rawCfg, err := json.Marshal(moderationCfg)
			require.NoError(t, err)
			moderationRepo := &contentModerationHandlerTestRepo{}
			moderationService := service.NewContentModerationService(
				&contentModerationHandlerSettingRepo{values: map[string]string{
					service.SettingKeyRiskControlEnabled:      "true",
					service.SettingKeyContentModerationConfig: string(rawCfg),
				}},
				moderationRepo, nil, nil, nil, nil, nil, nil,
			)
			handler.contentModerationService = moderationService
			handler.securityAuditCoordinator = securityaudit.NewCoordinator(
				securityaudit.NewLegacyModerationAdapter(moderationService), nil,
			)

			apiKey := &service.APIKey{
				ID: 99, GroupID: &groupID,
				Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI},
				User:  &service.User{ID: 100, Status: service.StatusActive},
			}
			router := gin.New()
			router.Use(func(c *gin.Context) {
				c.Set(string(middleware2.ContextKeyAPIKey), apiKey)
				c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100, Concurrency: 0})
				c.Next()
			})
			router.GET("/openai/v1/responses", handler.ResponsesWebSocket)
			server := httptest.NewServer(router)
			defer server.Close()

			dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
			client, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http")+"/openai/v1/responses", nil)
			cancelDial()
			require.NoError(t, err)
			defer func() { _ = client.CloseNow() }()
			writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
			longInstructions := "ws-instruction-marker " + strings.Repeat("w", service.DefaultOpenAIAccountAuditLongTextRuneThreshold+1)
			require.NoError(t, client.Write(writeCtx, coderws.MessageText, []byte(
				fmt.Sprintf(`{"type":"response.create","model":"gpt-5.1","instructions":%q,"input":"bad prompt"}`, longInstructions),
			)))
			cancelWrite()

			readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
			_, payload, readErr := client.Read(readCtx)
			cancelRead()
			if tt.wantPolicyClose {
				if readErr == nil {
					require.Contains(t, string(payload), securityaudit.ErrorCodePolicyBlocked)
					readCtx, cancelRead = context.WithTimeout(context.Background(), 3*time.Second)
					_, _, readErr = client.Read(readCtx)
					cancelRead()
				}
				var closeErr coderws.CloseError
				require.ErrorAs(t, readErr, &closeErr)
				require.Equal(t, coderws.StatusCode(4403), closeErr.Code)
			} else {
				require.NoError(t, readErr)
				require.Equal(t, "response.completed", gjson.GetBytes(payload, "type").String())
			}

			require.Eventually(t, func() bool {
				return moderationCalls.Load() == tt.wantModerationCalls
			}, time.Second, 10*time.Millisecond)
			require.Equal(t, tt.wantUpstreamCalls, upstreamCalls.Load())
			if tt.wantModerationCalls > 0 {
				captured, ok := moderationPayload.Load().(string)
				require.True(t, ok)
				require.Contains(t, captured, "ws-instruction-marker")
			}
		})
	}
}

func TestOpenAIHandlersOAuthProNoCurrentUserTextSkipsAuditorsAndReachesUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name          string
		path          string
		body          string
		responses     bool
		wantStatus    int
		wantUpstream  bool
		wantCheckCall int32
	}{
		{
			name: "responses image only", path: "/v1/responses", responses: true,
			body:       `{"model":"gpt-5.1","stream":false,"input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AA=="}]}]}`,
			wantStatus: http.StatusBadGateway, wantUpstream: true,
		},
		{
			name: "responses empty input", path: "/v1/responses", responses: true,
			body:       `{"model":"gpt-5.1","stream":false,"input":[]}`,
			wantStatus: http.StatusBadGateway, wantUpstream: true,
		},
		{
			name: "chat trailing tool", path: "/v1/chat/completions",
			body:       `{"model":"gpt-5.1","stream":false,"messages":[{"role":"user","content":"historical user text"},{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_1","content":"done"}]}`,
			wantStatus: http.StatusForbidden, wantCheckCall: 1,
		},
		{
			name: "chat trailing assistant", path: "/v1/chat/completions",
			body:       `{"model":"gpt-5.1","stream":false,"messages":[{"role":"user","content":"historical user text"},{"role":"assistant","content":"historical assistant output"}]}`,
			wantStatus: http.StatusBadGateway, wantUpstream: true,
		},
		{
			name: "chat image only", path: "/v1/chat/completions",
			body:       `{"model":"gpt-5.1","stream":false,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]}]}`,
			wantStatus: http.StatusBadGateway, wantUpstream: true,
		},
		{
			name: "chat empty messages", path: "/v1/chat/completions",
			body:       `{"model":"gpt-5.1","stream":false,"messages":[]}`,
			wantStatus: http.StatusBadGateway, wantUpstream: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &openAIResponsesFailoverCancelUpstream{}
			handler := newOpenAIResponsesStrictFailoverTestHandler(t, upstream, 3131)
			legacy := &openAIHandlerStrictLegacySpy{}
			prompt := &handlerPromptEngine{
				mode: securityaudit.ModeBlocking, strict: true,
				decision: &securityaudit.PromptDecision{Kind: securityaudit.DecisionBlock, AllowNextStage: false},
			}
			handler.securityAuditCoordinator = securityaudit.NewCoordinator(legacy, prompt)
			c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
			c.Request = httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.body))
			c.Request.Header.Set("Content-Type", "application/json")

			if tt.responses {
				handler.Responses(c)
			} else {
				handler.ChatCompletions(c)
			}

			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
			if tt.wantUpstream {
				require.NotEmpty(t, upstream.calls(), "request must continue beyond the audit gate")
			} else {
				require.Empty(t, upstream.calls(), "blocked current input must not reach upstream")
			}
			require.Zero(t, legacy.blockingCalls.Load(), "account eligibility already established strict admission")
			require.Equal(t, tt.wantCheckCall, legacy.checkCalls.Load(), "only auditable current input must call Legacy Moderation")
			evaluated, enqueued, requests := prompt.snapshot()
			require.Zero(t, evaluated, "request without current user text must not call Prompt Guard")
			require.Zero(t, enqueued)
			require.Empty(t, requests)
			require.NotContains(t, rec.Body.String(), securityaudit.ErrorCodeContextIncomplete)
			require.NotContains(t, rec.Body.String(), securityaudit.ErrorCodeAuditUnavailable)
		})
	}
}

func TestOpenAIGatewayHandlerResponses_ContinuationBindingReadFailureIsGenericServiceError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(3131)
	cache := &openAIResponsesStrictStickyCache{
		accountID:           1,
		responseGetErr:      errors.New("redis unavailable"),
		responseGetErrAfter: 0,
	}
	upstream := &openAIResponsesFailoverCancelUpstream{}
	handler := newOpenAIResponsesStrictFailoverTestHandlerWithState(t, upstream, groupID, cache, nil)
	c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
		`{"model":"gpt-5.1","stream":false,"previous_response_id":"resp_parent","input":"hello"}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")

	handler.Responses(c)

	require.Equal(t, int32(1), cache.responseGetCalls.Load(), "selection must read the authoritative binding once")
	require.Empty(t, upstream.calls(), "Redis failure must stop before any upstream request")
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "service_unavailable", gjson.GetBytes(rec.Body.Bytes(), "error.type").String())
	require.NotContains(t, rec.Body.String(), securityaudit.ErrorCodeAuditUnavailable)
	require.NotContains(t, rec.Body.String(), securityaudit.ErrorCodeLineageIncompatible)
}

func TestOpenAIGatewayHandlerResponses_StrictStickyBindFailureWithholdsSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, stream := range []bool{false, true} {
		stream := stream
		t.Run(map[bool]string{false: "non_stream", true: "stream"}[stream], func(t *testing.T) {
			groupID := int64(3131)
			bindErr := errors.New("response account redis unavailable")
			cache := &openAIResponsesStrictStickyCache{accountID: 1, responseBindErr: bindErr}
			lineage := &openAIResponsesStrictLineageStore{}
			upstream := &openAIResponsesStrictSuccessUpstream{stream: stream}
			handler := newOpenAIResponsesStrictFailoverTestHandlerWithState(t, upstream, groupID, cache, lineage)
			c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
			requestBody := `{"model":"gpt-5.1","stream":false,"input":"hello"}`
			if stream {
				requestBody = `{"model":"gpt-5.1","stream":true,"input":"hello"}`
			}
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(requestBody))
			c.Request.Header.Set("Content-Type", "application/json")

			handler.Responses(c)

			require.Equal(t, int32(1), upstream.calls.Load(), "local commit failure must not replay upstream")
			require.GreaterOrEqual(t, cache.responseBindAttempts.Load(), int32(1))
			require.Zero(t, lineage.binds.Load(), "ledger must not be committed after sticky binding fails")
			require.NotContains(t, rec.Body.String(), `"type":"response.completed"`)
			require.Equal(t, http.StatusServiceUnavailable, rec.Code)
			require.Equal(t, securityaudit.ErrorCodeAuditUnavailable, gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
		})
	}
}
