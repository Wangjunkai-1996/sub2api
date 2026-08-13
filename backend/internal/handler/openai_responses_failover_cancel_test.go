//go:build unit

package handler

import (
	"bytes"
	"context"
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

type openAIResponsesMemoryLineageStore struct {
	mu      sync.Mutex
	entries map[string]securityaudit.AuditSummary
	loads   atomic.Int32
	binds   atomic.Int32
}

func strictLineageMemoryKey(groupID *int64, apiKeyID int64, responseID string) string {
	group := int64(0)
	if groupID != nil {
		group = *groupID
	}
	return fmt.Sprintf("%d:%d:%s", group, apiKeyID, strings.TrimSpace(responseID))
}

func (s *openAIResponsesMemoryLineageStore) Load(_ context.Context, lookup securityaudit.LineageLookup) (*securityaudit.AuditSummary, error) {
	s.loads.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	summary, ok := s.entries[strictLineageMemoryKey(lookup.GroupID, lookup.APIKeyID, lookup.PreviousResponseID)]
	if !ok {
		return nil, securityaudit.ErrLineageNotFound
	}
	cloned := summary.Clone()
	return &cloned, nil
}

func (s *openAIResponsesMemoryLineageStore) BindAllowedResponse(_ context.Context, summary securityaudit.AuditSummary, responseID string) error {
	s.binds.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entries == nil {
		s.entries = make(map[string]securityaudit.AuditSummary)
	}
	s.entries[strictLineageMemoryKey(summary.GroupID, summary.APIKeyID, responseID)] = summary.Clone()
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
	accountRepo := openAIImagesFailoverAccountRepo{accounts: accounts}
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
		nil,
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
	accounts := []service.Account{
		{
			ID: 1, Name: "strict-producing-account", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true,
			Concurrency: 0, Priority: 0, GroupIDs: []int64{groupID},
			Credentials: map[string]any{
				"api_key":                      "sk-producing",
				"base_url":                     producingBaseURL,
				"pool_mode":                    true,
				"pool_mode_retry_count":        float64(1),
				"pool_mode_retry_status_codes": []any{float64(520)},
			},
			Extra: map[string]any{
				"openai_passthrough":              true,
				"openai_responses_supported":      true,
				"responses_websockets_v2_enabled": true,
			},
		},
		{
			ID: 2, Name: "strict-backup-account", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true,
			Concurrency: 0, Priority: 1, GroupIDs: []int64{groupID},
			Credentials: map[string]any{
				"api_key":  "sk-backup",
				"base_url": "https://api.example.test",
			},
			Extra: map[string]any{
				"openai_passthrough":              true,
				"openai_responses_supported":      true,
				"responses_websockets_v2_enabled": true,
			},
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
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	billingService := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billingService.Stop)
	gatewayService := service.NewOpenAIGatewayService(
		accountRepo, nil, nil, nil, nil, nil,
		cache, cfg, nil, nil,
		service.NewBillingService(cfg, nil), nil, billingService, upstream,
		&service.DeferredService{}, nil, nil, nil, nil, nil, nil, nil,
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
		&handlerLegacyEngine{strict: true, decision: &securityaudit.LegacyDecision{Allowed: true}},
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

func TestOpenAIGatewayHandlerResponses_StrictContinuationNeverRetriesOrSwitchesAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(3131)
	var wsCalls atomic.Int32
	received := make(chan []byte, 1)
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsCalls.Add(1)
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept strict continuation websocket: %v", err)
			return
		}
		defer func() { _ = conn.CloseNow() }()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, payload, err := conn.Read(ctx)
		if err != nil {
			t.Errorf("read strict continuation websocket request: %v", err)
			return
		}
		received <- append([]byte(nil), payload...)
		require.NoError(t, conn.Write(ctx, coderws.MessageText, []byte(`{"type":"error","error":{"code":"server_error","message":"forced failure"}}`)))
	}))
	defer wsServer.Close()
	upstream := &openAIResponsesFailoverCancelUpstream{}
	lineage := &handlerAllowLineageStore{summary: securityaudit.AuditSummary{
		Verdict: securityaudit.AuditVerdictAllow, ContextComplete: true,
		APIKeyID: 99, GroupID: &groupID, PromptHash: "parent-hash", RedactedContext: "parent context",
	}}
	legacy := &openAIHandlerStrictTurnAuditSpy{}
	prompt := &handlerPromptEngine{mode: securityaudit.ModeOff}
	handler := newOpenAIResponsesStrictFailoverTestHandlerWithState(t, upstream, groupID, nil, lineage, wsServer.URL)
	handler.securityAuditCoordinator = securityaudit.NewCoordinator(legacy, prompt).SetLineageStore(lineage)
	c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader([]byte(
		`{"model":"gpt-5.1","stream":false,"previous_response_id":"resp_parent","input":[{"type":"function_call_output","call_id":"call_1","output":"done"}]}`,
	)))
	c.Request.Header.Set("Content-Type", "application/json")

	handler.Responses(c)

	require.Empty(t, upstream.calls(), "strict continuation must not use the HTTP passthrough path")
	require.Equal(t, int32(1), wsCalls.Load(), "strict continuation must make exactly one WSv2 attempt")
	select {
	case payload := <-received:
		require.Equal(t, "resp_parent", gjson.GetBytes(payload, "previous_response_id").String())
		require.Equal(t, "function_call_output", gjson.GetBytes(payload, "input.0.type").String())
	case <-time.After(3 * time.Second):
		t.Fatal("strict continuation websocket payload was not received")
	}
	require.Equal(t, int32(1), legacy.blockingCalls.Load(), "handler must enter the strict audit path")
	require.Equal(t, int32(1), legacy.checkCalls.Load(), "tool continuation output must be audited before upstream")
	evaluated, enqueued, requests := prompt.snapshot()
	require.Zero(t, evaluated, "tool continuation must not call Prompt Guard")
	require.Zero(t, enqueued)
	require.Empty(t, requests)
	require.Equal(t, int32(1), lineage.loads.Load(), "strict continuation must still validate lineage")
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Equal(t, "upstream_error", gjson.GetBytes(rec.Body.Bytes(), "error.type").String())
}

func TestOpenAIHandlersStrictNoCurrentUserTextSkipsAuditorsAndReachesUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name                string
		path                string
		body                string
		responses           bool
		bypassesStrictAudit bool
	}{
		{
			name: "responses image only", path: "/v1/responses", responses: true, bypassesStrictAudit: true,
			body: `{"model":"gpt-5.1","stream":false,"input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AA=="}]}]}`,
		},
		{
			name: "responses empty input", path: "/v1/responses", responses: true,
			body: `{"model":"gpt-5.1","stream":false,"input":[]}`,
		},
		{
			name: "chat trailing tool", path: "/v1/chat/completions",
			body: `{"model":"gpt-5.1","stream":false,"messages":[{"role":"user","content":"historical user text"},{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{}"}}]},{"role":"tool","tool_call_id":"call_1","content":"done"}]}`,
		},
		{
			name: "chat trailing assistant", path: "/v1/chat/completions",
			body: `{"model":"gpt-5.1","stream":false,"messages":[{"role":"user","content":"historical user text"},{"role":"assistant","content":"historical assistant output"}]}`,
		},
		{
			name: "chat image only", path: "/v1/chat/completions", bypassesStrictAudit: true,
			body: `{"model":"gpt-5.1","stream":false,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]}]}`,
		},
		{
			name: "chat empty messages", path: "/v1/chat/completions",
			body: `{"model":"gpt-5.1","stream":false,"messages":[]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &openAIResponsesFailoverCancelUpstream{}
			var handler *OpenAIGatewayHandler
			if tt.responses {
				handler = newOpenAIResponsesStrictFailoverTestHandler(t, upstream, 3131)
			} else {
				handler = newOpenAIResponsesFailoverTestHandler(t, upstream)
			}
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

			require.Equal(t, http.StatusBadGateway, rec.Code, rec.Body.String())
			require.NotEmpty(t, upstream.calls(), "request must continue beyond the audit gate")
			expectedBlockingCalls := int32(1)
			if tt.bypassesStrictAudit {
				expectedBlockingCalls = 0
			}
			require.Equal(t, expectedBlockingCalls, legacy.blockingCalls.Load(), "pure image requests must bypass strict text audit")
			require.Zero(t, legacy.checkCalls.Load(), "request without current user text must not call Legacy Moderation")
			evaluated, enqueued, requests := prompt.snapshot()
			require.Zero(t, evaluated, "request without current user text must not call Prompt Guard")
			require.Zero(t, enqueued)
			require.Empty(t, requests)
			require.NotContains(t, rec.Body.String(), securityaudit.ErrorCodeContextIncomplete)
			require.NotContains(t, rec.Body.String(), securityaudit.ErrorCodeAuditUnavailable)
		})
	}
}

func TestOpenAIResponsesStrictForwardSanitizedImageCannotHideCurrentUserText(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &openAIResponsesFailoverCancelUpstream{}
	handler := newOpenAIResponsesStrictFailoverTestHandler(t, upstream, 3131)
	legacy := &openAIHandlerStrictLegacySpy{}
	handler.securityAuditCoordinator = securityaudit.NewCoordinator(
		legacy,
		&handlerPromptEngine{mode: securityaudit.ModeOff},
	)
	c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model":"gpt-5.1",
		"stream":false,
		"input":[
			{"type":"message","role":"user","content":"current user text"},
			{"type":"input_image","image_url":"data:image/png;base64,"}
		]
	}`))
	c.Request.Header.Set("Content-Type", "application/json")

	handler.Responses(c)

	require.Equal(t, int32(1), legacy.blockingCalls.Load())
	require.Equal(t, int32(1), legacy.checkCalls.Load(), "sanitized image tail must not skip Legacy Moderation")
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	require.Empty(t, upstream.calls(), "blocked current text must not reach upstream")
	require.Equal(t, securityaudit.ErrorCodePolicyBlocked, gjson.GetBytes(rec.Body.Bytes(), "error.type").String())
}

func TestOpenAIGatewayHandlerResponses_StrictContinuationSecondStickyReadFailureIsAuditUnavailable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(3131)
	cache := &openAIResponsesStrictStickyCache{
		accountID:           1,
		responseGetErr:      errors.New("redis unavailable after preflight"),
		responseGetErrAfter: 1,
	}
	upstream := &openAIResponsesFailoverCancelUpstream{}
	handler := newOpenAIResponsesStrictFailoverTestHandlerWithState(t, upstream, groupID, cache, nil)
	c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
		`{"model":"gpt-5.1","stream":false,"previous_response_id":"resp_parent","input":"hello"}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")

	handler.Responses(c)

	require.Equal(t, int32(2), cache.responseGetCalls.Load(), "selection must re-read the authoritative binding after preflight")
	require.Empty(t, upstream.calls(), "Redis failure must stop before any upstream request")
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, securityaudit.ErrorCodeAuditUnavailable, gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
}

func TestOpenAIGatewayHandlerResponses_StrictFirstHTTPThenContinuationWSv2ReusesLedgerAndAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(3131)
	var wsCalls atomic.Int32
	received := make(chan []byte, 1)
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wsCalls.Add(1)
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept strict continuation websocket: %v", err)
			return
		}
		defer func() { _ = conn.CloseNow() }()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, payload, err := conn.Read(ctx)
		if err != nil {
			t.Errorf("read strict continuation websocket request: %v", err)
			return
		}
		received <- append([]byte(nil), payload...)
		for _, event := range []string{
			`{"type":"response.created","response":{"id":"resp_two_turn_child","model":"gpt-5.1"}}`,
			`{"type":"response.completed","response":{"id":"resp_two_turn_child","status":"completed","model":"gpt-5.1","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"second safe answer"}]}],"usage":{"input_tokens":2,"output_tokens":2}}}`,
		} {
			if err := conn.Write(ctx, coderws.MessageText, []byte(event)); err != nil {
				t.Errorf("write strict continuation websocket event: %v", err)
				return
			}
		}
	}))
	defer wsServer.Close()

	cache := &openAIResponsesStrictStickyCache{}
	lineage := &openAIResponsesMemoryLineageStore{}
	httpUpstream := &openAIResponsesStrictSuccessUpstream{}
	handler := newOpenAIResponsesStrictFailoverTestHandlerWithState(t, httpUpstream, groupID, cache, lineage, wsServer.URL)

	first, firstRec := newOpenAIResponsesFailoverTestContext(t, nil)
	first.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
		`{"model":"gpt-5.1","stream":false,"input":"first safe prompt"}`,
	))
	first.Request.Header.Set("Content-Type", "application/json")
	handler.Responses(first)

	require.Equal(t, http.StatusOK, firstRec.Code)
	require.Equal(t, "resp_strict_sticky", gjson.GetBytes(firstRec.Body.Bytes(), "id").String())
	require.Equal(t, int32(1), httpUpstream.calls.Load(), "first turn must use the HTTP Responses path")
	require.GreaterOrEqual(t, cache.responseBindAttempts.Load(), int32(1), "first terminal must bind the producing account")
	require.Equal(t, int32(1), lineage.binds.Load(), "first terminal must persist the augmented audit ledger")

	second, secondRec := newOpenAIResponsesFailoverTestContext(t, nil)
	second.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
		`{"model":"gpt-5.1","stream":false,"previous_response_id":"resp_strict_sticky","input":"second safe prompt"}`,
	))
	second.Request.Header.Set("Content-Type", "application/json")
	handler.Responses(second)

	require.Equal(t, http.StatusOK, secondRec.Code)
	require.Equal(t, "resp_two_turn_child", gjson.GetBytes(secondRec.Body.Bytes(), "id").String())
	require.Equal(t, int32(1), httpUpstream.calls.Load(), "continuation must not return to HTTP passthrough")
	require.Equal(t, int32(1), wsCalls.Load(), "continuation must use one WSv2 attempt on the producing account")
	require.Equal(t, int32(1), lineage.loads.Load(), "continuation audit must reuse the first-turn ledger")
	require.Equal(t, int32(2), lineage.binds.Load(), "successful continuation must persist its child ledger")
	select {
	case payload := <-received:
		require.Equal(t, "response.create", gjson.GetBytes(payload, "type").String())
		require.Equal(t, "resp_strict_sticky", gjson.GetBytes(payload, "previous_response_id").String())
	case <-time.After(3 * time.Second):
		t.Fatal("strict continuation websocket payload was not received")
	}
}

func TestOpenAIResponsesWebSocketStrictTextImageDangerousTextLifecycle(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(12)
	cache := &openAIResponsesStrictStickyCache{}
	lineage := &openAIResponsesMemoryLineageStore{}
	upstreamPayloads := make(chan []byte, 3)
	upstreamDone := make(chan error, 1)
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := coderws.Accept(w, r, nil)
		if err != nil {
			upstreamDone <- err
			return
		}
		defer func() { _ = conn.CloseNow() }()
		for turn, responseID := range []string{"resp_strict_text", "resp_strict_image"} {
			readCtx, cancelRead := context.WithTimeout(r.Context(), 3*time.Second)
			_, payload, readErr := conn.Read(readCtx)
			cancelRead()
			if readErr != nil {
				upstreamDone <- readErr
				return
			}
			upstreamPayloads <- append([]byte(nil), payload...)
			output := `[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"safe answer"}]}]`
			if turn == 1 {
				output = `[{"type":"image_generation_call","id":"ig_synthetic","status":"completed","result":"synthetic-image"}]`
			}
			event := fmt.Sprintf(
				`{"type":"response.completed","response":{"id":%q,"status":"completed","model":"gpt-5.5","output":%s,"usage":{"input_tokens":1,"output_tokens":1}}}`,
				responseID, output,
			)
			writeCtx, cancelWrite := context.WithTimeout(r.Context(), 3*time.Second)
			writeErr := conn.Write(writeCtx, coderws.MessageText, []byte(event))
			cancelWrite()
			if writeErr != nil {
				upstreamDone <- writeErr
				return
			}
		}
		readCtx, cancelRead := context.WithTimeout(r.Context(), 500*time.Millisecond)
		_, payload, readErr := conn.Read(readCtx)
		cancelRead()
		if readErr == nil {
			upstreamPayloads <- append([]byte(nil), payload...)
			upstreamDone <- errors.New("dangerous third turn reached upstream")
			return
		}
		upstreamDone <- nil
	}))
	defer upstreamServer.Close()

	handler := newOpenAIResponsesStrictFailoverTestHandlerWithState(
		t, &openAIResponsesFailoverCancelUpstream{}, groupID, cache, lineage, upstreamServer.URL,
	)
	legacy := &openAIHandlerStrictTurnAuditSpy{}
	handler.securityAuditCoordinator = securityaudit.NewCoordinator(
		legacy,
		&handlerPromptEngine{mode: securityaudit.ModeOff},
	).SetLineageStore(lineage)

	user := &service.User{ID: 100, Username: "strict-ws", Status: service.StatusActive}
	apiKey := &service.APIKey{
		ID: 99, UserID: user.ID, User: user, GroupID: &groupID,
		Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive},
	}
	handlerDone := make(chan struct{})
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware2.ContextKeyAPIKey), apiKey)
		c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: user.ID, Concurrency: 0})
		c.Next()
	})
	router.GET("/openai/v1/responses", func(c *gin.Context) {
		handler.ResponsesWebSocket(c)
		close(handlerDone)
	})
	handlerServer := httptest.NewServer(router)
	defer handlerServer.Close()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(handlerServer.URL, "http")+"/openai/v1/responses", nil)
	cancelDial()
	require.NoError(t, err)
	defer func() { _ = clientConn.CloseNow() }()

	writeAndReadCompleted := func(payload, responseID string) {
		writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
		err := clientConn.Write(writeCtx, coderws.MessageText, []byte(payload))
		cancelWrite()
		require.NoError(t, err)
		readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
		_, event, readErr := clientConn.Read(readCtx)
		cancelRead()
		require.NoError(t, readErr)
		require.Equal(t, "response.completed", gjson.GetBytes(event, "type").String())
		require.Equal(t, responseID, gjson.GetBytes(event, "response.id").String())
	}

	writeAndReadCompleted(
		`{"type":"response.create","model":"gpt-5.5","input":"safe first turn"}`,
		"resp_strict_text",
	)
	writeAndReadCompleted(
		`{"type":"response.create","model":"gpt-5.5","previous_response_id":"resp_strict_text","input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AA=="}]}]}`,
		"resp_strict_image",
	)

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, []byte(
		`{"type":"response.create","model":"gpt-5.5","previous_response_id":"resp_strict_text","input":[{"type":"function_call_output","call_id":"call_synthetic","output":"synthetic cyber policy marker"}]}`,
	))
	cancelWrite()
	require.NoError(t, err)
	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, policyEvent, err := clientConn.Read(readCtx)
	cancelRead()
	require.NoError(t, err)
	require.Equal(t, securityaudit.ErrorCodePolicyBlocked, gjson.GetBytes(policyEvent, "error.code").String())

	readCtx, cancelRead = context.WithTimeout(context.Background(), 3*time.Second)
	_, _, err = clientConn.Read(readCtx)
	cancelRead()
	var closeErr coderws.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, coderws.StatusCode(4403), closeErr.Code)
	require.Equal(t, securityaudit.ErrorCodePolicyBlocked, closeErr.Reason)

	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("strict websocket handler did not finish")
	}
	select {
	case upstreamErr := <-upstreamDone:
		require.NoError(t, upstreamErr)
	case <-time.After(2 * time.Second):
		t.Fatal("upstream websocket did not finish")
	}
	require.Len(t, upstreamPayloads, 2, "dangerous third turn must stop before upstream")
	require.Equal(t, int32(2), legacy.blockingCalls.Load(), "image turn must bypass the text-audit scope and Moderations")
	require.Equal(t, int32(2), legacy.checkCalls.Load(), "image turn must not call the text auditor")
	require.Equal(t, int32(1), lineage.binds.Load(), "image response must not create text lineage")
	cache.mu.Lock()
	responseBindingCount := 0
	for key, accountID := range cache.sessionBindings {
		if accountID == 1 && strings.HasPrefix(key, fmt.Sprintf("%d:openai:response:", groupID)) {
			responseBindingCount++
		}
	}
	cache.mu.Unlock()
	require.Equal(t, 2, responseBindingCount, "text and image responses must both bind to the producing account")
	_, err = lineage.Load(context.Background(), securityaudit.LineageLookup{
		APIKeyID: 99, GroupID: &groupID, PreviousResponseID: "resp_strict_image",
	})
	require.ErrorIs(t, err, securityaudit.ErrLineageNotFound)

	continuation, continuationRecorder := newOpenAIResponsesFailoverTestContext(t, nil)
	continuation.Set(string(middleware2.ContextKeyAPIKey), apiKey)
	continuation.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: user.ID, Concurrency: 0})
	continuation.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
		`{"model":"gpt-5.5","previous_response_id":"resp_strict_image","input":"text after image"}`,
	))
	continuation.Request.Header.Set("Content-Type", "application/json")
	handler.Responses(continuation)
	require.Equal(t, http.StatusForbidden, continuationRecorder.Code)
	require.Equal(t, securityaudit.ErrorCodeLineageIncompatible, gjson.GetBytes(continuationRecorder.Body.Bytes(), "error.code").String())
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
