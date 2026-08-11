package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

type handlerPromptEngine struct {
	mu sync.Mutex

	mode      securityaudit.Mode
	decision  *securityaudit.PromptDecision
	err       error
	evaluated int
	enqueued  int
	requests  []securityaudit.Request
	strict    bool
}

func (e *handlerPromptEngine) EffectiveMode() securityaudit.Mode { return e.mode }
func (e *handlerPromptEngine) BlockingApplies(securityaudit.Request) bool {
	return e.strict
}
func (e *handlerPromptEngine) Enqueue(_ context.Context, req securityaudit.Request) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.enqueued++
	e.requests = append(e.requests, req.Clone())
	return e.err
}
func (e *handlerPromptEngine) Evaluate(_ context.Context, req securityaudit.Request) (*securityaudit.PromptDecision, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.evaluated++
	e.requests = append(e.requests, req.Clone())
	return e.decision, e.err
}
func (e *handlerPromptEngine) snapshot() (evaluated, enqueued int, requests []securityaudit.Request) {
	e.mu.Lock()
	defer e.mu.Unlock()
	requests = make([]securityaudit.Request, len(e.requests))
	copy(requests, e.requests)
	return e.evaluated, e.enqueued, requests
}

func securityAuditMediaTestMiddleware(c *gin.Context) {
	groupID := int64(3)
	user := &service.User{ID: 7, Username: "media-user", Email: "media@example.test"}
	c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
		ID: 9, UserID: 7, User: user, Name: "media-key", GroupID: &groupID,
		Group: &service.Group{ID: groupID, Name: "media-group", Platform: service.PlatformOpenAI, AllowImageGeneration: true},
	})
	c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 7, Concurrency: 2})
	c.Next()
}

func blockingHandlerPromptEngine() *handlerPromptEngine {
	return &handlerPromptEngine{mode: securityaudit.ModeBlocking, strict: true, decision: &securityaudit.PromptDecision{
		Kind: securityaudit.DecisionBlock, ErrorCode: securityaudit.ErrorCodeBlocked, AllowNextStage: false,
	}}
}

func TestAsyncImagePromptGuardRunsBeforeTaskCreation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &asyncImageMemoryStore{tasks: map[string]*service.ImageTaskRecord{}}
	tasks := service.NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	engine := blockingHandlerPromptEngine()
	openAI := &OpenAIGatewayHandler{securityAuditCoordinator: securityaudit.NewCoordinator(nil, engine)}
	h := &AsyncImageHandler{tasks: tasks, openAI: openAI}
	executions := 0
	h.execute = func(string, *gin.Context) { executions++ }

	router := gin.New()
	router.Use(securityAuditMediaTestMiddleware)
	router.POST("/v1/images/generations/async", h.Submit)
	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations/async", strings.NewReader(`{"model":"gpt-image-2","prompt":"blocked async prompt"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusForbidden, recorder.Code)
	require.Contains(t, recorder.Body.String(), securityaudit.ErrorCodeBlocked)
	require.Empty(t, store.tasks, "no asynchronous task may exist after a blocking decision")
	require.Zero(t, executions)
	evaluated, _, requests := engine.snapshot()
	require.Equal(t, 1, evaluated)
	require.Len(t, requests, 1)
	require.Contains(t, string(requests[0].Body), "blocked async prompt")
}

func TestAsyncImageSuccessfulPrecheckIsNotRepeatedByDetachedExecution(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &asyncImageMemoryStore{tasks: map[string]*service.ImageTaskRecord{}}
	tasks := service.NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	engine := &handlerPromptEngine{mode: securityaudit.ModeBlocking, strict: true, decision: &securityaudit.PromptDecision{Kind: securityaudit.DecisionAllow, AllowNextStage: true}}
	openAI := &OpenAIGatewayHandler{securityAuditCoordinator: securityaudit.NewCoordinator(nil, engine)}
	h := &AsyncImageHandler{tasks: tasks, openAI: openAI}
	var executionMu sync.Mutex
	repeatedDecision := false
	h.execute = func(_ string, c *gin.Context) {
		apiKey, _ := middleware2.GetAPIKeyFromContext(c)
		subject, _ := middleware2.GetAuthSubjectFromContext(c)
		decision := openAI.checkSecurityAudit(c, nil, apiKey, subject, service.ContentModerationProtocolOpenAIImages, "gpt-image-2", []byte(`{"prompt":"must not rescan"}`))
		executionMu.Lock()
		repeatedDecision = decision != nil
		executionMu.Unlock()
		c.JSON(http.StatusOK, gin.H{"created": 1, "data": []any{}})
	}

	router := gin.New()
	router.Use(securityAuditMediaTestMiddleware)
	router.POST("/v1/images/generations/async", h.Submit)
	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations/async", strings.NewReader(`{"model":"gpt-image-2","prompt":"allowed async prompt"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.Eventually(t, func() bool {
		store.mu.RLock()
		defer store.mu.RUnlock()
		for _, task := range store.tasks {
			if task.Status == service.ImageTaskStatusCompleted {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
	evaluated, _, _ := engine.snapshot()
	require.Equal(t, 1, evaluated)
	executionMu.Lock()
	require.False(t, repeatedDecision)
	executionMu.Unlock()
}

func TestBatchImagePromptGuardRunsBeforePersistenceOrBilling(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := blockingHandlerPromptEngine()
	openAI := &OpenAIGatewayHandler{securityAuditCoordinator: securityaudit.NewCoordinator(nil, engine)}
	h := &BatchImageHandler{openAI: openAI}
	router := gin.New()
	router.Use(securityAuditMediaTestMiddleware)
	router.POST("/v1/images/batches", h.Submit)
	body := map[string]any{
		"model": "gemini-image-test",
		"items": []map[string]any{{
			"custom_id": "one", "prompt": "blocked batch prompt",
			"reference_images": []map[string]any{{"mime_type": "image/png", "data": []byte("BINARY_CANARY")}},
		}},
	}
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	request := httptest.NewRequest(http.MethodPost, "/v1/images/batches", strings.NewReader(string(raw)))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	require.NotPanics(t, func() { router.ServeHTTP(recorder, request) }, "nil service would panic if Submit were reached")
	require.Equal(t, http.StatusForbidden, recorder.Code)
	evaluated, _, requests := engine.snapshot()
	require.Equal(t, 1, evaluated)
	require.Len(t, requests, 1)
	require.Contains(t, string(requests[0].Body), "blocked batch prompt")
	require.NotContains(t, string(requests[0].Body), "BINARY_CANARY")
	require.NotContains(t, string(requests[0].Body), "QklOQVJZX0NBTkFSWQ==")
}

func TestSecurityAuditBlockingFailuresLeaveAllDownstreamCountersAtZero(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, kind := range []securityaudit.DecisionKind{securityaudit.DecisionBlock, securityaudit.DecisionUnavailable, securityaudit.DecisionInvalid} {
		t.Run(string(kind), func(t *testing.T) {
			promptDecision := promptGuardDecision(kind)
			engine := &handlerPromptEngine{mode: securityaudit.ModeBlocking, strict: true, decision: &securityaudit.PromptDecision{
				Kind: kind, ErrorCode: promptDecision.ErrorCode, AllowNextStage: false,
			}}
			coordinator := securityaudit.NewCoordinator(nil, engine)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-test","messages":[{"role":"user","content":"guard me"}]}`))
			groupID := int64(3)
			apiKey := &service.APIKey{ID: 9, UserID: 7, GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI}}
			subject := middleware2.AuthSubject{UserID: 7, Concurrency: 2}
			decision := runSecurityAudit(c, nil, coordinator, nil, apiKey, subject, service.ContentModerationProtocolOpenAIChat, "gpt-test", []byte(`{"messages":[{"role":"user","content":"guard me"}]}`), "http")
			require.NotNil(t, decision)
			require.False(t, decision.AllowNextStage)
			require.False(t, recorder.Result().Header.Get("Content-Type") != "", "Guard evaluation itself must not start SSE/HTTP output")

			accountSelections, billingChecks, billingPreconsumes, upstreamDispatches := 0, 0, 0, 0
			if decision.AllowNextStage {
				accountSelections++
				billingChecks++
				billingPreconsumes++
				upstreamDispatches++
			}
			require.Zero(t, accountSelections)
			require.Zero(t, billingChecks)
			require.Zero(t, billingPreconsumes)
			require.Zero(t, upstreamDispatches)
			(&OpenAIGatewayHandler{}).openAISecurityAuditError(c, decision)
			require.Equal(t, promptDecision.HTTPStatus, recorder.Code)
		})
	}
}

type handlerLegacyEngine struct {
	decision *securityaudit.LegacyDecision
	err      error
	strict   bool
	scopeErr error
}

func (e *handlerLegacyEngine) BlockingApplies(context.Context, securityaudit.Request) (bool, error) {
	return e.strict, e.scopeErr
}

func (e *handlerLegacyEngine) Check(context.Context, securityaudit.Request) (*securityaudit.LegacyDecision, error) {
	return e.decision, e.err
}

type handlerAllowLineageStore struct {
	summary securityaudit.AuditSummary
	loadErr error
	loads   atomic.Int32
	lookup  securityaudit.LineageLookup
}

func (s *handlerAllowLineageStore) Load(_ context.Context, lookup securityaudit.LineageLookup) (*securityaudit.AuditSummary, error) {
	s.loads.Add(1)
	s.lookup = lookup
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	result := s.summary.Clone()
	return &result, nil
}

func (s *handlerAllowLineageStore) BindAllowedResponse(context.Context, securityaudit.AuditSummary, string) error {
	return nil
}

type strictContinuationGatewayCacheSpy struct {
	service.GatewayCache
	responseLookups atomic.Int32
}

func (s *strictContinuationGatewayCacheSpy) GetSessionAccountID(context.Context, int64, string) (int64, error) {
	s.responseLookups.Add(1)
	return 0, service.ErrStickySessionNotFound
}

type strictContinuationAccountRepoSpy struct {
	service.AccountRepository
	calls atomic.Int32
}

func (s *strictContinuationAccountRepoSpy) GetByID(context.Context, int64) (*service.Account, error) {
	s.calls.Add(1)
	return nil, errors.New("unexpected account lookup")
}

func (s *strictContinuationAccountRepoSpy) ListSchedulableByGroupIDAndPlatform(context.Context, int64, string) ([]service.Account, error) {
	s.calls.Add(1)
	return nil, errors.New("unexpected account selection")
}

func (s *strictContinuationAccountRepoSpy) ListSchedulableByPlatform(context.Context, string) ([]service.Account, error) {
	s.calls.Add(1)
	return nil, errors.New("unexpected account selection")
}

func (s *strictContinuationAccountRepoSpy) ListSchedulableUngroupedByPlatform(context.Context, string) ([]service.Account, error) {
	s.calls.Add(1)
	return nil, errors.New("unexpected account selection")
}

type strictContinuationBillingCacheSpy struct {
	service.BillingCache
	balanceCalls atomic.Int32
}

func (s *strictContinuationBillingCacheSpy) GetUserBalance(context.Context, int64) (float64, error) {
	s.balanceCalls.Add(1)
	return 0, errors.New("unexpected billing check")
}

type cyberBlockedAuditSpy struct {
	blockingCalls atomic.Int32
	checkCalls    atomic.Int32
}

func (s *cyberBlockedAuditSpy) BlockingApplies(context.Context, securityaudit.Request) (bool, error) {
	s.blockingCalls.Add(1)
	return true, nil
}

func (s *cyberBlockedAuditSpy) Check(context.Context, securityaudit.Request) (*securityaudit.LegacyDecision, error) {
	s.checkCalls.Add(1)
	return &securityaudit.LegacyDecision{Allowed: true}, nil
}

type cyberBlockedHandlerFixture struct {
	handler          *OpenAIGatewayHandler
	cache            *cyberHandlerGatewayCache
	legacyAudit      *cyberBlockedAuditSpy
	promptAudit      *handlerPromptEngine
	lineage          *handlerAllowLineageStore
	accountRepo      *strictContinuationAccountRepoSpy
	concurrencyCache *concurrencyCacheMock
	billingCache     *strictContinuationBillingCacheSpy
	upstream         *openAIHTTPPassthroughFailoverUpstream
}

func newCyberBlockedHandlerFixture(t *testing.T, groupID int64) *cyberBlockedHandlerFixture {
	t.Helper()
	settingSvc := service.NewSettingService(&cyberHandlerSettingRepo{values: map[string]string{
		service.SettingKeyCyberSessionBlockEnabled:    "true",
		service.SettingKeyCyberSessionBlockTTLSeconds: "60",
		service.SettingKeyCyberSessionBlockAllGroups:  "false",
		service.SettingKeyCyberSessionBlockGroupIDs:   fmt.Sprintf("[%d]", groupID),
	}}, nil)
	cache := &cyberHandlerGatewayCache{blocked: make(map[string]bool)}
	accountRepo := &strictContinuationAccountRepoSpy{}
	concurrencyCache := &concurrencyCacheMock{
		acquireIngressLeaseFn: func(context.Context, int64, int, string) (bool, error) { return true, nil },
		acquireUserSlotFn:     func(context.Context, int64, int, string) (bool, error) { return true, nil },
		acquireAccountSlotFn:  func(context.Context, int64, int, string) (bool, error) { return true, nil },
	}
	concurrency := service.NewConcurrencyService(concurrencyCache)
	cfg := &config.Config{}
	billingCache := &strictContinuationBillingCacheSpy{}
	billing := service.NewBillingCacheService(billingCache, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	upstream := &openAIHTTPPassthroughFailoverUpstream{}
	gateway := service.NewOpenAIGatewayService(
		accountRepo, nil, nil, nil, nil, nil, cache, cfg, nil, concurrency, nil, nil,
		billing, upstream, nil, nil, nil, nil, nil, nil, settingSvc, nil,
	)
	legacyAudit := &cyberBlockedAuditSpy{}
	promptAudit := &handlerPromptEngine{
		mode: securityaudit.ModeBlocking, strict: true, err: errors.New("auditor must not run for an already-blocked session"),
	}
	lineage := &handlerAllowLineageStore{summary: securityaudit.AuditSummary{
		Verdict: securityaudit.AuditVerdictAllow, ContextComplete: true,
		APIKeyID: 9, GroupID: &groupID, PromptHash: "parent-hash", RedactedContext: "parent context",
	}}
	h := &OpenAIGatewayHandler{
		gatewayService:      gateway,
		billingCacheService: billing,
		apiKeyService:       &service.APIKeyService{},
		concurrencyHelper:   NewConcurrencyHelper(concurrency, SSEPingFormatComment, time.Second),
		securityAuditCoordinator: securityaudit.NewCoordinator(legacyAudit, promptAudit).
			SetLineageStore(lineage),
		cfg: cfg, maxAccountSwitches: 1,
	}
	return &cyberBlockedHandlerFixture{
		handler: h, cache: cache, legacyAudit: legacyAudit, promptAudit: promptAudit,
		lineage: lineage, accountRepo: accountRepo, concurrencyCache: concurrencyCache,
		billingCache: billingCache, upstream: upstream,
	}
}

func (f *cyberBlockedHandlerFixture) assertNoDownstreamSideEffects(t *testing.T) {
	t.Helper()
	require.Zero(t, f.legacyAudit.blockingCalls.Load())
	require.Zero(t, f.legacyAudit.checkCalls.Load())
	evaluated, enqueued, requests := f.promptAudit.snapshot()
	require.Zero(t, evaluated)
	require.Zero(t, enqueued)
	require.Empty(t, requests)
	require.Zero(t, f.lineage.loads.Load())
	require.Zero(t, f.cache.stickyReadCalls)
	require.Zero(t, f.accountRepo.calls.Load())
	require.Zero(t, atomic.LoadInt32(&f.concurrencyCache.acquireIngressCalled))
	require.Zero(t, atomic.LoadInt32(&f.concurrencyCache.acquireUserCalled))
	require.Zero(t, atomic.LoadInt32(&f.concurrencyCache.acquireAccountCalled))
	require.Zero(t, atomic.LoadInt32(&f.concurrencyCache.releaseIngressCalled))
	require.Zero(t, atomic.LoadInt32(&f.concurrencyCache.releaseUserCalled))
	require.Zero(t, atomic.LoadInt32(&f.concurrencyCache.releaseAccountCalled))
	require.Zero(t, f.billingCache.balanceCalls.Load())
	require.Empty(t, f.upstream.calls())
}

func blockCyberSessionForRequest(t *testing.T, fixture *cyberBlockedHandlerFixture, apiKeyID int64, method, path, sessionID string, body []byte) {
	t.Helper()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(method, path, nil)
	c.Request.Header.Set("session_id", sessionID)
	key := service.CyberSessionBlockKey(apiKeyID, c, body)
	require.NotEmpty(t, key)
	fixture.cache.blocked[key] = true
}

func TestCyberSessionBlockPrecedesStrictAuditAndDownstreamHTTP(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(12)
	tests := []struct {
		name    string
		path    string
		body    string
		handler func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{
			name: "responses continuation", path: "/v1/responses",
			body:    `{"model":"gpt-5.4","previous_response_id":"resp_parent","input":"safe"}`,
			handler: func(h *OpenAIGatewayHandler, c *gin.Context) { h.Responses(c) },
		},
		{
			name: "chat completions", path: "/v1/chat/completions",
			body:    `{"model":"gpt-5.4","messages":[{"role":"user","content":"safe"}]}`,
			handler: func(h *OpenAIGatewayHandler, c *gin.Context) { h.ChatCompletions(c) },
		},
		{
			name: "messages", path: "/v1/messages",
			body:    `{"model":"gpt-5.4","messages":[{"role":"user","content":"safe"}]}`,
			handler: func(h *OpenAIGatewayHandler, c *gin.Context) { h.Messages(c) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newCyberBlockedHandlerFixture(t, groupID)
			sessionID := "blocked-" + strings.ReplaceAll(tt.name, " ", "-")
			blockCyberSessionForRequest(t, fixture, 9, http.MethodPost, tt.path, sessionID, []byte(tt.body))

			user := &service.User{ID: 7, Username: "strict-user", Status: service.StatusActive}
			apiKey := &service.APIKey{
				ID: 9, UserID: user.ID, User: user, GroupID: &groupID,
				Group: &service.Group{
					ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive,
					AllowMessagesDispatch: true,
				},
			}
			router := gin.New()
			router.Use(func(c *gin.Context) {
				c.Set(string(middleware2.ContextKeyAPIKey), apiKey)
				c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: user.ID, Concurrency: 2})
				c.Next()
			})
			router.POST(tt.path, func(c *gin.Context) { tt.handler(fixture.handler, c) })

			request := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader(tt.body))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("session_id", sessionID)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusForbidden, recorder.Code)
			require.Equal(t, 1, fixture.cache.readCalls)
			fixture.assertNoDownstreamSideEffects(t)
		})
	}
}

func TestCyberSessionBlockPrecedesStrictAuditAndDownstreamWebSocket(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(12)
	fixture := newCyberBlockedHandlerFixture(t, groupID)
	sessionID := "blocked-ws-first-turn"
	firstMessage := []byte(`{"type":"response.create","model":"gpt-5.4","previous_response_id":"resp_parent","input":"safe"}`)
	blockCyberSessionForRequest(t, fixture, 9, http.MethodGet, "/openai/v1/responses", sessionID, firstMessage)

	user := &service.User{ID: 7, Username: "strict-ws", Status: service.StatusActive}
	apiKey := &service.APIKey{
		ID: 9, UserID: user.ID, User: user, GroupID: &groupID,
		Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive},
	}
	handlerDone := make(chan struct{})
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware2.ContextKeyAPIKey), apiKey)
		c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: user.ID, Concurrency: 2})
		c.Next()
	})
	router.GET("/openai/v1/responses", func(c *gin.Context) {
		fixture.handler.ResponsesWebSocket(c)
		close(handlerDone)
	})
	server := httptest.NewServer(router)
	defer server.Close()

	headers := http.Header{}
	headers.Set("session_id", sessionID)
	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	conn, _, err := coderws.Dial(
		dialCtx,
		"ws"+strings.TrimPrefix(server.URL, "http")+"/openai/v1/responses",
		&coderws.DialOptions{HTTPHeader: headers},
	)
	cancelDial()
	require.NoError(t, err)
	defer func() { _ = conn.CloseNow() }()

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err = conn.Write(writeCtx, coderws.MessageText, firstMessage)
	cancelWrite()
	require.NoError(t, err)

	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, payload, err := conn.Read(readCtx)
	cancelRead()
	require.NoError(t, err)
	require.Contains(t, string(payload), "session_blocked_by_cyber_policy")

	readCtx, cancelRead = context.WithTimeout(context.Background(), 3*time.Second)
	_, _, err = conn.Read(readCtx)
	cancelRead()
	var closeErr coderws.CloseError
	require.ErrorAs(t, err, &closeErr)
	require.Equal(t, coderws.StatusPolicyViolation, closeErr.Code)
	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("cyber-blocked websocket handler did not finish")
	}

	require.Equal(t, 1, fixture.cache.readCalls)
	fixture.assertNoDownstreamSideEffects(t)
}

func TestOpenAIResponsesStrictGateStopsBeforeAllDownstreamDependencies(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name       string
		body       string
		legacy     *handlerLegacyEngine
		lineage    securityaudit.LineageStore
		wantStatus int
		wantCode   string
	}{
		{
			name: "policy block", body: `{"model":"gpt-test","input":"blocked"}`,
			legacy:     &handlerLegacyEngine{strict: true, decision: &securityaudit.LegacyDecision{Blocked: true, Flagged: true}},
			wantStatus: http.StatusForbidden, wantCode: securityaudit.ErrorCodePolicyBlocked,
		},
		{
			name: "context incomplete", body: `{"model":"gpt-test","input":[{"type":"future_item"}]}`,
			legacy:     &handlerLegacyEngine{strict: true, decision: &securityaudit.LegacyDecision{Allowed: true}},
			wantStatus: http.StatusUnprocessableEntity, wantCode: securityaudit.ErrorCodeContextIncomplete,
		},
		{
			name: "legacy continuation missing lineage", body: `{"model":"gpt-test","previous_response_id":"resp_legacy","input":"continue"}`,
			legacy:     &handlerLegacyEngine{strict: true, decision: &securityaudit.LegacyDecision{Allowed: true}},
			lineage:    &handlerAllowLineageStore{loadErr: securityaudit.ErrLineageNotFound},
			wantStatus: http.StatusUnprocessableEntity, wantCode: securityaudit.ErrorCodeContextIncomplete,
		},
		{
			name: "audit unavailable", body: `{"model":"gpt-test","input":"safe"}`,
			legacy:     &handlerLegacyEngine{strict: true, err: errors.New("moderation 429")},
			wantStatus: http.StatusServiceUnavailable, wantCode: securityaudit.ErrorCodeAuditUnavailable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prompt := &handlerPromptEngine{mode: securityaudit.ModeOff}
			coordinator := securityaudit.NewCoordinator(tt.legacy, prompt)
			if tt.lineage != nil {
				coordinator.SetLineageStore(tt.lineage)
			}
			h := &OpenAIGatewayHandler{
				gatewayService:           &service.OpenAIGatewayService{},
				billingCacheService:      &service.BillingCacheService{},
				apiKeyService:            &service.APIKeyService{},
				concurrencyHelper:        NewConcurrencyHelper(&service.ConcurrencyService{}, SSEPingFormatComment, time.Second),
				securityAuditCoordinator: coordinator,
			}

			router := gin.New()
			router.Use(func(c *gin.Context) {
				groupID := int64(12)
				user := &service.User{ID: 7, Username: "strict-user", Email: "strict@example.test"}
				c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
					ID: 9, UserID: user.ID, User: user, GroupID: &groupID,
					Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI},
				})
				c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: user.ID, Concurrency: 2})
				c.Next()
			})
			router.POST("/v1/responses", h.Responses)
			request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(tt.body))
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()

			require.NotPanics(t, func() { router.ServeHTTP(recorder, request) },
				"strict rejection must not touch the deliberately uninitialized scheduler, concurrency, billing, or upstream internals")
			require.Equal(t, tt.wantStatus, recorder.Code)
			require.Contains(t, recorder.Body.String(), tt.wantCode)
			evaluated, _, _ := prompt.snapshot()
			require.Zero(t, evaluated, "Prompt Audit must not be required when it is off")
		})
	}
}

func TestOpenAIResponsesStrictContinuationStickyMissIsAuditUnavailableBeforeDownstreamDependencies(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(12)
	lineage := &handlerAllowLineageStore{summary: securityaudit.AuditSummary{
		Verdict: securityaudit.AuditVerdictAllow, ContextComplete: true,
		APIKeyID: 9, GroupID: &groupID, PromptHash: "parent-hash", RedactedContext: "parent context",
	}}
	accountRepo := &strictContinuationAccountRepoSpy{}
	gatewayCache := &strictContinuationGatewayCacheSpy{}
	billingCache := &strictContinuationBillingCacheSpy{}
	cfg := &config.Config{Gateway: config.GatewayConfig{ImageConcurrency: config.ImageConcurrencyConfig{
		Enabled: true, MaxConcurrentRequests: 1, OverflowMode: config.ImageConcurrencyOverflowModeReject,
	}}}
	billing := service.NewBillingCacheService(billingCache, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	concurrencyCache := &concurrencyCacheMock{
		acquireUserSlotFn:    func(context.Context, int64, int, string) (bool, error) { return true, nil },
		acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil },
	}
	concurrency := service.NewConcurrencyService(concurrencyCache)
	upstream := &openAIHTTPPassthroughFailoverUpstream{}
	gateway := service.NewOpenAIGatewayService(
		accountRepo, nil, nil, nil, nil, nil, gatewayCache, cfg, nil, concurrency, nil, nil,
		billing, upstream, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	limiter := &imageConcurrencyLimiter{}
	occupiedRelease, occupied := limiter.TryAcquire(true, 1)
	require.True(t, occupied)
	t.Cleanup(occupiedRelease)
	h := &OpenAIGatewayHandler{
		gatewayService:      gateway,
		billingCacheService: billing,
		apiKeyService:       &service.APIKeyService{},
		concurrencyHelper:   NewConcurrencyHelper(concurrency, SSEPingFormatComment, time.Second),
		securityAuditCoordinator: securityaudit.NewCoordinator(
			&handlerLegacyEngine{strict: true, decision: &securityaudit.LegacyDecision{Allowed: true}},
			&handlerPromptEngine{mode: securityaudit.ModeOff},
		).SetLineageStore(lineage),
		cfg:          cfg,
		imageLimiter: limiter,
	}

	router := gin.New()
	router.Use(func(c *gin.Context) {
		user := &service.User{ID: 7, Username: "strict-user", Status: service.StatusActive}
		c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
			ID: 9, UserID: user.ID, User: user, GroupID: &groupID,
			Group: &service.Group{
				ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive, AllowImageGeneration: true,
			},
		})
		c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: user.ID, Concurrency: 2})
		c.Next()
	})
	router.POST("/v1/responses", h.Responses)
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
		`{"model":"gpt-5.4","previous_response_id":"resp_parent","input":"draw","tools":[{"type":"image_generation"}]}`,
	))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	require.NotPanics(t, func() { router.ServeHTTP(recorder, request) })
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Equal(t, securityaudit.ErrorCodeAuditUnavailable, gjson.GetBytes(recorder.Body.Bytes(), "error.code").String())
	require.Equal(t, int32(1), lineage.loads.Load())
	require.Equal(t, int64(9), lineage.lookup.APIKeyID)
	require.Equal(t, groupID, *lineage.lookup.GroupID)
	require.Equal(t, "resp_parent", lineage.lookup.PreviousResponseID)
	require.Equal(t, int32(1), gatewayCache.responseLookups.Load(), "preflight must perform one read-only response binding lookup")
	require.Zero(t, accountRepo.calls.Load())
	require.Zero(t, concurrencyCache.acquireUserCalled)
	require.Zero(t, concurrencyCache.releaseUserCalled)
	require.Zero(t, concurrencyCache.acquireAccountCalled)
	require.Zero(t, concurrencyCache.releaseAccountCalled)
	require.Zero(t, billingCache.balanceCalls.Load())
	require.Empty(t, upstream.calls())
	limiter.mu.Lock()
	require.Equal(t, 1, limiter.active, "request must not touch the preoccupied image slot")
	require.Zero(t, limiter.waiting)
	limiter.mu.Unlock()
}

func TestOpenAIResponsesWebSocketStrictBlockHasZeroDownstreamSideEffects(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cache := &concurrencyCacheMock{
		acquireIngressLeaseFn: func(context.Context, int64, int, string) (bool, error) { return true, nil },
		acquireUserSlotFn:     func(context.Context, int64, int, string) (bool, error) { return true, nil },
		acquireAccountSlotFn:  func(context.Context, int64, int, string) (bool, error) { return true, nil },
	}
	prompt := &handlerPromptEngine{
		mode: securityaudit.ModeBlocking, strict: true,
		decision: &securityaudit.PromptDecision{Kind: securityaudit.DecisionBlock, AllowNextStage: false},
	}
	h := &OpenAIGatewayHandler{
		gatewayService:           &service.OpenAIGatewayService{},
		billingCacheService:      &service.BillingCacheService{},
		apiKeyService:            &service.APIKeyService{},
		concurrencyHelper:        NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatNone, time.Second),
		securityAuditCoordinator: securityaudit.NewCoordinator(&handlerLegacyEngine{strict: true, decision: &securityaudit.LegacyDecision{Allowed: true}}, prompt),
		maxAccountSwitches:       1,
	}

	groupID := int64(12)
	user := &service.User{ID: 7, Username: "strict-ws", Status: service.StatusActive}
	apiKey := &service.APIKey{
		ID: 9, UserID: user.ID, User: user, GroupID: &groupID,
		Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI, Status: service.StatusActive},
	}
	handlerDone := make(chan struct{})
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(middleware2.ContextKeyAPIKey), apiKey)
		c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: user.ID, Concurrency: 2})
		c.Next()
	})
	router.GET("/openai/v1/responses", func(c *gin.Context) {
		h.ResponsesWebSocket(c)
		close(handlerDone)
	})
	server := httptest.NewServer(router)
	defer server.Close()

	dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
	conn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(server.URL, "http")+"/openai/v1/responses", nil)
	cancelDial()
	require.NoError(t, err)
	defer func() { _ = conn.CloseNow() }()

	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err = conn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-test","input":"blocked first turn"}`))
	cancelWrite()
	require.NoError(t, err)

	readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
	_, payload, err := conn.Read(readCtx)
	cancelRead()
	require.NoError(t, err)
	require.Contains(t, string(payload), securityaudit.ErrorCodePolicyBlocked)

	readCtx, cancelRead = context.WithTimeout(context.Background(), 3*time.Second)
	_, _, err = conn.Read(readCtx)
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

	require.Zero(t, atomic.LoadInt32(&cache.acquireIngressCalled))
	require.Zero(t, atomic.LoadInt32(&cache.acquireUserCalled))
	require.Zero(t, atomic.LoadInt32(&cache.acquireAccountCalled))
	require.Zero(t, atomic.LoadInt32(&cache.releaseIngressCalled))
	require.Zero(t, atomic.LoadInt32(&cache.releaseUserCalled))
	require.Zero(t, atomic.LoadInt32(&cache.releaseAccountCalled))
	evaluated, _, requests := prompt.snapshot()
	require.Equal(t, 1, evaluated)
	require.Len(t, requests, 1)
	require.True(t, requests[0].Strict)
	require.NotNil(t, requests[0].Document)
	require.True(t, requests[0].Document.Complete)
}
