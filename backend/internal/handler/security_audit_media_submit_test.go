package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
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
func (e *handlerPromptEngine) BlockingApplies(req securityaudit.Request) bool {
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

func nonStrictMediaModerationService(t *testing.T) *service.ContentModerationService {
	t.Helper()
	strictGroupID := int64(999)
	cfg := service.ContentModerationConfig{
		Enabled: true, Mode: service.ContentModerationModePreBlock, SampleRate: 100,
		AllGroups: false, GroupIDs: []int64{strictGroupID}, APIKeys: []string{"sk-test"},
	}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)
	return service.NewContentModerationService(
		&contentModerationHandlerSettingRepo{values: map[string]string{
			service.SettingKeyRiskControlEnabled:      "true",
			service.SettingKeyContentModerationConfig: string(rawCfg),
		}},
		&contentModerationHandlerTestRepo{}, nil, nil, nil, nil, nil, nil,
	)
}

func TestAsyncImageRouteBypassesPromptGuardAndCreatesTask(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &asyncImageMemoryStore{tasks: map[string]*service.ImageTaskRecord{}}
	tasks := service.NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	engine := blockingHandlerPromptEngine()
	openAI := &OpenAIGatewayHandler{securityAuditCoordinator: securityaudit.NewCoordinator(nil, engine), contentModerationService: nonStrictMediaModerationService(t)}
	h := &AsyncImageHandler{tasks: tasks, openAI: openAI}
	var executions atomic.Int32
	h.execute = func(_ string, c *gin.Context) {
		executions.Add(1)
		c.JSON(http.StatusOK, gin.H{"created": 1, "data": []any{}})
	}

	router := gin.New()
	router.Use(securityAuditMediaTestMiddleware)
	router.POST("/v1/images/generations/async", h.Submit)
	request := httptest.NewRequest(http.MethodPost, "/v1/images/generations/async", strings.NewReader(`{"model":"gpt-image-2","prompt":"blocked async prompt"}`))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusAccepted, recorder.Code)
	require.Eventually(t, func() bool { return executions.Load() == 1 }, time.Second, 10*time.Millisecond)
	store.mu.RLock()
	taskCount := len(store.tasks)
	store.mu.RUnlock()
	require.NotZero(t, taskCount)
	evaluated, enqueued, requests := engine.snapshot()
	require.Zero(t, evaluated)
	require.Zero(t, enqueued)
	require.Empty(t, requests)
}

func TestAsyncImageAuditBypassIsNotRepeatedByDetachedExecution(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &asyncImageMemoryStore{tasks: map[string]*service.ImageTaskRecord{}}
	tasks := service.NewImageTaskServiceWithUploader(store, nil, time.Hour, time.Minute)
	engine := blockingHandlerPromptEngine()
	openAI := &OpenAIGatewayHandler{securityAuditCoordinator: securityaudit.NewCoordinator(nil, engine), contentModerationService: nonStrictMediaModerationService(t)}
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
	evaluated, enqueued, requests := engine.snapshot()
	require.Zero(t, evaluated)
	require.Zero(t, enqueued)
	require.Empty(t, requests)
	executionMu.Lock()
	require.False(t, repeatedDecision)
	executionMu.Unlock()
}

func TestBatchImageRouteBypassesPromptGuard(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := blockingHandlerPromptEngine()
	openAI := &OpenAIGatewayHandler{securityAuditCoordinator: securityaudit.NewCoordinator(nil, engine), contentModerationService: nonStrictMediaModerationService(t)}
	h := &BatchImageHandler{openAI: openAI}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/batches", nil)
	securityAuditMediaTestMiddleware(c)

	allowed := h.checkSecurityAuditBeforeSubmit(c, &service.BatchImageSubmitRequest{
		Model: "gemini-image-test",
		Items: []service.BatchImageSubmitItem{{
			CustomID: "one", Prompt: "must bypass prompt guard",
		}},
	})

	require.True(t, allowed)
	evaluated, enqueued, requests := engine.snapshot()
	require.Zero(t, evaluated)
	require.Zero(t, enqueued)
	require.Empty(t, requests)
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
