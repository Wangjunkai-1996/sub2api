package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestCachesSecurityAuditCompletionSkipsWebSocketStages(t *testing.T) {
	require.True(t, cachesSecurityAuditCompletion("http"))
	require.True(t, cachesSecurityAuditCompletion(""))
	require.False(t, cachesSecurityAuditCompletion("first_turn"))
	require.False(t, cachesSecurityAuditCompletion("subsequent_turn"))
	require.True(t, isSecurityAuditWebSocketStage("first_turn"))
	require.True(t, isSecurityAuditWebSocketStage("subsequent_turn"))
	require.False(t, isSecurityAuditWebSocketStage("http"))
}

func TestBlockingAuditFailsClosedWithoutCoordinator(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Set(securityAuditCompletedContextKey, true)

	decision := runSecurityAuditWithAdmission(
		c,
		nil,
		nil,
		nil,
		nil,
		middleware2.AuthSubject{UserID: 7},
		service.ContentModerationProtocolOpenAIResponses,
		"gpt-test",
		[]byte(`{"input":"must be scanned"}`),
		"http",
		nil,
		true,
	)

	require.NotNil(t, decision)
	require.Equal(t, securityaudit.DecisionUnavailable, decision.Kind)
	require.Equal(t, securityaudit.ErrorCodeUnavailable, decision.ErrorCode)
	require.Equal(t, http.StatusServiceUnavailable, decision.HTTPStatus)
	require.False(t, decision.AllowNextStage)
}

func TestBlockingAuditDoesNotReuseNonBlockingCompletion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := &turnCountingEngine{mode: securityaudit.ModeAsync}
	coordinator := securityaudit.NewCoordinator(nil, engine)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	body := []byte(`{"input":"must reach blocking scanner"}`)

	nonBlocking := runSecurityAudit(
		c,
		nil,
		coordinator,
		nil,
		nil,
		middleware2.AuthSubject{UserID: 7},
		service.ContentModerationProtocolOpenAIResponses,
		"gpt-test",
		body,
		"http",
	)
	require.NotNil(t, nonBlocking)
	require.True(t, nonBlocking.AllowNextStage)

	blocking := runSecurityAuditWithAdmission(
		c,
		nil,
		coordinator,
		nil,
		nil,
		middleware2.AuthSubject{UserID: 7},
		service.ContentModerationProtocolOpenAIResponses,
		"gpt-test",
		body,
		"http",
		nil,
		true,
	)
	require.NotNil(t, blocking)
	require.Equal(t, securityaudit.DecisionUnavailable, blocking.Kind)
	require.False(t, blocking.AllowNextStage)
	require.Equal(t, int64(1), engine.enqueues.Load(), "the non-blocking path may enqueue once")
	require.Zero(t, engine.evaluates.Load(), "async mode must never be treated as a completed blocking scan")
}

func TestSecurityAdmissionDoesNotReuseEqualLengthDifferentBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	protocol := service.ContentModerationProtocolOpenAIResponses
	firstBody := []byte(`{"input":"safe"}`)
	secondBody := []byte(`{"input":"evil"}`)
	require.Len(t, secondBody, len(firstBody))

	firstState, err := classifyOpenAISecurityAdmission(protocol, firstBody, "untrusted")
	require.NoError(t, err)
	installOpenAISecurityAdmission(c, firstState)
	require.True(t, firstState.matchesBody(protocol, firstBody))
	require.False(t, firstState.matchesBody(protocol, secondBody))
	require.Nil(t, buildSecurityAuditRequest(
		c, nil, middleware2.AuthSubject{UserID: 7}, protocol, "gpt-test", secondBody, "subsequent_turn",
	).Admission, "equal length must not reuse offsets from another allocation")

	account := &service.Account{
		Platform: service.PlatformOpenAI,
		Type:     service.AccountTypeOAuth,
		Credentials: map[string]any{
			"plan_type": "pro",
		},
	}
	h := &OpenAIGatewayHandler{
		securityAuditCoordinator: securityaudit.NewCoordinator(nil, &turnCountingEngine{mode: securityaudit.ModeBlocking}),
	}
	c.Set(securityAuditWSTurnContextKey, 2)
	decision := h.checkSecurityAuditForSelectedOpenAIAccount(
		c, nil, nil, middleware2.AuthSubject{UserID: 7}, account,
		protocol, "gpt-test", secondBody, true,
	)
	require.NotNil(t, decision)
	require.True(t, decision.AllowNextStage)

	secondState := openAISecurityAdmissionFromContext(c)
	require.NotSame(t, firstState, secondState)
	require.True(t, secondState.matchesBody(protocol, secondBody))
	materialized, err := secondState.admission.MaterializeText(secondBody)
	require.NoError(t, err)
	require.Contains(t, materialized, "evil")
	require.NotContains(t, materialized, "safe")
}

func TestSelectedOpenAIProAccountAuditEligibility(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name    string
		account *service.Account
	}{
		{name: "nil account"},
		{name: "OpenAI API key", account: &service.Account{
			Platform: service.PlatformOpenAI,
			Type:     service.AccountTypeAPIKey,
			Credentials: map[string]any{
				"plan_type": "pro",
			},
		}},
		{name: "OpenAI OAuth Plus", account: &service.Account{
			Platform: service.PlatformOpenAI,
			Type:     service.AccountTypeOAuth,
			Credentials: map[string]any{
				"plan_type": "plus",
			},
		}},
		{name: "OpenAI OAuth Free", account: &service.Account{
			Platform: service.PlatformOpenAI,
			Type:     service.AccountTypeOAuth,
			Credentials: map[string]any{
				"plan_type": "free",
			},
		}},
		{name: "non-OpenAI OAuth Pro", account: &service.Account{
			Platform: service.PlatformGrok,
			Type:     service.AccountTypeOAuth,
			Credentials: map[string]any{
				"plan_type": "pro",
			},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := &turnCountingEngine{mode: securityaudit.ModeBlocking}
			h := &OpenAIGatewayHandler{securityAuditCoordinator: securityaudit.NewCoordinator(nil, engine)}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

			decision := h.checkSecurityAuditForSelectedOpenAIProAccount(
				c,
				nil,
				nil,
				middleware2.AuthSubject{UserID: 7},
				tt.account,
				service.ContentModerationProtocolOpenAIResponses,
				"gpt-test",
				[]byte(`{"input":"hello"}`),
			)

			require.Nil(t, decision)
			require.Zero(t, engine.evaluates.Load(), "ineligible accounts must stay on the in-memory fast path")
		})
	}
}

func TestSelectedOpenAIProAccountAuditCachesSuccessfulRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := &turnCountingEngine{mode: securityaudit.ModeBlocking}
	h := &OpenAIGatewayHandler{securityAuditCoordinator: securityaudit.NewCoordinator(nil, engine)}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	account := &service.Account{
		Platform: service.PlatformOpenAI,
		Type:     service.AccountTypeOAuth,
		Credentials: map[string]any{
			"plan_type": "pro",
		},
	}
	check := func() *securityaudit.Decision {
		return h.checkSecurityAuditForSelectedOpenAIProAccount(
			c,
			nil,
			nil,
			middleware2.AuthSubject{UserID: 7},
			account,
			service.ContentModerationProtocolOpenAIResponses,
			"gpt-test",
			[]byte(`{"input":"hello"}`),
		)
	}

	first := check()
	second := check()

	require.NotNil(t, first)
	require.True(t, first.AllowNextStage)
	require.Nil(t, second, "a successful HTTP audit must be reused during failover")
	require.Equal(t, int64(1), engine.evaluates.Load())
}

func TestSelectedOpenAIProAccountAuditStartsAfterIneligibleFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := &turnCountingEngine{mode: securityaudit.ModeBlocking}
	h := &OpenAIGatewayHandler{securityAuditCoordinator: securityaudit.NewCoordinator(nil, engine)}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	plus := &service.Account{
		Platform: service.PlatformOpenAI,
		Type:     service.AccountTypeOAuth,
		Credentials: map[string]any{
			"plan_type": "plus",
		},
	}
	pro := &service.Account{
		Platform: service.PlatformOpenAI,
		Type:     service.AccountTypeOAuth,
		Credentials: map[string]any{
			"plan_type": "pro",
		},
	}
	check := func(account *service.Account) *securityaudit.Decision {
		return h.checkSecurityAuditForSelectedOpenAIProAccount(
			c,
			nil,
			nil,
			middleware2.AuthSubject{UserID: 7},
			account,
			service.ContentModerationProtocolOpenAIResponses,
			"gpt-test",
			[]byte(`{"input":"hello"}`),
		)
	}

	require.Nil(t, check(plus))
	require.Zero(t, engine.evaluates.Load())
	decision := check(pro)
	require.NotNil(t, decision)
	require.True(t, decision.AllowNextStage)
	require.Equal(t, int64(1), engine.evaluates.Load())
}

func TestSelectedOpenAIProAccountAuditPropagatesBlockingDecision(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := &turnCountingEngine{
		mode: securityaudit.ModeBlocking,
		decisions: []*securityaudit.PromptDecision{
			{Kind: securityaudit.DecisionBlock, AllowNextStage: false},
		},
	}
	h := &OpenAIGatewayHandler{securityAuditCoordinator: securityaudit.NewCoordinator(nil, engine)}
	core, logs := observer.New(zap.InfoLevel)
	reqLog := zap.New(core)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	account := &service.Account{
		Platform: service.PlatformOpenAI,
		Type:     service.AccountTypeOAuth,
		Credentials: map[string]any{
			"plan_type": "pro",
		},
	}

	decision := h.checkSecurityAuditForSelectedOpenAIProAccount(
		c,
		reqLog,
		nil,
		middleware2.AuthSubject{UserID: 7},
		account,
		service.ContentModerationProtocolOpenAIResponses,
		"gpt-test",
		[]byte(`{"input":"blocked"}`),
	)

	require.NotNil(t, decision)
	require.Equal(t, securityaudit.DecisionBlock, decision.Kind)
	require.False(t, decision.AllowNextStage)
	require.False(t, securityAuditCanReselect(decision), "an explicit block must never trigger account fallback")
	require.Equal(t, int64(1), engine.evaluates.Load())
	state := openAISecurityAdmissionFromContext(c)
	require.NotNil(t, state)
	require.Equal(t, securityadmission.RequestAuditableText, state.admission.Class(),
		"the canonical routing/audit admission must remain immutable")
	doneLogs := logs.FilterMessage("security_audit.gateway_check_done").All()
	require.Len(t, doneLogs, 1)
	require.Equal(t, string(securityadmission.RequestKnownViolation), doneLogs[0].ContextMap()["request_class"])
	require.Equal(t, securityaudit.ErrorCodeBlocked, doneLogs[0].ContextMap()["reason"])
	require.Equal(t, "upstream_not_dispatched", doneLogs[0].ContextMap()["dispatch"])
	_, cached := c.Get(securityAuditCompletedContextKey)
	require.False(t, cached, "blocking decisions must never populate the request cache")
}

func TestRunSecurityAuditDoesNotSkipSubsequentWebSocketTurns(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := &turnCountingEngine{mode: securityaudit.ModeAsync}
	coordinator := securityaudit.NewCoordinator(nil, engine)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	subject := middleware2.AuthSubject{UserID: 7, Concurrency: 1}
	first := runSecurityAudit(c, nil, coordinator, nil, nil, subject, "openai_responses", "gpt-test",
		[]byte(`{"type":"response.create","response":{"input":"benign"}}`), "first_turn")
	require.NotNil(t, first)
	require.True(t, first.AllowNextStage)
	require.Equal(t, int64(1), engine.enqueues.Load())
	_, cached := c.Get(securityAuditCompletedContextKey)
	require.False(t, cached, "WebSocket stages must not set the HTTP completion cache")

	// Even if an HTTP path previously cached completion on this Context, WS turns
	// must still audit every response.create payload.
	c.Set(securityAuditCompletedContextKey, true)

	second := runSecurityAudit(c, nil, coordinator, nil, nil, subject, "openai_responses", "gpt-test",
		[]byte(`{"type":"response.create","response":{"input":"malicious follow-up"}}`), "subsequent_turn")
	require.NotNil(t, second)
	require.Equal(t, int64(2), engine.enqueues.Load(), "subsequent WebSocket turns must be audited again")
}

func TestRunSecurityAuditDeduplicatesRepeatedPayloadWithinWebSocketTurn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := &turnCountingEngine{mode: securityaudit.ModeBlocking}
	coordinator := securityaudit.NewCoordinator(nil, engine)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	payload := []byte(`{"type":"response.create","response":{"input":"same turn"}}`)
	c.Set(securityAuditWSTurnContextKey, 2)
	first := runSecurityAudit(c, nil, coordinator, nil, nil, middleware2.AuthSubject{UserID: 7}, "openai_responses", "gpt-test", payload, "subsequent_turn")
	second := runSecurityAudit(c, nil, coordinator, nil, nil, middleware2.AuthSubject{UserID: 7}, "openai_responses", "gpt-test", payload, "subsequent_turn")
	require.NotNil(t, first)
	require.NotNil(t, second)
	require.True(t, first.AllowNextStage)
	require.True(t, second.AllowNextStage)
	require.Equal(t, int64(1), engine.evaluates.Load())

	// The cache holds only one successful same-turn result.
	entry, exists := c.Get(securityAuditWSDedupeContextKey)
	require.True(t, exists)
	require.IsType(t, securityAuditWSDedupeEntry{}, entry)

	c.Set(securityAuditWSTurnContextKey, 3)
	runSecurityAudit(c, nil, coordinator, nil, nil, middleware2.AuthSubject{UserID: 7}, "openai_responses", "gpt-test", payload, "subsequent_turn")
	require.Equal(t, int64(2), engine.evaluates.Load())
}

func TestRunSecurityAuditDoesNotCacheFailedWebSocketDecision(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := &turnCountingEngine{
		mode: securityaudit.ModeBlocking,
		decisions: []*securityaudit.PromptDecision{
			{Kind: securityaudit.DecisionUnavailable, AllowNextStage: false},
			{Kind: securityaudit.DecisionAllow, AllowNextStage: true},
		},
	}
	coordinator := securityaudit.NewCoordinator(nil, engine)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Set(securityAuditWSTurnContextKey, 2)
	payload := []byte(`{"type":"response.create","response":{"input":"retry me"}}`)

	first := runSecurityAudit(c, nil, coordinator, nil, nil, middleware2.AuthSubject{UserID: 7}, "openai_responses", "gpt-test", payload, "subsequent_turn")
	_, cachedAfterFailure := c.Get(securityAuditWSDedupeContextKey)
	second := runSecurityAudit(c, nil, coordinator, nil, nil, middleware2.AuthSubject{UserID: 7}, "openai_responses", "gpt-test", payload, "subsequent_turn")

	require.False(t, first.AllowNextStage)
	require.False(t, cachedAfterFailure)
	require.True(t, second.AllowNextStage)
	require.Equal(t, int64(2), engine.evaluates.Load())
}

func TestRunSecurityAuditDoesNotCacheFlaggedWebSocketDecision(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := &turnCountingEngine{
		mode: securityaudit.ModeBlocking,
		decisions: []*securityaudit.PromptDecision{
			{Kind: securityaudit.DecisionFlag, AllowNextStage: true},
			{Kind: securityaudit.DecisionAllow, AllowNextStage: true},
		},
	}
	coordinator := securityaudit.NewCoordinator(nil, engine)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Set(securityAuditWSTurnContextKey, 2)
	payload := []byte(`{"type":"response.create","response":{"input":"retry flagged"}}`)

	first := runSecurityAudit(c, nil, coordinator, nil, nil, middleware2.AuthSubject{UserID: 7}, "openai_responses", "gpt-test", payload, "subsequent_turn")
	_, cachedAfterFlag := c.Get(securityAuditWSDedupeContextKey)
	second := runSecurityAudit(c, nil, coordinator, nil, nil, middleware2.AuthSubject{UserID: 7}, "openai_responses", "gpt-test", payload, "subsequent_turn")

	require.Equal(t, securityaudit.DecisionFlag, first.Kind)
	require.True(t, first.AllowNextStage)
	require.False(t, cachedAfterFlag)
	require.Equal(t, securityaudit.DecisionAllow, second.Kind)
	require.Equal(t, int64(2), engine.evaluates.Load())
}

func TestRunSecurityAuditLogsWebSocketChecksAndCacheHits(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := &turnCountingEngine{mode: securityaudit.ModeBlocking}
	coordinator := securityaudit.NewCoordinator(nil, engine)
	core, logs := observer.New(zap.InfoLevel)
	reqLog := zap.New(core)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Set(securityAuditWSTurnContextKey, 2)
	payload := []byte(`{"type":"response.create","response":{"input":"same turn"}}`)

	runSecurityAudit(c, reqLog, coordinator, nil, nil, middleware2.AuthSubject{UserID: 7}, "openai_responses", "gpt-test", payload, "subsequent_turn")
	runSecurityAudit(c, reqLog, coordinator, nil, nil, middleware2.AuthSubject{UserID: 7}, "openai_responses", "gpt-test", payload, "subsequent_turn")

	startLogs := logs.FilterMessage("security_audit.gateway_check_start").All()
	require.Len(t, startLogs, 1)
	require.Equal(t, false, startLogs[0].ContextMap()["cached"])

	doneLogs := logs.FilterMessage("security_audit.gateway_check_done").All()
	require.Len(t, doneLogs, 2)
	require.Equal(t, false, doneLogs[0].ContextMap()["cached"])
	require.Equal(t, true, doneLogs[1].ContextMap()["cached"])
	require.Equal(t, "allow", doneLogs[1].ContextMap()["decision"])
	require.Equal(t, "subsequent_turn", doneLogs[1].ContextMap()["stage"])
	require.Equal(t, int64(1), engine.evaluates.Load())
}

type turnCountingEngine struct {
	mode      securityaudit.Mode
	enqueues  atomic.Int64
	evaluates atomic.Int64
	decisions []*securityaudit.PromptDecision
}

func (e *turnCountingEngine) EffectiveMode() securityaudit.Mode { return e.mode }
func (e *turnCountingEngine) Enqueue(context.Context, securityaudit.Request) error {
	e.enqueues.Add(1)
	return nil
}
func (e *turnCountingEngine) Evaluate(context.Context, securityaudit.Request) (*securityaudit.PromptDecision, error) {
	call := e.evaluates.Add(1)
	if int(call) <= len(e.decisions) {
		return e.decisions[call-1], nil
	}
	return &securityaudit.PromptDecision{Kind: securityaudit.DecisionAllow, AllowNextStage: true}, nil
}
