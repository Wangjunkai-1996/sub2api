package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestCachesSecurityAuditCompletionSkipsWebSocketStages(t *testing.T) {
	require.True(t, cachesSecurityAuditCompletion("http"))
	require.True(t, cachesSecurityAuditCompletion(""))
	require.False(t, cachesSecurityAuditCompletion("first_turn"))
	require.False(t, cachesSecurityAuditCompletion("subsequent_turn"))
}

func TestRunSecurityAuditMissingCoordinatorFailsClosed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	decision := runSecurityAudit(
		c, nil, nil, nil, nil, middleware2.AuthSubject{UserID: 7},
		service.ContentModerationProtocolOpenAIResponses, "gpt-test", []byte(`{"input":"safe"}`), "http",
	)

	require.NotNil(t, decision)
	require.False(t, decision.AllowNextStage)
	require.Equal(t, http.StatusServiceUnavailable, decision.HTTPStatus)
	require.Equal(t, securityaudit.ErrorCodeAuditUnavailable, decision.ErrorCode)
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

type captureLineageStore struct {
	calls      int
	responseID string
	summary    securityaudit.AuditSummary
	bindErr    error
}

func (s *captureLineageStore) Load(context.Context, securityaudit.LineageLookup) (*securityaudit.AuditSummary, error) {
	return nil, securityaudit.ErrLineageNotFound
}

func (s *captureLineageStore) BindAllowedResponse(_ context.Context, summary securityaudit.AuditSummary, responseID string) error {
	s.calls++
	s.responseID = responseID
	s.summary = summary.Clone()
	return s.bindErr
}

func TestBindAllowedSecurityAuditResponseRequiresSuccessfulTerminalResponse(t *testing.T) {
	groupID := int64(12)
	store := &captureLineageStore{}
	h := &OpenAIGatewayHandler{
		securityAuditCoordinator: securityaudit.NewCoordinator(nil, nil).SetLineageStore(store),
	}
	summary := &securityaudit.AuditSummary{
		ParserVersion: "auditinput/v1", ConfigVersion: 3, APIKeyID: 9, GroupID: &groupID,
		PromptHash: "prompt-hash", DocumentHash: "request-document-hash", RedactedContext: "audited context",
		ContextComplete: true, Verdict: securityaudit.AuditVerdictAllow,
	}
	completeResult := func(responseID, event, status, output string) *service.OpenAIForwardResult {
		result := &service.OpenAIForwardResult{ResponseID: responseID, OpenAIWSMode: true, UpstreamTerminalEvent: event}
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		service.EnableOpenAIStrictLineageCapture(c)
		result.CaptureOpenAIResponsesLineageOutput(c, []byte(fmt.Sprintf(
			`{"type":%q,"response":{"id":%q,"status":%q,"output":%s}}`, event, responseID, status, output,
		)))
		return result
	}

	require.NoError(t, h.bindAllowedSecurityAuditResponse(context.Background(), nil, summary, completeResult(
		"resp_completed", "response.completed", "completed",
		`[{"type":"function_call","id":"fc_1","call_id":"call_1","name":"shell","arguments":"{\"cmd\":\"audited command\"}"}]`,
	)))
	require.Equal(t, 1, store.calls)
	require.Equal(t, "resp_completed", store.responseID)
	require.Contains(t, store.summary.RedactedContext, "audited command")
	require.NotEqual(t, summary.PromptHash, store.summary.PromptHash)
	require.NoError(t, h.bindAllowedSecurityAuditResponse(context.Background(), nil, summary, completeResult(
		"resp_http_completed", "response.done", "done",
		`[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"assistant answer"}]}]`,
	)))
	require.Equal(t, 2, store.calls)
	require.Equal(t, "resp_http_completed", store.responseID)
	require.Contains(t, store.summary.RedactedContext, "assistant answer")

	require.Error(t, h.bindAllowedSecurityAuditResponse(context.Background(), nil, summary, &service.OpenAIForwardResult{
		ResponseID: "resp_failed", OpenAIWSMode: true, UpstreamTerminalEvent: "response.failed",
	}))
	require.Error(t, h.bindAllowedSecurityAuditResponse(context.Background(), nil, summary, &service.OpenAIForwardResult{
		ResponseID: "resp_http_incomplete", UpstreamTerminalEvent: "response.incomplete",
	}))
	require.Error(t, h.bindAllowedSecurityAuditResponse(context.Background(), nil, summary, &service.OpenAIForwardResult{
		ResponseID: "resp_http_cancelled", UpstreamTerminalEvent: "response.cancelled",
	}))
	require.Error(t, h.bindAllowedSecurityAuditResponse(context.Background(), nil, summary, &service.OpenAIForwardResult{
		ResponseID: "resp_http_unknown",
	}))
	require.Error(t, h.bindAllowedSecurityAuditResponse(context.Background(), nil, summary, &service.OpenAIForwardResult{
		ResponseID: "resp_completed_without_output", UpstreamTerminalEvent: "response.completed",
	}))
	require.Error(t, h.bindAllowedSecurityAuditResponse(context.Background(), nil, summary, completeResult(
		"resp_unknown_output", "response.completed", "completed", `[{"type":"future_item","payload":"hidden"}]`,
	)))
	require.Error(t, h.bindAllowedSecurityAuditResponse(context.Background(), nil, summary, &service.OpenAIForwardResult{
		OpenAIWSMode: true, UpstreamTerminalEvent: "response.completed",
	}))
	require.Error(t, h.bindAllowedSecurityAuditResponse(context.Background(), nil, nil, &service.OpenAIForwardResult{ResponseID: "resp_no_audit"}))
	require.Equal(t, 2, store.calls)
}

func TestBindAllowedSecurityAuditResponsePropagatesStoreFailure(t *testing.T) {
	groupID := int64(12)
	bindErr := fmt.Errorf("redis set failed")
	store := &captureLineageStore{bindErr: bindErr}
	h := &OpenAIGatewayHandler{
		securityAuditCoordinator: securityaudit.NewCoordinator(nil, nil).SetLineageStore(store),
	}
	summary := &securityaudit.AuditSummary{
		ParserVersion: "auditinput/v1", ConfigVersion: 3, APIKeyID: 9, GroupID: &groupID,
		PromptHash: "prompt-hash", DocumentHash: "request-document-hash", RedactedContext: "audited context",
		ContextComplete: true, Verdict: securityaudit.AuditVerdictAllow,
	}
	result := &service.OpenAIForwardResult{ResponseID: "resp_bind_failure", UpstreamTerminalEvent: "response.completed"}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	service.EnableOpenAIStrictLineageCapture(c)
	result.CaptureOpenAIResponsesLineageOutput(c, []byte(
		`{"type":"response.completed","response":{"id":"resp_bind_failure","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"safe answer"}]}]}}`,
	))

	err := h.bindAllowedSecurityAuditResponse(context.Background(), nil, summary, result)
	require.ErrorIs(t, err, bindErr)
	require.Equal(t, 1, store.calls)
}

type turnCountingEngine struct {
	mode        securityaudit.Mode
	blocking    bool
	enqueues    atomic.Int64
	evaluates   atomic.Int64
	evaluateErr error
}

func (e *turnCountingEngine) EffectiveMode() securityaudit.Mode { return e.mode }
func (e *turnCountingEngine) BlockingApplies(securityaudit.Request) bool {
	return e.blocking
}
func (e *turnCountingEngine) Enqueue(context.Context, securityaudit.Request) error {
	e.enqueues.Add(1)
	return nil
}
func (e *turnCountingEngine) Evaluate(context.Context, securityaudit.Request) (*securityaudit.PromptDecision, error) {
	e.evaluates.Add(1)
	if e.evaluateErr != nil {
		return nil, e.evaluateErr
	}
	return &securityaudit.PromptDecision{Kind: securityaudit.DecisionAllow, AllowNextStage: true}, nil
}

type outOfScopeLegacyEngine struct {
	calls atomic.Int64
}

func (e *outOfScopeLegacyEngine) BlockingApplies(context.Context, securityaudit.Request) (bool, error) {
	return false, nil
}

func (e *outOfScopeLegacyEngine) Check(context.Context, securityaudit.Request) (*securityaudit.LegacyDecision, error) {
	e.calls.Add(1)
	return &securityaudit.LegacyDecision{Allowed: true}, nil
}

func TestRunSecurityAuditOutOfScopePlusSkipsDegradedBlockingPrompt(t *testing.T) {
	gin.SetMode(gin.TestMode)
	plusGroupID := int64(13)
	legacy := &outOfScopeLegacyEngine{}
	prompt := &turnCountingEngine{
		mode: securityaudit.ModeBlocking, evaluateErr: errors.New("degraded prompt audit"),
	}
	coordinator := securityaudit.NewCoordinator(legacy, prompt)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	apiKey := &service.APIKey{
		ID: 2, GroupID: &plusGroupID, Group: &service.Group{ID: plusGroupID, Name: "Plus"},
		User: &service.User{ID: 1013},
	}
	decision := runSecurityAudit(
		c, nil, coordinator, nil, apiKey, middleware2.AuthSubject{UserID: 1013},
		service.ContentModerationProtocolOpenAIResponses, "gpt-5.4", []byte(`{"input":"safe"}`), "http",
	)

	require.NotNil(t, decision)
	require.True(t, decision.AllowNextStage)
	require.Equal(t, securityaudit.DecisionAllow, decision.Kind)
	require.Equal(t, int64(1), legacy.calls.Load())
	require.Zero(t, prompt.evaluates.Load(), "out-of-scope Plus must not call a degraded blocking prompt auditor")
}
