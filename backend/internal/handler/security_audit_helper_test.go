package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
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

func TestStrictAuditRequestBypassesOnlyPureImageRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name     string
		protocol string
		path     string
		body     string
		bypass   bool
	}{
		{
			name: "responses pure image", protocol: service.ContentModerationProtocolOpenAIResponses, path: "/v1/responses",
			body:   `{"model":"gpt-5.1","input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AA=="}]}]}`,
			bypass: false,
		},
		{
			name: "responses text and sanitized image", protocol: service.ContentModerationProtocolOpenAIResponses, path: "/v1/responses",
			body:   `{"model":"gpt-5.1","input":[{"type":"message","role":"user","content":"current user text"},{"type":"input_image","image_url":"data:image/png;base64,"}]}`,
			bypass: false,
		},
		{
			name: "chat pure image", protocol: service.ContentModerationProtocolOpenAIChat, path: "/v1/chat/completions",
			body:   `{"model":"gpt-5.1","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]}]}`,
			bypass: false,
		},
		{
			name: "chat text and image", protocol: service.ContentModerationProtocolOpenAIChat, path: "/v1/chat/completions",
			body:   `{"model":"gpt-5.1","messages":[{"role":"user","content":[{"type":"text","text":"current user text"},{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}]}]}`,
			bypass: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, tt.path, nil)
			require.Equal(t, tt.bypass, strictAuditRequestBypassesTextAudit(c, tt.protocol, "gpt-5.1", []byte(tt.body)))
		})
	}
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

func TestRunSecurityAuditOutOfScopeGroupSkipsBeforeCoordinator(t *testing.T) {
	gin.SetMode(gin.TestMode)
	proGroupID := int64(12)
	plusGroupID := int64(13)
	cfg := service.ContentModerationConfig{
		Enabled: true, Mode: service.ContentModerationModePreBlock, SampleRate: 100,
		AllGroups: false, GroupIDs: []int64{proGroupID}, APIKeys: []string{"sk-test"},
	}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)
	moderationSvc := service.NewContentModerationService(
		&contentModerationHandlerSettingRepo{values: map[string]string{
			service.SettingKeyRiskControlEnabled:      "true",
			service.SettingKeyContentModerationConfig: string(rawCfg),
		}},
		&contentModerationHandlerTestRepo{}, nil, nil, nil, nil, nil, nil,
	)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	apiKey := &service.APIKey{ID: 2, GroupID: &plusGroupID, Group: &service.Group{ID: plusGroupID, Name: "Plus"}}

	decision := runSecurityAudit(
		c, nil, nil, moderationSvc, apiKey, middleware2.AuthSubject{UserID: 7},
		service.ContentModerationProtocolOpenAIResponses, "gpt-5.5", []byte(`{"input":"safe"}`), "http",
	)

	require.Nil(t, decision)
	require.False(t, service.IsOpenAIStrictAuditRequest(c))
}

type countingAccountAuditLegacyEngine struct {
	calls        atomic.Int32
	lastDocument atomic.Pointer[auditinput.Document]
	decision     *securityaudit.LegacyDecision
	err          error
}

func (e *countingAccountAuditLegacyEngine) Check(_ context.Context, request securityaudit.Request) (*securityaudit.LegacyDecision, error) {
	e.calls.Add(1)
	if request.Document != nil {
		e.lastDocument.Store(request.Document.Clone())
	}
	return e.decision, e.err
}

func TestEnsureSecurityAuditForAccountFailoverState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	clientGroupID := int64(99)
	apiKey := &service.APIKey{ID: 9, GroupID: &clientGroupID, Group: &service.Group{ID: clientGroupID, Platform: service.PlatformOpenAI}}
	subject := middleware2.AuthSubject{UserID: 7}
	legacy := &countingAccountAuditLegacyEngine{decision: &securityaudit.LegacyDecision{Allowed: true}}
	h := &OpenAIGatewayHandler{securityAuditCoordinator: securityaudit.NewCoordinator(legacy, nil)}
	state := newOpenAIAccountAuditState(
		service.ContentModerationProtocolOpenAIResponses,
		"gpt-5.4",
		[]byte(`{"model":"gpt-5.4","instructions":"tenant safety policy","input":"safe"}`),
		"http",
		service.DefaultOpenAIAccountAuditRoutingPolicy(),
	)
	account := func(id int64, accountType string) *service.Account {
		return &service.Account{
			ID: id, Platform: service.PlatformOpenAI, Type: accountType,
			Credentials: map[string]any{"plan_type": "pro"}, GroupIDs: []int64{12},
		}
	}

	decision, eligibility := h.ensureSecurityAuditForAccount(c, nil, apiKey, subject, account(1, service.AccountTypeAPIKey), state)
	require.Nil(t, decision)
	require.False(t, eligibility.Eligible)
	require.Zero(t, legacy.calls.Load(), "APIKey must never enter the coordinator")

	decision, eligibility = h.ensureSecurityAuditForAccount(c, nil, apiKey, subject, account(2, service.AccountTypeOAuth), state)
	require.NotNil(t, decision)
	require.True(t, decision.AllowNextStage)
	require.True(t, eligibility.Eligible)
	require.Equal(t, int32(1), legacy.calls.Load())
	require.Equal(t, "tenant safety policy\nsafe", legacy.lastDocument.Load().NormalizedText)

	decision, eligibility = h.ensureSecurityAuditForAccount(c, nil, apiKey, subject, account(3, service.AccountTypeOAuth), state)
	require.NotNil(t, decision)
	require.True(t, decision.AllowNextStage)
	require.True(t, eligibility.Eligible)
	require.Equal(t, int32(1), legacy.calls.Load(), "OAuth Pro failover must reuse the request audit")

	decision, eligibility = h.ensureSecurityAuditForAccount(c, nil, apiKey, subject, account(4, service.AccountTypeAPIKey), state)
	require.Nil(t, decision)
	require.False(t, eligibility.Eligible)
	require.Equal(t, int32(1), legacy.calls.Load(), "switching to APIKey must not add an audit call")
}

func TestEnsureSecurityAuditForAccountTerminalFailureCannotBeBypassed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	groupID := int64(12)
	apiKey := &service.APIKey{ID: 9, GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI}}
	legacy := &countingAccountAuditLegacyEngine{err: errors.New("audit unavailable")}
	h := &OpenAIGatewayHandler{securityAuditCoordinator: securityaudit.NewCoordinator(legacy, nil)}
	state := newOpenAIAccountAuditState(
		service.ContentModerationProtocolOpenAIChat,
		"gpt-5.4",
		[]byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"safe"}]}`),
		"http",
		service.DefaultOpenAIAccountAuditRoutingPolicy(),
	)
	oauth := &service.Account{
		ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Credentials: map[string]any{"plan_type": "pro"}, GroupIDs: []int64{12},
	}
	apiKeyAccount := &service.Account{ID: 2, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey}

	decision, eligibility := h.ensureSecurityAuditForAccount(c, nil, apiKey, middleware2.AuthSubject{UserID: 7}, oauth, state)
	require.NotNil(t, decision)
	require.False(t, decision.AllowNextStage)
	require.True(t, eligibility.Eligible)
	require.Equal(t, securityaudit.ErrorCodeAuditUnavailable, decision.ErrorCode)
	require.Equal(t, int32(1), legacy.calls.Load())

	decision, eligibility = h.ensureSecurityAuditForAccount(c, nil, apiKey, middleware2.AuthSubject{UserID: 7}, apiKeyAccount, state)
	require.NotNil(t, decision)
	require.False(t, decision.AllowNextStage)
	require.False(t, eligibility.Eligible)
	require.Equal(t, securityaudit.ErrorCodeAuditUnavailable, decision.ErrorCode)
	require.Equal(t, int32(1), legacy.calls.Load(), "terminal audit failure must not trigger another audit")
}

func TestOpenAIAccountAuditStateLongTextUsesCanonicalRuneCount(t *testing.T) {
	policy := service.DefaultOpenAIAccountAuditRoutingPolicy()
	build := func(protocol string, payload any) *openAIAccountAuditState {
		body, err := json.Marshal(payload)
		require.NoError(t, err)
		return newOpenAIAccountAuditState(
			protocol,
			"gpt-5.4",
			body,
			"http",
			policy,
		)
	}

	atLimit := build(service.ContentModerationProtocolOpenAIResponses, map[string]any{
		"model": "gpt-5.4", "input": strings.Repeat("界", service.DefaultOpenAIAccountAuditLongTextRuneThreshold),
	})
	require.Equal(t, service.DefaultOpenAIAccountAuditLongTextRuneThreshold, atLimit.auditTextRunes())
	require.False(t, atLimit.preferAPIKey(), "exactly 12k runes stays on normal scheduling")
	require.True(t, atLimit.auditContextReliable())
	require.Equal(t, "normal", atLimit.auditRoutingReason())

	responsesOverLimit := build(service.ContentModerationProtocolOpenAIResponses, map[string]any{
		"model":        "gpt-5.4",
		"instructions": strings.Repeat("界", service.DefaultOpenAIAccountAuditLongTextRuneThreshold),
		"input":        "😀",
	})
	require.Equal(t, service.DefaultOpenAIAccountAuditLongTextRuneThreshold+2, responsesOverLimit.auditTextRunes())
	require.True(t, responsesOverLimit.preferAPIKey(), "Responses instructions must participate in routing")
	require.Equal(t, "long_text", responsesOverLimit.auditRoutingReason())
	require.Contains(t, responsesOverLimit.document.NormalizedText, "😀")

	chatOverLimit := build(service.ContentModerationProtocolOpenAIChat, map[string]any{
		"model": "gpt-5.4",
		"messages": []map[string]any{
			{"role": "system", "content": strings.Repeat("甲", 6000)},
			{"role": "developer", "content": strings.Repeat("乙", 5999)},
			{"role": "user", "content": "界"},
		},
	})
	require.Equal(t, service.DefaultOpenAIAccountAuditLongTextRuneThreshold+2, chatOverLimit.auditTextRunes())
	require.True(t, chatOverLimit.preferAPIKey(), "Chat system and developer text must participate in routing")

	unreliable := build(service.ContentModerationProtocolOpenAIResponses, map[string]any{
		"model": "gpt-5.4", "instructions": 42, "input": "safe",
	})
	require.False(t, unreliable.auditContextReliable())
	require.True(t, unreliable.preferAPIKey(), "unreliable extraction must prefer APIKey")
	require.Equal(t, "context_unreliable", unreliable.auditRoutingReason())
	require.Equal(t, auditinput.IssueInvalidShape, unreliable.auditContextIssue())

	knownNoText := build(service.ContentModerationProtocolOpenAIResponses, map[string]any{
		"model": "gpt-5.4", "input": []any{},
	})
	require.True(t, knownNoText.auditContextReliable())
	require.Zero(t, knownNoText.auditTextRunes())
	require.False(t, knownNoText.preferAPIKey())
}

func TestEnsureSecurityAuditForAccountUnreliableContextPrefersAPIKeyAndFailsClosedOnOAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	groupID := int64(12)
	clientKey := &service.APIKey{ID: 9, GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI}}
	legacy := &countingAccountAuditLegacyEngine{decision: &securityaudit.LegacyDecision{Allowed: true}}
	h := &OpenAIGatewayHandler{securityAuditCoordinator: securityaudit.NewCoordinator(legacy, nil)}
	state := newOpenAIAccountAuditState(
		service.ContentModerationProtocolOpenAIResponses,
		"gpt-5.4",
		[]byte(`{"model":"gpt-5.4","instructions":42,"input":"safe"}`),
		"http",
		service.DefaultOpenAIAccountAuditRoutingPolicy(),
	)
	apiKeyAccount := &service.Account{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey}
	oauthPro := &service.Account{
		ID: 2, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Credentials: map[string]any{"plan_type": "pro"}, GroupIDs: []int64{groupID},
	}

	require.True(t, state.preferAPIKey())
	decision, eligibility := h.ensureSecurityAuditForAccount(c, nil, clientKey, middleware2.AuthSubject{UserID: 7}, apiKeyAccount, state)
	require.Nil(t, decision)
	require.False(t, eligibility.Eligible)
	require.Zero(t, legacy.calls.Load(), "APIKey must not call any audit engine")

	decision, eligibility = h.ensureSecurityAuditForAccount(c, nil, clientKey, middleware2.AuthSubject{UserID: 7}, oauthPro, state)
	require.True(t, eligibility.Eligible)
	require.NotNil(t, decision)
	require.False(t, decision.AllowNextStage)
	require.Equal(t, securityaudit.ErrorCodeContextIncomplete, decision.ErrorCode)
	require.Zero(t, legacy.calls.Load(), "incomplete context must fail closed before invoking an audit model")
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

func TestBindAllowedSecurityAuditResponseSkipsStoreFalseFullHistoryLineage(t *testing.T) {
	store := &captureLineageStore{}
	h := &OpenAIGatewayHandler{
		securityAuditCoordinator: securityaudit.NewCoordinator(nil, nil).SetLineageStore(store),
	}
	summary := &securityaudit.AuditSummary{
		Verdict: securityaudit.AuditVerdictAllow, ContextComplete: true,
		SkipResponseLineage: true,
	}

	require.False(t, securityAuditResponseLineageRequired(summary))
	require.True(t, securityAuditResponseLineageRequired(&securityaudit.AuditSummary{}))
	require.NoError(t, h.bindAllowedSecurityAuditResponse(context.Background(), nil, summary, nil))
	require.Zero(t, store.calls)
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

func TestRunSecurityAuditDeduplicatesRepeatedPayloadWithinWebSocketTurn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := &turnCountingEngine{mode: securityaudit.ModeBlocking, blocking: true}
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
		mode: securityaudit.ModeBlocking, blocking: true,
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
		mode: securityaudit.ModeBlocking, blocking: true,
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
	engine := &turnCountingEngine{mode: securityaudit.ModeBlocking, blocking: true}
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
	mode        securityaudit.Mode
	blocking    bool
	enqueues    atomic.Int64
	evaluates   atomic.Int64
	evaluateErr error
	decisions   []*securityaudit.PromptDecision
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
	call := e.evaluates.Add(1)
	if e.evaluateErr != nil {
		return nil, e.evaluateErr
	}
	if int(call) <= len(e.decisions) {
		return e.decisions[call-1], nil
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

func TestRunSecurityAuditScopeSkipsNonOpenAITextProtocolsAndResolvedGrok(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := &turnCountingEngine{mode: securityaudit.ModeBlocking, blocking: true}
	coordinator := securityaudit.NewCoordinator(nil, engine)
	groupID := int64(12)
	apiKey := &service.APIKey{ID: 9, GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI}}
	subject := middleware2.AuthSubject{UserID: 7}
	for _, protocol := range []string{service.ContentModerationProtocolAnthropicMessages, service.ContentModerationProtocolGemini, service.ContentModerationProtocolOpenAIImages} {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/test", nil)
		decision := runSecurityAudit(c, nil, coordinator, nil, apiKey, subject, protocol, "gpt-5.5", []byte(`{"input":"safe"}`), "http")
		require.Nil(t, decision, protocol)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request = c.Request.WithContext(service.WithResolvedTargetPlatform(c.Request.Context(), service.PlatformGrok))
	decision := runSecurityAudit(c, nil, coordinator, nil, apiKey, subject, service.ContentModerationProtocolOpenAIResponses, "gpt-5.5", []byte(`{"input":"safe"}`), "http")
	require.Nil(t, decision, "resolved Grok composite target must bypass strict OpenAI audit")
	require.Zero(t, engine.evaluates.Load())
}

func TestRunSecurityAuditImageRequestDependencyBoundary(t *testing.T) {
	gin.SetMode(gin.TestMode)
	groupID := int64(12)
	apiKey := &service.APIKey{
		ID: 9, GroupID: &groupID,
		Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI},
	}
	tests := []struct {
		name     string
		protocol string
		path     string
		body     string
		bypass   bool
	}{
		{
			name: "responses input image", protocol: service.ContentModerationProtocolOpenAIResponses, path: "/v1/responses",
			body: `{"model":"gpt-5.5","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"do not audit this text"},{"type":"input_image","image_url":"opaque"}]}]}`,
		},
		{
			name: "chat input image", protocol: service.ContentModerationProtocolOpenAIChat, path: "/v1/chat/completions",
			body: `{"model":"gpt-5.5","messages":[{"role":"user","content":[{"type":"text","text":"do not audit this text"},{"type":"image_url","image_url":{"url":"opaque"}}]}]}`,
		},
		{
			name: "responses explicit image generation tool", protocol: service.ContentModerationProtocolOpenAIResponses, path: "/v1/responses",
			body:   `{"model":"gpt-5.5","previous_response_id":"resp_legacy","input":"draw","tools":[{"type":"image_generation"}]}`,
			bypass: true,
		},
		{
			name: "responses explicit image tool choice", protocol: service.ContentModerationProtocolOpenAIResponses, path: "/v1/responses",
			body:   `{"model":"gpt-5.5","input":"draw","tool_choice":{"type":"image_generation"}}`,
			bypass: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, tt.path, nil)

			decision := runSecurityAudit(
				c, nil, nil, nil, apiKey, middleware2.AuthSubject{UserID: 7},
				tt.protocol, "gpt-5.5", []byte(tt.body), "http",
			)

			if tt.bypass {
				require.Nil(t, decision)
			} else {
				require.NotNil(t, decision, "text in a multimodal request must still enter strict audit")
				require.Equal(t, http.StatusServiceUnavailable, decision.HTTPStatus)
				require.Equal(t, securityaudit.ErrorCodeAuditUnavailable, decision.ErrorCode)
			}
			require.False(t, service.IsOpenAIStrictAuditRequest(c))
		})
	}
}
