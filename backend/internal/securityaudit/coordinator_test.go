package securityaudit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
	"github.com/stretchr/testify/require"
)

type fakeLegacyEngine struct {
	decision *LegacyDecision
	err      error
	calls    atomic.Int64
	check    func(context.Context, Request) (*LegacyDecision, error)
}

func (f *fakeLegacyEngine) Check(ctx context.Context, req Request) (*LegacyDecision, error) {
	f.calls.Add(1)
	if f.check != nil {
		return f.check(ctx, req)
	}
	return f.decision, f.err
}

type fakePromptEngine struct {
	mode      Mode
	strict    *bool
	decision  *PromptDecision
	err       error
	enqueues  atomic.Int64
	evaluates atomic.Int64
	evaluate  func(context.Context, Request) (*PromptDecision, error)
}

func (f *fakePromptEngine) EffectiveMode() Mode { return f.mode }
func (f *fakePromptEngine) BlockingApplies(Request) bool {
	return f.strict != nil && *f.strict
}
func (f *fakePromptEngine) Enqueue(context.Context, Request) error {
	f.enqueues.Add(1)
	return f.err
}

func TestCoordinatorNilReceiverFailsClosed(t *testing.T) {
	var coordinator *Coordinator
	decision := coordinator.Check(context.Background(), Request{Protocol: "openai_responses", Body: []byte(`{"input":"safe"}`)})

	require.False(t, decision.AllowNextStage)
	require.Equal(t, DecisionUnavailable, decision.Kind)
	require.Equal(t, http.StatusServiceUnavailable, decision.HTTPStatus)
	require.Equal(t, ErrorCodeAuditUnavailable, decision.ErrorCode)
}

func TestCoordinatorStrictInputAndEngineFailuresAreFailClosed(t *testing.T) {
	groupID := int64(12)
	strict := true
	tests := []struct {
		name       string
		body       string
		legacy     *fakeLegacyEngine
		prompt     *fakePromptEngine
		wantKind   DecisionKind
		wantStatus int
		wantCode   string
		legacyCall int64
		promptCall int64
	}{
		{
			name: "unknown item rejected before engines", body: `{"input":[{"type":"future_item","payload":"opaque"}]}`,
			legacy: &fakeLegacyEngine{}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict},
			wantKind: DecisionInvalid, wantStatus: http.StatusUnprocessableEntity, wantCode: ErrorCodeContextIncomplete,
		},
		{
			name: "nonempty empty context rejected before engines", body: `{"model":"gpt-test"}`,
			legacy: &fakeLegacyEngine{}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict},
			wantKind: DecisionInvalid, wantStatus: http.StatusUnprocessableEntity, wantCode: ErrorCodeContextIncomplete,
		},
		{
			name: "tool output without lineage rejected before engines", body: `{"input":[{"type":"function_call_output","call_id":"call_1","output":"done"}]}`,
			legacy: &fakeLegacyEngine{}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict},
			wantKind: DecisionInvalid, wantStatus: http.StatusUnprocessableEntity, wantCode: ErrorCodeContextIncomplete,
		},
		{
			name: "legacy error is unavailable", body: `{"input":"hello"}`,
			legacy: &fakeLegacyEngine{err: errors.New("429")}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}},
			wantKind: DecisionUnavailable, wantStatus: http.StatusServiceUnavailable, wantCode: ErrorCodeAuditUnavailable, legacyCall: 1, promptCall: 0,
		},
		{
			name: "prompt flag is blocked", body: `{"input":"hello"}`,
			legacy: &fakeLegacyEngine{decision: &LegacyDecision{Allowed: true}}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionFlag, AllowNextStage: true}},
			wantKind: DecisionBlock, wantStatus: http.StatusForbidden, wantCode: ErrorCodePolicyBlocked, legacyCall: 1, promptCall: 1,
		},
		{
			name: "prompt outage is unavailable", body: `{"input":"hello"}`,
			legacy: &fakeLegacyEngine{decision: &LegacyDecision{Allowed: true}}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict, err: errors.New("timeout")},
			wantKind: DecisionUnavailable, wantStatus: http.StatusServiceUnavailable, wantCode: ErrorCodeAuditUnavailable, legacyCall: 1, promptCall: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decision := NewCoordinator(tt.legacy, tt.prompt).Check(context.Background(), Request{
				APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses", Body: []byte(tt.body),
			})
			require.Equal(t, tt.wantKind, decision.Kind)
			require.Equal(t, tt.wantStatus, decision.HTTPStatus)
			require.Equal(t, tt.wantCode, decision.ErrorCode)
			require.False(t, decision.AllowNextStage)
			require.Nil(t, decision.Audit)
			require.Equal(t, tt.legacyCall, tt.legacy.calls.Load())
			require.Equal(t, tt.promptCall, tt.prompt.evaluates.Load())
		})
	}
}

func TestCoordinatorStrictAllowExposesStableAuditSummaryOnlyForProtectedGroup(t *testing.T) {
	groupID := int64(12)
	strictMode := true
	decision := NewCoordinator(
		&fakeLegacyEngine{decision: &LegacyDecision{Allowed: true}},
		&fakePromptEngine{mode: ModeBlocking, strict: &strictMode, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true, ConfigVersion: 42}},
	).Check(context.Background(), Request{
		APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses",
		Body: []byte(`{"input":"hello"}`),
	})

	require.True(t, decision.AllowNextStage)
	require.NotNil(t, decision.Audit)
	require.Equal(t, AuditVerdictAllow, decision.Audit.Verdict)
	require.Equal(t, int64(42), decision.Audit.ConfigVersion)
	require.Equal(t, int64(7), decision.Audit.APIKeyID)
	require.Empty(t, decision.Audit.PreviousResponseID)
	require.Equal(t, "hello", decision.Audit.NormalizedContext)
	require.NotEmpty(t, decision.Audit.PromptHash)
	require.True(t, decision.Audit.ContextComplete)

	strict := false
	outOfScope := NewCoordinator(
		&fakeLegacyEngine{},
		&fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}},
	).Check(context.Background(), Request{GroupID: &groupID, Protocol: "openai_responses", Body: []byte(`{"input":[{"type":"future_item"}]}`)})
	require.True(t, outOfScope.AllowNextStage)
	require.Nil(t, outOfScope.Audit)
}

type fakeLineageStore struct {
	loaded     *AuditSummary
	loadErr    error
	lookup     LineageLookup
	bound      *AuditSummary
	responseID string
}

func (s *fakeLineageStore) Load(_ context.Context, lookup LineageLookup) (*AuditSummary, error) {
	s.lookup = lookup
	return s.loaded, s.loadErr
}

func (s *fakeLineageStore) BindAllowedResponse(_ context.Context, summary AuditSummary, responseID string) error {
	s.bound = &summary
	s.responseID = responseID
	return nil
}

func TestCoordinatorLineageIsResolvedBeforeEnginesAndBoundOnlyFromAllowSummary(t *testing.T) {
	groupID := int64(12)
	strict := true
	store := &fakeLineageStore{loaded: &AuditSummary{
		Verdict: AuditVerdictAllow, ContextComplete: true, APIKeyID: 7, GroupID: &groupID,
		RedactedContext: "parent context", PromptHash: "parent-hash",
	}}
	coordinator := NewCoordinator(
		&fakeLegacyEngine{decision: &LegacyDecision{Allowed: true}},
		&fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true, ConfigVersion: 4}},
	).SetLineageStore(store)
	decision := coordinator.Check(context.Background(), Request{
		APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses",
		Body: []byte(`{"previous_response_id":"resp_parent","input":"child context"}`),
	})
	require.True(t, decision.AllowNextStage)
	require.Equal(t, "resp_parent", store.lookup.PreviousResponseID)
	require.Equal(t, "parent context\nchild context", decision.Audit.NormalizedContext)
	require.NoError(t, coordinator.BindAllowedResponse(context.Background(), *decision.Audit, "resp_child"))
	require.Equal(t, "resp_child", store.responseID)
	require.Equal(t, AuditVerdictAllow, store.bound.Verdict)

	missing := NewCoordinator(&fakeLegacyEngine{}, &fakePromptEngine{mode: ModeBlocking, strict: &strict}).SetLineageStore(&fakeLineageStore{loadErr: ErrLineageNotFound})
	missingDecision := missing.Check(context.Background(), Request{
		APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses",
		Body: []byte(`{"previous_response_id":"missing","input":"child"}`),
	})
	require.Equal(t, ErrorCodeContextIncomplete, missingDecision.ErrorCode)
	require.Equal(t, http.StatusUnprocessableEntity, missingDecision.HTTPStatus)
}

func TestCoordinatorStrictLineageAllowsMediaOnlyParent(t *testing.T) {
	groupID := int64(12)
	strict := true
	mediaDigest := strings.Repeat("d", 64)
	store := &fakeLineageStore{loaded: &AuditSummary{
		ParserVersion: auditinput.ParserVersion, Verdict: AuditVerdictAllow, ContextComplete: true,
		APIKeyID: 7, GroupID: &groupID, PromptHash: "parent-hash", MediaDigests: []string{mediaDigest},
	}}
	decision := NewCoordinator(
		&fakeLegacyEngine{decision: &LegacyDecision{Allowed: true}},
		&fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true, ConfigVersion: 4}},
	).SetLineageStore(store).Check(context.Background(), Request{
		APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses",
		Body: []byte(`{"previous_response_id":"resp_media_parent","input":"child context"}`),
	})

	require.True(t, decision.AllowNextStage)
	require.NotNil(t, decision.Audit)
	require.Equal(t, "child context", decision.Audit.RedactedContext)
	require.Equal(t, []string{mediaDigest}, decision.Audit.MediaDigests)
}

func TestCoordinatorStrictLineageFeedsCumulativeContextToEveryRequiredAuditor(t *testing.T) {
	groupID := int64(12)
	strict := true

	t.Run("split keyword blocks before prompt guard", func(t *testing.T) {
		store := &fakeLineageStore{loaded: &AuditSummary{
			Verdict: AuditVerdictAllow, ContextComplete: true, APIKeyID: 7, GroupID: &groupID,
			RedactedContext: "cy", PromptHash: "parent-hash",
		}}
		legacy := &fakeLegacyEngine{check: func(_ context.Context, req Request) (*LegacyDecision, error) {
			require.Equal(t, "cy\nber", req.AuditContext)
			require.Contains(t, string(req.Body), `"input":"ber"`)
			if strings.Contains(strings.ToLower(auditinput.FoldForMatching(req.AuditContext)), "cyber") {
				return &LegacyDecision{Blocked: true, Flagged: true}, nil
			}
			return &LegacyDecision{Allowed: true}, nil
		}}
		prompt := &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}}
		decision := NewCoordinator(legacy, prompt).SetLineageStore(store).Check(context.Background(), Request{
			APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses",
			Body: []byte(`{"previous_response_id":"resp_parent","input":"ber"}`),
		})
		require.Equal(t, DecisionBlock, decision.Kind)
		require.Equal(t, ErrorCodePolicyBlocked, decision.ErrorCode)
		require.Equal(t, int64(1), legacy.calls.Load())
		require.Zero(t, prompt.evaluates.Load(), "local keyword blocks must not consume Prompt Guard capacity")
	})

	t.Run("semantic guard receives chronological parent and current context", func(t *testing.T) {
		store := &fakeLineageStore{loaded: &AuditSummary{
			Verdict: AuditVerdictAllow, ContextComplete: true, APIKeyID: 7, GroupID: &groupID,
			RedactedContext: "harmful", PromptHash: "parent-hash",
		}}
		legacy := &fakeLegacyEngine{decision: &LegacyDecision{Allowed: true}}
		prompt := &fakePromptEngine{mode: ModeBlocking, strict: &strict, evaluate: func(_ context.Context, req Request) (*PromptDecision, error) {
			require.Equal(t, "harmful\nrequest", req.AuditContext)
			return &PromptDecision{Kind: DecisionBlock}, nil
		}}
		decision := NewCoordinator(legacy, prompt).SetLineageStore(store).Check(context.Background(), Request{
			APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses",
			Body: []byte(`{"previous_response_id":"resp_parent","input":"request"}`),
		})
		require.Equal(t, DecisionBlock, decision.Kind)
		require.Equal(t, ErrorCodePolicyBlocked, decision.ErrorCode)
		require.Equal(t, int64(1), legacy.calls.Load())
		require.Equal(t, int64(1), prompt.evaluates.Load())
	})
}
func (f *fakePromptEngine) Evaluate(ctx context.Context, req Request) (*PromptDecision, error) {
	f.evaluates.Add(1)
	if f.evaluate != nil {
		return f.evaluate(ctx, req)
	}
	return f.decision, f.err
}

func TestCoordinatorModesAndPriority(t *testing.T) {
	tests := []struct {
		name           string
		mode           Mode
		legacy         *LegacyDecision
		prompt         *PromptDecision
		promptErr      error
		wantKind       DecisionKind
		wantCode       string
		wantEnqueue    int64
		wantEvaluation int64
	}{
		{name: "off", mode: ModeOff, wantKind: DecisionAllow},
		{name: "async only enqueues", mode: ModeAsync, wantKind: DecisionAllow, wantEnqueue: 1},
		{name: "prompt block", mode: ModeBlocking, prompt: &PromptDecision{Kind: DecisionBlock}, wantKind: DecisionBlock, wantCode: ErrorCodeBlocked, wantEvaluation: 1},
		{name: "prompt unavailable", mode: ModeBlocking, promptErr: errors.New("down"), wantKind: DecisionUnavailable, wantCode: ErrorCodeUnavailable, wantEvaluation: 1},
		{name: "legacy wins both block", mode: ModeBlocking,
			legacy: &LegacyDecision{Blocked: true, StatusCode: http.StatusForbidden, ErrorCode: "content_policy_violation", Message: "legacy"},
			prompt: &PromptDecision{Kind: DecisionBlock}, wantKind: DecisionBlock, wantCode: "content_policy_violation", wantEvaluation: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			legacy := &fakeLegacyEngine{decision: tt.legacy}
			prompt := &fakePromptEngine{mode: tt.mode, decision: tt.prompt, err: tt.promptErr}
			decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{Body: []byte(`{}`)})
			require.Equal(t, tt.wantKind, decision.Kind)
			require.Equal(t, tt.wantCode, decision.ErrorCode)
			require.Equal(t, int64(1), legacy.calls.Load())
			require.Equal(t, tt.wantEnqueue, prompt.enqueues.Load())
			require.Equal(t, tt.wantEvaluation, prompt.evaluates.Load())
		})
	}
}

func TestCoordinatorDoesNotMutateRequestBody(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"hello"}]}`)
	original := append([]byte(nil), body...)
	prompt := &fakePromptEngine{mode: ModeAsync}
	decision := NewCoordinator(&fakeLegacyEngine{}, prompt).Check(context.Background(), Request{Body: body})
	require.True(t, decision.AllowNextStage)
	require.Equal(t, original, body)
}

func TestCoordinatorBlockingPriorityCoversBothEngineDecisionMatrix(t *testing.T) {
	legacyCases := []struct {
		name     string
		decision *LegacyDecision
	}{
		{name: "allow", decision: &LegacyDecision{Allowed: true, StatusCode: http.StatusOK, Action: "allow"}},
		{name: "flag", decision: &LegacyDecision{Allowed: true, Flagged: true, StatusCode: http.StatusOK, Action: "flag"}},
		{name: "block", decision: &LegacyDecision{Blocked: true, StatusCode: http.StatusForbidden, ErrorCode: "legacy_exact_code", Message: "legacy exact message", Action: "block"}},
	}
	promptCases := []struct {
		name     string
		decision *PromptDecision
		wantKind DecisionKind
		wantCode string
	}{
		{name: "allow", decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}, wantKind: DecisionAllow},
		{name: "flag", decision: &PromptDecision{Kind: DecisionFlag, AllowNextStage: true}, wantKind: DecisionFlag},
		{name: "block", decision: &PromptDecision{Kind: DecisionBlock}, wantKind: DecisionBlock, wantCode: ErrorCodeBlocked},
		{name: "unavailable", decision: &PromptDecision{Kind: DecisionUnavailable, ErrorCode: ErrorCodeUnavailable}, wantKind: DecisionUnavailable, wantCode: ErrorCodeUnavailable},
		{name: "invalid", decision: &PromptDecision{Kind: DecisionInvalid, ErrorCode: ErrorCodeInvalidResponse}, wantKind: DecisionInvalid, wantCode: ErrorCodeInvalidResponse},
	}

	for _, legacyCase := range legacyCases {
		for _, promptCase := range promptCases {
			t.Run(fmt.Sprintf("legacy_%s_prompt_%s", legacyCase.name, promptCase.name), func(t *testing.T) {
				legacy := &fakeLegacyEngine{decision: legacyCase.decision}
				prompt := &fakePromptEngine{mode: ModeBlocking, decision: promptCase.decision}
				decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{})

				require.Same(t, legacyCase.decision, decision.Legacy)
				require.Same(t, promptCase.decision, decision.Prompt)
				require.Equal(t, int64(1), legacy.calls.Load())
				require.Equal(t, int64(1), prompt.evaluates.Load())
				if legacyCase.name == "block" {
					require.Equal(t, DecisionBlock, decision.Kind)
					require.Equal(t, "legacy_exact_code", decision.ErrorCode)
					require.Equal(t, "legacy exact message", decision.ClientMessage)
					require.False(t, decision.AllowNextStage)
					return
				}
				require.Equal(t, promptCase.wantKind, decision.Kind)
				require.Equal(t, promptCase.wantCode, decision.ErrorCode)
				require.Equal(t, promptCase.decision.AllowNextStage, decision.AllowNextStage)
			})
		}
	}
}

func TestCoordinatorPreservesIndependentEngineFactsAndMapsOnlyGatewayOutcome(t *testing.T) {
	legacyDecision := &LegacyDecision{
		Allowed: true, Flagged: true, Message: "legacy finding", StatusCode: http.StatusAccepted,
		ErrorCode: "legacy_observation", Action: "legacy_action",
	}
	promptResult := &NormalizedResult{
		Decision: EventCritical, RiskLevel: RiskCritical, Action: ActionBlock,
		Categories: []string{"pii"}, ScannerScores: map[string]float64{"pii": 1},
	}
	promptDecision := &PromptDecision{Kind: DecisionBlock, Result: promptResult}
	decision := NewCoordinator(
		&fakeLegacyEngine{decision: legacyDecision},
		&fakePromptEngine{mode: ModeBlocking, decision: promptDecision},
	).Check(context.Background(), Request{})

	require.Same(t, legacyDecision, decision.Legacy)
	require.Same(t, promptDecision, decision.Prompt)
	require.Same(t, promptResult, decision.Prompt.Result)
	require.Equal(t, "legacy finding", decision.Legacy.Message)
	require.Equal(t, []string{"pii"}, decision.Prompt.Result.Categories)
	require.Equal(t, ErrorCodeBlocked, decision.ErrorCode)
}

func TestCoordinatorAsyncEnqueueFailuresNeverChangeResponseOrDownstreamDispatch(t *testing.T) {
	for _, enqueueErr := range []error{ErrQueueFull, ErrQueueAdmissionBusy, errors.New("redis unavailable"), errors.New("publish failed")} {
		prompt := &fakePromptEngine{mode: ModeAsync, err: enqueueErr}
		decision := NewCoordinator(&fakeLegacyEngine{decision: &LegacyDecision{Allowed: true}}, prompt).Check(context.Background(), Request{})
		downstreamDispatches := 0
		status := http.StatusOK
		responseBody := "unchanged-upstream-response"
		if decision.AllowNextStage {
			downstreamDispatches++
		} else {
			status = decision.HTTPStatus
			responseBody = decision.ClientMessage
		}
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "unchanged-upstream-response", responseBody)
		require.Equal(t, 1, downstreamDispatches)
		require.Equal(t, int64(1), prompt.enqueues.Load())
		require.Zero(t, prompt.evaluates.Load())
	}
}
