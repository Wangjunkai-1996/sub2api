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
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type fakeLegacyEngine struct {
	decision *LegacyDecision
	err      error
	strict   bool
	scopeErr error
	calls    atomic.Int64
	scopes   atomic.Int64
	check    func(context.Context, Request) (*LegacyDecision, error)
}

func (f *fakeLegacyEngine) BlockingApplies(context.Context, Request) (bool, error) {
	f.scopes.Add(1)
	return f.strict, f.scopeErr
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
	if f.strict != nil {
		return *f.strict
	}
	return f.mode == ModeBlocking
}
func (f *fakePromptEngine) Enqueue(context.Context, Request) error {
	f.enqueues.Add(1)
	return f.err
}

type proScopedLegacySpy struct {
	strictGroup     bool
	decision        *LegacyDecision
	err             error
	scopeCalls      atomic.Int64
	moderationCalls atomic.Int64
	strictSeen      atomic.Bool
}

func (s *proScopedLegacySpy) BlockingApplies(_ context.Context, req Request) (bool, error) {
	s.scopeCalls.Add(1)
	return s.strictGroup && service.StrictContentModerationRequestSupported(req.Protocol, req.Model), nil
}

func (s *proScopedLegacySpy) Check(_ context.Context, req Request) (*LegacyDecision, error) {
	if s.strictGroup && !service.StrictContentModerationRequestSupported(req.Protocol, req.Model) {
		return &LegacyDecision{Allowed: true}, nil
	}
	s.moderationCalls.Add(1)
	s.strictSeen.Store(req.Strict)
	return s.decision, s.err
}

type proScopedPromptSpy struct {
	mode      Mode
	decision  *PromptDecision
	err       error
	enqueues  atomic.Int64
	evaluates atomic.Int64
}

func (s *proScopedPromptSpy) EffectiveMode() Mode { return s.mode }

func (s *proScopedPromptSpy) BlockingApplies(req Request) bool {
	return s.mode == ModeBlocking && service.StrictContentModerationRequestSupported(req.Protocol, req.Model)
}

func (s *proScopedPromptSpy) Enqueue(_ context.Context, req Request) error {
	if !service.StrictContentModerationRequestSupported(req.Protocol, req.Model) {
		return nil
	}
	s.enqueues.Add(1)
	return s.err
}

func (s *proScopedPromptSpy) Evaluate(_ context.Context, req Request) (*PromptDecision, error) {
	if !service.StrictContentModerationRequestSupported(req.Protocol, req.Model) {
		return &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}, nil
	}
	s.evaluates.Add(1)
	return s.decision, s.err
}

func TestCoordinatorNilReceiverFailsClosed(t *testing.T) {
	var coordinator *Coordinator
	decision := coordinator.Check(context.Background(), Request{Protocol: "openai_responses", Body: []byte(`{"input":"safe"}`)})

	require.False(t, decision.AllowNextStage)
	require.Equal(t, DecisionUnavailable, decision.Kind)
	require.Equal(t, http.StatusServiceUnavailable, decision.HTTPStatus)
	require.Equal(t, ErrorCodeAuditUnavailable, decision.ErrorCode)
}

func TestCoordinatorContentModerationStrictGateDoesNotRequirePromptAudit(t *testing.T) {
	groupID := int64(12)

	t.Run("legacy allow creates lineage summary with prompt audit off", func(t *testing.T) {
		legacy := &fakeLegacyEngine{strict: true, check: func(_ context.Context, req Request) (*LegacyDecision, error) {
			require.True(t, req.Strict)
			require.NotNil(t, req.Document)
			require.True(t, req.Document.Complete)
			return &LegacyDecision{Allowed: true}, nil
		}}
		prompt := &fakePromptEngine{mode: ModeOff}
		decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
			APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses", Body: []byte(`{"input":"safe"}`),
		})

		require.True(t, decision.AllowNextStage)
		require.Equal(t, DecisionAllow, decision.Kind)
		require.NotNil(t, decision.Audit)
		require.Equal(t, int64(0), decision.Audit.ConfigVersion)
		require.Equal(t, AuditVerdictAllow, decision.Audit.Verdict)
		require.True(t, decision.Audit.ContextComplete)
		require.Equal(t, int64(1), legacy.calls.Load())
		require.Zero(t, prompt.evaluates.Load())
	})

	t.Run("image only input remains structurally complete without a 422", func(t *testing.T) {
		images := make([]string, 59)
		for index := range images {
			if index == 0 {
				images[index] = `{"type":"input_image","id":42,"image_url":{"malformed":true},"future_image_field":"ignored"}`
				continue
			}
			images[index] = fmt.Sprintf(`{"type":"input_image","image_url":"opaque-image-%02d"}`, index)
		}
		body := []byte(`{"store":false,"input":[{"type":"message","role":"user","content":[` + strings.Join(images, ",") + `]}]}`)
		legacy := &fakeLegacyEngine{strict: true, check: func(_ context.Context, req Request) (*LegacyDecision, error) {
			require.True(t, req.Strict)
			require.NotNil(t, req.Document)
			require.True(t, req.Document.Complete, "%+v", req.Document.Issues)
			require.True(t, req.Document.HasImages)
			require.Empty(t, req.Document.Media)
			require.Empty(t, req.Document.NormalizedText)
			return &LegacyDecision{Allowed: true}, nil
		}}
		prompt := &fakePromptEngine{mode: ModeOff}

		decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
			APIKeyID: 7, GroupID: &groupID, Protocol: auditinput.ProtocolOpenAIResponses, Body: body,
		})

		require.True(t, decision.AllowNextStage)
		require.Equal(t, DecisionAllow, decision.Kind)
		require.Equal(t, http.StatusOK, decision.HTTPStatus)
		require.NotNil(t, decision.Audit)
		require.Empty(t, decision.Audit.MediaDigests)
		require.Equal(t, int64(1), legacy.calls.Load())
		require.Zero(t, prompt.evaluates.Load())
	})

	t.Run("incomplete input stops before every auditor", func(t *testing.T) {
		legacy := &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}}
		prompt := &fakePromptEngine{mode: ModeOff}
		decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
			APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses",
			Body: []byte(`{"input":[{"type":"future_item","payload":"opaque"}]}`),
		})

		require.Equal(t, DecisionInvalid, decision.Kind)
		require.Equal(t, http.StatusUnprocessableEntity, decision.HTTPStatus)
		require.Equal(t, ErrorCodeContextIncomplete, decision.ErrorCode)
		require.Zero(t, legacy.calls.Load())
		require.Zero(t, prompt.evaluates.Load())
	})

	t.Run("legacy audit error fails closed", func(t *testing.T) {
		legacy := &fakeLegacyEngine{strict: true, err: errors.New("moderation 429")}
		prompt := &fakePromptEngine{mode: ModeOff}
		decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
			APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses", Body: []byte(`{"input":"safe"}`),
		})

		require.Equal(t, DecisionUnavailable, decision.Kind)
		require.Equal(t, http.StatusServiceUnavailable, decision.HTTPStatus)
		require.Equal(t, ErrorCodeAuditUnavailable, decision.ErrorCode)
		require.Equal(t, int64(1), legacy.calls.Load())
		require.Zero(t, prompt.evaluates.Load())
	})

	t.Run("scope lookup error fails closed before parsing and auditors", func(t *testing.T) {
		legacy := &fakeLegacyEngine{scopeErr: errors.New("settings unavailable")}
		prompt := &fakePromptEngine{mode: ModeOff}
		decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
			APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses", Body: []byte(`{"input":"safe"}`),
		})

		require.Equal(t, DecisionUnavailable, decision.Kind)
		require.Equal(t, ErrorCodeAuditUnavailable, decision.ErrorCode)
		require.Zero(t, legacy.calls.Load())
		require.Zero(t, prompt.evaluates.Load())
	})
}

func TestCoordinatorContentModerationOutOfScopeRemainsNonStrict(t *testing.T) {
	plusGroupID := int64(13)
	for _, test := range []struct {
		name   string
		prompt *fakePromptEngine
	}{
		{name: "prompt audit off", prompt: &fakePromptEngine{mode: ModeOff}},
		{name: "degraded blocking prompt remains out of scope", prompt: func() *fakePromptEngine {
			outOfScope := false
			return &fakePromptEngine{mode: ModeBlocking, strict: &outOfScope, err: errors.New("degraded prompt audit")}
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			legacy := &fakeLegacyEngine{check: func(_ context.Context, req Request) (*LegacyDecision, error) {
				require.False(t, req.Strict)
				require.Nil(t, req.Document)
				return &LegacyDecision{Allowed: true}, nil
			}}
			decision := NewCoordinator(legacy, test.prompt).Check(context.Background(), Request{
				GroupID: &plusGroupID, Protocol: "openai_responses",
				Body: []byte(`{"input":[{"type":"future_item","payload":"opaque"}]}`),
			})

			require.True(t, decision.AllowNextStage)
			require.Nil(t, decision.Audit)
			require.Equal(t, int64(1), legacy.calls.Load())
			require.Zero(t, test.prompt.evaluates.Load())
		})
	}
}

func TestCoordinatorProScopeSkipsNonGPTAndImageAuditors(t *testing.T) {
	groupID := int64(12)
	tests := []struct {
		name     string
		protocol string
		model    string
		body     string
	}{
		{
			name: "Responses non-GPT model", protocol: service.ContentModerationProtocolOpenAIResponses, model: "gemini-3-pro",
			body: `{"previous_response_id":"must_not_load","input":"safe"}`,
		},
		{
			name: "Chat non-GPT model", protocol: service.ContentModerationProtocolOpenAIChat, model: "claude-sonnet-4-5",
			body: `{"messages":[{"role":"user","content":"safe"}]}`,
		},
		{
			name: "Responses image model", protocol: service.ContentModerationProtocolOpenAIResponses, model: "gpt-image-2",
			body: `{"previous_response_id":"must_not_load","input":"draw"}`,
		},
		{
			name: "Chat image model", protocol: service.ContentModerationProtocolOpenAIChat, model: "gpt-image-1.5",
			body: `{"messages":[{"role":"user","content":"draw"}]}`,
		},
		{
			name: "Anthropic protocol", protocol: service.ContentModerationProtocolAnthropicMessages, model: "gpt-5.4",
			body: `{"messages":[{"role":"user","content":"safe"}]}`,
		},
		{
			name: "Gemini protocol", protocol: service.ContentModerationProtocolGemini, model: "gpt-5.4",
			body: `{"contents":[{"role":"user","parts":[{"text":"safe"}]}]}`,
		},
		{
			name: "OpenAI images protocol", protocol: service.ContentModerationProtocolOpenAIImages, model: "gpt-5.4",
			body: `{"prompt":"draw"}`,
		},
	}

	for _, mode := range []Mode{ModeBlocking, ModeAsync} {
		for _, tt := range tests {
			t.Run(string(mode)+"/"+tt.name, func(t *testing.T) {
				legacy := &proScopedLegacySpy{strictGroup: true}
				prompt := &proScopedPromptSpy{mode: mode}
				lineage := &fakeLineageStore{loadErr: errors.New("lineage must not be loaded")}
				decision := NewCoordinator(legacy, prompt).SetLineageStore(lineage).Check(context.Background(), Request{
					APIKeyID: 7, GroupID: &groupID, Protocol: tt.protocol, Model: tt.model, Body: []byte(tt.body),
				})

				require.True(t, decision.AllowNextStage)
				require.Equal(t, DecisionAllow, decision.Kind)
				require.Nil(t, decision.Audit)
				require.Equal(t, int64(1), legacy.scopeCalls.Load())
				require.Zero(t, legacy.moderationCalls.Load())
				require.Zero(t, prompt.enqueues.Load())
				require.Zero(t, prompt.evaluates.Load())
				require.Zero(t, lineage.loads.Load())
			})
		}
	}
}

func TestCoordinatorProScopeKeepsGPTTextFailClosed(t *testing.T) {
	groupID := int64(12)
	tests := []struct {
		name     string
		protocol string
		body     string
	}{
		{name: "Responses", protocol: service.ContentModerationProtocolOpenAIResponses, body: `{"input":"safe"}`},
		{name: "Chat", protocol: service.ContentModerationProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":"safe"}]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name+" moderation unavailable", func(t *testing.T) {
			legacy := &proScopedLegacySpy{strictGroup: true, err: errors.New("moderation unavailable")}
			prompt := &proScopedPromptSpy{mode: ModeBlocking, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}}
			decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
				APIKeyID: 7, GroupID: &groupID, Protocol: tt.protocol, Model: "gpt-5.4", Body: []byte(tt.body),
			})

			require.False(t, decision.AllowNextStage)
			require.Equal(t, DecisionUnavailable, decision.Kind)
			require.Equal(t, http.StatusServiceUnavailable, decision.HTTPStatus)
			require.Equal(t, ErrorCodeAuditUnavailable, decision.ErrorCode)
			require.Equal(t, int64(1), legacy.moderationCalls.Load())
			require.True(t, legacy.strictSeen.Load())
			require.Zero(t, prompt.evaluates.Load())
		})

		t.Run(tt.name+" prompt unavailable", func(t *testing.T) {
			legacy := &proScopedLegacySpy{strictGroup: true, decision: &LegacyDecision{Allowed: true}}
			prompt := &proScopedPromptSpy{mode: ModeBlocking, err: errors.New("prompt unavailable")}
			decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
				APIKeyID: 7, GroupID: &groupID, Protocol: tt.protocol, Model: "gpt-5.4", Body: []byte(tt.body),
			})

			require.False(t, decision.AllowNextStage)
			require.Equal(t, DecisionUnavailable, decision.Kind)
			require.Equal(t, http.StatusServiceUnavailable, decision.HTTPStatus)
			require.Equal(t, ErrorCodeAuditUnavailable, decision.ErrorCode)
			require.Equal(t, int64(1), legacy.moderationCalls.Load())
			require.True(t, legacy.strictSeen.Load())
			require.Equal(t, int64(1), prompt.evaluates.Load())
		})
	}
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
			legacy: &fakeLegacyEngine{strict: true}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict},
			wantKind: DecisionInvalid, wantStatus: http.StatusUnprocessableEntity, wantCode: ErrorCodeContextIncomplete,
		},
		{
			name: "nonempty empty context rejected before engines", body: `{"model":"gpt-test"}`,
			legacy: &fakeLegacyEngine{strict: true}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict},
			wantKind: DecisionInvalid, wantStatus: http.StatusUnprocessableEntity, wantCode: ErrorCodeContextIncomplete,
		},
		{
			name: "inspectable tool output without lineage reaches engines", body: `{"store":false,"input":[{"type":"function_call_output","call_id":"call_1","output":"done"}]}`,
			legacy: &fakeLegacyEngine{strict: true, check: func(_ context.Context, req Request) (*LegacyDecision, error) {
				require.Contains(t, req.AuditContext, "done")
				return &LegacyDecision{Allowed: true}, nil
			}}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionBlock}},
			wantKind: DecisionBlock, wantStatus: http.StatusForbidden, wantCode: ErrorCodePolicyBlocked, legacyCall: 1, promptCall: 1,
		},
		{
			name: "legacy error is unavailable", body: `{"input":"hello"}`,
			legacy: &fakeLegacyEngine{strict: true, err: errors.New("429")}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}},
			wantKind: DecisionUnavailable, wantStatus: http.StatusServiceUnavailable, wantCode: ErrorCodeAuditUnavailable, legacyCall: 1, promptCall: 0,
		},
		{
			name: "prompt flag is blocked", body: `{"input":"hello"}`,
			legacy: &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionFlag, AllowNextStage: true}},
			wantKind: DecisionBlock, wantStatus: http.StatusForbidden, wantCode: ErrorCodePolicyBlocked, legacyCall: 1, promptCall: 1,
		},
		{
			name: "prompt outage is unavailable", body: `{"input":"hello"}`,
			legacy: &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict, err: errors.New("timeout")},
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
		&fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}},
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

func TestCoordinatorStoreFalseBuildsLargeLocalContextWithoutResponseLineage(t *testing.T) {
	groupID := int64(12)
	largeInput := strings.Repeat("x", 1024*1024)
	store := &fakeLineageStore{}
	legacy := &fakeLegacyEngine{strict: true, check: func(_ context.Context, req Request) (*LegacyDecision, error) {
		require.Len(t, []rune(req.AuditContext), len(largeInput))
		require.Equal(t, largeInput, req.AuditContext)
		return &LegacyDecision{Allowed: true}, nil
	}}
	coordinator := NewCoordinator(legacy, &fakePromptEngine{mode: ModeAsync}).SetLineageStore(store)
	body := []byte(fmt.Sprintf(`{"store":false,"input":%q}`, largeInput))

	decision := coordinator.Check(context.Background(), Request{
		APIKeyID: 7, GroupID: &groupID, Protocol: auditinput.ProtocolOpenAIResponses, Body: body,
	})

	require.True(t, decision.AllowNextStage)
	require.NotNil(t, decision.Audit)
	require.True(t, decision.Audit.SkipResponseLineage)
	require.False(t, decision.Audit.ResponseLineageRequired())
	require.Empty(t, decision.Audit.NormalizedContext)
	require.Empty(t, decision.Audit.RedactedContext)
	require.NoError(t, coordinator.BindAllowedResponse(context.Background(), *decision.Audit, "resp_unused"))
	require.Nil(t, store.bound, "store=false full-history requests must not write lineage")
}

func TestResponseLineageSkipRequiresExplicitStoreFalseWithoutParent(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		body     string
		want     bool
	}{
		{name: "responses store false", protocol: auditinput.ProtocolOpenAIResponses, body: `{"store":false,"input":"safe"}`, want: true},
		{name: "responses default store", protocol: auditinput.ProtocolOpenAIResponses, body: `{"input":"safe"}`},
		{name: "responses store true", protocol: auditinput.ProtocolOpenAIResponses, body: `{"store":true,"input":"safe"}`},
		{name: "continuation still binds", protocol: auditinput.ProtocolOpenAIResponses, body: `{"store":false,"previous_response_id":"resp_parent","input":"safe"}`},
		{name: "chat is unrelated", protocol: auditinput.ProtocolOpenAIChat, body: `{"store":false,"messages":[{"role":"user","content":"safe"}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			document := auditinput.Parse(tt.protocol, []byte(tt.body))
			require.True(t, document.Complete, "%+v", document.Issues)
			require.Equal(t, tt.want, skipsResponseLineage(Request{
				Protocol: tt.protocol, Body: []byte(tt.body), Document: document,
			}))
		})
	}
}

func TestStrictInputIssueSummaryRedactsRawPaths(t *testing.T) {
	summary := summarizeStrictInputIssues([]auditinput.Issue{
		{Code: auditinput.IssueEncryptedContent, Path: "$.input[3].encrypted_content"},
		{Code: auditinput.IssueEncryptedContent, Path: "$.input[9].signature"},
		{Code: auditinput.IssueUnknownField, Path: "$.tools[1].secret_schema_key"},
	})

	require.Equal(t, []strictInputIssueLog{
		{Code: auditinput.IssueEncryptedContent, PathClass: "responses_input_item", Count: 2},
		{Code: auditinput.IssueUnknownField, PathClass: "tool_definition", Count: 1},
	}, summary)
	require.NotContains(t, fmt.Sprint(summary), "input[3]")
	require.NotContains(t, fmt.Sprint(summary), "signature")
	require.NotContains(t, fmt.Sprint(summary), "secret_schema_key")
}

type fakeLineageStore struct {
	loaded     *AuditSummary
	loadErr    error
	lookup     LineageLookup
	bound      *AuditSummary
	responseID string
	loads      atomic.Int64
}

func (s *fakeLineageStore) Load(_ context.Context, lookup LineageLookup) (*AuditSummary, error) {
	s.loads.Add(1)
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
		&fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}},
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

	missing := NewCoordinator(&fakeLegacyEngine{strict: true}, &fakePromptEngine{mode: ModeBlocking, strict: &strict}).SetLineageStore(&fakeLineageStore{loadErr: ErrLineageNotFound})
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
		&fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}},
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

func TestCoordinatorStrictLineageMaintainsCumulativeLocalContext(t *testing.T) {
	groupID := int64(12)
	strict := true

	t.Run("split keyword blocks before prompt guard", func(t *testing.T) {
		store := &fakeLineageStore{loaded: &AuditSummary{
			Verdict: AuditVerdictAllow, ContextComplete: true, APIKeyID: 7, GroupID: &groupID,
			RedactedContext: "cy", PromptHash: "parent-hash",
		}}
		legacy := &fakeLegacyEngine{strict: true, check: func(_ context.Context, req Request) (*LegacyDecision, error) {
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

	t.Run("prompt stage retains local lineage context while body remains current turn", func(t *testing.T) {
		store := &fakeLineageStore{loaded: &AuditSummary{
			Verdict: AuditVerdictAllow, ContextComplete: true, APIKeyID: 7, GroupID: &groupID,
			RedactedContext: "harmful", PromptHash: "parent-hash",
		}}
		legacy := &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}}
		prompt := &fakePromptEngine{mode: ModeBlocking, strict: &strict, evaluate: func(_ context.Context, req Request) (*PromptDecision, error) {
			require.Equal(t, "harmful\nrequest", req.AuditContext)
			require.JSONEq(t, `{"previous_response_id":"resp_parent","input":"request"}`, string(req.Body))
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
