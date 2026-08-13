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
	return s.strictGroup, nil
}

func (s *proScopedLegacySpy) Check(_ context.Context, req Request) (*LegacyDecision, error) {
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
	return s.mode == ModeBlocking
}

func (s *proScopedPromptSpy) Enqueue(_ context.Context, req Request) error {
	s.enqueues.Add(1)
	return s.err
}

func (s *proScopedPromptSpy) Evaluate(_ context.Context, req Request) (*PromptDecision, error) {
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
				images[index] = `{"type":"input_image","image_url":"opaque-image-00"}`
				continue
			}
			images[index] = fmt.Sprintf(`{"type":"input_image","image_url":"opaque-image-%02d"}`, index)
		}
		body := []byte(`{"store":false,"input":[{"type":"message","role":"user","content":[` + strings.Join(images, ",") + `]}]}`)
		legacy := &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Blocked: true}}
		prompt := &fakePromptEngine{mode: ModeOff}

		decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
			APIKeyID: 7, GroupID: &groupID, Protocol: auditinput.ProtocolOpenAIResponses, Body: body,
		})

		require.True(t, decision.AllowNextStage)
		require.Equal(t, DecisionAllow, decision.Kind)
		require.Equal(t, http.StatusOK, decision.HTTPStatus)
		require.NotNil(t, decision.Audit)
		require.Empty(t, decision.Audit.MediaDigests)
		require.Zero(t, legacy.calls.Load())
		require.Zero(t, prompt.evaluates.Load())
	})

	t.Run("invalid input stops before every auditor", func(t *testing.T) {
		legacy := &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}}
		prompt := &fakePromptEngine{mode: ModeOff}
		decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
			APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses",
			Body: []byte(`{"input":[{"type":"input_text","role":null,"text":"blocked current text"}]}`),
		})

		require.Equal(t, DecisionBlock, decision.Kind)
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

func TestCoordinatorStrictScopeDoesNotDependOnProtocolOrModel(t *testing.T) {
	groupID := int64(12)
	tests := []struct {
		name     string
		protocol string
		model    string
		body     string
	}{
		{
			name: "Responses non-GPT model", protocol: service.ContentModerationProtocolOpenAIResponses, model: "gemini-3-pro",
			body: `{"input":"safe"}`,
		},
		{
			name: "Chat non-GPT model", protocol: service.ContentModerationProtocolOpenAIChat, model: "claude-sonnet-4-5",
			body: `{"messages":[{"role":"user","content":"safe"}]}`,
		},
		{
			name: "Responses image model", protocol: service.ContentModerationProtocolOpenAIResponses, model: "gpt-image-2",
			body: `{"input":"draw"}`,
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
				legacy := &proScopedLegacySpy{strictGroup: true, decision: &LegacyDecision{Allowed: true}}
				prompt := &proScopedPromptSpy{mode: mode, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}}
				decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
					APIKeyID: 7, GroupID: &groupID, Protocol: tt.protocol, Model: tt.model, Body: []byte(tt.body),
				})

				require.True(t, decision.AllowNextStage)
				require.Equal(t, DecisionAllow, decision.Kind)
				require.NotNil(t, decision.Audit)
				require.Equal(t, int64(1), legacy.scopeCalls.Load())
				require.Equal(t, int64(1), legacy.moderationCalls.Load())
				if mode == ModeBlocking {
					require.Equal(t, int64(1), prompt.evaluates.Load())
					require.Zero(t, prompt.enqueues.Load())
				} else {
					require.Zero(t, prompt.evaluates.Load())
					require.Equal(t, int64(1), prompt.enqueues.Load())
				}
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
			name: "malformed current text rejected before engines", body: `{"input":[{"type":"input_text","role":null,"text":"blocked current text"}]}`,
			legacy: &fakeLegacyEngine{strict: true}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict},
			wantKind: DecisionBlock, wantStatus: http.StatusUnprocessableEntity, wantCode: ErrorCodeContextIncomplete,
		},
		{
			name: "legacy error is unavailable", body: `{"input":"hello"}`,
			legacy: &fakeLegacyEngine{strict: true, err: errors.New("429")}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}},
			wantKind: DecisionUnavailable, wantStatus: http.StatusServiceUnavailable, wantCode: ErrorCodeAuditUnavailable, legacyCall: 1, promptCall: 0,
		},
		{
			name: "prompt flag is blocked", body: `{"input":"hello"}`,
			legacy: &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true, StatusCode: http.StatusUnavailableForLegalReasons}}, prompt: &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionFlag, AllowNextStage: true}},
			wantKind: DecisionBlock, wantStatus: http.StatusUnavailableForLegalReasons, wantCode: ErrorCodePolicyBlocked, legacyCall: 1, promptCall: 1,
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

func TestCoordinatorStrictSafeEmptyTurnsSkipAllEngines(t *testing.T) {
	groupID := int64(12)
	strict := true
	tests := []struct {
		name     string
		protocol string
		body     string
	}{
		{name: "responses missing input", protocol: auditinput.ProtocolOpenAIResponses, body: `{"model":"gpt-test"}`},
		{name: "responses null input", protocol: auditinput.ProtocolOpenAIResponses, body: `{"model":"gpt-test","input":null}`},
		{name: "responses empty websocket input", protocol: auditinput.ProtocolOpenAIResponses, body: `{"type":"response.create","model":"gpt-test","input":[]}`},
		{name: "chat missing messages", protocol: auditinput.ProtocolOpenAIChat, body: `{"model":"gpt-test"}`},
		{name: "chat empty messages", protocol: auditinput.ProtocolOpenAIChat, body: `{"model":"gpt-test","messages":[]}`},
		{name: "chat trailing assistant", protocol: auditinput.ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":"historical"},{"role":"assistant","content":"done"}]}`},
		{name: "responses pure compaction", protocol: auditinput.ProtocolOpenAIResponses, body: `{"input":[{"type":"compaction_trigger"}]}`},
		{name: "responses pure image", protocol: auditinput.ProtocolOpenAIResponses, body: `{"input":[{"type":"input_image","image_url":"opaque"}]}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			legacy := &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Blocked: true}}
			prompt := &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionBlock}}
			decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
				APIKeyID: 7, GroupID: &groupID, Protocol: test.protocol, Body: []byte(test.body),
			})

			require.True(t, decision.AllowNextStage)
			require.Equal(t, DecisionAllow, decision.Kind)
			require.Equal(t, http.StatusOK, decision.HTTPStatus)
			require.NotNil(t, decision.Audit)
			require.True(t, decision.Audit.ContextComplete)
			require.Zero(t, legacy.calls.Load())
			require.Zero(t, prompt.evaluates.Load())
		})
	}
}

func TestCoordinatorStrictOpaqueTailFailsClosedWithoutRewindingHistory(t *testing.T) {
	groupID := int64(12)
	strict := true
	for _, body := range []string{
		`{"input":[{"type":"message","role":"user","content":"historical"},null]}`,
		`{"input":[{"type":"message","role":"user","content":"historical"},42]}`,
	} {
		legacy := &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}}
		prompt := &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}}
		decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
			APIKeyID: 7, GroupID: &groupID, Protocol: auditinput.ProtocolOpenAIResponses, Body: []byte(body),
		})

		require.Equal(t, DecisionBlock, decision.Kind)
		require.Equal(t, http.StatusUnprocessableEntity, decision.HTTPStatus)
		require.Equal(t, ErrorCodeContextIncomplete, decision.ErrorCode)
		require.Zero(t, legacy.calls.Load())
		require.Zero(t, prompt.evaluates.Load())
	}
}

func TestCoordinatorStrictResponsesToolOutputRunsAuditors(t *testing.T) {
	groupID := int64(12)
	strict := true
	legacy := &fakeLegacyEngine{strict: true, check: func(_ context.Context, req Request) (*LegacyDecision, error) {
		require.Equal(t, "dangerous tool output", req.Document.NormalizedText)
		require.NotContains(t, req.Document.NormalizedText, "historical")
		return &LegacyDecision{Allowed: true}, nil
	}}
	prompt := &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}}
	decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
		APIKeyID: 7, GroupID: &groupID, Protocol: auditinput.ProtocolOpenAIResponses,
		Body: []byte(`{"store":false,"input":[{"type":"message","role":"user","content":"historical"},{"type":"function_call_output","call_id":"call_1","output":"dangerous tool output"}]}`),
	})

	require.True(t, decision.AllowNextStage)
	require.Equal(t, int64(1), legacy.calls.Load())
	require.Equal(t, int64(1), prompt.evaluates.Load())
}

func TestCoordinatorStrictProductionShapedCyberToolIncrementBlocksBeforeUpstream(t *testing.T) {
	groupID := int64(12)
	strict := true
	tests := []struct {
		name string
		item string
	}{
		{
			name: "function call arguments",
			item: `{"type":"function_call","name":"synthetic_runner","arguments":"{\"task\":\"synthetic cyber policy marker\"}","call_id":"call_synthetic"}`,
		},
		{
			name: "function call output",
			item: `{"type":"function_call_output","call_id":"call_synthetic","output":"synthetic cyber policy marker"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			legacy := &fakeLegacyEngine{strict: true, check: func(_ context.Context, req Request) (*LegacyDecision, error) {
				require.NotNil(t, req.Document)
				require.True(t, req.Document.Complete, "%+v", req.Document.Issues)
				require.Contains(t, req.Document.FoldedText, "cyber")
				require.NotContains(t, req.Document.NormalizedText, "synthetic root instruction marker")
				require.NotContains(t, req.Document.NormalizedText, "synthetic historical user marker")
				require.NotContains(t, req.Document.NormalizedText, "synthetic historical assistant marker")
				require.NotEmpty(t, req.Document.Segments)
				for _, segment := range req.Document.Segments {
					require.True(t, strings.HasPrefix(segment.Path, "$.input[2]"), segment.Path)
				}
				return &LegacyDecision{Blocked: true, Flagged: true}, nil
			}}
			prompt := &fakePromptEngine{
				mode: ModeBlocking, strict: &strict,
				decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true},
			}
			body := []byte(`{
				"model":"gpt-5.5",
				"store":false,
				"instructions":"synthetic root instruction marker",
				"input":[
					{"type":"message","role":"user","content":"synthetic historical user marker"},
					{"type":"message","role":"assistant","content":"synthetic historical assistant marker"},
					` + tt.item + `
				]
			}`)

			decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
				RequestID: "d128b2c4-3875-45b7-823e-fa0b4960053e",
				APIKeyID:  7, GroupID: &groupID, Protocol: auditinput.ProtocolOpenAIResponses, Body: body,
			})

			require.Equal(t, DecisionBlock, decision.Kind)
			require.Equal(t, http.StatusForbidden, decision.HTTPStatus)
			require.Equal(t, ErrorCodePolicyBlocked, decision.ErrorCode)
			require.False(t, decision.AllowNextStage)
			require.Nil(t, decision.Audit)
			require.Equal(t, int64(1), legacy.calls.Load())
			require.Zero(t, prompt.evaluates.Load())
		})
	}
}

func TestCoordinatorStrictSanitizedProductionRegressionMatrix(t *testing.T) {
	groupID := int64(12)
	tests := []struct {
		name      string
		requestID string
		model     string
		body      string
		legacy    *LegacyDecision
		legacyErr error
		wantKind  DecisionKind
		wantCode  string
		wantHTTP  int
	}{
		{
			name: "cyber tool call without name", requestID: "d128b2c4-3875-45b7-823e-fa0b4960053e", model: "gpt-5.5",
			body:   `{"store":false,"instructions":"sanitized root instruction","input":[{"type":"message","role":"user","content":"sanitized historical user"},{"type":"message","role":"assistant","content":"sanitized historical assistant"},{"type":"function_call","call_id":"call_synthetic","arguments":"{\"task\":\"synthetic cyber policy marker\"}"}]}`,
			legacy: &LegacyDecision{Blocked: true, Flagged: true}, wantKind: DecisionBlock, wantCode: ErrorCodePolicyBlocked, wantHTTP: http.StatusForbidden,
		},
		{
			name: "illicit current text", requestID: "2e157a35-244d-49cd-baee-db133d18e432", model: "gpt-5.6-terra",
			body:   `{"input":[{"type":"message","role":"user","content":"synthetic illicit policy marker"}]}`,
			legacy: &LegacyDecision{Blocked: true, Flagged: true}, wantKind: DecisionBlock, wantCode: ErrorCodePolicyBlocked, wantHTTP: http.StatusForbidden,
		},
		{
			name: "harassment tool output", requestID: "e740da58-d7d7-4369-aec5-4b79f2a106c0", model: "gpt-5.6-sol",
			body:   `{"store":false,"input":[{"type":"message","role":"user","content":"sanitized history"},{"type":"function_call_output","call_id":"call_synthetic","output":"synthetic harassment policy marker"}]}`,
			legacy: &LegacyDecision{Blocked: true, Flagged: true}, wantKind: DecisionBlock, wantCode: ErrorCodePolicyBlocked, wantHTTP: http.StatusForbidden,
		},
		{
			name: "violence current text", requestID: "4dcc1742-9191-45b9-a607-8ea73c96b4c9", model: "gpt-5.5",
			body:   `{"input":"synthetic violence policy marker"}`,
			legacy: &LegacyDecision{Blocked: true, Flagged: true}, wantKind: DecisionBlock, wantCode: ErrorCodePolicyBlocked, wantHTTP: http.StatusForbidden,
		},
		{
			name: "moderations unavailable", requestID: "sanitized-moderations-429", model: "codex-auto-review",
			body:      `{"input":"synthetic safe current turn"}`,
			legacyErr: errors.New("moderations upstream 429"), wantKind: DecisionUnavailable, wantCode: ErrorCodeAuditUnavailable, wantHTTP: http.StatusServiceUnavailable,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			legacy := &fakeLegacyEngine{strict: true, decision: tt.legacy, err: tt.legacyErr, check: func(_ context.Context, req Request) (*LegacyDecision, error) {
				require.Equal(t, tt.requestID, req.RequestID)
				require.Equal(t, tt.model, req.Model)
				require.NotNil(t, req.Document)
				require.True(t, req.Document.Complete, "%+v", req.Document.Issues)
				require.NotContains(t, req.Document.NormalizedText, "sanitized historical user")
				require.NotContains(t, req.Document.NormalizedText, "sanitized historical assistant")
				return tt.legacy, tt.legacyErr
			}}
			decision := NewCoordinator(legacy, &fakePromptEngine{mode: ModeOff}).Check(context.Background(), Request{
				RequestID: tt.requestID, APIKeyID: 7, GroupID: &groupID,
				Protocol: auditinput.ProtocolOpenAIResponses, Model: tt.model, Body: []byte(tt.body),
			})

			require.Equal(t, tt.wantKind, decision.Kind)
			require.Equal(t, tt.wantCode, decision.ErrorCode)
			require.Equal(t, tt.wantHTTP, decision.HTTPStatus)
			require.False(t, decision.AllowNextStage)
			require.Equal(t, int64(1), legacy.calls.Load())
		})
	}
}

func TestCoordinatorStrictProductionVolumeBoundariesUseCurrentAuditableText(t *testing.T) {
	groupID := int64(12)
	models := []string{"gpt-5.5", "gpt-5.5-fast", "gpt-5.6-sol", "gpt-5.6-terra", "codex-auto-review"}
	for _, model := range models {
		model := model
		t.Run(model+" at limit", func(t *testing.T) {
			legacy := &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}}
			decision := NewCoordinator(legacy, &fakePromptEngine{mode: ModeOff}).Check(context.Background(), Request{
				APIKeyID: 7, GroupID: &groupID, Protocol: auditinput.ProtocolOpenAIResponses, Model: model,
				Body: []byte(fmt.Sprintf(`{"store":false,"instructions":%q,"input":%q}`,
					strings.Repeat("historical tool definition ", 20_000), strings.Repeat("界", 12_000))),
			})

			require.True(t, decision.AllowNextStage)
			require.Equal(t, DecisionAllow, decision.Kind)
			require.Equal(t, int64(1), legacy.calls.Load())
		})

		t.Run(model+" over former local limit", func(t *testing.T) {
			legacy := &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}}
			decision := NewCoordinator(legacy, &fakePromptEngine{mode: ModeOff}).Check(context.Background(), Request{
				APIKeyID: 7, GroupID: &groupID, Protocol: auditinput.ProtocolOpenAIResponses, Model: model,
				Body: []byte(fmt.Sprintf(`{"store":false,"input":%q}`, strings.Repeat("界", 12_001))),
			})

			require.True(t, decision.AllowNextStage)
			require.Equal(t, DecisionAllow, decision.Kind)
			require.Equal(t, http.StatusOK, decision.HTTPStatus)
			require.Equal(t, int64(1), legacy.calls.Load())
		})
	}
}

func TestCoordinatorStrictAuditDoesNotRejectTwelveThousandPlusOneLocally(t *testing.T) {
	groupID := int64(12)
	legacy := &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}}
	decision := NewCoordinator(legacy, &fakePromptEngine{mode: ModeOff}).Check(context.Background(), Request{
		APIKeyID: 7, GroupID: &groupID, Protocol: auditinput.ProtocolOpenAIResponses,
		Body: []byte(`{"input":"` + strings.Repeat("界", 12_001) + `"}`),
	})

	require.Equal(t, DecisionAllow, decision.Kind)
	require.Equal(t, http.StatusOK, decision.HTTPStatus)
	require.True(t, decision.AllowNextStage)
	require.Equal(t, int64(1), legacy.calls.Load())
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
	largeInput := strings.Repeat("x", 12_001)
	store := &fakeLineageStore{}
	legacy := &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}}
	coordinator := NewCoordinator(legacy, &fakePromptEngine{mode: ModeAsync}).SetLineageStore(store)
	body := []byte(fmt.Sprintf(`{"store":false,"input":%q}`, largeInput))

	decision := coordinator.Check(context.Background(), Request{
		APIKeyID: 7, GroupID: &groupID, Protocol: auditinput.ProtocolOpenAIResponses, Body: body,
	})

	require.True(t, decision.AllowNextStage)
	require.Equal(t, DecisionAllow, decision.Kind)
	require.Equal(t, http.StatusOK, decision.HTTPStatus)
	require.NotNil(t, decision.Audit)
	require.Equal(t, int64(1), legacy.calls.Load())
	require.Nil(t, store.bound)
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
		{Code: auditinput.IssueUnknownField, Path: "$.codex_output_schema"},
	})

	require.Equal(t, []strictInputIssueLog{
		{Code: auditinput.IssueEncryptedContent, PathClass: "responses_input_item", RootField: "", Count: 2},
		{Code: auditinput.IssueUnknownField, PathClass: "other", RootField: "codex_output_schema", Count: 1},
		{Code: auditinput.IssueUnknownField, PathClass: "tool_definition", RootField: "", Count: 1},
	}, summary)
	require.NotContains(t, fmt.Sprint(summary), "input[3]")
	require.NotContains(t, fmt.Sprint(summary), "signature")
	require.NotContains(t, fmt.Sprint(summary), "secret_schema_key")
	require.Contains(t, fmt.Sprint(summary), "codex_output_schema")
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
	require.Equal(t, ErrorCodeLineageIncompatible, missingDecision.ErrorCode)
	require.Equal(t, http.StatusForbidden, missingDecision.HTTPStatus)
}

func TestCoordinatorEmptyContinuationStillRequiresValidLineageBeforeSkippingEngines(t *testing.T) {
	groupID := int64(12)
	strict := true
	prior := &AuditSummary{
		ParserVersion: auditinput.ParserVersion, Verdict: AuditVerdictAllow, ContextComplete: true,
		APIKeyID: 7, GroupID: &groupID, RedactedContext: "verified parent context", PromptHash: "parent-hash",
	}
	store := &fakeLineageStore{loaded: prior}
	legacy := &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Blocked: true}}
	prompt := &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionBlock}}
	decision := NewCoordinator(legacy, prompt).SetLineageStore(store).Check(context.Background(), Request{
		APIKeyID: 7, GroupID: &groupID, Protocol: auditinput.ProtocolOpenAIResponses,
		Body: []byte(`{"model":"gpt-test","previous_response_id":"resp_parent","input":[]}`),
	})

	require.True(t, decision.AllowNextStage)
	require.Equal(t, DecisionAllow, decision.Kind)
	require.Equal(t, int64(1), store.loads.Load())
	require.Equal(t, "resp_parent", store.lookup.PreviousResponseID)
	require.NotNil(t, decision.Audit)
	require.Equal(t, "verified parent context", decision.Audit.NormalizedContext)
	require.Zero(t, legacy.calls.Load())
	require.Zero(t, prompt.evaluates.Load())

	missingLegacy := &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}}
	missingPrompt := &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}}
	missing := NewCoordinator(missingLegacy, missingPrompt).SetLineageStore(&fakeLineageStore{loadErr: ErrLineageNotFound}).Check(context.Background(), Request{
		APIKeyID: 7, GroupID: &groupID, Protocol: auditinput.ProtocolOpenAIResponses,
		Body: []byte(`{"model":"gpt-test","previous_response_id":"missing","input":null}`),
	})

	require.False(t, missing.AllowNextStage)
	require.Equal(t, DecisionBlock, missing.Kind)
	require.Equal(t, http.StatusForbidden, missing.HTTPStatus)
	require.Equal(t, ErrorCodeLineageIncompatible, missing.ErrorCode)
	require.Zero(t, missingLegacy.calls.Load())
	require.Zero(t, missingPrompt.evaluates.Load())
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

	t.Run("split keyword is retained locally but does not block current turn", func(t *testing.T) {
		store := &fakeLineageStore{loaded: &AuditSummary{
			Verdict: AuditVerdictAllow, ContextComplete: true, APIKeyID: 7, GroupID: &groupID,
			RedactedContext: "cy", PromptHash: "parent-hash",
		}}
		legacy := &fakeLegacyEngine{strict: true, check: func(_ context.Context, req Request) (*LegacyDecision, error) {
			require.Equal(t, "cy\nber", req.AuditContext)
			require.Contains(t, string(req.Body), `"input":"ber"`)
			require.NotNil(t, req.Document)
			require.Equal(t, "ber", req.Document.NormalizedText)
			if strings.Contains(strings.ToLower(auditinput.FoldForMatching(req.Document.NormalizedText)), "cyber") {
				return &LegacyDecision{Blocked: true, Flagged: true}, nil
			}
			return &LegacyDecision{Allowed: true}, nil
		}}
		prompt := &fakePromptEngine{mode: ModeBlocking, strict: &strict, decision: &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}}
		decision := NewCoordinator(legacy, prompt).SetLineageStore(store).Check(context.Background(), Request{
			APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses",
			Body: []byte(`{"previous_response_id":"resp_parent","input":"ber"}`),
		})
		require.Equal(t, DecisionAllow, decision.Kind)
		require.True(t, decision.AllowNextStage)
		require.NotNil(t, decision.Audit)
		require.Equal(t, "cy\nber", decision.Audit.NormalizedContext)
		require.Equal(t, int64(1), legacy.calls.Load())
		require.Equal(t, int64(1), prompt.evaluates.Load())
	})

	t.Run("prompt stage receives lineage summary but evaluates current turn", func(t *testing.T) {
		store := &fakeLineageStore{loaded: &AuditSummary{
			Verdict: AuditVerdictAllow, ContextComplete: true, APIKeyID: 7, GroupID: &groupID,
			RedactedContext: "harmful", PromptHash: "parent-hash",
		}}
		legacy := &fakeLegacyEngine{strict: true, decision: &LegacyDecision{Allowed: true}}
		prompt := &fakePromptEngine{mode: ModeBlocking, strict: &strict, evaluate: func(_ context.Context, req Request) (*PromptDecision, error) {
			require.Equal(t, "harmful\nrequest", req.AuditContext)
			require.JSONEq(t, `{"previous_response_id":"resp_parent","input":"request"}`, string(req.Body))
			require.NotNil(t, req.Document)
			require.Equal(t, "request", req.Document.NormalizedText)
			if strings.Contains(strings.ToLower(auditinput.FoldForMatching(req.Document.NormalizedText)), "harmful") {
				return &PromptDecision{Kind: DecisionBlock}, nil
			}
			return &PromptDecision{Kind: DecisionAllow, AllowNextStage: true}, nil
		}}
		decision := NewCoordinator(legacy, prompt).SetLineageStore(store).Check(context.Background(), Request{
			APIKeyID: 7, GroupID: &groupID, Protocol: "openai_responses",
			Body: []byte(`{"previous_response_id":"resp_parent","input":"request"}`),
		})
		require.Equal(t, DecisionAllow, decision.Kind)
		require.True(t, decision.AllowNextStage)
		require.NotNil(t, decision.Audit)
		require.Equal(t, "harmful\nrequest", decision.Audit.NormalizedContext)
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
