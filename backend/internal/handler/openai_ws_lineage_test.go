package handler

import (
	"errors"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func openAIWSLineageTestAccountAdmission(class securityadmission.AccountClass) *service.OpenAIAccountRequirementAdmission {
	selected := &service.Account{ID: 11, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth}
	owner := &service.Account{ID: 11, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth}
	return &service.OpenAIAccountRequirementAdmission{
		Selected:                 selected,
		EffectiveCredentialOwner: owner,
		Requirement:              securityadmission.AccountRequirementAny,
		AccountClass:             class,
	}
}

func openAIWSLineageTestState(t *testing.T, body string) *openAISecurityAdmissionState {
	t.Helper()
	state, err := classifyOpenAISecurityAdmission(
		string(securityadmission.ProtocolResponsesWebSocket),
		[]byte(body),
		securityadmission.LineageUntrusted,
	)
	require.NoError(t, err)
	return state
}

func establishOpenAIWSLineageProof(t *testing.T, tracker *openAIWSLineageTracker) {
	t.Helper()
	state := openAIWSLineageTestState(t, `{"type":"response.create","model":"gpt-5.1","input":"first-turn-canary"}`)
	require.Equal(t, securityadmission.RequestAuditableText, state.admission.Class())
	tracker.MarkTurnAdmitted(1, state, openAIWSLineageTestAccountAdmission(securityadmission.AccountAuditRequired), true)
	tracker.CompleteTurn(1, &service.OpenAIForwardResult{
		RequestID:             "resp_local_1",
		UpstreamTerminalEvent: "response.completed",
	}, nil)
}

func TestOpenAIWSLineageTrackerTrustsOnlyExactLocalCompletedChain(t *testing.T) {
	tracker := newOpenAIWSLineageTracker()
	establishOpenAIWSLineageProof(t, tracker)

	payload := []byte(`{"type":"response.create","previous_response_id":"resp_local_1","input":"second-turn-canary"}`)
	state, err := classifyOpenAISecurityAdmissionWithOptions(
		string(securityadmission.ProtocolResponsesWebSocket), payload, securityadmission.Options{
			ResolveLineage: func(previousResponseID string) securityadmission.LineageTrust {
				return tracker.ResolvePreviousResponseID(previousResponseID, 11)
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, securityadmission.LineageTrusted, state.admission.Lineage())
	require.Equal(t, securityadmission.RequestAuditableText, state.admission.Class())
	require.Equal(t, securityadmission.AccountRequirementAny, state.admission.Requirement())
}

func TestOpenAIWSLineageTrackerMismatchAndAccountChangeClearProof(t *testing.T) {
	tests := []struct {
		name      string
		payload   string
		accountID int64
	}{
		{name: "missing previous response", payload: `{"type":"response.create","input":"next"}`, accountID: 11},
		{name: "different response", payload: `{"type":"response.create","previous_response_id":"resp_other","input":"next"}`, accountID: 11},
		{name: "different account", payload: `{"type":"response.create","previous_response_id":"resp_local_1","input":"next"}`, accountID: 12},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracker := newOpenAIWSLineageTracker()
			establishOpenAIWSLineageProof(t, tracker)
			state, err := classifyOpenAISecurityAdmissionWithOptions(
				string(securityadmission.ProtocolResponsesWebSocket), []byte(test.payload), securityadmission.Options{
					ResolveLineage: func(previousResponseID string) securityadmission.LineageTrust {
						return tracker.ResolvePreviousResponseID(previousResponseID, test.accountID)
					},
				},
			)
			require.NoError(t, err)
			require.Equal(t, securityadmission.LineageUntrusted, state.admission.Lineage())
			require.Equal(t, securityadmission.LineageUntrusted, tracker.ResolvePreviousResponseID("resp_local_1", 11),
				"a mismatch must invalidate the old proof rather than allow later reuse")
		})
	}
}

func TestOpenAIWSLineageTrackerRequiresBlockingProOrKnownNoText(t *testing.T) {
	auditable := openAIWSLineageTestState(t, `{"type":"response.create","input":"auditable"}`)
	knownNoText := openAIWSLineageTestState(t, `{"type":"response.create","input":[]}`)
	require.Equal(t, securityadmission.RequestKnownNoText, knownNoText.admission.Class())

	tests := []struct {
		name           string
		state          *openAISecurityAdmissionState
		accountClass   securityadmission.AccountClass
		blockingPassed bool
		want           securityadmission.LineageTrust
	}{
		{name: "Pro blocking passed", state: auditable, accountClass: securityadmission.AccountAuditRequired, blockingPassed: true, want: securityadmission.LineageTrusted},
		{name: "Pro blocking missing", state: auditable, accountClass: securityadmission.AccountAuditRequired, blockingPassed: false, want: securityadmission.LineageUntrusted},
		{name: "exempt text was not scanned", state: auditable, accountClass: securityadmission.AccountAuditExemptVerified, blockingPassed: true, want: securityadmission.LineageUntrusted},
		{name: "canonical no text", state: knownNoText, accountClass: securityadmission.AccountAuditRequired, blockingPassed: false, want: securityadmission.LineageTrusted},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracker := newOpenAIWSLineageTracker()
			tracker.MarkTurnAdmitted(1, test.state, openAIWSLineageTestAccountAdmission(test.accountClass), test.blockingPassed)
			tracker.CompleteTurn(1, &service.OpenAIForwardResult{
				ResponseID:            "resp_local_1",
				UpstreamTerminalEvent: "response.done",
			}, nil)
			got := tracker.ResolvePreviousResponseID("resp_local_1", 11)
			require.Equal(t, test.want, got)
		})
	}
}

func TestOpenAIWSLineageTrackerRejectsNonTerminalAndInvalidResponseIDs(t *testing.T) {
	tests := []struct {
		name   string
		result *service.OpenAIForwardResult
		err    error
	}{
		{name: "turn error", result: &service.OpenAIForwardResult{RequestID: "resp_local_1", UpstreamTerminalEvent: "response.completed"}, err: errors.New("relay failed")},
		{name: "failed terminal", result: &service.OpenAIForwardResult{RequestID: "resp_local_1", UpstreamTerminalEvent: "response.failed"}},
		{name: "incomplete terminal", result: &service.OpenAIForwardResult{RequestID: "resp_local_1", UpstreamTerminalEvent: "response.incomplete"}},
		{name: "missing response id", result: &service.OpenAIForwardResult{UpstreamTerminalEvent: "response.completed"}},
		{name: "message id", result: &service.OpenAIForwardResult{RequestID: "msg_not_a_response", UpstreamTerminalEvent: "response.completed"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracker := newOpenAIWSLineageTracker()
			state := openAIWSLineageTestState(t, `{"type":"response.create","input":"first"}`)
			tracker.MarkTurnAdmitted(1, state, openAIWSLineageTestAccountAdmission(securityadmission.AccountAuditRequired), true)
			tracker.CompleteTurn(1, test.result, test.err)
			require.Equal(t, securityadmission.LineageUntrusted, tracker.ResolvePreviousResponseID("resp_local_1", 11))
		})
	}
}

func TestOpenAIWSLineageTrackerCannotBypassCanonicalDuplicateKeyCheck(t *testing.T) {
	tracker := newOpenAIWSLineageTracker()
	establishOpenAIWSLineageProof(t, tracker)
	payload := []byte(`{"type":"response.create","previous_response_id":"resp_local_1","previous_response_id":"resp_local_1","input":"next"}`)
	state, err := classifyOpenAISecurityAdmissionWithOptions(
		string(securityadmission.ProtocolResponsesWebSocket), payload, securityadmission.Options{
			ResolveLineage: func(previousResponseID string) securityadmission.LineageTrust {
				return tracker.ResolvePreviousResponseID(previousResponseID, 11)
			},
		},
	)
	require.NoError(t, err)
	require.Equal(t, securityadmission.RequestUninspectable, state.admission.Class())
	require.Equal(t, securityadmission.ReasonDuplicateJSONKey, state.admission.Reason())
	require.Equal(t, securityadmission.AccountRequirementAuditExempt, state.admission.Requirement())
}
