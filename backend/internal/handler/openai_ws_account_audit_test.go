package handler

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWSAccountAuditTrackerAPIKeyThenOAuthAuditsBeforeOAuth(t *testing.T) {
	tracker := newOpenAIWSAccountAuditTracker()
	policy := service.DefaultOpenAIAccountAuditRoutingPolicy()
	document := service.ExtractContentModerationDocument(service.ContentModerationProtocolOpenAIResponses, []byte(`{"input":"safe"}`))
	var calls atomic.Int32

	apiKeyResult := tracker.ensure(1, openAIWSAuditTestAccount(service.AccountTypeAPIKey), policy, document, false, func(*auditinput.Document) *securityaudit.Decision {
		calls.Add(1)
		return openAIWSAuditAllowDecision()
	})
	require.False(t, apiKeyResult.Required)
	require.False(t, apiKeyResult.Attempted)
	require.Zero(t, calls.Load())

	oauthResult := tracker.ensure(1, openAIWSAuditTestAccount(service.AccountTypeOAuth), policy, document, false, func(got *auditinput.Document) *securityaudit.Decision {
		calls.Add(1)
		require.Equal(t, document.Hash, got.Hash)
		return openAIWSAuditAllowDecision()
	})
	require.True(t, oauthResult.Required)
	require.True(t, oauthResult.Passed)
	require.Equal(t, int32(1), calls.Load())
}

func TestOpenAIWSAccountAuditTrackerOAuthFailoverReusesPass(t *testing.T) {
	tracker := newOpenAIWSAccountAuditTracker()
	policy := service.DefaultOpenAIAccountAuditRoutingPolicy()
	document := service.ExtractContentModerationDocument(service.ContentModerationProtocolOpenAIResponses, []byte(`{"input":"safe"}`))
	var calls atomic.Int32
	audit := func(*auditinput.Document) *securityaudit.Decision {
		calls.Add(1)
		return openAIWSAuditAllowDecision()
	}

	first := tracker.ensure(1, openAIWSAuditTestAccount(service.AccountTypeOAuth), policy, document, false, audit)
	secondAccount := openAIWSAuditTestAccount(service.AccountTypeOAuth)
	secondAccount.ID = 2
	second := tracker.ensure(1, secondAccount, policy, document, false, audit)

	require.True(t, first.Passed)
	require.True(t, second.Passed)
	require.Equal(t, int32(1), calls.Load())
}

func TestOpenAIWSAccountAuditTrackerOAuthThenAPIKeyDoesNotAuditAPIKey(t *testing.T) {
	tracker := newOpenAIWSAccountAuditTracker()
	policy := service.DefaultOpenAIAccountAuditRoutingPolicy()
	document := service.ExtractContentModerationDocument(service.ContentModerationProtocolOpenAIResponses, []byte(`{"input":"safe"}`))
	var calls atomic.Int32
	audit := func(*auditinput.Document) *securityaudit.Decision {
		calls.Add(1)
		return openAIWSAuditAllowDecision()
	}

	require.True(t, tracker.ensure(1, openAIWSAuditTestAccount(service.AccountTypeOAuth), policy, document, false, audit).Passed)
	apiKeyResult := tracker.ensure(1, openAIWSAuditTestAccount(service.AccountTypeAPIKey), policy, document, false, audit)

	require.False(t, apiKeyResult.Required)
	require.False(t, apiKeyResult.Attempted)
	require.Equal(t, int32(1), calls.Load())
}

func TestOpenAIWSAccountAuditTrackerTerminalFailureCannotFailoverToAPIKey(t *testing.T) {
	tracker := newOpenAIWSAccountAuditTracker()
	policy := service.DefaultOpenAIAccountAuditRoutingPolicy()
	document := service.ExtractContentModerationDocument(service.ContentModerationProtocolOpenAIResponses, []byte(`{"input":"blocked"}`))
	var calls atomic.Int32
	rejected := &securityaudit.Decision{
		Kind:          securityaudit.DecisionFlag,
		HTTPStatus:    403,
		ErrorCode:     "content_policy_violation",
		ClientMessage: "blocked",
	}

	first := tracker.ensure(1, openAIWSAuditTestAccount(service.AccountTypeOAuth), policy, document, false, func(*auditinput.Document) *securityaudit.Decision {
		calls.Add(1)
		return rejected
	})
	second := tracker.ensure(1, openAIWSAuditTestAccount(service.AccountTypeAPIKey), policy, document, false, func(*auditinput.Document) *securityaudit.Decision {
		calls.Add(1)
		return openAIWSAuditAllowDecision()
	})

	require.True(t, first.Terminal)
	require.True(t, second.Terminal)
	require.Equal(t, rejected.ErrorCode, second.Decision.ErrorCode)
	require.Equal(t, int32(1), calls.Load())
}

func TestOpenAIWSAccountAuditTrackerUnavailablePolicyCannotFailoverToAPIKey(t *testing.T) {
	tracker := newOpenAIWSAccountAuditTracker()
	policy := service.OpenAIAccountAuditRoutingPolicy{}
	document := service.ExtractContentModerationDocument(service.ContentModerationProtocolOpenAIResponses, []byte(`{"input":"safe"}`))
	var calls atomic.Int32

	first := tracker.ensure(1, openAIWSAuditTestAccount(service.AccountTypeOAuth), policy, document, false, func(*auditinput.Document) *securityaudit.Decision {
		calls.Add(1)
		return openAIWSAuditAllowDecision()
	})
	second := tracker.ensure(1, openAIWSAuditTestAccount(service.AccountTypeAPIKey), policy, document, false, func(*auditinput.Document) *securityaudit.Decision {
		calls.Add(1)
		return openAIWSAuditAllowDecision()
	})

	require.True(t, first.Terminal)
	require.True(t, first.Eligibility.Indeterminate)
	require.True(t, second.Terminal)
	require.Equal(t, securityaudit.ErrorCodeAuditUnavailable, second.Decision.ErrorCode)
	require.Zero(t, calls.Load())
}

func TestOpenAIWSAccountAuditTrackerConcurrentOAuthRetriesAuditOnce(t *testing.T) {
	tracker := newOpenAIWSAccountAuditTracker()
	policy := service.DefaultOpenAIAccountAuditRoutingPolicy()
	document := service.ExtractContentModerationDocument(service.ContentModerationProtocolOpenAIResponses, []byte(`{"input":"safe"}`))
	account := openAIWSAuditTestAccount(service.AccountTypeOAuth)
	var calls atomic.Int32
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result := tracker.ensure(1, account, policy, document, false, func(*auditinput.Document) *securityaudit.Decision {
				calls.Add(1)
				return openAIWSAuditAllowDecision()
			})
			require.True(t, result.Passed)
		}()
	}
	wg.Wait()
	require.Equal(t, int32(1), calls.Load())
}

func openAIWSAuditTestAccount(accountType string) *service.Account {
	return &service.Account{
		ID:          1,
		Platform:    service.PlatformOpenAI,
		Type:        accountType,
		Credentials: map[string]any{"plan_type": "pro"},
		GroupIDs:    []int64{12},
	}
}

func openAIWSAuditAllowDecision() *securityaudit.Decision {
	return &securityaudit.Decision{
		Kind:           securityaudit.DecisionAllow,
		AllowNextStage: true,
		Audit:          &securityaudit.AuditSummary{Verdict: securityaudit.AuditVerdictAllow},
	}
}
