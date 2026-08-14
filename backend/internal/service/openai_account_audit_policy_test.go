package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestOpenAIAccountAuditRoutingRuntimeDefaults(t *testing.T) {
	svc := NewSettingService(&cyberSettingsRepo{values: map[string]string{}}, &config.Config{})

	policy := svc.GetOpenAIAccountAuditRoutingRuntime(context.Background())
	require.True(t, policy.Available())
	require.Equal(t, []int64{12}, policy.AccountGroupIDs())
	require.Equal(t, DefaultOpenAIAccountAuditLongTextRuneThreshold, policy.LongTextRuneThreshold())
	require.True(t, policy.PreferAPIKeyEnabled())

	groups := policy.AccountGroupIDs()
	groups[0] = 99
	require.Equal(t, []int64{12}, policy.AccountGroupIDs())
}

func TestOpenAIAccountAuditRoutingSettingsUpdateRefreshesRuntime(t *testing.T) {
	repo := &cyberSettingsRepo{values: map[string]string{}}
	svc := NewSettingService(repo, &config.Config{})

	err := svc.UpdateOpenAIAccountAuditRoutingSettings(context.Background(), OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs:       []int64{19, 7, 19, -1},
		LongTextRuneThreshold: 24000,
		PreferAPIKeyEnabled:   false,
	})
	require.NoError(t, err)
	require.Equal(t, "[7,19]", repo.values[SettingKeyOpenAIAccountAuditGroupIDs])
	require.Equal(t, "24000", repo.values[SettingKeyOpenAIAccountAuditLongTextRuneThreshold])
	require.Equal(t, "false", repo.values[SettingKeyOpenAIAccountAuditPreferAPIKeyEnabled])

	policy := svc.GetOpenAIAccountAuditRoutingRuntime(context.Background())
	require.True(t, policy.Available())
	require.Equal(t, []int64{7, 19}, policy.AccountGroupIDs())
	require.Equal(t, 24000, policy.LongTextRuneThreshold())
	require.False(t, policy.PreferAPIKeyEnabled())
}

func TestOpenAIAccountAuditRoutingRuntimeFailureRetainsValuesButMarksUnavailable(t *testing.T) {
	repo := &cyberSettingsRepo{values: map[string]string{
		SettingKeyOpenAIAccountAuditGroupIDs:              "[21]",
		SettingKeyOpenAIAccountAuditLongTextRuneThreshold: "16000",
		SettingKeyOpenAIAccountAuditPreferAPIKeyEnabled:   "false",
	}}
	svc := NewSettingService(repo, &config.Config{})
	initial := svc.GetOpenAIAccountAuditRoutingRuntime(context.Background())
	require.True(t, initial.Available())

	repo.getMultipleErr = errors.New("settings unavailable")
	svc.openAIAccountAuditRoutingRuntimeCache.Store(&cachedOpenAIAccountAuditRoutingRuntime{
		policy: initial, expiresAt: time.Now().Add(-time.Second).UnixNano(),
	})
	policy := svc.GetOpenAIAccountAuditRoutingRuntime(context.Background())
	require.False(t, policy.Available())
	require.Equal(t, []int64{21}, policy.AccountGroupIDs())
	require.Equal(t, 16000, policy.LongTextRuneThreshold())
	require.False(t, policy.PreferAPIKeyEnabled())
}

func TestClassifyOpenAIAccountAuditEligibility(t *testing.T) {
	policy := newOpenAIAccountAuditRoutingPolicy(OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs:       []int64{12},
		LongTextRuneThreshold: 12000,
		PreferAPIKeyEnabled:   true,
	}, true)
	base := Account{
		ID:          1,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		GroupIDs:    []int64{12},
		Credentials: map[string]any{"plan_type": " Pro "},
	}

	tests := []struct {
		name          string
		mutate        func(*Account)
		eligible      bool
		indeterminate bool
		reason        OpenAIAccountAuditEligibilityReason
	}{
		{name: "eligible", eligible: true, reason: OpenAIAccountAuditEligible},
		{name: "apikey", mutate: func(a *Account) { a.Type = AccountTypeAPIKey }, reason: OpenAIAccountAuditIneligibleAccountType},
		{name: "setup token", mutate: func(a *Account) { a.Type = AccountTypeSetupToken }, reason: OpenAIAccountAuditIneligibleAccountType},
		{name: "other platform", mutate: func(a *Account) { a.Platform = PlatformGrok }, reason: OpenAIAccountAuditIneligiblePlatform},
		{name: "chatgptpro not accepted", mutate: func(a *Account) { a.Credentials["plan_type"] = "chatgptpro" }, reason: OpenAIAccountAuditIneligiblePlan},
		{name: "wrong group", mutate: func(a *Account) { a.GroupIDs = []int64{13} }, reason: OpenAIAccountAuditIneligibleGroup},
		{name: "repository account groups", mutate: func(a *Account) {
			a.GroupIDs = nil
			a.AccountGroups = []AccountGroup{{GroupID: 12}}
		}, eligible: true, reason: OpenAIAccountAuditEligible},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account := base
			account.Credentials = map[string]any{"plan_type": base.Credentials["plan_type"]}
			if tt.mutate != nil {
				tt.mutate(&account)
			}
			got := ClassifyOpenAIAccountAuditEligibility(&account, policy)
			require.Equal(t, tt.eligible, got.Eligible)
			require.Equal(t, tt.indeterminate, got.Indeterminate)
			require.Equal(t, tt.reason, got.Reason)
		})
	}
}

func TestClassifyOpenAIAccountAuditEligibilityUnavailablePolicy(t *testing.T) {
	policy := DefaultOpenAIAccountAuditRoutingPolicy()
	policy.available = false
	oauthPro := &Account{
		Platform: PlatformOpenAI, Type: AccountTypeOAuth, GroupIDs: []int64{12},
		Credentials: map[string]any{"plan_type": "pro"},
	}

	result := ClassifyOpenAIAccountAuditEligibility(oauthPro, policy)
	require.False(t, result.Eligible)
	require.True(t, result.Indeterminate)
	require.Equal(t, OpenAIAccountAuditPolicyUnavailable, result.Reason)

	oauthPro.Type = AccountTypeAPIKey
	result = ClassifyOpenAIAccountAuditEligibility(oauthPro, policy)
	require.False(t, result.Eligible)
	require.False(t, result.Indeterminate)
	require.Equal(t, OpenAIAccountAuditIneligibleAccountType, result.Reason)
}

func TestValidateOpenAIAccountAuditRoutingSettings(t *testing.T) {
	require.Error(t, ValidateOpenAIAccountAuditRoutingSettings(OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs:       nil,
		LongTextRuneThreshold: 12000,
	}))
	require.Error(t, ValidateOpenAIAccountAuditRoutingSettings(OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs:       []int64{12},
		LongTextRuneThreshold: 0,
	}))
}
