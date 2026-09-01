package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestValidApplyAccountPoolsInputConcurrencyOnlyMutation(t *testing.T) {
	concurrency := 8

	for _, operation := range []string{AccountPoolOperationAppend, AccountPoolOperationRemove} {
		t.Run(operation, func(t *testing.T) {
			require.True(t, validApplyAccountPoolsInput([]int64{1, 2}, ApplyAccountPoolsInput{
				Operation:            operation,
				ConcurrencyPerEgress: &concurrency,
			}))
		})
	}
}

func TestAccountEgressRuntimeRejectsInactiveOrExpiredProxy(t *testing.T) {
	proxyID := int64(9)
	identity := &EgressIdentity{ID: 5, PublicIP: "203.0.113.10", Status: EgressIdentityStatusActive}

	for _, proxy := range []*Proxy{
		{ID: proxyID, Status: "inactive"},
		{ID: proxyID, Status: StatusActive, ExpiresAt: timePointer(time.Now().Add(-time.Minute))},
	} {
		account := &Account{
			ID:          27,
			Platform:    PlatformOpenAI,
			Type:        AccountTypeOAuth,
			EgressMode:  EgressModePool,
			Concurrency: 4,
			EgressBindings: []AccountEgressBinding{{
				BindingID: StableAccountEgressBindingID(27, 44),
				AccountID: 27,
				RouteID:   44,
				IsPrimary: true,
				Status:    AccountEgressBindingStatusActive,
				Route: &EgressRoute{
					ID:               44,
					Kind:             EgressRouteKindProxy,
					ProxyID:          &proxyID,
					State:            EgressRouteStateActive,
					ExpectedIdentity: identity,
					Proxy:            proxy,
				},
			}},
		}

		config, err := AccountEgressPoolConfigForRuntime(account, 0)
		require.NoError(t, err)
		require.Len(t, config.Candidates, 1)
		require.False(t, config.Candidates[0].Healthy)
		require.Zero(t, config.EffectiveCapacity())
	}
}

func TestAccountEgressPoolRuntimeIsRestrictedToOpenAIOAuth(t *testing.T) {
	for _, tc := range []struct {
		name     string
		platform string
		typeName string
		want     bool
	}{
		{name: "openai oauth", platform: PlatformOpenAI, typeName: AccountTypeOAuth, want: true},
		{name: "openai api key", platform: PlatformOpenAI, typeName: AccountTypeAPIKey},
		{name: "openai setup token", platform: PlatformOpenAI, typeName: AccountTypeSetupToken},
		{name: "other platform oauth", platform: PlatformAnthropic, typeName: AccountTypeOAuth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := &Account{ID: 1, Platform: tc.platform, Type: tc.typeName, EgressMode: EgressModePool}
			require.Equal(t, tc.want, accountSupportsEgressPoolRuntime(account))
			if !tc.want {
				_, err := AccountEgressPoolConfigForRuntime(account, 0)
				require.ErrorIs(t, err, ErrAccountEgressConfigStale)
			}
		})
	}
}

func TestAccountEgressAdmissionKeepsOpenAINonOAuthOnLegacyPath(t *testing.T) {
	settings := NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)}, nil)
	for _, accountType := range []string{AccountTypeAPIKey, AccountTypeSetupToken} {
		t.Run(accountType, func(t *testing.T) {
			account := &Account{
				ID:          1,
				Platform:    PlatformOpenAI,
				Type:        accountType,
				EgressMode:  EgressModePool,
				Concurrency: 1,
			}
			result, err := acquireAccountSlotForSelectionWithBinding(
				context.Background(), nil, settings, account, "",
			)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.True(t, result.Acquired)
			require.Nil(t, result.Egress)
		})
	}

	oauth := &Account{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeOAuth, EgressMode: EgressModePool}
	_, err := acquireAccountSlotForSelectionWithBinding(context.Background(), nil, settings, oauth, "")
	require.ErrorIs(t, err, ErrAccountEgressUnavailable,
		"the supported account type must reach enforced pool admission")
}

func timePointer(value time.Time) *time.Time {
	return &value
}

func TestValidApplyAccountPoolsInputRejectsEmptyRouteNoOpAndReplace(t *testing.T) {
	concurrency := 8

	require.False(t, validApplyAccountPoolsInput([]int64{1}, ApplyAccountPoolsInput{
		Operation: AccountPoolOperationAppend,
	}))
	require.False(t, validApplyAccountPoolsInput([]int64{1}, ApplyAccountPoolsInput{
		Operation:            AccountPoolOperationReplace,
		ConcurrencyPerEgress: &concurrency,
	}))
}
