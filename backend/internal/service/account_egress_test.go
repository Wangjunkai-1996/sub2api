package service

import (
	"context"
	"math"
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

func TestAccountEgressRuntimeAcceptsRedactedSchedulerProxy(t *testing.T) {
	proxyID := int64(9)
	verifiedAt := time.Now()
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
				VerifiedAt:       &verifiedAt,
				ExpectedIdentity: &EgressIdentity{ID: 5, Status: EgressIdentityStatusActive},
				// Scheduler cache projections omit Proxy credentials.
			},
		}},
	}

	config, err := AccountEgressPoolConfigForRuntime(account, 0)
	require.NoError(t, err)
	require.Len(t, config.Candidates, 1)
	require.True(t, config.Candidates[0].Healthy)
	require.Equal(t, 4, config.EffectiveCapacity())
}

func TestAccountEgressRuntimeRequiresFreshVerifiedRoute(t *testing.T) {
	now := time.Now()
	proxyID := int64(9)
	for _, test := range []struct {
		name        string
		verifiedAt  *time.Time
		wantHealthy bool
	}{
		{name: "missing"},
		{name: "stale", verifiedAt: timePointer(now.Add(-EgressIdentityFreshness - time.Second))},
		{name: "small future skew", verifiedAt: timePointer(now.Add(30 * time.Second)), wantHealthy: true},
		{name: "far future", verifiedAt: timePointer(now.Add(accountEgressVerifiedAtFutureSkew + time.Second))},
	} {
		t.Run(test.name, func(t *testing.T) {
			account := &Account{
				ID: 28, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
				EgressMode: EgressModePool, EgressRevision: 1, Concurrency: 1,
				EgressBindings: []AccountEgressBinding{{
					BindingID: StableAccountEgressBindingID(28, 45), AccountID: 28, RouteID: 45,
					IsPrimary: true, Status: AccountEgressBindingStatusActive,
					Route: &EgressRoute{
						ID: 45, Kind: EgressRouteKindProxy, ProxyID: &proxyID,
						State: EgressRouteStateActive, VerifiedAt: test.verifiedAt,
						ExpectedIdentity: &EgressIdentity{ID: 6, Status: EgressIdentityStatusActive},
					},
				}},
			}
			config, err := AccountEgressPoolConfigForRuntime(account, 0)
			require.NoError(t, err)
			require.Equal(t, test.wantHealthy, config.Candidates[0].Healthy)
		})
	}
}

func TestResolvedAccountEgressBindingRejectsSaturatedVersionAuthorityMismatch(t *testing.T) {
	proxyID := int64(9)
	verifiedAt := time.Now()
	account := &Account{
		ID: 29, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		EgressMode: EgressModePool, EgressRevision: (math.MaxInt64 >> 31) + 2, Concurrency: 1,
		EgressBindings: []AccountEgressBinding{{
			BindingID: StableAccountEgressBindingID(29, 46), AccountID: 29, RouteID: 46,
			IsPrimary: true, Status: AccountEgressBindingStatusActive,
			Route: &EgressRoute{
				ID: 46, Kind: EgressRouteKindProxy, ProxyID: &proxyID,
				State: EgressRouteStateActive, VerifiedAt: &verifiedAt,
				ExpectedIdentity: &EgressIdentity{ID: 7, Status: EgressIdentityStatusActive},
			},
		}},
	}
	authorityRevision := accountEgressAuthorityRevision(account)
	previous := *account
	previous.EgressRevision--
	previousAuthorityRevision := accountEgressAuthorityRevision(&previous)
	resolved := &ResolvedAccountEgress{
		BindingID: StableAccountEgressBindingID(29, 46), RouteID: 46, IdentityID: "7",
		ConfigVersion: math.MaxInt64, AuthorityRevision: authorityRevision,
		Lease: &AccountEgressLease{ID: "saturated-version", AuthorityRevision: authorityRevision},
	}
	require.Equal(t, int64(math.MaxInt64), accountEgressRuntimeVersion(account))
	require.Equal(t, int64(math.MaxInt64), accountEgressRuntimeVersion(&previous))
	_, err := withResolvedAccountEgressSelection(account, resolved)
	require.NoError(t, err)

	t.Run("resolved revision", func(t *testing.T) {
		stale := *resolved
		stale.AuthorityRevision = previousAuthorityRevision
		_, err := withResolvedAccountEgressSelection(account, &stale)
		require.ErrorIs(t, err, ErrAccountEgressConfigStale)
	})

	t.Run("lease revision", func(t *testing.T) {
		stale := *resolved
		staleLease := *resolved.Lease
		staleLease.AuthorityRevision = previousAuthorityRevision
		stale.Lease = &staleLease
		_, err := withResolvedAccountEgressSelection(account, &stale)
		require.ErrorIs(t, err, ErrAccountEgressConfigStale)
	})
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

func TestSelectionAccountPrefersFreshLegacyAccountAndResolvedPoolAccount(t *testing.T) {
	stale := &Account{ID: 1, Name: "stale"}
	fresh := &Account{ID: 1, Name: "fresh"}
	require.Same(t, fresh, selectionAccount(&AcquireResult{Acquired: true, Account: stale}, fresh))

	resolved := &ResolvedAccountEgress{BindingID: "1:9"}
	selected := &Account{ID: 1, Name: "selected"}
	require.Same(t, selected, selectionAccount(&AcquireResult{
		Acquired: true,
		Account:  selected,
		Egress:   resolved,
	}, fresh))
}

func TestAccountEgressLeaseLostRecognizesOnlyAllocatorLoss(t *testing.T) {
	leaseCtx, cancel := context.WithCancelCause(context.Background())
	account := &Account{SelectedEgress: &ResolvedAccountEgress{Lease: &AccountEgressLease{ctx: leaseCtx}}}

	require.False(t, AccountEgressLeaseLost(account))
	cancel(ErrAccountEgressLeaseLost)
	require.True(t, AccountEgressLeaseLost(account))

	canceledCtx, cancelCanceled := context.WithCancelCause(context.Background())
	cancelCanceled(context.Canceled)
	account.SelectedEgress.Lease = &AccountEgressLease{ctx: canceledCtx}
	require.False(t, AccountEgressLeaseLost(account), "ordinary request cancellation is not allocator lease loss")
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
