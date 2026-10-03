package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type legacyEgressConcurrencyCacheStub struct {
	stubConcurrencyCache

	mu sync.Mutex

	legacyAcquireResult bool
	legacyRefreshResult bool
	legacyAcquireCalls  int
	legacyRefreshCalls  int
	legacyReleaseCalls  int
	legacyAccountID     int64
	legacyMax           int
	legacyRequestID     string
	legacyIdentityID    string
	plainAcquireCalls   int
}

func (c *legacyEgressConcurrencyCacheStub) AcquireAccountSlot(
	context.Context,
	int64,
	int,
	string,
) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.plainAcquireCalls++
	return true, nil
}

func (c *legacyEgressConcurrencyCacheStub) AcquireAccountSlotForEgress(
	_ context.Context,
	accountID int64,
	maxConcurrency int,
	requestID string,
	identityID string,
) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.legacyAcquireCalls++
	c.legacyAccountID = accountID
	c.legacyMax = maxConcurrency
	c.legacyRequestID = requestID
	c.legacyIdentityID = identityID
	return c.legacyAcquireResult, nil
}

func (c *legacyEgressConcurrencyCacheStub) RefreshAccountSlotForEgress(
	_ context.Context,
	accountID int64,
	requestID string,
	identityID string,
) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.legacyRefreshCalls++
	c.legacyAccountID = accountID
	c.legacyRequestID = requestID
	c.legacyIdentityID = identityID
	return c.legacyRefreshResult, nil
}

func (c *legacyEgressConcurrencyCacheStub) ReleaseAccountSlotForEgress(
	_ context.Context,
	accountID int64,
	requestID string,
	identityID string,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.legacyReleaseCalls++
	c.legacyAccountID = accountID
	c.legacyRequestID = requestID
	c.legacyIdentityID = identityID
	return nil
}

func legacyEgressTestAccount() *Account {
	proxyID := int64(91)
	identityID := int64(301)
	verifiedAt := time.Now()
	proxy := &Proxy{
		ID:       proxyID,
		Protocol: "http",
		Host:     "legacy-egress.example",
		Port:     9443,
		Status:   StatusActive,
	}
	return &Account{
		ID:             901,
		Platform:       PlatformOpenAI,
		Type:           AccountTypeOAuth,
		Status:         StatusActive,
		Schedulable:    true,
		Concurrency:    3,
		ProxyID:        &proxyID,
		Proxy:          proxy,
		EgressMode:     EgressModePool,
		EgressRevision: 7,
		EgressBindings: []AccountEgressBinding{{
			BindingID: StableAccountEgressBindingID(901, 41),
			AccountID: 901,
			RouteID:   41,
			Position:  0,
			IsPrimary: true,
			Status:    AccountEgressBindingStatusActive,
			Route: &EgressRoute{
				ID:                 41,
				Kind:               EgressRouteKindProxy,
				ProxyID:            &proxyID,
				ExpectedIdentityID: &identityID,
				ExpectedIdentity: &EgressIdentity{
					ID:     identityID,
					Status: EgressIdentityStatusActive,
				},
				State:      EgressRouteStateActive,
				VerifiedAt: &verifiedAt,
				Revision:   13,
				Proxy:      proxy,
			},
		}},
	}
}

func TestLegacyEgressAdmissionUsesIdentityCacheInOffAndShadow(t *testing.T) {
	for _, rollout := range []AccountEgressPoolRolloutMode{
		AccountEgressPoolRolloutOff,
		AccountEgressPoolRolloutShadow,
	} {
		t.Run(string(rollout), func(t *testing.T) {
			cache := &legacyEgressConcurrencyCacheStub{
				legacyAcquireResult: true,
				legacyRefreshResult: true,
			}
			concurrency := NewConcurrencyService(cache)
			settings := NewSettingService(accountEgressSettingRepoStub{value: string(rollout)}, nil)

			result, err := acquireAccountSlotForSelection(
				context.Background(),
				concurrency,
				settings,
				legacyEgressTestAccount(),
			)
			require.NoError(t, err)
			require.True(t, result.Acquired)
			require.NotNil(t, result.LegacyEgressAdmission)
			require.NotNil(t, result.LegacyEgressAdmission.Lease)
			require.Equal(t, "301", result.LegacyEgressAdmission.IdentityID)
			require.Same(t, result.LegacyEgressAdmission, result.Account.LegacyEgressAdmission)

			cache.mu.Lock()
			require.Equal(t, 1, cache.legacyAcquireCalls)
			require.Zero(t, cache.plainAcquireCalls)
			require.Equal(t, int64(901), cache.legacyAccountID)
			require.Equal(t, 3, cache.legacyMax)
			require.Equal(t, "301", cache.legacyIdentityID)
			cache.mu.Unlock()

			result.ReleaseFunc()
			cache.mu.Lock()
			require.Equal(t, 1, cache.legacyReleaseCalls)
			cache.mu.Unlock()
		})
	}
}

func TestLegacyEgressAdmissionFallsBackForUnsupportedCacheAndLegacyAccount(t *testing.T) {
	settings := NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutOff)}, nil)
	unsupported := &stubConcurrencyCache{acquireResults: map[int64]bool{901: true}}
	result, err := acquireAccountSlotForSelection(
		context.Background(),
		NewConcurrencyService(unsupported),
		settings,
		legacyEgressTestAccount(),
	)
	require.NoError(t, err)
	require.True(t, result.Acquired)
	require.Nil(t, result.LegacyEgressAdmission)
	result.ReleaseFunc()

	cache := &legacyEgressConcurrencyCacheStub{legacyAcquireResult: true}
	legacyAccount := legacyEgressTestAccount()
	legacyAccount.EgressMode = EgressModeLegacy
	result, err = acquireAccountSlotForSelection(
		context.Background(),
		NewConcurrencyService(cache),
		settings,
		legacyAccount,
	)
	require.NoError(t, err)
	require.True(t, result.Acquired)
	require.Nil(t, result.LegacyEgressAdmission)
	cache.mu.Lock()
	require.Equal(t, 1, cache.plainAcquireCalls)
	require.Zero(t, cache.legacyAcquireCalls)
	cache.mu.Unlock()
	result.ReleaseFunc()
}

func TestLegacyEgressAdmissionDoesNotRunInEnforce(t *testing.T) {
	cache := &legacyEgressConcurrencyCacheStub{legacyAcquireResult: true}
	settings := NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)}, nil)

	_, err := acquireAccountSlotForSelection(
		context.Background(),
		NewConcurrencyService(cache),
		settings,
		legacyEgressTestAccount(),
	)
	require.ErrorIs(t, err, ErrAccountEgressUnavailable)
	cache.mu.Lock()
	require.Zero(t, cache.legacyAcquireCalls)
	require.Zero(t, cache.plainAcquireCalls)
	cache.mu.Unlock()
}

func TestLegacyEgressHydrationRejectsVersionChangeAndReleases(t *testing.T) {
	cache := &legacyEgressConcurrencyCacheStub{
		legacyAcquireResult: true,
		legacyRefreshResult: true,
	}
	settings := NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutOff)}, nil)
	result, err := acquireAccountSlotForSelection(
		context.Background(),
		NewConcurrencyService(cache),
		settings,
		legacyEgressTestAccount(),
	)
	require.NoError(t, err)
	require.True(t, result.Acquired)

	changed := legacyEgressTestAccount()
	changed.EgressRevision++
	service := &OpenAIGatewayService{
		accountRepo: &openAIEgressHydrationAccountRepo{account: changed},
	}
	selection, err := service.newAcquiredSelectionResult(
		context.Background(),
		selectionAccount(result, changed),
		result.ReleaseFunc,
	)
	require.ErrorIs(t, err, ErrAccountEgressConfigStale)
	require.Nil(t, selection)
	cache.mu.Lock()
	require.Equal(t, 1, cache.legacyReleaseCalls)
	cache.mu.Unlock()
}

func TestPreserveLegacyEgressRejectsProxyMirrorChange(t *testing.T) {
	selected := legacyEgressTestAccount()
	admission, err := resolveLegacyAccountEgressAdmission(selected)
	require.NoError(t, err)
	selected, err = WithLegacyAccountEgressAdmission(selected, admission)
	require.NoError(t, err)

	latest := legacyEgressTestAccount()
	replacementProxyID := int64(92)
	latest.ProxyID = &replacementProxyID
	_, err = PreserveSelectedAccountEgress(latest, selected)
	require.ErrorIs(t, err, ErrAccountEgressConfigStale)
}

func TestLegacyEgressLeaseLossCancelsLeaseContext(t *testing.T) {
	cache := &legacyEgressConcurrencyCacheStub{
		legacyAcquireResult: true,
		legacyRefreshResult: false,
	}
	lease := newLegacyAccountEgressLeaseWithTiming(
		context.Background(),
		cache,
		901,
		"request-1",
		"301",
		50*time.Millisecond,
		2*time.Millisecond,
		10*time.Millisecond,
	)
	t.Cleanup(lease.Release)

	select {
	case <-lease.Context().Done():
		require.ErrorIs(t, context.Cause(lease.Context()), ErrAccountEgressLeaseLost)
	case <-time.After(time.Second):
		t.Fatal("legacy egress lease was not canceled after ownership loss")
	}
}

func TestOpenAIWSRequestBindingIDUsesLegacyEgressAdmission(t *testing.T) {
	account := legacyEgressTestAccount()
	admission, err := resolveLegacyAccountEgressAdmission(account)
	require.NoError(t, err)
	account.LegacyEgressAdmission = admission

	require.Equal(t, admission.BindingID, openAIWSRequestBindingID(openAIWSAcquireRequest{Account: account}))
}
