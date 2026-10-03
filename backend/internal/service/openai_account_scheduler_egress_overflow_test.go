package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type openAISchedulerEgressTransitionCache struct {
	schedulerTestConcurrencyCache
	*accountEgressCacheStub
}

type openAISchedulerAuthoritativeEgressLoadCache struct {
	egressAffinityConcurrencyCache
	loads map[int64]AccountEgressLoadInfo
}

func (c *openAISchedulerAuthoritativeEgressLoadCache) GetAccountEgressLoadsBatch(
	_ context.Context,
	configs []AccountEgressPoolConfig,
	_, _ time.Duration,
) (map[int64]AccountEgressLoadInfo, error) {
	result := make(map[int64]AccountEgressLoadInfo, len(configs))
	for _, config := range configs {
		if load, ok := c.loads[config.AccountID]; ok {
			result[config.AccountID] = load
		}
	}
	return result, nil
}

func openAISchedulerEgressOverflowAccount(id, groupID int64) Account {
	account := legacyEgressTestAccount()
	proxyID := id + 100_000
	routeID := id + 200_000
	identityID := id + 300_000

	account.ID = id
	account.GroupIDs = []int64{groupID}
	account.ProxyID = &proxyID
	account.Proxy.ID = proxyID
	account.Proxy.Host = fmt.Sprintf("scheduler-egress-%d.example", id)
	binding := &account.EgressBindings[0]
	binding.BindingID = StableAccountEgressBindingID(id, routeID)
	binding.AccountID = id
	binding.RouteID = routeID
	binding.Route.ID = routeID
	binding.Route.ProxyID = &proxyID
	binding.Route.ExpectedIdentityID = &identityID
	binding.Route.ExpectedIdentity.ID = identityID
	binding.Route.Proxy = account.Proxy
	return *account
}

func newOpenAISchedulerEgressOverflowService(
	t *testing.T,
	accounts []Account,
	topK int,
	acquireFn func(context.Context, AccountEgressCacheAcquireRequest, int) (AccountEgressAcquireResult, error),
) (*OpenAIGatewayService, *accountEgressCacheStub) {
	t.Helper()
	egressCache := &accountEgressCacheStub{acquireFn: acquireFn}
	concurrency := NewConcurrencyService(&egressAffinityConcurrencyCache{accountEgressCacheStub: egressCache})
	t.Cleanup(func() { concurrency.accountEgressAllocator.Close() })
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.LBTopK = topK
	return &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: accounts},
		cfg:                cfg,
		concurrencyService: concurrency,
		settingService:     NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)}, nil),
		rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true"),
	}, egressCache
}

func newOpenAISchedulerAuthoritativeEgressLoadService(
	t *testing.T,
	accounts []Account,
	topK int,
	loads map[int64]AccountEgressLoadInfo,
	acquireFn func(context.Context, AccountEgressCacheAcquireRequest, int) (AccountEgressAcquireResult, error),
) (*OpenAIGatewayService, *accountEgressCacheStub) {
	t.Helper()
	egressCache := &accountEgressCacheStub{acquireFn: acquireFn}
	cache := &openAISchedulerAuthoritativeEgressLoadCache{
		egressAffinityConcurrencyCache: egressAffinityConcurrencyCache{accountEgressCacheStub: egressCache},
		loads:                          loads,
	}
	concurrency := NewConcurrencyService(cache)
	t.Cleanup(func() { concurrency.accountEgressAllocator.Close() })
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.LBTopK = topK
	return &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: accounts},
		cfg:                cfg,
		concurrencyService: concurrency,
		settingService:     NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)}, nil),
		rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true"),
	}, egressCache
}

func TestGetAccountLoadsForSchedulingPreservesAuthoritativeEgressLoad(t *testing.T) {
	const (
		groupID   = int64(120)
		accountID = int64(40_001)
	)
	account := openAISchedulerEgressOverflowAccount(accountID, groupID)
	authoritative := AccountEgressLoadInfo{
		AccountID:         accountID,
		Status:            AccountEgressStatusExclusive,
		ActiveTotal:       2,
		WaitingCount:      4,
		EffectiveCapacity: 9,
		LoadRate:          143,
		ConfigVersion:     account.EgressRevision,
	}
	svc, _ := newOpenAISchedulerAuthoritativeEgressLoadService(
		t, []Account{account}, 1, map[int64]AccountEgressLoadInfo{accountID: authoritative}, nil,
	)

	loads, err := getAccountLoadsForScheduling(
		context.Background(), svc.concurrencyService, svc.settingService, []*Account{&account}, false,
	)
	require.NoError(t, err)
	require.Equal(t, &AccountLoadInfo{
		AccountID:          accountID,
		CurrentConcurrency: authoritative.ActiveTotal,
		WaitingCount:       authoritative.WaitingCount,
		LoadRate:           authoritative.LoadRate,
		EgressStatus:       authoritative.Status,
		EffectiveCapacity:  authoritative.EffectiveCapacity,
	}, loads[accountID])
}

func TestClassifyOpenAIEgressSchedulingAdmission(t *testing.T) {
	tests := []struct {
		name string
		load AccountLoadInfo
		want openAIEgressSchedulingAdmission
	}{
		{name: "acquired with idle capacity", load: AccountLoadInfo{EgressStatus: AccountEgressStatusAcquired, EffectiveCapacity: 2, CurrentConcurrency: 1}, want: openAIEgressSchedulingAdmissionImmediate},
		{name: "acquired at capacity", load: AccountLoadInfo{EgressStatus: AccountEgressStatusAcquired, EffectiveCapacity: 1, CurrentConcurrency: 1}, want: openAIEgressSchedulingAdmissionWaitable},
		{name: "acquired behind waiter", load: AccountLoadInfo{EgressStatus: AccountEgressStatusAcquired, EffectiveCapacity: 2, WaitingCount: 1}, want: openAIEgressSchedulingAdmissionWaitable},
		{name: "full", load: AccountLoadInfo{EgressStatus: AccountEgressStatusFull}, want: openAIEgressSchedulingAdmissionWaitable},
		{name: "not queue head", load: AccountLoadInfo{EgressStatus: AccountEgressStatusNotQueueHead}, want: openAIEgressSchedulingAdmissionWaitable},
		{name: "exclusive", load: AccountLoadInfo{EgressStatus: AccountEgressStatusExclusive}, want: openAIEgressSchedulingAdmissionWaitable},
		{name: "legacy draining", load: AccountLoadInfo{EgressStatus: AccountEgressStatusLegacyDraining}, want: openAIEgressSchedulingAdmissionWaitable},
		{name: "no eligible", load: AccountLoadInfo{EgressStatus: AccountEgressStatusNoEligibleEgress}, want: openAIEgressSchedulingAdmissionHardBlocked},
		{name: "required binding unavailable", load: AccountLoadInfo{EgressStatus: AccountEgressStatusRequiredBindingUnavailable}, want: openAIEgressSchedulingAdmissionHardBlocked},
		{name: "config stale", load: AccountLoadInfo{EgressStatus: AccountEgressStatusConfigStale}, want: openAIEgressSchedulingAdmissionHardBlocked},
		{name: "config unavailable", load: AccountLoadInfo{EgressStatus: AccountEgressStatusConfigUnavailable}, want: openAIEgressSchedulingAdmissionHardBlocked},
		{name: "queue full", load: AccountLoadInfo{EgressStatus: AccountEgressStatusQueueFull}, want: openAIEgressSchedulingAdmissionHardBlocked},
		{name: "unavailable", load: AccountLoadInfo{EgressStatus: AccountEgressStatus("UNAVAILABLE")}, want: openAIEgressSchedulingAdmissionHardBlocked},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, classifyOpenAIEgressSchedulingAdmission(&test.load))
		})
	}
	require.Equal(t, openAIEgressSchedulingAdmissionHardBlocked, classifyOpenAIEgressSchedulingAdmission(nil))
}

func TestAdvancedSchedulerEgressAdmissionClassifiesBeforeProbeBudget(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	const (
		groupID        = int64(132)
		accountBase    = int64(52_000)
		immediateCount = openAIAccountSelectionProbeLimit
		waitableID     = accountBase + immediateCount + 1
		hardBlockedID  = accountBase + immediateCount + 2
	)
	accounts := make([]Account, 0, immediateCount+2)
	loads := make(map[int64]AccountEgressLoadInfo, immediateCount+2)
	for offset := 1; offset <= immediateCount; offset++ {
		account := openAISchedulerEgressOverflowAccount(accountBase+int64(offset), groupID)
		accounts = append(accounts, account)
		loads[account.ID] = AccountEgressLoadInfo{
			AccountID:         account.ID,
			Status:            AccountEgressStatusAcquired,
			EffectiveCapacity: 2,
			LoadRate:          offset,
		}
	}
	waitable := openAISchedulerEgressOverflowAccount(waitableID, groupID)
	waitable.Priority = -100
	accounts = append(accounts, waitable)
	loads[waitableID] = AccountEgressLoadInfo{
		AccountID:         waitableID,
		Status:            AccountEgressStatusFull,
		ActiveTotal:       1,
		EffectiveCapacity: 1,
		LoadRate:          100,
	}
	hardBlocked := openAISchedulerEgressOverflowAccount(hardBlockedID, groupID)
	hardBlocked.Priority = -200
	accounts = append(accounts, hardBlocked)
	loads[hardBlockedID] = AccountEgressLoadInfo{
		AccountID:    hardBlockedID,
		Status:       AccountEgressStatusConfigStale,
		LoadRate:     0,
		WaitingCount: 0,
	}

	attempts := make(map[int64]int)
	svc, egressCache := newOpenAISchedulerAuthoritativeEgressLoadService(t, accounts, 7, loads, func(
		_ context.Context,
		request AccountEgressCacheAcquireRequest,
		_ int,
	) (AccountEgressAcquireResult, error) {
		accountID := request.Config.AccountID
		attempts[accountID]++
		if accountID != waitableID {
			return AccountEgressAcquireResult{Status: AccountEgressStatusFull, LeaseID: request.LeaseID}, nil
		}
		candidate := request.Config.Candidates[0]
		return AccountEgressAcquireResult{
			Status:            AccountEgressStatusAcquired,
			BindingID:         candidate.BindingID,
			RouteID:           candidate.RouteID,
			IdentityID:        candidate.IdentityID,
			LeaseID:           request.LeaseID,
			EffectiveCapacity: request.Config.EffectiveCapacity(),
			ConfigVersion:     request.Config.Version,
			AuthorityRevision: request.Config.AuthorityRevision,
		}, nil
	})

	requestGroupID := groupID
	selection, _, err := svc.SelectAccountWithScheduler(
		context.Background(), &requestGroupID, "", "", "gpt-5.1", nil,
		OpenAIUpstreamTransportAny, false,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, waitableID, selection.Account.ID)
	require.Zero(t, attempts[hardBlockedID], "hard-blocked candidates must never be probed")
	require.Equal(t, 1, attempts[waitableID], "waitable fallback must remain reachable after the probe budget is exhausted")
	for offset := 1; offset <= immediateCount; offset++ {
		require.Equal(t, 1, attempts[accountBase+int64(offset)])
	}
	acquireCalls, _, _, _ := egressCache.counts()
	require.Equal(t, immediateCount+1, acquireCalls)
	selection.ReleaseFunc()
}

func TestAdvancedSchedulerEgressAdmissionHardBlockedSkipsProbe(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	const (
		groupID   = int64(133)
		accountID = int64(53_001)
	)
	account := openAISchedulerEgressOverflowAccount(accountID, groupID)
	svc, egressCache := newOpenAISchedulerAuthoritativeEgressLoadService(
		t,
		[]Account{account},
		1,
		map[int64]AccountEgressLoadInfo{accountID: {
			AccountID: accountID,
			Status:    AccountEgressStatusConfigStale,
			LoadRate:  0,
		}},
		func(context.Context, AccountEgressCacheAcquireRequest, int) (AccountEgressAcquireResult, error) {
			t.Fatal("hard-blocked candidate was probed")
			return AccountEgressAcquireResult{}, nil
		},
	)

	requestGroupID := groupID
	selection, _, err := svc.SelectAccountWithScheduler(
		context.Background(), &requestGroupID, "", "", "gpt-5.1", nil,
		OpenAIUpstreamTransportAny, false,
	)
	require.Nil(t, selection)
	require.ErrorIs(t, err, ErrAccountEgressConfigStale)
	acquireCalls, _, _, _ := egressCache.counts()
	require.Zero(t, acquireCalls)
}

func TestAdvancedSchedulerEgressAdmissionSpillsBeyondTopK(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	const (
		groupID      = int64(121)
		accountBase  = int64(41_000)
		topK         = 7
		accountCount = 17
	)
	accounts := make([]Account, 0, accountCount)
	for offset := int64(1); offset <= accountCount; offset++ {
		accounts = append(accounts, openAISchedulerEgressOverflowAccount(accountBase+offset, groupID))
	}
	availableID := accountBase + topK + 1
	attempts := make(map[int64]int)
	svc, _ := newOpenAISchedulerEgressOverflowService(t, accounts, topK, func(
		_ context.Context,
		request AccountEgressCacheAcquireRequest,
		_ int,
	) (AccountEgressAcquireResult, error) {
		accountID := request.Config.AccountID
		attempts[accountID]++
		if accountID != availableID {
			return AccountEgressAcquireResult{Status: AccountEgressStatusFull, LeaseID: request.LeaseID}, nil
		}
		candidate := request.Config.Candidates[0]
		return AccountEgressAcquireResult{
			Status:            AccountEgressStatusAcquired,
			BindingID:         candidate.BindingID,
			RouteID:           candidate.RouteID,
			IdentityID:        candidate.IdentityID,
			LeaseID:           request.LeaseID,
			EffectiveCapacity: request.Config.EffectiveCapacity(),
			ConfigVersion:     request.Config.Version,
			AuthorityRevision: request.Config.AuthorityRevision,
		}, nil
	})

	requestGroupID := groupID
	selection, decision, err := svc.SelectAccountWithScheduler(
		context.Background(), &requestGroupID, "", "", "gpt-5.1", nil,
		OpenAIUpstreamTransportAny, false,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, availableID, selection.Account.ID)
	require.Equal(t, accountCount, decision.CandidateCount)
	require.Equal(t, topK, decision.TopK)
	require.Equal(t, 1, attempts[availableID], "the first account beyond Top-K must be probed")
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
}

func TestAdvancedSchedulerEgressAdmissionOverflowHonorsProbeLimit(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	const (
		groupID      = int64(122)
		accountBase  = int64(42_000)
		topK         = 7
		accountCount = 80
	)
	accounts := make([]Account, 0, accountCount)
	for offset := int64(1); offset <= accountCount; offset++ {
		accounts = append(accounts, openAISchedulerEgressOverflowAccount(accountBase+offset, groupID))
	}
	attempts := make(map[int64]int)
	svc, egressCache := newOpenAISchedulerEgressOverflowService(t, accounts, topK, func(
		_ context.Context,
		request AccountEgressCacheAcquireRequest,
		_ int,
	) (AccountEgressAcquireResult, error) {
		attempts[request.Config.AccountID]++
		return AccountEgressAcquireResult{Status: AccountEgressStatusFull, LeaseID: request.LeaseID}, nil
	})

	requestGroupID := groupID
	selection, decision, err := svc.SelectAccountWithScheduler(
		context.Background(), &requestGroupID, "", "", "gpt-5.1", nil,
		OpenAIUpstreamTransportAny, false,
	)
	require.ErrorIs(t, err, ErrAccountEgressCapacityFull)
	require.Nil(t, selection)
	require.Equal(t, accountCount, decision.CandidateCount)
	require.Equal(t, topK, decision.TopK)
	acquireCalls, _, _, _ := egressCache.counts()
	require.Equal(t, openAIAccountSelectionProbeLimit, acquireCalls)
	require.Len(t, attempts, openAIAccountSelectionProbeLimit, "the bounded pass should probe distinct accounts")
	for _, count := range attempts {
		require.Equal(t, 1, count)
	}
}

func TestAdvancedSchedulerSubscriptionEgressFullFallsThroughToRegularPool(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	const groupID = int64(123)
	subscription := openAISchedulerEgressOverflowAccount(43_001, groupID)
	subscription.Credentials = map[string]any{"plan_type": "team"}
	regular := openAISchedulerEgressOverflowAccount(43_002, groupID)
	// A free OAuth account still uses the enforced egress runtime, but is not
	// classified as a ChatGPT subscription pool member.
	regular.Credentials = map[string]any{"plan_type": "free"}

	attempts := make(map[int64]int)
	svc, _ := newOpenAISchedulerEgressOverflowService(t, []Account{subscription, regular}, 1, func(
		_ context.Context,
		request AccountEgressCacheAcquireRequest,
		_ int,
	) (AccountEgressAcquireResult, error) {
		accountID := request.Config.AccountID
		attempts[accountID]++
		if accountID == subscription.ID {
			return AccountEgressAcquireResult{Status: AccountEgressStatusFull, LeaseID: request.LeaseID}, nil
		}
		candidate := request.Config.Candidates[0]
		return AccountEgressAcquireResult{
			Status:            AccountEgressStatusAcquired,
			BindingID:         candidate.BindingID,
			RouteID:           candidate.RouteID,
			IdentityID:        candidate.IdentityID,
			LeaseID:           request.LeaseID,
			EffectiveCapacity: request.Config.EffectiveCapacity(),
			ConfigVersion:     request.Config.Version,
			AuthorityRevision: request.Config.AuthorityRevision,
		}, nil
	})
	svc.rateLimitService = newOpenAIAdvancedSchedulerRateLimitService("true", "", "true")

	requestGroupID := groupID
	selection, _, err := svc.SelectAccountWithScheduler(
		context.Background(), &requestGroupID, "", "", "gpt-5.1", nil,
		OpenAIUpstreamTransportAny, false,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, regular.ID, selection.Account.ID)
	require.Equal(t, 1, attempts[subscription.ID])
	require.Equal(t, 1, attempts[regular.ID])
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
}

func TestAdvancedSchedulerSubscriptionOnlyEgressFullPreservesCapacityError(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	const groupID = int64(129)
	subscription := openAISchedulerEgressOverflowAccount(49_001, groupID)
	subscription.Credentials = map[string]any{"plan_type": "team"}
	svc, egressCache := newOpenAISchedulerEgressOverflowService(t, []Account{subscription}, 1, func(
		_ context.Context,
		request AccountEgressCacheAcquireRequest,
		_ int,
	) (AccountEgressAcquireResult, error) {
		return AccountEgressAcquireResult{Status: AccountEgressStatusFull, LeaseID: request.LeaseID}, nil
	})
	svc.rateLimitService = newOpenAIAdvancedSchedulerRateLimitService("true", "", "true")

	requestGroupID := groupID
	selection, _, err := svc.SelectAccountWithScheduler(
		context.Background(), &requestGroupID, "", "", "gpt-5.1", nil,
		OpenAIUpstreamTransportAny, false,
	)
	require.Nil(t, selection)
	require.ErrorIs(t, err, ErrAccountEgressCapacityFull)
	require.NotContains(t, err.Error(), "selection_order_exhausted")
	acquireCalls, _, _, _ := egressCache.counts()
	require.Equal(t, 1, acquireCalls, "the rejected subscription admission must not be probed twice")
}

func TestAdvancedSchedulerSubscriptionCompactAdmissionWinsOverIncompatibleRegularPool(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	const groupID = int64(131)
	subscription := openAISchedulerEgressOverflowAccount(51_001, groupID)
	subscription.Credentials = map[string]any{"plan_type": "team"}
	subscription.Extra = map[string]any{"openai_compact_supported": true}
	regular := openAISchedulerEgressOverflowAccount(51_002, groupID)
	regular.Credentials = map[string]any{"plan_type": "free"}
	regular.Extra = map[string]any{"openai_compact_supported": false}

	svc, egressCache := newOpenAISchedulerEgressOverflowService(t, []Account{subscription, regular}, 1, func(
		_ context.Context,
		request AccountEgressCacheAcquireRequest,
		_ int,
	) (AccountEgressAcquireResult, error) {
		require.Equal(t, subscription.ID, request.Config.AccountID)
		return AccountEgressAcquireResult{Status: AccountEgressStatusFull, LeaseID: request.LeaseID}, nil
	})
	svc.rateLimitService = newOpenAIAdvancedSchedulerRateLimitService("true", "", "true")

	requestGroupID := groupID
	selection, _, err := svc.SelectAccountWithScheduler(
		context.Background(), &requestGroupID, "", "", "gpt-5.1", nil,
		OpenAIUpstreamTransportAny, true,
	)
	require.Nil(t, selection)
	require.ErrorIs(t, err, ErrAccountEgressCapacityFull)
	require.NotErrorIs(t, err, ErrNoAvailableCompactAccounts)
	acquireCalls, _, _, _ := egressCache.counts()
	require.Equal(t, 1, acquireCalls)
}

func TestAdvancedSchedulerWeightedStickyFullFallsThroughToLegacyWaitPlan(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	const (
		groupID     = int64(124)
		stickyID    = int64(44_001)
		legacyID    = int64(44_002)
		sessionHash = "weighted-sticky-egress-full"
	)
	sticky := openAISchedulerEgressOverflowAccount(stickyID, groupID)
	legacy := Account{
		ID:          legacyID,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		GroupIDs:    []int64{groupID},
	}

	svc, egressCache := newOpenAISchedulerEgressOverflowService(t, []Account{sticky, legacy}, 1, func(
		_ context.Context,
		request AccountEgressCacheAcquireRequest,
		_ int,
	) (AccountEgressAcquireResult, error) {
		return AccountEgressAcquireResult{Status: AccountEgressStatusFull, LeaseID: request.LeaseID}, nil
	})
	cache := &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:" + sessionHash: stickyID}}
	svc.cache = cache
	scheduler := &defaultOpenAIAccountScheduler{service: svc, stats: newOpenAIAccountRuntimeStats()}
	requestGroupID := groupID
	budget := newOpenAISelectionProbeBudget()

	selection, _, _, _, err := scheduler.finishLoadBalanceSelectionFallback(
		context.Background(),
		OpenAIAccountScheduleRequest{
			GroupID:         &requestGroupID,
			Platform:        PlatformOpenAI,
			SessionHash:     sessionHash,
			StickyAccountID: stickyID,
			StickyWeighted:  true,
			RequestedModel:  "gpt-5.1",
		},
		openAIAccountLoadSelectionAttempt{selectionOrder: []openAIAccountCandidateScore{{account: &legacy}}},
		budget,
		openAISelectionFilterStats{},
	)

	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, legacyID, selection.Account.ID)
	require.False(t, selection.Acquired)
	require.NotNil(t, selection.WaitPlan)
	require.Equal(t, legacyID, selection.WaitPlan.AccountID)
	acquireCalls, _, _, _ := egressCache.counts()
	require.Equal(t, 1, acquireCalls)
	require.Equal(t, stickyID, cache.sessionBindings["openai:"+sessionHash])
}

func TestAdvancedSchedulerFallbackDoesNotReprobeAttemptedEgressAccounts(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	const (
		groupID      = int64(125)
		accountBase  = int64(45_000)
		accountCount = 17
		legacyID     = int64(45_100)
	)
	accounts := make([]Account, 0, accountCount+1)
	selectionOrder := make([]openAIAccountCandidateScore, 0, accountCount+1)
	budget := newOpenAISelectionProbeBudget()
	budget.enableLimit()
	for offset := int64(1); offset <= accountCount; offset++ {
		account := openAISchedulerEgressOverflowAccount(accountBase+offset, groupID)
		accounts = append(accounts, account)
		selectionOrder = append(selectionOrder, openAIAccountCandidateScore{account: &accounts[len(accounts)-1]})
		require.True(t, budget.recordAcquire(account.ID))
	}
	legacy := Account{
		ID:          legacyID,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		GroupIDs:    []int64{groupID},
	}
	accounts = append(accounts, legacy)
	selectionOrder = append(selectionOrder, openAIAccountCandidateScore{
		account:   &accounts[len(accounts)-1],
		loadInfo:  &AccountLoadInfo{AccountID: legacyID, CurrentConcurrency: 1, LoadRate: 100},
		loadKnown: true,
	})

	svc, egressCache := newOpenAISchedulerEgressOverflowService(t, accounts, 7, func(
		_ context.Context,
		request AccountEgressCacheAcquireRequest,
		_ int,
	) (AccountEgressAcquireResult, error) {
		return AccountEgressAcquireResult{Status: AccountEgressStatusFull, LeaseID: request.LeaseID}, nil
	})
	for i := 0; i < accountCount; i++ {
		budget.recordEgressAdmissionFailure(
			context.Background(),
			svc.settingService,
			&accounts[i],
			fmt.Errorf("%w: FULL", ErrAccountEgressCapacityFull),
		)
	}
	scheduler := &defaultOpenAIAccountScheduler{service: svc, stats: newOpenAIAccountRuntimeStats()}
	requestGroupID := groupID

	selection, _, _, _, err := scheduler.finishLoadBalanceSelectionFallback(
		context.Background(),
		OpenAIAccountScheduleRequest{GroupID: &requestGroupID, Platform: PlatformOpenAI, RequestedModel: "gpt-5.1"},
		openAIAccountLoadSelectionAttempt{selectionOrder: selectionOrder},
		budget,
		openAISelectionFilterStats{},
	)

	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, legacyID, selection.Account.ID)
	require.NotNil(t, selection.WaitPlan)
	acquireCalls, _, _, _ := egressCache.counts()
	require.Zero(t, acquireCalls, "fallback must not re-acquire enforced accounts rejected by the primary pass")
}

func TestAdvancedSchedulerFallbackRechecksStaleLegacyAttemptAsFreshEgress(t *testing.T) {
	const (
		groupID   = int64(127)
		accountID = int64(47_001)
	)
	fresh := openAISchedulerEgressOverflowAccount(accountID, groupID)
	stale := fresh.CloneForRequest()
	stale.EgressMode = EgressModeLegacy

	legacyAcquires := make([]int64, 0, 1)
	egressCache := &accountEgressCacheStub{acquireFn: func(
		_ context.Context,
		request AccountEgressCacheAcquireRequest,
		_ int,
	) (AccountEgressAcquireResult, error) {
		candidate := request.Config.Candidates[0]
		return AccountEgressAcquireResult{
			Status:            AccountEgressStatusAcquired,
			BindingID:         candidate.BindingID,
			RouteID:           candidate.RouteID,
			IdentityID:        candidate.IdentityID,
			LeaseID:           request.LeaseID,
			EffectiveCapacity: request.Config.EffectiveCapacity(),
			ConfigVersion:     request.Config.Version,
			AuthorityRevision: request.Config.AuthorityRevision,
		}, nil
	}}
	concurrency := NewConcurrencyService(&openAISchedulerEgressTransitionCache{
		schedulerTestConcurrencyCache: schedulerTestConcurrencyCache{
			acquireResults: map[int64]bool{accountID: false},
			acquiredIDs:    &legacyAcquires,
		},
		accountEgressCacheStub: egressCache,
	})
	t.Cleanup(func() { concurrency.accountEgressAllocator.Close() })
	settings := NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)}, nil)
	svc := &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: []Account{fresh}},
		cfg:                &config.Config{RunMode: config.RunModeStandard},
		concurrencyService: concurrency,
		settingService:     settings,
		schedulerSnapshot: &SchedulerSnapshotService{cache: &openAISnapshotCacheStub{
			snapshotAccounts: []*Account{stale},
			accountsByID:     map[int64]*Account{accountID: stale},
		}},
	}
	scheduler := &defaultOpenAIAccountScheduler{service: svc, stats: newOpenAIAccountRuntimeStats()}
	requestGroupID := groupID
	req := OpenAIAccountScheduleRequest{
		GroupID:        &requestGroupID,
		Platform:       PlatformOpenAI,
		RequestedModel: "gpt-5.1",
	}
	selectionOrder := []openAIAccountCandidateScore{{account: stale}}
	budget := newOpenAISelectionProbeBudget()
	budget.enableLimit()

	selection, _, err := scheduler.tryAcquireOpenAISelectionOrderWithBudget(
		context.Background(), req, selectionOrder, budget,
	)
	require.NoError(t, err)
	require.Nil(t, selection)
	require.Equal(t, []int64{accountID}, legacyAcquires)
	acquireCalls, _, _, _ := egressCache.counts()
	require.Zero(t, acquireCalls)

	selection, _, _, _, err = scheduler.finishLoadBalanceSelectionFallback(
		context.Background(),
		req,
		openAIAccountLoadSelectionAttempt{
			selectionOrder: selectionOrder,
			candidateCount: 1,
			topK:           1,
		},
		budget,
		openAISelectionFilterStats{},
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, accountID, selection.Account.ID)
	require.NotNil(t, selection.Egress)
	acquireCalls, _, _, _ = egressCache.counts()
	require.Equal(t, 1, acquireCalls, "fresh enforced admission must not be hidden by the stale legacy attempt")
	selection.ReleaseFunc()
}

func TestAdvancedSchedulerFallbackPreservesDeferredEgressAdmissionError(t *testing.T) {
	const (
		groupID   = int64(128)
		accountID = int64(48_001)
	)
	account := openAISchedulerEgressOverflowAccount(accountID, groupID)
	svc, egressCache := newOpenAISchedulerEgressOverflowService(t, []Account{account}, 1, func(
		_ context.Context,
		request AccountEgressCacheAcquireRequest,
		_ int,
	) (AccountEgressAcquireResult, error) {
		return AccountEgressAcquireResult{Status: AccountEgressStatusFull, LeaseID: request.LeaseID}, nil
	})
	scheduler := &defaultOpenAIAccountScheduler{service: svc, stats: newOpenAIAccountRuntimeStats()}
	requestGroupID := groupID
	req := OpenAIAccountScheduleRequest{
		GroupID:        &requestGroupID,
		Platform:       PlatformOpenAI,
		RequestedModel: "gpt-5.1",
	}
	selectionOrder := []openAIAccountCandidateScore{{account: &account}}
	budget := newOpenAISelectionProbeBudget()
	budget.enableLimit()

	selection, _, acquireErr := scheduler.tryAcquireOpenAISelectionOrderWithBudget(
		context.Background(), req, selectionOrder, budget,
	)
	require.Nil(t, selection)
	require.ErrorIs(t, acquireErr, ErrAccountEgressCapacityFull)

	selection, _, _, _, err := scheduler.finishLoadBalanceSelectionFallback(
		context.Background(),
		req,
		openAIAccountLoadSelectionAttempt{
			selectionOrder: selectionOrder,
			candidateCount: 1,
			topK:           1,
			err:            acquireErr,
		},
		budget,
		openAISelectionFilterStats{},
	)
	require.Nil(t, selection)
	require.ErrorIs(t, err, ErrAccountEgressCapacityFull)
	require.NotContains(t, err.Error(), "selection_order_exhausted")
	acquireCalls, _, _, _ := egressCache.counts()
	require.Equal(t, 1, acquireCalls, "the same rejected egress admission identity must not be probed twice")
}

func TestAdvancedSchedulerFallbackRecheckLimitPreservesAdmissionError(t *testing.T) {
	const (
		groupID           = int64(130)
		rejectedAccountID = int64(50_001)
		legacyAccountID   = int64(50_002)
	)
	rejected := openAISchedulerEgressOverflowAccount(rejectedAccountID, groupID)
	legacy := Account{
		ID:          legacyAccountID,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		GroupIDs:    []int64{groupID},
	}
	svc, _ := newOpenAISchedulerEgressOverflowService(t, []Account{rejected, legacy}, 1, nil)
	svc.schedulerSnapshot = &SchedulerSnapshotService{cache: &openAISnapshotCacheStub{
		accountsByID: map[int64]*Account{legacyAccountID: &legacy},
	}}
	scheduler := &defaultOpenAIAccountScheduler{service: svc, stats: newOpenAIAccountRuntimeStats()}
	requestGroupID := groupID
	req := OpenAIAccountScheduleRequest{
		GroupID:        &requestGroupID,
		Platform:       PlatformOpenAI,
		RequestedModel: "gpt-5.1",
	}
	budget := newOpenAISelectionProbeBudget()
	budget.enableLimit()
	budget.rechecks = openAIAccountSelectionProbeLimit
	budget.recordEgressAdmissionFailure(
		context.Background(),
		svc.settingService,
		&rejected,
		fmt.Errorf("%w: FULL", ErrAccountEgressCapacityFull),
	)

	selection, _, _, _, err := scheduler.finishLoadBalanceSelectionFallback(
		context.Background(),
		req,
		openAIAccountLoadSelectionAttempt{
			selectionOrder: []openAIAccountCandidateScore{{account: &legacy}},
			candidateCount: 2,
			topK:           1,
		},
		budget,
		openAISelectionFilterStats{},
	)
	require.Nil(t, selection)
	require.ErrorIs(t, err, ErrAccountEgressCapacityFull)
	require.NotContains(t, err.Error(), "selection_order_exhausted")
}

func TestAdvancedSchedulerStickyEgressOverflowKeepsDurableBinding(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	const (
		groupID     = int64(126)
		stickyID    = int64(46_001)
		overflowID  = int64(46_002)
		sessionHash = "sticky-egress-overflow"
	)
	sticky := openAISchedulerEgressOverflowAccount(stickyID, groupID)
	sticky.Priority = 0
	overflow := openAISchedulerEgressOverflowAccount(overflowID, groupID)
	overflow.Priority = 100
	attempts := make(map[int64]int)
	svc, _ := newOpenAISchedulerEgressOverflowService(t, []Account{sticky, overflow}, 1, func(
		_ context.Context,
		request AccountEgressCacheAcquireRequest,
		_ int,
	) (AccountEgressAcquireResult, error) {
		accountID := request.Config.AccountID
		attempts[accountID]++
		if accountID == stickyID {
			return AccountEgressAcquireResult{Status: AccountEgressStatusFull, LeaseID: request.LeaseID}, nil
		}
		candidate := request.Config.Candidates[0]
		return AccountEgressAcquireResult{
			Status:            AccountEgressStatusAcquired,
			BindingID:         candidate.BindingID,
			RouteID:           candidate.RouteID,
			IdentityID:        candidate.IdentityID,
			LeaseID:           request.LeaseID,
			EffectiveCapacity: request.Config.EffectiveCapacity(),
			ConfigVersion:     request.Config.Version,
			AuthorityRevision: request.Config.AuthorityRevision,
		}, nil
	})
	cache := &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:" + sessionHash: stickyID}}
	cache.sessionBindings[openAISessionEgressAffinityCacheKey(sessionHash)] = sticky.EgressBindings[0].RouteID
	svc.cache = cache
	svc.rateLimitService = newOpenAIAdvancedSchedulerRateLimitService("true", "true")
	requestGroupID := groupID

	selection, _, err := svc.SelectAccountWithScheduler(
		context.Background(), &requestGroupID, "", sessionHash, "gpt-5.1", nil,
		OpenAIUpstreamTransportAny, false,
	)

	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, overflowID, selection.Account.ID)
	require.Equal(t, 1, attempts[stickyID])
	require.Equal(t, 1, attempts[overflowID])
	admissionCtx := ContextWithSelectionProfitGate(context.Background(), selection)
	require.True(t, preserveOpenAISelectionStickyBinding(admissionCtx))
	require.NoError(t, svc.BindStickySessionAfterProfitAdmission(admissionCtx, &requestGroupID, sessionHash, selection.Account.ID))
	require.NoError(t, svc.BindOpenAISessionEgressAffinity(admissionCtx, &requestGroupID, sessionHash, selection.Account))
	require.Equal(t, stickyID, cache.sessionBindings["openai:"+sessionHash], "transient egress saturation must not migrate the durable sticky binding")
	require.Equal(t, sticky.EgressBindings[0].RouteID, cache.sessionBindings[openAISessionEgressAffinityCacheKey(sessionHash)], "transient account spillover must not migrate the durable route affinity")
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
}
