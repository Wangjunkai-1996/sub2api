package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

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
