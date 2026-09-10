package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type egressAffinityConcurrencyCache struct {
	ConcurrencyCache
	*accountEgressCacheStub

	waitAllowed    bool
	waitErr        error
	waitIncrements int
	waitDecrements int
}

type egressAffinityGatewayCache struct {
	GatewayCache
	bindings map[string]int64
}

type responseEgressReadErrorCache struct {
	GatewayCache
	err error
}

type movableResponseEgressMarkerStore struct {
	OpenAIWSStateStore
	accountID    int64
	accountFound bool
	accountErr   error
	bindingID    string
	egressFound  bool
	egressErr    error
}

func (s *movableResponseEgressMarkerStore) GetResponseAccountWithError(
	context.Context,
	int64,
	string,
) (int64, bool, error) {
	return s.accountID, s.accountFound, s.accountErr
}

func (s *movableResponseEgressMarkerStore) GetResponseEgressWithError(
	context.Context,
	int64,
	string,
) (string, bool, error) {
	return s.bindingID, s.egressFound, s.egressErr
}

func (c *responseEgressReadErrorCache) GetSessionAccountID(context.Context, int64, string) (int64, error) {
	return 0, c.err
}

func (c *egressAffinityGatewayCache) GetSessionAccountID(
	_ context.Context,
	_ int64,
	key string,
) (int64, error) {
	if value, ok := c.bindings[key]; ok {
		return value, nil
	}
	return 0, ErrStickySessionNotFound
}

func (c *egressAffinityGatewayCache) SetSessionAccountID(
	_ context.Context,
	_ int64,
	key string,
	accountID int64,
	_ time.Duration,
) error {
	if c.bindings == nil {
		c.bindings = make(map[string]int64)
	}
	c.bindings[key] = accountID
	return nil
}

func (c *egressAffinityGatewayCache) RefreshSessionTTL(_ context.Context, _ int64, _ string, _ time.Duration) error {
	return nil
}

func (c *egressAffinityGatewayCache) DeleteSessionAccountID(_ context.Context, _ int64, key string) error {
	delete(c.bindings, key)
	return nil
}

func (c *egressAffinityConcurrencyCache) IncrementAccountWaitCount(
	context.Context,
	int64,
	int,
) (bool, error) {
	c.waitIncrements++
	return c.waitAllowed, c.waitErr
}

func (c *egressAffinityConcurrencyCache) DecrementAccountWaitCount(context.Context, int64) error {
	c.waitDecrements++
	return nil
}

func TestAccountEgressAdmissionForwardsPreferredBinding(t *testing.T) {
	account := legacyEgressTestAccount()
	poolConfig, err := AccountEgressPoolConfigForRuntime(account, 0)
	require.NoError(t, err)
	candidate := poolConfig.Candidates[0]
	cache := &egressAffinityConcurrencyCache{
		accountEgressCacheStub: &accountEgressCacheStub{acquireResults: []AccountEgressAcquireResult{{
			Status:            AccountEgressStatusAcquired,
			BindingID:         candidate.BindingID,
			RouteID:           candidate.RouteID,
			IdentityID:        candidate.IdentityID,
			EffectiveCapacity: poolConfig.EffectiveCapacity(),
			ConfigVersion:     poolConfig.Version,
			AuthorityRevision: poolConfig.AuthorityRevision,
		}}},
	}
	concurrency := NewConcurrencyService(cache)
	defer concurrency.accountEgressAllocator.Close()
	settings := NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)}, nil)

	ctx := WithPreferredAccountEgressBinding(context.Background(), candidate.BindingID)
	result, err := acquireAccountSlotForSelection(ctx, concurrency, settings, account)
	require.NoError(t, err)
	require.True(t, result.Acquired)
	require.Equal(t, candidate.BindingID, cache.lastAcquire.PreferredBindingID)
	require.True(t, cache.lastAcquire.ForcePool)
	result.ReleaseFunc()
}

func TestOpenAILegacyNonBatchEgressCapacityFallsBackToAnotherAccount(t *testing.T) {
	groupID := int64(87)
	first := openAILegacyNonBatchFailoverTestAccount(9301, 141, groupID, 0)
	second := openAILegacyNonBatchFailoverTestAccount(9302, 142, groupID, 1)
	cache := &egressAffinityConcurrencyCache{accountEgressCacheStub: &accountEgressCacheStub{
		acquireFn: func(_ context.Context, request AccountEgressCacheAcquireRequest, _ int) (AccountEgressAcquireResult, error) {
			if request.Config.AccountID == first.ID {
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
		},
	}}
	concurrency := NewConcurrencyService(cache)
	defer concurrency.accountEgressAllocator.Close()
	gatewayCache := &egressAffinityGatewayCache{bindings: make(map[string]int64)}
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Gateway.Scheduling.LoadBatchEnabled = false
	service := &OpenAIGatewayService{
		accountRepo:        stubOpenAIAccountRepo{accounts: []Account{*first, *second}},
		cache:              gatewayCache,
		cfg:                cfg,
		concurrencyService: concurrency,
		settingService: NewSettingService(
			accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)},
			nil,
		),
	}

	selection, err := service.SelectAccountWithLoadAwareness(
		context.Background(), &groupID, "legacy-non-batch-failover", "gpt-5.1", nil,
	)

	require.NoError(t, err)
	require.NotNil(t, selection)
	require.True(t, selection.Acquired)
	require.Equal(t, second.ID, selection.Account.ID)
	require.Equal(t, 2, cache.acquireCalls)
	stickyAccountID, err := service.getStickySessionAccountID(context.Background(), &groupID, "legacy-non-batch-failover")
	require.NoError(t, err)
	require.Equal(t, second.ID, stickyAccountID)
	preferredBindingID, found, err := service.getOpenAISessionEgressAffinity(context.Background(), &groupID, "legacy-non-batch-failover")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, second.EgressBindings[0].BindingID, preferredBindingID)
	selection.ReleaseFunc()
}

func TestOpenAILegacyNonBatchStickyEgressSpilloverKeepsOriginalBinding(t *testing.T) {
	groupID := int64(88)
	const sessionHash = "legacy-non-batch-sticky-spillover"
	sticky := openAILegacyNonBatchFailoverTestAccount(9401, 151, groupID, 0)
	spillover := openAILegacyNonBatchFailoverTestAccount(9402, 152, groupID, 1)
	cache := &egressAffinityConcurrencyCache{accountEgressCacheStub: &accountEgressCacheStub{
		acquireFn: func(_ context.Context, request AccountEgressCacheAcquireRequest, _ int) (AccountEgressAcquireResult, error) {
			if request.Config.AccountID == sticky.ID {
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
		},
	}}
	concurrency := NewConcurrencyService(cache)
	defer concurrency.accountEgressAllocator.Close()
	gatewayCache := &egressAffinityGatewayCache{bindings: make(map[string]int64)}
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Gateway.Scheduling.LoadBatchEnabled = false
	service := &OpenAIGatewayService{
		accountRepo:        stubOpenAIAccountRepo{accounts: []Account{*sticky, *spillover}},
		cache:              gatewayCache,
		cfg:                cfg,
		concurrencyService: concurrency,
		settingService: NewSettingService(
			accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)},
			nil,
		),
	}
	require.NoError(t, service.BindStickySession(context.Background(), &groupID, sessionHash, sticky.ID))
	sticky.SelectedEgress = &ResolvedAccountEgress{
		BindingID: sticky.EgressBindings[0].BindingID,
		RouteID:   sticky.EgressBindings[0].RouteID,
	}
	require.NoError(t, service.BindOpenAISessionEgressAffinity(context.Background(), &groupID, sessionHash, sticky))

	selection, err := service.SelectAccountWithLoadAwareness(
		context.Background(), &groupID, sessionHash, "gpt-5.1", nil,
	)

	require.NoError(t, err)
	require.NotNil(t, selection)
	require.True(t, selection.Acquired)
	require.Equal(t, spillover.ID, selection.Account.ID)
	require.Equal(t, 2, cache.acquireCalls)
	stickyAccountID, err := service.getStickySessionAccountID(context.Background(), &groupID, sessionHash)
	require.NoError(t, err)
	require.Equal(t, sticky.ID, stickyAccountID, "temporary capacity spillover must not migrate the conversation")
	preferredBindingID, found, err := service.getOpenAISessionEgressAffinity(context.Background(), &groupID, sessionHash)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, sticky.EgressBindings[0].BindingID, preferredBindingID)
	selection.ReleaseFunc()
}

func TestOpenAILegacyNonBatchEgressCapacityExhaustionTerminates(t *testing.T) {
	groupID := int64(89)
	first := openAILegacyNonBatchFailoverTestAccount(9501, 161, groupID, 0)
	second := openAILegacyNonBatchFailoverTestAccount(9502, 162, groupID, 1)
	cache := &egressAffinityConcurrencyCache{accountEgressCacheStub: &accountEgressCacheStub{
		acquireFn: func(_ context.Context, request AccountEgressCacheAcquireRequest, _ int) (AccountEgressAcquireResult, error) {
			return AccountEgressAcquireResult{Status: AccountEgressStatusFull, LeaseID: request.LeaseID}, nil
		},
	}}
	concurrency := NewConcurrencyService(cache)
	defer concurrency.accountEgressAllocator.Close()
	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Gateway.Scheduling.LoadBatchEnabled = false
	service := &OpenAIGatewayService{
		accountRepo:        stubOpenAIAccountRepo{accounts: []Account{*first, *second}},
		cfg:                cfg,
		concurrencyService: concurrency,
		settingService: NewSettingService(
			accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)},
			nil,
		),
	}

	selection, err := service.SelectAccountWithLoadAwareness(
		context.Background(), &groupID, "", "gpt-5.1", nil,
	)

	require.ErrorIs(t, err, ErrAccountEgressCapacityFull)
	require.Nil(t, selection)
	require.Equal(t, 2, cache.acquireCalls, "each eligible account must be attempted at most once")
}

func TestOpenAISessionEgressAffinityPreservesHealthyPreferenceAndRebindsUnhealthyRoute(t *testing.T) {
	groupID := int64(77)
	const sessionHash = "session-affinity"
	cache := &egressAffinityGatewayCache{bindings: make(map[string]int64)}
	service := &OpenAIGatewayService{cache: cache}
	account := openAISessionEgressAffinityTestAccount()

	require.NoError(t, service.BindStickySession(context.Background(), &groupID, sessionHash, account.ID))
	account.SelectedEgress = &ResolvedAccountEgress{
		BindingID: account.EgressBindings[0].BindingID,
		RouteID:   account.EgressBindings[0].RouteID,
	}
	require.NoError(t, service.bindOpenAISessionEgressAffinity(context.Background(), &groupID, sessionHash, account))

	preferredCtx := service.withOpenAISessionEgressPreference(context.Background(), &groupID, sessionHash)
	require.Equal(t, account.EgressBindings[0].BindingID, PreferredAccountEgressBindingFromContext(preferredCtx))

	account.SelectedEgress = &ResolvedAccountEgress{
		BindingID: account.EgressBindings[1].BindingID,
		RouteID:   account.EgressBindings[1].RouteID,
	}
	require.NoError(t, service.bindOpenAISessionEgressAffinity(context.Background(), &groupID, sessionHash, account))
	preferredCtx = service.withOpenAISessionEgressPreference(context.Background(), &groupID, sessionHash)
	require.Equal(t, account.EgressBindings[0].BindingID, PreferredAccountEgressBindingFromContext(preferredCtx),
		"capacity spillover must not migrate a healthy durable preference")

	account.EgressBindings[0].Route.State = EgressRouteStateInactive
	require.NoError(t, service.bindOpenAISessionEgressAffinity(context.Background(), &groupID, sessionHash, account))
	preferredCtx = service.withOpenAISessionEgressPreference(context.Background(), &groupID, sessionHash)
	require.Equal(t, account.EgressBindings[1].BindingID, PreferredAccountEgressBindingFromContext(preferredCtx))
}

func TestOpenAISessionEgressAffinityBindsAfterAccountAdmission(t *testing.T) {
	groupID := int64(78)
	const sessionHash = "session-affinity-after-admission"
	cache := &egressAffinityGatewayCache{bindings: make(map[string]int64)}
	service := &OpenAIGatewayService{cache: cache}
	account := openAISessionEgressAffinityTestAccount()
	account.SelectedEgress = &ResolvedAccountEgress{
		BindingID: account.EgressBindings[0].BindingID,
		RouteID:   account.EgressBindings[0].RouteID,
	}

	// A route cannot be persisted before the account sticky admission exists.
	require.NoError(t, service.BindOpenAISessionEgressAffinity(context.Background(), &groupID, sessionHash, account))
	preferredCtx := service.withOpenAISessionEgressPreference(context.Background(), &groupID, sessionHash)
	require.Empty(t, PreferredAccountEgressBindingFromContext(preferredCtx))
	for key := range cache.bindings {
		require.NotContains(t, key, ":openai:session:egress-affinity-route:")
	}

	require.NoError(t, service.BindStickySessionAfterProfitAdmission(context.Background(), &groupID, sessionHash, account.ID))
	require.NoError(t, service.BindOpenAISessionEgressAffinity(context.Background(), &groupID, sessionHash, account))
	preferredCtx = service.withOpenAISessionEgressPreference(context.Background(), &groupID, sessionHash)
	require.Equal(t, account.EgressBindings[0].BindingID, PreferredAccountEgressBindingFromContext(preferredCtx))
}

func TestPreviousResponseRequiredBindingWaitsWithoutSpilling(t *testing.T) {
	account := legacyEgressTestAccount()
	poolConfig, err := AccountEgressPoolConfigForRuntime(account, 0)
	require.NoError(t, err)
	candidate := poolConfig.Candidates[0]
	cache := &egressAffinityConcurrencyCache{
		accountEgressCacheStub: &accountEgressCacheStub{acquireResults: []AccountEgressAcquireResult{
			{Status: AccountEgressStatusFull},
			{
				Status:            AccountEgressStatusAcquired,
				BindingID:         candidate.BindingID,
				RouteID:           candidate.RouteID,
				IdentityID:        candidate.IdentityID,
				EffectiveCapacity: poolConfig.EffectiveCapacity(),
				ConfigVersion:     poolConfig.Version,
				AuthorityRevision: poolConfig.AuthorityRevision,
			},
		}},
		waitAllowed: true,
	}
	concurrency := NewConcurrencyService(cache)
	defer concurrency.accountEgressAllocator.Close()
	service := &OpenAIGatewayService{
		cfg: &config.Config{Gateway: config.GatewayConfig{Scheduling: config.GatewaySchedulingConfig{
			StickySessionMaxWaiting:  2,
			StickySessionWaitTimeout: time.Second,
		}}},
		concurrencyService: concurrency,
		settingService:     NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)}, nil),
	}

	ctx := WithRequiredAccountEgressBinding(context.Background(), candidate.BindingID)
	result, err := service.acquirePreviousResponseAccountSlot(ctx, account, true)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Acquired)
	require.Equal(t, candidate.BindingID, result.Egress.BindingID)
	require.Equal(t, 1, cache.waitIncrements)
	require.Equal(t, 1, cache.waitDecrements)
	require.Equal(t, candidate.BindingID, cache.lastAcquire.RequiredBindingID)
	result.ReleaseFunc()
}

func TestOpenAIPreviousResponseEgressFenceBypassesWeightedMovableSelection(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	defer resetOpenAIAdvancedSchedulerSettingCacheForTest()

	ctx := context.Background()
	groupID := int64(79)
	bound := legacyEgressTestAccount()
	bound.GroupIDs = []int64{groupID}
	bound.Extra = map[string]any{"openai_oauth_responses_websockets_v2_enabled": true}
	// Make the unbound account look better to the weighted scheduler. A hard
	// response fence must still keep the request on bound's route.
	other := Account{
		ID:          902,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Priority:    0,
		GroupIDs:    []int64{groupID},
		Extra:       map[string]any{"openai_apikey_responses_websockets_v2_enabled": true},
	}
	gatewayCache := &schedulerTestGatewayCache{sessionBindings: make(map[string]int64)}
	egressCache := &egressAffinityConcurrencyCache{
		accountEgressCacheStub: &accountEgressCacheStub{acquireResults: []AccountEgressAcquireResult{{
			Status:            AccountEgressStatusAcquired,
			BindingID:         bound.EgressBindings[0].BindingID,
			RouteID:           bound.EgressBindings[0].RouteID,
			IdentityID:        "301",
			ConfigVersion:     accountEgressRuntimeVersion(bound),
			AuthorityRevision: accountEgressAuthorityRevision(bound),
			EffectiveCapacity: 3,
		}}},
	}
	concurrency := NewConcurrencyService(egressCache)
	defer concurrency.accountEgressAllocator.Close()
	store := NewOpenAIWSStateStore(gatewayCache)
	require.NoError(t, store.BindResponseAccount(ctx, groupID, "resp_hard_weighted", bound.ID, time.Hour))
	require.NoError(t, bindOpenAIWSResponseEgress(store, ctx, groupID, "resp_hard_weighted", bound.EgressBindings[0].BindingID, time.Hour))

	cfg := newSchedulerTestOpenAIWSV2Config()
	svc := &OpenAIGatewayService{
		accountRepo:        stubOpenAIAccountRepo{accounts: []Account{*bound, other}},
		cache:              gatewayCache,
		cfg:                cfg,
		openaiWSStateStore: store,
		concurrencyService: concurrency,
		settingService:     NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)}, nil),
		rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true", "true"),
	}

	selection, decision, err := svc.SelectAccountWithSchedulerForCapability(
		ctx, &groupID, "resp_hard_weighted", "", "gpt-5.1", nil,
		OpenAIUpstreamTransportResponsesWebsocketV2,
		OpenAIEndpointCapabilityChatCompletions, false, true, true, PlatformOpenAI,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, bound.ID, selection.Account.ID)
	require.Equal(t, bound.EgressBindings[0].BindingID, selection.Account.SelectedEgress.BindingID)
	require.Equal(t, openAIAccountScheduleLayerPreviousResponse, decision.Layer)
	require.True(t, decision.StickyPreviousHit)
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
}

func TestOpenAIPreviousResponseEgressFenceSurvivesTemporaryAccountFailures(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	defer resetOpenAIAdvancedSchedulerSettingCacheForTest()

	for _, tc := range []struct {
		name     string
		prepare  func(*Account) []Account
		groupID  int64
		response string
	}{
		{
			name: "temporarily unschedulable account",
			prepare: func(bound *Account) []Account {
				until := time.Now().Add(time.Minute)
				bound.TempUnschedulableUntil = &until
				return []Account{*bound}
			},
			groupID:  89,
			response: "resp_hard_temp_unschedulable",
		},
		{
			name: "unhealthy shadow parent",
			prepare: func(bound *Account) []Account {
				parentID := int64(900)
				until := time.Now().Add(time.Minute)
				bound.ParentAccountID = &parentID
				return []Account{
					*bound,
					{
						ID:                      parentID,
						Platform:                PlatformOpenAI,
						Type:                    AccountTypeOAuth,
						Status:                  StatusActive,
						Schedulable:             true,
						TempUnschedulableUntil:  &until,
						TempUnschedulableReason: "upstream_transport",
					},
				}
			},
			groupID:  90,
			response: "resp_hard_parent_unhealthy",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			bound := legacyEgressTestAccount()
			bound.GroupIDs = []int64{tc.groupID}
			bound.Extra = map[string]any{"openai_oauth_responses_websockets_v2_enabled": true}
			accounts := tc.prepare(bound)
			other := Account{
				ID:          902,
				Platform:    PlatformOpenAI,
				Type:        AccountTypeAPIKey,
				Status:      StatusActive,
				Schedulable: true,
				Concurrency: 1,
				GroupIDs:    []int64{tc.groupID},
				Extra:       map[string]any{"openai_apikey_responses_websockets_v2_enabled": true},
			}
			accounts = append(accounts, other)

			gatewayCache := &schedulerTestGatewayCache{sessionBindings: make(map[string]int64)}
			store := NewOpenAIWSStateStore(gatewayCache)
			require.NoError(t, bindOpenAIWSResponseRoutingPair(
				store,
				ctx,
				tc.groupID,
				tc.response,
				bound.ID,
				bound.EgressBindings[0].BindingID,
				time.Hour,
			))
			svc := &OpenAIGatewayService{
				accountRepo:        stubOpenAIAccountRepo{accounts: accounts},
				cache:              gatewayCache,
				cfg:                newSchedulerTestOpenAIWSV2Config(),
				openaiWSStateStore: store,
				settingService: NewSettingService(
					accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)},
					nil,
				),
				rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("true", "true"),
			}

			for attempt := 0; attempt < 2; attempt++ {
				selection, _, err := svc.SelectAccountWithSchedulerForCapability(
					ctx, &tc.groupID, tc.response, "", "gpt-5.1", nil,
					OpenAIUpstreamTransportResponsesWebsocketV2,
					OpenAIEndpointCapabilityChatCompletions, false, true, true, PlatformOpenAI,
				)
				require.ErrorIs(t, err, ErrAccountEgressNoRoute)
				require.Nil(t, selection, "hard continuation must not fall back after a transient account failure")

				accountID, found, markerErr := getOpenAIWSResponseAccountWithError(
					store, ctx, tc.groupID, tc.response,
				)
				require.NoError(t, markerErr)
				require.True(t, found)
				require.Equal(t, bound.ID, accountID)
				bindingID, found, markerErr := getOpenAIWSResponseEgressWithError(
					store, ctx, tc.groupID, tc.response,
				)
				require.NoError(t, markerErr)
				require.True(t, found)
				require.Equal(t, bound.EgressBindings[0].BindingID, bindingID)
			}
		})
	}
}

func TestOpenAIMovablePreviousResponseAccountOnlyPoolFailsClosedBeforeWeightedFallback(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	defer resetOpenAIAdvancedSchedulerSettingCacheForTest()

	ctx := context.Background()
	groupID := int64(88)
	bound := legacyEgressTestAccount()
	bound.GroupIDs = []int64{groupID}
	bound.Extra = map[string]any{"openai_oauth_responses_websockets_v2_enabled": true}
	bound.Schedulable = false
	other := Account{
		ID:          902,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		Priority:    0,
		GroupIDs:    []int64{groupID},
		Extra:       map[string]any{"openai_apikey_responses_websockets_v2_enabled": true},
	}
	gatewayCache := &schedulerTestGatewayCache{sessionBindings: make(map[string]int64)}
	store := NewOpenAIWSStateStore(gatewayCache)
	require.NoError(t, store.BindResponseAccount(ctx, groupID, "resp_pool_account_only_weighted", bound.ID, time.Hour))
	svc := &OpenAIGatewayService{
		accountRepo:        stubOpenAIAccountRepo{accounts: []Account{*bound, other}},
		cache:              gatewayCache,
		cfg:                newSchedulerTestOpenAIWSV2Config(),
		openaiWSStateStore: store,
		settingService:     NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)}, nil),
		rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true", "true"),
	}

	selection, _, err := svc.SelectAccountWithSchedulerForCapability(
		ctx, &groupID, "resp_pool_account_only_weighted", "", "gpt-5.1",
		map[int64]struct{}{bound.ID: {}},
		OpenAIUpstreamTransportResponsesWebsocketV2,
		OpenAIEndpointCapabilityChatCompletions, false, true, true, PlatformOpenAI,
	)

	require.ErrorIs(t, err, ErrAccountEgressNoRoute)
	require.Nil(t, selection, "an enforced-pool continuation must not fall back to another account or IP")
	accountID, markerErr := store.GetResponseAccount(ctx, groupID, "resp_pool_account_only_weighted")
	require.NoError(t, markerErr)
	require.Equal(t, bound.ID, accountID, "strict inspection must run before best-effort lookup can delete the partial marker")
}

func TestOpenAIMovablePreviousResponseAccountOnlyPoolFailsClosedWhenAdvancedSchedulerDisabled(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	defer resetOpenAIAdvancedSchedulerSettingCacheForTest()

	ctx := context.Background()
	groupID := int64(89)
	bound := legacyEgressTestAccount()
	bound.GroupIDs = []int64{groupID}
	bound.Extra = map[string]any{"openai_oauth_responses_websockets_v2_enabled": true}
	bound.Schedulable = false
	backup := Account{
		ID:          903,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		GroupIDs:    []int64{groupID},
		Extra:       map[string]any{"openai_apikey_responses_websockets_v2_enabled": true},
	}
	gatewayCache := &schedulerTestGatewayCache{sessionBindings: make(map[string]int64)}
	store := NewOpenAIWSStateStore(gatewayCache)
	require.NoError(t, store.BindResponseAccount(ctx, groupID, "resp_pool_account_only_disabled", bound.ID, time.Hour))
	var acquiredIDs []int64
	svc := &OpenAIGatewayService{
		accountRepo:        stubOpenAIAccountRepo{accounts: []Account{*bound, backup}},
		cache:              gatewayCache,
		cfg:                newSchedulerTestOpenAIWSV2Config(),
		openaiWSStateStore: store,
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{acquiredIDs: &acquiredIDs}),
		settingService:     NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)}, nil),
		// An explicitly disabled advanced scheduler yields scheduler=nil. The
		// partial enforced-pool marker must be fenced before legacy load balancing.
		rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("false", "true"),
	}

	selection, _, err := svc.SelectAccountWithSchedulerForCapability(
		ctx, &groupID, "resp_pool_account_only_disabled", "", "gpt-5.1",
		map[int64]struct{}{bound.ID: {}},
		OpenAIUpstreamTransportResponsesWebsocketV2,
		OpenAIEndpointCapabilityChatCompletions, false, true, true, PlatformOpenAI,
	)

	require.ErrorIs(t, err, ErrAccountEgressNoRoute)
	require.Nil(t, selection, "an enforced-pool continuation must not fall back to the backup account")
	require.Empty(t, acquiredIDs, "the backup account must not be selected or admitted")
	accountID, markerErr := store.GetResponseAccount(ctx, groupID, "resp_pool_account_only_disabled")
	require.NoError(t, markerErr)
	require.Equal(t, bound.ID, accountID, "fail-closed inspection must preserve the partial marker")
}

func TestOpenAIGatewayServiceSelectPreviousResponseAccountOnlyPoolFailsClosed(t *testing.T) {
	ctx := context.Background()
	groupID := int64(91)
	const responseID = "resp_pool_account_only_direct"
	bound := legacyEgressTestAccount()
	bound.GroupIDs = []int64{groupID}
	bound.Extra = map[string]any{"openai_oauth_responses_websockets_v2_enabled": true}
	bound.Schedulable = false

	gatewayCache := &schedulerTestGatewayCache{sessionBindings: make(map[string]int64)}
	store := NewOpenAIWSStateStore(gatewayCache)
	require.NoError(t, store.BindResponseAccount(ctx, groupID, responseID, bound.ID, time.Hour))
	svc := &OpenAIGatewayService{
		accountRepo:        stubOpenAIAccountRepo{accounts: []Account{*bound}},
		cache:              gatewayCache,
		cfg:                newSchedulerTestOpenAIWSV2Config(),
		openaiWSStateStore: store,
		settingService:     NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)}, nil),
	}

	selection, err := svc.SelectAccountByPreviousResponseID(
		ctx, &groupID, responseID, "gpt-5.1", nil, false,
	)

	require.ErrorIs(t, err, ErrAccountEgressNoRoute)
	require.Nil(t, selection, "the direct continuation path must not move an enforced-pool response")
	accountID, markerErr := store.GetResponseAccount(ctx, groupID, responseID)
	require.NoError(t, markerErr)
	require.Equal(t, bound.ID, accountID, "fail-closed inspection must preserve the partial marker")
}

func TestOpenAIGatewayServiceNonMovablePreviousResponseRequiresBoundAccount(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	defer resetOpenAIAdvancedSchedulerSettingCacheForTest()

	const (
		groupID    = int64(92)
		responseID = "resp_non_movable_account_fence"
		boundID    = int64(9201)
		backupID   = int64(9202)
	)

	newService := func(boundSchedulable, bindResponse bool, acquiredIDs *[]int64) (*OpenAIGatewayService, OpenAIWSStateStore) {
		accounts := []Account{
			{
				ID:          boundID,
				Platform:    PlatformOpenAI,
				Type:        AccountTypeAPIKey,
				Status:      StatusActive,
				Schedulable: boundSchedulable,
				Concurrency: 1,
				Priority:    5,
				GroupIDs:    []int64{groupID},
			},
			{
				ID:          backupID,
				Platform:    PlatformOpenAI,
				Type:        AccountTypeAPIKey,
				Status:      StatusActive,
				Schedulable: true,
				Concurrency: 1,
				Priority:    0,
				GroupIDs:    []int64{groupID},
			},
		}
		gatewayCache := &schedulerTestGatewayCache{sessionBindings: make(map[string]int64)}
		store := NewOpenAIWSStateStore(gatewayCache)
		if bindResponse {
			require.NoError(t, store.BindResponseAccount(context.Background(), groupID, responseID, boundID, time.Hour))
		}
		return &OpenAIGatewayService{
			accountRepo:        schedulerTestOpenAIAccountRepo{accounts: accounts},
			cache:              gatewayCache,
			cfg:                newSchedulerTestOpenAIWSV2Config(),
			concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{acquiredIDs: acquiredIDs}),
			openaiWSStateStore: store,
		}, store
	}

	selectAccount := func(t *testing.T, svc *OpenAIGatewayService) (*AccountSelectionResult, OpenAIAccountScheduleDecision, error) {
		t.Helper()
		requestGroupID := groupID
		return svc.SelectAccountWithSchedulerForCapability(
			context.Background(), &requestGroupID, responseID, "", "gpt-5.1", nil,
			OpenAIUpstreamTransportAny, "", false, false, true, PlatformOpenAI,
		)
	}

	t.Run("bound account remains selected", func(t *testing.T) {
		var acquiredIDs []int64
		svc, _ := newService(true, true, &acquiredIDs)
		selection, decision, err := selectAccount(t, svc)
		require.NoError(t, err)
		require.NotNil(t, selection)
		require.Equal(t, boundID, selection.Account.ID)
		require.Equal(t, []int64{boundID}, acquiredIDs)
		require.True(t, decision.StickyPreviousHit)
		if selection.ReleaseFunc != nil {
			selection.ReleaseFunc()
		}
	})

	t.Run("unavailable bound account fails closed", func(t *testing.T) {
		var acquiredIDs []int64
		svc, store := newService(false, true, &acquiredIDs)
		selection, _, err := selectAccount(t, svc)
		require.ErrorIs(t, err, ErrAccountEgressNoRoute)
		require.Nil(t, selection)
		require.Empty(t, acquiredIDs, "the backup account must not be admitted")
		accountID, markerErr := store.GetResponseAccount(context.Background(), groupID, responseID)
		require.NoError(t, markerErr)
		require.Equal(t, boundID, accountID, "a transient failure must preserve the hard account fence")
	})

	t.Run("missing account marker fails closed", func(t *testing.T) {
		var acquiredIDs []int64
		svc, _ := newService(true, false, &acquiredIDs)
		selection, _, err := selectAccount(t, svc)
		require.ErrorIs(t, err, ErrAccountEgressNoRoute)
		require.Nil(t, selection)
		require.Empty(t, acquiredIDs, "an unknown response must not be sent to a different account")
	})
}

func TestOpenAIPreviousResponseEgressFenceRemainsHardWhenAdvancedSchedulerDisabled(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	defer resetOpenAIAdvancedSchedulerSettingCacheForTest()

	ctx := context.Background()
	groupID := int64(80)
	bound := legacyEgressTestAccount()
	bound.GroupIDs = []int64{groupID}
	bound.Extra = map[string]any{"openai_oauth_responses_websockets_v2_enabled": true}
	gatewayCache := &schedulerTestGatewayCache{sessionBindings: make(map[string]int64)}
	egressCache := &egressAffinityConcurrencyCache{
		accountEgressCacheStub: &accountEgressCacheStub{acquireResults: []AccountEgressAcquireResult{{
			Status:            AccountEgressStatusAcquired,
			BindingID:         bound.EgressBindings[0].BindingID,
			RouteID:           bound.EgressBindings[0].RouteID,
			IdentityID:        "301",
			ConfigVersion:     accountEgressRuntimeVersion(bound),
			AuthorityRevision: accountEgressAuthorityRevision(bound),
		}}},
	}
	concurrency := NewConcurrencyService(egressCache)
	defer concurrency.accountEgressAllocator.Close()
	store := NewOpenAIWSStateStore(gatewayCache)
	require.NoError(t, store.BindResponseAccount(ctx, groupID, "resp_hard_disabled", bound.ID, time.Hour))
	require.NoError(t, bindOpenAIWSResponseEgress(store, ctx, groupID, "resp_hard_disabled", bound.EgressBindings[0].BindingID, time.Hour))

	svc := &OpenAIGatewayService{
		accountRepo:        stubOpenAIAccountRepo{accounts: []Account{*bound}},
		cache:              gatewayCache,
		cfg:                newSchedulerTestOpenAIWSV2Config(),
		openaiWSStateStore: store,
		concurrencyService: concurrency,
		settingService:     NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)}, nil),
		// No advanced-scheduler setting means scheduler=nil. The response fence
		// must still be honored by the common scheduling entry point.
		rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("", ""),
	}

	selection, decision, err := svc.SelectAccountWithSchedulerForCapability(
		ctx, &groupID, "resp_hard_disabled", "", "gpt-5.1", nil,
		OpenAIUpstreamTransportResponsesWebsocketV2,
		OpenAIEndpointCapabilityChatCompletions, false, true, true, PlatformOpenAI,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, bound.ID, selection.Account.ID)
	require.Equal(t, openAIAccountScheduleLayerPreviousResponse, decision.Layer)
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
}

func TestOpenAIPreviousResponseEgressFenceFailsClosedWhenRolloutIsOff(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	defer resetOpenAIAdvancedSchedulerSettingCacheForTest()

	ctx := context.Background()
	groupID := int64(81)
	bound := legacyEgressTestAccount()
	bound.GroupIDs = []int64{groupID}
	bound.Extra = map[string]any{"openai_oauth_responses_websockets_v2_enabled": true}
	gatewayCache := &schedulerTestGatewayCache{sessionBindings: make(map[string]int64)}
	store := NewOpenAIWSStateStore(gatewayCache)
	require.NoError(t, store.BindResponseAccount(ctx, groupID, "resp_hard_off", bound.ID, time.Hour))
	require.NoError(t, bindOpenAIWSResponseEgress(store, ctx, groupID, "resp_hard_off", bound.EgressBindings[0].BindingID, time.Hour))
	svc := &OpenAIGatewayService{
		accountRepo:        stubOpenAIAccountRepo{accounts: []Account{*bound}},
		cache:              gatewayCache,
		cfg:                newSchedulerTestOpenAIWSV2Config(),
		openaiWSStateStore: store,
		settingService:     NewSettingService(accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutOff)}, nil),
		rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true", "true"),
	}

	selection, _, err := svc.SelectAccountWithSchedulerForCapability(
		ctx, &groupID, "resp_hard_off", "", "gpt-5.1", nil,
		OpenAIUpstreamTransportResponsesWebsocketV2,
		OpenAIEndpointCapabilityChatCompletions, false, true, true, PlatformOpenAI,
	)
	require.ErrorIs(t, err, ErrAccountEgressConfigStale)
	require.Nil(t, selection)
}

func TestOpenAIWSResponseEgressBindingFailsClosedOnRedisReadError(t *testing.T) {
	readErr := errors.New("redis unavailable")
	store := NewOpenAIWSStateStore(&responseEgressReadErrorCache{err: readErr})
	svc := &OpenAIGatewayService{openaiWSStateStore: store}

	bindingID, found, err := svc.openAIWSResponseEgressBinding(
		context.Background(),
		int64PtrForTest(82),
		"resp_egress_redis_error",
	)

	require.ErrorIs(t, err, ErrAccountEgressUnavailable)
	require.ErrorContains(t, err, readErr.Error())
	require.False(t, found)
	require.Empty(t, bindingID)
}

func TestMovableResponseEgressMarkersAllowPortableAccountOnlyBindings(t *testing.T) {
	for _, tc := range []struct {
		name        string
		accountID   int64
		accountType string
	}{
		{name: "api_key", accountID: 9201, accountType: AccountTypeAPIKey},
		{name: "legacy_oauth", accountID: 9202, accountType: AccountTypeOAuth},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &movableResponseEgressMarkerStore{
				OpenAIWSStateStore: NewOpenAIWSStateStore(nil),
				accountID:          tc.accountID,
				accountFound:       true,
			}
			svc := &OpenAIGatewayService{
				accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{{
					ID: tc.accountID, Platform: PlatformOpenAI, Type: tc.accountType, EgressMode: EgressModeLegacy,
				}}},
				openaiWSStateStore: store,
				settingService: NewSettingService(
					accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)},
					nil,
				),
			}

			state, err := svc.inspectOpenAIMovableResponseEgressMarkers(
				context.Background(), int64PtrForTest(83), "resp_portable_account_only",
			)

			require.NoError(t, err)
			require.Equal(t, tc.accountID, state.AccountID)
			require.True(t, state.HasAccountBinding)
			require.False(t, state.HasEgressBinding)
			require.False(t, state.PoolEnforced)
		})
	}
}

func TestMovableResponseEgressMarkersRejectEnforcedPoolAccountWithoutRoute(t *testing.T) {
	account := legacyEgressTestAccount()
	store := &movableResponseEgressMarkerStore{
		OpenAIWSStateStore: NewOpenAIWSStateStore(nil),
		accountID:          account.ID,
		accountFound:       true,
	}
	svc := &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: []Account{*account}},
		openaiWSStateStore: store,
		settingService: NewSettingService(
			accountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)},
			nil,
		),
	}

	state, err := svc.inspectOpenAIMovableResponseEgressMarkers(
		context.Background(), int64PtrForTest(84), "resp_pool_account_only",
	)

	require.ErrorIs(t, err, ErrAccountEgressNoRoute)
	require.Equal(t, account.ID, state.AccountID)
	require.True(t, state.HasAccountBinding)
	require.False(t, state.HasEgressBinding)
	require.True(t, state.PoolEnforced)
}

func TestMovableResponseEgressMarkersWrapAccountReadFailureAsUnavailable(t *testing.T) {
	readErr := errors.New("redis unavailable")
	store := &movableResponseEgressMarkerStore{
		OpenAIWSStateStore: NewOpenAIWSStateStore(nil),
		accountErr:         readErr,
	}
	svc := &OpenAIGatewayService{openaiWSStateStore: store}

	state, err := svc.inspectOpenAIMovableResponseEgressMarkers(
		context.Background(), int64PtrForTest(85), "resp_account_read_error",
	)

	require.ErrorIs(t, err, ErrAccountEgressUnavailable)
	require.ErrorContains(t, err, readErr.Error())
	require.Equal(t, openAIMovableResponseEgressMarkerState{}, state)
}

func TestMovableResponseEgressMarkersRejectAccountRouteMismatch(t *testing.T) {
	const accountID = int64(9202)
	store := &movableResponseEgressMarkerStore{
		OpenAIWSStateStore: NewOpenAIWSStateStore(nil),
		accountID:          accountID,
		accountFound:       true,
		bindingID:          StableAccountEgressBindingID(accountID+1, 41),
		egressFound:        true,
	}
	svc := &OpenAIGatewayService{openaiWSStateStore: store}

	state, err := svc.inspectOpenAIMovableResponseEgressMarkers(
		context.Background(), int64PtrForTest(86), "resp_marker_mismatch",
	)

	require.ErrorIs(t, err, ErrAccountEgressConfigStale)
	require.True(t, state.HasAccountBinding)
	require.True(t, state.HasEgressBinding)
}

func TestMovableResponseEgressMarkersReturnExplicitNoMarkerState(t *testing.T) {
	store := &movableResponseEgressMarkerStore{OpenAIWSStateStore: NewOpenAIWSStateStore(nil)}
	svc := &OpenAIGatewayService{openaiWSStateStore: store}

	state, err := svc.inspectOpenAIMovableResponseEgressMarkers(
		context.Background(), int64PtrForTest(87), "resp_no_markers",
	)

	require.NoError(t, err)
	require.Equal(t, openAIMovableResponseEgressMarkerState{}, state)
}

func openAISessionEgressAffinityTestAccount() *Account {
	account := legacyEgressTestAccount()
	secondProxyID := int64(92)
	secondIdentityID := int64(302)
	verifiedAt := time.Now()
	secondProxy := &Proxy{
		ID:       secondProxyID,
		Protocol: "http",
		Host:     "second-egress.example",
		Port:     9443,
		Status:   StatusActive,
	}
	account.EgressBindings = append(account.EgressBindings, AccountEgressBinding{
		BindingID: StableAccountEgressBindingID(account.ID, 42),
		AccountID: account.ID,
		RouteID:   42,
		Position:  1,
		Status:    AccountEgressBindingStatusActive,
		Route: &EgressRoute{
			ID:                 42,
			Kind:               EgressRouteKindProxy,
			ProxyID:            &secondProxyID,
			ExpectedIdentityID: &secondIdentityID,
			ExpectedIdentity: &EgressIdentity{
				ID:     secondIdentityID,
				Status: EgressIdentityStatusActive,
			},
			State:      EgressRouteStateActive,
			VerifiedAt: &verifiedAt,
			Revision:   14,
			Proxy:      secondProxy,
		},
	})
	return account
}

func openAILegacyNonBatchFailoverTestAccount(accountID, routeID, groupID int64, priority int) *Account {
	account := legacyEgressTestAccount()
	account.ID = accountID
	account.Priority = priority
	account.GroupIDs = []int64{groupID}
	binding := &account.EgressBindings[0]
	binding.AccountID = accountID
	binding.RouteID = routeID
	binding.BindingID = StableAccountEgressBindingID(accountID, routeID)
	binding.Route.ID = routeID
	return account
}
