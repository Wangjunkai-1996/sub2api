package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/stretchr/testify/require"
)

func TestOpenAIHardPreviousBindingUnavailableDoesNotFallbackAcrossSchedulers(t *testing.T) {
	for _, schedulerMode := range []struct {
		name    string
		enabled bool
	}{
		{name: "legacy"},
		{name: "advanced", enabled: true},
	} {
		for _, requirement := range []securityadmission.AccountRequirement{
			securityadmission.AccountRequirementAny,
			securityadmission.AccountRequirementAuditExempt,
		} {
			for _, failure := range []string{"failover_excluded", "quota_paused", "transport", "capability", "profit"} {
				name := schedulerMode.name + "/" + string(requirement) + "/" + failure
				t.Run(name, func(t *testing.T) {
					resetOpenAIAdvancedSchedulerSettingCacheForTest()
					t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

					groupID := int64(7300)
					bound := hardPreviousTestBoundAccount(7301, groupID, requirement)
					backup := hardPreviousTestBackupAccount(7302, groupID)
					cfg := newSchedulerTestOpenAIWSV2Config()
					cfg.Gateway.Scheduling.LoadBatchEnabled = false
					excludedIDs := map[int64]struct{}(nil)
					requiredTransport := OpenAIUpstreamTransportAny
					requiredCapability := OpenAIEndpointCapabilityChatCompletions
					ctx := context.Background()

					switch failure {
					case "failover_excluded":
						excludedIDs = map[int64]struct{}{bound.ID: {}}
					case "quota_paused":
						bound.Extra["codex_5h_used_percent"] = 96.0
						bound.Extra["auto_pause_5h_threshold"] = 0.95
						ctx = withOpenAIQuotaAutoPauseSettings(ctx, OpsOpenAIAccountQuotaAutoPauseSettings{DefaultThreshold5h: 0.95})
					case "transport":
						cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = false
						cfg.Gateway.OpenAIWS.ResponsesWebsockets = true
						requiredTransport = OpenAIUpstreamTransportResponsesWebsocket
					case "capability":
						bound.Credentials["openai_capabilities"] = []any{"chat_completions"}
						backup.Credentials["openai_capabilities"] = []any{"chat_completions", "embeddings"}
						requiredCapability = OpenAIEndpointCapabilityEmbeddings
					case "profit":
						profitControlTestAccountWithRate(&bound, 0.8)
						profitControlTestAccountWithRate(&backup, 0.3)
						ctx = profitControlTestCtx(profitControlTestGroup(groupID, 0.5, 0))
					}
					ctx = WithOpenAIAccountRequirement(ctx, requirement)
					schedulerEnabled := "false"
					if schedulerMode.enabled {
						schedulerEnabled = "true"
					}

					acquiredIDs := make([]int64, 0, 2)
					svc := &OpenAIGatewayService{
						accountRepo:        schedulerTestOpenAIAccountRepo{accounts: []Account{bound, backup}},
						cache:              &schedulerTestGatewayCache{},
						cfg:                cfg,
						rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService(schedulerEnabled),
						concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{acquiredIDs: &acquiredIDs}),
					}
					previousResponseID := "resp_hard_binding_unavailable"
					require.NoError(t, svc.getOpenAIWSStateStore().BindResponseAccount(ctx, groupID, previousResponseID, bound.ID, time.Hour))

					selection, _, err := svc.SelectAccountWithSchedulerForCapability(
						ctx,
						&groupID,
						previousResponseID,
						"",
						"gpt-5.1",
						excludedIDs,
						requiredTransport,
						requiredCapability,
						false,
						false,
						true,
						PlatformOpenAI,
					)
					require.ErrorIs(t, err, ErrNoAvailableAccounts)
					require.Nil(t, selection)
					require.NotContains(t, acquiredIDs, backup.ID, "hard continuation must not acquire the fallback account")
				})
			}
		}
	}
}

func TestOpenAIHardPreviousTrafficDirectorTerminalRejectDoesNotAdvance(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	groupID := int64(42)
	spec := testOpenAITrafficDirectorSpec()
	resolver := &openAITrafficDirectorResolverStub{policy: TrafficDirectorResolvedPolicy{
		Version: TrafficDirectorVersion{GroupID: groupID, Version: 1, Mode: domain.TrafficDirectorModeEnforced, Spec: &spec},
	}}
	health := &hardPreviousTerminalHealthStub{}
	bound := hardPreviousTestBoundAccount(101, groupID, securityadmission.AccountRequirementAny)
	backup := hardPreviousTestBackupAccount(202, groupID)
	acquiredIDs := make([]int64, 0, 2)
	svc := &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: []Account{bound, backup}},
		cache:              &schedulerTestGatewayCache{},
		cfg:                newSchedulerTestOpenAIWSV2Config(),
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{acquiredIDs: &acquiredIDs}),
	}
	svc.SetOpenAITrafficDirectorResolver(resolver)
	svc.SetOpenAITrafficDirectorHealthResolver(health)
	previousResponseID := "resp_hard_binding_unhealthy"
	require.NoError(t, svc.getOpenAIWSStateStore().BindResponseAccount(context.Background(), groupID, previousResponseID, bound.ID, time.Hour))

	selection, _, err := svc.SelectAccountWithSchedulerForCapability(
		context.Background(), &groupID, previousResponseID, "", "gpt-5.1", nil,
		OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions,
		false, false, true, PlatformOpenAI,
	)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
	require.Nil(t, selection)
	require.Contains(t, acquiredIDs, bound.ID)
	require.NotContains(t, acquiredIDs, backup.ID)
	require.Len(t, health.checks, 2, "bound account should pass eligibility and fail terminal health admission")
	require.NotNil(t, health.checks[0].AcquireProbe)
	require.False(t, *health.checks[0].AcquireProbe)
	require.Nil(t, health.checks[1].AcquireProbe)
}

func TestOpenAIHardPreviousCyberRejectDoesNotFallback(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "advanced"}[advanced], func(t *testing.T) {
			resetOpenAIAdvancedSchedulerSettingCacheForTest()
			t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

			groupID := int64(7350)
			bound := hardPreviousTestBoundAccount(7351, groupID, securityadmission.AccountRequirementAny)
			backup := hardPreviousTestBackupAccount(7352, groupID)
			cache := &schedulerCyberGatewayCache{
				schedulerTestGatewayCache: &schedulerTestGatewayCache{},
				deadlines:                 map[int64]time.Time{bound.ID: time.Now().Add(time.Hour)},
			}
			settings := map[string]string{
				SettingKeyOpenAICyberAccountCooldownEnabled:  "true",
				SettingKeyOpenAICyberAccountCooldownGroupIDs: "[7350]",
				openAIAdvancedSchedulerSettingKey:            map[bool]string{false: "false", true: "true"}[advanced],
			}
			settingService := NewSettingService(&openAIAdvancedSchedulerSettingRepoStub{values: settings}, newSchedulerTestOpenAIWSV2Config())
			var acquiredIDs, releasedIDs []int64
			svc := &OpenAIGatewayService{
				accountRepo:        schedulerTestOpenAIAccountRepo{accounts: []Account{bound, backup}},
				cache:              cache,
				cfg:                newSchedulerTestOpenAIWSV2Config(),
				settingService:     settingService,
				rateLimitService:   &RateLimitService{settingService: settingService},
				concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{acquiredIDs: &acquiredIDs, releasedIDs: &releasedIDs}),
			}
			previousResponseID := "resp_hard_binding_cyber"
			require.NoError(t, svc.getOpenAIWSStateStore().BindResponseAccount(context.Background(), groupID, previousResponseID, bound.ID, time.Hour))

			selection, _, err := svc.SelectAccountWithSchedulerForCapability(
				context.Background(), &groupID, previousResponseID, "", "gpt-5.1", nil,
				OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions,
				false, false, true, PlatformOpenAI,
			)
			require.ErrorIs(t, err, ErrNoAvailableAccounts)
			require.Nil(t, selection)
			require.Contains(t, acquiredIDs, bound.ID)
			require.Contains(t, releasedIDs, bound.ID)
			require.NotContains(t, acquiredIDs, backup.ID)
		})
	}
}

func TestOpenAIHardPreviousStateStoreFailureIsControlled(t *testing.T) {
	groupID := int64(7400)
	backup := hardPreviousTestBackupAccount(7402, groupID)
	acquiredIDs := make([]int64, 0, 1)
	svc := &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: []Account{backup}},
		cfg:                newSchedulerTestOpenAIWSV2Config(),
		openaiWSStateStore: previousResponseStoreErrorStub{err: errors.New("state store unavailable")},
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{acquiredIDs: &acquiredIDs}),
	}

	selection, _, err := svc.SelectAccountWithSchedulerForCapability(
		context.Background(), &groupID, "resp_unverifiable", "", "gpt-5.1", nil,
		OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions,
		false, false, true, PlatformOpenAI,
	)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
	require.Nil(t, selection)
	require.Empty(t, acquiredIDs)

	selection, decision, err := svc.SelectAccountWithSchedulerForCapability(
		context.Background(), &groupID, "resp_unverifiable", "", "gpt-5.1", nil,
		OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions,
		false, true, true, PlatformOpenAI,
	)
	require.NoError(t, err)
	require.NotNil(t, selection, "a self-contained movable continuation may rebuild on another account")
	require.Equal(t, backup.ID, selection.Account.ID)
	require.Equal(t, openAIAccountScheduleLayerLoadBalance, decision.Layer)
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
}

func TestOpenAIHardPreviousUnverifiableBindingDoesNotFallback(t *testing.T) {
	for _, schedulerMode := range []struct {
		name    string
		enabled bool
	}{
		{name: "legacy"},
		{name: "advanced", enabled: true},
	} {
		for _, storeCase := range []struct {
			name  string
			store OpenAIWSStateStore
		}{
			{name: "store_without_backing", store: NewOpenAIWSStateStore(nil)},
			{name: "explicit_miss", store: previousResponseStoreMissStub{}},
			{name: "lookup_error", store: previousResponseStoreErrorStub{err: errors.New("state store unavailable")}},
		} {
			t.Run(schedulerMode.name+"/"+storeCase.name, func(t *testing.T) {
				resetOpenAIAdvancedSchedulerSettingCacheForTest()
				t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

				groupID := int64(7410)
				backup := hardPreviousTestBackupAccount(7412, groupID)
				acquiredIDs := make([]int64, 0, 1)
				svc := &OpenAIGatewayService{
					accountRepo:        schedulerTestOpenAIAccountRepo{accounts: []Account{backup}},
					cfg:                newSchedulerTestOpenAIWSV2Config(),
					rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService(map[bool]string{false: "false", true: "true"}[schedulerMode.enabled]),
					concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{acquiredIDs: &acquiredIDs}),
					openaiWSStateStore: storeCase.store,
				}

				selection, _, err := svc.SelectAccountWithSchedulerForCapability(
					context.Background(), &groupID, "resp_unverifiable", "", "gpt-5.1", nil,
					OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions,
					false, false, true, PlatformOpenAI,
				)
				require.ErrorIs(t, err, ErrNoAvailableAccounts)
				require.Nil(t, selection)
				require.Empty(t, acquiredIDs, "an unverifiable hard continuation must not acquire the backup account")

				selection, _, err = svc.SelectAccountWithSchedulerForCapability(
					context.Background(), &groupID, "resp_unverifiable", "", "gpt-5.1", nil,
					OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions,
					false, true, true, PlatformOpenAI,
				)
				require.NoError(t, err)
				require.NotNil(t, selection, "an explicitly movable continuation may rebuild on another account")
				require.Equal(t, backup.ID, selection.Account.ID)
				if selection.ReleaseFunc != nil {
					selection.ReleaseFunc()
				}
			})
		}
	}
}

func TestOpenAIHardPreviousDeletedBindingRemainsClosedOnSecondRequest(t *testing.T) {
	for _, schedulerMode := range []struct {
		name    string
		enabled bool
	}{
		{name: "legacy"},
		{name: "advanced", enabled: true},
	} {
		t.Run(schedulerMode.name, func(t *testing.T) {
			resetOpenAIAdvancedSchedulerSettingCacheForTest()
			t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

			groupID := int64(7420)
			backup := hardPreviousTestBackupAccount(7422, groupID)
			cache := &schedulerTestGatewayCache{}
			acquiredIDs := make([]int64, 0, 1)
			svc := &OpenAIGatewayService{
				accountRepo:        schedulerTestOpenAIAccountRepo{accounts: []Account{backup}},
				cache:              cache,
				cfg:                newSchedulerTestOpenAIWSV2Config(),
				rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService(map[bool]string{false: "false", true: "true"}[schedulerMode.enabled]),
				concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{acquiredIDs: &acquiredIDs}),
			}
			store := svc.getOpenAIWSStateStore()
			const responseID = "resp_deleted_hard_binding"
			require.NoError(t, store.BindResponseAccount(context.Background(), groupID, responseID, 7499, time.Hour))

			for attempt := 1; attempt <= 2; attempt++ {
				selection, _, err := svc.SelectAccountWithSchedulerForCapability(
					context.Background(), &groupID, responseID, "", "gpt-5.1", nil,
					OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions,
					false, false, true, PlatformOpenAI,
				)
				require.ErrorIs(t, err, ErrNoAvailableAccounts, "attempt %d", attempt)
				require.Nil(t, selection, "attempt %d", attempt)
			}
			boundID, err := store.GetResponseAccount(context.Background(), groupID, responseID)
			require.NoError(t, err)
			require.Zero(t, boundID, "the vanished target should remain deleted")
			require.Empty(t, acquiredIDs, "neither request may acquire the backup account")
		})
	}
}

func TestOpenAIAdvancedSchedulerDirectHardPreviousMissDoesNotFallback(t *testing.T) {
	groupID := int64(7430)
	backup := hardPreviousTestBackupAccount(7432, groupID)
	acquiredIDs := make([]int64, 0, 1)
	svc := &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: []Account{backup}},
		cfg:                newSchedulerTestOpenAIWSV2Config(),
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{acquiredIDs: &acquiredIDs}),
		openaiWSStateStore: previousResponseStoreMissStub{},
	}
	scheduler := newDefaultOpenAIAccountScheduler(svc, nil)
	req := OpenAIAccountScheduleRequest{
		GroupID:                 &groupID,
		Platform:                PlatformOpenAI,
		PreviousResponseID:      "resp_direct_hard_miss",
		PreviousResponseCanMove: false,
		RequestedModel:          "gpt-5.1",
		RequiredCapability:      OpenAIEndpointCapabilityChatCompletions,
	}

	selection, _, err := scheduler.Select(context.Background(), req)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
	require.Nil(t, selection)
	require.Empty(t, acquiredIDs)

	req.PreviousResponseCanMove = true
	selection, _, err = scheduler.Select(context.Background(), req)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, backup.ID, selection.Account.ID)
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
}

func TestOpenAIWSStateStorePropagatesBindingLookupFailure(t *testing.T) {
	expected := errors.New("redis unavailable")
	store := NewOpenAIWSStateStore(previousResponseGatewayCacheErrorStub{err: expected})
	accountID, err := store.GetResponseAccount(context.Background(), 1, "resp_lookup_failure")
	require.ErrorIs(t, err, expected)
	require.Zero(t, accountID)
}

func hardPreviousTestBoundAccount(id, groupID int64, requirement securityadmission.AccountRequirement) Account {
	plan := "pro"
	if requirement == securityadmission.AccountRequirementAuditExempt {
		plan = "plus"
	}
	return Account{
		ID: id, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 100,
		GroupIDs:    []int64{groupID},
		Credentials: map[string]any{"plan_type": plan},
		Extra:       map[string]any{"openai_oauth_responses_websockets_v2_enabled": true},
	}
}

func hardPreviousTestBackupAccount(id, groupID int64) Account {
	return Account{
		ID: id, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0,
		GroupIDs:    []int64{groupID},
		Credentials: map[string]any{},
		Extra:       map[string]any{"openai_apikey_responses_websockets_v2_enabled": true},
	}
}

type previousResponseStoreErrorStub struct {
	OpenAIWSStateStore
	err error
}

type previousResponseStoreMissStub struct {
	OpenAIWSStateStore
}

func (previousResponseStoreMissStub) GetResponseAccount(context.Context, int64, string) (int64, error) {
	return 0, nil
}

func (s previousResponseStoreErrorStub) GetResponseAccount(context.Context, int64, string) (int64, error) {
	return 0, s.err
}

type previousResponseGatewayCacheErrorStub struct {
	GatewayCache
	err error
}

func (s previousResponseGatewayCacheErrorStub) GetSessionAccountID(context.Context, int64, string) (int64, error) {
	return 0, s.err
}

type hardPreviousTerminalHealthStub struct {
	checks []TrafficDirectorHealthCheckInput
}

func (s *hardPreviousTerminalHealthStub) AccountHealthy(context.Context, int64, string) (bool, error) {
	return true, nil
}

func (s *hardPreviousTerminalHealthStub) Check(_ context.Context, input TrafficDirectorHealthCheckInput) (TrafficDirectorHealthDecision, error) {
	s.checks = append(s.checks, input)
	decision := TrafficDirectorHealthDecision{
		AccountID:  input.AccountID,
		Model:      input.Model,
		HealthMode: input.HealthMode,
		State:      TrafficDirectorHealthStateHealthy,
		Allowed:    true,
	}
	if input.AcquireProbe == nil || *input.AcquireProbe {
		decision.State = TrafficDirectorHealthStateOpen
		decision.Allowed = false
	}
	return decision, nil
}
