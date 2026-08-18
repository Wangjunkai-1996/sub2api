package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type openAIAccountAuditRoutingLoadSequenceCache struct {
	ConcurrencyCache
	mu       sync.Mutex
	loadCall int
	cached   map[int64]*AccountLoadInfo
	fresh    map[int64]*AccountLoadInfo
}

func (c *openAIAccountAuditRoutingLoadSequenceCache) GetAccountsLoadBatch(context.Context, []AccountWithConcurrency) (map[int64]*AccountLoadInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loadCall++
	if c.loadCall == 1 {
		return c.cached, nil
	}
	return c.fresh, nil
}

func (c *openAIAccountAuditRoutingLoadSequenceCache) AcquireAccountSlot(context.Context, int64, int, string) (bool, error) {
	return true, nil
}

func (c *openAIAccountAuditRoutingLoadSequenceCache) ReleaseAccountSlot(context.Context, int64, string) error {
	return nil
}

func openAIAccountAuditRoutingTestPolicy() OpenAIAccountAuditRoutingPolicy {
	return newOpenAIAccountAuditRoutingPolicy(OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs:       []int64{12},
		LongTextRuneThreshold: 12000,
		PreferAPIKeyEnabled:   true,
	}, true)
}

func newOpenAIAccountAuditRoutingSchedulerTestService(
	t *testing.T,
	advanced bool,
	groupID int64,
	snapshotAccounts []*Account,
	dbAccounts []Account,
	cache *schedulerTestGatewayCache,
	concurrency ConcurrencyCache,
	configure func(*config.Config),
) *OpenAIGatewayService {
	t.Helper()
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	cfg := &config.Config{RunMode: config.RunModeStandard}
	cfg.Gateway.Scheduling.LoadBatchEnabled = true
	cfg.Gateway.OpenAIWS.LBTopK = 8
	if configure != nil {
		configure(cfg)
	}
	if cache == nil {
		cache = &schedulerTestGatewayCache{}
	}
	accountsByID := make(map[int64]*Account, len(snapshotAccounts))
	for _, account := range snapshotAccounts {
		if account != nil {
			accountsByID[account.ID] = account
		}
	}
	svc := &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: dbAccounts},
		cache:              cache,
		cfg:                cfg,
		schedulerSnapshot:  &SchedulerSnapshotService{cache: &openAISnapshotCacheStub{snapshotAccounts: snapshotAccounts, accountsByID: accountsByID}},
		concurrencyService: NewConcurrencyService(concurrency),
	}
	if advanced {
		svc.rateLimitService = newOpenAIAdvancedSchedulerRateLimitService("true")
	}
	_ = groupID
	return svc
}

func TestOpenAIAccountRoutingPreferAPIKeyAdvancedAndLegacy(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		name := "legacy"
		if advanced {
			name = "advanced"
		}
		t.Run(name, func(t *testing.T) {
			groupID := int64(51001)
			oauth := Account{ID: 51011, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0, GroupIDs: []int64{groupID}}
			apiKey := Account{ID: 51012, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 50, GroupIDs: []int64{groupID}}
			svc := newOpenAIAccountAuditRoutingSchedulerTestService(t, advanced, groupID, []*Account{&oauth, &apiKey}, []Account{oauth, apiKey}, nil, schedulerTestConcurrencyCache{
				loadMap: map[int64]*AccountLoadInfo{
					oauth.ID:  {AccountID: oauth.ID, LoadRate: 0},
					apiKey.ID: {AccountID: apiKey.ID, LoadRate: 90},
				},
			}, nil)
			ctx := WithOpenAIAccountRoutingOptions(context.Background(), OpenAIAccountRoutingOptions{Preference: OpenAIAccountRoutingPreferenceAPIKey})

			selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			require.NoError(t, err)
			require.NotNil(t, selection)
			require.Equal(t, apiKey.ID, selection.Account.ID)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}

func TestOpenAIAccountRoutingPreferAPIKeyFallsBackToOAuthAdvancedAndLegacy(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		name := "legacy"
		if advanced {
			name = "advanced"
		}
		t.Run(name, func(t *testing.T) {
			groupID := int64(51002)
			oauth := Account{ID: 51021, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{groupID}}
			svc := newOpenAIAccountAuditRoutingSchedulerTestService(t, advanced, groupID, []*Account{&oauth}, []Account{oauth}, nil, schedulerTestConcurrencyCache{}, nil)
			ctx := WithOpenAIAccountRoutingOptions(context.Background(), OpenAIAccountRoutingOptions{Preference: OpenAIAccountRoutingPreferenceAPIKey})

			selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			require.NoError(t, err)
			require.Equal(t, oauth.ID, selection.Account.ID)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}

func auditedOAuthRoutingOptions() OpenAIAccountRoutingOptions {
	return OpenAIAccountRoutingOptions{
		Preference:         OpenAIAccountRoutingPreferenceAuditedOAuth,
		AuditRoutingReason: OpenAIAccountAuditRoutingLongText,
		AuditPolicy:        openAIAccountAuditRoutingTestPolicy(),
	}
}

func TestOpenAIAccountRoutingAuditedOAuthPreferenceAdvancedAndLegacy(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		name := "legacy"
		if advanced {
			name = "advanced"
		}
		t.Run(name, func(t *testing.T) {
			groupID := int64(51009)
			pro := Account{
				ID: 51091, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
				Concurrency: 1, Priority: 150, GroupIDs: []int64{groupID, 12}, Credentials: map[string]any{"plan_type": "pro"},
			}
			apiKey := Account{ID: 51092, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0, GroupIDs: []int64{groupID}}
			plus := Account{
				ID: 51093, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
				Concurrency: 1, Priority: 0, GroupIDs: []int64{groupID, 12}, Credentials: map[string]any{"plan_type": "plus"},
			}
			svc := newOpenAIAccountAuditRoutingSchedulerTestService(t, advanced, groupID, []*Account{&pro, &apiKey, &plus}, []Account{pro, apiKey, plus}, nil, schedulerTestConcurrencyCache{
				loadMap: map[int64]*AccountLoadInfo{
					pro.ID: {AccountID: pro.ID, LoadRate: 90}, apiKey.ID: {AccountID: apiKey.ID}, plus.ID: {AccountID: plus.ID},
				},
			}, nil)
			ctx := WithOpenAIAccountRoutingOptions(context.Background(), auditedOAuthRoutingOptions())

			selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			require.NoError(t, err)
			require.NotNil(t, selection)
			require.Equal(t, pro.ID, selection.Account.ID)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}

func TestOpenAIAccountRoutingAuditedOAuthRejectsPlusAndFallsBackToAPIKeyAdvancedAndLegacy(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		name := "legacy"
		if advanced {
			name = "advanced"
		}
		t.Run(name, func(t *testing.T) {
			groupID := int64(51010)
			plus := Account{
				ID: 51101, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
				Concurrency: 1, Priority: 0, GroupIDs: []int64{groupID, 12}, Credentials: map[string]any{"plan_type": "plus"},
			}
			apiKey := Account{ID: 51102, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 150, GroupIDs: []int64{groupID}}
			svc := newOpenAIAccountAuditRoutingSchedulerTestService(t, advanced, groupID, []*Account{&plus, &apiKey}, []Account{plus, apiKey}, nil, schedulerTestConcurrencyCache{}, nil)
			ctx := WithOpenAIAccountRoutingOptions(context.Background(), auditedOAuthRoutingOptions())

			selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			require.NoError(t, err)
			require.NotNil(t, selection)
			require.Equal(t, apiKey.ID, selection.Account.ID, "Plus OAuth must not enter the audited long-text rollout pool")
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}

func TestOpenAIAccountRoutingAuditedOAuthFreshEligibilityRecheckAdvancedAndLegacy(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		name := "legacy"
		if advanced {
			name = "advanced"
		}
		t.Run(name, func(t *testing.T) {
			groupID := int64(51011)
			stalePro := Account{
				ID: 51111, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
				Concurrency: 1, Priority: 0, GroupIDs: []int64{groupID, 12}, Credentials: map[string]any{"plan_type": "pro"},
			}
			freshPlus := stalePro
			freshPlus.Credentials = map[string]any{"plan_type": "plus"}
			apiKey := Account{ID: 51112, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 150, GroupIDs: []int64{groupID}}
			svc := newOpenAIAccountAuditRoutingSchedulerTestService(t, advanced, groupID, []*Account{&stalePro, &apiKey}, []Account{freshPlus, apiKey}, nil, schedulerTestConcurrencyCache{}, nil)
			ctx := WithOpenAIAccountRoutingOptions(context.Background(), auditedOAuthRoutingOptions())

			selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			require.NoError(t, err)
			require.NotNil(t, selection)
			require.Equal(t, apiKey.ID, selection.Account.ID)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}

func TestOpenAIAccountRoutingFreshAuditedOAuthPromotionPrecedesAPIKeyAdvancedAndLegacy(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		name := "legacy"
		if advanced {
			name = "advanced"
		}
		t.Run(name, func(t *testing.T) {
			groupID := int64(51014)
			stalePlus := Account{
				ID: 51141, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
				Concurrency: 1, Priority: 0, GroupIDs: []int64{groupID, 12}, Credentials: map[string]any{"plan_type": "plus"},
			}
			freshPro := stalePlus
			freshPro.Credentials = map[string]any{"plan_type": "pro"}
			apiKey := Account{
				ID: 51142, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true,
				Concurrency: 1, Priority: 150, GroupIDs: []int64{groupID},
			}
			svc := newOpenAIAccountAuditRoutingSchedulerTestService(
				t, advanced, groupID, []*Account{&stalePlus, &apiKey}, []Account{freshPro, apiKey},
				nil, schedulerTestConcurrencyCache{}, nil,
			)
			ctx := WithOpenAIAccountRoutingOptions(context.Background(), auditedOAuthRoutingOptions())

			selection, _, err := svc.SelectAccountWithScheduler(
				ctx, &groupID, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false,
			)
			require.NoError(t, err)
			require.NotNil(t, selection)
			require.Equal(t, freshPro.ID, selection.Account.ID)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}

func TestOpenAIAccountRoutingBindingsOverrideAuditedOAuthPreferenceAdvancedAndLegacy(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		name := "legacy"
		if advanced {
			name = "advanced"
		}
		t.Run(name+"/session", func(t *testing.T) {
			groupID := int64(51012)
			pro := Account{ID: 51121, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{groupID, 12}, Credentials: map[string]any{"plan_type": "pro"}}
			apiKey := Account{ID: 51122, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{groupID}}
			cache := &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:audit-oauth-binding": apiKey.ID}}
			svc := newOpenAIAccountAuditRoutingSchedulerTestService(t, advanced, groupID, []*Account{&pro, &apiKey}, []Account{pro, apiKey}, cache, schedulerTestConcurrencyCache{}, nil)
			ctx := WithOpenAIAccountRoutingOptions(context.Background(), auditedOAuthRoutingOptions())

			selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "audit-oauth-binding", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			require.NoError(t, err)
			require.Equal(t, apiKey.ID, selection.Account.ID)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})

		t.Run(name+"/previous_response", func(t *testing.T) {
			groupID := int64(51013)
			pro := Account{ID: 51131, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{groupID, 12}, Credentials: map[string]any{"plan_type": "pro"}}
			apiKey := Account{ID: 51132, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{groupID}}
			svc := newOpenAIAccountAuditRoutingSchedulerTestService(t, advanced, groupID, []*Account{&pro, &apiKey}, []Account{pro, apiKey}, nil, schedulerTestConcurrencyCache{}, nil)
			require.NoError(t, svc.getOpenAIWSStateStore().BindResponseAccount(context.Background(), groupID, "resp_audited_oauth_bound", apiKey.ID, time.Hour))
			ctx := WithOpenAIHardBoundHTTPContinuation(context.Background())
			ctx = WithOpenAIAccountRoutingOptions(ctx, auditedOAuthRoutingOptions())

			selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "resp_audited_oauth_bound", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			require.NoError(t, err)
			require.Equal(t, apiKey.ID, selection.Account.ID)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}

func TestOpenAIAccountRoutingFreshLoadKeepsAPIKeyAheadOfCachedOAuth(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		name := "legacy"
		if advanced {
			name = "advanced"
		}
		t.Run(name, func(t *testing.T) {
			groupID := int64(51008)
			oauth := Account{ID: 51081, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0, GroupIDs: []int64{groupID}}
			apiKey := Account{ID: 51082, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 50, GroupIDs: []int64{groupID}}
			concurrency := &openAIAccountAuditRoutingLoadSequenceCache{
				cached: map[int64]*AccountLoadInfo{
					oauth.ID:  {AccountID: oauth.ID, LoadRate: 0, CurrentConcurrency: 0},
					apiKey.ID: {AccountID: apiKey.ID, LoadRate: 100, CurrentConcurrency: 1},
				},
				fresh: map[int64]*AccountLoadInfo{
					oauth.ID:  {AccountID: oauth.ID, LoadRate: 0, CurrentConcurrency: 0},
					apiKey.ID: {AccountID: apiKey.ID, LoadRate: 0, CurrentConcurrency: 0},
				},
			}
			svc := newOpenAIAccountAuditRoutingSchedulerTestService(t, advanced, groupID, []*Account{&oauth, &apiKey}, []Account{oauth, apiKey}, nil, concurrency, nil)
			ctx := WithOpenAIAccountRoutingOptions(context.Background(), OpenAIAccountRoutingOptions{Preference: OpenAIAccountRoutingPreferenceAPIKey})

			selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			require.NoError(t, err)
			require.NotNil(t, selection)
			require.Equal(t, apiKey.ID, selection.Account.ID)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}

func TestOpenAIAccountRoutingStickyBindingOverridesPreferenceAdvancedAndLegacy(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		name := "legacy"
		if advanced {
			name = "advanced"
		}
		t.Run(name, func(t *testing.T) {
			groupID := int64(51003)
			oauth := Account{ID: 51031, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 50, GroupIDs: []int64{groupID}}
			apiKey := Account{ID: 51032, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0, GroupIDs: []int64{groupID}}
			cache := &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:audit-routing-sticky": oauth.ID}}
			svc := newOpenAIAccountAuditRoutingSchedulerTestService(t, advanced, groupID, []*Account{&oauth, &apiKey}, []Account{oauth, apiKey}, cache, schedulerTestConcurrencyCache{}, nil)
			ctx := WithOpenAIAccountRoutingOptions(context.Background(), OpenAIAccountRoutingOptions{Preference: OpenAIAccountRoutingPreferenceAPIKey})

			selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "audit-routing-sticky", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			require.NoError(t, err)
			require.Equal(t, oauth.ID, selection.Account.ID)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}

func TestOpenAIAccountRoutingPreviousResponseHardBindingOverridesPreferenceAdvancedAndLegacy(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		name := "legacy"
		if advanced {
			name = "advanced"
		}
		t.Run(name, func(t *testing.T) {
			groupID := int64(51004)
			oauth := Account{ID: 51041, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 50, GroupIDs: []int64{groupID}}
			apiKey := Account{ID: 51042, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0, GroupIDs: []int64{groupID}}
			svc := newOpenAIAccountAuditRoutingSchedulerTestService(t, advanced, groupID, []*Account{&oauth, &apiKey}, []Account{oauth, apiKey}, nil, schedulerTestConcurrencyCache{}, nil)
			require.NoError(t, svc.getOpenAIWSStateStore().BindResponseAccount(context.Background(), groupID, "resp_audit_routing_bound", oauth.ID, time.Hour))
			ctx := WithOpenAIHardBoundHTTPContinuation(context.Background())
			ctx = WithOpenAIAccountRoutingOptions(ctx, OpenAIAccountRoutingOptions{Preference: OpenAIAccountRoutingPreferenceAPIKey})

			selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "resp_audit_routing_bound", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			require.NoError(t, err)
			require.Equal(t, oauth.ID, selection.Account.ID)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}

func TestOpenAIAccountRoutingFreshTypeRecheckPreservesAPIKeyPreference(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		name := "legacy"
		if advanced {
			name = "advanced"
		}
		t.Run(name, func(t *testing.T) {
			groupID := int64(51005)
			staleRetyped := Account{ID: 51051, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0, GroupIDs: []int64{groupID}}
			stableAPIKey := Account{ID: 51052, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 50, GroupIDs: []int64{groupID}}
			freshRetyped := staleRetyped
			freshRetyped.Type = AccountTypeOAuth
			freshRetyped.Credentials = map[string]any{"plan_type": "free"}
			svc := newOpenAIAccountAuditRoutingSchedulerTestService(t, advanced, groupID, []*Account{&staleRetyped, &stableAPIKey}, []Account{freshRetyped, stableAPIKey}, nil, schedulerTestConcurrencyCache{}, nil)
			ctx := WithOpenAIAccountRoutingOptions(context.Background(), OpenAIAccountRoutingOptions{Preference: OpenAIAccountRoutingPreferenceAPIKey})

			selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			require.NoError(t, err)
			require.Equal(t, stableAPIKey.ID, selection.Account.ID)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}

func TestOpenAIAccountRoutingFreshAPIKeyIsNotRejectedByStaleAuditRequirements(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		name := "legacy"
		if advanced {
			name = "advanced"
		}
		t.Run(name, func(t *testing.T) {
			groupID := int64(51007)
			staleOAuthPro := Account{
				ID: 51071, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
				Concurrency: 1, GroupIDs: []int64{groupID, 12}, Credentials: map[string]any{"plan_type": "pro"},
				Extra: map[string]any{"openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough},
			}
			freshAPIKey := staleOAuthPro
			freshAPIKey.Type = AccountTypeAPIKey
			freshAPIKey.Credentials = map[string]any{"api_key": "sk-fresh"}
			svc := newOpenAIAccountAuditRoutingSchedulerTestService(t, advanced, groupID, []*Account{&staleOAuthPro}, []Account{freshAPIKey}, nil, schedulerTestConcurrencyCache{}, func(cfg *config.Config) {
				wsCfg := newSchedulerTestOpenAIWSV2Config()
				cfg.Gateway.OpenAIWS = wsCfg.Gateway.OpenAIWS
				cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			})
			ctx := WithOpenAIAccountRoutingOptions(context.Background(), OpenAIAccountRoutingOptions{
				Preference:             OpenAIAccountRoutingPreferenceAPIKey,
				AuditPolicy:            openAIAccountAuditRoutingTestPolicy(),
				AuditRequiredTransport: OpenAIUpstreamTransportResponsesWebsocketV2AuditedIngress,
			})

			selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			require.NoError(t, err)
			require.NotNil(t, selection)
			require.Equal(t, freshAPIKey.ID, selection.Account.ID)
			require.Equal(t, AccountTypeAPIKey, selection.Account.Type)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}

func TestOpenAIAccountRoutingFreshPlanAndGroupRecheckAppliesConditionalRequirements(t *testing.T) {
	for _, advanced := range []bool{false, true} {
		for _, change := range []string{"plan", "group"} {
			name := "legacy/" + change
			if advanced {
				name = "advanced/" + change
			}
			t.Run(name, func(t *testing.T) {
				groupID := int64(51006)
				staleOAuth := Account{
					ID: 51061, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true,
					Concurrency: 1, Priority: 0, GroupIDs: []int64{groupID, 12}, Credentials: map[string]any{"plan_type": "free"},
					Extra: map[string]any{"openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough},
				}
				freshOAuth := staleOAuth
				freshOAuth.Credentials = map[string]any{"plan_type": "pro"}
				if change == "group" {
					staleOAuth.Credentials = map[string]any{"plan_type": "pro"}
					staleOAuth.GroupIDs = []int64{groupID, 13}
					freshOAuth.GroupIDs = []int64{groupID, 12}
				}
				apiKey := Account{ID: 51062, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 50, GroupIDs: []int64{groupID}}
				svc := newOpenAIAccountAuditRoutingSchedulerTestService(t, advanced, groupID, []*Account{&staleOAuth, &apiKey}, []Account{freshOAuth, apiKey}, nil, schedulerTestConcurrencyCache{}, func(cfg *config.Config) {
					wsCfg := newSchedulerTestOpenAIWSV2Config()
					cfg.Gateway.OpenAIWS = wsCfg.Gateway.OpenAIWS
					cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
				})
				ctx := WithOpenAIAccountRoutingOptions(context.Background(), OpenAIAccountRoutingOptions{
					AuditPolicy:            openAIAccountAuditRoutingTestPolicy(),
					AuditRequiredTransport: OpenAIUpstreamTransportResponsesWebsocketV2AuditedIngress,
				})

				selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
				require.NoError(t, err)
				require.Equal(t, apiKey.ID, selection.Account.ID)
				if selection.ReleaseFunc != nil {
					selection.ReleaseFunc()
				}
			})
		}
	}
}
