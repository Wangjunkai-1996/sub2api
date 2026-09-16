package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAISelectionDiagnosticsDoNotInventUnobservedEligibility(t *testing.T) {
	stats := openAISelectionFilterStats{pool: 3}
	stats.exclude("excluded")
	stats.exclude("model_not_supported")
	stats.exclude("runtime_blocked")
	summary := stats.summary("")
	require.Contains(t, summary, "scope=post_prefilter_snapshot")
	require.Contains(t, summary, "eligible_now=0")
	require.Contains(t, summary, "stop_reason=initial_filter_exhausted")
	require.NotContains(t, summary, "configured_total")
	// A bounded Top-K probe/admission failure does not prove that every account
	// outside the observed selection order is currently unavailable.
	stats = openAISelectionFilterStats{pool: 40}
	stats.exclude("excluded")
	summary = stats.summary("selection_order_exhausted")
	require.Contains(t, summary, "eligible_now=unknown")
	require.Contains(t, summary, "stop_reason=selection_order_exhausted")
	stats = openAISelectionFilterStats{pool: 1}
	stats.exclude("excluded")
	require.Contains(t, stats.summary(""), "stop_reason=request_exclusions_exhausted")
	require.Contains(t, (openAISelectionFilterStats{}).summary(""), "stop_reason=prefiltered_pool_empty")
}

func TestOpenAIAdvancedSchedulerTopKFailoverReachesLowerPriority(t *testing.T) {
	for _, weightedSticky := range []string{"false", "true"} {
		t.Run("weighted_sticky_"+weightedSticky, func(t *testing.T) {
			ctx := context.Background()
			groupID := int64(21)
			accounts := make([]Account, 3)
			for i := range accounts {
				accounts[i] = Account{
					ID: int64(i + 1), Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
					Status: StatusActive, Schedulable: true, Concurrency: 1,
					Priority: i * 100, GroupIDs: []int64{groupID},
				}
			}
			cfg := newSchedulerTestSubscriptionPriorityConfig()
			cfg.Gateway.OpenAIWS.SchedulerScoreWeights.Load = 0
			cfg.Gateway.OpenAIWS.SchedulerScoreWeights.Queue = 0
			svc := &OpenAIGatewayService{
				accountRepo: schedulerGroupAwareOpenAIAccountRepo{schedulerTestOpenAIAccountRepo{accounts: accounts}},
				cache:       &schedulerTestGatewayCache{}, cfg: cfg,
				rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true", weightedSticky),
				concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{}),
			}
			excluded := make(map[int64]struct{})
			for _, wantID := range []int64{1, 2, 3} {
				selection, decision, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "same-session", "gpt-5.1", excluded, OpenAIUpstreamTransportAny, false)
				require.NoError(t, err)
				require.NotNil(t, selection)
				require.Equal(t, wantID, selection.Account.ID)
				require.Equal(t, openAIAccountScheduleLayerLoadBalance, decision.Layer)
				require.Equal(t, 1, decision.TopK)
				if selection.ReleaseFunc != nil {
					selection.ReleaseFunc()
				}
				excluded[wantID] = struct{}{}
			}
			selection, _, err := svc.SelectAccountWithScheduler(ctx, &groupID, "", "same-session", "gpt-5.1", excluded, OpenAIUpstreamTransportAny, false)
			require.Nil(t, selection)
			require.ErrorIs(t, err, ErrNoAvailableAccounts)
			require.Contains(t, err.Error(), "pool=3, filtered: excluded=3")
			require.Contains(t, err.Error(), "eligible_now=0, stop_reason=request_exclusions_exhausted")
		})
	}
}
