package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type staleTopKAccountRepo struct {
	schedulerTestOpenAIAccountRepo
	reads  []int64
	onRead func(int64) error
}

func (r *staleTopKAccountRepo) GetByID(ctx context.Context, id int64) (*Account, error) {
	r.reads = append(r.reads, id)
	if r.onRead != nil {
		if err := r.onRead(id); err != nil {
			return nil, err
		}
	}
	return r.schedulerTestOpenAIAccountRepo.GetByID(ctx, id)
}

// Based on the local 20260917 schedulable investigation: a cached enabled
// primary whose DB state is disabled must not hide a healthy account past Top-K.
func TestAdvancedSchedulerStaleTopKReplenishesCandidates(t *testing.T) {
	dbFailure := errors.New("database unavailable")
	for _, tc := range []struct {
		name              string
		topK              int
		disabled          int
		wantID            int64
		wantChecks        int
		fullPrimary       bool
		busyBackup        bool
		noConcurrency     bool
		zeroConcurrency   bool
		weightedSticky    bool
		cancelBefore      bool
		cancelDuringCheck bool
		dbError           error
		wantError         error
		wantWait          bool
	}{
		{name: "top_k_1", topK: 1, disabled: 1, wantID: 2, wantChecks: 2},
		{name: "top_k_2_control", topK: 2, disabled: 1, wantID: 2, wantChecks: 2},
		{name: "several_disabled", topK: 1, disabled: 4, wantID: 5, wantChecks: 5},
		{name: "weighted_sticky", topK: 1, disabled: 1, wantID: 2, wantChecks: 2, weightedSticky: true},
		{name: "no_concurrency_service", topK: 1, disabled: 2, wantID: 3, wantChecks: 3, noConcurrency: true},
		{name: "unlimited_concurrency", topK: 1, disabled: 2, wantID: 3, wantChecks: 3, zeroConcurrency: true},
		{name: "disabled_primary_appears_full", topK: 1, disabled: 1, wantID: 2, wantChecks: 2, fullPrimary: true},
		{name: "healthy_primary_still_waits", topK: 1, disabled: 0, wantID: 1, wantChecks: 1, fullPrimary: true, wantWait: true},
		{name: "healthy_busy_backup_waits", topK: 1, disabled: 1, wantID: 2, wantChecks: 2, busyBackup: true, wantWait: true},
		{name: "last_probe_succeeds", topK: 1, disabled: openAIAccountSelectionProbeLimit - 1, wantID: openAIAccountSelectionProbeLimit, wantChecks: openAIAccountSelectionProbeLimit},
		{name: "probe_limit_stops", topK: 1, disabled: openAIAccountSelectionProbeLimit, wantChecks: openAIAccountSelectionProbeLimit, wantError: ErrNoAvailableAccounts},
		{name: "db_limit_without_slots", topK: 1, disabled: openAIAccountSelectionProbeLimit, wantChecks: openAIAccountSelectionProbeLimit, noConcurrency: true, wantError: ErrNoAvailableAccounts},
		{name: "cancel_before_probe", topK: 1, disabled: 1, cancelBefore: true, wantError: context.Canceled},
		{name: "cancel_during_db_check", topK: 1, disabled: 1, wantChecks: 1, cancelDuringCheck: true, wantError: context.Canceled},
		{name: "database_error_is_fatal", topK: 1, disabled: 1, wantChecks: 1, dbError: dbFailure, wantError: dbFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			const groupID = int64(91017)
			count := tc.disabled + 1
			if count < 2 {
				count = 2
			}
			repo := &staleTopKAccountRepo{}
			snapshotCache := &openAISnapshotCacheStub{accountsByID: make(map[int64]*Account)}
			var acquired, released []int64
			concurrencyCache := schedulerTestConcurrencyCache{
				acquiredIDs: &acquired, releasedIDs: &released, loadMap: make(map[int64]*AccountLoadInfo),
			}
			for i := 0; i < count; i++ {
				cached := &Account{
					ID: int64(i + 1), Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
					Status: StatusActive, Schedulable: true, Concurrency: 1,
					Priority: i * 100, GroupIDs: []int64{groupID},
				}
				if tc.zeroConcurrency {
					cached.Concurrency = 0
				}
				latest := *cached
				latest.Schedulable = i >= tc.disabled
				latest.UpdatedAt = time.Unix(2, 0)
				repo.accounts = append(repo.accounts, latest)
				snapshotCache.snapshotAccounts = append(snapshotCache.snapshotAccounts, cached)
				snapshotCache.accountsByID[cached.ID] = cached
				if (tc.fullPrimary && i == 0) || (tc.busyBackup && i == count-1) {
					concurrencyCache.loadMap[cached.ID] = &AccountLoadInfo{AccountID: cached.ID, CurrentConcurrency: 1, LoadRate: 100}
				}
			}
			repo.onRead = func(_ int64) error {
				if tc.cancelDuringCheck {
					cancel()
				}
				return tc.dbError
			}
			cfg := newSchedulerTestSubscriptionPriorityConfig()
			cfg.Gateway.OpenAIWS.LBTopK = tc.topK
			cfg.Gateway.OpenAIWS.SchedulerScoreWeights.Load = 0
			cfg.Gateway.OpenAIWS.SchedulerScoreWeights.Queue = 0
			weighted := "false"
			sessionHash := ""
			if tc.weightedSticky {
				weighted, sessionHash = "true", "stale-topk"
			}
			svc := &OpenAIGatewayService{
				accountRepo: repo, cfg: cfg, schedulerSnapshot: &SchedulerSnapshotService{cache: snapshotCache},
				cache:              &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:stale-topk": 1}},
				rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true", weighted, "false"),
				concurrencyService: NewConcurrencyService(concurrencyCache),
			}
			if tc.noConcurrency {
				svc.concurrencyService = nil
			}
			if tc.cancelBefore {
				cancel()
			}
			group := groupID
			selection, decision, err := svc.SelectAccountWithScheduler(ctx, &group, "", sessionHash, "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			if selection != nil && selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
			if tc.wantError != nil {
				require.ErrorIs(t, err, tc.wantError)
				require.Nil(t, selection)
			} else {
				require.NoError(t, err)
				require.NotNil(t, selection)
				require.Equal(t, tc.wantID, selection.Account.ID)
				require.Equal(t, tc.wantWait, selection.WaitPlan != nil)
				require.Equal(t, tc.topK, decision.TopK)
			}
			if tc.topK == 2 {
				// Weighted selection may choose the healthy backup first.
				require.NotEmpty(t, repo.reads)
				require.LessOrEqual(t, len(repo.reads), tc.wantChecks)
			} else {
				require.Len(t, repo.reads, tc.wantChecks, "DB reads: %v", repo.reads)
			}
			seen := make(map[int64]struct{})
			for _, id := range repo.reads {
				_, duplicate := seen[id]
				require.False(t, duplicate, "account %d checked repeatedly", id)
				seen[id] = struct{}{}
			}
			require.LessOrEqual(t, len(acquired), openAIAccountSelectionProbeLimit)
			require.Equal(t, acquired, released, "every acquired slot must be released once")
		})
	}
}
