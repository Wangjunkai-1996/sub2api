//go:build integration

package repository

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type AccountEgressCacheSuite struct {
	IntegrationRedisSuite
	cache *concurrencyCache
}

func TestAccountEgressCacheSuite(t *testing.T) {
	suite.Run(t, new(AccountEgressCacheSuite))
}

func (s *AccountEgressCacheSuite) SetupTest() {
	s.IntegrationRedisSuite.SetupTest()
	s.cache = NewConcurrencyCache(s.rdb, 15, 120).(*concurrencyCache)
}

func (s *AccountEgressCacheSuite) TestPerIdentityCapsSharedIdentityAffinityAndIdempotency() {
	first := accountEgressTestCandidate(0, 10001, "ip:a")
	shared := accountEgressTestCandidate(1, 10002, "ip:a")
	second := accountEgressTestCandidate(2, 10003, "ip:b")
	third := accountEgressTestCandidate(3, 10004, "ip:c")
	config := accountEgressTestConfig(20001, 2, 0, first, shared, second, third)
	syncAccountEgressTestConfig(s.T(), s.cache, config)

	require.Equal(s.T(), service.AccountEgressStatusAcquired, acquireAccountEgressTest(s.T(), s.cache, config, "shared-a", first.BindingID, "").Status)
	require.Equal(s.T(), service.AccountEgressStatusAcquired, acquireAccountEgressTest(s.T(), s.cache, config, "shared-b", shared.BindingID, "").Status)
	require.Equal(s.T(), service.AccountEgressStatusFull, acquireAccountEgressTest(s.T(), s.cache, config, "shared-overflow", shared.BindingID, "").Status)

	preferred := acquireAccountEgressTest(s.T(), s.cache, config, "preferred-spill", "", first.BindingID)
	require.Equal(s.T(), service.AccountEgressStatusAcquired, preferred.Status)
	require.NotEqual(s.T(), "ip:a", preferred.IdentityID)
	retry := acquireAccountEgressTest(s.T(), s.cache, config, "preferred-spill", "", first.BindingID)
	require.Equal(s.T(), preferred.BindingID, retry.BindingID)
	require.Equal(s.T(), preferred.ActiveTotal, retry.ActiveTotal)

	for i := 0; i < 3; i++ {
		result := acquireAccountEgressTest(s.T(), s.cache, config, fmt.Sprintf("fill-%d", i), "", "")
		require.Equal(s.T(), service.AccountEgressStatusAcquired, result.Status)
	}
	require.Equal(s.T(), service.AccountEgressStatusFull, acquireAccountEgressTest(s.T(), s.cache, config, "all-full", "", "").Status)
	for _, identityID := range []string{"ip:a", "ip:b", "ip:c"} {
		count, err := s.rdb.ZCard(s.ctx, accountEgressIdentityKey(config.AccountID, identityID)).Result()
		require.NoError(s.T(), err)
		require.Equal(s.T(), int64(2), count)
	}
}

func (s *AccountEgressCacheSuite) TestFIFOReleaseAndRefreshFencing() {
	candidate := accountEgressTestCandidate(0, 10101, "ip:a")
	config := accountEgressTestConfig(20101, 1, 3, candidate)
	syncAccountEgressTestConfig(s.T(), s.cache, config)

	owner := acquireAccountEgressTest(s.T(), s.cache, config, "owner", "", "")
	require.Equal(s.T(), service.AccountEgressStatusFull, acquireAccountEgressTest(s.T(), s.cache, config, "second", "", "").Status)
	require.Equal(s.T(), service.AccountEgressStatusNotQueueHead, acquireAccountEgressTest(s.T(), s.cache, config, "third", "", "").Status)
	releaseAccountEgressTest(s.T(), s.cache, config, owner)
	require.Equal(s.T(), service.AccountEgressStatusNotQueueHead, acquireAccountEgressTest(s.T(), s.cache, config, "third", "", "").Status)
	second := acquireAccountEgressTest(s.T(), s.cache, config, "second", "", "")
	require.Equal(s.T(), service.AccountEgressStatusAcquired, second.Status)

	ref := service.AccountEgressLeaseRef{AccountID: config.AccountID, ID: second.LeaseID, BindingID: second.BindingID, RouteID: second.RouteID, IdentityID: second.IdentityID, ConfigVersion: second.ConfigVersion, AuthorityRevision: second.AuthorityRevision}
	statuses, err := s.cache.RefreshAccountEgressLeases(s.ctx, []service.AccountEgressLeaseRef{ref}, service.AccountEgressLeaseTTL)
	require.NoError(s.T(), err)
	require.Equal(s.T(), service.AccountEgressLeaseRefreshActive, statuses[ref.Key()])
	require.NoError(s.T(), s.rdb.ZRem(s.ctx, accountEgressTotalKey(config.AccountID), accountEgressIDHash(second.LeaseID)).Err())
	statuses, err = s.cache.RefreshAccountEgressLeases(s.ctx, []service.AccountEgressLeaseRef{ref}, service.AccountEgressLeaseTTL)
	require.NoError(s.T(), err)
	require.Equal(s.T(), service.AccountEgressLeaseRefreshLost, statuses[ref.Key()], "refresh must not recreate a missing total-fence member")
}

func (s *AccountEgressCacheSuite) TestConfigVersionBatchLoadAndImmediateCapacityRestore() {
	first := accountEgressTestCandidate(0, 10201, "ip:a")
	second := accountEgressTestCandidate(1, 10202, "ip:b")
	config := accountEgressTestConfig(20201, 1, 2, first, second)
	config.Version = 4
	syncAccountEgressTestConfig(s.T(), s.cache, config)

	stale := config
	stale.Version = 3
	statuses, err := s.cache.SyncAccountEgressConfigs(s.ctx, []service.AccountEgressPoolConfig{stale})
	require.NoError(s.T(), err)
	require.Equal(s.T(), service.AccountEgressConfigSyncStale, statuses[config.AccountID])

	firstLease := acquireAccountEgressTest(s.T(), s.cache, config, "load-a", first.BindingID, "")
	secondLease := acquireAccountEgressTest(s.T(), s.cache, config, "load-b", second.BindingID, "")
	require.Equal(s.T(), service.AccountEgressStatusFull, acquireAccountEgressTest(s.T(), s.cache, config, "load-waiter", "", "").Status)
	loads, err := s.cache.GetAccountEgressLoadsBatch(s.ctx, []service.AccountEgressPoolConfig{config}, service.AccountEgressLeaseTTL, 2*time.Minute)
	require.NoError(s.T(), err)
	load := loads[config.AccountID]
	require.Equal(s.T(), 2, load.ActiveTotal)
	require.Equal(s.T(), 1, load.WaitingCount)
	require.Equal(s.T(), 2, load.EffectiveCapacity)
	require.Equal(s.T(), 150, load.LoadRate)

	releaseAccountEgressTest(s.T(), s.cache, config, firstLease)
	result := acquireAccountEgressTest(s.T(), s.cache, config, "load-waiter", "", "")
	require.Equal(s.T(), service.AccountEgressStatusAcquired, result.Status)
	releaseAccountEgressTest(s.T(), s.cache, config, secondLease)
}

func (s *AccountEgressCacheSuite) TestLegacyLiveAndWarmupGates() {
	config := accountEgressTestConfig(20301, 1, 0, accountEgressTestCandidate(0, 10301, "ip:a"))
	syncAccountEgressTestConfig(s.T(), s.cache, config)

	legacy, err := s.cache.AcquireLiveLease(s.ctx, config.AccountID, 1, 20302, 1, 20303, "legacy-live", false)
	require.NoError(s.T(), err)
	require.True(s.T(), legacy)
	require.Equal(s.T(), service.AccountEgressStatusLegacyDraining, acquireAccountEgressTest(s.T(), s.cache, config, "pool-blocked", "", "").Status)
	require.NoError(s.T(), s.cache.ReleaseLiveLease(s.ctx, config.AccountID, 20302, 20303, "legacy-live"))

	pool := acquireAccountEgressTest(s.T(), s.cache, config, "pool", "", "")
	require.Equal(s.T(), service.AccountEgressStatusAcquired, pool.Status)
	legacy, err = s.cache.AcquireLiveLease(s.ctx, config.AccountID, 1, 20302, 1, 20303, "legacy-blocked", false)
	require.NoError(s.T(), err)
	require.False(s.T(), legacy)
	warmup, err := s.cache.AcquireAccountExclusive(s.ctx, config.AccountID, "warmup", time.Minute)
	require.NoError(s.T(), err)
	require.False(s.T(), warmup)
	releaseAccountEgressTest(s.T(), s.cache, config, pool)
	warmup, err = s.cache.AcquireAccountExclusive(s.ctx, config.AccountID, "warmup", time.Minute)
	require.NoError(s.T(), err)
	require.True(s.T(), warmup)
	require.Equal(s.T(), service.AccountEgressStatusExclusive, acquireAccountEgressTest(s.T(), s.cache, config, "pool-exclusive", "", "").Status)
	released, err := s.cache.ReleaseAccountExclusive(s.ctx, config.AccountID, "warmup")
	require.NoError(s.T(), err)
	require.True(s.T(), released)
}

func (s *AccountEgressCacheSuite) TestConcurrentCacheInstancesNeverExceedIdentityLimit() {
	config := accountEgressTestConfig(20401, 4, 0, accountEgressTestCandidate(0, 10401, "ip:a"))
	syncAccountEgressTestConfig(s.T(), s.cache, config)
	other := NewConcurrencyCache(s.rdb, 15, 120).(*concurrencyCache)

	type outcome struct {
		result service.AccountEgressAcquireResult
		err    error
	}
	outcomes := make(chan outcome, 64)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			cache := s.cache
			if index%2 == 1 {
				cache = other
			}
			result, err := cache.AcquireAccountEgress(context.Background(), service.AccountEgressCacheAcquireRequest{
				AccountEgressAcquireRequest: service.AccountEgressAcquireRequest{Config: config, LeaseID: fmt.Sprintf("parallel-%d", index)},
				LeaseTTL:                    service.AccountEgressLeaseTTL,
				WaiterTTL:                   2 * time.Minute,
			})
			outcomes <- outcome{result: result, err: err}
		}(i)
	}
	wg.Wait()
	close(outcomes)
	acquired := 0
	for outcome := range outcomes {
		require.NoError(s.T(), outcome.err)
		if outcome.result.Status == service.AccountEgressStatusAcquired {
			acquired++
		}
	}
	require.Equal(s.T(), 4, acquired)
	count, err := s.rdb.ZCard(s.ctx, accountEgressIdentityKey(config.AccountID, "ip:a")).Result()
	require.NoError(s.T(), err)
	require.Equal(s.T(), int64(4), count)
}

func (s *AccountEgressCacheSuite) TestTransitionBridgeCountsLegacyByAdmissionIdentityAcrossClients() {
	primary := accountEgressTestCandidate(0, 10501, "ip:primary")
	second := accountEgressTestCandidate(1, 10502, "ip:second")
	third := accountEgressTestCandidate(2, 10503, "ip:third")
	config := accountEgressTestConfig(20501, 3, 0, primary, second, third)
	syncAccountEgressTestConfig(s.T(), s.cache, config)
	other := NewConcurrencyCache(s.rdb, 15, 120).(*concurrencyCache)

	for index := 0; index < 3; index++ {
		acquired, err := s.cache.AcquireAccountSlotForEgress(s.ctx, config.AccountID, 3, fmt.Sprintf("bridge-legacy-%d", index), primary.IdentityID)
		require.NoError(s.T(), err)
		require.True(s.T(), acquired)
	}

	type outcome struct {
		result service.AccountEgressAcquireResult
		err    error
	}
	outcomes := make(chan outcome, 32)
	var wg sync.WaitGroup
	for index := 0; index < 32; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			cache := s.cache
			if index%2 == 1 {
				cache = other
			}
			result, err := cache.AcquireAccountEgress(context.Background(), service.AccountEgressCacheAcquireRequest{
				AccountEgressAcquireRequest: service.AccountEgressAcquireRequest{Config: config, LeaseID: fmt.Sprintf("bridge-pool-%d", index)},
				LeaseTTL:                    service.AccountEgressLeaseTTL,
				WaiterTTL:                   2 * time.Minute,
			})
			outcomes <- outcome{result: result, err: err}
		}(index)
	}
	wg.Wait()
	close(outcomes)
	acquiredPool := 0
	for outcome := range outcomes {
		require.NoError(s.T(), outcome.err)
		if outcome.result.Status == service.AccountEgressStatusAcquired {
			acquiredPool++
		}
	}
	require.Equal(s.T(), 6, acquiredPool)
	require.Equal(s.T(), "transition", s.rdb.Get(s.ctx, accountEgressModeKey(config.AccountID)).Val())

	loads, err := s.cache.GetAccountEgressLoadsBatch(s.ctx, []service.AccountEgressPoolConfig{config}, service.AccountEgressLeaseTTL, 2*time.Minute)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 9, loads[config.AccountID].ActiveTotal)
	require.Equal(s.T(), map[string]int{
		primary.IdentityID: 3,
		second.IdentityID:  3,
		third.IdentityID:   3,
	}, loads[config.AccountID].IdentityLoads)

	require.NoError(s.T(), s.cache.ReleaseAccountSlotForEgress(s.ctx, config.AccountID, "bridge-legacy-0", primary.IdentityID))
	primaryPool := acquireAccountEgressTest(s.T(), other, config, "bridge-primary-after-release", primary.BindingID, "")
	require.Equal(s.T(), service.AccountEgressStatusAcquired, primaryPool.Status)
}
