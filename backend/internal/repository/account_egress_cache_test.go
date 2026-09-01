package repository

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newAccountEgressCacheTest(t *testing.T) (*concurrencyCache, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	cache := NewConcurrencyCache(rdb, 15, 120).(*concurrencyCache)
	return cache, server
}

type accountEgressCommandHook struct {
	mu       sync.Mutex
	commands []string
}

func (h *accountEgressCommandHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *accountEgressCommandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.record(cmd)
		return next(ctx, cmd)
	}
}

func (h *accountEgressCommandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.record(cmd)
		}
		return next(ctx, cmds)
	}
}

func (h *accountEgressCommandHook) record(cmd redis.Cmder) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.commands = append(h.commands, cmd.Name())
}

func (h *accountEgressCommandHook) Commands() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.commands...)
}

func accountEgressTestConfig(accountID int64, limit, maxWaiting int, candidates ...service.AccountEgressCandidate) service.AccountEgressPoolConfig {
	return service.AccountEgressPoolConfig{
		AccountID:              accountID,
		Version:                1,
		PerIdentityConcurrency: limit,
		MaxWaiting:             maxWaiting,
		Candidates:             candidates,
	}
}

func accountEgressTestCandidate(position int, routeID int64, identityID string) service.AccountEgressCandidate {
	return service.AccountEgressCandidate{
		BindingID:  "route:" + string(rune('a'+position)),
		RouteID:    routeID,
		IdentityID: identityID,
		Position:   position,
		Primary:    position == 0,
		Healthy:    true,
	}
}

func syncAccountEgressTestConfig(t *testing.T, cache *concurrencyCache, config service.AccountEgressPoolConfig) {
	t.Helper()
	result, err := cache.SyncAccountEgressConfigs(context.Background(), []service.AccountEgressPoolConfig{config})
	require.NoError(t, err)
	require.Equal(t, service.AccountEgressConfigSyncOK, result[config.AccountID])
}

func acquireAccountEgressTest(
	t *testing.T,
	cache *concurrencyCache,
	config service.AccountEgressPoolConfig,
	leaseID string,
	required string,
	preferred string,
) service.AccountEgressAcquireResult {
	t.Helper()
	result, err := cache.AcquireAccountEgress(context.Background(), service.AccountEgressCacheAcquireRequest{
		AccountEgressAcquireRequest: service.AccountEgressAcquireRequest{
			Config:             config,
			LeaseID:            leaseID,
			RequiredBindingID:  required,
			PreferredBindingID: preferred,
		},
		LeaseTTL:  service.AccountEgressLeaseTTL,
		WaiterTTL: 2 * time.Minute,
	})
	require.NoError(t, err)
	return result
}

func releaseAccountEgressTest(t *testing.T, cache *concurrencyCache, config service.AccountEgressPoolConfig, result service.AccountEgressAcquireResult) {
	t.Helper()
	require.NoError(t, cache.ReleaseAccountEgressLease(context.Background(), service.AccountEgressLeaseRef{
		AccountID:     config.AccountID,
		ID:            result.LeaseID,
		BindingID:     result.BindingID,
		IdentityID:    result.IdentityID,
		ConfigVersion: result.ConfigVersion,
	}))
}

func TestAccountEgressKeysShareAccountHashTag(t *testing.T) {
	accountID := int64(42)
	tag := "{acct:42}"
	keys := []string{
		accountEgressConfigKey(accountID),
		accountEgressWaitersKey(accountID),
		accountEgressRRKey(accountID),
		accountEgressExclusiveKey(accountID),
		accountEgressModeKey(accountID),
		accountEgressTotalKey(accountID),
		accountEgressLegacyRegularKey(accountID),
		accountEgressLegacyLiveKey(accountID),
		accountEgressIdentityKey(accountID, "public-ip:192.0.2.10"),
		accountEgressLeaseKey(accountID, "request/with:unsafe-key-chars"),
	}
	for _, key := range keys {
		require.Contains(t, key, tag)
	}
	require.NotContains(t, keys[len(keys)-2], "192.0.2.10")
	require.NotContains(t, keys[len(keys)-1], "unsafe-key-chars")
}

func TestAccountEgressConfigVersionFence(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	config := accountEgressTestConfig(1001, 2, 0, accountEgressTestCandidate(0, 11, "ip:a"))
	config.Version = 2
	syncAccountEgressTestConfig(t, cache, config)

	stale := config
	stale.Version = 1
	result, err := cache.SyncAccountEgressConfigs(context.Background(), []service.AccountEgressPoolConfig{stale})
	require.NoError(t, err)
	require.Equal(t, service.AccountEgressConfigSyncStale, result[config.AccountID])

	conflict := config
	conflict.PerIdentityConcurrency = 3
	result, err = cache.SyncAccountEgressConfigs(context.Background(), []service.AccountEgressPoolConfig{conflict})
	require.NoError(t, err)
	require.Equal(t, service.AccountEgressConfigSyncConflict, result[config.AccountID])
}

func TestAccountEgressDistinctIdentitiesEachRespectLimit(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	config := accountEgressTestConfig(1002, 2, 0,
		accountEgressTestCandidate(0, 21, "ip:a"),
		accountEgressTestCandidate(1, 22, "ip:b"),
		accountEgressTestCandidate(2, 23, "ip:c"),
	)
	syncAccountEgressTestConfig(t, cache, config)

	counts := map[string]int{}
	for i := 0; i < 6; i++ {
		result := acquireAccountEgressTest(t, cache, config, "distinct-"+string(rune('a'+i)), "", "")
		require.Equal(t, service.AccountEgressStatusAcquired, result.Status)
		require.Equal(t, 6, result.EffectiveCapacity)
		counts[result.IdentityID]++
	}
	require.Equal(t, map[string]int{"ip:a": 2, "ip:b": 2, "ip:c": 2}, counts)
	require.Equal(t, service.AccountEgressStatusFull, acquireAccountEgressTest(t, cache, config, "distinct-overflow", "", "").Status)
}

func TestAccountEgressBindingsSharingIdentityShareCapacity(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	first := accountEgressTestCandidate(0, 31, "ip:shared")
	second := accountEgressTestCandidate(1, 32, "ip:shared")
	config := accountEgressTestConfig(1003, 2, 0, first, second)
	syncAccountEgressTestConfig(t, cache, config)

	require.Equal(t, service.AccountEgressStatusAcquired, acquireAccountEgressTest(t, cache, config, "shared-1", first.BindingID, "").Status)
	require.Equal(t, service.AccountEgressStatusAcquired, acquireAccountEgressTest(t, cache, config, "shared-2", second.BindingID, "").Status)
	require.Equal(t, service.AccountEgressStatusFull, acquireAccountEgressTest(t, cache, config, "shared-3", second.BindingID, "").Status)
	count, err := cache.rdb.ZCard(context.Background(), accountEgressIdentityKey(config.AccountID, "ip:shared")).Result()
	require.NoError(t, err)
	require.Equal(t, int64(2), count)
}

func TestAccountEgressRequiredNeverDriftsAndPreferredSpills(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	first := accountEgressTestCandidate(0, 41, "ip:a")
	second := accountEgressTestCandidate(1, 42, "ip:b")
	config := accountEgressTestConfig(1004, 1, 0, first, second)
	syncAccountEgressTestConfig(t, cache, config)

	firstResult := acquireAccountEgressTest(t, cache, config, "affinity-1", first.BindingID, "")
	require.Equal(t, first.BindingID, firstResult.BindingID)
	require.Equal(t, service.AccountEgressStatusFull, acquireAccountEgressTest(t, cache, config, "affinity-required", first.BindingID, "").Status)
	spill := acquireAccountEgressTest(t, cache, config, "affinity-preferred", "", first.BindingID)
	require.Equal(t, service.AccountEgressStatusAcquired, spill.Status)
	require.Equal(t, second.BindingID, spill.BindingID)
	releaseAccountEgressTest(t, cache, config, firstResult)
	releaseAccountEgressTest(t, cache, config, spill)

	unhealthy := config
	unhealthy.Version = 2
	unhealthy.Candidates[0].Healthy = false
	syncAccountEgressTestConfig(t, cache, unhealthy)
	require.Equal(t, service.AccountEgressStatusRequiredBindingUnavailable, acquireAccountEgressTest(t, cache, unhealthy, "affinity-unhealthy", first.BindingID, "").Status)
}

func TestAccountEgressUnhealthyIdentityDrainsWithoutBlockingHealthyCapacity(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	first := accountEgressTestCandidate(0, 45, "ip:a")
	second := accountEgressTestCandidate(1, 46, "ip:b")
	config := accountEgressTestConfig(1010, 1, 0, first, second)
	syncAccountEgressTestConfig(t, cache, config)

	draining := acquireAccountEgressTest(t, cache, config, "draining-owner", first.BindingID, "")
	require.Equal(t, service.AccountEgressStatusAcquired, draining.Status)

	updated := config
	updated.Version = 2
	updated.Candidates = append([]service.AccountEgressCandidate(nil), config.Candidates...)
	updated.Candidates[0].Healthy = false
	syncAccountEgressTestConfig(t, cache, updated)

	healthy := acquireAccountEgressTest(t, cache, updated, "healthy-owner", "", "")
	require.Equal(t, service.AccountEgressStatusAcquired, healthy.Status)
	require.Equal(t, second.BindingID, healthy.BindingID)
	require.Equal(t, 2, healthy.ActiveTotal)
	require.Equal(t, 1, healthy.EffectiveCapacity)
	require.Equal(t, service.AccountEgressStatusFull, acquireAccountEgressTest(t, cache, updated, "healthy-overflow", "", "").Status)

	loads, err := cache.GetAccountEgressLoadsBatch(context.Background(), []service.AccountEgressPoolConfig{updated}, service.AccountEgressLeaseTTL, 2*time.Minute)
	require.NoError(t, err)
	require.Equal(t, service.AccountEgressStatusAcquired, loads[updated.AccountID].Status)
	require.Equal(t, 2, loads[updated.AccountID].ActiveTotal)
	require.Equal(t, 1, loads[updated.AccountID].EffectiveCapacity)
}

func TestAccountEgressAllUnhealthyReturnsNoEligibleEgress(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	candidate := accountEgressTestCandidate(0, 47, "ip:a")
	candidate.Healthy = false
	config := accountEgressTestConfig(1011, 1, 0, candidate)
	syncAccountEgressTestConfig(t, cache, config)

	result := acquireAccountEgressTest(t, cache, config, "all-unhealthy", "", "")
	require.Equal(t, service.AccountEgressStatusNoEligibleEgress, result.Status)
	require.Equal(t, 0, result.EffectiveCapacity)
}

func TestAccountEgressFIFOAndIdempotentRetry(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	candidate := accountEgressTestCandidate(0, 51, "ip:a")
	config := accountEgressTestConfig(1005, 1, 3, candidate)
	syncAccountEgressTestConfig(t, cache, config)

	owner := acquireAccountEgressTest(t, cache, config, "fifo-owner", "", "")
	require.Equal(t, service.AccountEgressStatusAcquired, owner.Status)
	require.Equal(t, service.AccountEgressStatusFull, acquireAccountEgressTest(t, cache, config, "fifo-second", "", "").Status)
	require.Equal(t, service.AccountEgressStatusNotQueueHead, acquireAccountEgressTest(t, cache, config, "fifo-third", "", "").Status)
	releaseAccountEgressTest(t, cache, config, owner)

	require.Equal(t, service.AccountEgressStatusNotQueueHead, acquireAccountEgressTest(t, cache, config, "fifo-third", "", "").Status)
	second := acquireAccountEgressTest(t, cache, config, "fifo-second", "", "")
	require.Equal(t, service.AccountEgressStatusAcquired, second.Status)
	retry := acquireAccountEgressTest(t, cache, config, "fifo-second", "", "")
	require.Equal(t, service.AccountEgressStatusAcquired, retry.Status)
	require.Equal(t, second.BindingID, retry.BindingID)
	require.Equal(t, 1, retry.ActiveTotal)
	releaseAccountEgressTest(t, cache, config, second)
	require.Equal(t, service.AccountEgressStatusAcquired, acquireAccountEgressTest(t, cache, config, "fifo-third", "", "").Status)
}

func TestAccountEgressRefreshCannotRecreateMissingLeaseAndReleaseRestoresCapacity(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	candidate := accountEgressTestCandidate(0, 61, "ip:a")
	config := accountEgressTestConfig(1006, 1, 0, candidate)
	syncAccountEgressTestConfig(t, cache, config)

	result := acquireAccountEgressTest(t, cache, config, "refresh-1", "", "")
	ref := service.AccountEgressLeaseRef{AccountID: config.AccountID, ID: result.LeaseID, BindingID: result.BindingID, IdentityID: result.IdentityID, ConfigVersion: result.ConfigVersion}
	owned, err := cache.RefreshAccountEgressLeases(context.Background(), []service.AccountEgressLeaseRef{ref}, service.AccountEgressLeaseTTL)
	require.NoError(t, err)
	require.True(t, owned[ref.Key()])

	require.NoError(t, cache.rdb.ZRem(context.Background(), accountEgressIdentityKey(config.AccountID, result.IdentityID), accountEgressIDHash(result.LeaseID)).Err())
	owned, err = cache.RefreshAccountEgressLeases(context.Background(), []service.AccountEgressLeaseRef{ref}, service.AccountEgressLeaseTTL)
	require.NoError(t, err)
	require.False(t, owned[ref.Key()])
	require.NoError(t, cache.ReleaseAccountEgressLease(context.Background(), ref))
	require.Equal(t, service.AccountEgressStatusAcquired, acquireAccountEgressTest(t, cache, config, "refresh-2", "", "").Status)
}

func TestAccountEgressLegacyPoolGateDrainsBothDirections(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	config := accountEgressTestConfig(1007, 1, 0, accountEgressTestCandidate(0, 71, "ip:a"))
	syncAccountEgressTestConfig(t, cache, config)
	ctx := context.Background()

	legacyAcquired, err := cache.AcquireAccountSlot(ctx, config.AccountID, 1, "legacy-regular")
	require.NoError(t, err)
	require.True(t, legacyAcquired)
	require.Equal(t, int64(1), cache.rdb.ZCard(ctx, accountEgressLegacyRegularKey(config.AccountID)).Val())
	require.Equal(t, service.AccountEgressStatusLegacyDraining, acquireAccountEgressTest(t, cache, config, "pool-blocked", "", "").Status)
	require.NoError(t, cache.ReleaseAccountSlot(ctx, config.AccountID, "legacy-regular"))
	require.Equal(t, int64(0), cache.rdb.ZCard(ctx, accountEgressLegacyRegularKey(config.AccountID)).Val())

	pool := acquireAccountEgressTest(t, cache, config, "pool-owner", "", "")
	require.Equal(t, service.AccountEgressStatusAcquired, pool.Status)
	legacyAcquired, err = cache.AcquireAccountSlot(ctx, config.AccountID, 1, "legacy-blocked")
	require.NoError(t, err)
	require.False(t, legacyAcquired)
	liveAcquired, err := cache.AcquireLiveLease(ctx, config.AccountID, 1, 9001, 1, 9002, "legacy-live-blocked", false)
	require.NoError(t, err)
	require.False(t, liveAcquired)
	releaseAccountEgressTest(t, cache, config, pool)

	liveAcquired, err = cache.AcquireLiveLease(ctx, config.AccountID, 1, 9001, 1, 9002, "legacy-live", false)
	require.NoError(t, err)
	require.True(t, liveAcquired)
	require.Equal(t, int64(1), cache.rdb.ZCard(ctx, accountEgressLegacyLiveKey(config.AccountID)).Val())
	require.Equal(t, service.AccountEgressStatusLegacyDraining, acquireAccountEgressTest(t, cache, config, "pool-live-blocked", "", "").Status)
}

func TestAccountEgressWarmupGateCoversPoolLeasesAndWaiters(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	candidate := accountEgressTestCandidate(0, 81, "ip:a")
	config := accountEgressTestConfig(1008, 1, 2, candidate)
	syncAccountEgressTestConfig(t, cache, config)
	ctx := context.Background()

	owner := acquireAccountEgressTest(t, cache, config, "warmup-owner", "", "")
	warmup, err := cache.AcquireAccountExclusive(ctx, config.AccountID, "warmup-token", time.Minute)
	require.NoError(t, err)
	require.False(t, warmup)
	require.Equal(t, service.AccountEgressStatusFull, acquireAccountEgressTest(t, cache, config, "warmup-waiter", "", "").Status)
	releaseAccountEgressTest(t, cache, config, owner)
	warmup, err = cache.AcquireAccountExclusive(ctx, config.AccountID, "warmup-token", time.Minute)
	require.NoError(t, err)
	require.False(t, warmup, "queued pool demand must prevent maintenance from taking the account")
	require.NoError(t, cache.RemoveAccountEgressWaiter(ctx, config.AccountID, "warmup-waiter"))
	warmup, err = cache.AcquireAccountExclusive(ctx, config.AccountID, "warmup-token", time.Minute)
	require.NoError(t, err)
	require.True(t, warmup)
	require.Equal(t, "warmup-token", cache.rdb.Get(ctx, accountEgressExclusiveKey(config.AccountID)).Val())

	blockedCtx, cancel := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancel()
	allocator := service.NewAccountEgressAllocator(cache)
	defer allocator.Close()
	_, err = allocator.Acquire(blockedCtx, service.AccountEgressAcquireRequest{Config: config, LeaseID: "warmup-blocked"})
	require.ErrorIs(t, err, service.ErrAccountEgressCapacityFull)
	released, err := cache.ReleaseAccountExclusive(ctx, config.AccountID, "warmup-token")
	require.NoError(t, err)
	require.True(t, released)
}

func TestAccountEgressBatchLoadAndConcurrentHardCap(t *testing.T) {
	cache, server := newAccountEgressCacheTest(t)
	candidate := accountEgressTestCandidate(0, 91, "ip:a")
	config := accountEgressTestConfig(1009, 3, 10, candidate)
	syncAccountEgressTestConfig(t, cache, config)

	otherClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = otherClient.Close() })
	otherCache := NewConcurrencyCache(otherClient, 15, 120).(*concurrencyCache)
	type outcome struct {
		result service.AccountEgressAcquireResult
		err    error
	}
	outcomes := make(chan outcome, 30)
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			selectedCache := cache
			if index%2 == 1 {
				selectedCache = otherCache
			}
			result, err := selectedCache.AcquireAccountEgress(context.Background(), service.AccountEgressCacheAcquireRequest{
				AccountEgressAcquireRequest: service.AccountEgressAcquireRequest{Config: config, LeaseID: "concurrent-" + string(rune('a'+index))},
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
		require.NoError(t, outcome.err)
		if outcome.result.Status == service.AccountEgressStatusAcquired {
			acquired++
		}
	}
	require.Equal(t, 3, acquired)
	count, err := cache.rdb.ZCard(context.Background(), accountEgressIdentityKey(config.AccountID, candidate.IdentityID)).Result()
	require.NoError(t, err)
	require.Equal(t, int64(3), count)

	loads, err := cache.GetAccountEgressLoadsBatch(context.Background(), []service.AccountEgressPoolConfig{config}, service.AccountEgressLeaseTTL, 2*time.Minute)
	require.NoError(t, err)
	load := loads[config.AccountID]
	require.Equal(t, 3, load.ActiveTotal)
	require.Equal(t, map[string]int{candidate.IdentityID: 3}, load.IdentityLoads)
	require.Equal(t, 3, load.EffectiveCapacity)
	require.Equal(t, 10, load.WaitingCount)
	require.GreaterOrEqual(t, load.LoadRate, 100)
}

func TestAccountEgressBatchLoadUsesOneAtomicSnapshotPerAccount(t *testing.T) {
	cache, server := newAccountEgressCacheTest(t)
	now := time.Unix(1_700_000_000, 0)
	server.SetTime(now)
	first := accountEgressTestConfig(1013, 3, 10,
		accountEgressTestCandidate(0, 111, "ip:a"),
		accountEgressTestCandidate(1, 112, "ip:b"),
	)
	second := accountEgressTestConfig(1014, 3, 10, accountEgressTestCandidate(0, 113, "ip:c"))
	syncAccountEgressTestConfig(t, cache, first)
	syncAccountEgressTestConfig(t, cache, second)

	leaseTTL := service.AccountEgressLeaseTTL
	waiterTTL := 2 * time.Minute
	freshLeaseScore := float64(now.UnixMilli())
	staleLeaseScore := float64(now.UnixMilli() - leaseTTL.Milliseconds() - 1)
	freshWaiterScore := float64(now.UnixMicro())
	staleWaiterScore := float64(now.UnixMicro() - waiterTTL.Microseconds() - 1)
	for _, key := range []string{
		accountEgressIdentityKey(first.AccountID, "ip:a"),
		accountEgressTotalKey(first.AccountID),
	} {
		require.NoError(t, cache.rdb.ZAdd(context.Background(), key,
			redis.Z{Score: freshLeaseScore, Member: "fresh"},
			redis.Z{Score: staleLeaseScore, Member: "stale"},
		).Err())
	}
	require.NoError(t, cache.rdb.ZAdd(context.Background(), accountEgressWaitersKey(first.AccountID),
		redis.Z{Score: freshWaiterScore, Member: "fresh"},
		redis.Z{Score: staleWaiterScore, Member: "stale"},
	).Err())

	hook := &accountEgressCommandHook{}
	cache.rdb.AddHook(hook)
	loads, err := cache.GetAccountEgressLoadsBatch(context.Background(), []service.AccountEgressPoolConfig{first, second}, leaseTTL, waiterTTL)
	require.NoError(t, err)
	require.Equal(t, []string{"eval", "eval"}, hook.Commands(), "each account must be read by one Lua snapshot without separate TIME or ZCARD commands")
	require.Equal(t, 1, loads[first.AccountID].ActiveTotal)
	require.Equal(t, 1, loads[first.AccountID].WaitingCount)
	require.Equal(t, map[string]int{"ip:a": 1, "ip:b": 0}, loads[first.AccountID].IdentityLoads)
	require.Equal(t, map[string]int{"ip:c": 0}, loads[second.AccountID].IdentityLoads)
	require.Equal(t, int64(1), cache.rdb.ZCard(context.Background(), accountEgressIdentityKey(first.AccountID, "ip:a")).Val())
	require.Equal(t, int64(1), cache.rdb.ZCard(context.Background(), accountEgressTotalKey(first.AccountID)).Val())
	require.Equal(t, int64(1), cache.rdb.ZCard(context.Background(), accountEgressWaitersKey(first.AccountID)).Val())
}

func TestAccountEgressThreeIdentitiesProvideNineIndependentSlots(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	first := accountEgressTestCandidate(0, 101, "ip:sys1")
	second := accountEgressTestCandidate(1, 102, "ip:rn67")
	third := accountEgressTestCandidate(2, 103, "ip:rn104")
	config := accountEgressTestConfig(1012, 3, 0, first, second, third)
	syncAccountEgressTestConfig(t, cache, config)

	for index := 0; index < 9; index++ {
		result := acquireAccountEgressTest(t, cache, config, fmt.Sprintf("nine-slots-%d", index), "", "")
		require.Equal(t, service.AccountEgressStatusAcquired, result.Status)
	}
	require.Equal(t, service.AccountEgressStatusFull, acquireAccountEgressTest(t, cache, config, "tenth-slot", "", "").Status)

	loads, err := cache.GetAccountEgressLoadsBatch(context.Background(), []service.AccountEgressPoolConfig{config}, service.AccountEgressLeaseTTL, 2*time.Minute)
	require.NoError(t, err)
	load := loads[config.AccountID]
	require.Equal(t, 9, load.ActiveTotal)
	require.Equal(t, 9, load.EffectiveCapacity)
	require.Equal(t, map[string]int{
		first.IdentityID:  3,
		second.IdentityID: 3,
		third.IdentityID:  3,
	}, load.IdentityLoads)
}
