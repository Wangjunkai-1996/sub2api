package repository

import (
	"context"
	"fmt"
	"math"
	"strconv"
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
		AuthorityRevision:      1,
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

// syncAccountEgressConfigAtKey models an older binary that still writes the
// pre-v2 config hash. It intentionally uses the production script so this
// regression test covers the same equal-version/different-digest race as a
// mixed blue-green deployment.
func syncAccountEgressConfigAtKey(t *testing.T, cache *concurrencyCache, key string, config service.AccountEgressPoolConfig) {
	t.Helper()
	digest, err := config.Digest()
	require.NoError(t, err)
	candidates := config.SortedCandidates()
	args := make([]any, 0, 6+len(candidates)*2)
	args = append(args, config.Version, config.AuthorityRevision, digest, config.PerIdentityConcurrency, config.MaxWaiting, len(candidates))
	for _, candidate := range candidates {
		args = append(args, accountEgressIDHash(candidate.BindingID), accountEgressBindingMapping(candidate))
	}
	raw, err := accountEgressSyncConfigScript.Run(context.Background(), cache.rdb, []string{key}, args...).Result()
	require.NoError(t, err)
	require.Equal(t, "OK", fmt.Sprint(raw))
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
		AccountID:         config.AccountID,
		ID:                result.LeaseID,
		BindingID:         result.BindingID,
		RouteID:           result.RouteID,
		IdentityID:        result.IdentityID,
		ConfigVersion:     result.ConfigVersion,
		AuthorityRevision: result.AuthorityRevision,
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

func TestAccountEgressConfigNamespaceIsAdditiveForMixedVersions(t *testing.T) {
	accountID := int64(43)
	legacyKey := accountEgressBaseKey(accountID) + ":config"
	currentKey := accountEgressConfigKey(accountID)

	require.NotEqual(t, legacyKey, currentKey)
	require.Equal(t, accountEgressBaseKey(accountID)+":config:v2", currentKey)
	require.Contains(t, legacyKey, "{acct:43}")
	require.Contains(t, currentKey, "{acct:43}")
}

func TestAccountEgressConfigNamespaceBlocksLegacyHealthOverwrite(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	current := accountEgressTestConfig(1000, 1, 0, accountEgressTestCandidate(0, 10, "ip:a"))
	syncAccountEgressTestConfig(t, cache, current)

	legacy := current
	legacy.Candidates = append([]service.AccountEgressCandidate(nil), current.Candidates...)
	legacy.Candidates[0].Healthy = false
	syncAccountEgressConfigAtKey(t, cache, accountEgressBaseKey(current.AccountID)+":config", legacy)

	digest, err := current.Digest()
	require.NoError(t, err)
	require.Equal(t, digest, cache.rdb.HGet(context.Background(), accountEgressConfigKey(current.AccountID), "digest").Val())
	require.Equal(t,
		accountEgressBindingMapping(legacy.Candidates[0]),
		cache.rdb.HGet(context.Background(), accountEgressBaseKey(current.AccountID)+":config", "binding:"+accountEgressIDHash(legacy.Candidates[0].BindingID)).Val(),
	)

	result := acquireAccountEgressTest(t, cache, current, "mixed-version", "", "")
	require.Equal(t, service.AccountEgressStatusAcquired, result.Status)
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

func TestAccountEgressAuthorityRevisionAdvancesWhenRuntimeVersionSaturates(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	config := accountEgressTestConfig(1031, 1, 0, accountEgressTestCandidate(0, 91, "ip:a"))
	config.Version = math.MaxInt64
	config.AuthorityRevision = 11
	syncAccountEgressTestConfig(t, cache, config)

	advanced := config
	advanced.AuthorityRevision = 12
	result, err := cache.SyncAccountEgressConfigs(context.Background(), []service.AccountEgressPoolConfig{advanced})
	require.NoError(t, err)
	require.Equal(t, service.AccountEgressConfigSyncOK, result[config.AccountID])
	require.Equal(t, "12", cache.rdb.HGet(context.Background(), accountEgressConfigKey(config.AccountID), "authority_revision").Val())

	acquired := acquireAccountEgressTest(t, cache, advanced, "saturated-version", "", "")
	require.Equal(t, service.AccountEgressStatusAcquired, acquired.Status)
	require.Equal(t, int64(12), acquired.AuthorityRevision)

	result, err = cache.SyncAccountEgressConfigs(context.Background(), []service.AccountEgressPoolConfig{config})
	require.NoError(t, err)
	require.Equal(t, service.AccountEgressConfigSyncStale, result[config.AccountID])
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
	ref := service.AccountEgressLeaseRef{AccountID: config.AccountID, ID: result.LeaseID, BindingID: result.BindingID, RouteID: result.RouteID, IdentityID: result.IdentityID, ConfigVersion: result.ConfigVersion, AuthorityRevision: result.AuthorityRevision}
	statuses, err := cache.RefreshAccountEgressLeases(context.Background(), []service.AccountEgressLeaseRef{ref}, service.AccountEgressLeaseTTL)
	require.NoError(t, err)
	require.Equal(t, service.AccountEgressLeaseRefreshActive, statuses[ref.Key()])

	require.NoError(t, cache.rdb.ZRem(context.Background(), accountEgressIdentityKey(config.AccountID, result.IdentityID), accountEgressIDHash(result.LeaseID)).Err())
	statuses, err = cache.RefreshAccountEgressLeases(context.Background(), []service.AccountEgressLeaseRef{ref}, service.AccountEgressLeaseTTL)
	require.NoError(t, err)
	require.Equal(t, service.AccountEgressLeaseRefreshLost, statuses[ref.Key()])
	require.NoError(t, cache.ReleaseAccountEgressLease(context.Background(), ref))
	require.Equal(t, service.AccountEgressStatusAcquired, acquireAccountEgressTest(t, cache, config, "refresh-2", "", "").Status)
}

func TestAccountEgressReleaseDoesNotDeleteReplacementLease(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(service.AccountEgressLeaseRef) (string, string, service.AccountEgressLeaseRef)
	}{
		{
			name: "binding",
			mutate: func(ref service.AccountEgressLeaseRef) (string, string, service.AccountEgressLeaseRef) {
				ref.BindingID = "replacement-binding"
				return "binding_hash", accountEgressIDHash(ref.BindingID), ref
			},
		},
		{
			name: "route",
			mutate: func(ref service.AccountEgressLeaseRef) (string, string, service.AccountEgressLeaseRef) {
				ref.RouteID++
				return "route_id", strconv.FormatInt(ref.RouteID, 10), ref
			},
		},
		{
			name: "version",
			mutate: func(ref service.AccountEgressLeaseRef) (string, string, service.AccountEgressLeaseRef) {
				ref.ConfigVersion++
				return "version", strconv.FormatInt(ref.ConfigVersion, 10), ref
			},
		},
		{
			name: "authority revision",
			mutate: func(ref service.AccountEgressLeaseRef) (string, string, service.AccountEgressLeaseRef) {
				ref.AuthorityRevision++
				return "authority_revision", strconv.FormatInt(ref.AuthorityRevision, 10), ref
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, _ := newAccountEgressCacheTest(t)
			config := accountEgressTestConfig(1032, 1, 0, accountEgressTestCandidate(0, 63, "ip:a"))
			syncAccountEgressTestConfig(t, cache, config)
			result := acquireAccountEgressTest(t, cache, config, "reused-lease", "", "")
			oldRef := service.AccountEgressLeaseRef{
				AccountID: config.AccountID, ID: result.LeaseID, BindingID: result.BindingID,
				RouteID: result.RouteID, IdentityID: result.IdentityID, ConfigVersion: result.ConfigVersion,
				AuthorityRevision: result.AuthorityRevision,
			}
			field, value, replacementRef := tt.mutate(oldRef)
			ctx := context.Background()
			require.NoError(t, cache.rdb.HSet(ctx, accountEgressLeaseKey(config.AccountID, result.LeaseID), field, value).Err())

			require.NoError(t, cache.ReleaseAccountEgressLease(ctx, oldRef))
			require.Equal(t, int64(1), cache.rdb.Exists(ctx, accountEgressLeaseKey(config.AccountID, result.LeaseID)).Val())
			require.Equal(t, int64(1), cache.rdb.ZCard(ctx, accountEgressIdentityKey(config.AccountID, result.IdentityID)).Val())
			require.Equal(t, int64(1), cache.rdb.ZCard(ctx, accountEgressTotalKey(config.AccountID)).Val())

			require.NoError(t, cache.ReleaseAccountEgressLease(ctx, replacementRef))
			require.Zero(t, cache.rdb.Exists(ctx, accountEgressLeaseKey(config.AccountID, result.LeaseID)).Val())
			require.Zero(t, cache.rdb.ZCard(ctx, accountEgressIdentityKey(config.AccountID, result.IdentityID)).Val())
			require.Zero(t, cache.rdb.ZCard(ctx, accountEgressTotalKey(config.AccountID)).Val())
		})
	}

	t.Run("missing metadata cleans orphaned members", func(t *testing.T) {
		cache, _ := newAccountEgressCacheTest(t)
		config := accountEgressTestConfig(1033, 1, 0, accountEgressTestCandidate(0, 64, "ip:a"))
		syncAccountEgressTestConfig(t, cache, config)
		result := acquireAccountEgressTest(t, cache, config, "orphaned-lease", "", "")
		ref := service.AccountEgressLeaseRef{
			AccountID: config.AccountID, ID: result.LeaseID, BindingID: result.BindingID,
			RouteID: result.RouteID, IdentityID: result.IdentityID, ConfigVersion: result.ConfigVersion,
			AuthorityRevision: result.AuthorityRevision,
		}
		ctx := context.Background()
		require.NoError(t, cache.rdb.Del(ctx, accountEgressLeaseKey(config.AccountID, result.LeaseID)).Err())

		require.NoError(t, cache.ReleaseAccountEgressLease(ctx, ref))
		require.Zero(t, cache.rdb.ZCard(ctx, accountEgressIdentityKey(config.AccountID, result.IdentityID)).Val())
		require.Zero(t, cache.rdb.ZCard(ctx, accountEgressTotalKey(config.AccountID)).Val())
	})
}

func TestAccountEgressRefreshFencesChangedAuthorityAndKeepsReservationUntilRelease(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(context.Context, *concurrencyCache, service.AccountEgressPoolConfig, service.AccountEgressAcquireResult)
	}{
		{
			name: "config version",
			mutate: func(ctx context.Context, cache *concurrencyCache, config service.AccountEgressPoolConfig, _ service.AccountEgressAcquireResult) {
				require.NoError(t, cache.rdb.HSet(ctx, accountEgressConfigKey(config.AccountID), "version", config.Version+1).Err())
			},
		},
		{
			name: "binding mapping",
			mutate: func(ctx context.Context, cache *concurrencyCache, config service.AccountEgressPoolConfig, result service.AccountEgressAcquireResult) {
				require.NoError(t, cache.rdb.HSet(
					ctx,
					accountEgressConfigKey(config.AccountID),
					"binding:"+accountEgressIDHash(result.BindingID),
					accountEgressIDHash("different-identity")+":1:"+strconv.FormatInt(result.RouteID, 10),
				).Err())
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache, _ := newAccountEgressCacheTest(t)
			candidate := accountEgressTestCandidate(0, 62, "ip:a")
			config := accountEgressTestConfig(1026, 1, 0, candidate)
			syncAccountEgressTestConfig(t, cache, config)
			result := acquireAccountEgressTest(t, cache, config, "fence-1", "", "")
			ref := service.AccountEgressLeaseRef{
				AccountID: config.AccountID, ID: result.LeaseID, BindingID: result.BindingID,
				RouteID: result.RouteID, IdentityID: result.IdentityID, ConfigVersion: result.ConfigVersion,
				AuthorityRevision: result.AuthorityRevision,
			}
			ctx := context.Background()
			test.mutate(ctx, cache, config, result)

			statuses, err := cache.RefreshAccountEgressLeases(ctx, []service.AccountEgressLeaseRef{ref}, service.AccountEgressLeaseTTL)
			require.NoError(t, err)
			require.Equal(t, service.AccountEgressLeaseRefreshFenced, statuses[ref.Key()])
			require.Equal(t, int64(1), cache.rdb.ZCard(ctx, accountEgressIdentityKey(config.AccountID, result.IdentityID)).Val())
			require.Equal(t, int64(1), cache.rdb.ZCard(ctx, accountEgressTotalKey(config.AccountID)).Val())

			statuses, err = cache.KeepaliveFencedAccountEgressLeases(ctx, []service.AccountEgressLeaseRef{ref}, service.AccountEgressLeaseTTL)
			require.NoError(t, err)
			require.Equal(t, service.AccountEgressLeaseRefreshFenced, statuses[ref.Key()])
			require.Equal(t, "fenced", cache.rdb.HGet(ctx, accountEgressLeaseKey(config.AccountID, result.LeaseID), "state").Val())

			require.NoError(t, cache.ReleaseAccountEgressLease(ctx, ref))
			require.Zero(t, cache.rdb.ZCard(ctx, accountEgressIdentityKey(config.AccountID, result.IdentityID)).Val())
			require.Zero(t, cache.rdb.ZCard(ctx, accountEgressTotalKey(config.AccountID)).Val())
		})
	}
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
	require.Equal(t, "pool", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())
	liveAcquired, err := cache.AcquireLiveLease(ctx, config.AccountID, 1, 9001, 1, 9002, "legacy-live-blocked", false)
	require.NoError(t, err)
	require.False(t, liveAcquired)
	require.Equal(t, "pool", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())
	releaseAccountEgressTest(t, cache, config, pool)
	require.Equal(t, "pool", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())

	liveAcquired, err = cache.AcquireLiveLease(ctx, config.AccountID, 1, 9001, 1, 9002, "legacy-live", false)
	require.NoError(t, err)
	require.True(t, liveAcquired)
	require.Equal(t, int64(1), cache.rdb.ZCard(ctx, accountEgressLegacyLiveKey(config.AccountID)).Val())
	require.Equal(t, service.AccountEgressStatusLegacyDraining, acquireAccountEgressTest(t, cache, config, "pool-live-blocked", "", "").Status)
}

func TestAccountEgressTransitionBridgesLegacyByAdmissionIdentity(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	primary := accountEgressTestCandidate(0, 72, "ip:primary")
	second := accountEgressTestCandidate(1, 73, "ip:second")
	third := accountEgressTestCandidate(2, 74, "ip:third")
	config := accountEgressTestConfig(1015, 3, 0, primary, second, third)
	syncAccountEgressTestConfig(t, cache, config)
	ctx := context.Background()

	for index := 0; index < 3; index++ {
		acquired, err := cache.AcquireAccountSlotForEgress(ctx, config.AccountID, 3, fmt.Sprintf("transition-legacy-%d", index), primary.IdentityID)
		require.NoError(t, err)
		require.True(t, acquired)
	}
	loads, err := cache.GetAccountEgressLoadsBatch(ctx, []service.AccountEgressPoolConfig{config}, service.AccountEgressLeaseTTL, 2*time.Minute)
	require.NoError(t, err)
	require.Equal(t, service.AccountEgressStatusAcquired, loads[config.AccountID].Status)
	require.Equal(t, 3, loads[config.AccountID].ActiveTotal)
	require.Equal(t, 33, loads[config.AccountID].LoadRate)
	require.Equal(t, 3, loads[config.AccountID].IdentityLoads[primary.IdentityID])

	poolLeases := make([]service.AccountEgressAcquireResult, 0, 7)
	for index := 0; index < 6; index++ {
		result := acquireAccountEgressTest(t, cache, config, fmt.Sprintf("transition-pool-%d", index), "", "")
		require.Equal(t, service.AccountEgressStatusAcquired, result.Status)
		poolLeases = append(poolLeases, result)
	}
	require.Equal(t, "transition", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())
	firstPoolRef := service.AccountEgressLeaseRef{
		AccountID: config.AccountID, ID: poolLeases[0].LeaseID, BindingID: poolLeases[0].BindingID,
		RouteID: poolLeases[0].RouteID, IdentityID: poolLeases[0].IdentityID, ConfigVersion: poolLeases[0].ConfigVersion,
		AuthorityRevision: poolLeases[0].AuthorityRevision,
	}
	statuses, err := cache.RefreshAccountEgressLeases(ctx, []service.AccountEgressLeaseRef{firstPoolRef}, service.AccountEgressLeaseTTL)
	require.NoError(t, err)
	require.Equal(t, service.AccountEgressLeaseRefreshActive, statuses[firstPoolRef.Key()])
	require.Equal(t, service.AccountEgressStatusFull, acquireAccountEgressTest(t, cache, config, "transition-tenth", "", "").Status)

	legacyBlocked, err := cache.AcquireAccountSlot(ctx, config.AccountID, 3, "transition-new-legacy-blocked")
	require.NoError(t, err)
	require.False(t, legacyBlocked)

	loads, err = cache.GetAccountEgressLoadsBatch(ctx, []service.AccountEgressPoolConfig{config}, service.AccountEgressLeaseTTL, 2*time.Minute)
	require.NoError(t, err)
	load := loads[config.AccountID]
	require.Equal(t, service.AccountEgressStatusAcquired, load.Status)
	require.Equal(t, 9, load.ActiveTotal)
	require.Equal(t, map[string]int{
		primary.IdentityID: 3,
		second.IdentityID:  3,
		third.IdentityID:   3,
	}, load.IdentityLoads)

	require.NoError(t, cache.ReleaseAccountSlotForEgress(ctx, config.AccountID, "transition-legacy-0", primary.IdentityID))
	primaryPool := acquireAccountEgressTest(t, cache, config, "transition-primary-pool", primary.BindingID, "")
	require.Equal(t, service.AccountEgressStatusAcquired, primaryPool.Status)
	poolLeases = append(poolLeases, primaryPool)

	loads, err = cache.GetAccountEgressLoadsBatch(ctx, []service.AccountEgressPoolConfig{config}, service.AccountEgressLeaseTTL, 2*time.Minute)
	require.NoError(t, err)
	require.Equal(t, 9, loads[config.AccountID].ActiveTotal)
	require.Equal(t, 3, loads[config.AccountID].IdentityLoads[primary.IdentityID])

	for index := 1; index < 3; index++ {
		require.NoError(t, cache.ReleaseAccountSlotForEgress(ctx, config.AccountID, fmt.Sprintf("transition-legacy-%d", index), primary.IdentityID))
	}
	loads, err = cache.GetAccountEgressLoadsBatch(ctx, []service.AccountEgressPoolConfig{config}, service.AccountEgressLeaseTTL, 2*time.Minute)
	require.NoError(t, err)
	require.Equal(t, "pool", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())
	require.Equal(t, 7, loads[config.AccountID].ActiveTotal)

	for _, lease := range poolLeases {
		releaseAccountEgressTest(t, cache, config, lease)
	}
}

func TestAccountEgressLegacyLoadKeepsAdmissionIdentityWhenPrimaryChanges(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	first := accountEgressTestCandidate(0, 75, "ip:first")
	second := accountEgressTestCandidate(1, 76, "ip:second")
	config := accountEgressTestConfig(1016, 1, 0, first, second)
	syncAccountEgressTestConfig(t, cache, config)
	ctx := context.Background()

	acquired, err := cache.AcquireAccountSlotForEgress(ctx, config.AccountID, 1, "legacy-on-first", first.IdentityID)
	require.NoError(t, err)
	require.True(t, acquired)

	updated := config
	updated.Version = 2
	updated.Candidates = append([]service.AccountEgressCandidate(nil), config.Candidates...)
	updated.Candidates[0].Primary = false
	updated.Candidates[1].Primary = true
	syncAccountEgressTestConfig(t, cache, updated)

	loads, err := cache.GetAccountEgressLoadsBatch(ctx, []service.AccountEgressPoolConfig{updated}, service.AccountEgressLeaseTTL, 2*time.Minute)
	require.NoError(t, err)
	require.Equal(t, map[string]int{first.IdentityID: 1, second.IdentityID: 0}, loads[config.AccountID].IdentityLoads)

	secondLease := acquireAccountEgressTest(t, cache, updated, "pool-on-second", second.BindingID, "")
	require.Equal(t, service.AccountEgressStatusAcquired, secondLease.Status)
	require.Equal(t, second.IdentityID, secondLease.IdentityID)
	require.Equal(t, service.AccountEgressStatusFull, acquireAccountEgressTest(t, cache, updated, "pool-over-first", first.BindingID, "").Status)

	releaseAccountEgressTest(t, cache, updated, secondLease)
	require.NoError(t, cache.ReleaseAccountSlotForEgress(ctx, config.AccountID, "legacy-on-first", first.IdentityID))
}

func TestLegacyAdmissionDoesNotInitiatePoolToLegacyTransition(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	candidate := accountEgressTestCandidate(0, 77, "ip:only")
	config := accountEgressTestConfig(1017, 1, 0, candidate)
	syncAccountEgressTestConfig(t, cache, config)
	ctx := context.Background()

	pool := acquireAccountEgressTest(t, cache, config, "pool-owner", "", "")
	require.Equal(t, service.AccountEgressStatusAcquired, pool.Status)
	legacy, err := cache.AcquireAccountSlotForEgress(ctx, config.AccountID, 1, "legacy-blocked", candidate.IdentityID)
	require.NoError(t, err)
	require.False(t, legacy)
	require.Equal(t, "pool", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())
	require.Equal(t, service.AccountEgressStatusFull, acquireAccountEgressTest(t, cache, config, "pool-still-open", "", "").Status)

	mode, err := cache.BeginAccountEgressLegacyTransition(ctx, config.AccountID)
	require.NoError(t, err)
	require.Equal(t, "to_legacy", mode)
	require.Equal(t, service.AccountEgressStatusLegacyDraining, acquireAccountEgressTest(t, cache, config, "pool-blocked", "", "").Status)
	releaseAccountEgressTest(t, cache, config, pool)
	require.Equal(t, "legacy", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())
	legacy, err = cache.AcquireAccountSlotForEgress(ctx, config.AccountID, 1, "legacy-allowed", candidate.IdentityID)
	require.NoError(t, err)
	require.True(t, legacy)
}

func TestForcePoolRepairsStaleToLegacyMarkerWithoutLegacyLease(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	first := accountEgressTestCandidate(0, 78, "ip:first")
	second := accountEgressTestCandidate(1, 79, "ip:second")
	config := accountEgressTestConfig(1018, 1, 0, first, second)
	syncAccountEgressTestConfig(t, cache, config)
	ctx := context.Background()

	owner := acquireAccountEgressTest(t, cache, config, "force-pool-owner", "", "")
	require.Equal(t, service.AccountEgressStatusAcquired, owner.Status)
	mode, err := cache.BeginAccountEgressLegacyTransition(ctx, config.AccountID)
	require.NoError(t, err)
	require.Equal(t, "to_legacy", mode)

	// No legacy mirror lease exists: this is the stale marker produced by the
	// old admission script when it merely observed an occupied pool.
	result, err := cache.AcquireAccountEgress(ctx, service.AccountEgressCacheAcquireRequest{
		AccountEgressAcquireRequest: service.AccountEgressAcquireRequest{
			Config:    config,
			LeaseID:   "force-pool-repair",
			ForcePool: true,
		},
		LeaseTTL:  service.AccountEgressLeaseTTL,
		WaiterTTL: 2 * time.Minute,
	})
	require.NoError(t, err)
	require.Equal(t, service.AccountEgressStatusAcquired, result.Status)
	require.Equal(t, second.BindingID, result.BindingID)
	require.Equal(t, "pool", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())

	releaseAccountEgressTest(t, cache, config, result)
	releaseAccountEgressTest(t, cache, config, owner)
}

func TestForcePoolDoesNotBypassActiveLegacyLease(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	candidate := accountEgressTestCandidate(0, 80, "ip:legacy")
	config := accountEgressTestConfig(1019, 1, 0, candidate)
	syncAccountEgressTestConfig(t, cache, config)
	ctx := context.Background()

	// Model a genuine transition fence with matching primary and mirror state.
	// The force flag is intentionally unable to erase this state.
	require.NoError(t, cache.rdb.Set(ctx, accountEgressModeKey(config.AccountID), "to_legacy", 0).Err())
	now := float64(time.Now().Unix())
	require.NoError(t, cache.rdb.ZAdd(ctx, accountSlotKey(config.AccountID), redis.Z{
		Score: now, Member: "legacy-active",
	}).Err())
	require.NoError(t, cache.rdb.ZAdd(ctx, accountEgressLegacyRegularKey(config.AccountID), redis.Z{
		Score: now, Member: "legacy-active",
	}).Err())
	result, err := cache.AcquireAccountEgress(ctx, service.AccountEgressCacheAcquireRequest{
		AccountEgressAcquireRequest: service.AccountEgressAcquireRequest{
			Config:    config,
			LeaseID:   "force-pool-blocked",
			ForcePool: true,
		},
		LeaseTTL:  service.AccountEgressLeaseTTL,
		WaiterTTL: 2 * time.Minute,
	})
	require.NoError(t, err)
	require.Equal(t, service.AccountEgressStatusLegacyDraining, result.Status)
	require.Equal(t, "to_legacy", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())
}

func TestForcePoolFailsClosedWhenLegacyPrimaryAndMirrorDiverge(t *testing.T) {
	tests := []struct {
		name        string
		primaryKey  func(int64) string
		mirrorKey   func(int64) string
		seedPrimary bool
	}{
		{name: "regular primary without mirror", primaryKey: accountSlotKey, mirrorKey: accountEgressLegacyRegularKey, seedPrimary: true},
		{name: "live primary without mirror", primaryKey: liveAccountSlotKey, mirrorKey: accountEgressLegacyLiveKey, seedPrimary: true},
		{name: "regular mirror without primary", primaryKey: accountSlotKey, mirrorKey: accountEgressLegacyRegularKey},
		{name: "live mirror without primary", primaryKey: liveAccountSlotKey, mirrorKey: accountEgressLegacyLiveKey},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cache, _ := newAccountEgressCacheTest(t)
			candidate := accountEgressTestCandidate(0, 81, "ip:legacy")
			config := accountEgressTestConfig(1020, 1, 0, candidate)
			syncAccountEgressTestConfig(t, cache, config)
			ctx := context.Background()

			require.NoError(t, cache.rdb.Set(ctx, accountEgressModeKey(config.AccountID), "to_legacy", 0).Err())
			key := test.mirrorKey(config.AccountID)
			if test.seedPrimary {
				key = test.primaryKey(config.AccountID)
			}
			require.NoError(t, cache.rdb.ZAdd(ctx, key, redis.Z{
				Score: float64(time.Now().Unix()), Member: "legacy-divergent",
			}).Err())

			result, err := cache.AcquireAccountEgress(ctx, service.AccountEgressCacheAcquireRequest{
				AccountEgressAcquireRequest: service.AccountEgressAcquireRequest{
					Config:    config,
					LeaseID:   "force-pool-divergent",
					ForcePool: true,
				},
				LeaseTTL:  service.AccountEgressLeaseTTL,
				WaiterTTL: 2 * time.Minute,
			})
			require.NoError(t, err)
			require.Equal(t, service.AccountEgressStatusConfigStale, result.Status)
			require.Equal(t, "to_legacy", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())
			require.Equal(t, int64(0), cache.rdb.ZCard(ctx, accountEgressTotalKey(config.AccountID)).Val())
		})
	}
}

func TestForcePoolIgnoresExpiredLegacyPrimaryLease(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	candidate := accountEgressTestCandidate(0, 82, "ip:legacy")
	config := accountEgressTestConfig(1021, 1, 0, candidate)
	syncAccountEgressTestConfig(t, cache, config)
	ctx := context.Background()

	require.NoError(t, cache.rdb.Set(ctx, accountEgressModeKey(config.AccountID), "to_legacy", 0).Err())
	require.NoError(t, cache.rdb.ZAdd(ctx, accountSlotKey(config.AccountID), redis.Z{
		Score:  float64(time.Now().Unix() - int64(cache.slotTTLSeconds) - 1),
		Member: "legacy-expired",
	}).Err())

	result, err := cache.AcquireAccountEgress(ctx, service.AccountEgressCacheAcquireRequest{
		AccountEgressAcquireRequest: service.AccountEgressAcquireRequest{
			Config:    config,
			LeaseID:   "force-pool-after-expiry",
			ForcePool: true,
		},
		LeaseTTL:  service.AccountEgressLeaseTTL,
		WaiterTTL: 2 * time.Minute,
	})
	require.NoError(t, err)
	require.Equal(t, service.AccountEgressStatusAcquired, result.Status)
	require.Equal(t, "pool", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())
	require.Equal(t, int64(0), cache.rdb.ZCard(ctx, accountSlotKey(config.AccountID)).Val())
	require.Equal(t, int64(1), cache.rdb.ZCard(ctx, accountEgressTotalKey(config.AccountID)).Val())
	releaseAccountEgressTest(t, cache, config, result)
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
