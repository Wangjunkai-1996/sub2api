package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestLiveLeaseReplacesRegularSlotsAndCountsTowardLimits(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	regular := NewConcurrencyCache(client, 15, 900)
	live, ok := regular.(service.LiveConcurrencyCache)
	require.True(t, ok)
	ctx := context.Background()

	accountAcquired, err := regular.AcquireAccountSlot(ctx, 10, 1, "regular-account")
	require.NoError(t, err)
	require.True(t, accountAcquired)
	userAcquired, err := regular.AcquireUserSlot(ctx, 20, 1, "regular-user")
	require.NoError(t, err)
	require.True(t, userAcquired)

	acquired, err := live.AcquireLiveLease(ctx, 10, 1, 20, 1, 30, "live-lease", true)
	require.NoError(t, err)
	require.True(t, acquired)
	require.NoError(t, regular.ReleaseAccountSlot(ctx, 10, "regular-account"))
	require.NoError(t, regular.ReleaseUserSlot(ctx, 20, "regular-user"))

	accountCount, err := regular.GetAccountConcurrency(ctx, 10)
	require.NoError(t, err)
	require.Equal(t, 1, accountCount)
	userCount, err := regular.GetUserConcurrency(ctx, 20)
	require.NoError(t, err)
	require.Equal(t, 1, userCount)
	accountAcquired, err = regular.AcquireAccountSlot(ctx, 10, 1, "ordinary-blocked")
	require.NoError(t, err)
	require.False(t, accountAcquired)

	refreshed, err := live.RefreshLiveLease(ctx, 10, 20, 30, "live-lease")
	require.NoError(t, err)
	require.True(t, refreshed)
	require.NoError(t, live.ReleaseLiveLease(ctx, 10, 20, 30, "live-lease"))
	accountAcquired, err = regular.AcquireAccountSlot(ctx, 10, 1, "ordinary-allowed")
	require.NoError(t, err)
	require.True(t, accountAcquired)
}

func TestLiveLeaseExpiresWithoutRefresh(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	regular := NewConcurrencyCache(client, 15, 900)
	live, ok := regular.(service.LiveConcurrencyCache)
	require.True(t, ok)
	ctx := context.Background()

	acquired, err := live.AcquireLiveLease(ctx, 10, 1, 20, 1, 30, "expired-live", false)
	require.NoError(t, err)
	require.True(t, acquired)

	redisServer.FastForward(61 * time.Second)
	acquired, err = regular.AcquireAccountSlot(ctx, 10, 1, "ordinary-after-expiry")
	require.NoError(t, err)
	require.True(t, acquired)
	refreshed, err := live.RefreshLiveLease(ctx, 10, 20, 30, "expired-live")
	require.NoError(t, err)
	require.False(t, refreshed)
}

func TestTransitionModeFencesLegacyAdmissionsAndKeepsLiveLeasesValid(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	ctx := context.Background()
	config := accountEgressTestConfig(11, 3, 0,
		accountEgressTestCandidate(0, 101, "ip:primary"),
		accountEgressTestCandidate(1, 102, "ip:second"),
		accountEgressTestCandidate(2, 103, "ip:third"),
	)
	syncAccountEgressTestConfig(t, cache, config)

	identityID := config.Candidates[0].IdentityID
	legacy, err := cache.AcquireAccountSlotForEgress(ctx, config.AccountID, 3, "legacy-regular", identityID)
	require.NoError(t, err)
	require.True(t, legacy)
	pool := acquireAccountEgressTest(t, cache, config, "pool-owner", "", "")
	require.Equal(t, service.AccountEgressStatusAcquired, pool.Status)
	require.Equal(t, "transition", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())

	legacy, err = cache.AcquireAccountSlotForEgress(ctx, config.AccountID, 3, "legacy-regular", identityID)
	require.NoError(t, err)
	require.True(t, legacy, "an existing legacy request must remain refreshable")
	legacy, err = cache.AcquireAccountSlot(ctx, config.AccountID, 3, "legacy-new")
	require.NoError(t, err)
	require.False(t, legacy, "transition must reject new legacy admissions while pool leases exist")

	legacyLive, err := cache.AcquireLiveLeaseForLegacyEgress(ctx, config.AccountID, 3, 21, 3, 31, "legacy-live", identityID, true)
	require.NoError(t, err)
	require.True(t, legacyLive, "an admitted legacy request must be able to promote to Live")
	require.NoError(t, cache.ReleaseAccountSlotForEgress(ctx, config.AccountID, "legacy-regular", identityID))
	legacyLive, err = cache.RefreshLiveLeaseForLegacyEgress(ctx, config.AccountID, 21, 31, "legacy-live", identityID)
	require.NoError(t, err)
	require.True(t, legacyLive)

	mode, err := cache.BeginAccountEgressLegacyTransition(ctx, config.AccountID)
	require.NoError(t, err)
	require.Equal(t, "to_legacy", mode)

	egressRef := service.AccountEgressLeaseRef{
		AccountID:     config.AccountID,
		ID:            pool.LeaseID,
		BindingID:     pool.BindingID,
		IdentityID:    pool.IdentityID,
		ConfigVersion: pool.ConfigVersion,
	}
	poolLive, err := cache.AcquireLiveLeaseForEgress(ctx, egressRef, 22, 3, 32, "pool-live", true)
	require.NoError(t, err)
	require.True(t, poolLive)
	poolLive, err = cache.RefreshLiveLeaseForEgress(ctx, egressRef, 22, 32, "pool-live")
	require.NoError(t, err)
	require.True(t, poolLive)
	require.NoError(t, cache.ReleaseLiveLeaseForEgress(ctx, egressRef, 22, 32, "pool-live"))

	releaseAccountEgressTest(t, cache, config, pool)
	require.Equal(t, "legacy", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())
	require.NoError(t, cache.ReleaseLiveLeaseForLegacyEgress(ctx, config.AccountID, 21, 31, "legacy-live", identityID))
	legacy, err = cache.AcquireAccountSlotForEgress(ctx, config.AccountID, 3, "legacy-after-pool", identityID)
	require.NoError(t, err)
	require.True(t, legacy, "off-mode traffic can return to legacy after pool leases drain")
	require.Equal(t, "legacy", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())
}

func TestLegacyEgressRegularMirrorRefreshCannotRecreateMissingIdentity(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	ctx := context.Background()
	const accountID int64 = 12
	const identityID = "ip:regular"

	acquired, err := cache.AcquireAccountSlotForEgress(ctx, accountID, 3, "regular", identityID)
	require.NoError(t, err)
	require.True(t, acquired)
	for _, key := range []string{
		accountSlotKey(accountID),
		accountEgressLegacyRegularKey(accountID),
		accountEgressLegacyRegularIdentityKey(accountID, identityID),
	} {
		require.Equal(t, int64(1), cache.rdb.ZCard(ctx, key).Val())
	}

	refreshed, err := cache.RefreshAccountSlotForEgress(ctx, accountID, "regular", identityID)
	require.NoError(t, err)
	require.True(t, refreshed)
	require.NoError(t, cache.rdb.ZRem(ctx, accountEgressLegacyRegularIdentityKey(accountID, identityID), "regular").Err())
	refreshed, err = cache.RefreshAccountSlotForEgress(ctx, accountID, "regular", identityID)
	require.NoError(t, err)
	require.False(t, refreshed)
	require.Equal(t, int64(0), cache.rdb.ZCard(ctx, accountEgressLegacyRegularIdentityKey(accountID, identityID)).Val())

	require.NoError(t, cache.ReleaseAccountSlotForEgress(ctx, accountID, "regular", identityID))
	require.Equal(t, int64(0), cache.rdb.ZCard(ctx, accountSlotKey(accountID)).Val())
	require.Equal(t, int64(0), cache.rdb.ZCard(ctx, accountEgressLegacyRegularKey(accountID)).Val())
}

func TestLegacyEgressLiveMirrorRefreshCannotRecreateMissingIdentity(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	ctx := context.Background()
	const accountID int64 = 13
	const userID int64 = 23
	const apiKeyID int64 = 33
	const identityID = "ip:live"

	acquired, err := cache.AcquireLiveLeaseForLegacyEgress(ctx, accountID, 3, userID, 3, apiKeyID, "live", identityID, false)
	require.NoError(t, err)
	require.True(t, acquired)
	for _, key := range []string{
		liveAccountSlotKey(accountID),
		liveUserSlotKey(userID),
		liveAPIKeySlotKey(apiKeyID),
		accountEgressLegacyLiveKey(accountID),
		accountEgressLegacyLiveIdentityKey(accountID, identityID),
	} {
		require.Equal(t, int64(1), cache.rdb.ZCard(ctx, key).Val())
	}

	refreshed, err := cache.RefreshLiveLeaseForLegacyEgress(ctx, accountID, userID, apiKeyID, "live", identityID)
	require.NoError(t, err)
	require.True(t, refreshed)
	require.NoError(t, cache.rdb.ZRem(ctx, accountEgressLegacyLiveIdentityKey(accountID, identityID), "live").Err())
	refreshed, err = cache.RefreshLiveLeaseForLegacyEgress(ctx, accountID, userID, apiKeyID, "live", identityID)
	require.NoError(t, err)
	require.False(t, refreshed)
	require.Equal(t, int64(0), cache.rdb.ZCard(ctx, accountEgressLegacyLiveIdentityKey(accountID, identityID)).Val())

	require.NoError(t, cache.ReleaseLiveLeaseForLegacyEgress(ctx, accountID, userID, apiKeyID, "live", identityID))
	for _, key := range []string{
		liveAccountSlotKey(accountID),
		liveUserSlotKey(userID),
		liveAPIKeySlotKey(apiKeyID),
		accountEgressLegacyLiveKey(accountID),
	} {
		require.Equal(t, int64(0), cache.rdb.ZCard(ctx, key).Val())
	}
}

func TestTransitionWithoutPoolLeaseNeverReopensLegacyAdmission(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	ctx := context.Background()
	const accountID int64 = 14
	const identityID = "ip:transition"

	regular, err := cache.AcquireAccountSlotForEgress(ctx, accountID, 3, "regular", identityID)
	require.NoError(t, err)
	require.True(t, regular)
	mode, err := cache.BeginAccountEgressPoolTransition(ctx, accountID)
	require.NoError(t, err)
	require.Equal(t, "transition", mode)

	live, err := cache.AcquireLiveLeaseForLegacyEgress(ctx, accountID, 3, 24, 3, 34, "live", identityID, true)
	require.NoError(t, err)
	require.True(t, live)
	require.Equal(t, "transition", cache.rdb.Get(ctx, accountEgressModeKey(accountID)).Val())
	blocked, err := cache.AcquireAccountSlotForEgress(ctx, accountID, 3, "new-legacy", identityID)
	require.NoError(t, err)
	require.False(t, blocked)
	refreshed, err := cache.RefreshAccountSlotForEgress(ctx, accountID, "regular", identityID)
	require.NoError(t, err)
	require.True(t, refreshed)
	refreshed, err = cache.RefreshLiveLeaseForLegacyEgress(ctx, accountID, 24, 34, "live", identityID)
	require.NoError(t, err)
	require.True(t, refreshed)

	require.NoError(t, cache.ReleaseLiveLeaseForLegacyEgress(ctx, accountID, 24, 34, "live", identityID))
	require.NoError(t, cache.ReleaseAccountSlotForEgress(ctx, accountID, "regular", identityID))
}

func TestLegacyLiveAdmissionInitiatesPoolToLegacyTransition(t *testing.T) {
	cache, _ := newAccountEgressCacheTest(t)
	ctx := context.Background()
	candidate := accountEgressTestCandidate(0, 104, "ip:live-fallback")
	config := accountEgressTestConfig(15, 2, 0, candidate)
	syncAccountEgressTestConfig(t, cache, config)
	pool := acquireAccountEgressTest(t, cache, config, "pool", "", "")
	require.Equal(t, service.AccountEgressStatusAcquired, pool.Status)

	live, err := cache.AcquireLiveLeaseForLegacyEgress(ctx, config.AccountID, 2, 25, 2, 35, "legacy-live", candidate.IdentityID, false)
	require.NoError(t, err)
	require.False(t, live)
	require.Equal(t, "to_legacy", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())
	require.Equal(t, service.AccountEgressStatusLegacyDraining, acquireAccountEgressTest(t, cache, config, "new-pool-blocked", "", "").Status)

	releaseAccountEgressTest(t, cache, config, pool)
	require.Equal(t, "legacy", cache.rdb.Get(ctx, accountEgressModeKey(config.AccountID)).Val())
	live, err = cache.AcquireLiveLeaseForLegacyEgress(ctx, config.AccountID, 2, 25, 2, 35, "legacy-live", candidate.IdentityID, false)
	require.NoError(t, err)
	require.True(t, live)
	require.NoError(t, cache.ReleaseLiveLeaseForLegacyEgress(ctx, config.AccountID, 25, 35, "legacy-live", candidate.IdentityID))
}

func TestWarmupExclusiveRefreshYieldsToRegisteredBusinessWaiter(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	cache := NewConcurrencyCache(client, 15, 900)
	exclusive, ok := cache.(service.AccountExclusiveSlotCache)
	require.True(t, ok)
	ctx := context.Background()
	const accountID int64 = 40

	acquired, err := exclusive.AcquireAccountExclusive(ctx, accountID, "warmup-owner", 2*time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	queued, err := cache.IncrementAccountWaitCount(ctx, accountID, 10)
	require.NoError(t, err)
	require.True(t, queued, "real traffic must be able to register behind warmup")

	refreshed, err := exclusive.RefreshAccountExclusive(ctx, accountID, "warmup-owner", 2*time.Minute)
	require.NoError(t, err)
	require.False(t, refreshed, "warmup must yield once real traffic is waiting")

	released, err := exclusive.ReleaseAccountExclusive(ctx, accountID, "wrong-owner")
	require.NoError(t, err)
	require.False(t, released, "waiter detection must not weaken token fencing")
	released, err = exclusive.ReleaseAccountExclusive(ctx, accountID, "warmup-owner")
	require.NoError(t, err)
	require.True(t, released)
}
