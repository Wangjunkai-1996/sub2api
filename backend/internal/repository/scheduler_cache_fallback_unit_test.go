//go:build unit

package repository

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type schedulerFallbackAccountRepo struct {
	service.AccountRepository

	calls       atomic.Int64
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	accounts    []service.Account
}

func (r *schedulerFallbackAccountRepo) ListSchedulableByGroupIDAndPlatform(ctx context.Context, _ int64, _ string) ([]service.Account, error) {
	r.calls.Add(1)
	r.enteredOnce.Do(func() { close(r.entered) })
	select {
	case <-r.release:
		return append([]service.Account(nil), r.accounts...), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func seedLegacySchedulerMetadata(t *testing.T, cache *schedulerCache, bucket service.SchedulerBucket, account service.Account) {
	t.Helper()
	ctx := context.Background()
	token, err := cache.CaptureBucketWriteToken(ctx, bucket)
	require.NoError(t, err)
	require.NoError(t, cache.SetSnapshot(ctx, bucket, token, []service.Account{account}))

	// v0.1.185 wrote the Account projection directly, before the top-level
	// projection marker existed.  Reproduce that wire payload in the real cache.
	legacyPayload, err := json.Marshal(buildSchedulerMetadataAccount(account))
	require.NoError(t, err)
	require.NoError(t, cache.rdb.Set(ctx, schedulerAccountMetaKey(strconv.FormatInt(account.ID, 10)), legacyPayload, 0).Err())

	accounts, hit, err := cache.GetSnapshot(ctx, bucket)
	require.NoError(t, err)
	require.False(t, hit)
	require.Nil(t, accounts)
}

func newSchedulerFallbackTestService(cache *schedulerCache, repo service.AccountRepository) *service.SchedulerSnapshotService {
	return service.NewSchedulerSnapshotService(cache, nil, repo, nil, &config.Config{
		RunMode: config.RunModeStandard,
		Gateway: config.GatewayConfig{Scheduling: config.GatewaySchedulingConfig{
			DbFallbackEnabled:        true,
			DbFallbackTimeoutSeconds: 2,
			DbFallbackMaxQPS:         1,
		}},
	})
}

func TestSchedulerLegacyMetadataConcurrentFallbackQueriesDBOnce(t *testing.T) {
	cache := newSchedulerCacheUnit(t)
	bucket := service.SchedulerBucket{GroupID: 481, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeSingle}
	account := service.Account{
		ID:          48101,
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeOAuth,
		Status:      service.StatusActive,
		Schedulable: true,
		Extra:       map[string]any{"openai_oauth_passthrough": true},
	}
	seedLegacySchedulerMetadata(t, cache, bucket, account)

	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	repo := &schedulerFallbackAccountRepo{
		entered:  make(chan struct{}),
		release:  release,
		accounts: []service.Account{account},
	}
	snapshot := newSchedulerFallbackTestService(cache, repo)

	const callers = 24
	start := make(chan struct{})
	type result struct {
		accounts []service.Account
		err      error
	}
	results := make(chan result, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			accounts, _, err := snapshot.ListSchedulableAccounts(context.Background(), &bucket.GroupID, bucket.Platform, false)
			results <- result{accounts: accounts, err: err}
		}()
	}
	close(start)

	select {
	case <-repo.entered:
	case <-time.After(time.Second):
		t.Fatal("scheduler fallback did not reach the database")
	}
	require.Never(t, func() bool { return repo.calls.Load() > 1 }, 100*time.Millisecond, 10*time.Millisecond)
	releaseOnce.Do(func() { close(release) })
	wg.Wait()
	close(results)

	for result := range results {
		require.NoError(t, result.err)
		require.Len(t, result.accounts, 1)
		require.Equal(t, account.ID, result.accounts[0].ID)
	}
	require.Equal(t, int64(1), repo.calls.Load(), "one bucket miss must produce one shared DB query")

	accounts, hit, err := cache.GetSnapshot(context.Background(), bucket)
	require.NoError(t, err)
	require.True(t, hit, "the shared fallback must repair the legacy projection")
	require.Len(t, accounts, 1)
	require.True(t, accounts[0].IsOpenAIPassthroughEnabled())
}

func TestSchedulerFallbackLeaderDeadlineDoesNotCancelFollower(t *testing.T) {
	cache := newSchedulerCacheUnit(t)
	bucket := service.SchedulerBucket{GroupID: 482, Platform: service.PlatformOpenAI, Mode: service.SchedulerModeSingle}
	account := service.Account{
		ID:          48201,
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeOAuth,
		Status:      service.StatusActive,
		Schedulable: true,
	}
	seedLegacySchedulerMetadata(t, cache, bucket, account)

	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	repo := &schedulerFallbackAccountRepo{
		entered:  make(chan struct{}),
		release:  release,
		accounts: []service.Account{account},
	}
	snapshot := newSchedulerFallbackTestService(cache, repo)

	leaderCtx, cancelLeader := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelLeader()
	leaderResult := make(chan error, 1)
	go func() {
		_, _, err := snapshot.ListSchedulableAccounts(leaderCtx, &bucket.GroupID, bucket.Platform, false)
		leaderResult <- err
	}()
	select {
	case <-repo.entered:
	case <-time.After(time.Second):
		t.Fatal("leader did not reach the database")
	}

	type fallbackResult struct {
		accounts []service.Account
		err      error
	}
	followerResult := make(chan fallbackResult, 1)
	go func() {
		accounts, _, err := snapshot.ListSchedulableAccounts(context.Background(), &bucket.GroupID, bucket.Platform, false)
		followerResult <- fallbackResult{accounts: accounts, err: err}
	}()

	require.ErrorIs(t, <-leaderResult, context.DeadlineExceeded)
	require.Equal(t, int64(1), repo.calls.Load(), "leader deadline must not start a follower DB query")
	releaseOnce.Do(func() { close(release) })
	follower := <-followerResult
	require.NoError(t, follower.err, "the detached shared load must remain available to the follower")
	require.Len(t, follower.accounts, 1)
	require.Equal(t, account.ID, follower.accounts[0].ID)
	require.Equal(t, int64(1), repo.calls.Load())
}
