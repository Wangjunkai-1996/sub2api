//go:build unit

package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newOpenAI429RecoveryTest(t *testing.T) (*concurrencyCache, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	server.SetTime(time.Unix(1700000000, 0))
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewConcurrencyCache(client, 15, 900).(*concurrencyCache), server
}

func TestOpenAI429RecoveryRoundsRejectStaleFailures(t *testing.T) {
	cache, server := newOpenAI429RecoveryTest(t)
	ctx := context.Background()
	now := time.Unix(1700000000, 0)
	initial, err := cache.AcquireOpenAI429Attempt(ctx, 1, "", "initial", 30*time.Second)
	require.NoError(t, err)
	require.True(t, initial.Allowed)
	late, err := cache.AcquireOpenAI429Attempt(ctx, 1, "", "late", 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, initial.Generation, late.Generation)
	applied, err := cache.FailOpenAI429Attempt(ctx, 1, "", initial.Generation, "initial", "round-1", 0)
	require.NoError(t, err)
	require.True(t, applied)

	for index, delay := range []time.Duration{2, 4, 8, 15, 15} {
		pending, err := cache.AcquireOpenAI429Attempt(ctx, 1, "", "waiting", 30*time.Second)
		require.NoError(t, err)
		require.False(t, pending.Allowed)
		assert.Equal(t, delay*time.Second, pending.RetryAfter)
		now = now.Add(time.Second)
		server.SetTime(now)
		applied, err = cache.FailOpenAI429Attempt(ctx, 1, "", late.Generation, "late", "stale", 90*time.Second)
		require.NoError(t, err)
		assert.False(t, applied, "old in-flight failures must not extend or advance a newer round")
		pending, err = cache.AcquireOpenAI429Attempt(ctx, 1, "", "waiting", 30*time.Second)
		require.NoError(t, err)
		assert.Equal(t, (delay-1)*time.Second, pending.RetryAfter)
		now = now.Add((delay - 1) * time.Second)
		server.SetTime(now)
		probe, err := cache.AcquireOpenAI429Attempt(ctx, 1, "", "probe", 30*time.Second)
		require.NoError(t, err)
		require.True(t, probe.Allowed)
		require.True(t, probe.Probe)
		if index < 4 {
			applied, err = cache.FailOpenAI429Attempt(ctx, 1, "", probe.Generation, "probe", probe.Generation+"-next", 0)
			require.NoError(t, err)
			require.True(t, applied)
		}
	}
}

func TestOpenAI429RecoverySharesOneProbeAndFencesExpiredOwner(t *testing.T) {
	cache, server := newOpenAI429RecoveryTest(t)
	otherClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = otherClient.Close() })
	other := NewConcurrencyCache(otherClient, 15, 900).(*concurrencyCache)
	ctx := context.Background()
	initial, err := cache.AcquireOpenAI429Attempt(ctx, 1, "", "initial", 30*time.Second)
	require.NoError(t, err)
	applied, err := cache.FailOpenAI429Attempt(ctx, 1, "", initial.Generation, "initial", "cooldown", 0)
	require.NoError(t, err)
	require.True(t, applied)
	server.SetTime(time.Unix(1700000002, 0))
	first, err := cache.AcquireOpenAI429Attempt(ctx, 1, "", "first", 30*time.Second)
	require.NoError(t, err)
	require.True(t, first.Probe)
	blocked, err := other.AcquireOpenAI429Attempt(ctx, 1, "", "second", 30*time.Second)
	require.NoError(t, err)
	assert.False(t, blocked.Allowed)
	assert.Equal(t, 30*time.Second, blocked.RetryAfter)

	server.SetTime(time.Unix(1700000033, 0))
	second, err := other.AcquireOpenAI429Attempt(ctx, 1, "", "second", 30*time.Second)
	require.NoError(t, err)
	require.True(t, second.Probe)
	accepted, err := cache.AcceptOpenAI429Attempt(ctx, 1, "", first.Generation, "first", "stale-success")
	require.NoError(t, err)
	assert.False(t, accepted)
	released, err := cache.ReleaseOpenAI429Probe(ctx, 1, "", first.Generation, "first")
	require.NoError(t, err)
	assert.False(t, released)
	refreshed, err := cache.RefreshOpenAI429Probe(ctx, 1, "", first.Generation, "first", 30*time.Second)
	require.NoError(t, err)
	assert.False(t, refreshed)
	refreshed, err = other.RefreshOpenAI429Probe(ctx, 1, "", second.Generation, "second", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, refreshed)
	released, err = other.ReleaseOpenAI429Probe(ctx, 1, "", second.Generation, "second")
	require.NoError(t, err)
	assert.True(t, released)
	third, err := cache.AcquireOpenAI429Attempt(ctx, 1, "", "third", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, third.Probe)
}

func TestOpenAI429RecoveryHonorsRetryAfterAndScope(t *testing.T) {
	cache, _ := newOpenAI429RecoveryTest(t)
	ctx := context.Background()
	initial, err := cache.AcquireOpenAI429Attempt(ctx, 1, "limited-model", "initial", 30*time.Second)
	require.NoError(t, err)
	applied, err := cache.FailOpenAI429Attempt(ctx, 1, "limited-model", initial.Generation, "initial", "cooldown", 90*time.Second)
	require.NoError(t, err)
	require.True(t, applied)
	blocked, err := cache.AcquireOpenAI429Attempt(ctx, 1, "limited-model", "waiting", 30*time.Second)
	require.NoError(t, err)
	assert.False(t, blocked.Allowed)
	assert.Equal(t, 90*time.Second, blocked.RetryAfter, "Retry-After is a lower bound, not an eight-second cap")
	otherModel, err := cache.AcquireOpenAI429Attempt(ctx, 1, "other-model", "other-model", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, otherModel.Allowed)
	otherAccount, err := cache.AcquireOpenAI429Attempt(ctx, 2, "limited-model", "other-account", 30*time.Second)
	require.NoError(t, err)
	assert.True(t, otherAccount.Allowed)
}

func TestOpenAI429RecoveryConcurrentInstancesAdmitOneProbe(t *testing.T) {
	cache, server := newOpenAI429RecoveryTest(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	peer := NewConcurrencyCache(client, 15, 900).(*concurrencyCache)
	ctx := context.Background()
	initial, err := cache.AcquireOpenAI429Attempt(ctx, 17, "", "initial", 30*time.Second)
	require.NoError(t, err)
	applied, err := cache.FailOpenAI429Attempt(ctx, 17, "", initial.Generation, "initial", "cooling", 0)
	require.NoError(t, err)
	require.True(t, applied)
	server.SetTime(time.Unix(1700000002, 0))
	type result struct {
		admission service.OpenAI429Admission
		err       error
	}
	start := make(chan struct{})
	results := make(chan result, 2)
	for token, instance := range map[string]*concurrencyCache{"instance-a": cache, "instance-b": peer} {
		go func() {
			<-start
			admission, err := instance.AcquireOpenAI429Attempt(ctx, 17, "", token, 30*time.Second)
			results <- result{admission: admission, err: err}
		}()
	}
	close(start)
	admitted := 0
	for range 2 {
		result := <-results
		require.NoError(t, result.err)
		if result.admission.Allowed {
			admitted++
			assert.True(t, result.admission.Probe)
		} else {
			assert.Equal(t, 30*time.Second, result.admission.RetryAfter)
		}
	}
	assert.Equal(t, 1, admitted)
}

func TestOpenAI429RecoveryAcceptancePreservesGeneratingContext(t *testing.T) {
	cache, server := newOpenAI429RecoveryTest(t)
	svc := service.NewConcurrencyService(cache)
	ctx := context.Background()
	initial, _, err := svc.BeginOpenAI429Attempt(ctx, 1, "")
	require.NoError(t, err)
	require.NotNil(t, initial)
	defer initial.Release()
	applied, err := initial.RateLimited(ctx, 0)
	require.NoError(t, err)
	require.True(t, applied)
	server.SetTime(time.Unix(1700000002, 0))
	probe, _, err := svc.BeginOpenAI429Attempt(ctx, 1, "")
	require.NoError(t, err)
	require.NotNil(t, probe)
	defer probe.Release()
	require.True(t, probe.Probe())
	applied, err = probe.Accepted(ctx)
	require.NoError(t, err)
	require.True(t, applied)
	assert.NoError(t, probe.Context().Err(), "acceptance must not interrupt a long-running generation")
	next, _, err := svc.BeginOpenAI429Attempt(ctx, 1, "")
	require.NoError(t, err)
	require.NotNil(t, next)
	defer next.Release()
	assert.False(t, next.Probe(), "remaining capacity is restored on valid generation, not only response completion")
	applied, err = next.RateLimited(ctx, 0)
	require.NoError(t, err)
	require.True(t, applied)
	applied, err = probe.Accepted(ctx)
	require.NoError(t, err)
	assert.False(t, applied, "old successful probes must not clear newer cooldowns")
	blocked, remaining, err := svc.BeginOpenAI429Attempt(ctx, 1, "")
	require.NoError(t, err)
	assert.Nil(t, blocked)
	assert.Equal(t, 2*time.Second, remaining)
	probe.Release()
	require.NoError(t, cache.rdb.Close())
	applied, err = probe.Accepted(ctx)
	assert.NoError(t, err, "later output events must not repeat recovery writes")
	assert.False(t, applied)
}

func TestOpenAI429RecoveryCancellationReleasesProbeAndRedisFailureDeniesAdmission(t *testing.T) {
	cache, server := newOpenAI429RecoveryTest(t)
	svc := service.NewConcurrencyService(cache)
	ctx := context.Background()
	initial, _, err := svc.BeginOpenAI429Attempt(ctx, 1, "")
	require.NoError(t, err)
	_, err = initial.RateLimited(ctx, 0)
	require.NoError(t, err)
	server.SetTime(time.Unix(1700000002, 0))
	probeCtx, cancel := context.WithCancel(ctx)
	probe, _, err := svc.BeginOpenAI429Attempt(probeCtx, 1, "")
	require.NoError(t, err)
	require.NotNil(t, probe)
	cancel()
	probe.Release()
	next, _, err := svc.BeginOpenAI429Attempt(ctx, 1, "")
	require.NoError(t, err)
	require.NotNil(t, next)
	require.True(t, next.Probe())
	next.Release()
	require.NoError(t, cache.rdb.Close())
	denied, _, err := svc.BeginOpenAI429Attempt(ctx, 1, "")
	assert.Nil(t, denied)
	assert.ErrorIs(t, err, service.ErrOpenAI429RecoveryUnavailable)
}
