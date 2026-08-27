package repository

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestTrafficDirectorHealthRedisStore_StateMachineAndRecovery(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store := NewTrafficDirectorHealthRedisStore(client)
	ctx := context.Background()
	base := time.UnixMilli(1_800_000_000_000).UTC()
	first := trafficDirectorHealthFailureRequest(91, "gpt-5", base, "")

	snapshot, err := store.RecordTrafficDirectorHealthFailure(ctx, first)
	require.NoError(t, err)
	require.Equal(t, service.TrafficDirectorHealthStateSuspect, snapshot.State)
	require.Equal(t, 1, snapshot.FailureStreak)
	require.True(t, snapshot.MutationApplied)

	second := first
	second.Now = base.Add(time.Second)
	snapshot, err = store.RecordTrafficDirectorHealthFailure(ctx, second)
	require.NoError(t, err)
	require.Equal(t, service.TrafficDirectorHealthStateOpen, snapshot.State)
	require.Equal(t, 2, snapshot.FailureStreak)
	require.Equal(t, base.Add(time.Second+10*time.Second), snapshot.OpenUntil)

	blocked, err := store.CheckTrafficDirectorHealth(ctx, trafficDirectorHealthCheckRequest(91, "gpt-5", base.Add(5*time.Second), true, "blocked-token"))
	require.NoError(t, err)
	require.Equal(t, service.TrafficDirectorHealthStateOpen, blocked.State)
	require.False(t, blocked.ProbeAcquired)

	probe, err := store.CheckTrafficDirectorHealth(ctx, trafficDirectorHealthCheckRequest(91, "gpt-5", base.Add(12*time.Second), true, "probe-1"))
	require.NoError(t, err)
	require.Equal(t, service.TrafficDirectorHealthStateHalfOpen, probe.State)
	require.True(t, probe.ProbeAcquired)
	require.Equal(t, base.Add(12*time.Second+2*time.Minute), probe.ProbeUntil)

	otherProbe, err := store.CheckTrafficDirectorHealth(ctx, trafficDirectorHealthCheckRequest(91, "gpt-5", base.Add(13*time.Second), true, "probe-2"))
	require.NoError(t, err)
	require.Equal(t, service.TrafficDirectorHealthStateHalfOpen, otherProbe.State)
	require.False(t, otherProbe.ProbeAcquired)

	renewed, err := store.RenewTrafficDirectorHealthProbe(ctx, service.TrafficDirectorHealthStoreProbeRequest{
		AccountID:       91,
		NormalizedModel: "gpt-5",
		Now:             base.Add(30 * time.Second),
		ProbeToken:      "probe-1",
		ProbeLease:      2 * time.Minute,
	})
	require.NoError(t, err)
	require.True(t, renewed)

	halfFailure := trafficDirectorHealthFailureRequest(91, "gpt-5", base.Add(31*time.Second), "probe-1")
	snapshot, err = store.RecordTrafficDirectorHealthFailure(ctx, halfFailure)
	require.NoError(t, err)
	require.Equal(t, service.TrafficDirectorHealthStateOpen, snapshot.State)
	require.Equal(t, 3, snapshot.FailureStreak)
	require.Equal(t, base.Add(31*time.Second+45*time.Second), snapshot.OpenUntil)

	lateSuccess, err := store.RecordTrafficDirectorHealthSuccess(ctx, service.TrafficDirectorHealthStoreSuccessRequest{
		AccountID:       91,
		NormalizedModel: "gpt-5",
		ProbeToken:      "probe-1",
	})
	require.NoError(t, err)
	require.False(t, lateSuccess, "a stale probe result must not clear a newer open state")

	probe, err = store.CheckTrafficDirectorHealth(ctx, trafficDirectorHealthCheckRequest(91, "gpt-5", base.Add(77*time.Second), true, "probe-3"))
	require.NoError(t, err)
	require.True(t, probe.ProbeAcquired)
	restored, err := store.RecordTrafficDirectorHealthSuccess(ctx, service.TrafficDirectorHealthStoreSuccessRequest{
		AccountID:       91,
		NormalizedModel: "gpt-5",
		ProbeToken:      "probe-3",
	})
	require.NoError(t, err)
	require.True(t, restored)

	healthy, err := store.CheckTrafficDirectorHealth(ctx, trafficDirectorHealthCheckRequest(91, "gpt-5", base.Add(78*time.Second), true, "probe-4"))
	require.NoError(t, err)
	require.Equal(t, service.TrafficDirectorHealthStateHealthy, healthy.State)
	require.False(t, healthy.ProbeAcquired)
}

func TestTrafficDirectorHealthRedisStore_OnlyOneHalfOpenProbeAcrossClients(t *testing.T) {
	server := miniredis.RunT(t)
	clientOne := redis.NewClient(&redis.Options{Addr: server.Addr()})
	clientTwo := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() {
		require.NoError(t, clientOne.Close())
		require.NoError(t, clientTwo.Close())
	})
	storeOne := NewTrafficDirectorHealthRedisStore(clientOne)
	storeTwo := NewTrafficDirectorHealthRedisStore(clientTwo)
	ctx := context.Background()
	base := time.UnixMilli(1_800_100_000_000).UTC()
	for i := 0; i < 2; i++ {
		snapshot, err := storeOne.RecordTrafficDirectorHealthFailure(ctx, trafficDirectorHealthFailureRequest(92, "gpt-5", base.Add(time.Duration(i)*time.Second), ""))
		require.NoError(t, err)
		if i == 1 {
			require.Equal(t, service.TrafficDirectorHealthStateOpen, snapshot.State)
		}
	}

	const callers = 32
	var wg sync.WaitGroup
	probes := make(chan bool, callers)
	errorsCh := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			store := storeOne
			if index%2 == 1 {
				store = storeTwo
			}
			snapshot, err := store.CheckTrafficDirectorHealth(ctx, trafficDirectorHealthCheckRequest(
				92,
				"gpt-5",
				base.Add(12*time.Second),
				true,
				"probe-"+string(rune('a'+index)),
			))
			if err != nil {
				errorsCh <- err
				return
			}
			probes <- snapshot.ProbeAcquired
		}(i)
	}
	wg.Wait()
	close(probes)
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}
	acquired := 0
	for probe := range probes {
		if probe {
			acquired++
		}
	}
	require.Equal(t, 1, acquired)
}

func TestTrafficDirectorHealthRedisStore_RenewedProbeSurvivesFailureStreakTTL(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store := NewTrafficDirectorHealthRedisStore(client)
	ctx := context.Background()
	base := time.UnixMilli(1_800_125_000_000).UTC()

	for i := 0; i < 2; i++ {
		_, err := store.RecordTrafficDirectorHealthFailure(ctx, trafficDirectorHealthFailureRequest(
			98,
			"gpt-5",
			base.Add(time.Duration(i)*time.Second),
			"",
		))
		require.NoError(t, err)
	}

	server.FastForward(29 * time.Minute)
	check := trafficDirectorHealthCheckRequest(98, "gpt-5", base.Add(29*time.Minute), true, "long-probe")
	check.ProbeLease = 10 * time.Minute
	probe, err := store.CheckTrafficDirectorHealth(ctx, check)
	require.NoError(t, err)
	require.True(t, probe.ProbeAcquired)

	for _, elapsed := range []time.Duration{38 * time.Minute, 47 * time.Minute, 56 * time.Minute} {
		server.FastForward(9 * time.Minute)
		renewed, renewErr := store.RenewTrafficDirectorHealthProbe(ctx, service.TrafficDirectorHealthStoreProbeRequest{
			AccountID:       98,
			NormalizedModel: "gpt-5",
			Now:             base.Add(elapsed),
			ProbeToken:      "long-probe",
			ProbeLease:      10 * time.Minute,
		})
		require.NoError(t, renewErr)
		require.True(t, renewed)
	}

	server.FastForward(3 * time.Minute)
	other := trafficDirectorHealthCheckRequest(98, "gpt-5", base.Add(59*time.Minute), true, "other-probe")
	other.ProbeLease = 10 * time.Minute
	blocked, err := store.CheckTrafficDirectorHealth(ctx, other)
	require.NoError(t, err)
	require.Equal(t, service.TrafficDirectorHealthStateHalfOpen, blocked.State)
	require.False(t, blocked.ProbeAcquired, "a renewed long-running probe must retain unique ownership")
	require.Equal(t, base.Add(66*time.Minute), blocked.ProbeUntil)

	failure, err := store.RecordTrafficDirectorHealthFailure(ctx, trafficDirectorHealthFailureRequest(
		98,
		"gpt-5",
		base.Add(59*time.Minute+time.Second),
		"long-probe",
	))
	require.NoError(t, err)
	require.True(t, failure.MutationApplied)
	require.Equal(t, service.TrafficDirectorHealthStateOpen, failure.State)
	require.Equal(t, 3, failure.FailureStreak)
}

func TestTrafficDirectorHealthRedisStore_ProbeOwnerCanReleaseImmediately(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store := NewTrafficDirectorHealthRedisStore(client)
	ctx := context.Background()
	base := time.UnixMilli(1_800_150_000_000).UTC()

	for i := 0; i < 2; i++ {
		_, err := store.RecordTrafficDirectorHealthFailure(ctx, trafficDirectorHealthFailureRequest(
			97,
			"gpt-5",
			base.Add(time.Duration(i)*time.Second),
			"",
		))
		require.NoError(t, err)
	}
	first, err := store.CheckTrafficDirectorHealth(ctx, trafficDirectorHealthCheckRequest(
		97, "gpt-5", base.Add(12*time.Second), true, "probe-owner",
	))
	require.NoError(t, err)
	require.True(t, first.ProbeAcquired)

	released, err := store.ReleaseTrafficDirectorHealthProbe(ctx, service.TrafficDirectorHealthStoreProbeReleaseRequest{
		AccountID: 97, NormalizedModel: "gpt-5", ProbeToken: "stale-owner",
	})
	require.NoError(t, err)
	require.False(t, released)
	released, err = store.ReleaseTrafficDirectorHealthProbe(ctx, service.TrafficDirectorHealthStoreProbeReleaseRequest{
		AccountID: 97, NormalizedModel: "gpt-5", ProbeToken: "probe-owner",
	})
	require.NoError(t, err)
	require.True(t, released)

	second, err := store.CheckTrafficDirectorHealth(ctx, trafficDirectorHealthCheckRequest(
		97, "gpt-5", base.Add(13*time.Second), true, "probe-next",
	))
	require.NoError(t, err)
	require.True(t, second.ProbeAcquired, "released ownership must not block the next real probe for the full lease")
}

func TestTrafficDirectorHealthRedisStore_StreakExpiresAfterThirtyMinutes(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store := NewTrafficDirectorHealthRedisStore(client)
	ctx := context.Background()
	base := time.UnixMilli(1_800_200_000_000).UTC()

	_, err := store.RecordTrafficDirectorHealthFailure(ctx, trafficDirectorHealthFailureRequest(93, "gpt-5", base, ""))
	require.NoError(t, err)
	reset, err := store.CheckTrafficDirectorHealth(ctx, trafficDirectorHealthCheckRequest(93, "gpt-5", base.Add(30*time.Minute), true, "probe-reset"))
	require.NoError(t, err)
	require.Equal(t, service.TrafficDirectorHealthStateHealthy, reset.State)

	secondStreak, err := store.RecordTrafficDirectorHealthFailure(ctx, trafficDirectorHealthFailureRequest(93, "gpt-5", base.Add(30*time.Minute+time.Second), ""))
	require.NoError(t, err)
	require.Equal(t, service.TrafficDirectorHealthStateSuspect, secondStreak.State)
	require.Equal(t, 1, secondStreak.FailureStreak)
}

func TestTrafficDirectorHealthRedisStore_ObserveSuccessRestoresOpenWithoutWeakeningEnforce(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store := NewTrafficDirectorHealthRedisStore(client)
	ctx := context.Background()
	base := time.UnixMilli(1_800_250_000_000).UTC()

	for i := 0; i < 2; i++ {
		snapshot, err := store.RecordTrafficDirectorHealthFailure(ctx, trafficDirectorHealthFailureRequest(
			96,
			"gpt-5",
			base.Add(time.Duration(i)*time.Second),
			"",
		))
		require.NoError(t, err)
		if i == 1 {
			require.Equal(t, service.TrafficDirectorHealthStateOpen, snapshot.State)
		}
	}

	restored, err := store.RecordTrafficDirectorHealthSuccess(ctx, service.TrafficDirectorHealthStoreSuccessRequest{
		AccountID:       96,
		NormalizedModel: "gpt-5",
	})
	require.NoError(t, err)
	require.False(t, restored, "an enforce success without a half-open probe must not close the breaker")

	stillOpen, err := store.CheckTrafficDirectorHealth(ctx, trafficDirectorHealthCheckRequest(
		96,
		"gpt-5",
		base.Add(2*time.Second),
		false,
		"",
	))
	require.NoError(t, err)
	require.Equal(t, service.TrafficDirectorHealthStateOpen, stillOpen.State)

	restored, err = store.RecordTrafficDirectorHealthSuccess(ctx, service.TrafficDirectorHealthStoreSuccessRequest{
		AccountID:            96,
		NormalizedModel:      "gpt-5",
		AllowObserveRecovery: true,
	})
	require.NoError(t, err)
	require.True(t, restored)

	healthy, err := store.CheckTrafficDirectorHealth(ctx, trafficDirectorHealthCheckRequest(
		96,
		"gpt-5",
		base.Add(3*time.Second),
		false,
		"",
	))
	require.NoError(t, err)
	require.Equal(t, service.TrafficDirectorHealthStateHealthy, healthy.State)
}

func TestTrafficDirectorHealthRedisStore_NormalizedModelAndAccountIsolation(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	store := NewTrafficDirectorHealthRedisStore(client)
	ctx := context.Background()
	base := time.UnixMilli(1_800_300_000_000).UTC()

	_, err := store.RecordTrafficDirectorHealthFailure(ctx, trafficDirectorHealthFailureRequest(94, "GPT-5", base, ""))
	require.Error(t, err, "repository accepts only already-normalized model identities")
	_, err = store.RecordTrafficDirectorHealthFailure(ctx, trafficDirectorHealthFailureRequest(94, "gpt-5", base, ""))
	require.NoError(t, err)
	_, err = store.RecordTrafficDirectorHealthFailure(ctx, trafficDirectorHealthFailureRequest(95, "gpt-5", base, ""))
	require.NoError(t, err)

	first, err := store.CheckTrafficDirectorHealth(ctx, trafficDirectorHealthCheckRequest(94, "gpt-5", base.Add(time.Second), true, "probe-94"))
	require.NoError(t, err)
	second, err := store.CheckTrafficDirectorHealth(ctx, trafficDirectorHealthCheckRequest(95, "gpt-5", base.Add(time.Second), true, "probe-95"))
	require.NoError(t, err)
	require.Equal(t, service.TrafficDirectorHealthStateSuspect, first.State)
	require.Equal(t, service.TrafficDirectorHealthStateSuspect, second.State)
	require.False(t, first.ProbeAcquired)
	require.False(t, second.ProbeAcquired)
}

func trafficDirectorHealthFailureRequest(
	accountID int64,
	model string,
	now time.Time,
	probeToken string,
) service.TrafficDirectorHealthStoreFailureRequest {
	return service.TrafficDirectorHealthStoreFailureRequest{
		AccountID:        accountID,
		NormalizedModel:  model,
		Now:              now,
		ProbeToken:       probeToken,
		FailureStreakTTL: 30 * time.Minute,
		ShortOpen:        10 * time.Second,
		LongOpen:         45 * time.Second,
	}
}

func trafficDirectorHealthCheckRequest(
	accountID int64,
	model string,
	now time.Time,
	acquire bool,
	probeToken string,
) service.TrafficDirectorHealthStoreCheckRequest {
	return service.TrafficDirectorHealthStoreCheckRequest{
		AccountID:        accountID,
		NormalizedModel:  model,
		Now:              now,
		FailureStreakTTL: 30 * time.Minute,
		AcquireProbe:     acquire,
		ProbeToken:       probeToken,
		ProbeLease:       2 * time.Minute,
	}
}
