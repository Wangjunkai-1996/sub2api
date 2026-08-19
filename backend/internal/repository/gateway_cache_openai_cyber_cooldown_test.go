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

func TestGatewayCacheOpenAICyberAccountCooldownStrike(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	store, ok := NewGatewayCache(client).(service.OpenAICyberAccountCooldownStore)
	require.True(t, ok)

	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0).UTC()
	window := 10 * time.Minute
	firstDuration := time.Minute
	escalatedDuration := 30 * time.Minute
	first, err := store.RecordOpenAICyberAccountCooldownStrike(ctx, 10616, "event-a", window, firstDuration, escalatedDuration, now)
	require.NoError(t, err)
	require.Equal(t, service.OpenAICyberAccountCooldownStrike{
		Strikes:              1,
		EventRecordedAt:      now,
		EventCooldownUntil:   now.Add(firstDuration),
		AccountCooldownUntil: now.Add(firstDuration),
	}, first)

	duplicate, err := store.RecordOpenAICyberAccountCooldownStrike(ctx, 10616, "event-a", window, firstDuration, escalatedDuration, now.Add(10*time.Second))
	require.NoError(t, err)
	require.Equal(t, service.OpenAICyberAccountCooldownStrike{
		Strikes:              1,
		Duplicate:            true,
		EventRecordedAt:      now,
		EventCooldownUntil:   now.Add(firstDuration),
		AccountCooldownUntil: now.Add(firstDuration),
	}, duplicate)

	secondAt := now.Add(30 * time.Second)
	second, err := store.RecordOpenAICyberAccountCooldownStrike(ctx, 10616, "event-b", window, firstDuration, escalatedDuration, secondAt)
	require.NoError(t, err)
	require.Equal(t, service.OpenAICyberAccountCooldownStrike{
		Strikes:              2,
		EventRecordedAt:      secondAt,
		EventCooldownUntil:   secondAt.Add(escalatedDuration),
		AccountCooldownUntil: secondAt.Add(escalatedDuration),
	}, second)

	// Repeating the first event after another strike must retain its original
	// tier/deadline while returning the later account-wide deadline.
	interleavedDuplicate, err := store.RecordOpenAICyberAccountCooldownStrike(ctx, 10616, "event-a", window, firstDuration, escalatedDuration, now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, service.OpenAICyberAccountCooldownStrike{
		Strikes:              1,
		Duplicate:            true,
		EventRecordedAt:      now,
		EventCooldownUntil:   now.Add(firstDuration),
		AccountCooldownUntil: secondAt.Add(escalatedDuration),
	}, interleavedDuplicate)

	deadline, err := store.GetOpenAICyberAccountCooldownDeadline(ctx, 10616)
	require.NoError(t, err)
	require.Equal(t, secondAt.Add(escalatedDuration), deadline)

	otherAccount, err := store.RecordOpenAICyberAccountCooldownStrike(ctx, 10617, "event-b", window, firstDuration, escalatedDuration, secondAt)
	require.NoError(t, err)
	require.Equal(t, 1, otherAccount.Strikes)
	require.Equal(t, secondAt.Add(firstDuration), otherAccount.AccountCooldownUntil)

	missing, err := store.GetOpenAICyberAccountCooldownDeadline(ctx, 99999)
	require.NoError(t, err)
	require.True(t, missing.IsZero())
}

func TestGatewayCacheOpenAICyberAccountCooldownTTLTracksLongerCooldown(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	store, ok := NewGatewayCache(client).(service.OpenAICyberAccountCooldownStore)
	require.True(t, ok)
	ctx := context.Background()
	now := time.Unix(1_800_000_000, 0).UTC()
	window := 2 * time.Minute
	firstDuration := time.Minute
	escalatedDuration := 10 * time.Minute

	_, err := store.RecordOpenAICyberAccountCooldownStrike(ctx, 10616, "event-a", window, firstDuration, escalatedDuration, now)
	require.NoError(t, err)
	_, err = store.RecordOpenAICyberAccountCooldownStrike(ctx, 10616, "event-b", window, firstDuration, escalatedDuration, now.Add(30*time.Second))
	require.NoError(t, err)

	stateKey, eventsKey, _ := openAICyberAccountCooldownKeys(10616, "event-a")
	require.Equal(t, escalatedDuration, redisServer.TTL(stateKey))
	require.Equal(t, escalatedDuration, redisServer.TTL(eventsKey))

	redisServer.FastForward(window + time.Second)
	deadline, err := store.GetOpenAICyberAccountCooldownDeadline(ctx, 10616)
	require.NoError(t, err)
	require.Equal(t, now.Add(30*time.Second).Add(escalatedDuration), deadline)

	duplicate, err := store.RecordOpenAICyberAccountCooldownStrike(ctx, 10616, "event-a", window, firstDuration, escalatedDuration, now.Add(window+time.Second))
	require.NoError(t, err)
	require.True(t, duplicate.Duplicate, "event dedupe must outlive the shorter strike window")
	require.Equal(t, 1, duplicate.Strikes)
}

func TestGatewayCacheOpenAICyberAccountCooldownRejectsInvalidInput(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	store, ok := NewGatewayCache(client).(service.OpenAICyberAccountCooldownStore)
	require.True(t, ok)

	_, err := store.RecordOpenAICyberAccountCooldownStrike(context.Background(), 0, "", 0, 0, 0, time.Now())
	require.Error(t, err)
}
