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
	first, err := store.RecordOpenAICyberAccountCooldownStrike(ctx, 10616, "event-a", 24*time.Hour, now)
	require.NoError(t, err)
	require.Equal(t, service.OpenAICyberAccountCooldownStrike{Strikes: 1}, first)

	duplicate, err := store.RecordOpenAICyberAccountCooldownStrike(ctx, 10616, "event-a", 24*time.Hour, now.Add(time.Minute))
	require.NoError(t, err)
	require.Equal(t, service.OpenAICyberAccountCooldownStrike{Strikes: 1, Duplicate: true}, duplicate)

	second, err := store.RecordOpenAICyberAccountCooldownStrike(ctx, 10616, "event-b", 24*time.Hour, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.Equal(t, service.OpenAICyberAccountCooldownStrike{Strikes: 2}, second)

	otherAccount, err := store.RecordOpenAICyberAccountCooldownStrike(ctx, 10617, "event-b", 24*time.Hour, now.Add(2*time.Minute))
	require.NoError(t, err)
	require.Equal(t, service.OpenAICyberAccountCooldownStrike{Strikes: 1}, otherAccount)

	redisServer.FastForward(24 * time.Hour)
	reset, err := store.RecordOpenAICyberAccountCooldownStrike(ctx, 10616, "event-c", 24*time.Hour, now.Add(24*time.Hour+2*time.Minute))
	require.NoError(t, err)
	require.Equal(t, service.OpenAICyberAccountCooldownStrike{Strikes: 1}, reset)
}

func TestGatewayCacheOpenAICyberAccountCooldownRejectsInvalidInput(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	store := NewGatewayCache(client).(service.OpenAICyberAccountCooldownStore)

	_, err := store.RecordOpenAICyberAccountCooldownStrike(context.Background(), 0, "", 0, time.Now())
	require.Error(t, err)
}
