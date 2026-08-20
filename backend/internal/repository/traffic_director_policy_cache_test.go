package repository

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestTrafficDirectorPolicyRedisCache_RoundTrip(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	cache := NewTrafficDirectorPolicyRedisCache(client)
	policy := trafficDirectorPolicyRedisTestVersion(61, 7, "checksum-7")

	require.NoError(t, cache.SetTrafficDirectorPolicyVersion(context.Background(), &policy, 2*time.Hour))
	loaded, err := cache.GetTrafficDirectorPolicyVersion(context.Background(), 61, 7)
	require.NoError(t, err)
	require.Equal(t, policy, *loaded)
	require.Equal(t, 2*time.Hour, server.TTL(trafficDirectorPolicyRedisKey(61, 7)))
	require.Equal(t, "traffic_director:policy:v1:{61}:7", trafficDirectorPolicyRedisKey(61, 7))
}

func TestTrafficDirectorPolicyRedisCache_Miss(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	cache := NewTrafficDirectorPolicyRedisCache(client)

	loaded, err := cache.GetTrafficDirectorPolicyVersion(context.Background(), 62, 8)
	require.NoError(t, err)
	require.Nil(t, loaded)
}

func TestTrafficDirectorPolicyRedisCache_RejectsMalformedOrMismatchedPayload(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	cache := NewTrafficDirectorPolicyRedisCache(client)

	require.NoError(t, client.Set(
		context.Background(),
		trafficDirectorPolicyRedisKey(63, 9),
		`{"group_id":`,
		time.Hour,
	).Err())
	loaded, err := cache.GetTrafficDirectorPolicyVersion(context.Background(), 63, 9)
	require.Error(t, err)
	require.Nil(t, loaded)

	require.NoError(t, client.Set(
		context.Background(),
		trafficDirectorPolicyRedisKey(63, 10),
		`{"group_id":999,"version":10,"mode":"legacy","checksum":"x"}`,
		time.Hour,
	).Err())
	loaded, err = cache.GetTrafficDirectorPolicyVersion(context.Background(), 63, 10)
	require.Error(t, err)
	require.Nil(t, loaded)
}

func TestTrafficDirectorPolicyRedisCache_KeysIsolateGroupAndImmutableVersion(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	cache := NewTrafficDirectorPolicyRedisCache(client)
	policies := []service.TrafficDirectorVersion{
		trafficDirectorPolicyRedisTestVersion(64, 1, "group-64-version-1"),
		trafficDirectorPolicyRedisTestVersion(64, 2, "group-64-version-2"),
		trafficDirectorPolicyRedisTestVersion(65, 1, "group-65-version-1"),
	}
	for index := range policies {
		require.NoError(t, cache.SetTrafficDirectorPolicyVersion(context.Background(), &policies[index], time.Hour))
	}

	for _, expected := range policies {
		loaded, err := cache.GetTrafficDirectorPolicyVersion(
			context.Background(),
			expected.GroupID,
			expected.Version,
		)
		require.NoError(t, err)
		require.Equal(t, expected.Checksum, loaded.Checksum)
		require.Equal(t, expected.GroupID, loaded.GroupID)
		require.Equal(t, expected.Version, loaded.Version)
	}
	require.Len(t, server.Keys(), 3)
}

func trafficDirectorPolicyRedisTestVersion(
	groupID int64,
	version int64,
	checksum string,
) service.TrafficDirectorVersion {
	return service.TrafficDirectorVersion{
		GroupID:  groupID,
		Version:  version,
		Mode:     domain.TrafficDirectorModeEnforced,
		Checksum: checksum,
		Spec: &domain.TrafficDirectorSpec{
			SchemaVersion: domain.TrafficDirectorSchemaVersion,
			HealthMode:    domain.TrafficDirectorHealthModeOff,
			Pools: []domain.TrafficDirectorPool{
				{
					Key:          "primary",
					WeightBPS:    domain.TrafficDirectorWeightTotalBPS,
					AccountIDs:   []int64{101},
					MinAvailable: 1,
				},
			},
		},
	}
}
