package repository

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net"
	"os"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAIRecoveryAliasRealRedisRoundTrip(t *testing.T) {
	addr := os.Getenv("KKAI_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set KKAI_TEST_REDIS_ADDR to an isolated loopback Redis")
	}
	host, _, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", host, "live alias test must not contact a remote Redis")

	client := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, client.Ping(ctx).Err())
	policy, err := client.ConfigGet(ctx, "maxmemory-policy").Result()
	require.NoError(t, err)
	require.Equal(t, "noeviction", policy["maxmemory-policy"])

	var randomScope [32]byte
	_, err = rand.Read(randomScope[:])
	require.NoError(t, err)
	scope := hex.EncodeToString(randomScope[:])
	alias, err := service.NewOpenAIRecoveryAliasID()
	require.NoError(t, err)
	cache := NewGatewayCache(client).(service.OpenAIRecoveryAliasCache)
	t.Cleanup(func() { require.NoError(t, cache.DeleteOpenAIRecoveryAlias(context.Background(), scope, alias)) })

	metadata := []byte(`{"state":"published"}`)
	snapshot := []byte(`{"input":"complete"}`)
	published, err := cache.PublishOpenAIRecoveryAlias(ctx, scope, alias, metadata, snapshot, time.Minute)
	require.NoError(t, err)
	require.True(t, published)
	published, err = cache.PublishOpenAIRecoveryAlias(ctx, scope, alias, []byte(`{"state":"overwritten"}`), nil, time.Minute)
	require.NoError(t, err)
	assert.False(t, published)
	storedMetadata, storedSnapshot, err := cache.GetOpenAIRecoveryAlias(ctx, scope, alias)
	require.NoError(t, err)
	assert.Equal(t, metadata, storedMetadata)
	assert.Equal(t, snapshot, storedSnapshot)

	require.NoError(t, cache.DeleteOpenAIRecoveryAlias(ctx, scope, alias))
	storedMetadata, storedSnapshot, err = cache.GetOpenAIRecoveryAlias(ctx, scope, alias)
	require.NoError(t, err)
	assert.Empty(t, storedMetadata)
	assert.Empty(t, storedSnapshot)
}

func TestOpenAIRecoveryAliasDisconnectedRedisFailsClosed(t *testing.T) {
	addr := os.Getenv("KKAI_TEST_REDIS_DOWN_ADDR")
	if addr == "" {
		t.Skip("set KKAI_TEST_REDIS_DOWN_ADDR to a stopped isolated loopback Redis")
	}
	host, _, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", host, "outage test must not contact a remote Redis")

	client := redis.NewClient(&redis.Options{Addr: addr, DialTimeout: 200 * time.Millisecond, MaxRetries: 0})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	cache := NewGatewayCache(client).(service.OpenAIRecoveryAliasCache)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	scope := "a69f122452e25a6292b3de8bf043291bb127d4e4d282e07cbb46c73a52732b7a"
	alias := "resp_lr_11223344556677889900aabbccddeeff0011"
	published, err := cache.PublishOpenAIRecoveryAlias(ctx, scope, alias, []byte(`{"state":"published"}`), []byte(`{"input":"complete"}`), time.Minute)
	require.Error(t, err)
	assert.False(t, published)
	metadata, snapshot, err := cache.GetOpenAIRecoveryAlias(ctx, scope, alias)
	require.Error(t, err)
	assert.Empty(t, metadata)
	assert.Empty(t, snapshot)
}
