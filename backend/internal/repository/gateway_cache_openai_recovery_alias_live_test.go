package repository

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
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

func TestOpenAIRecoveryAliasRealRedisCapacityAndOOM(t *testing.T) {
	addr := os.Getenv("KKAI_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set KKAI_TEST_REDIS_ADDR to an empty isolated loopback Redis")
	}
	host, _, err := net.SplitHostPort(addr)
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", host)
	client := redis.NewClient(&redis.Options{Addr: addr, DB: 1, MaxRetries: 0})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	require.NoError(t, client.Ping(ctx).Err())
	count, err := client.DBSize(ctx).Result()
	require.NoError(t, err)
	require.Zero(t, count, "capacity test requires an empty dedicated Redis")
	policy, err := client.ConfigGet(ctx, "maxmemory-policy").Result()
	require.NoError(t, err)
	require.Equal(t, "noeviction", policy["maxmemory-policy"])
	limit, err := client.ConfigGet(ctx, "maxmemory").Result()
	require.NoError(t, err)
	require.Equal(t, "33554432", limit["maxmemory"], "capacity test requires a 32 MiB dedicated Redis")
	usedMemory := func() int64 {
		info, err := client.Info(ctx, "memory").Result()
		require.NoError(t, err)
		for _, line := range strings.Split(info, "\n") {
			if value, ok := strings.CutPrefix(strings.TrimSpace(line), "used_memory:"); ok {
				used, err := strconv.ParseInt(value, 10, 64)
				require.NoError(t, err)
				return used
			}
		}
		t.Fatal("Redis INFO memory omitted used_memory")
		return 0
	}
	baseline := usedMemory()
	metadata := []byte(strings.Repeat("m", openAIRecoveryAliasMaxMetadataBytes))
	cache := NewGatewayCache(client).(service.OpenAIRecoveryAliasCache)
	for i := 1; i <= 512; i++ {
		scope := fmt.Sprintf("%064x", i)
		alias := fmt.Sprintf("resp_lr_%036x", i)
		published, err := cache.PublishOpenAIRecoveryAlias(ctx, scope, alias, metadata, nil, time.Minute)
		require.NoError(t, err)
		require.True(t, published)
	}
	loaded := usedMemory()
	t.Logf("512 zero-snapshot aliases across distinct scopes: used_memory before=%d after=%d delta=%d bytes (%.0f per alias)", baseline, loaded, loaded-baseline, float64(loaded-baseline)/512)
	globalKey := openAIRecoveryAliasCachePrefix + "global:active"
	count, err = client.ZCard(ctx, globalKey).Result()
	require.NoError(t, err)
	require.EqualValues(t, 512, count)

	// The isolated instance may be reconfigured to force noeviction failure.
	require.NoError(t, client.ConfigSet(ctx, "maxmemory", strconv.FormatInt(loaded, 10)).Err())
	t.Cleanup(func() { require.NoError(t, client.ConfigSet(context.Background(), "maxmemory", "33554432").Err()) })
	scope := strings.Repeat("f", 64)
	alias := "resp_lr_" + strings.Repeat("f", 36)
	published, err := cache.PublishOpenAIRecoveryAlias(ctx, scope, alias, metadata, nil, time.Minute)
	require.Error(t, err)
	require.False(t, published)
	stored, _, readErr := cache.GetOpenAIRecoveryAlias(ctx, scope, alias)
	require.NoError(t, readErr)
	require.Empty(t, stored, "OOM must not leave an alias payload")
	count, err = client.ZCard(ctx, globalKey).Result()
	require.NoError(t, err)
	require.EqualValues(t, 512, count, "OOM must not leave an index entry")

	require.NoError(t, client.ConfigSet(ctx, "maxmemory", "33554432").Err())
	for _, margin := range []int64{64, 128, 256, 512, 1024, 2048, 4096} {
		scope := fmt.Sprintf("%064x", 1000+margin)
		alias := fmt.Sprintf("resp_lr_%036x", 1000+margin)
		before := usedMemory()
		require.NoError(t, client.ConfigSet(ctx, "maxmemory", strconv.FormatInt(before+margin, 10)).Err())
		published, publishErr := cache.PublishOpenAIRecoveryAlias(ctx, scope, alias, []byte(`{}`), nil, time.Minute)
		require.NoError(t, client.ConfigSet(ctx, "maxmemory", "33554432").Err())
		if publishErr != nil {
			require.False(t, published)
			exists, err := client.Exists(ctx, openAIRecoveryAliasCacheKey(scope, alias)).Result()
			require.NoError(t, err)
			require.Zero(t, exists, "OOM at %d bytes margin left a partial alias", margin)
			indexed, err := client.ZCard(ctx, globalKey).Result()
			require.NoError(t, err)
			require.EqualValues(t, 512, indexed, "OOM at %d bytes margin left an index entry", margin)
			continue
		}
		require.True(t, published)
		require.NoError(t, cache.DeleteOpenAIRecoveryAlias(ctx, scope, alias))
	}
	shortScope := strings.Repeat("d", 64)
	shortAlias := "resp_lr_" + strings.Repeat("d", 36)
	published, err = cache.PublishOpenAIRecoveryAlias(ctx, shortScope, shortAlias, []byte(`{}`), nil, 150*time.Millisecond)
	require.NoError(t, err)
	require.True(t, published)
	time.Sleep(200 * time.Millisecond)
	published, err = cache.PublishOpenAIRecoveryAlias(ctx, scope, alias, []byte(`{}`), nil, time.Minute)
	require.NoError(t, err)
	require.True(t, published)
	count, err = client.ZCard(ctx, globalKey).Result()
	require.NoError(t, err)
	require.EqualValues(t, 513, count, "expired alias must be purged before admitting a new one")
	_, err = client.ZScore(ctx, globalKey, openAIRecoveryAliasCacheKey(shortScope, shortAlias)).Result()
	require.ErrorIs(t, err, redis.Nil)
	blockedScope := strings.Repeat("e", 64)
	blockedAlias := "resp_lr_" + strings.Repeat("e", 36)
	blockedKey := openAIRecoveryAliasCacheKey(blockedScope, blockedAlias)
	keys := append([]string{blockedKey}, openAIRecoveryAliasIndexKeys(blockedScope)...)
	now := time.Now()
	result, err := publishOpenAIRecoveryAliasScript.Run(ctx, client, keys,
		[]byte(`{}`), "", time.Minute.Milliseconds(), now.Add(time.Minute).UnixMilli(), blockedKey, now.UnixMilli(),
		openAIRecoveryAliasScopeLimit, openAIRecoveryAliasScopeBytesLimit, openAIRecoveryAliasGlobalBytesLimit, count,
	).Int64()
	require.NoError(t, err)
	require.EqualValues(t, -1, result)
	exists, err := client.Exists(ctx, blockedKey).Result()
	require.NoError(t, err)
	require.Zero(t, exists, "global count rejection must not create a payload")
}
