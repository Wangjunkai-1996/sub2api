package repository

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAIRecoveryAliasCachePublishesAtomicallyAndExpires(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	cache, ok := NewGatewayCache(client).(service.OpenAIRecoveryAliasCache)
	require.True(t, ok)
	ctx := context.Background()
	scope := strings.Repeat("a", 64)
	otherScope := strings.Repeat("b", 64)
	alias := "resp_lr_" + strings.Repeat("c", 36)

	metadata, snapshot, err := cache.GetOpenAIRecoveryAlias(ctx, scope, alias)
	require.NoError(t, err)
	assert.Empty(t, metadata)
	assert.Empty(t, snapshot)

	published, err := cache.PublishOpenAIRecoveryAlias(ctx, scope, alias, []byte(`{"state":"published"}`), []byte(`{"input":"complete"}`), time.Hour)
	require.NoError(t, err)
	require.True(t, published)
	published, err = cache.PublishOpenAIRecoveryAlias(ctx, scope, alias, []byte(`{"state":"wrong"}`), nil, time.Hour)
	require.NoError(t, err)
	assert.False(t, published)
	metadata, snapshot, err = cache.GetOpenAIRecoveryAlias(ctx, scope, alias)
	require.NoError(t, err)
	assert.JSONEq(t, `{"state":"published"}`, string(metadata))
	assert.JSONEq(t, `{"input":"complete"}`, string(snapshot))
	metadata, _, err = cache.GetOpenAIRecoveryAlias(ctx, otherScope, alias)
	require.NoError(t, err)
	assert.Empty(t, metadata)
	secondAlias := "resp_lr_" + strings.Repeat("d", 36)
	metadata, _, err = cache.GetOpenAIRecoveryAlias(ctx, scope, secondAlias)
	require.NoError(t, err)
	assert.Empty(t, metadata)
	published, err = cache.PublishOpenAIRecoveryAlias(ctx, scope, secondAlias, []byte(`{"state":"published"}`), nil, time.Hour)
	require.NoError(t, err)
	require.True(t, published)
	metadata, _, err = cache.GetOpenAIRecoveryAlias(ctx, scope, secondAlias)
	require.NoError(t, err)
	assert.JSONEq(t, `{"state":"published"}`, string(metadata))

	server.FastForward(time.Hour + time.Second)
	metadata, _, err = cache.GetOpenAIRecoveryAlias(ctx, scope, alias)
	require.NoError(t, err)
	assert.Empty(t, metadata)
}

func TestOpenAIRecoveryAliasCacheCapacityAndDelete(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	cache, ok := NewGatewayCache(client).(service.OpenAIRecoveryAliasCache)
	require.True(t, ok)
	ctx := context.Background()
	scope := strings.Repeat("a", 64)
	for i := 0; i < openAIRecoveryAliasScopeLimit; i++ {
		alias := fmt.Sprintf("resp_lr_%036x", i)
		published, err := cache.PublishOpenAIRecoveryAlias(ctx, scope, alias, []byte(`{"state":"published"}`), nil, time.Hour)
		require.NoError(t, err)
		require.True(t, published)
	}
	blockedAlias := "resp_lr_" + strings.Repeat("b", 36)
	published, err := cache.PublishOpenAIRecoveryAlias(ctx, scope, blockedAlias, []byte(`{"state":"published"}`), nil, time.Hour)
	require.ErrorIs(t, err, service.ErrOpenAIRecoveryAliasCapacity)
	assert.False(t, published)
	metadata, _, err := cache.GetOpenAIRecoveryAlias(ctx, scope, blockedAlias)
	require.NoError(t, err)
	assert.Empty(t, metadata)

	firstAlias := fmt.Sprintf("resp_lr_%036x", 0)
	require.NoError(t, cache.DeleteOpenAIRecoveryAlias(ctx, scope, firstAlias))
	published, err = cache.PublishOpenAIRecoveryAlias(ctx, scope, blockedAlias, []byte(`{"state":"published"}`), nil, time.Hour)
	require.NoError(t, err)
	assert.True(t, published)
}

func TestOpenAIRecoveryAliasCacheSnapshotByteLimits(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	ctx := context.Background()
	scope := strings.Repeat("a", 64)
	keys := append([]string{openAIRecoveryAliasCacheKey(scope, "resp_lr_"+strings.Repeat("a", 36))}, openAIRecoveryAliasIndexKeys(scope)...)
	for i, test := range []struct {
		alias  string
		size   int
		scope  int
		global int
		want   int64
	}{
		{"resp_lr_" + strings.Repeat("a", 36), 5, 8, 10, 1},
		{"resp_lr_" + strings.Repeat("b", 36), 4, 8, 10, -1},
		{"resp_lr_" + strings.Repeat("c", 36), 3, 8, 10, 1},
		{"resp_lr_" + strings.Repeat("d", 36), 3, 12, 10, -1},
	} {
		aliasKey := openAIRecoveryAliasCacheKey(scope, test.alias)
		keys[0] = aliasKey
		result, err := publishOpenAIRecoveryAliasScript.Run(ctx, client, keys, []byte(`{}`), strings.Repeat("x", test.size), time.Hour.Milliseconds(), time.Now().Add(time.Hour).UnixMilli(), aliasKey, time.Now().UnixMilli(), openAIRecoveryAliasScopeLimit, test.scope, test.global).Int64()
		require.NoError(t, err, "case %d", i)
		assert.Equal(t, test.want, result, "case %d", i)
	}
	// Deleting a published snapshot releases both scope and global byte quota.
	aliasKey := openAIRecoveryAliasCacheKey(scope, "resp_lr_"+strings.Repeat("a", 36))
	keys[0] = aliasKey
	require.NoError(t, deleteOpenAIRecoveryAliasScript.Run(ctx, client, keys, aliasKey).Err())
	aliasKey = openAIRecoveryAliasCacheKey(scope, "resp_lr_"+strings.Repeat("d", 36))
	keys[0] = aliasKey
	result, err := publishOpenAIRecoveryAliasScript.Run(ctx, client, keys, []byte(`{}`), "xxx", time.Hour.Milliseconds(), time.Now().Add(time.Hour).UnixMilli(), aliasKey, time.Now().UnixMilli(), openAIRecoveryAliasScopeLimit, 8, 10).Int64()
	require.NoError(t, err)
	assert.EqualValues(t, 1, result)
}

func TestOpenAIRecoveryAliasCacheConcurrentSnapshotLimit(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	cache, ok := NewGatewayCache(client).(service.OpenAIRecoveryAliasCache)
	require.True(t, ok)
	ctx := context.Background()
	leaseKey := openAIRecoveryAliasCachePrefix + "global:writes"
	for i := 0; i < 32; i++ {
		require.NoError(t, client.ZAdd(ctx, leaseKey, redis.Z{Score: float64(time.Now().Add(time.Minute).UnixMilli()), Member: fmt.Sprint(i)}).Err())
	}
	alias := "resp_lr_" + strings.Repeat("a", 36)
	published, err := cache.PublishOpenAIRecoveryAlias(ctx, strings.Repeat("a", 64), alias, []byte(`{}`), []byte(`{}`), time.Hour)
	require.ErrorIs(t, err, service.ErrOpenAIRecoveryAliasCapacity)
	assert.False(t, published)
	require.NoError(t, client.ZRem(ctx, leaseKey, "0").Err())
	published, err = cache.PublishOpenAIRecoveryAlias(ctx, strings.Repeat("a", 64), alias, []byte(`{}`), []byte(`{}`), time.Hour)
	require.NoError(t, err)
	assert.True(t, published)
	count, err := client.ZCard(ctx, leaseKey).Result()
	require.NoError(t, err)
	assert.EqualValues(t, 31, count)
}
