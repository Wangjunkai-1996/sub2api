package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestGatewayCacheCompareAndDeleteSessionAccountIDIsConditional(t *testing.T) {
	redisServer := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	cache := NewGatewayCache(client)
	conditional, ok := cache.(service.GatewaySessionBindingCompareDeleter)
	require.True(t, ok)

	ctx := context.Background()
	const groupID int64 = 17
	const sessionHash = "openai:session-cas"
	require.NoError(t, cache.SetSessionAccountID(ctx, groupID, sessionHash, 101, time.Minute))

	deleted, err := conditional.CompareAndDeleteSessionAccountID(ctx, groupID, sessionHash, 202)
	require.NoError(t, err)
	require.False(t, deleted)
	accountID, err := cache.GetSessionAccountID(ctx, groupID, sessionHash)
	require.NoError(t, err)
	require.Equal(t, int64(101), accountID)

	deleted, err = conditional.CompareAndDeleteSessionAccountID(ctx, groupID, sessionHash, 101)
	require.NoError(t, err)
	require.True(t, deleted)
	_, err = cache.GetSessionAccountID(ctx, groupID, sessionHash)
	require.Error(t, err)
	require.True(t, errors.Is(err, service.ErrStickySessionNotFound))
}
