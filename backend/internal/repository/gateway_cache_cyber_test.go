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

func TestGatewayCacheCyberBlockUsesOnlyExactKeys(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	store, ok := NewGatewayCache(client).(service.CyberSessionBlockStore)
	require.True(t, ok)
	ctx := context.Background()

	require.NoError(t, client.Set(ctx, "cyber_session_scope:old-shared-scope", "1", time.Minute).Err())
	require.NoError(t, client.Set(ctx, cyberSessionBlockPrefix+"old-transcript", "1", time.Minute).Err())
	require.NoError(t, store.SetCyberSessionBlocked(ctx, "new-exact-session", time.Minute))
	blocked, err := store.FindCyberSessionBlocked(ctx, "new-exact-session")
	require.NoError(t, err)
	require.True(t, blocked)
	require.Greater(t, server.TTL(cyberSessionBlockPrefix+"new-exact-session"), time.Duration(0))

	blocked, err = store.FindCyberSessionBlocked(ctx, "unrelated-session")
	require.NoError(t, err)
	require.False(t, blocked)
	blocked, err = store.FindCyberSessionBlocked(ctx, "")
	require.NoError(t, err)
	require.False(t, blocked)
}
