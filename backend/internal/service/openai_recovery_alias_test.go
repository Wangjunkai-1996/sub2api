package service

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type openAIRecoveryAliasTestCache struct {
	GatewayCache
	metadata []byte
	snapshot []byte
}

func (c *openAIRecoveryAliasTestCache) PublishOpenAIRecoveryAlias(_ context.Context, _, _ string, metadata, snapshot []byte, _ time.Duration) (bool, error) {
	if c.metadata != nil {
		return false, nil
	}
	c.metadata = append([]byte(nil), metadata...)
	c.snapshot = append([]byte(nil), snapshot...)
	return true, nil
}

func (c *openAIRecoveryAliasTestCache) GetOpenAIRecoveryAlias(context.Context, string, string) ([]byte, []byte, error) {
	return c.metadata, c.snapshot, nil
}

func (c *openAIRecoveryAliasTestCache) DeleteOpenAIRecoveryAlias(context.Context, string, string) error {
	c.metadata = nil
	c.snapshot = nil
	return nil
}

func TestOpenAIRecoveryAliasScopeRequiresVerifiedAttribution(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(4835))
	_, ok := OpenAIRecoveryScope(ctx, 123)
	require.False(t, ok)
	ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(27))
	ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "https://api.kkrich.ltd")
	scope, ok := OpenAIRecoveryScope(ctx, 123)
	require.True(t, ok)
	require.Len(t, scope, 64)
	other, ok := OpenAIRecoveryScope(ctx, 124)
	require.True(t, ok)
	assert.NotEqual(t, scope, other)
}

func TestOpenAIRecoveryAliasPublishesOneWinnerBeforeContinuation(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(4835))
	ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(27))
	ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "https://api.kkrich.ltd")
	scope, ok := OpenAIRecoveryScope(ctx, 123)
	require.True(t, ok)
	alias, err := NewOpenAIRecoveryAliasID()
	require.NoError(t, err)
	require.True(t, IsOpenAIRecoveryAliasID(alias))
	assert.True(t, strings.HasPrefix(alias, "resp_lr_"))

	svc := &OpenAIGatewayService{cache: &openAIRecoveryAliasTestCache{}}
	_, err = svc.ResolveOpenAIRecoveryAlias(ctx, scope, alias)
	require.ErrorIs(t, err, ErrOpenAIRecoveryAliasNotFound)

	winner := OpenAIRecoveryAliasWinner{ResponseID: "resp_actual", AccountID: 17016, RouteFingerprint: strings.Repeat("f", 64), Snapshot: []byte(`{"input":"complete"}`)}
	require.NoError(t, svc.PublishOpenAIRecoveryAlias(ctx, scope, alias, winner, time.Hour))
	resolved, err := svc.ResolveOpenAIRecoveryAlias(ctx, scope, alias)
	require.NoError(t, err)
	assert.Equal(t, winner.ResponseID, resolved.ResponseID)
	assert.Equal(t, winner.AccountID, resolved.AccountID)
	assert.Equal(t, winner.RouteFingerprint, resolved.RouteFingerprint)
	assert.Equal(t, winner.Snapshot, resolved.Snapshot)
	assert.Equal(t, "strip_old_echo", resolved.TurnStatePolicy)
	require.ErrorIs(t, svc.PublishOpenAIRecoveryAlias(ctx, scope, alias, winner, time.Hour), ErrOpenAIRecoveryAliasConflict)

	otherScope := strings.Repeat("a", 64)
	if otherScope == scope {
		otherScope = strings.Repeat("b", 64)
	}
	_, err = svc.ResolveOpenAIRecoveryAlias(ctx, otherScope, alias)
	require.ErrorIs(t, err, ErrOpenAIRecoveryAliasInvalid)
	require.NoError(t, svc.DeleteOpenAIRecoveryAlias(ctx, scope, alias))
	_, err = svc.ResolveOpenAIRecoveryAlias(ctx, scope, alias)
	require.True(t, errors.Is(err, ErrOpenAIRecoveryAliasNotFound))
}
