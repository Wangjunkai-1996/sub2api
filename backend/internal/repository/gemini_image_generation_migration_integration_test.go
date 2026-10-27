//go:build integration

package repository

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	dbmigrations "github.com/Wei-Shaw/sub2api/migrations"
	"github.com/stretchr/testify/require"
)

func TestGeminiImageGenerationMigrationOnlyEnablesDedicatedGroups(t *testing.T) {
	// The shared harness supplies the real schema and invalidation triggers;
	// every fixture, migration update and outbox entry rolls back with this tx.
	tx := testTx(t)
	ctx := context.Background()
	var userID int64
	require.NoError(t, tx.QueryRowContext(ctx, `
INSERT INTO users (email, password_hash)
VALUES ('migration-255@example.com', 'test-password-hash') RETURNING id`).Scan(&userID))

	type groupCase struct {
		id             int64
		name           string
		cacheKey       string
		wantEnabled    bool
		wantOutboxRows int
	}
	var groups []groupCase
	for _, platform := range []string{"gemini", "antigravity", "composite", "openai", "anthropic", "grok"} {
		for _, enabled := range []bool{false, true} {
			g := groupCase{
				name:        fmt.Sprintf("migration-255-%s-%t", platform, enabled),
				wantEnabled: enabled || platform == "gemini" || platform == "antigravity",
			}
			if !enabled && g.wantEnabled {
				g.wantOutboxRows = 1
			}
			require.NoError(t, tx.QueryRowContext(ctx, `
INSERT INTO groups (name, platform, allow_image_generation)
VALUES ($1, $2, $3) RETURNING id`, g.name, platform, enabled).Scan(&g.id))
			key := fmt.Sprintf("sk-migration-255-%d", g.id)
			_, err := tx.ExecContext(ctx, `
INSERT INTO api_keys (user_id, key, name, group_id)
VALUES ($1, $2, 'migration-255', $3)`, userID, key, g.id)
			require.NoError(t, err)
			g.cacheKey = fmt.Sprintf("%x", sha256.Sum256([]byte(key)))
			groups = append(groups, g)
		}
	}

	var initialOutboxRows int
	require.NoError(t, tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM auth_cache_invalidation_outbox o
JOIN api_keys k ON o.cache_key = encode(sha256(convert_to(k.key, 'UTF8')), 'hex')
WHERE k.user_id = $1`, userID).Scan(&initialOutboxRows))
	require.Zero(t, initialOutboxRows, "fixtures must not supply the invalidations being tested")

	var expectedUpdates int64
	require.NoError(t, tx.QueryRowContext(ctx, `
SELECT COUNT(*) FROM groups
WHERE platform IN ('gemini', 'antigravity') AND allow_image_generation = false`).Scan(&expectedUpdates))
	require.GreaterOrEqual(t, expectedUpdates, int64(2))
	migrationSQL, err := dbmigrations.FS.ReadFile("255_enable_gemini_image_generation_groups.sql")
	require.NoError(t, err)
	for pass := range 2 {
		result, err := tx.ExecContext(ctx, string(migrationSQL))
		require.NoError(t, err)
		updated, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, expectedUpdates, updated, "migration pass %d", pass+1)
		expectedUpdates = 0
		for _, g := range groups {
			var enabled bool
			var outboxRows int
			require.NoError(t, tx.QueryRowContext(ctx,
				"SELECT allow_image_generation FROM groups WHERE id = $1", g.id).Scan(&enabled))
			require.Equal(t, g.wantEnabled, enabled, g.name)
			require.NoError(t, tx.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM auth_cache_invalidation_outbox WHERE cache_key = $1", g.cacheKey).Scan(&outboxRows))
			require.Equal(t, g.wantOutboxRows, outboxRows, "%s: only changed groups invalidate their bound key, once", g.name)
		}
	}
}
