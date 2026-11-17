//go:build integration

package repository

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"net/url"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/stretchr/testify/require"
)

func TestV0220MigrationsUpgradePreservesKKAILedgerAndOldRunner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	db := newV0220MigrationDatabase(t, ctx)
	added := map[string]bool{
		"247_scoped_admin_api_keys.sql":     true,
		"248_group_image_count_billing.sql": true,
		"249_mcp_oauth.sql":                 true,
	}
	legacy := fstest.MapFS{}
	names, err := fs.Glob(migrations.FS, "*.sql")
	require.NoError(t, err)
	for _, name := range names {
		if added[name] {
			continue
		}
		data, err := fs.ReadFile(migrations.FS, name)
		require.NoError(t, err)
		legacy[name] = &fstest.MapFile{Data: data}
	}
	require.NoError(t, applyIntegrationMigrationsFS(ctx, db, legacy))
	before := readV0220MigrationLedger(t, ctx, db)
	for _, name := range []string{
		"247_affiliate_ledger_operation_id.sql", "248_account_egress_direct_default.sql",
		"249_retire_kkai_fork_features.sql", "255_enable_gemini_image_generation_groups.sql",
	} {
		require.Contains(t, before, name)
	}
	for name := range added {
		require.NotContains(t, before, name)
	}
	var groupID int64
	require.NoError(t, db.QueryRowContext(ctx,
		`INSERT INTO groups (name, platform) VALUES ('v0220-before', 'openai') RETURNING id`).Scan(&groupID))

	require.NoError(t, ApplyMigrations(ctx, db))
	after := readV0220MigrationLedger(t, ctx, db)
	require.Len(t, after, len(before)+len(added))
	for name, entry := range before {
		require.Equal(t, entry, after[name], "existing checksum/applied_at changed: %s", name)
	}
	for name := range added {
		require.Contains(t, after, name)
	}
	// TestMain initialized the shared fresh database from the complete,
	// interleaved file set; both orders must record the same checksums.
	fresh := readV0220MigrationLedger(t, ctx, integrationDB)
	for name, entry := range after {
		require.Contains(t, fresh, name)
		require.Equal(t, entry.checksum, fresh[name].checksum, name)
	}
	for _, table := range []string{"scoped_admin_api_keys", "mcp_oauth_grants", "mcp_oauth_codes", "mcp_oauth_credentials"} {
		var exists bool
		require.NoError(t, db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+table).Scan(&exists))
		require.True(t, exists, table)
	}
	var enabled bool
	require.NoError(t, db.QueryRowContext(ctx, `SELECT image_count_billing FROM groups WHERE id = $1`, groupID).Scan(&enabled))
	require.False(t, enabled, "existing groups must retain opt-in billing")
	require.NoError(t, db.QueryRowContext(ctx,
		`INSERT INTO groups (name, platform) VALUES ('v0220-after', 'openai') RETURNING image_count_billing`).Scan(&enabled))
	require.False(t, enabled, "new groups must default to false")

	_, err = db.ExecContext(ctx, `UPDATE groups SET image_count_billing = true WHERE id = $1`, groupID)
	require.NoError(t, err)
	require.NoError(t, ApplyMigrations(ctx, db), "new runner is idempotent")
	// This exercises the unchanged runner with the old embedded file set. It
	// proves ledger compatibility, not old-binary business compatibility.
	require.NoError(t, applyMigrationsFS(ctx, db, legacy), "old runner must tolerate three unknown ledger entries")
	require.Equal(t, after, readV0220MigrationLedger(t, ctx, db))
	require.NoError(t, db.QueryRowContext(ctx, `SELECT image_count_billing FROM groups WHERE id = $1`, groupID).Scan(&enabled))
	require.True(t, enabled, "restarting either runner must preserve an explicit setting")
}

func newV0220MigrationDatabase(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	dsn, err := url.Parse(integrationPostgresDSN)
	require.NoError(t, err)
	require.NotEmpty(t, dsn.Host, "database must use the TestMain container DSN")
	name := fmt.Sprintf("sub2api_v0220_%d", time.Now().UnixNano())
	_, err = integrationDB.ExecContext(ctx, "CREATE DATABASE "+name)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := integrationDB.ExecContext(cleanupCtx, "DROP DATABASE "+name+" WITH (FORCE)")
		require.NoError(t, err)
	})
	dsn.Path = "/" + name
	db, err := sql.Open("postgres", dsn.String())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	require.NoError(t, db.PingContext(ctx))
	return db
}

type v0220MigrationLedgerEntry struct {
	checksum  string
	appliedAt string
}

func readV0220MigrationLedger(t *testing.T, ctx context.Context, db *sql.DB) map[string]v0220MigrationLedgerEntry {
	t.Helper()
	rows, err := db.QueryContext(ctx, `SELECT filename, checksum, applied_at::text FROM schema_migrations ORDER BY filename`)
	require.NoError(t, err)
	defer rows.Close()
	ledger := map[string]v0220MigrationLedgerEntry{}
	for rows.Next() {
		var name string
		var entry v0220MigrationLedgerEntry
		require.NoError(t, rows.Scan(&name, &entry.checksum, &entry.appliedAt))
		ledger[name] = entry
	}
	require.NoError(t, rows.Err())
	return ledger
}
