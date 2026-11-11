//go:build integration

package repository

import (
	"context"
	"database/sql"
	"io/fs"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/stretchr/testify/require"
)

func TestV0221MigrationsUpgradePreservesLedgerDataAndDefaults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	db := newV0220MigrationDatabase(t, ctx)
	added := map[string]bool{
		"249_add_usage_logs_api_key_cost_index_notx.sql": true,
		"250_openai_cache_write_ttl.sql":                 true,
	}
	legacy := fstest.MapFS{}
	names, err := fs.Glob(migrations.FS, "*.sql")
	require.NoError(t, err)
	for _, name := range names {
		if !added[name] {
			data, err := fs.ReadFile(migrations.FS, name)
			require.NoError(t, err)
			legacy[name] = &fstest.MapFile{Data: data}
		}
	}
	require.Len(t, legacy, 320, "the upgrade fixture must represent the v0.2.20 KKAI ledger")
	require.NoError(t, applyIntegrationMigrationsFS(ctx, db, legacy))
	beforeLedger := readV0220MigrationLedger(t, ctx, db)
	require.Len(t, beforeLedger, 320)
	for _, name := range []string{
		"249_mcp_oauth.sql", "249_retire_kkai_fork_features.sql",
		"250_purge_retired_kkai_fork_data.sql", "255_enable_gemini_image_generation_groups.sql",
	} {
		require.Contains(t, beforeLedger, name, "same numeric prefixes must remain distinct ledger entries")
	}
	_, err = db.ExecContext(ctx, `
INSERT INTO users (email, password_hash, balance) VALUES ('v0221@example.com', 'synthetic', 123.456);
INSERT INTO accounts (name, platform, type) VALUES ('v0221', 'openai', 'apikey');
INSERT INTO api_keys (user_id, key, name)
SELECT id, 'synthetic-v0221-key', 'v0221' FROM users WHERE email = 'v0221@example.com';
INSERT INTO usage_logs (user_id, api_key_id, account_id, request_id, model,
    input_tokens, cache_creation_tokens, total_cost, actual_cost)
SELECT k.user_id, k.id, a.id, 'v0221-before', 'gpt-6-luna', 7, 19, 0.15, 0.25
FROM api_keys k CROSS JOIN accounts a WHERE k.name = 'v0221' AND a.name = 'v0221';
INSERT INTO channels (name) VALUES ('v0221');
INSERT INTO channel_model_pricing (channel_id, models, cache_write_price)
SELECT id, '["gpt-6-luna"]', 0.0000008125 FROM channels WHERE name = 'v0221';
INSERT INTO channel_pricing_intervals (pricing_id, cache_write_price)
SELECT p.id, 0.00000125 FROM channel_model_pricing p JOIN channels c ON c.id = p.channel_id WHERE c.name = 'v0221';
INSERT INTO channel_account_stats_pricing_rules (channel_id, name)
SELECT id, 'v0221' FROM channels WHERE name = 'v0221';
INSERT INTO channel_account_stats_model_pricing (rule_id, models, cache_write_price)
SELECT id, '["gpt-6-luna"]', 0.00000075 FROM channel_account_stats_pricing_rules WHERE name = 'v0221';
INSERT INTO channel_account_stats_pricing_intervals (pricing_id, cache_write_price)
SELECT p.id, 0.0000015 FROM channel_account_stats_model_pricing p
JOIN channel_account_stats_pricing_rules r ON r.id = p.rule_id WHERE r.name = 'v0221';`)
	require.NoError(t, err)
	beforeData := v0221MigrationDataSnapshot(t, ctx, db)

	require.NoError(t, ApplyMigrations(ctx, db))
	afterLedger := readV0220MigrationLedger(t, ctx, db)
	require.Len(t, afterLedger, 322)
	for name, entry := range beforeLedger {
		require.Equal(t, entry, afterLedger[name], "historical checksum or applied_at changed: %s", name)
	}
	freshLedger := readV0220MigrationLedger(t, ctx, integrationDB)
	for name, entry := range afterLedger {
		require.Contains(t, freshLedger, name)
		require.Equal(t, entry.checksum, freshLedger[name].checksum, name)
	}
	require.Equal(t, beforeData, v0221MigrationDataSnapshot(t, ctx, db), "migration must preserve old row values")
	var defaultsOK bool
	require.NoError(t, db.QueryRowContext(ctx, `SELECT bool_and(cache_creation_30m_tokens = 0 AND cache_creation_12h_tokens = 0) FROM usage_logs`).Scan(&defaultsOK))
	require.True(t, defaultsOK)
	pricingTables := []string{
		"channel_model_pricing", "channel_pricing_intervals",
		"channel_account_stats_model_pricing", "channel_account_stats_pricing_intervals",
	}
	for _, table := range pricingTables {
		require.NoError(t, db.QueryRowContext(ctx, "SELECT bool_and(cache_write_12h_price IS NULL) FROM "+table).Scan(&defaultsOK))
		require.True(t, defaultsOK, table)
	}
	indexID := requireV0221CostIndex(t, ctx, db)

	// Old inserts omit the new columns and must continue to work after upgrade.
	require.NoError(t, db.QueryRowContext(ctx, `
INSERT INTO usage_logs (user_id, api_key_id, account_id, request_id, model)
SELECT user_id, api_key_id, account_id, 'v0221-after', model FROM usage_logs WHERE request_id = 'v0221-before'
RETURNING cache_creation_30m_tokens = 0 AND cache_creation_12h_tokens = 0`).Scan(&defaultsOK))
	require.True(t, defaultsOK)
	for _, row := range []struct{ table, parent string }{
		{"channel_model_pricing", "channel_id"}, {"channel_pricing_intervals", "pricing_id"},
		{"channel_account_stats_model_pricing", "rule_id"}, {"channel_account_stats_pricing_intervals", "pricing_id"},
	} {
		require.NoError(t, db.QueryRowContext(ctx, "INSERT INTO "+row.table+" ("+row.parent+") SELECT "+row.parent+" FROM "+row.table+" ORDER BY id LIMIT 1 RETURNING cache_write_12h_price IS NULL").Scan(&defaultsOK))
		require.True(t, defaultsOK, row.table)
	}
	_, err = db.ExecContext(ctx, `UPDATE usage_logs SET cache_creation_30m_tokens = 3, cache_creation_12h_tokens = 16 WHERE request_id = 'v0221-before'`)
	require.NoError(t, err)
	for _, table := range pricingTables {
		_, err = db.ExecContext(ctx, "UPDATE "+table+" SET cache_write_12h_price = 0.00000325")
		require.NoError(t, err)
	}
	require.NoError(t, ApplyMigrations(ctx, db), "completed migrations must be idempotent")
	// This proves the old embedded file set is tolerated, not old-binary business compatibility.
	require.NoError(t, applyMigrationsFS(ctx, db, legacy))
	require.Equal(t, afterLedger, readV0220MigrationLedger(t, ctx, db))
	require.Equal(t, indexID, requireV0221CostIndex(t, ctx, db), "rerun must retain the valid index")
	require.NoError(t, db.QueryRowContext(ctx, `SELECT cache_creation_30m_tokens = 3 AND cache_creation_12h_tokens = 16 FROM usage_logs WHERE request_id = 'v0221-before'`).Scan(&defaultsOK))
	require.True(t, defaultsOK)
	for _, table := range pricingTables {
		require.NoError(t, db.QueryRowContext(ctx, "SELECT bool_and(cache_write_12h_price = 0.00000325) FROM "+table).Scan(&defaultsOK))
		require.True(t, defaultsOK, "rerun must retain explicit prices: %s", table)
	}
}

func TestV0221CostIndexMigrationRepairsInvalidIndex(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	db := newV0220MigrationDatabase(t, ctx)
	_, err := db.ExecContext(ctx, `
CREATE TABLE usage_logs (api_key_id BIGINT, created_at TIMESTAMPTZ, actual_cost NUMERIC(20,10));
INSERT INTO usage_logs VALUES (1, '2026-11-11T00:00:00Z', 0.1), (1, '2026-11-11T00:00:00Z', 0.2);`)
	require.NoError(t, err)
	// A failed concurrent build leaves the same invalid index state as an interrupted build.
	_, err = db.ExecContext(ctx, `CREATE UNIQUE INDEX CONCURRENTLY idx_usage_logs_api_key_created_cost ON usage_logs (api_key_id, created_at) INCLUDE (actual_cost)`)
	require.Error(t, err)
	var valid bool
	require.NoError(t, db.QueryRowContext(ctx, `SELECT indisvalid FROM pg_index WHERE indexrelid = 'idx_usage_logs_api_key_created_cost'::regclass`).Scan(&valid))
	require.False(t, valid)
	data, err := fs.ReadFile(migrations.FS, usageLogsAPIKeyCostIndexMigration)
	require.NoError(t, err)
	oneMigration := fstest.MapFS{usageLogsAPIKeyCostIndexMigration: &fstest.MapFile{Data: data}}
	require.NoError(t, applyMigrationsFS(ctx, db, oneMigration))
	indexID := requireV0221CostIndex(t, ctx, db)
	ledger := readV0220MigrationLedger(t, ctx, db)
	require.Len(t, ledger, 1)
	require.Contains(t, ledger, usageLogsAPIKeyCostIndexMigration)
	require.NoError(t, applyMigrationsFS(ctx, db, oneMigration))
	require.Equal(t, ledger, readV0220MigrationLedger(t, ctx, db))
	require.Equal(t, indexID, requireV0221CostIndex(t, ctx, db))
	var count int
	require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM usage_logs`).Scan(&count))
	require.Equal(t, 2, count, "index repair must preserve duplicate business rows")
}

func v0221MigrationDataSnapshot(t *testing.T, ctx context.Context, db *sql.DB) map[string]string {
	t.Helper()
	snapshot := map[string]string{}
	for _, table := range []string{
		"users", "accounts", "api_keys", "usage_logs", "channels", "channel_model_pricing",
		"channel_pricing_intervals", "channel_account_stats_pricing_rules",
		"channel_account_stats_model_pricing", "channel_account_stats_pricing_intervals",
	} {
		var data string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT jsonb_agg(to_jsonb(r) - ARRAY['cache_creation_30m_tokens', 'cache_creation_12h_tokens', 'cache_write_12h_price'] ORDER BY id)::text FROM `+table+` r`).Scan(&data))
		snapshot[table] = data
	}
	return snapshot
}

func requireV0221CostIndex(t *testing.T, ctx context.Context, db *sql.DB) int64 {
	t.Helper()
	var id int64
	var valid, ready, unique bool
	var keys, attributes int
	var definition string
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT indexrelid::bigint, indisvalid, indisready, indisunique, indnkeyatts, indnatts, pg_get_indexdef(indexrelid)
FROM pg_index WHERE indexrelid = 'idx_usage_logs_api_key_created_cost'::regclass`).Scan(&id, &valid, &ready, &unique, &keys, &attributes, &definition))
	require.True(t, valid)
	require.True(t, ready)
	require.False(t, unique)
	require.Equal(t, 2, keys)
	require.Equal(t, 3, attributes)
	require.Contains(t, definition, "USING btree (api_key_id, created_at) INCLUDE (actual_cost)")
	return id
}
