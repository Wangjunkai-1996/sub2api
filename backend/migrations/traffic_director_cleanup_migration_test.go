package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrafficDirectorCleanupMigration(t *testing.T) {
	content, err := FS.ReadFile("239_remove_traffic_director.sql")
	require.NoError(t, err)

	raw := string(content)
	sql := strings.Join(strings.Fields(raw), " ")
	require.Contains(t, sql, "SET LOCAL lock_timeout = '3s'")
	require.Contains(t, sql, "SET LOCAL statement_timeout = '30s'")

	functionStart := strings.Index(raw, "CREATE OR REPLACE FUNCTION enqueue_group_auth_cache_invalidation()")
	require.NotEqual(t, -1, functionStart)
	functionStartSQL := strings.Index(sql, "CREATE OR REPLACE FUNCTION enqueue_group_auth_cache_invalidation()")
	require.NotEqual(t, -1, functionStartSQL)
	functionEndOffset := strings.Index(raw[functionStart:], "\n$$;")
	require.NotEqual(t, -1, functionEndOffset)
	functionSQL := raw[functionStart : functionStart+functionEndOffset]

	require.Contains(t, functionSQL, "OLD.scheduler_type IS NOT DISTINCT FROM NEW.scheduler_type")
	require.Contains(t, functionSQL, "OLD.advanced_scheduler_overrides IS NOT DISTINCT FROM NEW.advanced_scheduler_overrides")
	require.Contains(t, functionSQL, "INSERT INTO auth_cache_invalidation_outbox (cache_key)")
	require.NotContains(t, functionSQL, "traffic_director_")

	dropTrigger := strings.Index(sql, "DROP TRIGGER IF EXISTS traffic_director_versions_immutable ON traffic_director_versions")
	dropTable := strings.Index(sql, "DROP TABLE IF EXISTS traffic_director_versions")
	dropFunction := strings.Index(sql, "DROP FUNCTION IF EXISTS prevent_traffic_director_version_mutation()")
	dropConstraints := strings.Index(sql, "DROP CONSTRAINT IF EXISTS groups_traffic_director_version_check")
	dropColumns := strings.Index(sql, "DROP COLUMN IF EXISTS traffic_director_mode")

	require.Greater(t, dropTrigger, functionStartSQL)
	require.Greater(t, dropTable, dropTrigger)
	require.Greater(t, dropFunction, dropTable)
	require.Greater(t, dropConstraints, dropFunction)
	require.Greater(t, dropColumns, dropConstraints)

	require.Contains(t, sql, "IF to_regclass('traffic_director_versions') IS NOT NULL THEN")
	require.Contains(t, sql, "DROP CONSTRAINT IF EXISTS groups_traffic_director_policy_check")
	require.Contains(t, sql, "DROP COLUMN IF EXISTS traffic_director_version")
	require.Contains(t, sql, "DROP COLUMN IF EXISTS traffic_director_spec")
	require.NotContains(t, strings.ToUpper(sql), " CASCADE")
}
