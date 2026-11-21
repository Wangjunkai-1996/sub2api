package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGroupSchedulerOverridesMigration(t *testing.T) {
	content, err := FS.ReadFile("227_group_scheduler_overrides.sql")
	require.NoError(t, err)

	sql := strings.Join(strings.Fields(string(content)), " ")
	require.Contains(t, sql, "scheduler_type VARCHAR(16) NOT NULL DEFAULT 'inherit'")
	require.Contains(t, sql, "CHECK (scheduler_type IN ('inherit', 'basic', 'advanced'))")
	require.Contains(t, sql, "advanced_scheduler_overrides JSONB NOT NULL DEFAULT '{}'::jsonb")
	require.Contains(t, sql, "CHECK (jsonb_typeof(advanced_scheduler_overrides) = 'object')")
	require.Contains(t, sql, "CREATE OR REPLACE FUNCTION enqueue_group_auth_cache_invalidation()")
	require.Contains(t, sql, "OLD.scheduler_type IS NOT DISTINCT FROM NEW.scheduler_type")
	require.Contains(t, sql, "OLD.advanced_scheduler_overrides IS NOT DISTINCT FROM NEW.advanced_scheduler_overrides")
	require.Contains(t, sql, "INSERT INTO auth_cache_invalidation_outbox (cache_key)")
}
