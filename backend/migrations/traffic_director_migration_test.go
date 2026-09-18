package migrations

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrafficDirectorMigration(t *testing.T) {
	content, err := FS.ReadFile("228_traffic_director.sql")
	require.NoError(t, err)
	checksum := sha256.Sum256([]byte(strings.TrimSpace(string(content))))
	require.Equal(t, "22a0e0d9c7f7ff18b1b557ebc6ab2ea4aba08f0b43a21a47308eeea3ad44c407", hex.EncodeToString(checksum[:]))

	sql := strings.Join(strings.Fields(string(content)), " ")
	require.Contains(t, sql, "traffic_director_mode VARCHAR(16) NOT NULL DEFAULT 'legacy'")
	require.Contains(t, sql, "traffic_director_version BIGINT NOT NULL DEFAULT 0")
	require.Contains(t, sql, "traffic_director_spec JSONB")
	require.Contains(t, sql, "traffic_director_version > 0 OR traffic_director_mode = 'legacy'")
	require.Contains(t, sql, "traffic_director_mode IN ('shadow', 'enforced') AND traffic_director_spec IS NOT NULL AND jsonb_typeof(traffic_director_spec) = 'object'")
	require.Contains(t, sql, "CREATE TABLE IF NOT EXISTS traffic_director_versions")
	require.Contains(t, sql, "UNIQUE (group_id, version)")
	require.Contains(t, sql, "UNIQUE (group_id, operation_key)")
	require.Contains(t, sql, "FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE")
	require.Contains(t, sql, "mode IN ('shadow', 'enforced') AND spec IS NOT NULL AND jsonb_typeof(spec) = 'object'")
	require.Contains(t, sql, "unassigned_account_ids BIGINT[] NOT NULL DEFAULT '{}'::BIGINT[]")
	require.Contains(t, sql, "CREATE TRIGGER traffic_director_versions_immutable")
	require.Contains(t, sql, "IF pg_trigger_depth() > 1 AND NOT EXISTS ( SELECT 1 FROM groups WHERE id = OLD.group_id ) THEN RETURN OLD;")
	require.Contains(t, sql, "current_setting('sub2api.traffic_director_history_cleanup', true) = 'on'")
	require.Contains(t, sql, "AND deleted_at IS NOT NULL")
	require.Contains(t, sql, "CREATE OR REPLACE FUNCTION enqueue_group_auth_cache_invalidation()")
	require.Contains(t, sql, "OLD.traffic_director_mode IS NOT DISTINCT FROM NEW.traffic_director_mode")
	require.Contains(t, sql, "OLD.traffic_director_version IS NOT DISTINCT FROM NEW.traffic_director_version")
	require.Contains(t, sql, "OLD.traffic_director_spec IS NOT DISTINCT FROM NEW.traffic_director_spec")
}
