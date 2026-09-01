package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAccountEgressPoolMigrations(t *testing.T) {
	foundation := compactMigrationSQL(t, "240_account_egress_pool_foundation.sql")
	backfill := compactMigrationSQL(t, "241_account_egress_pool_backfill.sql")
	validation := compactMigrationSQL(t, "242_validate_account_egress_pool_constraints.sql")
	permissions := compactMigrationSQL(t, "243_account_egress_pool_runtime_permissions.sql")

	for _, sql := range []string{foundation, backfill, validation, permissions} {
		require.Contains(t, sql, "SET LOCAL lock_timeout = '3s'")
		require.Contains(t, sql, "SET LOCAL statement_timeout = '30s'")
		require.NotContains(t, strings.ToUpper(sql), "DROP TABLE")
		require.NotContains(t, strings.ToUpper(sql), "DROP COLUMN")
	}

	require.Contains(t, foundation, "CREATE TABLE IF NOT EXISTS egress_identities")
	require.Contains(t, foundation, "public_ip inet NOT NULL UNIQUE")
	require.Contains(t, foundation, "CREATE TABLE IF NOT EXISTS egress_routes")
	require.Contains(t, foundation, "CONSTRAINT egress_routes_shape_check")
	require.Contains(t, foundation, "CONSTRAINT egress_routes_proxy_unique UNIQUE (proxy_id)")
	require.Contains(t, foundation, "CONSTRAINT egress_routes_runtime_scope_unique UNIQUE (runtime_scope)")
	require.Contains(t, foundation, "ADD COLUMN IF NOT EXISTS egress_mode VARCHAR(16) NOT NULL DEFAULT 'legacy'")
	require.Contains(t, foundation, "ADD COLUMN IF NOT EXISTS egress_revision BIGINT NOT NULL DEFAULT 1")
	require.Contains(t, foundation, "CHECK (egress_mode IN ('legacy', 'pool')) NOT VALID")
	require.Contains(t, foundation, "CHECK (egress_mode <> 'pool' OR concurrency BETWEEN 1 AND 10000) NOT VALID")
	require.Contains(t, foundation, "CREATE TABLE IF NOT EXISTS account_egress_bindings")
	require.Contains(t, foundation, "PRIMARY KEY (account_id, route_id)")
	require.Contains(t, foundation, "WHERE is_primary")
	require.Contains(t, foundation, "VALUES ('account_egress_pool_rollout_mode', 'off', NOW())")

	require.Contains(t, backfill, "SELECT 'proxy', p.id, 'pending_verification'")
	require.Contains(t, backfill, "VALUES ('direct', 'default', 'pending_verification'")
	require.Contains(t, backfill, "INSERT INTO account_egress_bindings")
	require.Contains(t, backfill, "WHERE a.parent_account_id IS NULL")
	require.Contains(t, backfill, "AND a.deleted_at IS NULL")
	require.NotContains(t, strings.ToUpper(backfill), "UPDATE ACCOUNTS")

	require.Contains(t, validation, "ALTER TABLE accounts VALIDATE CONSTRAINT accounts_egress_mode_check")
	require.Contains(t, validation, "ALTER TABLE accounts VALIDATE CONSTRAINT accounts_egress_revision_check")
	require.Contains(t, validation, "ALTER TABLE accounts VALIDATE CONSTRAINT accounts_pool_concurrency_check")

	require.Contains(t, permissions, "relation.relname = 'accounts'")
	require.Contains(t, permissions, "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE")
	require.Contains(t, permissions, "public.egress_identities, public.egress_routes, public.account_egress_bindings")
	require.Contains(t, permissions, "GRANT USAGE, SELECT, UPDATE ON SEQUENCE")
	require.Contains(t, permissions, "public.egress_identities_id_seq, public.egress_routes_id_seq")
	require.Contains(t, permissions, "ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public")
}

func compactMigrationSQL(t *testing.T, name string) string {
	t.Helper()
	content, err := FS.ReadFile(name)
	require.NoError(t, err)
	return strings.Join(strings.Fields(string(content)), " ")
}
