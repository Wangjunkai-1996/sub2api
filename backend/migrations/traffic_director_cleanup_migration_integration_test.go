//go:build integration

package migrations_test

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/repository"
	dbmigrations "github.com/Wei-Shaw/sub2api/migrations"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const trafficDirectorCleanupPostgresImage = "postgres:18.1-alpine3.23"

func TestTrafficDirectorCleanupMigration_PostgresPaths(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	if !trafficDirectorCleanupDockerAvailable(ctx) {
		if os.Getenv("CI") != "" {
			t.Fatal("docker is not available (CI=true)")
		}
		t.Skip("docker is not available; skipping PostgreSQL migration integration test")
	}

	postgresImage := strings.TrimSpace(os.Getenv("SUB2API_TEST_POSTGRES_IMAGE"))
	if postgresImage == "" {
		postgresImage = trafficDirectorCleanupPostgresImage
	}
	pgContainer, err := tcpostgres.Run(
		ctx,
		postgresImage,
		tcpostgres.WithDatabase("sub2api_test"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		tcpostgres.BasicWaitStrategies(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, pgContainer.Terminate(context.Background()))
	})

	dsn, err := pgContainer.ConnectionString(ctx, "sslmode=disable", "TimeZone=UTC")
	require.NoError(t, err)
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	require.NoError(t, db.PingContext(ctx))

	t.Run("empty database full migration", func(t *testing.T) {
		require.NoError(t, repository.ApplyMigrations(ctx, db))
		assertTrafficDirectorRemoved(t, ctx, db)

		var applied bool
		require.NoError(t, db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM schema_migrations
    WHERE filename = '239_remove_traffic_director.sql'
)`).Scan(&applied))
		require.True(t, applied)

		// The runner must remain a no-op once the forward migration is recorded.
		require.NoError(t, repository.ApplyMigrations(ctx, db))
	})

	t.Run("database with migration 228 already applied", func(t *testing.T) {
		migration228, err := dbmigrations.FS.ReadFile("228_traffic_director.sql")
		require.NoError(t, err)
		execMigrationSQL(t, ctx, db, string(migration228))

		var groupID int64
		require.NoError(t, db.QueryRowContext(ctx, `
INSERT INTO groups (name)
VALUES ('traffic-director-cleanup-upgrade')
RETURNING id`).Scan(&groupID))
		_, err = db.ExecContext(ctx, `
INSERT INTO traffic_director_versions (
    group_id,
    version,
    mode,
    spec,
    checksum,
    operation_key,
    request_fingerprint
)
VALUES ($1, 1, 'legacy', NULL, repeat('0', 64), 'cleanup-test', repeat('1', 64))`, groupID)
		require.NoError(t, err)

		// Recreate the production upgrade shape: 228 and all intervening
		// migrations are recorded, while only the new cleanup is pending.
		_, err = db.ExecContext(ctx, `
DELETE FROM schema_migrations
WHERE filename = '239_remove_traffic_director.sql'`)
		require.NoError(t, err)
		require.NoError(t, repository.ApplyMigrations(ctx, db))
		assertTrafficDirectorRemoved(t, ctx, db)

		// Exercise the surviving groups trigger after its referenced columns
		// have been removed. A stale migration-228 function body errors here.
		_, err = db.ExecContext(ctx, `
UPDATE groups
SET status = 'disabled'
WHERE id = $1`, groupID)
		require.NoError(t, err)

		cleanup, err := dbmigrations.FS.ReadFile("239_remove_traffic_director.sql")
		require.NoError(t, err)
		execMigrationSQL(t, ctx, db, string(cleanup))
		assertTrafficDirectorRemoved(t, ctx, db)
	})
}

func assertTrafficDirectorRemoved(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()

	var historyTable sql.NullString
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT to_regclass('public.traffic_director_versions')").Scan(&historyTable))
	require.False(t, historyTable.Valid)

	var mutationFunction sql.NullString
	require.NoError(t, db.QueryRowContext(ctx,
		"SELECT to_regprocedure('public.prevent_traffic_director_version_mutation()')").Scan(&mutationFunction))
	require.False(t, mutationFunction.Valid)

	var remainingColumns int
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'groups'
  AND column_name IN (
      'traffic_director_mode',
      'traffic_director_version',
      'traffic_director_spec'
  )`).Scan(&remainingColumns))
	require.Zero(t, remainingColumns)

	var remainingConstraints int
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM pg_constraint
WHERE conrelid = 'public.groups'::regclass
  AND conname IN (
      'groups_traffic_director_version_check',
      'groups_traffic_director_policy_check'
  )`).Scan(&remainingConstraints))
	require.Zero(t, remainingConstraints)

	var groupTriggerExists bool
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgrelid = 'public.groups'::regclass
      AND tgname = 'trg_groups_auth_cache_invalidation'
      AND NOT tgisinternal
)`).Scan(&groupTriggerExists))
	require.True(t, groupTriggerExists)

	var groupFunctionDefinition string
	require.NoError(t, db.QueryRowContext(ctx, `
SELECT pg_get_functiondef(
    'public.enqueue_group_auth_cache_invalidation()'::regprocedure
)`).Scan(&groupFunctionDefinition))
	require.NotContains(t, groupFunctionDefinition, "traffic_director_")
	require.Contains(t, groupFunctionDefinition, "advanced_scheduler_overrides")
}

func execMigrationSQL(t *testing.T, ctx context.Context, db *sql.DB, migrationSQL string) {
	t.Helper()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = tx.ExecContext(ctx, migrationSQL)
	if err != nil {
		_ = tx.Rollback()
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}

func trafficDirectorCleanupDockerAvailable(ctx context.Context) bool {
	cmd := exec.CommandContext(ctx, "docker", "info")
	cmd.Env = os.Environ()
	cmd.Stdout = nil
	cmd.Stderr = nil
	return cmd.Run() == nil
}
