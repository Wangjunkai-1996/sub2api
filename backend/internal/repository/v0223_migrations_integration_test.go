//go:build integration

package repository

import (
	"context"
	"database/sql"
	"maps"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Wei-Shaw/sub2api/migrations"
	"github.com/stretchr/testify/require"
)

func v0222MigrationFiles(t *testing.T) fstest.MapFS {
	t.Helper()
	manifest, err := os.ReadFile("testdata/v0222_migrations.txt")
	require.NoError(t, err)
	files := fstest.MapFS{}
	for _, name := range strings.Fields(string(manifest)) {
		data, err := migrations.FS.ReadFile(name)
		require.NoError(t, err)
		files[name] = &fstest.MapFile{Data: data}
	}
	require.Len(t, files, 324)
	return files
}

func TestV0223MigrationsUpgradePreservesLedgerAndPricing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	db := newV0220MigrationDatabase(t, ctx)
	legacy := v0222MigrationFiles(t)
	require.NoError(t, applyIntegrationMigrationsFS(ctx, db, legacy))
	before := readV0220MigrationLedger(t, ctx, db)
	require.Len(t, before, 324)
	const added = "252_channel_pricing_ultrafast_multiplier.sql"
	require.NotContains(t, before, added)
	require.Contains(t, before, "252_gemini_cached_contents.sql")
	target := maps.Clone(legacy)
	migration, err := migrations.FS.ReadFile(added)
	require.NoError(t, err)
	target[added] = &fstest.MapFile{Data: migration}
	var channelID, pricingID int64
	require.NoError(t, db.QueryRowContext(ctx, `INSERT INTO channels (name) VALUES ('v0223') RETURNING id`).Scan(&channelID))
	require.NoError(t, db.QueryRowContext(ctx, `INSERT INTO channel_model_pricing (channel_id, models, fast_multiplier, cache_write_12h_price)
		VALUES ($1, '["gpt-6-astra"]', 2.5, 0.00000325) RETURNING id`, channelID).Scan(&pricingID))
	oldFields := func() string {
		t.Helper()
		var fields string
		require.NoError(t, db.QueryRowContext(ctx, `SELECT (to_jsonb(p) - 'ultrafast_multiplier')::text FROM channel_model_pricing p WHERE id=$1`, pricingID).Scan(&fields))
		return fields
	}
	beforeRow := oldFields()
	require.NoError(t, applyMigrationsFS(ctx, db, target))
	after := readV0220MigrationLedger(t, ctx, db)
	require.Len(t, after, 325)
	require.Equal(t, readV0220MigrationLedger(t, ctx, integrationDB)[added].checksum, after[added].checksum)
	require.Contains(t, after, added)
	for name, entry := range before {
		require.Equal(t, entry, after[name], name)
	}
	require.Equal(t, beforeRow, oldFields())
	var multiplier sql.NullFloat64
	readMultiplier := func() {
		t.Helper()
		require.NoError(t, db.QueryRowContext(ctx, `SELECT ultrafast_multiplier FROM channel_model_pricing WHERE id=$1`, pricingID).Scan(&multiplier))
	}
	readMultiplier()
	require.False(t, multiplier.Valid)
	_, err = db.ExecContext(ctx, `UPDATE channel_model_pricing SET ultrafast_multiplier=1.25 WHERE id=$1`, pricingID)
	require.NoError(t, err)
	require.NoError(t, applyMigrationsFS(ctx, db, target))
	require.NoError(t, applyMigrationsFS(ctx, db, legacy))
	require.Equal(t, after, readV0220MigrationLedger(t, ctx, db))
	require.Equal(t, beforeRow, oldFields())
	readMultiplier()
	require.Equal(t, sql.NullFloat64{Float64: 1.25, Valid: true}, multiplier)
}
