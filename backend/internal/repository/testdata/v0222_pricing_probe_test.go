//go:build integration

package repository

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestV0222BinaryOnV0223PricingSchema(t *testing.T) {
	ctx := context.Background()
	db := integrationDB
	before := readV0220MigrationLedger(t, ctx, db)
	const added = "252_channel_pricing_ultrafast_multiplier.sql"
	require.Len(t, before, 324, "compile this probe against the v0.2.22 KKAI source")
	require.NotContains(t, before, added)
	migration, err := os.ReadFile(os.Getenv("SUB2API_V0223_PRICING_MIGRATION"))
	require.NoError(t, err)
	require.NoError(t, applyMigrationsFS(ctx, db, fstest.MapFS{added: {Data: migration}}))
	after := readV0220MigrationLedger(t, ctx, db)
	require.Len(t, after, 325)
	require.NoError(t, ApplyMigrations(ctx, db))
	require.Equal(t, after, readV0220MigrationLedger(t, ctx, db))
	var channelID int64
	require.NoError(t, db.QueryRowContext(ctx, `INSERT INTO channels (name) VALUES ('old-writer') RETURNING id`).Scan(&channelID))
	_, err = db.ExecContext(ctx, `INSERT INTO channel_model_pricing (channel_id, models, fast_multiplier) VALUES ($1, '["gpt-6-astra"]', 2.5)`, channelID)
	require.NoError(t, err)
	repo := NewChannelRepository(db)
	channel, err := repo.GetByID(ctx, channelID)
	require.NoError(t, err)
	require.Len(t, channel.ModelPricing, 1)
	require.Equal(t, []string{"gpt-6-astra"}, channel.ModelPricing[0].Models)
	assertStored := func(fast float64, ultrafast sql.NullFloat64) {
		t.Helper()
		var gotFast float64
		var gotUltrafast sql.NullFloat64
		require.NoError(t, db.QueryRowContext(ctx, `SELECT fast_multiplier, ultrafast_multiplier FROM channel_model_pricing WHERE channel_id=$1`, channelID).Scan(&gotFast, &gotUltrafast))
		require.Equal(t, fast, gotFast)
		require.Equal(t, ultrafast, gotUltrafast)
	}
	require.NoError(t, repo.Update(ctx, channel))
	assertStored(2.5, sql.NullFloat64{})
	_, err = db.ExecContext(ctx, `UPDATE channel_model_pricing SET ultrafast_multiplier=1.25 WHERE channel_id=$1`, channelID)
	require.NoError(t, err)
	channel, err = repo.GetByID(ctx, channelID)
	require.NoError(t, err)
	require.Len(t, channel.ModelPricing, 1)
	fast := 3.0
	channel.ModelPricing[0].FastMultiplier = &fast
	require.NoError(t, repo.UpdateModelPricing(ctx, &channel.ModelPricing[0]))
	assertStored(fast, sql.NullFloat64{Float64: 1.25, Valid: true})
	require.NoError(t, repo.Update(ctx, channel))
	assertStored(fast, sql.NullFloat64{})
}
