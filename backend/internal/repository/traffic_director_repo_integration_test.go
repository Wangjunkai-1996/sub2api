//go:build integration

package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestTrafficDirectorMembershipSnapshotLocksOnlyTargetGroupIntegration(t *testing.T) {
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	targetGroup := mustCreateGroup(t, integrationEntClient, &service.Group{
		Name:           fmt.Sprintf("traffic-director-lock-%d", suffix),
		Platform:       service.PlatformOpenAI,
		Status:         service.StatusActive,
		RateMultiplier: 1,
	})
	otherGroup := mustCreateGroup(t, integrationEntClient, &service.Group{
		Name:           fmt.Sprintf("traffic-director-other-%d", suffix),
		Platform:       service.PlatformOpenAI,
		Status:         service.StatusActive,
		RateMultiplier: 1,
	})
	targetAccount := mustCreateAccount(t, integrationEntClient, &service.Account{
		Name:        fmt.Sprintf("traffic-director-lock-account-%d", suffix),
		Platform:    service.PlatformOpenAI,
		Status:      service.StatusActive,
		Schedulable: true,
	})
	otherAccount := mustCreateAccount(t, integrationEntClient, &service.Account{
		Name:        fmt.Sprintf("traffic-director-other-account-%d", suffix),
		Platform:    service.PlatformOpenAI,
		Status:      service.StatusActive,
		Schedulable: true,
	})
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(),
			"DELETE FROM account_groups WHERE account_id IN ($1, $2)",
			targetAccount.ID,
			otherAccount.ID,
		)
		_, _ = integrationDB.ExecContext(context.Background(),
			"DELETE FROM accounts WHERE id IN ($1, $2)", targetAccount.ID, otherAccount.ID,
		)
		_, _ = integrationDB.ExecContext(context.Background(),
			"DELETE FROM groups WHERE id IN ($1, $2)", targetGroup.ID, otherGroup.ID,
		)
	})

	lockTx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = lockTx.Rollback() }()

	var lockedGroupID int64
	err = lockTx.QueryRowContext(ctx,
		"SELECT id FROM groups WHERE id = $1 FOR UPDATE", targetGroup.ID,
	).Scan(&lockedGroupID)
	require.NoError(t, err)
	require.Equal(t, targetGroup.ID, lockedGroupID)
	accounts, err := lockTrafficDirectorGroupAccounts(ctx, lockTx, targetGroup.ID)
	require.NoError(t, err)
	require.Empty(t, accounts)

	// An unrelated Group must remain writable while publication validates the
	// target Group. A table-wide membership lock would make this time out.
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO account_groups (account_id, group_id, priority)
		VALUES ($1, $2, 10)
	`, otherAccount.ID, otherGroup.ID)
	require.NoError(t, err)

	// The target Group insert needs a foreign-key KEY SHARE lock on the parent,
	// which conflicts with the publication's FOR UPDATE lock until it commits.
	mutationCtx, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	_, err = integrationDB.ExecContext(mutationCtx, `
		INSERT INTO account_groups (account_id, group_id, priority)
		VALUES ($1, $2, 10)
	`, targetAccount.ID, targetGroup.ID)
	require.Error(t, err)
	require.ErrorIs(t, mutationCtx.Err(), context.DeadlineExceeded)

	require.NoError(t, lockTx.Rollback())
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO account_groups (account_id, group_id, priority)
		VALUES ($1, $2, 10)
	`, targetAccount.ID, targetGroup.ID)
	require.NoError(t, err)
}

func TestTrafficDirectorRepositoryPublicationLifecycleIntegration(t *testing.T) {
	ctx := context.Background()
	suffix := time.Now().UnixNano()
	group := mustCreateGroup(t, integrationEntClient, &service.Group{
		Name:           fmt.Sprintf("traffic-director-%d", suffix),
		Platform:       service.PlatformOpenAI,
		Status:         service.StatusActive,
		RateMultiplier: 1,
	})
	primary := mustCreateAccount(t, integrationEntClient, &service.Account{
		Name:        fmt.Sprintf("traffic-director-primary-%d", suffix),
		Platform:    service.PlatformOpenAI,
		Status:      service.StatusActive,
		Schedulable: true,
	})
	paused := mustCreateAccount(t, integrationEntClient, &service.Account{
		Name:        fmt.Sprintf("traffic-director-paused-%d", suffix),
		Platform:    service.PlatformOpenAI,
		Status:      service.StatusDisabled,
		Schedulable: true,
	})

	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(ctx, "DELETE FROM scheduler_outbox WHERE group_id = $1", group.ID)
		_, _ = integrationDB.ExecContext(ctx, "DELETE FROM groups WHERE id = $1", group.ID)
		_, _ = integrationDB.ExecContext(ctx, "DELETE FROM accounts WHERE id IN ($1, $2)", primary.ID, paused.ID)
	})

	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO account_groups (account_id, group_id, priority)
		VALUES ($1, $3, 10), ($2, $3, 20)
	`, primary.ID, paused.ID, group.ID)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, "UPDATE accounts SET schedulable = FALSE WHERE id = $1", paused.ID)
	require.NoError(t, err)

	repo := NewTrafficDirectorRepository(integrationDB)
	command := service.TrafficDirectorPublishCommand{
		GroupID:         group.ID,
		ExpectedVersion: 0,
		Mode:            domain.TrafficDirectorModeShadow,
		Spec: &domain.TrafficDirectorSpec{
			SchemaVersion: domain.TrafficDirectorSchemaVersion,
			HealthMode:    domain.TrafficDirectorHealthModeObserve,
			Pools: []domain.TrafficDirectorPool{{
				Key:          "primary",
				WeightBPS:    domain.TrafficDirectorWeightTotalBPS,
				AccountIDs:   []int64{primary.ID},
				MinAvailable: 1,
			}},
		},
		IdempotencyKey: "integration-publish",
		Note:           "initial policy",
	}
	command.RequestFingerprint = mustTrafficDirectorFingerprint(t, command)

	published, err := repo.PublishTrafficDirector(ctx, command)
	require.NoError(t, err)
	require.Equal(t, int64(1), published.Version.Version)
	require.Equal(t, []int64{paused.ID}, published.UnassignedAccountIDs)

	head, err := repo.GetTrafficDirectorHead(ctx, group.ID)
	require.NoError(t, err)
	require.Equal(t, int64(1), head.Version)
	require.Equal(t, domain.TrafficDirectorModeShadow, head.Mode)
	// GetTrafficDirectorHead is the scheduler's lightweight pointer read; the
	// immutable spec is resolved separately through the versioned policy cache.
	require.Nil(t, head.Spec)

	versions, total, err := repo.ListTrafficDirectorVersions(ctx, group.ID, 20, 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), total)
	require.Len(t, versions, 2)
	require.Equal(t, int64(1), versions[0].Version)
	require.Equal(t, service.TrafficDirectorLegacyVersion, versions[1].Version)

	replayed, err := repo.PublishTrafficDirector(ctx, command)
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, published.Version, replayed.Version)
	require.Equal(t, published.UnassignedAccountIDs, replayed.UnassignedAccountIDs)

	// PostgreSQL CHECK treats NULL as unknown (and therefore accepted), so the
	// constraints must explicitly reject missing specs in non-legacy modes.
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE groups
		SET traffic_director_spec = NULL
		WHERE id = $1
	`, group.ID)
	require.Error(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO traffic_director_versions (
			group_id, version, mode, spec, checksum,
			operation_key, request_fingerprint
		) VALUES ($1, 2, 'shadow', NULL, $2, 'null-spec', $3)
	`, group.ID, published.Version.Checksum, command.RequestFingerprint)
	require.Error(t, err)

	var historyCount, outboxCount int
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM traffic_director_versions WHERE group_id = $1", group.ID,
	).Scan(&historyCount))
	require.Equal(t, 1, historyCount)
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM scheduler_outbox
		WHERE group_id = $1 AND event_type = $2
	`, group.ID, service.SchedulerOutboxEventGroupChanged).Scan(&outboxCount))
	require.Equal(t, 1, outboxCount)

	_, err = integrationDB.ExecContext(ctx, `
		UPDATE traffic_director_versions SET note = 'tampered'
		WHERE group_id = $1 AND version = 1
	`, group.ID)
	require.Error(t, err)
	_, err = integrationDB.ExecContext(ctx, `
		DELETE FROM traffic_director_versions
		WHERE group_id = $1 AND version = 1
	`, group.ID)
	require.Error(t, err)

	// A nested application trigger also has pg_trigger_depth() > 1. It must not
	// be able to erase history while the parent Group still exists.
	driverTable := fmt.Sprintf("traffic_director_delete_driver_%d", suffix)
	driverFunction := fmt.Sprintf("traffic_director_nested_delete_%d", suffix)
	_, err = integrationDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TABLE %s (group_id BIGINT NOT NULL)", driverTable,
	))
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, fmt.Sprintf(`
		CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			DELETE FROM traffic_director_versions WHERE group_id = NEW.group_id;
			RETURN NEW;
		END;
		$$
	`, driverFunction))
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(ctx, fmt.Sprintf(
		"CREATE TRIGGER nested_delete AFTER INSERT ON %s FOR EACH ROW EXECUTE FUNCTION %s()",
		driverTable,
		driverFunction,
	))
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), fmt.Sprintf("DROP TABLE IF EXISTS %s", driverTable))
		_, _ = integrationDB.ExecContext(context.Background(), fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", driverFunction))
	})
	_, err = integrationDB.ExecContext(ctx, fmt.Sprintf(
		"INSERT INTO %s (group_id) VALUES ($1)", driverTable,
	), group.ID)
	require.Error(t, err)

	// A soft-deleted parent alone must not authorize a direct history delete.
	// The lifecycle path opts in with a transaction-local marker; roll both
	// checks back so the FK-cascade assertion below still observes the row.
	softDeleteTx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = softDeleteTx.ExecContext(ctx,
		"UPDATE groups SET deleted_at = NOW() WHERE id = $1", group.ID,
	)
	require.NoError(t, err)
	_, err = softDeleteTx.ExecContext(ctx,
		"DELETE FROM traffic_director_versions WHERE group_id = $1 AND version = 1", group.ID,
	)
	require.Error(t, err)
	require.NoError(t, softDeleteTx.Rollback())

	// Out-of-band soft deletion intentionally leaves immutable history behind,
	// but that orphaned history must not remain addressable through the admin or
	// policy-store API.
	_, err = integrationDB.ExecContext(ctx,
		"UPDATE groups SET deleted_at = NOW() WHERE id = $1", group.ID,
	)
	require.NoError(t, err)
	_, err = repo.GetTrafficDirectorVersion(ctx, group.ID, 1)
	require.ErrorIs(t, err, service.ErrTrafficDirectorGroupNotFound)
	_, err = integrationDB.ExecContext(ctx,
		"UPDATE groups SET deleted_at = NULL WHERE id = $1", group.ID,
	)
	require.NoError(t, err)

	markedCleanupTx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = markedCleanupTx.ExecContext(ctx,
		"UPDATE groups SET deleted_at = NOW() WHERE id = $1", group.ID,
	)
	require.NoError(t, err)
	_, err = markedCleanupTx.ExecContext(ctx, trafficDirectorHistoryCleanupQuery)
	require.NoError(t, err)
	_, err = markedCleanupTx.ExecContext(ctx,
		"DELETE FROM traffic_director_versions WHERE group_id = $1 AND version = 1", group.ID,
	)
	require.NoError(t, err)
	require.NoError(t, markedCleanupTx.Rollback())

	// The immutability trigger must still permit the history FK cascade.
	_, err = integrationDB.ExecContext(ctx, "DELETE FROM groups WHERE id = $1", group.ID)
	require.NoError(t, err)
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM traffic_director_versions WHERE group_id = $1", group.ID,
	).Scan(&historyCount))
	require.Zero(t, historyCount)
}
