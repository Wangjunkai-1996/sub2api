package repository

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

var trafficDirectorVersionColumns = []string{
	"group_id",
	"version",
	"mode",
	"spec",
	"checksum",
	"operator_id",
	"note",
	"rollback_from_version",
	"created_at",
	"operation_key",
	"request_fingerprint",
	"unassigned_account_ids",
}

var trafficDirectorVersionSummaryColumns = []string{
	"group_id",
	"version",
	"mode",
	"checksum",
	"operator_id",
	"note",
	"rollback_from_version",
	"created_at",
}

func TestTrafficDirectorRepositoryPublishIsAtomicAndReplayPrecedesCAS(t *testing.T) {
	repo, mock, cleanup := newTrafficDirectorRepositoryTest(t)
	defer cleanup()

	command := trafficDirectorTestCommand(t, domain.TrafficDirectorModeEnforced)
	command.ConfirmUnassignedAccounts = true
	command.RequestFingerprint = mustTrafficDirectorFingerprint(t, command)
	checksum, err := service.TrafficDirectorSpecChecksum(*command.Spec)
	require.NoError(t, err)
	specJSON, err := encodeTrafficDirectorSpec(command.Spec)
	require.NoError(t, err)
	createdAt := time.Date(2026, 8, 20, 8, 30, 0, 0, time.UTC)

	mock.ExpectBegin()
	expectTrafficDirectorGroupLock(mock, service.PlatformOpenAI, 0)
	expectTrafficDirectorOperationMiss(mock, command)
	expectTrafficDirectorLockedAccounts(mock,
		[]any{int64(101), "primary", "active", true},
		[]any{int64(202), "paused", "disabled", false},
	)
	mock.ExpectQuery("INSERT INTO traffic_director_versions").
		WithArgs(
			command.GroupID,
			int64(1),
			command.Mode,
			sqlmock.AnyArg(),
			checksum,
			command.IdempotencyKey,
			command.RequestFingerprint,
			sqlmock.AnyArg(),
			command.Note,
			sqlmock.AnyArg(),
			sqlmock.AnyArg(),
		).
		WillReturnRows(sqlmock.NewRows(trafficDirectorVersionColumns).AddRow(
			command.GroupID,
			int64(1),
			command.Mode,
			specJSON,
			checksum,
			nil,
			command.Note,
			nil,
			createdAt,
			command.IdempotencyKey,
			command.RequestFingerprint,
			"{202}",
		))
	mock.ExpectExec("UPDATE groups SET traffic_director_mode").
		WithArgs(command.Mode, int64(1), sqlmock.AnyArg(), command.GroupID, int64(0)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO scheduler_outbox").
		WithArgs(service.SchedulerOutboxEventGroupChanged, nil, command.GroupID, nil, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	result, err := repo.PublishTrafficDirector(context.Background(), command)
	require.NoError(t, err)
	require.False(t, result.Replayed)
	require.Equal(t, int64(1), result.Version.Version)
	require.Equal(t, []int64{202}, result.UnassignedAccountIDs)
	require.Equal(t, createdAt, result.Version.CreatedAt)

	// A successful retry carries the now-stale ExpectedVersion=0. The immutable
	// operation row must win before CAS and membership validation.
	mock.ExpectBegin()
	expectTrafficDirectorGroupLock(mock, service.PlatformOpenAI, 1)
	mock.ExpectQuery("SELECT group_id, version, mode, spec, checksum").
		WithArgs(command.GroupID, command.IdempotencyKey).
		WillReturnRows(sqlmock.NewRows(trafficDirectorVersionColumns).AddRow(
			command.GroupID,
			int64(1),
			command.Mode,
			specJSON,
			checksum,
			nil,
			command.Note,
			nil,
			createdAt,
			command.IdempotencyKey,
			command.RequestFingerprint,
			"{202}",
		))
	mock.ExpectCommit()

	replayed, err := repo.PublishTrafficDirector(context.Background(), command)
	require.NoError(t, err)
	require.True(t, replayed.Replayed)
	require.Equal(t, result.Version, replayed.Version)
	require.Equal(t, result.UnassignedAccountIDs, replayed.UnassignedAccountIDs)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTrafficDirectorRepositoryRejectsIdempotencyKeyReuse(t *testing.T) {
	repo, mock, cleanup := newTrafficDirectorRepositoryTest(t)
	defer cleanup()

	command := trafficDirectorTestCommand(t, domain.TrafficDirectorModeShadow)
	command.Note = "different request"
	command.RequestFingerprint = mustTrafficDirectorFingerprint(t, command)
	existingFingerprint := mustTrafficDirectorFingerprint(t, trafficDirectorTestCommand(t, domain.TrafficDirectorModeShadow))
	checksum, err := service.TrafficDirectorSpecChecksum(*command.Spec)
	require.NoError(t, err)
	specJSON, err := encodeTrafficDirectorSpec(command.Spec)
	require.NoError(t, err)

	mock.ExpectBegin()
	expectTrafficDirectorGroupLock(mock, service.PlatformOpenAI, 4)
	mock.ExpectQuery("SELECT group_id, version, mode, spec, checksum").
		WithArgs(command.GroupID, command.IdempotencyKey).
		WillReturnRows(sqlmock.NewRows(trafficDirectorVersionColumns).AddRow(
			command.GroupID, int64(1), command.Mode, specJSON, checksum, nil, "", nil,
			time.Now(), command.IdempotencyKey, existingFingerprint, "{}",
		))
	mock.ExpectRollback()

	_, err = repo.PublishTrafficDirector(context.Background(), command)
	require.ErrorIs(t, err, service.ErrTrafficDirectorIdempotencyConflict)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTrafficDirectorRepositoryRejectsCASConflictBeforeMembershipRead(t *testing.T) {
	repo, mock, cleanup := newTrafficDirectorRepositoryTest(t)
	defer cleanup()

	command := trafficDirectorTestCommand(t, domain.TrafficDirectorModeShadow)
	mock.ExpectBegin()
	expectTrafficDirectorGroupLock(mock, service.PlatformOpenAI, 3)
	expectTrafficDirectorOperationMiss(mock, command)
	mock.ExpectRollback()

	_, err := repo.PublishTrafficDirector(context.Background(), command)
	require.ErrorIs(t, err, service.ErrTrafficDirectorVersionConflict)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTrafficDirectorRepositoryUsesLockedAuthoritativeMembership(t *testing.T) {
	repo, mock, cleanup := newTrafficDirectorRepositoryTest(t)
	defer cleanup()

	command := trafficDirectorTestCommand(t, domain.TrafficDirectorModeShadow)
	mock.ExpectBegin()
	expectTrafficDirectorGroupLock(mock, service.PlatformOpenAI, 0)
	expectTrafficDirectorOperationMiss(mock, command)
	expectTrafficDirectorLockedAccounts(mock, []any{int64(202), "other", "active", true})
	mock.ExpectRollback()

	_, err := repo.PublishTrafficDirector(context.Background(), command)
	require.ErrorIs(t, err, service.ErrTrafficDirectorValidation)
	require.Contains(t, err.Error(), "does not belong to the group")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTrafficDirectorRepositoryRequiresEnforcedUnassignedConfirmation(t *testing.T) {
	repo, mock, cleanup := newTrafficDirectorRepositoryTest(t)
	defer cleanup()

	command := trafficDirectorTestCommand(t, domain.TrafficDirectorModeEnforced)
	mock.ExpectBegin()
	expectTrafficDirectorGroupLock(mock, service.PlatformOpenAI, 0)
	expectTrafficDirectorOperationMiss(mock, command)
	expectTrafficDirectorLockedAccounts(mock,
		[]any{int64(101), "primary", "active", true},
		[]any{int64(202), "paused", "disabled", false},
	)
	mock.ExpectRollback()

	_, err := repo.PublishTrafficDirector(context.Background(), command)
	require.ErrorIs(t, err, service.ErrTrafficDirectorValidation)
	require.Contains(t, err.Error(), "explicit confirmation")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTrafficDirectorRepositoryLegacyPublicationUsesEmptyPostgresArray(t *testing.T) {
	repo, mock, cleanup := newTrafficDirectorRepositoryTest(t)
	defer cleanup()

	command := trafficDirectorTestCommand(t, domain.TrafficDirectorModeLegacy)
	command.Spec = nil
	command.RequestFingerprint = mustTrafficDirectorFingerprint(t, command)
	checksum := service.TrafficDirectorLegacyChecksum()
	createdAt := time.Date(2026, 8, 20, 9, 0, 0, 0, time.UTC)

	mock.ExpectBegin()
	expectTrafficDirectorGroupLock(mock, service.PlatformOpenAI, 0)
	expectTrafficDirectorOperationMiss(mock, command)
	expectTrafficDirectorLockedAccounts(mock, []any{int64(101), "primary", "active", true})
	mock.ExpectQuery("INSERT INTO traffic_director_versions").
		WithArgs(
			command.GroupID,
			int64(1),
			command.Mode,
			nil,
			checksum,
			command.IdempotencyKey,
			command.RequestFingerprint,
			sqlmock.AnyArg(),
			command.Note,
			nil,
			"{}",
		).
		WillReturnRows(sqlmock.NewRows(trafficDirectorVersionColumns).AddRow(
			command.GroupID,
			int64(1),
			command.Mode,
			nil,
			checksum,
			nil,
			command.Note,
			nil,
			createdAt,
			command.IdempotencyKey,
			command.RequestFingerprint,
			"{}",
		))
	mock.ExpectExec("UPDATE groups SET traffic_director_mode").
		WithArgs(command.Mode, int64(1), nil, command.GroupID, int64(0)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO scheduler_outbox").
		WithArgs(service.SchedulerOutboxEventGroupChanged, nil, command.GroupID, nil, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	result, err := repo.PublishTrafficDirector(context.Background(), command)
	require.NoError(t, err)
	require.NotNil(t, result.UnassignedAccountIDs)
	require.Empty(t, result.UnassignedAccountIDs)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTrafficDirectorRepositoryValidatesRollbackSource(t *testing.T) {
	repo, mock, cleanup := newTrafficDirectorRepositoryTest(t)
	defer cleanup()

	command := trafficDirectorTestCommand(t, domain.TrafficDirectorModeShadow)
	command.ExpectedVersion = 2
	sourceVersion := int64(1)
	command.RollbackFromVersion = &sourceVersion
	command.RequestFingerprint = mustTrafficDirectorFingerprint(t, command)

	mock.ExpectBegin()
	expectTrafficDirectorGroupLock(mock, service.PlatformOpenAI, 2)
	expectTrafficDirectorOperationMiss(mock, command)
	expectTrafficDirectorLockedAccounts(mock, []any{int64(101), "primary", "active", true})
	mock.ExpectQuery("SELECT mode, checksum FROM traffic_director_versions").
		WithArgs(command.GroupID, sourceVersion).
		WillReturnRows(sqlmock.NewRows([]string{"mode", "checksum"}).AddRow(command.Mode, "mismatched"))
	mock.ExpectRollback()

	_, err := repo.PublishTrafficDirector(context.Background(), command)
	require.ErrorIs(t, err, service.ErrTrafficDirectorValidation)
	require.Contains(t, err.Error(), "does not match source version")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTrafficDirectorRepositoryRollsBackWhenSchedulerOutboxFails(t *testing.T) {
	repo, mock, cleanup := newTrafficDirectorRepositoryTest(t)
	defer cleanup()

	command := trafficDirectorTestCommand(t, domain.TrafficDirectorModeShadow)
	checksum, err := service.TrafficDirectorSpecChecksum(*command.Spec)
	require.NoError(t, err)
	specJSON, err := encodeTrafficDirectorSpec(command.Spec)
	require.NoError(t, err)

	mock.ExpectBegin()
	expectTrafficDirectorGroupLock(mock, service.PlatformOpenAI, 0)
	expectTrafficDirectorOperationMiss(mock, command)
	expectTrafficDirectorLockedAccounts(mock, []any{int64(101), "primary", "active", true})
	mock.ExpectQuery("INSERT INTO traffic_director_versions").
		WillReturnRows(sqlmock.NewRows(trafficDirectorVersionColumns).AddRow(
			command.GroupID, int64(1), command.Mode, specJSON, checksum, nil, command.Note, nil,
			time.Now(), command.IdempotencyKey, command.RequestFingerprint, "{}",
		))
	mock.ExpectExec("UPDATE groups SET traffic_director_mode").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO scheduler_outbox").
		WillReturnError(errors.New("outbox unavailable"))
	mock.ExpectRollback()

	_, err = repo.PublishTrafficDirector(context.Background(), command)
	require.ErrorContains(t, err, "enqueue traffic director scheduler event")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTrafficDirectorRepositorySynthesizesLegacyHeadAndIncludesUnschedulableAccounts(t *testing.T) {
	repo, mock, cleanup := newTrafficDirectorRepositoryTest(t)
	defer cleanup()

	mock.ExpectQuery("SELECT id, name, platform").
		WithArgs(int64(42)).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "platform", "traffic_director_version", "traffic_director_mode", "traffic_director_spec",
		}).AddRow(int64(42), "openai", service.PlatformOpenAI, int64(0), "shadow", []byte(`{"ignored":true}`)))
	expectTrafficDirectorAccounts(mock,
		[]any{int64(101), "primary", "active", true},
		[]any{int64(202), "paused", "disabled", false},
	)

	state, err := repo.GetTrafficDirectorGroupState(context.Background(), 42)
	require.NoError(t, err)
	require.Equal(t, domain.TrafficDirectorModeLegacy, state.Head.Mode)
	require.Nil(t, state.Head.Spec)
	require.Len(t, state.Accounts, 2)
	require.False(t, state.Accounts[1].Schedulable)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestTrafficDirectorRepositoryListsVersionsFromOneSnapshot(t *testing.T) {
	repo, mock, cleanup := newTrafficDirectorRepositoryTest(t)
	defer cleanup()

	createdAt := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	spec := trafficDirectorTestCommand(t, domain.TrafficDirectorModeShadow).Spec
	checksum, err := service.TrafficDirectorSpecChecksum(*spec)
	require.NoError(t, err)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT COUNT\\(v.version\\)").
		WithArgs(int64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectQuery("SELECT group_id, version, mode, checksum, operator_id, note,").
		WithArgs(int64(42), int64(1), 0).
		WillReturnRows(sqlmock.NewRows(trafficDirectorVersionSummaryColumns).AddRow(
			int64(42), int64(1), domain.TrafficDirectorModeShadow, checksum,
			nil, "", nil, createdAt,
		))
	mock.ExpectCommit()

	versions, total, err := repo.ListTrafficDirectorVersions(context.Background(), 42, 2, 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), total)
	require.Len(t, versions, 2)
	require.Equal(t, int64(1), versions[0].Version)
	require.Equal(t, checksum, versions[0].Checksum)
	require.Equal(t, service.TrafficDirectorLegacyVersion, versions[1].Version)
	require.NoError(t, mock.ExpectationsWereMet())
}

func newTrafficDirectorRepositoryTest(
	t *testing.T,
) (*trafficDirectorRepository, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	return &trafficDirectorRepository{db: db}, mock, func() {
		mock.ExpectClose()
		require.NoError(t, db.Close())
	}
}

func trafficDirectorTestCommand(t *testing.T, mode string) service.TrafficDirectorPublishCommand {
	t.Helper()
	command := service.TrafficDirectorPublishCommand{
		GroupID:         42,
		ExpectedVersion: 0,
		Mode:            mode,
		Spec: &domain.TrafficDirectorSpec{
			SchemaVersion: domain.TrafficDirectorSchemaVersion,
			HealthMode:    domain.TrafficDirectorHealthModeObserve,
			Pools: []domain.TrafficDirectorPool{{
				Key:          "primary",
				WeightBPS:    domain.TrafficDirectorWeightTotalBPS,
				AccountIDs:   []int64{101},
				MinAvailable: 1,
			}},
		},
		IdempotencyKey: "publish-42",
		Note:           "",
	}
	command.RequestFingerprint = mustTrafficDirectorFingerprint(t, command)
	return command
}

func mustTrafficDirectorFingerprint(t *testing.T, command service.TrafficDirectorPublishCommand) string {
	t.Helper()
	fingerprint, err := service.TrafficDirectorPublishFingerprint(command)
	require.NoError(t, err)
	return fingerprint
}

func expectTrafficDirectorGroupLock(mock sqlmock.Sqlmock, platform string, version int64) {
	mock.ExpectQuery("SELECT platform, traffic_director_version FROM groups").
		WithArgs(int64(42)).
		WillReturnRows(sqlmock.NewRows([]string{"platform", "traffic_director_version"}).AddRow(platform, version))
}

func expectTrafficDirectorOperationMiss(mock sqlmock.Sqlmock, command service.TrafficDirectorPublishCommand) {
	mock.ExpectQuery("SELECT group_id, version, mode, spec, checksum").
		WithArgs(command.GroupID, command.IdempotencyKey).
		WillReturnRows(sqlmock.NewRows(trafficDirectorVersionColumns))
}

func expectTrafficDirectorAccounts(mock sqlmock.Sqlmock, accounts ...[]any) {
	rows := sqlmock.NewRows([]string{"id", "name", "status", "schedulable"})
	for _, account := range accounts {
		values := make([]driver.Value, len(account))
		for index := range account {
			values[index] = account[index]
		}
		rows.AddRow(values...)
	}
	mock.ExpectQuery("SELECT a.id, a.name, a.status, a.schedulable").
		WithArgs(int64(42)).
		WillReturnRows(rows)
}

func expectTrafficDirectorLockedAccounts(mock sqlmock.Sqlmock, accounts ...[]any) {
	expectTrafficDirectorAccounts(mock, accounts...)
}

func TestTrafficDirectorRepositoryConstructorSatisfiesContract(t *testing.T) {
	var _ service.TrafficDirectorRepository = NewTrafficDirectorRepository((*sql.DB)(nil))
}
