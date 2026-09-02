package repository

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
)

func TestProxyUpdateInvalidatesBoundProbeSnapshotsAndEnqueuesOutboxAtomically(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })

	mock.ExpectBegin()
	expectProxyUpdateLockCycle(mock, 9, nil, proxyUpdatePlanRow{
		id: 9, host: "old.example", username: "user", password: "pass", status: service.StatusActive,
	})
	mock.ExpectExec(`(?s)UPDATE "proxies" SET`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE "proxies" SET "backup_proxy_id" = NULL WHERE "backup_proxy_id" = \$1`).
		WithArgs(int64(9)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	expectProxyUpdateReload(mock, 9, "new.example", "user", "pass")
	mock.ExpectQuery(`(?s)INSERT INTO egress_routes.*ON CONFLICT \(proxy_id\) DO UPDATE.*expected_identity_id=NULL.*RETURNING id`).
		WithArgs(service.EgressRouteKindProxy, int64(9), service.EgressRouteStatePendingVerification).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(44)))
	mock.ExpectQuery(`(?s)WITH roots AS.*root\.platform=\$2.*root\.egress_mode=\$3.*SELECT account_id AS id FROM roots`).
		WithArgs(int64(44), service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(27)))
	mock.ExpectQuery(`(?s)SELECT id.*FROM accounts.*proxy_id = \$1.*ORDER BY id`).
		WithArgs(int64(9)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(17)).AddRow(int64(18)))
	expectLockedAccountIDs(mock, 17, 18, 27)
	mock.ExpectExec(`(?s)UPDATE accounts.*egress_revision=egress_revision\+1.*id=ANY\(\$1\)`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO scheduler_outbox")).
		WithArgs(service.SchedulerOutboxEventAccountBulkChanged, nil, nil, accountIDsPayloadMatcher{want: []int64{27}}).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`(?s)UPDATE accounts.*- 'upstream_billing_probe'.*- 'ollama_cloud_usage_snapshot'.*RETURNING id`).
		WithArgs(int64(9)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(17)).AddRow(int64(18)))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO scheduler_outbox (event_type, account_id, group_id, payload)")).
		WithArgs(service.SchedulerOutboxEventAccountBulkChanged, nil, nil, accountIDsPayloadMatcher{want: []int64{17, 18}}).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	repo := newProxyRepositoryWithSQL(client, db)
	proxy := &service.Proxy{
		ID:       9,
		Name:     "proxy",
		Protocol: "http",
		Host:     "new.example",
		Port:     8080,
		Username: "user",
		Password: "pass",
		Status:   service.StatusActive,
	}

	err = repo.Update(context.Background(), proxy)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProxyUpdateRollsBackWhenProbeInvalidationOutboxFails(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })

	mock.ExpectBegin()
	expectProxyUpdateLockCycle(mock, 9, nil, proxyUpdatePlanRow{
		id: 9, host: "old.example", status: service.StatusActive,
	})
	mock.ExpectExec(`(?s)UPDATE "proxies" SET`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE "proxies" SET "backup_proxy_id" = NULL WHERE "backup_proxy_id" = \$1`).
		WithArgs(int64(9)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	expectProxyUpdateReload(mock, 9, "new.example", "", "")
	mock.ExpectQuery(`(?s)INSERT INTO egress_routes.*ON CONFLICT \(proxy_id\) DO UPDATE.*expected_identity_id=NULL.*RETURNING id`).
		WithArgs(service.EgressRouteKindProxy, int64(9), service.EgressRouteStatePendingVerification).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(44)))
	mock.ExpectQuery(`(?s)WITH roots AS.*root\.platform=\$2.*root\.egress_mode=\$3.*SELECT account_id AS id FROM roots`).
		WithArgs(int64(44), service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	mock.ExpectQuery(`(?s)SELECT id.*FROM accounts.*proxy_id = \$1.*ORDER BY id`).
		WithArgs(int64(9)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(17)))
	expectLockedAccountIDs(mock, 17)
	mock.ExpectQuery(`(?s)UPDATE accounts.*- 'upstream_billing_probe'.*- 'ollama_cloud_usage_snapshot'.*RETURNING id`).
		WithArgs(int64(9)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(17)))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO scheduler_outbox (event_type, account_id, group_id, payload)")).
		WillReturnError(errors.New("outbox failed"))
	mock.ExpectRollback()

	repo := newProxyRepositoryWithSQL(client, db)
	proxy := &service.Proxy{ID: 9, Name: "proxy", Protocol: "http", Host: "new.example", Port: 8080, Status: service.StatusActive}

	err = repo.Update(context.Background(), proxy)

	require.EqualError(t, err, "outbox failed")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProxyUpdateSkipsProbeInvalidationForNonIdentityChange(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })

	mock.ExpectBegin()
	expectProxyUpdateLockCycle(mock, 9, nil, proxyUpdatePlanRow{
		id: 9, host: "same.example", status: service.StatusActive,
	})
	mock.ExpectExec(`(?s)UPDATE "proxies" SET`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE "proxies" SET "backup_proxy_id" = NULL WHERE "backup_proxy_id" = \$1`).
		WithArgs(int64(9)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	expectProxyUpdateReload(mock, 9, "same.example", "", "")
	mock.ExpectCommit()

	repo := newProxyRepositoryWithSQL(client, db)
	proxy := &service.Proxy{ID: 9, Name: "renamed", Protocol: "http", Host: "same.example", Port: 8080, Status: service.StatusActive}

	err = repo.Update(context.Background(), proxy)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProxyUpdateOuterTransactionReturnsRetryableDriftWithoutRelocking(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })

	mock.ExpectBegin()
	expectProxyUpdateLockPlan(mock, 9, nil,
		proxyUpdatePlanRow{id: 9, host: "same.example", status: service.StatusActive},
	)
	mock.ExpectQuery(`(?s)SELECT id.*FROM proxies.*id=ANY\(\$1\).*ORDER BY id.*FOR NO KEY UPDATE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(9)))
	expectProxyUpdateLockPlan(mock, 9, nil,
		proxyUpdatePlanRow{id: 9, host: "same.example", status: service.StatusActive},
		proxyUpdatePlanRow{id: 40, host: "reverse.example", status: service.StatusActive, backupProxyID: int64(9)},
	)
	mock.ExpectRollback()

	tx, err := client.Tx(context.Background())
	require.NoError(t, err)
	txCtx := dbent.NewTxContext(context.Background(), tx)
	err = newProxyRepositoryWithSQL(client, db).Update(txCtx, &service.Proxy{
		ID: 9, Name: "proxy", Protocol: "http", Host: "same.example", Port: 8080,
		Status: service.StatusActive,
	})

	require.ErrorIs(t, err, errProxyUpdateLockSetChanged)
	require.ErrorIs(t, err, service.ErrEgressPoolConflict)
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProxyUpdateOwnedTransactionRetriesAfterDriftRollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })

	mock.ExpectBegin()
	expectProxyUpdateLockPlan(mock, 9, nil,
		proxyUpdatePlanRow{id: 9, host: "same.example", status: service.StatusActive},
	)
	mock.ExpectQuery(`(?s)SELECT id.*FROM proxies.*id=ANY\(\$1\).*ORDER BY id.*FOR NO KEY UPDATE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(9)))
	expectProxyUpdateLockPlan(mock, 9, nil,
		proxyUpdatePlanRow{id: 9, host: "same.example", status: service.StatusActive},
		proxyUpdatePlanRow{id: 40, host: "reverse.example", status: service.StatusActive, backupProxyID: int64(9)},
	)
	mock.ExpectRollback()

	mock.ExpectBegin()
	expectProxyUpdateLockCycle(mock, 9, nil,
		proxyUpdatePlanRow{id: 9, host: "same.example", status: service.StatusActive},
		proxyUpdatePlanRow{id: 40, host: "reverse.example", status: service.StatusActive, backupProxyID: int64(9)},
	)
	mock.ExpectExec(`(?s)UPDATE "proxies" SET`).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`UPDATE "proxies" SET "backup_proxy_id" = NULL WHERE "backup_proxy_id" = \$1`).
		WithArgs(int64(9)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	expectProxyUpdateReload(mock, 9, "same.example", "", "")
	mock.ExpectCommit()

	err = newProxyRepositoryWithSQL(client, db).Update(context.Background(), &service.Proxy{
		ID: 9, Name: "proxy", Protocol: "http", Host: "same.example", Port: 8080,
		Status: service.StatusActive,
	})

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReadProxyUpdateLockPlanIncludesOldNewAndReverseSources(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	expectProxyUpdateLockPlan(mock, 9, int64(30),
		proxyUpdatePlanRow{id: 9, host: "self.example", status: service.StatusActive, backupProxyID: int64(20)},
		proxyUpdatePlanRow{id: 20, host: "old.example", status: service.StatusActive, backupProxyID: int64(9)},
		proxyUpdatePlanRow{id: 30, host: "new.example", status: service.StatusActive},
		proxyUpdatePlanRow{id: 40, host: "reverse.example", status: service.StatusActive, backupProxyID: int64(9)},
	)

	newTarget := int64(30)
	plan, err := readProxyUpdateLockPlan(context.Background(), db, 9, &newTarget)

	require.NoError(t, err)
	require.Equal(t, []int64{9, 20, 30, 40}, plan.proxyIDs)
	require.Equal(t, int64(20), plan.states[9].backupProxyID.Int64)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProxyUpdateRejectsDeletedBackupTargetAfterOrderedLocks(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })
	deletedAt := time.Now()

	mock.ExpectBegin()
	expectProxyUpdateLockCycle(mock, 9, int64(20),
		proxyUpdatePlanRow{id: 9, host: "self.example", status: service.StatusActive},
		proxyUpdatePlanRow{id: 20, host: "target.example", status: service.StatusActive, deletedAt: deletedAt},
	)
	mock.ExpectRollback()

	target := int64(20)
	err = newProxyRepositoryWithSQL(client, db).Update(context.Background(), &service.Proxy{
		ID: 9, Name: "proxy", Protocol: "http", Host: "self.example", Port: 8080,
		Status: service.StatusActive, FallbackMode: service.FallbackModeProxy, BackupProxyID: &target,
	})

	require.ErrorIs(t, err, service.ErrProxyNotFound)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateProxyUpdateTargetRequiresActiveUnexpired(t *testing.T) {
	now := time.Now()
	targetID := int64(20)
	current := proxyUpdateLockState{identity: proxyProbeIdentity{status: service.StatusActive}}
	for _, target := range []proxyUpdateLockState{
		{identity: proxyProbeIdentity{status: service.StatusDisabled}},
		{identity: proxyProbeIdentity{status: service.StatusActive, hasExpiresAt: true, expiresAtUnixNs: now.UnixNano()}},
	} {
		_, err := validateProxyUpdateLockPlan(proxyUpdateLockPlan{states: map[int64]proxyUpdateLockState{
			9: current, targetID: target,
		}}, 9, &targetID, now)
		require.ErrorIs(t, err, service.ErrEgressPoolInvalid)
	}
}

func expectProxyUpdateReload(mock sqlmock.Sqlmock, id int64, host, username, password string) {
	now := time.Now()
	mock.ExpectQuery(`(?s)SELECT .* FROM "proxies" WHERE "id" = \$1`).
		WithArgs(id).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "created_at", "updated_at", "deleted_at", "name", "protocol", "host", "port",
			"username", "password", "status", "expires_at", "fallback_mode", "backup_proxy_id", "expiry_warn_days",
		}).AddRow(
			id, now, now, nil, "proxy", "http", host, 8080,
			username, password, service.StatusActive, nil, service.FallbackModeNone, nil, 0,
		))
}

type proxyUpdatePlanRow struct {
	id            int64
	host          string
	username      string
	password      string
	status        string
	expiresAt     any
	backupProxyID any
	deletedAt     any
}

func expectProxyUpdateLockCycle(mock sqlmock.Sqlmock, proxyID int64, newBackupProxyID any, planRows ...proxyUpdatePlanRow) {
	expectProxyUpdateLockPlan(mock, proxyID, newBackupProxyID, planRows...)
	lockedRows := sqlmock.NewRows([]string{"id"})
	for _, row := range planRows {
		lockedRows.AddRow(row.id)
	}
	mock.ExpectQuery(`(?s)SELECT id.*FROM proxies.*id=ANY\(\$1\).*ORDER BY id.*FOR NO KEY UPDATE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(lockedRows)
	expectProxyUpdateLockPlan(mock, proxyID, newBackupProxyID, planRows...)
}

func expectProxyUpdateLockPlan(mock sqlmock.Sqlmock, proxyID int64, newBackupProxyID any, planRows ...proxyUpdatePlanRow) {
	rows := sqlmock.NewRows([]string{
		"id", "protocol", "host", "port", "username", "password", "status", "expires_at", "backup_proxy_id", "deleted_at",
	})
	for _, row := range planRows {
		rows.AddRow(row.id, "http", row.host, 8080, row.username, row.password, row.status, row.expiresAt, row.backupProxyID, row.deletedAt)
	}
	mock.ExpectQuery(`(?s)WITH current AS.*SELECT candidate\.id, candidate\.protocol.*ORDER BY candidate\.id`).
		WithArgs(proxyID, newBackupProxyID).
		WillReturnRows(rows)
}

func TestEnqueueProxyAccountChangesChunksLargePayloads(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	accountIDs := make([]int64, 1001)
	for i := range accountIDs {
		accountIDs[i] = int64(i + 1)
	}
	for start := 0; start < len(accountIDs); start += proxyProbeOutboxAccountChunkSize {
		end := start + proxyProbeOutboxAccountChunkSize
		if end > len(accountIDs) {
			end = len(accountIDs)
		}
		mock.ExpectExec(regexp.QuoteMeta("INSERT INTO scheduler_outbox (event_type, account_id, group_id, payload)")).
			WithArgs(service.SchedulerOutboxEventAccountBulkChanged, nil, nil, accountIDsPayloadMatcher{want: accountIDs[start:end]}).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	err = enqueueProxyProbeAccountChanges(context.Background(), db, accountIDs)

	require.NoError(t, err)
	require.NoError(t, mock.ExpectationsWereMet())
}
