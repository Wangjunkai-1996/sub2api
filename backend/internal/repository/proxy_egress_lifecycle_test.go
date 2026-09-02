package repository

import (
	"context"
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

func TestCountProxyAccountReferencesIncludesOpenAIPoolBindings(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\).*account_egress_bindings`).
		WithArgs(int64(9), service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(2)))

	count, err := countProxyAccountReferences(context.Background(), db, 9)

	require.NoError(t, err)
	require.Equal(t, int64(2), count)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestGetProxyAccountCountsUsesEffectivePoolReferences(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(`(?s)SELECT proxy_id, COUNT\(\*\).*account_egress_bindings.*a\.platform=\$1.*a\.egress_mode=\$2`).
		WithArgs(service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"proxy_id", "count"}).AddRow(int64(9), int64(2)))

	counts, err := (&proxyRepository{sql: db}).GetAccountCountsForProxies(context.Background())

	require.NoError(t, err)
	require.Equal(t, map[int64]int64{9: 2}, counts)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestListProxyAccountSummariesUsesSameEffectiveReferences(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(`(?s)SELECT a\.id, a\.name.*account_egress_bindings.*a\.platform=\$2.*a\.egress_mode=\$3`).
		WithArgs(int64(9), service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "platform", "type", "notes"}).
			AddRow(int64(17), "pool account", service.PlatformOpenAI, "oauth", nil))

	summaries, err := (&proxyRepository{sql: db}).ListAccountSummariesByProxyID(context.Background(), 9)

	require.NoError(t, err)
	require.Len(t, summaries, 1)
	require.Equal(t, int64(17), summaries[0].ID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProxyDeleteRejectsSecondaryPoolBindingAtomically(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)SELECT id.*FROM proxies.*id=ANY\(\$1\).*FOR NO KEY UPDATE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(9)))
	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\).*FROM proxies.*backup_proxy_id`).
		WithArgs(int64(9)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))
	mock.ExpectQuery(`(?s)SELECT id.*FROM egress_routes.*proxy_id=ANY\(\$1\).*ORDER BY id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(44)))
	mock.ExpectQuery(`(?s)SELECT id.*FROM egress_routes.*id=ANY\(\$1\).*FOR UPDATE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(44)))
	mock.ExpectQuery(`(?s)SELECT id.*FROM \(.*account_egress_bindings.*\) referenced_accounts.*ORDER BY id`).
		WithArgs(int64(9), service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(17)))
	expectLockedAccountIDs(mock, 17)
	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\).*account_egress_bindings`).
		WithArgs(int64(9), service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectRollback()

	repo := newProxyRepositoryWithSQL(client, db)
	err = repo.Delete(context.Background(), 9)

	require.ErrorIs(t, err, service.ErrProxyInUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProxyDeleteRejectsLiveFallbackReferenceAfterLock(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })

	mock.ExpectBegin()
	mock.ExpectQuery(`(?s)SELECT id.*FROM proxies.*id=ANY\(\$1\).*FOR NO KEY UPDATE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(9)))
	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\).*FROM proxies.*backup_proxy_id`).
		WithArgs(int64(9)).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectRollback()

	err = newProxyRepositoryWithSQL(client, db).Delete(context.Background(), 9)

	require.ErrorIs(t, err, service.ErrProxyInUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExpiredProxyFallbackExcludesOpenAIPoolAccounts(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })

	now := time.Date(2026, time.September, 3, 10, 0, 0, 0, time.UTC)
	expiresAt := now.Add(-time.Hour)
	mock.ExpectBegin()
	expectProxyFallbackSnapshot(mock, 9, service.FallbackModeDirect, expiresAt)
	mock.ExpectQuery(`(?s)SELECT id.*FROM proxies.*id=ANY\(\$1\).*FOR NO KEY UPDATE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(9)))
	expectProxyFallbackSnapshot(mock, 9, service.FallbackModeDirect, expiresAt)
	mock.ExpectExec(`(?s)UPDATE proxies.*status=\$1.*expires_at <= \$4.*fallback_mode=\$5.*backup_proxy_id IS NOT DISTINCT FROM \$6`).
		WithArgs(service.StatusExpired, int64(9), service.StatusActive, now, service.FallbackModeDirect, nil).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)SELECT id.*FROM egress_routes.*proxy_id=ANY\(\$1\).*ORDER BY id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(44)))
	mock.ExpectQuery(`(?s)SELECT id.*FROM egress_routes.*id=ANY\(\$1\).*FOR UPDATE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(44)))
	mock.ExpectQuery(`(?s)INSERT INTO egress_routes.*ON CONFLICT \(proxy_id\) DO UPDATE.*state=EXCLUDED\.state.*RETURNING id`).
		WithArgs(service.EgressRouteKindProxy, int64(9), service.EgressRouteStateExpired).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(44)))
	mock.ExpectQuery(`(?s)WITH roots AS.*root\.platform=\$2.*root\.egress_mode=\$3.*SELECT account_id AS id FROM roots`).
		WithArgs(int64(44), service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(27)))
	mock.ExpectQuery(`(?s)SELECT id.*FROM accounts.*proxy_id=\$1.*NOT \(platform=\$2 AND egress_mode=\$3\).*ORDER BY id`).
		WithArgs(int64(9), service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(17)))
	mock.ExpectQuery(`(?s)SELECT id.*FROM accounts.*id=ANY\(\$1\).*FOR UPDATE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(17)).AddRow(int64(27)))
	mock.ExpectExec(`(?s)UPDATE accounts.*egress_revision=egress_revision\+1.*id=ANY\(\$1\)`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO scheduler_outbox")).
		WithArgs(service.SchedulerOutboxEventAccountBulkChanged, nil, nil, accountIDsPayloadMatcher{want: []int64{27}}).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`(?s)UPDATE accounts SET proxy_id=NULL.*AND NOT \(platform=\$2 AND egress_mode=\$3\).*RETURNING id`).
		WithArgs(int64(9), service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(17)))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO scheduler_outbox")).
		WithArgs(service.SchedulerOutboxEventAccountBulkChanged, nil, nil, accountIDsPayloadMatcher{want: []int64{17}}).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	changedIDs, err := (&proxyRepository{client: client, sql: db}).sweepOneExpiredProxy(context.Background(), 9, now)

	require.NoError(t, err)
	require.Equal(t, []int64{17}, changedIDs)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExpiredProxySweepRechecksRenewalAfterProxyLock(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	now := time.Date(2026, time.September, 3, 10, 0, 0, 0, time.UTC)
	expectProxyFallbackSnapshot(mock, 9, service.FallbackModeDirect, now.Add(-time.Hour))
	mock.ExpectQuery(`(?s)SELECT id.*FROM proxies.*id=ANY\(\$1\).*FOR NO KEY UPDATE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(9)))
	expectProxyFallbackSnapshot(mock, 9, service.FallbackModeDirect, now.Add(time.Hour))

	changedIDs, err := (&proxyRepository{}).sweepOneExpiredProxyOnExec(context.Background(), db, 9, now)
	require.NoError(t, err)
	require.Empty(t, changedIDs)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExpiredProxySweepCASMissDoesNotTouchRoutesOrAccounts(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	now := time.Date(2026, time.September, 3, 10, 0, 0, 0, time.UTC)
	expiresAt := now.Add(-time.Hour)
	expectProxyFallbackSnapshot(mock, 9, service.FallbackModeDirect, expiresAt)
	mock.ExpectQuery(`(?s)SELECT id.*FROM proxies.*id=ANY\(\$1\).*FOR NO KEY UPDATE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(9)))
	expectProxyFallbackSnapshot(mock, 9, service.FallbackModeDirect, expiresAt)
	mock.ExpectExec(`(?s)UPDATE proxies.*status=\$1.*expires_at <= \$4.*fallback_mode=\$5.*backup_proxy_id IS NOT DISTINCT FROM \$6`).
		WithArgs(service.StatusExpired, int64(9), service.StatusActive, now, service.FallbackModeDirect, nil).
		WillReturnResult(sqlmock.NewResult(0, 0))

	changedIDs, err := (&proxyRepository{}).sweepOneExpiredProxyOnExec(context.Background(), db, 9, now)
	require.NoError(t, err)
	require.Empty(t, changedIDs)
	require.NoError(t, mock.ExpectationsWereMet())
}

func expectProxyFallbackSnapshot(mock sqlmock.Sqlmock, proxyID int64, fallbackMode string, expiresAt time.Time) {
	now := expiresAt.Add(-time.Hour)
	mock.ExpectQuery(`(?s)SELECT id, name, protocol, host, port, username, password, status,.*FROM proxies.*deleted_at IS NULL.*ORDER BY id`).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "name", "protocol", "host", "port", "username", "password", "status",
			"created_at", "updated_at", "expires_at", "fallback_mode", "backup_proxy_id", "expiry_warn_days",
		}).AddRow(proxyID, "proxy", "http", "proxy.example", 8080, nil, nil, service.StatusActive,
			now, now, expiresAt, fallbackMode, nil, 7))
}
