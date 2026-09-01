package repository

import (
	"context"
	"regexp"
	"testing"

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
	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\).*account_egress_bindings`).
		WithArgs(int64(9), service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(1)))
	mock.ExpectRollback()

	repo := newProxyRepositoryWithSQL(client, db)
	err = repo.Delete(context.Background(), 9)

	require.ErrorIs(t, err, service.ErrProxyInUse)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestExpiredProxyFallbackExcludesOpenAIPoolAccounts(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectExec(regexp.QuoteMeta("UPDATE proxies SET status=$1, updated_at=NOW() WHERE id=$2 AND deleted_at IS NULL")).
		WithArgs(service.StatusExpired, int64(9)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)INSERT INTO egress_routes.*ON CONFLICT \(proxy_id\) DO UPDATE.*state=EXCLUDED\.state.*RETURNING id`).
		WithArgs(service.EgressRouteKindProxy, int64(9), service.EgressRouteStateExpired).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(44)))
	mock.ExpectQuery(`(?s)WITH roots AS.*root\.platform=\$2.*root\.egress_mode=\$3.*UPDATE accounts.*RETURNING a\.id`).
		WithArgs(int64(44), service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(27)))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO scheduler_outbox")).
		WithArgs(service.SchedulerOutboxEventAccountChanged, int64(27), nil, nil, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery(`(?s)UPDATE accounts SET proxy_id=NULL.*AND NOT \(platform=\$2 AND egress_mode=\$3\).*RETURNING id`).
		WithArgs(int64(9), service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(17)))

	changedIDs, err := (&proxyRepository{}).sweepOneExpiredProxyOnExec(context.Background(), db, 9, nil, true)

	require.NoError(t, err)
	require.Equal(t, []int64{17}, changedIDs)
	require.NoError(t, mock.ExpectationsWereMet())
}
