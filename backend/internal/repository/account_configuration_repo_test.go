package repository

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	dbaccountgroup "github.com/Wei-Shaw/sub2api/ent/accountgroup"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
)

func TestUpdateAccountConfigurationRollsBackFieldsAndEgressWhenGroupsFail(t *testing.T) {
	db, mock, client := newAccountConfigurationMock(t)
	repo := newAccountRepositoryWithSQL(client, db, nil)

	mock.ExpectBegin()
	expectAccountConfigurationPlan(mock, 27, []int64{11})
	expectProxyShareLock(mock, 23)
	mock.ExpectQuery(`(?s)SELECT id.*FROM egress_routes.*id=ANY\(\$1\).*FOR SHARE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(11)))
	expectLockedAccountIDs(mock, 27)
	expectAccountConfigurationPlan(mock, 27, []int64{11})
	expectAccountEntity(mock, 27, `{}`)
	mock.ExpectExec(`UPDATE "accounts" SET "updated_at" = \$1, "name" = \$2 WHERE "id" = \$3`).
		WithArgs(sqlmock.AnyArg(), "renamed", int64(27)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	expectUpdatedAccountEntity(mock, 27, `{}`)
	expectAccountPoolSnapshot(mock, 27, 1, 1, service.EgressModeLegacy, 0)
	expectAccountPoolBindings(mock, 27, nil)
	expectEligibleRoutes(mock, []eligibleRouteRow{{id: 11, proxyID: 23, identityID: 31}})
	mock.ExpectExec(`DELETE FROM account_egress_bindings WHERE account_id=\$1`).
		WithArgs(int64(27)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`(?s)INSERT INTO account_egress_bindings.*unnest\(\$2::bigint\[\]\)`).
		WithArgs(int64(27), sqlmock.AnyArg(), int64(11), service.AccountEgressBindingStatusActive).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`(?s)UPDATE accounts.*SET egress_mode=\$2, concurrency=\$3, proxy_id=\$4`).
		WithArgs(int64(27), service.EgressModePool, 1, int64(23)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)UPDATE accounts.*WHERE parent_account_id=\$1.*RETURNING id`).
		WithArgs(int64(27), int64(23)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	expectSchedulerOutbox(mock, 27)
	mock.ExpectQuery(`(?s)SELECT account_id, group_id.*FROM account_groups.*account_id=ANY\(\$1\)`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"account_id", "group_id"}).AddRow(int64(27), int64(5)))
	mock.ExpectExec(`DELETE FROM "account_groups" WHERE "account_groups"\."account_id" = \$1`).
		WithArgs(int64(27)).
		WillReturnError(errors.New("group write failed"))
	mock.ExpectRollback()

	groupIDs := []int64{6}
	_, _, err := repo.updateAccountConfigurationOnce(context.Background(), service.AccountConfigurationMutation{
		Desired:    &service.Account{ID: 27, Name: "renamed"},
		Fields:     service.AccountConfigurationFieldMask{Name: true},
		EgressPool: &service.ReplaceAccountPoolInput{Mode: service.EgressModePool, RouteIDs: []int64{11}, PrimaryRouteID: 11},
		GroupIDs:   &groupIDs,
	})
	require.EqualError(t, err, "group write failed")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestUpdateAccountConfigurationHydratesBeforeCommit(t *testing.T) {
	db, mock, client := newAccountConfigurationMock(t)
	repo := newAccountRepositoryWithSQL(client, db, nil)

	mock.ExpectBegin()
	expectAccountConfigurationPlan(mock, 27, nil)
	expectLockedAccountIDs(mock, 27)
	expectAccountConfigurationPlan(mock, 27, nil)
	expectAccountEntity(mock, 27, `{}`)
	mock.ExpectExec(`UPDATE "accounts" SET "updated_at" = \$1, "name" = \$2 WHERE "id" = \$3`).
		WithArgs(sqlmock.AnyArg(), "renamed", int64(27)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	expectUpdatedAccountEntity(mock, 27, `{}`)
	mock.ExpectQuery(`(?s)SELECT account_id, group_id.*FROM account_groups.*account_id=ANY\(\$1\)`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"account_id", "group_id"}))
	expectSchedulerOutbox(mock, 27)

	// Hydration queries are intentionally expected before Commit. Registering no
	// post-commit query makes a regression to "commit then GetByID" fail here.
	expectAccountEntity(mock, 27, `{}`)
	mock.ExpectQuery(`(?s)SELECT .* FROM "account_groups".*"account_id" IN \(\$1\)`).
		WithArgs(int64(27)).
		WillReturnRows(sqlmock.NewRows(dbaccountgroup.Columns))
	mock.ExpectCommit()

	updated, affectedIDs, err := repo.updateAccountConfigurationOnce(context.Background(), service.AccountConfigurationMutation{
		Desired: &service.Account{ID: 27, Name: "renamed"},
		Fields:  service.AccountConfigurationFieldMask{Name: true},
	})
	require.NoError(t, err)
	require.NotNil(t, updated)
	require.Equal(t, int64(27), updated.ID)
	require.Equal(t, []int64{27}, affectedIDs)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReadAccountConfigurationLockPlanSeparatesLegacyTargetFromSources(t *testing.T) {
	_, mock, client := newAccountConfigurationMock(t)
	mock.ExpectBegin()
	tx, err := client.Tx(context.Background())
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	mock.ExpectQuery(`(?s)SELECT parent_account_id, proxy_id, proxy_fallback_origin_id.*FROM accounts.*WHERE id=\$1`).
		WithArgs(int64(27)).
		WillReturnRows(sqlmock.NewRows([]string{"parent_account_id", "proxy_id", "proxy_fallback_origin_id"}).
			AddRow(nil, int64(23), int64(24)))
	mock.ExpectQuery(`(?s)SELECT id, proxy_id, proxy_fallback_origin_id.*FROM accounts.*ORDER BY id`).
		WithArgs(int64(27)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "proxy_id", "proxy_fallback_origin_id"}).
			AddRow(int64(27), int64(23), int64(24)))
	mock.ExpectQuery(`(?s)SELECT id.*FROM egress_routes.*proxy_id=ANY\(\$1\).*ORDER BY id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	targetProxyID := int64(29)
	plan, err := readAccountConfigurationLockPlan(context.Background(), tx, service.AccountConfigurationMutation{
		Desired: &service.Account{ID: 27, ProxyID: &targetProxyID},
		Fields:  service.AccountConfigurationFieldMask{ProxyID: true},
	})
	require.NoError(t, err)
	require.Equal(t, []int64{23, 24, 29}, plan.proxyIDs)
	require.Equal(t, []int64{29}, plan.targetProxyIDs)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestProxyShareLockAllowsDeletedSourceButRejectsDeletedTarget(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	deletedAt := time.Now()
	mock.ExpectQuery(`(?s)SELECT id, status, expires_at, deleted_at.*FROM proxies.*ORDER BY id.*FOR SHARE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "status", "expires_at", "deleted_at"}).
			AddRow(int64(23), service.StatusActive, nil, deletedAt).
			AddRow(int64(29), service.StatusActive, nil, nil))

	locked, err := lockProxiesForShareInOrder(context.Background(), db, []int64{29, 23})
	require.NoError(t, err)
	require.NoError(t, validateWritableProxyTargets([]int64{29}, locked, time.Now()))
	require.ErrorIs(t, validateWritableProxyTargets([]int64{23}, locked, time.Now()), service.ErrProxyNotFound)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestWritableProxyTargetMustBeActiveAndUnexpired(t *testing.T) {
	now := time.Now()
	for _, state := range []proxyShareLockState{
		{status: service.StatusDisabled},
		{status: service.StatusActive, expiresAt: sql.NullTime{Time: now, Valid: true}},
	} {
		err := validateWritableProxyTargets([]int64{29}, map[int64]proxyShareLockState{29: state}, now)
		require.ErrorIs(t, err, service.ErrEgressPoolInvalid)
	}
}

func newAccountConfigurationMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock, *dbent.Client) {
	t.Helper()
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })
	return db, mock, client
}

func expectAccountConfigurationPlan(mock sqlmock.Sqlmock, accountID int64, routeIDs []int64) {
	mock.ExpectQuery(`(?s)SELECT parent_account_id, proxy_id, proxy_fallback_origin_id.*FROM accounts.*WHERE id=\$1`).
		WithArgs(accountID).
		WillReturnRows(sqlmock.NewRows([]string{"parent_account_id", "proxy_id", "proxy_fallback_origin_id"}).AddRow(nil, nil, nil))
	mock.ExpectQuery(`(?s)SELECT id, proxy_id, proxy_fallback_origin_id.*FROM accounts.*\(id=\$1 OR parent_account_id=\$1\).*ORDER BY id`).
		WithArgs(accountID).
		WillReturnRows(sqlmock.NewRows([]string{"id", "proxy_id", "proxy_fallback_origin_id"}).AddRow(accountID, nil, nil))
	if routeIDs != nil {
		mock.ExpectQuery(`(?s)SELECT route_id.*FROM account_egress_bindings.*account_id=\$1.*ORDER BY route_id`).
			WithArgs(accountID).
			WillReturnRows(sqlmock.NewRows([]string{"route_id"}))
		mock.ExpectQuery(`(?s)SELECT DISTINCT proxy_id.*FROM egress_routes.*id=ANY\(\$1\).*ORDER BY proxy_id`).
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"proxy_id"}).AddRow(int64(23)))
		mock.ExpectQuery(`(?s)SELECT DISTINCT proxy_id.*FROM egress_routes.*id=ANY\(\$1\).*ORDER BY proxy_id`).
			WithArgs(sqlmock.AnyArg()).
			WillReturnRows(sqlmock.NewRows([]string{"proxy_id"}).AddRow(int64(23)))
	}
}

func expectProxyShareLock(mock sqlmock.Sqlmock, proxyIDs ...int64) {
	rows := sqlmock.NewRows([]string{"id", "status", "expires_at", "deleted_at"})
	for _, proxyID := range proxyIDs {
		rows.AddRow(proxyID, service.StatusActive, nil, nil)
	}
	mock.ExpectQuery(`(?s)SELECT id.*FROM proxies.*id=ANY\(\$1\).*FOR SHARE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(rows)
}

func expectLockedAccountIDs(mock sqlmock.Sqlmock, accountIDs ...int64) {
	rows := sqlmock.NewRows([]string{"id"})
	for _, accountID := range accountIDs {
		rows.AddRow(accountID)
	}
	mock.ExpectQuery(`(?s)SELECT id.*FROM accounts.*id=ANY\(\$1\).*FOR UPDATE`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(rows)
}

func expectAccountEntity(mock sqlmock.Sqlmock, accountID int64, extra string) {
	mock.ExpectQuery(`(?s)SELECT .* FROM "accounts".*WHERE .*"id" = \$1.*LIMIT 2`).
		WithArgs(accountID).
		WillReturnRows(updatedAccountRows(accountID, extra))
}

func expectUpdatedAccountEntity(mock sqlmock.Sqlmock, accountID int64, extra string) {
	mock.ExpectQuery(`(?s)SELECT .* FROM "accounts" WHERE "id" = \$1$`).
		WithArgs(accountID).
		WillReturnRows(updatedAccountRows(accountID, extra))
}
