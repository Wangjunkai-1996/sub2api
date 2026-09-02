package repository

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestRecordProbeObservationSameStateAndIdentityPreservesRevision(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const (
		routeID    = int64(41)
		revision   = int64(7)
		identityID = int64(13)
		observedIP = "198.51.100.17"
	)
	observedAt := time.Date(2026, time.September, 1, 9, 30, 0, 0, time.UTC)
	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	mock.ExpectQuery(`(?s)SELECT er\.revision, er\.state, er\.expected_identity_id, host\(ei\.public_ip\).*FOR UPDATE OF er`).
		WithArgs(routeID).
		WillReturnRows(sqlmock.NewRows([]string{"revision", "state", "expected_identity_id", "public_ip"}).
			AddRow(revision, service.EgressRouteStateActive, identityID, observedIP))
	mock.ExpectExec(`(?s)UPDATE egress_routes.*revision=revision\+CASE WHEN \$5 THEN 1 ELSE 0 END`).
		WithArgs(service.EgressRouteStateActive, observedIP, observedAt, service.EgressRouteStateActive, false, routeID, revision).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = recordProbeObservationTx(context.Background(), tx, service.EgressProbeObservation{
		RouteID:          routeID,
		ExpectedRevision: revision,
		ObservedIP:       observedIP,
	}, observedAt, "")
	require.NoError(t, err)

	mock.ExpectCommit()
	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestRecordProbeObservationStateChangeInvalidatesRootAndShadow(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	const (
		routeID       = int64(41)
		revision      = int64(7)
		identityID    = int64(13)
		expectedIP    = "198.51.100.17"
		observedIP    = "198.51.100.18"
		rootAccount   = int64(27)
		shadowAccount = int64(28)
	)
	observedAt := time.Date(2026, time.September, 1, 9, 31, 0, 0, time.UTC)
	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	mock.ExpectQuery(`(?s)SELECT er\.revision, er\.state, er\.expected_identity_id, host\(ei\.public_ip\).*FOR UPDATE OF er`).
		WithArgs(routeID).
		WillReturnRows(sqlmock.NewRows([]string{"revision", "state", "expected_identity_id", "public_ip"}).
			AddRow(revision, service.EgressRouteStateActive, identityID, expectedIP))
	mock.ExpectExec(`(?s)UPDATE egress_routes.*revision=revision\+CASE WHEN \$5 THEN 1 ELSE 0 END`).
		WithArgs(service.EgressRouteStateIdentityMismatch, observedIP, observedAt, service.EgressRouteStateActive, true, routeID, revision).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)WITH roots AS .*JOIN roots r ON r\.account_id=a\.parent_account_id.*ORDER BY id`).
		WithArgs(routeID, service.PlatformOpenAI, service.EgressModePool).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(rootAccount).AddRow(shadowAccount))
	expectLockedAccountIDs(mock, rootAccount, shadowAccount)
	mock.ExpectExec(`(?s)UPDATE accounts.*egress_revision=egress_revision\+1.*id=ANY\(\$1\)`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO scheduler_outbox")).
		WithArgs(service.SchedulerOutboxEventAccountBulkChanged, nil, nil, accountIDsPayloadMatcher{want: []int64{rootAccount, shadowAccount}}).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = recordProbeObservationTx(context.Background(), tx, service.EgressProbeObservation{
		RouteID:          routeID,
		ExpectedRevision: revision,
		ObservedIP:       observedIP,
	}, observedAt, "")
	require.NoError(t, err)

	mock.ExpectCommit()
	require.NoError(t, tx.Commit())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAccountPoolMutationConcurrencyOnlyPreservesBindings(t *testing.T) {
	for _, operation := range []string{service.AccountPoolOperationAppend, service.AccountPoolOperationRemove} {
		t.Run(operation, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			mock.ExpectBegin()
			tx, err := db.BeginTx(context.Background(), nil)
			require.NoError(t, err)
			mock.ExpectQuery(`(?s)SELECT egress_revision, parent_account_id.*FOR UPDATE`).
				WithArgs(int64(27)).
				WillReturnRows(sqlmock.NewRows([]string{"egress_revision", "parent_account_id", "egress_mode"}).AddRow(int64(9), nil, service.EgressModePool))
			mock.ExpectQuery(`(?s)SELECT route_id, is_primary.*account_egress_bindings`).
				WithArgs(int64(27)).
				WillReturnRows(sqlmock.NewRows([]string{"route_id", "is_primary"}).
					AddRow(int64(101), true).
					AddRow(int64(102), false))

			concurrency := 8
			result, err := accountPoolMutationToReplaceInputTx(context.Background(), tx, 27, service.ApplyAccountPoolsInput{
				Operation:            operation,
				ConcurrencyPerEgress: &concurrency,
			})

			require.NoError(t, err)
			require.Equal(t, service.EgressModePool, result.Mode)
			require.Equal(t, []int64{101, 102}, result.RouteIDs)
			require.Equal(t, int64(101), result.PrimaryRouteID)
			require.Equal(t, 8, requireValue(t, result.ConcurrencyPerEgress))
			require.Equal(t, int64(9), requireValue(t, result.ExpectedRevision))

			mock.ExpectRollback()
			require.NoError(t, tx.Rollback())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestAccountPoolMutationConcurrencyOnlyRejectsLegacyAccountBeforeBindings(t *testing.T) {
	for _, operation := range []string{service.AccountPoolOperationAppend, service.AccountPoolOperationRemove} {
		t.Run(operation, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			mock.ExpectBegin()
			tx, err := db.BeginTx(context.Background(), nil)
			require.NoError(t, err)
			mock.ExpectQuery(`(?s)SELECT egress_revision, parent_account_id, egress_mode.*FOR UPDATE`).
				WithArgs(int64(27)).
				WillReturnRows(sqlmock.NewRows([]string{"egress_revision", "parent_account_id", "egress_mode"}).
					AddRow(int64(9), nil, service.EgressModeLegacy))

			concurrency := 8
			_, err = accountPoolMutationToReplaceInputTx(context.Background(), tx, 27, service.ApplyAccountPoolsInput{
				Operation:            operation,
				ConcurrencyPerEgress: &concurrency,
			})
			require.ErrorIs(t, err, service.ErrEgressPoolInvalid)

			mock.ExpectRollback()
			require.NoError(t, tx.Rollback())
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestValidateSelectedRoutesRejectsDirectRouteForPool(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(`(?s)SELECT er\.id, er\.kind, er\.proxy_id.*FOR SHARE OF er`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "kind", "proxy_id", "state", "expected_identity_id",
			"verified_at", "identity_status", "proxy_status", "expires_at", "deleted_at",
		}).AddRow(
			int64(11), service.EgressRouteKindDirect, nil, service.EgressRouteStateActive, int64(31),
			nil, service.EgressIdentityStatusActive, "", nil, nil,
		))

	_, err = validateSelectedRoutesTx(context.Background(), db, []int64{11}, true)
	require.ErrorIs(t, err, service.ErrEgressPoolInvalid)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestValidateSelectedRoutesRequiresFreshVerification(t *testing.T) {
	now := time.Now()
	for _, test := range []struct {
		name       string
		verifiedAt any
	}{
		{name: "missing"},
		{name: "stale", verifiedAt: now.Add(-service.EgressIdentityFreshness - time.Minute)},
		{name: "far future", verifiedAt: now.Add(2 * time.Minute)},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			mock.ExpectQuery(`(?s)SELECT er\.id, er\.kind, er\.proxy_id.*FOR SHARE OF er`).
				WithArgs(sqlmock.AnyArg()).
				WillReturnRows(sqlmock.NewRows([]string{
					"id", "kind", "proxy_id", "state", "expected_identity_id", "verified_at",
					"identity_status", "proxy_status", "expires_at", "deleted_at",
				}).AddRow(
					int64(11), service.EgressRouteKindProxy, int64(21), service.EgressRouteStateActive, int64(31), test.verifiedAt,
					service.EgressIdentityStatusActive, service.StatusActive, nil, nil,
				))

			_, err = validateSelectedRoutesTx(context.Background(), db, []int64{11}, true)
			require.ErrorIs(t, err, service.ErrEgressPoolInvalid)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestLoadAccountEgressAuthoritiesUsesOneSortedBatchQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(`(?s)SELECT id, egress_mode, egress_revision.*id=ANY\(\$1\).*ORDER BY id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "egress_mode", "egress_revision"}).
			AddRow(int64(7), service.EgressModePool, int64(12)).
			AddRow(int64(11), service.EgressModeLegacy, int64(3)))

	got, err := (&egressRepository{db: db}).LoadAccountEgressAuthorities(
		context.Background(), []int64{11, 7, 11, 0, -1},
	)
	require.NoError(t, err)
	require.Equal(t, map[int64]service.AccountEgressAuthority{
		7:  {AccountID: 7, Mode: service.EgressModePool, Revision: 12},
		11: {AccountID: 11, Mode: service.EgressModeLegacy, Revision: 3},
	}, got)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReadAccountPoolLockPlanIncludesCurrentFallbackAndRequestedRouteProxies(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(`(?s)SELECT id, proxy_id, proxy_fallback_origin_id.*FROM accounts.*ORDER BY id`).
		WithArgs(int64(27)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "proxy_id", "proxy_fallback_origin_id"}).
			AddRow(int64(27), int64(23), int64(24)))
	mock.ExpectQuery(`(?s)SELECT route_id.*FROM account_egress_bindings.*ORDER BY route_id`).
		WithArgs(int64(27)).
		WillReturnRows(sqlmock.NewRows([]string{"route_id"}).AddRow(int64(11)))
	mock.ExpectQuery(`(?s)SELECT DISTINCT proxy_id.*FROM egress_routes.*id=ANY\(\$1\).*ORDER BY proxy_id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"proxy_id"}).AddRow(int64(25)))
	mock.ExpectQuery(`(?s)SELECT DISTINCT proxy_id.*FROM egress_routes.*id=ANY\(\$1\).*ORDER BY proxy_id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"proxy_id"}).AddRow(int64(23)).AddRow(int64(25)))
	mock.ExpectQuery(`(?s)SELECT id.*FROM egress_routes.*proxy_id=ANY\(\$1\).*ORDER BY id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).
			AddRow(int64(11)).AddRow(int64(12)).AddRow(int64(13)))

	plan, err := readAccountPoolLockPlanTx(context.Background(), db, 27, service.ReplaceAccountPoolInput{
		Mode: service.EgressModePool, RouteIDs: []int64{12}, PrimaryRouteID: 12,
	})
	require.NoError(t, err)
	require.Equal(t, []int64{27}, plan.accountIDs)
	require.Equal(t, []int64{11, 12, 13}, plan.routeIDs)
	require.Equal(t, []int64{23, 24, 25}, plan.proxyIDs)
	require.Equal(t, []int64{25}, plan.targetProxyIDs)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReadBulkAccountPoolLockPlanKeepsRemovedProxySourceOnly(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectQuery(`(?s)SELECT id, proxy_id, proxy_fallback_origin_id, egress_mode.*FROM accounts.*ORDER BY id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id", "proxy_id", "proxy_fallback_origin_id", "egress_mode"}).
			AddRow(int64(27), int64(23), int64(24), service.EgressModePool))
	mock.ExpectQuery(`(?s)SELECT account_id, route_id.*FROM account_egress_bindings.*ORDER BY account_id, route_id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"account_id", "route_id"}).
			AddRow(int64(27), int64(11)).AddRow(int64(27), int64(12)))
	mock.ExpectQuery(`(?s)SELECT DISTINCT proxy_id.*FROM egress_routes.*id=ANY\(\$1\).*ORDER BY proxy_id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"proxy_id"}).AddRow(int64(25)))
	mock.ExpectQuery(`(?s)SELECT DISTINCT proxy_id.*FROM egress_routes.*id=ANY\(\$1\).*ORDER BY proxy_id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"proxy_id"}).AddRow(int64(23)).AddRow(int64(25)))
	mock.ExpectQuery(`(?s)SELECT id.*FROM egress_routes.*proxy_id=ANY\(\$1\).*ORDER BY id`).
		WithArgs(sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(11)).AddRow(int64(12)))

	plan, err := readBulkAccountPoolLockPlanTx(
		context.Background(), db, []int64{27}, []int64{11}, service.AccountPoolOperationRemove, service.EgressModePool,
	)
	require.NoError(t, err)
	require.Equal(t, []int64{11, 12}, plan.routeIDs)
	require.Equal(t, []int64{23, 24, 25}, plan.proxyIDs)
	require.Equal(t, []int64{25}, plan.targetProxyIDs)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestAccountPoolMutationRejectsEmptyReplace(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	concurrency := 8
	_, err = accountPoolMutationToReplaceInputTx(context.Background(), tx, 27, service.ApplyAccountPoolsInput{
		Operation:            service.AccountPoolOperationReplace,
		ConcurrencyPerEgress: &concurrency,
	})
	require.ErrorIs(t, err, service.ErrEgressPoolInvalid)

	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceAccountPoolNoopPreservesRevision(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	expectAccountPoolSnapshot(mock, 27, 4, 7, service.EgressModePool, int64(23))
	expectAccountPoolBindings(mock, 27, [][2]any{{int64(11), true}, {int64(12), false}})
	expectEligibleRoutes(mock, []eligibleRouteRow{
		{id: 11, proxyID: int64(23), identityID: int64(31)},
		{id: 12, proxyID: int64(24), identityID: int64(32)},
	})

	revision := int64(7)
	concurrency := 4
	err = replaceAccountPoolLockedTx(context.Background(), tx, 27, service.ReplaceAccountPoolInput{
		Mode:                 service.EgressModePool,
		RouteIDs:             []int64{11, 12},
		PrimaryRouteID:       11,
		ConcurrencyPerEgress: &concurrency,
		ExpectedRevision:     &revision,
	})
	require.NoError(t, err)

	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceAccountPoolLegacySwitchPreservesBindingsAndProxyMirror(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	expectAccountPoolSnapshot(mock, 27, 4, 7, service.EgressModePool, int64(23))
	expectAccountPoolBindings(mock, 27, [][2]any{{int64(11), true}, {int64(12), false}})
	mock.ExpectExec(`(?s)UPDATE accounts.*SET egress_mode=\$2, concurrency=\$3, proxy_id=\$4`).
		WithArgs(int64(27), service.EgressModeLegacy, 4, int64(23)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)UPDATE accounts.*WHERE parent_account_id=\$1.*RETURNING id`).
		WithArgs(int64(27), int64(23)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(28)))
	expectSchedulerOutbox(mock, 27)
	expectSchedulerOutbox(mock, 28)

	revision := int64(7)
	err = replaceAccountPoolLockedTx(context.Background(), tx, 27, service.ReplaceAccountPoolInput{
		Mode:             service.EgressModeLegacy,
		ExpectedRevision: &revision,
	})
	require.NoError(t, err)

	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceAccountPoolConcurrencyOnlyDoesNotRevShadows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	expectAccountPoolSnapshot(mock, 27, 4, 7, service.EgressModePool, int64(23))
	expectAccountPoolBindings(mock, 27, [][2]any{{int64(11), true}, {int64(12), false}})
	expectEligibleRoutes(mock, []eligibleRouteRow{
		{id: 11, proxyID: int64(23), identityID: int64(31)},
		{id: 12, proxyID: int64(24), identityID: int64(32)},
	})
	mock.ExpectExec(`(?s)UPDATE accounts.*SET egress_mode=\$2, concurrency=\$3, proxy_id=\$4`).
		WithArgs(int64(27), service.EgressModePool, 6, int64(23)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	expectSchedulerOutbox(mock, 27)

	revision := int64(7)
	concurrency := 6
	err = replaceAccountPoolLockedTx(context.Background(), tx, 27, service.ReplaceAccountPoolInput{
		Mode:                 service.EgressModePool,
		RouteIDs:             []int64{11, 12},
		PrimaryRouteID:       11,
		ConcurrencyPerEgress: &concurrency,
		ExpectedRevision:     &revision,
	})
	require.NoError(t, err)

	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestReplaceAccountPoolRouteChangeRevisionsRootAndShadows(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	require.NoError(t, err)
	expectAccountPoolSnapshot(mock, 27, 4, 7, service.EgressModePool, int64(23))
	expectAccountPoolBindings(mock, 27, [][2]any{{int64(11), true}, {int64(12), false}})
	expectEligibleRoutes(mock, []eligibleRouteRow{
		{id: 12, proxyID: int64(24), identityID: int64(32)},
		{id: 13, proxyID: int64(25), identityID: int64(33)},
	})
	mock.ExpectExec(`DELETE FROM account_egress_bindings WHERE account_id=\$1`).
		WithArgs(int64(27)).WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`(?s)INSERT INTO account_egress_bindings.*unnest\(\$2::bigint\[\]\)`).
		WithArgs(int64(27), sqlmock.AnyArg(), int64(12), service.AccountEgressBindingStatusActive).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectExec(`(?s)UPDATE accounts.*SET egress_mode=\$2, concurrency=\$3, proxy_id=\$4`).
		WithArgs(int64(27), service.EgressModePool, 4, int64(24)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(`(?s)UPDATE accounts.*WHERE parent_account_id=\$1.*RETURNING id`).
		WithArgs(int64(27), int64(24)).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(28)))
	expectSchedulerOutbox(mock, 27)
	expectSchedulerOutbox(mock, 28)

	revision := int64(7)
	concurrency := 4
	err = replaceAccountPoolLockedTx(context.Background(), tx, 27, service.ReplaceAccountPoolInput{
		Mode:                 service.EgressModePool,
		RouteIDs:             []int64{12, 13},
		PrimaryRouteID:       12,
		ConcurrencyPerEgress: &concurrency,
		ExpectedRevision:     &revision,
	})
	require.NoError(t, err)

	mock.ExpectRollback()
	require.NoError(t, tx.Rollback())
	require.NoError(t, mock.ExpectationsWereMet())
}

type eligibleRouteRow struct {
	id         int64
	proxyID    int64
	identityID int64
}

func expectAccountPoolSnapshot(mock sqlmock.Sqlmock, accountID int64, concurrency int, revision int64, mode string, proxyID int64) {
	mock.ExpectQuery(`(?s)SELECT concurrency, egress_revision, parent_account_id, egress_mode, proxy_id.*FOR UPDATE`).
		WithArgs(accountID).
		WillReturnRows(sqlmock.NewRows([]string{"concurrency", "egress_revision", "parent_account_id", "egress_mode", "proxy_id"}).
			AddRow(concurrency, revision, nil, mode, proxyID))
}

func expectAccountPoolBindings(mock sqlmock.Sqlmock, accountID int64, bindings [][2]any) {
	rows := sqlmock.NewRows([]string{"route_id", "is_primary"})
	for _, binding := range bindings {
		rows.AddRow(binding[0], binding[1])
	}
	mock.ExpectQuery(`(?s)SELECT route_id, is_primary.*account_egress_bindings`).
		WithArgs(accountID).WillReturnRows(rows)
}

func expectEligibleRoutes(mock sqlmock.Sqlmock, routes []eligibleRouteRow) {
	rows := sqlmock.NewRows([]string{
		"id", "kind", "proxy_id", "state", "expected_identity_id",
		"verified_at", "identity_status", "proxy_status", "expires_at", "deleted_at",
	})
	verifiedAt := time.Now()
	for _, route := range routes {
		rows.AddRow(route.id, service.EgressRouteKindProxy, route.proxyID, service.EgressRouteStateActive,
			route.identityID, verifiedAt, service.EgressIdentityStatusActive, service.StatusActive, nil, nil)
	}
	mock.ExpectQuery(`(?s)SELECT er\.id, er\.kind, er\.proxy_id.*WHERE er\.id=ANY\(\$1\)`).
		WithArgs(sqlmock.AnyArg()).WillReturnRows(rows)
}

func expectSchedulerOutbox(mock sqlmock.Sqlmock, accountID int64) {
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO scheduler_outbox")).
		WithArgs(service.SchedulerOutboxEventAccountChanged, accountID, nil, nil, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
}

func requireValue[T any](t *testing.T, value *T) T {
	t.Helper()
	require.NotNil(t, value)
	return *value
}
