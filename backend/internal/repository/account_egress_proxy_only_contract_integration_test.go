//go:build integration

package repository

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestEgressRepositoryApplyAccountPoolsReplacesTwoAccountsWithThreeProxyRoutes(t *testing.T) {
	ctx := context.Background()
	routeIDs := make([]int64, 0, 3)
	proxyIDs := make([]int64, 0, 3)
	for index, publicIP := range []string{"192.0.2.201", "192.0.2.202", "192.0.2.203"} {
		proxyID := insertProxyOnlyContractProxy(t)
		identityID := insertProxyOnlyContractIdentity(t, publicIP)
		routeID := insertProxyOnlyContractProxyRoute(t, proxyID)
		_, err := integrationDB.ExecContext(ctx, `
			UPDATE egress_routes
			SET expected_identity_id=$2, state=$3, last_observed_ip=$4::inet,
				last_probed_at=NOW(), verified_at=NOW(), revision=revision+1
			WHERE id=$1`, routeID, identityID, service.EgressRouteStateActive, publicIP)
		require.NoError(t, err, "activate proxy route %d", index)
		routeIDs = append(routeIDs, routeID)
		proxyIDs = append(proxyIDs, proxyID)
	}

	accountIDs := []int64{
		insertProxyOnlyContractAccount(t, service.EgressModeLegacy, nil),
		insertProxyOnlyContractAccount(t, service.EgressModeLegacy, nil),
	}
	initialRevisions := make(map[int64]int64, len(accountIDs))
	for _, accountID := range accountIDs {
		var revision int64
		require.NoError(t, integrationDB.QueryRowContext(ctx,
			`SELECT egress_revision FROM accounts WHERE id=$1`, accountID,
		).Scan(&revision))
		initialRevisions[accountID] = revision
	}

	primaryRouteID := routeIDs[0]
	concurrencyPerEgress := 3
	repo := NewEgressRepository(integrationEntClient, integrationDB)
	require.NoError(t, repo.ApplyAccountPools(ctx, accountIDs, service.ApplyAccountPoolsInput{
		Operation:            service.AccountPoolOperationReplace,
		RouteIDs:             routeIDs,
		PrimaryRouteID:       &primaryRouteID,
		ConcurrencyPerEgress: &concurrencyPerEgress,
	}))

	for _, accountID := range accountIDs {
		var (
			mode        string
			concurrency int
			proxyID     int64
			revision    int64
		)
		require.NoError(t, integrationDB.QueryRowContext(ctx, `
			SELECT egress_mode, concurrency, proxy_id, egress_revision
			FROM accounts WHERE id=$1`, accountID,
		).Scan(&mode, &concurrency, &proxyID, &revision))
		require.Equal(t, service.EgressModePool, mode)
		require.Equal(t, concurrencyPerEgress, concurrency)
		require.Equal(t, proxyIDs[0], proxyID, "proxy_id must mirror the primary route")
		require.Equal(t, initialRevisions[accountID]+1, revision)

		rows, err := integrationDB.QueryContext(ctx, `
			SELECT route_id, position, is_primary, status
			FROM account_egress_bindings
			WHERE account_id=$1
			ORDER BY position`, accountID)
		require.NoError(t, err)
		var boundRouteIDs []int64
		for rows.Next() {
			var (
				routeID  int64
				position int
				primary  bool
				status   string
			)
			require.NoError(t, rows.Scan(&routeID, &position, &primary, &status))
			require.Equal(t, len(boundRouteIDs), position)
			require.Equal(t, len(boundRouteIDs) == 0, primary)
			require.Equal(t, service.AccountEgressBindingStatusActive, status)
			boundRouteIDs = append(boundRouteIDs, routeID)
		}
		require.NoError(t, rows.Err())
		require.NoError(t, rows.Close())
		require.True(t, slices.Equal(routeIDs, boundRouteIDs))
	}
}

func TestAccountEgressProxyOnlyConcurrentPoolSwitchAndDirectBinding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	accountID := insertProxyOnlyContractAccount(t, "legacy", nil)
	routeID := insertProxyOnlyContractDirectRoute(t)

	poolTx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = poolTx.Rollback() }()
	_, err = poolTx.ExecContext(ctx, `UPDATE accounts SET egress_mode='pool' WHERE id=$1`, accountID)
	require.NoError(t, err)

	bindingTx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = bindingTx.Rollback() }()
	var bindingPID int
	require.NoError(t, bindingTx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&bindingPID))

	insertResult := make(chan error, 1)
	go func() {
		_, insertErr := bindingTx.ExecContext(ctx, `
			INSERT INTO account_egress_bindings
				(account_id, route_id, position, is_primary, status, created_at, updated_at)
			VALUES ($1, $2, 0, TRUE, 'active', NOW(), NOW())`, accountID, routeID)
		insertResult <- insertErr
	}()
	waitForBlockedPostgresTransaction(t, ctx, bindingPID)
	require.NoError(t, poolTx.Commit())

	insertErr := <-insertResult
	requirePostgresCheckViolation(t, insertErr)
	require.NoError(t, bindingTx.Rollback())

	var bindingCount int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM account_egress_bindings WHERE account_id=$1 AND route_id=$2`,
		accountID, routeID).Scan(&bindingCount))
	require.Zero(t, bindingCount)
}

func TestAccountEgressProxyOnlyRejectsRouteBecomingDirect(t *testing.T) {
	ctx := context.Background()
	accountID := insertProxyOnlyContractAccount(t, "pool", nil)
	proxyID := insertProxyOnlyContractProxy(t)
	routeID := insertProxyOnlyContractProxyRoute(t, proxyID)
	t.Cleanup(func() {
		_, _ = integrationDB.Exec(`DELETE FROM accounts WHERE id=$1`, accountID)
		_, _ = integrationDB.Exec(`DELETE FROM egress_routes WHERE id=$1`, routeID)
		_, _ = integrationDB.Exec(`DELETE FROM proxies WHERE id=$1`, proxyID)
	})
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO account_egress_bindings
			(account_id, route_id, position, is_primary, status, created_at, updated_at)
		VALUES ($1, $2, 0, TRUE, 'active', NOW(), NOW())`, accountID, routeID)
	require.NoError(t, err)

	_, err = integrationDB.ExecContext(ctx, `
		UPDATE egress_routes
		SET kind='direct', proxy_id=NULL, runtime_scope=$2
		WHERE id=$1`, routeID, fmt.Sprintf("contract-route-update-%d", time.Now().UnixNano()))
	requirePostgresCheckViolation(t, err)
}

func TestAccountEgressProxyOnlyRejectsRestoringDeletedPoolAccountWithDirectBinding(t *testing.T) {
	ctx := context.Background()
	deletedAt := time.Now().UTC()
	accountID := insertProxyOnlyContractAccount(t, "pool", &deletedAt)
	routeID := insertProxyOnlyContractDirectRoute(t)
	t.Cleanup(func() {
		_, _ = integrationDB.Exec(`DELETE FROM accounts WHERE id=$1`, accountID)
		_, _ = integrationDB.Exec(`DELETE FROM egress_routes WHERE id=$1`, routeID)
	})
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO account_egress_bindings
			(account_id, route_id, position, is_primary, status, created_at, updated_at)
		VALUES ($1, $2, 0, TRUE, 'active', NOW(), NOW())`, accountID, routeID)
	require.NoError(t, err)

	_, err = integrationDB.ExecContext(ctx, `UPDATE accounts SET deleted_at=NULL WHERE id=$1`, accountID)
	requirePostgresCheckViolation(t, err)
}

func TestAccountEgressProxyOnlySerializesRestoreAgainstRouteBecomingDirect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	deletedAt := time.Now().UTC()
	accountID := insertProxyOnlyContractAccount(t, "pool", &deletedAt)
	proxyID := insertProxyOnlyContractProxy(t)
	routeID := insertProxyOnlyContractProxyRoute(t, proxyID)
	t.Cleanup(func() {
		_, _ = integrationDB.Exec(`DELETE FROM accounts WHERE id=$1`, accountID)
		_, _ = integrationDB.Exec(`DELETE FROM egress_routes WHERE id=$1`, routeID)
		_, _ = integrationDB.Exec(`DELETE FROM proxies WHERE id=$1`, proxyID)
	})
	_, err := integrationDB.ExecContext(ctx, `
		INSERT INTO account_egress_bindings
			(account_id, route_id, position, is_primary, status, created_at, updated_at)
		VALUES ($1, $2, 0, TRUE, 'active', NOW(), NOW())`, accountID, routeID)
	require.NoError(t, err)

	restoreTx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = restoreTx.Rollback() }()
	_, err = restoreTx.ExecContext(ctx, `UPDATE accounts SET deleted_at=NULL WHERE id=$1`, accountID)
	require.NoError(t, err)

	routeTx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = routeTx.Rollback() }()
	var routePID int
	require.NoError(t, routeTx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&routePID))

	updateResult := make(chan error, 1)
	go func() {
		_, updateErr := routeTx.ExecContext(ctx, `
			UPDATE egress_routes
			SET kind='direct', proxy_id=NULL, runtime_scope=$2
			WHERE id=$1`, routeID, fmt.Sprintf("contract-restore-race-%d", time.Now().UnixNano()))
		updateResult <- updateErr
	}()
	waitForBlockedPostgresTransaction(t, ctx, routePID)
	require.NoError(t, restoreTx.Commit())

	requirePostgresCheckViolation(t, <-updateResult)
	require.NoError(t, routeTx.Rollback())
}

func TestAccountEgressProxyTargetValidationSeesDeleteCommittedWhileWaiting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	proxyID := insertProxyOnlyContractProxy(t)

	deleteTx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = deleteTx.Rollback() }()
	_, err = deleteTx.ExecContext(ctx, `UPDATE proxies SET deleted_at=NOW() WHERE id=$1`, proxyID)
	require.NoError(t, err)

	writerTx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = writerTx.Rollback() }()
	var writerPID int
	require.NoError(t, writerTx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&writerPID))

	validationResult := make(chan error, 1)
	go func() {
		locked, lockErr := lockProxiesForShareInOrder(ctx, writerTx, []int64{proxyID})
		if lockErr == nil {
			lockErr = validateWritableProxyTargets([]int64{proxyID}, locked, time.Now())
		}
		validationResult <- lockErr
	}()
	waitForBlockedPostgresTransaction(t, ctx, writerPID)
	require.NoError(t, deleteTx.Commit())

	require.ErrorIs(t, <-validationResult, service.ErrProxyNotFound)
	require.NoError(t, writerTx.Rollback())
}

func insertProxyOnlyContractAccount(t *testing.T, mode string, deletedAt *time.Time) int64 {
	t.Helper()
	var id int64
	err := integrationDB.QueryRowContext(context.Background(), `
		INSERT INTO accounts
			(name, platform, type, credentials, extra, status, egress_mode, deleted_at, created_at, updated_at)
		VALUES ($1, 'openai', 'oauth', '{}'::jsonb, '{}'::jsonb, 'active', $2, $3, NOW(), NOW())
		RETURNING id`, fmt.Sprintf("proxy-only-contract-%d", time.Now().UnixNano()), mode, deletedAt).Scan(&id)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = integrationDB.Exec(`DELETE FROM accounts WHERE id=$1`, id) })
	return id
}

func insertProxyOnlyContractDirectRoute(t *testing.T) int64 {
	t.Helper()
	var id int64
	err := integrationDB.QueryRowContext(context.Background(), `
		INSERT INTO egress_routes (kind, runtime_scope, state, revision, created_at, updated_at)
		VALUES ('direct', $1, 'pending_verification', 1, NOW(), NOW())
		RETURNING id`, fmt.Sprintf("proxy-only-contract-%d", time.Now().UnixNano())).Scan(&id)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = integrationDB.Exec(`DELETE FROM egress_routes WHERE id=$1`, id) })
	return id
}

func insertProxyOnlyContractProxy(t *testing.T) int64 {
	t.Helper()
	var id int64
	err := integrationDB.QueryRowContext(context.Background(), `
		INSERT INTO proxies
			(name, protocol, host, port, status, fallback_mode, expiry_warn_days, created_at, updated_at)
		VALUES ($1, 'http', '127.0.0.1', 8080, 'active', 'none', 7, NOW(), NOW())
		RETURNING id`, fmt.Sprintf("proxy-only-contract-%d", time.Now().UnixNano())).Scan(&id)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = integrationDB.Exec(`DELETE FROM proxies WHERE id=$1`, id) })
	return id
}

func insertProxyOnlyContractIdentity(t *testing.T, publicIP string) int64 {
	t.Helper()
	var id int64
	err := integrationDB.QueryRowContext(context.Background(), `
		INSERT INTO egress_identities (public_ip, status, created_at, updated_at)
		VALUES ($1::inet, 'active', NOW(), NOW())
		RETURNING id`, publicIP).Scan(&id)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = integrationDB.Exec(`DELETE FROM egress_identities WHERE id=$1`, id) })
	return id
}

func insertProxyOnlyContractProxyRoute(t *testing.T, proxyID int64) int64 {
	t.Helper()
	var id int64
	err := integrationDB.QueryRowContext(context.Background(), `
		INSERT INTO egress_routes (kind, proxy_id, state, revision, created_at, updated_at)
		VALUES ('proxy', $1, 'pending_verification', 1, NOW(), NOW())
		RETURNING id`, proxyID).Scan(&id)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = integrationDB.Exec(`DELETE FROM egress_routes WHERE id=$1`, id) })
	return id
}

func waitForBlockedPostgresTransaction(t *testing.T, ctx context.Context, backendPID int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var blockers pq.Int64Array
		err := integrationDB.QueryRowContext(ctx, `SELECT pg_blocking_pids($1)`, backendPID).Scan(&blockers)
		require.NoError(t, err)
		if len(blockers) > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("concurrent transaction did not block on the account constraint lock")
}

func requirePostgresCheckViolation(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	var postgresErr *pq.Error
	require.True(t, errors.As(err, &postgresErr), "expected PostgreSQL error, got %T: %v", err, err)
	require.Equal(t, pq.ErrorCode("23514"), postgresErr.Code)
}
