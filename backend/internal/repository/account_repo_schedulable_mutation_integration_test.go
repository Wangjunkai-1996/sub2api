//go:build integration

package repository

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type failSchedulableOutboxQueryExecutor struct{ sqlExecutor }

func (e *failSchedulableOutboxQueryExecutor) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if strings.Contains(query, "WITH updated AS") && strings.Contains(query, "scheduler_outbox") {
		args = append([]any(nil), args...)
		args[2] = nil
	}
	return e.sqlExecutor.QueryContext(ctx, query, args...)
}

type cancelSchedulableQueryExecutor struct {
	sqlExecutor
	cancel context.CancelFunc
}

func (e *cancelSchedulableQueryExecutor) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rows, err := e.sqlExecutor.QueryContext(ctx, query, args...)
	if err == nil && strings.Contains(query, "WITH updated AS") {
		e.cancel()
	}
	return rows, err
}

func TestAccountRepositorySetSchedulablePersistsSwitchAndOutbox(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	client := tx.Client()
	cache := &schedulerCacheRecorder{}
	repo := newAccountRepositoryWithSQL(client, tx, cache)
	account := mustCreateAccount(t, client, &service.Account{Name: "schedulable-atomic-switch", Schedulable: true})

	for _, schedulable := range []bool{false, true} {
		require.NoError(t, repo.SetSchedulable(ctx, account.ID, schedulable))
		got, err := repo.GetByID(ctx, account.ID)
		require.NoError(t, err)
		require.Equal(t, schedulable, got.Schedulable)
	}
	var count int
	require.NoError(t, scanSingleRow(ctx, tx, "SELECT COUNT(*) FROM scheduler_outbox WHERE account_id = $1 AND event_type = $2",
		[]any{account.ID, service.SchedulerOutboxEventAccountChanged}, &count))
	// Both transitions share the account_changed dedup key until the worker
	// consumes the first event.
	require.Equal(t, 1, count)
	require.Len(t, cache.setAccounts, 1, "only disabling requires immediate snapshot propagation")
	require.False(t, cache.setAccounts[0].Schedulable)

	_, err := client.Account.UpdateOneID(account.ID).SetDeletedAt(time.Now()).Save(ctx)
	require.NoError(t, err)
	require.ErrorIs(t, repo.SetSchedulable(ctx, account.ID, false), service.ErrAccountNotFound)
	require.ErrorIs(t, repo.SetSchedulable(ctx, -1, false), service.ErrAccountNotFound)
	require.NoError(t, scanSingleRow(ctx, tx, "SELECT COUNT(*) FROM scheduler_outbox WHERE account_id = $1",
		[]any{account.ID}, &count))
	require.Equal(t, 1, count, "missing and deleted accounts must not create propagation events")
}

func TestAccountRepositorySetSchedulableRollsBackWhenOutboxFails(t *testing.T) {
	ctx := context.Background()
	client := testEntClient(t)
	account := mustCreateAccount(t, client, &service.Account{Name: "schedulable-atomic-outbox-failure", Schedulable: true})
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), "DELETE FROM scheduler_outbox WHERE account_id = $1", account.ID)
		_ = client.Account.DeleteOneID(account.ID).Exec(context.Background())
	})
	cache := &schedulerCacheRecorder{}
	repo := newAccountRepositoryWithSQL(client, &failSchedulableOutboxQueryExecutor{sqlExecutor: integrationDB}, cache)

	err := repo.SetSchedulable(ctx, account.ID, false)

	require.Error(t, err)
	got, readErr := repo.GetByID(ctx, account.ID)
	require.NoError(t, readErr)
	require.True(t, got.Schedulable, "outbox failure must roll back the account update")
	require.Equal(t, account.UpdatedAt, got.UpdatedAt)
	require.Empty(t, cache.setAccounts, "an uncommitted account change must not reach the cache")
	var count int
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM scheduler_outbox WHERE account_id = $1", account.ID).Scan(&count))
	require.Zero(t, count)
}

func TestAccountRepositorySetSchedulableDetachesSnapshotAfterCommit(t *testing.T) {
	client := testEntClient(t)
	account := mustCreateAccount(t, client, &service.Account{Name: "schedulable-detached-sync", Schedulable: true})
	t.Cleanup(func() { _ = client.Account.DeleteOneID(account.ID).Exec(context.Background()) })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cache := &schedulerCacheRecorder{}
	repo := newAccountRepositoryWithSQL(client, &cancelSchedulableQueryExecutor{sqlExecutor: integrationDB, cancel: cancel}, cache)

	require.NoError(t, repo.SetSchedulable(ctx, account.ID, false))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.Len(t, cache.setAccounts, 1)
	require.NoError(t, cache.setCtxErr, "post-commit snapshot sync must use a detached context")
	require.False(t, cache.setAccounts[0].Schedulable)
}
