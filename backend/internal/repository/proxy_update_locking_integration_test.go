//go:build integration

package repository

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestProxyUpdateLockingSerializesDoubleUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sourceID := insertProxyUpdateLockingProxy(t, nil)
	firstTargetID := insertProxyUpdateLockingProxy(t, nil)
	secondTargetID := insertProxyUpdateLockingProxy(t, nil)
	repo := newProxyRepositoryWithSQL(integrationEntClient, integrationDB)

	start := make(chan struct{})
	results := make(chan error, 2)
	go func() {
		<-start
		results <- repo.Update(ctx, proxyUpdateLockingInput(sourceID, &firstTargetID, nil))
	}()
	go func() {
		<-start
		results <- repo.Update(ctx, proxyUpdateLockingInput(sourceID, &secondTargetID, nil))
	}()
	close(start)

	require.NoError(t, <-results)
	require.NoError(t, <-results)
	var sourceBackup int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT backup_proxy_id FROM proxies WHERE id=$1`, sourceID).Scan(&sourceBackup))
	require.Contains(t, []int64{firstTargetID, secondTargetID}, sourceBackup)
	var reciprocalBackup int64
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT backup_proxy_id FROM proxies WHERE id=$1`, sourceBackup).Scan(&reciprocalBackup))
	require.Equal(t, sourceID, reciprocalBackup)
}

func TestProxyUpdateLockingSerializesWithDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sourceID := insertProxyUpdateLockingProxy(t, nil)
	targetID := insertProxyUpdateLockingProxy(t, nil)
	repo := newProxyRepositoryWithSQL(integrationEntClient, integrationDB)

	start := make(chan struct{})
	updateResult := make(chan error, 1)
	deleteResult := make(chan error, 1)
	go func() {
		<-start
		updateResult <- repo.Update(ctx, proxyUpdateLockingInput(sourceID, &targetID, nil))
	}()
	go func() {
		<-start
		deleteResult <- repo.Delete(ctx, targetID)
	}()
	close(start)

	updateErr := <-updateResult
	deleteErr := <-deleteResult
	require.True(t,
		(updateErr == nil && errors.Is(deleteErr, service.ErrProxyInUse)) ||
			(errors.Is(updateErr, service.ErrProxyNotFound) && deleteErr == nil),
		"unexpected update/delete results: update=%v delete=%v", updateErr, deleteErr,
	)
	var invalidFallbacks int
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM proxies source
		JOIN proxies target ON target.id=source.backup_proxy_id
		WHERE source.deleted_at IS NULL AND target.deleted_at IS NOT NULL`).Scan(&invalidFallbacks))
	require.Zero(t, invalidFallbacks)
}

func TestProxyUpdateLockingSerializesWithExpirySweep(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	now := time.Now().UTC()
	past := now.Add(-time.Hour)
	future := now.Add(time.Hour)
	sourceID := insertProxyUpdateLockingProxy(t, &past)
	targetID := insertProxyUpdateLockingProxy(t, nil)
	repo := newProxyRepositoryWithSQL(integrationEntClient, integrationDB)

	start := make(chan struct{})
	updateResult := make(chan error, 1)
	sweepResult := make(chan error, 1)
	go func() {
		<-start
		updateResult <- repo.Update(ctx, proxyUpdateLockingInput(sourceID, &targetID, &future))
	}()
	go func() {
		<-start
		_, err := repo.SweepExpiredProxies(ctx, now)
		sweepResult <- err
	}()
	close(start)

	require.NoError(t, <-updateResult)
	require.NoError(t, <-sweepResult)
	var status string
	var expiresAt time.Time
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT status, expires_at FROM proxies WHERE id=$1`, sourceID).Scan(&status, &expiresAt))
	require.Equal(t, service.StatusActive, status)
	require.True(t, expiresAt.After(now))
}

func insertProxyUpdateLockingProxy(t *testing.T, expiresAt *time.Time) int64 {
	t.Helper()
	var id int64
	err := integrationDB.QueryRowContext(context.Background(), `
		INSERT INTO proxies
			(name, protocol, host, port, status, expires_at, fallback_mode, expiry_warn_days, created_at, updated_at)
		VALUES ($1, 'http', '127.0.0.1', 8080, 'active', $2, 'none', 7, NOW(), NOW())
		RETURNING id`, fmt.Sprintf("proxy-update-locking-%d", time.Now().UnixNano()), expiresAt).Scan(&id)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.Exec(`UPDATE proxies SET backup_proxy_id=NULL WHERE id=$1 OR backup_proxy_id=$1`, id)
		_, _ = integrationDB.Exec(`DELETE FROM egress_routes WHERE proxy_id=$1`, id)
		_, _ = integrationDB.Exec(`DELETE FROM proxies WHERE id=$1`, id)
	})
	return id
}

func proxyUpdateLockingInput(id int64, backupProxyID *int64, expiresAt *time.Time) *service.Proxy {
	mode := service.FallbackModeNone
	if backupProxyID != nil {
		mode = service.FallbackModeProxy
	}
	return &service.Proxy{
		ID: id, Name: fmt.Sprintf("proxy-update-locking-%d", id), Protocol: "http", Host: "127.0.0.1", Port: 8080,
		Status: service.StatusActive, ExpiresAt: expiresAt, FallbackMode: mode, BackupProxyID: backupProxyID, ExpiryWarnDays: 7,
	}
}
