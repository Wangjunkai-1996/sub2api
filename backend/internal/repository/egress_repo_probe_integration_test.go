//go:build integration

package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestEgressRepositoryRecordProbeObservationRefreshesVerifiedAt(t *testing.T) {
	ctx := context.Background()
	observedAt := time.Now().UTC().Truncate(time.Microsecond)
	staleAt := observedAt.Add(-service.EgressIdentityFreshness - time.Minute)
	expectedIP := uniqueProbeObservationIP(1)
	routeID := insertProbeObservationRoute(t, expectedIP, staleAt)
	repo := NewEgressRepository(integrationEntClient, integrationDB)

	updated, err := repo.RecordProbeObservation(ctx, service.EgressProbeObservation{
		RouteID:          routeID,
		ExpectedRevision: 1,
		ObservedIP:       expectedIP,
		ObservedAt:       observedAt,
	})

	require.NoError(t, err)
	require.Equal(t, service.EgressRouteStateActive, updated.State)
	require.Equal(t, int64(1), updated.Revision)
	require.NotNil(t, updated.VerifiedAt)
	require.WithinDuration(t, observedAt, *updated.VerifiedAt, time.Microsecond)
	require.NotNil(t, updated.LastProbedAt)
	require.WithinDuration(t, observedAt, *updated.LastProbedAt, time.Microsecond)
}

func TestEgressRepositoryRecordProbeObservationMismatchPreservesVerifiedAt(t *testing.T) {
	ctx := context.Background()
	observedAt := time.Now().UTC().Truncate(time.Microsecond)
	verifiedAt := observedAt.Add(-time.Minute)
	expectedIP := uniqueProbeObservationIP(2)
	routeID := insertProbeObservationRoute(t, expectedIP, verifiedAt)
	repo := NewEgressRepository(integrationEntClient, integrationDB)

	updated, err := repo.RecordProbeObservation(ctx, service.EgressProbeObservation{
		RouteID:          routeID,
		ExpectedRevision: 1,
		ObservedIP:       uniqueProbeObservationIP(3),
		ObservedAt:       observedAt,
	})

	require.NoError(t, err)
	require.Equal(t, service.EgressRouteStateIdentityMismatch, updated.State)
	require.Equal(t, int64(2), updated.Revision)
	require.NotNil(t, updated.VerifiedAt)
	require.WithinDuration(t, verifiedAt, *updated.VerifiedAt, time.Microsecond)
}

func insertProbeObservationRoute(t *testing.T, publicIP string, verifiedAt time.Time) int64 {
	t.Helper()
	ctx := context.Background()
	var proxyID int64
	err := integrationDB.QueryRowContext(ctx, `
		INSERT INTO proxies
			(name, protocol, host, port, status, fallback_mode, expiry_warn_days, created_at, updated_at)
		VALUES ($1, 'http', '127.0.0.1', 18080, 'active', 'none', 7, NOW(), NOW())
		RETURNING id`, fmt.Sprintf("probe-observation-%d", time.Now().UnixNano())).Scan(&proxyID)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = integrationDB.Exec(`DELETE FROM proxies WHERE id=$1`, proxyID) })

	var identityID int64
	err = integrationDB.QueryRowContext(ctx, `
		INSERT INTO egress_identities (public_ip, status, created_at, updated_at)
		VALUES ($1::inet, 'active', NOW(), NOW())
		RETURNING id`, publicIP).Scan(&identityID)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = integrationDB.Exec(`DELETE FROM egress_identities WHERE id=$1`, identityID) })

	var routeID int64
	err = integrationDB.QueryRowContext(ctx, `
		INSERT INTO egress_routes
			(kind, proxy_id, expected_identity_id, state, last_observed_ip,
			 last_probed_at, verified_at, revision, created_at, updated_at)
		VALUES ('proxy', $1, $2, 'active', $3::inet, $4, $4, 1, NOW(), NOW())
		RETURNING id`, proxyID, identityID, publicIP, verifiedAt).Scan(&routeID)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = integrationDB.Exec(`DELETE FROM egress_routes WHERE id=$1`, routeID) })
	return routeID
}

func uniqueProbeObservationIP(offset uint64) string {
	value := uint64(time.Now().UnixNano()) + offset
	return fmt.Sprintf("2001:db8:%x:%x::1", (value>>16)&0xffff, value&0xffff)
}
