//go:build integration

package repository

import (
	"context"
	"testing"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	dbegressroute "github.com/Wei-Shaw/sub2api/ent/egressroute"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestEgressRepositoryListAssignableRoutesExcludesSoftDeletedProxy(t *testing.T) {
	ctx := context.Background()
	tx := testEntTx(t)
	client := tx.Client()
	repo := &egressRepository{client: client}

	direct, err := client.EgressRoute.Create().
		SetKind(dbegressroute.Kind(service.EgressRouteKindDirect)).
		SetRuntimeScope("assignable-soft-delete-test").
		SetState(dbegressroute.State(service.EgressRouteStatePendingVerification)).
		Save(ctx)
	require.NoError(t, err)

	activeProxy := createAssignableRouteProxy(t, ctx, client, "assignable-active", service.StatusActive, 18101)
	inactiveProxy := createAssignableRouteProxy(t, ctx, client, "assignable-inactive", service.StatusDisabled, 18102)
	deletedProxy := createAssignableRouteProxy(t, ctx, client, "assignable-deleted", service.StatusActive, 18103)

	activeRoute := createAssignableProxyRoute(t, ctx, client, activeProxy.ID, service.EgressRouteStatePendingVerification)
	inactiveRoute := createAssignableProxyRoute(t, ctx, client, inactiveProxy.ID, service.EgressRouteStateInactive)
	deletedRoute := createAssignableProxyRoute(t, ctx, client, deletedProxy.ID, service.EgressRouteStatePendingVerification)

	_, err = client.Proxy.UpdateOneID(deletedProxy.ID).SetDeletedAt(time.Now()).Save(ctx)
	require.NoError(t, err)

	routes, err := repo.ListAssignableRoutes(ctx)
	require.NoError(t, err)

	byID := make(map[int64]service.EgressRoute, len(routes))
	for _, route := range routes {
		byID[route.ID] = route
	}
	require.Contains(t, byID, direct.ID, "direct routes remain assignable")
	require.Contains(t, byID, activeRoute.ID, "active non-deleted proxy routes remain assignable")
	require.Contains(t, byID, inactiveRoute.ID, "inactive non-deleted proxy routes remain visible for explanation")
	require.NotContains(t, byID, deletedRoute.ID, "soft-deleted proxy routes must be hidden")
	require.NotNil(t, byID[activeRoute.ID].Proxy)
	require.NotNil(t, byID[inactiveRoute.ID].Proxy)
}

func createAssignableRouteProxy(
	t *testing.T,
	ctx context.Context,
	client *dbent.Client,
	name string,
	status string,
	port int,
) *dbent.Proxy {
	t.Helper()
	proxy, err := client.Proxy.Create().
		SetName(name).
		SetProtocol("http").
		SetHost("127.0.0.1").
		SetPort(port).
		SetStatus(status).
		Save(ctx)
	require.NoError(t, err)
	return proxy
}

func createAssignableProxyRoute(
	t *testing.T,
	ctx context.Context,
	client *dbent.Client,
	proxyID int64,
	state string,
) *dbent.EgressRoute {
	t.Helper()
	route, err := client.EgressRoute.Create().
		SetKind(dbegressroute.Kind(service.EgressRouteKindProxy)).
		SetProxyID(proxyID).
		SetState(dbegressroute.State(state)).
		Save(ctx)
	require.NoError(t, err)
	return route
}
