package dto

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestAssignableEgressRouteFromServiceRedactsTransportSecrets(t *testing.T) {
	proxyID := int64(9)
	observedIP := "203.0.113.10"
	expiresAt := time.Now().Add(time.Hour)
	route := &service.EgressRoute{
		ID:             17,
		Kind:           service.EgressRouteKindProxy,
		ProxyID:        &proxyID,
		State:          service.EgressRouteStateActive,
		LastObservedIP: &observedIP,
		Revision:       3,
		ExpectedIdentity: &service.EgressIdentity{
			ID:       5,
			PublicIP: observedIP,
			Status:   service.EgressIdentityStatusActive,
		},
		Proxy: &service.Proxy{
			ID:        proxyID,
			Name:      "racknerd-a",
			Protocol:  "socks5",
			Host:      "secret-host.example",
			Port:      1080,
			Username:  "secret-user",
			Password:  "secret-pass",
			Status:    service.StatusActive,
			ExpiresAt: &expiresAt,
		},
	}

	out := AssignableEgressRouteFromService(route)
	require.NotNil(t, out)
	require.True(t, out.Eligible)
	require.Equal(t, "racknerd-a", out.Name)

	raw, err := json.Marshal(out)
	require.NoError(t, err)
	serialized := string(raw)
	for _, secret := range []string{"secret-host", "1080", "secret-user", "secret-pass", "socks5://"} {
		require.NotContains(t, serialized, secret)
	}
}

func TestAccountEgressViewsUseDistinctEligibleIdentitiesAndMarkInheritance(t *testing.T) {
	parentID := int64(21)
	directScope := service.DefaultDirectEgressRuntimeScope
	identity := &service.EgressIdentity{ID: 7, PublicIP: "203.0.113.11", Status: service.EgressIdentityStatusActive}
	account := &service.Account{
		ID:              22,
		ParentAccountID: &parentID,
		EgressMode:      service.EgressModePool,
		EgressRevision:  8,
		Concurrency:     4,
		EgressBindings: []service.AccountEgressBinding{
			{
				RouteID: 2, Position: 1, Status: service.AccountEgressBindingStatusActive,
				Route: &service.EgressRoute{ID: 2, Kind: service.EgressRouteKindDirect, RuntimeScope: &directScope, State: service.EgressRouteStateActive, ExpectedIdentity: identity},
			},
			{
				RouteID: 1, Position: 0, IsPrimary: true, Status: service.AccountEgressBindingStatusActive,
				Route: &service.EgressRoute{ID: 1, Kind: service.EgressRouteKindDirect, RuntimeScope: &directScope, State: service.EgressRouteStateActive, ExpectedIdentity: identity},
			},
		},
	}

	mode, pool, summary := AccountEgressViewsFromService(account)
	require.Equal(t, "inherited", mode)
	require.Equal(t, []int64{1, 2}, pool.RouteIDs)
	require.Nil(t, pool.Revision, "a shadow must not expose its own revision as the parent pool revision")
	require.True(t, pool.Inherited)
	require.Equal(t, 2, summary.ConfiguredRouteCount)
	require.Equal(t, 1, summary.EligibleRouteCount)
	require.Equal(t, 0, summary.DegradedRouteCount, "duplicate healthy routes sharing an identity are not degraded")
	require.Equal(t, 4, summary.EffectiveCapacity)
}

func TestAccountEgressViewsCountOnlyIneligibleBindingsAsDegraded(t *testing.T) {
	directScope := service.DefaultDirectEgressRuntimeScope
	identity := &service.EgressIdentity{ID: 1, PublicIP: "203.0.113.12", Status: service.EgressIdentityStatusActive}
	account := &service.Account{
		EgressMode:  service.EgressModePool,
		Concurrency: 3,
		EgressBindings: []service.AccountEgressBinding{
			{
				RouteID: 1, Status: service.AccountEgressBindingStatusActive,
				Route: &service.EgressRoute{ID: 1, Kind: service.EgressRouteKindDirect, RuntimeScope: &directScope, State: service.EgressRouteStateActive, ExpectedIdentity: identity},
			},
			{
				RouteID: 2, Status: service.AccountEgressBindingStatusActive,
				Route: &service.EgressRoute{ID: 2, Kind: service.EgressRouteKindDirect, RuntimeScope: &directScope, State: service.EgressRouteStatePendingVerification},
			},
			{RouteID: 3, Status: service.AccountEgressBindingStatusDraining},
		},
	}

	_, _, summary := AccountEgressViewsFromService(account)
	require.NotNil(t, summary)
	require.Equal(t, 1, summary.EligibleRouteCount)
	require.Equal(t, 2, summary.DegradedRouteCount)
	require.Equal(t, 3, summary.EffectiveCapacity)
}
