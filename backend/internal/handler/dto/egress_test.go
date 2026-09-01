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
	require.Equal(t, "racknerd-a", out.DisplayName)
	require.Equal(t, "racknerd-a", out.ProxyName)
	require.Equal(t, "socks5", out.Protocol)
	require.NotNil(t, out.PublicIP)
	require.Equal(t, observedIP, *out.PublicIP)
	require.NotNil(t, out.IPAddress)
	require.Equal(t, observedIP, *out.IPAddress)
	require.Equal(t, service.EgressIdentityStatusActive, out.IdentityStatus)

	raw, err := json.Marshal(out)
	require.NoError(t, err)
	serialized := string(raw)
	for _, secret := range []string{"secret-host", "1080", "secret-user", "secret-pass", "socks5://"} {
		require.NotContains(t, serialized, secret)
	}
}

func TestAssignableEgressProbeResultIncludesSafePerRouteFailure(t *testing.T) {
	observedAt := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)
	out := AssignableEgressProbeResultFromService(&service.EgressProbeResult{
		RouteID:    91,
		LatencyMs:  -1,
		ObservedAt: observedAt,
		ReasonCode: service.EgressProbeReasonProbeFailed,
	})

	require.NotNil(t, out)
	require.Equal(t, int64(91), out.ID)
	require.NotNil(t, out.ProbeSuccess)
	require.False(t, *out.ProbeSuccess)
	require.Nil(t, out.ProbeLatencyMs, "a probe that never produced latency must not be shown as 0ms")
	require.Equal(t, service.EgressProbeReasonProbeFailed, out.ProbeReasonCode)
	require.Equal(t, "Could not reach the public IP probe through this route.", out.ProbeMessage)
	require.NotNil(t, out.ProbeObservedAt)
	require.Equal(t, observedAt, *out.ProbeObservedAt)

	raw, err := json.Marshal(out)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "proxy_url")
	require.NotContains(t, string(raw), "credentials")
}

func TestAssignableEgressProbeResultIncludesObservedIPOnPersistenceFailure(t *testing.T) {
	out := AssignableEgressProbeResultFromService(&service.EgressProbeResult{
		RouteID:    92,
		ObservedIP: "198.51.100.92",
		LatencyMs:  43,
		ReasonCode: service.EgressProbeReasonPersistenceFailed,
	})

	require.NotNil(t, out)
	require.Equal(t, "198.51.100.92", out.ProbeObservedIP)
	require.NotNil(t, out.ProbeLatencyMs)
	require.Equal(t, int64(43), *out.ProbeLatencyMs)
	require.Equal(t, "The verification result could not be saved.", out.ProbeMessage)
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
