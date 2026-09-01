package dto

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// AssignableEgressRoute is the deliberately redacted admin projection. Keep
// transport coordinates and credentials on the service model only.
type AssignableEgressRoute struct {
	ID         int64      `json:"id"`
	Kind       string     `json:"kind"`
	Name       string     `json:"name"`
	ProxyID    *int64     `json:"proxy_id,omitempty"`
	Revision   int64      `json:"revision"`
	State      string     `json:"state"`
	Eligible   bool       `json:"eligible"`
	ReasonCode string     `json:"reason_code,omitempty"`
	ObservedIP *string    `json:"observed_ip,omitempty"`
	VerifiedAt *time.Time `json:"verified_at,omitempty"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	// Probe fields are populated only by the verify endpoint. They stay
	// optional so the assignable-list projection remains compact.
	ProbeSuccess    *bool      `json:"probe_success,omitempty"`
	ProbeLatencyMs  *int64     `json:"probe_latency_ms,omitempty"`
	ProbeReasonCode string     `json:"probe_reason_code,omitempty"`
	ProbeObservedAt *time.Time `json:"probe_observed_at,omitempty"`
}

type AccountEgressPool struct {
	RouteIDs             []int64                 `json:"route_ids"`
	PrimaryRouteID       *int64                  `json:"primary_route_id"`
	ConcurrencyPerEgress int                     `json:"concurrency_per_egress"`
	Revision             *int64                  `json:"revision,omitempty"`
	Routes               []AssignableEgressRoute `json:"routes"`
	Inherited            bool                    `json:"inherited,omitempty"`
	InheritedFromID      *int64                  `json:"inherited_from_account_id,omitempty"`
}

type AccountEgressSummary struct {
	ConfiguredRouteCount int                     `json:"configured_route_count"`
	EligibleRouteCount   int                     `json:"eligible_route_count"`
	DegradedRouteCount   int                     `json:"degraded_route_count"`
	ConcurrencyPerEgress int                     `json:"concurrency_per_egress"`
	EffectiveCapacity    int                     `json:"effective_capacity"`
	CurrentConcurrency   *int                    `json:"current_concurrency,omitempty"`
	PrimaryRouteID       *int64                  `json:"primary_route_id"`
	Routes               []AssignableEgressRoute `json:"routes"`
	Inherited            bool                    `json:"inherited,omitempty"`
	InheritedFromID      *int64                  `json:"inherited_from_account_id,omitempty"`
}

func AssignableEgressRouteFromService(route *service.EgressRoute) *AssignableEgressRoute {
	if route == nil {
		return nil
	}
	eligible, reason := egressRouteEligibility(route, time.Now())
	return &AssignableEgressRoute{
		ID:         route.ID,
		Kind:       route.Kind,
		Name:       egressRouteDisplayName(route),
		ProxyID:    cloneInt64Pointer(route.ProxyID),
		Revision:   route.Revision,
		State:      route.State,
		Eligible:   eligible,
		ReasonCode: reason,
		ObservedIP: cloneStringPointer(route.LastObservedIP),
		VerifiedAt: cloneTimePointer(route.VerifiedAt),
		ExpiresAt:  egressRouteExpiresAt(route),
	}
}

func AccountEgressViewsFromService(account *service.Account) (string, *AccountEgressPool, *AccountEgressSummary) {
	if account == nil {
		return "", nil, nil
	}
	mode := account.EgressMode
	if mode != service.EgressModePool {
		return mode, nil, nil
	}

	bindings := append([]service.AccountEgressBinding(nil), account.EgressBindings...)
	sort.SliceStable(bindings, func(i, j int) bool {
		if bindings[i].Position != bindings[j].Position {
			return bindings[i].Position < bindings[j].Position
		}
		return bindings[i].RouteID < bindings[j].RouteID
	})

	routeIDs := make([]int64, 0, len(bindings))
	routes := make([]AssignableEgressRoute, 0, len(bindings))
	identities := make(map[int64]struct{}, len(bindings))
	degradedCount := 0
	var primaryRouteID *int64
	for i := range bindings {
		binding := &bindings[i]
		routeIDs = append(routeIDs, binding.RouteID)
		if binding.IsPrimary {
			value := binding.RouteID
			primaryRouteID = &value
		}
		if route := AssignableEgressRouteFromService(binding.Route); route != nil {
			routes = append(routes, *route)
		}
		if binding.Status != service.AccountEgressBindingStatusActive || binding.Route == nil {
			degradedCount++
			continue
		}
		eligible, _ := egressRouteEligibility(binding.Route, time.Now())
		if !eligible {
			// Degraded is a route/binding count. Capacity is still deduplicated
			// below by public identity, so two healthy routes sharing one IP do
			// not make either route appear degraded.
			degradedCount++
			continue
		}
		if binding.Route.ExpectedIdentity != nil && binding.Route.ExpectedIdentity.ID > 0 {
			identities[binding.Route.ExpectedIdentity.ID] = struct{}{}
		}
	}

	inherited := account.ParentAccountID != nil
	if inherited {
		mode = "inherited"
	}
	var revision *int64
	if !inherited && account.EgressRevision > 0 {
		value := account.EgressRevision
		revision = &value
	}
	perEgress := account.ConcurrencyPerEgress()
	eligibleCount := len(identities)
	configuredCount := len(bindings)

	pool := &AccountEgressPool{
		RouteIDs:             routeIDs,
		PrimaryRouteID:       primaryRouteID,
		ConcurrencyPerEgress: perEgress,
		Revision:             revision,
		Routes:               routes,
		Inherited:            inherited,
		InheritedFromID:      cloneInt64Pointer(account.ParentAccountID),
	}
	summary := &AccountEgressSummary{
		ConfiguredRouteCount: configuredCount,
		EligibleRouteCount:   eligibleCount,
		DegradedRouteCount:   degradedCount,
		ConcurrencyPerEgress: perEgress,
		EffectiveCapacity:    eligibleCount * perEgress,
		PrimaryRouteID:       primaryRouteID,
		Routes:               append([]AssignableEgressRoute(nil), routes...),
		Inherited:            inherited,
		InheritedFromID:      cloneInt64Pointer(account.ParentAccountID),
	}
	return mode, pool, summary
}

func egressRouteDisplayName(route *service.EgressRoute) string {
	if route == nil {
		return ""
	}
	if route.Kind == service.EgressRouteKindDirect {
		return "Direct"
	}
	if route.Proxy != nil && strings.TrimSpace(route.Proxy.Name) != "" {
		return strings.TrimSpace(route.Proxy.Name)
	}
	return fmt.Sprintf("Route #%d", route.ID)
}

func egressRouteEligibility(route *service.EgressRoute, now time.Time) (bool, string) {
	if route == nil {
		return false, "route_unavailable"
	}
	switch route.State {
	case service.EgressRouteStateActive:
	case service.EgressRouteStatePendingVerification:
		return false, "pending_verification"
	case service.EgressRouteStateInactive:
		return false, "route_inactive"
	case service.EgressRouteStateExpired:
		return false, "route_expired"
	case service.EgressRouteStateIdentityMismatch:
		return false, "identity_mismatch"
	case service.EgressRouteStateRetired:
		return false, "route_retired"
	default:
		return false, "route_unavailable"
	}
	if route.ExpectedIdentity == nil || route.ExpectedIdentity.ID <= 0 ||
		route.ExpectedIdentity.Status != service.EgressIdentityStatusActive {
		return false, "identity_unavailable"
	}
	switch route.Kind {
	case service.EgressRouteKindDirect:
		if route.RuntimeScope == nil || strings.TrimSpace(*route.RuntimeScope) == "" {
			return false, "direct_route_unavailable"
		}
	case service.EgressRouteKindProxy:
		if route.ProxyID == nil || route.Proxy == nil {
			return false, "proxy_unavailable"
		}
		if !route.Proxy.IsActive() {
			return false, "proxy_inactive"
		}
		if route.Proxy.IsExpired(now) {
			return false, "proxy_expired"
		}
	default:
		return false, "route_kind_invalid"
	}
	return true, ""
}

func egressRouteExpiresAt(route *service.EgressRoute) *time.Time {
	if route == nil || route.Proxy == nil {
		return nil
	}
	return cloneTimePointer(route.Proxy.ExpiresAt)
}

func cloneInt64Pointer(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneStringPointer(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
