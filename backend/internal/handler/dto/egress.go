package dto

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// AssignableEgressRoute is the deliberately redacted admin projection. Keep
// transport coordinates and credentials on the service model only.
type AssignableEgressRoute struct {
	ID             int64      `json:"id"`
	Kind           string     `json:"kind"`
	Name           string     `json:"name"`
	DisplayName    string     `json:"display_name"`
	ProxyName      string     `json:"proxy_name,omitempty"`
	Protocol       string     `json:"protocol,omitempty"`
	ProxyID        *int64     `json:"proxy_id,omitempty"`
	Revision       int64      `json:"revision"`
	State          string     `json:"state"`
	Eligible       bool       `json:"eligible"`
	ReasonCode     string     `json:"reason_code,omitempty"`
	PublicIP       *string    `json:"public_ip,omitempty"`
	IPAddress      *string    `json:"ip_address,omitempty"`
	IdentityStatus string     `json:"identity_status,omitempty"`
	ObservedIP     *string    `json:"observed_ip,omitempty"`
	VerifiedAt     *time.Time `json:"verified_at,omitempty"`
	ExpiresAt      *time.Time `json:"expires_at,omitempty"`
	// Probe fields are populated only by the verify endpoint. They stay
	// optional so the assignable-list projection remains compact.
	ProbeSuccess    *bool      `json:"probe_success,omitempty"`
	ProbeLatencyMs  *int64     `json:"probe_latency_ms,omitempty"`
	ProbeReasonCode string     `json:"probe_reason_code,omitempty"`
	ProbeMessage    string     `json:"probe_message,omitempty"`
	ProbeObservedIP string     `json:"probe_observed_ip,omitempty"`
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
	ConfiguredRouteCount int                            `json:"configured_route_count"`
	EligibleRouteCount   int                            `json:"eligible_route_count"`
	DegradedRouteCount   int                            `json:"degraded_route_count"`
	ConcurrencyPerEgress int                            `json:"concurrency_per_egress"`
	EffectiveCapacity    int                            `json:"effective_capacity"`
	CurrentConcurrency   *int                           `json:"current_concurrency,omitempty"`
	PrimaryRouteID       *int64                         `json:"primary_route_id"`
	Routes               []AssignableEgressRoute        `json:"routes"`
	Inherited            bool                           `json:"inherited,omitempty"`
	InheritedFromID      *int64                         `json:"inherited_from_account_id,omitempty"`
	Bindings             []AccountEgressCapacityBinding `json:"bindings,omitempty"`
}

type AccountEgressCapacityBinding struct {
	RouteID            int64   `json:"route_id"`
	Name               string  `json:"name"`
	ObservedIP         *string `json:"observed_ip,omitempty"`
	Eligible           bool    `json:"eligible"`
	CurrentConcurrency int     `json:"current_concurrency"`
}

func AssignableEgressRouteFromService(route *service.EgressRoute) *AssignableEgressRoute {
	if route == nil {
		return nil
	}
	eligible, reason := egressRouteEligibility(route, time.Now())
	displayName := egressRouteDisplayName(route)
	identityIP := egressRouteIdentityIP(route)
	return &AssignableEgressRoute{
		ID:             route.ID,
		Kind:           route.Kind,
		Name:           displayName,
		DisplayName:    displayName,
		ProxyName:      egressRouteProxyName(route),
		Protocol:       egressRouteProtocol(route),
		ProxyID:        cloneInt64Pointer(route.ProxyID),
		Revision:       route.Revision,
		State:          route.State,
		Eligible:       eligible,
		ReasonCode:     reason,
		PublicIP:       cloneStringPointer(identityIP),
		IPAddress:      identityIP,
		IdentityStatus: egressRouteIdentityStatus(route),
		ObservedIP:     cloneStringPointer(route.LastObservedIP),
		VerifiedAt:     cloneTimePointer(route.VerifiedAt),
		ExpiresAt:      egressRouteExpiresAt(route),
	}
}

// AssignableEgressProbeResultFromService preserves the batch verify contract:
// every requested route gets a redacted item, including per-route failures.
func AssignableEgressProbeResultFromService(result *service.EgressProbeResult) *AssignableEgressRoute {
	if result == nil {
		return nil
	}
	item := AssignableEgressRouteFromService(result.Route)
	if item == nil {
		item = &AssignableEgressRoute{
			ID:       result.RouteID,
			State:    service.EgressRouteStateInactive,
			Eligible: false,
		}
	}
	success := result.Success
	item.ProbeSuccess = &success
	if result.LatencyMs >= 0 {
		latency := result.LatencyMs
		item.ProbeLatencyMs = &latency
	}
	item.ProbeReasonCode = strings.TrimSpace(result.ReasonCode)
	if !result.Success && item.ProbeReasonCode == "" {
		item.ProbeReasonCode = service.EgressProbeReasonProbeFailed
	}
	item.ProbeMessage = egressProbeMessage(item.ProbeReasonCode)
	item.ProbeObservedIP = strings.TrimSpace(result.ObservedIP)
	item.ProbeObservedAt = nonZeroTimePointer(result.ObservedAt)
	return item
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

// AccountEgressCapacityBindingsFromService projects one row per unique public
// identity. Multiple routes sharing an IP must never appear as extra capacity.
func AccountEgressCapacityBindingsFromService(account *service.Account, identityLoads map[string]int) []AccountEgressCapacityBinding {
	if account == nil || len(identityLoads) == 0 {
		return nil
	}

	bindings := append([]service.AccountEgressBinding(nil), account.EgressBindings...)
	sort.SliceStable(bindings, func(i, j int) bool {
		if bindings[i].Position != bindings[j].Position {
			return bindings[i].Position < bindings[j].Position
		}
		return bindings[i].RouteID < bindings[j].RouteID
	})

	type projection struct {
		binding AccountEgressCapacityBinding
		primary bool
	}
	projections := make([]projection, 0, len(identityLoads))
	identityIndexes := make(map[int64]int, len(identityLoads))
	now := time.Now()
	for i := range bindings {
		binding := &bindings[i]
		route := binding.Route
		if route == nil || route.ExpectedIdentity == nil || route.ExpectedIdentity.ID <= 0 {
			continue
		}
		identityID := route.ExpectedIdentity.ID
		active, known := identityLoads[strconv.FormatInt(identityID, 10)]
		if !known {
			continue
		}
		eligible, _ := egressRouteEligibility(route, now)
		eligible = eligible && binding.Status == service.AccountEgressBindingStatusActive
		item := AccountEgressCapacityBinding{
			RouteID:            binding.RouteID,
			Name:               egressRouteDisplayName(route),
			ObservedIP:         cloneStringPointer(egressRouteIdentityIP(route)),
			Eligible:           eligible,
			CurrentConcurrency: active,
		}
		if index, exists := identityIndexes[identityID]; exists {
			identityEligible := projections[index].binding.Eligible || item.Eligible
			if binding.IsPrimary && !projections[index].primary {
				item.Eligible = identityEligible
				projections[index] = projection{binding: item, primary: true}
			} else {
				projections[index].binding.Eligible = identityEligible
			}
			continue
		}
		identityIndexes[identityID] = len(projections)
		projections = append(projections, projection{binding: item, primary: binding.IsPrimary})
	}

	result := make([]AccountEgressCapacityBinding, len(projections))
	for i := range projections {
		result[i] = projections[i].binding
	}
	return result
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

func egressRouteProtocol(route *service.EgressRoute) string {
	if route == nil || route.Proxy == nil {
		return ""
	}
	return strings.TrimSpace(route.Proxy.Protocol)
}

func egressRouteProxyName(route *service.EgressRoute) string {
	if route == nil || route.Proxy == nil {
		return ""
	}
	return strings.TrimSpace(route.Proxy.Name)
}

func egressRouteIdentityIP(route *service.EgressRoute) *string {
	if route == nil || route.ExpectedIdentity == nil {
		return nil
	}
	ip := strings.TrimSpace(route.ExpectedIdentity.PublicIP)
	if ip == "" {
		return nil
	}
	return &ip
}

func egressRouteIdentityStatus(route *service.EgressRoute) string {
	if route == nil || route.ExpectedIdentity == nil {
		return ""
	}
	return strings.TrimSpace(route.ExpectedIdentity.Status)
}

func egressProbeMessage(reasonCode string) string {
	switch strings.TrimSpace(reasonCode) {
	case "":
		return ""
	case service.EgressProbeReasonRouteNotFound:
		return "Route was not found."
	case service.EgressProbeReasonRouteUnavailable:
		return "Route is inactive, expired, retired, or missing its transport configuration."
	case service.EgressProbeReasonProbeFailed:
		return "Could not reach the public IP probe through this route."
	case service.EgressProbeReasonInvalidObservation:
		return "The probe did not return a valid public IP address."
	case service.EgressProbeReasonRevisionConflict:
		return "Route changed during verification; reload it and try again."
	case service.EgressProbeReasonPersistenceFailed:
		return "The verification result could not be saved."
	case service.EgressProbeReasonRequestCanceled:
		return "Verification was canceled or timed out."
	default:
		return "Route verification failed."
	}
}

func nonZeroTimePointer(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
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
