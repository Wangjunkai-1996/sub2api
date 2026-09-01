package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type requiredAccountEgressBindingContextKey struct{}

// WithRequiredAccountEgressBinding carries a continuation's route fence into
// the existing scheduler admission path. It contains only the stable binding
// identifier; transport credentials remain on the request-local Account.
func WithRequiredAccountEgressBinding(ctx context.Context, bindingID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if bindingID = strings.TrimSpace(bindingID); bindingID == "" {
		return ctx
	}
	return context.WithValue(ctx, requiredAccountEgressBindingContextKey{}, bindingID)
}

func RequiredAccountEgressBindingFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	bindingID, _ := ctx.Value(requiredAccountEgressBindingContextKey{}).(string)
	return strings.TrimSpace(bindingID)
}

// CloneForRequest returns an account value that callers may safely decorate
// with a selected egress without mutating a scheduler or repository snapshot.
func (a *Account) CloneForRequest() *Account {
	if a == nil {
		return nil
	}
	cloned := *a
	cloned.Credentials = cloneStringAnyMap(a.Credentials)
	cloned.Extra = cloneStringAnyMap(a.Extra)
	cloned.GroupIDs = append([]int64(nil), a.GroupIDs...)
	cloned.AccountGroups = append([]AccountGroup(nil), a.AccountGroups...)
	cloned.Groups = append([]*Group(nil), a.Groups...)
	cloned.EgressBindings = cloneAccountEgressBindings(a.EgressBindings)
	if a.Proxy != nil {
		proxy := *a.Proxy
		cloned.Proxy = &proxy
	}
	cloned.SelectedEgress = nil
	return &cloned
}

func cloneAccountEgressBindings(bindings []AccountEgressBinding) []AccountEgressBinding {
	if bindings == nil {
		return nil
	}
	cloned := make([]AccountEgressBinding, len(bindings))
	for i := range bindings {
		cloned[i] = bindings[i]
		cloned[i].Route = cloneAccountEgressRoute(bindings[i].Route)
	}
	return cloned
}

func cloneAccountEgressRoute(route *EgressRoute) *EgressRoute {
	if route == nil {
		return nil
	}
	cloned := *route
	cloned.ProxyID = cloneAccountInt64(route.ProxyID)
	cloned.RuntimeScope = cloneAccountString(route.RuntimeScope)
	cloned.ExpectedIdentityID = cloneAccountInt64(route.ExpectedIdentityID)
	cloned.LastObservedIP = cloneAccountString(route.LastObservedIP)
	cloned.LastError = cloneAccountString(route.LastError)
	if route.ExpectedIdentity != nil {
		identity := *route.ExpectedIdentity
		cloned.ExpectedIdentity = &identity
	}
	if route.Proxy != nil {
		proxy := *route.Proxy
		cloned.Proxy = &proxy
	}
	return &cloned
}

func cloneAccountInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneAccountString(value *string) *string {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func cloneStringAnyMap(source map[string]any) map[string]any {
	if source == nil {
		return nil
	}
	cloned := make(map[string]any, len(source))
	for key, value := range source {
		cloned[key] = value
	}
	return cloned
}

func accountSupportsEgressPoolRuntime(account *Account) bool {
	return account != nil && account.Platform == PlatformOpenAI && account.Type == AccountTypeOAuth
}

// AccountEgressPoolConfigForRuntime projects only non-retired, identity-known
// bindings. Unhealthy bindings stay in the config as ineligible candidates so
// their state participates in the digest and cannot be silently reintroduced.
func AccountEgressPoolConfigForRuntime(account *Account, maxWaiting int) (AccountEgressPoolConfig, error) {
	if !accountSupportsEgressPoolRuntime(account) || account.ID <= 0 || account.EgressMode != EgressModePool {
		return AccountEgressPoolConfig{}, ErrAccountEgressConfigStale
	}
	config := AccountEgressPoolConfig{
		AccountID:              account.ID,
		Version:                accountEgressRuntimeVersion(account),
		PerIdentityConcurrency: account.Concurrency,
		MaxWaiting:             maxWaiting,
		Candidates:             make([]AccountEgressCandidate, 0, len(account.EgressBindings)),
	}
	now := time.Now()
	for i := range account.EgressBindings {
		binding := &account.EgressBindings[i]
		route := binding.Route
		if route == nil || route.ExpectedIdentity == nil || route.ExpectedIdentity.ID <= 0 {
			continue
		}
		healthy := binding.Status == AccountEgressBindingStatusActive &&
			route.State == EgressRouteStateActive &&
			route.ExpectedIdentity.Status == EgressIdentityStatusActive
		if healthy {
			switch route.Kind {
			case EgressRouteKindDirect:
				healthy = route.RuntimeScope != nil && strings.TrimSpace(*route.RuntimeScope) != ""
			case EgressRouteKindProxy:
				healthy = route.ProxyID != nil && route.Proxy != nil && route.Proxy.IsActive() && !route.Proxy.IsExpired(now)
			default:
				healthy = false
			}
		}
		config.Candidates = append(config.Candidates, AccountEgressCandidate{
			BindingID:  binding.BindingID,
			RouteID:    binding.RouteID,
			IdentityID: strconv.FormatInt(route.ExpectedIdentity.ID, 10),
			Position:   binding.Position,
			Primary:    binding.IsPrimary,
			Healthy:    healthy,
		})
	}
	if err := config.Validate(); err != nil {
		return AccountEgressPoolConfig{}, fmt.Errorf("%w: %v", ErrAccountEgressConfigStale, err)
	}
	return config, nil
}

func accountEgressRuntimeVersion(account *Account) int64 {
	if account == nil {
		return 0
	}
	const routeFenceMask int64 = (1 << 31) - 1
	var routeFence int64
	for i := range account.EgressBindings {
		if route := account.EgressBindings[i].Route; route != nil && route.Revision > 0 {
			if routeFence > routeFenceMask-route.Revision {
				routeFence = routeFenceMask
			} else {
				routeFence += route.Revision
			}
		}
	}
	accountRevision := account.EgressRevision
	if accountRevision <= 0 {
		accountRevision = 1
	}
	if accountRevision > math.MaxInt64>>31 {
		return math.MaxInt64
	}
	return accountRevision<<31 | routeFence
}

func resolvedAccountEgressBinding(account *Account, resolved *ResolvedAccountEgress) (*AccountEgressBinding, error) {
	if account == nil || resolved == nil || resolved.Lease == nil {
		return nil, ErrAccountEgressConfigStale
	}
	if !accountSupportsEgressPoolRuntime(account) || account.EgressMode != EgressModePool ||
		resolved.ConfigVersion != accountEgressRuntimeVersion(account) {
		return nil, ErrAccountEgressConfigStale
	}
	var selected *AccountEgressBinding
	for i := range account.EgressBindings {
		binding := &account.EgressBindings[i]
		if binding.BindingID == resolved.BindingID && binding.RouteID == resolved.RouteID {
			selected = binding
			break
		}
	}
	if selected == nil || selected.Route == nil || selected.Route.ExpectedIdentity == nil {
		return nil, ErrAccountEgressConfigStale
	}
	if strconv.FormatInt(selected.Route.ExpectedIdentity.ID, 10) != resolved.IdentityID ||
		selected.Status != AccountEgressBindingStatusActive ||
		selected.Route.State != EgressRouteStateActive ||
		selected.Route.ExpectedIdentity.Status != EgressIdentityStatusActive {
		return nil, ErrAccountEgressConfigStale
	}
	if selected.Route.Kind == EgressRouteKindProxy {
		if selected.Route.ProxyID == nil {
			return nil, ErrAccountEgressConfigStale
		}
		// Scheduler projections intentionally redact proxy credentials. When a
		// proxy is hydrated it must be usable; WithResolvedAccountEgress performs
		// the final non-nil transport check against the authoritative account.
		if selected.Route.Proxy != nil &&
			(!selected.Route.Proxy.IsActive() || selected.Route.Proxy.IsExpired(time.Now())) {
			return nil, ErrAccountEgressConfigStale
		}
	}
	return selected, nil
}

// withResolvedAccountEgressSelection attaches only the allocator decision to a
// request-local account. Scheduler-cache projections intentionally contain no
// proxy credentials, so transport resolution is deferred until the final
// authoritative account read.
func withResolvedAccountEgressSelection(account *Account, resolved *ResolvedAccountEgress) (*Account, error) {
	if _, err := resolvedAccountEgressBinding(account, resolved); err != nil {
		return nil, err
	}
	cloned := account.CloneForRequest()
	cloned.SelectedEgress = resolved
	return cloned, nil
}

// WithResolvedAccountEgress validates the allocator result against the
// authoritative hydrated account and applies transport state only to a
// request-local clone.
func WithResolvedAccountEgress(account *Account, resolved *ResolvedAccountEgress) (*Account, error) {
	selected, err := resolvedAccountEgressBinding(account, resolved)
	if err != nil {
		return nil, err
	}

	cloned := account.CloneForRequest()
	cloned.SelectedEgress = resolved
	switch selected.Route.Kind {
	case EgressRouteKindDirect:
		cloned.ProxyID = nil
		cloned.Proxy = nil
	case EgressRouteKindProxy:
		if selected.Route.ProxyID == nil || selected.Route.Proxy == nil {
			return nil, ErrAccountEgressConfigStale
		}
		proxyID := *selected.Route.ProxyID
		proxy := *selected.Route.Proxy
		cloned.ProxyID = &proxyID
		cloned.Proxy = &proxy
	default:
		return nil, ErrAccountEgressConfigStale
	}
	return cloned, nil
}

// PreserveSelectedAccountEgress reapplies request-local routing after a fresh
// account read used by terminal policy checks.
func PreserveSelectedAccountEgress(latest, selected *Account) (*Account, error) {
	if selected == nil || selected.SelectedEgress == nil {
		if latest == nil {
			return nil, errors.New("latest account is nil")
		}
		return latest.CloneForRequest(), nil
	}
	// Scheduler cache projections intentionally omit route.Proxy. Reuse the
	// already-authoritative request transport only when the cached binding still
	// names the same proxy route; WithResolvedAccountEgress performs the final
	// revision/status/identity fence and still fails closed on any mismatch.
	latest = latest.CloneForRequest()
	if selectedBinding := findAccountEgressBinding(selected, selected.SelectedEgress); selectedBinding != nil &&
		selectedBinding.Route != nil && selectedBinding.Route.Kind == EgressRouteKindProxy &&
		selectedBinding.Route.Proxy != nil {
		for i := range latest.EgressBindings {
			binding := &latest.EgressBindings[i]
			if binding.BindingID != selected.SelectedEgress.BindingID || binding.RouteID != selected.SelectedEgress.RouteID ||
				binding.Route == nil || binding.Route.Kind != EgressRouteKindProxy || binding.Route.Proxy != nil ||
				binding.Route.ProxyID == nil || selectedBinding.Route.ProxyID == nil ||
				*binding.Route.ProxyID != *selectedBinding.Route.ProxyID {
				continue
			}
			proxy := *selectedBinding.Route.Proxy
			binding.Route.Proxy = &proxy
			break
		}
	}
	return WithResolvedAccountEgress(latest, selected.SelectedEgress)
}

func findAccountEgressBinding(account *Account, resolved *ResolvedAccountEgress) *AccountEgressBinding {
	if account == nil || resolved == nil {
		return nil
	}
	for i := range account.EgressBindings {
		binding := &account.EgressBindings[i]
		if binding.BindingID == resolved.BindingID && binding.RouteID == resolved.RouteID {
			return binding
		}
	}
	return nil
}

// OpenAIAccountControlProxyURL resolves account-scoped OAuth/control traffic
// without consuming business capacity. Pool accounts are fenced to their one
// verified primary route; an incomplete projection fails closed instead of
// falling back to direct.
func OpenAIAccountControlProxyURL(account *Account) (string, error) {
	if account == nil || account.Platform != PlatformOpenAI {
		return "", ErrAccountEgressNoRoute
	}
	if account.EgressMode != EgressModePool {
		if account.Proxy == nil {
			return "", nil
		}
		if !account.Proxy.IsActive() || account.Proxy.IsExpired(time.Now()) {
			return "", ErrAccountEgressNoRoute
		}
		return account.Proxy.URL(), nil
	}

	var primary *AccountEgressBinding
	for i := range account.EgressBindings {
		binding := &account.EgressBindings[i]
		if !binding.IsPrimary {
			continue
		}
		if primary != nil {
			return "", ErrAccountEgressConfigStale
		}
		primary = binding
	}
	if primary == nil || primary.Status != AccountEgressBindingStatusActive || primary.Route == nil ||
		primary.Route.State != EgressRouteStateActive || primary.Route.ExpectedIdentity == nil ||
		primary.Route.ExpectedIdentity.Status != EgressIdentityStatusActive {
		return "", ErrAccountEgressNoRoute
	}
	switch primary.Route.Kind {
	case EgressRouteKindDirect:
		if primary.Route.RuntimeScope == nil || strings.TrimSpace(*primary.Route.RuntimeScope) != DefaultDirectEgressRuntimeScope {
			return "", ErrAccountEgressNoRoute
		}
		return "", nil
	case EgressRouteKindProxy:
		if primary.Route.ProxyID == nil || primary.Route.Proxy == nil || !primary.Route.Proxy.IsActive() || primary.Route.Proxy.IsExpired(time.Now()) {
			return "", ErrAccountEgressNoRoute
		}
		return primary.Route.Proxy.URL(), nil
	default:
		return "", ErrAccountEgressNoRoute
	}
}

func ContextWithSelectedAccountEgress(ctx context.Context, account *Account) context.Context {
	if account == nil || account.SelectedEgress == nil {
		return ctx
	}
	selected := account.SelectedEgress
	ctx = WithHTTPUpstreamEgress(ctx, HTTPUpstreamEgress{
		BindingID:    selected.BindingID,
		RouteID:      selected.RouteID,
		IdentityID:   selected.IdentityID,
		PoolRevision: selected.ConfigVersion,
	})
	if selected.Lease == nil {
		return ctx
	}
	routed, cancel := context.WithCancelCause(ctx)
	leaseCtx := selected.Lease.Context()
	context.AfterFunc(leaseCtx, func() {
		cancel(context.Cause(leaseCtx))
	})
	return routed
}
