package service

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

const (
	legacyAccountEgressLeaseTTL              = 90 * time.Second
	legacyAccountEgressLeaseRefreshInterval  = 25 * time.Second
	legacyAccountEgressLeaseOperationTimeout = 3 * time.Second
)

// LegacyAccountEgressAdmission fences a rollout-off admission to the exact
// primary route that the request was admitted against. It is request-local and
// contains no proxy URL or credentials.
type LegacyAccountEgressAdmission struct {
	AccountID     int64
	BindingID     string
	RouteID       int64
	IdentityID    string
	ConfigVersion int64
	RouteKind     string
	ProxyID       *int64
	Lease         *LegacyAccountEgressLease
	leaseParent   context.Context
}

func (a *LegacyAccountEgressAdmission) clone() *LegacyAccountEgressAdmission {
	if a == nil {
		return nil
	}
	cloned := *a
	cloned.ProxyID = cloneAccountInt64(a.ProxyID)
	return &cloned
}

type legacyAccountEgressAdmissionContextKey struct{}

func contextWithLegacyAccountEgressAdmission(ctx context.Context, admission *LegacyAccountEgressAdmission) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if admission == nil {
		return ctx
	}
	// WaitPlan derives a short-lived timeout context from the request. The Redis
	// acquire must observe that timeout, while the resulting lease must survive
	// until the original request ends.
	if admission.leaseParent == nil {
		admission.leaseParent = ctx
	}
	return context.WithValue(ctx, legacyAccountEgressAdmissionContextKey{}, admission)
}

func (a *LegacyAccountEgressAdmission) leaseContext(fallback context.Context) context.Context {
	if a != nil && a.leaseParent != nil {
		return a.leaseParent
	}
	if fallback != nil {
		return fallback
	}
	return context.Background()
}

func legacyAccountEgressAdmissionFromContext(ctx context.Context, accountID int64) *LegacyAccountEgressAdmission {
	if ctx == nil || accountID <= 0 {
		return nil
	}
	admission, _ := ctx.Value(legacyAccountEgressAdmissionContextKey{}).(*LegacyAccountEgressAdmission)
	if admission == nil || admission.AccountID != accountID {
		return nil
	}
	return admission
}

func resolveLegacyAccountEgressAdmission(account *Account) (*LegacyAccountEgressAdmission, error) {
	if !accountSupportsEgressPoolRuntime(account) || account.ID <= 0 || account.EgressMode != EgressModePool {
		return nil, ErrAccountEgressConfigStale
	}
	var primary *AccountEgressBinding
	for i := range account.EgressBindings {
		binding := &account.EgressBindings[i]
		if !binding.IsPrimary {
			continue
		}
		if primary != nil {
			return nil, ErrAccountEgressConfigStale
		}
		primary = binding
	}
	if primary == nil || primary.BindingID == "" || primary.RouteID <= 0 ||
		primary.Status != AccountEgressBindingStatusActive || primary.Route == nil ||
		primary.Route.ID != primary.RouteID || primary.Route.State != EgressRouteStateActive ||
		primary.Route.ExpectedIdentity == nil || primary.Route.ExpectedIdentity.ID <= 0 ||
		primary.Route.ExpectedIdentity.Status != EgressIdentityStatusActive {
		return nil, ErrAccountEgressConfigStale
	}

	admission := &LegacyAccountEgressAdmission{
		AccountID:     account.ID,
		BindingID:     primary.BindingID,
		RouteID:       primary.RouteID,
		IdentityID:    strconv.FormatInt(primary.Route.ExpectedIdentity.ID, 10),
		ConfigVersion: accountEgressRuntimeVersion(account),
		RouteKind:     primary.Route.Kind,
	}
	switch primary.Route.Kind {
	case EgressRouteKindDirect:
		if primary.Route.RuntimeScope == nil || strings.TrimSpace(*primary.Route.RuntimeScope) != DefaultDirectEgressRuntimeScope ||
			account.ProxyID != nil || account.Proxy != nil {
			return nil, ErrAccountEgressConfigStale
		}
	case EgressRouteKindProxy:
		if primary.Route.ProxyID == nil || account.ProxyID == nil || *primary.Route.ProxyID != *account.ProxyID {
			return nil, ErrAccountEgressConfigStale
		}
		if account.Proxy != nil && account.Proxy.ID != *primary.Route.ProxyID {
			return nil, ErrAccountEgressConfigStale
		}
		if primary.Route.Proxy != nil && primary.Route.Proxy.ID != *primary.Route.ProxyID {
			return nil, ErrAccountEgressConfigStale
		}
		admission.ProxyID = cloneAccountInt64(primary.Route.ProxyID)
	default:
		return nil, ErrAccountEgressConfigStale
	}
	if admission.ConfigVersion <= 0 {
		return nil, ErrAccountEgressConfigStale
	}
	return admission, nil
}

func legacyAccountEgressAdmissionMatches(left, right *LegacyAccountEgressAdmission) bool {
	if left == nil || right == nil || left.AccountID != right.AccountID || left.BindingID != right.BindingID ||
		left.RouteID != right.RouteID || left.IdentityID != right.IdentityID ||
		left.ConfigVersion != right.ConfigVersion || left.RouteKind != right.RouteKind {
		return false
	}
	if left.ProxyID == nil || right.ProxyID == nil {
		return left.ProxyID == nil && right.ProxyID == nil
	}
	return *left.ProxyID == *right.ProxyID
}

// WithLegacyAccountEgressAdmission validates an authoritative account read and
// reapplies only the transport admitted by the marker. A changed primary route
// fails closed instead of silently sending the request through a different IP.
func WithLegacyAccountEgressAdmission(account *Account, admission *LegacyAccountEgressAdmission) (*Account, error) {
	resolved, err := resolveLegacyAccountEgressAdmission(account)
	if err != nil || !legacyAccountEgressAdmissionMatches(resolved, admission) {
		return nil, ErrAccountEgressConfigStale
	}
	primary := findAccountEgressBindingByID(account, admission.BindingID, admission.RouteID)
	if primary == nil || primary.Route == nil {
		return nil, ErrAccountEgressConfigStale
	}

	cloned := account.CloneForRequest()
	cloned.LegacyEgressAdmission = admission.clone()
	switch admission.RouteKind {
	case EgressRouteKindDirect:
		cloned.ProxyID = nil
		cloned.Proxy = nil
	case EgressRouteKindProxy:
		proxy := primary.Route.Proxy
		if proxy == nil || admission.ProxyID == nil || proxy.ID != *admission.ProxyID ||
			!proxy.IsActive() || proxy.IsExpired(time.Now()) {
			return nil, ErrAccountEgressConfigStale
		}
		proxyID := proxy.ID
		proxyCopy := *proxy
		cloned.ProxyID = &proxyID
		cloned.Proxy = &proxyCopy
	default:
		return nil, ErrAccountEgressConfigStale
	}
	return cloned, nil
}

func findAccountEgressBindingByID(account *Account, bindingID string, routeID int64) *AccountEgressBinding {
	if account == nil {
		return nil
	}
	for i := range account.EgressBindings {
		binding := &account.EgressBindings[i]
		if binding.BindingID == bindingID && binding.RouteID == routeID {
			return binding
		}
	}
	return nil
}

// LegacyAccountEgressLease keeps the admission-time identity mirror alive for
// long HTTP/SSE requests and cancels its context when ownership is lost.
type LegacyAccountEgressLease struct {
	cache      AccountEgressLegacySlotCache
	accountID  int64
	requestID  string
	identityID string

	ctx    context.Context
	cancel context.CancelCauseFunc

	refreshInterval  time.Duration
	ttl              time.Duration
	operationTimeout time.Duration

	mu            sync.Mutex
	lastConfirmed time.Time
	stopOnce      sync.Once
	releaseOnce   sync.Once
	stopCh        chan struct{}
	doneCh        chan struct{}
	contextStop   func() bool
}

func newLegacyAccountEgressLease(
	requestCtx context.Context,
	cache AccountEgressLegacySlotCache,
	accountID int64,
	requestID string,
	identityID string,
) *LegacyAccountEgressLease {
	return newLegacyAccountEgressLeaseWithTiming(
		requestCtx,
		cache,
		accountID,
		requestID,
		identityID,
		legacyAccountEgressLeaseTTL,
		legacyAccountEgressLeaseRefreshInterval,
		legacyAccountEgressLeaseOperationTimeout,
	)
}

func newLegacyAccountEgressLeaseWithTiming(
	requestCtx context.Context,
	cache AccountEgressLegacySlotCache,
	accountID int64,
	requestID string,
	identityID string,
	ttl time.Duration,
	refreshInterval time.Duration,
	operationTimeout time.Duration,
) *LegacyAccountEgressLease {
	if requestCtx == nil {
		requestCtx = context.Background()
	}
	leaseCtx, cancel := context.WithCancelCause(context.Background())
	lease := &LegacyAccountEgressLease{
		cache:            cache,
		accountID:        accountID,
		requestID:        requestID,
		identityID:       identityID,
		ctx:              leaseCtx,
		cancel:           cancel,
		refreshInterval:  refreshInterval,
		ttl:              ttl,
		operationTimeout: operationTimeout,
		lastConfirmed:    time.Now(),
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
	}
	lease.contextStop = context.AfterFunc(requestCtx, lease.Release)
	go lease.refreshLoop()
	return lease
}

func (l *LegacyAccountEgressLease) Context() context.Context {
	if l == nil || l.ctx == nil {
		return context.Background()
	}
	return l.ctx
}

func (l *LegacyAccountEgressLease) Release() {
	if l == nil {
		return
	}
	l.stopOnce.Do(func() { close(l.stopCh) })
	if l.contextStop != nil {
		l.contextStop()
	}
	l.releaseOnce.Do(func() {
		if l.cache != nil {
			ctx, cancel := context.WithTimeout(context.Background(), l.operationTimeout)
			err := l.cache.ReleaseAccountSlotForEgress(ctx, l.accountID, l.requestID, l.identityID)
			cancel()
			if err != nil {
				logger.L().Warn("account_egress_legacy_lease_release_failed",
					zap.Int64("account_id", l.accountID),
					zap.String("identity_id", l.identityID),
					zap.Error(err),
				)
			}
		}
		l.cancel(context.Canceled)
	})
}

func (l *LegacyAccountEgressLease) refreshLoop() {
	defer close(l.doneCh)
	ticker := time.NewTicker(l.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-l.stopCh:
			return
		case now := <-ticker.C:
			if !l.refresh(now) {
				return
			}
		}
	}
}

func (l *LegacyAccountEgressLease) refresh(now time.Time) bool {
	ctx, cancel := context.WithTimeout(context.Background(), l.operationTimeout)
	owned, err := l.cache.RefreshAccountSlotForEgress(ctx, l.accountID, l.requestID, l.identityID)
	cancel()
	if err == nil && owned {
		l.mu.Lock()
		l.lastConfirmed = now
		l.mu.Unlock()
		return true
	}
	if err == nil && !owned {
		logger.L().Error("account_egress_legacy_lease_lost",
			zap.Int64("account_id", l.accountID),
			zap.String("identity_id", l.identityID),
			zap.Error(ErrAccountEgressLeaseLost),
		)
		l.cancel(ErrAccountEgressLeaseLost)
		l.stopOnce.Do(func() { close(l.stopCh) })
		return false
	}
	l.mu.Lock()
	lastConfirmed := l.lastConfirmed
	l.mu.Unlock()
	logger.L().Warn("account_egress_legacy_lease_refresh_failed",
		zap.Int64("account_id", l.accountID),
		zap.String("identity_id", l.identityID),
		zap.Duration("unconfirmed_for", now.Sub(lastConfirmed)),
		zap.Error(err),
	)
	if now.Sub(lastConfirmed) >= l.ttl {
		logger.L().Error("account_egress_legacy_lease_lost",
			zap.Int64("account_id", l.accountID),
			zap.String("identity_id", l.identityID),
			zap.Duration("unconfirmed_for", now.Sub(lastConfirmed)),
			zap.Error(err),
		)
		l.cancel(errors.Join(ErrAccountEgressLeaseLost, err))
		l.stopOnce.Do(func() { close(l.stopCh) })
		return false
	}
	return true
}
