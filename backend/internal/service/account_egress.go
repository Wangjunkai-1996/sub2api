package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

const (
	EgressModeLegacy = "legacy"
	EgressModePool   = "pool"

	EgressIdentityStatusActive  = "active"
	EgressIdentityStatusRetired = "retired"

	EgressRouteKindProxy  = "proxy"
	EgressRouteKindDirect = "direct"

	EgressRouteStatePendingVerification = "pending_verification"
	EgressRouteStateActive              = "active"
	EgressRouteStateInactive            = "inactive"
	EgressRouteStateExpired             = "expired"
	EgressRouteStateIdentityMismatch    = "identity_mismatch"
	EgressRouteStateRetired             = "retired"

	AccountEgressBindingStatusActive   = "active"
	AccountEgressBindingStatusDraining = "draining"

	DefaultDirectEgressRuntimeScope = "default"
	MaxAccountEgressRoutes          = 32
	MaxBulkAccountEgressAccounts    = 1000
	MaxEgressVerifyBatchSize        = 32
	MaxEgressVerifyConcurrency      = 4

	AccountPoolOperationAppend  = "append"
	AccountPoolOperationRemove  = "remove"
	AccountPoolOperationReplace = "replace"

	EgressProbeReasonRouteNotFound      = "route_not_found"
	EgressProbeReasonRouteUnavailable   = "route_unavailable"
	EgressProbeReasonProbeFailed        = "probe_failed"
	EgressProbeReasonInvalidObservation = "invalid_observation"
	EgressProbeReasonRevisionConflict   = "revision_conflict"
	EgressProbeReasonPersistenceFailed  = "persistence_failed"
	EgressProbeReasonRequestCanceled    = "request_canceled"
)

var (
	ErrEgressRouteNotFound       = infraerrors.NotFound("EGRESS_ROUTE_NOT_FOUND", "egress route not found")
	ErrEgressRouteConflict       = infraerrors.Conflict("EGRESS_ROUTE_REVISION_CONFLICT", "egress route changed; verify it again")
	ErrEgressRouteInvalid        = infraerrors.BadRequest("EGRESS_ROUTE_INVALID", "egress route is invalid")
	ErrEgressPoolInvalid         = infraerrors.BadRequest("ACCOUNT_EGRESS_POOL_INVALID", "account egress pool is invalid")
	ErrEgressAccountUnsupported  = infraerrors.BadRequest("ACCOUNT_EGRESS_ACCOUNT_UNSUPPORTED", "egress pools are supported only for OpenAI OAuth accounts")
	ErrEgressPoolConflict        = infraerrors.Conflict("ACCOUNT_EGRESS_REVISION_CONFLICT", "account egress pool changed; reload it and try again")
	ErrEgressPoolVersionRequired = infraerrors.Conflict("EGRESS_POOL_VERSION_REQUIRED", "account uses an egress pool; reload it and update the pool contract")
	ErrEgressMutationFrozen      = infraerrors.New(http.StatusLocked, "ACCOUNT_EGRESS_MUTATION_FROZEN", "account egress mutations are temporarily frozen")
)

// EgressIdentity is the durable normalized public IP used as one capacity unit.
type EgressIdentity struct {
	ID        int64
	PublicIP  string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// EgressRoute is an internal route model. Proxy intentionally retains service
// credentials for transport consumers; admin DTOs must map to redacted fields.
type EgressRoute struct {
	ID                 int64
	Kind               string
	ProxyID            *int64
	RuntimeScope       *string
	ExpectedIdentityID *int64
	ExpectedIdentity   *EgressIdentity
	State              string
	LastObservedIP     *string
	LastProbedAt       *time.Time
	VerifiedAt         *time.Time
	Revision           int64
	LastError          *string
	CreatedAt          time.Time
	UpdatedAt          time.Time
	Proxy              *Proxy
}

// AccountEgressBinding is identified by its stable account_id:route_id pair.
type AccountEgressBinding struct {
	BindingID string
	AccountID int64
	RouteID   int64
	Position  int
	IsPrimary bool
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
	Route     *EgressRoute
}

func StableAccountEgressBindingID(accountID, routeID int64) string {
	return fmt.Sprintf("%d:%d", accountID, routeID)
}

// AccountEgressPoolConfigDomain is the resolved runtime view. For a shadow,
// SourceAccountID is its parent while AccountID and concurrency remain the
// shadow's own scheduling identity and capacity setting.
type AccountEgressPoolConfigDomain struct {
	AccountID            int64
	SourceAccountID      int64
	EgressMode           string
	EgressRevision       int64
	ConcurrencyPerEgress int
	PrimaryProxyID       *int64
	Bindings             []AccountEgressBinding
}

// AccountEgressAuthority is the writer-database fence consulted by lease
// refresh batches. Missing accounts are intentionally absent from the result.
type AccountEgressAuthority struct {
	AccountID int64
	Mode      string
	Revision  int64
}

type ReplaceAccountPoolInput struct {
	Mode                 string
	RouteIDs             []int64
	PrimaryRouteID       int64
	ConcurrencyPerEgress *int
	ExpectedRevision     *int64
}

// ApplyAccountPoolsInput mutates each selected account against its latest
// locked binding set. Bulk mutations deliberately do not accept a revision:
// the repository performs the read/modify/write inside one transaction.
type ApplyAccountPoolsInput struct {
	Operation            string
	RouteIDs             []int64
	PrimaryRouteID       *int64
	ConcurrencyPerEgress *int
}

type ConfirmEgressIdentityInput struct {
	RouteID          int64
	ExpectedRevision int64
	ObservedIP       string
}

type EgressProbeObservation struct {
	RouteID          int64
	ExpectedRevision int64
	ObservedIP       string
	LatencyMs        int64
	ObservedAt       time.Time
	ProbeError       string
}

// EgressProbeResult is safe to map into an admin DTO only after Route has been
// explicitly redacted. ReasonCode is stable and never includes proxy secrets.
type EgressProbeResult struct {
	RouteID    int64
	Success    bool
	ObservedIP string
	LatencyMs  int64
	ObservedAt time.Time
	ReasonCode string
	Route      *EgressRoute
}

// EgressRepository owns durable egress configuration and the transaction that
// mirrors the primary route to accounts.proxy_id and publishes scheduler outbox.
type EgressRepository interface {
	LoadAccountEgressAuthorities(ctx context.Context, accountIDs []int64) (map[int64]AccountEgressAuthority, error)
	ResolveAccountPool(ctx context.Context, accountID int64) (*AccountEgressPoolConfigDomain, error)
	ListAssignableRoutes(ctx context.Context) ([]EgressRoute, error)
	GetRoute(ctx context.Context, routeID int64) (*EgressRoute, error)
	EnsureProxyRoute(ctx context.Context, proxyID int64) (*EgressRoute, error)
	EnsureDirectRoute(ctx context.Context, runtimeScope string) (*EgressRoute, error)
	RecordProbeObservation(ctx context.Context, observation EgressProbeObservation) (*EgressRoute, error)
	ConfirmIdentity(ctx context.Context, input ConfirmEgressIdentityInput) (*EgressRoute, error)
	ReplaceAccountPool(ctx context.Context, accountID int64, input ReplaceAccountPoolInput) (*AccountEgressPoolConfigDomain, error)
	ReplaceAccountPools(ctx context.Context, accountIDs []int64, input ReplaceAccountPoolInput) error
	ApplyAccountPools(ctx context.Context, accountIDs []int64, input ApplyAccountPoolsInput) error
	SyncProxyRouteLifecycle(ctx context.Context, proxyID int64, proxyStatus string) error
}

type EgressService struct {
	repo   EgressRepository
	prober ProxyExitInfoProber
}

func NewEgressService(repo EgressRepository, prober ProxyExitInfoProber) *EgressService {
	return &EgressService{repo: repo, prober: prober}
}

func (s *EgressService) ResolveAccountPool(ctx context.Context, accountID int64) (*AccountEgressPoolConfigDomain, error) {
	if s == nil || s.repo == nil {
		return nil, ErrEgressRouteInvalid
	}
	return s.repo.ResolveAccountPool(ctx, accountID)
}

func (s *EgressService) ListAssignableRoutes(ctx context.Context) ([]EgressRoute, error) {
	if s == nil || s.repo == nil {
		return nil, ErrEgressRouteInvalid
	}
	return s.repo.ListAssignableRoutes(ctx)
}

func (s *EgressService) GetRoute(ctx context.Context, routeID int64) (*EgressRoute, error) {
	if s == nil || s.repo == nil || routeID <= 0 {
		return nil, ErrEgressRouteInvalid
	}
	return s.repo.GetRoute(ctx, routeID)
}

func (s *EgressService) EnsureProxyRoute(ctx context.Context, proxyID int64) (*EgressRoute, error) {
	if s == nil || s.repo == nil || proxyID <= 0 {
		return nil, ErrEgressRouteInvalid
	}
	return s.repo.EnsureProxyRoute(ctx, proxyID)
}

func (s *EgressService) EnsureDirectRoute(ctx context.Context, runtimeScope string) (*EgressRoute, error) {
	if s == nil || s.repo == nil {
		return nil, ErrEgressRouteInvalid
	}
	runtimeScope = strings.TrimSpace(runtimeScope)
	if runtimeScope == "" || len(runtimeScope) > 128 {
		return nil, ErrEgressRouteInvalid
	}
	return s.repo.EnsureDirectRoute(ctx, runtimeScope)
}

func (s *EgressService) RecordProbeObservation(ctx context.Context, observation EgressProbeObservation) (*EgressRoute, error) {
	if s == nil || s.repo == nil || observation.RouteID <= 0 || observation.ExpectedRevision <= 0 || observation.LatencyMs < 0 {
		return nil, ErrEgressRouteInvalid
	}
	observation.ProbeError = strings.TrimSpace(observation.ProbeError)
	if observation.ProbeError == "" {
		ip, err := canonicalPublicEgressIP(observation.ObservedIP)
		if err != nil {
			return nil, err
		}
		observation.ObservedIP = ip
	}
	if observation.ObservedAt.IsZero() {
		observation.ObservedAt = time.Now()
	}
	return s.repo.RecordProbeObservation(ctx, observation)
}

func (s *EgressService) ConfirmIdentity(ctx context.Context, input ConfirmEgressIdentityInput) (*EgressRoute, error) {
	if s == nil || s.repo == nil || input.RouteID <= 0 || input.ExpectedRevision <= 0 {
		return nil, ErrEgressRouteInvalid
	}
	ip, err := canonicalPublicEgressIP(input.ObservedIP)
	if err != nil {
		return nil, err
	}
	input.ObservedIP = ip
	return s.repo.ConfirmIdentity(ctx, input)
}

// ProbeRoutes performs real server-side exit probes without exposing proxy
// credentials to handlers. Results preserve input order and failures are
// isolated per route; only malformed batch requests return a top-level error.
func (s *EgressService) ProbeRoutes(ctx context.Context, routeIDs []int64) ([]EgressProbeResult, error) {
	if s == nil || s.repo == nil || s.prober == nil || !validEgressProbeRouteIDs(routeIDs) {
		return nil, ErrEgressRouteInvalid
	}

	results := make([]EgressProbeResult, len(routeIDs))
	sem := make(chan struct{}, MaxEgressVerifyConcurrency)
	var wg sync.WaitGroup
	for index, routeID := range routeIDs {
		wg.Add(1)
		go func(index int, routeID int64) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[index] = EgressProbeResult{
					RouteID:    routeID,
					LatencyMs:  -1,
					ObservedAt: time.Now(),
					ReasonCode: EgressProbeReasonRequestCanceled,
				}
				return
			}
			results[index] = s.probeRoute(ctx, routeID)
		}(index, routeID)
	}
	wg.Wait()
	return results, nil
}

func (s *EgressService) probeRoute(ctx context.Context, routeID int64) EgressProbeResult {
	result := EgressProbeResult{RouteID: routeID, LatencyMs: -1, ObservedAt: time.Now()}
	route, err := s.repo.GetRoute(ctx, routeID)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			result.ReasonCode = EgressProbeReasonRequestCanceled
		} else if errors.Is(err, ErrEgressRouteNotFound) {
			result.ReasonCode = EgressProbeReasonRouteNotFound
		} else {
			result.ReasonCode = EgressProbeReasonPersistenceFailed
		}
		return result
	}
	result.Route = route
	if !routeCanBeProbed(route, result.ObservedAt) {
		result.ReasonCode = EgressProbeReasonRouteUnavailable
		return result
	}

	proxyURL := ""
	if route.Kind == EgressRouteKindProxy {
		proxyURL = route.Proxy.URL()
	}
	exitInfo, latencyMs, probeErr := s.prober.ProbeProxy(ctx, proxyURL)
	result.LatencyMs = latencyMs
	if probeErr != nil {
		if errors.Is(probeErr, context.Canceled) || errors.Is(probeErr, context.DeadlineExceeded) {
			result.ReasonCode = EgressProbeReasonRequestCanceled
			return result
		}
		return s.persistProbeFailure(ctx, result, route, EgressProbeReasonProbeFailed)
	}
	if exitInfo == nil {
		return s.persistProbeFailure(ctx, result, route, EgressProbeReasonInvalidObservation)
	}
	observedIP, err := canonicalPublicEgressIP(exitInfo.IP)
	if err != nil {
		return s.persistProbeFailure(ctx, result, route, EgressProbeReasonInvalidObservation)
	}
	result.ObservedIP = observedIP
	updated, err := s.repo.RecordProbeObservation(ctx, EgressProbeObservation{
		RouteID:          route.ID,
		ExpectedRevision: route.Revision,
		ObservedIP:       observedIP,
		LatencyMs:        latencyMs,
		ObservedAt:       result.ObservedAt,
	})
	if err != nil {
		result.ReasonCode = egressProbePersistenceReason(err)
		return result
	}
	result.Success = true
	result.Route = updated
	return result
}

func (s *EgressService) persistProbeFailure(ctx context.Context, result EgressProbeResult, route *EgressRoute, reasonCode string) EgressProbeResult {
	updated, err := s.repo.RecordProbeObservation(ctx, EgressProbeObservation{
		RouteID:          route.ID,
		ExpectedRevision: route.Revision,
		LatencyMs:        result.LatencyMs,
		ObservedAt:       result.ObservedAt,
		ProbeError:       reasonCode,
	})
	if err != nil {
		result.ReasonCode = egressProbePersistenceReason(err)
		return result
	}
	result.Route = updated
	result.ReasonCode = reasonCode
	return result
}

func egressProbePersistenceReason(err error) string {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return EgressProbeReasonRequestCanceled
	}
	if errors.Is(err, ErrEgressRouteConflict) {
		return EgressProbeReasonRevisionConflict
	}
	return EgressProbeReasonPersistenceFailed
}

func routeCanBeProbed(route *EgressRoute, now time.Time) bool {
	if route == nil || route.State == EgressRouteStateExpired || route.State == EgressRouteStateRetired {
		return false
	}
	switch route.Kind {
	case EgressRouteKindDirect:
		return route.RuntimeScope != nil && strings.TrimSpace(*route.RuntimeScope) != ""
	case EgressRouteKindProxy:
		return route.Proxy != nil && route.Proxy.IsActive() && !route.Proxy.IsExpired(now)
	default:
		return false
	}
}

func (s *EgressService) ReplaceAccountPool(ctx context.Context, accountID int64, input ReplaceAccountPoolInput) (*AccountEgressPoolConfigDomain, error) {
	if s == nil || s.repo == nil || accountID <= 0 || !validReplaceAccountPoolInput(input) {
		return nil, ErrEgressPoolInvalid
	}
	if strings.TrimSpace(input.Mode) == "" {
		input.Mode = EgressModePool
	}
	return s.repo.ReplaceAccountPool(ctx, accountID, input)
}

func (s *EgressService) ReplaceAccountPools(ctx context.Context, accountIDs []int64, input ReplaceAccountPoolInput) error {
	if s == nil || s.repo == nil || len(accountIDs) == 0 || !validReplaceAccountPoolInput(input) {
		return ErrEgressPoolInvalid
	}
	if len(accountIDs) > MaxBulkAccountEgressAccounts {
		return ErrEgressPoolInvalid
	}
	if len(accountIDs) > 1 && input.ExpectedRevision != nil {
		return ErrEgressPoolInvalid
	}
	seenAccounts := make(map[int64]struct{}, len(accountIDs))
	for _, accountID := range accountIDs {
		if accountID <= 0 {
			return ErrEgressPoolInvalid
		}
		seenAccounts[accountID] = struct{}{}
	}
	if len(seenAccounts) == 0 || len(seenAccounts) > MaxBulkAccountEgressAccounts {
		return ErrEgressPoolInvalid
	}
	if strings.TrimSpace(input.Mode) == "" {
		input.Mode = EgressModePool
	}
	return s.repo.ReplaceAccountPools(ctx, accountIDs, input)
}

func (s *EgressService) ApplyAccountPools(ctx context.Context, accountIDs []int64, input ApplyAccountPoolsInput) error {
	if s == nil || s.repo == nil || !validApplyAccountPoolsInput(accountIDs, input) {
		return ErrEgressPoolInvalid
	}
	return s.repo.ApplyAccountPools(ctx, accountIDs, input)
}

func (s *EgressService) SyncProxyRouteLifecycle(ctx context.Context, proxyID int64, proxyStatus string) error {
	if s == nil || s.repo == nil || proxyID <= 0 || strings.TrimSpace(proxyStatus) == "" {
		return ErrEgressRouteInvalid
	}
	return s.repo.SyncProxyRouteLifecycle(ctx, proxyID, proxyStatus)
}

func validReplaceAccountPoolInput(input ReplaceAccountPoolInput) bool {
	mode := strings.TrimSpace(input.Mode)
	if mode == "" {
		mode = EgressModePool
	}
	if mode != EgressModeLegacy && mode != EgressModePool {
		return false
	}
	if mode == EgressModeLegacy && (len(input.RouteIDs) != 0 || input.PrimaryRouteID != 0) {
		return false
	}
	if len(input.RouteIDs) > MaxAccountEgressRoutes || (mode == EgressModePool && len(input.RouteIDs) == 0) {
		return false
	}
	seen := make(map[int64]struct{}, len(input.RouteIDs))
	primaryFound := false
	for _, routeID := range input.RouteIDs {
		if routeID <= 0 {
			return false
		}
		if _, duplicate := seen[routeID]; duplicate {
			return false
		}
		seen[routeID] = struct{}{}
		primaryFound = primaryFound || routeID == input.PrimaryRouteID
	}
	if len(input.RouteIDs) > 0 && !primaryFound {
		return false
	}
	if input.ConcurrencyPerEgress != nil && mode == EgressModePool && (*input.ConcurrencyPerEgress < 1 || *input.ConcurrencyPerEgress > 10000) {
		return false
	}
	if input.ExpectedRevision != nil && *input.ExpectedRevision <= 0 {
		return false
	}
	return true
}

// ValidateReplaceAccountPoolInput exposes the same structural validation used
// by EgressService to repository/transaction orchestrators without exporting
// the implementation details of the validator.
func ValidateReplaceAccountPoolInput(input ReplaceAccountPoolInput) bool {
	return validReplaceAccountPoolInput(input)
}

func validApplyAccountPoolsInput(accountIDs []int64, input ApplyAccountPoolsInput) bool {
	if len(accountIDs) == 0 || len(accountIDs) > MaxBulkAccountEgressAccounts {
		return false
	}
	seenAccounts := make(map[int64]struct{}, len(accountIDs))
	for _, accountID := range accountIDs {
		if accountID <= 0 {
			return false
		}
		seenAccounts[accountID] = struct{}{}
	}
	if len(seenAccounts) == 0 {
		return false
	}

	switch strings.TrimSpace(input.Operation) {
	case AccountPoolOperationAppend, AccountPoolOperationRemove, AccountPoolOperationReplace:
	default:
		return false
	}
	if len(input.RouteIDs) > MaxAccountEgressRoutes {
		return false
	}
	// An empty route list is a deliberate concurrency-only mutation for
	// append/remove. It leaves the current binding set untouched. Replace
	// still needs an explicit non-empty route set, and a no-op mutation without
	// a concurrency value is rejected to avoid silently accepting an empty
	// request.
	if len(input.RouteIDs) == 0 {
		if input.ConcurrencyPerEgress == nil || strings.TrimSpace(input.Operation) == AccountPoolOperationReplace {
			return false
		}
	}
	seenRoutes := make(map[int64]struct{}, len(input.RouteIDs))
	for _, routeID := range input.RouteIDs {
		if routeID <= 0 {
			return false
		}
		if _, duplicate := seenRoutes[routeID]; duplicate {
			return false
		}
		seenRoutes[routeID] = struct{}{}
	}
	if input.PrimaryRouteID != nil && *input.PrimaryRouteID <= 0 {
		return false
	}
	if input.ConcurrencyPerEgress != nil && (*input.ConcurrencyPerEgress < 1 || *input.ConcurrencyPerEgress > 10000) {
		return false
	}
	return true
}

func validEgressProbeRouteIDs(routeIDs []int64) bool {
	if len(routeIDs) == 0 || len(routeIDs) > MaxEgressVerifyBatchSize {
		return false
	}
	seen := make(map[int64]struct{}, len(routeIDs))
	for _, routeID := range routeIDs {
		if routeID <= 0 {
			return false
		}
		if _, duplicate := seen[routeID]; duplicate {
			return false
		}
		seen[routeID] = struct{}{}
	}
	return true
}

func canonicalPublicEgressIP(raw string) (string, error) {
	addr, err := netip.ParseAddr(strings.TrimSpace(raw))
	if err != nil {
		return "", ErrEgressRouteInvalid
	}
	addr = addr.Unmap()
	if !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() || addr.IsMulticast() || addr.IsUnspecified() {
		return "", ErrEgressRouteInvalid
	}
	return addr.String(), nil
}
