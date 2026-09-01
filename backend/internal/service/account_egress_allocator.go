package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

const (
	AccountEgressLeaseTTL             = 90 * time.Second
	AccountEgressLeaseRefreshInterval = 25 * time.Second
	accountEgressOperationTimeout     = 3 * time.Second
	accountEgressWaitPollInterval     = 50 * time.Millisecond
)

var (
	ErrAccountEgressCapacityFull = errors.New("account egress capacity full")
	ErrAccountEgressUnavailable  = errors.New("account egress allocator unavailable")
	ErrAccountEgressNoRoute      = errors.New("account egress route unavailable")
	ErrAccountEgressConfigStale  = errors.New("account egress config stale")
	ErrAccountEgressLeaseLost    = errors.New("account egress lease lost")
)

type AccountEgressStatus string

const (
	AccountEgressStatusAcquired                   AccountEgressStatus = "ACQUIRED"
	AccountEgressStatusFull                       AccountEgressStatus = "FULL"
	AccountEgressStatusNotQueueHead               AccountEgressStatus = "NOT_QUEUE_HEAD"
	AccountEgressStatusNoEligibleEgress           AccountEgressStatus = "NO_ELIGIBLE_EGRESS"
	AccountEgressStatusRequiredBindingUnavailable AccountEgressStatus = "REQUIRED_BINDING_UNAVAILABLE"
	AccountEgressStatusConfigStale                AccountEgressStatus = "CONFIG_STALE"
	AccountEgressStatusConfigUnavailable          AccountEgressStatus = "CONFIG_UNAVAILABLE"
	AccountEgressStatusExclusive                  AccountEgressStatus = "EXCLUSIVE"
	AccountEgressStatusQueueFull                  AccountEgressStatus = "QUEUE_FULL"
	AccountEgressStatusLegacyDraining             AccountEgressStatus = "LEGACY_DRAINING"
)

type AccountEgressConfigSyncStatus string

const (
	AccountEgressConfigSyncOK       AccountEgressConfigSyncStatus = "OK"
	AccountEgressConfigSyncStale    AccountEgressConfigSyncStatus = "CONFIG_STALE"
	AccountEgressConfigSyncConflict AccountEgressConfigSyncStatus = "CONFIG_CONFLICT"
)

// AccountEgressCandidate is the Redis-facing runtime projection of an egress
// route. Transport details such as proxy URLs deliberately stay outside it.
type AccountEgressCandidate struct {
	BindingID  string
	RouteID    int64
	IdentityID string
	Position   int
	Primary    bool
	Healthy    bool
}

type AccountEgressPoolConfig struct {
	AccountID              int64
	Version                int64
	PerIdentityConcurrency int
	MaxWaiting             int
	Candidates             []AccountEgressCandidate
}

func (c AccountEgressPoolConfig) Validate() error {
	if c.AccountID <= 0 {
		return errors.New("account egress account id must be positive")
	}
	if c.Version <= 0 {
		return errors.New("account egress config version must be positive")
	}
	if c.PerIdentityConcurrency <= 0 {
		return errors.New("account egress per-identity concurrency must be positive")
	}
	if c.MaxWaiting < 0 {
		return errors.New("account egress max waiting must not be negative")
	}
	if len(c.Candidates) == 0 || len(c.Candidates) > MaxAccountEgressRoutes {
		return fmt.Errorf("account egress config must contain between 1 and %d candidates", MaxAccountEgressRoutes)
	}

	bindingIDs := make(map[string]struct{}, len(c.Candidates))
	routeIDs := make(map[int64]struct{}, len(c.Candidates))
	positions := make(map[int]struct{}, len(c.Candidates))
	primaryCount := 0
	for i, candidate := range c.Candidates {
		if strings.TrimSpace(candidate.BindingID) == "" {
			return fmt.Errorf("account egress candidate %d has an empty binding id", i)
		}
		if candidate.RouteID <= 0 {
			return fmt.Errorf("account egress candidate %q has an invalid route id", candidate.BindingID)
		}
		if strings.TrimSpace(candidate.IdentityID) == "" {
			return fmt.Errorf("account egress candidate %q has an empty identity id", candidate.BindingID)
		}
		if candidate.Position < 0 {
			return fmt.Errorf("account egress candidate %q has a negative position", candidate.BindingID)
		}
		if _, exists := bindingIDs[candidate.BindingID]; exists {
			return fmt.Errorf("duplicate account egress binding id %q", candidate.BindingID)
		}
		if _, exists := routeIDs[candidate.RouteID]; exists {
			return fmt.Errorf("duplicate account egress route id %d", candidate.RouteID)
		}
		if _, exists := positions[candidate.Position]; exists {
			return fmt.Errorf("duplicate account egress position %d", candidate.Position)
		}
		bindingIDs[candidate.BindingID] = struct{}{}
		routeIDs[candidate.RouteID] = struct{}{}
		positions[candidate.Position] = struct{}{}
		if candidate.Primary {
			primaryCount++
		}
	}
	if primaryCount != 1 {
		return errors.New("account egress config must have exactly one primary binding")
	}
	return nil
}

func (c AccountEgressPoolConfig) SortedCandidates() []AccountEgressCandidate {
	candidates := append([]AccountEgressCandidate(nil), c.Candidates...)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].Position != candidates[j].Position {
			return candidates[i].Position < candidates[j].Position
		}
		return candidates[i].BindingID < candidates[j].BindingID
	})
	return candidates
}

func (c AccountEgressPoolConfig) Digest() (string, error) {
	if err := c.Validate(); err != nil {
		return "", err
	}
	canonical := struct {
		AccountID              int64
		PerIdentityConcurrency int
		MaxWaiting             int
		Candidates             []AccountEgressCandidate
	}{
		AccountID:              c.AccountID,
		PerIdentityConcurrency: c.PerIdentityConcurrency,
		MaxWaiting:             c.MaxWaiting,
		Candidates:             c.SortedCandidates(),
	}
	encoded, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("marshal account egress config digest: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func (c AccountEgressPoolConfig) EffectiveCapacity() int {
	identities := make(map[string]struct{}, len(c.Candidates))
	for _, candidate := range c.Candidates {
		if candidate.Healthy {
			identities[candidate.IdentityID] = struct{}{}
		}
	}
	return len(identities) * c.PerIdentityConcurrency
}

func (c AccountEgressPoolConfig) Candidate(bindingID string) (AccountEgressCandidate, bool) {
	for _, candidate := range c.Candidates {
		if candidate.BindingID == bindingID {
			return candidate, true
		}
	}
	return AccountEgressCandidate{}, false
}

type AccountEgressAcquireRequest struct {
	Config             AccountEgressPoolConfig
	LeaseID            string
	RequiredBindingID  string
	PreferredBindingID string
}

type AccountEgressCacheAcquireRequest struct {
	AccountEgressAcquireRequest
	LeaseTTL  time.Duration
	WaiterTTL time.Duration
}

type AccountEgressAcquireResult struct {
	Status            AccountEgressStatus
	LeaseID           string
	BindingID         string
	RouteID           int64
	IdentityID        string
	ActiveTotal       int
	WaitingCount      int
	EffectiveCapacity int
	ConfigVersion     int64
	RedisTime         time.Time
}

type AccountEgressLeaseRef struct {
	AccountID     int64
	ID            string
	BindingID     string
	IdentityID    string
	ConfigVersion int64
}

func (r AccountEgressLeaseRef) Key() string {
	return strconv.FormatInt(r.AccountID, 10) + ":" + r.ID
}

type AccountEgressLoadInfo struct {
	AccountID         int64
	Status            AccountEgressStatus
	ActiveTotal       int
	IdentityLoads     map[string]int
	WaitingCount      int
	EffectiveCapacity int
	LoadRate          int
	ConfigVersion     int64
}

type AccountEgressCache interface {
	SyncAccountEgressConfigs(context.Context, []AccountEgressPoolConfig) (map[int64]AccountEgressConfigSyncStatus, error)
	AcquireAccountEgress(context.Context, AccountEgressCacheAcquireRequest) (AccountEgressAcquireResult, error)
	RemoveAccountEgressWaiter(context.Context, int64, string) error
	RefreshAccountEgressLeases(context.Context, []AccountEgressLeaseRef, time.Duration) (map[string]bool, error)
	ReleaseAccountEgressLease(context.Context, AccountEgressLeaseRef) error
	GetAccountEgressLoadsBatch(context.Context, []AccountEgressPoolConfig, time.Duration, time.Duration) (map[int64]AccountEgressLoadInfo, error)
}

type ResolvedAccountEgress struct {
	BindingID         string
	RouteID           int64
	IdentityID        string
	Candidate         AccountEgressCandidate
	Lease             *AccountEgressLease
	ActiveTotal       int
	EffectiveCapacity int
	ConfigVersion     int64
}

type accountEgressLeaseState struct {
	lease         *AccountEgressLease
	lastConfirmed time.Time
	nextRefresh   time.Time
}

type AccountEgressAllocator struct {
	cache            AccountEgressCache
	leaseTTL         time.Duration
	refreshInterval  time.Duration
	operationTimeout time.Duration
	now              func() time.Time

	mu        sync.Mutex
	leases    map[string]*accountEgressLeaseState
	started   bool
	closed    bool
	closeCh   chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
}

func NewAccountEgressAllocator(cache AccountEgressCache) *AccountEgressAllocator {
	return newAccountEgressAllocatorWithTiming(
		cache,
		AccountEgressLeaseTTL,
		AccountEgressLeaseRefreshInterval,
		accountEgressOperationTimeout,
		time.Now,
	)
}

func newAccountEgressAllocatorWithTiming(
	cache AccountEgressCache,
	leaseTTL time.Duration,
	refreshInterval time.Duration,
	operationTimeout time.Duration,
	now func() time.Time,
) *AccountEgressAllocator {
	if leaseTTL <= 0 {
		leaseTTL = AccountEgressLeaseTTL
	}
	if refreshInterval <= 0 {
		refreshInterval = AccountEgressLeaseRefreshInterval
	}
	if operationTimeout <= 0 {
		operationTimeout = accountEgressOperationTimeout
	}
	if now == nil {
		now = time.Now
	}
	return &AccountEgressAllocator{
		cache:            cache,
		leaseTTL:         leaseTTL,
		refreshInterval:  refreshInterval,
		operationTimeout: operationTimeout,
		now:              now,
		leases:           make(map[string]*accountEgressLeaseState),
		closeCh:          make(chan struct{}),
		doneCh:           make(chan struct{}),
	}
}

func (a *AccountEgressAllocator) Acquire(ctx context.Context, request AccountEgressAcquireRequest) (*ResolvedAccountEgress, error) {
	if a == nil || a.cache == nil {
		return nil, ErrAccountEgressUnavailable
	}
	if err := request.Config.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAccountEgressConfigStale, err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if request.LeaseID == "" {
		request.LeaseID = generateRequestID()
	}

	syncResult, err := a.cache.SyncAccountEgressConfigs(ctx, []AccountEgressPoolConfig{request.Config})
	if err != nil {
		return nil, fmt.Errorf("%w: sync config: %v", ErrAccountEgressUnavailable, err)
	}
	if syncResult[request.Config.AccountID] != AccountEgressConfigSyncOK {
		return nil, ErrAccountEgressConfigStale
	}

	defer a.removeWaiter(request.Config.AccountID, request.LeaseID)
	var result AccountEgressAcquireResult
	for {
		result, err = a.cache.AcquireAccountEgress(ctx, AccountEgressCacheAcquireRequest{
			AccountEgressAcquireRequest: request,
			LeaseTTL:                    a.leaseTTL,
			WaiterTTL:                   2 * time.Minute,
		})
		if err != nil {
			return nil, fmt.Errorf("%w: acquire: %v", ErrAccountEgressUnavailable, err)
		}
		if result.Status == AccountEgressStatusAcquired {
			break
		}
		if request.Config.MaxWaiting <= 0 || !accountEgressStatusCanWait(result.Status) {
			return nil, accountEgressStatusError(result.Status)
		}

		timer := time.NewTimer(accountEgressWaitPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return nil, fmt.Errorf("%w: wait deadline exceeded", ErrAccountEgressCapacityFull)
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	candidate, ok := request.Config.Candidate(result.BindingID)
	if !ok || candidate.RouteID != result.RouteID || candidate.IdentityID != result.IdentityID || !candidate.Healthy {
		ref := AccountEgressLeaseRef{
			AccountID:     request.Config.AccountID,
			ID:            result.LeaseID,
			BindingID:     result.BindingID,
			IdentityID:    result.IdentityID,
			ConfigVersion: result.ConfigVersion,
		}
		releaseCtx, cancel := context.WithTimeout(context.Background(), a.operationTimeout)
		_ = a.cache.ReleaseAccountEgressLease(releaseCtx, ref)
		cancel()
		return nil, ErrAccountEgressConfigStale
	}

	leaseCtx, cancel := context.WithCancelCause(ctx)
	lease := &AccountEgressLease{
		ID:            result.LeaseID,
		BindingID:     result.BindingID,
		IdentityID:    result.IdentityID,
		ConfigVersion: result.ConfigVersion,
		accountID:     request.Config.AccountID,
		ctx:           leaseCtx,
		cancel:        cancel,
		allocator:     a,
	}
	if !a.register(lease) {
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), a.operationTimeout)
		_ = a.cache.ReleaseAccountEgressLease(releaseCtx, lease.ref())
		releaseCancel()
		return nil, ErrAccountEgressUnavailable
	}
	lease.contextStop = context.AfterFunc(ctx, lease.Release)
	return &ResolvedAccountEgress{
		BindingID:         result.BindingID,
		RouteID:           result.RouteID,
		IdentityID:        result.IdentityID,
		Candidate:         candidate,
		Lease:             lease,
		ActiveTotal:       result.ActiveTotal,
		EffectiveCapacity: result.EffectiveCapacity,
		ConfigVersion:     result.ConfigVersion,
	}, nil
}

func accountEgressStatusError(status AccountEgressStatus) error {
	switch status {
	case AccountEgressStatusFull, AccountEgressStatusNotQueueHead, AccountEgressStatusExclusive, AccountEgressStatusQueueFull, AccountEgressStatusLegacyDraining:
		return fmt.Errorf("%w: %s", ErrAccountEgressCapacityFull, status)
	case AccountEgressStatusNoEligibleEgress, AccountEgressStatusRequiredBindingUnavailable:
		return fmt.Errorf("%w: %s", ErrAccountEgressNoRoute, status)
	case AccountEgressStatusConfigStale, AccountEgressStatusConfigUnavailable:
		return fmt.Errorf("%w: %s", ErrAccountEgressConfigStale, status)
	default:
		return fmt.Errorf("%w: unexpected status %q", ErrAccountEgressUnavailable, status)
	}
}

func accountEgressStatusCanWait(status AccountEgressStatus) bool {
	switch status {
	case AccountEgressStatusFull, AccountEgressStatusNotQueueHead, AccountEgressStatusExclusive, AccountEgressStatusLegacyDraining:
		return true
	default:
		return false
	}
}

func (a *AccountEgressAllocator) removeWaiter(accountID int64, leaseID string) {
	if a == nil || a.cache == nil || accountID <= 0 || leaseID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), a.operationTimeout)
	defer cancel()
	if err := a.cache.RemoveAccountEgressWaiter(ctx, accountID, leaseID); err != nil {
		logger.L().Warn("account_egress_waiter_remove_failed",
			zap.Int64("account_id", accountID),
			zap.String("lease_id", leaseID),
			zap.Error(err),
		)
	}
}

func (a *AccountEgressAllocator) GetAccountEgressLoads(
	ctx context.Context,
	configs []AccountEgressPoolConfig,
) (map[int64]AccountEgressLoadInfo, error) {
	if a == nil || a.cache == nil {
		return nil, ErrAccountEgressUnavailable
	}
	for _, config := range configs {
		if err := config.Validate(); err != nil {
			return nil, fmt.Errorf("%w: account %d: %v", ErrAccountEgressConfigStale, config.AccountID, err)
		}
	}
	if len(configs) == 0 {
		return map[int64]AccountEgressLoadInfo{}, nil
	}
	syncResult, err := a.cache.SyncAccountEgressConfigs(ctx, configs)
	if err != nil {
		return nil, fmt.Errorf("%w: sync configs: %v", ErrAccountEgressUnavailable, err)
	}
	loads, err := a.cache.GetAccountEgressLoadsBatch(ctx, configs, a.leaseTTL, 2*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("%w: load batch: %v", ErrAccountEgressUnavailable, err)
	}
	for _, config := range configs {
		if syncResult[config.AccountID] != AccountEgressConfigSyncOK {
			load := loads[config.AccountID]
			load.AccountID = config.AccountID
			load.Status = AccountEgressStatusConfigStale
			load.ConfigVersion = config.Version
			loads[config.AccountID] = load
		}
	}
	return loads, nil
}

func (a *AccountEgressAllocator) register(lease *AccountEgressLease) bool {
	now := a.now()
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		lease.markLost()
		return false
	}
	if _, exists := a.leases[lease.ref().Key()]; exists {
		a.mu.Unlock()
		return false
	}
	if !a.started {
		a.started = true
		go a.refreshLoop()
	}
	a.leases[lease.ref().Key()] = &accountEgressLeaseState{
		lease:         lease,
		lastConfirmed: now,
		nextRefresh:   now.Add(a.refreshInterval),
	}
	a.mu.Unlock()
	return true
}

func (a *AccountEgressAllocator) unregister(ref AccountEgressLeaseRef) {
	a.mu.Lock()
	delete(a.leases, ref.Key())
	a.mu.Unlock()
}

func (a *AccountEgressAllocator) refreshLoop() {
	defer close(a.doneCh)
	ticker := time.NewTicker(a.refreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-a.closeCh:
			return
		case <-ticker.C:
			a.refreshDue()
		}
	}
}

func (a *AccountEgressAllocator) refreshDue() {
	now := a.now()
	a.mu.Lock()
	refs := make([]AccountEgressLeaseRef, 0, len(a.leases))
	for _, state := range a.leases {
		if !state.nextRefresh.After(now) {
			refs = append(refs, state.lease.ref())
			state.nextRefresh = now.Add(a.refreshInterval)
		}
	}
	a.mu.Unlock()
	if len(refs) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), a.operationTimeout)
	owned, err := a.cache.RefreshAccountEgressLeases(ctx, refs, a.leaseTTL)
	cancel()
	a.applyRefreshResult(refs, owned, err, now)
}

func (a *AccountEgressAllocator) applyRefreshResult(
	refs []AccountEgressLeaseRef,
	owned map[string]bool,
	err error,
	now time.Time,
) {
	var lost []*AccountEgressLease
	a.mu.Lock()
	for _, ref := range refs {
		state := a.leases[ref.Key()]
		if state == nil {
			continue
		}
		if err == nil && owned[ref.Key()] {
			state.lastConfirmed = now
			continue
		}
		if err == nil || now.Sub(state.lastConfirmed) >= a.leaseTTL {
			delete(a.leases, ref.Key())
			lost = append(lost, state.lease)
		}
	}
	a.mu.Unlock()

	for _, lease := range lost {
		lease.markLost()
	}
	if err != nil {
		logger.L().Warn("account_egress_lease_refresh_failed",
			zap.Int("lease_count", len(refs)),
			zap.Error(err),
		)
	}
}

func (a *AccountEgressAllocator) refreshOne(ctx context.Context, lease *AccountEgressLease, tolerateTransient bool) error {
	if a == nil || a.cache == nil || lease == nil {
		return ErrAccountEgressUnavailable
	}
	ref := lease.ref()
	owned, err := a.cache.RefreshAccountEgressLeases(ctx, []AccountEgressLeaseRef{ref}, a.leaseTTL)
	if err != nil {
		if tolerateTransient {
			now := a.now()
			a.mu.Lock()
			state := a.leases[ref.Key()]
			if state != nil && now.Sub(state.lastConfirmed) < a.leaseTTL {
				a.mu.Unlock()
				return nil
			}
			if state != nil {
				delete(a.leases, ref.Key())
			}
			a.mu.Unlock()
			lease.markLost()
			return fmt.Errorf("%w: refresh safety window expired: %v", ErrAccountEgressLeaseLost, err)
		}
		return fmt.Errorf("%w: refresh: %v", ErrAccountEgressUnavailable, err)
	}
	if !owned[ref.Key()] {
		a.mu.Lock()
		delete(a.leases, ref.Key())
		a.mu.Unlock()
		lease.markLost()
		return ErrAccountEgressLeaseLost
	}
	now := a.now()
	a.mu.Lock()
	if state := a.leases[ref.Key()]; state != nil {
		state.lastConfirmed = now
		state.nextRefresh = now.Add(a.refreshInterval)
	}
	a.mu.Unlock()
	return nil
}

func validateRestoredAccountEgressLeaseRef(ref AccountEgressLeaseRef) error {
	if ref.AccountID <= 0 || strings.TrimSpace(ref.ID) == "" || strings.TrimSpace(ref.BindingID) == "" ||
		strings.TrimSpace(ref.IdentityID) == "" || ref.ConfigVersion <= 0 {
		return ErrAccountEgressConfigStale
	}
	accountID, routeID, ok := parseStableAccountEgressBindingID(ref.BindingID)
	if !ok || accountID != ref.AccountID || routeID <= 0 {
		return ErrAccountEgressConfigStale
	}
	identityID, err := strconv.ParseInt(strings.TrimSpace(ref.IdentityID), 10, 64)
	if err != nil || identityID <= 0 {
		return ErrAccountEgressConfigStale
	}
	return nil
}

func (a *AccountEgressAllocator) restore(ctx context.Context, ref AccountEgressLeaseRef) (*AccountEgressLease, error) {
	if a == nil || a.cache == nil {
		return nil, ErrAccountEgressUnavailable
	}
	if err := validateRestoredAccountEgressLeaseRef(ref); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}

	a.mu.Lock()
	state := a.leases[ref.Key()]
	a.mu.Unlock()
	if state != nil && state.lease != nil {
		if state.lease.ref() != ref {
			return nil, ErrAccountEgressConfigStale
		}
		if !state.lease.Detach() {
			return nil, ErrAccountEgressLeaseLost
		}
		if err := state.lease.Refresh(ctx); err != nil {
			return nil, err
		}
		return state.lease, nil
	}

	leaseCtx, cancel := context.WithCancelCause(context.Background())
	lease := &AccountEgressLease{
		ID:            ref.ID,
		BindingID:     ref.BindingID,
		IdentityID:    ref.IdentityID,
		ConfigVersion: ref.ConfigVersion,
		accountID:     ref.AccountID,
		ctx:           leaseCtx,
		cancel:        cancel,
		allocator:     a,
		detached:      true,
	}
	if !a.register(lease) {
		cancel(ErrAccountEgressUnavailable)
		return nil, ErrAccountEgressUnavailable
	}
	if err := lease.Refresh(ctx); err != nil {
		lease.Release()
		return nil, err
	}
	return lease, nil
}

func (a *AccountEgressAllocator) Close() {
	if a == nil {
		return
	}
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		started := a.started
		leases := make([]*AccountEgressLease, 0, len(a.leases))
		for _, state := range a.leases {
			leases = append(leases, state.lease)
		}
		a.mu.Unlock()
		close(a.closeCh)
		if started {
			<-a.doneCh
		}
		for _, lease := range leases {
			lease.Release()
		}
	})
}

type AccountEgressLease struct {
	ID            string
	BindingID     string
	IdentityID    string
	ConfigVersion int64

	accountID int64
	allocator *AccountEgressAllocator

	contextStopMu sync.RWMutex
	ctx           context.Context
	cancel        context.CancelCauseFunc
	contextStop   func() bool
	detached      bool
	terminalCause error

	releaseOnce sync.Once
}

func (l *AccountEgressLease) Context() context.Context {
	if l == nil {
		return context.Background()
	}
	l.contextStopMu.RLock()
	ctx := l.ctx
	l.contextStopMu.RUnlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (l *AccountEgressLease) Refresh(ctx context.Context) error {
	if l == nil || l.allocator == nil {
		return ErrAccountEgressUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return l.allocator.refreshOne(ctx, l, false)
}

// RefreshWithinSafetyWindow treats an indeterminate cache error as retryable
// only while the last confirmed ownership is still inside the lease TTL. A
// definitive ownership miss always fails immediately.
func (l *AccountEgressLease) RefreshWithinSafetyWindow(ctx context.Context) error {
	if l == nil || l.allocator == nil {
		return ErrAccountEgressUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return l.allocator.refreshOne(ctx, l, true)
}

// Detach transfers ownership of the lease from a request context to a
// long-lived operation (for example an OpenAI Live call). It replaces the
// request-derived context as well as stopping automatic release, so cancellation
// of the HTTP request cannot poison the long-lived lease context.
func (l *AccountEgressLease) Detach() bool {
	if l == nil {
		return false
	}
	l.contextStopMu.Lock()
	defer l.contextStopMu.Unlock()
	if l.detached {
		return l.terminalCause == nil
	}
	if l.terminalCause != nil || l.contextStop == nil {
		return false
	}
	stop := l.contextStop
	l.contextStop = nil
	if !stop() {
		return false
	}
	leaseCtx, cancel := context.WithCancelCause(context.Background())
	l.ctx = leaseCtx
	l.cancel = cancel
	l.detached = true
	return true
}

func (l *AccountEgressLease) Release() {
	l.finish(true)
}

// Abandon stops this process from refreshing a transferred lease without
// deleting the Redis lease. It is used when another controller owns the same
// long-lived operation and may still be actively using that exact lease.
func (l *AccountEgressLease) Abandon() {
	l.finish(false)
}

func (l *AccountEgressLease) finish(releaseRemote bool) {
	if l == nil {
		return
	}
	l.releaseOnce.Do(func() {
		l.contextStopMu.Lock()
		if l.contextStop != nil {
			l.contextStop()
			l.contextStop = nil
		}
		if l.terminalCause == nil {
			l.terminalCause = context.Canceled
		}
		leaseCancel := l.cancel
		l.contextStopMu.Unlock()
		if leaseCancel != nil {
			leaseCancel(nil)
		}
		if l.allocator == nil {
			return
		}
		ref := l.ref()
		l.allocator.unregister(ref)
		if !releaseRemote {
			return
		}
		ctx, releaseCancel := context.WithTimeout(context.Background(), l.allocator.operationTimeout)
		defer releaseCancel()
		if err := l.allocator.cache.ReleaseAccountEgressLease(ctx, ref); err != nil {
			logger.L().Warn("account_egress_lease_release_failed",
				zap.Int64("account_id", ref.AccountID),
				zap.String("lease_id", ref.ID),
				zap.Error(err),
			)
		}
	})
}

func (l *AccountEgressLease) markLost() {
	if l == nil {
		return
	}
	l.contextStopMu.Lock()
	if l.terminalCause == nil {
		l.terminalCause = ErrAccountEgressLeaseLost
	}
	cancel := l.cancel
	l.contextStopMu.Unlock()
	if cancel != nil {
		cancel(ErrAccountEgressLeaseLost)
	}
}

func (l *AccountEgressLease) ref() AccountEgressLeaseRef {
	if l == nil {
		return AccountEgressLeaseRef{}
	}
	return AccountEgressLeaseRef{
		AccountID:     l.accountID,
		ID:            l.ID,
		BindingID:     l.BindingID,
		IdentityID:    l.IdentityID,
		ConfigVersion: l.ConfigVersion,
	}
}

func (s *ConcurrencyService) AcquireAccountEgress(ctx context.Context, request AccountEgressAcquireRequest) (*ResolvedAccountEgress, error) {
	if s == nil || s.accountEgressAllocator == nil {
		return nil, ErrAccountEgressUnavailable
	}
	return s.accountEgressAllocator.Acquire(ctx, request)
}

// RestoreAccountEgressLease attaches this process to an existing Redis lease.
// It never allocates replacement capacity: Refresh must prove that the exact
// account/binding/identity lease is still present, otherwise recovery fails
// closed and the caller must terminate the long-lived operation.
func (s *ConcurrencyService) RestoreAccountEgressLease(ctx context.Context, ref AccountEgressLeaseRef) (*AccountEgressLease, error) {
	if s == nil || s.accountEgressAllocator == nil {
		return nil, ErrAccountEgressUnavailable
	}
	return s.accountEgressAllocator.restore(ctx, ref)
}

func (s *ConcurrencyService) GetAccountEgressLoads(
	ctx context.Context,
	configs []AccountEgressPoolConfig,
) (map[int64]AccountEgressLoadInfo, error) {
	if s == nil || s.accountEgressAllocator == nil {
		return nil, ErrAccountEgressUnavailable
	}
	return s.accountEgressAllocator.GetAccountEgressLoads(ctx, configs)
}
