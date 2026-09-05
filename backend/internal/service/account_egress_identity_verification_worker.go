package service

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"
)

const (
	// EgressIdentityReverifyInterval is the shared cadence for periodically
	// checking that a proxy still presents its expected public identity.
	EgressIdentityReverifyInterval time.Duration = time.Minute
	// EgressIdentityFreshness is the maximum probe age runtime admission may
	// accept. It deliberately spans several cycles so one transient failure does
	// not immediately remove otherwise healthy capacity.
	EgressIdentityFreshness time.Duration = 5 * time.Minute

	egressIdentityReverifyTimeout = 45 * time.Second
)

// egressIdentityVerificationService is the credential-owning service surface
// required by the periodic verifier. The worker never logs returned route
// objects or raw errors because either may include proxy credentials.
type egressIdentityVerificationService interface {
	ListAssignableRoutes(ctx context.Context) ([]EgressRoute, error)
	ProbeRoutes(ctx context.Context, routeIDs []int64) ([]EgressProbeResult, error)
}

// EgressIdentityVerificationWorker periodically revalidates proxy exit
// identities. ProbeRoutes owns probing and persistence; this worker only picks
// a fair backlog, submits bounded batches, and manages its lifecycle.
type EgressIdentityVerificationWorker struct {
	service  egressIdentityVerificationService
	interval time.Duration
	timeout  time.Duration

	mu          sync.Mutex
	ctx         context.Context
	cancel      context.CancelFunc
	started     bool
	stopped     bool
	wg          sync.WaitGroup
	attemptMu   sync.Mutex
	attemptedAt map[int64]time.Time
}

func NewEgressIdentityVerificationWorker(service *EgressService) *EgressIdentityVerificationWorker {
	if service == nil {
		return newEgressIdentityVerificationWorker(nil)
	}
	return newEgressIdentityVerificationWorker(service)
}

func newEgressIdentityVerificationWorker(service egressIdentityVerificationService) *EgressIdentityVerificationWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &EgressIdentityVerificationWorker{
		service:     service,
		interval:    EgressIdentityReverifyInterval,
		timeout:     egressIdentityReverifyTimeout,
		ctx:         ctx,
		cancel:      cancel,
		attemptedAt: make(map[int64]time.Time),
	}
}

// Start is concurrency-safe and starts at most one verification loop.
func (w *EgressIdentityVerificationWorker) Start() {
	if w == nil || w.service == nil {
		return
	}

	w.mu.Lock()
	if w.started || w.stopped {
		w.mu.Unlock()
		return
	}
	if w.interval <= 0 {
		w.interval = EgressIdentityReverifyInterval
	}
	if w.timeout <= 0 {
		w.timeout = egressIdentityReverifyTimeout
	}
	w.started = true
	w.wg.Add(1)
	ctx, interval, timeout := w.ctx, w.interval, w.timeout
	w.mu.Unlock()

	go w.run(ctx, interval, timeout)
}

// Stop is concurrency-safe, cancels an in-flight cycle, and waits for the
// verification loop to exit. A worker cannot be restarted after it is stopped.
func (w *EgressIdentityVerificationWorker) Stop() {
	if w == nil {
		return
	}

	w.mu.Lock()
	if !w.stopped {
		w.stopped = true
		if w.cancel != nil {
			w.cancel()
		}
	}
	w.mu.Unlock()
	w.wg.Wait()
}

func (w *EgressIdentityVerificationWorker) run(ctx context.Context, interval, timeout time.Duration) {
	defer w.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		w.verifyOnce(ctx, timeout)

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-timer.C:
		}
	}
}

func (w *EgressIdentityVerificationWorker) verifyOnce(parent context.Context, timeout time.Duration) {
	if timeout <= 0 {
		timeout = egressIdentityReverifyTimeout
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	routes, err := w.service.ListAssignableRoutes(ctx)
	if err != nil {
		logEgressIdentityVerificationFailure(parent, ctx, "list")
		return
	}
	routeIDs := w.selectEgressIdentityVerificationRouteIDs(routes, time.Now())
	if len(routeIDs) == 0 {
		return
	}

	for start := 0; start < len(routeIDs); start += MaxEgressVerifyConcurrency {
		if ctx.Err() != nil {
			logEgressIdentityVerificationFailure(parent, ctx, "probe")
			return
		}

		end := start + MaxEgressVerifyConcurrency
		if end > len(routeIDs) {
			end = len(routeIDs)
		}
		batch := routeIDs[start:end:end]
		w.recordEgressIdentityVerificationAttempt(batch, time.Now())
		results, err := w.service.ProbeRoutes(ctx, batch)
		if err != nil {
			logEgressIdentityVerificationFailure(parent, ctx, "probe")
			return
		}

		expected := make(map[int64]struct{}, len(batch))
		for _, routeID := range batch {
			expected[routeID] = struct{}{}
		}
		succeeded := make(map[int64]struct{}, len(results))
		for _, result := range results {
			if _, requested := expected[result.RouteID]; result.Success && requested {
				succeeded[result.RouteID] = struct{}{}
			}
		}
		failed := len(expected) - len(succeeded)
		if failed > 0 {
			// Only bounded counts are logged. Route objects, proxy endpoints and raw
			// errors are intentionally excluded because they may carry credentials.
			slog.Warn("egress_identity_reverify_routes_failed",
				"attempted", len(batch),
				"failed", failed,
			)
		}
	}
}

func logEgressIdentityVerificationFailure(parent, cycle context.Context, stage string) {
	if parent.Err() != nil {
		return
	}
	slog.Warn("egress_identity_reverify_cycle_failed",
		"stage", stage,
		"timed_out", errors.Is(cycle.Err(), context.DeadlineExceeded),
	)
}

func (w *EgressIdentityVerificationWorker) selectEgressIdentityVerificationRouteIDs(routes []EgressRoute, now time.Time) []int64 {
	type candidate struct {
		id             int64
		effectiveAt    time.Time
		hasEffectiveAt bool
	}

	candidates := make([]candidate, 0, len(routes))
	eligible := make(map[int64]struct{}, len(routes))
	for i := range routes {
		route := &routes[i]
		if route.ID <= 0 || route.Kind != EgressRouteKindProxy ||
			route.State == EgressRouteStateRetired || route.State == EgressRouteStateExpired ||
			!routeCanBeProbed(route, now) {
			continue
		}
		if _, duplicate := eligible[route.ID]; duplicate {
			continue
		}
		eligible[route.ID] = struct{}{}
		item := candidate{id: route.ID}
		if route.LastProbedAt != nil {
			item.effectiveAt = *route.LastProbedAt
			item.hasEffectiveAt = true
		}
		candidates = append(candidates, item)
	}

	w.attemptMu.Lock()
	if w.attemptedAt == nil {
		w.attemptedAt = make(map[int64]time.Time)
	}
	for routeID := range w.attemptedAt {
		if _, stillEligible := eligible[routeID]; !stillEligible {
			delete(w.attemptedAt, routeID)
		}
	}
	for i := range candidates {
		attemptedAt, attempted := w.attemptedAt[candidates[i].id]
		if attempted && (!candidates[i].hasEffectiveAt || attemptedAt.After(candidates[i].effectiveAt)) {
			candidates[i].effectiveAt = attemptedAt
			candidates[i].hasEffectiveAt = true
		}
	}
	w.attemptMu.Unlock()

	sort.Slice(candidates, func(i, j int) bool {
		left, right := candidates[i], candidates[j]
		if !left.hasEffectiveAt || !right.hasEffectiveAt {
			if !left.hasEffectiveAt && !right.hasEffectiveAt {
				return left.id < right.id
			}
			return !left.hasEffectiveAt
		}
		if left.effectiveAt.Equal(right.effectiveAt) {
			return left.id < right.id
		}
		return left.effectiveAt.Before(right.effectiveAt)
	})

	routeIDs := make([]int64, len(candidates))
	for i := range candidates {
		routeIDs[i] = candidates[i].id
	}
	return routeIDs
}

func (w *EgressIdentityVerificationWorker) recordEgressIdentityVerificationAttempt(routeIDs []int64, attemptedAt time.Time) {
	w.attemptMu.Lock()
	defer w.attemptMu.Unlock()
	if w.attemptedAt == nil {
		w.attemptedAt = make(map[int64]time.Time)
	}
	for _, routeID := range routeIDs {
		w.attemptedAt[routeID] = attemptedAt
	}
}
