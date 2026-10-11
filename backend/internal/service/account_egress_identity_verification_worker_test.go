package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type egressIdentityVerificationServiceStub struct {
	mu sync.Mutex

	routes    []EgressRoute
	listErrs  []error
	listCalls int
	probed    [][]int64
	probeFn   func(context.Context, []int64) ([]EgressProbeResult, error)
}

func (s *egressIdentityVerificationServiceStub) ListAssignableRoutes(context.Context) ([]EgressRoute, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalls++
	if len(s.listErrs) > 0 {
		err := s.listErrs[0]
		s.listErrs = s.listErrs[1:]
		if err != nil {
			return nil, err
		}
	}
	return append([]EgressRoute(nil), s.routes...), nil
}

func (s *egressIdentityVerificationServiceStub) ProbeRoutes(ctx context.Context, routeIDs []int64) ([]EgressProbeResult, error) {
	ids := append([]int64(nil), routeIDs...)
	s.mu.Lock()
	s.probed = append(s.probed, ids)
	probeFn := s.probeFn
	s.mu.Unlock()
	if probeFn != nil {
		return probeFn(ctx, ids)
	}
	results := make([]EgressProbeResult, len(ids))
	for i, id := range ids {
		results[i] = EgressProbeResult{RouteID: id, Success: true}
	}
	return results, nil
}

func activeVerificationProxyRoute(id int64, lastProbedAt *time.Time, now time.Time) EgressRoute {
	expiresAt := now.Add(time.Hour)
	return EgressRoute{
		ID:           id,
		Kind:         EgressRouteKindProxy,
		State:        EgressRouteStateActive,
		LastProbedAt: lastProbedAt,
		Proxy: &Proxy{
			ID:        id,
			Status:    StatusActive,
			ExpiresAt: &expiresAt,
		},
	}
}

func TestEgressIdentityVerificationTimingContract(t *testing.T) {
	require.Greater(t, EgressIdentityFreshness, EgressIdentityReverifyInterval)
	require.Less(t, egressIdentityReverifyTimeout, EgressIdentityReverifyInterval)
}

func TestSelectEgressIdentityVerificationRoutesFiltersAndOrdersOldestFirst(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	oldest := now.Add(-10 * time.Minute)
	newest := now.Add(-time.Minute)
	routes := []EgressRoute{
		activeVerificationProxyRoute(3, &newest, now),
		activeVerificationProxyRoute(1, nil, now),
		activeVerificationProxyRoute(2, &oldest, now),
		{ID: 4, Kind: EgressRouteKindDirect, State: EgressRouteStateActive},
		activeVerificationProxyRoute(5, nil, now),
		activeVerificationProxyRoute(6, nil, now),
		activeVerificationProxyRoute(7, nil, now),
		activeVerificationProxyRoute(1, &oldest, now),
		{ID: 0, Kind: EgressRouteKindProxy, State: EgressRouteStateActive, Proxy: &Proxy{Status: StatusActive}},
	}
	routes[4].State = EgressRouteStateRetired
	routes[5].State = EgressRouteStateExpired
	routes[6].Proxy.Status = StatusDisabled

	expiredAt := now
	routes = append(routes, EgressRoute{
		ID: 8, Kind: EgressRouteKindProxy, State: EgressRouteStateActive,
		Proxy: &Proxy{ID: 8, Status: StatusActive, ExpiresAt: &expiredAt},
	})

	worker := newEgressIdentityVerificationWorker(nil)
	require.Equal(t, []int64{1, 2, 3}, worker.selectEgressIdentityVerificationRouteIDs(routes, now))
}

func TestSelectEgressIdentityVerificationRoutesReturnsEntireSortedBacklog(t *testing.T) {
	now := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	routes := make([]EgressRoute, 0, MaxEgressVerifyBatchSize+8)
	for id := int64(1); id <= int64(MaxEgressVerifyBatchSize+8); id++ {
		lastProbedAt := now.Add(-time.Duration(id) * time.Minute)
		routes = append(routes, activeVerificationProxyRoute(id, &lastProbedAt, now))
	}

	worker := newEgressIdentityVerificationWorker(nil)
	got := worker.selectEgressIdentityVerificationRouteIDs(routes, now)
	require.Len(t, got, MaxEgressVerifyBatchSize+8)
	for i, id := range got {
		require.Equal(t, int64(MaxEgressVerifyBatchSize+8-i), id)
	}
}

func TestSelectEgressIdentityVerificationRoutesUsesLatestProbeOrAttempt(t *testing.T) {
	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	tenMinutesAgo := now.Add(-10 * time.Minute)
	oneMinuteAgo := now.Add(-time.Minute)
	routes := []EgressRoute{
		activeVerificationProxyRoute(1, nil, now),
		activeVerificationProxyRoute(2, &tenMinutesAgo, now),
		activeVerificationProxyRoute(3, &oneMinuteAgo, now),
		activeVerificationProxyRoute(4, &tenMinutesAgo, now),
	}
	worker := newEgressIdentityVerificationWorker(nil)
	worker.attemptedAt[1] = now.Add(-2 * time.Minute)
	worker.attemptedAt[3] = now.Add(-5 * time.Minute)
	worker.attemptedAt[4] = now

	require.Equal(t, []int64{2, 1, 3, 4}, worker.selectEgressIdentityVerificationRouteIDs(routes, now))
}

func TestEgressIdentityVerificationWorkerSubmitsEntireLargeBacklogInBatches(t *testing.T) {
	now := time.Now()
	const routeCount = 161
	routes := make([]EgressRoute, 0, routeCount)
	for id := int64(1); id <= routeCount; id++ {
		routes = append(routes, activeVerificationProxyRoute(id, nil, now))
	}
	stub := &egressIdentityVerificationServiceStub{
		routes: routes,
		probeFn: func(_ context.Context, routeIDs []int64) ([]EgressProbeResult, error) {
			results := make([]EgressProbeResult, len(routeIDs))
			for i, routeID := range routeIDs {
				// The first batch deliberately fails per route. Those failures must
				// not prevent the remaining backlog from being submitted.
				results[i] = EgressProbeResult{RouteID: routeID, Success: routeID > MaxEgressVerifyConcurrency}
			}
			return results, nil
		},
	}
	worker := newEgressIdentityVerificationWorker(stub)

	worker.verifyOnce(context.Background(), time.Second)

	stub.mu.Lock()
	defer stub.mu.Unlock()
	require.Len(t, stub.probed, 41)
	flattened := make([]int64, 0, routeCount)
	for batchIndex, batch := range stub.probed {
		if batchIndex < 40 {
			require.Len(t, batch, MaxEgressVerifyConcurrency)
		} else {
			require.Len(t, batch, 1)
		}
		flattened = append(flattened, batch...)
	}
	for i, routeID := range flattened {
		require.Equal(t, int64(i+1), routeID)
	}
}

func TestEgressIdentityVerificationWorkerTimeoutStopsLaterBatches(t *testing.T) {
	now := time.Now()
	routes := make([]EgressRoute, 0, MaxEgressVerifyConcurrency*3)
	for id := int64(1); id <= int64(MaxEgressVerifyConcurrency*3); id++ {
		routes = append(routes, activeVerificationProxyRoute(id, nil, now))
	}
	stub := &egressIdentityVerificationServiceStub{routes: routes}
	stub.probeFn = func(ctx context.Context, routeIDs []int64) ([]EgressProbeResult, error) {
		stub.mu.Lock()
		callCount := len(stub.probed)
		stub.mu.Unlock()
		if callCount == 2 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		results := make([]EgressProbeResult, len(routeIDs))
		for i, routeID := range routeIDs {
			results[i] = EgressProbeResult{RouteID: routeID, Success: true}
		}
		return results, nil
	}
	worker := newEgressIdentityVerificationWorker(stub)

	worker.verifyOnce(context.Background(), 20*time.Millisecond)

	stub.mu.Lock()
	defer stub.mu.Unlock()
	require.Len(t, stub.probed, 2)
	require.Equal(t, int64(1), stub.probed[0][0])
	require.Equal(t, int64(MaxEgressVerifyConcurrency+1), stub.probed[1][0])
}

func TestEgressIdentityVerificationWorkerTimeoutAdvancesNextCycle(t *testing.T) {
	now := time.Now()
	routes := make([]EgressRoute, 0, MaxEgressVerifyConcurrency*3)
	for id := int64(1); id <= int64(MaxEgressVerifyConcurrency*3); id++ {
		routes = append(routes, activeVerificationProxyRoute(id, nil, now))
	}
	slow := true
	stub := &egressIdentityVerificationServiceStub{routes: routes}
	stub.probeFn = func(ctx context.Context, routeIDs []int64) ([]EgressProbeResult, error) {
		if slow {
			select {
			case <-time.After(60 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		results := make([]EgressProbeResult, len(routeIDs))
		for i, routeID := range routeIDs {
			results[i] = EgressProbeResult{RouteID: routeID, Success: true}
		}
		return results, nil
	}
	worker := newEgressIdentityVerificationWorker(stub)

	worker.verifyOnce(context.Background(), 100*time.Millisecond)
	slow = false
	worker.verifyOnce(context.Background(), time.Second)

	stub.mu.Lock()
	defer stub.mu.Unlock()
	require.GreaterOrEqual(t, len(stub.probed), 3)
	require.Equal(t, []int64{1, 2, 3, 4}, stub.probed[0])
	require.Equal(t, []int64{5, 6, 7, 8}, stub.probed[1])
	require.Equal(t, []int64{9, 10, 11, 12}, stub.probed[2], "next cycle must start with routes never submitted in the timed-out cycle")
}

func TestEgressIdentityVerificationWorkerAttemptStateIsBoundedByEligibleRoutes(t *testing.T) {
	now := time.Now()
	routes := []EgressRoute{
		activeVerificationProxyRoute(1, nil, now),
		activeVerificationProxyRoute(2, nil, now),
		activeVerificationProxyRoute(3, nil, now),
	}
	routes[2].State = EgressRouteStateRetired
	stub := &egressIdentityVerificationServiceStub{routes: routes}
	worker := newEgressIdentityVerificationWorker(stub)
	worker.attemptedAt[3] = now.Add(-time.Minute)
	worker.attemptedAt[999] = now.Add(-time.Minute)

	worker.verifyOnce(context.Background(), time.Second)

	worker.attemptMu.Lock()
	defer worker.attemptMu.Unlock()
	require.Len(t, worker.attemptedAt, 2)
	require.Contains(t, worker.attemptedAt, int64(1))
	require.Contains(t, worker.attemptedAt, int64(2))
	require.NotContains(t, worker.attemptedAt, int64(3))
	require.NotContains(t, worker.attemptedAt, int64(999))
}

func TestEgressIdentityVerificationWorkerContinuesAfterCycleFailure(t *testing.T) {
	now := time.Now()
	probed := make(chan struct{}, 1)
	stub := &egressIdentityVerificationServiceStub{
		routes:   []EgressRoute{activeVerificationProxyRoute(11, nil, now)},
		listErrs: []error{errors.New("temporary database failure")},
		probeFn: func(_ context.Context, routeIDs []int64) ([]EgressProbeResult, error) {
			select {
			case probed <- struct{}{}:
			default:
			}
			return []EgressProbeResult{{RouteID: routeIDs[0], Success: true}}, nil
		},
	}
	worker := newEgressIdentityVerificationWorker(stub)
	worker.interval = 5 * time.Millisecond
	worker.timeout = 100 * time.Millisecond

	worker.Start()
	worker.Start()
	select {
	case <-probed:
	case <-time.After(time.Second):
		t.Fatal("worker did not recover after a failed cycle")
	}
	worker.Stop()
	worker.Stop()

	stub.mu.Lock()
	defer stub.mu.Unlock()
	require.GreaterOrEqual(t, stub.listCalls, 2)
	require.NotEmpty(t, stub.probed)
}

func TestEgressIdentityVerificationWorkerStopCancelsInFlightProbe(t *testing.T) {
	now := time.Now()
	probeStarted := make(chan struct{})
	probeStopped := make(chan struct{})
	var once sync.Once
	stub := &egressIdentityVerificationServiceStub{
		routes: []EgressRoute{activeVerificationProxyRoute(21, nil, now)},
		probeFn: func(ctx context.Context, _ []int64) ([]EgressProbeResult, error) {
			once.Do(func() { close(probeStarted) })
			<-ctx.Done()
			close(probeStopped)
			return nil, ctx.Err()
		},
	}
	worker := newEgressIdentityVerificationWorker(stub)
	worker.Start()
	worker.Start()

	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not start its probe")
	}
	stopped := make(chan struct{})
	go func() {
		worker.Stop()
		worker.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("worker stop did not cancel and join the in-flight probe")
	}
	select {
	case <-probeStopped:
	default:
		t.Fatal("in-flight probe did not observe cancellation")
	}

	stub.mu.Lock()
	defer stub.mu.Unlock()
	require.Len(t, stub.probed, 1)
}

type egressIdentityVerificationRepositoryStub struct {
	EgressRepository

	mu           sync.Mutex
	route        EgressRoute
	observations []EgressProbeObservation
}

func (r *egressIdentityVerificationRepositoryStub) ListAssignableRoutes(context.Context) ([]EgressRoute, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return []EgressRoute{r.route}, nil
}

func (r *egressIdentityVerificationRepositoryStub) GetRoute(context.Context, int64) (*EgressRoute, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	route := r.route
	return &route, nil
}

func (r *egressIdentityVerificationRepositoryStub) RecordProbeObservation(_ context.Context, observation EgressProbeObservation) (*EgressRoute, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.observations = append(r.observations, observation)
	r.route.LastProbedAt = &observation.ObservedAt
	r.route.LastError = &observation.ProbeError
	r.route.Revision++
	if observation.ProbeError == "" {
		r.route.VerifiedAt = &observation.ObservedAt
	}
	route := r.route
	return &route, nil
}

type credentialBearingFailedEgressProber struct {
	seenProxyURL string
}

func (p *credentialBearingFailedEgressProber) ProbeProxy(_ context.Context, proxyURL string) (*ProxyExitInfo, int64, error) {
	p.seenProxyURL = proxyURL
	return nil, 17, fmt.Errorf("dial %s: authentication rejected", proxyURL)
}

func TestEgressIdentityVerificationFailurePreservesVerifiedAtAndRedactsLogs(t *testing.T) {
	now := time.Now()
	verifiedAt := now.Add(-time.Minute)
	route := activeVerificationProxyRoute(31, &verifiedAt, now)
	route.VerifiedAt = &verifiedAt
	route.Revision = 7
	route.Proxy.Protocol = "http"
	route.Proxy.Host = "proxy.example.test"
	route.Proxy.Port = 8080
	route.Proxy.Username = "egress-user"
	route.Proxy.Password = "egress-password"
	repo := &egressIdentityVerificationRepositoryStub{route: route}
	prober := &credentialBearingFailedEgressProber{}
	worker := NewEgressIdentityVerificationWorker(NewEgressService(repo, prober))

	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })

	worker.verifyOnce(context.Background(), 100*time.Millisecond)

	repo.mu.Lock()
	defer repo.mu.Unlock()
	require.Len(t, repo.observations, 1)
	require.Equal(t, EgressProbeReasonProbeFailed, repo.observations[0].ProbeError)
	require.Equal(t, verifiedAt, *repo.route.VerifiedAt)
	require.NotNil(t, repo.route.LastProbedAt)
	require.Contains(t, prober.seenProxyURL, "egress-user:egress-password")
	require.NotContains(t, logs.String(), "egress-user")
	require.NotContains(t, logs.String(), "egress-password")
}
