package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type probeRouteRepositoryStub struct {
	EgressRepository
	route        EgressRoute
	observations []EgressProbeObservation
}

func (r *probeRouteRepositoryStub) GetRoute(context.Context, int64) (*EgressRoute, error) {
	route := r.route
	return &route, nil
}

func (r *probeRouteRepositoryStub) RecordProbeObservation(_ context.Context, observation EgressProbeObservation) (*EgressRoute, error) {
	r.observations = append(r.observations, observation)
	r.route.LastProbedAt = &observation.ObservedAt
	if observation.ProbeError != "" {
		r.route.State = EgressRouteStateInactive
		r.route.LastError = &observation.ProbeError
	} else if r.route.ExpectedIdentity != nil && r.route.ExpectedIdentity.PublicIP != observation.ObservedIP {
		r.route.State = EgressRouteStateIdentityMismatch
	} else {
		r.route.State = EgressRouteStateActive
		r.route.LastError = nil
	}
	r.route.Revision++
	route := r.route
	return &route, nil
}

type probeSequenceResponse struct {
	info *ProxyExitInfo
	err  error
}

type probeSequenceProber struct {
	responses []probeSequenceResponse
	calls     int
}

func (p *probeSequenceProber) ProbeProxy(context.Context, string) (*ProxyExitInfo, int64, error) {
	index := p.calls
	p.calls++
	if index >= len(p.responses) {
		return nil, 1, errors.New("unexpected probe call")
	}
	response := p.responses[index]
	return response.info, 1, response.err
}

func probeRouteForTest(state, ip string) EgressRoute {
	expiresAt := time.Now().Add(time.Hour)
	identity := &EgressIdentity{ID: 1, PublicIP: ip, Status: EgressIdentityStatusActive}
	return EgressRoute{
		ID: 1, Kind: EgressRouteKindProxy, State: state, Revision: 1,
		ExpectedIdentity: identity, ExpectedIdentityID: &identity.ID,
		ProxyID: &identity.ID,
		Proxy:   &Proxy{ID: 1, Status: StatusActive, ExpiresAt: &expiresAt},
	}
}

func TestProbeRouteProbeDeadlineFromProberIsPersistedFailure(t *testing.T) {
	repo := &probeRouteRepositoryStub{route: probeRouteForTest(EgressRouteStateActive, "198.51.100.10")}
	prober := &probeSequenceProber{responses: []probeSequenceResponse{{err: context.DeadlineExceeded}}}
	result := NewEgressService(repo, prober).probeRoute(context.Background(), 1)

	require.False(t, result.Success)
	require.Equal(t, EgressProbeReasonProbeFailed, result.ReasonCode)
	require.Len(t, repo.observations, 1)
}

func TestProbeRouteUpstream503DoesNotDisableRoute(t *testing.T) {
	repo := &probeRouteRepositoryStub{route: probeRouteForTest(EgressRouteStateActive, "198.51.100.10")}
	prober := &probeSequenceProber{responses: []probeSequenceResponse{{err: errors.New("all probe URLs failed, last error: request failed with status: 503")}}}
	result := NewEgressService(repo, prober).probeRoute(context.Background(), 1)

	require.False(t, result.Success)
	require.Equal(t, EgressProbeReasonUpstreamUnavailable, result.ReasonCode)
	require.Empty(t, repo.observations)
	require.Equal(t, EgressRouteStateActive, repo.route.State)
}

func TestProbeRouteRecoveryRequiresIndependentSecondObservation(t *testing.T) {
	repo := &probeRouteRepositoryStub{route: probeRouteForTest(EgressRouteStateInactive, "198.51.100.10")}
	info := &ProxyExitInfo{IP: "198.51.100.10"}
	prober := &probeSequenceProber{responses: []probeSequenceResponse{{info: info}, {info: info}}}
	result := NewEgressService(repo, prober).probeRoute(context.Background(), 1)

	require.True(t, result.Success)
	require.Equal(t, EgressRouteStateActive, result.Route.State)
	require.Equal(t, 2, prober.calls)
	require.Len(t, repo.observations, 1)
}

func TestProbeRouteRecoveryFailureKeepsRouteIsolated(t *testing.T) {
	repo := &probeRouteRepositoryStub{route: probeRouteForTest(EgressRouteStateIdentityMismatch, "198.51.100.10")}
	prober := &probeSequenceProber{responses: []probeSequenceResponse{
		{info: &ProxyExitInfo{IP: "198.51.100.10"}},
		{err: errors.New("proxy connection failed")},
	}}
	result := NewEgressService(repo, prober).probeRoute(context.Background(), 1)

	require.False(t, result.Success)
	require.Equal(t, EgressProbeReasonProbeFailed, result.ReasonCode)
	require.Equal(t, EgressRouteStateInactive, repo.route.State)
	require.Equal(t, 2, prober.calls)
}

func TestProbeRouteRecoveryDisagreementKeepsRouteIsolated(t *testing.T) {
	repo := &probeRouteRepositoryStub{route: probeRouteForTest(EgressRouteStateInactive, "198.51.100.10")}
	prober := &probeSequenceProber{responses: []probeSequenceResponse{
		{info: &ProxyExitInfo{IP: "198.51.100.11"}},
		{info: &ProxyExitInfo{IP: "198.51.100.10"}},
	}}
	result := NewEgressService(repo, prober).probeRoute(context.Background(), 1)

	require.False(t, result.Success)
	require.Equal(t, EgressProbeReasonInvalidObservation, result.ReasonCode)
	require.Equal(t, EgressRouteStateInactive, repo.route.State)
	require.Equal(t, 2, prober.calls)
	require.Len(t, repo.observations, 1)
}

func TestProbeRouteSuccessReflectsPersistedIdentityMismatch(t *testing.T) {
	repo := &probeRouteRepositoryStub{route: probeRouteForTest(EgressRouteStateActive, "198.51.100.10")}
	prober := &probeSequenceProber{responses: []probeSequenceResponse{{info: &ProxyExitInfo{IP: "198.51.100.11"}}}}
	result := NewEgressService(repo, prober).probeRoute(context.Background(), 1)

	require.False(t, result.Success)
	require.Equal(t, EgressProbeReasonIdentityMismatch, result.ReasonCode)
	require.Equal(t, EgressRouteStateIdentityMismatch, result.Route.State)
}

type unavailableEgressProbeRepository struct {
	EgressRepository
}

func (unavailableEgressProbeRepository) GetRoute(context.Context, int64) (*EgressRoute, error) {
	return nil, ErrEgressRouteNotFound
}

type unexpectedEgressProber struct{}

func (unexpectedEgressProber) ProbeProxy(context.Context, string) (*ProxyExitInfo, int64, error) {
	return nil, -1, errors.New("probe should not be called for a missing route")
}

func TestProbeRoutesReturnsPerRouteFailureWithoutSyntheticLatency(t *testing.T) {
	svc := NewEgressService(unavailableEgressProbeRepository{}, unexpectedEgressProber{})

	results, err := svc.ProbeRoutes(context.Background(), []int64{91})

	require.NoError(t, err, "a route failure must remain inside the batch result")
	require.Len(t, results, 1)
	require.Equal(t, int64(91), results[0].RouteID)
	require.False(t, results[0].Success)
	require.Equal(t, int64(-1), results[0].LatencyMs)
	require.Equal(t, EgressProbeReasonRouteNotFound, results[0].ReasonCode)
	require.False(t, results[0].ObservedAt.IsZero())
}
