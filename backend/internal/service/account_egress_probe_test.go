package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

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
