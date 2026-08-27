package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

func TestTrafficDirectorRuntimeMetricsUseFixedBuckets(t *testing.T) {
	metrics := &trafficDirectorRuntimeMetrics{}
	metrics.observeRoutingDecision(domain.TrafficDirectorModeLegacy)
	metrics.observeRoutingDecision(domain.TrafficDirectorModeShadow)
	metrics.observeRoutingDecision(domain.TrafficDirectorModeEnforced)
	metrics.observeRoutingDecision("unsupported")
	metrics.observePolicyResolution(TrafficDirectorPolicySourceL1, false)
	metrics.observePolicyResolution(TrafficDirectorPolicySourceL2, false)
	metrics.observePolicyResolution(TrafficDirectorPolicySourceDB, false)
	metrics.observePolicyResolution(TrafficDirectorPolicySourceLegacy, true)
	metrics.observePolicyResolution("custom", false)
	metrics.poolExhausted.Add(2)
	metrics.fallbackTransition.Add(1)
	metrics.noAvailable.Add(1)
	metrics.healthFailOpen.Add(1)
	metrics.policyUnavailable.Add(1)

	require.Equal(t, TrafficDirectorRuntimeMetricsSnapshot{
		Scope: "process_lifetime",
		RoutingDecisions: TrafficDirectorRoutingMetricsSnapshot{
			Total: 3, LegacyTotal: 1, ShadowTotal: 1, EnforcedTotal: 1,
		},
		PoolRouting: TrafficDirectorPoolRoutingMetricsSnapshot{
			ExhaustedTotal: 2, FallbackTransitionsTotal: 1, NoAvailablePoolTotal: 1,
		},
		Health: TrafficDirectorHealthRuntimeMetricsSnapshot{FailOpenTotal: 1},
		Policy: TrafficDirectorPolicyRuntimeMetricsSnapshot{
			L1HitTotal: 1, RedisHitTotal: 1, DBFallbackTotal: 1,
			LegacyFallbackTotal: 1, UnknownSourceTotal: 1, UnavailableTotal: 1,
		},
	}, metrics.snapshot())
}

func TestTrafficDirectorPlanRuntimeMetricsDeduplicateTerminalEvents(t *testing.T) {
	var metrics trafficDirectorPlanRuntimeMetrics
	require.True(t, metrics.markPoolExhausted("stable"))
	require.False(t, metrics.markPoolExhausted("stable"))
	require.True(t, metrics.markPoolExhausted("backup"))
	require.False(t, metrics.markPoolExhausted(""))
	require.True(t, metrics.markNoAvailable())
	require.False(t, metrics.markNoAvailable())

	// The plan-local guard is also safe when scheduler and wait-retry paths race.
	var wg sync.WaitGroup
	var newlyRecorded atomic.Int64
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if metrics.markPoolExhausted("concurrent") {
				newlyRecorded.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(1), newlyRecorded.Load())
}

func TestTrafficDirectorRuntimeMetricsTrackOneRequestAcrossFallbacks(t *testing.T) {
	spec := testOpenAITrafficDirectorSpec()
	resolver := &openAITrafficDirectorResolverStub{policy: TrafficDirectorResolvedPolicy{
		Version: TrafficDirectorVersion{
			GroupID: 42,
			Version: 1,
			Mode:    domain.TrafficDirectorModeEnforced,
			Spec:    &spec,
		},
		Source: TrafficDirectorPolicySourceDB,
	}}
	svc := &OpenAIGatewayService{accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{
		testOpenAIAccountForTrafficDirector(101, 42),
		testOpenAIAccountForTrafficDirector(202, 42),
	}}}
	svc.SetOpenAITrafficDirectorResolver(resolver)
	groupID := int64(42)
	ctx := context.WithValue(context.Background(), ctxkey.RequestID, "metrics-fallback-request")
	ctx = svc.WithOpenAITrafficDirectorRequestContext(ctx)
	before := SnapshotTrafficDirectorRuntimeMetrics()

	first, _, err := svc.SelectAccountWithSchedulerForCapability(ctx, &groupID, "", "", "gpt-5", nil, OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions, false, false, false)
	require.NoError(t, err)
	require.NotNil(t, first)
	if first.ReleaseFunc != nil {
		first.ReleaseFunc()
	}

	second, _, err := svc.SelectAccountWithSchedulerForCapability(ctx, &groupID, "", "", "gpt-5", map[int64]struct{}{101: {}}, OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions, false, false, false)
	require.NoError(t, err)
	require.NotNil(t, second)
	if second.ReleaseFunc != nil {
		second.ReleaseFunc()
	}

	_, _, err = svc.SelectAccountWithSchedulerForCapability(ctx, &groupID, "", "", "gpt-5", map[int64]struct{}{101: {}, 202: {}}, OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions, false, false, false)
	require.ErrorIs(t, err, ErrTrafficDirectorNoAvailablePool)

	after := SnapshotTrafficDirectorRuntimeMetrics()
	require.Equal(t, before.RoutingDecisions.EnforcedTotal+1, after.RoutingDecisions.EnforcedTotal)
	require.Equal(t, before.Policy.DBFallbackTotal+1, after.Policy.DBFallbackTotal)
	require.Equal(t, before.PoolRouting.ExhaustedTotal+2, after.PoolRouting.ExhaustedTotal)
	require.Equal(t, before.PoolRouting.FallbackTransitionsTotal+1, after.PoolRouting.FallbackTransitionsTotal)
	require.Equal(t, before.PoolRouting.NoAvailablePoolTotal+1, after.PoolRouting.NoAvailablePoolTotal)
}
