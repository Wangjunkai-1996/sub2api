package service

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Wei-Shaw/sub2api/internal/domain"
)

// TrafficDirectorRuntimeMetricsSnapshot is a process-lifetime, low-cardinality
// view of Traffic Director routing. Every bucket is a fixed enum; request,
// group, account, model, pool, and routing-key values are deliberately absent.
type TrafficDirectorRuntimeMetricsSnapshot struct {
	Scope            string                                      `json:"scope"`
	RoutingDecisions TrafficDirectorRoutingMetricsSnapshot       `json:"routing_decisions"`
	PoolRouting      TrafficDirectorPoolRoutingMetricsSnapshot   `json:"pool_routing"`
	Health           TrafficDirectorHealthRuntimeMetricsSnapshot `json:"health"`
	Policy           TrafficDirectorPolicyRuntimeMetricsSnapshot `json:"policy"`
}

type TrafficDirectorRoutingMetricsSnapshot struct {
	Total         uint64 `json:"total"`
	LegacyTotal   uint64 `json:"legacy_total"`
	ShadowTotal   uint64 `json:"shadow_total"`
	EnforcedTotal uint64 `json:"enforced_total"`
}

type TrafficDirectorPoolRoutingMetricsSnapshot struct {
	ExhaustedTotal           uint64 `json:"exhausted_total"`
	FallbackTransitionsTotal uint64 `json:"fallback_transitions_total"`
	NoAvailablePoolTotal     uint64 `json:"no_available_pool_total"`
}

type TrafficDirectorHealthRuntimeMetricsSnapshot struct {
	FailOpenTotal uint64 `json:"fail_open_total"`
}

type TrafficDirectorPolicyRuntimeMetricsSnapshot struct {
	L1HitTotal          uint64 `json:"l1_hit_total"`
	RedisHitTotal       uint64 `json:"redis_hit_total"`
	DBFallbackTotal     uint64 `json:"db_fallback_total"`
	LegacyFallbackTotal uint64 `json:"legacy_fallback_total"`
	UnknownSourceTotal  uint64 `json:"unknown_source_total"`
	UnavailableTotal    uint64 `json:"unavailable_total"`
}

type trafficDirectorRuntimeMetrics struct {
	routingLegacy   atomic.Uint64
	routingShadow   atomic.Uint64
	routingEnforced atomic.Uint64

	poolExhausted      atomic.Uint64
	fallbackTransition atomic.Uint64
	noAvailable        atomic.Uint64
	healthFailOpen     atomic.Uint64

	policyL1Hit          atomic.Uint64
	policyRedisHit       atomic.Uint64
	policyDBFallback     atomic.Uint64
	policyLegacyFallback atomic.Uint64
	policyUnknownSource  atomic.Uint64
	policyUnavailable    atomic.Uint64
}

var defaultTrafficDirectorRuntimeMetrics trafficDirectorRuntimeMetrics

// SnapshotTrafficDirectorRuntimeMetrics returns counters accumulated by this
// process since startup. Deployments with multiple instances should aggregate
// snapshots externally instead of adding high-cardinality instance labels here.
func SnapshotTrafficDirectorRuntimeMetrics() TrafficDirectorRuntimeMetricsSnapshot {
	return defaultTrafficDirectorRuntimeMetrics.snapshot()
}

func (m *trafficDirectorRuntimeMetrics) snapshot() TrafficDirectorRuntimeMetricsSnapshot {
	if m == nil {
		return TrafficDirectorRuntimeMetricsSnapshot{Scope: "process_lifetime"}
	}
	legacy := m.routingLegacy.Load()
	shadow := m.routingShadow.Load()
	enforced := m.routingEnforced.Load()
	return TrafficDirectorRuntimeMetricsSnapshot{
		Scope: "process_lifetime",
		RoutingDecisions: TrafficDirectorRoutingMetricsSnapshot{
			Total:         legacy + shadow + enforced,
			LegacyTotal:   legacy,
			ShadowTotal:   shadow,
			EnforcedTotal: enforced,
		},
		PoolRouting: TrafficDirectorPoolRoutingMetricsSnapshot{
			ExhaustedTotal:           m.poolExhausted.Load(),
			FallbackTransitionsTotal: m.fallbackTransition.Load(),
			NoAvailablePoolTotal:     m.noAvailable.Load(),
		},
		Health: TrafficDirectorHealthRuntimeMetricsSnapshot{
			FailOpenTotal: m.healthFailOpen.Load(),
		},
		Policy: TrafficDirectorPolicyRuntimeMetricsSnapshot{
			L1HitTotal:          m.policyL1Hit.Load(),
			RedisHitTotal:       m.policyRedisHit.Load(),
			DBFallbackTotal:     m.policyDBFallback.Load(),
			LegacyFallbackTotal: m.policyLegacyFallback.Load(),
			UnknownSourceTotal:  m.policyUnknownSource.Load(),
			UnavailableTotal:    m.policyUnavailable.Load(),
		},
	}
}

func (m *trafficDirectorRuntimeMetrics) observeRoutingDecision(mode string) {
	if m == nil {
		return
	}
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case domain.TrafficDirectorModeLegacy:
		m.routingLegacy.Add(1)
	case domain.TrafficDirectorModeShadow:
		m.routingShadow.Add(1)
	case domain.TrafficDirectorModeEnforced:
		m.routingEnforced.Add(1)
	}
}

func (m *trafficDirectorRuntimeMetrics) observePolicyResolution(source string, degraded bool) {
	if m == nil {
		return
	}
	switch strings.ToLower(strings.TrimSpace(source)) {
	case TrafficDirectorPolicySourceL1:
		m.policyL1Hit.Add(1)
	case TrafficDirectorPolicySourceL2:
		m.policyRedisHit.Add(1)
	case TrafficDirectorPolicySourceDB:
		m.policyDBFallback.Add(1)
	case TrafficDirectorPolicySourceLegacy:
		if degraded {
			m.policyLegacyFallback.Add(1)
		}
	default:
		m.policyUnknownSource.Add(1)
	}
}

func recordTrafficDirectorLegacyRoutingDecision() {
	defaultTrafficDirectorRuntimeMetrics.observeRoutingDecision(domain.TrafficDirectorModeLegacy)
}

// claimTrafficDirectorPolicyResolution makes policy source and routing-mode
// counters request-scoped. Plan resolution may be re-entered by failover loops.
func claimTrafficDirectorPolicyResolution(state *openAITrafficDirectorRequestState) bool {
	if state == nil {
		return true
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.runtimePolicyResolutionRecorded {
		return false
	}
	state.runtimePolicyResolutionRecorded = true
	return true
}

func recordTrafficDirectorResolvedPolicy(
	state *openAITrafficDirectorRequestState,
	mode string,
	resolved TrafficDirectorResolvedPolicy,
) {
	if !claimTrafficDirectorPolicyResolution(state) {
		return
	}
	defaultTrafficDirectorRuntimeMetrics.observeRoutingDecision(mode)
	defaultTrafficDirectorRuntimeMetrics.observePolicyResolution(resolved.Source, resolved.Degraded)
}

func recordTrafficDirectorPolicyUnavailable(state *openAITrafficDirectorRequestState) {
	if claimTrafficDirectorPolicyResolution(state) {
		defaultTrafficDirectorRuntimeMetrics.policyUnavailable.Add(1)
	}
}

type trafficDirectorPlanRuntimeMetrics struct {
	mu                  sync.Mutex
	exhaustedPoolKeys   map[string]struct{}
	noAvailableRecorded bool
}

func (m *trafficDirectorPlanRuntimeMetrics) markPoolExhausted(poolKey string) bool {
	poolKey = strings.TrimSpace(poolKey)
	if m == nil || poolKey == "" {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.exhaustedPoolKeys == nil {
		m.exhaustedPoolKeys = make(map[string]struct{})
	}
	if _, exists := m.exhaustedPoolKeys[poolKey]; exists {
		return false
	}
	m.exhaustedPoolKeys[poolKey] = struct{}{}
	return true
}

func (m *trafficDirectorPlanRuntimeMetrics) markNoAvailable() bool {
	if m == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.noAvailableRecorded {
		return false
	}
	m.noAvailableRecorded = true
	return true
}

func recordTrafficDirectorPoolExhausted(plan *openAITrafficDirectorRequestPlan, poolKey string) {
	if plan != nil && plan.runtimeMetrics.markPoolExhausted(poolKey) {
		defaultTrafficDirectorRuntimeMetrics.poolExhausted.Add(1)
	}
}

func recordTrafficDirectorFallbackTransition() {
	defaultTrafficDirectorRuntimeMetrics.fallbackTransition.Add(1)
}

func recordTrafficDirectorNoAvailablePool(plan *openAITrafficDirectorRequestPlan) {
	if plan != nil && plan.runtimeMetrics.markNoAvailable() {
		defaultTrafficDirectorRuntimeMetrics.noAvailable.Add(1)
	}
}

func recordTrafficDirectorNoAvailablePoolFromContext(ctx context.Context) {
	state := openAITrafficDirectorRequestStateFromContext(ctx)
	if state == nil {
		return
	}
	state.mu.Lock()
	plans := make([]*openAITrafficDirectorRequestPlan, 0, len(state.plans))
	for _, entry := range state.plans {
		if entry.plan != nil && strings.EqualFold(entry.plan.mode, domain.TrafficDirectorModeEnforced) {
			plans = append(plans, entry.plan)
		}
	}
	state.mu.Unlock()
	for _, plan := range plans {
		recordTrafficDirectorNoAvailablePool(plan)
	}
}

func recordTrafficDirectorHealthFailOpen() {
	defaultTrafficDirectorRuntimeMetrics.healthFailOpen.Add(1)
}
