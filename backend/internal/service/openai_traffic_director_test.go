package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

type openAITrafficDirectorResolverStub struct {
	policy TrafficDirectorResolvedPolicy
	calls  atomic.Int64
}

func (r *openAITrafficDirectorResolverStub) ResolveOpenAITrafficDirector(context.Context, int64) (TrafficDirectorResolvedPolicy, error) {
	r.calls.Add(1)
	return r.policy, nil
}

type openAITrafficDirectorHeadReaderStub struct {
	head  *TrafficDirectorHead
	err   error
	calls atomic.Int64
}

func (r *openAITrafficDirectorHeadReaderStub) GetTrafficDirectorHead(context.Context, int64) (*TrafficDirectorHead, error) {
	r.calls.Add(1)
	return r.head, r.err
}

type openAITrafficDirectorHealthStub struct {
	healthy bool
	err     error
	models  []string
}

type openAITrafficDirectorHealthDecisionStub struct {
	decision TrafficDirectorHealthDecision
	err      error
}

func (h *openAITrafficDirectorHealthDecisionStub) AccountHealthy(context.Context, int64, string) (bool, error) {
	return h.decision.Allowed, h.err
}

func (h *openAITrafficDirectorHealthDecisionStub) Check(context.Context, TrafficDirectorHealthCheckInput) (TrafficDirectorHealthDecision, error) {
	return h.decision, h.err
}

func (h *openAITrafficDirectorHealthStub) AccountHealthy(_ context.Context, _ int64, model string) (bool, error) {
	h.models = append(h.models, model)
	return h.healthy, h.err
}

func testOpenAITrafficDirectorSpec() domain.TrafficDirectorSpec {
	return domain.TrafficDirectorSpec{
		SchemaVersion: domain.TrafficDirectorSchemaVersion,
		HealthMode:    domain.TrafficDirectorHealthModeEnforce,
		Pools: []domain.TrafficDirectorPool{
			{Key: "primary", WeightBPS: 10000, AccountIDs: []int64{101}, MinAvailable: 1, FallbackPoolKey: "backup"},
			{Key: "backup", WeightBPS: 0, AccountIDs: []int64{202}, MinAvailable: 1},
		},
	}
}

func testOpenAIAccountForTrafficDirector(id int64, groupID int64) Account {
	return Account{
		ID:          id,
		Name:        "account",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
		GroupIDs:    []int64{groupID},
	}
}

func TestOpenAITrafficDirectorPlanIsResolvedOncePerRequest(t *testing.T) {
	resolver := &openAITrafficDirectorResolverStub{policy: TrafficDirectorResolvedPolicy{
		Version: TrafficDirectorVersion{GroupID: 42, Version: 7, Mode: domain.TrafficDirectorModeShadow, Spec: func() *domain.TrafficDirectorSpec {
			spec := testOpenAITrafficDirectorSpec()
			return &spec
		}()},
	}}
	svc := &OpenAIGatewayService{}
	svc.SetOpenAITrafficDirectorResolver(resolver)
	groupID := int64(42)
	ctx := context.WithValue(context.Background(), ctxkey.RequestID, "request-1")
	ctx = svc.WithOpenAITrafficDirectorRequestContext(ctx)

	first, err := svc.resolveOpenAITrafficDirectorPlan(ctx, &groupID, PlatformOpenAI, "")
	require.NoError(t, err)
	second, err := svc.resolveOpenAITrafficDirectorPlan(ctx, &groupID, PlatformOpenAI, "")
	require.NoError(t, err)
	require.Same(t, first, second)
	require.Equal(t, int64(1), resolver.calls.Load())
}

func TestOpenAITrafficDirectorPlatformBoundary(t *testing.T) {
	spec := testOpenAITrafficDirectorSpec()
	resolver := &openAITrafficDirectorResolverStub{policy: TrafficDirectorResolvedPolicy{
		Version: TrafficDirectorVersion{GroupID: 42, Version: 7, Mode: domain.TrafficDirectorModeShadow, Spec: &spec},
	}}
	svc := &OpenAIGatewayService{}
	svc.SetOpenAITrafficDirectorResolver(resolver)
	groupID := int64(42)
	newGroupContext := func(platform string) context.Context {
		return context.WithValue(context.Background(), ctxkey.Group, &Group{
			ID:       groupID,
			Platform: platform,
			Status:   StatusActive,
			Hydrated: true,
		})
	}

	// A non-OpenAI Group must remain on the legacy scheduler even when the
	// shared OpenAI-compatible caller normalized its platform argument.
	plan, err := svc.resolveOpenAITrafficDirectorPlan(
		newGroupContext(PlatformAnthropic), &groupID, PlatformOpenAI, "request-1",
	)
	require.NoError(t, err)
	require.Nil(t, plan)

	// Composite routing is eligible only after its concrete target is explicit.
	compositeAnthropic := WithResolvedTargetPlatform(newGroupContext(PlatformComposite), PlatformAnthropic)
	plan, err = svc.resolveOpenAITrafficDirectorPlan(compositeAnthropic, &groupID, PlatformOpenAI, "request-2")
	require.NoError(t, err)
	require.Nil(t, plan)

	compositeOpenAI := WithResolvedTargetPlatform(newGroupContext(PlatformComposite), PlatformOpenAI)
	plan, err = svc.resolveOpenAITrafficDirectorPlan(compositeOpenAI, &groupID, PlatformOpenAI, "request-3")
	require.NoError(t, err)
	require.NotNil(t, plan)

	// Without an authoritative Group snapshot, only an explicit OpenAI argument
	// may opt into the new resolver; an empty platform is deliberately unknown.
	plan, err = svc.resolveOpenAITrafficDirectorPlan(context.Background(), &groupID, PlatformAnthropic, "request-4")
	require.NoError(t, err)
	require.Nil(t, plan)
	plan, err = svc.resolveOpenAITrafficDirectorPlan(context.Background(), &groupID, "", "request-5")
	require.NoError(t, err)
	require.Nil(t, plan)

	// The unified scheduler normalizes legacy OpenAI-compatible platform values.
	// That compatibility step must not turn an unauthoritative Anthropic call into
	// a Traffic Director lookup when no Group/target snapshot is present.
	svc.accountRepo = schedulerTestOpenAIAccountRepo{}
	_, _, _ = svc.SelectAccountWithSchedulerForCapability(
		context.Background(),
		&groupID,
		"",
		"",
		"gpt-5",
		nil,
		OpenAIUpstreamTransportAny,
		OpenAIEndpointCapabilityChatCompletions,
		false,
		false,
		true,
		PlatformAnthropic,
	)
	require.Equal(t, int64(1), resolver.calls.Load())
}

func TestOpenAITrafficDirectorPolicyResolverUsesAuthGroupHead(t *testing.T) {
	policy := newTrafficDirectorPolicyCacheTestVersion(t, 42, 7, domain.TrafficDirectorModeShadow)
	store := newTrafficDirectorPolicyStoreStub(policy)
	headReader := &openAITrafficDirectorHeadReaderStub{err: errors.New("unexpected head lookup")}
	resolver := NewOpenAITrafficDirectorPolicyResolver(
		headReader,
		NewTrafficDirectorPolicyCache(store, nil, 8, 0),
	)
	ctx := context.WithValue(context.Background(), ctxkey.Group, &Group{
		ID:                     42,
		Platform:               PlatformOpenAI,
		Status:                 StatusActive,
		Hydrated:               true,
		TrafficDirectorMode:    domain.TrafficDirectorModeShadow,
		TrafficDirectorVersion: 7,
	})

	resolved, err := resolver.ResolveOpenAITrafficDirector(ctx, 42)
	require.NoError(t, err)
	require.Equal(t, int64(7), resolved.Version.Version)
	require.Equal(t, domain.TrafficDirectorModeShadow, resolved.Version.Mode)
	require.Zero(t, headReader.calls.Load())
	require.Equal(t, int64(1), store.totalCalls.Load())

	legacyCtx := context.WithValue(context.Background(), ctxkey.Group, &Group{
		ID:                     42,
		Platform:               PlatformOpenAI,
		Status:                 StatusActive,
		Hydrated:               true,
		TrafficDirectorMode:    domain.TrafficDirectorModeLegacy,
		TrafficDirectorVersion: TrafficDirectorLegacyVersion,
	})
	legacy, err := resolver.ResolveOpenAITrafficDirector(legacyCtx, 42)
	require.NoError(t, err)
	require.Equal(t, TrafficDirectorLegacyVersion, legacy.Version.Version)
	require.Equal(t, TrafficDirectorPolicySourceLegacy, legacy.Source)
	require.Zero(t, headReader.calls.Load())
}

func TestOpenAITrafficDirectorMissingDependenciesFailClosedForEnforcedHead(t *testing.T) {
	groupID := int64(42)
	ctx := context.WithValue(context.Background(), ctxkey.Group, &Group{
		ID:                     groupID,
		Platform:               PlatformOpenAI,
		Status:                 StatusActive,
		Hydrated:               true,
		TrafficDirectorMode:    domain.TrafficDirectorModeEnforced,
		TrafficDirectorVersion: 3,
	})

	svc := &OpenAIGatewayService{}
	ctx = svc.WithOpenAITrafficDirectorRequestContext(ctx)
	plan, err := svc.resolveOpenAITrafficDirectorPlan(ctx, &groupID, PlatformOpenAI, "request-1")
	require.Nil(t, plan)
	require.ErrorIs(t, err, ErrTrafficDirectorPolicyUnavailable)

	var nilResolver *OpenAITrafficDirectorPolicyResolver
	_, err = nilResolver.ResolveOpenAITrafficDirector(context.Background(), groupID)
	require.ErrorIs(t, err, ErrTrafficDirectorPolicyUnavailable)

	resolverWithoutHeadReader := &OpenAITrafficDirectorPolicyResolver{
		cache: NewTrafficDirectorPolicyCache(nil, nil, 8, 0),
	}
	_, err = resolverWithoutHeadReader.ResolveOpenAITrafficDirector(context.Background(), groupID)
	require.ErrorIs(t, err, ErrTrafficDirectorPolicyUnavailable)
}

func TestOpenAITrafficDirectorEnforcedRequestHeadRequiresExactResolvedPolicy(t *testing.T) {
	spec := testOpenAITrafficDirectorSpec()
	groupID := int64(42)
	tests := []struct {
		name        string
		headVersion int64
		resolved    TrafficDirectorVersion
		wantErr     bool
	}{
		{
			name:        "exact immutable version",
			headVersion: 3,
			resolved: TrafficDirectorVersion{
				GroupID: groupID,
				Version: 3,
				Mode:    domain.TrafficDirectorModeEnforced,
				Spec:    &spec,
			},
		},
		{
			name:        "stale immutable version",
			headVersion: 3,
			resolved: TrafficDirectorVersion{
				GroupID: groupID,
				Version: 2,
				Mode:    domain.TrafficDirectorModeEnforced,
				Spec:    &spec,
			},
			wantErr: true,
		},
		{
			name:        "resolver mode downgrade",
			headVersion: 3,
			resolved: TrafficDirectorVersion{
				GroupID: groupID,
				Version: TrafficDirectorLegacyVersion,
				Mode:    domain.TrafficDirectorModeLegacy,
			},
			wantErr: true,
		},
		{
			name:        "resolver group mismatch",
			headVersion: TrafficDirectorLegacyVersion,
			resolved: TrafficDirectorVersion{
				GroupID: 99,
				Version: 1,
				Mode:    domain.TrafficDirectorModeEnforced,
				Spec:    &spec,
			},
			wantErr: true,
		},
		{
			name:        "invalid enforced version zero",
			headVersion: TrafficDirectorLegacyVersion,
			resolved: TrafficDirectorVersion{
				GroupID: groupID,
				Version: TrafficDirectorLegacyVersion,
				Mode:    domain.TrafficDirectorModeEnforced,
				Spec:    &spec,
			},
			wantErr: true,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			resolver := &openAITrafficDirectorResolverStub{policy: TrafficDirectorResolvedPolicy{
				Version: testCase.resolved,
			}}
			svc := &OpenAIGatewayService{}
			svc.SetOpenAITrafficDirectorResolver(resolver)
			ctx := context.WithValue(context.Background(), ctxkey.Group, &Group{
				ID:                     groupID,
				Platform:               PlatformOpenAI,
				Status:                 StatusActive,
				Hydrated:               true,
				TrafficDirectorMode:    domain.TrafficDirectorModeEnforced,
				TrafficDirectorVersion: testCase.headVersion,
			})
			ctx = svc.WithOpenAITrafficDirectorRequestContext(ctx)

			plan, err := svc.resolveOpenAITrafficDirectorPlan(ctx, &groupID, PlatformOpenAI, "request-exact-head")
			if testCase.wantErr {
				require.Nil(t, plan)
				require.ErrorIs(t, err, ErrTrafficDirectorPolicyUnavailable)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, plan)
			require.Equal(t, testCase.headVersion, plan.policy.Version)
		})
	}
}

func TestOpenAITrafficDirectorLegacyContextBypassesRequestStateAndResolver(t *testing.T) {
	resolver := &openAITrafficDirectorResolverStub{}
	svc := &OpenAIGatewayService{}
	svc.SetOpenAITrafficDirectorResolver(resolver)
	groupID := int64(42)
	ctx := context.WithValue(context.Background(), ctxkey.Group, &Group{
		ID:                     groupID,
		Platform:               PlatformOpenAI,
		Status:                 StatusActive,
		Hydrated:               true,
		TrafficDirectorMode:    domain.TrafficDirectorModeLegacy,
		TrafficDirectorVersion: 9,
	})

	withRetry := svc.WithOpenAITrafficDirectorRetryLoopContext(ctx)
	require.Same(t, ctx, withRetry)
	plan, err := svc.resolveOpenAITrafficDirectorPlan(withRetry, &groupID, PlatformOpenAI, "request-1")
	require.NoError(t, err)
	require.Nil(t, plan)
	require.Zero(t, resolver.calls.Load())
}

func TestOpenAITrafficDirectorV1BypassSkipsPolicyResolution(t *testing.T) {
	resolver := &openAITrafficDirectorResolverStub{}
	svc := &OpenAIGatewayService{}
	svc.SetOpenAITrafficDirectorResolver(resolver)
	groupID := int64(42)
	ctx := context.WithValue(context.Background(), ctxkey.Group, &Group{
		ID:                     groupID,
		Platform:               PlatformOpenAI,
		Status:                 StatusActive,
		Hydrated:               true,
		TrafficDirectorMode:    domain.TrafficDirectorModeEnforced,
		TrafficDirectorVersion: 9,
	})
	ctx = withOpenAITrafficDirectorV1Bypass(ctx)

	withRetry := svc.WithOpenAITrafficDirectorRetryLoopContext(ctx)
	require.Same(t, ctx, withRetry)
	plan, err := svc.resolveOpenAITrafficDirectorPlan(withRetry, &groupID, PlatformOpenAI, "live-call")
	require.NoError(t, err)
	require.Nil(t, plan)
	require.Zero(t, resolver.calls.Load())
}

func TestOpenAITrafficDirectorShadowComparesFinalLegacySelectionOnce(t *testing.T) {
	spec := testOpenAITrafficDirectorSpec()
	resolver := &openAITrafficDirectorResolverStub{policy: TrafficDirectorResolvedPolicy{
		Version: TrafficDirectorVersion{GroupID: 42, Version: 7, Mode: domain.TrafficDirectorModeShadow, Spec: &spec},
	}}
	svc := &OpenAIGatewayService{}
	svc.SetOpenAITrafficDirectorResolver(resolver)
	groupID := int64(42)
	ctx := context.WithValue(context.Background(), ctxkey.RequestID, "request-shadow")
	ctx = svc.WithOpenAITrafficDirectorRequestContext(ctx)

	plan, err := svc.resolveOpenAITrafficDirectorPlan(ctx, &groupID, PlatformOpenAI, "")
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.False(t, plan.shadowLogged)

	svc.logTrafficDirectorShadowSelectionOnce(ctx, &groupID, "gpt-5", 101)
	require.True(t, plan.shadowLogged)
	svc.logTrafficDirectorShadowSelectionOnce(ctx, &groupID, "gpt-5", 202)
	require.True(t, plan.shadowLogged)
}

func TestOpenAITrafficDirectorPoolChainAdvancesMonotonically(t *testing.T) {
	spec := testOpenAITrafficDirectorSpec()
	evaluation, err := EvaluateTrafficDirector(spec, 42, "request-1")
	require.NoError(t, err)
	plan := &openAITrafficDirectorRequestPlan{
		mode:          domain.TrafficDirectorModeEnforced,
		evaluation:    evaluation,
		poolByKey:     map[string]domain.TrafficDirectorPool{"primary": spec.Pools[0], "backup": spec.Pools[1]},
		poolByAccount: map[int64]string{101: "primary", 202: "backup"},
	}
	pool, ok := plan.currentPool()
	require.True(t, ok)
	require.Equal(t, "primary", pool.Key)
	require.True(t, plan.advancePool())
	pool, ok = plan.currentPool()
	require.True(t, ok)
	require.Equal(t, "backup", pool.Key)
	require.False(t, plan.advancePool())
	// Once the chain is exhausted, the plan cannot bounce back to the home pool.
	require.Equal(t, 1, plan.currentIndex)
}

func TestOpenAITrafficDirectorPoolLocalSelectionDoesNotAdvanceFallback(t *testing.T) {
	spec := testOpenAITrafficDirectorSpec()
	resolver := &openAITrafficDirectorResolverStub{policy: TrafficDirectorResolvedPolicy{
		Version: TrafficDirectorVersion{GroupID: 42, Version: 1, Mode: domain.TrafficDirectorModeEnforced, Spec: &spec},
	}}
	svc := &OpenAIGatewayService{accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{
		testOpenAIAccountForTrafficDirector(202, 42),
	}}}
	svc.SetOpenAITrafficDirectorResolver(resolver)
	groupID := int64(42)
	ctx := context.WithValue(context.Background(), ctxkey.RequestID, "request-pool-local")
	ctx = svc.WithOpenAITrafficDirectorRequestContext(ctx)
	ctx = withOpenAITrafficDirectorPoolAdvanceSuppressed(ctx)

	plan, allowed, err := svc.trafficDirectorSelectPool(
		ctx,
		&groupID,
		PlatformOpenAI,
		"",
		"gpt-image-1",
		OpenAIUpstreamTransportHTTPSSE,
		"",
		OpenAIImagesCapabilityNative,
		false,
		nil,
	)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
	require.Empty(t, allowed)
	require.NotNil(t, plan)
	require.Zero(t, plan.currentIndex,
		"a pool-local capability probe must not consume the explicit fallback")
}

func TestOpenAITrafficDirectorPoolEligibilityExcludesOwnedHalfOpenProbe(t *testing.T) {
	spec := testOpenAITrafficDirectorSpec()
	plan := &openAITrafficDirectorRequestPlan{
		key:    openAITrafficDirectorPlanKey{groupID: 42, platform: PlatformOpenAI},
		policy: TrafficDirectorVersion{GroupID: 42, Version: 7, Mode: domain.TrafficDirectorModeEnforced, Spec: &spec},
	}
	health := &openAITrafficDirectorHealthDecisionStub{decision: TrafficDirectorHealthDecision{
		State:      TrafficDirectorHealthStateHalfOpen,
		Allowed:    false,
		ProbeUntil: time.Now().Add(time.Minute),
	}}
	svc := &OpenAIGatewayService{}
	svc.SetOpenAITrafficDirectorHealthResolver(health)

	require.False(t, svc.trafficDirectorAccountEligibleForPool(context.Background(), plan, 101, "gpt-5"),
		"a probe owned by another request must not satisfy min_available")
	health.decision.ProbeUntil = time.Time{}
	require.True(t, svc.trafficDirectorAccountEligibleForPool(context.Background(), plan, 101, "gpt-5"),
		"an expired half-open lease remains eligible for final probe admission")
}

func TestOpenAITrafficDirectorHealthEnforceAndFailOpen(t *testing.T) {
	spec := testOpenAITrafficDirectorSpec()
	plan := &openAITrafficDirectorRequestPlan{
		key:    openAITrafficDirectorPlanKey{groupID: 42},
		policy: TrafficDirectorVersion{Spec: &spec},
	}
	svc := &OpenAIGatewayService{}
	health := &openAITrafficDirectorHealthStub{healthy: false}
	svc.SetOpenAITrafficDirectorHealthResolver(health)
	require.False(t, svc.trafficDirectorAccountHealthy(context.Background(), plan, 101, "gpt-5"))
	require.Equal(t, []string{"gpt-5"}, health.models)

	health.err = errors.New("redis unavailable")
	require.True(t, svc.trafficDirectorAccountHealthy(context.Background(), plan, 101, "gpt-5"))
}

func TestOpenAITrafficDirectorUnifiedSelectionFallsBackAfterExclusion(t *testing.T) {
	spec := testOpenAITrafficDirectorSpec()
	resolver := &openAITrafficDirectorResolverStub{policy: TrafficDirectorResolvedPolicy{
		Version: TrafficDirectorVersion{GroupID: 42, Version: 1, Mode: domain.TrafficDirectorModeEnforced, Spec: &spec},
	}}
	svc := &OpenAIGatewayService{accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{
		testOpenAIAccountForTrafficDirector(101, 42),
		testOpenAIAccountForTrafficDirector(202, 42),
	}}}
	svc.SetOpenAITrafficDirectorResolver(resolver)
	groupID := int64(42)
	ctx := context.WithValue(context.Background(), ctxkey.RequestID, "request-1")
	ctx = svc.WithOpenAITrafficDirectorRequestContext(ctx)

	first, _, err := svc.SelectAccountWithSchedulerForCapability(ctx, &groupID, "", "", "gpt-5", nil, OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions, false, false, false)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, int64(101), first.Account.ID)
	if first.ReleaseFunc != nil {
		first.ReleaseFunc()
	}

	second, _, err := svc.SelectAccountWithSchedulerForCapability(ctx, &groupID, "", "", "gpt-5", map[int64]struct{}{101: {}}, OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions, false, false, false)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.Equal(t, int64(202), second.Account.ID)
	if second.ReleaseFunc != nil {
		second.ReleaseFunc()
	}

	_, _, err = svc.SelectAccountWithSchedulerForCapability(ctx, &groupID, "", "", "gpt-5", map[int64]struct{}{101: {}, 202: {}}, OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions, false, false, false)
	require.ErrorIs(t, err, ErrTrafficDirectorNoAvailablePool)
}

func TestOpenAITrafficDirectorMovablePreviousResponseStaysInsidePool(t *testing.T) {
	defer resetOpenAIAdvancedSchedulerSettingCacheForTest()

	for _, testCase := range []struct {
		name     string
		advanced bool
	}{
		{name: "basic"},
		{name: "advanced_sticky_weighted_disabled", advanced: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			resetOpenAIAdvancedSchedulerSettingCacheForTest()
			spec := testOpenAITrafficDirectorSpec()
			resolver := &openAITrafficDirectorResolverStub{policy: TrafficDirectorResolvedPolicy{
				Version: TrafficDirectorVersion{GroupID: 42, Version: 1, Mode: domain.TrafficDirectorModeEnforced, Spec: &spec},
			}}
			svc := &OpenAIGatewayService{accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{
				testOpenAIAccountForTrafficDirector(101, 42),
				testOpenAIAccountForTrafficDirector(202, 42),
			}}}
			if testCase.advanced {
				svc.rateLimitService = newOpenAIAdvancedSchedulerRateLimitService("true", "false")
				svc.concurrencyService = NewConcurrencyService(schedulerTestConcurrencyCache{})
			}
			svc.SetOpenAITrafficDirectorResolver(resolver)
			require.NoError(t, svc.getOpenAIWSStateStore().BindResponseAccount(context.Background(), 42, "resp_backup", 202, time.Hour))

			groupID := int64(42)
			ctx := context.WithValue(context.Background(), ctxkey.RequestID, "request-movable-previous-"+testCase.name)
			ctx = svc.WithOpenAITrafficDirectorRequestContext(ctx)
			selection, _, err := svc.SelectAccountWithSchedulerForCapability(
				ctx,
				&groupID,
				"resp_backup",
				"",
				"gpt-5",
				nil,
				OpenAIUpstreamTransportAny,
				OpenAIEndpointCapabilityChatCompletions,
				false,
				true,
				false,
			)
			require.NoError(t, err)
			require.NotNil(t, selection)
			require.Equal(t, int64(101), selection.Account.ID,
				"movable previous-response affinity must not override the selected pool")
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}
