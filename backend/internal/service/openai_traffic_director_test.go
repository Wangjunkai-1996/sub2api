package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/stretchr/testify/require"
)

type openAITrafficDirectorResolverStub struct {
	policy TrafficDirectorResolvedPolicy
	calls  atomic.Int64
}

type trafficDirectorEligibilityGroupRepoStub struct {
	GroupRepository
	group *Group
}

type trafficDirectorRequirementSnapshotCache struct {
	SchedulerCache
	snapshotAccounts []*Account
	freshByID        map[int64]*Account
	metadataByID     map[int64]*Account
	getAccountCalls  map[int64]int
}

type trafficDirectorRequirementAccountRepo struct {
	AccountRepository
	accountsByID map[int64]*Account
	getByIDCalls map[int64]int
}

func (r *trafficDirectorRequirementAccountRepo) GetByID(_ context.Context, accountID int64) (*Account, error) {
	if r.getByIDCalls == nil {
		r.getByIDCalls = make(map[int64]int)
	}
	r.getByIDCalls[accountID]++
	account := r.accountsByID[accountID]
	if account == nil {
		return nil, errors.New("account not found")
	}
	clone := *account
	return &clone, nil
}

func (c *trafficDirectorRequirementSnapshotCache) GetSnapshot(context.Context, SchedulerBucket) ([]*Account, bool, error) {
	accounts := make([]*Account, 0, len(c.snapshotAccounts))
	for _, account := range c.snapshotAccounts {
		if account == nil {
			continue
		}
		clone := *account
		accounts = append(accounts, &clone)
	}
	return accounts, true, nil
}

func (c *trafficDirectorRequirementSnapshotCache) GetAccount(_ context.Context, accountID int64) (*Account, error) {
	if c.getAccountCalls == nil {
		c.getAccountCalls = make(map[int64]int)
	}
	c.getAccountCalls[accountID]++
	account := c.freshByID[accountID]
	if account == nil {
		return nil, nil
	}
	clone := *account
	return &clone, nil
}

func (c *trafficDirectorRequirementSnapshotCache) GetAccountMetadataByIDs(_ context.Context, accountIDs []int64) (map[int64]*Account, error) {
	accounts := make(map[int64]*Account, len(accountIDs))
	for _, accountID := range accountIDs {
		account := c.metadataByID[accountID]
		if account == nil {
			accounts[accountID] = nil
			continue
		}
		clone := *account
		accounts[accountID] = &clone
	}
	return accounts, nil
}

func (r trafficDirectorEligibilityGroupRepoStub) GetByID(context.Context, int64) (*Group, error) {
	return r.group, nil
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
	checks   []TrafficDirectorHealthCheckInput
}

type openAITrafficDirectorSingleProbeStub struct {
	checks       []TrafficDirectorHealthCheckInput
	acquireCalls int
}

func (h *openAITrafficDirectorHealthDecisionStub) AccountHealthy(context.Context, int64, string) (bool, error) {
	return h.decision.Allowed, h.err
}

func (h *openAITrafficDirectorHealthDecisionStub) Check(_ context.Context, input TrafficDirectorHealthCheckInput) (TrafficDirectorHealthDecision, error) {
	h.checks = append(h.checks, input)
	return h.decision, h.err
}

func (h *openAITrafficDirectorSingleProbeStub) AccountHealthy(context.Context, int64, string) (bool, error) {
	return true, nil
}

func (h *openAITrafficDirectorSingleProbeStub) Check(
	_ context.Context,
	input TrafficDirectorHealthCheckInput,
) (TrafficDirectorHealthDecision, error) {
	h.checks = append(h.checks, input)
	decision := TrafficDirectorHealthDecision{
		AccountID:  input.AccountID,
		Model:      input.Model,
		HealthMode: input.HealthMode,
		State:      TrafficDirectorHealthStateHalfOpen,
	}
	if input.AcquireProbe != nil && !*input.AcquireProbe {
		return decision, nil
	}
	h.acquireCalls++
	if h.acquireCalls == 1 {
		decision.Allowed = true
		decision.HalfOpenProbe = true
		decision.ProbeToken = "single-probe"
		return decision, nil
	}
	decision.ProbeUntil = time.Now().Add(time.Minute)
	return decision, nil
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

func TestOpenAITrafficDirectorMissingResolverFallsBackForPublishedShadow(t *testing.T) {
	groupID := int64(42)
	svc := &OpenAIGatewayService{}
	ctx := context.WithValue(context.Background(), ctxkey.Group, &Group{
		ID:                     groupID,
		Platform:               PlatformOpenAI,
		Status:                 StatusActive,
		Hydrated:               true,
		TrafficDirectorMode:    domain.TrafficDirectorModeShadow,
		TrafficDirectorVersion: 3,
	})
	ctx = svc.WithOpenAITrafficDirectorRequestContext(ctx)
	before := SnapshotTrafficDirectorRuntimeMetrics()

	for range 2 {
		plan, err := svc.resolveOpenAITrafficDirectorPlan(ctx, &groupID, PlatformOpenAI, "request-shadow-no-resolver")
		require.NoError(t, err)
		require.Nil(t, plan)
	}

	after := SnapshotTrafficDirectorRuntimeMetrics()
	require.Equal(t, before.RoutingDecisions.LegacyTotal+1, after.RoutingDecisions.LegacyTotal)
	require.Equal(t, before.RoutingDecisions.ShadowTotal, after.RoutingDecisions.ShadowTotal)
	require.Equal(t, before.Policy.LegacyFallbackTotal+1, after.Policy.LegacyFallbackTotal)
	require.Equal(t, before.Policy.UnavailableTotal, after.Policy.UnavailableTotal)
}

func TestOpenAITrafficDirectorMissingResolverWithoutHeadKeepsUnavailableMetric(t *testing.T) {
	groupID := int64(42)
	svc := &OpenAIGatewayService{}
	ctx := svc.WithOpenAITrafficDirectorRequestContext(context.Background())
	before := SnapshotTrafficDirectorRuntimeMetrics()

	for range 2 {
		plan, err := svc.resolveOpenAITrafficDirectorPlan(ctx, &groupID, PlatformOpenAI, "request-no-head-no-resolver")
		require.NoError(t, err)
		require.Nil(t, plan)
	}

	after := SnapshotTrafficDirectorRuntimeMetrics()
	require.Equal(t, before.Policy.UnavailableTotal+1, after.Policy.UnavailableTotal)
	require.Equal(t, before.Policy.LegacyFallbackTotal, after.Policy.LegacyFallbackTotal)
}

func TestOpenAITrafficDirectorMissingResolverRejectsInvalidHead(t *testing.T) {
	groupID := int64(42)
	for _, testCase := range []struct {
		name    string
		mode    string
		version int64
	}{
		{name: "invalid mode", mode: "unsupported", version: 3},
		{name: "negative version", mode: domain.TrafficDirectorModeShadow, version: -1},
		{name: "version zero shadow", mode: domain.TrafficDirectorModeShadow, version: TrafficDirectorLegacyVersion},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			svc := &OpenAIGatewayService{}
			ctx := context.WithValue(context.Background(), ctxkey.Group, &Group{
				ID:                     groupID,
				Platform:               PlatformOpenAI,
				Status:                 StatusActive,
				Hydrated:               true,
				TrafficDirectorMode:    testCase.mode,
				TrafficDirectorVersion: testCase.version,
			})
			ctx = svc.WithOpenAITrafficDirectorRequestContext(ctx)

			plan, err := svc.resolveOpenAITrafficDirectorPlan(ctx, &groupID, PlatformOpenAI, "request-invalid-head")
			require.Nil(t, plan)
			require.ErrorIs(t, err, ErrTrafficDirectorPolicyUnavailable)
		})
	}
}

func TestOpenAITrafficDirectorMissingResolverShadowUsesLegacyScheduler(t *testing.T) {
	groupID := int64(42)
	account := testOpenAIAccountForTrafficDirector(101, groupID)
	svc := &OpenAIGatewayService{accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{account}}}
	ctx := context.WithValue(context.Background(), ctxkey.RequestID, "request-shadow-legacy-scheduler")
	ctx = context.WithValue(ctx, ctxkey.Group, &Group{
		ID:                     groupID,
		Platform:               PlatformOpenAI,
		Status:                 StatusActive,
		Hydrated:               true,
		TrafficDirectorMode:    domain.TrafficDirectorModeShadow,
		TrafficDirectorVersion: 3,
	})

	selection, decision, err := svc.SelectAccountWithSchedulerForCapability(
		ctx,
		&groupID,
		"",
		"",
		"gpt-5",
		nil,
		OpenAIUpstreamTransportAny,
		OpenAIEndpointCapabilityChatCompletions,
		false,
		false,
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, account.ID, selection.Account.ID)
	require.Equal(t, openAIAccountScheduleLayerLoadBalance, decision.Layer)
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
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

func TestOpenAITrafficDirectorHealthModelMatchesEndpointForwarding(t *testing.T) {
	account := func(accountType string, mapping map[string]any, passthrough bool) *Account {
		return &Account{
			ID:          101,
			Platform:    PlatformOpenAI,
			Type:        accountType,
			Credentials: map[string]any{"model_mapping": mapping},
			Extra:       map[string]any{"openai_passthrough": passthrough},
		}
	}
	withCompactMapping := func(base *Account) *Account {
		base.Credentials["compact_model_mapping"] = map[string]any{"channel-model": "compact-upstream"}
		return base
	}
	withAnthropicProtocol := func(base *Account) *Account {
		base.Platform = PlatformKimi
		base.Credentials["api_protocol"] = APIProtocolAnthropic
		return base
	}

	tests := []struct {
		name           string
		ctx            context.Context
		account        *Account
		requestedModel string
		requireCompact bool
		want           string
	}{
		{
			name:           "ordinary endpoints map passthrough accounts",
			ctx:            WithOpenAITrafficDirectorHealthModel(context.Background(), "channel-model"),
			account:        account(AccountTypeAPIKey, map[string]any{"channel-model": "account-upstream"}, true),
			requestedModel: "public-model",
			want:           "account-upstream",
		},
		{
			name:           "responses passthrough keeps channel model",
			ctx:            WithOpenAIResponsesTrafficDirectorHealthModel(context.Background(), "channel-model", false),
			account:        account(AccountTypeAPIKey, map[string]any{"channel-model": "must-not-apply"}, true),
			requestedModel: "public-model",
			want:           "channel-model",
		},
		{
			name:           "responses compact uses compact mapping",
			ctx:            WithOpenAIResponsesTrafficDirectorHealthModel(context.Background(), "channel-model", true),
			account:        withCompactMapping(account(AccountTypeOAuth, map[string]any{"channel-model": "must-not-apply"}, true)),
			requestedModel: "public-model",
			requireCompact: true,
			want:           "compact-upstream",
		},
		{
			name: "responses native Anthropic precedes passthrough",
			ctx:  WithOpenAIResponsesTrafficDirectorHealthModel(context.Background(), "channel-model", false),
			account: withAnthropicProtocol(account(AccountTypeAPIKey, map[string]any{
				"channel-model": "account-upstream",
			}, true)),
			requestedModel: "public-model",
			want:           "account-upstream",
		},
		{
			name: "responses native Anthropic ignores compact mapping",
			ctx:  WithOpenAIResponsesTrafficDirectorHealthModel(context.Background(), "channel-model", true),
			account: withAnthropicProtocol(withCompactMapping(account(AccountTypeAPIKey, map[string]any{
				"channel-model": "account-upstream",
			}, false))),
			requestedModel: "public-model",
			requireCompact: true,
			want:           "account-upstream",
		},
		{
			name: "messages normalizes before account mapping",
			ctx: WithOpenAIMessagesTrafficDirectorHealthModel(
				context.Background(), "gpt-5.4-high", "dispatch-fallback",
			),
			account: account(AccountTypeAPIKey, map[string]any{
				"gpt-5.4": "account-upstream",
			}, false),
			requestedModel: "gpt-5.4-high",
			want:           "account-upstream",
		},
		{
			name: "messages dispatch model is an unmapped fallback",
			ctx: WithOpenAIMessagesTrafficDirectorHealthModel(
				context.Background(), "unmapped-client-model", "dispatch-fallback",
			),
			account: account(AccountTypeAPIKey, map[string]any{
				"dispatch-fallback": "must-not-remap",
			}, false),
			requestedModel: "unmapped-client-model",
			want:           "dispatch-fallback",
		},
		{
			name: "count tokens normalizes before account mapping",
			ctx: WithOpenAICountTokensTrafficDirectorHealthModel(
				context.Background(), "gpt-5.4-high", "dispatch-fallback",
			),
			account: account(AccountTypeAPIKey, map[string]any{
				"gpt-5.4": "account-upstream",
			}, false),
			requestedModel: "gpt-5.4-high",
			want:           "account-upstream",
		},
		{
			name:           "images API key applies mapping even in passthrough mode",
			ctx:            WithOpenAIImagesTrafficDirectorHealthModel(context.Background(), "gpt-image-2"),
			account:        account(AccountTypeAPIKey, map[string]any{"gpt-image-2": "gpt-image-custom"}, true),
			requestedModel: "gpt-image-2",
			want:           "gpt-image-custom",
		},
		{
			name:           "images OAuth ignores account mapping",
			ctx:            WithOpenAIImagesTrafficDirectorHealthModel(context.Background(), "gpt-image-2"),
			account:        account(AccountTypeOAuth, map[string]any{"gpt-image-2": "must-not-apply"}, false),
			requestedModel: "gpt-image-2",
			want:           "gpt-image-2",
		},
		{
			name:           "responses image-only model uses carrier model",
			ctx:            WithOpenAIResponsesTrafficDirectorHealthModel(context.Background(), "gpt-image-2", false),
			account:        account(AccountTypeOAuth, nil, false),
			requestedModel: "gpt-image-2",
			want:           openAIImagesResponsesMainModel,
		},
		{
			name:           "responses image-only passthrough keeps image model",
			ctx:            WithOpenAIResponsesTrafficDirectorHealthModel(context.Background(), "gpt-image-2", false),
			account:        account(AccountTypeAPIKey, nil, true),
			requestedModel: "gpt-image-2",
			want:           "gpt-image-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selectionModel := openAITrafficDirectorHealthModelForRequest(
				tt.ctx,
				tt.account,
				tt.requestedModel,
				tt.requireCompact,
			)
			reportModel := canonicalOpenAITrafficDirectorHealthModel(
				tt.ctx,
				tt.account,
				openAITrafficDirectorHealthModelContextForRequest(tt.ctx, tt.requestedModel, tt.requireCompact).model,
			)
			require.Equal(t, tt.want, selectionModel)
			require.Equal(t, NormalizeTrafficDirectorHealthModel(tt.want), reportModel)
		})
	}
}

func TestOpenAITrafficDirectorHealthUsesForwardedModelChain(t *testing.T) {
	groupID := int64(42)
	spec := testOpenAITrafficDirectorSpec()
	plan := &openAITrafficDirectorRequestPlan{
		key:           openAITrafficDirectorPlanKey{groupID: groupID, platform: PlatformOpenAI},
		mode:          domain.TrafficDirectorModeEnforced,
		policy:        TrafficDirectorVersion{GroupID: groupID, Version: 7, Mode: domain.TrafficDirectorModeEnforced, Spec: &spec},
		poolByAccount: map[int64]string{101: "primary"},
	}
	account := testOpenAIAccountForTrafficDirector(101, groupID)
	account.Credentials = map[string]any{
		"model_mapping": map[string]any{
			"gpt-5":         "gpt-5",
			"gpt-5-channel": "account-upstream-model",
		},
	}
	health := &openAITrafficDirectorHealthDecisionStub{decision: TrafficDirectorHealthDecision{
		State:   TrafficDirectorHealthStateHealthy,
		Allowed: true,
	}}
	svc := &OpenAIGatewayService{accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{account}}}
	svc.SetOpenAITrafficDirectorHealthResolver(health)

	ctx := svc.WithOpenAITrafficDirectorRequestContext(context.Background())
	ctx = WithOpenAITrafficDirectorHealthModel(ctx, "gpt-5-channel")
	state := openAITrafficDirectorRequestStateFromContext(ctx)
	state.plans[plan.key] = openAITrafficDirectorPlanEntry{plan: plan}

	allowed, err := svc.trafficDirectorEligibleAccountIDs(
		ctx,
		&groupID,
		PlatformOpenAI,
		"gpt-5",
		OpenAIUpstreamTransportAny,
		OpenAIEndpointCapabilityChatCompletions,
		"",
		false,
		nil,
		plan,
		spec.Pools[0],
	)
	require.NoError(t, err)
	require.Contains(t, allowed, account.ID)

	admitted, cleanup := svc.trafficDirectorHealthAdmission(ctx, &account, "gpt-5", false)
	require.True(t, admitted)
	require.Nil(t, cleanup)
	require.Len(t, health.checks, 2)
	for _, check := range health.checks {
		require.Equal(t, "account-upstream-model", check.Model)
	}
}

func TestOpenAITrafficDirectorEligibleAccountIDs_AuditExemptRequirementFiltersCachedPool(t *testing.T) {
	groupID := int64(42)
	proParentID := int64(9100)
	proParent := testOpenAIAccountForTrafficDirector(proParentID, groupID)
	proParent.Credentials = map[string]any{"plan_type": "pro"}

	account := func(id int64, accountType, planType string) *Account {
		candidate := testOpenAIAccountForTrafficDirector(id, groupID)
		candidate.Type = accountType
		if planType != "" {
			candidate.Credentials = map[string]any{"plan_type": planType}
		}
		return &candidate
	}
	pro := account(101, AccountTypeOAuth, "pro")
	unknown := account(102, AccountTypeOAuth, "future_plan")
	shadow := account(103, AccountTypeOAuth, "")
	shadow.ParentAccountID = &proParentID
	shadow.QuotaDimension = QuotaDimensionSpark
	plus := account(104, AccountTypeOAuth, "plus")
	team := account(105, AccountTypeOAuth, "team")
	apiKey := account(106, AccountTypeAPIKey, "")
	candidates := []*Account{pro, unknown, shadow, plus, team, apiKey}

	cache := &trafficDirectorRequirementSnapshotCache{
		snapshotAccounts: candidates,
		freshByID: map[int64]*Account{
			pro.ID: pro, unknown.ID: unknown, shadow.ID: shadow,
			plus.ID: plus, team.ID: team, apiKey.ID: apiKey,
		},
		metadataByID: map[int64]*Account{proParent.ID: &proParent},
	}
	svc := &OpenAIGatewayService{
		schedulerSnapshot: &SchedulerSnapshotService{cache: cache},
	}
	plan := &openAITrafficDirectorRequestPlan{
		key:           openAITrafficDirectorPlanKey{groupID: groupID, platform: PlatformOpenAI},
		mode:          domain.TrafficDirectorModeEnforced,
		poolByAccount: map[int64]string{},
	}
	pool := domain.TrafficDirectorPool{Key: "primary", WeightBPS: 10000, MinAvailable: 1}
	for _, candidate := range candidates {
		pool.AccountIDs = append(pool.AccountIDs, candidate.ID)
		plan.poolByAccount[candidate.ID] = pool.Key
	}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)

	allowed, err := svc.trafficDirectorEligibleAccountIDs(
		ctx, &groupID, PlatformOpenAI, "gpt-5",
		OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions, "", false,
		nil, plan, pool,
	)
	require.NoError(t, err)
	require.Len(t, allowed, 3)
	require.Contains(t, allowed, plus.ID)
	require.Contains(t, allowed, team.ID)
	require.Contains(t, allowed, apiKey.ID)
	require.NotContains(t, allowed, pro.ID)
	require.NotContains(t, allowed, unknown.ID)
	require.NotContains(t, allowed, shadow.ID, "a Spark shadow must inherit its Pro parent's audit requirement")
}

func TestOpenAITrafficDirectorEligibleAccountIDs_AuditExemptRequirementRejectsFreshPro(t *testing.T) {
	groupID := int64(42)
	cachedPlus := testOpenAIAccountForTrafficDirector(101, groupID)
	cachedPlus.Credentials = map[string]any{"plan_type": "plus"}
	freshPro := cachedPlus
	freshPro.Credentials = map[string]any{"plan_type": "pro"}
	cache := &trafficDirectorRequirementSnapshotCache{
		snapshotAccounts: []*Account{&cachedPlus},
	}
	repo := &trafficDirectorRequirementAccountRepo{
		accountsByID: map[int64]*Account{cachedPlus.ID: &freshPro},
	}
	svc := &OpenAIGatewayService{
		accountRepo:       repo,
		schedulerSnapshot: NewSchedulerSnapshotService(cache, nil, repo, nil, nil),
	}
	pool := domain.TrafficDirectorPool{
		Key: "primary", WeightBPS: 10000, AccountIDs: []int64{cachedPlus.ID}, MinAvailable: 1,
	}
	plan := &openAITrafficDirectorRequestPlan{
		key:           openAITrafficDirectorPlanKey{groupID: groupID, platform: PlatformOpenAI},
		mode:          domain.TrafficDirectorModeEnforced,
		poolByAccount: map[int64]string{cachedPlus.ID: pool.Key},
	}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)

	allowed, err := svc.trafficDirectorEligibleAccountIDs(
		ctx, &groupID, PlatformOpenAI, "gpt-5",
		OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions, "", false,
		nil, plan, pool,
	)
	require.NoError(t, err)
	require.Empty(t, allowed)
	require.Equal(t, 1, cache.getAccountCalls[cachedPlus.ID])
	require.Equal(t, 1, repo.getByIDCalls[cachedPlus.ID], "a cached Plus candidate must be reclassified by the fresh DB row")
}

func TestOpenAITrafficDirectorHardPreviousAccount_AuditExemptRequirementAppliesAtBothSnapshots(t *testing.T) {
	groupID := int64(42)
	plan := &openAITrafficDirectorRequestPlan{
		key:           openAITrafficDirectorPlanKey{groupID: groupID, platform: PlatformOpenAI},
		mode:          domain.TrafficDirectorModeEnforced,
		poolByAccount: map[int64]string{101: "primary"},
	}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)

	t.Run("cached Pro is rejected before fresh lookup", func(t *testing.T) {
		cachedPro := testOpenAIAccountForTrafficDirector(101, groupID)
		cachedPro.Credentials = map[string]any{"plan_type": "pro"}
		freshPlus := cachedPro
		freshPlus.Credentials = map[string]any{"plan_type": "plus"}
		cache := &trafficDirectorRequirementSnapshotCache{
			freshByID: map[int64]*Account{cachedPro.ID: &freshPlus},
		}
		repo := &trafficDirectorRequirementAccountRepo{
			accountsByID: map[int64]*Account{cachedPro.ID: &freshPlus},
		}
		svc := &OpenAIGatewayService{
			accountRepo:       repo,
			schedulerSnapshot: NewSchedulerSnapshotService(cache, nil, repo, nil, nil),
		}

		selected := svc.trafficDirectorHardPreviousAccount(
			ctx, &groupID, PlatformOpenAI, "gpt-5",
			OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions, "", false,
			plan, &cachedPro,
		)
		require.Nil(t, selected)
		require.Zero(t, cache.getAccountCalls[cachedPro.ID], "an incompatible cached binding must not reach fresh admission")
		require.Zero(t, repo.getByIDCalls[cachedPro.ID])
	})

	t.Run("fresh Pro cannot inherit cached Plus admission", func(t *testing.T) {
		cachedPlus := testOpenAIAccountForTrafficDirector(101, groupID)
		cachedPlus.Credentials = map[string]any{"plan_type": "plus"}
		freshPro := cachedPlus
		freshPro.Credentials = map[string]any{"plan_type": "pro"}
		cache := &trafficDirectorRequirementSnapshotCache{}
		repo := &trafficDirectorRequirementAccountRepo{
			accountsByID: map[int64]*Account{cachedPlus.ID: &freshPro},
		}
		svc := &OpenAIGatewayService{
			accountRepo:       repo,
			schedulerSnapshot: NewSchedulerSnapshotService(cache, nil, repo, nil, nil),
		}

		selected := svc.trafficDirectorHardPreviousAccount(
			ctx, &groupID, PlatformOpenAI, "gpt-5",
			OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions, "", false,
			plan, &cachedPlus,
		)
		require.Nil(t, selected)
		require.Equal(t, 1, cache.getAccountCalls[cachedPlus.ID])
		require.Equal(t, 1, repo.getByIDCalls[cachedPlus.ID], "hard previous must reapply the requirement to the fresh DB row")
	})
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

func TestOpenAITrafficDirectorHardPreviousAcquiresHalfOpenProbeOnce(t *testing.T) {
	groupID := int64(42)
	spec := testOpenAITrafficDirectorSpec()
	spec.Pools = []domain.TrafficDirectorPool{{
		Key:          "primary",
		WeightBPS:    10000,
		AccountIDs:   []int64{101},
		MinAvailable: 1,
	}}
	resolver := &openAITrafficDirectorResolverStub{policy: TrafficDirectorResolvedPolicy{
		Version: TrafficDirectorVersion{GroupID: groupID, Version: 1, Mode: domain.TrafficDirectorModeEnforced, Spec: &spec},
	}}
	health := &openAITrafficDirectorSingleProbeStub{}
	account := testOpenAIAccountForTrafficDirector(101, groupID)
	account.Extra = map[string]any{"responses_websockets_v2_enabled": true}
	svc := &OpenAIGatewayService{
		accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{account}},
		cfg:         newSchedulerTestOpenAIWSV2Config(),
	}
	svc.SetOpenAITrafficDirectorResolver(resolver)
	svc.SetOpenAITrafficDirectorHealthResolver(health)
	require.NoError(t, svc.getOpenAIWSStateStore().BindResponseAccount(
		context.Background(), groupID, "resp_hard_previous_half_open", account.ID, time.Hour,
	))

	ctx := context.WithValue(context.Background(), ctxkey.RequestID, "request-hard-previous-half-open")
	selection, decision, err := svc.SelectAccountWithSchedulerForCapability(
		ctx,
		&groupID,
		"resp_hard_previous_half_open",
		"",
		"gpt-5",
		nil,
		OpenAIUpstreamTransportAny,
		OpenAIEndpointCapabilityChatCompletions,
		false,
		false,
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, account.ID, selection.Account.ID)
	require.Equal(t, openAIAccountScheduleLayerPreviousResponse, decision.Layer)
	require.Equal(t, 1, health.acquireCalls, "hard previous must perform one final health admission")

	noProbeChecks := 0
	for _, check := range health.checks {
		if check.AcquireProbe != nil && !*check.AcquireProbe {
			noProbeChecks++
		}
	}
	require.Equal(t, 1, noProbeChecks, "hard previous should perform one non-owning eligibility check")
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
}

func TestOpenAITrafficDirectorHardPreviousRejectsAccountOutsidePolicy(t *testing.T) {
	groupID := int64(42)
	spec := testOpenAITrafficDirectorSpec()
	spec.Pools = []domain.TrafficDirectorPool{{
		Key:          "primary",
		WeightBPS:    10000,
		AccountIDs:   []int64{101},
		MinAvailable: 1,
	}}
	resolver := &openAITrafficDirectorResolverStub{policy: TrafficDirectorResolvedPolicy{
		Version: TrafficDirectorVersion{GroupID: groupID, Version: 1, Mode: domain.TrafficDirectorModeEnforced, Spec: &spec},
	}}
	allowed := testOpenAIAccountForTrafficDirector(101, groupID)
	removed := testOpenAIAccountForTrafficDirector(303, groupID)
	removed.Extra = map[string]any{"responses_websockets_v2_enabled": true}
	svc := &OpenAIGatewayService{
		accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{allowed, removed}},
		cfg:         newSchedulerTestOpenAIWSV2Config(),
	}
	svc.SetOpenAITrafficDirectorResolver(resolver)
	require.NoError(t, svc.getOpenAIWSStateStore().BindResponseAccount(
		context.Background(), groupID, "resp_removed_from_policy", removed.ID, time.Hour,
	))

	ctx := context.WithValue(context.Background(), ctxkey.RequestID, "request-hard-previous-policy-boundary")
	selection, _, err := svc.SelectAccountWithSchedulerForCapability(
		ctx,
		&groupID,
		"resp_removed_from_policy",
		"",
		"gpt-5",
		nil,
		OpenAIUpstreamTransportAny,
		OpenAIEndpointCapabilityChatCompletions,
		false,
		false,
		false,
	)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
	require.Nil(t, selection,
		"a hard binding removed from policy must fail closed instead of migrating to an allowed account")
}

func TestOpenAITrafficDirectorHardPreviousPreservesPoolEligibilityBoundaries(t *testing.T) {
	groupID := int64(42)
	spec := testOpenAITrafficDirectorSpec()
	plan := &openAITrafficDirectorRequestPlan{
		key:           openAITrafficDirectorPlanKey{groupID: groupID, platform: PlatformOpenAI},
		mode:          domain.TrafficDirectorModeEnforced,
		policy:        TrafficDirectorVersion{GroupID: groupID, Version: 1, Mode: domain.TrafficDirectorModeEnforced, Spec: &spec},
		poolByAccount: map[int64]string{101: "primary"},
	}

	t.Run("privacy requirement", func(t *testing.T) {
		account := testOpenAIAccountForTrafficDirector(101, groupID)
		snapshot := &SchedulerSnapshotService{
			accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{account}},
			groupRepo: trafficDirectorEligibilityGroupRepoStub{group: &Group{
				ID:                groupID,
				Platform:          PlatformOpenAI,
				RequirePrivacySet: true,
			}},
		}
		svc := &OpenAIGatewayService{schedulerSnapshot: snapshot}
		require.Nil(t, svc.trafficDirectorHardPreviousAccount(
			context.Background(),
			&groupID,
			PlatformOpenAI,
			"gpt-5",
			OpenAIUpstreamTransportAny,
			OpenAIEndpointCapabilityChatCompletions,
			"",
			false,
			plan,
			&account,
		))
	})

	t.Run("upstream channel restriction", func(t *testing.T) {
		account := testOpenAIAccountForTrafficDirector(101, groupID)
		channelSvc := &ChannelService{}
		channelSvc.cache.Store(&channelCache{
			channelByGroupID: map[int64]*Channel{
				groupID: {
					ID:                 7,
					Status:             StatusActive,
					RestrictModels:     true,
					BillingModelSource: BillingModelSourceUpstream,
				},
			},
			groupPlatform: map[int64]string{groupID: PlatformOpenAI},
			loadedAt:      time.Now(),
		})
		svc := &OpenAIGatewayService{channelService: channelSvc}
		require.Nil(t, svc.trafficDirectorHardPreviousAccount(
			context.Background(),
			&groupID,
			PlatformOpenAI,
			"gpt-5",
			OpenAIUpstreamTransportAny,
			OpenAIEndpointCapabilityChatCompletions,
			"",
			false,
			plan,
			&account,
		))
	})
}

func TestOpenAITrafficDirectorProfitGateDefersAdmissionUntilLatestAccount(t *testing.T) {
	groupID := int64(42)
	spec := testOpenAITrafficDirectorSpec()
	spec.Pools = []domain.TrafficDirectorPool{{
		Key:          "primary",
		WeightBPS:    10000,
		AccountIDs:   []int64{101},
		MinAvailable: 1,
	}}
	resolver := &openAITrafficDirectorResolverStub{policy: TrafficDirectorResolvedPolicy{
		Version: TrafficDirectorVersion{GroupID: groupID, Version: 1, Mode: domain.TrafficDirectorModeEnforced, Spec: &spec},
	}}
	health := &openAITrafficDirectorHealthDecisionStub{decision: TrafficDirectorHealthDecision{
		State:   TrafficDirectorHealthStateHealthy,
		Allowed: true,
	}}
	account := testOpenAIAccountForTrafficDirector(101, groupID)
	rate := 0.1
	account.RateMultiplier = &rate
	account.Credentials = map[string]any{
		"model_mapping": map[string]any{
			"gpt-5":         "gpt-5",
			"channel-model": "upstream-a",
		},
	}
	svc := &OpenAIGatewayService{accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{account}}}
	svc.SetOpenAITrafficDirectorResolver(resolver)
	svc.SetOpenAITrafficDirectorHealthResolver(health)

	ctx := context.WithValue(context.Background(), ctxkey.RequestID, "request-profit-health-snapshot")
	ctx = svc.WithOpenAITrafficDirectorRequestContext(ctx)
	ctx = context.WithValue(ctx, openAIProfitControlGateCtxKey{}, &openAIProfitControlGate{
		groupID:   groupID,
		platform:  PlatformOpenAI,
		threshold: 1,
	})
	ctx = WithOpenAITrafficDirectorHealthModel(ctx, "channel-model")
	selection, _, err := svc.SelectAccountWithSchedulerForCapability(
		ctx,
		&groupID,
		"",
		"",
		"gpt-5",
		nil,
		OpenAIUpstreamTransportAny,
		OpenAIEndpointCapabilityChatCompletions,
		false,
		false,
		false,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.True(t, selection.ProfitGateActive())

	for _, check := range health.checks {
		require.NotNil(t, check.AcquireProbe, "profit-gated selection must defer final admission")
		require.False(t, *check.AcquireProbe)
	}
	latest := *selection.Account
	latest.Credentials = map[string]any{
		"model_mapping": map[string]any{
			"gpt-5":         "gpt-5",
			"channel-model": "upstream-b",
		},
	}
	selection.Account = &latest
	require.True(t, selection.AdmitTrafficDirector(ContextWithSelectionProfitGate(ctx, selection), selection.ReleaseFunc))
	require.Equal(t, "upstream-b", health.checks[len(health.checks)-1].Model)
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
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
