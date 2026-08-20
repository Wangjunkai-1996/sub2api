package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
)

// OpenAITrafficDirectorResolver resolves the immutable policy named by the
// request's Group head. It is intentionally narrow so the OpenAI scheduler
// does not depend on repository or cache implementations. When authentication
// supplied a Group snapshot, that snapshot is the request's consistency
// boundary; callers must reject a returned policy that does not match it.
type OpenAITrafficDirectorResolver interface {
	ResolveOpenAITrafficDirector(ctx context.Context, groupID int64) (TrafficDirectorResolvedPolicy, error)
}

// OpenAITrafficDirectorHealthResolver is the optional health gate used by an
// enforced policy. Health errors fail open for the account, but are recorded so
// an unavailable health backend cannot silently change pool routing.
type OpenAITrafficDirectorHealthResolver interface {
	AccountHealthy(ctx context.Context, accountID int64, normalizedModel string) (bool, error)
}

// OpenAITrafficDirectorPolicyResolver adapts the group-head repository and the
// immutable policy cache to the narrow scheduler resolver interface.
type OpenAITrafficDirectorPolicyResolver struct {
	headReader interface {
		GetTrafficDirectorHead(context.Context, int64) (*TrafficDirectorHead, error)
	}
	cache TrafficDirectorPolicyCache
}

func NewOpenAITrafficDirectorPolicyResolver(
	headReader interface {
		GetTrafficDirectorHead(context.Context, int64) (*TrafficDirectorHead, error)
	},
	cache TrafficDirectorPolicyCache,
) OpenAITrafficDirectorResolver {
	return &OpenAITrafficDirectorPolicyResolver{headReader: headReader, cache: cache}
}

func (r *OpenAITrafficDirectorPolicyResolver) ResolveOpenAITrafficDirector(
	ctx context.Context,
	groupID int64,
) (TrafficDirectorResolvedPolicy, error) {
	if groupID <= 0 {
		return TrafficDirectorResolvedPolicy{
			Version: TrafficDirectorVersion{GroupID: groupID, Version: TrafficDirectorLegacyVersion, Mode: domain.TrafficDirectorModeLegacy},
			Source:  TrafficDirectorPolicySourceLegacy,
		}, nil
	}
	if r == nil {
		return TrafficDirectorResolvedPolicy{}, ErrTrafficDirectorPolicyUnavailable
	}
	head, ok := trafficDirectorHeadFromContext(ctx, groupID)
	if !ok {
		if r.headReader == nil {
			return TrafficDirectorResolvedPolicy{}, ErrTrafficDirectorPolicyUnavailable
		}
		var err error
		head, err = r.headReader.GetTrafficDirectorHead(ctx, groupID)
		if err != nil {
			return TrafficDirectorResolvedPolicy{}, err
		}
	}
	if head == nil {
		return TrafficDirectorResolvedPolicy{}, ErrTrafficDirectorPolicyUnavailable
	}
	if r.cache == nil {
		if head.Mode == domain.TrafficDirectorModeEnforced {
			return TrafficDirectorResolvedPolicy{}, ErrTrafficDirectorPolicyUnavailable
		}
		return TrafficDirectorResolvedPolicy{
			Version:  syntheticTrafficDirectorLegacyPolicy(groupID),
			Degraded: head.Mode == domain.TrafficDirectorModeShadow,
			Source:   TrafficDirectorPolicySourceLegacy,
		}, nil
	}
	if compiledCache, ok := r.cache.(TrafficDirectorCompiledPolicyCache); ok {
		return compiledCache.GetTrafficDirectorCompiledPolicy(ctx, *head)
	}
	return r.cache.GetTrafficDirectorPolicy(ctx, *head)
}

// trafficDirectorHeadFromContext reads the auth-cache projection installed by
// API-key middleware. That projection is the policy consistency boundary for
// this request: the full immutable policy must match these exact coordinates.
// Legacy requests therefore avoid a new database lookup on the scheduler hot
// path, while an explicitly non-legacy version-zero tuple remains invalid and
// must fail closed instead of being silently rewritten to legacy.
func trafficDirectorHeadFromContext(ctx context.Context, groupID int64) (*TrafficDirectorHead, bool) {
	if ctx == nil || groupID <= 0 {
		return nil, false
	}
	group, ok := ctx.Value(ctxkey.Group).(*Group)
	if !ok || !IsGroupContextValid(group) || group.ID != groupID ||
		!strings.EqualFold(strings.TrimSpace(group.Platform), PlatformOpenAI) {
		return nil, false
	}
	mode := strings.ToLower(strings.TrimSpace(group.TrafficDirectorMode))
	version := group.TrafficDirectorVersion
	if version == TrafficDirectorLegacyVersion && mode == "" {
		mode = domain.TrafficDirectorModeLegacy
	}
	return &TrafficDirectorHead{
		GroupID: groupID,
		Version: version,
		Mode:    mode,
	}, true
}

// validateOpenAITrafficDirectorResolvedHead prevents a resolver implementation
// from weakening an authenticated Group snapshot. In particular, an enforced
// request may only use the exact immutable group/version/mode named by that
// snapshot. Shadow may degrade only to the synthetic legacy policy, matching
// the policy-cache contract.
func validateOpenAITrafficDirectorResolvedHead(
	ctx context.Context,
	groupID int64,
	resolved TrafficDirectorResolvedPolicy,
) error {
	resolvedMode, resolvedModeValid := normalizeTrafficDirectorPolicyMode(resolved.Version.Mode)
	if !resolvedModeValid || resolved.Version.GroupID != groupID ||
		resolved.Version.Version < TrafficDirectorLegacyVersion ||
		(resolved.Version.Version == TrafficDirectorLegacyVersion && resolvedMode != domain.TrafficDirectorModeLegacy) {
		return ErrTrafficDirectorPolicyUnavailable.WithCause(fmt.Errorf(
			"invalid traffic director resolved policy: expected group=%d, got group=%d version=%d mode=%q",
			groupID,
			resolved.Version.GroupID,
			resolved.Version.Version,
			resolved.Version.Mode,
		))
	}
	head, ok := trafficDirectorHeadFromContext(ctx, groupID)
	if !ok {
		return nil
	}
	expectedMode, valid := normalizeTrafficDirectorPolicyMode(head.Mode)
	if !valid || head.Version < TrafficDirectorLegacyVersion ||
		(head.Version == TrafficDirectorLegacyVersion && expectedMode != domain.TrafficDirectorModeLegacy) {
		return ErrTrafficDirectorPolicyUnavailable.WithCause(fmt.Errorf(
			"invalid traffic director request head: group=%d version=%d mode=%q",
			head.GroupID,
			head.Version,
			head.Mode,
		))
	}

	if expectedMode == domain.TrafficDirectorModeShadow && resolved.Degraded &&
		resolved.Version.GroupID == groupID &&
		resolved.Version.Version == TrafficDirectorLegacyVersion &&
		resolvedMode == domain.TrafficDirectorModeLegacy {
		return nil
	}
	if resolved.Version.Version != head.Version || resolvedMode != expectedMode {
		return ErrTrafficDirectorPolicyUnavailable.WithCause(fmt.Errorf(
			"traffic director policy does not match request head: expected group=%d version=%d mode=%s, got group=%d version=%d mode=%q",
			head.GroupID,
			head.Version,
			expectedMode,
			resolved.Version.GroupID,
			resolved.Version.Version,
			resolved.Version.Mode,
		))
	}
	return nil
}

func trafficDirectorLegacyModeFromContext(ctx context.Context, groupID int64) bool {
	if ctx == nil || groupID <= 0 {
		return false
	}
	group, ok := ctx.Value(ctxkey.Group).(*Group)
	if !ok || !IsGroupContextValid(group) || group.ID != groupID {
		return false
	}
	mode := strings.ToLower(strings.TrimSpace(group.TrafficDirectorMode))
	return mode == domain.TrafficDirectorModeLegacy ||
		(group.TrafficDirectorVersion == TrafficDirectorLegacyVersion && mode == "" &&
			strings.EqualFold(strings.TrimSpace(group.Platform), PlatformOpenAI))
}

type openAITrafficDirectorPlanKey struct {
	groupID    int64
	platform   string
	routingKey string
}

type openAITrafficDirectorRequestPlan struct {
	key            openAITrafficDirectorPlanKey
	mode           string
	policy         TrafficDirectorVersion
	evaluation     TrafficDirectorEvaluation
	poolByKey      map[string]domain.TrafficDirectorPool
	poolByAccount  map[int64]string
	currentIndex   int
	shadowLogged   bool
	runtimeMetrics trafficDirectorPlanRuntimeMetrics
}

type openAITrafficDirectorPlanEntry struct {
	plan *openAITrafficDirectorRequestPlan
	err  error
}

// The state is a mutable pointer held by the request context. This makes pool
// advancement monotonic across handler failover iterations while keeping the
// service itself free of request-lifetime state.
type openAITrafficDirectorRequestState struct {
	mu      sync.Mutex
	request string
	plans   map[openAITrafficDirectorPlanKey]openAITrafficDirectorPlanEntry
	// A request can perform more than one health admission for the same
	// account/model (concurrent retries or a session turn). Keep every probe
	// token until its corresponding outcome is reported; a single-value map
	// would silently overwrite the earlier token.
	healthAttempts map[openAITrafficDirectorHealthAttemptKey][]openAITrafficDirectorHealthAttempt
	// Policy resolution and its routing-mode decision are counted once even
	// when one request re-enters selection through account failover.
	runtimePolicyResolutionRecorded bool
}

type openAITrafficDirectorRequestStateContextKey struct{}
type openAITrafficDirectorRetryLoopContextKey struct{}
type openAITrafficDirectorV1BypassContextKey struct{}
type openAITrafficDirectorPoolAdvanceSuppressedContextKey struct{}
type openAITrafficDirectorHealthModelContextKey struct{}

type openAITrafficDirectorHealthModelKind uint8

const (
	openAITrafficDirectorHealthModelAccountMapped openAITrafficDirectorHealthModelKind = iota
	openAITrafficDirectorHealthModelResponses
	openAITrafficDirectorHealthModelMessages
	openAITrafficDirectorHealthModelCountTokens
	openAITrafficDirectorHealthModelImages
)

type openAITrafficDirectorHealthModelContext struct {
	model              string
	defaultMappedModel string
	requireCompact     bool
	kind               openAITrafficDirectorHealthModelKind
}

func withOpenAITrafficDirectorHealthModel(
	ctx context.Context,
	modelContext openAITrafficDirectorHealthModelContext,
) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	modelContext.model = strings.TrimSpace(modelContext.model)
	modelContext.defaultMappedModel = strings.TrimSpace(modelContext.defaultMappedModel)
	if modelContext.model == "" {
		return ctx
	}
	return context.WithValue(ctx, openAITrafficDirectorHealthModelContextKey{}, modelContext)
}

// WithOpenAITrafficDirectorHealthModel records the channel-mapped model for
// endpoints that always apply ordinary account mapping before forwarding.
func WithOpenAITrafficDirectorHealthModel(ctx context.Context, model string) context.Context {
	return withOpenAITrafficDirectorHealthModel(ctx, openAITrafficDirectorHealthModelContext{
		model: model,
		kind:  openAITrafficDirectorHealthModelAccountMapped,
	})
}

// WithOpenAIResponsesTrafficDirectorHealthModel preserves the raw/passthrough
// and legacy compact branches used specifically by Forward.
func WithOpenAIResponsesTrafficDirectorHealthModel(ctx context.Context, model string, requireCompact bool) context.Context {
	return withOpenAITrafficDirectorHealthModel(ctx, openAITrafficDirectorHealthModelContext{
		model:          model,
		requireCompact: requireCompact,
		kind:           openAITrafficDirectorHealthModelResponses,
	})
}

// WithOpenAIMessagesTrafficDirectorHealthModel preserves the Messages dispatch
// fallback without reversing its account-mapping-first precedence.
func WithOpenAIMessagesTrafficDirectorHealthModel(ctx context.Context, model, defaultMappedModel string) context.Context {
	return withOpenAITrafficDirectorHealthModel(ctx, openAITrafficDirectorHealthModelContext{
		model:              model,
		defaultMappedModel: defaultMappedModel,
		kind:               openAITrafficDirectorHealthModelMessages,
	})
}

func WithOpenAICountTokensTrafficDirectorHealthModel(ctx context.Context, model, defaultMappedModel string) context.Context {
	return withOpenAITrafficDirectorHealthModel(ctx, openAITrafficDirectorHealthModelContext{
		model:              model,
		defaultMappedModel: defaultMappedModel,
		kind:               openAITrafficDirectorHealthModelCountTokens,
	})
}

func WithOpenAIImagesTrafficDirectorHealthModel(ctx context.Context, model string) context.Context {
	return withOpenAITrafficDirectorHealthModel(ctx, openAITrafficDirectorHealthModelContext{
		model: model,
		kind:  openAITrafficDirectorHealthModelImages,
	})
}

func openAITrafficDirectorHealthModelContextForRequest(
	ctx context.Context,
	requestedModel string,
	requireCompact bool,
) openAITrafficDirectorHealthModelContext {
	if ctx != nil {
		if healthModel, ok := ctx.Value(openAITrafficDirectorHealthModelContextKey{}).(openAITrafficDirectorHealthModelContext); ok {
			if model := strings.TrimSpace(healthModel.model); model != "" {
				healthModel.model = model
				return healthModel
			}
		}
	}
	kind := openAITrafficDirectorHealthModelAccountMapped
	if requireCompact {
		kind = openAITrafficDirectorHealthModelResponses
	}
	return openAITrafficDirectorHealthModelContext{
		model:          strings.TrimSpace(requestedModel),
		requireCompact: requireCompact,
		kind:           kind,
	}
}

func resolveOpenAITrafficDirectorHealthModel(
	account *Account,
	healthModel openAITrafficDirectorHealthModelContext,
) string {
	model := strings.TrimSpace(healthModel.model)
	if model == "" {
		return ""
	}

	switch healthModel.kind {
	case openAITrafficDirectorHealthModelResponses:
		if account != nil && account.IsAnthropicProtocol() {
			billingModel := resolveOpenAIForwardModel(account, model, "")
			return strings.TrimSpace(normalizeOpenAIModelForUpstream(account, billingModel))
		}
		upstreamModel := resolveOpenAIAccountUpstreamModelForRequest(account, model, healthModel.requireCompact)
		if healthModel.requireCompact || account == nil ||
			shouldForwardOpenAIResponsesViaRawChatCompletions(account) || account.IsOpenAIPassthroughEnabled() {
			return strings.TrimSpace(upstreamModel)
		}
		if isOpenAIImageGenerationModel(upstreamModel) {
			return openAIImagesResponsesMainModel
		}
		return strings.TrimSpace(upstreamModel)
	case openAITrafficDirectorHealthModelMessages:
		if account == nil || !account.IsAnthropicProtocol() {
			model = NormalizeOpenAICompatRequestedModel(model)
		}
		billingModel := resolveOpenAIForwardModel(account, model, healthModel.defaultMappedModel)
		return strings.TrimSpace(normalizeOpenAIModelForUpstream(account, billingModel))
	case openAITrafficDirectorHealthModelCountTokens:
		model = NormalizeOpenAICompatRequestedModel(model)
		billingModel := resolveOpenAIForwardModel(account, model, healthModel.defaultMappedModel)
		return strings.TrimSpace(normalizeOpenAIModelForUpstream(account, billingModel))
	case openAITrafficDirectorHealthModelImages:
		if account == nil || account.Type == AccountTypeOAuth {
			return model
		}
		if account.Type == AccountTypeAPIKey {
			return strings.TrimSpace(account.GetMappedModel(model))
		}
		return model
	default:
		billingModel := resolveOpenAIForwardModel(account, model, "")
		return strings.TrimSpace(normalizeOpenAIModelForUpstream(account, billingModel))
	}
}

func openAITrafficDirectorHealthModelForRequest(
	ctx context.Context,
	account *Account,
	requestedModel string,
	requireCompact bool,
) string {
	return resolveOpenAITrafficDirectorHealthModel(
		account,
		openAITrafficDirectorHealthModelContextForRequest(ctx, requestedModel, requireCompact),
	)
}

// withOpenAITrafficDirectorV1Bypass keeps an endpoint on the existing OpenAI
// scheduler while explicitly excluding it from Traffic Director V1 policy
// resolution, pool restriction, and health admission.
func withOpenAITrafficDirectorV1Bypass(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, openAITrafficDirectorV1BypassContextKey{}, true)
}

func openAITrafficDirectorV1Bypassed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	bypassed, _ := ctx.Value(openAITrafficDirectorV1BypassContextKey{}).(bool)
	return bypassed
}

func withOpenAITrafficDirectorPoolAdvanceSuppressed(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, openAITrafficDirectorPoolAdvanceSuppressedContextKey{}, true)
}

func openAITrafficDirectorPoolAdvanceSuppressed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	suppressed, _ := ctx.Value(openAITrafficDirectorPoolAdvanceSuppressedContextKey{}).(bool)
	return suppressed
}

// WithOpenAITrafficDirectorRequestContext installs request-local policy state.
// Handlers should call this once before entering an account failover loop.
func (s *OpenAIGatewayService) WithOpenAITrafficDirectorRequestContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if openAITrafficDirectorV1Bypassed(ctx) {
		return ctx
	}
	if group, ok := ctx.Value(ctxkey.Group).(*Group); ok && group != nil &&
		trafficDirectorLegacyModeFromContext(ctx, group.ID) {
		return ctx
	}
	if existing, ok := ctx.Value(openAITrafficDirectorRequestStateContextKey{}).(*openAITrafficDirectorRequestState); ok && existing != nil {
		return ctx
	}
	return context.WithValue(ctx, openAITrafficDirectorRequestStateContextKey{}, &openAITrafficDirectorRequestState{
		plans: make(map[openAITrafficDirectorPlanKey]openAITrafficDirectorPlanEntry),
	})
}

// WithOpenAITrafficDirectorRetryLoopContext marks a handler context whose
// caller can exclude a failed wait-plan account and re-enter selection.
// Single-shot endpoints keep their existing error response semantics.
func (s *OpenAIGatewayService) WithOpenAITrafficDirectorRetryLoopContext(ctx context.Context) context.Context {
	ctx = s.WithOpenAITrafficDirectorRequestContext(ctx)
	if openAITrafficDirectorRequestStateFromContext(ctx) == nil {
		if !openAITrafficDirectorV1Bypassed(ctx) {
			if group, ok := ctx.Value(ctxkey.Group).(*Group); ok && group != nil &&
				IsGroupContextValid(group) && strings.EqualFold(strings.TrimSpace(group.Platform), PlatformOpenAI) &&
				trafficDirectorLegacyModeFromContext(ctx, group.ID) {
				recordTrafficDirectorLegacyRoutingDecision()
			}
		}
		return ctx
	}
	return context.WithValue(ctx, openAITrafficDirectorRetryLoopContextKey{}, true)
}

func openAITrafficDirectorRetryLoopEnabled(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	enabled, _ := ctx.Value(openAITrafficDirectorRetryLoopContextKey{}).(bool)
	return enabled
}

func openAITrafficDirectorRequestStateFromContext(ctx context.Context) *openAITrafficDirectorRequestState {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(openAITrafficDirectorRequestStateContextKey{}).(*openAITrafficDirectorRequestState)
	return state
}

func (s *OpenAIGatewayService) trafficDirectorResolver() OpenAITrafficDirectorResolver {
	if s == nil {
		return nil
	}
	s.openaiTrafficDirectorMu.RLock()
	resolver := s.openaiTrafficDirectorResolver
	s.openaiTrafficDirectorMu.RUnlock()
	return resolver
}

func (s *OpenAIGatewayService) prepareOpenAITrafficDirectorSelectionContext(
	ctx context.Context,
	groupID *int64,
	platform string,
) context.Context {
	if openAITrafficDirectorV1Bypassed(ctx) || groupID == nil || *groupID <= 0 || s.trafficDirectorResolver() == nil ||
		trafficDirectorLegacyModeFromContext(ctx, *groupID) ||
		!trafficDirectorOpenAIPlatformAllowed(ctx, *groupID, platform) {
		return ctx
	}
	return s.WithOpenAITrafficDirectorRequestContext(ctx)
}

func (s *OpenAIGatewayService) trafficDirectorHealthResolver() OpenAITrafficDirectorHealthResolver {
	if s == nil {
		return nil
	}
	s.openaiTrafficDirectorMu.RLock()
	resolver := s.openaiTrafficDirectorHealthResolver
	s.openaiTrafficDirectorMu.RUnlock()
	return resolver
}

func trafficDirectorRoutingKey(ctx context.Context, sessionHash string, state *openAITrafficDirectorRequestState) string {
	if key := strings.TrimSpace(sessionHash); key != "" {
		return key
	}
	if ctx != nil {
		if key, _ := ctx.Value(ctxkey.RequestID).(string); strings.TrimSpace(key) != "" {
			return strings.TrimSpace(key)
		}
	}
	if state != nil {
		state.mu.Lock()
		if strings.TrimSpace(state.request) == "" {
			state.request = fmt.Sprintf("local-%p", state)
		}
		key := state.request
		state.mu.Unlock()
		return key
	}
	return "local-openai-request"
}

func cloneTrafficDirectorAccountSet(in map[int64]struct{}) map[int64]struct{} {
	if len(in) == 0 {
		return nil
	}
	out := make(map[int64]struct{}, len(in))
	for id := range in {
		out[id] = struct{}{}
	}
	return out
}

// trafficDirectorAccountBelongsToGroup is stricter than the legacy simple-mode
// scheduler's group compatibility helper. A published policy is an explicit
// account allow-list, so a hard previous_response binding must never bypass the
// current Group relationship even when the legacy scheduler itself runs in
// simple mode.
func trafficDirectorAccountBelongsToGroup(account *Account, groupID *int64) bool {
	if account == nil || groupID == nil || *groupID <= 0 {
		return false
	}
	for _, id := range account.GroupIDs {
		if id == *groupID {
			return true
		}
	}
	for _, relation := range account.AccountGroups {
		if relation.GroupID == *groupID {
			return true
		}
	}
	return false
}

func (s *OpenAIGatewayService) resolveOpenAITrafficDirectorPlan(
	ctx context.Context,
	groupID *int64,
	platform string,
	sessionHash string,
) (*openAITrafficDirectorRequestPlan, error) {
	if openAITrafficDirectorV1Bypassed(ctx) || groupID == nil || *groupID <= 0 || trafficDirectorLegacyModeFromContext(ctx, *groupID) ||
		!trafficDirectorOpenAIPlatformAllowed(ctx, *groupID, platform) {
		return nil, nil
	}
	resolver := s.trafficDirectorResolver()
	if resolver == nil {
		state := openAITrafficDirectorRequestStateFromContext(ctx)
		recordTrafficDirectorPolicyUnavailable(state)
		if head, ok := trafficDirectorHeadFromContext(ctx, *groupID); ok {
			mode, valid := normalizeTrafficDirectorPolicyMode(head.Mode)
			if !valid || mode == domain.TrafficDirectorModeEnforced ||
				(head.Version > TrafficDirectorLegacyVersion && mode != domain.TrafficDirectorModeLegacy) {
				return nil, ErrTrafficDirectorPolicyUnavailable
			}
		}
		return nil, nil
	}
	state := openAITrafficDirectorRequestStateFromContext(ctx)
	routingKey := trafficDirectorRoutingKey(ctx, sessionHash, state)
	key := openAITrafficDirectorPlanKey{groupID: *groupID, platform: PlatformOpenAI, routingKey: routingKey}
	if state != nil {
		state.mu.Lock()
		if entry, ok := state.plans[key]; ok {
			state.mu.Unlock()
			return entry.plan, entry.err
		}
		// A few OpenAI endpoints materialize a pool-mode session hash only
		// after the first account is selected. Keep the request's original
		// evaluated plan in that case; changing the routing-key representation
		// must never reset a request that has already advanced its pool chain.
		for existingKey, entry := range state.plans {
			if existingKey.groupID == key.groupID && existingKey.platform == key.platform {
				state.mu.Unlock()
				return entry.plan, entry.err
			}
		}
		state.mu.Unlock()
	}

	resolved, err := resolver.ResolveOpenAITrafficDirector(ctx, *groupID)
	if err != nil {
		if state != nil {
			state.mu.Lock()
			state.plans[key] = openAITrafficDirectorPlanEntry{err: err}
			state.mu.Unlock()
		}
		recordTrafficDirectorPolicyUnavailable(state)
		return nil, err
	}
	if err = validateOpenAITrafficDirectorResolvedHead(ctx, *groupID, resolved); err != nil {
		if state != nil {
			state.mu.Lock()
			state.plans[key] = openAITrafficDirectorPlanEntry{err: err}
			state.mu.Unlock()
		}
		recordTrafficDirectorPolicyUnavailable(state)
		return nil, err
	}
	mode := strings.TrimSpace(resolved.Version.Mode)
	if mode == "" {
		mode = domain.TrafficDirectorModeLegacy
	}
	plan := &openAITrafficDirectorRequestPlan{
		key:           key,
		mode:          mode,
		policy:        resolved.Version,
		poolByKey:     make(map[string]domain.TrafficDirectorPool),
		poolByAccount: make(map[int64]string),
	}
	if mode == domain.TrafficDirectorModeShadow || mode == domain.TrafficDirectorModeEnforced {
		if resolved.Version.Spec == nil {
			err = ErrTrafficDirectorPolicyUnavailable
		} else if resolved.compiled != nil {
			plan.evaluation, err = resolved.compiled.evaluate(*groupID, routingKey)
			if err == nil {
				for key, pool := range resolved.compiled.poolsByKey {
					plan.poolByKey[key] = pool
					for _, accountID := range pool.AccountIDs {
						plan.poolByAccount[accountID] = key
					}
				}
			}
		} else {
			plan.evaluation, err = EvaluateTrafficDirector(*resolved.Version.Spec, *groupID, routingKey)
			if err == nil {
				for _, pool := range resolved.Version.Spec.Pools {
					plan.poolByKey[pool.Key] = pool
					for _, accountID := range pool.AccountIDs {
						plan.poolByAccount[accountID] = pool.Key
					}
				}
			}
		}
	}
	if err != nil {
		if state != nil {
			state.mu.Lock()
			state.plans[key] = openAITrafficDirectorPlanEntry{err: err}
			state.mu.Unlock()
		}
		recordTrafficDirectorPolicyUnavailable(state)
		return nil, err
	}
	if state != nil {
		state.mu.Lock()
		state.plans[key] = openAITrafficDirectorPlanEntry{plan: plan}
		state.mu.Unlock()
	}
	recordTrafficDirectorResolvedPolicy(state, mode, resolved)
	return plan, nil
}

// trafficDirectorOpenAIPlatformAllowed is deliberately stricter than the
// general OpenAI-compatible scheduler normalization. Traffic Director V1 is
// enabled only for an explicitly resolved OpenAI target; treating an unknown
// or Anthropic/composite platform as OpenAI here would silently change another
// platform's routing behavior.
func trafficDirectorOpenAIPlatformAllowed(ctx context.Context, groupID int64, requestedPlatform string) bool {
	if groupID <= 0 {
		return false
	}
	if target, ok := ResolvedTargetPlatformFromContext(ctx); ok {
		return strings.EqualFold(strings.TrimSpace(target), PlatformOpenAI)
	}
	if ctx != nil {
		if group, ok := ctx.Value(ctxkey.Group).(*Group); ok && group != nil && group.ID == groupID {
			if !IsGroupContextValid(group) {
				return false
			}
			return strings.EqualFold(strings.TrimSpace(group.Platform), PlatformOpenAI)
		}
	}
	requestedPlatform = strings.TrimSpace(requestedPlatform)
	// An empty platform is not authoritative. The ordinary scheduler may treat
	// it as OpenAI for compatibility, but Traffic Director must stay disabled
	// until a trusted Group/target snapshot or an explicit OpenAI argument exists.
	return requestedPlatform != "" && strings.EqualFold(requestedPlatform, PlatformOpenAI)
}

func (p *openAITrafficDirectorRequestPlan) currentPool() (domain.TrafficDirectorPool, bool) {
	if p == nil || p.mode != domain.TrafficDirectorModeEnforced || p.currentIndex < 0 || p.currentIndex >= len(p.evaluation.FallbackPoolKeys)+1 {
		return domain.TrafficDirectorPool{}, false
	}
	key := p.evaluation.HomePoolKey
	if p.currentIndex > 0 {
		key = p.evaluation.FallbackPoolKeys[p.currentIndex-1]
	}
	pool, ok := p.poolByKey[key]
	return pool, ok
}

func (p *openAITrafficDirectorRequestPlan) advancePool() bool {
	if p == nil {
		return false
	}
	if p.currentIndex >= len(p.evaluation.FallbackPoolKeys) {
		return false
	}
	p.currentIndex++
	recordTrafficDirectorFallbackTransition()
	return true
}

func (s *OpenAIGatewayService) trafficDirectorAccountHealthy(
	ctx context.Context,
	policy *openAITrafficDirectorRequestPlan,
	accountID int64,
	normalizedModel string,
) bool {
	return s.trafficDirectorAccountHealthDecision(ctx, policy, accountID, normalizedModel, nil)
}

func (s *OpenAIGatewayService) trafficDirectorAccountHealthDecision(
	ctx context.Context,
	policy *openAITrafficDirectorRequestPlan,
	accountID int64,
	normalizedModel string,
	acquireProbe *bool,
) bool {
	if policy == nil || policy.policy.Spec == nil {
		return true
	}
	mode := policy.policy.Spec.HealthMode
	if mode != domain.TrafficDirectorHealthModeEnforce {
		return true
	}
	decision, err := s.checkOpenAITrafficDirectorHealthCanonical(ctx, accountID, normalizedModel, mode, acquireProbe)
	if err != nil {
		slog.Warn("openai.traffic_director.health_unavailable",
			"group_id", policy.key.groupID,
			"account_id", accountID,
			"model", normalizedModel,
			"error", err,
		)
		return true
	}
	return decision.Allowed
}

func (s *OpenAIGatewayService) trafficDirectorAccountEligibleForPool(
	ctx context.Context,
	policy *openAITrafficDirectorRequestPlan,
	accountID int64,
	normalizedModel string,
) bool {
	if policy == nil || policy.policy.Spec == nil || policy.policy.Spec.HealthMode != domain.TrafficDirectorHealthModeEnforce {
		return true
	}
	noProbe := false
	decision, err := s.checkOpenAITrafficDirectorHealthCanonical(
		ctx,
		accountID,
		normalizedModel,
		policy.policy.Spec.HealthMode,
		&noProbe,
	)
	if err != nil {
		// A health backend failure is fail-open, but remains within this pool.
		return true
	}
	// A half-open account is a candidate only when no other process owns the
	// global probe lease. The final admission step will acquire that probe.
	return decision.Allowed ||
		(decision.State == TrafficDirectorHealthStateHalfOpen && decision.ProbeUntil.IsZero())
}

func (s *OpenAIGatewayService) trafficDirectorEligibleAccountIDs(
	ctx context.Context,
	groupID *int64,
	platform string,
	requestedModel string,
	requiredTransport OpenAIUpstreamTransport,
	requiredCapability OpenAIEndpointCapability,
	requiredImageCapability OpenAIImagesCapability,
	requireCompact bool,
	excludedIDs map[int64]struct{},
	policy *openAITrafficDirectorRequestPlan,
	pool domain.TrafficDirectorPool,
) (map[int64]struct{}, error) {
	accounts, err := s.listSchedulableAccounts(ctx, groupID, platform)
	if err != nil {
		return nil, err
	}
	configured := make(map[int64]struct{}, len(pool.AccountIDs))
	for _, id := range pool.AccountIDs {
		configured[id] = struct{}{}
	}
	allowed := make(map[int64]struct{})
	needsUpstreamCheck := s.needsUpstreamChannelRestrictionCheck(ctx, groupID)
	var schedGroup *Group
	if groupID != nil && s.schedulerSnapshot != nil {
		schedGroup, _ = s.schedulerSnapshot.GetGroupByID(ctx, *groupID)
	}
	for i := range accounts {
		account := &accounts[i]
		if _, ok := configured[account.ID]; !ok {
			continue
		}
		if _, excluded := excludedIDs[account.ID]; excluded {
			continue
		}
		if !isOpenAICompatibleAccountEligibleForRequestBeforeProfit(ctx, account, platform, requestedModel, requireCompact, requiredCapability) {
			continue
		}
		if schedGroup != nil && schedGroup.RequirePrivacySet && !account.IsPrivacySet() {
			continue
		}
		if !accountSupportsOpenAICapabilities(account, requiredCapability, requiredImageCapability) ||
			!s.isOpenAIAccountTransportCompatible(account, requiredTransport) {
			continue
		}
		fresh := s.resolveFreshSchedulableOpenAIAccountBeforeProfit(ctx, account, platform, requestedModel, requireCompact, requiredCapability)
		if fresh == nil || !trafficDirectorAccountBelongsToGroup(fresh, groupID) {
			continue
		}
		if needsUpstreamCheck && groupID != nil && s.isUpstreamModelRestrictedByChannel(ctx, *groupID, fresh, requestedModel, requireCompact) {
			continue
		}
		if vetoed, _ := openAIProfitControlVetoReason(ctx, fresh); vetoed {
			continue
		}
		healthModel := openAITrafficDirectorHealthModelForRequest(ctx, fresh, requestedModel, requireCompact)
		if !s.trafficDirectorAccountEligibleForPool(ctx, policy, fresh.ID, healthModel) {
			continue
		}
		allowed[fresh.ID] = struct{}{}
	}
	return allowed, nil
}

func (s *OpenAIGatewayService) trafficDirectorHardPreviousAccount(
	ctx context.Context,
	groupID *int64,
	platform string,
	requestedModel string,
	requiredTransport OpenAIUpstreamTransport,
	requiredCapability OpenAIEndpointCapability,
	requiredImageCapability OpenAIImagesCapability,
	requireCompact bool,
	policy *openAITrafficDirectorRequestPlan,
	account *Account,
) *Account {
	if policy == nil || account == nil {
		return nil
	}
	if _, configured := policy.poolByAccount[account.ID]; !configured {
		return nil
	}
	if !isOpenAICompatibleAccountEligibleForRequestBeforeProfit(ctx, account, platform, requestedModel, requireCompact, requiredCapability) {
		return nil
	}
	if !accountSupportsOpenAICapabilities(account, requiredCapability, requiredImageCapability) ||
		!s.isOpenAIAccountTransportCompatible(account, requiredTransport) {
		return nil
	}
	requirePrivacy := false
	if groupID != nil && s.schedulerSnapshot != nil {
		group, _ := s.schedulerSnapshot.GetGroupByID(ctx, *groupID)
		requirePrivacy = group != nil && group.RequirePrivacySet
	}
	fresh := s.resolveFreshSchedulableOpenAIAccountBeforeProfit(ctx, account, platform, requestedModel, requireCompact, requiredCapability)
	if fresh == nil || !trafficDirectorAccountBelongsToGroup(fresh, groupID) {
		return nil
	}
	if requirePrivacy && !fresh.IsPrivacySet() {
		return nil
	}
	if s.needsUpstreamChannelRestrictionCheck(ctx, groupID) && groupID != nil &&
		s.isUpstreamModelRestrictedByChannel(ctx, *groupID, fresh, requestedModel, requireCompact) {
		return nil
	}
	if vetoed, _ := openAIProfitControlVetoReason(ctx, fresh); vetoed {
		return nil
	}
	healthModel := openAITrafficDirectorHealthModelForRequest(ctx, fresh, requestedModel, requireCompact)
	if !s.trafficDirectorAccountEligibleForPool(ctx, policy, fresh.ID, healthModel) {
		return nil
	}
	return fresh
}

// openAITrafficDirectorAllowedIDsKey is used by both legacy and advanced
// schedulers. A nil set means no Traffic Director restriction is active.
type openAITrafficDirectorAllowedIDsKey struct{}

func withOpenAITrafficDirectorAllowedIDs(ctx context.Context, ids map[int64]struct{}) context.Context {
	return context.WithValue(ctx, openAITrafficDirectorAllowedIDsKey{}, cloneTrafficDirectorAccountSet(ids))
}

func openAITrafficDirectorAllowsAccount(ctx context.Context, accountID int64) bool {
	if ctx == nil {
		return true
	}
	ids, ok := ctx.Value(openAITrafficDirectorAllowedIDsKey{}).(map[int64]struct{})
	if !ok || ids == nil {
		return true
	}
	_, allowed := ids[accountID]
	return allowed
}

func (s *OpenAIGatewayService) trafficDirectorSelectPool(
	ctx context.Context,
	groupID *int64,
	platform string,
	sessionHash string,
	requestedModel string,
	requiredTransport OpenAIUpstreamTransport,
	requiredCapability OpenAIEndpointCapability,
	requiredImageCapability OpenAIImagesCapability,
	requireCompact bool,
	excludedIDs map[int64]struct{},
) (*openAITrafficDirectorRequestPlan, map[int64]struct{}, error) {
	plan, err := s.resolveOpenAITrafficDirectorPlan(ctx, groupID, platform, sessionHash)
	if err != nil || plan == nil || plan.mode != domain.TrafficDirectorModeEnforced {
		return plan, nil, err
	}
	state := openAITrafficDirectorRequestStateFromContext(ctx)
	if state == nil {
		// Calls outside an HTTP handler still get monotonic behavior for the
		// duration of this selection; handlers install a persistent state.
		ctx = s.WithOpenAITrafficDirectorRequestContext(ctx)
		state = openAITrafficDirectorRequestStateFromContext(ctx)
		plan, err = s.resolveOpenAITrafficDirectorPlan(ctx, groupID, platform, sessionHash)
		if err != nil {
			return plan, nil, err
		}
	}
	for {
		// Do not hold the request-state mutex while querying accounts or the
		// distributed health store. Health checks record half-open probe tokens
		// in the same state and would otherwise self-deadlock. The index is
		// rechecked under the mutex before every advancement so concurrent
		// failover callers still observe a monotonic chain.
		state.mu.Lock()
		pool, ok := plan.currentPool()
		state.mu.Unlock()
		if !ok {
			recordTrafficDirectorNoAvailablePool(plan)
			return plan, nil, ErrTrafficDirectorNoAvailablePool
		}
		allowed, eligibilityErr := s.trafficDirectorEligibleAccountIDs(ctx, groupID, platform, requestedModel, requiredTransport, requiredCapability, requiredImageCapability, requireCompact, excludedIDs, plan, pool)
		if eligibilityErr != nil {
			return plan, nil, eligibilityErr
		}
		if len(allowed) >= pool.MinAvailable {
			return plan, allowed, nil
		}
		recordTrafficDirectorPoolExhausted(plan, pool.Key)
		if openAITrafficDirectorPoolAdvanceSuppressed(ctx) {
			return plan, nil, ErrNoAvailableAccounts
		}
		state.mu.Lock()
		current, currentOK := plan.currentPool()
		if !currentOK {
			state.mu.Unlock()
			recordTrafficDirectorNoAvailablePool(plan)
			return plan, nil, ErrTrafficDirectorNoAvailablePool
		}
		if current.Key != pool.Key {
			// Another concurrent selector already advanced the request. Re-run
			// eligibility against that newer pool instead of moving twice.
			state.mu.Unlock()
			continue
		}
		advanced := plan.advancePool()
		state.mu.Unlock()
		if !advanced {
			recordTrafficDirectorNoAvailablePool(plan)
			return plan, nil, ErrTrafficDirectorNoAvailablePool
		}
	}
}

func (s *OpenAIGatewayService) advanceOpenAITrafficDirectorPool(
	ctx context.Context,
	groupID *int64,
	platform string,
	sessionHash string,
) (enforced bool, advanced bool, err error) {
	plan, err := s.resolveOpenAITrafficDirectorPlan(ctx, groupID, platform, sessionHash)
	if err != nil || plan == nil || plan.mode != domain.TrafficDirectorModeEnforced {
		return false, false, err
	}
	state := openAITrafficDirectorRequestStateFromContext(ctx)
	if state == nil {
		pool, _ := plan.currentPool()
		recordTrafficDirectorPoolExhausted(plan, pool.Key)
		advanced = plan.advancePool()
		if !advanced {
			recordTrafficDirectorNoAvailablePool(plan)
		}
		return true, advanced, nil
	}
	state.mu.Lock()
	pool, _ := plan.currentPool()
	recordTrafficDirectorPoolExhausted(plan, pool.Key)
	advanced = plan.advancePool()
	state.mu.Unlock()
	if !advanced {
		recordTrafficDirectorNoAvailablePool(plan)
	}
	return true, advanced, nil
}

// trafficDirectorHealthAdmission performs the health check for one actual
// account attempt. The returned cleanup consumes any locally held probe token
// when the attempt is abandoned before an upstream outcome is reported.
func (s *OpenAIGatewayService) trafficDirectorHealthAdmission(
	ctx context.Context,
	account *Account,
	requestedModel string,
	requireCompact bool,
) (bool, func()) {
	if account == nil || !s.OpenAITrafficDirectorEnforcedInContext(ctx) {
		return true, nil
	}
	mode := peekOpenAITrafficDirectorHealthMode(ctx, account.ID)
	if mode != domain.TrafficDirectorHealthModeEnforce {
		return true, nil
	}
	healthModel := openAITrafficDirectorHealthModelForRequest(ctx, account, requestedModel, requireCompact)
	decision, err := s.checkOpenAITrafficDirectorHealthCanonical(ctx, account.ID, healthModel, mode, nil)
	if err != nil {
		// Check is fail-open by contract. Keep the account inside the already
		// evaluated pool and leave the diagnostic to the health helper.
		return true, nil
	}
	if !decision.Allowed {
		return false, nil
	}
	if !decision.HalfOpenProbe || strings.TrimSpace(decision.ProbeToken) == "" {
		return true, nil
	}
	// The request-state attempt is consumed by the normal outcome reporter.
	// If no upstream attempt is made, consuming it here still stops the renewal
	// goroutine and prevents the token from being attached to a later retry.
	return true, func() {
		_, _ = s.abandonOpenAITrafficDirectorHealthAttempt(ctx, account.ID, decision.Model)
	}
}

// admitOpenAITrafficDirectorAccount is kept as a narrow compatibility helper
// for service-side callers that already hold an acquired slot.
func (s *OpenAIGatewayService) admitOpenAITrafficDirectorAccount(
	ctx context.Context,
	account *Account,
	requestedModel string,
) bool {
	allowed, _ := s.trafficDirectorHealthAdmission(ctx, account, requestedModel, false)
	return allowed
}

func (s *OpenAIGatewayService) trafficDirectorWouldBePool(ctx context.Context, groupID *int64, platform, sessionHash string) string {
	plan, err := s.resolveOpenAITrafficDirectorPlan(ctx, groupID, platform, sessionHash)
	if err != nil || plan == nil || plan.mode != domain.TrafficDirectorModeShadow {
		return ""
	}
	return plan.evaluation.HomePoolKey
}

func (s *OpenAIGatewayService) logTrafficDirectorShadowSelectionOnce(
	ctx context.Context,
	groupID *int64,
	requestedModel string,
	selectedAccountID int64,
) {
	if groupID == nil || *groupID <= 0 || selectedAccountID <= 0 {
		return
	}
	state := openAITrafficDirectorRequestStateFromContext(ctx)
	if state == nil {
		return
	}
	state.mu.Lock()
	var plan *openAITrafficDirectorRequestPlan
	for _, entry := range state.plans {
		if entry.err == nil && entry.plan != nil && entry.plan.key.groupID == *groupID &&
			entry.plan.mode == domain.TrafficDirectorModeShadow {
			plan = entry.plan
			break
		}
	}
	if plan == nil || plan.shadowLogged {
		state.mu.Unlock()
		return
	}
	plan.shadowLogged = true
	homePool := plan.evaluation.HomePoolKey
	selectedPool := plan.poolByAccount[selectedAccountID]
	state.mu.Unlock()

	slog.Info("openai.traffic_director.shadow_decision",
		"group_id", plan.key.groupID,
		"model", requestedModel,
		"would_be_pool", homePool,
		"fallback_pool_chain", plan.evaluation.FallbackPoolKeys,
		"legacy_selected_account_id", selectedAccountID,
		"legacy_selected_account_pool", selectedPool,
		"legacy_selected_account_configured", selectedPool != "",
		"legacy_selection_matches_pool", selectedPool == homePool,
	)
}

func (s *OpenAIGatewayService) logTrafficDirectorPreviousOverride(
	ctx context.Context,
	plan *openAITrafficDirectorRequestPlan,
	accountID int64,
	normalizedModel string,
) {
	if plan == nil || plan.mode != domain.TrafficDirectorModeEnforced {
		return
	}
	pool := plan.poolByAccount[accountID]
	current := domain.TrafficDirectorPool{}
	if state := openAITrafficDirectorRequestStateFromContext(ctx); state != nil {
		state.mu.Lock()
		current, _ = plan.currentPool()
		state.mu.Unlock()
	} else {
		current, _ = plan.currentPool()
	}
	if pool == current.Key {
		return
	}
	slog.Info("openai.traffic_director.previous_response_override",
		"group_id", plan.key.groupID,
		"account_id", accountID,
		"model", normalizedModel,
		"home_pool", plan.evaluation.HomePoolKey,
		"current_pool", current.Key,
		"account_pool", pool,
		"reason", "hard_previous_response",
	)
}

func (s *OpenAIGatewayService) OpenAITrafficDirectorEnforcedInContext(ctx context.Context) bool {
	state := openAITrafficDirectorRequestStateFromContext(ctx)
	if state == nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, entry := range state.plans {
		if entry.err == nil && entry.plan != nil && entry.plan.mode == domain.TrafficDirectorModeEnforced {
			return true
		}
	}
	return false
}

func (s *OpenAIGatewayService) OpenAITrafficDirectorRetryEnabledInContext(ctx context.Context) bool {
	return openAITrafficDirectorRetryLoopEnabled(ctx) && s.OpenAITrafficDirectorEnforcedInContext(ctx)
}

func isTrafficDirectorNoAvailablePool(err error) bool {
	return errors.Is(err, ErrTrafficDirectorNoAvailablePool)
}
