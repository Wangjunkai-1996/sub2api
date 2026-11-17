package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const openAISessionEgressAffinityRouteCachePrefix = "openai:session:egress-affinity-route:"

// openAIMovableResponseEgressMarkerState describes the durable routing state
// observed before a movable previous_response_id enters weighted scheduling.
// Account-only bindings are portable for API-key/legacy accounts, but an
// enforced OAuth egress pool requires the matching route half as a hard fence.
type openAIMovableResponseEgressMarkerState struct {
	AccountID         int64
	BindingID         string
	HasAccountBinding bool
	HasEgressBinding  bool
	PoolEnforced      bool
}

// inspectOpenAIMovableResponseEgressMarkers prevents a partial durable marker
// from turning an enforced-pool continuation into an ordinary weighted pick.
// It deliberately preserves account-only API-key/legacy behavior because
// those upstream response IDs remain portable under the existing scheduler.
func (s *OpenAIGatewayService) inspectOpenAIMovableResponseEgressMarkers(
	ctx context.Context,
	groupID *int64,
	responseID string,
) (openAIMovableResponseEgressMarkerState, error) {
	state := openAIMovableResponseEgressMarkerState{}
	responseID = strings.TrimSpace(responseID)
	if s == nil || responseID == "" {
		return state, nil
	}
	store := s.getOpenAIWSStateStore()
	if store == nil {
		return state, nil
	}

	groupIDValue := derefGroupID(groupID)
	accountID, hasAccount, err := getOpenAIWSResponseAccountWithError(store, ctx, groupIDValue, responseID)
	if err != nil {
		if errors.Is(err, ErrAccountEgressNoRoute) || errors.Is(err, ErrAccountEgressConfigStale) {
			return state, err
		}
		return state, fmt.Errorf("%w: read movable response account binding: %v", ErrAccountEgressUnavailable, err)
	}
	state.AccountID = accountID
	state.HasAccountBinding = hasAccount

	bindingID, hasEgress, err := getOpenAIWSResponseEgressWithError(store, ctx, groupIDValue, responseID)
	if err != nil {
		if errors.Is(err, ErrAccountEgressNoRoute) || errors.Is(err, ErrAccountEgressConfigStale) {
			return state, err
		}
		return state, fmt.Errorf("%w: read movable response egress binding: %v", ErrAccountEgressUnavailable, err)
	}
	bindingID = strings.TrimSpace(bindingID)
	state.BindingID = bindingID
	state.HasEgressBinding = hasEgress && bindingID != ""

	if !state.HasAccountBinding {
		if state.HasEgressBinding {
			return state, fmt.Errorf("%w: response %s egress marker has no account marker", ErrAccountEgressNoRoute, responseID)
		}
		return state, nil
	}
	if state.AccountID <= 0 {
		return state, fmt.Errorf("%w: response %s has invalid account marker %d", ErrAccountEgressConfigStale, responseID, state.AccountID)
	}
	if state.HasEgressBinding {
		boundAccountID, _, valid := parseStableAccountEgressBindingID(state.BindingID)
		if !valid || boundAccountID != state.AccountID {
			return state, fmt.Errorf(
				"%w: response %s account marker %d does not match egress binding %q",
				ErrAccountEgressConfigStale,
				responseID,
				state.AccountID,
				state.BindingID,
			)
		}
	}

	if s.accountRepo == nil {
		return state, fmt.Errorf("%w: inspect movable response account %d", ErrAccountEgressUnavailable, state.AccountID)
	}
	account, err := s.accountRepo.GetByID(ctx, state.AccountID)
	if err != nil {
		if errors.Is(err, ErrAccountNotFound) {
			return state, fmt.Errorf("%w: response %s account %d no longer exists", ErrAccountEgressNoRoute, responseID, state.AccountID)
		}
		return state, fmt.Errorf("%w: inspect movable response account %d: %v", ErrAccountEgressUnavailable, state.AccountID, err)
	}
	if account == nil {
		return state, fmt.Errorf("%w: response %s account %d is unavailable", ErrAccountEgressNoRoute, responseID, state.AccountID)
	}
	state.PoolEnforced = accountUsesEnforcedEgressPool(ctx, s.settingService, account)
	if state.PoolEnforced && !state.HasEgressBinding {
		return state, fmt.Errorf("%w: response %s account %d has no egress binding", ErrAccountEgressNoRoute, responseID, state.AccountID)
	}
	if state.HasEgressBinding && !state.PoolEnforced {
		return state, fmt.Errorf("%w: response %s egress pool is not currently enforced", ErrAccountEgressConfigStale, responseID)
	}
	return state, nil
}

// withOpenAISessionEgressPreference adds a soft route preference for ordinary
// conversation turns. The allocator may spill to another healthy identity when
// this route is full; previous_response_id uses the separate required fence.
func (s *OpenAIGatewayService) withOpenAISessionEgressPreference(
	ctx context.Context,
	groupID *int64,
	sessionHash string,
) context.Context {
	bindingID, ok, err := s.getOpenAISessionEgressAffinity(ctx, groupID, sessionHash)
	if err != nil || !ok {
		return ctx
	}
	return WithPreferredAccountEgressBinding(ctx, bindingID)
}

func (s *OpenAIGatewayService) getOpenAISessionEgressAffinity(
	ctx context.Context,
	groupID *int64,
	sessionHash string,
) (string, bool, error) {
	if s == nil || s.cache == nil || strings.TrimSpace(sessionHash) == "" {
		return "", false, nil
	}
	accountID, err := s.getStickySessionAccountID(ctx, groupID, sessionHash)
	if err != nil {
		if errors.Is(err, ErrStickySessionNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	if accountID <= 0 {
		return "", false, nil
	}

	cacheCtx, cancel := withOpenAIWSStateStoreRedisTimeout(ctx)
	defer cancel()
	routeID, err := s.cache.GetSessionAccountID(
		cacheCtx,
		derefGroupID(groupID),
		openAISessionEgressAffinityCacheKey(sessionHash),
	)
	if err != nil {
		if errors.Is(err, ErrStickySessionNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	if routeID <= 0 {
		return "", false, nil
	}
	return StableAccountEgressBindingID(accountID, routeID), true, nil
}

// openAIWSResponseEgressBinding returns the durable route fence for a response
// when one exists. A response with this marker was produced through an
// identity-aware egress pool and must not be sent through ordinary account
// scheduling, even when the advanced scheduler is disabled or movable sticky
// weighting is enabled.
func (s *OpenAIGatewayService) openAIWSResponseEgressBinding(
	ctx context.Context,
	groupID *int64,
	responseID string,
) (string, bool, error) {
	if s == nil || strings.TrimSpace(responseID) == "" {
		return "", false, nil
	}
	store := s.getOpenAIWSStateStore()
	if store == nil {
		return "", false, nil
	}
	bindingID, ok, err := getOpenAIWSResponseEgressWithError(
		store,
		ctx,
		derefGroupID(groupID),
		strings.TrimSpace(responseID),
	)
	if err != nil {
		// Preserve typed marker inconsistency/no-route errors so callers can
		// distinguish a corrupt durable fence from a transient Redis outage.
		if errors.Is(err, ErrAccountEgressNoRoute) || errors.Is(err, ErrAccountEgressConfigStale) {
			return "", false, err
		}
		return "", false, fmt.Errorf("%w: read response egress binding: %v", ErrAccountEgressUnavailable, err)
	}
	bindingID = strings.TrimSpace(bindingID)
	return bindingID, ok && bindingID != "", nil
}

// bindOpenAISessionEgressAffinity records the first healthy route selected for
// a sticky account. Capacity spillover does not migrate the durable preference;
// a missing or unhealthy preferred route is replaced by the selected route.
func (s *OpenAIGatewayService) bindOpenAISessionEgressAffinity(
	ctx context.Context,
	groupID *int64,
	sessionHash string,
	account *Account,
) error {
	if s == nil || s.cache == nil || account == nil || account.SelectedEgress == nil ||
		strings.TrimSpace(sessionHash) == "" || preserveOpenAISelectionStickyBinding(ctx) {
		return nil
	}
	selected := account.SelectedEgress
	if selected.RouteID <= 0 || strings.TrimSpace(selected.BindingID) == "" {
		return nil
	}

	stickyAccountID, stickyErr := s.getStickySessionAccountID(ctx, groupID, sessionHash)
	if stickyErr != nil {
		if errors.Is(stickyErr, ErrStickySessionNotFound) {
			// Do not leave a route marker without its account fence. This can
			// happen when account admission failed or a profit gate rejected it.
			return nil
		}
		return stickyErr
	}
	if stickyAccountID <= 0 || stickyAccountID != account.ID {
		return nil
	}

	preferredBindingID, found, readErr := s.getOpenAISessionEgressAffinity(ctx, groupID, sessionHash)
	if readErr != nil {
		return readErr
	}
	routeID := selected.RouteID
	if found {
		boundAccountID, preferredRouteID, valid := parseStableAccountEgressBindingID(preferredBindingID)
		if valid && boundAccountID == account.ID && accountEgressBindingIsHealthy(account, preferredBindingID) {
			routeID = preferredRouteID
		}
	}

	cacheCtx, cancel := withOpenAIWSStateStoreDetachedRedisTimeout(ctx)
	defer cancel()
	if err := s.cache.SetSessionAccountID(
		cacheCtx,
		derefGroupID(groupID),
		openAISessionEgressAffinityCacheKey(sessionHash),
		routeID,
		s.openAIWSSessionStickyTTL(),
	); err != nil {
		return fmt.Errorf("bind OpenAI session egress affinity: %w", err)
	}
	return nil
}

// BindOpenAISessionEgressAffinity persists the route selected by a real
// account admission. Callers should bind the account sticky session first;
// without that account fence this method intentionally does nothing.
func (s *OpenAIGatewayService) BindOpenAISessionEgressAffinity(
	ctx context.Context,
	groupID *int64,
	sessionHash string,
	account *Account,
) error {
	return s.bindOpenAISessionEgressAffinity(ctx, groupID, sessionHash, account)
}

func accountEgressBindingIsHealthy(account *Account, bindingID string) bool {
	config, err := AccountEgressPoolConfigForRuntime(account, 0)
	if err != nil {
		return false
	}
	candidate, ok := config.Candidate(bindingID)
	return ok && candidate.Healthy
}

func openAISessionEgressAffinityCacheKey(sessionHash string) string {
	return openAIWSEgressCacheKey(openAISessionEgressAffinityRouteCachePrefix, sessionHash)
}
