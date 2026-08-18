package service

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultOpenAIAccountAuditLongTextRuneThreshold = 12000

	openAIAccountAuditRoutingRuntimeCacheTTL  = 60 * time.Second
	openAIAccountAuditRoutingRuntimeErrorTTL  = 5 * time.Second
	openAIAccountAuditRoutingRuntimeDBTimeout = 5 * time.Second
	openAIAccountAuditRoutingRuntimeSFKey     = "openai_account_audit_routing_runtime"
)

var defaultOpenAIAccountAuditGroupIDs = []int64{12}

type OpenAIAccountAuditRoutingSettings struct {
	AccountGroupIDs             []int64
	LongTextRuneThreshold       int
	PreferAPIKeyEnabled         bool
	LongTextOAuthRolloutPercent int
}

type OpenAIAccountAuditRoutingPolicy struct {
	groupIDs                    map[int64]struct{}
	orderedGroupIDs             []int64
	longTextRuneThreshold       int
	preferAPIKeyEnabled         bool
	longTextOAuthRolloutPercent int
	available                   bool
}

type OpenAIAccountAuditRoutingReason string

const (
	OpenAIAccountAuditRoutingNormal            OpenAIAccountAuditRoutingReason = "normal"
	OpenAIAccountAuditRoutingContextUnreliable OpenAIAccountAuditRoutingReason = "context_unreliable"
	OpenAIAccountAuditRoutingLongText          OpenAIAccountAuditRoutingReason = "long_text"
	OpenAIAccountAuditRoutingStateUnavailable  OpenAIAccountAuditRoutingReason = "state_unavailable"
)

type OpenAIAccountRoutingPreference string

const (
	OpenAIAccountRoutingPreferenceNone         OpenAIAccountRoutingPreference = ""
	OpenAIAccountRoutingPreferenceAPIKey       OpenAIAccountRoutingPreference = "api_key"
	OpenAIAccountRoutingPreferenceAuditedOAuth OpenAIAccountRoutingPreference = "audited_oauth"
)

// OpenAIAccountRoutingOptions is a request-local scheduler contract. The
// preference only affects unbound load-balanced selection. Conditional
// requirements are applied to accounts that are eligible, or may be eligible,
// for local audit after their fresh database state is loaded.
type OpenAIAccountRoutingOptions struct {
	Preference                      OpenAIAccountRoutingPreference
	AuditRoutingReason              OpenAIAccountAuditRoutingReason
	AuditPolicy                     OpenAIAccountAuditRoutingPolicy
	AuditRequiredTransport          OpenAIUpstreamTransport
	AuditRequiredEndpointCapability OpenAIEndpointCapability
}

type openAIAccountRoutingOptionsContextKey struct{}

type openAIAccountRoutingTypeTier uint8

const (
	openAIAccountRoutingTypeAny openAIAccountRoutingTypeTier = iota
	openAIAccountRoutingTypeAPIKey
	openAIAccountRoutingTypeAuditedOAuth
	openAIAccountRoutingTypeNonAPIKey
	openAIAccountRoutingTypeNonAuditedOAuth
)

func (o OpenAIAccountRoutingOptions) effectivePreference() OpenAIAccountRoutingPreference {
	switch o.Preference {
	case OpenAIAccountRoutingPreferenceAPIKey, OpenAIAccountRoutingPreferenceAuditedOAuth:
		return o.Preference
	default:
		return OpenAIAccountRoutingPreferenceNone
	}
}

func (o OpenAIAccountRoutingOptions) PrefersAPIKey() bool {
	return o.effectivePreference() == OpenAIAccountRoutingPreferenceAPIKey
}

func (o OpenAIAccountRoutingOptions) PrefersAuditedOAuth() bool {
	return o.effectivePreference() == OpenAIAccountRoutingPreferenceAuditedOAuth
}

func openAIAccountRoutingTypeTiers(options OpenAIAccountRoutingOptions) []openAIAccountRoutingTypeTier {
	switch options.effectivePreference() {
	case OpenAIAccountRoutingPreferenceAPIKey:
		return []openAIAccountRoutingTypeTier{openAIAccountRoutingTypeAPIKey, openAIAccountRoutingTypeNonAPIKey}
	case OpenAIAccountRoutingPreferenceAuditedOAuth:
		return []openAIAccountRoutingTypeTier{
			openAIAccountRoutingTypeAuditedOAuth,
			openAIAccountRoutingTypeAPIKey,
			openAIAccountRoutingTypeNonAuditedOAuth,
		}
	default:
		return []openAIAccountRoutingTypeTier{openAIAccountRoutingTypeAny}
	}
}

func openAIAccountMatchesRoutingTypeTier(account *Account, tier openAIAccountRoutingTypeTier, options OpenAIAccountRoutingOptions) bool {
	if account == nil {
		return false
	}
	switch tier {
	case openAIAccountRoutingTypeAPIKey:
		return account.Type == AccountTypeAPIKey
	case openAIAccountRoutingTypeAuditedOAuth:
		return ClassifyOpenAIAccountAuditEligibility(account, options.AuditPolicy).Eligible
	case openAIAccountRoutingTypeNonAPIKey:
		return account.Type != AccountTypeAPIKey
	case openAIAccountRoutingTypeNonAuditedOAuth:
		return account.Type != AccountTypeAPIKey &&
			!ClassifyOpenAIAccountAuditEligibility(account, options.AuditPolicy).Eligible
	default:
		return true
	}
}

func openAIAccountRoutingTypeTierForAccount(account *Account, options OpenAIAccountRoutingOptions) (openAIAccountRoutingTypeTier, int, bool) {
	for index, tier := range openAIAccountRoutingTypeTiers(options) {
		if openAIAccountMatchesRoutingTypeTier(account, tier, options) {
			return tier, index, true
		}
	}
	return openAIAccountRoutingTypeAny, 0, false
}

func WithOpenAIAccountRoutingOptions(ctx context.Context, options OpenAIAccountRoutingOptions) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, openAIAccountRoutingOptionsContextKey{}, options)
}

func openAIAccountRoutingOptionsFromContext(ctx context.Context) OpenAIAccountRoutingOptions {
	if ctx == nil {
		return OpenAIAccountRoutingOptions{}
	}
	options, _ := ctx.Value(openAIAccountRoutingOptionsContextKey{}).(OpenAIAccountRoutingOptions)
	return options
}

func (o OpenAIAccountRoutingOptions) requirementsFor(
	account *Account,
	baseTransport OpenAIUpstreamTransport,
	baseCapability OpenAIEndpointCapability,
) (OpenAIUpstreamTransport, OpenAIEndpointCapability) {
	if o.AuditRequiredTransport == OpenAIUpstreamTransportAny && o.AuditRequiredEndpointCapability == "" {
		return baseTransport, baseCapability
	}
	eligibility := ClassifyOpenAIAccountAuditEligibility(account, o.AuditPolicy)
	if !eligibility.Eligible && !eligibility.Indeterminate {
		return baseTransport, baseCapability
	}
	transport := baseTransport
	if o.AuditRequiredTransport != OpenAIUpstreamTransportAny {
		transport = o.AuditRequiredTransport
	}
	capability := baseCapability
	if o.AuditRequiredEndpointCapability != "" {
		capability = o.AuditRequiredEndpointCapability
	}
	return transport, capability
}

func normalizeOpenAIAccountAuditGroupIDs(groupIDs []int64) []int64 {
	seen := make(map[int64]struct{}, len(groupIDs))
	normalized := make([]int64, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		if groupID <= 0 {
			continue
		}
		if _, ok := seen[groupID]; ok {
			continue
		}
		seen[groupID] = struct{}{}
		normalized = append(normalized, groupID)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i] < normalized[j] })
	return normalized
}

func newOpenAIAccountAuditRoutingPolicy(settings OpenAIAccountAuditRoutingSettings, available bool) OpenAIAccountAuditRoutingPolicy {
	groupIDs := normalizeOpenAIAccountAuditGroupIDs(settings.AccountGroupIDs)
	if len(groupIDs) == 0 {
		groupIDs = append([]int64(nil), defaultOpenAIAccountAuditGroupIDs...)
	}
	threshold := settings.LongTextRuneThreshold
	if threshold <= 0 {
		threshold = DefaultOpenAIAccountAuditLongTextRuneThreshold
	}
	groupSet := make(map[int64]struct{}, len(groupIDs))
	for _, groupID := range groupIDs {
		groupSet[groupID] = struct{}{}
	}
	return OpenAIAccountAuditRoutingPolicy{
		groupIDs:                    groupSet,
		orderedGroupIDs:             groupIDs,
		longTextRuneThreshold:       threshold,
		preferAPIKeyEnabled:         settings.PreferAPIKeyEnabled,
		longTextOAuthRolloutPercent: settings.LongTextOAuthRolloutPercent,
		available:                   available,
	}
}

func NewOpenAIAccountAuditRoutingPolicy(settings OpenAIAccountAuditRoutingSettings) (OpenAIAccountAuditRoutingPolicy, error) {
	settings.AccountGroupIDs = normalizeOpenAIAccountAuditGroupIDs(settings.AccountGroupIDs)
	if err := ValidateOpenAIAccountAuditRoutingSettings(settings); err != nil {
		return OpenAIAccountAuditRoutingPolicy{}, err
	}
	return newOpenAIAccountAuditRoutingPolicy(settings, true), nil
}

func DefaultOpenAIAccountAuditRoutingPolicy() OpenAIAccountAuditRoutingPolicy {
	return newOpenAIAccountAuditRoutingPolicy(OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs:             defaultOpenAIAccountAuditGroupIDs,
		LongTextRuneThreshold:       DefaultOpenAIAccountAuditLongTextRuneThreshold,
		PreferAPIKeyEnabled:         true,
		LongTextOAuthRolloutPercent: 0,
	}, true)
}

func (p OpenAIAccountAuditRoutingPolicy) AccountGroupIDs() []int64 {
	return append([]int64(nil), p.orderedGroupIDs...)
}

func (p OpenAIAccountAuditRoutingPolicy) LongTextRuneThreshold() int {
	if p.longTextRuneThreshold <= 0 {
		return DefaultOpenAIAccountAuditLongTextRuneThreshold
	}
	return p.longTextRuneThreshold
}

func (p OpenAIAccountAuditRoutingPolicy) PreferAPIKeyEnabled() bool {
	return p.preferAPIKeyEnabled
}

func (p OpenAIAccountAuditRoutingPolicy) LongTextOAuthRolloutPercent() int {
	if p.longTextOAuthRolloutPercent < 0 || p.longTextOAuthRolloutPercent > 100 {
		return 0
	}
	return p.longTextOAuthRolloutPercent
}

func (p OpenAIAccountAuditRoutingPolicy) LongTextOAuthRolloutSelected(stableKey string) bool {
	percent := p.LongTextOAuthRolloutPercent()
	if percent <= 0 {
		return false
	}
	if percent >= 100 {
		return true
	}
	stableKey = strings.TrimSpace(stableKey)
	if stableKey == "" {
		return false
	}
	digest := sha256.Sum256([]byte(stableKey))
	bucket := binary.BigEndian.Uint64(digest[:8]) % 100
	return bucket < uint64(percent)
}

func (p OpenAIAccountAuditRoutingPolicy) Available() bool {
	return p.available
}

func (p OpenAIAccountAuditRoutingPolicy) includesGroup(groupID int64) bool {
	_, ok := p.groupIDs[groupID]
	return ok
}

type OpenAIAccountAuditEligibilityReason string

const (
	OpenAIAccountAuditEligible              OpenAIAccountAuditEligibilityReason = "eligible"
	OpenAIAccountAuditIneligibleNilAccount  OpenAIAccountAuditEligibilityReason = "nil_account"
	OpenAIAccountAuditIneligiblePlatform    OpenAIAccountAuditEligibilityReason = "platform"
	OpenAIAccountAuditIneligibleAccountType OpenAIAccountAuditEligibilityReason = "account_type"
	OpenAIAccountAuditIneligiblePlan        OpenAIAccountAuditEligibilityReason = "plan_type"
	OpenAIAccountAuditIneligibleGroup       OpenAIAccountAuditEligibilityReason = "account_group"
	OpenAIAccountAuditPolicyUnavailable     OpenAIAccountAuditEligibilityReason = "policy_unavailable"
)

type OpenAIAccountAuditEligibility struct {
	Eligible       bool
	Indeterminate  bool
	Reason         OpenAIAccountAuditEligibilityReason
	MatchedGroupID int64
}

// ClassifyOpenAIAccountAuditEligibility applies the exact local-audit account contract.
// APIKey, setup-token, and every non-OAuth type are permanently ineligible.
func ClassifyOpenAIAccountAuditEligibility(account *Account, policy OpenAIAccountAuditRoutingPolicy) OpenAIAccountAuditEligibility {
	if account == nil {
		return OpenAIAccountAuditEligibility{Reason: OpenAIAccountAuditIneligibleNilAccount}
	}
	if account.Platform != PlatformOpenAI {
		return OpenAIAccountAuditEligibility{Reason: OpenAIAccountAuditIneligiblePlatform}
	}
	if account.Type != AccountTypeOAuth {
		return OpenAIAccountAuditEligibility{Reason: OpenAIAccountAuditIneligibleAccountType}
	}
	if strings.ToLower(strings.TrimSpace(account.GetCredential("plan_type"))) != "pro" {
		return OpenAIAccountAuditEligibility{Reason: OpenAIAccountAuditIneligiblePlan}
	}
	if !policy.Available() {
		return OpenAIAccountAuditEligibility{Indeterminate: true, Reason: OpenAIAccountAuditPolicyUnavailable}
	}
	for _, groupID := range account.GroupIDs {
		if policy.includesGroup(groupID) {
			return OpenAIAccountAuditEligibility{Eligible: true, Reason: OpenAIAccountAuditEligible, MatchedGroupID: groupID}
		}
	}
	for _, accountGroup := range account.AccountGroups {
		if policy.includesGroup(accountGroup.GroupID) {
			return OpenAIAccountAuditEligibility{Eligible: true, Reason: OpenAIAccountAuditEligible, MatchedGroupID: accountGroup.GroupID}
		}
	}
	return OpenAIAccountAuditEligibility{Reason: OpenAIAccountAuditIneligibleGroup}
}

type cachedOpenAIAccountAuditRoutingRuntime struct {
	policy    OpenAIAccountAuditRoutingPolicy
	expiresAt int64
}

func ValidateOpenAIAccountAuditRoutingSettings(settings OpenAIAccountAuditRoutingSettings) error {
	if len(normalizeOpenAIAccountAuditGroupIDs(settings.AccountGroupIDs)) == 0 {
		return fmt.Errorf("%s must contain at least one positive account group ID", SettingKeyOpenAIAccountAuditGroupIDs)
	}
	if settings.LongTextRuneThreshold <= 0 {
		return fmt.Errorf("%s must be greater than zero", SettingKeyOpenAIAccountAuditLongTextRuneThreshold)
	}
	if settings.LongTextOAuthRolloutPercent < 0 || settings.LongTextOAuthRolloutPercent > 100 {
		return fmt.Errorf("%s must be between 0 and 100", SettingKeyOpenAIAccountAuditLongTextOAuthRolloutPercent)
	}
	return nil
}

func (s *SettingService) UpdateOpenAIAccountAuditRoutingSettings(ctx context.Context, settings OpenAIAccountAuditRoutingSettings) error {
	if s == nil || s.settingRepo == nil {
		return fmt.Errorf("setting repository is unavailable")
	}
	settings.AccountGroupIDs = normalizeOpenAIAccountAuditGroupIDs(settings.AccountGroupIDs)
	if err := ValidateOpenAIAccountAuditRoutingSettings(settings); err != nil {
		return err
	}
	groupIDsJSON, err := json.Marshal(settings.AccountGroupIDs)
	if err != nil {
		return fmt.Errorf("marshal OpenAI account audit group IDs: %w", err)
	}
	if err := s.settingRepo.SetMultiple(ctx, map[string]string{
		SettingKeyOpenAIAccountAuditGroupIDs:                    string(groupIDsJSON),
		SettingKeyOpenAIAccountAuditLongTextRuneThreshold:       strconv.Itoa(settings.LongTextRuneThreshold),
		SettingKeyOpenAIAccountAuditPreferAPIKeyEnabled:         strconv.FormatBool(settings.PreferAPIKeyEnabled),
		SettingKeyOpenAIAccountAuditLongTextOAuthRolloutPercent: strconv.Itoa(settings.LongTextOAuthRolloutPercent),
	}); err != nil {
		return err
	}
	policy := newOpenAIAccountAuditRoutingPolicy(settings, true)
	s.replaceOpenAIAccountAuditRoutingRuntime(&cachedOpenAIAccountAuditRoutingRuntime{
		policy:    policy,
		expiresAt: time.Now().Add(openAIAccountAuditRoutingRuntimeCacheTTL).UnixNano(),
	})
	return nil
}

func (s *SettingService) replaceOpenAIAccountAuditRoutingRuntime(entry *cachedOpenAIAccountAuditRoutingRuntime) {
	if s == nil || entry == nil {
		return
	}
	s.openAIAccountAuditRoutingRuntimeMu.Lock()
	defer s.openAIAccountAuditRoutingRuntimeMu.Unlock()
	s.openAIAccountAuditRoutingGeneration++
	s.openAIAccountAuditRoutingRuntimeSF.Forget(openAIAccountAuditRoutingRuntimeSFKey)
	s.openAIAccountAuditRoutingRuntimeCache.Store(entry)
}

func (s *SettingService) openAIAccountAuditRoutingLoadGeneration() uint64 {
	s.openAIAccountAuditRoutingRuntimeMu.Lock()
	defer s.openAIAccountAuditRoutingRuntimeMu.Unlock()
	return s.openAIAccountAuditRoutingGeneration
}

func (s *SettingService) storeOpenAIAccountAuditRoutingRuntimeForGeneration(
	generation uint64,
	entry *cachedOpenAIAccountAuditRoutingRuntime,
) *cachedOpenAIAccountAuditRoutingRuntime {
	s.openAIAccountAuditRoutingRuntimeMu.Lock()
	defer s.openAIAccountAuditRoutingRuntimeMu.Unlock()
	if generation != s.openAIAccountAuditRoutingGeneration {
		if current, ok := s.openAIAccountAuditRoutingRuntimeCache.Load().(*cachedOpenAIAccountAuditRoutingRuntime); ok && current != nil {
			return current
		}
		return entry
	}
	s.openAIAccountAuditRoutingRuntimeCache.Store(entry)
	return entry
}

// refreshOpenAIAccountAuditLongTextOAuthRolloutPercent keeps the hot-path
// policy cache aligned with a generic SystemSettings write. Other audit policy
// fields are preserved; if no policy has been loaded yet, the expired sentinel
// makes the first request load the complete policy from storage.
func (s *SettingService) refreshOpenAIAccountAuditLongTextOAuthRolloutPercent(percent int) {
	if s == nil || percent < 0 || percent > 100 {
		return
	}
	s.openAIAccountAuditRoutingRuntimeMu.Lock()
	defer s.openAIAccountAuditRoutingRuntimeMu.Unlock()
	s.openAIAccountAuditRoutingGeneration++
	s.openAIAccountAuditRoutingRuntimeSF.Forget(openAIAccountAuditRoutingRuntimeSFKey)
	policy := DefaultOpenAIAccountAuditRoutingPolicy()
	policy.available = false
	expiresAt := int64(0)
	if cached, ok := s.openAIAccountAuditRoutingRuntimeCache.Load().(*cachedOpenAIAccountAuditRoutingRuntime); ok && cached != nil {
		policy = cached.policy
		if policy.Available() {
			expiresAt = time.Now().Add(openAIAccountAuditRoutingRuntimeCacheTTL).UnixNano()
		}
	}
	policy.longTextOAuthRolloutPercent = percent
	s.openAIAccountAuditRoutingRuntimeCache.Store(&cachedOpenAIAccountAuditRoutingRuntime{
		policy:    policy,
		expiresAt: expiresAt,
	})
}

func (s *SettingService) GetOpenAIAccountAuditRoutingRuntime(ctx context.Context) OpenAIAccountAuditRoutingPolicy {
	defaultPolicy := DefaultOpenAIAccountAuditRoutingPolicy()
	if s == nil || s.settingRepo == nil {
		defaultPolicy.available = false
		return defaultPolicy
	}
	if cached, ok := s.openAIAccountAuditRoutingRuntimeCache.Load().(*cachedOpenAIAccountAuditRoutingRuntime); ok && cached != nil && time.Now().UnixNano() < cached.expiresAt {
		return cached.policy
	}

	result, _, _ := s.openAIAccountAuditRoutingRuntimeSF.Do(openAIAccountAuditRoutingRuntimeSFKey, func() (any, error) {
		if cached, ok := s.openAIAccountAuditRoutingRuntimeCache.Load().(*cachedOpenAIAccountAuditRoutingRuntime); ok && cached != nil && time.Now().UnixNano() < cached.expiresAt {
			return cached, nil
		}
		generation := s.openAIAccountAuditRoutingLoadGeneration()
		dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), openAIAccountAuditRoutingRuntimeDBTimeout)
		defer cancel()
		values, err := s.settingRepo.GetMultiple(dbCtx, []string{
			SettingKeyOpenAIAccountAuditGroupIDs,
			SettingKeyOpenAIAccountAuditLongTextRuneThreshold,
			SettingKeyOpenAIAccountAuditPreferAPIKeyEnabled,
			SettingKeyOpenAIAccountAuditLongTextOAuthRolloutPercent,
		})
		if err != nil {
			policy := defaultPolicy
			if stale, ok := s.openAIAccountAuditRoutingRuntimeCache.Load().(*cachedOpenAIAccountAuditRoutingRuntime); ok && stale != nil {
				policy = stale.policy
			}
			policy.available = false
			slog.Warn("failed to get OpenAI account audit routing settings; retaining last policy", "error", err)
			entry := &cachedOpenAIAccountAuditRoutingRuntime{
				policy:    policy,
				expiresAt: time.Now().Add(openAIAccountAuditRoutingRuntimeErrorTTL).UnixNano(),
			}
			return s.storeOpenAIAccountAuditRoutingRuntimeForGeneration(generation, entry), nil
		}

		settings := OpenAIAccountAuditRoutingSettings{
			AccountGroupIDs:             defaultOpenAIAccountAuditGroupIDs,
			LongTextRuneThreshold:       DefaultOpenAIAccountAuditLongTextRuneThreshold,
			PreferAPIKeyEnabled:         true,
			LongTextOAuthRolloutPercent: 0,
		}
		available := true
		if raw, ok := values[SettingKeyOpenAIAccountAuditGroupIDs]; ok && strings.TrimSpace(raw) != "" {
			var groupIDs []int64
			if parseErr := json.Unmarshal([]byte(raw), &groupIDs); parseErr == nil && len(normalizeOpenAIAccountAuditGroupIDs(groupIDs)) > 0 {
				settings.AccountGroupIDs = normalizeOpenAIAccountAuditGroupIDs(groupIDs)
			} else {
				available = false
				slog.Warn("invalid OpenAI account audit group IDs; using defaults", "error", parseErr)
			}
		}
		if raw, ok := values[SettingKeyOpenAIAccountAuditLongTextRuneThreshold]; ok {
			if threshold, parseErr := strconv.Atoi(strings.TrimSpace(raw)); parseErr == nil && threshold > 0 {
				settings.LongTextRuneThreshold = threshold
			} else {
				available = false
			}
		}
		if raw, ok := values[SettingKeyOpenAIAccountAuditPreferAPIKeyEnabled]; ok {
			switch strings.ToLower(strings.TrimSpace(raw)) {
			case "true":
				settings.PreferAPIKeyEnabled = true
			case "false":
				settings.PreferAPIKeyEnabled = false
			default:
				available = false
			}
		}
		if raw, ok := values[SettingKeyOpenAIAccountAuditLongTextOAuthRolloutPercent]; ok {
			if percent, parseErr := strconv.Atoi(strings.TrimSpace(raw)); parseErr == nil && percent >= 0 && percent <= 100 {
				settings.LongTextOAuthRolloutPercent = percent
			} else {
				available = false
			}
		}
		entry := &cachedOpenAIAccountAuditRoutingRuntime{
			policy:    newOpenAIAccountAuditRoutingPolicy(settings, available),
			expiresAt: time.Now().Add(openAIAccountAuditRoutingRuntimeCacheTTL).UnixNano(),
		}
		return s.storeOpenAIAccountAuditRoutingRuntimeForGeneration(generation, entry), nil
	})
	if entry, ok := result.(*cachedOpenAIAccountAuditRoutingRuntime); ok && entry != nil {
		return entry.policy
	}
	return defaultPolicy
}

func (s *OpenAIGatewayService) OpenAIAccountAuditRoutingPolicy(ctx context.Context) OpenAIAccountAuditRoutingPolicy {
	if s == nil || s.settingService == nil {
		policy := DefaultOpenAIAccountAuditRoutingPolicy()
		policy.available = false
		return policy
	}
	return s.settingService.GetOpenAIAccountAuditRoutingRuntime(ctx)
}

func (s *OpenAIGatewayService) ClassifyOpenAIAccountAuditEligibility(
	ctx context.Context,
	account *Account,
) OpenAIAccountAuditEligibility {
	return ClassifyOpenAIAccountAuditEligibility(account, s.OpenAIAccountAuditRoutingPolicy(ctx))
}

func (s *OpenAIGatewayService) openAIAccountMeetsRoutingRequirements(
	ctx context.Context,
	account *Account,
	baseTransport OpenAIUpstreamTransport,
	baseCapability OpenAIEndpointCapability,
) bool {
	options := openAIAccountRoutingOptionsFromContext(ctx)
	requiredTransport, requiredCapability := options.requirementsFor(account, baseTransport, baseCapability)
	return accountSupportsOpenAICapabilities(account, requiredCapability, "") &&
		s.isOpenAIAccountTransportCompatible(account, requiredTransport)
}

func prioritizeOpenAIAccountsForRouting(accounts []*Account, options OpenAIAccountRoutingOptions) []*Account {
	if options.effectivePreference() == OpenAIAccountRoutingPreferenceNone || len(accounts) < 2 {
		return accounts
	}
	ordered := make([]*Account, 0, len(accounts))
	for _, tier := range openAIAccountRoutingTypeTiers(options) {
		for _, account := range accounts {
			if openAIAccountMatchesRoutingTypeTier(account, tier, options) {
				ordered = append(ordered, account)
			}
		}
	}
	return ordered
}
