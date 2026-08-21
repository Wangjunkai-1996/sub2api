package service

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
)

var (
	// ErrOpenAIAccountRequirementIncompatible means the terminal account exists,
	// but its effective credential owner cannot satisfy the request-local hard
	// constraint. Callers may release the slot and reselect while preserving the
	// same requirement.
	ErrOpenAIAccountRequirementIncompatible = fmt.Errorf("%w: account requirement incompatible", ErrNoAvailableAccounts)
	// ErrOpenAIAccountAdmissionUnavailable means terminal admission could not
	// establish a fresh selected row or effective credential owner.
	ErrOpenAIAccountAdmissionUnavailable = fmt.Errorf("%w: terminal account admission unavailable", ErrNoAvailableAccounts)
	// ErrOpenAIPreviousResponseBindingUnavailable means a response binding was
	// found, but its account cannot safely serve this continuation. Callers must
	// map this to a controlled no-available result when migration is forbidden.
	// A missing binding may enter ordinary selection only when the caller has
	// independently proved that the continuation can be rebuilt and migrated.
	ErrOpenAIPreviousResponseBindingUnavailable = fmt.Errorf("%w: previous response binding unavailable", ErrNoAvailableAccounts)
)

type openAIAccountRequirementContextKey struct{}

type openAIAccountRequirementContextValue struct {
	requirement securityadmission.AccountRequirement

	mu      sync.RWMutex
	parents map[int64]*Account
}

// OpenAIAccountRequirementAdmission is the fresh terminal account snapshot.
// Selected and EffectiveCredentialOwner are the rows that the caller must bind
// to any downstream permit or token-refresh recheck.
type OpenAIAccountRequirementAdmission struct {
	Selected                 *Account
	EffectiveCredentialOwner *Account
	Requirement              securityadmission.AccountRequirement
	AccountClass             securityadmission.AccountClass
}

// OpenAICredentialProof binds a long-lived upstream connection to the exact
// selected row and effective credential document used at handshake time. The
// token itself is never retained.
type OpenAICredentialProof struct {
	selectedAccountID       int64
	selectedAccountPlatform string
	selectedAccountType     string
	effectiveOwnerID        int64
	effectiveOwnerPlatform  string
	effectiveOwnerType      string
	accountClass            securityadmission.AccountClass
	authMode                string
	tokenVersion            int64
	tokenHash               [sha256.Size]byte
	hasToken                bool
	agentIdentityHash       [sha256.Size]byte
	hasAgentIdentityHash    bool
}

type openAITerminalAdmissionContextKey struct{}

// openAIFinalizedCredential is the immutable credential snapshot authorized
// for the next upstream dispatch. It deliberately retains only a token hash;
// callers continue to pass the actual token through the existing stack.
type openAIFinalizedCredential struct {
	admission *OpenAIAccountRequirementAdmission
	authMode  string
	tokenHash [sha256.Size]byte
	hasToken  bool
}

type openAITerminalAdmissionContextValue struct {
	admission *OpenAIAccountRequirementAdmission

	mu        sync.RWMutex
	finalized *openAIFinalizedCredential
}

func newOpenAICredentialProof(
	admission *OpenAIAccountRequirementAdmission,
	authMode string,
	tokenHash [sha256.Size]byte,
	hasToken bool,
) (*OpenAICredentialProof, error) {
	if admission == nil || admission.Selected == nil || admission.EffectiveCredentialOwner == nil {
		return nil, NewOpenAISecurityCredentialFailoverError(nil, "credential proof snapshot unavailable")
	}
	owner := admission.EffectiveCredentialOwner
	proof := &OpenAICredentialProof{
		selectedAccountID:       admission.Selected.ID,
		selectedAccountPlatform: admission.Selected.Platform,
		selectedAccountType:     admission.Selected.Type,
		effectiveOwnerID:        owner.ID,
		effectiveOwnerPlatform:  owner.Platform,
		effectiveOwnerType:      owner.Type,
		accountClass:            admission.AccountClass,
		authMode:                authMode,
		hasToken:                hasToken,
	}
	if authMode == OpenAIAuthModeAgentIdentity {
		if hasToken {
			return nil, NewOpenAISecurityCredentialFailoverError(admission.Selected, "unexpected agent identity bearer token")
		}
		identityHash, err := agentIdentityCredentialMaterialDigest(owner)
		if err != nil {
			return nil, NewOpenAISecurityCredentialFailoverError(admission.Selected, "agent identity credential material unavailable")
		}
		proof.agentIdentityHash = identityHash
		proof.hasAgentIdentityHash = true
		return proof, nil
	}
	if !hasToken || owner.IsOpenAIAgentIdentity() {
		return nil, NewOpenAISecurityCredentialFailoverError(admission.Selected, "bearer credential proof unavailable")
	}
	proof.tokenVersion = owner.GetCredentialAsInt64("_token_version")
	proof.tokenHash = tokenHash
	return proof, nil
}

// openAIWSFinalizedCredentialProofFromContext projects the immutable terminal
// credential into the hash-only identity used by the WS pool. Legacy callers
// without terminal admission remain in the unbound pool partition.
func openAIWSFinalizedCredentialProofFromContext(ctx context.Context, selected *Account) (*OpenAICredentialProof, error) {
	if OpenAIAccountTerminalAdmissionFromContext(ctx) == nil {
		return nil, nil
	}
	credential, err := openAIFinalizedCredentialFromContext(ctx, selected)
	if err != nil {
		return nil, err
	}
	if credential == nil || credential.admission == nil || credential.admission.Selected == nil ||
		credential.admission.EffectiveCredentialOwner == nil {
		return nil, NewOpenAISecurityCredentialFailoverError(selected, "finalized websocket credential unavailable")
	}
	return newOpenAICredentialProof(
		credential.admission,
		credential.authMode,
		credential.tokenHash,
		credential.hasToken,
	)
}

// WithOpenAIAccountTerminalAdmission binds the fresh terminal snapshot to the
// request. Credential acquisition uses it to detect a plan/owner change after
// OAuth refresh before any upstream request is constructed.
func WithOpenAIAccountTerminalAdmission(ctx context.Context, admission *OpenAIAccountRequirementAdmission) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, openAITerminalAdmissionContextKey{}, &openAITerminalAdmissionContextValue{
		admission: cloneOpenAIAccountRequirementAdmission(admission),
	})
}

func OpenAIAccountTerminalAdmissionFromContext(ctx context.Context) *OpenAIAccountRequirementAdmission {
	if ctx == nil {
		return nil
	}
	switch value := ctx.Value(openAITerminalAdmissionContextKey{}).(type) {
	case *openAITerminalAdmissionContextValue:
		if value == nil {
			return nil
		}
		return value.admission
	case *OpenAIAccountRequirementAdmission:
		// Keep compatibility with request contexts created by older in-package
		// callers while rolling out the wrapped finalized state.
		return value
	default:
		return nil
	}
}

func openAITerminalAdmissionState(ctx context.Context) *openAITerminalAdmissionContextValue {
	if ctx == nil {
		return nil
	}
	state, _ := ctx.Value(openAITerminalAdmissionContextKey{}).(*openAITerminalAdmissionContextValue)
	return state
}

func cloneOpenAICredentialAccountSnapshot(account *Account) *Account {
	if account == nil {
		return nil
	}
	clone := *account
	clone.Credentials = shallowCopyMap(account.Credentials)
	clone.Extra = shallowCopyMap(account.Extra)
	if account.ParentAccountID != nil {
		parentID := *account.ParentAccountID
		clone.ParentAccountID = &parentID
	}
	return &clone
}

func cloneOpenAIAccountRequirementAdmission(admission *OpenAIAccountRequirementAdmission) *OpenAIAccountRequirementAdmission {
	if admission == nil {
		return nil
	}
	clone := *admission
	clone.Selected = cloneOpenAICredentialAccountSnapshot(admission.Selected)
	clone.EffectiveCredentialOwner = cloneOpenAICredentialAccountSnapshot(admission.EffectiveCredentialOwner)
	return &clone
}

func clearOpenAIFinalizedCredential(ctx context.Context) {
	state := openAITerminalAdmissionState(ctx)
	if state == nil {
		return
	}
	state.mu.Lock()
	state.finalized = nil
	state.mu.Unlock()
}

func setOpenAIFinalizedCredential(
	ctx context.Context,
	admission *OpenAIAccountRequirementAdmission,
	token string,
	authMode string,
) error {
	state := openAITerminalAdmissionState(ctx)
	if state == nil {
		if OpenAIAccountTerminalAdmissionFromContext(ctx) == nil {
			return nil
		}
		return NewOpenAISecurityCredentialFailoverError(nil, "finalized credential state unavailable")
	}
	snapshot := cloneOpenAIAccountRequirementAdmission(admission)
	if snapshot == nil || snapshot.Selected == nil || snapshot.EffectiveCredentialOwner == nil {
		return NewOpenAISecurityCredentialFailoverError(nil, "finalized credential snapshot unavailable")
	}
	credential := &openAIFinalizedCredential{
		admission: snapshot,
		authMode:  authMode,
		hasToken:  strings.TrimSpace(token) != "",
	}
	if authMode == OpenAIAuthModeAgentIdentity {
		if credential.hasToken || !snapshot.EffectiveCredentialOwner.IsOpenAIAgentIdentity() {
			return NewOpenAISecurityCredentialFailoverError(snapshot.Selected, "agent identity mode changed")
		}
	} else {
		if !credential.hasToken || snapshot.EffectiveCredentialOwner.IsOpenAIAgentIdentity() {
			return NewOpenAISecurityCredentialFailoverError(snapshot.Selected, "bearer credential mode changed")
		}
		credential.tokenHash = sha256.Sum256([]byte(token))
	}
	state.mu.Lock()
	state.finalized = credential
	state.mu.Unlock()
	return nil
}

func openAIFinalizedCredentialFromContext(
	ctx context.Context,
	selected *Account,
) (*openAIFinalizedCredential, error) {
	terminal := OpenAIAccountTerminalAdmissionFromContext(ctx)
	if terminal == nil {
		return nil, nil
	}
	state := openAITerminalAdmissionState(ctx)
	if state == nil {
		return nil, NewOpenAISecurityCredentialFailoverError(selected, "finalized credential state unavailable")
	}
	state.mu.RLock()
	credential := state.finalized
	state.mu.RUnlock()
	if credential == nil || credential.admission == nil || credential.admission.Selected == nil ||
		credential.admission.EffectiveCredentialOwner == nil {
		return nil, NewOpenAISecurityCredentialFailoverError(selected, "finalized credential snapshot unavailable")
	}
	if selected == nil || selected.ID != credential.admission.Selected.ID {
		return nil, NewOpenAISecurityCredentialFailoverError(selected, "finalized selected account changed")
	}
	return credential, nil
}

func openAIFinalizedCredentialForAuthentication(
	ctx context.Context,
	selected *Account,
	token string,
) (*openAIFinalizedCredential, error) {
	credential, err := openAIFinalizedCredentialFromContext(ctx, selected)
	if err != nil || credential == nil {
		return credential, err
	}
	if credential.authMode == OpenAIAuthModeAgentIdentity {
		if strings.TrimSpace(token) != "" {
			return nil, NewOpenAISecurityCredentialFailoverError(selected, "unexpected bearer token for agent identity")
		}
		return credential, nil
	}
	if !credential.hasToken || strings.TrimSpace(token) == "" {
		return nil, NewOpenAISecurityCredentialFailoverError(selected, "finalized bearer token unavailable")
	}
	currentHash := sha256.Sum256([]byte(token))
	if subtle.ConstantTimeCompare(credential.tokenHash[:], currentHash[:]) != 1 {
		return nil, NewOpenAISecurityCredentialFailoverError(selected, "credential token changed after final admission")
	}
	return credential, nil
}

// NewOpenAISecurityCredentialFailoverError is returned before upstream
// dispatch when the refreshed effective owner no longer matches the terminal
// snapshot. It deliberately behaves as an account-scoped credential failover.
func NewOpenAISecurityCredentialFailoverError(account *Account, reason string) *UpstreamFailoverError {
	accountID := int64(0)
	if account != nil {
		accountID = account.ID
	}
	return &UpstreamFailoverError{
		StatusCode:        503,
		Stage:             GatewayFailureStageAccountAuth,
		Scope:             GatewayFailureScopeAccount,
		Reason:            GatewayFailureReason("security_credential_owner_changed"),
		NextAccountAction: NextAccountRetry,
		ClientStatusCode:  http.StatusServiceUnavailable,
		ClientMessage:     fmt.Sprintf("security credential admission changed for account %d: %s", accountID, strings.TrimSpace(reason)),
	}
}

// WithOpenAIAccountRequirement installs the request-local account hard
// constraint used by every OpenAI scheduler path. The constraint is monotonic:
// once any classification or final-model check requires an audit-exempt
// account, a stale caller cannot downgrade the request back to Any.
func WithOpenAIAccountRequirement(ctx context.Context, requirement securityadmission.AccountRequirement) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	requirement = normalizeOpenAIAccountRequirement(requirement)
	if current, ok := ctx.Value(openAIAccountRequirementContextKey{}).(*openAIAccountRequirementContextValue); ok && current != nil {
		current.mu.Lock()
		current.requirement = mergeOpenAIAccountRequirements(current.requirement, requirement)
		if current.parents == nil {
			current.parents = make(map[int64]*Account)
		}
		current.mu.Unlock()
		return ctx
	}
	return context.WithValue(ctx, openAIAccountRequirementContextKey{}, &openAIAccountRequirementContextValue{
		requirement: requirement,
		parents:     make(map[int64]*Account),
	})
}

func normalizeOpenAIAccountRequirement(requirement securityadmission.AccountRequirement) securityadmission.AccountRequirement {
	if requirement == "" {
		return securityadmission.AccountRequirementAny
	}
	return requirement
}

func mergeOpenAIAccountRequirements(
	left securityadmission.AccountRequirement,
	right securityadmission.AccountRequirement,
) securityadmission.AccountRequirement {
	left = normalizeOpenAIAccountRequirement(left)
	right = normalizeOpenAIAccountRequirement(right)
	if left == securityadmission.AccountRequirementAuditExempt || right == securityadmission.AccountRequirementAuditExempt {
		return securityadmission.AccountRequirementAuditExempt
	}
	if left != securityadmission.AccountRequirementAny {
		return left
	}
	return right
}

// OpenAIAccountRequirementFromContext returns the request-local hard
// constraint. Requests that do not opt in retain the existing scheduler
// universe.
func OpenAIAccountRequirementFromContext(ctx context.Context) securityadmission.AccountRequirement {
	if ctx == nil {
		return securityadmission.AccountRequirementAny
	}
	if value, ok := ctx.Value(openAIAccountRequirementContextKey{}).(*openAIAccountRequirementContextValue); ok && value != nil {
		value.mu.RLock()
		requirement := normalizeOpenAIAccountRequirement(value.requirement)
		value.mu.RUnlock()
		return requirement
	}
	return securityadmission.AccountRequirementAny
}

func openAIAccountRequirementState(ctx context.Context) *openAIAccountRequirementContextValue {
	if ctx == nil {
		return nil
	}
	value, _ := ctx.Value(openAIAccountRequirementContextKey{}).(*openAIAccountRequirementContextValue)
	return value
}

func (s *OpenAIGatewayService) lookupOpenAIRequirementParent(ctx context.Context, parentID int64) *Account {
	if parentID <= 0 || s == nil || s.accountRepo == nil {
		return nil
	}
	state := openAIAccountRequirementState(ctx)
	if state == nil {
		parent, _ := s.accountRepo.GetByID(ctx, parentID)
		return parent
	}
	state.mu.Lock()
	parent, resolved := state.parents[parentID]
	state.mu.Unlock()
	if resolved {
		return parent
	}

	parent, _ = s.accountRepo.GetByID(ctx, parentID)
	state.mu.Lock()
	if cached, exists := state.parents[parentID]; exists {
		parent = cached
	} else {
		state.parents[parentID] = parent
	}
	state.mu.Unlock()
	return parent
}

func cacheOpenAIRequirementParent(ctx context.Context, parentID int64, parent *Account) {
	state := openAIAccountRequirementState(ctx)
	if state == nil || parentID <= 0 {
		return
	}
	state.mu.Lock()
	state.parents[parentID] = parent
	state.mu.Unlock()
}

// preloadOpenAIRequirementParents resolves all direct shadow parents needed by
// an audit-exempt candidate pass in one batch. Both hits and misses are cached
// so classification never falls back to one repository read per candidate.
// Terminal admission intentionally bypasses this cache and reloads the selected
// row and its effective owner from the repository.
func (s *OpenAIGatewayService) preloadOpenAIRequirementParents(ctx context.Context, accounts []Account) {
	if s == nil {
		return
	}
	if OpenAIAccountRequirementFromContext(ctx) != securityadmission.AccountRequirementAuditExempt {
		for i := range accounts {
			if openAIAccountModelRequiresAuditExempt(ctx, &accounts[i]) {
				// Select installs a request-local pointer before candidate loading, so
				// this updates all derived contexts and every later failover attempt.
				_ = WithOpenAIAccountRequirement(ctx, securityadmission.AccountRequirementAuditExempt)
				break
			}
		}
	}
	if OpenAIAccountRequirementFromContext(ctx) != securityadmission.AccountRequirementAuditExempt {
		return
	}
	state := openAIAccountRequirementState(ctx)
	if state == nil {
		return
	}

	var parentIDs []int64
	var seen map[int64]struct{}
	state.mu.Lock()
	for i := range accounts {
		parentID := accounts[i].ParentAccountID
		if parentID == nil || *parentID <= 0 {
			continue
		}
		if _, resolved := state.parents[*parentID]; resolved {
			continue
		}
		if seen == nil {
			seen = make(map[int64]struct{})
			parentIDs = make([]int64, 0, len(accounts))
		}
		if _, duplicate := seen[*parentID]; duplicate {
			continue
		}
		seen[*parentID] = struct{}{}
		parentIDs = append(parentIDs, *parentID)
	}
	state.mu.Unlock()
	if len(parentIDs) == 0 {
		return
	}

	parentsByID := make(map[int64]*Account, len(parentIDs))
	if s.schedulerSnapshot != nil {
		// GetAccountMetadataByIDs can return cache hits together with a DB
		// fallback error. Keep those verified hits and cache every unresolved ID
		// as nil so candidate classification cannot degrade into per-parent reads.
		parentsByID, _ = s.schedulerSnapshot.GetAccountMetadataByIDs(ctx, parentIDs)
	} else if s.accountRepo != nil {
		loaded, _ := s.accountRepo.GetByIDs(ctx, parentIDs)
		for _, parent := range loaded {
			if parent != nil {
				parentsByID[parent.ID] = parent
			}
		}
	}

	state.mu.Lock()
	defer state.mu.Unlock()
	for _, parentID := range parentIDs {
		if _, resolved := state.parents[parentID]; resolved {
			continue
		}
		parent := parentsByID[parentID]
		if parent != nil && parent.ID != parentID {
			parent = nil
		}
		state.parents[parentID] = parent
	}
}

// ClassifyOpenAIEffectiveCredentialOwner is the pure in-memory classification
// used after token refresh. The caller must pass the actual credential owner,
// not a shadow scheduling row.
func ClassifyOpenAIEffectiveCredentialOwner(owner *Account) securityadmission.AccountClass {
	if owner == nil || owner.IsShadow() {
		return securityadmission.AccountUnknown
	}
	switch owner.Type {
	case AccountTypeAPIKey, AccountTypeSetupToken, AccountTypeUpstream, AccountTypeBedrock:
		return securityadmission.AccountAuditExemptVerified
	case AccountTypeOAuth:
		if owner.Platform != PlatformOpenAI {
			switch owner.Platform {
			case PlatformAnthropic, PlatformGemini, PlatformAntigravity, PlatformGrok,
				PlatformKimi, PlatformZhipu, PlatformDeepseek:
				return securityadmission.AccountAuditExemptVerified
			default:
				return securityadmission.AccountUnknown
			}
		}
		switch strings.ToLower(strings.TrimSpace(owner.GetCredential("plan_type"))) {
		case "pro":
			return securityadmission.AccountAuditRequired
		case "free", "plus", "team", "business", "self_serve_business", "self_serve_business_usage_based":
			return securityadmission.AccountAuditExemptVerified
		default:
			return securityadmission.AccountUnknown
		}
	default:
		return securityadmission.AccountUnknown
	}
}

// ClassifyOpenAIAccountAuditClass resolves a scheduling shadow through the
// request-local cache, then classifies its effective credential owner.
func (s *OpenAIGatewayService) ClassifyOpenAIAccountAuditClass(ctx context.Context, account *Account) securityadmission.AccountClass {
	owner := account
	if owner == nil {
		return securityadmission.AccountUnknown
	}
	if owner.IsShadow() {
		if owner.ParentAccountID == nil {
			return securityadmission.AccountUnknown
		}
		owner = s.lookupOpenAIRequirementParent(ctx, *owner.ParentAccountID)
		if owner == nil || owner.IsShadow() {
			return securityadmission.AccountUnknown
		}
	}
	return ClassifyOpenAIEffectiveCredentialOwner(owner)
}

func openAIAccountClassSatisfiesRequirement(class securityadmission.AccountClass, requirement securityadmission.AccountRequirement) bool {
	switch requirement {
	case "", securityadmission.AccountRequirementAny:
		return true
	case securityadmission.AccountRequirementAuditExempt:
		return class == securityadmission.AccountAuditExemptVerified
	default:
		return false
	}
}

func openAIAccountModelRequiresAuditExempt(ctx context.Context, account *Account) bool {
	forwardModel, ok := openAIForwardModelFromContext(ctx)
	if !ok {
		return false
	}
	if securityadmission.ModelImpliesRemoteSearch(forwardModel.model) {
		return true
	}

	var mappedModel string
	switch forwardModel.resolution {
	case openAIForwardModelResolutionDirect:
		mappedModel = resolveOpenAIForwardModel(account, forwardModel.model, "")
	case openAIForwardModelResolutionMessages:
		model := NormalizeOpenAICompatRequestedModel(forwardModel.model)
		mappedModel = resolveOpenAIForwardModel(account, model, forwardModel.defaultMappedModel)
	default:
		// Responses passthrough deliberately ignores normal account mapping.
		// Every other Responses path applies it before optional compact mapping
		// and OAuth normalization.
		mappedModel = forwardModel.model
		if account == nil || !account.IsOpenAIPassthroughEnabled() {
			mappedModel = resolveOpenAIForwardModel(account, mappedModel, "")
		}
		if forwardModel.useCompactModelMapping {
			mappedModel = resolveOpenAICompactForwardModel(account, mappedModel)
		}
	}
	if securityadmission.ModelImpliesRemoteSearch(mappedModel) {
		return true
	}
	upstreamModel, resolved := resolveOpenAISecurityUpstreamModel(ctx, account)
	return resolved && securityadmission.ModelImpliesRemoteSearch(upstreamModel)
}

func effectiveOpenAIAccountRequirement(
	ctx context.Context,
	account *Account,
	requirement securityadmission.AccountRequirement,
) securityadmission.AccountRequirement {
	requirement = mergeOpenAIAccountRequirements(requirement, OpenAIAccountRequirementFromContext(ctx))
	if !openAIAccountModelRequiresAuditExempt(ctx, account) {
		return requirement
	}
	_ = WithOpenAIAccountRequirement(ctx, securityadmission.AccountRequirementAuditExempt)
	return securityadmission.AccountRequirementAuditExempt
}

func (s *OpenAIGatewayService) openAIAccountRequirementCompatible(
	ctx context.Context,
	account *Account,
	requirement securityadmission.AccountRequirement,
) (bool, string) {
	requirement = effectiveOpenAIAccountRequirement(ctx, account, requirement)
	if requirement == securityadmission.AccountRequirementAny {
		return true, ""
	}
	if requirement != securityadmission.AccountRequirementAuditExempt {
		return false, "security_unknown_account_requirement"
	}
	class := s.ClassifyOpenAIAccountAuditClass(ctx, account)
	if class == securityadmission.AccountAuditExemptVerified {
		return true, ""
	}
	return false, "security_" + string(class)
}

// AdmitOpenAIAccountRequirement performs the terminal, fresh account check.
// Unlike candidate filtering, it always reloads the selected row and, for a
// shadow, its effective credential owner. This is the authoritative boundary
// immediately before audit/token-based dispatch.
func (s *OpenAIGatewayService) AdmitOpenAIAccountRequirement(
	ctx context.Context,
	selected *Account,
) (*OpenAIAccountRequirementAdmission, error) {
	if selected == nil || selected.ID <= 0 || s == nil || s.accountRepo == nil {
		return nil, ErrOpenAIAccountAdmissionUnavailable
	}

	latest, err := s.accountRepo.GetByID(ctx, selected.ID)
	if err != nil || latest == nil {
		return nil, fmt.Errorf("%w: reload selected account %d", ErrOpenAIAccountAdmissionUnavailable, selected.ID)
	}
	if !latest.IsSchedulable() {
		return nil, fmt.Errorf("%w: selected account %d is not schedulable", ErrOpenAIAccountRequirementIncompatible, latest.ID)
	}
	requirement := effectiveOpenAIAccountRequirement(ctx, latest, "")
	owner := latest
	if latest.IsShadow() {
		if latest.ParentAccountID == nil || *latest.ParentAccountID <= 0 {
			return nil, fmt.Errorf("%w: selected shadow %d has no parent", ErrOpenAIAccountAdmissionUnavailable, latest.ID)
		}
		owner, err = s.accountRepo.GetByID(ctx, *latest.ParentAccountID)
		if err != nil || owner == nil || owner.IsShadow() {
			return nil, fmt.Errorf("%w: reload credential owner for shadow %d", ErrOpenAIAccountAdmissionUnavailable, latest.ID)
		}
		if !owner.IsCredentialUsableForShadow() {
			return nil, fmt.Errorf("%w: credential owner %d is not usable", ErrOpenAIAccountRequirementIncompatible, owner.ID)
		}
		cacheOpenAIRequirementParent(ctx, *latest.ParentAccountID, owner)
	}

	class := ClassifyOpenAIEffectiveCredentialOwner(owner)
	admission := &OpenAIAccountRequirementAdmission{
		Selected:                 latest,
		EffectiveCredentialOwner: owner,
		Requirement:              requirement,
		AccountClass:             class,
	}
	if !openAIAccountClassSatisfiesRequirement(class, requirement) {
		return admission, fmt.Errorf("%w: selected_account_id=%d owner_account_id=%d account_class=%s",
			ErrOpenAIAccountRequirementIncompatible, latest.ID, owner.ID, class)
	}
	return admission, nil
}

// admitOpenAIPreviousResponseAccount performs the security-only part of
// previous_response_id resolution. A previous-response binding is allowed to
// fall through to ordinary scheduling only when the request explicitly permits
// migration. Therefore an active audit-exempt requirement must establish the
// bound row and its effective owner before the resolver evaluates transient
// schedulability (quota, rate-limit, capability, and similar state).
//
// The nil account result means that the request has no security requirement;
// callers can retain the historical resolver path without an extra DB read.
func (s *OpenAIGatewayService) admitOpenAIPreviousResponseAccount(
	ctx context.Context,
	accountID int64,
) (*Account, error) {
	if OpenAIAccountRequirementFromContext(ctx) != securityadmission.AccountRequirementAuditExempt {
		return nil, nil
	}
	if accountID <= 0 {
		return nil, ErrOpenAIAccountRequirementIncompatible
	}
	admission, err := s.AdmitOpenAIAccountRequirement(ctx, &Account{ID: accountID})
	if err != nil || admission == nil || admission.Selected == nil {
		// Do not expose whether the row disappeared, changed plan, or could not
		// be reloaded. All are security failures for a non-movable binding; the
		// scheduler maps this sentinel to a controlled no-available response.
		return nil, ErrOpenAIAccountRequirementIncompatible
	}
	return admission.Selected, nil
}

func (s *OpenAIGatewayService) validateOpenAISecurityCredentialAfterRefresh(
	ctx context.Context,
	selected *Account,
	effectiveOwner *Account,
) error {
	terminal := OpenAIAccountTerminalAdmissionFromContext(ctx)
	if terminal == nil {
		return nil
	}
	if effectiveOwner == nil && selected != nil && selected.IsShadow() && selected.ParentAccountID != nil {
		effectiveOwner = s.lookupOpenAIRequirementParent(ctx, *selected.ParentAccountID)
	}
	if effectiveOwner == nil {
		return NewOpenAISecurityCredentialFailoverError(selected, "effective credential owner unavailable")
	}
	if terminal.EffectiveCredentialOwner != nil && terminal.EffectiveCredentialOwner.ID > 0 &&
		effectiveOwner.ID != terminal.EffectiveCredentialOwner.ID {
		return NewOpenAISecurityCredentialFailoverError(selected, "effective credential owner changed")
	}
	class := ClassifyOpenAIEffectiveCredentialOwner(effectiveOwner)
	if class != terminal.AccountClass || !openAIAccountClassSatisfiesRequirement(class, terminal.Requirement) {
		return NewOpenAISecurityCredentialFailoverError(selected,
			fmt.Sprintf("account_class=%s expected=%s requirement=%s", class, terminal.AccountClass, terminal.Requirement))
	}
	return nil
}

// finalizeOpenAISecurityCredential is the final DB boundary between token
// acquisition and upstream dispatch. It catches selected-row, shadow-parent,
// plan, and token changes that occur after scheduler terminal admission.
func (s *OpenAIGatewayService) finalizeOpenAISecurityCredential(
	ctx context.Context,
	selected *Account,
	effectiveOwner *Account,
	token string,
	authMode string,
) (*OpenAIAccountRequirementAdmission, error) {
	terminal := OpenAIAccountTerminalAdmissionFromContext(ctx)
	if terminal == nil {
		return nil, nil
	}
	if selected == nil || terminal.Selected == nil || selected.ID != terminal.Selected.ID {
		return nil, NewOpenAISecurityCredentialFailoverError(selected, "selected account changed")
	}
	if effectiveOwner != nil {
		if err := s.validateOpenAISecurityCredentialAfterRefresh(ctx, selected, effectiveOwner); err != nil {
			return nil, err
		}
	}

	fresh, err := s.AdmitOpenAIAccountRequirement(ctx, selected)
	if err != nil || fresh == nil || fresh.Selected == nil || fresh.EffectiveCredentialOwner == nil {
		return nil, NewOpenAISecurityCredentialFailoverError(selected, "fresh credential admission unavailable")
	}
	if !sameOpenAISelectedCredentialBinding(terminal.Selected, fresh.Selected) {
		return nil, NewOpenAISecurityCredentialFailoverError(selected, "selected credential binding changed")
	}
	if err := s.validateOpenAISecurityCredentialAfterRefresh(ctx, fresh.Selected, fresh.EffectiveCredentialOwner); err != nil {
		return nil, err
	}
	if authMode != OpenAIAuthModeAgentIdentity {
		expected := openAICredentialTokenForProof(fresh.EffectiveCredentialOwner, authMode)
		if !secureOpenAITokenEqual(token, expected) {
			return nil, NewOpenAISecurityCredentialFailoverError(selected, "credential token changed")
		}
	}
	return fresh, nil
}

// CaptureOpenAICredentialProof establishes the immutable handshake proof used
// by Responses WebSocket turns. Agent Identity uses a digest of stable signing
// and tenant material because imported credentials may not have a version.
func (s *OpenAIGatewayService) CaptureOpenAICredentialProof(
	ctx context.Context,
	selected *Account,
	token string,
	authMode string,
) (*OpenAICredentialProof, error) {
	// Capture is the last handshake boundary. Revoke the token-acquisition
	// permit first so every proof failure leaves no reusable dispatch snapshot.
	clearOpenAIFinalizedCredential(ctx)
	terminal := OpenAIAccountTerminalAdmissionFromContext(ctx)
	if terminal == nil || terminal.EffectiveCredentialOwner == nil {
		return nil, NewOpenAISecurityCredentialFailoverError(selected, "terminal credential admission unavailable")
	}
	fresh, err := s.finalizeOpenAISecurityCredential(ctx, selected, terminal.EffectiveCredentialOwner, token, authMode)
	if err != nil {
		return nil, err
	}
	hasToken := strings.TrimSpace(token) != ""
	var tokenHash [sha256.Size]byte
	if hasToken {
		tokenHash = sha256.Sum256([]byte(token))
	}
	proof, err := newOpenAICredentialProof(fresh, authMode, tokenHash, hasToken)
	if err != nil {
		return nil, err
	}
	// Capture is a later authoritative DB boundary than token acquisition.
	// Publish only after the proof is complete, keeping subsequent HTTP/WS
	// header construction on this same fresh snapshot.
	if err := setOpenAIFinalizedCredential(ctx, fresh, token, authMode); err != nil {
		return nil, err
	}
	return proof, nil
}

// ValidateOpenAICredentialProof compares a fresh per-turn admission with the
// connection proof before a response.create payload can be written upstream.
func ValidateOpenAICredentialProof(admission *OpenAIAccountRequirementAdmission, proof *OpenAICredentialProof) error {
	if admission == nil || admission.Selected == nil || admission.EffectiveCredentialOwner == nil || proof == nil {
		return NewOpenAISecurityCredentialFailoverError(nil, "websocket credential proof unavailable")
	}
	owner := admission.EffectiveCredentialOwner
	if admission.Selected.ID != proof.selectedAccountID ||
		admission.Selected.Platform != proof.selectedAccountPlatform ||
		admission.Selected.Type != proof.selectedAccountType ||
		owner.ID != proof.effectiveOwnerID ||
		owner.Platform != proof.effectiveOwnerPlatform ||
		owner.Type != proof.effectiveOwnerType ||
		admission.AccountClass != proof.accountClass {
		return NewOpenAISecurityCredentialFailoverError(admission.Selected, "websocket credential identity changed")
	}
	if proof.hasToken {
		if proof.hasAgentIdentityHash || owner.GetCredentialAsInt64("_token_version") != proof.tokenVersion {
			return NewOpenAISecurityCredentialFailoverError(admission.Selected, "websocket credential identity changed")
		}
		currentToken := openAICredentialTokenForProof(owner, proof.authMode)
		currentHash := sha256.Sum256([]byte(currentToken))
		if strings.TrimSpace(currentToken) == "" || subtle.ConstantTimeCompare(proof.tokenHash[:], currentHash[:]) != 1 {
			return NewOpenAISecurityCredentialFailoverError(admission.Selected, "websocket credential token changed")
		}
		return nil
	}
	if proof.authMode != OpenAIAuthModeAgentIdentity || !proof.hasAgentIdentityHash {
		return NewOpenAISecurityCredentialFailoverError(admission.Selected, "websocket credential proof is unverifiable")
	}
	currentHash, err := agentIdentityCredentialMaterialDigest(owner)
	if err != nil || subtle.ConstantTimeCompare(proof.agentIdentityHash[:], currentHash[:]) != 1 {
		return NewOpenAISecurityCredentialFailoverError(admission.Selected, "websocket agent identity credential changed")
	}
	return nil
}

func sameOpenAISelectedCredentialBinding(before, after *Account) bool {
	if before == nil || after == nil || before.ID != after.ID || before.Platform != after.Platform || before.Type != after.Type {
		return false
	}
	if before.ParentAccountID == nil || after.ParentAccountID == nil {
		return before.ParentAccountID == nil && after.ParentAccountID == nil
	}
	return *before.ParentAccountID == *after.ParentAccountID
}

func openAICredentialTokenForProof(owner *Account, authMode string) string {
	if owner == nil {
		return ""
	}
	switch authMode {
	case "oauth":
		if owner.Platform == PlatformGrok {
			return owner.GetGrokAccessToken()
		}
		return owner.GetOpenAIAccessToken()
	case "apikey":
		if owner.Platform == PlatformGrok {
			return strings.TrimSpace(owner.GetCredential("api_key"))
		}
		return strings.TrimSpace(owner.GetOpenAIProtocolAPIKey())
	default:
		return ""
	}
}
