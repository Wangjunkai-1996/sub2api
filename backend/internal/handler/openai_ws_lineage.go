package handler

import (
	"strings"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type openAIWSTurnLineageCandidate struct {
	selectedAccountID int64
	effectiveOwnerID  int64
	accountClass      securityadmission.AccountClass
	eligible          bool
}

type openAIWSLineageProof struct {
	responseID        string
	selectedAccountID int64
	effectiveOwnerID  int64
	accountClass      securityadmission.AccountClass
}

// openAIWSLineageTracker is connection-local. It never treats a remote
// previous_response_id as trusted: a proof is created only from a normally
// completed turn that passed the terminal credential boundary on this
// connection.
type openAIWSLineageTracker struct {
	mu       sync.Mutex
	pending  map[int]openAIWSTurnLineageCandidate
	proof    openAIWSLineageProof
	hasProof bool
}

func newOpenAIWSLineageTracker() *openAIWSLineageTracker {
	return &openAIWSLineageTracker{pending: make(map[int]openAIWSTurnLineageCandidate, 4)}
}

// MarkTurnAdmitted records that a turn crossed terminal credential admission.
// Auditable text can establish lineage only when a selected Pro account
// actually completed its blocking scan. Explicit canonical no-text turns are
// independently sufficient because there is no client text to carry forward.
func (t *openAIWSLineageTracker) MarkTurnAdmitted(
	turn int,
	state *openAISecurityAdmissionState,
	accountAdmission *service.OpenAIAccountRequirementAdmission,
	blockingScanPassed bool,
) {
	if t == nil || turn <= 0 {
		return
	}
	candidate := openAIWSTurnLineageCandidate{}
	if state != nil && accountAdmission != nil && accountAdmission.Selected != nil &&
		accountAdmission.EffectiveCredentialOwner != nil {
		candidate.selectedAccountID = accountAdmission.Selected.ID
		candidate.effectiveOwnerID = accountAdmission.EffectiveCredentialOwner.ID
		candidate.accountClass = accountAdmission.AccountClass
		switch state.admission.Class() {
		case securityadmission.RequestKnownNoText:
			candidate.eligible = true
		case securityadmission.RequestAuditableText:
			candidate.eligible = accountAdmission.AccountClass == securityadmission.AccountAuditRequired && blockingScanPassed
		}
	}

	t.mu.Lock()
	if candidate.eligible {
		t.pending[turn] = candidate
	} else {
		delete(t.pending, turn)
	}
	t.mu.Unlock()
}

// CompleteTurn promotes an admitted turn into lineage proof only after a
// successful terminal event with an exact Responses response ID. All other
// outcomes invalidate both the pending candidate and any older proof.
func (t *openAIWSLineageTracker) CompleteTurn(turn int, result *service.OpenAIForwardResult, turnErr error) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	candidate, exists := t.pending[turn]
	delete(t.pending, turn)
	t.clearProofLocked()
	if !exists || !candidate.eligible || turnErr != nil || result == nil {
		return
	}
	switch strings.TrimSpace(result.UpstreamTerminalEvent) {
	case "response.completed", "response.done":
	default:
		return
	}
	responseID := strings.TrimSpace(result.ResponseID)
	if responseID == "" {
		// Responses WebSocket relays historically store response.id in
		// RequestID. Prefer the dedicated field when a transport provides it.
		responseID = strings.TrimSpace(result.RequestID)
	}
	if service.ClassifyOpenAIPreviousResponseIDKind(responseID) != service.OpenAIPreviousResponseIDKindResponseID {
		return
	}
	t.proof = openAIWSLineageProof{
		responseID:        responseID,
		selectedAccountID: candidate.selectedAccountID,
		effectiveOwnerID:  candidate.effectiveOwnerID,
		accountClass:      candidate.accountClass,
	}
	t.hasProof = true
}

// ResolvePreviousResponseID returns trusted only for the immediately preceding
// local response ID and the same selected account. Classify calls it while
// parsing previous_response_id, so lineage proof does not require a second
// scan of the request body. The canonical parser still owns duplicate-key and
// shape validation.
func (t *openAIWSLineageTracker) ResolvePreviousResponseID(previousResponseID string, selectedAccountID int64) securityadmission.LineageTrust {
	if t == nil {
		return securityadmission.LineageUntrusted
	}
	previousResponseID = strings.TrimSpace(previousResponseID)
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.hasProof || previousResponseID == "" || previousResponseID != t.proof.responseID ||
		selectedAccountID <= 0 || selectedAccountID != t.proof.selectedAccountID ||
		t.proof.effectiveOwnerID <= 0 || t.proof.accountClass == "" {
		t.clearProofLocked()
		return securityadmission.LineageUntrusted
	}
	return securityadmission.LineageTrusted
}

func (t *openAIWSLineageTracker) Invalidate() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.clearProofLocked()
	clear(t.pending)
	t.mu.Unlock()
}

func (t *openAIWSLineageTracker) clearProofLocked() {
	t.proof = openAIWSLineageProof{}
	t.hasProof = false
}
