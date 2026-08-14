package handler

import (
	"net/http"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type openAIWSAccountAuditTurn struct {
	document             *auditinput.Document
	auditAttempted       bool
	auditPassed          bool
	auditTerminalFailure bool
	auditSummary         *securityaudit.AuditSummary
	auditDecision        *securityaudit.Decision
	auditState           openAIWSSecurityAuditTurnState
}

type openAIWSAccountAuditTracker struct {
	mu    sync.Mutex
	turns map[int]*openAIWSAccountAuditTurn
}

type openAIWSAccountAuditResult struct {
	Eligibility service.OpenAIAccountAuditEligibility
	Required    bool
	Attempted   bool
	Passed      bool
	Terminal    bool
	Summary     *securityaudit.AuditSummary
	Decision    *securityaudit.Decision
	TurnState   openAIWSSecurityAuditTurnState
}

func newOpenAIWSAccountAuditTracker() *openAIWSAccountAuditTracker {
	return &openAIWSAccountAuditTracker{turns: make(map[int]*openAIWSAccountAuditTurn)}
}

// ensure audits an eligible account at most once for a WS turn. An ineligible
// attempt does not mark the turn as skipped because a later failover may select
// an eligible OAuth Pro account. A terminal audit result, however, is final for
// the turn and cannot be bypassed by switching to an ineligible account.
func (t *openAIWSAccountAuditTracker) ensure(
	turn int,
	account *service.Account,
	policy service.OpenAIAccountAuditRoutingPolicy,
	document *auditinput.Document,
	imageBypass bool,
	audit func(*auditinput.Document) *securityaudit.Decision,
) openAIWSAccountAuditResult {
	if turn <= 0 {
		turn = 1
	}
	if t == nil {
		t = newOpenAIWSAccountAuditTracker()
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	state := t.turns[turn]
	if state == nil {
		state = &openAIWSAccountAuditTurn{}
		t.turns[turn] = state
	}
	if state.document == nil && document != nil {
		state.document = document.Clone()
	}
	if state.auditTerminalFailure {
		return openAIWSAccountAuditResult{
			Attempted: true,
			Terminal:  true,
			Decision:  cloneOpenAIWSAccountAuditDecision(state.auditDecision),
			TurnState: state.auditState,
		}
	}

	eligibility := service.ClassifyOpenAIAccountAuditEligibility(account, policy)
	if eligibility.Indeterminate {
		decision := openAIWSAuditUnavailableDecision()
		state.auditTerminalFailure = true
		state.auditDecision = cloneOpenAIWSAccountAuditDecision(decision)
		return openAIWSAccountAuditResult{
			Eligibility: eligibility,
			Required:    true,
			Terminal:    true,
			Decision:    cloneOpenAIWSAccountAuditDecision(decision),
			TurnState:   state.auditState,
		}
	}
	if !eligibility.Eligible {
		return openAIWSAccountAuditResult{Eligibility: eligibility, TurnState: state.auditState}
	}
	if state.auditPassed {
		return openAIWSAccountAuditResult{
			Eligibility: eligibility,
			Required:    state.auditState == openAIWSSecurityAuditTurnAudited,
			Attempted:   state.auditAttempted,
			Passed:      true,
			Summary:     cloneOpenAIWSAccountAuditSummary(state.auditSummary),
			TurnState:   state.auditState,
		}
	}
	if imageBypass {
		state.auditPassed = true
		state.auditState = openAIWSSecurityAuditTurnImageBypass
		return openAIWSAccountAuditResult{
			Eligibility: eligibility,
			Passed:      true,
			TurnState:   state.auditState,
		}
	}

	state.auditAttempted = true
	state.auditState = openAIWSSecurityAuditTurnAudited
	var decision *securityaudit.Decision
	if audit != nil {
		decision = audit(state.document.Clone())
	}
	if decision == nil || !decision.AllowNextStage || decision.Audit == nil {
		if decision == nil || decision.AllowNextStage {
			decision = openAIWSAuditUnavailableDecision()
		}
		state.auditTerminalFailure = true
		state.auditDecision = cloneOpenAIWSAccountAuditDecision(decision)
		return openAIWSAccountAuditResult{
			Eligibility: eligibility,
			Required:    true,
			Attempted:   true,
			Terminal:    true,
			Decision:    cloneOpenAIWSAccountAuditDecision(state.auditDecision),
			TurnState:   state.auditState,
		}
	}

	state.auditPassed = true
	state.auditSummary = cloneOpenAIWSAccountAuditSummary(decision.Audit)
	return openAIWSAccountAuditResult{
		Eligibility: eligibility,
		Required:    true,
		Attempted:   true,
		Passed:      true,
		Summary:     cloneOpenAIWSAccountAuditSummary(state.auditSummary),
		Decision:    cloneOpenAIWSAccountAuditDecision(decision),
		TurnState:   state.auditState,
	}
}

func (t *openAIWSAccountAuditTracker) snapshot(turn int) openAIWSAccountAuditTurn {
	if t == nil {
		return openAIWSAccountAuditTurn{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	state := t.turns[turn]
	if state == nil {
		return openAIWSAccountAuditTurn{}
	}
	cloned := *state
	cloned.document = state.document.Clone()
	cloned.auditSummary = cloneOpenAIWSAccountAuditSummary(state.auditSummary)
	cloned.auditDecision = cloneOpenAIWSAccountAuditDecision(state.auditDecision)
	return cloned
}

func (t *openAIWSAccountAuditTracker) delete(turn int) {
	if t == nil {
		return
	}
	t.mu.Lock()
	delete(t.turns, turn)
	t.mu.Unlock()
}

func cloneOpenAIWSAccountAuditSummary(summary *securityaudit.AuditSummary) *securityaudit.AuditSummary {
	if summary == nil {
		return nil
	}
	cloned := summary.Clone()
	return &cloned
}

func cloneOpenAIWSAccountAuditDecision(decision *securityaudit.Decision) *securityaudit.Decision {
	if decision == nil {
		return nil
	}
	cloned := *decision
	cloned.Audit = cloneOpenAIWSAccountAuditSummary(decision.Audit)
	return &cloned
}

func openAIWSAuditTextRunes(document *auditinput.Document) int {
	if document == nil {
		return 0
	}
	return document.AuditTextRunes
}

func openAIWSAuditUnavailableDecision() *securityaudit.Decision {
	return &securityaudit.Decision{
		Kind:          securityaudit.DecisionUnavailable,
		HTTPStatus:    http.StatusServiceUnavailable,
		ErrorCode:     securityaudit.ErrorCodeAuditUnavailable,
		ClientMessage: "安全审计暂时不可用，请稍后重试",
	}
}
