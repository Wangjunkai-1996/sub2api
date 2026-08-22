package handler

import (
	"crypto/sha256"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const securityAuditCompletedContextKey = "sub2api.security_audit.completed"
const securityAuditBlockingCompletedContextKey = "sub2api.security_audit.blocking_completed"
const securityAuditProofCompletedContextKey = "sub2api.security_audit.proof_completed"
const securityAuditWSTurnContextKey = "sub2api.security_audit.ws_turn"
const securityAuditWSDedupeContextKey = "sub2api.security_audit.ws_dedupe"
const securityAuditProofWSDedupeContextKey = "sub2api.security_audit.proof_ws_dedupe"

type securityAuditWSDedupeEntry struct {
	stage           string
	turn            int
	bodyHash        [sha256.Size]byte
	requireBlocking bool
	decision        securityaudit.Decision
}

// cachesSecurityAuditCompletion reports whether a successful audit may be
// reused for the rest of the gin request. WebSocket turns share one Context
// across many response.create frames and must be audited independently.
func cachesSecurityAuditCompletion(stage string) bool {
	switch strings.TrimSpace(stage) {
	case "", "http":
		return true
	default:
		return false
	}
}

func isSecurityAuditWebSocketStage(stage string) bool {
	switch strings.TrimSpace(stage) {
	case "first_turn", "subsequent_turn":
		return true
	default:
		return false
	}
}

func (h *GatewayHandler) checkSecurityAudit(c *gin.Context, reqLog *zap.Logger, apiKey *service.APIKey, subject middleware2.AuthSubject, protocol, model string, body []byte) *securityaudit.Decision {
	if h == nil {
		return nil
	}
	return runSecurityAudit(c, reqLog, h.securityAuditCoordinator, h.contentModerationService, apiKey, subject, protocol, model, body, "http")
}

func (h *OpenAIGatewayHandler) checkSecurityAudit(c *gin.Context, reqLog *zap.Logger, apiKey *service.APIKey, subject middleware2.AuthSubject, protocol, model string, body []byte) *securityaudit.Decision {
	if h == nil {
		return nil
	}
	return runSecurityAudit(c, reqLog, h.securityAuditCoordinator, h.contentModerationService, apiKey, subject, protocol, model, body, "http")
}

func (h *OpenAIGatewayHandler) checkSecurityAuditForSelectedOpenAIProAccount(
	c *gin.Context,
	reqLog *zap.Logger,
	apiKey *service.APIKey,
	subject middleware2.AuthSubject,
	account *service.Account,
	protocol string,
	model string,
	body []byte,
) *securityaudit.Decision {
	return h.checkSecurityAuditForSelectedOpenAIAccount(c, reqLog, apiKey, subject, account, protocol, model, body, false)
}

// checkSecurityAuditForSelectedOpenAIAccount audits the selected account when
// its effective credential owner is not already verified. The request-local
// admission is reused across failover attempts; a missing admission is
// classified here for compatibility with focused unit callers, but production
// handlers classify immediately after reading body.
func (h *OpenAIGatewayHandler) checkSecurityAuditForSelectedOpenAIAccount(
	c *gin.Context,
	reqLog *zap.Logger,
	apiKey *service.APIKey,
	subject middleware2.AuthSubject,
	account *service.Account,
	protocol string,
	model string,
	body []byte,
	forceCurrentTurn bool,
) *securityaudit.Decision {
	if h == nil || c == nil || c.Request == nil {
		return securityAuditUnavailableDecision()
	}
	// Focused callers may invoke this helper before selection. There is no
	// effective credential owner to audit in that case; the scheduler/terminal
	// admission remains responsible for rejecting a missing selected account.
	if account == nil {
		return nil
	}
	accountClass := h.selectedOpenAIAccountAuditClass(c, account)
	if accountClass == securityadmission.AccountAuditExemptVerified {
		return nil
	}
	state := openAISecurityAdmissionFromContext(c)
	if !state.matchesBody(protocol, body) {
		lineage := securityadmission.LineageUntrusted
		if state != nil {
			lineage = state.lineage
		}
		classified, err := classifyOpenAISecurityAdmission(protocol, body, lineage)
		if err != nil {
			return securityAuditUnavailableDecision()
		}
		state = classified
		installOpenAISecurityAdmission(c, state)
	}
	if state.admission.Class() == securityadmission.RequestKnownNoText {
		logOpenAISecurityAdmission(c, reqLog, state, accountClass, "known_no_text_skip")
		return nil
	}
	if state.admission.Class() != securityadmission.RequestAuditableText {
		// Resource-limited, remote/encrypted, and otherwise opaque requests remain
		// fail-closed for an audit-required account; only canonical text can enter
		// the synchronous risk-control audit path.
		logOpenAISecurityAdmission(c, reqLog, state, accountClass, "reject_uninspectable_pro")
		return securityAuditUnavailableDecision()
	}
	// The generic gateway keeps a nil coordinator as a compatibility fast path,
	// but a selected audit-required account cannot dispatch without an actual
	// scanner or legacy proof.
	if h.securityAuditCoordinator == nil {
		logOpenAISecurityAdmission(c, reqLog, state, accountClass, "reject_missing_audit_coordinator")
		return securityAuditUnavailableDecision()
	}
	logOpenAISecurityAdmission(c, reqLog, state, accountClass, "selected_account_scan")
	stage := admissionStage(forceCurrentTurn)
	if turn, ok := securityAuditWSTurn(c); ok {
		if turn <= 1 {
			stage = "first_turn"
		} else {
			stage = "subsequent_turn"
		}
	}
	decision := runSecurityAuditWithProof(c, reqLog, h.securityAuditCoordinator, h.contentModerationService,
		apiKey, subject, protocol, model, body, stage, &state.admission)
	// A Pro request must have a concrete synchronous audit proof. A successful
	// Prompt Guard allow/flag is sufficient; when Prompt Guard is unavailable,
	// the existing risk-control (legacy moderation) result is used instead, and
	// a configuration skip/out-of-scope result is not treated as proof.
	if decision != nil && decision.AllowNextStage && securityAuditDecisionHasProof(decision) {
		markSecurityAuditProof(c, stage, body, *decision)
		return decision
	}
	if decision != nil && decision.AllowNextStage && !securityAuditDecisionHasProof(decision) {
		// runSecurityAuditWithAdmission may have set an optimistic HTTP or
		// WebSocket dedupe marker before this proof check. Clear it so a
		// subsequent failover cannot reinterpret the rejected result as a cache
		// hit.
		clearSecurityAuditCompletion(c, stage)
		return securityAuditUnavailableDecision()
	}
	// Prompt Guard is an additional signal, not the only way to satisfy the
	// existing risk-control contract.  When it is unavailable/invalid, rerun
	// the same canonical admission through the configured LegacyModerationAdapter
	// before considering the request unavailable.  A missing or failed legacy
	// result still fails closed; no upstream dispatch can occur without an audit
	// decision.
	// A nil result is the successful HTTP cache hit. Only a concrete unavailable
	// decision should enter the legacy fallback path; otherwise a cached Pro
	// request would be turned into a spurious 503 on its next failover attempt.
	if decision != nil && !decision.AllowNextStage && securityAuditCanReselect(decision) {
		// Coordinator.Check already executes the legacy adapter in parallel with
		// Prompt Guard. Reuse that result when present; rerunning the adapter would
		// duplicate moderation side effects and audit records.
		if decision.Legacy != nil {
			legacyDecision := securityaudit.DecisionFromLegacy(decision.Legacy)
			if legacyDecision.AllowNextStage && securityAuditDecisionHasProof(&legacyDecision) {
				markSecurityAuditProof(c, stage, body, legacyDecision)
			}
			return &legacyDecision
		}
		legacyDecision := h.runLegacySecurityAuditForSelectedOpenAIAccount(
			c, reqLog, apiKey, subject, protocol, model, body, stage, &state.admission,
		)
		if legacyDecision != nil {
			if legacyDecision.AllowNextStage && securityAuditDecisionHasProof(legacyDecision) {
				markSecurityAuditProof(c, stage, body, *legacyDecision)
			}
			return legacyDecision
		}
	}
	return decision
}

func clearSecurityAuditCompletion(c *gin.Context, stage string) {
	if c == nil {
		return
	}
	if cachesSecurityAuditCompletion(stage) {
		c.Set(securityAuditCompletedContextKey, false)
		c.Set(securityAuditProofCompletedContextKey, false)
		return
	}
	if isSecurityAuditWebSocketStage(stage) {
		// Gin v1.9.1 does not expose Context.Delete. Set nil through the
		// lock-protected API; cache readers require a concrete dedupe entry, so
		// this is equivalent to deleting the marker without racing on c.Keys.
		c.Set(securityAuditWSDedupeContextKey, nil)
		c.Set(securityAuditProofWSDedupeContextKey, nil)
	}
}

func (h *OpenAIGatewayHandler) selectedOpenAIAccountAuditClass(c *gin.Context, account *service.Account) securityadmission.AccountClass {
	if account == nil {
		return securityadmission.AccountUnknown
	}
	if h != nil && h.gatewayService != nil && c != nil && c.Request != nil {
		return h.gatewayService.ClassifyOpenAIAccountAuditClass(c.Request.Context(), account)
	}
	return service.ClassifyOpenAIEffectiveCredentialOwner(account)
}

func (h *OpenAIGatewayHandler) selectedOpenAIAccountMayUseAuditFallback(c *gin.Context, account *service.Account) bool {
	return h.selectedOpenAIAccountAuditClass(c, account) == securityadmission.AccountAuditRequired
}

func (h *OpenAIGatewayHandler) runLegacySecurityAuditForSelectedOpenAIAccount(
	c *gin.Context,
	reqLog *zap.Logger,
	apiKey *service.APIKey,
	subject middleware2.AuthSubject,
	protocol, model string,
	body []byte,
	stage string,
	admission *securityadmission.Admission,
) *securityaudit.Decision {
	if h == nil || h.securityAuditCoordinator == nil || c == nil || c.Request == nil {
		return securityAuditUnavailableDecision()
	}
	request := buildSecurityAuditRequestForAdmission(c, apiKey, subject, protocol, model, body, stage, admission)
	request.RequireBlocking = false
	logSecurityAuditStart(reqLog, request, len(body), false)
	decision := h.securityAuditCoordinator.CheckLegacy(c.Request.Context(), request)
	logSecurityAuditDone(reqLog, request, decision, false)
	return &decision
}

// markSecurityAuditProof stores only a validated Pro-account audit proof. It
// is intentionally separate from the ordinary completion cache: a generic
// fail-open allow may be reused by regular traffic but can never satisfy the
// selected-Pro proof obligation.
func markSecurityAuditProof(
	c *gin.Context,
	stage string,
	body []byte,
	decision securityaudit.Decision,
) {
	if c == nil || !decision.AllowNextStage {
		return
	}
	if cachesSecurityAuditCompletion(stage) {
		c.Set(securityAuditProofCompletedContextKey, true)
		return
	}
	if !isSecurityAuditWebSocketStage(stage) {
		return
	}
	turn, ok := securityAuditWSTurn(c)
	if !ok {
		return
	}
	c.Set(securityAuditProofWSDedupeContextKey, securityAuditWSDedupeEntry{
		stage: stage, turn: turn, bodyHash: sha256.Sum256(body),
		requireBlocking: false, decision: decision,
	})
}

func admissionStage(currentTurn bool) string {
	if currentTurn {
		return "subsequent_turn"
	}
	return "http"
}

func securityAuditUnavailableDecision() *securityaudit.Decision {
	return &securityaudit.Decision{
		Kind: securityaudit.DecisionUnavailable, HTTPStatus: http.StatusServiceUnavailable,
		ErrorCode: securityaudit.ErrorCodeUnavailable, ClientMessage: "提示词安全审计暂时不可用，请稍后重试",
		AllowNextStage: false,
	}
}

func runSecurityAudit(c *gin.Context, reqLog *zap.Logger, coordinator *securityaudit.Coordinator, legacy *service.ContentModerationService, apiKey *service.APIKey, subject middleware2.AuthSubject, protocol, model string, body []byte, stage string) *securityaudit.Decision {
	var admission *securityadmission.Admission
	if state := openAISecurityAdmissionFromContext(c); state.matchesBody(protocol, body) {
		admission = &state.admission
	}
	return runSecurityAuditWithAdmission(c, reqLog, coordinator, legacy, apiKey, subject, protocol, model, body, stage, admission, false)
}

func runSecurityAuditWithProof(c *gin.Context, reqLog *zap.Logger, coordinator *securityaudit.Coordinator, legacy *service.ContentModerationService, apiKey *service.APIKey, subject middleware2.AuthSubject, protocol, model string, body []byte, stage string, admission *securityadmission.Admission) *securityaudit.Decision {
	return runSecurityAuditWithAdmissionOptions(c, reqLog, coordinator, legacy, apiKey, subject, protocol, model, body, stage, admission, false, true)
}

func runSecurityAuditWithAdmission(c *gin.Context, reqLog *zap.Logger, coordinator *securityaudit.Coordinator, legacy *service.ContentModerationService, apiKey *service.APIKey, subject middleware2.AuthSubject, protocol, model string, body []byte, stage string, admission *securityadmission.Admission, requireBlocking bool) *securityaudit.Decision {
	return runSecurityAuditWithAdmissionOptions(c, reqLog, coordinator, legacy, apiKey, subject, protocol, model, body, stage, admission, requireBlocking, false)
}

func runSecurityAuditWithAdmissionOptions(c *gin.Context, reqLog *zap.Logger, coordinator *securityaudit.Coordinator, legacy *service.ContentModerationService, apiKey *service.APIKey, subject middleware2.AuthSubject, protocol, model string, body []byte, stage string, admission *securityadmission.Admission, requireBlocking, requireProof bool) *securityaudit.Decision {
	if c == nil || c.Request == nil {
		return nil
	}
	// A selected Pro account has a blocking-audit proof obligation. A missing
	// coordinator means no blocking scanner exists; legacy moderation (or a
	// completion marker from an earlier non-blocking check) cannot satisfy it.
	// Keep this guard ahead of the request cache so the failure is fail-closed.
	if coordinator == nil && requireBlocking {
		request := buildSecurityAuditRequestForAdmission(c, apiKey, subject, protocol, model, body, stage, admission)
		request.RequireBlocking = true
		logSecurityAuditStart(reqLog, request, len(body), false)
		decision := securityAuditUnavailableDecision()
		logSecurityAuditDone(reqLog, request, *decision, false)
		return decision
	}
	cacheCompletion := cachesSecurityAuditCompletion(stage)
	if cacheCompletion {
		completionKey := securityAuditCompletedContextKey
		switch {
		case requireProof:
			// Pro proof is deliberately independent from ordinary request
			// completion. A previous fail-open allow must not satisfy this check.
			completionKey = securityAuditProofCompletedContextKey
		case requireBlocking:
			// A successful non-blocking/async check is not proof that the
			// blocking scanner ran for a selected Pro account.
			completionKey = securityAuditBlockingCompletedContextKey
		}
		if completed, exists := c.Get(completionKey); exists && completed == true {
			return nil
		}
	}
	if coordinator == nil {
		legacyDecision := runContentModeration(c, reqLog, legacy, apiKey, subject, protocol, model, body)
		if legacyDecision == nil {
			return nil
		}
		decision := securityaudit.Decision{Kind: securityaudit.DecisionAllow, HTTPStatus: http.StatusOK, AllowNextStage: true}
		decision.Legacy = &securityaudit.LegacyDecision{
			Allowed: legacyDecision.Allowed, Blocked: legacyDecision.Blocked, Flagged: legacyDecision.Flagged,
			Audited: legacyDecision.Audited,
			Message: legacyDecision.Message, StatusCode: legacyDecision.StatusCode,
			ErrorCode: "content_policy_violation", Action: legacyDecision.Action,
		}
		if legacyDecision.Blocked {
			decision.Kind, decision.HTTPStatus, decision.ErrorCode, decision.ClientMessage, decision.AllowNextStage = securityaudit.DecisionBlock, contentModerationStatus(legacyDecision), "content_policy_violation", legacyDecision.Message, false
		}
		if decision.AllowNextStage && cacheCompletion && !requireProof {
			c.Set(securityAuditCompletedContextKey, true)
		}
		return &decision
	}
	request := buildSecurityAuditRequestForAdmission(c, apiKey, subject, protocol, model, body, stage, admission)
	request.RequireBlocking = requireBlocking
	if isSecurityAuditWebSocketStage(request.Stage) {
		if turnNo, ok := securityAuditWSTurn(c); ok {
			bodyHash := sha256.Sum256(body)
			dedupeKey := securityAuditWSDedupeContextKey
			if requireProof {
				dedupeKey = securityAuditProofWSDedupeContextKey
			}
			if cached, exists := c.Get(dedupeKey); exists {
				if entry, ok := cached.(securityAuditWSDedupeEntry); ok &&
					entry.stage == request.Stage && entry.turn == turnNo && entry.bodyHash == bodyHash &&
					entry.requireBlocking == requireBlocking {
					decision := entry.decision
					logSecurityAuditDone(reqLog, request, decision, true)
					return &decision
				}
			}
			logSecurityAuditStart(reqLog, request, len(body), false)
			decision := coordinator.Check(c.Request.Context(), request)
			deriveKnownViolationAdmission(&request, decision)
			if decision.Kind == securityaudit.DecisionAllow && !requireProof {
				c.Set(dedupeKey, securityAuditWSDedupeEntry{
					stage: request.Stage, turn: turnNo, bodyHash: bodyHash,
					requireBlocking: requireBlocking, decision: decision,
				})
			}
			logSecurityAuditDone(reqLog, request, decision, false)
			return &decision
		}
	}
	logSecurityAuditStart(reqLog, request, len(body), false)
	decision := coordinator.Check(c.Request.Context(), request)
	deriveKnownViolationAdmission(&request, decision)
	if decision.AllowNextStage && cacheCompletion && !requireProof {
		if requireBlocking {
			c.Set(securityAuditBlockingCompletedContextKey, true)
		} else {
			c.Set(securityAuditCompletedContextKey, true)
		}
	}
	logSecurityAuditDone(reqLog, request, decision, false)
	return &decision
}

// securityAuditDecisionHasProof distinguishes a completed scanner decision
// from a fail-open "allow" used when either audit service is disabled, out of
// scope, sampled, queued asynchronously, or unavailable. AllowNextStage alone
// is never proof; the producing service must set its explicit Audited bit.
func securityAuditDecisionHasProof(decision *securityaudit.Decision) bool {
	if decision == nil || !decision.AllowNextStage {
		return false
	}
	if decision.Prompt != nil && decision.Prompt.Audited && decision.Prompt.AllowNextStage &&
		(decision.Prompt.Kind == securityaudit.DecisionAllow || decision.Prompt.Kind == securityaudit.DecisionFlag) {
		return true
	}
	return decision.Legacy != nil && (decision.Legacy.Audited || decision.Legacy.Blocked)
}

// deriveKnownViolationAdmission annotates observability with the authoritative
// synchronous block outcome. It never mutates the canonical Admission retained
// in request-local state, which may already be shared with async audit work or
// reused by routing and failover.
func deriveKnownViolationAdmission(request *securityaudit.Request, decision securityaudit.Decision) {
	if request == nil || request.Admission == nil || decision.Kind != securityaudit.DecisionBlock || decision.AllowNextStage {
		return
	}
	derived := request.Admission.WithKnownViolation(securityadmission.ReasonKnownViolation)
	request.Admission = &derived
}

func logSecurityAuditStart(reqLog *zap.Logger, request securityaudit.Request, bodyBytes int, cached bool) {
	if reqLog == nil {
		return
	}
	reqLog.Info("security_audit.gateway_check_start",
		zap.String("request_id", request.RequestID), zap.Int64("user_id", request.UserID),
		zap.Int64("account_id", request.AccountID), zap.String("account_class", request.AccountClass),
		zap.Int64("api_key_id", request.APIKeyID), zap.Int64p("group_id", request.GroupID),
		zap.String("endpoint", request.Endpoint), zap.String("provider", request.Provider),
		zap.String("protocol", request.Protocol), zap.String("model", request.Model), zap.String("stage", request.Stage),
		zap.Int("body_bytes", bodyBytes), zap.Bool("cached", cached),
		zap.Bool("require_blocking", request.RequireBlocking),
		zap.String("request_class", canonicalRequestClass(request)),
		zap.String("request_reason", canonicalRequestReason(request)),
		zap.String("reason", canonicalRequestReason(request)),
		zap.String("dispatch", securityAuditStartDispatch(cached)))
}

func logSecurityAuditDone(reqLog *zap.Logger, request securityaudit.Request, decision securityaudit.Decision, cached bool) {
	if reqLog == nil {
		return
	}
	reqLog.Info("security_audit.gateway_check_done",
		zap.String("request_id", request.RequestID), zap.String("decision", string(decision.Kind)),
		zap.Int64("account_id", request.AccountID), zap.String("account_class", request.AccountClass),
		zap.String("error_code", decision.ErrorCode), zap.Bool("allow_next_stage", decision.AllowNextStage),
		zap.String("stage", request.Stage), zap.Bool("cached", cached),
		zap.String("request_class", canonicalRequestClass(request)),
		zap.String("reason", securityAuditDecisionReason(request, decision)),
		zap.String("dispatch", securityAuditDoneDispatch(decision, cached)))
}

func securityAuditStartDispatch(cached bool) string {
	if cached {
		return "scanner_result_reuse"
	}
	return "scanner_pending"
}

func securityAuditDoneDispatch(decision securityaudit.Decision, cached bool) string {
	if cached && decision.AllowNextStage {
		return "scanner_result_reuse"
	}
	if decision.AllowNextStage {
		return "scanner_passed"
	}
	return "upstream_not_dispatched"
}

func securityAuditDecisionReason(request securityaudit.Request, decision securityaudit.Decision) string {
	if reason := strings.TrimSpace(decision.ErrorCode); reason != "" {
		return reason
	}
	return canonicalRequestReason(request)
}

func canonicalRequestClass(request securityaudit.Request) string {
	if request.Admission == nil {
		return ""
	}
	return string(request.Admission.Class())
}

func canonicalRequestReason(request securityaudit.Request) string {
	if request.Admission == nil {
		return ""
	}
	return string(request.Admission.Reason())
}

func securityAuditWSTurn(c *gin.Context) (int, bool) {
	turn, exists := c.Get(securityAuditWSTurnContextKey)
	if !exists {
		return 0, false
	}
	turnNo, ok := turn.(int)
	return turnNo, ok
}

func buildSecurityAuditRequest(c *gin.Context, apiKey *service.APIKey, subject middleware2.AuthSubject, protocol, model string, body []byte, stage string) securityaudit.Request {
	legacy := buildContentModerationInput(c, apiKey, subject, protocol, model, body)
	request := securityaudit.Request{
		RequestID: legacy.RequestID, UserID: legacy.UserID, UserEmail: legacy.UserEmail,
		APIKeyID: legacy.APIKeyID, APIKeyName: legacy.APIKeyName, GroupID: cloneSecurityAuditGroupID(legacy.GroupID),
		GroupName: legacy.GroupName, Provider: legacy.Provider, Endpoint: legacy.Endpoint,
		Protocol: legacy.Protocol, Model: legacy.Model, Body: body, Stage: strings.TrimSpace(stage),
	}
	if state := openAISecurityAdmissionFromContext(c); state.matchesBody(protocol, body) {
		request.Admission = &state.admission
	}
	if terminal := service.OpenAIAccountTerminalAdmissionFromContext(c.Request.Context()); terminal != nil {
		if terminal.Selected != nil {
			request.AccountID = terminal.Selected.ID
		}
		request.AccountClass = string(terminal.AccountClass)
	}
	if apiKey != nil && apiKey.User != nil {
		request.Username = apiKey.User.Username
		if request.UserEmail == "" {
			request.UserEmail = apiKey.User.Email
		}
	}
	if request.Stage == "" {
		request.Stage = "http"
	}
	return request
}

// buildSecurityAuditRequestForAdmission is the explicit variant used by the
// audit runner.  Passing a nil admission is intentional for a bounded fallback
// path: it asks the configured LegacyModerationAdapter/PromptService to parse
// the body itself instead of accidentally re-attaching a canonical admission
// that was already proven uninspectable.
func buildSecurityAuditRequestForAdmission(
	c *gin.Context,
	apiKey *service.APIKey,
	subject middleware2.AuthSubject,
	protocol, model string,
	body []byte,
	stage string,
	admission *securityadmission.Admission,
) securityaudit.Request {
	request := buildSecurityAuditRequest(c, apiKey, subject, protocol, model, body, stage)
	request.Admission = admission
	return request
}

func securityAuditStatus(decision *securityaudit.Decision) int {
	if decision == nil || decision.HTTPStatus < 400 || decision.HTTPStatus > 599 {
		return http.StatusForbidden
	}
	return decision.HTTPStatus
}

func securityAuditErrorCode(decision *securityaudit.Decision) string {
	if decision == nil || strings.TrimSpace(decision.ErrorCode) == "" {
		return "content_policy_violation"
	}
	return decision.ErrorCode
}

func securityAuditMessage(decision *securityaudit.Decision) string {
	if decision == nil {
		return "Request blocked by content policy"
	}
	if decision.Legacy != nil && decision.Legacy.Blocked && strings.TrimSpace(decision.Legacy.Message) != "" {
		return decision.Legacy.Message
	}
	if strings.TrimSpace(decision.ClientMessage) != "" {
		return decision.ClientMessage
	}
	return "Request blocked by content policy"
}

func cloneSecurityAuditGroupID(value *int64) *int64 {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}
