package handler

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const securityAuditCompletedContextKey = "sub2api.security_audit.completed"
const securityAuditWSTurnContextKey = "sub2api.security_audit.ws_turn"
const securityAuditWSDedupeContextKey = "sub2api.security_audit.ws_dedupe"

type securityAuditWSDedupeEntry struct {
	stage    string
	turn     int
	bodyHash [sha256.Size]byte
	decision securityaudit.Decision
}

type openAIAccountAuditState struct {
	mu       sync.Mutex
	protocol string
	model    string
	body     []byte
	stage    string
	document *auditinput.Document
	policy   service.OpenAIAccountAuditRoutingPolicy
	passed   bool
	terminal bool
	decision securityaudit.Decision
	summary  *securityaudit.AuditSummary
}

func newOpenAIAccountAuditState(
	protocol string,
	model string,
	body []byte,
	stage string,
	policy service.OpenAIAccountAuditRoutingPolicy,
) *openAIAccountAuditState {
	if strings.TrimSpace(stage) == "" {
		stage = "http"
	}
	return &openAIAccountAuditState{
		protocol: strings.TrimSpace(protocol),
		model:    strings.TrimSpace(model),
		body:     append([]byte(nil), body...),
		stage:    strings.TrimSpace(stage),
		document: auditinput.ParseForTextAudit(protocol, body),
		policy:   policy,
	}
}

func (s *openAIAccountAuditState) auditTextRunes() int {
	if s == nil || s.document == nil {
		return 0
	}
	return s.document.AuditTextRunes
}

func (s *openAIAccountAuditState) preferAPIKey() bool {
	return s != nil && s.policy.PreferAPIKeyEnabled() &&
		s.auditTextRunes() > s.policy.LongTextRuneThreshold()
}

func (s *openAIAccountAuditState) routingOptions(
	auditTransport service.OpenAIUpstreamTransport,
	auditCapability service.OpenAIEndpointCapability,
) service.OpenAIAccountRoutingOptions {
	if s == nil {
		return service.OpenAIAccountRoutingOptions{}
	}
	return service.OpenAIAccountRoutingOptions{
		PreferAPIKey:                    s.preferAPIKey(),
		AuditPolicy:                     s.policy,
		AuditRequiredTransport:          auditTransport,
		AuditRequiredEndpointCapability: auditCapability,
	}
}

func (s *openAIAccountAuditState) summaryClone() *securityaudit.AuditSummary {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.summary == nil {
		return nil
	}
	cloned := s.summary.Clone()
	return &cloned
}

func auditUnavailableForAccountPolicy() *securityaudit.Decision {
	return &securityaudit.Decision{
		Kind: securityaudit.DecisionUnavailable, HTTPStatus: http.StatusServiceUnavailable,
		ErrorCode:     securityaudit.ErrorCodeAuditUnavailable,
		ClientMessage: "安全审计暂时不可用，请稍后重试",
	}
}

func (h *OpenAIGatewayHandler) ensureSecurityAuditForAccount(
	c *gin.Context,
	reqLog *zap.Logger,
	apiKey *service.APIKey,
	subject middleware2.AuthSubject,
	account *service.Account,
	state *openAIAccountAuditState,
) (*securityaudit.Decision, service.OpenAIAccountAuditEligibility) {
	if state == nil {
		eligibility := service.OpenAIAccountAuditEligibility{Indeterminate: true, Reason: service.OpenAIAccountAuditPolicyUnavailable}
		return auditUnavailableForAccountPolicy(), eligibility
	}
	state.mu.Lock()
	defer state.mu.Unlock()

	eligibility := service.ClassifyOpenAIAccountAuditEligibility(account, state.policy)
	if state.terminal {
		decision := state.decision
		return &decision, eligibility
	}
	if reqLog != nil {
		fields := []zap.Field{
			zap.Int("audit_text_runes", state.auditTextRunes()),
			zap.Bool("prefer_apikey", state.preferAPIKey()),
			zap.Bool("audit_required", eligibility.Eligible || eligibility.Indeterminate),
			zap.String("audit_eligibility_reason", string(eligibility.Reason)),
			zap.Int64("audit_account_group_id", eligibility.MatchedGroupID),
		}
		if account != nil {
			fields = append(fields, zap.Int64("account_id", account.ID), zap.String("account_type", account.Type))
		}
		reqLog.Info("security_audit.account_admission", fields...)
	}
	if !eligibility.Eligible {
		if eligibility.Indeterminate {
			decision := auditUnavailableForAccountPolicy()
			state.terminal = true
			state.decision = *decision
			return decision, eligibility
		}
		return nil, eligibility
	}
	if state.passed {
		decision := state.decision
		if reqLog != nil {
			reqLog.Info("security_audit.account_check_reused",
				zap.Int64("account_id", account.ID),
				zap.String("account_type", account.Type),
				zap.Int("audit_text_runes", state.auditTextRunes()),
			)
		}
		return &decision, eligibility
	}
	if h == nil || h.securityAuditCoordinator == nil || c == nil || c.Request == nil {
		decision := auditUnavailableForAccountPolicy()
		state.terminal = true
		state.decision = *decision
		return decision, eligibility
	}
	if !strictAuditProtocolApplies(c, apiKey, state.protocol) ||
		strictAuditRequestBypassesTextAudit(c, state.protocol, state.model, state.body) {
		return nil, eligibility
	}

	request := buildSecurityAuditRequest(c, apiKey, subject, state.protocol, state.model, state.body, state.stage)
	request.Document = state.document.Clone()
	request.ForceStrictAdmission = true
	logSecurityAuditStart(reqLog, request, len(state.body), false)
	decision := h.securityAuditCoordinator.Check(c.Request.Context(), request)
	logSecurityAuditDone(reqLog, request, decision, false)
	if !decision.AllowNextStage {
		state.terminal = true
		state.decision = decision
		return &decision, eligibility
	}
	if decision.Audit == nil {
		unavailable := auditUnavailableForAccountPolicy()
		state.terminal = true
		state.decision = *unavailable
		return unavailable, eligibility
	}
	service.MarkOpenAIStrictAuditRequest(c)
	state.passed = true
	state.decision = decision
	cloned := decision.Audit.Clone()
	state.summary = &cloned
	return &decision, eligibility
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

func (h *OpenAIGatewayHandler) checkSecurityAuditStage(c *gin.Context, reqLog *zap.Logger, apiKey *service.APIKey, subject middleware2.AuthSubject, protocol, model string, body []byte, stage string) *securityaudit.Decision {
	if h == nil {
		return nil
	}
	return runSecurityAudit(c, reqLog, h.securityAuditCoordinator, h.contentModerationService, apiKey, subject, protocol, model, body, stage)
}

func cloneSecurityAuditSummary(decision *securityaudit.Decision) *securityaudit.AuditSummary {
	if decision == nil || !decision.AllowNextStage || decision.Audit == nil {
		return nil
	}
	cloned := decision.Audit.Clone()
	return &cloned
}

func securityAuditResponseLineageRequired(summary *securityaudit.AuditSummary) bool {
	return summary != nil && summary.ResponseLineageRequired()
}

func (h *OpenAIGatewayHandler) bindAllowedSecurityAuditResponse(ctx context.Context, reqLog *zap.Logger, summary *securityaudit.AuditSummary, result *service.OpenAIForwardResult) error {
	if summary != nil && !summary.ResponseLineageRequired() {
		return nil
	}
	if h == nil || h.securityAuditCoordinator == nil || summary == nil {
		return fmt.Errorf("strict audit lineage coordinator is unavailable")
	}
	if result == nil || !result.CompletedForLineage() || strings.TrimSpace(result.ResponseID) == "" {
		if reqLog != nil {
			responseIDLen := 0
			if result != nil {
				responseIDLen = len(strings.TrimSpace(result.ResponseID))
			}
			reqLog.Warn("security_audit.lineage_output_incomplete",
				zap.Int64("api_key_id", summary.APIKeyID),
				zap.Int64p("group_id", summary.GroupID),
				zap.Int("response_id_len", responseIDLen),
			)
		}
		return fmt.Errorf("%w: successful response output is incomplete", securityaudit.ErrLineageInvalid)
	}
	output, complete := result.OpenAIResponsesLineageOutput()
	if !complete {
		return fmt.Errorf("%w: successful response output is incomplete", securityaudit.ErrLineageInvalid)
	}
	augmented, err := securityaudit.AppendResponsesOutput(summary.Clone(), output)
	if err != nil {
		if reqLog != nil {
			reqLog.Warn("security_audit.lineage_output_incomplete",
				zap.Int64("api_key_id", summary.APIKeyID),
				zap.Int64p("group_id", summary.GroupID),
				zap.Int("response_id_len", len(strings.TrimSpace(result.ResponseID))),
				zap.Error(err),
			)
		}
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	} else {
		ctx = context.WithoutCancel(ctx)
	}
	if err := h.securityAuditCoordinator.BindAllowedResponse(ctx, augmented, result.ResponseID); err != nil {
		if reqLog != nil {
			reqLog.Warn("security_audit.lineage_bind_failed",
				zap.Int64("api_key_id", summary.APIKeyID),
				zap.Int64p("group_id", summary.GroupID),
				zap.Int("response_id_len", len(strings.TrimSpace(result.ResponseID))),
				zap.Error(err),
			)
		}
		return err
	}
	return nil
}

func runSecurityAudit(c *gin.Context, reqLog *zap.Logger, coordinator *securityaudit.Coordinator, legacy *service.ContentModerationService, apiKey *service.APIKey, subject middleware2.AuthSubject, protocol, model string, body []byte, stage string) *securityaudit.Decision {
	if c == nil || c.Request == nil {
		return nil
	}
	// Strict admission is intentionally limited to OpenAI text protocols. A
	// composite group is only eligible after its target platform has resolved to
	// OpenAI; an unresolved composite target must not accidentally audit Grok.
	if !strictAuditProtocolApplies(c, apiKey, protocol) {
		return nil
	}
	model = clientRequestedModel(c, model)
	if strictAuditRequestBypassesTextAudit(c, protocol, model, body) {
		return nil
	}
	if legacy != nil {
		scopeInput := buildContentModerationInput(c, apiKey, subject, protocol, model, body)
		applies, err := legacy.StrictPreBlockApplies(c.Request.Context(), scopeInput.GroupID)
		if err != nil {
			return &securityaudit.Decision{
				Kind: securityaudit.DecisionUnavailable, HTTPStatus: http.StatusServiceUnavailable,
				ErrorCode:     securityaudit.ErrorCodeAuditUnavailable,
				ClientMessage: "安全审计暂时不可用，请稍后重试",
			}
		}
		if !applies {
			return nil
		}
	}
	cacheCompletion := cachesSecurityAuditCompletion(stage)
	if cacheCompletion {
		if completed, exists := c.Get(securityAuditCompletedContextKey); exists && completed == true {
			return nil
		}
	}
	if coordinator == nil {
		return &securityaudit.Decision{
			Kind: securityaudit.DecisionUnavailable, HTTPStatus: http.StatusServiceUnavailable,
			ErrorCode:     securityaudit.ErrorCodeAuditUnavailable,
			ClientMessage: "安全审计暂时不可用，请稍后重试",
		}
	}
	request := buildSecurityAuditRequest(c, apiKey, subject, protocol, model, body, stage)
	if isSecurityAuditWebSocketStage(request.Stage) {
		if turnNo, ok := securityAuditWSTurn(c); ok {
			bodyHash := sha256.Sum256(body)
			if cached, exists := c.Get(securityAuditWSDedupeContextKey); exists {
				if entry, ok := cached.(securityAuditWSDedupeEntry); ok &&
					entry.stage == request.Stage && entry.turn == turnNo && entry.bodyHash == bodyHash {
					decision := entry.decision
					logSecurityAuditDone(reqLog, request, decision, true)
					return &decision
				}
			}
			logSecurityAuditStart(reqLog, request, len(body), false)
			decision := coordinator.Check(c.Request.Context(), request)
			if decision.Kind == securityaudit.DecisionAllow {
				c.Set(securityAuditWSDedupeContextKey, securityAuditWSDedupeEntry{
					stage: request.Stage, turn: turnNo, bodyHash: bodyHash, decision: decision,
				})
			}
			logSecurityAuditDone(reqLog, request, decision, false)
			return &decision
		}
	}
	logSecurityAuditStart(reqLog, request, len(body), false)
	decision := coordinator.Check(c.Request.Context(), request)
	if decision.AllowNextStage && decision.Audit != nil {
		service.MarkOpenAIStrictAuditRequest(c)
	}
	if decision.AllowNextStage && cacheCompletion {
		c.Set(securityAuditCompletedContextKey, true)
	}
	logSecurityAuditDone(reqLog, request, decision, false)
	return &decision
}

func strictAuditRequestBypassesTextAudit(c *gin.Context, protocol, model string, body []byte) bool {
	if strings.EqualFold(strings.TrimSpace(protocol), service.ContentModerationProtocolOpenAIResponses) {
		endpoint := "/v1/responses"
		if c != nil && c.Request != nil && strings.TrimSpace(c.Request.URL.Path) != "" {
			endpoint = c.Request.URL.Path
		}
		if service.IsExplicitImageGenerationIntent(endpoint, model, body) {
			return true
		}
	}
	return false
}

func strictAuditProtocolApplies(c *gin.Context, apiKey *service.APIKey, protocol string) bool {
	switch strings.ToLower(strings.TrimSpace(protocol)) {
	case service.ContentModerationProtocolOpenAIResponses, service.ContentModerationProtocolOpenAIChat:
	default:
		return false
	}
	platform := ""
	if apiKey != nil && apiKey.Group != nil {
		platform = strings.TrimSpace(apiKey.Group.Platform)
	}
	if c != nil && c.Request != nil {
		ctx := c.Request.Context()
		if resolved, ok := service.ResolvedTargetPlatformFromContext(ctx); ok {
			platform = strings.TrimSpace(resolved)
		}
		if forced, ok := middleware2.GetForcePlatformFromContext(c); ok {
			platform = strings.TrimSpace(forced)
		}
	}
	if platform == "" {
		return true
	}
	if platform == service.PlatformComposite {
		return false
	}
	return platform == service.PlatformOpenAI
}

func logSecurityAuditStart(reqLog *zap.Logger, request securityaudit.Request, bodyBytes int, cached bool) {
	if reqLog == nil {
		return
	}
	reqLog.Info("security_audit.gateway_check_start",
		zap.String("request_id", request.RequestID), zap.Int64("user_id", request.UserID),
		zap.Int64("api_key_id", request.APIKeyID), zap.Int64p("group_id", request.GroupID),
		zap.String("endpoint", request.Endpoint), zap.String("provider", request.Provider),
		zap.String("protocol", request.Protocol), zap.String("model", request.Model), zap.String("stage", request.Stage),
		zap.Int("body_bytes", bodyBytes), zap.Int("body_chars", len([]rune(string(request.Body)))), zap.Bool("cached", cached))
}

func logSecurityAuditDone(reqLog *zap.Logger, request securityaudit.Request, decision securityaudit.Decision, cached bool) {
	if reqLog == nil {
		return
	}
	reqLog.Info("security_audit.gateway_check_done",
		zap.String("request_id", request.RequestID), zap.String("decision", string(decision.Kind)),
		zap.String("error_code", decision.ErrorCode), zap.Bool("allow_next_stage", decision.AllowNextStage),
		zap.String("stage", request.Stage), zap.Bool("cached", cached))
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
