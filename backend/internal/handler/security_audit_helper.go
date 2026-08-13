package handler

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const securityAuditCompletedContextKey = "sub2api.security_audit.completed"

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
	if reqLog != nil {
		reqLog.Info("security_audit.gateway_check_start",
			zap.String("request_id", request.RequestID), zap.Int64("user_id", request.UserID),
			zap.Int64("api_key_id", request.APIKeyID), zap.Int64p("group_id", request.GroupID),
			zap.String("endpoint", request.Endpoint), zap.String("provider", request.Provider),
			zap.String("protocol", request.Protocol), zap.String("model", request.Model), zap.String("stage", request.Stage),
			zap.Int("body_bytes", len(body)))
	}
	decision := coordinator.Check(c.Request.Context(), request)
	if decision.AllowNextStage && decision.Audit != nil {
		service.MarkOpenAIStrictAuditRequest(c)
	}
	if decision.AllowNextStage && cacheCompletion {
		c.Set(securityAuditCompletedContextKey, true)
	}
	if reqLog != nil {
		reqLog.Info("security_audit.gateway_check_done",
			zap.String("request_id", request.RequestID), zap.String("decision", string(decision.Kind)),
			zap.String("error_code", decision.ErrorCode), zap.Bool("allow_next_stage", decision.AllowNextStage),
			zap.String("stage", request.Stage))
	}
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
	if !service.OpenAIRequestBodyMayContainImageInput(body) {
		return false
	}
	document := auditinput.ParseForTextAudit(protocol, body)
	return document != nil && document.Complete && document.HasImages && strings.TrimSpace(document.NormalizedText) == ""
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
