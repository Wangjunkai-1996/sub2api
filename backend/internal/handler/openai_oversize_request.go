package handler

import (
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

const openAIResponsesLiteHeader = "X-OpenAI-Internal-Codex-Responses-Lite"

func isOpenAIOversizeAdmission(state *openAISecurityAdmissionState) bool {
	return state != nil && state.admission.Class() == securityadmission.RequestUninspectable &&
		state.admission.Reason() == securityadmission.ReasonLargeBody
}

func extractOpenAIOversizeRoutingEnvelope(
	state *openAISecurityAdmissionState,
	protocol securityadmission.Protocol,
	body []byte,
) (securityadmission.RoutingEnvelope, error) {
	if !isOpenAIOversizeAdmission(state) {
		return securityadmission.RoutingEnvelope{}, securityadmission.ErrRoutingEnvelopeUnavailable
	}
	return securityadmission.ExtractRoutingEnvelope(string(protocol), body)
}

func extractOpenAICompleteOversizeRoutingEnvelope(
	state *openAISecurityAdmissionState,
	protocol securityadmission.Protocol,
	body []byte,
) (securityadmission.RoutingEnvelope, error) {
	if !isOpenAIOversizeAdmission(state) {
		return securityadmission.RoutingEnvelope{}, securityadmission.ErrRoutingEnvelopeUnavailable
	}
	return securityadmission.ExtractCompleteRoutingEnvelope(string(protocol), body)
}

func openAIOversizeReasoningPolicyConfigured(c *gin.Context, apiKey *service.APIKey) bool {
	maxEffort, mappings, applies := openAIReasoningEffortPolicyForRequest(c, apiKey)
	return applies && (strings.TrimSpace(maxEffort) != "" || len(mappings) > 0)
}

func openAIOversizeResponsesPreprocessingReason(
	c *gin.Context,
	apiKey *service.APIKey,
	_ string,
) string {
	if isOpenAILegacyCompactPath(c) {
		return "oversize_compact_normalization_required"
	}
	if c != nil && strings.EqualFold(strings.TrimSpace(c.GetHeader(openAIResponsesLiteHeader)), "true") {
		return "oversize_responses_lite_normalization_required"
	}
	if openAIOversizeReasoningPolicyConfigured(c, apiKey) {
		return "oversize_reasoning_policy_required"
	}
	// Image intent can be declared in tools or tool_choice anywhere in the
	// opaque suffix. It affects group permission, account capability,
	// concurrency, and billing, so a bounded routing envelope cannot safely
	// admit an oversized Responses payload even when the group allows images.
	return "oversize_image_intent_uninspectable"
}
