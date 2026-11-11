package handler

import (
	"errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"net/http"
)

const openAIDeferredSelectionKey = "openai_deferred_selection_error"

func (h *OpenAIGatewayHandler) admitOpenAIAccountSlot(c *gin.Context, selection *service.AccountSelectionResult) bool {
	err := h.gatewayService.RecheckOpenAIAccountSchedulable(c.Request.Context(), selection.Account)
	if err == nil {
		err = h.gatewayService.AdmitOpenAI429Selection(c.Request.Context(), selection)
	}
	if err == nil {
		c.Set(openAIDeferredSelectionKey, nil)
		return true
	}
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
		selection.ReleaseFunc = nil
	}
	selection.Acquired = false
	c.Set(openAIDeferredSelectionKey, err)
	return false
}

func (h *OpenAIGatewayHandler) handleOpenAIDeferredSelection(c *gin.Context, err error, streamStarted, anthropic bool) bool {
	value, exists := c.Get(openAIDeferredSelectionKey)
	deferred, ok := value.(error)
	if !exists || !ok || deferred == nil {
		return false
	}
	if errors.Is(err, service.ErrNoAvailableAccounts) || err == nil {
		err = deferred
	}
	classification := classifySelectionFailureError(err, noAccountErrorClassification{
		Status: http.StatusServiceUnavailable, ErrType: "api_error", ErrCode: "account_pool_exhausted", Message: "Service temporarily unavailable",
	})
	if anthropic {
		h.handleAnthropicSelectionFailure(c, classification, streamStarted)
	} else {
		h.handleSelectionFailure(c, classification, streamStarted)
	}
	return true
}

// A selected account can become disabled or rate-limited while queued. Keep that reason until
// reselection has either found another account or exhausted the remaining pool.
func openAIDeferredSelectionFailure(c *gin.Context, classification noAccountErrorClassification) noAccountErrorClassification {
	if classification.ErrCode != "account_pool_exhausted" {
		return classification
	}
	if value, exists := c.Get(openAIDeferredSelectionKey); exists {
		if err, ok := value.(error); ok {
			return classifySelectionFailureError(err, classification)
		}
	}
	return classification
}
