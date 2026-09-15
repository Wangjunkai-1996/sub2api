package handler

import (
	"errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"net/http"
)

const openAI429DeferredSelectionKey = "openai_429_deferred_selection_error"

func (h *OpenAIGatewayHandler) admitOpenAI429AccountSlot(c *gin.Context, selection *service.AccountSelectionResult) bool {
	err := h.gatewayService.AdmitOpenAI429Selection(c.Request.Context(), selection)
	if err == nil {
		c.Set(openAI429DeferredSelectionKey, nil)
		return true
	}
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
		selection.ReleaseFunc = nil
	}
	selection.Acquired = false
	c.Set(openAI429DeferredSelectionKey, err)
	return false
}

func (h *OpenAIGatewayHandler) handleOpenAI429DeferredSelection(c *gin.Context, err error, streamStarted, anthropic bool) bool {
	value, exists := c.Get(openAI429DeferredSelectionKey)
	deferred, ok := value.(error)
	if !exists || !ok || deferred == nil {
		return false
	}
	if errors.Is(err, service.ErrNoAvailableAccounts) || err == nil {
		err = deferred
	}
	classification := classifySelectionFailureError(err, noAccountErrorClassification{
		Status: http.StatusServiceUnavailable, ErrType: "api_error", Message: "Service temporarily unavailable",
	})
	if anthropic {
		h.handleAnthropicSelectionFailure(c, classification, streamStarted)
	} else {
		h.handleSelectionFailure(c, classification, streamStarted)
	}
	return true
}

// A WaitPlan can become rate-limited while queued. Keep that reason until
// reselection has either found another account or exhausted the remaining pool.
func openAI429DeferredSelectionFailure(c *gin.Context, classification noAccountErrorClassification) noAccountErrorClassification {
	if classification.ErrCode != "account_pool_exhausted" {
		return classification
	}
	if value, exists := c.Get(openAI429DeferredSelectionKey); exists {
		if err, ok := value.(error); ok {
			return classifySelectionFailureError(err, classification)
		}
	}
	return classification
}
