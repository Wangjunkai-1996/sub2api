//go:build unit

package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAI429QueuedAccountExhaustionPreservesRetryHint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, anthropic := range []bool{false, true} {
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		c.Set(openAIDeferredSelectionKey, &service.OpenAI429CooldownError{RetryAfter: 1500 * time.Millisecond})
		h := &OpenAIGatewayHandler{}
		require.True(t, h.handleOpenAIDeferredSelection(c, service.ErrNoAvailableAccounts, false, anthropic))
		assert.Equal(t, http.StatusTooManyRequests, recorder.Code)
		assert.Equal(t, "2", recorder.Header().Get("Retry-After"))
		assert.Contains(t, recorder.Body.String(), `"code":"account_pool_rate_limited"`)
		assert.Contains(t, recorder.Body.String(), `"type":"rate_limit_error"`)
	}
}

func TestOpenAI429SuccessfulReselectionClearsQueuedFailure(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Set(openAIDeferredSelectionKey, &service.OpenAI429CooldownError{RetryAfter: time.Second})
	h := &OpenAIGatewayHandler{gatewayService: &service.OpenAIGatewayService{}}
	selection := &service.AccountSelectionResult{Account: &service.Account{ID: 7, Type: service.AccountTypeAPIKey}, Acquired: true}
	require.True(t, h.admitOpenAIAccountSlot(c, selection))
	assert.False(t, h.handleOpenAIDeferredSelection(c, service.ErrNoAvailableAccounts, false, false))
	assert.Empty(t, recorder.Header().Get("Retry-After"))
	assert.Empty(t, recorder.Body.String())
}
