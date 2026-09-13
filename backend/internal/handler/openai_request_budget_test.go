package handler

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestBeginOpenAIResponsesRequestBudgetSplitsHardAndRetryDeadlines(t *testing.T) {
	gin.SetMode(gin.TestMode)
	req := httptest.NewRequest("POST", "/v1/responses", nil)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = req
	startedAt := time.Now()
	h := &OpenAIGatewayHandler{cfg: &config.Config{Gateway: config.GatewayConfig{
		OpenAIRequestBudgetSeconds: 900,
		OpenAIRetryBudgetSeconds:   300,
	}}}
	cleanup := h.beginOpenAIResponsesRequestBudget(c, startedAt)
	defer cleanup()

	hardDeadline, ok := openAIRequestBudgetDeadline(c)
	require.True(t, ok)
	require.WithinDuration(t, startedAt.Add(900*time.Second), hardDeadline, time.Second)
	require.False(t, service.OpenAIRetryBudgetExpired(c.Request.Context()))
}

func TestOpenAIRequestRetryBudgetUsesCompatibilityDefaultAndStopsOnlyRetry(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	startedAt := time.Now().Add(-301 * time.Second)
	h := &OpenAIGatewayHandler{cfg: &config.Config{Gateway: config.GatewayConfig{
		OpenAIRequestBudgetSeconds: 900,
		OpenAIRetryBudgetSeconds:   0,
	}}}
	cleanup := h.beginOpenAIResponsesRequestBudget(c, startedAt)
	defer cleanup()

	require.True(t, service.OpenAIRetryBudgetExpired(c.Request.Context()))
	require.False(t, openAIRequestBudgetExpired(c))
}
