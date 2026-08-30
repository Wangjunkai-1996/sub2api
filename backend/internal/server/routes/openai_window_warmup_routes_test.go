package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/handler"
	adminhandler "github.com/Wei-Shaw/sub2api/internal/handler/admin"
	servermiddleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWindowWarmupRoutesRequireAdminAuthentication(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	accountHandler := adminhandler.NewAccountHandler(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	handlers := &handler.Handlers{Admin: &handler.AdminHandlers{Account: accountHandler}}
	adminAuth := servermiddleware.AdminAuthMiddleware(func(c *gin.Context) {
		if c.GetHeader("Authorization") == "" {
			servermiddleware.AbortWithError(c, http.StatusUnauthorized, "UNAUTHORIZED", "Authorization required")
			return
		}
		servermiddleware.AbortWithError(c, http.StatusForbidden, "FORBIDDEN", "Admin access required")
	})
	auditLog := servermiddleware.AuditLogMiddleware(func(c *gin.Context) { c.Next() })
	stepUp := servermiddleware.StepUpAuthMiddleware(func(c *gin.Context) { c.Next() })
	RegisterAdminRoutes(router.Group("/api/v1"), handlers, adminAuth, auditLog, stepUp, nil, nil)

	routes := []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/api/v1/admin/codex-window-warmup/metrics"},
		{method: http.MethodGet, path: "/api/v1/admin/codex-window-warmup/jobs"},
		{method: http.MethodPost, path: "/api/v1/admin/codex-window-warmup/requeue-batch"},
		{method: http.MethodPost, path: "/api/v1/admin/codex-window-warmup/policy-batch"},
		{method: http.MethodPost, path: "/api/v1/admin/accounts/42/codex-warmup/requeue"},
		{method: http.MethodPost, path: "/api/v1/admin/accounts/42/codex-warmup/unblock"},
	}
	for _, route := range routes {
		for _, authCase := range []struct {
			name       string
			auth       string
			wantStatus int
		}{
			{name: "unauthenticated", wantStatus: http.StatusUnauthorized},
			{name: "non-admin", auth: "Bearer user-token", wantStatus: http.StatusForbidden},
		} {
			t.Run(route.path+"/"+authCase.name, func(t *testing.T) {
				recorder := httptest.NewRecorder()
				request := httptest.NewRequest(route.method, route.path, nil)
				if authCase.auth != "" {
					request.Header.Set("Authorization", authCase.auth)
				}
				router.ServeHTTP(recorder, request)
				require.Equal(t, authCase.wantStatus, recorder.Code)
			})
		}
	}
}
