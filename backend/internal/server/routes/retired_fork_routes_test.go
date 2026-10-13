package routes

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestRetiredForkRoutesReturnNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	pass := func(c *gin.Context) { c.Next() }
	RegisterAdminRoutes(router.Group("/api/v1"), &handler.Handlers{Admin: &handler.AdminHandlers{}},
		middleware.AdminAuthMiddleware(pass), middleware.AuditLogMiddleware(pass),
		middleware.StepUpAuthMiddleware(pass), nil, nil)
	for _, route := range []struct{ method, path string }{
		{http.MethodGet, "/admin/codex-window-warmup/metrics"},
		{http.MethodGet, "/admin/codex-window-warmup/jobs"},
		{http.MethodPost, "/admin/codex-window-warmup/requeue-batch"},
		{http.MethodPost, "/admin/codex-window-warmup/policy-batch"},
		{http.MethodGet, "/admin/accounts/42/codex-warmup"},
		{http.MethodPost, "/admin/accounts/42/codex-warmup/requeue"},
		{http.MethodPost, "/admin/accounts/42/codex-warmup/unblock"},
		{http.MethodGet, "/admin/accounts/42/codex-ticket"},
		{http.MethodPut, "/admin/accounts/42/codex-ticket"},
		{http.MethodPost, "/admin/accounts/42/codex-ticket/harvest"},
	} {
		t.Run(route.method+route.path, func(t *testing.T) {
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, httptest.NewRequest(route.method, "/api/v1"+route.path, nil))
			require.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}
