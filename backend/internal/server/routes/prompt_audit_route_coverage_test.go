package routes

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/handler"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	servermiddleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestEveryGatewayPOSTRouteIsClassifiedForPromptAuditCoverage(t *testing.T) {
	routeSource, err := os.ReadFile("gateway.go")
	require.NoError(t, err)
	pattern := regexp.MustCompile(`(?:gateway|gemini|r|codexDirect|antigravityV1|antigravityV1Beta)\.POST\("([^"]+)"`)
	matches := pattern.FindAllStringSubmatch(string(routeSource), -1)
	actual := map[string]struct{}{}
	for _, match := range matches {
		actual[match[1]] = struct{}{}
	}

	audited := map[string][]string{
		"/messages":            {"gateway_handler.go"},
		"/responses":           {"gateway_handler_responses.go"},
		"/responses/*subpath":  {"gateway_handler_responses.go"},
		"/chat/completions":    {"gateway_handler_chat_completions.go"},
		"/images/generations":  {"grok_media.go"},
		"/images/edits":        {"grok_media.go"},
		"/videos":              {"grok_media.go"},
		"/videos/generations":  {"grok_media.go"},
		"/videos/edits":        {"grok_media.go"},
		"/videos/extensions":   {"grok_media.go"},
		"/models/*modelAction": {"gemini_v1beta_handler.go"},
		"/tts":                 {"grok_audio.go"},
		"/web_search":          {"gateway_web_search.go"},
		"/x_search":            {"gateway_web_search.go"},
	}
	excluded := map[string]string{
		"/messages/count_tokens":     "tokenization only; it does not execute a model request",
		"/embeddings":                "OpenAI proactive prompt auditing is disabled",
		"/alpha/search":              "OpenAI proactive prompt auditing is disabled",
		"/live":                      "OpenAI proactive prompt auditing is disabled",
		"/realtime/calls":            "OpenAI proactive prompt auditing is disabled",
		"/images/generations/async":  "OpenAI proactive prompt auditing is disabled",
		"/images/edits/async":        "OpenAI proactive prompt auditing is disabled",
		"/images/batches":            "OpenAI proactive prompt auditing is disabled",
		"/images/batches/:id/cancel": "control-plane cancellation with no user prompt",
		"/stt":                       "speech transcription is not a text-generation prompt",
		"/custom-voices":             "voice profile management has no model prompt",
	}

	unclassified := make([]string, 0)
	for route := range actual {
		if _, ok := audited[route]; ok {
			continue
		}
		if _, ok := excluded[route]; ok {
			continue
		}
		unclassified = append(unclassified, route)
	}
	sort.Strings(unclassified)
	require.Empty(t, unclassified, "new gateway POST routes must be audited or explicitly classified with a no-prompt reason")

	for route, files := range audited {
		_, exists := actual[route]
		require.Truef(t, exists, "stale prompt-audit route manifest entry %s", route)
		for _, filename := range files {
			source, readErr := os.ReadFile(filepath.Join("..", "..", "handler", filename))
			require.NoError(t, readErr)
			sourceText := string(source)
			hasDirectAudit := strings.Contains(sourceText, "checkSecurityAudit")
			hasSelectedAccountAudit := strings.Contains(sourceText, "newOpenAIAccountAuditState") &&
				strings.Contains(sourceText, "ensureSecurityAuditForAccount")
			require.Truef(t, hasDirectAudit || hasSelectedAccountAudit, "%s route handler %s bypasses Coordinator", route, filename)
		}
	}

	for route, reason := range excluded {
		require.NotEmpty(t, strings.TrimSpace(reason))
		_, exists := actual[route]
		require.Truef(t, exists, "stale excluded route %s", route)
	}
}

func TestResponsesWebSocketDoesNotRunPromptAudit(t *testing.T) {
	routeSource, err := os.ReadFile("gateway.go")
	require.NoError(t, err)
	require.GreaterOrEqual(t, strings.Count(string(routeSource), `.GET("/responses"`), 2)
	handlerSource, err := os.ReadFile(filepath.Join("..", "..", "handler", "openai_gateway_handler.go"))
	require.NoError(t, err)
	require.Contains(t, string(handlerSource), `func (h *OpenAIGatewayHandler) ResponsesWebSocket`)
	require.NotContains(t, string(handlerSource), `newOpenAIAccountAuditState`)
	require.NotContains(t, string(handlerSource), `ensureSecurityAuditForAccount`)
}

func TestPromptAuditAdminRoutesRejectUnauthenticatedAndNonAdminRequests(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handlers := &handler.Handlers{Admin: &handler.AdminHandlers{
		PromptAudit: securityaudit.NewPromptAdminHandler(nil),
	}}
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

	for _, tc := range []struct {
		name       string
		auth       string
		wantStatus int
	}{
		{name: "unauthenticated", wantStatus: http.StatusUnauthorized},
		{name: "non-admin", auth: "Bearer user-token", wantStatus: http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/api/v1/admin/prompt-audit/config", nil)
			if tc.auth != "" {
				request.Header.Set("Authorization", tc.auth)
			}
			router.ServeHTTP(recorder, request)
			require.Equal(t, tc.wantStatus, recorder.Code)
		})
	}
}
