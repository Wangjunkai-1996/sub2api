package handler

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newGenericOpenAIProtocolAdmissionContext(path string, body []byte) (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 1})
	c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 7})
	return c, recorder
}

func TestGenericOpenAIProtocolHandlersRejectCanonicalUninspectableBeforeRouting(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		body     string
		protocol securityadmission.Protocol
		invoke   func(*GatewayHandler, *gin.Context)
	}{
		{
			name: "responses duplicate input", path: "/v1/responses",
			body:     `{"model":"x","input":"first","input":"second"}`,
			protocol: securityadmission.ProtocolOpenAIResponses,
			invoke:   func(h *GatewayHandler, c *gin.Context) { h.Responses(c) },
		},
		{
			name: "chat duplicate content", path: "/v1/chat/completions",
			body:     `{"model":"x","messages":[{"role":"user","content":"first","content":"second"}]}`,
			protocol: securityadmission.ProtocolOpenAIChat,
			invoke:   func(h *GatewayHandler, c *gin.Context) { h.ChatCompletions(c) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, recorder := newGenericOpenAIProtocolAdmissionContext(test.path, []byte(test.body))
			test.invoke(&GatewayHandler{}, c)

			require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
			require.Contains(t, recorder.Body.String(), "security admission gate")
			state := openAISecurityAdmissionFromContext(c)
			require.NotNil(t, state)
			require.Equal(t, test.protocol, state.admission.Protocol())
			require.Equal(t, securityadmission.RequestUninspectable, state.admission.Class())
			require.Equal(t, securityadmission.ReasonDuplicateJSONKey, state.admission.Reason())
		})
	}
}

func TestGenericOpenAIProtocolHandlersInstallOversizeAdmission(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		prefix   string
		suffix   string
		protocol securityadmission.Protocol
		invoke   func(*GatewayHandler, *gin.Context)
	}{
		{
			name: "responses", path: "/v1/responses", prefix: `{"model":"x","input":"`, suffix: `"}`,
			protocol: securityadmission.ProtocolOpenAIResponses,
			invoke:   func(h *GatewayHandler, c *gin.Context) { h.Responses(c) },
		},
		{
			name: "chat", path: "/v1/chat/completions", prefix: `{"model":"x","messages":[{"role":"user","content":"`, suffix: `"}]}`,
			protocol: securityadmission.ProtocolOpenAIChat,
			invoke:   func(h *GatewayHandler, c *gin.Context) { h.ChatCompletions(c) },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := []byte(test.prefix + strings.Repeat("x", securityadmission.DefaultBodyCapBytes) + test.suffix)
			c, recorder := newGenericOpenAIProtocolAdmissionContext(test.path, body)
			test.invoke(&GatewayHandler{}, c)

			require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
			state := openAISecurityAdmissionFromContext(c)
			require.NotNil(t, state)
			require.Equal(t, test.protocol, state.admission.Protocol())
			require.Equal(t, securityadmission.RequestUninspectable, state.admission.Class())
			require.Equal(t, securityadmission.ReasonLargeBody, state.admission.Reason())
		})
	}
}

func TestGenericOpenAIProtocolCanonicalAdmissionPrecedesLegacyAudit(t *testing.T) {
	for _, test := range []struct {
		file     string
		function string
	}{
		{file: "gateway_handler_responses.go", function: "Responses"},
		{file: "gateway_handler_chat_completions.go", function: "ChatCompletions"},
	} {
		source := stripGoComments(goFunctionSource(t, test.file, test.function))
		classifyIndex := strings.Index(source, "classifyOpenAISecurityAdmission(")
		auditIndex := strings.Index(source, "checkSecurityAudit(")
		require.NotEqual(t, -1, classifyIndex)
		require.NotEqual(t, -1, auditIndex)
		require.Less(t, classifyIndex, auditIndex)
		require.Contains(t, source, "RequestKnownViolation")
		require.Contains(t, source, "RequestKnownNoText")
		require.Contains(t, source, "RequestUninspectable")
	}
}
