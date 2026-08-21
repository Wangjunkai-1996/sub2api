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

func newGatewayMessagesAdmissionContext(body []byte) (*gin.Context, *httptest.ResponseRecorder) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, EndpointMessages, bytes.NewReader(body))
	c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{ID: 1})
	c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 7})
	return c, recorder
}

func TestGatewayMessagesRejectsCanonicalUninspectableBeforeRouting(t *testing.T) {
	body := []byte(`{"model":"claude-sonnet","messages":[{"role":"user","content":"first","content":"second"}]}`)
	c, recorder := newGatewayMessagesAdmissionContext(body)

	(&GatewayHandler{}).Messages(c)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Contains(t, recorder.Body.String(), "security admission gate")
	state := openAISecurityAdmissionFromContext(c)
	require.NotNil(t, state)
	require.Equal(t, securityadmission.ProtocolAnthropicMessages, state.admission.Protocol())
	require.Equal(t, securityadmission.RequestUninspectable, state.admission.Class())
	require.Equal(t, securityadmission.ReasonDuplicateJSONKey, state.admission.Reason())
	require.Equal(t, securityadmission.AccountRequirementAuditExempt,
		service.OpenAIAccountRequirementFromContext(c.Request.Context()))
}

func TestGatewayMessagesOversizeGateInstallsFailClosedAdmission(t *testing.T) {
	body := []byte(`{"model":"claude-sonnet","messages":[{"role":"user","content":"` +
		strings.Repeat("x", securityadmission.DefaultBodyCapBytes) + `"}]}`)
	c, recorder := newGatewayMessagesAdmissionContext(body)

	(&GatewayHandler{}).Messages(c)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	state := openAISecurityAdmissionFromContext(c)
	require.NotNil(t, state)
	require.Equal(t, securityadmission.RequestUninspectable, state.admission.Class())
	require.Equal(t, securityadmission.ReasonLargeBody, state.admission.Reason())
	require.Equal(t, securityadmission.AccountRequirementAuditExempt,
		service.OpenAIAccountRequirementFromContext(c.Request.Context()))
}

func TestGatewayMessagesCanonicalAdmissionPrecedesLegacyAudit(t *testing.T) {
	source := stripGoComments(goFunctionSource(t, "gateway_handler.go", "Messages"))
	classifyIndex := strings.Index(source, "classifyOpenAISecurityAdmission(")
	auditIndex := strings.Index(source, "checkSecurityAudit(")
	require.NotEqual(t, -1, classifyIndex, "Messages must install canonical admission")
	require.NotEqual(t, -1, auditIndex, "Messages must retain the audit stage")
	require.Less(t, classifyIndex, auditIndex, "canonical admission must precede legacy/coordinator audit")
	require.Contains(t, source, "RequestKnownViolation")
	require.Contains(t, source, "RequestKnownNoText")
	require.Contains(t, source, "RequestUninspectable")
}
