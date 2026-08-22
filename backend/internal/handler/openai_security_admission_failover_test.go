package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAISecurityAdmissionUnavailableDoesNotMaskUpstreamFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	c.Request = c.Request.WithContext(service.WithOpenAIAccountRequirement(
		c.Request.Context(), securityadmission.AccountRequirementAuditExempt,
	))

	state := &openAISecurityAdmissionState{}
	upstream502 := &service.UpstreamFailoverError{StatusCode: http.StatusBadGateway}

	// A prior upstream attempt is authoritative: callers must use the normal
	// failover mapper instead of replacing the provider status with security 503.
	require.False(t, openAISecurityAdmissionUnavailable(c, state, upstream502))

	// Without an upstream attempt, an audit-exempt request with no eligible
	// account remains a security-capacity failure.
	require.True(t, openAISecurityAdmissionUnavailable(c, state, nil))

	// The same rule applies when the request reached the explicit audit fallback
	// path: only an actual upstream error can supersede the security 503.
	state.fallback = true
	require.True(t, openAISecurityAdmissionUnavailable(c, state, nil))
	require.False(t, openAISecurityAdmissionUnavailable(c, state, upstream502))
}
