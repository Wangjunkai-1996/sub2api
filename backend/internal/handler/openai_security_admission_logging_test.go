package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestOpenAISecurityAdmissionFailureAndDispatchLogsAreStructured(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := []byte(`{"model":"gpt-5","input":"security log canary"}`)
	state, err := classifyOpenAISecurityAdmission(
		string(securityadmission.ProtocolOpenAIResponses), body, securityadmission.LineageUntrusted,
	)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	requestContext := context.WithValue(context.Background(), ctxkey.RequestID, "req-security-log")
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(requestContext)
	installOpenAISecurityAdmission(c, state)
	c.Request = c.Request.WithContext(service.WithOpenAIAccountTerminalAdmission(c.Request.Context(), &service.OpenAIAccountRequirementAdmission{
		Selected:     &service.Account{ID: 42},
		AccountClass: securityadmission.AccountAuditRequired,
	}))

	core, logs := observer.New(zap.DebugLevel)
	reqLog := zap.New(core)
	warnOpenAISecurityAdmission(c, reqLog, "security_admission.terminal_rejected", state, 42,
		securityadmission.AccountUnknown, "account_requirement_incompatible", "upstream_not_dispatched")
	observeOpenAISecurityDispatch(c, reqLog, &service.Account{ID: 42}, "upstream_dispatch")

	failureEntries := logs.FilterMessage("security_admission.terminal_rejected").All()
	require.Len(t, failureEntries, 1)
	require.Equal(t, map[string]any{
		"request_id":    "req-security-log",
		"account_id":    int64(42),
		"request_class": string(securityadmission.RequestAuditableText),
		"account_class": string(securityadmission.AccountAuditRequired),
		"reason":        "account_requirement_incompatible",
		"dispatch":      "upstream_not_dispatched",
	}, securityLogContractFields(failureEntries[0].ContextMap()))

	dispatchEntries := logs.FilterMessage("security_admission.dispatch").All()
	require.Len(t, dispatchEntries, 1)
	require.Equal(t, map[string]any{
		"request_id":    "req-security-log",
		"account_id":    int64(42),
		"request_class": string(securityadmission.RequestAuditableText),
		"account_class": string(securityadmission.AccountAuditRequired),
		"reason":        string(state.admission.Reason()),
		"dispatch":      "upstream_dispatch",
	}, securityLogContractFields(dispatchEntries[0].ContextMap()))
}

func TestBlockingScannerFailureLogIsStructured(t *testing.T) {
	body := []byte(`{"model":"gpt-5","input":"blocking failure canary"}`)
	state, err := classifyOpenAISecurityAdmission(
		string(securityadmission.ProtocolOpenAIResponses), body, securityadmission.LineageUntrusted,
	)
	require.NoError(t, err)

	core, logs := observer.New(zap.InfoLevel)
	logSecurityAuditDone(zap.New(core), securityaudit.Request{
		RequestID:    "req-scanner-failure",
		AccountID:    77,
		AccountClass: string(securityadmission.AccountAuditRequired),
		Admission:    &state.admission,
		Stage:        "http",
	}, securityaudit.Decision{
		Kind:           securityaudit.DecisionUnavailable,
		ErrorCode:      securityaudit.ErrorCodeUnavailable,
		AllowNextStage: false,
	}, false)

	entries := logs.FilterMessage("security_audit.gateway_check_done").All()
	require.Len(t, entries, 1)
	require.Equal(t, map[string]any{
		"request_id":    "req-scanner-failure",
		"account_id":    int64(77),
		"request_class": string(securityadmission.RequestAuditableText),
		"account_class": string(securityadmission.AccountAuditRequired),
		"reason":        securityaudit.ErrorCodeUnavailable,
		"dispatch":      "upstream_not_dispatched",
	}, securityLogContractFields(entries[0].ContextMap()))
}

func securityLogContractFields(fields map[string]any) map[string]any {
	return map[string]any{
		"request_id":    fields["request_id"],
		"account_id":    fields["account_id"],
		"request_class": fields["request_class"],
		"account_class": fields["account_class"],
		"reason":        fields["reason"],
		"dispatch":      fields["dispatch"],
	}
}
