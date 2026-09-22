package service

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAIServiceBusyIsRequestScopedTransient(t *testing.T) {
	const message = "The service is busy. Please retry later."
	for _, payload := range []string{
		`{"error":{"type":"server_error","message":"` + message + `"}}`,
		`{"type":"response.failed","response":{"error":{"type":"server_error","message":"` + message + `"}}}`,
	} {
		require.True(t, isOpenAIRequestScopedCapacityShed("", []byte(payload)))
		failoverErr := newOpenAIUpstreamFailoverError(http.StatusBadGateway, nil, []byte(payload), message, true)
		require.True(t, failoverErr.RequestScopedTransient)
		require.True(t, failoverErr.ShouldRetryNextAccount())
		require.False(t, failoverErr.ShouldReportAccountScheduleFailure())
	}
	for _, payload := range []string{
		`{"error":{"type":"content_policy_violation","message":"Blocked by content policy"},"echo":"` + message + `"}`,
		`{"error":{"type":"permission_error","message":"Access denied"}}`,
		`{"error":{"code":"model_not_found","message":"Model is not supported"}}`,
	} {
		require.False(t, isOpenAIRequestScopedCapacityShed("", []byte(payload)))
	}
}
