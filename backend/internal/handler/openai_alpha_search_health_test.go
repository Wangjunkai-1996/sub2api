package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type alphaSearchHealthReporterStub struct {
	successCalls int
	failureCalls int
	lastFailure  service.TrafficDirectorHealthFailureInput
}

func (s *alphaSearchHealthReporterStub) AccountHealthy(context.Context, int64, string) (bool, error) {
	return true, nil
}

func (s *alphaSearchHealthReporterStub) Check(_ context.Context, input service.TrafficDirectorHealthCheckInput) (service.TrafficDirectorHealthDecision, error) {
	return service.TrafficDirectorHealthDecision{
		AccountID:     input.AccountID,
		Model:         input.Model,
		HealthMode:    input.HealthMode,
		State:         service.TrafficDirectorHealthStateHalfOpen,
		Allowed:       true,
		HalfOpenProbe: true,
		ProbeToken:    "alpha-probe",
	}, nil
}

func (s *alphaSearchHealthReporterStub) RecordSuccess(context.Context, service.TrafficDirectorHealthSuccessInput) (bool, error) {
	s.successCalls++
	return true, nil
}

func (s *alphaSearchHealthReporterStub) RecordFailure(_ context.Context, input service.TrafficDirectorHealthFailureInput) (service.TrafficDirectorHealthFailureResult, error) {
	s.failureCalls++
	s.lastFailure = input
	return service.TrafficDirectorHealthFailureResult{Eligible: true, Recorded: true}, nil
}

func TestReportAlphaSearchTrafficDirectorOutcomeUsesWrittenUpstreamStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name            string
		statusCode      int
		wantSuccessCall int
		wantFailureCall int
	}{
		{name: "rate limit does not recover probe", statusCode: http.StatusTooManyRequests},
		{name: "server error records failure", statusCode: http.StatusBadGateway, wantFailureCall: 1},
		{name: "success recovers probe", statusCode: http.StatusOK, wantSuccessCall: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reporter := &alphaSearchHealthReporterStub{}
			gateway := &service.OpenAIGatewayService{}
			gateway.SetOpenAITrafficDirectorHealthResolver(reporter)
			ctx := gateway.WithOpenAITrafficDirectorHealthRequestContext(context.Background())
			account := &service.Account{ID: 81, Platform: service.PlatformOpenAI}
			decision, err := gateway.CheckOpenAITrafficDirectorHealth(ctx, account, "gpt-5", domain.TrafficDirectorHealthModeEnforce)
			require.NoError(t, err)
			require.True(t, decision.HalfOpenProbe)

			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/alpha/search", nil).WithContext(ctx)
			writerSizeBeforeForward := c.Writer.Size()
			c.Data(tt.statusCode, "application/json", []byte(`{"status":"upstream"}`))

			h := &OpenAIGatewayHandler{gatewayService: gateway}
			h.reportAlphaSearchTrafficDirectorOutcome(c, account, "gpt-5", nil, nil, writerSizeBeforeForward)

			require.Equal(t, tt.wantSuccessCall, reporter.successCalls)
			require.Equal(t, tt.wantFailureCall, reporter.failureCalls)
			if tt.wantFailureCall > 0 {
				require.Equal(t, tt.statusCode, reporter.lastFailure.Failure.StatusCode)
			}
		})
	}
}
