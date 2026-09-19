//go:build unit

package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type openAIResponsesCancelOnWrite struct {
	gin.ResponseWriter
	marker string
	cancel context.CancelFunc
}

func (w *openAIResponsesCancelOnWrite) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	if strings.Contains(string(data[:n]), w.marker) {
		w.cancel()
	}
	return n, err
}

func TestOpenAIResponsesStreamFailureClientCancellationAttribution(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name              string
		cancelOn          string
		wantReportedCount int
	}{
		{"cancel_after_upstream_error", "stream_read_error", 1},
		{"cancel_before_upstream_error", "partial-text", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			group := &service.Group{
				ID: 3131, Platform: service.PlatformOpenAI, Status: service.StatusActive,
				Hydrated: true, SchedulerType: service.GroupSchedulerTypeAdvanced,
			}
			ctx = context.WithValue(ctx, ctxkey.Group, group)
			upstream := &openAICompatFailoverUpstream{failure: "after_text"}
			h := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, []service.Account{{
				ID: 1, Name: "stream-error-account", Platform: service.PlatformOpenAI,
				Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true,
				Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"},
				Extra:       map[string]any{"openai_passthrough": false},
			}})
			c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
				`{"model":"gpt-5.1","stream":true,"input":"hello"}`,
			)).WithContext(ctx)
			c.Request.Header.Set("Content-Type", "application/json")
			c.Writer = &openAIResponsesCancelOnWrite{ResponseWriter: c.Writer, marker: tc.cancelOn, cancel: cancel}

			h.Responses(c)

			require.ErrorIs(t, ctx.Err(), context.Canceled, rec.Body.String())
			assert.Equal(t, []int64{1}, upstream.accountIDs, "a committed response must not be replayed")
			assert.Contains(t, rec.Body.String(), "partial-text")
			metrics := h.gatewayService.SnapshotOpenAIAccountSchedulerMetrics()
			assert.Equal(t, int64(1), metrics.SelectTotal, "the advanced scheduler must be active")
			assert.Zero(t, metrics.AccountSwitchTotal)
			assert.Equal(t, tc.wantReportedCount, metrics.RuntimeStatsAccountCount,
				"only the upstream failure observed before client cancellation should reach scheduling feedback")
			if tc.wantReportedCount > 0 {
				assert.Contains(t, rec.Body.String(), "stream_read_error")
			} else {
				assert.NotContains(t, rec.Body.String(), "stream_read_error")
			}
		})
	}
}
