package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpsErrorDurationUsesIngressClockAndFreezesBeforeQueue(t *testing.T) {
	for _, mode := range []string{"json", "terminal_sse", "stream", "recovered", "session_blocked", "cyber_policy"} {
		t.Run(mode, func(t *testing.T) {
			setupOpsErrorLogTestQueue(t, 3)
			t.Cleanup(func() { resetOpsErrorLoggerStateForTest(t) })
			repo := &ingressRejectOpsRepo{}
			ops := service.NewOpsService(repo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			h := &OpenAIGatewayHandler{opsService: ops}
			router := gin.New()
			router.Use(OpsErrorLoggerMiddleware(ops))
			knownStart := time.Now().Add(-250 * time.Millisecond)
			var requestContext *gin.Context
			router.POST("/v1/responses", func(c *gin.Context) {
				requestContext = c
				require.Equal(t, knownStart, c.Request.Context().Value(ctxkey.RequestStartTime))
				switch mode {
				case "json":
					c.JSON(400, gin.H{"error": gin.H{"type": "invalid_request_error", "message": "invalid input"}})
				case "terminal_sse":
					_, err := c.Writer.WriteString("event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"server_error\",\"message\":\"stream failed\"}}}\n\n")
					require.NoError(t, err)
					c.Writer.Flush()
				case "stream":
					_, err := c.Writer.WriteString(": keepalive\n\n")
					require.NoError(t, err)
					c.Writer.Flush()
					service.MarkOpsStreamFailure(c, "upstream_error", "stream_incomplete", "stream failed", 502)
				case "recovered":
					service.SetOpsUpstreamError(c, 503, "earlier failure", "")
					c.Status(200)
				case "session_blocked":
					c.Status(http.StatusForbidden)
					h.enqueueCyberSessionBlockedOpsEntry(c, &service.APIKey{ID: 21}, "gpt-5", "test-block-key")
				case "cyber_policy":
					service.MarkOpsCyberPolicy(c, service.CyberPolicyMark{Code: "cyber_policy", Message: "blocked", UpstreamStatus: 403})
					h.recordCyberPolicyIfMarked(c, &service.APIKey{ID: 21}, nil, nil, "gpt-5", false, nil, service.ChannelUsageFields{}, "")
				}
			})
			request := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			request = request.WithContext(context.WithValue(request.Context(), ctxkey.RequestStartTime, knownStart))
			router.ServeHTTP(httptest.NewRecorder(), request)
			var job opsErrorLogJob
			select {
			case job = <-opsErrorLogQueue:
			case <-time.After(time.Second):
				t.Fatal("error was not queued")
			}
			require.NotNil(t, job.entry.DurationMs)
			measured := *job.entry.DurationMs
			require.GreaterOrEqual(t, measured, int64(250))
			require.LessOrEqual(t, measured, time.Since(knownStart).Milliseconds())
			// Simulate context reuse while the worker is delayed. Persistence must
			// retain the scalar captured before enqueueing, not read the live clock.
			requestContext.Set(opsRequestDurationMsKey, int64(60000))
			requestContext.Request = requestContext.Request.WithContext(context.WithValue(context.Background(), ctxkey.RequestStartTime, time.Now().Add(-time.Hour)))
			flushOpsErrorLogBatch([]opsErrorLogJob{job})
			require.Len(t, repo.entries, 1)
			require.Equal(t, measured, *repo.entries[0].DurationMs)
			require.Empty(t, opsErrorLogQueue)
		})
	}
}

func TestOpsErrorDurationWithoutAccessLogger(t *testing.T) {
	setupOpsErrorLogTestQueue(t, 1)
	t.Cleanup(func() { resetOpsErrorLoggerStateForTest(t) })
	ops := service.NewOpsService(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	router := gin.New()
	router.Use(OpsErrorLoggerMiddleware(ops))
	router.POST("/v1/responses", func(c *gin.Context) {
		start, ok := c.Request.Context().Value(ctxkey.RequestStartTime).(time.Time)
		require.True(t, ok)
		require.False(t, start.IsZero())
		c.JSON(400, gin.H{"error": gin.H{"type": "invalid_request_error", "message": "invalid input"}})
	})
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/v1/responses", nil))
	require.Len(t, opsErrorLogQueue, 1)
	job := <-opsErrorLogQueue
	require.NotNil(t, job.entry.DurationMs)
	require.GreaterOrEqual(t, *job.entry.DurationMs, int64(0))
}

func TestOpsErrorDurationMissingZeroAndWebSocket(t *testing.T) {
	for _, mode := range []string{"missing", "zero_clock", "future_clock", "zero_duration", "ws_transport", "ws_upgrade"} {
		t.Run(mode, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
			switch mode {
			case "zero_clock":
				c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ctxkey.RequestStartTime, time.Time{}))
			case "future_clock":
				c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ctxkey.RequestStartTime, time.Now().Add(time.Hour)))
			case "zero_duration":
				c.Set(opsRequestDurationMsKey, int64(0))
			case "ws_transport", "ws_upgrade":
				c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), ctxkey.RequestStartTime, time.Now().Add(-time.Hour)))
				c.Set(opsRequestDurationMsKey, int64(3600000))
				if mode == "ws_transport" {
					service.SetOpenAIClientTransport(c, service.OpenAIClientTransportWS)
				} else {
					c.Request.Header.Set("Connection", "Upgrade")
					c.Request.Header.Set("Upgrade", "websocket")
				}
			}
			entry := new(service.OpsInsertErrorLogInput)
			applyOpsLatencyFieldsFromContext(c, entry)
			if mode == "zero_duration" {
				require.NotNil(t, entry.DurationMs)
				require.Zero(t, *entry.DurationMs)
			} else {
				require.Nil(t, entry.DurationMs)
			}
		})
	}
}

func TestAccessLoggerPublishesIngressClock(t *testing.T) {
	router := gin.New()
	router.Use(middleware2.Logger())
	router.GET("/health", func(c *gin.Context) {
		start, ok := c.Request.Context().Value(ctxkey.RequestStartTime).(time.Time)
		require.True(t, ok)
		require.False(t, start.IsZero())
		require.False(t, start.After(time.Now()))
		c.Status(200)
	})
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))
}
