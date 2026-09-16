package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/model"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const replayTestBusySSE = "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"Service is busy. Please try again.\"}}}\n\n"

func runReplayBoundaryStream(t *testing.T, passthrough bool, c *gin.Context, body io.ReadCloser) error {
	t.Helper()
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	resp := &http.Response{StatusCode: 200, Header: http.Header{"X-Request-Id": {"replay-test"}}, Body: body}
	if passthrough {
		_, err := svc.handleStreamingResponsePassthrough(c.Request.Context(), resp, c, account, time.Now(), "model", "model")
		return err
	}
	_, err := svc.handleStreamingResponse(c.Request.Context(), resp, c, account, time.Now(), "model", "model")
	return err
}

func TestOpenAIHTTPStreamReplayBoundaryEvidence(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, tc := range []struct {
			name, event, eventType, reason string
			replay                         bool
		}{
			{"preamble", `{"type":"response.created","response":{"id":"resp_1"}}`, "", "", true},
			{"empty_delta", `{"type":"response.output_text.delta","delta":""}`, "", "", true},
			{"empty_reasoning", `{"type":"response.output_item.added","item":{"type":"reasoning","summary":[]}}`, "", "", true},
			{"text", `{"type":"response.output_text.delta","delta":"SECRET_CONTENT"}`, "response.output_text.delta", "nonempty_or_malformed_delta", false},
			{"encrypted", `{"type":"response.output_item.added","item":{"type":"reasoning","encrypted_content":"SECRET_CONTENT"}}`, "response.output_item.added", "encrypted_reasoning", false},
			{"tool", `{"type":"response.function_call_arguments.delta","delta":"SECRET_CONTENT"}`, "response.function_call_arguments.delta", "tool_arguments", false},
			{"unknown", `{"type":"response.future_event","value":"SECRET_CONTENT"}`, "response.future_event", "conservative_event", false},
		} {
			t.Run(fmt.Sprintf("passthrough_%v/%s", passthrough, tc.name), func(t *testing.T) {
				logs, restore := captureStructuredLog(t)
				defer restore()
				c, rec := newPassthroughKeepaliveTestContext(t)
				// Stable comments alone must not appear as a replay-blocking event.
				n, err := c.Writer.WriteString(": keepalive\n\n")
				require.NoError(t, err)
				recordOpenAIStreamKeepaliveBytes(c, n)
				err = runReplayBoundaryStream(t, passthrough, c, io.NopCloser(strings.NewReader("data: "+tc.event+"\n\n"+replayTestBusySSE)))
				var failover *UpstreamFailoverError
				require.Equal(t, tc.replay, errors.As(err, &failover))
				require.Equal(t, !tc.replay, logs.ContainsMessage("gateway.failover_suppressed_after_semantic_output"))
				require.Equal(t, !tc.replay, logs.ContainsMessage("gateway.stream_replay_boundary_committed"))
				if tc.replay {
					require.Equal(t, ": keepalive\n\n", rec.Body.String())
				} else {
					require.True(t, logs.ContainsFieldValue("first_replay_blocking_event", tc.eventType))
					require.True(t, logs.ContainsFieldValue("replay_blocking_classification", tc.reason))
					require.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"response.failed"`))
					for _, event := range logs.events {
						require.NotContains(t, fmt.Sprint(event.Fields), "SECRET_CONTENT")
					}
				}
			})
		}
	}
}

func TestOpenAIPassthroughPreambleStageBounded(t *testing.T) {
	for _, size := range []int{4 * 1024, 64 * 1024, openAIFirstOutputStageMaxBytes, openAIFirstOutputStageMaxBytes + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			c, rec := newPassthroughKeepaliveTestContext(t)
			// A valid comment line fills exactly size bytes including its newline.
			preamble := ":" + strings.Repeat("x", size-2) + "\n"
			body := &passthroughCloseTrackingReadCloser{Reader: strings.NewReader(preamble + replayTestBusySSE)}
			err := runReplayBoundaryStream(t, true, c, body)
			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.Empty(t, rec.Body.String(), "buffer limits must never commit preamble")
			require.False(t, c.Writer.Written())
			if size > openAIFirstOutputStageMaxBytes {
				require.True(t, body.closed)
			}
		})
	}
}

func TestOpenAIHTTPStreamReplayOnlyDeliversWinningAttempt(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(fmt.Sprint(passthrough), func(t *testing.T) {
			c, rec := newPassthroughKeepaliveTestContext(t)
			losing := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"losing\"}}\n\n" + replayTestBusySSE
			var failover *UpstreamFailoverError
			require.ErrorAs(t, runReplayBoundaryStream(t, passthrough, c, io.NopCloser(strings.NewReader(losing))), &failover)
			require.Empty(t, rec.Body.String())
			// Crossing the in-memory threshold also verifies spool commit, not just overflow.
			winning := ":" + strings.Repeat("p", 65*1024) + "\n\n" +
				"data: {\"type\":\"response.output_text.delta\",\"delta\":\"winning answer\"}\n\n" +
				"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"winner\",\"status\":\"completed\"}}\n\n"
			require.NoError(t, runReplayBoundaryStream(t, passthrough, c, io.NopCloser(strings.NewReader(winning))))
			require.NotContains(t, rec.Body.String(), "losing")
			require.NotContains(t, rec.Body.String(), "response.failed")
			require.Contains(t, rec.Body.String(), "winning answer")
			require.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"response.output_text.delta"`))
			require.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"response.completed"`))
		})
	}
}

func TestOpenAIPassthroughKeepalivePreservesFailureFraming(t *testing.T) {
	c, rec := newPassthroughKeepaliveTestContext(t)
	rule := newNonFailoverPassthroughRule(http.StatusBadRequest, "input exceeds the context window", http.StatusBadRequest, "")
	rule.Platforms = []string{PlatformOpenAI}
	ruleSvc := &ErrorPassthroughService{}
	ruleSvc.setLocalCache([]*model.ErrorPassthroughRule{rule})
	BindErrorPassthroughService(c, ruleSvc)
	c.Header("Content-Type", "text/event-stream")
	n, err := c.Writer.WriteString(": keepalive\n\n")
	require.NoError(t, err)
	recordOpenAIStreamKeepaliveBytes(c, n)
	stream := "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"type\":\"invalid_request_error\",\"code\":\"context_length_exceeded\",\"message\":\"input exceeds the context window\"}}}\n\n"
	err = runReplayBoundaryStream(t, true, c, io.NopCloser(strings.NewReader(stream)))
	var failover *UpstreamFailoverError
	require.Error(t, err)
	require.False(t, errors.As(err, &failover))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "text/event-stream", rec.Header().Get("Content-Type"))
	require.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"response.failed"`))
	require.True(t, strings.HasPrefix(rec.Body.String(), ": keepalive\n\ndata: "))
}

type replayPartialWriter struct{ gin.ResponseWriter }

func (w replayPartialWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	n, _ := w.ResponseWriter.Write(data[:1])
	return n, io.ErrClosedPipe
}

func TestOpenAIPassthroughPartialStageWriteNeverReplays(t *testing.T) {
	c, rec := newPassthroughKeepaliveTestContext(t)
	c.Writer = replayPartialWriter{c.Writer}
	stream := "data: {\"type\":\"response.created\"}\n\n" +
		"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n" + replayTestBusySSE
	err := runReplayBoundaryStream(t, true, c, io.NopCloser(strings.NewReader(stream)))
	var failover *UpstreamFailoverError
	require.Error(t, err)
	require.False(t, errors.As(err, &failover))
	require.Equal(t, 1, rec.Body.Len())
}

func TestOpenAIPassthroughLocalDispatchStopDoesNotRecordUpstreamError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := WithOpenAIModelDispatchBudget(context.Background())
	for attempt := 0; attempt < maxOpenAIModelDispatches; attempt++ {
		require.NoError(t, takeOpenAIModelDispatch(ctx, 1))
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
	upstream := &httpUpstreamRecorder{}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
		Credentials: map[string]any{"api_key": "sk-test", "base_url": "https://api.example.test"},
		Extra:       map[string]any{"openai_passthrough": true}, Status: StatusActive, Schedulable: true}
	_, err := svc.Forward(ctx, c, account, []byte(`{"model":"gpt-5.2","input":"hello"}`))
	require.True(t, IsOpenAIModelDispatchStop(err), "%v", err)
	require.ErrorIs(t, err, ErrOpenAIModelDispatchBudgetExhausted)
	require.Empty(t, upstream.requests)
	_, hasUpstreamError := c.Get(OpsUpstreamErrorsKey)
	require.False(t, hasUpstreamError)
	require.False(t, c.Writer.Written())
}
