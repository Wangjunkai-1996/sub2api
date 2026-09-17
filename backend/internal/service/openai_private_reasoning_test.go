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
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const privateReasoningEvent = `{"type":"response.output_item.added","output_index":0,"item":{"id":"reasoning_1","type":"reasoning","encrypted_content":"private-ciphertext","summary":[]}}`

func TestOpenAIPrivateReasoningClassification(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		private       bool
	}{
		{"encrypted", privateReasoningEvent, true},
		{"done", strings.Replace(privateReasoningEvent, ".added", ".done", 1), true},
		{"unknown item field", strings.Replace(privateReasoningEvent, `"summary":[]`, `"summary":[],"payload":"opaque"`, 1), false},
		{"unknown root size field", strings.Replace(privateReasoningEvent, `"output_index":0`, `"output_index":0,"size":1`, 1), false},
		{"unknown event field", strings.Replace(privateReasoningEvent, `"output_index":0`, `"output_index":0,"future":"opaque"`, 1), false},
		{"event name supplies type", strings.Replace(privateReasoningEvent, `{"type":"response.output_item.added",`, `{`, 1), true},
		{"unknown status", strings.Replace(privateReasoningEvent, `"summary":[]`, `"summary":[],"status":"bogus"`, 1), false},
		{"nonempty content", strings.Replace(privateReasoningEvent, `"summary":[]`, `"summary":[],"content":[{"type":"input_text"}]`, 1), false},
		{"duplicate root item", `{"type":"response.output_item.added","output_index":0,"item":{"id":"reasoning_1","type":"reasoning","encrypted_content":"private-ciphertext","summary":[]},"item":{"type":"message","content":[{"type":"output_text"}]}}`, false},
		{"duplicate item type", strings.Replace(privateReasoningEvent, `"type":"reasoning"`, `"type":"reasoning","type":"message"`, 1), false},
		{"malformed index", strings.Replace(privateReasoningEvent, `"output_index":0`, `"output_index":null`, 1), false},
		{"text", strings.Replace(privateReasoningEvent, `"summary":[]`, `"summary":[{"type":"summary_text","text":"visible"}]`, 1), false},
		{"malformed content", strings.Replace(privateReasoningEvent, `"summary":[]`, `"summary":null`, 1), false},
		{"compaction", strings.Replace(privateReasoningEvent, `"type":"reasoning"`, `"type":"compaction"`, 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			eventType := effectiveOpenAISSEEventType([]byte(tc.payload), "response.output_item.added")
			require.Equal(t, tc.private, openAIStreamPrivateEncryptedReasoningEvent([]byte(tc.payload), eventType))
			require.True(t, openAIStreamDataStartsClientOutput(tc.payload, eventType), "WS must retain its conservative boundary")
		})
	}

	mismatched := strings.Replace(privateReasoningEvent, `response.output_item.added`, `response.output_item.done`, 1)
	require.True(t, openAIHTTPStreamDataStartsClientOutput(mismatched, "response.output_item.done", "response.output_item.added"))
}

func TestOpenAIPrivateReasoningSuccessfulDeliveryAndRouting(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, textOutput := range []bool{false, true} {
			t.Run(fmt.Sprintf("passthrough_%t/text_%t", passthrough, textOutput), func(t *testing.T) {
				c, rec := newTurnStateTestContext(t, 7, "sess-winner")
				store := newStreamingResponseBindingOrderStore()
				svc := &OpenAIGatewayService{openaiWSStateStore: store}
				account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
				prefix := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"winner\"}}\n\n" + "data: " + privateReasoningEvent + "\n\n"
				if textOutput {
					prefix += "data: {\"type\":\"response.output_text.delta\",\"delta\":\"answer\"}\n\n"
				}
				resp := &http.Response{StatusCode: 200, Header: http.Header{"X-Request-Id": {"winner-header"}, "X-Codex-Turn-State": {"winner-turn"}}, Body: io.NopCloser(strings.NewReader(prefix + "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"winner\",\"status\":\"completed\"}}\n\n"))}
				var err error
				if passthrough {
					_, err = svc.handleStreamingResponsePassthrough(c.Request.Context(), resp, c, account, time.Now(), "model", "model")
				} else {
					_, err = svc.handleStreamingResponse(c.Request.Context(), resp, c, account, time.Now(), "model", "model")
				}
				require.NoError(t, err)
				require.True(t, strings.HasPrefix(rec.Body.String(), prefix), rec.Body.String())
				require.Equal(t, 1, strings.Count(rec.Body.String(), privateReasoningEvent))
				require.Equal(t, "winner-header", rec.Result().Header.Get("X-Request-Id"))
				require.Equal(t, "winner-turn", rec.Result().Header.Get("X-Codex-Turn-State"))
				require.Len(t, rec.Result().Header.Values("X-Request-Id"), 1)
				require.Len(t, rec.Result().Header.Values("X-Codex-Turn-State"), 1)
				origin, recorded := svc.openaiCodexTurnStateOrigins.Load(openAICodexTurnStateSeed(c))
				require.True(t, recorded)
				require.Equal(t, int64(1), origin.(openAICodexTurnStateOrigin).accountID)
				bound, _ := store.bindCounts()
				require.Equal(t, 1, bound)
			})
		}
	}
}

func TestOpenAIPrivateReasoningHeartbeatThenFailoverDoesNotLeakWinnerHeaders(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(fmt.Sprint(passthrough), func(t *testing.T) {
			c, rec := newTurnStateTestContext(t, 7, "sess-heartbeat")
			writer := &openAICompatHeartbeatTestWriter{ResponseWriter: c.Writer, written: make(chan struct{})}
			c.Writer = writer
			reader, upstream := io.Pipe()
			defer reader.Close()
			defer upstream.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = io.WriteString(upstream, "data: "+privateReasoningEvent+"\n\n")
				select {
				case <-writer.written:
					_ = upstream.CloseWithError(io.ErrUnexpectedEOF)
				case <-time.After(4 * time.Second):
					_ = upstream.CloseWithError(errors.New("heartbeat missing"))
				}
			}()
			store := newStreamingResponseBindingOrderStore()
			svc := &OpenAIGatewayService{openaiWSStateStore: store, cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize, StreamKeepaliveInterval: 1}}}
			loser := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
			losingResponse := &http.Response{StatusCode: 200, Header: http.Header{"X-Request-Id": {"loser-header"}, "X-Codex-Turn-State": {"loser-turn"}}, Body: reader}
			var err error
			if passthrough {
				_, err = svc.handleStreamingResponsePassthrough(c.Request.Context(), losingResponse, c, loser, time.Now(), "model", "model")
			} else {
				_, err = svc.handleStreamingResponse(c.Request.Context(), losingResponse, c, loser, time.Now(), "model", "model")
			}
			<-done
			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.True(t, c.Writer.Written(), "the losing attempt must have emitted a real heartbeat")
			require.Equal(t, -1, OpenAICompactKeepaliveAdjustedWrittenSize(c))
			heartbeat := rec.Body.String()
			require.NotEmpty(t, heartbeat)
			require.NotContains(t, heartbeat, privateReasoningEvent)

			account := &Account{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
			stream := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"winner\"}\n\n" +
				"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"winner\",\"status\":\"completed\"}}\n\n"
			resp := &http.Response{StatusCode: 200, Header: http.Header{"X-Request-Id": {"winner-header"}, "X-Codex-Turn-State": {"winner-turn"}}, Body: io.NopCloser(strings.NewReader(stream))}
			if passthrough {
				_, err = svc.handleStreamingResponsePassthrough(c.Request.Context(), resp, c, account, time.Now(), "model", "model")
			} else {
				_, err = svc.handleStreamingResponse(c.Request.Context(), resp, c, account, time.Now(), "model", "model")
			}
			require.NoError(t, err)
			require.True(t, strings.HasPrefix(rec.Body.String(), heartbeat+"data: {\"type\":\"response.output_text.delta\",\"delta\":\"winner\"}\n\n"))
			require.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"response.completed"`))
			require.NotContains(t, rec.Body.String(), privateReasoningEvent)
			require.Empty(t, rec.Result().Header.Get("X-Request-Id"), "a committed local heartbeat owns the response headers")
			require.Empty(t, rec.Result().Header.Get("X-Codex-Turn-State"), "winner turn-state must not be sent after headers commit")
			_, provenanceRecorded := svc.openaiCodexTurnStateOrigins.Load(openAICodexTurnStateSeed(c))
			require.False(t, provenanceRecorded, "a winner turn-state not sent to the client must not be recorded")
			bound, _ := store.bindCounts()
			require.Equal(t, 1, bound, "the winning response must still receive its continuation fence")
		})
	}
}

func TestOpenAIPrivateReasoningFailureKeepsHeadersAndRoutingPrivate(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(fmt.Sprint(passthrough), func(t *testing.T) {
			c, rec := newPassthroughKeepaliveTestContext(t)
			store := newStreamingResponseBindingOrderStore()
			svc := &OpenAIGatewayService{openaiWSStateStore: store}
			account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
			stream := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"loser\"}}\n\n" + "data: " + privateReasoningEvent + "\n\n" + "data: {\"type\":\"error\",\"error\":{\"type\":\"upstream_error\",\"code\":\"stream_read_error\",\"message\":\"stream_read_error\"}}\n\n"
			resp := &http.Response{StatusCode: 200, Header: http.Header{"X-Request-Id": {"loser-header"}, "X-Codex-Turn-State": {"loser-turn"}}, Body: io.NopCloser(strings.NewReader(stream))}
			var err error
			if passthrough {
				_, err = svc.handleStreamingResponsePassthrough(c.Request.Context(), resp, c, account, time.Now(), "model", "model")
			} else {
				_, err = svc.handleStreamingResponse(c.Request.Context(), resp, c, account, time.Now(), "model", "model")
			}
			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.Empty(t, rec.Body.String())
			require.Empty(t, c.Writer.Header().Get("X-Request-Id"))
			require.Empty(t, c.Writer.Header().Get("X-Codex-Turn-State"))
			bound, _ := store.bindCounts()
			require.Zero(t, bound)
		})
	}
}

func TestOpenAIPrivateReasoningReadErrorAndStageLimit(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, failure := range []string{"read", "attempt_timeout", "canceled", "stage_limit", "after_output"} {
			t.Run(fmt.Sprintf("passthrough_%t/%s", passthrough, failure), func(t *testing.T) {
				c, rec := newPassthroughKeepaliveTestContext(t)
				stream := "data: " + privateReasoningEvent + "\n\n"
				readErr := error(io.ErrUnexpectedEOF)
				if failure == "attempt_timeout" {
					readErr = context.DeadlineExceeded
				}
				if failure == "after_output" {
					stream = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"delivered\"}\n\n" + stream
				}
				if failure == "stage_limit" {
					stream = strings.Repeat(stream, openAIFirstOutputStageMaxBytes/len(stream)+1)
				}
				if failure == "canceled" {
					ctx, cancel := context.WithCancel(c.Request.Context())
					cancel()
					c.Request = c.Request.WithContext(ctx)
					readErr = context.Canceled
				}
				err := runReplayBoundaryStream(t, passthrough, c, &openAIResponseFlushReadError{payload: []byte(stream), err: readErr})
				var failover *UpstreamFailoverError
				replay := failure != "canceled" && failure != "after_output"
				require.Equal(t, replay, errors.As(err, &failover), "%v", err)
				if failure == "stage_limit" {
					require.Contains(t, string(failover.ResponseBody), "staging")
				}
				if failure != "after_output" {
					require.Empty(t, rec.Body.String())
				}
			})
		}
	}
}

func TestOpenAIPrivateReasoningPartialWriteNeverReplays(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(fmt.Sprint(passthrough), func(t *testing.T) {
			c, rec := newPassthroughKeepaliveTestContext(t)
			c.Writer = replayPartialWriter{ResponseWriter: c.Writer}
			stream := "data: " + privateReasoningEvent + "\n\n" +
				"data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n" + replayTestBusySSE
			err := runReplayBoundaryStream(t, passthrough, c, io.NopCloser(strings.NewReader(stream)))
			var failover *UpstreamFailoverError
			require.Error(t, err)
			require.False(t, errors.As(err, &failover), "partial downstream writes are not safe to replay")
			require.Equal(t, 1, rec.Body.Len())
		})
	}
}

func TestOpenAIPrivateReasoningPassthroughOversizedLineBoundary(t *testing.T) {
	for _, afterOutput := range []bool{false, true} {
		t.Run(fmt.Sprint(afterOutput), func(t *testing.T) {
			c, rec := newPassthroughKeepaliveTestContext(t)
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: 64 * 1024}}}
			prefix := "data: " + privateReasoningEvent + "\n\n"
			if afterOutput {
				prefix += "data: {\"type\":\"response.output_text.delta\",\"delta\":\"delivered\"}\n\n"
			}
			oversized := strings.Replace(privateReasoningEvent, "private-ciphertext", strings.Repeat("x", 128*1024), 1)
			resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(prefix + "data: " + oversized + "\n\n"))}
			account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
			_, err := svc.handleStreamingResponsePassthrough(c.Request.Context(), resp, c, account, time.Now(), "model", "model")
			require.Error(t, err)
			var failover *UpstreamFailoverError
			require.Equal(t, !afterOutput, errors.As(err, &failover), "%v", err)
			if afterOutput {
				require.Contains(t, rec.Body.String(), `"delta":"delivered"`)
			} else {
				require.False(t, c.Writer.Written())
				require.Empty(t, rec.Body.String())
			}
			require.NotContains(t, rec.Body.String(), oversized)
		})
	}
}

func TestOpenAIPrivateReasoningUnknownFieldsFreezeReplay(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, tc := range []struct{ name, payload, event string }{
			{"root_size", strings.Replace(privateReasoningEvent, `"output_index":0`, `"output_index":0,"size":42`, 1), ""},
			{"item_size", strings.Replace(privateReasoningEvent, `"summary":[]`, `"summary":[],"size":42`, 1), ""},
			{"future_payload", strings.Replace(privateReasoningEvent, `"summary":[]`, `"summary":[],"future_payload":"visible"`, 1), ""},
			{"conflicting_event_type", privateReasoningEvent, "event: response.output_item.done\n"},
			{"unknown_event_type", privateReasoningEvent, "event: future.output\n"},
		} {
			t.Run(fmt.Sprintf("passthrough_%t/%s", passthrough, tc.name), func(t *testing.T) {
				c, rec := newPassthroughKeepaliveTestContext(t)
				stream := tc.event + "data: " + tc.payload + "\n\n" + replayTestBusySSE
				err := runReplayBoundaryStream(t, passthrough, c, io.NopCloser(strings.NewReader(stream)))
				var failover *UpstreamFailoverError
				require.Error(t, err)
				require.NotErrorAs(t, err, &failover)
				require.Contains(t, rec.Body.String(), tc.payload)
			})
		}
	}
}

func TestOpenAIPrivateReasoningKeepsFirstOutputDeadlineArmed(t *testing.T) {
	c, rec := gin.CreateTestContext(httptest.NewRecorder())
	_ = rec
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{OpenAIFirstOutputTimeoutSeconds: 1}}}
	pr, pw := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.WriteString(pw, "data: "+privateReasoningEvent+"\n\n")
		_, _ = io.WriteString(pw, ": pending\n")
	}()
	defer pw.Close()
	resp := &http.Response{StatusCode: 200, Header: http.Header{}, Body: pr}
	_, err := svc.handleStreamingResponse(c.Request.Context(), resp, c, &Account{ID: 1, Platform: PlatformOpenAI}, time.Now(), "model", "model")
	var failover *UpstreamFailoverError
	require.ErrorAs(t, err, &failover)
	require.True(t, failover.FirstOutputTimeout)
	require.False(t, c.Writer.Written())
	<-done
}
