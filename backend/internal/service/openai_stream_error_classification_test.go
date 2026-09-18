package service

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAIStreamBareErrorStructuredTransientClassification(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		retry         bool
	}{
		{"stream read type", `{"type":"error","error":{"type":"stream_read_error","message":"Stream failed"}}`, true},
		{"stream read code", `{"type":"error","error":{"code":"stream_read_error","message":"Stream failed"}}`, true},
		{"server type", `{"type":"error","error":{"type":"server_error","message":"Internal failure"}}`, true},
		{"server code", `{"type":"error","error":{"code":"server_error","message":"Internal failure"}}`, true},
		{"internal code", `{"type":"error","error":{"code":"internal_error","message":"Internal failure"}}`, true},
		{"internal server code", `{"type":"error","error":{"code":"internal_server_error","message":"Internal failure"}}`, true},
		{"unavailable code", `{"type":"error","code":"service_unavailable","message":"Unavailable"}`, true},
		{"nested read code", `{"type":"response.failed","response":{"error":{"code":"upstream_stream_read_error","message":"Interrupted"}}}`, true},
		{"http2 code", `{"error":{"code":"upstream_http2_stream_error","message":"Interrupted"}}`, true},
		{"truncated code", `{"error":{"code":"upstream_stream_truncated","message":"Interrupted"}}`, true},
		{"generic upstream error", `{"type":"error","error":{"type":"upstream_error","message":"Upstream failed"}}`, false},
		{"unknown error", `{"type":"error","error":{"code":"unknown_error","message":"Failed"}}`, false},
		{"echoed code", `{"type":"error","error":{"message":"Failed"},"echo":{"code":"server_error","type":"stream_read_error"}}`, false},
		{"message is not a code", `{"type":"error","error":{"message":"server_error: stream_read_error"}}`, false},
		{"policy with server wrapper", `{"type":"error","error":{"type":"server_error","code":"content_policy_violation","message":"Blocked"}}`, false},
		{"policy with retry hint", `{"type":"error","error":{"code":"content_policy_violation","message":"Please retry with different content"}}`, false},
		{"invalid request with server wrapper", `{"type":"error","error":{"type":"invalid_request_error","code":"server_error","message":"Bad request"}}`, false},
		{"context window", `{"type":"error","error":{"type":"server_error","code":"context_length_exceeded","message":"Too long"}}`, false},
		{"cyber policy", `{"type":"error","error":{"type":"server_error","code":"cyber_policy","message":"Blocked"}}`, false},
		{"canceled read", `{"type":"error","error":{"type":"stream_read_error","message":"context canceled"}}`, false},
		{"canceled request", `{"type":"error","error":{"type":"server_error","code":"request_cancelled","message":"Please retry"}}`, false},
		{"disconnected client", `{"type":"error","error":{"type":"stream_read_error","message":"client disconnected"}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := []byte(tc.payload)
			require.Equal(t, tc.retry, openAIStreamErrorEventShouldFailover(payload, extractOpenAISSEErrorMessage(payload)))
		})
	}
}

func TestOpenAIHTTPStreamStructuredTransientPreservesReplayBoundary(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, errorType := range []string{"stream_read_error", "server_error"} {
			for _, semanticOutput := range []bool{false, true} {
				name := errorType
				if passthrough {
					name += "/passthrough"
				}
				if semanticOutput {
					name += "/after_output"
				}
				t.Run(name, func(t *testing.T) {
					c, rec := newPassthroughKeepaliveTestContext(t)
					stream := replayTestKeepaliveSSE
					if semanticOutput {
						stream += "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
					}
					stream += "data: {\"type\":\"error\",\"error\":{\"type\":\"" + errorType + "\",\"message\":\"Stream failed\"}}\n\n"
					err := runReplayBoundaryStream(t, passthrough, c, io.NopCloser(strings.NewReader(stream)))
					var failover *UpstreamFailoverError
					require.Equal(t, !semanticOutput, errors.As(err, &failover))
					if semanticOutput {
						require.Contains(t, rec.Body.String(), "partial")
					} else {
						require.Empty(t, rec.Body.String())
					}
				})
			}
		}
	}
}
