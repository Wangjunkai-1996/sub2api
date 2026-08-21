package service

import (
	"context"
	"sort"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func buildOpenAIWSLateTypePayload(size int, eventType string) []byte {
	prefix := []byte(`{"padding":"`)
	suffix := []byte(`","type":"` + eventType + `"}`)
	if size < len(prefix)+len(suffix) {
		size = len(prefix) + len(suffix)
	}
	body := make([]byte, size)
	copy(body, prefix)
	for i := len(prefix); i < len(body)-len(suffix); i++ {
		body[i] = 'x'
	}
	copy(body[len(body)-len(suffix):], suffix)
	return body
}

func buildOpenAIWSEarlyTypePayload(size int, eventType string) []byte {
	prefix := []byte(`{"type":"` + eventType + `","padding":"`)
	suffix := []byte(`"}`)
	if size < len(prefix)+len(suffix) {
		size = len(prefix) + len(suffix)
	}
	body := make([]byte, size)
	copy(body, prefix)
	for i := len(prefix); i < len(body)-len(suffix); i++ {
		body[i] = 'x'
	}
	copy(body[len(body)-len(suffix):], suffix)
	return body
}

func TestOpenAIWSPreflightClientFrameBoundsOversizeTypeLookup(t *testing.T) {
	tests := []struct {
		name      string
		body      []byte
		wantKind  openAIWSClientFrameKind
		wantEvent string
		overSize  bool
	}{
		{
			name: "normal response create", body: []byte(`{"type":"response.create","model":"gpt-5.1"}`),
			wantKind: openAIWSClientFrameKindResponseCreate, wantEvent: "response.create",
		},
		{
			name: "normal non turn", body: []byte(`{"type":"response.cancel"}`),
			wantKind: openAIWSClientFrameKindNonTurn, wantEvent: "response.cancel",
		},
		{
			name: "oversize early response create", body: buildOpenAIWSEarlyTypePayload(4<<20, "response.create"),
			wantKind: openAIWSClientFrameKindResponseCreate, wantEvent: "response.create", overSize: true,
		},
		{
			name: "oversize early non turn", body: buildOpenAIWSEarlyTypePayload(4<<20, "response.cancel"),
			wantKind: openAIWSClientFrameKindNonTurn, wantEvent: "response.cancel", overSize: true,
		},
		{
			name: "oversize late type 4MiB", body: buildOpenAIWSLateTypePayload(4<<20, "response.create"),
			wantKind: openAIWSClientFrameKindUnknown, overSize: true,
		},
		{
			name: "oversize late type 32MiB", body: buildOpenAIWSLateTypePayload(32<<20, "response.create"),
			wantKind: openAIWSClientFrameKindUnknown, overSize: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			preflight := openAIWSPreflightClientFrame(test.body)
			require.Equal(t, test.wantKind, preflight.kind)
			require.Equal(t, test.wantEvent, preflight.eventType)
			require.Equal(t, test.overSize, preflight.oversize)
		})
	}
}

func TestPassthroughLifecycleRejectsOversizeLateTypeBeforeTurnAndUpstream(t *testing.T) {
	controlCtx, cancelControl := context.WithCancelCause(context.Background())
	defer cancelControl(context.Canceled)
	upstream := newStagedPassthroughConn()
	upstream.Send(`{"type":"response.completed","response":{"id":"resp_preflight_first","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`)

	var nonTurnCalls atomic.Int32
	var requestCalls atomic.Int32
	hooks := &OpenAIWSIngressHooks{
		BeforeNonTurnFrame: func([]byte) error {
			nonTurnCalls.Add(1)
			return nil
		},
		BeforeRequest: func(int, []byte, string) error {
			requestCalls.Add(1)
			return nil
		},
	}
	service := newPassthroughLifecycleService(passthroughLifecycleConfig(), upstream)
	server, serverErr := startPassthroughLifecycleServerWithHooks(
		t, controlCtx, service, passthroughLifecycleAccount(), hooks,
	)
	defer server.Close()
	clientConn := dialPassthroughLifecycleClient(t, server)
	defer func() { _ = clientConn.CloseNow() }()

	require.Equal(t, "response.create", stringValueFromJSON(requirePassthroughUpstreamWrite(t, upstream, time.Second), "type"))
	completed, err := readPassthroughLifecycleFrame(t, clientConn, time.Second)
	require.NoError(t, err)
	require.Equal(t, "response.completed", stringValueFromJSON(completed, "type"))

	lateType := buildOpenAIWSLateTypePayload(4<<20, "response.create")
	writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
	err = clientConn.Write(writeCtx, coderws.MessageText, lateType)
	cancelWrite()
	require.NoError(t, err)

	_, err = readPassthroughLifecycleFrame(t, clientConn, 3*time.Second)
	var websocketCloseErr coderws.CloseError
	require.ErrorAs(t, err, &websocketCloseErr)
	require.Equal(t, coderws.StatusTryAgainLater, websocketCloseErr.Code)
	require.Equal(t, "oversized websocket frame type is unavailable", websocketCloseErr.Reason)

	select {
	case proxyErr := <-serverErr:
		var closeErr *OpenAIWSClientCloseError
		require.ErrorAs(t, proxyErr, &closeErr)
		require.Equal(t, coderws.StatusTryAgainLater, closeErr.StatusCode())
	case <-time.After(3 * time.Second):
		t.Fatal("passthrough late-type preflight did not terminate")
	}
	require.Equal(t, int32(1), nonTurnCalls.Load())
	require.Zero(t, requestCalls.Load(), "late type must not enter response.create admission")
	select {
	case unexpected := <-upstream.writes:
		t.Fatalf("oversized late-type frame reached upstream: %d bytes", len(unexpected))
	case <-time.After(100 * time.Millisecond):
	}
}

func stringValueFromJSON(payload []byte, path string) string {
	result := gjson.GetBytes(payload, path)
	return result.String()
}

func BenchmarkOpenAIWSPreflightClientFrameLateType(b *testing.B) {
	for _, size := range []int{4 << 20, 32 << 20} {
		body := buildOpenAIWSLateTypePayload(size, "response.create")
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			run := func() {
				preflight := openAIWSPreflightClientFrame(body)
				if !preflight.oversize || preflight.kind != openAIWSClientFrameKindUnknown {
					b.Fatalf("unexpected preflight: %+v", preflight)
				}
			}
			b.ReportMetric(float64(len(body)), "bytes/request")
			b.ReportMetric(float64(securityadmission.RoutingEnvelopeWindowBytes), "window-bytes/request")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				run()
			}
			b.StopTimer()
			const samples = 512
			measurements := make([]int64, samples)
			for i := range measurements {
				startedAt := time.Now()
				run()
				measurements[i] = time.Since(startedAt).Nanoseconds()
			}
			sort.Slice(measurements, func(i, j int) bool { return measurements[i] < measurements[j] })
			index := (len(measurements)*99 + 99) / 100
			b.ReportMetric(float64(measurements[index-1]), "p99-ns/op")
		})
	}
}
