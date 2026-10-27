package service

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestOpenAINetworkTraceTLSReuseAndPreSendComposition(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	client := server.Client()
	defer client.CloseIdleConnections()

	for i := 0; i < 2; i++ {
		request, err := http.NewRequest(http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		request, preSend := trackOpenAIUpstreamSend(request)
		request, trace := startOpenAINetworkTrace(request, 7)
		response, err := client.Do(request)
		require.NoError(t, err)
		core, logs := observer.New(zap.InfoLevel)
		trace.log(zap.New(core), response, err)
		_, err = io.Copy(io.Discard, response.Body)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		require.True(t, preSend.requestStarted.Load(), "the existing replay-safety trace must still run")
		fields := logs.All()[0].ContextMap()
		require.Equal(t, "response", fields["result"])
		require.EqualValues(t, 1, fields["connection_requests"])
		require.EqualValues(t, i, fields["connections_reused"])
		require.Contains(t, fields, "connection_acquire_ms")
		require.Contains(t, fields, "attempt_ms")
		require.NotContains(t, fields, "dns_ms", "an IP literal does not require DNS")
		if i == 0 {
			require.Contains(t, fields, "tcp_ms")
			require.Contains(t, fields, "tls_ms")
		} else {
			require.NotContains(t, fields, "tcp_ms")
			require.NotContains(t, fields, "tls_ms")
		}
	}
}

func TestOpenAINetworkTraceRedirectOmitsAmbiguousFirstByte(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/done", http.StatusFound)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	defer server.Close()
	client := server.Client()
	defer client.CloseIdleConnections()
	request, err := http.NewRequest(http.MethodGet, server.URL+"/redirect", nil)
	require.NoError(t, err)
	request, trace := startOpenAINetworkTrace(request, 7)
	response, err := client.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	core, logs := observer.New(zap.InfoLevel)
	trace.log(zap.New(core), response, nil)
	fields := logs.All()[0].ContextMap()
	require.EqualValues(t, 2, fields["connection_requests"])
	require.NotContains(t, fields, "write_to_first_byte_ms")
}

func TestOpenAINetworkTracePreservesRequestAndOnlyLogsSafeFields(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ctx = context.WithValue(ctx, ctxkey.RequestID, "request-123")
	ctx = context.WithValue(ctx, ctxkey.ClientRequestID, "client-456")
	ctx = logger.IntoContext(ctx, zap.New(core).With(zap.String("path", "/secret-context-path")))
	ctx = WithOpenAIModelDispatchBudget(ctx)
	ctx.Value(openAIModelDispatchBudgetKey{}).(*openAIModelDispatchBudget).attempts = 3
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://secret-user:secret-password@secret-host.test/secret-path?token=secret-query", strings.NewReader("secret-body"))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer secret-token")
	traced, trace := startOpenAINetworkTrace(request, 7)
	require.True(t, request.Body == traced.Body, "the body reader must be preserved")
	require.Same(t, request.URL, traced.URL)
	require.Equal(t, request.ContentLength, traced.ContentLength)
	traced.Header.Set("X-Shared", "same-header-map")
	require.Equal(t, "same-header-map", request.Header.Get("X-Shared"))
	deadline, ok := traced.Context().Deadline()
	require.True(t, ok)
	originalDeadline, _ := ctx.Deadline()
	require.Equal(t, originalDeadline, deadline)

	callbacks := httptrace.ContextClientTrace(traced.Context())
	callbacks.GetConn("secret-proxy:1234")
	callbacks.DNSStart(httptrace.DNSStartInfo{Host: "secret-host.test"})
	callbacks.DNSDone(httptrace.DNSDoneInfo{Addrs: []net.IPAddr{{IP: net.ParseIP("192.0.2.9")}}})
	callbacks.ConnectStart("tcp", "192.0.2.9:443")
	callbacks.ConnectDone("tcp", "192.0.2.9:443", nil)
	callbacks.TLSHandshakeStart()
	callbacks.TLSHandshakeDone(tls.ConnectionState{ServerName: "secret-host.test"}, nil)
	callbacks.GotConn(httptrace.GotConnInfo{})
	callbacks.WroteRequest(httptrace.WroteRequestInfo{})
	callbacks.GotFirstResponseByte()
	trace.log(zap.New(core), &http.Response{StatusCode: http.StatusBadGateway, Request: traced}, errors.New("secret-raw-error"))
	require.Len(t, logs.All(), 1)
	entry := logs.All()[0]
	require.Equal(t, "openai.network_trace", entry.Message)
	fields := entry.ContextMap()
	require.Equal(t, "transport_error", fields["result"])
	require.Equal(t, "request-123", fields["request_id"])
	require.Equal(t, "client-456", fields["client_request_id"])
	require.EqualValues(t, 3, fields["dispatch_attempt"])
	require.Equal(t, true, fields[logger.OpsSystemLogSkipField])
	allowed := []string{"component", "account_id", logger.OpsSystemLogSkipField, "request_id", "client_request_id", "dispatch_attempt",
		"result", "attempt_ms", "status_code", "dns_ms", "tcp_ms", "tls_ms", "connection_acquire_ms",
		"connection_requests", "connections_reused", "write_to_first_byte_ms"}
	require.Len(t, fields, len(allowed))
	for _, key := range allowed {
		require.Contains(t, fields, key)
	}
	encoded, err := json.Marshal(fields)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "secret")
	require.NotContains(t, string(encoded), "192.0.2.9")
	cancel()
	require.ErrorIs(t, traced.Context().Err(), context.Canceled)
}

func TestOpenAINetworkTraceConcurrentAndIncompletePhases(t *testing.T) {
	for _, finishBeforeLog := range []bool{false, true} {
		t.Run(map[bool]string{false: "late_callbacks", true: "completed"}[finishBeforeLog], func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "http://example.test", nil)
			request, trace := startOpenAINetworkTrace(request, 7)
			callbacks := httptrace.ContextClientTrace(request.Context())
			var started, done sync.WaitGroup
			proceed := make(chan struct{})
			for i := 0; i < 8; i++ {
				started.Add(1)
				done.Add(1)
				go func() {
					defer done.Done()
					callbacks.ConnectStart("tcp", "secret-address")
					started.Done()
					<-proceed
					callbacks.ConnectDone("tcp", "secret-address", nil)
				}()
			}
			started.Wait()
			if finishBeforeLog {
				close(proceed)
				done.Wait()
			}
			core, logs := observer.New(zap.InfoLevel)
			trace.log(zap.New(core), nil, context.Canceled)
			if !finishBeforeLog {
				close(proceed)
				done.Wait()
			}
			trace.log(zap.New(core), nil, nil)
			require.Len(t, logs.All(), 1, "late callbacks must not emit another event")
			fields := logs.All()[0].ContextMap()
			_, measured := fields["tcp_ms"]
			require.Equal(t, finishBeforeLog, measured)
			require.NotContains(t, fields, "response_headers_ms")
			require.NotContains(t, fields, "dns_ms")
		})
	}

	// Two overlapping dials cover 5 ms; a later independent dial adds 2 ms.
	trace := &openAINetworkTrace{}
	start := time.Now()
	trace.begin(openAINetworkTCP, start)
	trace.begin(openAINetworkTCP, start.Add(time.Millisecond))
	trace.end(openAINetworkTCP, start.Add(3*time.Millisecond))
	trace.end(openAINetworkTCP, start.Add(5*time.Millisecond))
	trace.begin(openAINetworkTCP, start.Add(8*time.Millisecond))
	trace.end(openAINetworkTCP, start.Add(10*time.Millisecond))
	require.Equal(t, 7*time.Millisecond, trace.phases[openAINetworkTCP].elapsed)
}

func TestOpenAINetworkTraceFailureClassifications(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		err    error
		result string
	}{
		{name: "canceled", err: context.Canceled, result: "canceled"},
		{name: "deadline", err: context.DeadlineExceeded, result: "timeout"},
		{name: "network_timeout", err: &net.DNSError{IsTimeout: true}, result: "timeout"},
		{name: "transport", err: errors.New("secret-error"), result: "transport_error"},
		{name: "empty", result: "no_response"},
		{name: "http_error", status: http.StatusTooManyRequests, result: "http_error"},
		{name: "response", status: http.StatusOK, result: "response"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "http://example.test", nil)
			_, trace := startOpenAINetworkTrace(request, 7)
			var response *http.Response
			if tc.status != 0 {
				response = &http.Response{StatusCode: tc.status}
			}
			core, logs := observer.New(zap.InfoLevel)
			trace.log(zap.New(core), response, tc.err)
			fields := logs.All()[0].ContextMap()
			require.Equal(t, tc.result, fields["result"])
			_, hasStatus := fields["status_code"]
			require.Equal(t, response != nil, hasStatus)
			require.NotContains(t, fields, "write_to_first_byte_ms")
			require.NotContains(t, fields, "error")
		})
	}
}

type openAINetworkTraceUpstream struct {
	HTTPUpstream
	do func(*http.Request) (*http.Response, error)
}

func (u *openAINetworkTraceUpstream) Do(request *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return u.do(request)
}

func TestOpenAINetworkTraceNativeFeatureGate(t *testing.T) {
	for _, setting := range []string{"nil_config", "disabled", "enabled"} {
		t.Run(setting, func(t *testing.T) {
			var cfg *config.Config
			if setting != "nil_config" {
				cfg = &config.Config{}
				cfg.Gateway.OpenAINetworkTraceEnabled = setting == "enabled"
			}
			request := httptest.NewRequest(http.MethodPost, "http://example.test", strings.NewReader("body"))
			request, preSend := trackOpenAIUpstreamSend(request)
			originalTrace := httptrace.ContextClientTrace(request.Context())
			response := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("response"))}
			transportErr := errors.New("transport sentinel")
			calls := 0
			upstream := &openAINetworkTraceUpstream{do: func(actual *http.Request) (*http.Response, error) {
				calls++
				actualTrace := httptrace.ContextClientTrace(actual.Context())
				if setting == "enabled" {
					require.NotSame(t, originalTrace, actualTrace)
				} else {
					require.Same(t, originalTrace, actualTrace)
				}
				require.True(t, request.Body == actual.Body, "the body reader must be preserved")
				actualTrace.WroteRequest(httptrace.WroteRequestInfo{})
				return response, transportErr
			}}
			svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
			actualResponse, actualErr := svc.doOpenAIUpstream(request, "", &Account{ID: 7})
			require.Same(t, response, actualResponse)
			require.Same(t, transportErr, actualErr)
			require.Equal(t, 1, calls)
			require.True(t, preSend.requestStarted.Load())
			require.NoError(t, response.Body.Close())
		})
	}
}
