package repository

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type openAIHTTP2BodyTestRoundTripper func(*http.Request) (*http.Response, error)

func (f openAIHTTP2BodyTestRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type constantReadErrorCloser struct {
	err error
}

func (r constantReadErrorCloser) Read([]byte) (int, error) { return 0, r.err }
func (constantReadErrorCloser) Close() error               { return nil }

func newOpenAIHTTP2BodyFallbackTestService(threshold int) *httpUpstreamService {
	return NewHTTPUpstream(&config.Config{Gateway: config.GatewayConfig{
		OpenAIHTTP2: config.GatewayOpenAIHTTP2Config{
			Enabled:                   true,
			AllowProxyFallbackToHTTP1: true,
			FallbackErrorThreshold:    threshold,
			FallbackWindowSeconds:     60,
			FallbackTTLSeconds:        600,
		},
	}}).(*httpUpstreamService)
}

func installOpenAIHTTP2BodyTestTransport(
	t *testing.T,
	upstream *httpUpstreamService,
	proxyURL string,
	body func() io.ReadCloser,
) {
	t.Helper()
	entry, err := upstream.getClientEntry(proxyURL, 42, 3, service.HTTPUpstreamProfileOpenAI, false, false)
	require.NoError(t, err)
	require.Equal(t, upstreamProtocolModeOpenAIH2, entry.protocolMode)
	entry.client.Transport = openAIHTTP2BodyTestRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       body(),
		}, nil
	})
}

func performOpenAIHTTP2BodyTestRequest(
	t *testing.T,
	upstream *httpUpstreamService,
	proxyURL string,
) *http.Response {
	t.Helper()
	ctx := service.WithHTTPUpstreamProfile(t.Context(), service.HTTPUpstreamProfileOpenAI)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/responses", nil)
	require.NoError(t, err)
	resp, err := upstream.Do(req, proxyURL, 42, 3)
	require.NoError(t, err)
	return resp
}

func openAIHTTP2BodyFallbackErrorCount(t *testing.T, upstream *httpUpstreamService, proxyKey string) int {
	t.Helper()
	raw, ok := upstream.openAIHTTP2Fallbacks.Load(proxyKey)
	require.True(t, ok)
	state, ok := raw.(*openAIHTTP2FallbackState)
	require.True(t, ok)
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.errorCount
}

func TestHTTPUpstreamOpenAIHTTP2BodyFailuresActivateFallbackOncePerResponse(t *testing.T) {
	const proxyURL = "http://proxy.local:8080"
	upstream := newOpenAIHTTP2BodyFallbackTestService(2)
	installOpenAIHTTP2BodyTestTransport(t, upstream, proxyURL, func() io.ReadCloser {
		return constantReadErrorCloser{err: io.ErrUnexpectedEOF}
	})

	first := performOpenAIHTTP2BodyTestRequest(t, upstream, proxyURL)
	for range 4 {
		_, err := first.Body.Read(make([]byte, 1))
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	}
	require.NoError(t, first.Body.Close())
	require.False(t, upstream.isOpenAIHTTP2FallbackActive(proxyURL), "one response must count at most once")

	second := performOpenAIHTTP2BodyTestRequest(t, upstream, proxyURL)
	_, err := second.Body.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.NoError(t, second.Body.Close())
	require.True(t, upstream.isOpenAIHTTP2FallbackActive(proxyURL), "two independent body failures must activate H1 fallback")
}

func TestHTTPUpstreamOpenAIHTTP2RoundTripUnexpectedEOFSelectsH1Fallback(t *testing.T) {
	const proxyURL = "http://proxy.local:8080"
	upstream := newOpenAIHTTP2BodyFallbackTestService(2)
	entry, err := upstream.getClientEntry(proxyURL, 42, 3, service.HTTPUpstreamProfileOpenAI, false, false)
	require.NoError(t, err)
	require.Equal(t, upstreamProtocolModeOpenAIH2, entry.protocolMode)
	entry.client.Transport = openAIHTTP2BodyTestRoundTripper(func(*http.Request) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})

	for attempt := range 2 {
		ctx := service.WithHTTPUpstreamProfile(t.Context(), service.HTTPUpstreamProfileOpenAI)
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, "https://chatgpt.com/backend-api/codex/responses", nil)
		require.NoError(t, reqErr)
		resp, doErr := upstream.Do(req, proxyURL, 42, 3)
		require.Nil(t, resp)
		require.ErrorIs(t, doErr, io.ErrUnexpectedEOF)
		if attempt == 0 {
			require.False(t, upstream.isOpenAIHTTP2FallbackActive(proxyURL))
		}
	}

	require.True(t, upstream.isOpenAIHTTP2FallbackActive(proxyURL))
	fallbackEntry, err := upstream.getClientEntry(proxyURL, 42, 3, service.HTTPUpstreamProfileOpenAI, false, false)
	require.NoError(t, err)
	require.Equal(t, upstreamProtocolModeOpenAIH1Fallback, fallbackEntry.protocolMode)
}

func TestHTTPUpstreamOpenAIHTTP2SuccessWaitsForCleanBodyEOF(t *testing.T) {
	const proxyURL = "http://proxy.local:8080"
	upstream := newOpenAIHTTP2BodyFallbackTestService(2)
	upstream.recordOpenAIHTTP2Failure(
		service.HTTPUpstreamProfileOpenAI,
		upstreamProtocolModeOpenAIH2,
		proxyURL,
		io.ErrUnexpectedEOF,
	)
	require.Equal(t, 1, openAIHTTP2BodyFallbackErrorCount(t, upstream, proxyURL))

	installOpenAIHTTP2BodyTestTransport(t, upstream, proxyURL, func() io.ReadCloser {
		return io.NopCloser(strings.NewReader("ok"))
	})
	resp := performOpenAIHTTP2BodyTestRequest(t, upstream, proxyURL)
	require.Equal(t, 1, openAIHTTP2BodyFallbackErrorCount(t, upstream, proxyURL), "headers alone must not reset body failures")

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "ok", string(body))
	require.NoError(t, resp.Body.Close())
	require.Zero(t, openAIHTTP2BodyFallbackErrorCount(t, upstream, proxyURL), "clean EOF should reset the error window")
}

func TestOpenAIHTTP2ObservedBodyIgnoresCanceledReads(t *testing.T) {
	for _, tc := range []struct {
		name      string
		readErr   error
		cancelCtx bool
	}{
		{name: "context canceled error", readErr: context.Canceled},
		{name: "deadline exceeded error", readErr: context.DeadlineExceeded},
		{name: "request context already canceled", readErr: io.ErrUnexpectedEOF, cancelCtx: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			if tc.cancelCtx {
				cancel()
			} else {
				defer cancel()
			}
			var terminalCalls atomic.Int64
			body := &openAIHTTP2ObservedBody{
				ReadCloser: constantReadErrorCloser{err: tc.readErr},
				requestCtx: ctx,
				onTerminal: func(error) { terminalCalls.Add(1) },
			}

			_, err := body.Read(make([]byte, 1))
			require.ErrorIs(t, err, tc.readErr)
			require.Zero(t, terminalCalls.Load())
		})
	}
}

func TestOpenAIHTTP2ObservedBodyReportsTerminalReadOnceConcurrently(t *testing.T) {
	var terminalCalls atomic.Int64
	body := &openAIHTTP2ObservedBody{
		ReadCloser: constantReadErrorCloser{err: io.ErrUnexpectedEOF},
		requestCtx: t.Context(),
		onTerminal: func(error) { terminalCalls.Add(1) },
	}

	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = body.Read(make([]byte, 1))
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), terminalCalls.Load())
}

func TestObserveOpenAIHTTP2ResponseBodyLeavesOtherProfilesAndH1Untouched(t *testing.T) {
	const proxyURL = "http://proxy.local:8080"
	upstream := newOpenAIHTTP2BodyFallbackTestService(1)

	defaultBody := &constantReadErrorCloser{err: io.ErrUnexpectedEOF}
	require.Same(t, defaultBody, upstream.observeOpenAIHTTP2ResponseBody(
		t.Context(), defaultBody, service.HTTPUpstreamProfileDefault, upstreamProtocolModeOpenAIH2, proxyURL,
	))
	h1Body := &constantReadErrorCloser{err: io.ErrUnexpectedEOF}
	require.Same(t, h1Body, upstream.observeOpenAIHTTP2ResponseBody(
		t.Context(), h1Body, service.HTTPUpstreamProfileOpenAI, upstreamProtocolModeOpenAIH1, proxyURL,
	))
	require.False(t, upstream.isOpenAIHTTP2FallbackActive(proxyURL))
}

func TestIsOpenAIHTTP2CompatibilityErrorClassifiesBodyDisconnectsOnly(t *testing.T) {
	require.True(t, isOpenAIHTTP2CompatibilityError(io.ErrUnexpectedEOF))
	require.True(t, isOpenAIHTTP2CompatibilityError(errors.New("unexpected EOF")))
	require.True(t, isOpenAIHTTP2CompatibilityError(errors.New("http2: client connection force closed via ClientConn.Close")))
	require.True(t, isOpenAIHTTP2CompatibilityError(errors.New("http2: client connection lost")))
	require.False(t, isOpenAIHTTP2CompatibilityError(io.EOF))
	require.False(t, isOpenAIHTTP2CompatibilityError(context.Canceled))
	require.False(t, isOpenAIHTTP2CompatibilityError(context.DeadlineExceeded))
	require.False(t, isOpenAIHTTP2CompatibilityError(errors.New("application decode failed")))
	require.False(t, isOpenAIHTTP2CompatibilityError(&timeoutNetError{}))
}

type timeoutNetError struct{}

func (*timeoutNetError) Error() string   { return "read timeout" }
func (*timeoutNetError) Timeout() bool   { return true }
func (*timeoutNetError) Temporary() bool { return true }
