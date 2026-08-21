package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
)

type dispatchObserverUpstream struct {
	calls int
}

func (u *dispatchObserverUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return u.dispatch(req)
}

func (u *dispatchObserverUpstream) dispatch(req *http.Request) (*http.Response, error) {
	u.calls++
	if req != nil && req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
	}
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

func (u *dispatchObserverUpstream) DoWithTLS(req *http.Request, _ string, _ int64, _ int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.dispatch(req)
}

func TestObservedHTTPUpstreamNotifiesOnlyAtFirstTransportAttempt(t *testing.T) {
	upstream := &dispatchObserverUpstream{}
	observed := observeHTTPUpstreamDispatch(upstream)
	notifications := 0
	ctx := WithOpenAIUpstreamDispatchObserver(context.Background(), func() { notifications++ })
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.com/v1/responses", bytes.NewBufferString(`{"input":"canary"}`))
	require.NoError(t, err)
	require.Zero(t, notifications, "installing the observer must not claim a dispatch")

	resp, err := observed.Do(req, "", 42, 1)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	retryReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.com/v1/responses", bytes.NewBufferString(`{"input":"retry"}`))
	require.NoError(t, err)
	resp, err = observed.DoWithTLS(retryReq, "", 42, 1, &tlsfingerprint.Profile{})
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	require.Equal(t, 2, upstream.calls)
	require.Equal(t, 1, notifications, "same-account retries share one dispatch observation")
}

func TestObservedHTTPUpstreamWithoutTransportCallDoesNotNotify(t *testing.T) {
	notifications := 0
	_ = WithOpenAIUpstreamDispatchObserver(context.Background(), func() { notifications++ })
	require.Zero(t, notifications)
}
