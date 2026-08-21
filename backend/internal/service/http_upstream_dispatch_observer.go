package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptrace"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
)

type openAIUpstreamDispatchObserverContextKey struct{}

type openAIUpstreamDispatchObserver struct {
	once    sync.Once
	observe func()
}

// WithOpenAIUpstreamDispatchObserver records the first transport attempt made
// by one selected-account forwarding attempt. Derived and detached contexts
// share the same once guard, so same-account retries do not inflate the metric.
func WithOpenAIUpstreamDispatchObserver(ctx context.Context, observe func()) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if observe == nil {
		return ctx
	}
	return context.WithValue(ctx, openAIUpstreamDispatchObserverContextKey{}, &openAIUpstreamDispatchObserver{observe: observe})
}

func notifyOpenAIUpstreamDispatch(ctx context.Context) {
	if ctx == nil {
		return
	}
	observer, _ := ctx.Value(openAIUpstreamDispatchObserverContextKey{}).(*openAIUpstreamDispatchObserver)
	if observer == nil || observer.observe == nil {
		return
	}
	observer.once.Do(observer.observe)
}

type dispatchObservingHTTPUpstream struct {
	HTTPUpstream
}

func observeHTTPUpstreamDispatch(upstream HTTPUpstream) HTTPUpstream {
	if upstream == nil {
		return nil
	}
	if _, ok := upstream.(*dispatchObservingHTTPUpstream); ok {
		return upstream
	}
	return &dispatchObservingHTTPUpstream{HTTPUpstream: upstream}
}

func (u *dispatchObservingHTTPUpstream) Do(req *http.Request, proxyURL string, accountID int64, accountConcurrency int) (*http.Response, error) {
	return u.HTTPUpstream.Do(withOpenAIUpstreamDispatchTrace(req), proxyURL, accountID, accountConcurrency)
}

func (u *dispatchObservingHTTPUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, accountConcurrency int, profile *tlsfingerprint.Profile) (*http.Response, error) {
	return u.HTTPUpstream.DoWithTLS(withOpenAIUpstreamDispatchTrace(req), proxyURL, accountID, accountConcurrency, profile)
}

func withOpenAIUpstreamDispatchTrace(req *http.Request) *http.Request {
	if req == nil {
		return nil
	}
	if observer, _ := req.Context().Value(openAIUpstreamDispatchObserverContextKey{}).(*openAIUpstreamDispatchObserver); observer == nil {
		return req
	}
	notify := func() { notifyOpenAIUpstreamDispatch(req.Context()) }
	ctx := httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		ConnectStart: func(_, _ string) { notify() },
		GotConn:      func(httptrace.GotConnInfo) { notify() },
		WroteHeaders: notify,
		WroteRequest: func(httptrace.WroteRequestInfo) { notify() },
	})
	traced := req.Clone(ctx)
	if traced.Body != nil && traced.Body != http.NoBody {
		traced.Body = &dispatchObservingReadCloser{ReadCloser: traced.Body, notify: notify}
	}
	return traced
}

type dispatchObservingReadCloser struct {
	io.ReadCloser
	notify func()
}

func (r *dispatchObservingReadCloser) Read(p []byte) (int, error) {
	if r.notify != nil {
		r.notify()
	}
	return r.ReadCloser.Read(p)
}
