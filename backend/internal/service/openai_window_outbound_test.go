package service

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
)

type openAIWindowOutboundTokenStub struct {
	token        string
	refreshed    string
	getErr       error
	refreshErr   error
	getCalls     int
	refreshCalls int
	rejected     string
}

func (s *openAIWindowOutboundTokenStub) GetAccessToken(context.Context, *Account) (string, error) {
	s.getCalls++
	return s.token, s.getErr
}

func (s *openAIWindowOutboundTokenStub) RefreshAfterUnauthorized(_ context.Context, _ *Account, rejected string) (string, error) {
	s.refreshCalls++
	s.rejected = rejected
	return s.refreshed, s.refreshErr
}

type openAIWindowOutboundHTTPStub struct {
	responses   []*http.Response
	err         error
	requests    []*http.Request
	proxyURLs   []string
	profiles    []*tlsfingerprint.Profile
	markWritten bool
}

// openAIWindowPartialWriteUpstream runs a real net/http Transport against a
// local listener, then closes the connection after receiving only a prefix of
// the request. The body reader also fails after a short prefix so the transport
// exercises WroteRequestInfo.Err rather than the clean success callback.
type openAIWindowPartialWriteUpstream struct {
	listener net.Listener
}

func (s *openAIWindowPartialWriteUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return s.DoWithTLS(req, "", 0, 0, nil)
}

func (s *openAIWindowPartialWriteUpstream) DoWithTLS(req *http.Request, _ string, _ int64, _ int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	clone := req.Clone(req.Context())
	target := &url.URL{Scheme: "http", Host: s.listener.Addr().String(), Path: "/"}
	clone.URL = target
	clone.Host = "chatgpt.com"
	clone.Body = &openAIWindowFailingBody{ReadCloser: clone.Body, remaining: 8}
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	return client.Do(clone)
}

type openAIWindowFailingBody struct {
	io.ReadCloser
	remaining int
}

func (b *openAIWindowFailingBody) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if len(p) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= n
	return n, err
}

func (s *openAIWindowOutboundHTTPStub) Do(req *http.Request, proxyURL string, accountID int64, concurrency int) (*http.Response, error) {
	return s.DoWithTLS(req, proxyURL, accountID, concurrency, nil)
}

func (s *openAIWindowOutboundHTTPStub) DoWithTLS(
	req *http.Request,
	proxyURL string,
	_ int64,
	_ int,
	profile *tlsfingerprint.Profile,
) (*http.Response, error) {
	copy := req.Clone(req.Context())
	copy.Header = req.Header.Clone()
	copy.Host = req.Host
	s.requests = append(s.requests, copy)
	s.proxyURLs = append(s.proxyURLs, proxyURL)
	s.profiles = append(s.profiles, profile)
	if s.markWritten {
		if trace := httptrace.ContextClientTrace(req.Context()); trace != nil && trace.WroteRequest != nil {
			trace.WroteRequest(httptrace.WroteRequestInfo{})
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	if len(s.responses) == 0 {
		return nil, errors.New("no stub response")
	}
	resp := s.responses[0]
	s.responses = s.responses[1:]
	return resp, nil
}

type openAIWindowOutboundPluginStub struct {
	response *http.Response
	handled  bool
	err      error
	calls    int
	request  *http.Request
	proxyURL string
}

func (s *openAIWindowOutboundPluginStub) RoundTripOpenAIOAuth(
	_ context.Context,
	request *http.Request,
	proxyURL string,
	_ *Account,
) (*http.Response, bool, error) {
	s.calls++
	s.request = request.Clone(request.Context())
	s.request.Header = request.Header.Clone()
	s.proxyURL = proxyURL
	return s.response, s.handled, s.err
}

type openAIWindowTLSResolverStub struct {
	profile *tlsfingerprint.Profile
	calls   int
}

func (s *openAIWindowTLSResolverStub) ResolveTLSProfile(*Account) *tlsfingerprint.Profile {
	s.calls++
	return s.profile
}

type openAIWindowForcedRefreshRepo struct {
	AccountRepository
	account     *Account
	updateCalls int
}

func (r *openAIWindowForcedRefreshRepo) GetByID(context.Context, int64) (*Account, error) {
	return r.account, nil
}

func (r *openAIWindowForcedRefreshRepo) UpdateCredentials(_ context.Context, _ int64, credentials map[string]any) error {
	r.updateCalls++
	r.account.Credentials = shallowCopyMap(credentials)
	return nil
}

type openAIWindowForcedRefreshExecutor struct {
	refreshCalls int
}

func (e *openAIWindowForcedRefreshExecutor) CacheKey(account *Account) string {
	return OpenAITokenCacheKey(account)
}

func (e *openAIWindowForcedRefreshExecutor) CanRefresh(*Account) bool { return true }

func (e *openAIWindowForcedRefreshExecutor) NeedsRefresh(*Account, time.Duration) bool {
	return false
}

func (e *openAIWindowForcedRefreshExecutor) Refresh(_ context.Context, account *Account) (map[string]any, error) {
	e.refreshCalls++
	credentials := shallowCopyMap(account.Credentials)
	credentials["access_token"] = "forced-fresh-token"
	credentials["expires_at"] = time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
	return credentials, nil
}

type openAIWindowForcedRefreshCache struct {
	token           string
	deleteCalls     int
	setCalls        int
	lockUnavailable bool
}

func (c *openAIWindowForcedRefreshCache) GetAccessToken(context.Context, string) (string, error) {
	return c.token, nil
}

func (c *openAIWindowForcedRefreshCache) SetAccessToken(_ context.Context, _ string, token string, _ time.Duration) error {
	c.setCalls++
	c.token = token
	return nil
}

func (c *openAIWindowForcedRefreshCache) DeleteAccessToken(context.Context, string) error {
	c.deleteCalls++
	c.token = ""
	return nil
}

func (c *openAIWindowForcedRefreshCache) AcquireRefreshLock(context.Context, string, time.Duration) (bool, error) {
	return !c.lockUnavailable, nil
}

func (c *openAIWindowForcedRefreshCache) ReleaseRefreshLock(context.Context, string) error {
	return nil
}

func openAIWindowOutboundAccount() *Account {
	proxyID := int64(7)
	return &Account{
		ID:          77,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 3,
		ProxyID:     &proxyID,
		Proxy: &Proxy{
			Protocol: "http",
			Host:     "127.0.0.1",
			Port:     7897,
		},
		Credentials: map[string]any{
			"access_token":       "stored-token-must-not-be-read-by-adapter",
			"refresh_token":      "refresh-token",
			"chatgpt_account_id": "chatgpt-warmup",
		},
	}
}

func openAIWindowOutboundRequest(account *Account) OpenAIOutboundRequest {
	model := "codex-auto-review"
	return OpenAIOutboundRequest{
		Account:  account,
		Model:    model,
		Payload:  BuildOpenAIWindowWarmupPayload(model),
		Headers:  buildOpenAIWindowWarmupHeaders(account),
		Endpoint: chatgptCodexURL,
	}
}

func openAIWindowCompletedResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":                          {"text/event-stream"},
			"X-Codex-Secondary-Window-Minutes":      {"300"},
			"X-Codex-Secondary-Reset-After-Seconds": {"3600"},
			"X-Request-Id":                          {"req_warmup"},
		},
		Body: io.NopCloser(strings.NewReader("event: response.completed\n" +
			`data: {"type":"response.completed","response":{"status":"completed"}}` + "\n\n")),
	}
}

func TestOpenAIWindowOutboundAdapterBuiltInReusesOAuthProxyTLSAndEvidence(t *testing.T) {
	account := openAIWindowOutboundAccount()
	tokens := &openAIWindowOutboundTokenStub{token: "runtime-oauth-token"}
	upstream := &openAIWindowOutboundHTTPStub{responses: []*http.Response{openAIWindowCompletedResponse()}}
	profile := &tlsfingerprint.Profile{Name: "warmup-test-profile"}
	tlsResolver := &openAIWindowTLSResolverStub{profile: profile}
	adapter := &OpenAIWindowOutboundAdapter{
		tokenProvider: tokens,
		httpUpstream:  upstream,
		tlsProfiles:   tlsResolver,
	}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(account))
	require.NoError(t, err)
	require.True(t, result.Terminal)
	require.Equal(t, "response.completed", result.TerminalType)
	require.NotNil(t, result.ResetAt)
	require.Equal(t, "req_warmup", result.RequestID)
	require.False(t, result.EOF)
	require.Equal(t, 1, tokens.getCalls)
	require.Equal(t, 1, tlsResolver.calls)
	require.Len(t, upstream.requests, 1)
	require.Equal(t, "Bearer runtime-oauth-token", upstream.requests[0].Header.Get("Authorization"))
	require.Equal(t, "chatgpt.com", upstream.requests[0].Host)
	require.Equal(t, chatgptCodexURL, upstream.requests[0].URL.String())
	require.Equal(t, "chatgpt-warmup", upstream.requests[0].Header.Get("chatgpt-account-id"))
	require.Equal(t, account.Proxy.URL(), upstream.proxyURLs[0])
	require.Same(t, profile, upstream.profiles[0])
}

func TestOpenAIWindowOutboundAdapterUsesPluginWithoutBuiltInFallback(t *testing.T) {
	account := openAIWindowOutboundAccount()
	tokens := &openAIWindowOutboundTokenStub{token: "plugin-oauth-token"}
	plugin := &openAIWindowOutboundPluginStub{
		handled:  true,
		response: openAIWindowCompletedResponse(),
	}
	upstream := &openAIWindowOutboundHTTPStub{}
	adapter := &OpenAIWindowOutboundAdapter{
		tokenProvider:   tokens,
		httpUpstream:    upstream,
		pluginTransport: plugin,
	}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(account))
	require.NoError(t, err)
	require.True(t, result.Terminal)
	require.Equal(t, 1, plugin.calls)
	require.Empty(t, upstream.requests)
	require.Equal(t, "Bearer plugin-oauth-token", plugin.request.Header.Get("Authorization"))
	require.Equal(t, account.Proxy.URL(), plugin.proxyURL)
	require.True(t, HTTPUpstreamRedirectsDisabled(plugin.request.Context()))
}

func TestOpenAIWindowOutboundAdapterPluginRequestSentErrorIsUncertainWithoutFallback(t *testing.T) {
	plugin := &openAIWindowOutboundPluginStub{
		handled: true,
		err: &PluginTransportError{
			Code:        "UPSTREAM_EOF",
			Message:     "eof",
			RequestSent: true,
		},
	}
	upstream := &openAIWindowOutboundHTTPStub{}
	adapter := &OpenAIWindowOutboundAdapter{
		tokenProvider:   &openAIWindowOutboundTokenStub{token: "plugin-token"},
		httpUpstream:    upstream,
		pluginTransport: plugin,
	}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(openAIWindowOutboundAccount()))
	require.Error(t, err)
	require.True(t, result.Started)
	require.Empty(t, upstream.requests)
}

func TestOpenAIWindowOutboundAdapterPluginEmptyResponseIsPossiblySent(t *testing.T) {
	plugin := &openAIWindowOutboundPluginStub{handled: true}
	upstream := &openAIWindowOutboundHTTPStub{}
	adapter := &OpenAIWindowOutboundAdapter{
		tokenProvider:   &openAIWindowOutboundTokenStub{token: "plugin-token"},
		httpUpstream:    upstream,
		pluginTransport: plugin,
	}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(openAIWindowOutboundAccount()))

	require.Error(t, err)
	require.NotNil(t, result)
	require.True(t, result.Started)
	var transportErr *PluginTransportError
	require.ErrorAs(t, err, &transportErr)
	require.True(t, transportErr.RequestSent)
	require.Equal(t, "PLUGIN_EMPTY_RESPONSE", transportErr.Code)
	require.Empty(t, upstream.requests)
}

func TestOpenAIWindowOutboundAdapterPluginExplicitNotSentOverridesContextTimeout(t *testing.T) {
	plugin := &openAIWindowOutboundPluginStub{
		handled: true,
		err: errors.Join(
			context.DeadlineExceeded,
			&PluginTransportError{Code: "PLUGIN_CONNECT_TIMEOUT", Message: "before upstream", RequestSent: false},
		),
	}
	upstream := &openAIWindowOutboundHTTPStub{}
	adapter := &OpenAIWindowOutboundAdapter{
		tokenProvider:   &openAIWindowOutboundTokenStub{token: "plugin-token"},
		httpUpstream:    upstream,
		pluginTransport: plugin,
	}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(openAIWindowOutboundAccount()))

	require.Error(t, err)
	require.NotNil(t, result)
	require.False(t, result.Started)
	require.Empty(t, upstream.requests)
}

func TestOpenAIWindowOutboundAdapterRefreshesOAuthOnceAfter401(t *testing.T) {
	account := openAIWindowOutboundAccount()
	tokens := &openAIWindowOutboundTokenStub{token: "rejected-token", refreshed: "refreshed-token"}
	upstream := &openAIWindowOutboundHTTPStub{responses: []*http.Response{
		{
			StatusCode: http.StatusUnauthorized,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"token_expired"}}`)),
		},
		openAIWindowCompletedResponse(),
	}}
	adapter := &OpenAIWindowOutboundAdapter{tokenProvider: tokens, httpUpstream: upstream}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(account))
	require.NoError(t, err)
	require.True(t, result.Terminal)
	require.Equal(t, 1, tokens.getCalls)
	require.Equal(t, 1, tokens.refreshCalls)
	require.Equal(t, "rejected-token", tokens.rejected)
	require.Len(t, upstream.requests, 2)
	require.Equal(t, "Bearer rejected-token", upstream.requests[0].Header.Get("Authorization"))
	require.Equal(t, "Bearer refreshed-token", upstream.requests[1].Header.Get("Authorization"))
	require.Nil(t, result.AuthFailure)
}

func TestOpenAIWindowOutboundAdapterRefreshesOAuthAfterPartialBody401(t *testing.T) {
	account := openAIWindowOutboundAccount()
	tokens := &openAIWindowOutboundTokenStub{token: "rejected-token", refreshed: "refreshed-token"}
	partialBody := io.MultiReader(
		strings.NewReader(`{"error":`),
		iotest.ErrReader(io.ErrUnexpectedEOF),
	)
	upstream := &openAIWindowOutboundHTTPStub{responses: []*http.Response{
		{
			StatusCode: http.StatusUnauthorized,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(partialBody),
		},
		openAIWindowCompletedResponse(),
	}}
	adapter := &OpenAIWindowOutboundAdapter{tokenProvider: tokens, httpUpstream: upstream}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(account))

	require.NoError(t, err)
	require.True(t, result.Terminal)
	require.Equal(t, 1, tokens.refreshCalls)
	require.Equal(t, "rejected-token", tokens.rejected)
	require.Len(t, upstream.requests, 2)
	require.Equal(t, "Bearer rejected-token", upstream.requests[0].Header.Get("Authorization"))
	require.Equal(t, "Bearer refreshed-token", upstream.requests[1].Header.Get("Authorization"))
	require.Nil(t, result.AuthFailure)
}

func TestOpenAIWindowOutboundAdapterClassifiesRejectedReplay(t *testing.T) {
	account := openAIWindowOutboundAccount()
	tokens := &openAIWindowOutboundTokenStub{token: "rejected-token", refreshed: "refreshed-token"}
	upstream := &openAIWindowOutboundHTTPStub{responses: []*http.Response{
		{StatusCode: http.StatusUnauthorized, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":"unauthorized"}`))},
		{StatusCode: http.StatusUnauthorized, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":"still unauthorized"}`))},
	}}
	adapter := &OpenAIWindowOutboundAdapter{tokenProvider: tokens, httpUpstream: upstream}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(account))

	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, result.StatusCode)
	require.Equal(t, OpenAIWindowWarmupAuthReplayRejected, result.AuthFailure.Disposition)
	require.Equal(t, account.Credentials, result.AuthFailure.ExpectedCredentials)
	require.Len(t, upstream.requests, 2)
}

func TestOpenAIWindowOutboundAdapterClassifiesRefreshFailure(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want OpenAIWindowWarmupAuthDisposition
	}{
		{name: "terminal", err: errors.New("invalid_grant: revoked"), want: OpenAIWindowWarmupAuthRefreshTerminal},
		{name: "transient", err: errors.New("oauth endpoint temporarily unavailable"), want: OpenAIWindowWarmupAuthRefreshTransient},
	} {
		t.Run(test.name, func(t *testing.T) {
			account := openAIWindowOutboundAccount()
			tokens := &openAIWindowOutboundTokenStub{token: "rejected-token", refreshErr: test.err}
			upstream := &openAIWindowOutboundHTTPStub{responses: []*http.Response{{
				StatusCode: http.StatusUnauthorized,
				Header:     make(http.Header), Body: io.NopCloser(strings.NewReader(`{"error":"unauthorized"}`)),
			}}}
			adapter := &OpenAIWindowOutboundAdapter{tokenProvider: tokens, httpUpstream: upstream}

			result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(account))

			require.NotNil(t, result)
			require.Equal(t, test.want, result.AuthFailure.Disposition)
			if test.want == OpenAIWindowWarmupAuthRefreshTransient {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestOpenAITokenProviderRefreshAfterUnauthorizedIgnoresFutureExpiry(t *testing.T) {
	account := openAIWindowOutboundAccount()
	account.Credentials["access_token"] = "rejected-but-unexpired-token"
	account.Credentials["expires_at"] = time.Now().UTC().Add(12 * time.Hour).Format(time.RFC3339)
	repo := &openAIWindowForcedRefreshRepo{account: account}
	cache := &openAIWindowForcedRefreshCache{token: "rejected-but-unexpired-token"}
	refreshExecutor := &openAIWindowForcedRefreshExecutor{}
	provider := NewOpenAITokenProvider(repo, cache, nil)
	provider.SetRefreshAPI(NewOAuthRefreshAPI(repo, cache), refreshExecutor)

	token, err := provider.RefreshAfterUnauthorized(context.Background(), account, "rejected-but-unexpired-token")
	require.NoError(t, err)
	require.Equal(t, "forced-fresh-token", token)
	require.Equal(t, 1, refreshExecutor.refreshCalls)
	require.Equal(t, 1, repo.updateCalls)
	require.Equal(t, 1, cache.deleteCalls)
	require.Equal(t, 1, cache.setCalls)
	require.Equal(t, "forced-fresh-token", cache.token)
	require.Equal(t, "forced-fresh-token", account.GetOpenAIAccessToken())
}

func TestOpenAITokenProviderRefreshAfterUnauthorizedReusesDurableReplacement(t *testing.T) {
	stale := openAIWindowOutboundAccount()
	stale.Credentials["access_token"] = "rejected-token"
	fresh := *stale
	fresh.Credentials = shallowCopyMap(stale.Credentials)
	fresh.Credentials["access_token"] = "already-refreshed-token"
	repo := &openAIWindowForcedRefreshRepo{account: &fresh}
	cache := &openAIWindowForcedRefreshCache{token: "rejected-token"}
	refreshExecutor := &openAIWindowForcedRefreshExecutor{}
	provider := NewOpenAITokenProvider(repo, cache, nil)
	provider.SetRefreshAPI(NewOAuthRefreshAPI(repo, cache), refreshExecutor)

	token, err := provider.RefreshAfterUnauthorized(context.Background(), stale, "rejected-token")
	require.NoError(t, err)
	require.Equal(t, "already-refreshed-token", token)
	require.Zero(t, refreshExecutor.refreshCalls)
	require.Zero(t, repo.updateCalls)
	require.Equal(t, "already-refreshed-token", stale.GetOpenAIAccessToken())
}

func TestOpenAITokenProviderRefreshAfterUnauthorizedLockHeldUsesDurableReplacement(t *testing.T) {
	stale := openAIWindowOutboundAccount()
	stale.Credentials["access_token"] = "rejected-token"
	fresh := *stale
	fresh.Credentials = shallowCopyMap(stale.Credentials)
	fresh.Credentials["access_token"] = "other-instance-token"
	repo := &openAIWindowForcedRefreshRepo{account: &fresh}
	cache := &openAIWindowForcedRefreshCache{token: "rejected-token", lockUnavailable: true}
	refreshExecutor := &openAIWindowForcedRefreshExecutor{}
	provider := NewOpenAITokenProvider(repo, cache, nil)
	provider.SetRefreshAPI(NewOAuthRefreshAPI(repo, cache), refreshExecutor)

	token, err := provider.RefreshAfterUnauthorized(context.Background(), stale, "rejected-token")
	require.NoError(t, err)
	require.Equal(t, "other-instance-token", token)
	require.Zero(t, refreshExecutor.refreshCalls)
	require.Equal(t, "other-instance-token", cache.token)
}

func TestOpenAIWindowOutboundAdapterRefreshContentionIsTransient(t *testing.T) {
	account := openAIWindowOutboundAccount()
	tokens := &openAIWindowOutboundTokenStub{
		token:      "rejected-token",
		refreshErr: errOpenAITokenRefreshInProgress,
	}
	upstream := &openAIWindowOutboundHTTPStub{responses: []*http.Response{{
		StatusCode: http.StatusUnauthorized,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"error":"unauthorized"}`)),
	}}}
	adapter := &OpenAIWindowOutboundAdapter{tokenProvider: tokens, httpUpstream: upstream}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(account))
	require.ErrorIs(t, err, errOpenAITokenRefreshInProgress)
	require.NotNil(t, result)
	require.Zero(t, result.StatusCode)
	require.False(t, result.Started)
	require.Equal(t, OpenAIWindowWarmupAuthRefreshInProgress, result.AuthFailure.Disposition)
	require.Len(t, upstream.requests, 1)
}

func TestOpenAIWindowOutboundAdapterPATUsesBearerAndDoesNotReplay401(t *testing.T) {
	account := openAIWindowOutboundAccount()
	account.Credentials[openAIAuthModeCredentialKey] = OpenAIAuthModePersonalAccessToken
	delete(account.Credentials, "refresh_token")
	tokens := &openAIWindowOutboundTokenStub{token: "pat-token", refreshed: "must-not-be-used"}
	upstream := &openAIWindowOutboundHTTPStub{responses: []*http.Response{{
		StatusCode: http.StatusUnauthorized,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"error":"unauthorized"}`)),
	}}}
	adapter := &OpenAIWindowOutboundAdapter{tokenProvider: tokens, httpUpstream: upstream}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(account))
	require.NoError(t, err)
	require.Equal(t, http.StatusUnauthorized, result.StatusCode)
	require.Equal(t, OpenAIWindowWarmupAuthNotRefreshable, result.AuthFailure.Disposition)
	require.Len(t, upstream.requests, 1)
	require.Equal(t, "Bearer pat-token", upstream.requests[0].Header.Get("Authorization"))
	require.Zero(t, tokens.refreshCalls)
}

func TestOpenAIWindowOutboundAdapterEmptyStaticCredentialsNeedReauth(t *testing.T) {
	for _, mode := range []string{"access-token-only", OpenAIAuthModePersonalAccessToken} {
		t.Run(mode, func(t *testing.T) {
			account := openAIWindowOutboundAccount()
			delete(account.Credentials, "refresh_token")
			if mode == OpenAIAuthModePersonalAccessToken {
				account.Credentials[openAIAuthModeCredentialKey] = mode
			}
			upstream := &openAIWindowOutboundHTTPStub{}
			adapter := &OpenAIWindowOutboundAdapter{
				tokenProvider: &openAIWindowOutboundTokenStub{},
				httpUpstream:  upstream,
			}

			result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(account))

			require.ErrorIs(t, err, ErrOpenAIWindowWarmupNeedsReauth)
			require.NotNil(t, result)
			require.Equal(t, OpenAIWindowWarmupAuthNotRefreshable, result.AuthFailure.Disposition)
			require.Empty(t, upstream.requests)
		})
	}
}

func TestOpenAIWindowOutboundAdapterInvalidAgentIdentityNeedsReauth(t *testing.T) {
	account := openAIWindowOutboundAccount()
	account.Credentials = map[string]any{
		openAIAuthModeCredentialKey: OpenAIAuthModeAgentIdentity,
		"agent_runtime_id":          "runtime-without-private-key",
	}
	upstream := &openAIWindowOutboundHTTPStub{}
	adapter := &OpenAIWindowOutboundAdapter{httpUpstream: upstream}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(account))

	require.ErrorIs(t, err, ErrOpenAIWindowWarmupNeedsReauth)
	require.NotNil(t, result)
	require.Equal(t, OpenAIWindowWarmupAuthRefreshTerminal, result.AuthFailure.Disposition)
	require.Empty(t, upstream.requests)
}

func TestOpenAIWindowOutboundAdapterAgentIdentityRecoversInvalidTaskOnce(t *testing.T) {
	key, privateKey := newTestAgentIdentityKey(t)
	account := openAIWindowOutboundAccount()
	// The task-registration helper honors the account proxy. Keep this unit
	// test self-contained; proxy reuse is covered by the built-in adapter test.
	account.ProxyID = nil
	account.Proxy = nil
	account.Credentials = map[string]any{
		"auth_mode":          OpenAIAuthModeAgentIdentity,
		"agent_runtime_id":   key.runtimeID,
		"agent_private_key":  privateKey,
		"task_id":            "task-warmup-old",
		"chatgpt_account_id": "account-agent-warmup",
	}
	repo := &accountTestAgentIdentityRepo{account: account}
	registerCalls := 0
	registerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		registerCalls++
		_, _ = io.WriteString(w, `{"task_id":"task-warmup-new"}`)
	}))
	defer registerServer.Close()
	oldBase := openAIAgentIdentityAuthAPIBaseURL
	openAIAgentIdentityAuthAPIBaseURL = registerServer.URL
	t.Cleanup(func() { openAIAgentIdentityAuthAPIBaseURL = oldBase })

	upstream := &openAIWindowOutboundHTTPStub{responses: []*http.Response{
		{
			StatusCode: http.StatusUnauthorized,
			Header:     http.Header{"Content-Type": {"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"invalid_task_id"}}`)),
		},
		openAIWindowCompletedResponse(),
	}}
	invalidator := &agentIdentityWSInvalidationRecorder{}
	adapter := &OpenAIWindowOutboundAdapter{
		accountRepo:     repo,
		httpUpstream:    upstream,
		agentIdentityWS: invalidator,
	}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(account))
	require.NoError(t, err)
	require.True(t, result.Terminal, "status=%d terminal_type=%q requests=%d register_calls=%d", result.StatusCode, result.TerminalType, len(upstream.requests), registerCalls)
	require.Equal(t, 1, registerCalls)
	require.Equal(t, "task-warmup-new", account.GetCredential("task_id"))
	require.Len(t, upstream.requests, 2)
	for _, request := range upstream.requests {
		require.True(t, strings.HasPrefix(request.Header.Get("Authorization"), "AgentAssertion "))
		require.NotContains(t, request.Header.Get("Authorization"), privateKey)
	}
	require.NotEqual(t, upstream.requests[0].Header.Get("Authorization"), upstream.requests[1].Header.Get("Authorization"))
	require.Equal(t, []int64{account.ID}, invalidator.accountIDs)
}

func TestOpenAIWindowOutboundAdapterPreservesPostWriteEOF(t *testing.T) {
	upstream := &openAIWindowOutboundHTTPStub{err: io.ErrUnexpectedEOF, markWritten: true}
	adapter := &OpenAIWindowOutboundAdapter{
		tokenProvider: &openAIWindowOutboundTokenStub{token: "oauth-token"},
		httpUpstream:  upstream,
	}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(openAIWindowOutboundAccount()))
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.True(t, result.Started)
	require.True(t, result.EOF)
}

func TestOpenAIWindowOutboundAdapterMarksRealPartialWriteAsPossiblySent(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	readN := make(chan int, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			readN <- 0
			return
		}
		buf := make([]byte, 128)
		n, _ := conn.Read(buf)
		readN <- n
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetLinger(0)
		}
		_ = conn.Close()
	}()

	upstream := &openAIWindowPartialWriteUpstream{listener: listener}
	adapter := &OpenAIWindowOutboundAdapter{
		tokenProvider: &openAIWindowOutboundTokenStub{token: "oauth-token"},
		httpUpstream:  upstream,
	}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(openAIWindowOutboundAccount()))
	require.Error(t, err)
	require.NotNil(t, result)
	require.True(t, result.Started, "a real transport error after request bytes were written must be possibly_sent")
	require.Greater(t, <-readN, 0, "the listener must observe a request prefix")
}

func TestOpenAIWindowOutboundAdapterKeepsExplicitPreSendFailureRetryable(t *testing.T) {
	upstream := &openAIWindowOutboundHTTPStub{
		err: MarkHTTPUpstreamRequestNotSent(errors.New("proxy pool is exhausted")),
	}
	adapter := &OpenAIWindowOutboundAdapter{
		tokenProvider: &openAIWindowOutboundTokenStub{token: "oauth-token"},
		httpUpstream:  upstream,
	}

	result, err := adapter.Execute(context.Background(), openAIWindowOutboundRequest(openAIWindowOutboundAccount()))

	require.Error(t, err)
	require.NotNil(t, result)
	require.False(t, result.Started)
	require.NotContains(t, err.Error(), "possibly_sent")
}

func TestOpenAIWindowOutboundAdapterRejectsMutableOrSparkProbeContracts(t *testing.T) {
	account := openAIWindowOutboundAccount()
	base := openAIWindowOutboundRequest(account)
	tests := []struct {
		name   string
		mutate func(*OpenAIOutboundRequest)
	}{
		{name: "endpoint", mutate: func(request *OpenAIOutboundRequest) { request.Endpoint = "https://example.com/responses" }},
		{name: "payload", mutate: func(request *OpenAIOutboundRequest) {
			request.Payload = []byte(`{"model":"codex-auto-review","input":"secret"}`)
		}},
		{name: "spark", mutate: func(request *OpenAIOutboundRequest) {
			request.Model = "gpt-5.3-codex-spark"
			request.Payload = BuildOpenAIWindowWarmupPayload(request.Model)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := base
			request.Payload = append([]byte(nil), base.Payload...)
			test.mutate(&request)
			adapter := &OpenAIWindowOutboundAdapter{
				tokenProvider: &openAIWindowOutboundTokenStub{token: "oauth-token"},
				httpUpstream:  &openAIWindowOutboundHTTPStub{},
			}
			result, err := adapter.Execute(context.Background(), request)
			require.Error(t, err)
			require.Nil(t, result)
		})
	}
}
