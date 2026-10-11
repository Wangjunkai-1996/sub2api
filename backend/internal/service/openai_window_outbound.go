package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
)

type openAIWindowAccessTokenProvider interface {
	GetAccessToken(context.Context, *Account) (string, error)
	RefreshAfterUnauthorized(context.Context, *Account, string) (string, error)
}

type openAIWindowPluginTransport interface {
	RoundTripOpenAIOAuth(context.Context, *http.Request, string, *Account) (*http.Response, bool, error)
}

type openAIWindowTLSProfileResolver interface {
	ResolveTLSProfile(*Account) *tlsfingerprint.Profile
}

// OpenAIWindowOutboundAdapter is the production adapter for the warmup probe.
// It owns credentials and transport selection, while the probe owns the fixed
// request and the core service owns durable scheduling semantics.
type OpenAIWindowOutboundAdapter struct {
	accountRepo     AccountRepository
	tokenProvider   openAIWindowAccessTokenProvider
	httpUpstream    HTTPUpstream
	tlsProfiles     openAIWindowTLSProfileResolver
	pluginTransport openAIWindowPluginTransport
	agentIdentityWS agentIdentityWSConnectionInvalidator
	agentTaskMu     sync.Mutex
}

func NewOpenAIWindowOutboundAdapter(
	accountRepo AccountRepository,
	tokenProvider *OpenAITokenProvider,
	httpUpstream HTTPUpstream,
	tlsProfiles *TLSFingerprintProfileService,
) *OpenAIWindowOutboundAdapter {
	return &OpenAIWindowOutboundAdapter{
		accountRepo:   accountRepo,
		tokenProvider: tokenProvider,
		httpUpstream:  httpUpstream,
		tlsProfiles:   tlsProfiles,
	}
}

func (e *OpenAIWindowOutboundAdapter) SetPluginManager(manager *PluginManager) {
	if e != nil {
		e.pluginTransport = manager
	}
}

func (e *OpenAIWindowOutboundAdapter) SetAgentIdentityWSInvalidator(invalidator agentIdentityWSConnectionInvalidator) {
	if e != nil {
		e.agentIdentityWS = invalidator
	}
}

func (e *OpenAIWindowOutboundAdapter) Execute(ctx context.Context, request OpenAIOutboundRequest) (*OpenAIOutboundResult, error) {
	if e == nil {
		return nil, fmt.Errorf("%w: outbound adapter is nil", ErrOpenAIWindowWarmupBlockedConfig)
	}
	if err := validateOpenAIWindowOutboundRequest(request); err != nil {
		return nil, err
	}
	if e.httpUpstream == nil && e.pluginTransport == nil {
		return nil, fmt.Errorf("%w: outbound transport is not configured", ErrOpenAIWindowWarmupBlockedConfig)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	timeout := request.Timeout
	if timeout <= 0 {
		timeout = openAIWindowWarmupDefaultTimeout
	}
	execCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	request, auth, err := e.prepareAuthorizedAttempt(execCtx, request)
	if err != nil {
		return openAIWindowAuthorizationFailure(request.Account, err)
	}
	result, err := e.executePrepared(execCtx, request, auth)
	if result == nil || result.StatusCode != http.StatusUnauthorized {
		return annotateOpenAIWindowAuthResult(result, request.Account, ""), err
	}
	// Only a refreshable OAuth bearer can recover from the status alone. Agent
	// Identity recovery depends on a complete structured error body, while PAT
	// and access-token-only accounts have no safe replay credential.
	if err != nil && (request.Account.IsOpenAIAgentIdentity() || e.tokenProvider == nil ||
		request.Account.IsOpenAIPersonalAccessToken() || strings.TrimSpace(request.Account.GetOpenAIRefreshToken()) == "") {
		return annotateOpenAIWindowAuthResult(result, request.Account, ""), err
	}

	// A 401 is authoritative evidence that the warmup request was rejected, so
	// one credential recovery and replay is safe. No other response is replayed.
	request, err = e.prepareAttempt(execCtx, request)
	if err != nil {
		return result, err
	}
	var refreshedToken, rejectedToken string
	if request.Account.IsOpenAIAgentIdentity() {
		if !isAgentIdentityTaskInvalidHTTPResponse(result.StatusCode, result.Body) {
			return annotateOpenAIWindowAuthResult(result, request.Account, OpenAIWindowWarmupAuthRefreshTerminal), nil
		}
		expectedTaskID := strings.TrimSpace(request.Account.GetCredential("task_id"))
		if recoverErr := ensureAgentIdentityTaskForAccount(
			execCtx,
			e.accountRepo,
			e.agentIdentityWS,
			&e.agentTaskMu,
			request.Account,
			expectedTaskID,
		); recoverErr != nil {
			disposition := OpenAIWindowWarmupAuthRefreshTransient
			if isPermanentWarmupAgentIdentityError(recoverErr) {
				disposition = OpenAIWindowWarmupAuthRefreshTerminal
			}
			result = annotateOpenAIWindowAuthResult(result, request.Account, disposition)
			if disposition == OpenAIWindowWarmupAuthRefreshTransient {
				return result, withOpenAIWindowWarmupAuthFailure(errors.New("openai agent identity recovery failed"), result.AuthFailure)
			}
			return result, nil
		}
	} else {
		if e.tokenProvider == nil || request.Account.IsOpenAIPersonalAccessToken() ||
			strings.TrimSpace(request.Account.GetOpenAIRefreshToken()) == "" {
			return annotateOpenAIWindowAuthResult(result, request.Account, OpenAIWindowWarmupAuthNotRefreshable), nil
		}
		rejectedToken = strings.TrimSpace(strings.TrimPrefix(auth.Get("Authorization"), "Bearer "))
		refreshedToken, err = e.tokenProvider.RefreshAfterUnauthorized(execCtx, request.Account, rejectedToken)
		if err == nil {
			refreshedToken = strings.TrimSpace(refreshedToken)
			if refreshedToken == "" {
				err = fmt.Errorf("%w: token provider returned an empty refreshed token", ErrOpenAIWindowWarmupNeedsReauth)
			}
		}
	}
	if err != nil {
		if errors.Is(err, errOpenAITokenRefreshInProgress) {
			failure := newOpenAIWindowWarmupAuthFailure(request.Account, http.StatusUnauthorized, OpenAIWindowWarmupAuthRefreshInProgress)
			return &OpenAIOutboundResult{AuthFailure: failure}, withOpenAIWindowWarmupAuthFailure(fmt.Errorf("token_refresh_in_progress: %w", err), failure)
		}
		disposition := OpenAIWindowWarmupAuthRefreshTransient
		if isOpenAIWindowTerminalRefreshError(err) {
			disposition = OpenAIWindowWarmupAuthRefreshTerminal
		}
		result = annotateOpenAIWindowAuthResult(result, request.Account, disposition)
		if disposition == OpenAIWindowWarmupAuthRefreshTransient {
			return result, withOpenAIWindowWarmupAuthFailure(errors.New("openai oauth refresh failed"), result.AuthFailure)
		}
		return result, nil
	}
	var (
		replayRequest OpenAIOutboundRequest
		replayAuth    http.Header
		prepareErr    error
	)
	if request.Account.IsOpenAIAgentIdentity() {
		// Agent Identity assertions are generated from the current task and must
		// continue through the normal authorization path after recovery.
		replayRequest, replayAuth, prepareErr = e.prepareAuthorizedAttempt(execCtx, request)
	} else {
		// OAuth replay uses the refresh result captured above. prepareAttempt still
		// reloads and fences the account before any second POST is attempted.
		replayRequest, prepareErr = e.prepareAttempt(execCtx, request)
		if prepareErr == nil {
			replayToken := refreshedToken
			if replayRequest.SendGuard != nil {
				// The identity lease below locks replayRequest.Account.Credentials.
				// Use the bearer from that same durable snapshot so a concurrent
				// refresh cannot leave us locking T3 while replaying stale T2.
				durableToken := strings.TrimSpace(replayRequest.Account.GetOpenAIAccessToken())
				if durableToken == "" || durableToken == rejectedToken {
					prepareErr = fmt.Errorf("%w: refreshed bearer is not durable", ErrOpenAIWindowWarmupCredentialsChanged)
				} else {
					replayToken = durableToken
				}
			}
			// Unguarded callers do not acquire an identity lease and keep the exact
			// refresh result, avoiding another read through a lagging token cache.
			replayAuth = make(http.Header)
			replayAuth.Set("Authorization", "Bearer "+replayToken)
		}
	}
	if prepareErr != nil {
		if errors.Is(prepareErr, ErrOpenAIWindowWarmupSendGuardClosed) ||
			errors.Is(prepareErr, ErrOpenAIWindowWarmupCredentialsChanged) {
			return result, prepareErr
		}
		failureResult, failureErr := openAIWindowAuthorizationFailure(request.Account, prepareErr)
		if failureResult != nil && failureResult.AuthFailure != nil {
			result.AuthFailure = failureResult.AuthFailure
		}
		return result, failureErr
	}
	result, err = e.executePrepared(execCtx, replayRequest, replayAuth)
	return annotateOpenAIWindowAuthResult(result, replayRequest.Account, OpenAIWindowWarmupAuthReplayRejected), err
}

func openAIWindowAuthorizationFailure(account *Account, err error) (*OpenAIOutboundResult, error) {
	wrapped := fmt.Errorf("resolve openai warmup authorization: %w", err)
	if !errors.Is(err, ErrOpenAIWindowWarmupNeedsReauth) {
		return nil, wrapped
	}
	disposition := OpenAIWindowWarmupAuthRefreshTerminal
	if account != nil && !account.IsOpenAIAgentIdentity() &&
		(account.IsOpenAIPersonalAccessToken() || strings.TrimSpace(account.GetOpenAIRefreshToken()) == "") {
		disposition = OpenAIWindowWarmupAuthNotRefreshable
	}
	failure := newOpenAIWindowWarmupAuthFailure(account, http.StatusUnauthorized, disposition)
	return &OpenAIOutboundResult{AuthFailure: failure}, withOpenAIWindowWarmupAuthFailure(wrapped, failure)
}

func newOpenAIWindowWarmupAuthFailure(account *Account, status int, disposition OpenAIWindowWarmupAuthDisposition) *OpenAIWindowWarmupAuthFailure {
	failure := &OpenAIWindowWarmupAuthFailure{StatusCode: status, Disposition: disposition}
	if account != nil {
		failure.AccountID = account.ID
		failure.ExpectedCredentials = shallowCopyMap(account.Credentials)
	}
	return failure
}

func annotateOpenAIWindowAuthResult(result *OpenAIOutboundResult, account *Account, disposition OpenAIWindowWarmupAuthDisposition) *OpenAIOutboundResult {
	if result == nil {
		return nil
	}
	switch result.StatusCode {
	case http.StatusUnauthorized:
		if disposition == "" {
			disposition = OpenAIWindowWarmupAuthRefreshTerminal
		}
	case http.StatusForbidden:
		if isHTMLResponse(result.Body) {
			disposition = OpenAIWindowWarmupAuthForbiddenHTML
		} else {
			disposition = OpenAIWindowWarmupAuthForbidden
		}
	default:
		return result
	}
	result.AuthFailure = newOpenAIWindowWarmupAuthFailure(account, result.StatusCode, disposition)
	return result
}

func isOpenAIWindowTerminalRefreshError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"invalid_grant", "invalid_refresh_token", "token_expired", "app_session_terminated",
		"refresh_token_reused", "refresh_token_invalidated", "access_denied",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func validateOpenAIWindowOutboundRequest(request OpenAIOutboundRequest) error {
	if !warmupProbeAccountEligible(request.Account) {
		return fmt.Errorf("%w: outbound account is ineligible", ErrOpenAIWindowWarmupBlockedConfig)
	}
	if strings.TrimSpace(request.Endpoint) != chatgptCodexURL {
		return fmt.Errorf("%w: outbound endpoint is not allowed", ErrOpenAIWindowWarmupBlockedConfig)
	}
	model, err := NormalizeOpenAIWindowWarmupProbeModel(request.Model)
	if err != nil {
		return err
	}
	expected := BuildOpenAIWindowWarmupPayload(model)
	if !bytes.Equal(request.Payload, expected) {
		return fmt.Errorf("%w: outbound payload does not match the fixed probe contract", ErrOpenAIWindowWarmupBlockedConfig)
	}
	return nil
}

func (e *OpenAIWindowOutboundAdapter) prepareAttempt(ctx context.Context, request OpenAIOutboundRequest) (OpenAIOutboundRequest, error) {
	if request.SendGuard != nil {
		if err := request.SendGuard.Check(ctx, request.Account.ID); err != nil {
			return request, fmt.Errorf("%w: %v", ErrOpenAIWindowWarmupSendGuardClosed, err)
		}
		if request.IdentityGeneration <= 0 || e.accountRepo == nil {
			return request, fmt.Errorf("%w: guarded outbound identity fence is not configured", ErrOpenAIWindowWarmupBlockedConfig)
		}
	}
	if request.IdentityGeneration <= 0 || e.accountRepo == nil {
		return request, nil
	}
	latest, err := e.accountRepo.GetByID(ctx, request.Account.ID)
	if err != nil || latest == nil {
		if err == nil {
			err = ErrAccountNotFound
		}
		return request, fmt.Errorf("reload openai warmup account: %w", err)
	}
	if latest.OpenAIWarmupIdentityGeneration != request.IdentityGeneration ||
		openAIWindowWarmupIdentityKey(latest) != openAIWindowWarmupIdentityKey(request.Account) {
		return request, ErrOpenAIWindowWarmupCredentialsChanged
	}
	if request.SendGuard != nil &&
		(OpenAIWindowWarmupPolicyForAccount(latest) != OpenAIWindowWarmupPolicyForAccount(request.Account) ||
			!sameOpenAIWindowWarmupTime(latest.LastUsedAt, request.Account.LastUsedAt)) {
		return request, ErrOpenAIWindowWarmupCredentialsChanged
	}
	request.Account = latest
	return request, nil
}

func sameOpenAIWindowWarmupTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func (e *OpenAIWindowOutboundAdapter) prepareAuthorizedAttempt(ctx context.Context, request OpenAIOutboundRequest) (OpenAIOutboundRequest, http.Header, error) {
	prepared, err := e.prepareAttempt(ctx, request)
	if err != nil {
		return request, nil, err
	}
	credentialSnapshot := shallowCopyMap(prepared.Account.Credentials)
	auth, err := e.authorizationHeaders(ctx, prepared.Account)
	if err != nil {
		return prepared, nil, err
	}
	// Token refresh and Agent Identity task registration may have persisted new
	// credentials while authorization was being resolved. Reload once more and
	// build the headers from that exact durable snapshot used for the send.
	prepared, err = e.prepareAttempt(ctx, prepared)
	if err != nil {
		return prepared, nil, err
	}
	if !reflect.DeepEqual(credentialSnapshot, prepared.Account.Credentials) {
		auth, err = e.authorizationHeaders(ctx, prepared.Account)
		if err != nil {
			return prepared, nil, err
		}
	}
	return prepared, auth, nil
}

func (e *OpenAIWindowOutboundAdapter) authorizationHeaders(ctx context.Context, account *Account) (http.Header, error) {
	if account == nil {
		return nil, errors.New("account is nil")
	}
	if account.IsOpenAIAgentIdentity() {
		headers, err := buildAgentIdentityAuthenticationHeaders(ctx, e.accountRepo, e.agentIdentityWS, &e.agentTaskMu, account)
		if err != nil {
			if isPermanentWarmupAgentIdentityError(err) {
				return nil, fmt.Errorf("%w: agent identity credentials are invalid", ErrOpenAIWindowWarmupNeedsReauth)
			}
			return nil, err
		}
		return headers, nil
	}
	if e.tokenProvider == nil {
		return nil, fmt.Errorf("%w: token provider is not configured", ErrOpenAIWindowWarmupBlockedConfig)
	}
	token, err := e.tokenProvider.GetAccessToken(ctx, account)
	if err != nil {
		if account.IsOpenAIPersonalAccessToken() || strings.TrimSpace(account.GetOpenAIRefreshToken()) == "" {
			return nil, fmt.Errorf("%w: access token unavailable", ErrOpenAIWindowWarmupNeedsReauth)
		}
		return nil, err
	}
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("%w: token provider returned an empty token", ErrOpenAIWindowWarmupNeedsReauth)
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+token)
	return headers, nil
}

func isPermanentWarmupAgentIdentityError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{
		"private key", "runtime id is missing", "runtime or task id is missing",
		"credentials are unavailable", "failed to sign agent", "encrypted agent task id",
		"decryption key", "decrypt encrypted agent task id", "decrypted agent task id is empty",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func (e *OpenAIWindowOutboundAdapter) executePrepared(
	ctx context.Context,
	request OpenAIOutboundRequest,
	auth http.Header,
) (*OpenAIOutboundResult, error) {
	if request.SendGuard != nil {
		if err := request.SendGuard.Check(ctx, request.Account.ID); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrOpenAIWindowWarmupSendGuardClosed, err)
		}
		leaseRepo, ok := e.accountRepo.(OpenAIWindowWarmupIdentityLeaseRepository)
		if !ok {
			return nil, fmt.Errorf("%w: guarded outbound identity lease is not configured", ErrOpenAIWindowWarmupBlockedConfig)
		}
		release, acquired, err := leaseRepo.AcquireOpenAIWindowWarmupIdentityLease(
			ctx,
			request.Account.ID,
			request.IdentityGeneration,
			OpenAIWindowWarmupPolicyForAccount(request.Account),
			request.Account.LastUsedAt,
			request.Account.Credentials,
			request.Account.ProxyID,
			request.Account.Proxy,
		)
		if err != nil {
			return nil, fmt.Errorf("acquire openai warmup identity lease: %w", err)
		}
		if !acquired {
			return nil, ErrOpenAIWindowWarmupCredentialsChanged
		}
		defer release()
		// The row lock may have waited for an earlier credential update. Recheck
		// dynamic policy only after the exact durable identity is locked.
		if err := request.SendGuard.Check(ctx, request.Account.ID); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrOpenAIWindowWarmupSendGuardClosed, err)
		}
	}
	var wroteRequest atomic.Bool
	var gotFirstByte atomic.Bool
	trace := &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			// WroteRequest is called after net/http has attempted to write the
			// request.  A non-nil Err can mean that only a prefix was written;
			// treating that callback as "not sent" would permit a duplicate POST.
			// The error itself is retained by the transport path, while this bit
			// deliberately records the conservative "may have been sent" fact.
			_ = info
			wroteRequest.Store(true)
		},
		GotFirstResponseByte: func() { gotFirstByte.Store(true) },
	}
	requestCtx := httptrace.WithClientTrace(ctx, trace)
	requestCtx = WithHTTPUpstreamProfile(requestCtx, HTTPUpstreamProfileOpenAI)
	// This request carries an OAuth bearer/agent assertion and must never be
	// replayed by a redirecting client.  The built-in HTTP adapter enforces the
	// marker; plugin transports receive the same marker and must honor the host
	// contract before opening their upstream connection.
	requestCtx = WithHTTPUpstreamRedirectsDisabled(requestCtx)
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, chatgptCodexURL, bytes.NewReader(request.Payload))
	if err != nil {
		return nil, fmt.Errorf("create openai warmup request: %w", err)
	}
	req.Host = "chatgpt.com"
	req.Header = cloneWarmupHeaders(request.Headers)
	setOpenAIChatGPTAccountHeaders(req.Header, request.Account)
	enforceCodexIdentityHeadersWithUA(req.Header, request.Account.GetOpenAIUserAgent())
	request.Account.ApplyHeaderOverrides(req.Header)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("OpenAI-Beta", "responses=experimental")
	for key, values := range auth {
		req.Header.Del(key)
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	proxyURL := ""
	if request.Account.ProxyID != nil && request.Account.Proxy != nil {
		proxyURL = request.Account.Proxy.URL()
	}
	var resp *http.Response
	if e.pluginTransport != nil {
		var handled bool
		resp, handled, err = e.pluginTransport.RoundTripOpenAIOAuth(requestCtx, req, proxyURL, request.Account)
		if handled {
			if err == nil && resp == nil {
				// Once a plugin claims the request, an empty response is a protocol
				// failure rather than evidence that no upstream call happened. Fence
				// any replay conservatively because the plugin may have submitted the
				// POST before losing its response stream.
				err = &PluginTransportError{
					Code:        "PLUGIN_EMPTY_RESPONSE",
					Message:     "plugin claimed the request but returned no response",
					RequestSent: true,
				}
			}
			if err != nil {
				var pluginErr *PluginTransportError
				if errors.As(err, &pluginErr) && pluginErr != nil && pluginErr.RequestSent {
					wroteRequest.Store(true)
				}
				if (pluginErr == nil || pluginErr.RequestSent) &&
					(errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) {
					wroteRequest.Store(true)
				}
				knownNotSent := false
				if pluginErr != nil && !pluginErr.RequestSent {
					// The plugin protocol explicitly permits false only when it
					// knows the upstream Transport was never called.
					knownNotSent = true
				}
				return outboundTransportFailure(err, wroteRequest.Load(), gotFirstByte.Load(), knownNotSent)
			}
			return readOpenAIWindowOutboundResponse(resp)
		}
	}
	if e.httpUpstream == nil {
		return nil, errors.New("openai warmup built-in transport is not configured")
	}
	var profile *tlsfingerprint.Profile
	if e.tlsProfiles != nil {
		profile = e.tlsProfiles.ResolveTLSProfile(request.Account)
	}
	resp, err = e.httpUpstream.DoWithTLS(req, proxyURL, request.Account.ID, request.Account.Concurrency, profile)
	if err != nil {
		return outboundTransportFailure(err, wroteRequest.Load(), gotFirstByte.Load(), IsHTTPUpstreamRequestNotSent(err))
	}
	return readOpenAIWindowOutboundResponse(resp)
}

func outboundTransportFailure(err error, wroteRequest, gotFirstByte, knownNotSent bool) (*OpenAIOutboundResult, error) {
	started := wroteRequest || gotFirstByte
	if !started && err != nil && !knownNotSent {
		// The built-in HTTPUpstream exposes only a returned error, not the point
		// at which its RoundTripper failed.  DNS/TLS/connection-reset failures
		// can race with request transmission, so absence of a trace callback is
		// not proof that the POST was never accepted.  Mark it as possibly sent
		// and let the passive reconciliation path fence replay.
		started = true
	}
	result := &OpenAIOutboundResult{
		Started: started,
		EOF:     errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF),
	}
	return result, err
}

func readOpenAIWindowOutboundResponse(resp *http.Response) (*OpenAIOutboundResult, error) {
	if resp == nil {
		return nil, errors.New("openai warmup transport returned an empty response")
	}
	if resp.Body == nil {
		resp.Body = http.NoBody
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, openAIWindowWarmupMaxBodyBytes+1))
	_ = resp.Body.Close()
	truncated := len(body) > openAIWindowWarmupMaxBodyBytes
	if truncated {
		body = body[:openAIWindowWarmupMaxBodyBytes]
	}
	bodyEvidence := parseWarmupSSEEvidence(body)
	resetAt := warmupResetFromHeaders(resp.Header)
	if resetAt == nil {
		resetAt = bodyEvidence.ResetAt
	}
	result := &OpenAIOutboundResult{
		StatusCode:   resp.StatusCode,
		Headers:      cloneWarmupHeaders(resp.Header),
		Body:         append([]byte(nil), body...),
		Terminal:     bodyEvidence.Terminal,
		TerminalType: bodyEvidence.TerminalType,
		ResetAt:      cloneWarmupTime(resetAt),
		Started:      true,
		EOF:          readErr == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 && !bodyEvidence.Terminal,
		RequestID:    strings.TrimSpace(resp.Header.Get("x-request-id")),
	}
	if truncated {
		return result, errors.New("possibly_sent: openai warmup response exceeded evidence limit")
	}
	if readErr != nil {
		result.EOF = errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) || !bodyEvidence.Terminal
		return result, fmt.Errorf("read openai warmup response: %w", readErr)
	}
	return result, nil
}
