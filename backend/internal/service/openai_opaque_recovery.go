package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ErrOpenAIOpaqueRouteUnavailable stops recovery before a different route can
// receive context which the gateway cannot safely migrate.
var ErrOpenAIOpaqueRouteUnavailable = errors.New("opaque recovery route unavailable")

type openAIOpaqueRecoveryKey struct{}

type openAIOpaqueRecovery struct {
	dispatchMu       sync.Mutex
	mu               sync.Mutex
	route            *openAIOpaqueRecoveryRoute
	dispatched       bool
	expectedRoute    string
	sourceBody       []byte
	wireBody         []byte
	wireMethod       string
	wireURL          string
	wireProxyHash    [32]byte
	fingerprint      *codexFingerprintIDs
	fingerprintReady bool
}

type openAIOpaqueRecoveryRoute struct {
	accountID, proxyID, routeID int64
	platform, accountType       string
	bindingID, identityID       string
	baseURL, egressMode         string
	proxyHash                   [32]byte
	passthrough, codex          bool
}

// WithOpenAIOpaqueRecovery adds request-local route state to a verified managed
// request. No opaque payload is stored outside the lifetime of that request.
func WithOpenAIOpaqueRecovery(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if openAIOpaqueRecoveryFromContext(ctx) != nil {
		return ctx
	}
	ctx = WithOpenAIModelDispatchBudget(ctx)
	return context.WithValue(ctx, openAIOpaqueRecoveryKey{}, &openAIOpaqueRecovery{})
}

func openAIOpaqueRecoveryFromContext(ctx context.Context) *openAIOpaqueRecovery {
	if ctx == nil || !IsOpenAIManagedRecovery(ctx) {
		return nil
	}
	state, _ := ctx.Value(openAIOpaqueRecoveryKey{}).(*openAIOpaqueRecovery)
	return state
}

func IsOpenAIOpaqueRecovery(ctx context.Context) bool {
	return openAIOpaqueRecoveryFromContext(ctx) != nil
}

func openAIOpaqueRouteForAccount(account *Account) openAIOpaqueRecoveryRoute {
	route := openAIOpaqueRecoveryRoute{
		accountID: account.ID, platform: account.Platform, accountType: account.Type,
		baseURL: account.GetOpenAIBaseURL(), egressMode: account.EgressMode,
		passthrough: account.IsOpenAIPassthroughEnabled(), codex: account.UsesOpenAICodexProtocol(),
	}
	if account.ProxyID != nil {
		route.proxyID = *account.ProxyID
	}
	if account.Proxy != nil {
		route.proxyHash = sha256.Sum256([]byte(account.Proxy.URL()))
	}
	if account.SelectedEgress != nil {
		route.bindingID = account.SelectedEgress.BindingID
		route.routeID = account.SelectedEgress.RouteID
		route.identityID = account.SelectedEgress.IdentityID
	} else if account.LegacyEgressAdmission != nil {
		route.bindingID = account.LegacyEgressAdmission.BindingID
		route.routeID = account.LegacyEgressAdmission.RouteID
		route.identityID = account.LegacyEgressAdmission.IdentityID
	}
	return route
}

// ValidateOpenAIOpaqueRecoveryRoute checks every admission/refresh after the
// first physical dispatch. Local failures before dispatch retain safe failover.
func ValidateOpenAIOpaqueRecoveryRoute(ctx context.Context, account *Account) error {
	state := openAIOpaqueRecoveryFromContext(ctx)
	if state == nil {
		return nil
	}
	if account == nil || account.ID <= 0 || AccountEgressLeaseLost(account) {
		return ErrOpenAIOpaqueRouteUnavailable
	}
	route := openAIOpaqueRouteForAccount(account)
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.dispatched && state.route != nil && *state.route != route {
		return ErrOpenAIOpaqueRouteUnavailable
	}
	return nil
}

func openAIOpaqueRecoveryPinnedRoute(ctx context.Context) (openAIOpaqueRecoveryRoute, bool) {
	state := openAIOpaqueRecoveryFromContext(ctx)
	if state == nil {
		return openAIOpaqueRecoveryRoute{}, false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.dispatched || state.route == nil {
		return openAIOpaqueRecoveryRoute{}, false
	}
	return *state.route, true
}

// OpenAIOpaqueRecoveryRoutePinned reports whether an upstream request has
// started, after which account failover is unsafe for this opaque context.
func OpenAIOpaqueRecoveryRoutePinned(ctx context.Context) bool {
	_, pinned := openAIOpaqueRecoveryPinnedRoute(ctx)
	return pinned
}

// WithOpenAIOpaqueRecoveryRouteFingerprint binds a new continuation to the
// route recorded with its prior logical response. Legacy records may be empty.
func WithOpenAIOpaqueRecoveryRouteFingerprint(ctx context.Context, fingerprint string) context.Context {
	if state := openAIOpaqueRecoveryFromContext(ctx); state != nil {
		state.mu.Lock()
		state.expectedRoute = fingerprint
		state.mu.Unlock()
	}
	return ctx
}

func openAIRecoveryRouteFingerprint(request *http.Request, proxyURL string, account *Account) string {
	route := openAIOpaqueRouteForAccount(account)
	value := fmt.Sprintf("v1:%d:%d:%d:%t:%t:%q:%q:%q:%q:%q:%q:%x:%q:%q:%x",
		route.accountID, route.proxyID, route.routeID, route.passthrough, route.codex,
		route.platform, route.accountType, route.bindingID, route.identityID, route.baseURL, route.egressMode,
		route.proxyHash, request.Method, request.URL.String(), sha256.Sum256([]byte(proxyURL)))
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}

// OpenAIRecoveryRouteFingerprint returns only a digest of the last dispatched
// route, so alias metadata can bind later opaque turns without storing secrets.
func OpenAIRecoveryRouteFingerprint(ctx context.Context) string {
	if ctx == nil || !IsOpenAIManagedRecovery(ctx) {
		return ""
	}
	budget, _ := ctx.Value(openAIModelDispatchBudgetKey{}).(*openAIModelDispatchBudget)
	if budget == nil {
		return ""
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	return budget.routeFingerprint
}

// Both HTTP errors and HTTP-200 SSE failures can reject opaque context. Keep
// their payload for the handler instead of normalizing them into retryable 502s.
func newOpenAIOpaqueContextRejection(ctx context.Context, status int, headers http.Header, payload []byte) *UpstreamFailoverError {
	if !IsOpenAIOpaqueRecovery(ctx) {
		return nil
	}
	for _, path := range []string{"response.error.code", "error.code", "code"} {
		code := strings.ToLower(strings.TrimSpace(gjson.GetBytes(payload, path).String()))
		if code == "invalid_encrypted_content" || code == "thinking_signature_invalid" {
			if status < http.StatusBadRequest {
				status = http.StatusBadRequest
			}
			return &UpstreamFailoverError{
				StatusCode: status, ResponseBody: payload, ResponseHeaders: headers.Clone(),
				Reason: "invalid_encrypted_content", NextAccountAction: NextAccountStop,
			}
		}
	}
	return nil
}

func rememberOpenAIOpaqueRequest(ctx context.Context, body []byte) {
	if state := openAIOpaqueRecoveryFromContext(ctx); state != nil {
		state.mu.Lock()
		if !state.dispatched {
			state.sourceBody = body
			state.wireBody = nil
			state.wireURL = ""
			state.fingerprint = nil
			state.fingerprintReady = false
		}
		state.mu.Unlock()
	}
}

func resolveOpenAIRecoveryFingerprintIDs(ctx context.Context, account *Account, headers http.Header) *codexFingerprintIDs {
	state := openAIOpaqueRecoveryFromContext(ctx)
	if state == nil {
		return resolveCodexFingerprintIDsFromRequest(account, headers)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.fingerprintReady {
		state.fingerprint = resolveCodexFingerprintIDsFromRequest(account, headers)
		state.fingerprintReady = true
	}
	return state.fingerprint
}

// prepareOpenAIOpaqueDispatch runs at the actual HTTP dispatch boundary, after
// normal account adaptation. Keep input and continuation context verbatim and
// replay the first prepared wire body; compatibility repairs cannot rewrite it.
func prepareOpenAIOpaqueDispatch(request *http.Request, proxyURL string, account *Account) error {
	state := openAIOpaqueRecoveryFromContext(request.Context())
	if state == nil {
		return nil
	}
	if err := ValidateOpenAIOpaqueRecoveryRoute(request.Context(), account); err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.expectedRoute != "" && state.expectedRoute != openAIRecoveryRouteFingerprint(request, proxyURL, account) {
		return ErrOpenAIOpaqueRouteUnavailable
	}
	if !state.dispatched {
		route := openAIOpaqueRouteForAccount(account)
		state.route = &route
	}
	proxyHash := sha256.Sum256([]byte(proxyURL))
	if state.wireURL != "" && (state.wireURL != request.URL.String() || state.wireMethod != request.Method || state.wireProxyHash != proxyHash) {
		return ErrOpenAIOpaqueRouteUnavailable
	}
	if state.wireBody == nil {
		if request.GetBody == nil {
			return ErrOpenAIOpaqueRouteUnavailable
		}
		reader, err := request.GetBody()
		if err != nil {
			return ErrOpenAIOpaqueRouteUnavailable
		}
		body, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return ErrOpenAIOpaqueRouteUnavailable
		}
		for _, field := range []string{"input", "previous_response_id"} {
			original := gjson.GetBytes(state.sourceBody, field)
			if original.Exists() && original.Raw != gjson.GetBytes(body, field).Raw {
				body, err = sjson.SetRawBytes(body, field, []byte(original.Raw))
				if err != nil {
					return ErrOpenAIOpaqueRouteUnavailable
				}
			}
		}
		state.wireBody = body
		state.wireURL = request.URL.String()
		state.wireMethod = request.Method
		state.wireProxyHash = proxyHash
	}
	if request.Body != nil {
		_ = request.Body.Close()
	}
	body := state.wireBody
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	request.ContentLength = int64(len(body))
	return nil
}
