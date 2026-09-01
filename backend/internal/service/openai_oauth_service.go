package service

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
)

// OpenAIOAuthService handles OpenAI OAuth authentication flows
type OpenAIOAuthService struct {
	sessionStore         *openai.SessionStore
	proxyRepo            ProxyRepository
	egressService        *EgressService
	settingService       *SettingService
	oauthClient          OpenAIOAuthClient
	privacyClientFactory PrivacyClientFactory // 用于调用 chatgpt.com/backend-api（ImpersonateChrome）
}

// NewOpenAIOAuthService creates a new OpenAI OAuth service
func NewOpenAIOAuthService(proxyRepo ProxyRepository, oauthClient OpenAIOAuthClient) *OpenAIOAuthService {
	return &OpenAIOAuthService{
		sessionStore: openai.NewSessionStore(),
		proxyRepo:    proxyRepo,
		oauthClient:  oauthClient,
	}
}

// SetPrivacyClientFactory 注入 ImpersonateChrome 客户端工厂，
// 用于调用 chatgpt.com/backend-api 获取账号信息（plan_type 等）。
func (s *OpenAIOAuthService) SetPrivacyClientFactory(factory PrivacyClientFactory) {
	s.privacyClientFactory = factory
}

// SetSettingService injects the settings resolver used by OAuth bootstrap
// flows. The setter keeps the historical constructor signature compatible with
// focused service tests and other callers that do not need defaults.
func (s *OpenAIOAuthService) SetSettingService(settingService *SettingService) {
	s.settingService = settingService
}

// SetEgressService injects the route resolver used to fence OAuth sessions to
// one stable egress without persisting a credential-bearing proxy URL.
func (s *OpenAIOAuthService) SetEgressService(egressService *EgressService) {
	s.egressService = egressService
}

type openAIOAuthEgressSelection struct {
	RouteID         int64
	RouteRevision   int64
	ProxyID         *int64
	Direct          bool
	RequireVerified bool
}

// ResolveOpenAIOAuthProxyURL resolves an explicit proxy ID first, then the
// optional system default. An absent default intentionally returns an empty URL
// (direct connection); malformed, missing, or inactive configured defaults are
// returned as errors and must never silently become direct connections.
func (s *OpenAIOAuthService) ResolveOpenAIOAuthProxyURL(ctx context.Context, proxyID *int64) (string, error) {
	proxy, err := s.resolveOpenAIOAuthProxy(ctx, proxyID)
	if err != nil {
		return "", err
	}
	if proxy == nil {
		return "", nil
	}
	return proxy.URL(), nil
}

// ResolveOpenAIOAuthEgressURL resolves either the pool-aware route selector or
// the legacy proxy selector for one control-plane request. It returns the URL
// only to the immediate transport caller; no session or DTO persists it.
func (s *OpenAIOAuthService) ResolveOpenAIOAuthEgressURL(ctx context.Context, proxyID, routeID *int64) (string, error) {
	selection, err := s.resolveOpenAIOAuthSelection(ctx, proxyID, routeID)
	if err != nil {
		return "", err
	}
	if selection.RouteID > 0 {
		route, err := s.egressService.GetRoute(ctx, selection.RouteID)
		if err != nil {
			return "", err
		}
		return openAIOAuthRouteProxyURL(route, selection.RequireVerified)
	}
	if selection.Direct {
		return "", nil
	}
	return s.ResolveOpenAIOAuthProxyURL(ctx, selection.ProxyID)
}

// ResolveOpenAIAccountControlProxyURL keeps token refresh, privacy, quota and
// other account-scoped control traffic on the pool's current primary route.
// Control traffic deliberately does not acquire a business concurrency lease.
func (s *OpenAIOAuthService) ResolveOpenAIAccountControlProxyURL(ctx context.Context, account *Account) (string, error) {
	if account == nil || account.Platform != PlatformOpenAI {
		return "", infraerrors.New(http.StatusBadRequest, "OPENAI_OAUTH_INVALID_ACCOUNT", "account is not an OpenAI account")
	}
	if account.EgressMode != EgressModePool {
		return s.ResolveOpenAIOAuthProxyURL(ctx, account.ProxyID)
	}
	if s == nil || s.egressService == nil {
		return "", infraerrors.New(http.StatusServiceUnavailable, "OPENAI_OAUTH_EGRESS_ROUTE_UNAVAILABLE", "egress route resolver is unavailable")
	}
	var primary *AccountEgressBinding
	for i := range account.EgressBindings {
		binding := &account.EgressBindings[i]
		if !binding.IsPrimary {
			continue
		}
		if primary != nil {
			return "", infraerrors.New(http.StatusServiceUnavailable, "OPENAI_OAUTH_EGRESS_ROUTE_UNAVAILABLE", "account has multiple primary egress routes")
		}
		primary = binding
	}
	if primary == nil || primary.Status != AccountEgressBindingStatusActive {
		return "", infraerrors.New(http.StatusServiceUnavailable, "OPENAI_OAUTH_EGRESS_ROUTE_UNAVAILABLE", "account primary egress route is unavailable")
	}
	route, err := s.egressService.GetRoute(ctx, primary.RouteID)
	if err != nil {
		return "", err
	}
	return openAIOAuthRouteProxyURL(route, true)
}

func (s *OpenAIOAuthService) resolveOpenAIOAuthProxy(ctx context.Context, proxyID *int64) (*Proxy, error) {
	if proxyID != nil {
		if s == nil || s.proxyRepo == nil {
			return nil, infraerrors.New(http.StatusBadRequest, "OPENAI_OAUTH_PROXY_NOT_FOUND", "proxy repository is unavailable")
		}
		proxy, err := s.proxyRepo.GetByID(ctx, *proxyID)
		if err != nil {
			if !errors.Is(err, ErrProxyNotFound) {
				return nil, infraerrors.Newf(http.StatusInternalServerError, "OPENAI_OAUTH_PROXY_READ_FAILED", "failed to load proxy %d: %v", *proxyID, err)
			}
			return nil, infraerrors.Newf(http.StatusBadRequest, "OPENAI_OAUTH_PROXY_NOT_FOUND", "proxy not found: %v", err)
		}
		if proxy == nil {
			return nil, infraerrors.Newf(http.StatusBadRequest, "OPENAI_OAUTH_PROXY_NOT_FOUND", "proxy not found: %d", *proxyID)
		}
		if !proxy.IsActive() || proxy.IsExpired(time.Now()) {
			return nil, infraerrors.New(http.StatusBadRequest, "OPENAI_OAUTH_PROXY_UNAVAILABLE", "proxy is inactive or expired")
		}
		return proxy, nil
	}

	if s == nil || s.settingService == nil {
		return nil, nil
	}
	proxy, err := s.settingService.ResolveOpenAIOAuthDefaultProxy(ctx)
	if err != nil {
		return nil, err
	}
	return proxy, nil
}

func (s *OpenAIOAuthService) resolveOpenAIOAuthSelection(ctx context.Context, proxyID, routeID *int64) (*openAIOAuthEgressSelection, error) {
	if proxyID != nil && routeID != nil {
		return nil, infraerrors.New(http.StatusBadRequest, "OPENAI_OAUTH_EGRESS_CONFLICT", "proxy_id and egress_route_id cannot be used together")
	}
	if routeID != nil {
		if *routeID <= 0 || s == nil || s.egressService == nil {
			return nil, infraerrors.New(http.StatusBadRequest, "OPENAI_OAUTH_EGRESS_ROUTE_UNAVAILABLE", "egress route is unavailable")
		}
		route, err := s.egressService.GetRoute(ctx, *routeID)
		if err != nil {
			return nil, err
		}
		if _, err := openAIOAuthRouteProxyURL(route, true); err != nil {
			return nil, err
		}
		return openAIOAuthSelectionFromRoute(route, true), nil
	}

	proxy, err := s.resolveOpenAIOAuthProxy(ctx, proxyID)
	if err != nil {
		return nil, err
	}
	selection := &openAIOAuthEgressSelection{Direct: proxy == nil}
	if proxy != nil {
		selection.ProxyID = cloneOpenAIOAuthInt64(&proxy.ID)
	}
	// Legacy OAuth sessions are fenced by their proxy ID (or the explicit
	// direct marker), not by an egress route revision. Routine route probes may
	// advance route revisions and must not invalidate an authorization that did
	// not opt into the pool contract.
	return selection, nil
}

func openAIOAuthSelectionFromRoute(route *EgressRoute, requireVerified bool) *openAIOAuthEgressSelection {
	selection := &openAIOAuthEgressSelection{RequireVerified: requireVerified}
	if route == nil {
		return selection
	}
	selection.RouteID = route.ID
	selection.RouteRevision = route.Revision
	selection.ProxyID = cloneOpenAIOAuthInt64(route.ProxyID)
	selection.Direct = route.Kind == EgressRouteKindDirect
	return selection
}

func (s *OpenAIOAuthService) resolveOpenAIOAuthSessionProxyURL(
	ctx context.Context,
	session *openai.OAuthSession,
	proxyID, routeID *int64,
) (string, error) {
	if session == nil {
		return "", infraerrors.New(http.StatusBadRequest, "OPENAI_OAUTH_SESSION_NOT_FOUND", "session not found or expired")
	}
	if proxyID != nil && routeID != nil {
		return "", infraerrors.New(http.StatusBadRequest, "OPENAI_OAUTH_EGRESS_CONFLICT", "proxy_id and egress_route_id cannot be used together")
	}

	if session.EgressRouteID > 0 {
		if routeID != nil && *routeID != session.EgressRouteID {
			return "", infraerrors.New(http.StatusConflict, "OPENAI_OAUTH_EGRESS_SESSION_STALE", "OAuth egress route changed; restart authorization")
		}
		if s == nil || s.egressService == nil {
			return "", infraerrors.New(http.StatusServiceUnavailable, "OPENAI_OAUTH_EGRESS_ROUTE_UNAVAILABLE", "egress route resolver is unavailable")
		}
		route, err := s.egressService.GetRoute(ctx, session.EgressRouteID)
		if err != nil {
			return "", err
		}
		if route.Revision != session.EgressRouteRevision {
			return "", infraerrors.New(http.StatusConflict, "OPENAI_OAUTH_EGRESS_SESSION_STALE", "OAuth egress route changed; restart authorization")
		}
		if proxyID != nil && (route.ProxyID == nil || *route.ProxyID != *proxyID) {
			return "", infraerrors.New(http.StatusConflict, "OPENAI_OAUTH_EGRESS_SESSION_STALE", "OAuth egress route changed; restart authorization")
		}
		return openAIOAuthRouteProxyURL(route, session.RequireVerifiedEgress)
	}

	// Compatibility for sessions created by older focused tests or by a service
	// instance without an egress repository. The session still stores only an
	// ID/direct marker, never a credential-bearing URL.
	if session.ProxyID != nil {
		if routeID != nil || (proxyID != nil && *proxyID != *session.ProxyID) {
			return "", infraerrors.New(http.StatusConflict, "OPENAI_OAUTH_EGRESS_SESSION_STALE", "OAuth proxy changed; restart authorization")
		}
		return s.ResolveOpenAIOAuthProxyURL(ctx, session.ProxyID)
	}
	if session.DirectEgress {
		if proxyID != nil || routeID != nil {
			return "", infraerrors.New(http.StatusConflict, "OPENAI_OAUTH_EGRESS_SESSION_STALE", "OAuth egress changed; restart authorization")
		}
		return "", nil
	}
	if routeID != nil {
		selection, err := s.resolveOpenAIOAuthSelection(ctx, nil, routeID)
		if err != nil {
			return "", err
		}
		route, err := s.egressService.GetRoute(ctx, selection.RouteID)
		if err != nil {
			return "", err
		}
		return openAIOAuthRouteProxyURL(route, true)
	}
	return s.ResolveOpenAIOAuthProxyURL(ctx, proxyID)
}

func openAIOAuthRouteProxyURL(route *EgressRoute, requireVerified bool) (string, error) {
	if route == nil || route.ID <= 0 || route.Revision <= 0 {
		return "", infraerrors.New(http.StatusServiceUnavailable, "OPENAI_OAUTH_EGRESS_ROUTE_UNAVAILABLE", "egress route is unavailable")
	}
	if route.State == EgressRouteStateExpired || route.State == EgressRouteStateRetired || route.State == EgressRouteStateInactive || route.State == EgressRouteStateIdentityMismatch {
		return "", infraerrors.New(http.StatusServiceUnavailable, "OPENAI_OAUTH_EGRESS_ROUTE_UNAVAILABLE", "egress route is unavailable")
	}
	if requireVerified && (route.State != EgressRouteStateActive || route.ExpectedIdentity == nil || route.ExpectedIdentity.Status != EgressIdentityStatusActive) {
		return "", infraerrors.New(http.StatusServiceUnavailable, "OPENAI_OAUTH_EGRESS_ROUTE_UNVERIFIED", "egress route must be verified before OAuth authorization")
	}
	switch route.Kind {
	case EgressRouteKindDirect:
		if route.RuntimeScope == nil || strings.TrimSpace(*route.RuntimeScope) != DefaultDirectEgressRuntimeScope {
			return "", infraerrors.New(http.StatusServiceUnavailable, "OPENAI_OAUTH_EGRESS_ROUTE_UNAVAILABLE", "direct egress route is unavailable")
		}
		return "", nil
	case EgressRouteKindProxy:
		if route.ProxyID == nil || route.Proxy == nil || !route.Proxy.IsActive() || route.Proxy.IsExpired(time.Now()) {
			return "", infraerrors.New(http.StatusServiceUnavailable, "OPENAI_OAUTH_EGRESS_ROUTE_UNAVAILABLE", "proxy egress route is unavailable")
		}
		return route.Proxy.URL(), nil
	default:
		return "", infraerrors.New(http.StatusServiceUnavailable, "OPENAI_OAUTH_EGRESS_ROUTE_UNAVAILABLE", "egress route is unavailable")
	}
}

func cloneOpenAIOAuthInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// ResolveOpenAIOAuthDefaultProxyID returns the configured default proxy ID for
// account-import paths that persist an Account directly instead of going
// through AdminService. An absent setting remains a deliberate no-op.
func (s *OpenAIOAuthService) ResolveOpenAIOAuthDefaultProxyID(ctx context.Context) (*int64, error) {
	if s == nil || s.settingService == nil {
		return nil, nil
	}
	proxy, err := s.settingService.ResolveOpenAIOAuthDefaultProxy(ctx)
	if err != nil {
		return nil, err
	}
	if proxy == nil {
		return nil, nil
	}
	proxyID := proxy.ID
	return &proxyID, nil
}

// OpenAIAuthURLResult contains the authorization URL and session info
type OpenAIAuthURLResult struct {
	AuthURL   string `json:"auth_url"`
	SessionID string `json:"session_id"`
}

// GenerateAuthURL generates an OpenAI OAuth authorization URL
func (s *OpenAIOAuthService) GenerateAuthURL(ctx context.Context, proxyID *int64, redirectURI, platform string) (*OpenAIAuthURLResult, error) {
	return s.GenerateAuthURLWithRoute(ctx, proxyID, nil, redirectURI, platform)
}

// GenerateAuthURLWithRoute supports the pool-aware OAuth contract while
// retaining proxy_id compatibility for legacy callers. A route and proxy may
// never be supplied together because that would make the session's egress
// fence ambiguous.
func (s *OpenAIOAuthService) GenerateAuthURLWithRoute(ctx context.Context, proxyID, routeID *int64, redirectURI, platform string) (*OpenAIAuthURLResult, error) {
	selection, err := s.resolveOpenAIOAuthSelection(ctx, proxyID, routeID)
	if err != nil {
		return nil, err
	}

	// Generate PKCE values
	state, err := openai.GenerateState()
	if err != nil {
		return nil, infraerrors.Newf(http.StatusInternalServerError, "OPENAI_OAUTH_STATE_FAILED", "failed to generate state: %v", err)
	}

	codeVerifier, err := openai.GenerateCodeVerifier()
	if err != nil {
		return nil, infraerrors.Newf(http.StatusInternalServerError, "OPENAI_OAUTH_VERIFIER_FAILED", "failed to generate code verifier: %v", err)
	}

	codeChallenge := openai.GenerateCodeChallenge(codeVerifier)

	// Generate session ID
	sessionID, err := openai.GenerateSessionID()
	if err != nil {
		return nil, infraerrors.Newf(http.StatusInternalServerError, "OPENAI_OAUTH_SESSION_FAILED", "failed to generate session ID: %v", err)
	}

	// Use default redirect URI if not specified
	if redirectURI == "" {
		redirectURI = openai.DefaultRedirectURI
	}
	normalizedPlatform := normalizeOpenAIOAuthPlatform(platform)
	clientID, _ := openai.OAuthClientConfigByPlatform(normalizedPlatform)

	// Store session
	session := &openai.OAuthSession{
		State:                 state,
		CodeVerifier:          codeVerifier,
		ClientID:              clientID,
		EgressRouteID:         selection.RouteID,
		EgressRouteRevision:   selection.RouteRevision,
		RequireVerifiedEgress: selection.RequireVerified,
		ProxyID:               cloneOpenAIOAuthInt64(selection.ProxyID),
		DirectEgress:          selection.Direct,
		RedirectURI:           redirectURI,
		CreatedAt:             time.Now(),
	}
	s.sessionStore.Set(sessionID, session)

	// Build authorization URL
	authURL := openai.BuildAuthorizationURLForPlatform(state, codeChallenge, redirectURI, normalizedPlatform)

	return &OpenAIAuthURLResult{
		AuthURL:   authURL,
		SessionID: sessionID,
	}, nil
}

// OpenAIExchangeCodeInput represents the input for code exchange
type OpenAIExchangeCodeInput struct {
	SessionID     string
	Code          string
	State         string
	RedirectURI   string
	ProxyID       *int64
	EgressRouteID *int64
}

// OpenAITokenInfo represents the token information for OpenAI
type OpenAITokenInfo struct {
	AccessToken           string `json:"access_token"`
	RefreshToken          string `json:"refresh_token"`
	IDToken               string `json:"id_token,omitempty"`
	ExpiresIn             int64  `json:"expires_in"`
	ExpiresAt             int64  `json:"expires_at"`
	ClientID              string `json:"client_id,omitempty"`
	AuthMode              string `json:"auth_mode,omitempty"`
	Email                 string `json:"email,omitempty"`
	ChatGPTAccountID      string `json:"chatgpt_account_id,omitempty"`
	ChatGPTUserID         string `json:"chatgpt_user_id,omitempty"`
	ChatGPTAccountFedRAMP bool   `json:"chatgpt_account_is_fedramp,omitempty"`
	OrganizationID        string `json:"organization_id,omitempty"`
	PlanType              string `json:"plan_type,omitempty"`
	SubscriptionExpiresAt string `json:"subscription_expires_at,omitempty"`
	PrivacyMode           string `json:"privacy_mode,omitempty"`
	EgressRouteID         *int64 `json:"egress_route_id,omitempty"`
	EgressRouteRevision   int64  `json:"egress_route_revision,omitempty"`
	OAuthUsesEgressPool   bool   `json:"-"`
	OAuthProxyID          *int64 `json:"-"`
}

// ExchangeCode exchanges authorization code for tokens
func (s *OpenAIOAuthService) ExchangeCode(ctx context.Context, input *OpenAIExchangeCodeInput) (*OpenAITokenInfo, error) {
	// Get session
	session, ok := s.sessionStore.Get(input.SessionID)
	if !ok {
		return nil, infraerrors.New(http.StatusBadRequest, "OPENAI_OAUTH_SESSION_NOT_FOUND", "session not found or expired")
	}
	if input.State == "" {
		return nil, infraerrors.New(http.StatusBadRequest, "OPENAI_OAUTH_STATE_REQUIRED", "oauth state is required")
	}
	if subtle.ConstantTimeCompare([]byte(input.State), []byte(session.State)) != 1 {
		return nil, infraerrors.New(http.StatusBadRequest, "OPENAI_OAUTH_INVALID_STATE", "invalid oauth state")
	}

	proxyURL, err := s.resolveOpenAIOAuthSessionProxyURL(ctx, session, input.ProxyID, input.EgressRouteID)
	if err != nil {
		return nil, err
	}

	// Use redirect URI from session or input
	redirectURI := session.RedirectURI
	if input.RedirectURI != "" {
		redirectURI = input.RedirectURI
	}
	clientID := strings.TrimSpace(session.ClientID)
	if clientID == "" {
		clientID = openai.ClientID
	}

	// Exchange code for token
	tokenResp, err := s.oauthClient.ExchangeCode(ctx, input.Code, session.CodeVerifier, redirectURI, proxyURL, clientID)
	if err != nil {
		return nil, err
	}

	// Parse ID token to get user info
	var userInfo *openai.UserInfo
	if tokenResp.IDToken != "" {
		claims, parseErr := openai.ParseIDToken(tokenResp.IDToken)
		if parseErr != nil {
			slog.Warn("openai_oauth_id_token_parse_failed", "error", parseErr)
		} else {
			userInfo = claims.GetUserInfo()
		}
	}

	// Delete session after successful exchange
	s.sessionStore.Delete(input.SessionID)

	tokenInfo := &OpenAITokenInfo{
		AccessToken:         tokenResp.AccessToken,
		RefreshToken:        tokenResp.RefreshToken,
		IDToken:             tokenResp.IDToken,
		ExpiresIn:           int64(tokenResp.ExpiresIn),
		ExpiresAt:           time.Now().Unix() + int64(tokenResp.ExpiresIn),
		ClientID:            clientID,
		EgressRouteRevision: session.EgressRouteRevision,
		OAuthUsesEgressPool: session.RequireVerifiedEgress,
		OAuthProxyID:        cloneOpenAIOAuthInt64(session.ProxyID),
	}
	if session.EgressRouteID > 0 {
		tokenInfo.EgressRouteID = cloneOpenAIOAuthInt64(&session.EgressRouteID)
	}

	if userInfo != nil {
		tokenInfo.Email = userInfo.Email
		tokenInfo.ChatGPTAccountID = userInfo.ChatGPTAccountID
		tokenInfo.ChatGPTUserID = userInfo.ChatGPTUserID
		tokenInfo.OrganizationID = userInfo.OrganizationID
		tokenInfo.PlanType = userInfo.PlanType
	}

	s.enrichTokenInfo(ctx, tokenInfo, proxyURL)

	return tokenInfo, nil
}

// RefreshToken refreshes an OpenAI OAuth token
func (s *OpenAIOAuthService) RefreshToken(ctx context.Context, refreshToken string, proxyURL string) (*OpenAITokenInfo, error) {
	return s.RefreshTokenWithClientID(ctx, refreshToken, proxyURL, "")
}

// RefreshTokenWithClientID refreshes an OpenAI OAuth token with optional client_id.
func (s *OpenAIOAuthService) RefreshTokenWithClientID(ctx context.Context, refreshToken string, proxyURL string, clientID string) (*OpenAITokenInfo, error) {
	if strings.TrimSpace(proxyURL) == "" {
		resolvedProxyURL, err := s.ResolveOpenAIOAuthProxyURL(ctx, nil)
		if err != nil {
			return nil, err
		}
		proxyURL = resolvedProxyURL
	}
	return s.RefreshTokenWithResolvedEgress(ctx, refreshToken, proxyURL, clientID)
}

// RefreshTokenWithResolvedEgress refreshes through a caller-resolved route.
// An empty proxyURL is an explicit direct route and must not fall back to the
// configured legacy default proxy.
func (s *OpenAIOAuthService) RefreshTokenWithResolvedEgress(ctx context.Context, refreshToken string, proxyURL string, clientID string) (*OpenAITokenInfo, error) {
	tokenResp, err := s.oauthClient.RefreshTokenWithClientID(ctx, refreshToken, proxyURL, clientID)
	if err != nil {
		return nil, err
	}

	// Parse ID token to get user info
	var userInfo *openai.UserInfo
	if tokenResp.IDToken != "" {
		claims, parseErr := openai.ParseIDToken(tokenResp.IDToken)
		if parseErr != nil {
			slog.Warn("openai_oauth_id_token_parse_failed", "error", parseErr)
		} else {
			userInfo = claims.GetUserInfo()
		}
	}

	tokenInfo := &OpenAITokenInfo{
		AccessToken:  tokenResp.AccessToken,
		RefreshToken: tokenResp.RefreshToken,
		IDToken:      tokenResp.IDToken,
		ExpiresIn:    int64(tokenResp.ExpiresIn),
		ExpiresAt:    time.Now().Unix() + int64(tokenResp.ExpiresIn),
	}
	if trimmed := strings.TrimSpace(clientID); trimmed != "" {
		tokenInfo.ClientID = trimmed
	}

	if userInfo != nil {
		tokenInfo.Email = userInfo.Email
		tokenInfo.ChatGPTAccountID = userInfo.ChatGPTAccountID
		tokenInfo.ChatGPTUserID = userInfo.ChatGPTUserID
		tokenInfo.OrganizationID = userInfo.OrganizationID
		tokenInfo.PlanType = userInfo.PlanType
	}

	s.enrichTokenInfo(ctx, tokenInfo, proxyURL)

	return tokenInfo, nil
}

// enrichTokenInfo 通过 ChatGPT backend-api 补全 tokenInfo 并设置隐私（best-effort）。
// 从 accounts/check 获取最新 plan_type、subscription_expires_at、email，
// 然后尝试关闭训练数据共享。适用于所有获取/刷新 token 的路径。
func (s *OpenAIOAuthService) enrichTokenInfo(ctx context.Context, tokenInfo *OpenAITokenInfo, proxyURL string) {
	if tokenInfo.AccessToken == "" || s.privacyClientFactory == nil {
		return
	}

	// 从 access_token JWT 中提取 orgID（poid），用于匹配正确的账号
	orgID := tokenInfo.OrganizationID
	if orgID == "" {
		if atClaims, err := openai.DecodeIDToken(tokenInfo.AccessToken); err == nil && atClaims.OpenAIAuth != nil {
			orgID = atClaims.OpenAIAuth.POID
		}
	}
	// accounts/check 命中的记录不属于个人账号时，必须改用个人订阅端点拿到期时间，
	// 否则会把 workspace 权益的 expires_at 当成个人订阅到期日展示。
	forcePersonalSubscriptionLookup := false
	if info := fetchChatGPTAccountInfo(ctx, s.privacyClientFactory, tokenInfo.AccessToken, proxyURL, orgID); info != nil {
		// chatgpt_plan_type from the ID token is the canonical personal-plan value.
		// accounts/check is a multi-account/workspace endpoint; inactive team or
		// business workspaces can otherwise overwrite Pro/Free with internal
		// workspace billing plan names such as self_serve_business_usage_based.
		appliedAccountInfoPlanType := shouldApplyChatGPTAccountInfoPlanType(tokenInfo.PlanType, info.PlanType)
		if appliedAccountInfoPlanType {
			tokenInfo.PlanType = info.PlanType
		}
		// plan_type 与 subscription_expires_at 必须描述同一份订阅。套餐取自
		// accounts/check 时，到期时间跟着取同一条记录；套餐保留了 JWT 里的个人值时，
		// 只有该记录确实就是个人账号才能用它的 entitlement.expires_at——poid 指向的
		// 默认 Personal workspace 与 chatgpt_account_id 可以是两个不同的标识，
		// 混用会显示成「个人 Pro + workspace 到期时间」。
		if info.SubscriptionExpiresAt != "" {
			if appliedAccountInfoPlanType || chatGPTAccountInfoBelongsToTokenAccount(tokenInfo, info) {
				tokenInfo.SubscriptionExpiresAt = info.SubscriptionExpiresAt
			} else {
				forcePersonalSubscriptionLookup = true
			}
		}
		if tokenInfo.Email == "" && info.Email != "" {
			tokenInfo.Email = info.Email
		}
	}
	if forcePersonalSubscriptionLookup || strings.TrimSpace(tokenInfo.SubscriptionExpiresAt) == "" {
		if expiresAt := fetchChatGPTSubscriptionExpiresAt(ctx, s.privacyClientFactory, tokenInfo.AccessToken, proxyURL, resolveChatGPTSubscriptionAccountID(tokenInfo, orgID)); expiresAt != "" {
			tokenInfo.SubscriptionExpiresAt = expiresAt
		}
	}

	// 尝试设置隐私（关闭训练数据共享），best-effort
	tokenInfo.PrivacyMode = disableOpenAITraining(ctx, s.privacyClientFactory, tokenInfo.AccessToken, proxyURL)
}

func shouldApplyChatGPTAccountInfoPlanType(current, candidate string) bool {
	return strings.TrimSpace(candidate) != "" && strings.TrimSpace(current) == ""
}

// chatGPTAccountInfoBelongsToTokenAccount 判断 accounts/check 命中的那条记录是不是
// token 自己的个人 ChatGPT 账号。两侧任一缺 ID 时无法区分，返回 true 保持既有行为。
func chatGPTAccountInfoBelongsToTokenAccount(tokenInfo *OpenAITokenInfo, info *ChatGPTAccountInfo) bool {
	personalID := strings.TrimSpace(tokenInfo.ChatGPTAccountID)
	sourceID := strings.TrimSpace(info.AccountID)
	if personalID == "" || sourceID == "" {
		return true
	}
	return strings.EqualFold(personalID, sourceID)
}

func resolveChatGPTSubscriptionAccountID(tokenInfo *OpenAITokenInfo, orgID string) string {
	for _, candidate := range []string{
		tokenInfo.ChatGPTAccountID,
		tokenInfo.OrganizationID,
		orgID,
	} {
		if trimmed := strings.TrimSpace(candidate); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// RefreshAccountToken refreshes token for an OpenAI OAuth account
func (s *OpenAIOAuthService) RefreshAccountToken(ctx context.Context, account *Account) (*OpenAITokenInfo, error) {
	if account.Platform != PlatformOpenAI {
		return nil, infraerrors.New(http.StatusBadRequest, "OPENAI_OAUTH_INVALID_ACCOUNT", "account is not an OpenAI account")
	}
	if account.Type != AccountTypeOAuth {
		return nil, infraerrors.New(http.StatusBadRequest, "OPENAI_OAUTH_INVALID_ACCOUNT_TYPE", "account is not an OAuth account")
	}

	proxyURL, err := s.ResolveOpenAIAccountControlProxyURL(ctx, account)
	if err != nil {
		return nil, err
	}

	accessToken := account.GetCredential("access_token")
	if account.IsOpenAIPersonalAccessToken() {
		if accessToken == "" {
			return nil, infraerrors.New(http.StatusBadRequest, "OPENAI_CODEX_PAT_REQUIRED", "access token is required")
		}
		return s.ValidateCodexPersonalAccessToken(ctx, accessToken, proxyURL)
	}

	refreshToken := account.GetCredential("refresh_token")
	if refreshToken == "" {
		if accessToken != "" {
			tokenInfo := &OpenAITokenInfo{
				AccessToken:           accessToken,
				RefreshToken:          "",
				IDToken:               account.GetCredential("id_token"),
				ClientID:              account.GetCredential("client_id"),
				Email:                 account.GetCredential("email"),
				ChatGPTAccountID:      account.GetCredential("chatgpt_account_id"),
				ChatGPTUserID:         account.GetCredential("chatgpt_user_id"),
				OrganizationID:        account.GetCredential("organization_id"),
				PlanType:              account.GetCredential("plan_type"),
				SubscriptionExpiresAt: account.GetCredential("subscription_expires_at"),
			}
			if expiresAt := account.GetCredentialAsTime("expires_at"); expiresAt != nil {
				tokenInfo.ExpiresAt = expiresAt.Unix()
				tokenInfo.ExpiresIn = int64(time.Until(*expiresAt).Seconds())
			}
			s.enrichTokenInfo(ctx, tokenInfo, proxyURL)
			return tokenInfo, nil
		}
		return nil, infraerrors.New(http.StatusBadRequest, "OPENAI_OAUTH_NO_REFRESH_TOKEN", "no refresh token available")
	}

	clientID := account.GetCredential("client_id")
	if account.EgressMode == EgressModePool {
		return s.RefreshTokenWithResolvedEgress(ctx, refreshToken, proxyURL, clientID)
	}
	return s.RefreshTokenWithClientID(ctx, refreshToken, proxyURL, clientID)
}

// BuildAccountCredentials builds credentials map from token info
func (s *OpenAIOAuthService) BuildAccountCredentials(tokenInfo *OpenAITokenInfo) map[string]any {
	creds := map[string]any{
		"access_token": tokenInfo.AccessToken,
	}
	if tokenInfo.ExpiresAt > 0 {
		creds["expires_at"] = time.Unix(tokenInfo.ExpiresAt, 0).Format(time.RFC3339)
	}
	// 仅在刷新响应返回了新的 refresh_token 时才更新，防止用空值覆盖已有令牌
	if strings.TrimSpace(tokenInfo.RefreshToken) != "" {
		creds["refresh_token"] = tokenInfo.RefreshToken
	}

	if tokenInfo.IDToken != "" {
		creds["id_token"] = tokenInfo.IDToken
	}
	if tokenInfo.Email != "" {
		creds["email"] = tokenInfo.Email
	}
	if tokenInfo.ChatGPTAccountID != "" {
		creds["chatgpt_account_id"] = tokenInfo.ChatGPTAccountID
	}
	if tokenInfo.ChatGPTUserID != "" {
		creds["chatgpt_user_id"] = tokenInfo.ChatGPTUserID
	}
	if tokenInfo.OrganizationID != "" {
		creds["organization_id"] = tokenInfo.OrganizationID
	}
	if tokenInfo.PlanType != "" {
		creds["plan_type"] = tokenInfo.PlanType
	}
	if tokenInfo.SubscriptionExpiresAt != "" {
		creds["subscription_expires_at"] = tokenInfo.SubscriptionExpiresAt
	}
	if strings.TrimSpace(tokenInfo.ClientID) != "" {
		creds["client_id"] = strings.TrimSpace(tokenInfo.ClientID)
	}
	if tokenInfo.AuthMode == OpenAIAuthModePersonalAccessToken {
		creds[openAIAuthModeCredentialKey] = OpenAIAuthModePersonalAccessToken
		creds[openAIAuthModeLegacyCredentialKey] = "personal_access_token"
		creds["token_type"] = "Bearer"
		creds["chatgpt_account_is_fedramp"] = tokenInfo.ChatGPTAccountFedRAMP
	} else if tokenInfo.ChatGPTAccountFedRAMP {
		creds["chatgpt_account_is_fedramp"] = true
	}

	return NormalizeOpenAIPersonalAccessTokenCredentials(nil, tokenInfo, creds)
}

// Stop stops the session store cleanup goroutine
func (s *OpenAIOAuthService) Stop() {
	s.sessionStore.Stop()
}

func normalizeOpenAIOAuthPlatform(platform string) string {
	return openai.OAuthPlatformOpenAI
}
