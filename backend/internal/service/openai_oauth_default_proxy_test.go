//go:build unit

package service

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/stretchr/testify/require"
)

func newOpenAIDefaultProxySettingService(settingRepo SettingRepository, proxyRepo ProxyRepository) *SettingService {
	svc := NewSettingService(settingRepo, nil)
	svc.SetProxyRepository(proxyRepo)
	return svc
}

func activeOpenAIDefaultProxy(id int64, host string, port int) *Proxy {
	return &Proxy{
		ID:       id,
		Protocol: "http",
		Host:     host,
		Port:     port,
		Status:   StatusActive,
	}
}

func TestSettingServiceResolveOpenAIOAuthDefaultProxy(t *testing.T) {
	t.Run("missing setting is a no-op", func(t *testing.T) {
		proxyCalls := 0
		settings := &settingRepoStub{values: map[string]string{}}
		proxies := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
			proxyCalls++
			return nil, errors.New("unexpected proxy lookup")
		}}

		proxy, err := newOpenAIDefaultProxySettingService(settings, proxies).ResolveOpenAIOAuthDefaultProxy(context.Background())

		require.NoError(t, err)
		require.Nil(t, proxy)
		require.Zero(t, proxyCalls)
	})

	t.Run("empty setting is a no-op", func(t *testing.T) {
		proxyCalls := 0
		settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "  "}}
		proxies := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
			proxyCalls++
			return nil, errors.New("unexpected proxy lookup")
		}}

		proxy, err := newOpenAIDefaultProxySettingService(settings, proxies).ResolveOpenAIOAuthDefaultProxy(context.Background())

		require.NoError(t, err)
		require.Nil(t, proxy)
		require.Zero(t, proxyCalls)
	})

	t.Run("invalid proxy id is rejected", func(t *testing.T) {
		settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "not-an-id"}}

		proxy, err := newOpenAIDefaultProxySettingService(settings, nil).ResolveOpenAIOAuthDefaultProxy(context.Background())

		require.Nil(t, proxy)
		require.Equal(t, "OPENAI_OAUTH_DEFAULT_PROXY_INVALID", infraerrors.Reason(err))
	})

	t.Run("missing proxy is rejected", func(t *testing.T) {
		settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "17"}}
		proxies := &mockProxyRepoForOAuth{getByIDFunc: func(_ context.Context, id int64) (*Proxy, error) {
			require.Equal(t, int64(17), id)
			return nil, ErrProxyNotFound
		}}

		proxy, err := newOpenAIDefaultProxySettingService(settings, proxies).ResolveOpenAIOAuthDefaultProxy(context.Background())

		require.Nil(t, proxy)
		require.Equal(t, "OPENAI_OAUTH_DEFAULT_PROXY_NOT_FOUND", infraerrors.Reason(err))
	})

	t.Run("inactive proxy is rejected", func(t *testing.T) {
		settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "18"}}
		proxies := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
			return &Proxy{ID: 18, Status: StatusDisabled}, nil
		}}

		proxy, err := newOpenAIDefaultProxySettingService(settings, proxies).ResolveOpenAIOAuthDefaultProxy(context.Background())

		require.Nil(t, proxy)
		require.Equal(t, "OPENAI_OAUTH_DEFAULT_PROXY_INACTIVE", infraerrors.Reason(err))
	})

	t.Run("expired proxy is rejected", func(t *testing.T) {
		expiresAt := time.Now().Add(-time.Minute)
		settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "18"}}
		proxies := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
			return &Proxy{ID: 18, Status: StatusActive, ExpiresAt: &expiresAt}, nil
		}}

		proxy, err := newOpenAIDefaultProxySettingService(settings, proxies).ResolveOpenAIOAuthDefaultProxy(context.Background())

		require.Nil(t, proxy)
		require.Equal(t, "OPENAI_OAUTH_DEFAULT_PROXY_EXPIRED", infraerrors.Reason(err))
	})

	t.Run("active proxy is returned", func(t *testing.T) {
		settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "19"}}
		want := activeOpenAIDefaultProxy(19, "2001:db8::19", 3128)
		proxies := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
			return want, nil
		}}

		proxy, err := newOpenAIDefaultProxySettingService(settings, proxies).ResolveOpenAIOAuthDefaultProxy(context.Background())

		require.NoError(t, err)
		require.Same(t, want, proxy)
		require.Equal(t, "http://[2001:db8::19]:3128", proxy.URL())
	})
}

type openAIDefaultProxyOAuthClientStub struct {
	exchangeCalls   int32
	refreshCalls    int32
	proxyURL        string
	refreshProxyURL string
	refreshClientID string
}

func (s *openAIDefaultProxyOAuthClientStub) ExchangeCode(_ context.Context, _, _, _, proxyURL, _ string) (*openai.TokenResponse, error) {
	atomic.AddInt32(&s.exchangeCalls, 1)
	s.proxyURL = proxyURL
	return &openai.TokenResponse{AccessToken: "at-test", RefreshToken: "rt-test", ExpiresIn: 3600}, nil
}

func (s *openAIDefaultProxyOAuthClientStub) RefreshToken(context.Context, string, string) (*openai.TokenResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *openAIDefaultProxyOAuthClientStub) RefreshTokenWithClientID(_ context.Context, _, proxyURL, clientID string) (*openai.TokenResponse, error) {
	atomic.AddInt32(&s.refreshCalls, 1)
	s.refreshProxyURL = proxyURL
	s.refreshClientID = clientID
	return &openai.TokenResponse{AccessToken: "at-refresh", RefreshToken: "rt-refresh", ExpiresIn: 3600}, nil
}

func TestOpenAIOAuthServiceDefaultProxy(t *testing.T) {
	t.Run("explicit missing proxy is rejected", func(t *testing.T) {
		proxyID := int64(20)
		proxies := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
			return nil, nil
		}}
		svc := NewOpenAIOAuthService(proxies, nil)
		defer svc.Stop()

		_, err := svc.ResolveOpenAIOAuthProxyURL(context.Background(), &proxyID)
		require.Equal(t, "OPENAI_OAUTH_PROXY_NOT_FOUND", infraerrors.Reason(err))
	})

	t.Run("generate session and exchange use configured default", func(t *testing.T) {
		settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "21"}}
		proxy := activeOpenAIDefaultProxy(21, "2001:db8::21", 8080)
		proxies := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
			return proxy, nil
		}}
		client := &openAIDefaultProxyOAuthClientStub{}
		svc := NewOpenAIOAuthService(proxies, client)
		svc.SetSettingService(newOpenAIDefaultProxySettingService(settings, proxies))
		defer svc.Stop()

		result, err := svc.GenerateAuthURL(context.Background(), nil, "", PlatformOpenAI)
		require.NoError(t, err)
		session, ok := svc.sessionStore.Get(result.SessionID)
		require.True(t, ok)
		require.Equal(t, proxy.ID, *session.ProxyID)
		encodedSession, marshalErr := json.Marshal(session)
		require.NoError(t, marshalErr)
		require.NotContains(t, string(encodedSession), "proxy_url")

		_, err = svc.ExchangeCode(context.Background(), &OpenAIExchangeCodeInput{
			SessionID: result.SessionID,
			Code:      "auth-code",
			State:     session.State,
		})
		require.NoError(t, err)
		require.Equal(t, int32(1), atomic.LoadInt32(&client.exchangeCalls))
		require.Equal(t, proxy.URL(), client.proxyURL)
	})

	t.Run("explicit proxy wins without reading default", func(t *testing.T) {
		settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "broken"}}
		explicitProxyID := int64(22)
		explicitProxy := activeOpenAIDefaultProxy(explicitProxyID, "2001:db8::22", 8080)
		proxies := &mockProxyRepoForOAuth{getByIDFunc: func(_ context.Context, id int64) (*Proxy, error) {
			require.Equal(t, explicitProxyID, id)
			return explicitProxy, nil
		}}
		client := &openAIDefaultProxyOAuthClientStub{}
		svc := NewOpenAIOAuthService(proxies, client)
		svc.SetSettingService(newOpenAIDefaultProxySettingService(settings, proxies))
		defer svc.Stop()

		result, err := svc.GenerateAuthURL(context.Background(), &explicitProxyID, "", PlatformOpenAI)
		require.NoError(t, err)
		session, ok := svc.sessionStore.Get(result.SessionID)
		require.True(t, ok)
		require.Equal(t, explicitProxy.ID, *session.ProxyID)
		encodedSession, marshalErr := json.Marshal(session)
		require.NoError(t, marshalErr)
		require.NotContains(t, string(encodedSession), "proxy_url")
		require.Zero(t, settings.getValueCalls)
	})

	t.Run("invalid default stops exchange before upstream", func(t *testing.T) {
		settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "invalid"}}
		client := &openAIDefaultProxyOAuthClientStub{}
		svc := NewOpenAIOAuthService(nil, client)
		svc.SetSettingService(newOpenAIDefaultProxySettingService(settings, nil))
		defer svc.Stop()
		svc.sessionStore.Set("sid", &openai.OAuthSession{
			State:        "expected-state",
			CodeVerifier: "verifier",
			RedirectURI:  openai.DefaultRedirectURI,
			CreatedAt:    time.Now(),
		})

		_, err := svc.ExchangeCode(context.Background(), &OpenAIExchangeCodeInput{
			SessionID: "sid",
			Code:      "auth-code",
			State:     "expected-state",
		})

		require.Equal(t, "OPENAI_OAUTH_DEFAULT_PROXY_INVALID", infraerrors.Reason(err))
		require.Zero(t, atomic.LoadInt32(&client.exchangeCalls))
	})
}

func TestOpenAIOAuthServiceRefreshTokenUsesDefaultProxy(t *testing.T) {
	settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "23"}}
	proxy := activeOpenAIDefaultProxy(23, "2001:db8::23", 8080)
	proxies := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
		return proxy, nil
	}}
	client := &openAIDefaultProxyOAuthClientStub{}
	svc := NewOpenAIOAuthService(proxies, client)
	svc.SetSettingService(newOpenAIDefaultProxySettingService(settings, proxies))
	defer svc.Stop()

	info, err := svc.RefreshTokenWithClientID(context.Background(), "rt-input", "", "client-id")
	require.NoError(t, err)
	require.Equal(t, "at-refresh", info.AccessToken)
	require.Equal(t, int32(1), atomic.LoadInt32(&client.refreshCalls))
	require.Equal(t, proxy.URL(), client.refreshProxyURL)
	require.Equal(t, "client-id", client.refreshClientID)
}

func TestOpenAIOAuthServiceRefreshAccountTokenFailsOnExplicitProxyReadError(t *testing.T) {
	proxyID := int64(24)
	proxies := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
		return nil, errors.New("database unavailable")
	}}
	svc := NewOpenAIOAuthService(proxies, &openAIDefaultProxyOAuthClientStub{})
	defer svc.Stop()

	_, err := svc.RefreshAccountToken(context.Background(), &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		ProxyID:  &proxyID,
		Credentials: map[string]any{
			"refresh_token": "rt-test",
		},
	})
	require.Equal(t, "OPENAI_OAUTH_PROXY_READ_FAILED", infraerrors.Reason(err))
}

func TestOpenAIOAuthServiceCodexPATUsesDefaultProxy(t *testing.T) {
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "http://openai.invalid/api/accounts/v1/user-auth-credential/whoami", r.URL.String())
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{
			"email":"proxy-user@example.com",
			"chatgpt_user_id":"user-proxy",
			"chatgpt_account_id":"acct-proxy",
			"chatgpt_plan_type":"pro",
			"chatgpt_account_is_fedramp":false
		}`))
	}))
	defer proxyServer.Close()

	parsedProxyURL, err := url.Parse(proxyServer.URL)
	require.NoError(t, err)
	proxyHost, proxyPortRaw, err := net.SplitHostPort(parsedProxyURL.Host)
	require.NoError(t, err)
	proxyPort, err := strconv.Atoi(proxyPortRaw)
	require.NoError(t, err)

	originalWhoamiURL := openAICodexPATWhoamiURL
	openAICodexPATWhoamiURL = "http://openai.invalid/api/accounts/v1/user-auth-credential/whoami"
	defer func() { openAICodexPATWhoamiURL = originalWhoamiURL }()

	settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "31"}}
	proxy := activeOpenAIDefaultProxy(31, proxyHost, proxyPort)
	proxies := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
		return proxy, nil
	}}
	svc := NewOpenAIOAuthService(proxies, nil)
	svc.SetSettingService(newOpenAIDefaultProxySettingService(settings, proxies))
	defer svc.Stop()

	info, err := svc.ValidateCodexPersonalAccessToken(context.Background(), "at-proxy-token", "")

	require.NoError(t, err)
	require.Equal(t, "proxy-user@example.com", info.Email)
	require.Equal(t, "acct-proxy", info.ChatGPTAccountID)
}

func TestOpenAIOAuthServiceCodexPATInvalidDefaultDoesNotCallUpstream(t *testing.T) {
	var upstreamCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		atomic.AddInt32(&upstreamCalls, 1)
	}))
	defer server.Close()

	originalWhoamiURL := openAICodexPATWhoamiURL
	openAICodexPATWhoamiURL = server.URL
	defer func() { openAICodexPATWhoamiURL = originalWhoamiURL }()

	settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "invalid"}}
	svc := NewOpenAIOAuthService(nil, nil)
	svc.SetSettingService(newOpenAIDefaultProxySettingService(settings, nil))
	defer svc.Stop()

	_, err := svc.ValidateCodexPersonalAccessToken(context.Background(), "at-test-token", "")

	require.Equal(t, "OPENAI_OAUTH_DEFAULT_PROXY_INVALID", infraerrors.Reason(err))
	require.Zero(t, atomic.LoadInt32(&upstreamCalls))
}

func TestAdminServiceCreateAccountOpenAIOAuthDefaultProxy(t *testing.T) {
	t.Run("OpenAI OAuth inherits the eligible pool", func(t *testing.T) {
		settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "41"}}
		proxy := activeOpenAIDefaultProxy(41, "203.0.113.41", 3128)
		proxies := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
			return proxy, nil
		}}
		now := time.Now()
		otherProxyID := int64(42)
		routes := []EgressRoute{
			eligibleProxyRoute(141, proxy.ID, "203.0.113.41", now),
			eligibleProxyRoute(142, otherProxyID, "203.0.113.42", now),
		}
		repo := newDuplicateAccountRepoStub()
		svc := &adminServiceImpl{
			accountRepo:          repo,
			accountDuplicateRepo: repo,
			settingService:       newOpenAIDefaultProxySettingService(settings, proxies),
			egressService:        NewEgressService(&openAIOAuthEgressRepoStub{routes: routes}, nil),
		}

		account, err := svc.CreateAccount(context.Background(), &CreateAccountInput{
			Name:                 "oauth-default-proxy",
			Platform:             PlatformOpenAI,
			Type:                 AccountTypeOAuth,
			Credentials:          map[string]any{"access_token": "at-test"},
			SkipDefaultGroupBind: true,
		})

		require.NoError(t, err)
		require.Nil(t, account.ProxyID)
		require.Equal(t, EgressModePool, account.EgressMode)
		require.NotNil(t, account.EgressPoolWrite)
		require.Equal(t, []int64{141, 142}, account.EgressPoolWrite.RouteIDs)
		require.Equal(t, int64(141), account.EgressPoolWrite.PrimaryRouteID)
		require.Equal(t, DefaultOpenAIOAuthEgressConcurrency, account.Concurrency)
	})

	t.Run("explicit proxy skips default", func(t *testing.T) {
		settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "invalid"}}
		explicitProxyID := int64(42)
		repo := &longContextBillingRepoStub{}
		svc := &adminServiceImpl{
			accountRepo:    repo,
			settingService: newOpenAIDefaultProxySettingService(settings, nil),
		}

		account, err := svc.CreateAccount(context.Background(), &CreateAccountInput{
			Name:                 "oauth-explicit-proxy",
			Platform:             PlatformOpenAI,
			Type:                 AccountTypeOAuth,
			Credentials:          map[string]any{"access_token": "at-test"},
			ProxyID:              &explicitProxyID,
			SkipDefaultGroupBind: true,
		})

		require.NoError(t, err)
		require.Equal(t, explicitProxyID, *account.ProxyID)
		require.Zero(t, settings.getValueCalls)
	})

	t.Run("OpenAI API key skips default", func(t *testing.T) {
		settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "invalid"}}
		repo := &longContextBillingRepoStub{}
		svc := &adminServiceImpl{
			accountRepo:    repo,
			settingService: newOpenAIDefaultProxySettingService(settings, nil),
		}

		account, err := svc.CreateAccount(context.Background(), &CreateAccountInput{
			Name:                 "api-key-direct",
			Platform:             PlatformOpenAI,
			Type:                 AccountTypeAPIKey,
			Credentials:          map[string]any{"api_key": "sk-test"},
			SkipDefaultGroupBind: true,
		})

		require.NoError(t, err)
		require.Nil(t, account.ProxyID)
		require.Zero(t, settings.getValueCalls)
	})
}
