//go:build unit

package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestCRSSyncOpenAIOAuthInheritsDefaultProxy(t *testing.T) {
	repo := newCRSLongContextAccountRepo()
	proxy := activeOpenAIDefaultProxy(51, "2001:db8::51", 8080)
	proxyRepo := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
		return proxy, nil
	}}
	settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "51"}}
	oauthService := NewOpenAIOAuthService(proxyRepo, nil)
	oauthService.SetSettingService(newOpenAIDefaultProxySettingService(settings, proxyRepo))
	defer oauthService.Stop()

	result, err := runCRSDefaultProxySync(t, repo, proxyRepo, oauthService)
	require.NoError(t, err)
	require.Equal(t, 1, result.Created)
	account := repo.accounts["crs-default-proxy"]
	require.NotNil(t, account)
	require.NotNil(t, account.ProxyID)
	require.Equal(t, int64(51), *account.ProxyID)
}

func TestCRSSyncOpenAIOAuthPreservesExplicitProxy(t *testing.T) {
	existingProxyID := int64(99)
	repo := newCRSLongContextAccountRepo(&Account{
		ID:          7,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		ProxyID:     &existingProxyID,
		Credentials: map[string]any{"access_token": "existing-token"},
		Extra:       map[string]any{"crs_account_id": "crs-default-proxy"},
	})
	defaultProxy := activeOpenAIDefaultProxy(51, "2001:db8::51", 8080)
	proxyRepo := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
		return defaultProxy, nil
	}}
	settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "51"}}
	oauthService := NewOpenAIOAuthService(proxyRepo, nil)
	oauthService.SetSettingService(newOpenAIDefaultProxySettingService(settings, proxyRepo))
	defer oauthService.Stop()

	result, err := runCRSDefaultProxySync(t, repo, proxyRepo, oauthService)
	require.NoError(t, err)
	require.Equal(t, 1, result.Updated)
	require.NotNil(t, repo.accounts["crs-default-proxy"].ProxyID)
	require.Equal(t, existingProxyID, *repo.accounts["crs-default-proxy"].ProxyID)
}

func TestCRSSyncOpenAIOAuthCreateUsesDirectEgressPool(t *testing.T) {
	repo := newCRSLongContextAccountRepo()
	route := activeDirectOpenAIOAuthRoute(82, 4)
	egressRepo := &crsSyncEgressRepo{
		openAIOAuthEgressRepoStub: &openAIOAuthEgressRepoStub{route: route},
		config: &AccountEgressPoolConfigDomain{
			EgressMode:           EgressModePool,
			EgressRevision:       1,
			ConcurrencyPerEgress: 3,
			Bindings: []AccountEgressBinding{{
				AccountID: 1,
				RouteID:   route.ID,
				IsPrimary: true,
				Status:    AccountEgressBindingStatusActive,
			}},
		},
	}

	result, err := runCRSDefaultProxySyncWithEgress(
		t,
		repo,
		nil,
		nil,
		NewEgressService(egressRepo, nil),
	)
	require.NoError(t, err)
	require.Equal(t, 1, result.Created)
	account := repo.accounts["crs-default-proxy"]
	require.NotNil(t, account)
	require.Equal(t, EgressModePool, account.EgressMode)
	require.Equal(t, route.ID, account.EgressBindings[0].RouteID)
	require.Equal(t, 1, egressRepo.replaceCalls)
}

func runCRSDefaultProxySync(t *testing.T, repo AccountRepository, proxyRepo ProxyRepository, oauthService *OpenAIOAuthService) (*SyncFromCRSResult, error) {
	return runCRSDefaultProxySyncWithEgress(t, repo, proxyRepo, oauthService, nil)
}

type crsSyncEgressRepo struct {
	*openAIOAuthEgressRepoStub
	config       *AccountEgressPoolConfigDomain
	replaceCalls int
}

func (r *crsSyncEgressRepo) ReplaceAccountPool(_ context.Context, accountID int64, input ReplaceAccountPoolInput) (*AccountEgressPoolConfigDomain, error) {
	r.replaceCalls++
	config := *r.config
	config.AccountID = accountID
	config.Bindings = append([]AccountEgressBinding(nil), r.config.Bindings...)
	if len(config.Bindings) > 0 {
		config.Bindings[0].AccountID = accountID
	}
	return &config, nil
}

func runCRSDefaultProxySyncWithEgress(t *testing.T, repo AccountRepository, proxyRepo ProxyRepository, oauthService *OpenAIOAuthService, egressService *EgressService) (*SyncFromCRSResult, error) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/web/auth/login" {
			_, _ = w.Write([]byte(`{"success":true,"token":"admin-token"}`))
			return
		}
		require.Equal(t, "/admin/sync/export-accounts", r.URL.Path)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data": map[string]any{
				"openaiOAuthAccounts": []any{
					map[string]any{
						"kind":        "openai",
						"id":          "crs-default-proxy",
						"name":        "CRS OpenAI",
						"isActive":    true,
						"schedulable": true,
						"credentials": map[string]any{"access_token": "at-test"},
					},
				},
			},
		})
	}))
	defer server.Close()

	cfg := &config.Config{}
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	svc := NewCRSSyncService(repo, proxyRepo, nil, oauthService, nil, cfg)
	svc.SetEgressService(egressService)
	return svc.SyncFromCRS(context.Background(), SyncFromCRSInput{
		BaseURL:  server.URL,
		Username: "admin",
		Password: "password",
	})
}
