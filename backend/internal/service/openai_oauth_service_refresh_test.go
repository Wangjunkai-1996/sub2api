package service

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/imroc/req/v3"
	"github.com/stretchr/testify/require"
)

type openaiOAuthClientRefreshStub struct {
	refreshCalls    int32
	refreshResponse *openai.TokenResponse
	refreshErr      error
}

func (s *openaiOAuthClientRefreshStub) ExchangeCode(ctx context.Context, code, codeVerifier, redirectURI, proxyURL, clientID string) (*openai.TokenResponse, error) {
	return nil, errors.New("not implemented")
}

func (s *openaiOAuthClientRefreshStub) RefreshToken(ctx context.Context, refreshToken, proxyURL string) (*openai.TokenResponse, error) {
	atomic.AddInt32(&s.refreshCalls, 1)
	return s.refreshResult()
}

func (s *openaiOAuthClientRefreshStub) RefreshTokenWithClientID(ctx context.Context, refreshToken, proxyURL string, clientID string) (*openai.TokenResponse, error) {
	atomic.AddInt32(&s.refreshCalls, 1)
	return s.refreshResult()
}

func (s *openaiOAuthClientRefreshStub) refreshResult() (*openai.TokenResponse, error) {
	if s.refreshErr != nil {
		return nil, s.refreshErr
	}
	if s.refreshResponse != nil {
		return s.refreshResponse, nil
	}
	return nil, errors.New("not implemented")
}

func openAIRefreshTestIDToken(t *testing.T, planType string) string {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"exp": time.Now().Add(time.Hour).Unix(),
		"https://api.openai.com/auth": map[string]any{
			"chatgpt_plan_type": planType,
		},
	})
	require.NoError(t, err)
	return "e30." + base64.RawURLEncoding.EncodeToString(payload) + ".test"
}

func TestOpenAIOAuthService_RefreshAccountToken_NoRefreshTokenUsesExistingAccessToken(t *testing.T) {
	client := &openaiOAuthClientRefreshStub{}
	svc := NewOpenAIOAuthService(nil, client)
	var privacyClientCalls int32
	svc.SetPrivacyClientFactory(func(proxyURL string) (*req.Client, error) {
		atomic.AddInt32(&privacyClientCalls, 1)
		return nil, errors.New("stop before request")
	})

	expiresAt := time.Now().Add(30 * time.Minute).UTC().Format(time.RFC3339)
	account := &Account{
		ID:       77,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Credentials: map[string]any{
			"access_token": "existing-access-token",
			"expires_at":   expiresAt,
			"client_id":    "client-id-1",
		},
	}

	info, err := svc.RefreshAccountToken(context.Background(), account)
	require.NoError(t, err)
	require.NotNil(t, info)
	require.Equal(t, "existing-access-token", info.AccessToken)
	require.Equal(t, "client-id-1", info.ClientID)
	require.Zero(t, atomic.LoadInt32(&client.refreshCalls), "existing access token should be reused without calling refresh")
	require.Positive(t, atomic.LoadInt32(&privacyClientCalls), "existing access token should still run enrichment")
}

func TestOpenAIOAuthService_RefreshAccountToken_PATIgnoresStaleRefreshToken(t *testing.T) {
	client := &openaiOAuthClientRefreshStub{}
	var whoamiCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&whoamiCalls, 1)
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{
			"email":"user@example.com",
			"chatgpt_user_id":"user-123",
			"chatgpt_account_id":"acct-123",
			"chatgpt_plan_type":"plus",
			"chatgpt_account_is_fedramp":false
		}`))
	}))
	defer server.Close()

	originalURL := openAICodexPATWhoamiURL
	openAICodexPATWhoamiURL = server.URL
	defer func() { openAICodexPATWhoamiURL = originalURL }()

	svc := NewOpenAIOAuthService(nil, client)
	defer svc.Stop()

	account := &Account{
		ID:       77,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Credentials: map[string]any{
			"access_token":  "at-test-token",
			"refresh_token": "stale-refresh-token",
			"expires_at":    time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
			"auth_mode":     "personal_access_token",
		},
	}

	info, err := svc.RefreshAccountToken(context.Background(), account)
	require.NoError(t, err)
	require.Equal(t, OpenAIAuthModePersonalAccessToken, info.AuthMode)
	require.Equal(t, "at-test-token", info.AccessToken)
	require.Empty(t, info.RefreshToken)
	require.Equal(t, int32(1), atomic.LoadInt32(&whoamiCalls))
	require.Zero(t, atomic.LoadInt32(&client.refreshCalls), "PAT accounts must not call OAuth refresh even if stale refresh_token remains")
}

func TestOpenAITokenRefresher_NeedsRefresh_SkipsAccountWithoutRefreshToken(t *testing.T) {
	refresher := NewOpenAITokenRefresher(nil, nil)
	expiresAt := time.Now().Add(time.Minute).UTC().Format(time.RFC3339)

	withoutRT := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Credentials: map[string]any{
			"access_token": "access-token",
			"expires_at":   expiresAt,
		},
	}
	require.False(t, refresher.NeedsRefresh(withoutRT, 5*time.Minute))

	withRT := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Credentials: map[string]any{
			"access_token":  "access-token",
			"refresh_token": "refresh-token",
			"expires_at":    expiresAt,
		},
	}
	require.True(t, refresher.NeedsRefresh(withRT, 5*time.Minute))

	patWithStaleRT := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Credentials: map[string]any{
			"access_token":  "at-test-token",
			"refresh_token": "stale-refresh-token",
			"expires_at":    expiresAt,
			"auth_mode":     OpenAIAuthModePersonalAccessToken,
		},
	}
	require.False(t, refresher.NeedsRefresh(patWithStaleRT, 5*time.Minute))
}

func TestOpenAITokenRefresher_RefreshDropsUnprovenPreviousPlan(t *testing.T) {
	tests := []struct {
		name    string
		idToken string
	}{
		{name: "missing id token"},
		{name: "blank plan claim", idToken: openAIRefreshTestIDToken(t, "   ")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &openaiOAuthClientRefreshStub{refreshResponse: &openai.TokenResponse{
				AccessToken:  "fresh-access-token",
				RefreshToken: "fresh-refresh-token",
				IDToken:      tt.idToken,
				ExpiresIn:    3600,
			}}
			svc := NewOpenAIOAuthService(nil, client)
			defer svc.Stop()
			var enrichCalls int32
			svc.SetPrivacyClientFactory(func(string) (*req.Client, error) {
				atomic.AddInt32(&enrichCalls, 1)
				return nil, errors.New("enrichment unavailable")
			})
			refresher := NewOpenAITokenRefresher(svc, nil)
			account := &Account{
				ID: 91, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
				Credentials: map[string]any{
					"access_token":  "old-access-token",
					"refresh_token": "old-refresh-token",
					"plan_type":     "plus",
					"model_mapping": map[string]any{"gpt-5": "gpt-5-codex"},
				},
			}

			credentials, err := refresher.Refresh(context.Background(), account)
			require.NoError(t, err)
			require.Equal(t, int32(1), atomic.LoadInt32(&client.refreshCalls))
			require.Positive(t, atomic.LoadInt32(&enrichCalls))
			require.Equal(t, "fresh-access-token", credentials["access_token"])
			require.Equal(t, "fresh-refresh-token", credentials["refresh_token"])
			require.NotContains(t, credentials, "plan_type")
			require.Equal(t, map[string]any{"gpt-5": "gpt-5-codex"}, credentials["model_mapping"])
			require.Equal(t, "plus", account.Credentials["plan_type"], "refresh must not mutate the caller's credential snapshot")

			effective := *account
			effective.Credentials = credentials
			require.Equal(t, securityadmission.AccountUnknown, ClassifyOpenAIEffectiveCredentialOwner(&effective))
		})
	}
}

func TestOpenAITokenRefresher_RefreshAppliesFreshPlan(t *testing.T) {
	tests := []struct {
		name      string
		oldPlan   string
		freshPlan string
		wantClass securityadmission.AccountClass
	}{
		{name: "fresh plus replaces pro", oldPlan: "pro", freshPlan: "plus", wantClass: securityadmission.AccountAuditExemptVerified},
		{name: "fresh pro replaces plus", oldPlan: "plus", freshPlan: "pro", wantClass: securityadmission.AccountAuditRequired},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &openaiOAuthClientRefreshStub{refreshResponse: &openai.TokenResponse{
				AccessToken:  "fresh-access-token",
				RefreshToken: "fresh-refresh-token",
				IDToken:      openAIRefreshTestIDToken(t, tt.freshPlan),
				ExpiresIn:    3600,
			}}
			svc := NewOpenAIOAuthService(nil, client)
			defer svc.Stop()
			refresher := NewOpenAITokenRefresher(svc, nil)
			account := &Account{
				ID: 92, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
				Credentials: map[string]any{
					"access_token":  "old-access-token",
					"refresh_token": "old-refresh-token",
					"plan_type":     tt.oldPlan,
				},
			}

			credentials, err := refresher.Refresh(context.Background(), account)
			require.NoError(t, err)
			require.Equal(t, tt.freshPlan, credentials["plan_type"])
			effective := *account
			effective.Credentials = credentials
			require.Equal(t, tt.wantClass, ClassifyOpenAIEffectiveCredentialOwner(&effective))
		})
	}
}

func TestOpenAITokenRefresher_RefreshFailureReturnsNoCredentials(t *testing.T) {
	client := &openaiOAuthClientRefreshStub{refreshErr: errors.New("refresh unavailable")}
	svc := NewOpenAIOAuthService(nil, client)
	defer svc.Stop()
	refresher := NewOpenAITokenRefresher(svc, nil)
	account := &Account{
		ID: 93, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Credentials: map[string]any{
			"access_token":  "old-access-token",
			"refresh_token": "old-refresh-token",
			"plan_type":     "plus",
		},
	}

	credentials, err := refresher.Refresh(context.Background(), account)
	require.ErrorContains(t, err, "refresh unavailable")
	require.Nil(t, credentials)
	require.Equal(t, "plus", account.Credentials["plan_type"])
}

func TestOpenAITokenRefresher_Refresh_PATRemovesStaleOAuthFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{
			"email":"user@example.com",
			"chatgpt_user_id":"user-123",
			"chatgpt_account_id":"acct-123",
			"chatgpt_plan_type":"plus",
			"chatgpt_account_is_fedramp":true
		}`))
	}))
	defer server.Close()

	originalURL := openAICodexPATWhoamiURL
	openAICodexPATWhoamiURL = server.URL
	defer func() { openAICodexPATWhoamiURL = originalURL }()

	svc := NewOpenAIOAuthService(nil, nil)
	defer svc.Stop()
	refresher := NewOpenAITokenRefresher(svc, nil)

	account := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Credentials: map[string]any{
			"access_token":  "at-test-token",
			"refresh_token": "stale-refresh-token",
			"id_token":      "stale-id-token",
			"expires_at":    time.Now().Add(-time.Hour).UTC().Format(time.RFC3339),
			"expires_in":    3600,
			"client_id":     "stale-client",
			"auth_mode":     OpenAIAuthModePersonalAccessToken,
			"model_mapping": map[string]any{"gpt-5": "gpt-5-codex"},
		},
	}

	credentials, err := refresher.Refresh(context.Background(), account)
	require.NoError(t, err)
	require.Equal(t, "at-test-token", credentials["access_token"])
	require.Equal(t, OpenAIAuthModePersonalAccessToken, credentials["auth_mode"])
	require.Equal(t, "personal_access_token", credentials["openai_auth_mode"])
	require.NotContains(t, credentials, "refresh_token")
	require.NotContains(t, credentials, "id_token")
	require.NotContains(t, credentials, "expires_at")
	require.NotContains(t, credentials, "expires_in")
	require.NotContains(t, credentials, "client_id")
	require.Equal(t, map[string]any{"gpt-5": "gpt-5-codex"}, credentials["model_mapping"])
}

func TestOpenAITokenProvider_NoRefreshTokenExpiredAccessTokenReturnsError(t *testing.T) {
	provider := NewOpenAITokenProvider(nil, nil, nil)
	expiresAt := time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	account := &Account{
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Credentials: map[string]any{
			"access_token": "expired-access-token",
			"expires_at":   expiresAt,
		},
	}

	token, err := provider.GetAccessToken(context.Background(), account)
	require.Error(t, err)
	require.Empty(t, token)
	require.Contains(t, err.Error(), "refresh_token is missing")
}
