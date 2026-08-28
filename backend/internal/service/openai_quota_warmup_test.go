package service

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

type warmupQuotaRefreshExecutor struct {
	err   error
	calls int
}

func (e *warmupQuotaRefreshExecutor) CacheKey(account *Account) string {
	return OpenAITokenCacheKey(account)
}

func (e *warmupQuotaRefreshExecutor) CanRefresh(*Account) bool { return true }

func (e *warmupQuotaRefreshExecutor) NeedsRefresh(*Account, time.Duration) bool { return false }

func (e *warmupQuotaRefreshExecutor) Refresh(_ context.Context, account *Account) (map[string]any, error) {
	e.calls++
	if e.err != nil {
		return nil, e.err
	}
	credentials := shallowCopyMap(account.Credentials)
	credentials["access_token"] = "warmup-refreshed-token"
	return credentials, nil
}

func newWarmupQuotaTestService(t *testing.T, upstream http.Handler) (*OpenAIQuotaService, *httptest.Server) {
	t.Helper()
	account := &Account{
		ID: 100, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive,
		Credentials: map[string]any{"chatgpt_account_id": "org-warmup"},
	}
	repo := &stubQuotaAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	tokenCache := &stubQuotaTokenCache{tokens: map[string]string{OpenAITokenCacheKey(account): "fake-token"}}
	server := httptest.NewServer(upstream)
	service := NewOpenAIQuotaService(repo, nil, NewOpenAITokenProvider(repo, tokenCache, nil), newQuotaRedirectingFactory(server))
	return service, server
}

func TestQueryUsageForWarmupSkipsResetCreditDetails(t *testing.T) {
	usageCalls := 0
	detailCalls := 0
	service, server := newWarmupQuotaTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "application/json")
		switch r.URL.Path {
		case "/backend-api/wham/usage":
			usageCalls++
			_, _ = w.Write([]byte(`{"rate_limit":{"allowed":true,"primary_window":null,"secondary_window":null}}`))
		case "/backend-api/wham/rate-limit-reset-credits":
			detailCalls++
			_, _ = w.Write([]byte(`{"available_count":1}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	usage, err := service.QueryUsageForWarmup(context.Background(), 100)

	require.NoError(t, err)
	require.NotNil(t, usage)
	require.Equal(t, 1, usageCalls)
	require.Zero(t, detailCalls)
}

func TestQueryUsageForWarmupNeverLogsOrReturnsResponseBody(t *testing.T) {
	const secretBody = "upstream-secret-response-body"
	service, server := newWarmupQuotaTestService(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":"` + secretBody + `"}`))
	}))
	defer server.Close()

	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	_, err := service.QueryUsageForWarmup(context.Background(), 100)

	require.Error(t, err)
	require.NotContains(t, err.Error(), secretBody)
	require.NotContains(t, logs.String(), secretBody)
	require.Contains(t, logs.String(), "source=window_warmup")
}

func TestQueryUsageForWarmupPreservesSanitizedUpstreamStatus(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusNotFound} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			service, server := newWarmupQuotaTestService(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"error":"must-not-escape"}`))
			}))
			defer server.Close()

			_, err := service.QueryUsageForWarmup(context.Background(), 100)

			require.Error(t, err)
			require.Equal(t, status, infraerrors.Code(err))
			require.NotContains(t, err.Error(), "must-not-escape")
		})
	}
}

func TestQueryUsageForWarmupRefreshesOAuthOnceAfterUnauthorized(t *testing.T) {
	account := &Account{
		ID: 100, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive,
		Credentials: map[string]any{
			"access_token":       "rejected-token",
			"refresh_token":      "refresh-token",
			"chatgpt_account_id": "org-warmup",
		},
	}
	repo := &openAIWindowForcedRefreshRepo{account: account}
	cache := &openAIWindowForcedRefreshCache{token: "rejected-token"}
	refreshExecutor := &openAIWindowForcedRefreshExecutor{}
	provider := NewOpenAITokenProvider(repo, cache, nil)
	provider.SetRefreshAPI(NewOAuthRefreshAPI(repo, cache), refreshExecutor)
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Header.Get("Authorization") == "Bearer rejected-token" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		require.Equal(t, "Bearer forced-fresh-token", r.Header.Get("Authorization"))
		w.Header().Set("content-type", "application/json")
		_, _ = w.Write([]byte(`{"rate_limit":{"allowed":true,"primary_window":null,"secondary_window":null}}`))
	}))
	defer server.Close()
	service := NewOpenAIQuotaService(repo, nil, provider, newQuotaRedirectingFactory(server))

	usage, err := service.QueryUsageForWarmup(context.Background(), account.ID)

	require.NoError(t, err)
	require.NotNil(t, usage)
	require.Equal(t, 2, calls)
	require.Equal(t, 1, refreshExecutor.refreshCalls)
	require.Equal(t, 1, repo.updateCalls)
}

func TestQueryUsageForWarmupClassifiesRejectedReplay(t *testing.T) {
	account := &Account{
		ID: 100, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive,
		Credentials: map[string]any{
			"access_token": "rejected-token", "refresh_token": "refresh-token", "chatgpt_account_id": "org-warmup",
		},
	}
	repo := &openAIWindowForcedRefreshRepo{account: account}
	cache := &openAIWindowForcedRefreshCache{token: "rejected-token"}
	provider := NewOpenAITokenProvider(repo, cache, nil)
	provider.SetRefreshAPI(NewOAuthRefreshAPI(repo, cache), &openAIWindowForcedRefreshExecutor{})
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	service := NewOpenAIQuotaService(repo, nil, provider, newQuotaRedirectingFactory(server))

	_, err := service.QueryUsageForWarmup(context.Background(), account.ID)

	require.Error(t, err)
	require.Equal(t, http.StatusUnauthorized, infraerrors.Code(err))
	failure := openAIWindowWarmupAuthFailureFromError(err)
	require.NotNil(t, failure)
	require.Equal(t, OpenAIWindowWarmupAuthReplayRejected, failure.Disposition)
	require.Equal(t, account.Credentials, failure.ExpectedCredentials)
	require.Equal(t, 2, calls)
}

func TestQueryUsageForWarmupPATDoesNotReplayUnauthorized(t *testing.T) {
	account := &Account{
		ID: 100, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive,
		Credentials: map[string]any{
			"access_token":              "pat-token",
			"chatgpt_account_id":        "org-warmup",
			openAIAuthModeCredentialKey: OpenAIAuthModePersonalAccessToken,
		},
	}
	repo := &openAIWindowForcedRefreshRepo{account: account}
	cache := &openAIWindowForcedRefreshCache{token: "pat-token"}
	provider := NewOpenAITokenProvider(repo, cache, nil)
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	service := NewOpenAIQuotaService(repo, nil, provider, newQuotaRedirectingFactory(server))

	_, err := service.QueryUsageForWarmup(context.Background(), account.ID)

	require.Error(t, err)
	require.Equal(t, http.StatusUnauthorized, infraerrors.Code(err))
	require.Equal(t, OpenAIWindowWarmupAuthNotRefreshable, openAIWindowWarmupAuthFailureFromError(err).Disposition)
	require.Equal(t, 1, calls)
}

func TestQueryUsageForWarmupClassifiesRefreshFailure(t *testing.T) {
	for _, test := range []struct {
		name       string
		refreshErr error
		wantStatus int
		want       OpenAIWindowWarmupAuthDisposition
	}{
		{name: "terminal", refreshErr: errors.New("invalid_grant: revoked"), wantStatus: http.StatusUnauthorized, want: OpenAIWindowWarmupAuthRefreshTerminal},
		{name: "transient", refreshErr: errors.New("oauth endpoint unavailable"), wantStatus: http.StatusServiceUnavailable, want: OpenAIWindowWarmupAuthRefreshTransient},
	} {
		t.Run(test.name, func(t *testing.T) {
			account := &Account{
				ID: 100, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive,
				Credentials: map[string]any{"access_token": "rejected-token", "refresh_token": "refresh-token", "chatgpt_account_id": "org-warmup"},
			}
			repo := &openAIWindowForcedRefreshRepo{account: account}
			cache := &openAIWindowForcedRefreshCache{token: "rejected-token"}
			refreshExecutor := &warmupQuotaRefreshExecutor{err: test.refreshErr}
			provider := NewOpenAITokenProvider(repo, cache, nil)
			provider.SetRefreshAPI(NewOAuthRefreshAPI(repo, cache), refreshExecutor)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
			}))
			defer server.Close()
			service := NewOpenAIQuotaService(repo, nil, provider, newQuotaRedirectingFactory(server))

			_, err := service.QueryUsageForWarmup(context.Background(), account.ID)

			require.Error(t, err)
			require.Equal(t, test.wantStatus, infraerrors.Code(err))
			failure := openAIWindowWarmupAuthFailureFromError(err)
			require.NotNil(t, failure)
			require.Equal(t, test.want, failure.Disposition)
			require.Equal(t, account.Credentials, failure.ExpectedCredentials)
		})
	}
}

func TestQueryUsageForWarmupRefreshContentionIsRetryable(t *testing.T) {
	account := &Account{
		ID: 100, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive,
		Credentials: map[string]any{
			"access_token":       "rejected-token",
			"refresh_token":      "refresh-token",
			"chatgpt_account_id": "org-warmup",
		},
	}
	repo := &openAIWindowForcedRefreshRepo{account: account}
	cache := &openAIWindowForcedRefreshCache{token: "rejected-token"}
	refreshExecutor := &warmupQuotaRefreshExecutor{err: errOpenAITokenRefreshInProgress}
	provider := NewOpenAITokenProvider(repo, cache, nil)
	provider.SetRefreshAPI(NewOAuthRefreshAPI(repo, cache), refreshExecutor)
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	service := NewOpenAIQuotaService(repo, nil, provider, newQuotaRedirectingFactory(server))

	_, err := service.QueryUsageForWarmup(context.Background(), account.ID)

	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, infraerrors.Code(err))
	require.Equal(t, "OPENAI_QUOTA_REFRESH_IN_PROGRESS", infraerrors.Reason(err))
	require.Equal(t, 1, calls, "the rejected bearer must not be replayed")
	require.Equal(t, 1, refreshExecutor.calls)
}
