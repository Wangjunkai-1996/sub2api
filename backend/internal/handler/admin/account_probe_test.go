package admin

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type accountProbeRepo struct {
	service.AccountRepository
	account service.Account
	clears  int
}

func (r *accountProbeRepo) GetByID(context.Context, int64) (*service.Account, error) {
	copy := r.account
	return &copy, nil
}

func (r *accountProbeRepo) SetError(_ context.Context, _ int64, message string) error {
	r.account.Status, r.account.ErrorMessage, r.account.Schedulable = service.StatusError, message, false
	return nil
}

func (r *accountProbeRepo) ClearError(context.Context, int64) error {
	r.clears++
	r.account.Status, r.account.ErrorMessage = service.StatusActive, ""
	return nil
}

func (r *accountProbeRepo) UpdateExtra(context.Context, int64, map[string]any) error { return nil }

type accountProbeUpstream struct {
	before func()
	status int
	calls  int
}

func (u *accountProbeUpstream) Do(_ *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.calls++
	if u.before != nil {
		u.before()
	}
	body := "data: {\"type\":\"response.completed\"}\n\n"
	if u.status != http.StatusOK {
		body = `{"error":{"code":"token_revoked"}}`
	}
	return &http.Response{StatusCode: u.status, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
}

func (u *accountProbeUpstream) DoWithTLS(r *http.Request, proxy string, id int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(r, proxy, id, concurrency)
}

func TestAccountProbePreservesRuntimeState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, scenario := range []string{"concurrent_401", "existing_error", "manual_pause", "cooldown", "probe_401", "interactive_test"} {
		t.Run(scenario, func(t *testing.T) {
			repo := &accountProbeRepo{account: service.Account{
				ID: 42, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
				Status: service.StatusActive, Schedulable: true,
				Credentials: map[string]any{"access_token": "test-at", "chatgpt_account_id": "workspace"},
			}}
			upstream := &accountProbeUpstream{status: http.StatusOK}
			path := "/probe"
			switch scenario {
			case "concurrent_401", "interactive_test":
				upstream.before = func() { _ = repo.SetError(context.Background(), 42, "Token revoked (401): invalidated oauth token") }
				if scenario == "interactive_test" {
					path = "/test"
				}
			case "existing_error":
				_ = repo.SetError(context.Background(), 42, "Token revoked (401): invalidated oauth token")
			case "manual_pause":
				repo.account.Schedulable = false
			case "cooldown":
				until := time.Now().Add(time.Hour)
				repo.account.TempUnschedulableUntil = &until
			case "probe_401":
				upstream.status = http.StatusUnauthorized
			}
			cfg := &config.Config{}
			tester := service.NewAccountTestService(repo, nil, nil, nil, nil, upstream, cfg, nil)
			handler := &AccountHandler{accountTestService: tester, rateLimitService: service.NewRateLimitService(repo, nil, cfg, nil, nil)}
			router := gin.New()
			router.POST("/accounts/:id/probe", handler.Probe)
			router.POST("/accounts/:id/test", handler.Test)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/accounts/42"+path, strings.NewReader(`{"model_id":"gpt-5.6-luna"}`))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, 1, upstream.calls)
			if scenario == "interactive_test" {
				require.Equal(t, 1, repo.clears, "existing interactive recovery stays available")
				require.Equal(t, service.StatusActive, repo.account.Status)
				return
			}
			require.Zero(t, repo.clears)
			if scenario == "cooldown" {
				require.True(t, repo.account.TempUnschedulableUntil.After(time.Now()))
			} else {
				require.False(t, repo.account.Schedulable)
			}
			if scenario == "concurrent_401" || scenario == "existing_error" || scenario == "probe_401" {
				require.Equal(t, service.StatusError, repo.account.Status)
				require.NotEmpty(t, repo.account.ErrorMessage)
			}
			if scenario == "probe_401" {
				require.Contains(t, rec.Body.String(), `"type":"error"`)
			} else {
				require.Contains(t, rec.Body.String(), `"type":"test_complete"`)
			}
		})
	}
}

func TestAccountProbeRejectsMediaAndUnsupportedAccounts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, scenario := range []string{"media_alias", "apikey", "other_platform", "missing_model", "invalid_id"} {
		t.Run(scenario, func(t *testing.T) {
			repo := &accountProbeRepo{account: service.Account{ID: 42, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth}}
			body, id := `{"model_id":"alias"}`, "42"
			switch scenario {
			case "media_alias":
				repo.account.Credentials = map[string]any{"model_mapping": map[string]any{"alias": "gpt-image-2"}}
			case "apikey":
				repo.account.Type = service.AccountTypeAPIKey
			case "other_platform":
				repo.account.Platform = service.PlatformGrok
			case "missing_model":
				body = `{}`
			case "invalid_id":
				id = "-1"
			}
			upstream := &accountProbeUpstream{status: http.StatusOK}
			h := &AccountHandler{accountTestService: service.NewAccountTestService(repo, nil, nil, nil, nil, upstream, &config.Config{}, nil)}
			router := gin.New()
			router.POST("/accounts/:id/probe", h.Probe)
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/accounts/"+id+"/probe", strings.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(rec, req)
			require.Zero(t, upstream.calls)
			require.NotContains(t, rec.Body.String(), `"type":"test_complete"`)
		})
	}
}
