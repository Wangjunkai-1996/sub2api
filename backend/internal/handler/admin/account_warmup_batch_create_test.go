package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func setupWarmupBatchCreateRouter(settingRepo *accountDataSettingRepo) (*gin.Engine, *stubAdminService) {
	gin.SetMode(gin.TestMode)
	adminSvc := newStubAdminService()
	handler := NewAccountHandler(adminSvc, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	handler.SetOpenAIWindowWarmupService(nil, service.NewSettingService(settingRepo, &config.Config{}))
	router := gin.New()
	router.POST("/api/v1/admin/accounts/batch", handler.BatchCreate)
	return router, adminSvc
}

func postWarmupBatchCreate(t *testing.T, router *gin.Engine, accounts []map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{"accounts": accounts})
	require.NoError(t, err)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/accounts/batch", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(recorder, request)
	return recorder
}

func TestBatchCreateOpenAIOAuthInheritsWarmupDefaultOnce(t *testing.T) {
	settings := &accountDataSettingRepo{values: map[string]string{
		service.SettingKeyOpenAIWindowWarmupDefaultPolicy: service.OpenAIWindowWarmupPolicyContinuous,
	}}
	router, adminSvc := setupWarmupBatchCreateRouter(settings)

	recorder := postWarmupBatchCreate(t, router, []map[string]any{
		{"name": "openai-1", "platform": "openai", "type": "oauth", "credentials": map[string]any{"access_token": "token-1"}},
		{"name": "openai-2", "platform": "openai", "type": "oauth", "credentials": map[string]any{"access_token": "token-2"}},
		{"name": "anthropic", "platform": "anthropic", "type": "oauth", "credentials": map[string]any{"access_token": "token-3"}},
	})

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, 1, settings.getAllCalls)
	require.Len(t, adminSvc.createdAccounts, 3)
	for _, input := range adminSvc.createdAccounts[:2] {
		require.Equal(t, service.OpenAIWindowWarmupPolicyContinuous, input.Extra[service.OpenAICodexWarmupPolicyExtraKey])
	}
	require.NotContains(t, adminSvc.createdAccounts[2].Extra, service.OpenAICodexWarmupPolicyExtraKey)
}

func TestBatchCreateWarmupExplicitOffAndSettingsFailureArePreflighted(t *testing.T) {
	t.Run("explicit off is canonical and does not read settings", func(t *testing.T) {
		settings := &accountDataSettingRepo{getAllErr: errors.New("settings must not be read")}
		router, adminSvc := setupWarmupBatchCreateRouter(settings)
		recorder := postWarmupBatchCreate(t, router, []map[string]any{{
			"name": "openai", "platform": "openai", "type": "oauth",
			"credentials": map[string]any{"access_token": "token"}, "openai_codex_warmup_policy": "off",
		}})

		require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
		require.Zero(t, settings.getAllCalls)
		require.Len(t, adminSvc.createdAccounts, 1)
		require.Equal(t, service.OpenAIWindowWarmupPolicyOff, adminSvc.createdAccounts[0].Extra[service.OpenAICodexWarmupPolicyExtraKey])
	})

	t.Run("settings failure prevents every account write", func(t *testing.T) {
		settings := &accountDataSettingRepo{getAllErr: errors.New("settings unavailable")}
		router, adminSvc := setupWarmupBatchCreateRouter(settings)
		recorder := postWarmupBatchCreate(t, router, []map[string]any{
			{"name": "anthropic", "platform": "anthropic", "type": "oauth", "credentials": map[string]any{"access_token": "token-1"}},
			{"name": "openai", "platform": "openai", "type": "oauth", "credentials": map[string]any{"access_token": "token-2"}},
		})

		require.NotEqual(t, http.StatusOK, recorder.Code)
		require.Equal(t, 1, settings.getAllCalls)
		require.Empty(t, adminSvc.createdAccounts)
	})
}

func TestBatchCreateRejectsWarmupPolicyForIneligibleAccountBeforeWrites(t *testing.T) {
	settings := &accountDataSettingRepo{values: map[string]string{
		service.SettingKeyOpenAIWindowWarmupDefaultPolicy: service.OpenAIWindowWarmupPolicyContinuous,
	}}
	router, adminSvc := setupWarmupBatchCreateRouter(settings)
	recorder := postWarmupBatchCreate(t, router, []map[string]any{
		{"name": "openai", "platform": "openai", "type": "oauth", "credentials": map[string]any{"access_token": "token-1"}},
		{"name": "openai-key", "platform": "openai", "type": "apikey", "credentials": map[string]any{"api_key": "key"}, "openai_codex_warmup_policy": "continuous"},
	})

	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Zero(t, settings.getAllCalls)
	require.Empty(t, adminSvc.createdAccounts)
}
