package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func openAICyberCooldownSettingsRequest(t *testing.T, handler *SettingHandler, method string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	var raw []byte
	var err error
	if body != nil {
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(method, "/api/v1/admin/settings", bytes.NewReader(raw))
	if body != nil {
		c.Request.Header.Set("Content-Type", "application/json")
	}
	if method == http.MethodPut {
		handler.UpdateSettings(c)
	} else {
		handler.GetSettings(c)
	}
	return recorder
}

func TestOpenAICyberAccountCooldownSettingsGetPutRoundTrip(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &settingHandlerRepoStub{values: map[string]string{service.SettingKeyPromoCodeEnabled: "true"}}
	svc := service.NewSettingService(repo, &config.Config{Default: config.DefaultConfig{UserConcurrency: 5}})
	handler := NewSettingHandler(svc, nil, nil, nil, nil, nil, nil)

	put := openAICyberCooldownSettingsRequest(t, handler, http.MethodPut, map[string]any{
		"openai_cyber_account_cooldown_enabled":           true,
		"openai_cyber_account_cooldown_window_seconds":    7200,
		"openai_cyber_account_cooldown_first_seconds":     600,
		"openai_cyber_account_cooldown_escalated_seconds": 3600,
		"openai_cyber_account_cooldown_group_ids":         []int64{13, 12, 0, 13},
	})
	require.Equal(t, http.StatusOK, put.Code, put.Body.String())
	require.Equal(t, "true", repo.values[service.SettingKeyOpenAICyberAccountCooldownEnabled])
	require.Equal(t, "7200", repo.values[service.SettingKeyOpenAICyberAccountCooldownWindowSeconds])
	require.Equal(t, "600", repo.values[service.SettingKeyOpenAICyberAccountCooldownFirstSeconds])
	require.Equal(t, "3600", repo.values[service.SettingKeyOpenAICyberAccountCooldownEscalatedSeconds])
	require.Equal(t, "[12,13]", repo.values[service.SettingKeyOpenAICyberAccountCooldownGroupIDs])

	get := openAICyberCooldownSettingsRequest(t, handler, http.MethodGet, nil)
	require.Equal(t, http.StatusOK, get.Code, get.Body.String())
	var response struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &response))
	require.JSONEq(t, "true", string(response.Data[service.SettingKeyOpenAICyberAccountCooldownEnabled]))
	require.JSONEq(t, "7200", string(response.Data[service.SettingKeyOpenAICyberAccountCooldownWindowSeconds]))
	require.JSONEq(t, "600", string(response.Data[service.SettingKeyOpenAICyberAccountCooldownFirstSeconds]))
	require.JSONEq(t, "3600", string(response.Data[service.SettingKeyOpenAICyberAccountCooldownEscalatedSeconds]))
	require.JSONEq(t, "[12,13]", string(response.Data[service.SettingKeyOpenAICyberAccountCooldownGroupIDs]))
	require.NotContains(t, response.Data, "cyber_session_block_enabled")
	require.NotContains(t, response.Data, "cyber_session_block_ttl_seconds")
	require.NotContains(t, response.Data, "cyber_session_block_all_groups")
	require.NotContains(t, response.Data, "cyber_session_block_group_ids")
	require.NotContains(t, response.Data, "openai_account_audit_long_text_oauth_rollout_percent")
}

func TestOpenAICyberAccountCooldownSettingsDefaultsToGroup12(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &settingHandlerRepoStub{values: map[string]string{service.SettingKeyPromoCodeEnabled: "true"}}
	svc := service.NewSettingService(repo, &config.Config{Default: config.DefaultConfig{UserConcurrency: 5}})
	handler := NewSettingHandler(svc, nil, nil, nil, nil, nil, nil)

	get := openAICyberCooldownSettingsRequest(t, handler, http.MethodGet, nil)
	require.Equal(t, http.StatusOK, get.Code, get.Body.String())
	var response struct {
		Data struct {
			Enabled   bool    `json:"openai_cyber_account_cooldown_enabled"`
			Window    int     `json:"openai_cyber_account_cooldown_window_seconds"`
			First     int     `json:"openai_cyber_account_cooldown_first_seconds"`
			Escalated int     `json:"openai_cyber_account_cooldown_escalated_seconds"`
			GroupIDs  []int64 `json:"openai_cyber_account_cooldown_group_ids"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &response))
	require.False(t, response.Data.Enabled)
	require.Equal(t, 86400, response.Data.Window)
	require.Equal(t, 3600, response.Data.First)
	require.Equal(t, 86400, response.Data.Escalated)
	require.Equal(t, []int64{12}, response.Data.GroupIDs)
}

func TestOpenAICyberAccountCooldownSettingsRejectInvalidRangeAndOrder(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, body := range []map[string]any{
		{"openai_cyber_account_cooldown_window_seconds": 59},
		{
			"openai_cyber_account_cooldown_first_seconds":     3600,
			"openai_cyber_account_cooldown_escalated_seconds": 600,
		},
	} {
		repo := &settingHandlerRepoStub{values: map[string]string{service.SettingKeyPromoCodeEnabled: "true"}}
		svc := service.NewSettingService(repo, &config.Config{Default: config.DefaultConfig{UserConcurrency: 5}})
		handler := NewSettingHandler(svc, nil, nil, nil, nil, nil, nil)
		response := openAICyberCooldownSettingsRequest(t, handler, http.MethodPut, body)
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	}
}
