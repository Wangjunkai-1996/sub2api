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

func cyberSettingsRequest(t *testing.T, handler *SettingHandler, method string, body map[string]any) *httptest.ResponseRecorder {
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

func TestCyberSessionBlockSettings_GetPutRoundTrip(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &settingHandlerRepoStub{values: map[string]string{service.SettingKeyPromoCodeEnabled: "true"}}
	svc := service.NewSettingService(repo, &config.Config{Default: config.DefaultConfig{UserConcurrency: 5}})
	handler := NewSettingHandler(svc, nil, nil, nil, nil, nil, nil)

	put := cyberSettingsRequest(t, handler, http.MethodPut, map[string]any{
		"cyber_session_block_enabled":     true,
		"cyber_session_block_ttl_seconds": 120,
		"cyber_session_block_all_groups":  false,
		"cyber_session_block_group_ids":   []int64{13, 12, 0, 13},
	})
	require.Equal(t, http.StatusOK, put.Code, put.Body.String())
	require.Equal(t, "true", repo.values[service.SettingKeyCyberSessionBlockEnabled])
	require.Equal(t, "120", repo.values[service.SettingKeyCyberSessionBlockTTLSeconds])
	require.Equal(t, "false", repo.values[service.SettingKeyCyberSessionBlockAllGroups])
	require.Equal(t, `[12,13]`, repo.values[service.SettingKeyCyberSessionBlockGroupIDs])

	get := cyberSettingsRequest(t, handler, http.MethodGet, nil)
	require.Equal(t, http.StatusOK, get.Code, get.Body.String())
	var response struct {
		Data struct {
			Enabled   bool    `json:"cyber_session_block_enabled"`
			TTL       int     `json:"cyber_session_block_ttl_seconds"`
			AllGroups bool    `json:"cyber_session_block_all_groups"`
			GroupIDs  []int64 `json:"cyber_session_block_group_ids"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(get.Body.Bytes(), &response))
	require.True(t, response.Data.Enabled)
	require.Equal(t, 120, response.Data.TTL)
	require.False(t, response.Data.AllGroups)
	require.Equal(t, []int64{12, 13}, response.Data.GroupIDs)
}

func TestCyberSessionBlockSettings_PutRejectsEmptySelectedScope(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := &settingHandlerRepoStub{values: map[string]string{service.SettingKeyPromoCodeEnabled: "true"}}
	svc := service.NewSettingService(repo, &config.Config{Default: config.DefaultConfig{UserConcurrency: 5}})
	handler := NewSettingHandler(svc, nil, nil, nil, nil, nil, nil)

	response := cyberSettingsRequest(t, handler, http.MethodPut, map[string]any{
		"cyber_session_block_all_groups": false,
		"cyber_session_block_group_ids":  []int64{0, -1},
	})
	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "cyber_session_block_group_ids")
	_, persisted := repo.values[service.SettingKeyCyberSessionBlockAllGroups]
	require.False(t, persisted)
}
