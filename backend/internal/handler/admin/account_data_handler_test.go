package admin

import (
	"bytes"
	"context"
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

type accountDataSettingRepo struct {
	values      map[string]string
	getAllErr   error
	getAllCalls int
}

func (r *accountDataSettingRepo) Get(context.Context, string) (*service.Setting, error) {
	panic("unexpected Get call")
}

func (r *accountDataSettingRepo) GetValue(context.Context, string) (string, error) {
	panic("unexpected GetValue call")
}

func (r *accountDataSettingRepo) Set(context.Context, string, string) error {
	panic("unexpected Set call")
}

func (r *accountDataSettingRepo) GetMultiple(context.Context, []string) (map[string]string, error) {
	panic("unexpected GetMultiple call")
}

func (r *accountDataSettingRepo) SetMultiple(context.Context, map[string]string) error {
	panic("unexpected SetMultiple call")
}

func (r *accountDataSettingRepo) GetAll(context.Context) (map[string]string, error) {
	r.getAllCalls++
	if r.getAllErr != nil {
		return nil, r.getAllErr
	}
	out := make(map[string]string, len(r.values))
	for key, value := range r.values {
		out[key] = value
	}
	return out, nil
}

func (r *accountDataSettingRepo) Delete(context.Context, string) error {
	panic("unexpected Delete call")
}

type dataResponse struct {
	Code int         `json:"code"`
	Data dataPayload `json:"data"`
}

type dataPayload struct {
	Type           string        `json:"type"`
	Version        int           `json:"version"`
	Proxies        []dataProxy   `json:"proxies"`
	Accounts       []dataAccount `json:"accounts"`
	SkippedShadows int           `json:"skipped_shadows"`
}

type dataProxy struct {
	ProxyKey string `json:"proxy_key"`
	Name     string `json:"name"`
	Protocol string `json:"protocol"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Status   string `json:"status"`
}

type dataAccount struct {
	Name        string         `json:"name"`
	Platform    string         `json:"platform"`
	Type        string         `json:"type"`
	Credentials map[string]any `json:"credentials"`
	Extra       map[string]any `json:"extra"`
	ProxyKey    *string        `json:"proxy_key"`
	Concurrency int            `json:"concurrency"`
	Priority    int            `json:"priority"`
}

func setupAccountDataRouter() (*gin.Engine, *stubAdminService) {
	router, adminSvc, _ := setupAccountDataRouterWithSettings(&accountDataSettingRepo{values: map[string]string{
		service.SettingKeyOpenAIWindowWarmupDefaultPolicy: service.OpenAIWindowWarmupPolicyOff,
	}})
	return router, adminSvc
}

func setupAccountDataRouterWithSettings(settingRepo *accountDataSettingRepo) (*gin.Engine, *stubAdminService, *AccountHandler) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	adminSvc := newStubAdminService()

	h := NewAccountHandler(
		adminSvc,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	h.SetOpenAIWindowWarmupService(nil, service.NewSettingService(settingRepo, &config.Config{}))

	router.GET("/api/v1/admin/accounts/data", h.ExportData)
	router.POST("/api/v1/admin/accounts/data", h.ImportData)
	return router, adminSvc, h
}

func TestExportDataIncludesSecrets(t *testing.T) {
	router, adminSvc := setupAccountDataRouter()

	proxyID := int64(11)
	adminSvc.proxies = []service.Proxy{
		{
			ID:       proxyID,
			Name:     "proxy",
			Protocol: "http",
			Host:     "127.0.0.1",
			Port:     8080,
			Username: "user",
			Password: "pass",
			Status:   service.StatusActive,
		},
		{
			ID:       12,
			Name:     "orphan",
			Protocol: "https",
			Host:     "10.0.0.1",
			Port:     443,
			Username: "o",
			Password: "p",
			Status:   service.StatusActive,
		},
	}
	adminSvc.accounts = []service.Account{
		{
			ID:          21,
			Name:        "account",
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeOAuth,
			Credentials: map[string]any{"token": "secret"},
			Extra:       map[string]any{"note": "x"},
			ProxyID:     &proxyID,
			Concurrency: 3,
			Priority:    50,
			Status:      service.StatusDisabled,
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/accounts/data", nil)
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp dataResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, 0, resp.Code)
	require.Empty(t, resp.Data.Type)
	require.Equal(t, 0, resp.Data.Version)
	require.Len(t, resp.Data.Proxies, 1)
	require.Equal(t, "pass", resp.Data.Proxies[0].Password)
	require.Len(t, resp.Data.Accounts, 1)
	require.Equal(t, "secret", resp.Data.Accounts[0].Credentials["token"])
}

func TestExportDataWithoutProxies(t *testing.T) {
	router, adminSvc := setupAccountDataRouter()

	proxyID := int64(11)
	adminSvc.proxies = []service.Proxy{
		{
			ID:       proxyID,
			Name:     "proxy",
			Protocol: "http",
			Host:     "127.0.0.1",
			Port:     8080,
			Username: "user",
			Password: "pass",
			Status:   service.StatusActive,
		},
	}
	adminSvc.accounts = []service.Account{
		{
			ID:          21,
			Name:        "account",
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeOAuth,
			Credentials: map[string]any{"token": "secret"},
			ProxyID:     &proxyID,
			Concurrency: 3,
			Priority:    50,
			Status:      service.StatusDisabled,
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/accounts/data?include_proxies=false", nil)
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp dataResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, 0, resp.Code)
	require.Len(t, resp.Data.Proxies, 0)
	require.Len(t, resp.Data.Accounts, 1)
	require.Nil(t, resp.Data.Accounts[0].ProxyKey)
}

// TestExportDataExcludesSparkShadow 验证外审第5轮 P1/P2:导出时排除 spark 影子账号
// (影子无凭据、导入侧强制 credentials 非空,混入会产出无法还原的坏备份),并透出跳过计数。
func TestExportDataExcludesSparkShadow(t *testing.T) {
	router, adminSvc := setupAccountDataRouter()

	parentID := int64(21)
	adminSvc.accounts = []service.Account{
		{
			ID:          parentID,
			Name:        "mother",
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeOAuth,
			Credentials: map[string]any{"token": "secret"},
			Status:      service.StatusActive,
		},
		{
			ID:              22,
			Name:            "mother (Spark)",
			Platform:        service.PlatformOpenAI,
			Type:            service.AccountTypeOAuth,
			Credentials:     map[string]any{}, // 影子恒空凭据
			ParentAccountID: &parentID,        // 影子标记
			QuotaDimension:  service.QuotaDimensionSpark,
			Status:          service.StatusActive,
		},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/accounts/data?include_proxies=false", nil)
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp dataResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, 0, resp.Code)
	require.Len(t, resp.Data.Accounts, 1, "影子应被排除,仅导出母账号")
	require.Equal(t, "mother", resp.Data.Accounts[0].Name)
	require.Equal(t, 1, resp.Data.SkippedShadows, "跳过的影子数量应透出")
}

func TestExportDataPassesAccountFiltersAndSort(t *testing.T) {
	router, adminSvc := setupAccountDataRouter()
	adminSvc.accounts = []service.Account{
		{ID: 1, Name: "acc-1", Status: service.StatusActive},
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/admin/accounts/data?platform=openai&type=oauth&status=active&group=12&privacy_mode=blocked&search=keyword&sort_by=priority&sort_order=desc",
		nil,
	)
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	require.Equal(t, 1, adminSvc.lastListAccounts.calls)
	require.Equal(t, "openai", adminSvc.lastListAccounts.platform)
	require.Equal(t, "oauth", adminSvc.lastListAccounts.accountType)
	require.Equal(t, "active", adminSvc.lastListAccounts.status)
	require.Equal(t, int64(12), adminSvc.lastListAccounts.groupID)
	require.Equal(t, "blocked", adminSvc.lastListAccounts.privacyMode)
	require.Equal(t, "keyword", adminSvc.lastListAccounts.search)
	require.Equal(t, "priority", adminSvc.lastListAccounts.sortBy)
	require.Equal(t, "desc", adminSvc.lastListAccounts.sortOrder)
}

func TestExportDataSelectedIDsOverrideFilters(t *testing.T) {
	router, adminSvc := setupAccountDataRouter()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/admin/accounts/data?ids=1,2&platform=openai&search=keyword&sort_by=priority&sort_order=desc",
		nil,
	)
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp dataResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Equal(t, 0, resp.Code)
	require.Len(t, resp.Data.Accounts, 2)
	require.Equal(t, 0, adminSvc.lastListAccounts.calls)
}

func TestImportDataReusesProxyAndSkipsDefaultGroup(t *testing.T) {
	router, adminSvc := setupAccountDataRouter()

	adminSvc.proxies = []service.Proxy{
		{
			ID:       1,
			Name:     "proxy",
			Protocol: "socks5",
			Host:     "1.2.3.4",
			Port:     1080,
			Username: "u",
			Password: "p",
			Status:   service.StatusActive,
		},
	}

	dataPayload := map[string]any{
		"data": map[string]any{
			"type":    dataType,
			"version": dataVersion,
			"proxies": []map[string]any{
				{
					"proxy_key": "socks5|1.2.3.4|1080|u|p",
					"name":      "proxy",
					"protocol":  "socks5",
					"host":      "1.2.3.4",
					"port":      1080,
					"username":  "u",
					"password":  "p",
					"status":    "active",
				},
			},
			"accounts": []map[string]any{
				{
					"name":        "acc",
					"platform":    service.PlatformOpenAI,
					"type":        service.AccountTypeOAuth,
					"credentials": map[string]any{"token": "x"},
					"proxy_key":   "socks5|1.2.3.4|1080|u|p",
					"concurrency": 3,
					"priority":    50,
				},
			},
		},
		"skip_default_group_bind": true,
	}

	body, _ := json.Marshal(dataPayload)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/accounts/data", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	require.Len(t, adminSvc.createdProxies, 0)
	require.Len(t, adminSvc.createdAccounts, 1)
	require.True(t, adminSvc.createdAccounts[0].SkipDefaultGroupBind)
}

func TestImportDataOpenAIOAuthInheritsContinuousWarmupDefaultOncePerBatch(t *testing.T) {
	settingRepo := &accountDataSettingRepo{values: map[string]string{
		service.SettingKeyOpenAIWindowWarmupDefaultPolicy: service.OpenAIWindowWarmupPolicyContinuous,
	}}
	_, adminSvc, handler := setupAccountDataRouterWithSettings(settingRepo)

	result, err := handler.importData(context.Background(), DataImportRequest{Data: DataPayload{
		Accounts: []DataAccount{
			{
				Name: "openai-1", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
				Credentials: map[string]any{"access_token": "token-1"},
			},
			{
				Name: "openai-2", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
				Credentials: map[string]any{"access_token": "token-2"}, Extra: map[string]any{"preserved": true},
			},
		},
	}})

	require.NoError(t, err)
	require.Equal(t, 2, result.AccountCreated)
	require.Equal(t, 1, settingRepo.getAllCalls, "the batch must read global settings only once")
	require.Len(t, adminSvc.createdAccounts, 2)
	for _, input := range adminSvc.createdAccounts {
		require.Equal(t, service.OpenAIWindowWarmupPolicyContinuous, input.Extra[service.OpenAICodexWarmupPolicyExtraKey])
	}
	require.Equal(t, true, adminSvc.createdAccounts[1].Extra["preserved"])
}

func TestImportDataOpenAIOAuthPreservesExplicitWarmupPolicies(t *testing.T) {
	settingRepo := &accountDataSettingRepo{getAllErr: errors.New("settings must not be read")}
	_, adminSvc, handler := setupAccountDataRouterWithSettings(settingRepo)
	accounts := []DataAccount{
		{
			Name: "canonical-off", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
			Credentials: map[string]any{"access_token": "token-1"},
			Extra:       map[string]any{service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyOff},
		},
		{
			Name: "legacy-once", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
			Credentials: map[string]any{"access_token": "token-2"},
			Extra:       map[string]any{service.CodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyInitialOnce},
		},
		{
			Name: "legacy-continuous", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
			Credentials: map[string]any{"access_token": "token-3"},
			Extra:       map[string]any{service.OpenAIWindowWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous},
		},
	}

	result, err := handler.importData(context.Background(), DataImportRequest{Data: DataPayload{Accounts: accounts}})

	require.NoError(t, err)
	require.Equal(t, 3, result.AccountCreated)
	require.Zero(t, settingRepo.getAllCalls)
	require.Len(t, adminSvc.createdAccounts, 3)
	want := []string{
		service.OpenAIWindowWarmupPolicyOff,
		service.OpenAIWindowWarmupPolicyInitialOnce,
		service.OpenAIWindowWarmupPolicyContinuous,
	}
	for i, input := range adminSvc.createdAccounts {
		require.Equal(t, want[i], input.Extra[service.OpenAICodexWarmupPolicyExtraKey])
		require.NotContains(t, input.Extra, service.CodexWarmupPolicyExtraKey)
		require.NotContains(t, input.Extra, service.OpenAIWindowWarmupPolicyExtraKey)
	}
}

func TestImportDataNonOpenAIAccountsDoNotReadOrRewriteWarmupPolicy(t *testing.T) {
	settingRepo := &accountDataSettingRepo{getAllErr: errors.New("settings unavailable")}
	_, adminSvc, handler := setupAccountDataRouterWithSettings(settingRepo)
	extra := map[string]any{
		service.OpenAICodexWarmupPolicyExtraKey: "not-an-openai-policy",
		"preserved":                             true,
	}

	result, err := handler.importData(context.Background(), DataImportRequest{Data: DataPayload{
		Accounts: []DataAccount{
			{
				Name: "anthropic-oauth", Platform: service.PlatformAnthropic, Type: service.AccountTypeOAuth,
				Credentials: map[string]any{"access_token": "token-1"}, Extra: extra,
			},
			{
				Name: "openai-apikey", Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
				Credentials: map[string]any{"api_key": "key-1"},
			},
		},
	}})

	require.NoError(t, err)
	require.Equal(t, 2, result.AccountCreated)
	require.Zero(t, settingRepo.getAllCalls)
	require.Len(t, adminSvc.createdAccounts, 2)
	require.Equal(t, extra, adminSvc.createdAccounts[0].Extra)
	require.Nil(t, adminSvc.createdAccounts[1].Extra)
}

func TestImportDataWarmupSettingReadFailurePreventsAllWrites(t *testing.T) {
	settingRepo := &accountDataSettingRepo{getAllErr: errors.New("settings database unavailable")}
	_, adminSvc, handler := setupAccountDataRouterWithSettings(settingRepo)

	_, err := handler.importData(context.Background(), DataImportRequest{Data: DataPayload{
		Proxies: []DataProxy{{
			Name: "proxy", Protocol: "http", Host: "127.0.0.1", Port: 8080, Status: service.StatusActive,
		}},
		Accounts: []DataAccount{{
			Name: "openai", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
			Credentials: map[string]any{"access_token": "token"},
		}},
	}})

	require.ErrorContains(t, err, "settings database unavailable")
	require.Equal(t, 1, settingRepo.getAllCalls)
	require.Empty(t, adminSvc.createdProxies)
	require.Empty(t, adminSvc.updatedProxies)
	require.Empty(t, adminSvc.createdAccounts)
}

func TestImportDataInvalidExplicitWarmupPolicyPreventsAllWrites(t *testing.T) {
	settingRepo := &accountDataSettingRepo{values: map[string]string{
		service.SettingKeyOpenAIWindowWarmupDefaultPolicy: service.OpenAIWindowWarmupPolicyContinuous,
	}}
	_, adminSvc, handler := setupAccountDataRouterWithSettings(settingRepo)

	_, err := handler.importData(context.Background(), DataImportRequest{Data: DataPayload{
		Proxies: []DataProxy{{
			Name: "proxy", Protocol: "http", Host: "127.0.0.1", Port: 8080, Status: service.StatusActive,
		}},
		Accounts: []DataAccount{{
			Name: "openai", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
			Credentials: map[string]any{"access_token": "token"},
			Extra:       map[string]any{service.OpenAICodexWarmupPolicyExtraKey: "sometimes"},
		}},
	}})

	require.Error(t, err)
	require.Zero(t, settingRepo.getAllCalls)
	require.Empty(t, adminSvc.createdProxies)
	require.Empty(t, adminSvc.updatedProxies)
	require.Empty(t, adminSvc.createdAccounts)
}
