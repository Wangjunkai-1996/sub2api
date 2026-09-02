//go:build unit

package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestAccountEgressPoolInputExplicitLegacyUpdate(t *testing.T) {
	mode := service.EgressModeLegacy
	input, err := accountEgressPoolInput(&mode, nil, false)

	require.NoError(t, err)
	require.NotNil(t, input)
	require.Equal(t, service.EgressModeLegacy, input.Mode)
	require.Empty(t, input.RouteIDs)
	require.Nil(t, input.ExpectedRevision)
}

func TestAccountEgressPoolInputLegacyCreateUsesCompatibilityPath(t *testing.T) {
	mode := service.EgressModeLegacy
	input, err := accountEgressPoolInput(&mode, nil, true)

	require.NoError(t, err)
	require.Nil(t, input)
}

func TestAccountEgressPoolInputPoolUpdateRequiresRevision(t *testing.T) {
	mode := service.EgressModePool
	concurrency := 4
	_, err := accountEgressPoolInput(&mode, &AccountEgressPoolRequest{
		RouteIDs:             []int64{11},
		PrimaryRouteID:       ptrAccountEgressRequestInt64(11),
		ConcurrencyPerEgress: &concurrency,
	}, false)

	require.Error(t, err)
	require.Equal(t, "ACCOUNT_EGRESS_REVISION_REQUIRED", infraerrors.Reason(err))
}

func TestValidateOpenAIEgressWriteOnlyAllowsOAuth(t *testing.T) {
	require.NoError(t, validateOpenAIEgressWrite(service.PlatformOpenAI, service.AccountTypeOAuth, false))

	for _, tc := range []struct {
		name        string
		platform    string
		accountType string
		shadow      bool
	}{
		{name: "openai api key", platform: service.PlatformOpenAI, accountType: service.AccountTypeAPIKey},
		{name: "other platform oauth", platform: service.PlatformGemini, accountType: service.AccountTypeOAuth},
		{name: "spark shadow", platform: service.PlatformOpenAI, accountType: service.AccountTypeOAuth, shadow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, validateOpenAIEgressWrite(tc.platform, tc.accountType, tc.shadow))
		})
	}
}

func TestOpenAIAuthImportRequestsBindCompleteEgressPool(t *testing.T) {
	body := []byte(`{
		"egress_mode":"pool",
		"egress_pool":{"route_ids":[11,12,13],"primary_route_id":11,"concurrency_per_egress":3}
	}`)

	var sessionReq CodexSessionImportRequest
	require.NoError(t, json.Unmarshal(body, &sessionReq))
	var patReq OpenAICodexPATCreateRequest
	require.NoError(t, json.Unmarshal(body, &patReq))

	for _, request := range []struct {
		mode *string
		pool *AccountEgressPoolRequest
	}{{sessionReq.EgressMode, sessionReq.EgressPool}, {patReq.EgressMode, patReq.EgressPool}} {
		pool, err := accountEgressPoolInput(request.mode, request.pool, true)
		require.NoError(t, err)
		require.Equal(t, []int64{11, 12, 13}, pool.RouteIDs)
		require.Equal(t, int64(11), pool.PrimaryRouteID)
		require.Equal(t, 3, *pool.ConcurrencyPerEgress)
	}
}

func TestCreateAccountFromCodexPATRejectsConflictingEgressAuthorities(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name   string
		extra  string
		reason string
	}{
		{name: "legacy proxy", extra: `,"proxy_id":9`, reason: "ACCOUNT_EGRESS_POOL_PROXY_CONFLICT"},
		{name: "legacy route", extra: `,"egress_route_id":9`, reason: "ACCOUNT_EGRESS_POOL_ROUTE_CONFLICT"},
		{name: "different concurrency", extra: `,"concurrency":4`, reason: "ACCOUNT_EGRESS_POOL_CONCURRENCY_CONFLICT"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router := gin.New()
			router.POST("/pat", (&OpenAIOAuthHandler{}).CreateAccountFromCodexPAT)
			body := `{"access_token":"at-test","egress_mode":"pool","egress_pool":{"route_ids":[11,12],"primary_route_id":11,"concurrency_per_egress":3}` + tc.extra + `}`
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/pat", bytes.NewBufferString(body))
			request.Header.Set("Content-Type", "application/json")

			router.ServeHTTP(recorder, request)

			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Contains(t, recorder.Body.String(), tc.reason)
		})
	}
}

func TestResolveOpenAICodexPATCreateEgressUsesPoolPrimaryForAuthentication(t *testing.T) {
	mode := service.EgressModePool
	concurrency := service.DefaultOpenAIOAuthEgressConcurrency
	resolved, err := resolveOpenAICodexPATCreateEgress(OpenAICodexPATCreateRequest{
		EgressMode: &mode,
		EgressPool: &AccountEgressPoolRequest{
			RouteIDs:             []int64{11, 12, 13},
			PrimaryRouteID:       ptrAccountEgressRequestInt64(12),
			ConcurrencyPerEgress: &concurrency,
		},
	})

	require.NoError(t, err)
	require.Nil(t, resolved.proxyID)
	require.NotNil(t, resolved.authRouteID)
	require.Equal(t, int64(12), *resolved.authRouteID)
	require.Equal(t, 3, resolved.concurrency)
	require.NotNil(t, resolved.pool)
	require.Equal(t, []int64{11, 12, 13}, resolved.pool.RouteIDs)
	require.Equal(t, int64(12), resolved.pool.PrimaryRouteID)
}

func ptrAccountEgressRequestInt64(value int64) *int64 {
	return &value
}
