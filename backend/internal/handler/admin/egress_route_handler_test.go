package admin

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestEgressAssignableCatalogAdvertisesMutationCapability(t *testing.T) {
	gin.SetMode(gin.TestMode)
	route := service.EgressRoute{
		ID:       41,
		Kind:     service.EgressRouteKindProxy,
		State:    service.EgressRouteStateActive,
		Revision: 2,
		Proxy: &service.Proxy{
			ID:       9,
			Name:     "proxy-option",
			Protocol: "http",
			Host:     "127.0.0.1",
			Port:     8080,
			Username: "must-not-leak",
			Password: "must-not-leak",
			Status:   service.StatusActive,
		},
	}
	handler := NewEgressRouteHandler(service.NewEgressService(&accountDataEgressRepo{
		routes: []service.EgressRoute{route},
	}, nil))
	router := gin.New()
	router.GET("/assignable", handler.ListAssignable)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/assignable", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.NotContains(t, rec.Body.String(), "must-not-leak")
	var envelope response.Response
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &envelope))
	data, ok := envelope.Data.(map[string]any)
	require.True(t, ok)
	capabilities, ok := data["capabilities"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, true, capabilities["mutation_enabled"])
}

func TestEgressAssignableCatalogFreezesMutationWithoutService(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/assignable", NewEgressRouteHandler(nil).ListAssignable)

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/assignable", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"mutation_enabled":false`)
	require.Contains(t, rec.Body.String(), accountEgressMutationFrozenReason)
}

func TestAccountEgressMutationFrozenUsesLockedStatus(t *testing.T) {
	err := (&AccountHandler{}).requireAccountEgressMutation()
	require.Equal(t, http.StatusLocked, int(errAccountEgressMutationFrozen.Code))
	require.Equal(t, accountEgressMutationFrozenReason, errAccountEgressMutationFrozen.Reason)
	require.ErrorIs(t, err, errAccountEgressMutationFrozen)
}

func TestCreateAccountEgressMutationReturnsLockedWhenFrozen(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := &AccountHandler{adminService: newStubAdminService()}
	router.POST("/accounts", handler.Create)
	body := []byte(`{
		"name":"frozen",
		"platform":"openai",
		"type":"oauth",
		"credentials":{"access_token":"test"},
		"egress_mode":"pool",
		"egress_pool":{"route_ids":[41],"primary_route_id":41,"concurrency_per_egress":1}
	}`)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/accounts", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)

	require.Equal(t, http.StatusLocked, rec.Code)
	require.Contains(t, rec.Body.String(), accountEgressMutationFrozenReason)
}
