//go:build unit

package handler

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/testutil"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type openAISessionAffinityHandlerCache struct {
	testutil.StubGatewayCache

	mu       sync.Mutex
	bindings map[string]int64
	setCalls map[string]int
}

func (c *openAISessionAffinityHandlerCache) GetSessionAccountID(_ context.Context, groupID int64, key string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if value, ok := c.bindings[fmt.Sprintf("%d:%s", groupID, key)]; ok {
		return value, nil
	}
	return 0, service.ErrStickySessionNotFound
}

func (c *openAISessionAffinityHandlerCache) SetSessionAccountID(_ context.Context, groupID int64, key string, value int64, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bindings == nil {
		c.bindings = make(map[string]int64)
	}
	cacheKey := fmt.Sprintf("%d:%s", groupID, key)
	c.bindings[cacheKey] = value
	if c.setCalls == nil {
		c.setCalls = make(map[string]int)
	}
	c.setCalls[cacheKey]++
	return nil
}

func TestOpenAIHTTPAdmissionBindsSessionEgressAffinityAfterAccountSticky(t *testing.T) {
	cache := &openAISessionAffinityHandlerCache{bindings: make(map[string]int64)}
	serviceUnderTest := service.NewOpenAIGatewayService(
		nil, nil, nil, nil, nil, nil,
		cache,
		nil, nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
	)
	h := &OpenAIGatewayHandler{gatewayService: serviceUnderTest}

	groupID := int64(91)
	const sessionHash = "http-affinity-handler"
	accountID := int64(701)
	routeID := int64(41)
	identityID := int64(301)
	runtimeScope := "default"
	account := &service.Account{
		ID:          accountID,
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeOAuth,
		EgressMode:  service.EgressModePool,
		Concurrency: 1,
		EgressBindings: []service.AccountEgressBinding{{
			BindingID: service.StableAccountEgressBindingID(accountID, routeID),
			AccountID: accountID,
			RouteID:   routeID,
			Position:  0,
			IsPrimary: true,
			Status:    service.AccountEgressBindingStatusActive,
			Route: &service.EgressRoute{
				ID:                 routeID,
				Kind:               service.EgressRouteKindDirect,
				RuntimeScope:       &runtimeScope,
				ExpectedIdentityID: &identityID,
				ExpectedIdentity: &service.EgressIdentity{
					ID:     identityID,
					Status: service.EgressIdentityStatusActive,
				},
				State:    service.EgressRouteStateActive,
				Revision: 1,
			},
		}},
	}
	account.SelectedEgress = &service.ResolvedAccountEgress{
		BindingID: account.EgressBindings[0].BindingID,
		RouteID:   routeID,
	}
	require.NoError(t, serviceUnderTest.BindStickySessionAfterProfitAdmission(
		context.Background(),
		&groupID,
		sessionHash,
		accountID,
	))

	h.bindOpenAISessionEgressAffinityAfterAdmission(
		context.Background(),
		&groupID,
		sessionHash,
		account,
		zap.NewNop(),
	)

	cache.mu.Lock()
	defer cache.mu.Unlock()
	accountKey := fmt.Sprintf("%d:openai:%s", groupID, sessionHash)
	require.Equal(t, accountID, cache.bindings[accountKey])

	var routeBindings []int64
	for key, value := range cache.bindings {
		if strings.Contains(key, ":openai:session:egress-affinity-route:") {
			routeBindings = append(routeBindings, value)
		}
	}
	require.Equal(t, []int64{routeID}, routeBindings)
}

func TestOpenAIHTTPAdmissionDoesNotRebindEagerSessionEgressAffinity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cache := &openAISessionAffinityHandlerCache{bindings: make(map[string]int64)}
	serviceUnderTest := service.NewOpenAIGatewayService(
		nil, nil, nil, nil, nil, nil,
		cache,
		nil, nil, nil, nil, nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil, nil,
	)
	h := &OpenAIGatewayHandler{gatewayService: serviceUnderTest}

	groupID := int64(92)
	const sessionHash = "http-affinity-eager"
	account := &service.Account{
		ID:          702,
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeOAuth,
		Status:      service.StatusActive,
		Schedulable: true,
		Concurrency: 1,
		SelectedEgress: &service.ResolvedAccountEgress{
			BindingID: service.StableAccountEgressBindingID(702, 42),
			RouteID:   42,
		},
	}
	require.NoError(t, serviceUnderTest.BindStickySessionAfterProfitAdmission(
		context.Background(), &groupID, sessionHash, account.ID,
	))
	require.NoError(t, serviceUnderTest.BindOpenAISessionEgressAffinity(
		context.Background(), &groupID, sessionHash, account,
	))

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
	streamStarted := false
	release, result := h.acquireResponsesAccountSlot(
		c,
		&groupID,
		sessionHash,
		&service.AccountSelectionResult{Account: account, Acquired: true, ReleaseFunc: func() {}},
		false,
		&streamStarted,
		zap.NewNop(),
	)

	require.Equal(t, openAISlotAcquireOK, result)
	require.NotNil(t, release)
	release()
	cache.mu.Lock()
	defer cache.mu.Unlock()
	routeSetCalls := 0
	for key, calls := range cache.setCalls {
		if strings.Contains(key, ":openai:session:egress-affinity-route:") {
			routeSetCalls += calls
		}
	}
	require.Equal(t, 1, routeSetCalls, "handler must not repeat the scheduler's eager route binding")
}
