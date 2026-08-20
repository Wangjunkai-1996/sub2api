package handler

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWebSocketWaitPlanUsesBoundedAccountQueue(t *testing.T) {
	waitPlan := &service.AccountWaitPlan{
		AccountID:      101,
		MaxConcurrency: 1,
		Timeout:        time.Second,
		MaxWaiting:     3,
	}

	t.Run("queue full does not poll for a slot", func(t *testing.T) {
		cache := &helperConcurrencyCacheStub{accountWaitDenied: true}
		h := &OpenAIGatewayHandler{
			concurrencyHelper: NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatNone, time.Millisecond),
		}
		c, _ := newHelperTestContext(http.MethodGet, "/v1/responses")

		release, queueFull, err := h.acquireOpenAIWebSocketWaitPlanSlot(context.Background(), c, 101, waitPlan)
		require.NoError(t, err)
		require.True(t, queueFull)
		require.Nil(t, release)

		cache.mu.Lock()
		defer cache.mu.Unlock()
		require.Equal(t, 1, cache.accountWaitCalls)
		require.Equal(t, 3, cache.accountWaitMax)
		require.Zero(t, cache.accountWaitReleases)
		require.Zero(t, cache.accountAcquireCalls)
	})

	t.Run("admitted waiter is decremented after slot acquisition", func(t *testing.T) {
		cache := &helperConcurrencyCacheStub{accountSeq: []bool{true}}
		h := &OpenAIGatewayHandler{
			concurrencyHelper: NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatNone, time.Millisecond),
		}
		c, _ := newHelperTestContext(http.MethodGet, "/v1/responses")

		release, queueFull, err := h.acquireOpenAIWebSocketWaitPlanSlot(context.Background(), c, 101, waitPlan)
		require.NoError(t, err)
		require.False(t, queueFull)
		require.NotNil(t, release)

		cache.mu.Lock()
		require.Equal(t, 1, cache.accountWaitCalls)
		require.Equal(t, 1, cache.accountWaitReleases)
		require.Equal(t, 1, cache.accountAcquireCalls)
		cache.mu.Unlock()
		release()
	})
}
