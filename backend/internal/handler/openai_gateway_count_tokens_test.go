package handler

import (
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestAcquireCountTokensAccountSlotMaterializesWaitPlan(t *testing.T) {
	cache := &helperConcurrencyCacheStub{accountSeq: []bool{false, true}}
	h := &OpenAIGatewayHandler{
		gatewayService:    &service.OpenAIGatewayService{},
		concurrencyHelper: NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatNone, time.Millisecond),
	}
	c, _ := newHelperTestContext(http.MethodPost, "/v1/messages/count_tokens")
	groupID := int64(42)
	selection := &service.AccountSelectionResult{
		Account: &service.Account{ID: 101, Concurrency: 1},
		WaitPlan: &service.AccountWaitPlan{
			AccountID:      101,
			MaxConcurrency: 1,
			Timeout:        time.Second,
			MaxWaiting:     1,
		},
	}

	release, err := h.acquireCountTokensAccountSlot(c, &groupID, "session", selection, zap.NewNop())
	require.NoError(t, err)
	require.NotNil(t, release)

	cache.mu.Lock()
	require.Equal(t, 2, cache.accountAcquireCalls, "WaitPlan must poll until it owns a real account slot")
	cache.mu.Unlock()

	release()
	cache.mu.Lock()
	require.Equal(t, 1, cache.accountReleaseCalls)
	cache.mu.Unlock()
}
