package handler

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type countTokensLegacyEgressCacheStub struct {
	*helperConcurrencyCacheStub
	legacyAcquireCalls int
	legacyReleaseCalls int
	legacyIdentityIDs  []string
}

func (s *countTokensLegacyEgressCacheStub) AcquireAccountSlotForEgress(
	ctx context.Context,
	accountID int64,
	maxConcurrency int,
	requestID string,
	identityID string,
) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.legacyAcquireCalls++
	s.legacyIdentityIDs = append(s.legacyIdentityIDs, identityID)
	if len(s.accountSeq) == 0 {
		return false, nil
	}
	acquired := s.accountSeq[0]
	s.accountSeq = s.accountSeq[1:]
	return acquired, nil
}

func (s *countTokensLegacyEgressCacheStub) RefreshAccountSlotForEgress(
	context.Context,
	int64,
	string,
	string,
) (bool, error) {
	return true, nil
}

func (s *countTokensLegacyEgressCacheStub) ReleaseAccountSlotForEgress(
	ctx context.Context,
	accountID int64,
	requestID string,
	identityID string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.legacyReleaseCalls++
	return nil
}

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

func TestAcquireCountTokensAccountSlotPreservesLegacyEgressWhileWaiting(t *testing.T) {
	base := &helperConcurrencyCacheStub{accountSeq: []bool{false, true}}
	cache := &countTokensLegacyEgressCacheStub{helperConcurrencyCacheStub: base}
	h := &OpenAIGatewayHandler{
		gatewayService:    &service.OpenAIGatewayService{},
		concurrencyHelper: NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatNone, time.Millisecond),
	}
	c, _ := newHelperTestContext(http.MethodPost, "/v1/messages/count_tokens")
	groupID := int64(42)
	selection := &service.AccountSelectionResult{
		Account: &service.Account{
			ID:          101,
			Concurrency: 1,
			LegacyEgressAdmission: &service.LegacyAccountEgressAdmission{
				AccountID:  101,
				IdentityID: "301",
			},
		},
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
	require.Zero(t, cache.accountAcquireCalls)
	require.Equal(t, 2, cache.legacyAcquireCalls)
	require.Equal(t, []string{"301", "301"}, cache.legacyIdentityIDs)
	require.Zero(t, cache.legacyReleaseCalls, "the wait timeout context must not release an active request lease")
	cache.mu.Unlock()

	release()
	cache.mu.Lock()
	require.Equal(t, 1, cache.legacyReleaseCalls)
	cache.mu.Unlock()
}
