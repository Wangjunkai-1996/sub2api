package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAIManagedRetryAccountUsesFreshAdmission(t *testing.T) {
	groupID := int64(10)
	for _, tc := range []struct {
		name      string
		preferred Account
		wantID    int64
		noCache   bool
	}{
		{"eligible", Account{ID: 1, GroupIDs: []int64{groupID}}, 1, false},
		{"eligible without sticky cache", Account{ID: 1, GroupIDs: []int64{groupID}}, 1, true},
		{"out of group", Account{ID: 1, GroupIDs: []int64{99}}, 2, false},
		{"cooling", Account{ID: 1, GroupIDs: []int64{groupID}, RateLimitResetAt: timePointer(time.Now().Add(time.Hour))}, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			preferred := tc.preferred
			preferred.Platform, preferred.Type, preferred.Status = PlatformOpenAI, AccountTypeAPIKey, StatusActive
			preferred.Schedulable, preferred.Concurrency = true, 1
			preferred.Priority = 10
			backup := Account{ID: 2, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1, GroupIDs: []int64{groupID}}
			cache := &schedulerTestGatewayCache{}
			svc := &OpenAIGatewayService{
				accountRepo: schedulerGroupAwareOpenAIAccountRepo{schedulerTestOpenAIAccountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{preferred, backup}}},
				cache:       cache, cfg: &config.Config{},
				rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true"),
				concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{}),
			}
			if tc.noCache {
				svc.cache = nil
			}
			ctx := WithOpenAIManagedRetryAccount(WithOpenAIManagedRecovery(context.Background()), preferred.ID)
			selection, _, err := svc.SelectAccountWithSchedulerForCapability(ctx, &groupID, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, "", false, false, true)
			require.NoError(t, err)
			require.NotNil(t, selection)
			require.NotNil(t, selection.Account)
			assert.Equal(t, tc.wantID, selection.Account.ID)
			assert.Empty(t, cache.sessionBindings["openai:managed_retry"])
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
		})
	}
}

func TestOpenAIManagedDispatchObserverCountsActualAttemptsAcrossOldRetryDeadline(t *testing.T) {
	ctx := WithOpenAIManagedRecovery(WithOpenAIRetryBudgetDeadline(context.Background(), time.Now().Add(-time.Second)))
	ctx = WithOpenAIModelDispatchBudget(ctx)
	var observed []int
	ctx = WithOpenAIModelDispatchObserver(ctx, func(attempt int) error {
		observed = append(observed, attempt)
		return nil
	})
	for attempt := 1; attempt <= 11; attempt++ {
		require.NoError(t, takeOpenAIModelDispatch(ctx, int64(attempt)))
	}
	require.ErrorIs(t, takeOpenAIModelDispatch(ctx, 12), ErrOpenAIModelDispatchBudgetExhausted)
	assert.Equal(t, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}, observed)
}

func TestOpenAIManagedDispatchStopsIfResponseWriterRejectsAttempt(t *testing.T) {
	ctx := WithOpenAIModelDispatchBudget(WithOpenAIManagedRecovery(context.Background()))
	writerErr := errors.New("client output unavailable")
	ctx = WithOpenAIModelDispatchObserver(ctx, func(int) error { return writerErr })
	err := takeOpenAIModelDispatch(ctx, 1)
	require.ErrorIs(t, err, writerErr)
	assert.True(t, IsOpenAIModelDispatchStop(err))
	var firstAttempt int
	ctx = WithOpenAIModelDispatchObserver(ctx, func(attempt int) error { firstAttempt = attempt; return nil })
	require.NoError(t, takeOpenAIModelDispatch(ctx, 1))
	assert.Equal(t, 1, firstAttempt)
}
