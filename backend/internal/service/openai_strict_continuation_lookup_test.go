package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenAIGatewayService_HardBoundContinuationDBRecheckFailureKeepsBinding(t *testing.T) {
	ctx := WithOpenAIHardBoundHTTPContinuation(context.Background())
	groupID := int64(12)
	account := &Account{
		ID:          1879,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeAPIKey,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 2,
		GroupIDs:    []int64{groupID},
		Extra: map[string]any{
			"openai_responses_supported":      true,
			"responses_websockets_v2_enabled": true,
		},
	}
	cache := &schedulerTestGatewayCache{}
	acquiredIDs := []int64{}
	svc := &OpenAIGatewayService{
		accountRepo: schedulerLookupErrorOpenAIAccountRepo{err: errors.New("database unavailable")},
		cache:       cache,
		cfg:         newSchedulerTestOpenAIWSV2Config(),
		schedulerSnapshot: &SchedulerSnapshotService{cache: &openAISnapshotCacheStub{
			accountsByID: map[int64]*Account{account.ID: account},
		}},
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{acquiredIDs: &acquiredIDs}),
	}
	store := svc.getOpenAIWSStateStore()
	require.NoError(t, store.BindResponseAccount(ctx, groupID, "resp_db_recheck", account.ID, time.Hour))

	selection, _, err := svc.SelectAccountWithSchedulerForCapability(
		ctx,
		&groupID,
		"resp_db_recheck",
		"",
		"gpt-5.4",
		nil,
		OpenAIUpstreamTransportResponsesWebsocketV2,
		OpenAIEndpointCapabilityResponses,
		false,
		true,
		true,
	)

	require.ErrorIs(t, err, ErrOpenAIResponseAccountStoreUnavailable)
	require.Nil(t, selection)
	require.Empty(t, acquiredIDs, "lookup failure must be resolved before acquiring a concurrency slot")
	accountID, getErr := store.GetResponseAccount(ctx, groupID, "resp_db_recheck")
	require.NoError(t, getErr)
	require.Equal(t, account.ID, accountID)
	require.Empty(t, cache.deletedSessions)
}
