//go:build unit

package repository

import (
	"context"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type schedulerRedisCommandCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func newSchedulerRedisCommandCounter() *schedulerRedisCommandCounter {
	return &schedulerRedisCommandCounter{counts: make(map[string]int)}
}

func (h *schedulerRedisCommandCounter) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *schedulerRedisCommandCounter) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.mu.Lock()
		h.counts[cmd.Name()]++
		h.mu.Unlock()
		return next(ctx, cmd)
	}
}

func (h *schedulerRedisCommandCounter) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (h *schedulerRedisCommandCounter) count(command string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.counts[command]
}

func TestSchedulerCacheGetAccountMetadataByIDsUsesOneMGetAndPreservesOwnerFields(t *testing.T) {
	const accountCount = 100
	ctx := context.Background()
	cache := newSchedulerCacheUnit(t)
	accounts := make([]service.Account, 0, accountCount-1)
	requested := make([]int64, 0, accountCount)

	for id := int64(1); id <= accountCount; id++ {
		requested = append(requested, id)
		if id == accountCount {
			continue
		}
		accounts = append(accounts, service.Account{
			ID:          id,
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeOAuth,
			Credentials: map[string]any{"plan_type": "plus", "access_token": "must-not-enter-metadata"},
		})
	}
	nestedParentID := int64(701)
	accounts[1].ParentAccountID = &nestedParentID
	accounts[1].QuotaDimension = service.QuotaDimensionSpark
	accounts[2].Credentials["plan_type"] = "pro"
	_, err := cache.writeAccountIDs(ctx, accounts)
	require.NoError(t, err)

	counter := newSchedulerRedisCommandCounter()
	cache.rdb.AddHook(counter)
	got, err := cache.GetAccountMetadataByIDs(ctx, requested)

	require.NoError(t, err)
	require.Equal(t, 1, counter.count("mget"), "100 IDs fit in the configured 128-key MGET chunk")
	require.Len(t, got, accountCount)
	require.Nil(t, got[accountCount], "cache misses must be explicit")
	require.Equal(t, "plus", got[1].GetCredential("plan_type"))
	require.Equal(t, "pro", got[3].GetCredential("plan_type"))
	require.Empty(t, got[1].GetCredential("access_token"), "batch reader must use filtered scheduler metadata")
	require.NotNil(t, got[2].ParentAccountID)
	require.Equal(t, nestedParentID, *got[2].ParentAccountID)
	require.Equal(t, service.QuotaDimensionSpark, got[2].QuotaDimension)
	require.True(t, got[2].IsShadow(), "nested ownership must not be flattened into a verified owner")
}

var _ service.SchedulerAccountMetadataBatchReader = (*schedulerCache)(nil)
