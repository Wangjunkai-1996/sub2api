//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type schedulerAccountMetadataBatchCache struct {
	SchedulerCache

	accounts map[int64]*Account
	calls    int
	ids      []int64
}

func (c *schedulerAccountMetadataBatchCache) GetAccountMetadataByIDs(_ context.Context, accountIDs []int64) (map[int64]*Account, error) {
	c.calls++
	c.ids = append([]int64(nil), accountIDs...)
	result := make(map[int64]*Account, len(accountIDs))
	for _, accountID := range accountIDs {
		result[accountID] = c.accounts[accountID]
	}
	return result, nil
}

type schedulerAccountMetadataBatchRepo struct {
	AccountRepository

	accounts map[int64]*Account
	calls    int
	ids      []int64
}

func (r *schedulerAccountMetadataBatchRepo) GetByIDs(_ context.Context, accountIDs []int64) ([]*Account, error) {
	r.calls++
	r.ids = append([]int64(nil), accountIDs...)
	result := make([]*Account, 0, len(accountIDs))
	for i := len(accountIDs) - 1; i >= 0; i-- {
		if account := r.accounts[accountIDs[i]]; account != nil {
			result = append(result, account)
		}
	}
	return result, nil
}

func TestSchedulerSnapshotGetAccountMetadataByIDsBatchesCacheAndDBMisses(t *testing.T) {
	const accountCount = 100
	cache := &schedulerAccountMetadataBatchCache{accounts: make(map[int64]*Account)}
	repo := &schedulerAccountMetadataBatchRepo{accounts: make(map[int64]*Account)}
	requested := make([]int64, 0, accountCount)

	for id := int64(1); id <= accountCount; id++ {
		requested = append(requested, id)
		account := &Account{
			ID:          id,
			Platform:    PlatformOpenAI,
			Type:        AccountTypeOAuth,
			Credentials: map[string]any{"plan_type": "plus"},
		}
		if id%2 == 1 {
			cache.accounts[id] = account
		} else if id != accountCount {
			repo.accounts[id] = account
		}
	}
	nestedParentID := int64(901)
	cache.accounts[3].ParentAccountID = &nestedParentID
	cache.accounts[3].QuotaDimension = QuotaDimensionSpark
	repo.accounts[4].Credentials["plan_type"] = "pro"

	svc := NewSchedulerSnapshotService(cache, nil, repo, nil, nil)
	got, err := svc.GetAccountMetadataByIDs(context.Background(), requested)

	require.NoError(t, err)
	require.Equal(t, 1, cache.calls, "candidate traversal must use one cache batch")
	require.Equal(t, requested, cache.ids)
	require.Equal(t, 1, repo.calls, "all cache misses must share one DB batch")
	require.Len(t, repo.ids, accountCount/2)
	require.Len(t, got, accountCount, "missing IDs must remain explicit in the result")
	require.Nil(t, got[accountCount])
	require.Equal(t, "plus", got[1].GetCredential("plan_type"))
	require.Equal(t, "pro", got[4].GetCredential("plan_type"))
	require.NotNil(t, got[3].ParentAccountID)
	require.Equal(t, nestedParentID, *got[3].ParentAccountID)
	require.Equal(t, QuotaDimensionSpark, got[3].QuotaDimension)
	require.True(t, got[3].IsShadow(), "a nested parent must stay visibly unresolved for fail-closed classification")
}

func TestSchedulerSnapshotGetAccountMetadataByIDsRetainsInvalidAndMissingIDs(t *testing.T) {
	repo := &schedulerAccountMetadataBatchRepo{accounts: map[int64]*Account{
		8: {ID: 8, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{"plan_type": "team"}},
	}}
	svc := NewSchedulerSnapshotService(nil, nil, repo, nil, nil)

	got, err := svc.GetAccountMetadataByIDs(context.Background(), []int64{0, -1, 8, 9, 8})

	require.NoError(t, err)
	require.Equal(t, 1, repo.calls)
	require.Equal(t, []int64{8, 9}, repo.ids)
	require.Len(t, got, 4)
	require.Nil(t, got[0])
	require.Nil(t, got[-1])
	require.Nil(t, got[9])
	require.Equal(t, "team", got[8].GetCredential("plan_type"))
}
