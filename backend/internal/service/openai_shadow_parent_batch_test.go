package service

import (
	"context"
	"errors"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/stretchr/testify/require"
)

type openAIShadowParentBatchCache struct {
	SchedulerCache

	snapshotAccounts []*Account
	accountsByID     map[int64]*Account
	batchAccounts    map[int64]*Account
	batchCalls       int
	batchIDs         []int64
	getAccountCalls  map[int64]int
}

func (c *openAIShadowParentBatchCache) GetSnapshot(context.Context, SchedulerBucket) ([]*Account, bool, error) {
	accounts := make([]*Account, 0, len(c.snapshotAccounts))
	for _, account := range c.snapshotAccounts {
		if account == nil {
			continue
		}
		clone := *account
		accounts = append(accounts, &clone)
	}
	return accounts, true, nil
}

func (c *openAIShadowParentBatchCache) GetAccount(_ context.Context, accountID int64) (*Account, error) {
	if c.getAccountCalls == nil {
		c.getAccountCalls = make(map[int64]int)
	}
	c.getAccountCalls[accountID]++
	account := c.accountsByID[accountID]
	if account == nil {
		return nil, nil
	}
	clone := *account
	return &clone, nil
}

func (c *openAIShadowParentBatchCache) GetAccountMetadataByIDs(_ context.Context, accountIDs []int64) (map[int64]*Account, error) {
	c.batchCalls++
	c.batchIDs = append([]int64(nil), accountIDs...)
	accounts := make(map[int64]*Account, len(accountIDs))
	for _, accountID := range accountIDs {
		accounts[accountID] = c.batchAccounts[accountID]
	}
	return accounts, nil
}

type openAIShadowParentBatchRepo struct {
	AccountRepository

	accountsByID  map[int64]*Account
	getByIDCalls  map[int64]int
	getByIDsCalls int
	getByIDsIDs   []int64
	getByIDsErr   error
}

func (r *openAIShadowParentBatchRepo) GetByID(_ context.Context, accountID int64) (*Account, error) {
	if r.getByIDCalls == nil {
		r.getByIDCalls = make(map[int64]int)
	}
	r.getByIDCalls[accountID]++
	account := r.accountsByID[accountID]
	if account == nil {
		return nil, errors.New("account not found")
	}
	clone := *account
	return &clone, nil
}

func (r *openAIShadowParentBatchRepo) GetByIDs(_ context.Context, accountIDs []int64) ([]*Account, error) {
	r.getByIDsCalls++
	r.getByIDsIDs = append([]int64(nil), accountIDs...)
	accounts := make([]*Account, 0, len(accountIDs))
	for _, accountID := range accountIDs {
		if account := r.accountsByID[accountID]; account != nil {
			clone := *account
			accounts = append(accounts, &clone)
		}
	}
	return accounts, r.getByIDsErr
}

type openAIShadowParentBatchFixture struct {
	service   *OpenAIGatewayService
	cache     *openAIShadowParentBatchCache
	repo      *openAIShadowParentBatchRepo
	parentIDs []int64
	grandID   int64
}

func newOpenAIShadowParentBatchFixture() openAIShadowParentBatchFixture {
	const accountCount = 100
	cache := &openAIShadowParentBatchCache{
		accountsByID:  make(map[int64]*Account, accountCount*2),
		batchAccounts: make(map[int64]*Account, accountCount/2),
	}
	repo := &openAIShadowParentBatchRepo{accountsByID: make(map[int64]*Account, accountCount*2)}
	parentIDs := make([]int64, 0, accountCount)
	grandID := int64(990001)

	for index := 0; index < accountCount; index++ {
		shadowID := int64(100001 + index)
		parentID := int64(200001 + index)
		parentIDs = append(parentIDs, parentID)

		shadow := &Account{
			ID:              shadowID,
			Platform:        PlatformOpenAI,
			Type:            AccountTypeOAuth,
			Status:          StatusActive,
			Schedulable:     true,
			Concurrency:     1,
			Priority:        index,
			ParentAccountID: &parentID,
			QuotaDimension:  QuotaDimensionSpark,
		}
		parent := &Account{
			ID:          parentID,
			Platform:    PlatformOpenAI,
			Type:        AccountTypeOAuth,
			Status:      StatusActive,
			Schedulable: true,
			Credentials: map[string]any{"plan_type": "plus"},
		}
		if index == accountCount-3 {
			parent.Credentials["plan_type"] = "pro"
		}
		if index == accountCount-2 {
			parent.ParentAccountID = &grandID
			parent.QuotaDimension = QuotaDimensionSpark
		}

		cache.snapshotAccounts = append(cache.snapshotAccounts, shadow)
		cache.accountsByID[shadowID] = shadow
		repo.accountsByID[shadowID] = shadow
		// Odd parent IDs are metadata-cache hits. Even IDs exercise the one
		// GetByIDs fallback; the final ID remains an explicit nil miss.
		if parentID%2 == 1 {
			cache.batchAccounts[parentID] = parent
		}
		if index != accountCount-1 {
			repo.accountsByID[parentID] = parent
		}
	}
	repo.accountsByID[grandID] = &Account{
		ID:          grandID,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Status:      StatusActive,
		Schedulable: true,
		Credentials: map[string]any{"plan_type": "plus"},
	}

	snapshot := NewSchedulerSnapshotService(cache, nil, repo, nil, nil)
	return openAIShadowParentBatchFixture{
		service: &OpenAIGatewayService{
			accountRepo:       repo,
			schedulerSnapshot: snapshot,
			cfg:               &config.Config{RunMode: config.RunModeSimple},
		},
		cache:     cache,
		repo:      repo,
		parentIDs: parentIDs,
		grandID:   grandID,
	}
}

func assertOpenAIShadowParentBatchTraversal(t *testing.T, fixture openAIShadowParentBatchFixture) {
	t.Helper()
	require.Equal(t, 1, fixture.cache.batchCalls, "all direct parents must share one metadata-cache batch")
	require.ElementsMatch(t, fixture.parentIDs, fixture.cache.batchIDs)
	require.Equal(t, 1, fixture.repo.getByIDsCalls, "all metadata-cache misses must share one DB batch")
	require.NotContains(t, fixture.cache.batchIDs, fixture.grandID, "nested shadow ownership must not be traversed")
	for _, parentID := range fixture.parentIDs {
		require.Zero(t, fixture.cache.getAccountCalls[parentID], "parent %d must not use a per-ID cache read", parentID)
		require.Zero(t, fixture.repo.getByIDCalls[parentID], "parent %d must not use a per-ID DB read", parentID)
	}
}

func TestListSchedulableOpenAIAccounts_BatchPreloadsOneHundredShadowParentsWithoutGetByID(t *testing.T) {
	fixture := newOpenAIShadowParentBatchFixture()
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)

	accounts, err := fixture.service.listSchedulableAccounts(ctx, nil, PlatformOpenAI)
	require.NoError(t, err)
	require.Len(t, accounts, 100)
	compatible := 0
	for i := range accounts {
		if ok, _ := fixture.service.openAIAccountRequirementCompatible(ctx, &accounts[i], ""); ok {
			compatible++
		}
	}

	require.Equal(t, 97, compatible, "Pro, an unresolved parent, and a nested shadow parent must fail closed")
	require.Empty(t, fixture.repo.getByIDCalls, "candidate classification must not issue any GetByID query")
	assertOpenAIShadowParentBatchTraversal(t, fixture)
}

func TestOpenAIShadowParentBatchPreload_CoversLegacyAndAdvancedCandidateTraversal(t *testing.T) {
	t.Run("legacy", func(t *testing.T) {
		fixture := newOpenAIShadowParentBatchFixture()
		ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)

		account, err := fixture.service.SelectAccountForModelWithExclusions(ctx, nil, "", "", nil)

		require.NoError(t, err)
		require.NotNil(t, account)
		assertOpenAIShadowParentBatchTraversal(t, fixture)
	})

	t.Run("advanced", func(t *testing.T) {
		fixture := newOpenAIShadowParentBatchFixture()
		scheduler := newDefaultOpenAIAccountScheduler(fixture.service, nil)

		selection, _, err := scheduler.Select(context.Background(), OpenAIAccountScheduleRequest{
			Platform:    PlatformOpenAI,
			Requirement: securityadmission.AccountRequirementAuditExempt,
		})

		require.NoError(t, err)
		require.NotNil(t, selection)
		require.NotNil(t, selection.Account)
		if selection.ReleaseFunc != nil {
			selection.ReleaseFunc()
		}
		assertOpenAIShadowParentBatchTraversal(t, fixture)
	})
}

func TestPreloadOpenAIRequirementParents_CachesPartialErrorMissesAndNestedOwner(t *testing.T) {
	plusParentID := int64(301001)
	missingParentID := int64(301002)
	nestedParentID := int64(301003)
	grandID := int64(301004)
	plusParent := &Account{
		ID:          plusParentID,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Status:      StatusActive,
		Schedulable: true,
		Credentials: map[string]any{"plan_type": "plus"},
	}
	nestedParent := &Account{
		ID:              nestedParentID,
		Platform:        PlatformOpenAI,
		Type:            AccountTypeOAuth,
		Status:          StatusActive,
		Schedulable:     true,
		ParentAccountID: &grandID,
		QuotaDimension:  QuotaDimensionSpark,
	}
	cache := &openAIShadowParentBatchCache{batchAccounts: map[int64]*Account{
		plusParentID:   plusParent,
		nestedParentID: nestedParent,
	}}
	repo := &openAIShadowParentBatchRepo{
		accountsByID: map[int64]*Account{
			missingParentID: {
				ID:          missingParentID,
				Platform:    PlatformOpenAI,
				Type:        AccountTypeOAuth,
				Status:      StatusActive,
				Schedulable: true,
				Credentials: map[string]any{"plan_type": "plus"},
			},
			grandID: {
				ID:          grandID,
				Platform:    PlatformOpenAI,
				Type:        AccountTypeOAuth,
				Status:      StatusActive,
				Schedulable: true,
				Credentials: map[string]any{"plan_type": "plus"},
			},
		},
		getByIDsErr: errors.New("partial metadata DB failure"),
	}
	svc := &OpenAIGatewayService{
		accountRepo:       repo,
		schedulerSnapshot: NewSchedulerSnapshotService(cache, nil, repo, nil, nil),
	}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)
	shadows := []Account{
		{ID: 401001, ParentAccountID: &plusParentID},
		{ID: 401002, ParentAccountID: &missingParentID},
		{ID: 401003, ParentAccountID: &nestedParentID},
	}

	svc.preloadOpenAIRequirementParents(ctx, shadows)

	require.Equal(t, securityadmission.AccountAuditExemptVerified, svc.ClassifyOpenAIAccountAuditClass(ctx, &shadows[0]))
	require.Equal(t, securityadmission.AccountUnknown, svc.ClassifyOpenAIAccountAuditClass(ctx, &shadows[1]))
	require.Equal(t, securityadmission.AccountUnknown, svc.ClassifyOpenAIAccountAuditClass(ctx, &shadows[2]))
	require.Equal(t, 1, cache.batchCalls)
	require.Equal(t, 1, repo.getByIDsCalls)
	require.Equal(t, []int64{missingParentID}, repo.getByIDsIDs)
	require.NotContains(t, cache.batchIDs, grandID)
	require.Zero(t, repo.getByIDCalls[missingParentID], "a partial-error miss must stay cached as nil")
	require.Zero(t, repo.getByIDCalls[grandID], "nested shadows must not be resolved recursively")
}

var _ SchedulerAccountMetadataBatchReader = (*openAIShadowParentBatchCache)(nil)
