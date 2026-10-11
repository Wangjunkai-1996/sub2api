//go:build unit

package service

import (
	"context"
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

type accountEgressConfigurationRepoStub struct {
	mutation AccountConfigurationMutation
	calls    int
}

func (r *accountEgressConfigurationRepoStub) UpdateAccountConfiguration(_ context.Context, mutation AccountConfigurationMutation) (*Account, error) {
	r.mutation = mutation
	r.calls++
	return mutation.Desired, nil
}

func poolAccountForAdminUpdate() *Account {
	proxyID := int64(23)
	return &Account{
		ID:             301,
		Name:           "pool-account",
		Platform:       PlatformOpenAI,
		Type:           AccountTypeOAuth,
		Status:         StatusActive,
		Schedulable:    true,
		ProxyID:        &proxyID,
		EgressMode:     EgressModePool,
		EgressRevision: 7,
		Concurrency:    4,
		EgressBindings: []AccountEgressBinding{
			{RouteID: 11, Position: 0, IsPrimary: true},
			{RouteID: 12, Position: 1},
		},
	}
}

func TestUpdatePoolAccountLegacyProxyMirrorIsNoop(t *testing.T) {
	account := poolAccountForAdminUpdate()
	repo := &updateAccountCredsRepoStub{account: account}
	svc := &adminServiceImpl{accountRepo: repo}
	proxyID := *account.ProxyID

	_, err := svc.UpdateAccount(context.Background(), account.ID, &UpdateAccountInput{ProxyID: &proxyID})

	require.NoError(t, err)
	require.Equal(t, 1, repo.updateCalls)
	require.Equal(t, int64(23), *repo.account.ProxyID)
	require.Nil(t, repo.account.EgressPoolWrite)
}

func TestUpdatePoolAccountLegacyProxyChangeRequiresPoolVersion(t *testing.T) {
	account := poolAccountForAdminUpdate()
	repo := &updateAccountCredsRepoStub{account: account}
	svc := &adminServiceImpl{accountRepo: repo}
	proxyID := int64(99)

	_, err := svc.UpdateAccount(context.Background(), account.ID, &UpdateAccountInput{ProxyID: &proxyID})

	require.Error(t, err)
	require.Equal(t, "EGRESS_POOL_VERSION_REQUIRED", infraerrors.Reason(err))
	require.Zero(t, repo.updateCalls)
}

func TestUpdatePoolAccountLegacyConcurrencyUsesPoolAggregate(t *testing.T) {
	account := poolAccountForAdminUpdate()
	repo := &updateAccountCredsRepoStub{account: account}
	configurationRepo := &accountEgressConfigurationRepoStub{}
	svc := &adminServiceImpl{accountRepo: repo, accountConfigRepo: configurationRepo}
	concurrency := 6

	updated, err := svc.UpdateAccount(context.Background(), account.ID, &UpdateAccountInput{Concurrency: &concurrency})

	require.NoError(t, err)
	require.Zero(t, repo.updateCalls, "pool changes must bypass the non-atomic legacy update")
	require.Equal(t, 1, configurationRepo.calls)
	require.True(t, configurationRepo.mutation.Fields.Concurrency)
	require.Same(t, configurationRepo.mutation.Desired, updated)
	require.Equal(t, 6, updated.Concurrency)
	pool := configurationRepo.mutation.EgressPool
	require.NotNil(t, pool)
	require.Same(t, updated.EgressPoolWrite, pool)
	require.Equal(t, []int64{11, 12}, pool.RouteIDs)
	require.Equal(t, int64(11), pool.PrimaryRouteID)
	require.NotNil(t, pool.ExpectedRevision)
	require.Equal(t, int64(7), *pool.ExpectedRevision)
	require.NotNil(t, pool.ConcurrencyPerEgress)
	require.Equal(t, 6, *pool.ConcurrencyPerEgress)
}

func TestCreateAccountPoolRejectsOpenAIAPIKey(t *testing.T) {
	svc := &adminServiceImpl{}

	_, err := svc.CreateAccount(context.Background(), &CreateAccountInput{
		Platform: PlatformOpenAI,
		Type:     AccountTypeAPIKey,
		EgressPool: &ReplaceAccountPoolInput{
			Mode:           EgressModePool,
			RouteIDs:       []int64{11},
			PrimaryRouteID: 11,
		},
	})

	require.ErrorIs(t, err, ErrEgressAccountUnsupported)
}

func TestUpdateAccountPoolRejectsOpenAIAPIKey(t *testing.T) {
	account := poolAccountForAdminUpdate()
	account.Type = AccountTypeAPIKey
	account.EgressMode = EgressModeLegacy
	repo := &updateAccountCredsRepoStub{account: account}
	svc := &adminServiceImpl{accountRepo: repo}
	revision := int64(7)

	_, err := svc.UpdateAccount(context.Background(), account.ID, &UpdateAccountInput{
		EgressPool: &ReplaceAccountPoolInput{
			Mode:             EgressModePool,
			RouteIDs:         []int64{11},
			PrimaryRouteID:   11,
			ExpectedRevision: &revision,
		},
	})

	require.ErrorIs(t, err, ErrEgressAccountUnsupported)
	require.Zero(t, repo.updateCalls)
}

func TestUpdatePoolAccountRejectsChangingTypeToAPIKey(t *testing.T) {
	account := poolAccountForAdminUpdate()
	repo := &updateAccountCredsRepoStub{account: account}
	svc := &adminServiceImpl{accountRepo: repo}

	_, err := svc.UpdateAccount(context.Background(), account.ID, &UpdateAccountInput{
		Type: AccountTypeAPIKey,
	})

	require.ErrorIs(t, err, ErrEgressAccountUnsupported)
	require.Zero(t, repo.updateCalls)
	require.Equal(t, AccountTypeOAuth, repo.account.Type)
}

func TestUpdateAccountRejectsPoolAndTypeChangeToAPIKeyTogether(t *testing.T) {
	account := poolAccountForAdminUpdate()
	account.EgressMode = EgressModeLegacy
	repo := &updateAccountCredsRepoStub{account: account}
	svc := &adminServiceImpl{accountRepo: repo}
	revision := int64(7)

	_, err := svc.UpdateAccount(context.Background(), account.ID, &UpdateAccountInput{
		Type: AccountTypeAPIKey,
		EgressPool: &ReplaceAccountPoolInput{
			Mode:             EgressModePool,
			RouteIDs:         []int64{11},
			PrimaryRouteID:   11,
			ExpectedRevision: &revision,
		},
	})

	require.ErrorIs(t, err, ErrEgressAccountUnsupported)
	require.Zero(t, repo.updateCalls)
	require.Equal(t, AccountTypeOAuth, repo.account.Type)
}
