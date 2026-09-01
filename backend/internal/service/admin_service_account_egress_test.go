//go:build unit

package service

import (
	"context"
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

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
	svc := &adminServiceImpl{accountRepo: repo}
	concurrency := 6

	_, err := svc.UpdateAccount(context.Background(), account.ID, &UpdateAccountInput{Concurrency: &concurrency})

	require.NoError(t, err)
	require.Equal(t, 1, repo.updateCalls)
	require.Equal(t, 6, repo.account.Concurrency)
	require.NotNil(t, repo.account.EgressPoolWrite)
	require.Equal(t, []int64{11, 12}, repo.account.EgressPoolWrite.RouteIDs)
	require.Equal(t, int64(11), repo.account.EgressPoolWrite.PrimaryRouteID)
	require.NotNil(t, repo.account.EgressPoolWrite.ExpectedRevision)
	require.Equal(t, int64(7), *repo.account.EgressPoolWrite.ExpectedRevision)
	require.NotNil(t, repo.account.EgressPoolWrite.ConcurrencyPerEgress)
	require.Equal(t, 6, *repo.account.EgressPoolWrite.ConcurrencyPerEgress)
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
