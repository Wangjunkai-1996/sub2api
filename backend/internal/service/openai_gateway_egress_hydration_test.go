package service

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

type openAIEgressHydrationAccountRepo struct {
	AccountRepository
	account *Account
	calls   int
}

func (r *openAIEgressHydrationAccountRepo) GetByID(context.Context, int64) (*Account, error) {
	r.calls++
	return r.account.CloneForRequest(), nil
}

func openAIEgressHydrationAccount(accountRevision, routeRevision int64, proxy *Proxy) *Account {
	proxyID := int64(91)
	identityID := int64(301)
	if proxy != nil && proxy.Status == "" {
		activeProxy := *proxy
		activeProxy.Status = StatusActive
		proxy = &activeProxy
	}
	return &Account{
		ID:             901,
		Platform:       PlatformOpenAI,
		Type:           AccountTypeOAuth,
		Status:         StatusActive,
		Schedulable:    true,
		Concurrency:    4,
		EgressMode:     EgressModePool,
		EgressRevision: accountRevision,
		EgressBindings: []AccountEgressBinding{{
			BindingID: "901:41",
			AccountID: 901,
			RouteID:   41,
			Position:  0,
			IsPrimary: true,
			Status:    AccountEgressBindingStatusActive,
			Route: &EgressRoute{
				ID:                 41,
				Kind:               EgressRouteKindProxy,
				ProxyID:            &proxyID,
				ExpectedIdentityID: &identityID,
				ExpectedIdentity: &EgressIdentity{
					ID:     identityID,
					Status: EgressIdentityStatusActive,
				},
				State:    EgressRouteStateActive,
				Revision: routeRevision,
				Proxy:    proxy,
			},
		}},
	}
}

func openAIEgressHydrationResolved(account *Account) *ResolvedAccountEgress {
	return &ResolvedAccountEgress{
		BindingID:     "901:41",
		RouteID:       41,
		IdentityID:    "301",
		ConfigVersion: accountEgressRuntimeVersion(account),
		Lease:         &AccountEgressLease{ID: "lease-901"},
	}
}

func TestOpenAISelectionHydratesSelectedEgressFromAuthoritativeAccount(t *testing.T) {
	authoritative := openAIEgressHydrationAccount(7, 13, &Proxy{
		ID:       91,
		Protocol: "http",
		Host:     "proxy-authoritative.example",
		Port:     9443,
		Username: "runtime-user",
		Password: "runtime-password",
	})
	projected := openAIEgressHydrationAccount(7, 13, nil)
	resolved := openAIEgressHydrationResolved(projected)
	selectedProjection, err := withResolvedAccountEgressSelection(projected, resolved)
	require.NoError(t, err)

	repo := &openAIEgressHydrationAccountRepo{account: authoritative}
	service := &OpenAIGatewayService{accountRepo: repo}
	selection, err := service.newSelectionResult(context.Background(), selectedProjection, true, func() {}, nil)
	require.NoError(t, err)
	require.Equal(t, 1, repo.calls)
	require.NotNil(t, selection.Account.Proxy)
	require.Equal(t, "proxy-authoritative.example", selection.Account.Proxy.Host)
	require.Equal(t, "runtime-user", selection.Account.Proxy.Username)
	require.Same(t, resolved, selection.Account.SelectedEgress)
	require.Same(t, resolved, selection.Egress)
}

func TestOpenAISelectionFailsClosedWhenAuthoritativeEgressVersionChanges(t *testing.T) {
	projected := openAIEgressHydrationAccount(7, 13, nil)
	resolved := openAIEgressHydrationResolved(projected)
	selectedProjection, err := withResolvedAccountEgressSelection(projected, resolved)
	require.NoError(t, err)

	authoritative := openAIEgressHydrationAccount(8, 13, &Proxy{ID: 91, Protocol: "http", Host: "new.example", Port: 9443})
	service := &OpenAIGatewayService{accountRepo: &openAIEgressHydrationAccountRepo{account: authoritative}}
	released := 0
	selection, err := service.newAcquiredSelectionResult(context.Background(), selectedProjection, func() { released++ })
	require.ErrorIs(t, err, ErrAccountEgressConfigStale)
	require.Nil(t, selection)
	require.Equal(t, 1, released)
}

func TestOpenAISelectionNeverFallsBackToDirectWhenSelectedProxyIsUnavailable(t *testing.T) {
	projected := openAIEgressHydrationAccount(7, 13, nil)
	resolved := openAIEgressHydrationResolved(projected)
	selectedProjection, err := withResolvedAccountEgressSelection(projected, resolved)
	require.NoError(t, err)

	service := &OpenAIGatewayService{accountRepo: &openAIEgressHydrationAccountRepo{
		account: openAIEgressHydrationAccount(7, 13, nil),
	}}
	released := 0
	selection, err := service.newAcquiredSelectionResult(context.Background(), selectedProjection, func() { released++ })
	require.ErrorIs(t, err, ErrAccountEgressConfigStale)
	require.Nil(t, selection)
	require.Equal(t, 1, released)
}

func TestPreserveSelectedAccountEgressRestoresProxyForRedactedSnapshot(t *testing.T) {
	authoritative := openAIEgressHydrationAccount(7, 13, &Proxy{ID: 91, Protocol: "http", Host: "authoritative.example", Port: 9443})
	resolved := openAIEgressHydrationResolved(authoritative)
	selected, err := WithResolvedAccountEgress(authoritative, resolved)
	require.NoError(t, err)

	redacted := openAIEgressHydrationAccount(7, 13, nil)
	preserved, err := PreserveSelectedAccountEgress(redacted, selected)
	require.NoError(t, err)
	require.NotNil(t, preserved.Proxy)
	require.Equal(t, "authoritative.example", preserved.Proxy.Host)
	require.Same(t, resolved, preserved.SelectedEgress)
}

func TestPreserveSelectedAccountEgressRejectsRedactedSnapshotVersionChange(t *testing.T) {
	authoritative := openAIEgressHydrationAccount(7, 13, &Proxy{ID: 91, Protocol: "http", Host: "authoritative.example", Port: 9443})
	resolved := openAIEgressHydrationResolved(authoritative)
	selected, err := WithResolvedAccountEgress(authoritative, resolved)
	require.NoError(t, err)

	redacted := openAIEgressHydrationAccount(7, 14, nil)
	_, err = PreserveSelectedAccountEgress(redacted, selected)
	require.ErrorIs(t, err, ErrAccountEgressConfigStale)
}
