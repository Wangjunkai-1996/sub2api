//go:build unit

package service

import (
	"context"
	"errors"
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

type openAIOAuthEgressRepoStub struct {
	route             *EgressRoute
	ensureProxyCalls  int
	ensureDirectCalls int
}

func (r *openAIOAuthEgressRepoStub) ResolveAccountPool(context.Context, int64) (*AccountEgressPoolConfigDomain, error) {
	return nil, errors.New("not implemented")
}

func (r *openAIOAuthEgressRepoStub) ListAssignableRoutes(context.Context) ([]EgressRoute, error) {
	return nil, errors.New("not implemented")
}

func (r *openAIOAuthEgressRepoStub) GetRoute(_ context.Context, routeID int64) (*EgressRoute, error) {
	if r.route == nil || r.route.ID != routeID {
		return nil, ErrEgressRouteNotFound
	}
	return r.route, nil
}

func (r *openAIOAuthEgressRepoStub) EnsureProxyRoute(context.Context, int64) (*EgressRoute, error) {
	r.ensureProxyCalls++
	return r.route, nil
}

func (r *openAIOAuthEgressRepoStub) EnsureDirectRoute(context.Context, string) (*EgressRoute, error) {
	r.ensureDirectCalls++
	return r.route, nil
}

func (r *openAIOAuthEgressRepoStub) RecordProbeObservation(context.Context, EgressProbeObservation) (*EgressRoute, error) {
	return nil, errors.New("not implemented")
}

func (r *openAIOAuthEgressRepoStub) ConfirmIdentity(context.Context, ConfirmEgressIdentityInput) (*EgressRoute, error) {
	return nil, errors.New("not implemented")
}

func (r *openAIOAuthEgressRepoStub) ReplaceAccountPool(context.Context, int64, ReplaceAccountPoolInput) (*AccountEgressPoolConfigDomain, error) {
	return nil, errors.New("not implemented")
}

func (r *openAIOAuthEgressRepoStub) ReplaceAccountPools(context.Context, []int64, ReplaceAccountPoolInput) error {
	return errors.New("not implemented")
}

func (r *openAIOAuthEgressRepoStub) ApplyAccountPools(context.Context, []int64, ApplyAccountPoolsInput) error {
	return errors.New("not implemented")
}

func (r *openAIOAuthEgressRepoStub) SyncProxyRouteLifecycle(context.Context, int64, string) error {
	return errors.New("not implemented")
}

func activeDirectOpenAIOAuthRoute(id, revision int64) *EgressRoute {
	runtimeScope := DefaultDirectEgressRuntimeScope
	identityID := int64(91)
	return &EgressRoute{
		ID:                 id,
		Kind:               EgressRouteKindDirect,
		RuntimeScope:       &runtimeScope,
		ExpectedIdentityID: &identityID,
		ExpectedIdentity: &EgressIdentity{
			ID:       identityID,
			PublicIP: "203.0.113.91",
			Status:   EgressIdentityStatusActive,
		},
		State:    EgressRouteStateActive,
		Revision: revision,
	}
}

func TestOpenAIOAuthLegacySessionDoesNotUseRouteRevisionFence(t *testing.T) {
	proxyID := int64(71)
	proxy := activeOpenAIDefaultProxy(proxyID, "proxy.example.test", 3128)
	proxyRepo := &mockProxyRepoForOAuth{getByIDFunc: func(context.Context, int64) (*Proxy, error) {
		return proxy, nil
	}}
	egressRepo := &openAIOAuthEgressRepoStub{route: activeDirectOpenAIOAuthRoute(81, 3)}
	client := &openAIDefaultProxyOAuthClientStub{}
	svc := NewOpenAIOAuthService(proxyRepo, client)
	svc.SetEgressService(NewEgressService(egressRepo, nil))
	defer svc.Stop()

	result, err := svc.GenerateAuthURLWithRoute(context.Background(), &proxyID, nil, "", PlatformOpenAI)
	require.NoError(t, err)
	session, ok := svc.sessionStore.Get(result.SessionID)
	require.True(t, ok)
	require.Zero(t, session.EgressRouteID)
	require.Zero(t, session.EgressRouteRevision)
	require.False(t, session.RequireVerifiedEgress)
	require.Equal(t, proxyID, *session.ProxyID)
	require.Zero(t, egressRepo.ensureProxyCalls)
	require.Zero(t, egressRepo.ensureDirectCalls)

	_, err = svc.ExchangeCode(context.Background(), &OpenAIExchangeCodeInput{
		SessionID: result.SessionID,
		Code:      "code",
		State:     session.State,
	})
	require.NoError(t, err)
	require.Equal(t, proxy.URL(), client.proxyURL)
}

func TestOpenAIOAuthPoolSessionRejectsRouteRevisionChange(t *testing.T) {
	route := activeDirectOpenAIOAuthRoute(82, 4)
	repo := &openAIOAuthEgressRepoStub{route: route}
	client := &openAIDefaultProxyOAuthClientStub{}
	svc := NewOpenAIOAuthService(nil, client)
	svc.SetEgressService(NewEgressService(repo, nil))
	defer svc.Stop()

	result, err := svc.GenerateAuthURLWithRoute(context.Background(), nil, &route.ID, "", PlatformOpenAI)
	require.NoError(t, err)
	session, ok := svc.sessionStore.Get(result.SessionID)
	require.True(t, ok)
	require.Equal(t, route.ID, session.EgressRouteID)
	require.Equal(t, route.Revision, session.EgressRouteRevision)
	require.True(t, session.RequireVerifiedEgress)

	repo.route.Revision++
	_, err = svc.ExchangeCode(context.Background(), &OpenAIExchangeCodeInput{
		SessionID: result.SessionID,
		Code:      "code",
		State:     session.State,
	})
	require.Equal(t, "OPENAI_OAUTH_EGRESS_SESSION_STALE", infraerrors.Reason(err))
}

func TestOpenAIOAuthResolvedDirectRefreshDoesNotUseDefaultProxy(t *testing.T) {
	settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "invalid"}}
	client := &openAIDefaultProxyOAuthClientStub{}
	svc := NewOpenAIOAuthService(nil, client)
	svc.SetSettingService(newOpenAIDefaultProxySettingService(settings, nil))
	defer svc.Stop()

	info, err := svc.RefreshTokenWithResolvedEgress(context.Background(), "rt", "", "client-id")
	require.NoError(t, err)
	require.Equal(t, "at-refresh", info.AccessToken)
	require.Empty(t, client.refreshProxyURL)
	require.Zero(t, settings.getValueCalls)
}

func TestOpenAIOAuthPoolAccountRefreshKeepsPrimaryDirectRoute(t *testing.T) {
	route := activeDirectOpenAIOAuthRoute(83, 5)
	repo := &openAIOAuthEgressRepoStub{route: route}
	settings := &settingRepoStub{values: map[string]string{SettingKeyOpenAIOAuthDefaultProxyID: "invalid"}}
	client := &openAIDefaultProxyOAuthClientStub{}
	svc := NewOpenAIOAuthService(nil, client)
	svc.SetEgressService(NewEgressService(repo, nil))
	svc.SetSettingService(newOpenAIDefaultProxySettingService(settings, nil))
	defer svc.Stop()

	account := &Account{
		ID:          101,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		EgressMode:  EgressModePool,
		Credentials: map[string]any{"refresh_token": "rt"},
		EgressBindings: []AccountEgressBinding{{
			BindingID: StableAccountEgressBindingID(101, route.ID),
			AccountID: 101,
			RouteID:   route.ID,
			IsPrimary: true,
			Status:    AccountEgressBindingStatusActive,
		}},
	}

	info, err := svc.RefreshAccountToken(context.Background(), account)
	require.NoError(t, err)
	require.Equal(t, "at-refresh", info.AccessToken)
	require.Empty(t, client.refreshProxyURL)
	require.Zero(t, settings.getValueCalls)
}

var _ EgressRepository = (*openAIOAuthEgressRepoStub)(nil)
