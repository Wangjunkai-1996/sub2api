package admin

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/handler/dto"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestSetAccountEgressRuntimeLoadUsesPoolTotalAndIdentityBreakdown(t *testing.T) {
	directScope := service.DefaultDirectEgressRuntimeScope
	account := &service.Account{
		ID:          91,
		Platform:    service.PlatformOpenAI,
		Type:        service.AccountTypeOAuth,
		EgressMode:  service.EgressModePool,
		Concurrency: 3,
		EgressBindings: []service.AccountEgressBinding{
			accountHandlerEgressBinding(91, 10, 1, "51.81.109.154", directScope),
			accountHandlerEgressBinding(91, 11, 2, "67.215.237.47", directScope),
			accountHandlerEgressBinding(91, 12, 3, "104.223.77.152", directScope),
		},
	}
	response := dto.AccountFromService(account)
	load := &service.AccountEgressLoadInfo{
		AccountID:     account.ID,
		Status:        service.AccountEgressStatusAcquired,
		ActiveTotal:   6,
		IdentityLoads: map[string]int{"1": 1, "2": 2, "3": 3},
	}

	current := setAccountEgressRuntimeLoad(response, account, 3, load)
	require.Equal(t, 6, current)
	require.Equal(t, 6, *response.EgressSummary.CurrentConcurrency)
	require.Len(t, response.EgressSummary.Bindings, 3)
	require.Equal(t, []int{1, 2, 3}, []int{
		response.EgressSummary.Bindings[0].CurrentConcurrency,
		response.EgressSummary.Bindings[1].CurrentConcurrency,
		response.EgressSummary.Bindings[2].CurrentConcurrency,
	})
}

func TestSetAccountEgressRuntimeLoadPreservesLegacyCountWhileDraining(t *testing.T) {
	account := &service.Account{EgressMode: service.EgressModePool}
	response := &dto.Account{EgressSummary: &dto.AccountEgressSummary{}}
	load := &service.AccountEgressLoadInfo{
		Status:        service.AccountEgressStatusLegacyDraining,
		ActiveTotal:   0,
		IdentityLoads: map[string]int{"1": 0},
	}

	require.Equal(t, 3, setAccountEgressRuntimeLoad(response, account, 3, load))
	require.Equal(t, 3, *response.EgressSummary.CurrentConcurrency)
	require.Empty(t, response.EgressSummary.Bindings, "legacy totals must not be presented as per-IP pool usage")
}

func accountHandlerEgressBinding(accountID, routeID, identityID int64, publicIP, directScope string) service.AccountEgressBinding {
	return service.AccountEgressBinding{
		BindingID: service.StableAccountEgressBindingID(accountID, routeID),
		AccountID: accountID,
		RouteID:   routeID,
		Position:  int(routeID),
		Status:    service.AccountEgressBindingStatusActive,
		Route: &service.EgressRoute{
			ID:           routeID,
			Kind:         service.EgressRouteKindDirect,
			RuntimeScope: &directScope,
			State:        service.EgressRouteStateActive,
			ExpectedIdentity: &service.EgressIdentity{
				ID:       identityID,
				PublicIP: publicIP,
				Status:   service.EgressIdentityStatusActive,
			},
		},
	}
}
