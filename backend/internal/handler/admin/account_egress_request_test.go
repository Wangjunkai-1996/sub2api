//go:build unit

package admin

import (
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestAccountEgressPoolInputExplicitLegacyUpdate(t *testing.T) {
	mode := service.EgressModeLegacy
	input, err := accountEgressPoolInput(&mode, nil, false)

	require.NoError(t, err)
	require.NotNil(t, input)
	require.Equal(t, service.EgressModeLegacy, input.Mode)
	require.Empty(t, input.RouteIDs)
	require.Nil(t, input.ExpectedRevision)
}

func TestAccountEgressPoolInputLegacyCreateUsesCompatibilityPath(t *testing.T) {
	mode := service.EgressModeLegacy
	input, err := accountEgressPoolInput(&mode, nil, true)

	require.NoError(t, err)
	require.Nil(t, input)
}

func TestAccountEgressPoolInputPoolUpdateRequiresRevision(t *testing.T) {
	mode := service.EgressModePool
	concurrency := 4
	_, err := accountEgressPoolInput(&mode, &AccountEgressPoolRequest{
		RouteIDs:             []int64{11},
		PrimaryRouteID:       ptrAccountEgressRequestInt64(11),
		ConcurrencyPerEgress: &concurrency,
	}, false)

	require.Error(t, err)
	require.Equal(t, "ACCOUNT_EGRESS_REVISION_REQUIRED", infraerrors.Reason(err))
}

func TestValidateOpenAIEgressWriteOnlyAllowsOAuth(t *testing.T) {
	require.NoError(t, validateOpenAIEgressWrite(service.PlatformOpenAI, service.AccountTypeOAuth, false))

	for _, tc := range []struct {
		name        string
		platform    string
		accountType string
		shadow      bool
	}{
		{name: "openai api key", platform: service.PlatformOpenAI, accountType: service.AccountTypeAPIKey},
		{name: "other platform oauth", platform: service.PlatformGemini, accountType: service.AccountTypeOAuth},
		{name: "spark shadow", platform: service.PlatformOpenAI, accountType: service.AccountTypeOAuth, shadow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, validateOpenAIEgressWrite(tc.platform, tc.accountType, tc.shadow))
		})
	}
}

func ptrAccountEgressRequestInt64(value int64) *int64 {
	return &value
}
