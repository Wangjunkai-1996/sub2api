//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/stretchr/testify/require"
)

func gatewaySecurityTestAccount(id int64, platform, accountType, plan string, priority int) Account {
	account := Account{
		ID:          id,
		Name:        "security-admission-test",
		Platform:    platform,
		Type:        accountType,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 2,
		Priority:    priority,
	}
	if plan != "" {
		account.Credentials = map[string]any{"plan_type": plan}
	}
	return account
}

func gatewaySecurityTestContext(platform string) context.Context {
	ctx := WithOpenAIAccountRequirement(
		context.Background(),
		securityadmission.AccountRequirementAuditExempt,
	)
	return context.WithValue(ctx, ctxkey.ForcePlatform, platform)
}

func TestFilterGatewayAccountsBySecurityAdmission(t *testing.T) {
	parentID := int64(99)
	accounts := []Account{
		gatewaySecurityTestAccount(1, PlatformAnthropic, AccountTypeAPIKey, "", 1),
		gatewaySecurityTestAccount(2, PlatformAntigravity, AccountTypeOAuth, "", 1),
		gatewaySecurityTestAccount(3, PlatformOpenAI, AccountTypeOAuth, "pro", 1),
		gatewaySecurityTestAccount(4, PlatformOpenAI, AccountTypeOAuth, "", 1),
		{
			ID:              5,
			Platform:        PlatformOpenAI,
			Type:            AccountTypeOAuth,
			ParentAccountID: &parentID,
		},
	}

	t.Run("any account preserves candidates", func(t *testing.T) {
		filtered := filterGatewayAccountsBySecurityAdmission(context.Background(), accounts)
		require.Len(t, filtered, len(accounts))
		require.Equal(t, accounts, filtered)
	})

	t.Run("audit exempt keeps only locally verified owners", func(t *testing.T) {
		ctx := WithOpenAIAccountRequirement(
			context.Background(),
			securityadmission.AccountRequirementAuditExempt,
		)
		filtered := filterGatewayAccountsBySecurityAdmission(ctx, accounts)

		require.Len(t, filtered, 2)
		require.Equal(t, []int64{1, 2}, []int64{filtered[0].ID, filtered[1].ID})
		// Generic candidate filtering deliberately treats OpenAI scheduling
		// shadows as unknown. Generic traffic has no shadow mapping contract, and
		// resolving parent accounts here would introduce a repository N+1.
	})
}

func TestGatewaySecurityAdmissionSelectionFiltersUnsafeOpenAIAccounts(t *testing.T) {
	pro := gatewaySecurityTestAccount(11, PlatformOpenAI, AccountTypeOAuth, "pro", 0)
	unknown := gatewaySecurityTestAccount(12, PlatformOpenAI, AccountTypeOAuth, "", 0)
	verified := gatewaySecurityTestAccount(13, PlatformOpenAI, AccountTypeOAuth, "plus", 100)
	accounts := []Account{pro, unknown, verified}
	accountsByID := map[int64]*Account{
		pro.ID:      &pro,
		unknown.ID:  &unknown,
		verified.ID: &verified,
	}
	ctx := gatewaySecurityTestContext(PlatformOpenAI)

	t.Run("legacy", func(t *testing.T) {
		cfg := testConfig()
		cfg.Gateway.Scheduling.LoadBatchEnabled = false
		svc := &GatewayService{
			accountRepo: &mockAccountRepoForPlatform{
				accounts:     accounts,
				accountsByID: accountsByID,
			},
			cache: &mockGatewayCacheForPlatform{},
			cfg:   cfg,
		}

		selection, err := svc.SelectAccountWithLoadAwareness(ctx, nil, "", "", nil, "", 0)
		require.NoError(t, err)
		require.NotNil(t, selection)
		require.NotNil(t, selection.Account)
		require.Equal(t, verified.ID, selection.Account.ID)
		if selection.ReleaseFunc != nil {
			selection.ReleaseFunc()
		}

		selection, err = svc.SelectAccountWithLoadAwareness(
			ctx,
			nil,
			"",
			"",
			map[int64]struct{}{verified.ID: {}},
			"",
			0,
		)
		require.Nil(t, selection)
		require.ErrorIs(t, err, ErrNoAvailableAccounts)
	})

	t.Run("load aware", func(t *testing.T) {
		cfg := testConfig()
		cfg.Gateway.Scheduling.LoadBatchEnabled = true
		svc := &GatewayService{
			accountRepo: &mockAccountRepoForPlatform{
				accounts:     accounts,
				accountsByID: accountsByID,
			},
			cache:              &mockGatewayCacheForPlatform{},
			cfg:                cfg,
			concurrencyService: NewConcurrencyService(stubConcurrencyCache{}),
		}

		selection, err := svc.SelectAccountWithLoadAwareness(ctx, nil, "", "", nil, "", 0)
		require.NoError(t, err)
		require.NotNil(t, selection)
		require.NotNil(t, selection.Account)
		require.Equal(t, verified.ID, selection.Account.ID)
		if selection.ReleaseFunc != nil {
			selection.ReleaseFunc()
		}

		selection, err = svc.SelectAccountWithLoadAwareness(
			ctx,
			nil,
			"",
			"",
			map[int64]struct{}{verified.ID: {}},
			"",
			0,
		)
		require.Nil(t, selection)
		require.ErrorIs(t, err, ErrNoAvailableAccounts)
	})
}

func TestGatewaySecurityAdmissionDefersStickyBindingUntilTerminalAdmission(t *testing.T) {
	verified := gatewaySecurityTestAccount(21, PlatformAnthropic, AccountTypeAPIKey, "", 1)
	ctx := gatewaySecurityTestContext(PlatformAnthropic)

	for _, test := range []struct {
		name      string
		loadAware bool
	}{
		{name: "legacy"},
		{name: "load aware", loadAware: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := &mockGatewayCacheForPlatform{sessionBindings: make(map[string]int64)}
			cfg := testConfig()
			cfg.Gateway.Scheduling.LoadBatchEnabled = test.loadAware
			svc := &GatewayService{
				accountRepo: &mockAccountRepoForPlatform{
					accounts:     []Account{verified},
					accountsByID: map[int64]*Account{verified.ID: &verified},
				},
				cache: cache,
				cfg:   cfg,
			}
			if test.loadAware {
				svc.concurrencyService = NewConcurrencyService(stubConcurrencyCache{})
			}
			selection, err := svc.SelectAccountWithLoadAwareness(
				ctx, nil, "security-sticky", "", nil, "", 0,
			)
			require.NoError(t, err)
			require.NotNil(t, selection)
			require.NotNil(t, selection.Account)
			require.Equal(t, verified.ID, selection.Account.ID)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}

			require.NotContains(t, cache.sessionBindings, "security-sticky",
				"security-gated selection must not bind before terminal admission")
		})
	}

	t.Run("matching admission binds", func(t *testing.T) {
		cache := &mockGatewayCacheForPlatform{sessionBindings: make(map[string]int64)}
		svc := &GatewayService{cache: cache}
		terminalCtx := WithOpenAIAccountTerminalAdmission(ctx, &OpenAIAccountRequirementAdmission{
			Selected:                 &verified,
			EffectiveCredentialOwner: &verified,
			Requirement:              securityadmission.AccountRequirementAuditExempt,
			AccountClass:             securityadmission.AccountAuditExemptVerified,
		})

		err := svc.BindStickySessionAfterProfitAdmission(
			terminalCtx, nil, "security-sticky", verified.ID,
		)
		require.NoError(t, err)
		require.Equal(t, verified.ID, cache.sessionBindings["security-sticky"])
	})

	for _, test := range []struct {
		name      string
		admission *OpenAIAccountRequirementAdmission
	}{
		{name: "missing terminal admission"},
		{
			name: "wrong selected account",
			admission: &OpenAIAccountRequirementAdmission{
				Selected:                 &Account{ID: verified.ID + 1},
				EffectiveCredentialOwner: &verified,
				Requirement:              securityadmission.AccountRequirementAuditExempt,
				AccountClass:             securityadmission.AccountAuditExemptVerified,
			},
		},
		{
			name: "wrong account class",
			admission: &OpenAIAccountRequirementAdmission{
				Selected:                 &verified,
				EffectiveCredentialOwner: &verified,
				Requirement:              securityadmission.AccountRequirementAuditExempt,
				AccountClass:             securityadmission.AccountAuditRequired,
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := &mockGatewayCacheForPlatform{sessionBindings: make(map[string]int64)}
			svc := &GatewayService{cache: cache}
			bindCtx := ctx
			if test.admission != nil {
				bindCtx = WithOpenAIAccountTerminalAdmission(bindCtx, test.admission)
			}

			err := svc.BindStickySessionAfterProfitAdmission(
				bindCtx, nil, "security-sticky", verified.ID,
			)
			require.ErrorIs(t, err, ErrOpenAIAccountAdmissionUnavailable)
			require.NotContains(t, cache.sessionBindings, "security-sticky")
		})
	}
}
