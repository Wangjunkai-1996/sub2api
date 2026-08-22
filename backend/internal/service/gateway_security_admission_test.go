//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/stretchr/testify/require"
)

type gatewaySecuritySessionLimitProbe struct {
	SessionLimitCache
	allowed   bool
	err       error
	calls     int
	accountID int64
	sessionID string
	idle      time.Duration
}

type gatewaySecurityStickyRefreshProbe struct {
	*mockGatewayCacheForPlatform
	refreshCalls int
}

func (p *gatewaySecurityStickyRefreshProbe) RefreshSessionTTL(
	_ context.Context,
	_ int64,
	_ string,
	_ time.Duration,
) error {
	p.refreshCalls++
	return nil
}

func (p *gatewaySecuritySessionLimitProbe) RegisterSession(
	_ context.Context,
	accountID int64,
	sessionID string,
	_ int,
	idle time.Duration,
) (bool, error) {
	p.calls++
	p.accountID = accountID
	p.sessionID = sessionID
	p.idle = idle
	return p.allowed, p.err
}

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
		gatewaySecurityTestAccount(6, PlatformAnthropic, AccountTypeServiceAccount, "", 1),
		gatewaySecurityTestAccount(7, PlatformGemini, AccountTypeServiceAccount, "", 1),
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

		require.Len(t, filtered, 4)
		require.Equal(t, []int64{1, 2, 6, 7}, []int64{filtered[0].ID, filtered[1].ID, filtered[2].ID, filtered[3].ID})
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

func TestGatewaySecurityAdmissionDefersSessionRegistrationUntilTerminalAdmission(t *testing.T) {
	account := gatewaySecurityTestAccount(31, PlatformAnthropic, AccountTypeOAuth, "", 1)
	account.Extra = map[string]any{
		"max_sessions":                 1,
		"session_idle_timeout_minutes": 7,
	}
	probe := &gatewaySecuritySessionLimitProbe{allowed: true}
	svc := &GatewayService{sessionLimitCache: probe}
	gatedCtx := WithOpenAIAccountRequirement(
		context.Background(), securityadmission.AccountRequirementAuditExempt,
	)

	// Scheduler probes under the security gate must not reserve quota before
	// the fresh terminal account check.
	require.True(t, svc.checkAndRegisterSession(gatedCtx, &account, "session-before-terminal"))
	require.Zero(t, probe.calls)

	terminalCtx := WithOpenAIAccountTerminalAdmission(gatedCtx, &OpenAIAccountRequirementAdmission{
		Selected:                 &account,
		EffectiveCredentialOwner: &account,
		Requirement:              securityadmission.AccountRequirementAuditExempt,
		AccountClass:             securityadmission.AccountAuditExemptVerified,
	})
	require.True(t, svc.RegisterSessionAfterSecurityAdmission(terminalCtx, &account, "session-after-terminal"))
	require.Equal(t, 1, probe.calls)
	require.Equal(t, account.ID, probe.accountID)
	require.Equal(t, "session-after-terminal", probe.sessionID)
	require.Equal(t, 7*time.Minute, probe.idle)
}

func TestGatewaySecurityAdmissionRejectsSessionAfterTerminalQuotaCheck(t *testing.T) {
	account := gatewaySecurityTestAccount(32, PlatformAnthropic, AccountTypeSetupToken, "", 1)
	account.Extra = map[string]any{"max_sessions": 1}
	probe := &gatewaySecuritySessionLimitProbe{allowed: false}
	svc := &GatewayService{sessionLimitCache: probe}
	gatedCtx := WithOpenAIAccountRequirement(
		context.Background(), securityadmission.AccountRequirementAuditExempt,
	)
	terminalCtx := WithOpenAIAccountTerminalAdmission(gatedCtx, &OpenAIAccountRequirementAdmission{
		Selected:                 &account,
		EffectiveCredentialOwner: &account,
		Requirement:              securityadmission.AccountRequirementAuditExempt,
		AccountClass:             securityadmission.AccountAuditExemptVerified,
	})

	require.True(t, svc.checkAndRegisterSession(gatedCtx, &account, "session-before-terminal"))
	require.False(t, svc.RegisterSessionAfterSecurityAdmission(terminalCtx, &account, "session-after-terminal"))
	require.Equal(t, 1, probe.calls)
}

func TestGatewaySecurityAdmissionSessionRegistrationRequiresMatchingTerminal(t *testing.T) {
	account := gatewaySecurityTestAccount(34, PlatformAnthropic, AccountTypeOAuth, "", 1)
	account.Extra = map[string]any{"max_sessions": 1}
	probe := &gatewaySecuritySessionLimitProbe{allowed: true}
	svc := &GatewayService{sessionLimitCache: probe}
	gatedCtx := WithOpenAIAccountRequirement(
		context.Background(), securityadmission.AccountRequirementAuditExempt,
	)

	cases := []struct {
		name      string
		admission *OpenAIAccountRequirementAdmission
	}{
		{name: "missing terminal"},
		{
			name: "different selected account",
			admission: &OpenAIAccountRequirementAdmission{
				Selected:     &Account{ID: account.ID + 1},
				AccountClass: securityadmission.AccountAuditExemptVerified,
			},
		},
		{
			name: "unverified terminal class",
			admission: &OpenAIAccountRequirementAdmission{
				Selected:     &account,
				AccountClass: securityadmission.AccountAuditRequired,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := gatedCtx
			if tc.admission != nil {
				ctx = WithOpenAIAccountTerminalAdmission(ctx, tc.admission)
			}
			require.False(t, svc.RegisterSessionAfterSecurityAdmission(ctx, &account, "session"))
			require.Zero(t, probe.calls)
		})
	}
}

func TestGatewayStickyRefreshDefersUntilTerminalAdmissionGate(t *testing.T) {
	rate := 0.5
	account := gatewaySecurityTestAccount(33, PlatformAnthropic, AccountTypeOAuth, "", 1)
	account.RateMultiplier = &rate
	accountRepo := &mockAccountRepoForPlatform{
		accounts:     []Account{account},
		accountsByID: map[int64]*Account{account.ID: &account},
	}
	cfg := testConfig()
	cfg.Gateway.Scheduling.LoadBatchEnabled = true

	profitGateCtx := context.WithValue(
		context.WithValue(context.Background(), ctxkey.ForcePlatform, PlatformAnthropic),
		openAIProfitControlGateCtxKey{},
		&openAIProfitControlGate{groupID: 1, platform: PlatformAnthropic, threshold: 2, pricingAt: time.Now()},
	)
	cases := []struct {
		name string
		ctx  context.Context
		want int
	}{
		{
			name: "security gate",
			ctx:  gatewaySecurityTestContext(PlatformAnthropic),
			want: 0,
		},
		{name: "profit gate", ctx: profitGateCtx, want: 0},
		{
			name: "no gate preserves refresh",
			ctx:  context.WithValue(context.Background(), ctxkey.ForcePlatform, PlatformAnthropic),
			want: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cache := &gatewaySecurityStickyRefreshProbe{
				mockGatewayCacheForPlatform: &mockGatewayCacheForPlatform{
					sessionBindings: map[string]int64{"sticky-refresh": account.ID},
				},
			}
			svc := &GatewayService{
				accountRepo:        accountRepo,
				cache:              cache,
				cfg:                cfg,
				concurrencyService: NewConcurrencyService(stubConcurrencyCache{}),
			}

			selection, err := svc.SelectAccountWithLoadAwareness(tc.ctx, nil, "sticky-refresh", "", nil, "", 0)
			require.NoError(t, err)
			require.NotNil(t, selection)
			if selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
			require.Equal(t, tc.want, cache.refreshCalls)
		})
	}
}
