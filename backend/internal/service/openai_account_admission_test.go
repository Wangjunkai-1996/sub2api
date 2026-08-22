package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/stretchr/testify/require"
)

// admissionTestAccountRepo deliberately implements only the repository methods
// used by the admission and scheduler paths under test. The embedded interface
// keeps this fixture small while making an unexpected repository call fail
// loudly instead of silently changing the production contract.
type admissionTestAccountRepo struct {
	AccountRepository

	mu       sync.Mutex
	accounts map[int64]*Account
	getCalls map[int64]int
}

func (r *admissionTestAccountRepo) GetByID(_ context.Context, id int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.getCalls == nil {
		r.getCalls = make(map[int64]int)
	}
	r.getCalls[id]++
	account := r.accounts[id]
	if account == nil {
		return nil, errors.New("account not found")
	}
	clone := *account
	return &clone, nil
}

func (r *admissionTestAccountRepo) setAccount(account *Account) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.accounts == nil {
		r.accounts = make(map[int64]*Account)
	}
	clone := *account
	r.accounts[account.ID] = &clone
}

func (r *admissionTestAccountRepo) calls(id int64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.getCalls[id]
}

func TestClassifyOpenAIEffectiveCredentialOwner_UsesExplicitAccountStates(t *testing.T) {
	for _, test := range []struct {
		name  string
		owner *Account
		want  securityadmission.AccountClass
	}{
		{
			name:  "nil is unknown",
			owner: nil,
			want:  securityadmission.AccountUnknown,
		},
		{
			name: "api key stays exempt even with pro metadata",
			owner: &Account{
				Platform:    PlatformOpenAI,
				Type:        AccountTypeAPIKey,
				Credentials: map[string]any{"plan_type": "pro"},
			},
			want: securityadmission.AccountAuditExemptVerified,
		},
		{
			name: "setup token stays exempt",
			owner: &Account{
				Platform:    PlatformOpenAI,
				Type:        AccountTypeSetupToken,
				Credentials: map[string]any{"plan_type": "pro"},
			},
			want: securityadmission.AccountAuditExemptVerified,
		},
		{
			name: "pro oauth requires audit",
			owner: &Account{
				Platform:    PlatformOpenAI,
				Type:        AccountTypeOAuth,
				Credentials: map[string]any{"plan_type": " Pro "},
			},
			want: securityadmission.AccountAuditRequired,
		},
		{
			name: "recognized oauth plans are exempt",
			owner: &Account{
				Platform:    PlatformOpenAI,
				Type:        AccountTypeOAuth,
				Credentials: map[string]any{"plan_type": "self_serve_business_usage_based"},
			},
			want: securityadmission.AccountAuditExemptVerified,
		},
		{
			name: "missing oauth plan is unknown",
			owner: &Account{
				Platform: PlatformOpenAI,
				Type:     AccountTypeOAuth,
			},
			want: securityadmission.AccountUnknown,
		},
		{
			name: "abnormal oauth plan is unknown",
			owner: &Account{
				Platform:    PlatformOpenAI,
				Type:        AccountTypeOAuth,
				Credentials: map[string]any{"plan_type": "abnormal"},
			},
			want: securityadmission.AccountUnknown,
		},
		{
			name: "new oauth enum is unknown",
			owner: &Account{
				Platform:    PlatformOpenAI,
				Type:        AccountTypeOAuth,
				Credentials: map[string]any{"plan_type": "future_entitlement"},
			},
			want: securityadmission.AccountUnknown,
		},
		{
			name: "non openai owner is exempt from openai oauth audit",
			owner: &Account{
				Platform:    PlatformGrok,
				Type:        AccountTypeOAuth,
				Credentials: map[string]any{"plan_type": "pro"},
			},
			want: securityadmission.AccountAuditExemptVerified,
		},
		{
			name: "Vertex Anthropic service account is verified",
			owner: &Account{
				Platform: PlatformAnthropic,
				Type:     AccountTypeServiceAccount,
			},
			want: securityadmission.AccountAuditExemptVerified,
		},
		{
			name: "Vertex Gemini service account is verified",
			owner: &Account{
				Platform: PlatformGemini,
				Type:     AccountTypeServiceAccount,
			},
			want: securityadmission.AccountAuditExemptVerified,
		},
		{
			name: "service account on unsupported platform stays unknown",
			owner: &Account{
				Platform: PlatformOpenAI,
				Type:     AccountTypeServiceAccount,
			},
			want: securityadmission.AccountUnknown,
		},
		{
			name: "shadow never classifies itself",
			owner: func() *Account {
				parentID := int64(42)
				return &Account{Platform: PlatformOpenAI, Type: AccountTypeOAuth, ParentAccountID: &parentID, Credentials: map[string]any{"plan_type": "plus"}}
			}(),
			want: securityadmission.AccountUnknown,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, ClassifyOpenAIEffectiveCredentialOwner(test.owner))
		})
	}
}

func TestClassifyOpenAIAccountAuditClass_ShadowUsesRequestLocalParentCache(t *testing.T) {
	parentID := int64(7001)
	shadow := &Account{ID: 7002, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ParentAccountID: &parentID}
	parent := &Account{ID: parentID, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{"plan_type": "plus"}}
	repo := &admissionTestAccountRepo{accounts: map[int64]*Account{parentID: parent}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)

	require.Equal(t, securityadmission.AccountAuditExemptVerified, svc.ClassifyOpenAIAccountAuditClass(ctx, shadow))
	require.Equal(t, securityadmission.AccountAuditExemptVerified, svc.ClassifyOpenAIAccountAuditClass(ctx, shadow))
	require.Equal(t, 1, repo.calls(parentID), "same request must reuse the parent snapshot")

	repo.mu.Lock()
	repo.accounts[parentID] = &Account{ID: parentID, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{"plan_type": "pro"}}
	repo.mu.Unlock()
	require.Equal(t, securityadmission.AccountAuditExemptVerified, svc.ClassifyOpenAIAccountAuditClass(ctx, shadow), "request-local cache is immutable")

	otherCtx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)
	require.Equal(t, securityadmission.AccountAuditRequired, svc.ClassifyOpenAIAccountAuditClass(otherCtx, shadow))
	require.Equal(t, 2, repo.calls(parentID), "a new request gets a fresh parent snapshot")
}

func TestClassifyOpenAIAccountAuditClass_ShadowRequiresVerifiableParent(t *testing.T) {
	parentID := int64(7011)
	shadow := &Account{ID: 7012, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ParentAccountID: &parentID}
	repo := &admissionTestAccountRepo{accounts: map[int64]*Account{
		parentID: {ID: parentID, Platform: PlatformOpenAI, Type: AccountTypeAPIKey},
	}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)
	require.Equal(t, securityadmission.AccountAuditExemptVerified, svc.ClassifyOpenAIAccountAuditClass(ctx, shadow), "a confirmed API key parent is an exempt effective owner")

	missingParentID := int64(7013)
	missingShadow := &Account{ID: 7014, ParentAccountID: &missingParentID}
	require.Equal(t, securityadmission.AccountUnknown, svc.ClassifyOpenAIAccountAuditClass(ctx, missingShadow))

	secondParentID := int64(7015)
	repo.setAccount(&Account{ID: missingParentID, ParentAccountID: &secondParentID, Platform: PlatformOpenAI, Type: AccountTypeOAuth})
	repo.setAccount(&Account{ID: secondParentID, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{"plan_type": "plus"}})
	otherCtx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)
	require.Equal(t, securityadmission.AccountUnknown, svc.ClassifyOpenAIAccountAuditClass(otherCtx, missingShadow), "nested shadow lineage is not verifiable")
}

func TestAdmitOpenAIAccountRequirement_FreshReloadRejectsPlanChange(t *testing.T) {
	selectedID := int64(7021)
	repo := &admissionTestAccountRepo{accounts: map[int64]*Account{
		selectedID: {
			ID:          selectedID,
			Platform:    PlatformOpenAI,
			Type:        AccountTypeOAuth,
			Status:      StatusActive,
			Schedulable: true,
			Credentials: map[string]any{"plan_type": "pro"},
		},
	}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)

	admission, err := svc.AdmitOpenAIAccountRequirement(ctx, &Account{ID: selectedID, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{"plan_type": "plus"}})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOpenAIAccountRequirementIncompatible)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
	require.NotNil(t, admission)
	require.Equal(t, securityadmission.AccountAuditRequired, admission.AccountClass)
	require.Equal(t, "pro", admission.EffectiveCredentialOwner.GetCredential("plan_type"))
	require.Equal(t, 1, repo.calls(selectedID))
}

func TestAdmitOpenAIAccountRequirement_ShadowReloadsFreshParent(t *testing.T) {
	shadowID := int64(7031)
	parentID := int64(7032)
	parent := &Account{ID: parentID, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Credentials: map[string]any{"plan_type": "plus"}}
	repo := &admissionTestAccountRepo{accounts: map[int64]*Account{
		shadowID: {ID: shadowID, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, ParentAccountID: &parentID},
		parentID: parent,
	}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)

	admission, err := svc.AdmitOpenAIAccountRequirement(ctx, &Account{ID: shadowID})
	require.NoError(t, err)
	require.NotNil(t, admission)
	require.Equal(t, shadowID, admission.Selected.ID)
	require.Equal(t, parentID, admission.EffectiveCredentialOwner.ID)
	require.Equal(t, securityadmission.AccountAuditExemptVerified, admission.AccountClass)
	require.Equal(t, 1, repo.calls(shadowID))
	require.Equal(t, 1, repo.calls(parentID))

	// The no-audit request class retains the legacy shadow universe; terminal
	// admission must not require the parent to be OAuth when the effective owner
	// is a confirmed API key.
	repo.setAccount(&Account{ID: parentID, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive})
	anyCtx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAny)
	admission, err = svc.AdmitOpenAIAccountRequirement(anyCtx, &Account{ID: shadowID})
	require.NoError(t, err)
	require.Equal(t, securityadmission.AccountAuditExemptVerified, admission.AccountClass)
}

func TestAdmitOpenAIAccountRequirement_MissingRowsFailClosed(t *testing.T) {
	svc := &OpenAIGatewayService{accountRepo: &admissionTestAccountRepo{accounts: map[int64]*Account{}}}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)

	_, err := svc.AdmitOpenAIAccountRequirement(ctx, &Account{ID: 7041})
	require.ErrorIs(t, err, ErrOpenAIAccountAdmissionUnavailable)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)

	parentID := int64(7043)
	repo := &admissionTestAccountRepo{accounts: map[int64]*Account{
		7042: {ID: 7042, ParentAccountID: &parentID, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true},
	}}
	svc.accountRepo = repo
	_, err = svc.AdmitOpenAIAccountRequirement(ctx, &Account{ID: 7042})
	require.ErrorIs(t, err, ErrOpenAIAccountAdmissionUnavailable)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
}

func TestOpenAIAccountRequirementCompatible_AnyDoesNotClassify(t *testing.T) {
	// The any-account path is intentionally a zero-cost no-op. This protects the
	// hot path from accidental parent DB lookups after the security gate is added.
	parentID := int64(7052)
	repo := &admissionTestAccountRepo{accounts: map[int64]*Account{
		parentID: {ID: parentID, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Credentials: map[string]any{"plan_type": "pro"}},
	}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	account := &Account{ID: 7051, ParentAccountID: &parentID}
	require.True(t, func() bool {
		ok, _ := svc.openAIAccountRequirementCompatible(context.Background(), account, securityadmission.AccountRequirementAny)
		return ok
	}())
	require.Equal(t, 0, repo.calls(parentID))
}

func TestOpenAIAccountRequirementCannotDowngradeAuditExempt(t *testing.T) {
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)
	ctx = WithOpenAIAccountRequirement(ctx, securityadmission.AccountRequirementAny)
	require.Equal(t, securityadmission.AccountRequirementAuditExempt, OpenAIAccountRequirementFromContext(ctx))

	pro := &Account{
		ID: 7053, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Credentials: map[string]any{"plan_type": "pro"},
	}
	compatible, _ := (&OpenAIGatewayService{}).openAIAccountRequirementCompatible(
		ctx,
		pro,
		securityadmission.AccountRequirementAny,
	)
	require.False(t, compatible, "an explicit stale Any value must not weaken the request-local hard constraint")
}

func TestOpenAIAccountRequirementCompatible_AccountMappedSearchModelUpgradesConstraint(t *testing.T) {
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAny)
	ctx = WithOpenAIDirectForwardModel(ctx, "public-alias")
	pro := &Account{
		ID: 7054, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Credentials: map[string]any{
			"plan_type": "pro",
			"model_mapping": map[string]any{
				"public-alias": "gpt-5-search-api",
			},
		},
	}
	require.Equal(t, "gpt-5-search-api", pro.GetMappedModel("public-alias"))
	resolvedModel, resolved := resolveOpenAISecurityUpstreamModel(ctx, pro)
	require.True(t, resolved)
	require.Equal(t, "gpt-5.4", resolvedModel, "OAuth normalization happens after the security-sensitive mapping target")

	compatible, reason := (&OpenAIGatewayService{}).openAIAccountRequirementCompatible(
		ctx,
		pro,
		securityadmission.AccountRequirementAny,
	)
	require.False(t, compatible, reason)
	require.Equal(t, securityadmission.AccountRequirementAuditExempt, OpenAIAccountRequirementFromContext(ctx))
}

func TestAdmitOpenAIAccountRequirement_FreshAccountMappingToSearchRejectsPro(t *testing.T) {
	const accountID = int64(7055)
	repo := &admissionTestAccountRepo{accounts: map[int64]*Account{
		accountID: {
			ID: accountID, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
			Status: StatusActive, Schedulable: true,
			Credentials: map[string]any{
				"plan_type": "pro",
				"model_mapping": map[string]any{
					"public-alias": "gpt-5-search-api",
				},
			},
		},
	}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAny)
	ctx = WithOpenAIDirectForwardModel(ctx, "public-alias")

	admission, err := svc.AdmitOpenAIAccountRequirement(ctx, &Account{ID: accountID})
	require.ErrorIs(t, err, ErrOpenAIAccountRequirementIncompatible)
	require.NotNil(t, admission)
	require.Equal(t, securityadmission.AccountRequirementAuditExempt, admission.Requirement)
	require.Equal(t, securityadmission.AccountRequirementAuditExempt, OpenAIAccountRequirementFromContext(ctx))
}

func requirementSchedulerService(accounts []Account) *OpenAIGatewayService {
	cfg := &config.Config{}
	cfg.Gateway.Scheduling.LoadBatchEnabled = false
	return &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: accounts},
		cfg:                cfg,
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{}),
	}
}

func TestOpenAIAdvancedScheduler_RequirementFiltersProAndUnknown(t *testing.T) {
	accounts := []Account{
		{ID: 7061, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 100, Credentials: map[string]any{"plan_type": "pro"}},
		{ID: 7062, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0, Credentials: map[string]any{"plan_type": "plus"}},
		{ID: 7063, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: -100, Credentials: map[string]any{"plan_type": "future_plan"}},
	}
	svc := requirementSchedulerService(accounts)
	scheduler := newDefaultOpenAIAccountScheduler(svc, nil).(*defaultOpenAIAccountScheduler)
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)
	selection, _, err := scheduler.Select(ctx, OpenAIAccountScheduleRequest{Platform: PlatformOpenAI})
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, int64(7062), selection.Account.ID)
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}

	// An explicit stale Any value cannot weaken the request-local hard constraint.
	compatible, reason := scheduler.isAccountRequestCompatibleReason(ctx, &accounts[0], OpenAIAccountScheduleRequest{
		Platform:    PlatformOpenAI,
		Requirement: securityadmission.AccountRequirementAny,
	})
	require.False(t, compatible, reason)
}

func TestOpenAIAdvancedScheduler_RequirementDropsIncompatibleSessionSticky(t *testing.T) {
	const sessionHash = "requirement_sticky"
	accounts := []Account{
		{ID: 7071, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0, Credentials: map[string]any{"plan_type": "pro"}},
		{ID: 7072, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 10, Credentials: map[string]any{"plan_type": "plus"}},
	}
	cache := &schedulerTestGatewayCache{sessionBindings: map[string]int64{"openai:" + sessionHash: 7071}}
	svc := requirementSchedulerService(accounts)
	svc.cache = cache
	scheduler := newDefaultOpenAIAccountScheduler(svc, nil).(*defaultOpenAIAccountScheduler)
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)

	selection, decision, err := scheduler.Select(ctx, OpenAIAccountScheduleRequest{
		Platform:    PlatformOpenAI,
		SessionHash: sessionHash,
	})
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, int64(7072), selection.Account.ID)
	require.Equal(t, openAIAccountScheduleLayerLoadBalance, decision.Layer)
	require.Equal(t, int64(7071), cache.sessionBindings["openai:"+sessionHash], "security-gated selection must preserve the previous sticky binding until terminal admission")
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
	terminalCtx := WithOpenAIAccountTerminalAdmission(ctx, &OpenAIAccountRequirementAdmission{
		Selected:                 &accounts[1],
		EffectiveCredentialOwner: &accounts[1],
		Requirement:              securityadmission.AccountRequirementAuditExempt,
		AccountClass:             securityadmission.AccountAuditExemptVerified,
	})
	require.NoError(t, svc.BindStickySessionAfterProfitAdmission(terminalCtx, nil, sessionHash, accounts[1].ID))
	require.Equal(t, int64(7072), cache.sessionBindings["openai:"+sessionHash], "verified terminal admission may bind the successful account")
}

func TestOpenAILegacyScheduler_RequirementFiltersProAndUnknown(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	defer resetOpenAIAdvancedSchedulerSettingCacheForTest()

	accounts := []Account{
		{ID: 7081, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0, Credentials: map[string]any{"plan_type": "pro"}},
		{ID: 7082, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 10, Credentials: map[string]any{"plan_type": "plus"}},
		{ID: 7083, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: -10, Credentials: map[string]any{"plan_type": "future_plan"}},
	}
	svc := requirementSchedulerService(accounts)
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)
	selection, _, err := svc.SelectAccountWithScheduler(ctx, nil, "", "", "", nil, OpenAIUpstreamTransportAny, false)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, int64(7082), selection.Account.ID)
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
}

func TestOpenAIScheduler_RequirementPreservesPreviousResponseBinding(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	defer resetOpenAIAdvancedSchedulerSettingCacheForTest()

	groupID := int64(7090)
	previousResponseID := "requirement_previous_response"
	accounts := []Account{
		{ID: 7091, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0, GroupIDs: []int64{groupID}, Credentials: map[string]any{"plan_type": "pro"}, Extra: map[string]any{"openai_oauth_responses_websockets_v2_enabled": true}},
		{ID: 7092, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 10, GroupIDs: []int64{groupID}, Credentials: map[string]any{"plan_type": "plus"}},
	}
	cfg := newSchedulerTestOpenAIWSV2Config()
	cfg.Gateway.Scheduling.LoadBatchEnabled = false
	svc := &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: accounts},
		cache:              &schedulerTestGatewayCache{},
		cfg:                cfg,
		rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true"),
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{}),
	}
	store := svc.getOpenAIWSStateStore()
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)
	require.NoError(t, store.BindResponseAccount(ctx, groupID, previousResponseID, 7091, time.Hour))

	selection, _, err := svc.SelectAccountWithSchedulerForCapability(
		ctx, &groupID, previousResponseID, "", "", nil,
		OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions,
		false, false, true, PlatformOpenAI,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
	require.Nil(t, selection, "non-movable previous_response_id must not silently migrate")

	selection, decision, err := svc.SelectAccountWithSchedulerForCapability(
		ctx, &groupID, previousResponseID, "", "", nil,
		OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions,
		false, true, true, PlatformOpenAI,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, int64(7092), selection.Account.ID)
	require.Equal(t, openAIAccountScheduleLayerLoadBalance, decision.Layer)
	require.False(t, decision.StickyPreviousHit)
	if selection.ReleaseFunc != nil {
		selection.ReleaseFunc()
	}
}

func TestOpenAIPreviousResponseResolver_RequirementRejectsProBeforeSlotAcquire(t *testing.T) {
	groupID := int64(7100)
	account := Account{
		ID: 7101, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		GroupIDs:    []int64{groupID},
		Credentials: map[string]any{"plan_type": "pro"},
		Extra:       map[string]any{"openai_oauth_responses_websockets_v2_enabled": true},
	}
	cfg := newSchedulerTestOpenAIWSV2Config()
	acquired := make([]int64, 0, 1)
	svc := &OpenAIGatewayService{
		accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{account}},
		cache:       &schedulerTestGatewayCache{},
		cfg:         cfg,
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{
			acquiredIDs: &acquired,
		}),
	}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)
	store := svc.getOpenAIWSStateStore()
	require.NoError(t, store.BindResponseAccount(ctx, groupID, "direct_previous_pro", account.ID, time.Hour))

	selection, err := svc.SelectAccountByPreviousResponseID(ctx, &groupID, "direct_previous_pro", "", nil, false)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrOpenAIAccountRequirementIncompatible)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
	require.Nil(t, selection)
	require.Empty(t, acquired, "incompatible previous account must be rejected before slot acquisition")
	require.Zero(t, svc.ResolveAccountIDByPreviousResponseIDForScheduler(ctx, &groupID, "direct_previous_pro", "", nil, "", false))
}

func TestOpenAIScheduler_RequirementPreviousResponsePausedProCannotFallback(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	defer resetOpenAIAdvancedSchedulerSettingCacheForTest()

	groupID := int64(7110)
	previousResponseID := "paused_pro_previous_response"
	pausedPro := Account{
		ID: 7111, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		GroupIDs:    []int64{groupID},
		Credentials: map[string]any{"plan_type": "pro"},
		Extra: map[string]any{
			"openai_oauth_responses_websockets_v2_enabled": true,
			"codex_5h_used_percent":                        96.0,
			"auto_pause_5h_threshold":                      0.95,
		},
	}
	backupPlus := Account{
		ID: 7112, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		GroupIDs: []int64{groupID}, Credentials: map[string]any{"plan_type": "plus"},
	}
	acquired := make([]int64, 0, 1)
	cfg := newSchedulerTestOpenAIWSV2Config()
	cfg.Gateway.Scheduling.LoadBatchEnabled = false
	svc := &OpenAIGatewayService{
		accountRepo: schedulerTestOpenAIAccountRepo{accounts: []Account{pausedPro, backupPlus}},
		cache:       &schedulerTestGatewayCache{},
		cfg:         cfg,
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{
			acquiredIDs: &acquired,
		}),
		rateLimitService: newOpenAIAdvancedSchedulerRateLimitService("true"),
	}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)
	ctx = withOpenAIQuotaAutoPauseSettings(ctx, OpsOpenAIAccountQuotaAutoPauseSettings{DefaultThreshold5h: 0.95})
	store := svc.getOpenAIWSStateStore()
	require.NoError(t, store.BindResponseAccount(ctx, groupID, previousResponseID, pausedPro.ID, time.Hour))

	selection, _, err := svc.SelectAccountWithSchedulerForCapability(
		ctx, &groupID, previousResponseID, "", "", nil,
		OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions,
		false, false, true, PlatformOpenAI,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrNoAvailableAccounts)
	require.Nil(t, selection, "a non-movable Pro binding must not fall through to the Plus backup")
	require.Empty(t, acquired, "the incompatible bound account must be rejected before slot acquisition")
}

type previousResponseRecheckFailRepo struct {
	AccountRepository
	accounts []Account
	boundID  int64
	getCalls int
	mu       sync.Mutex
}

func (r *previousResponseRecheckFailRepo) GetByID(_ context.Context, id int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if id == r.boundID {
		r.getCalls++
		if r.getCalls >= 2 {
			return nil, errors.New("simulated previous-response DB recheck failure")
		}
	}
	for index := range r.accounts {
		if r.accounts[index].ID == id {
			account := r.accounts[index]
			return &account, nil
		}
	}
	return nil, errors.New("account not found")
}

func (r *previousResponseRecheckFailRepo) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.getCalls
}

func (r *previousResponseRecheckFailRepo) ListSchedulableByGroupIDAndPlatform(_ context.Context, _ int64, platform string) ([]Account, error) {
	return r.listByPlatform(platform), nil
}

func (r *previousResponseRecheckFailRepo) ListSchedulableByPlatform(_ context.Context, platform string) ([]Account, error) {
	return r.listByPlatform(platform), nil
}

func (r *previousResponseRecheckFailRepo) ListSchedulableUngroupedByPlatform(_ context.Context, platform string) ([]Account, error) {
	return r.listByPlatform(platform), nil
}

func (r *previousResponseRecheckFailRepo) listByPlatform(platform string) []Account {
	r.mu.Lock()
	defer r.mu.Unlock()
	accounts := make([]Account, 0, len(r.accounts))
	for _, account := range r.accounts {
		if account.Platform == platform {
			accounts = append(accounts, account)
		}
	}
	return accounts
}

func TestOpenAIScheduler_RequirementPreviousResponseReusesFreshPreSlotAdmission(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	defer resetOpenAIAdvancedSchedulerSettingCacheForTest()

	groupID := int64(7120)
	previousResponseID := "db_recheck_failure_previous_response"
	boundPlus := Account{
		ID: 7121, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		GroupIDs: []int64{groupID}, Credentials: map[string]any{"plan_type": "plus"},
		Extra: map[string]any{"openai_oauth_responses_websockets_v2_enabled": true},
	}
	backupPlus := Account{
		ID: 7122, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		GroupIDs: []int64{groupID}, Credentials: map[string]any{"plan_type": "plus"},
	}
	repo := &previousResponseRecheckFailRepo{
		accounts: []Account{boundPlus, backupPlus},
		boundID:  boundPlus.ID,
	}
	snapshotCache := &openAISnapshotCacheStub{
		accountsByID: map[int64]*Account{boundPlus.ID: &boundPlus},
	}
	acquired := make([]int64, 0, 1)
	cfg := newSchedulerTestOpenAIWSV2Config()
	cfg.Gateway.Scheduling.LoadBatchEnabled = false
	svc := &OpenAIGatewayService{
		accountRepo:        repo,
		cache:              &schedulerTestGatewayCache{},
		cfg:                cfg,
		schedulerSnapshot:  &SchedulerSnapshotService{cache: snapshotCache},
		concurrencyService: NewConcurrencyService(schedulerTestConcurrencyCache{acquiredIDs: &acquired}),
		rateLimitService:   newOpenAIAdvancedSchedulerRateLimitService("true"),
	}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)
	store := svc.getOpenAIWSStateStore()
	require.NoError(t, store.BindResponseAccount(ctx, groupID, previousResponseID, boundPlus.ID, time.Hour))

	selection, _, err := svc.SelectAccountWithSchedulerForCapability(
		ctx, &groupID, previousResponseID, "", "", nil,
		OpenAIUpstreamTransportAny, OpenAIEndpointCapabilityChatCompletions,
		false, false, true, PlatformOpenAI,
	)
	require.NoError(t, err)
	require.NotNil(t, selection)
	require.Equal(t, boundPlus.ID, selection.Account.ID)
	require.Equal(t, []int64{boundPlus.ID}, acquired)
	require.Equal(t, 1, repo.calls(), "the audit-exempt previous binding should reuse its first fresh row before slot acquisition")
	if selection.ReleaseFunc != nil {
		defer selection.ReleaseFunc()
	}

	// The handler's post-slot terminal admission remains a distinct fresh DB
	// boundary. A failure there still closes the request instead of dispatching
	// or silently migrating the hard previous-response binding.
	terminal, terminalErr := svc.AdmitOpenAIAccountRequirement(ctx, selection.Account)
	require.Error(t, terminalErr)
	require.ErrorIs(t, terminalErr, ErrNoAvailableAccounts)
	require.Nil(t, terminal)
	require.Equal(t, 2, repo.calls())
}

func TestOpenAICredentialProof_RejectsWebSocketCredentialDrift(t *testing.T) {
	account := &Account{
		ID: 7131, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{
			"access_token":   "stable-token",
			"plan_type":      "pro",
			"_token_version": int64(11),
		},
	}
	repo := &admissionTestAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAny)
	terminal, err := svc.AdmitOpenAIAccountRequirement(ctx, account)
	require.NoError(t, err)
	ctx = WithOpenAIAccountTerminalAdmission(ctx, terminal)
	proof, err := svc.CaptureOpenAICredentialProof(ctx, account, "stable-token", "oauth")
	require.NoError(t, err)
	require.NotNil(t, proof)

	fresh, err := svc.AdmitOpenAIAccountRequirement(ctx, account)
	require.NoError(t, err)
	require.NoError(t, ValidateOpenAICredentialProof(fresh, proof))

	drifted := *account
	drifted.Credentials = shallowCopyMap(account.Credentials)
	drifted.Credentials["access_token"] = "rotated-token"
	repo.setAccount(&drifted)
	fresh, err = svc.AdmitOpenAIAccountRequirement(ctx, account)
	require.NoError(t, err)
	require.Error(t, ValidateOpenAICredentialProof(fresh, proof))

	drifted.Credentials["access_token"] = "stable-token"
	drifted.Credentials["_token_version"] = int64(12)
	repo.setAccount(&drifted)
	fresh, err = svc.AdmitOpenAIAccountRequirement(ctx, account)
	require.NoError(t, err)
	require.Error(t, ValidateOpenAICredentialProof(fresh, proof))

	drifted.Credentials["_token_version"] = int64(11)
	drifted.Credentials["plan_type"] = "plus"
	repo.setAccount(&drifted)
	fresh, err = svc.AdmitOpenAIAccountRequirement(ctx, account)
	require.NoError(t, err)
	require.Error(t, ValidateOpenAICredentialProof(fresh, proof))
}

func TestOpenAICredentialProof_AgentIdentityUsesStableMaterialWithoutVersion(t *testing.T) {
	key, privateKey := newTestAgentIdentityKey(t)
	account := &Account{
		ID: 7141, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{
			"auth_mode":                  OpenAIAuthModeAgentIdentity,
			"agent_runtime_id":           key.runtimeID,
			"agent_private_key":          privateKey,
			"task_id":                    "task-original",
			"chatgpt_account_id":         "tenant-original",
			"chatgpt_user_id":            "user-original",
			"chatgpt_account_is_fedramp": false,
			"plan_type":                  "plus",
		},
	}
	repo := &admissionTestAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	ctx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAny)
	terminal, err := svc.AdmitOpenAIAccountRequirement(ctx, account)
	require.NoError(t, err)
	ctx = WithOpenAIAccountTerminalAdmission(ctx, terminal)

	proof, err := svc.CaptureOpenAICredentialProof(ctx, account, "", OpenAIAuthModeAgentIdentity)
	require.NoError(t, err)
	require.NotNil(t, proof)
	require.True(t, proof.hasAgentIdentityHash)
	require.Zero(t, proof.tokenVersion, "Agent Identity must not depend on an optional bearer-token version")
	fresh, err := svc.AdmitOpenAIAccountRequirement(ctx, account)
	require.NoError(t, err)
	require.NoError(t, ValidateOpenAICredentialProof(fresh, proof))

	// A task registration and optional metadata version can rotate under the
	// same signing identity. The handler and ingress pool proofs must remain
	// consistent across that transparent recovery.
	recovered := *account
	recovered.Credentials = shallowCopyMap(account.Credentials)
	recovered.Credentials["task_id"] = "task-recovered"
	recovered.Credentials["_token_version"] = int64(21)
	repo.setAccount(&recovered)
	fresh, err = svc.AdmitOpenAIAccountRequirement(ctx, account)
	require.NoError(t, err)
	require.NoError(t, ValidateOpenAICredentialProof(fresh, proof))

	for _, test := range []struct {
		name  string
		key   string
		value any
	}{
		{name: "runtime", key: "agent_runtime_id", value: "runtime-drifted"},
		{name: "private key", key: "agent_private_key", value: "private-key-drifted"},
		{name: "tenant", key: "chatgpt_account_id", value: "tenant-drifted"},
		{name: "user", key: "chatgpt_user_id", value: "user-drifted"},
		{name: "fedramp", key: "chatgpt_account_is_fedramp", value: true},
	} {
		t.Run(test.name+" drift is rejected", func(t *testing.T) {
			drifted := recovered
			drifted.Credentials = shallowCopyMap(recovered.Credentials)
			drifted.Credentials[test.key] = test.value
			repo.setAccount(&drifted)
			current, admissionErr := svc.AdmitOpenAIAccountRequirement(ctx, account)
			require.NoError(t, admissionErr)
			require.Error(t, ValidateOpenAICredentialProof(current, proof))
		})
	}

	incomplete := recovered
	incomplete.Credentials = shallowCopyMap(recovered.Credentials)
	delete(incomplete.Credentials, "agent_private_key")
	repo.setAccount(&incomplete)
	terminal, err = svc.AdmitOpenAIAccountRequirement(ctx, account)
	require.NoError(t, err)
	failureCtx := WithOpenAIAccountTerminalAdmission(ctx, terminal)
	proof, err = svc.CaptureOpenAICredentialProof(failureCtx, account, "", OpenAIAuthModeAgentIdentity)
	require.Error(t, err)
	require.Nil(t, proof)
	_, err = openAIFinalizedCredentialFromContext(failureCtx, account)
	require.Error(t, err, "a failed websocket proof must leave no reusable finalized credential")
}
