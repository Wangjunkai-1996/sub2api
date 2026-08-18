package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type blockingOpenAIAccountAuditSettingsRepo struct {
	SettingRepository

	mu              sync.Mutex
	values          map[string]string
	firstGetErr     error
	firstGetStarted chan struct{}
	firstGetRelease chan struct{}
	getCalls        int
}

func (r *blockingOpenAIAccountAuditSettingsRepo) GetMultiple(ctx context.Context, keys []string) (map[string]string, error) {
	r.mu.Lock()
	r.getCalls++
	first := r.getCalls == 1
	values := make(map[string]string, len(keys))
	for _, key := range keys {
		if value, ok := r.values[key]; ok {
			values[key] = value
		}
	}
	err := error(nil)
	if first {
		err = r.firstGetErr
		if r.firstGetStarted != nil {
			close(r.firstGetStarted)
		}
	}
	release := r.firstGetRelease
	r.mu.Unlock()

	if first && release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return values, err
}

func (r *blockingOpenAIAccountAuditSettingsRepo) SetMultiple(_ context.Context, settings map[string]string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.values == nil {
		r.values = make(map[string]string, len(settings))
	}
	for key, value := range settings {
		r.values[key] = value
	}
	return nil
}

func openAIAccountAuditSettingsValues(groupIDs, threshold, preferAPIKey, rollout string) map[string]string {
	return map[string]string{
		SettingKeyOpenAIAccountAuditGroupIDs:                    groupIDs,
		SettingKeyOpenAIAccountAuditLongTextRuneThreshold:       threshold,
		SettingKeyOpenAIAccountAuditPreferAPIKeyEnabled:         preferAPIKey,
		SettingKeyOpenAIAccountAuditLongTextOAuthRolloutPercent: rollout,
	}
}

func waitForOpenAIAccountAuditSettingsRead(t *testing.T, started <-chan struct{}) {
	t.Helper()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for OpenAI account audit settings read")
	}
}

func TestOpenAIAccountAuditRoutingRuntimeDefaults(t *testing.T) {
	svc := NewSettingService(&cyberSettingsRepo{values: map[string]string{}}, &config.Config{})

	policy := svc.GetOpenAIAccountAuditRoutingRuntime(context.Background())
	require.True(t, policy.Available())
	require.Equal(t, []int64{12}, policy.AccountGroupIDs())
	require.Equal(t, DefaultOpenAIAccountAuditLongTextRuneThreshold, policy.LongTextRuneThreshold())
	require.True(t, policy.PreferAPIKeyEnabled())
	require.Zero(t, policy.LongTextOAuthRolloutPercent())

	groups := policy.AccountGroupIDs()
	groups[0] = 99
	require.Equal(t, []int64{12}, policy.AccountGroupIDs())
}

func TestOpenAIAccountAuditRoutingRuntimeUpdateWinsAgainstInflightLoad(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	repo := &blockingOpenAIAccountAuditSettingsRepo{
		values:          openAIAccountAuditSettingsValues("[12]", "12000", "true", "5"),
		firstGetStarted: started,
		firstGetRelease: release,
	}
	svc := NewSettingService(repo, &config.Config{})
	loaded := make(chan OpenAIAccountAuditRoutingPolicy, 1)
	go func() {
		loaded <- svc.GetOpenAIAccountAuditRoutingRuntime(context.Background())
	}()
	waitForOpenAIAccountAuditSettingsRead(t, started)

	err := svc.UpdateOpenAIAccountAuditRoutingSettings(context.Background(), OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs:             []int64{19},
		LongTextRuneThreshold:       24000,
		PreferAPIKeyEnabled:         false,
		LongTextOAuthRolloutPercent: 35,
	})
	require.NoError(t, err)
	close(release)

	var inflight OpenAIAccountAuditRoutingPolicy
	select {
	case inflight = <-loaded:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for in-flight OpenAI account audit settings load")
	}
	require.Equal(t, []int64{19}, inflight.AccountGroupIDs())
	require.Equal(t, 24000, inflight.LongTextRuneThreshold())
	require.False(t, inflight.PreferAPIKeyEnabled())
	require.Equal(t, 35, inflight.LongTextOAuthRolloutPercent())

	current := svc.GetOpenAIAccountAuditRoutingRuntime(context.Background())
	require.Equal(t, []int64{19}, current.AccountGroupIDs())
	require.Equal(t, 35, current.LongTextOAuthRolloutPercent())
}

func TestOpenAIAccountAuditRoutingRuntimeErrorLoadCannotRestoreUnavailableCacheAfterRefresh(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	repo := &blockingOpenAIAccountAuditSettingsRepo{
		values:          openAIAccountAuditSettingsValues("[12]", "12000", "true", "5"),
		firstGetErr:     errors.New("temporary settings read failure"),
		firstGetStarted: started,
		firstGetRelease: release,
	}
	svc := NewSettingService(repo, &config.Config{})
	loaded := make(chan OpenAIAccountAuditRoutingPolicy, 1)
	go func() {
		loaded <- svc.GetOpenAIAccountAuditRoutingRuntime(context.Background())
	}()
	waitForOpenAIAccountAuditSettingsRead(t, started)

	require.NoError(t, repo.SetMultiple(context.Background(), openAIAccountAuditSettingsValues("[27]", "18000", "false", "41")))
	svc.refreshOpenAIAccountAuditLongTextOAuthRolloutPercent(41)
	close(release)

	select {
	case inflight := <-loaded:
		require.False(t, inflight.Available())
		require.Equal(t, 41, inflight.LongTextOAuthRolloutPercent())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for failed in-flight OpenAI account audit settings load")
	}

	refreshed := svc.GetOpenAIAccountAuditRoutingRuntime(context.Background())
	require.True(t, refreshed.Available())
	require.Equal(t, []int64{27}, refreshed.AccountGroupIDs())
	require.Equal(t, 18000, refreshed.LongTextRuneThreshold())
	require.False(t, refreshed.PreferAPIKeyEnabled())
	require.Equal(t, 41, refreshed.LongTextOAuthRolloutPercent())
}

func TestOpenAIAccountAuditRoutingSettingsUpdateRefreshesRuntime(t *testing.T) {
	repo := &cyberSettingsRepo{values: map[string]string{}}
	svc := NewSettingService(repo, &config.Config{})

	err := svc.UpdateOpenAIAccountAuditRoutingSettings(context.Background(), OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs:             []int64{19, 7, 19, -1},
		LongTextRuneThreshold:       24000,
		PreferAPIKeyEnabled:         false,
		LongTextOAuthRolloutPercent: 35,
	})
	require.NoError(t, err)
	require.Equal(t, "[7,19]", repo.values[SettingKeyOpenAIAccountAuditGroupIDs])
	require.Equal(t, "24000", repo.values[SettingKeyOpenAIAccountAuditLongTextRuneThreshold])
	require.Equal(t, "false", repo.values[SettingKeyOpenAIAccountAuditPreferAPIKeyEnabled])
	require.Equal(t, "35", repo.values[SettingKeyOpenAIAccountAuditLongTextOAuthRolloutPercent])

	policy := svc.GetOpenAIAccountAuditRoutingRuntime(context.Background())
	require.True(t, policy.Available())
	require.Equal(t, []int64{7, 19}, policy.AccountGroupIDs())
	require.Equal(t, 24000, policy.LongTextRuneThreshold())
	require.False(t, policy.PreferAPIKeyEnabled())
	require.Equal(t, 35, policy.LongTextOAuthRolloutPercent())
}

func TestOpenAIAccountAuditRoutingRuntimeFailureRetainsValuesButMarksUnavailable(t *testing.T) {
	repo := &cyberSettingsRepo{values: map[string]string{
		SettingKeyOpenAIAccountAuditGroupIDs:                    "[21]",
		SettingKeyOpenAIAccountAuditLongTextRuneThreshold:       "16000",
		SettingKeyOpenAIAccountAuditPreferAPIKeyEnabled:         "false",
		SettingKeyOpenAIAccountAuditLongTextOAuthRolloutPercent: "25",
	}}
	svc := NewSettingService(repo, &config.Config{})
	initial := svc.GetOpenAIAccountAuditRoutingRuntime(context.Background())
	require.True(t, initial.Available())

	repo.getMultipleErr = errors.New("settings unavailable")
	svc.openAIAccountAuditRoutingRuntimeCache.Store(&cachedOpenAIAccountAuditRoutingRuntime{
		policy: initial, expiresAt: time.Now().Add(-time.Second).UnixNano(),
	})
	policy := svc.GetOpenAIAccountAuditRoutingRuntime(context.Background())
	require.False(t, policy.Available())
	require.Equal(t, []int64{21}, policy.AccountGroupIDs())
	require.Equal(t, 16000, policy.LongTextRuneThreshold())
	require.False(t, policy.PreferAPIKeyEnabled())
	require.Equal(t, 25, policy.LongTextOAuthRolloutPercent())
}

func TestClassifyOpenAIAccountAuditEligibility(t *testing.T) {
	policy := newOpenAIAccountAuditRoutingPolicy(OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs:       []int64{12},
		LongTextRuneThreshold: 12000,
		PreferAPIKeyEnabled:   true,
	}, true)
	base := Account{
		ID:          1,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		GroupIDs:    []int64{12},
		Credentials: map[string]any{"plan_type": " Pro "},
	}

	tests := []struct {
		name          string
		mutate        func(*Account)
		eligible      bool
		indeterminate bool
		reason        OpenAIAccountAuditEligibilityReason
	}{
		{name: "eligible", eligible: true, reason: OpenAIAccountAuditEligible},
		{name: "apikey", mutate: func(a *Account) { a.Type = AccountTypeAPIKey }, reason: OpenAIAccountAuditIneligibleAccountType},
		{name: "setup token", mutate: func(a *Account) { a.Type = AccountTypeSetupToken }, reason: OpenAIAccountAuditIneligibleAccountType},
		{name: "other platform", mutate: func(a *Account) { a.Platform = PlatformGrok }, reason: OpenAIAccountAuditIneligiblePlatform},
		{name: "chatgptpro not accepted", mutate: func(a *Account) { a.Credentials["plan_type"] = "chatgptpro" }, reason: OpenAIAccountAuditIneligiblePlan},
		{name: "wrong group", mutate: func(a *Account) { a.GroupIDs = []int64{13} }, reason: OpenAIAccountAuditIneligibleGroup},
		{name: "repository account groups", mutate: func(a *Account) {
			a.GroupIDs = nil
			a.AccountGroups = []AccountGroup{{GroupID: 12}}
		}, eligible: true, reason: OpenAIAccountAuditEligible},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			account := base
			account.Credentials = map[string]any{"plan_type": base.Credentials["plan_type"]}
			if tt.mutate != nil {
				tt.mutate(&account)
			}
			got := ClassifyOpenAIAccountAuditEligibility(&account, policy)
			require.Equal(t, tt.eligible, got.Eligible)
			require.Equal(t, tt.indeterminate, got.Indeterminate)
			require.Equal(t, tt.reason, got.Reason)
		})
	}
}

func TestClassifyOpenAIAccountAuditEligibilityUnavailablePolicy(t *testing.T) {
	policy := DefaultOpenAIAccountAuditRoutingPolicy()
	policy.available = false
	oauthPro := &Account{
		Platform: PlatformOpenAI, Type: AccountTypeOAuth, GroupIDs: []int64{12},
		Credentials: map[string]any{"plan_type": "pro"},
	}

	result := ClassifyOpenAIAccountAuditEligibility(oauthPro, policy)
	require.False(t, result.Eligible)
	require.True(t, result.Indeterminate)
	require.Equal(t, OpenAIAccountAuditPolicyUnavailable, result.Reason)

	oauthPro.Type = AccountTypeAPIKey
	result = ClassifyOpenAIAccountAuditEligibility(oauthPro, policy)
	require.False(t, result.Eligible)
	require.False(t, result.Indeterminate)
	require.Equal(t, OpenAIAccountAuditIneligibleAccountType, result.Reason)
}

func TestValidateOpenAIAccountAuditRoutingSettings(t *testing.T) {
	require.Error(t, ValidateOpenAIAccountAuditRoutingSettings(OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs:       nil,
		LongTextRuneThreshold: 12000,
	}))
	require.Error(t, ValidateOpenAIAccountAuditRoutingSettings(OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs:       []int64{12},
		LongTextRuneThreshold: 0,
	}))
	for _, percent := range []int{-1, 101} {
		require.Error(t, ValidateOpenAIAccountAuditRoutingSettings(OpenAIAccountAuditRoutingSettings{
			AccountGroupIDs:             []int64{12},
			LongTextRuneThreshold:       12000,
			LongTextOAuthRolloutPercent: percent,
		}))
	}
}

func TestOpenAIAccountAuditLongTextOAuthRolloutIsStableAndBounded(t *testing.T) {
	zero := newOpenAIAccountAuditRoutingPolicy(OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs: []int64{12}, LongTextRuneThreshold: 12000,
		PreferAPIKeyEnabled: true, LongTextOAuthRolloutPercent: 0,
	}, true)
	require.False(t, zero.LongTextOAuthRolloutSelected("session:stable"))

	full := newOpenAIAccountAuditRoutingPolicy(OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs: []int64{12}, LongTextRuneThreshold: 12000,
		PreferAPIKeyEnabled: true, LongTextOAuthRolloutPercent: 100,
	}, true)
	require.True(t, full.LongTextOAuthRolloutSelected("session:stable"))
	require.True(t, full.LongTextOAuthRolloutSelected(""), "100 percent must not depend on a fallback key")

	partial := newOpenAIAccountAuditRoutingPolicy(OpenAIAccountAuditRoutingSettings{
		AccountGroupIDs: []int64{12}, LongTextRuneThreshold: 12000,
		PreferAPIKeyEnabled: true, LongTextOAuthRolloutPercent: 37,
	}, true)
	first := partial.LongTextOAuthRolloutSelected("session:stable")
	for range 10 {
		require.Equal(t, first, partial.LongTextOAuthRolloutSelected("session:stable"))
	}
	require.False(t, partial.LongTextOAuthRolloutSelected(""))
}
