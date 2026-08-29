package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestProvideOpenAIWindowWarmupOptionsReadsSettingsAndKeepsSafetyControlsDynamic(t *testing.T) {
	repo := &panelRateLimitSettingRepo{values: map[string]string{
		SettingKeyOpenAIWindowWarmupEnabled:               "true",
		SettingKeyOpenAIWindowWarmupDefaultPolicy:         OpenAIWindowWarmupPolicyContinuous,
		SettingKeyOpenAIWindowWarmupAllowlist:             `[42,7,42]`,
		SettingKeyOpenAIWindowWarmupProbeModel:            "codex-auto-review",
		SettingKeyOpenAIWindowWarmupWorkerConcurrency:     "2",
		SettingKeyOpenAIWindowWarmupGlobalQPS:             "0.1",
		SettingKeyOpenAIWindowWarmupBatchSize:             "12",
		SettingKeyOpenAIWindowWarmupScanSeconds:           "15",
		SettingKeyOpenAIWindowWarmupRequestTimeoutSeconds: "40",
		SettingKeyOpenAIWindowWarmupLeaseSeconds:          "100",
		SettingKeyOpenAIWindowWarmupResetGraceSeconds:     "0",
	}}
	settingService := NewSettingService(repo, &config.Config{})

	options := ProvideOpenAIWindowWarmupOptions(settingService)
	require.Equal(t, 2, options.WorkerConcurrency)
	require.Equal(t, 0.1, options.GlobalQPS)
	require.Equal(t, 12, options.BatchSize)
	require.Equal(t, 15*time.Second, options.ScanInterval)
	require.Equal(t, 40*time.Second, options.RequestTimeout)
	require.Equal(t, 100*time.Second, options.LeaseDuration)
	require.Zero(t, options.ResetGrace)
	require.True(t, options.ResetGraceSet)
	require.Equal(t, "codex-auto-review", options.Model)

	enabled, err := options.KillSwitch.Enabled(context.Background())
	require.NoError(t, err)
	require.True(t, enabled)
	allowlist, err := options.Allowlist.AccountIDs(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int64{42, 7}, allowlist)

	require.NoError(t, repo.Set(context.Background(), SettingKeyOpenAIWindowWarmupEnabled, "false"))
	require.NoError(t, repo.Set(context.Background(), SettingKeyOpenAIWindowWarmupAllowlist, `[9]`))
	enabled, err = options.KillSwitch.Enabled(context.Background())
	require.NoError(t, err)
	require.False(t, enabled)
	allowlist, err = options.Allowlist.AccountIDs(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int64{9}, allowlist)
}

func TestProvideOpenAIWindowWarmupOptionsFailsClosedWithoutSettings(t *testing.T) {
	options := ProvideOpenAIWindowWarmupOptions(nil)
	require.True(t, options.ResetGraceSet)
	enabled, err := options.KillSwitch.Enabled(context.Background())
	require.Error(t, err)
	require.False(t, enabled)
	allowlist, err := options.Allowlist.AccountIDs(context.Background())
	require.Error(t, err)
	require.Empty(t, allowlist)
}

func TestProvideOpenAIWindowWarmupOptionsDistinguishesEmptyFromInvalidAllowlist(t *testing.T) {
	repo := &panelRateLimitSettingRepo{values: map[string]string{
		SettingKeyOpenAIWindowWarmupAllowlist: `[]`,
	}}
	options := ProvideOpenAIWindowWarmupOptions(NewSettingService(repo, &config.Config{}))

	allowlist, err := options.Allowlist.AccountIDs(context.Background())
	require.NoError(t, err)
	require.Empty(t, allowlist, "a valid empty array selects the full eligible cohort")

	for _, invalid := range []string{`{`, `null`, `{}`, `"[]"`, `[-1]`, `[0]`} {
		require.NoError(t, repo.Set(context.Background(), SettingKeyOpenAIWindowWarmupAllowlist, invalid))
		allowlist, err = options.Allowlist.AccountIDs(context.Background())
		require.Error(t, err, invalid)
		require.Empty(t, allowlist, invalid)
	}

	repo.getValueErr = errors.New("settings unavailable")
	allowlist, err = options.Allowlist.AccountIDs(context.Background())
	require.Error(t, err)
	require.Empty(t, allowlist)
}

func TestValidateOpenAIWindowWarmupSettingsRejectsNonPositiveAllowlistIDs(t *testing.T) {
	for _, allowlist := range [][]int64{{0}, {-1}, {42, 0}} {
		settings := &SystemSettings{}
		applyDefaultOpenAIWindowWarmupSettings(settings)
		settings.OpenAIWindowWarmupAllowlist = allowlist

		require.Error(t, validateOpenAIWindowWarmupSettings(settings), allowlist)
	}

	settings := &SystemSettings{}
	applyDefaultOpenAIWindowWarmupSettings(settings)
	settings.OpenAIWindowWarmupAllowlist = []int64{42, 7, 42}
	require.NoError(t, validateOpenAIWindowWarmupSettings(settings))
	require.Equal(t, []int64{42, 7}, settings.OpenAIWindowWarmupAllowlist)
}
