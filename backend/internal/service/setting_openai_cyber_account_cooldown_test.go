package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestOpenAICyberAccountCooldownSettingsDefaultsDisabled(t *testing.T) {
	svc := NewSettingService(&cyberSettingsRepo{values: map[string]string{}}, &config.Config{})

	settings, err := svc.GetAllSettings(context.Background())
	require.NoError(t, err)
	require.False(t, settings.OpenAICyberAccountCooldownEnabled)
	require.Equal(t, 86400, settings.OpenAICyberAccountCooldownWindowSeconds)
	require.Equal(t, 3600, settings.OpenAICyberAccountCooldownFirstSeconds)
	require.Equal(t, 86400, settings.OpenAICyberAccountCooldownEscalatedSeconds)
	require.False(t, svc.GetOpenAICyberAccountCooldownRuntime(context.Background()).Enabled())
}

func TestOpenAICyberAccountCooldownRuntimeFirstLoadFailureUsesConservativePolicy(t *testing.T) {
	svc := NewSettingService(&cyberSettingsRepo{
		values:         map[string]string{},
		getMultipleErr: errors.New("settings unavailable"),
	}, &config.Config{})

	policy := svc.GetOpenAICyberAccountCooldownRuntime(context.Background())
	require.True(t, policy.Enabled())
	require.Equal(t, 24*time.Hour, policy.Window())
	require.Equal(t, 24*time.Hour, policy.FirstDuration())
	require.Equal(t, 24*time.Hour, policy.EscalatedDuration())
}

func TestOpenAICyberAccountCooldownRuntimeFailureRetainsStalePolicy(t *testing.T) {
	repo := &cyberSettingsRepo{values: map[string]string{
		SettingKeyOpenAICyberAccountCooldownEnabled:          "false",
		SettingKeyOpenAICyberAccountCooldownWindowSeconds:    "7200",
		SettingKeyOpenAICyberAccountCooldownFirstSeconds:     "600",
		SettingKeyOpenAICyberAccountCooldownEscalatedSeconds: "3600",
	}}
	svc := NewSettingService(repo, &config.Config{})
	initial := svc.GetOpenAICyberAccountCooldownRuntime(context.Background())
	require.False(t, initial.Enabled())

	repo.getMultipleErr = errors.New("settings unavailable")
	svc.openAICyberAccountCooldownRuntimeCache.Store(&cachedOpenAICyberAccountCooldownRuntime{
		policy: initial, expiresAt: time.Now().Add(-time.Second).UnixNano(),
	})
	policy := svc.GetOpenAICyberAccountCooldownRuntime(context.Background())
	require.False(t, policy.Enabled())
	require.Equal(t, 2*time.Hour, policy.Window())
	require.Equal(t, 10*time.Minute, policy.FirstDuration())
	require.Equal(t, time.Hour, policy.EscalatedDuration())
}

func TestOpenAICyberAccountCooldownSettingsPersistAndRefreshRuntime(t *testing.T) {
	repo := &cyberSettingsRepo{values: map[string]string{}}
	svc := NewSettingService(repo, &config.Config{})

	require.NoError(t, svc.UpdateSettings(context.Background(), &SystemSettings{
		OpenAICyberAccountCooldownEnabled:          true,
		OpenAICyberAccountCooldownWindowSeconds:    7200,
		OpenAICyberAccountCooldownFirstSeconds:     600,
		OpenAICyberAccountCooldownEscalatedSeconds: 3600,
	}))
	require.Equal(t, "true", repo.values[SettingKeyOpenAICyberAccountCooldownEnabled])
	require.Equal(t, "7200", repo.values[SettingKeyOpenAICyberAccountCooldownWindowSeconds])
	require.Equal(t, "600", repo.values[SettingKeyOpenAICyberAccountCooldownFirstSeconds])
	require.Equal(t, "3600", repo.values[SettingKeyOpenAICyberAccountCooldownEscalatedSeconds])

	policy := svc.GetOpenAICyberAccountCooldownRuntime(context.Background())
	require.True(t, policy.Enabled())
	require.Equal(t, 2*time.Hour, policy.Window())
	require.Equal(t, 10*time.Minute, policy.FirstDuration())
	require.Equal(t, time.Hour, policy.EscalatedDuration())
}

func TestOpenAICyberAccountCooldownSettingsRejectInvalidDurations(t *testing.T) {
	repo := &cyberSettingsRepo{values: map[string]string{}}
	svc := NewSettingService(repo, &config.Config{})

	err := svc.UpdateSettings(context.Background(), &SystemSettings{
		OpenAICyberAccountCooldownWindowSeconds:    59,
		OpenAICyberAccountCooldownFirstSeconds:     600,
		OpenAICyberAccountCooldownEscalatedSeconds: 3600,
	})
	require.Error(t, err)

	err = svc.UpdateSettings(context.Background(), &SystemSettings{
		OpenAICyberAccountCooldownWindowSeconds:    3600,
		OpenAICyberAccountCooldownFirstSeconds:     3600,
		OpenAICyberAccountCooldownEscalatedSeconds: 600,
	})
	require.Error(t, err)
}
