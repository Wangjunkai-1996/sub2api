package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type cyberCooldownSettingsRepo struct {
	SettingRepository
	values         map[string]string
	getMultipleErr error
}

func (r *cyberCooldownSettingsRepo) GetValue(_ context.Context, key string) (string, error) {
	value, ok := r.values[key]
	if !ok {
		return "", ErrSettingNotFound
	}
	return value, nil
}

func (r *cyberCooldownSettingsRepo) GetMultiple(_ context.Context, keys []string) (map[string]string, error) {
	if r.getMultipleErr != nil {
		return nil, r.getMultipleErr
	}
	result := make(map[string]string, len(keys))
	for _, key := range keys {
		if value, ok := r.values[key]; ok {
			result[key] = value
		}
	}
	return result, nil
}

func (r *cyberCooldownSettingsRepo) SetMultiple(_ context.Context, settings map[string]string) error {
	if r.values == nil {
		r.values = make(map[string]string)
	}
	for key, value := range settings {
		r.values[key] = value
	}
	return nil
}

func (r *cyberCooldownSettingsRepo) GetAll(context.Context) (map[string]string, error) {
	result := make(map[string]string, len(r.values))
	for key, value := range r.values {
		result[key] = value
	}
	return result, nil
}

func TestOpenAICyberAccountCooldownSettingsDefaultsDisabled(t *testing.T) {
	svc := NewSettingService(&cyberCooldownSettingsRepo{values: map[string]string{}}, &config.Config{})

	settings, err := svc.GetAllSettings(context.Background())
	require.NoError(t, err)
	require.False(t, settings.OpenAICyberAccountCooldownEnabled)
	require.Equal(t, 86400, settings.OpenAICyberAccountCooldownWindowSeconds)
	require.Equal(t, 3600, settings.OpenAICyberAccountCooldownFirstSeconds)
	require.Equal(t, 86400, settings.OpenAICyberAccountCooldownEscalatedSeconds)
	require.Equal(t, []int64{12}, settings.OpenAICyberAccountCooldownGroupIDs)
	policy := svc.GetOpenAICyberAccountCooldownRuntime(context.Background())
	require.False(t, policy.Enabled())
	require.Equal(t, []int64{12}, policy.GroupIDs())
}

func TestOpenAICyberAccountCooldownRuntimeFirstLoadFailureUsesConservativePolicy(t *testing.T) {
	svc := NewSettingService(&cyberCooldownSettingsRepo{
		values:         map[string]string{},
		getMultipleErr: errors.New("settings unavailable"),
	}, &config.Config{})

	policy := svc.GetOpenAICyberAccountCooldownRuntime(context.Background())
	require.True(t, policy.Enabled())
	require.Equal(t, 24*time.Hour, policy.Window())
	require.Equal(t, 24*time.Hour, policy.FirstDuration())
	require.Equal(t, 24*time.Hour, policy.EscalatedDuration())
	require.Equal(t, []int64{12}, policy.GroupIDs())
}

func TestOpenAICyberAccountCooldownRuntimeFailureRetainsStalePolicy(t *testing.T) {
	repo := &cyberCooldownSettingsRepo{values: map[string]string{
		SettingKeyOpenAICyberAccountCooldownEnabled:          "false",
		SettingKeyOpenAICyberAccountCooldownWindowSeconds:    "7200",
		SettingKeyOpenAICyberAccountCooldownFirstSeconds:     "600",
		SettingKeyOpenAICyberAccountCooldownEscalatedSeconds: "3600",
		SettingKeyOpenAICyberAccountCooldownGroupIDs:         "[12,13]",
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
	require.Equal(t, []int64{12, 13}, policy.GroupIDs())
}

func TestOpenAICyberAccountCooldownSettingsPersistAndRefreshRuntime(t *testing.T) {
	repo := &cyberCooldownSettingsRepo{values: map[string]string{}}
	svc := NewSettingService(repo, &config.Config{})

	require.NoError(t, svc.UpdateSettings(context.Background(), &SystemSettings{
		OpenAICyberAccountCooldownEnabled:          true,
		OpenAICyberAccountCooldownWindowSeconds:    7200,
		OpenAICyberAccountCooldownFirstSeconds:     600,
		OpenAICyberAccountCooldownEscalatedSeconds: 3600,
		OpenAICyberAccountCooldownGroupIDs:         []int64{13, 12, 0, 13},
	}))
	require.Equal(t, "true", repo.values[SettingKeyOpenAICyberAccountCooldownEnabled])
	require.Equal(t, "7200", repo.values[SettingKeyOpenAICyberAccountCooldownWindowSeconds])
	require.Equal(t, "600", repo.values[SettingKeyOpenAICyberAccountCooldownFirstSeconds])
	require.Equal(t, "3600", repo.values[SettingKeyOpenAICyberAccountCooldownEscalatedSeconds])
	require.Equal(t, "[12,13]", repo.values[SettingKeyOpenAICyberAccountCooldownGroupIDs])

	policy := svc.GetOpenAICyberAccountCooldownRuntime(context.Background())
	require.True(t, policy.Enabled())
	require.Equal(t, 2*time.Hour, policy.Window())
	require.Equal(t, 10*time.Minute, policy.FirstDuration())
	require.Equal(t, time.Hour, policy.EscalatedDuration())
	require.Equal(t, []int64{12, 13}, policy.GroupIDs())
}

func TestOpenAICyberAccountCooldownSettingsRejectInvalidDurations(t *testing.T) {
	repo := &cyberCooldownSettingsRepo{values: map[string]string{}}
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
