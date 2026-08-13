package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type cyberSettingsRepo struct {
	values         map[string]string
	getMultipleErr error
}

func (r *cyberSettingsRepo) Get(context.Context, string) (*Setting, error) {
	panic("unexpected Get call")
}

func (r *cyberSettingsRepo) GetValue(_ context.Context, key string) (string, error) {
	value, ok := r.values[key]
	if !ok {
		return "", ErrSettingNotFound
	}
	return value, nil
}

func (r *cyberSettingsRepo) Set(context.Context, string, string) error {
	panic("unexpected Set call")
}

func (r *cyberSettingsRepo) GetMultiple(_ context.Context, keys []string) (map[string]string, error) {
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

func (r *cyberSettingsRepo) SetMultiple(_ context.Context, settings map[string]string) error {
	if r.values == nil {
		r.values = make(map[string]string)
	}
	for key, value := range settings {
		r.values[key] = value
	}
	return nil
}

func (r *cyberSettingsRepo) GetAll(context.Context) (map[string]string, error) {
	result := make(map[string]string, len(r.values))
	for key, value := range r.values {
		result[key] = value
	}
	return result, nil
}

func (r *cyberSettingsRepo) Delete(context.Context, string) error {
	panic("unexpected Delete call")
}

func TestCyberSessionBlockSettings_MissingScopeDefaultsToAllGroups(t *testing.T) {
	svc := NewSettingService(&cyberSettingsRepo{values: map[string]string{}}, &config.Config{})

	settings, err := svc.GetAllSettings(context.Background())
	require.NoError(t, err)
	require.True(t, settings.CyberSessionBlockAllGroups)
	require.Empty(t, settings.CyberSessionBlockGroupIDs)

	policy := svc.GetCyberSessionBlockRuntime(context.Background())
	require.False(t, policy.Enabled())
	require.True(t, policy.AllGroups())
	require.True(t, policy.IncludesGroup(nil))
}

func TestCyberSessionBlockSettings_LegacyEnabledPolicyCoversEveryGroup(t *testing.T) {
	repo := &cyberSettingsRepo{values: map[string]string{
		SettingKeyCyberSessionBlockEnabled:    "true",
		SettingKeyCyberSessionBlockTTLSeconds: "60",
	}}
	svc := NewSettingService(repo, &config.Config{})

	policy := svc.GetCyberSessionBlockRuntime(context.Background())
	group12 := int64(12)
	group13 := int64(13)
	require.True(t, policy.Enabled())
	require.Equal(t, time.Minute, policy.TTL())
	require.True(t, policy.AllGroups())
	require.True(t, policy.IncludesGroup(&group12))
	require.True(t, policy.IncludesGroup(&group13))
	require.True(t, policy.IncludesGroup(nil))
}

func TestCyberSessionBlockSettings_NormalizesAndPersistsSelectedGroups(t *testing.T) {
	repo := &cyberSettingsRepo{values: map[string]string{}}
	svc := NewSettingService(repo, &config.Config{})

	err := svc.UpdateSettings(context.Background(), &SystemSettings{
		CyberSessionBlockEnabled:    true,
		CyberSessionBlockTTLSeconds: 90,
		CyberSessionBlockAllGroups:  false,
		CyberSessionBlockGroupIDs:   []int64{13, 0, 12, 13, -5},
	})
	require.NoError(t, err)
	require.Equal(t, "false", repo.values[SettingKeyCyberSessionBlockAllGroups])
	require.Equal(t, `[12,13]`, repo.values[SettingKeyCyberSessionBlockGroupIDs])

	settings, err := svc.GetAllSettings(context.Background())
	require.NoError(t, err)
	require.False(t, settings.CyberSessionBlockAllGroups)
	require.Equal(t, []int64{12, 13}, settings.CyberSessionBlockGroupIDs)

	group12 := int64(12)
	group99 := int64(99)
	policy := svc.GetCyberSessionBlockRuntime(context.Background())
	require.True(t, policy.Enabled())
	require.Equal(t, 90*time.Second, policy.TTL())
	require.True(t, policy.IncludesGroup(&group12))
	require.False(t, policy.IncludesGroup(&group99))
	require.False(t, policy.IncludesGroup(nil))

	groupIDs := policy.GroupIDs()
	groupIDs[0] = 99
	require.Equal(t, []int64{12, 13}, policy.GroupIDs(), "policy must not expose its cached group slice")
}

func TestCyberSessionBlockSettings_UpdateRefreshesWarmRuntimePolicy(t *testing.T) {
	repo := &cyberSettingsRepo{values: map[string]string{
		SettingKeyCyberSessionBlockEnabled:    "true",
		SettingKeyCyberSessionBlockTTLSeconds: "60",
		SettingKeyCyberSessionBlockAllGroups:  "true",
		SettingKeyCyberSessionBlockGroupIDs:   `[]`,
	}}
	svc := NewSettingService(repo, &config.Config{})
	group12 := int64(12)
	group13 := int64(13)

	before := svc.GetCyberSessionBlockRuntime(context.Background())
	require.True(t, before.IncludesGroup(&group13))

	settings, err := svc.GetAllSettings(context.Background())
	require.NoError(t, err)
	settings.CyberSessionBlockAllGroups = false
	settings.CyberSessionBlockGroupIDs = []int64{12}
	require.NoError(t, svc.UpdateSettings(context.Background(), settings))

	after := svc.GetCyberSessionBlockRuntime(context.Background())
	require.True(t, after.Enabled())
	require.True(t, after.IncludesGroup(&group12))
	require.False(t, after.IncludesGroup(&group13), "settings writes must replace the warm policy immediately")
}

func TestCyberSessionBlockSettings_InvalidOrUnavailablePolicyFailsOpen(t *testing.T) {
	tests := []struct {
		name   string
		values map[string]string
		err    error
	}{
		{
			name: "invalid all groups",
			values: map[string]string{
				SettingKeyCyberSessionBlockEnabled:   "true",
				SettingKeyCyberSessionBlockAllGroups: "sometimes",
			},
		},
		{
			name: "invalid group ids",
			values: map[string]string{
				SettingKeyCyberSessionBlockEnabled:   "true",
				SettingKeyCyberSessionBlockAllGroups: "false",
				SettingKeyCyberSessionBlockGroupIDs:  `[12.5]`,
			},
		},
		{
			name: "selected scope is empty",
			values: map[string]string{
				SettingKeyCyberSessionBlockEnabled:   "true",
				SettingKeyCyberSessionBlockAllGroups: "false",
				SettingKeyCyberSessionBlockGroupIDs:  `[]`,
			},
		},
		{
			name:   "repository unavailable",
			values: map[string]string{},
			err:    errors.New("database unavailable"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := NewSettingService(&cyberSettingsRepo{values: tt.values, getMultipleErr: tt.err}, &config.Config{})
			policy := svc.GetCyberSessionBlockRuntime(context.Background())
			require.False(t, policy.Enabled())
			group12 := int64(12)
			require.False(t, policy.IncludesGroup(&group12))
			require.False(t, policy.IncludesGroup(nil))
		})
	}
}
