package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type accountEgressSettingRepoStub struct {
	SettingRepository
	value string
	err   error
}

type mutableAccountEgressSettingRepoStub struct {
	SettingRepository
	value string
	err   error
}

func (s *mutableAccountEgressSettingRepoStub) GetValue(context.Context, string) (string, error) {
	return s.value, s.err
}

func (s accountEgressSettingRepoStub) GetValue(context.Context, string) (string, error) {
	return s.value, s.err
}

func TestAccountEgressPoolRolloutModeFailClosed(t *testing.T) {
	tests := []struct {
		name  string
		value string
		err   error
		want  AccountEgressPoolRolloutMode
	}{
		{name: "enforce", value: " enforce ", want: AccountEgressPoolRolloutEnforce},
		{name: "shadow", value: "SHADOW", want: AccountEgressPoolRolloutShadow},
		{name: "invalid", value: "enabled", want: AccountEgressPoolRolloutOff},
		{name: "read error", err: errors.New("database unavailable"), want: AccountEgressPoolRolloutOff},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := NewSettingService(accountEgressSettingRepoStub{value: tt.value, err: tt.err}, nil)
			require.Equal(t, tt.want, svc.GetAccountEgressPoolRolloutMode(t.Context()))
		})
	}
}

func TestAccountEgressPoolRolloutModeUsesStaleSuccessfulValueOnReadError(t *testing.T) {
	repo := &mutableAccountEgressSettingRepoStub{value: string(AccountEgressPoolRolloutEnforce)}
	svc := NewSettingService(repo, nil)
	require.Equal(t, AccountEgressPoolRolloutEnforce, svc.GetAccountEgressPoolRolloutMode(t.Context()))

	repo.err = errors.New("database unavailable")
	svc.accountEgressRolloutCache.Store(&cachedAccountEgressRolloutMode{
		mode:              AccountEgressPoolRolloutEnforce,
		expiresAt:         time.Now().Add(-time.Second),
		hasSuccessfulRead: true,
	})
	require.Equal(t, AccountEgressPoolRolloutEnforce, svc.GetAccountEgressPoolRolloutMode(t.Context()))

	cached := svc.accountEgressRolloutCache.Load().(*cachedAccountEgressRolloutMode)
	require.True(t, cached.hasSuccessfulRead)
	require.True(t, time.Now().Before(cached.expiresAt))
}

func TestAccountEgressPoolRolloutModeExplicitOffOverridesStaleValue(t *testing.T) {
	tests := []struct {
		name  string
		value string
		err   error
	}{
		{name: "not found", err: ErrSettingNotFound},
		{name: "invalid", value: "enabled"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := NewSettingService(accountEgressSettingRepoStub{value: tt.value, err: tt.err}, nil)
			svc.accountEgressRolloutCache.Store(&cachedAccountEgressRolloutMode{
				mode:              AccountEgressPoolRolloutEnforce,
				expiresAt:         time.Now().Add(-time.Second),
				hasSuccessfulRead: true,
			})
			require.Equal(t, AccountEgressPoolRolloutOff, svc.GetAccountEgressPoolRolloutMode(t.Context()))
		})
	}
}
