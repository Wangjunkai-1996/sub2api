package service

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

type accountEgressSettingRepoStub struct {
	SettingRepository
	value string
	err   error
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
