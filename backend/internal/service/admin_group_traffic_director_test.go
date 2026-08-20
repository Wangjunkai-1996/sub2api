package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/stretchr/testify/require"
)

type trafficDirectorPlatformGroupRepo struct {
	GroupRepository
	group   *Group
	updated *Group
}

func (r *trafficDirectorPlatformGroupRepo) GetByID(context.Context, int64) (*Group, error) {
	cloned := *r.group
	return &cloned, nil
}

func (r *trafficDirectorPlatformGroupRepo) Update(_ context.Context, group *Group) error {
	r.updated = group
	return nil
}

func TestAdminServicePlatformChangeUsesCurrentTrafficDirectorMode(t *testing.T) {
	tests := []struct {
		name        string
		mode        string
		version     int64
		wantError   bool
		wantUpdated bool
	}{
		{name: "enforced policy blocks platform change", mode: domain.TrafficDirectorModeEnforced, version: 4, wantError: true},
		{name: "legacy rollback version permits platform change", mode: domain.TrafficDirectorModeLegacy, version: 5, wantUpdated: true},
		{name: "pre-migration legacy state permits platform change", version: TrafficDirectorLegacyVersion, wantUpdated: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &trafficDirectorPlatformGroupRepo{group: &Group{
				ID:                     7,
				Name:                   "openai-group",
				Platform:               PlatformOpenAI,
				TrafficDirectorMode:    tt.mode,
				TrafficDirectorVersion: tt.version,
			}}
			svc := &adminServiceImpl{groupRepo: repo}

			group, err := svc.UpdateGroup(context.Background(), 7, &UpdateGroupInput{Platform: PlatformAnthropic})
			if tt.wantError {
				require.ErrorContains(t, err, "publish Traffic Director legacy")
				require.Nil(t, group)
				require.Nil(t, repo.updated)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, group)
			require.Equal(t, tt.wantUpdated, repo.updated != nil)
			require.Equal(t, PlatformAnthropic, group.Platform)
		})
	}
}
