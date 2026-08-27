package repository

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func schedulerRepositoryTestPointer[T any](value T) *T { return &value }

func TestAPIKeyRepositoryGetByKeyForAuthPreservesSchedulerProjectionSQLite(t *testing.T) {
	repo, client := newAPIKeyRepoSQLite(t)
	ctx := context.Background()
	user := mustCreateAPIKeyRepoUser(t, ctx, client, "getbykey-auth-scheduler-unit@test.com")
	overrides := service.AdvancedSchedulerOverrides{
		StickyWeightedEnabled: schedulerRepositoryTestPointer(false),
		WeightLoad:            schedulerRepositoryTestPointer(0.0),
		LBTopK:                schedulerRepositoryTestPointer(6),
	}

	group, err := client.Group.Create().
		SetName("g-auth-scheduler-unit").
		SetPlatform(service.PlatformOpenAI).
		SetStatus(service.StatusActive).
		SetSubscriptionType(service.SubscriptionTypeStandard).
		SetRateMultiplier(1).
		SetSchedulerType(service.GroupSchedulerTypeAdvanced).
		SetAdvancedSchedulerOverrides(overrides).
		SetTrafficDirectorMode(domain.TrafficDirectorModeShadow).
		SetTrafficDirectorVersion(4).
		Save(ctx)
	require.NoError(t, err)

	key := &service.APIKey{
		UserID:  user.ID,
		Key:     "sk-getbykey-auth-scheduler-unit",
		Name:    "Scheduler Key Unit",
		GroupID: &group.ID,
		Status:  service.StatusActive,
	}
	require.NoError(t, repo.Create(ctx, key))

	got, err := repo.GetByKeyForAuth(ctx, key.Key)
	require.NoError(t, err)
	require.NotNil(t, got.Group)
	require.Equal(t, service.GroupSchedulerTypeAdvanced, got.Group.SchedulerType)
	require.NotNil(t, got.Group.AdvancedSchedulerOverrides.StickyWeightedEnabled)
	require.False(t, *got.Group.AdvancedSchedulerOverrides.StickyWeightedEnabled)
	require.NotNil(t, got.Group.AdvancedSchedulerOverrides.WeightLoad)
	require.Zero(t, *got.Group.AdvancedSchedulerOverrides.WeightLoad)
	require.NotNil(t, got.Group.AdvancedSchedulerOverrides.LBTopK)
	require.Equal(t, 6, *got.Group.AdvancedSchedulerOverrides.LBTopK)
	require.Equal(t, domain.TrafficDirectorModeShadow, got.Group.TrafficDirectorMode)
	require.Equal(t, int64(4), got.Group.TrafficDirectorVersion)
}
