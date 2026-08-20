package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/stretchr/testify/require"
)

func schedulerServiceTestPointer[T any](value T) *T { return &value }

func TestGroupSchedulerConfigValidation(t *testing.T) {
	normalized, err := NormalizeGroupSchedulerType("")
	require.NoError(t, err)
	require.Equal(t, GroupSchedulerTypeInherit, normalized)

	require.NoError(t, ValidateAdvancedSchedulerOverrides(AdvancedSchedulerOverrides{
		StickyWeightedEnabled: schedulerServiceTestPointer(false),
		WeightPriority:        schedulerServiceTestPointer(0.0),
	}))
	require.Error(t, ValidateAdvancedSchedulerOverrides(AdvancedSchedulerOverrides{
		LBTopK: schedulerServiceTestPointer(0),
	}))
	require.Error(t, ValidateAdvancedSchedulerOverrides(AdvancedSchedulerOverrides{
		WeightLoad: schedulerServiceTestPointer(-0.1),
	}))
	_, err = NormalizeGroupSchedulerType("random")
	require.Error(t, err)
}

func TestGroupSchedulerAuthSnapshotRoundTripPreservesSparseOverrides(t *testing.T) {
	groupID := int64(41)
	apiKey := &APIKey{
		ID:      7,
		UserID:  9,
		GroupID: &groupID,
		Status:  StatusActive,
		User:    &User{ID: 9, Status: StatusActive, Role: RoleUser},
		Group: &Group{
			ID:                     groupID,
			Name:                   "advanced",
			Platform:               PlatformOpenAI,
			Status:                 StatusActive,
			SchedulerType:          GroupSchedulerTypeAdvanced,
			TrafficDirectorMode:    domain.TrafficDirectorModeShadow,
			TrafficDirectorVersion: 7,
			AdvancedSchedulerOverrides: AdvancedSchedulerOverrides{
				StickyWeightedEnabled: schedulerServiceTestPointer(false),
				WeightLoad:            schedulerServiceTestPointer(0.0),
				LBTopK:                schedulerServiceTestPointer(5),
			},
		},
	}
	svc := &APIKeyService{}

	snapshot := svc.snapshotFromAPIKey(context.Background(), apiKey)
	require.NotNil(t, snapshot)
	require.Equal(t, 22, snapshot.Version)
	payload, err := json.Marshal(snapshot)
	require.NoError(t, err)
	require.NotContains(t, string(payload), "traffic_director_spec")
	var cached APIKeyAuthSnapshot
	require.NoError(t, json.Unmarshal(payload, &cached))

	restored := svc.snapshotToAPIKey("sk-scheduler", &cached)
	require.NotNil(t, restored.Group)
	require.Equal(t, GroupSchedulerTypeAdvanced, restored.Group.SchedulerType)
	require.NotNil(t, restored.Group.AdvancedSchedulerOverrides.StickyWeightedEnabled)
	require.False(t, *restored.Group.AdvancedSchedulerOverrides.StickyWeightedEnabled)
	require.NotNil(t, restored.Group.AdvancedSchedulerOverrides.WeightLoad)
	require.Zero(t, *restored.Group.AdvancedSchedulerOverrides.WeightLoad)
	require.Nil(t, restored.Group.AdvancedSchedulerOverrides.WeightPriority)
	require.Equal(t, domain.TrafficDirectorModeShadow, restored.Group.TrafficDirectorMode)
	require.Equal(t, int64(7), restored.Group.TrafficDirectorVersion)
}

func TestCloneGroupForDuplicateDeepCopiesSchedulerOverrides(t *testing.T) {
	source := &Group{
		Name:          "source",
		SchedulerType: GroupSchedulerTypeAdvanced,
		AdvancedSchedulerOverrides: AdvancedSchedulerOverrides{
			SubscriptionPriorityEnabled: schedulerServiceTestPointer(false),
			WeightQueue:                 schedulerServiceTestPointer(0.0),
		},
	}

	duplicate := cloneGroupForDuplicate(source, "operation")
	require.Equal(t, source.SchedulerType, duplicate.SchedulerType)
	require.NotSame(t, source.AdvancedSchedulerOverrides.SubscriptionPriorityEnabled, duplicate.AdvancedSchedulerOverrides.SubscriptionPriorityEnabled)
	require.NotSame(t, source.AdvancedSchedulerOverrides.WeightQueue, duplicate.AdvancedSchedulerOverrides.WeightQueue)
	*duplicate.AdvancedSchedulerOverrides.SubscriptionPriorityEnabled = true
	*duplicate.AdvancedSchedulerOverrides.WeightQueue = 3
	require.False(t, *source.AdvancedSchedulerOverrides.SubscriptionPriorityEnabled)
	require.Zero(t, *source.AdvancedSchedulerOverrides.WeightQueue)
}
