package dto

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func schedulerDTOTestPointer[T any](value T) *T { return &value }

func TestGroupSchedulerConfigIsAdminOnlyAndPreservesSparseValues(t *testing.T) {
	group := &service.Group{
		ID:            4,
		SchedulerType: service.GroupSchedulerTypeAdvanced,
		AdvancedSchedulerOverrides: service.AdvancedSchedulerOverrides{
			StickyWeightedEnabled: schedulerDTOTestPointer(false),
			WeightLoad:            schedulerDTOTestPointer(0.0),
		},
	}

	publicFields := marshalToMap(t, GroupFromService(group))
	require.NotContains(t, publicFields, "scheduler_type")
	require.NotContains(t, publicFields, "advanced_scheduler_overrides")

	admin := GroupFromServiceAdmin(group)
	require.Equal(t, service.GroupSchedulerTypeAdvanced, admin.SchedulerType)
	adminFields := marshalToMap(t, admin)
	require.Equal(t, service.GroupSchedulerTypeAdvanced, adminFields["scheduler_type"])
	overrides, ok := adminFields["advanced_scheduler_overrides"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, false, overrides["sticky_weighted_enabled"])
	require.Equal(t, float64(0), overrides["weight_load"])
	require.NotContains(t, overrides, "weight_priority")
}
