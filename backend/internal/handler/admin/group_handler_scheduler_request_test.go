package admin

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpdateGroupRequestPreservesSparseSchedulerFalseAndZero(t *testing.T) {
	var req UpdateGroupRequest
	require.NoError(t, json.Unmarshal([]byte(`{
		"scheduler_type": "advanced",
		"advanced_scheduler_overrides": {
			"sticky_weighted_enabled": false,
			"subscription_priority_enabled": false,
			"weight_priority": 0,
			"weight_load": 0
		}
	}`), &req))

	require.NotNil(t, req.SchedulerType)
	require.Equal(t, "advanced", *req.SchedulerType)
	require.NotNil(t, req.AdvancedSchedulerOverrides)
	require.NotNil(t, req.AdvancedSchedulerOverrides.StickyWeightedEnabled)
	require.False(t, *req.AdvancedSchedulerOverrides.StickyWeightedEnabled)
	require.NotNil(t, req.AdvancedSchedulerOverrides.SubscriptionPriorityEnabled)
	require.False(t, *req.AdvancedSchedulerOverrides.SubscriptionPriorityEnabled)
	require.NotNil(t, req.AdvancedSchedulerOverrides.WeightPriority)
	require.Zero(t, *req.AdvancedSchedulerOverrides.WeightPriority)
	require.NotNil(t, req.AdvancedSchedulerOverrides.WeightLoad)
	require.Zero(t, *req.AdvancedSchedulerOverrides.WeightLoad)
	require.Nil(t, req.AdvancedSchedulerOverrides.WeightQueue)
}
