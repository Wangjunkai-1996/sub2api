package domain

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func schedulerDomainTestPointer[T any](value T) *T { return &value }

func TestAdvancedSchedulerOverridesJSONPreservesExplicitFalseAndZero(t *testing.T) {
	overrides := AdvancedSchedulerOverrides{
		StickyWeightedEnabled: schedulerDomainTestPointer(false),
		WeightLoad:            schedulerDomainTestPointer(0.0),
		LBTopK:                schedulerDomainTestPointer(7),
	}

	payload, err := json.Marshal(overrides)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"sticky_weighted_enabled": false,
		"weight_load": 0,
		"lb_top_k": 7
	}`, string(payload))

	var restored AdvancedSchedulerOverrides
	require.NoError(t, json.Unmarshal(payload, &restored))
	require.NotNil(t, restored.StickyWeightedEnabled)
	require.False(t, *restored.StickyWeightedEnabled)
	require.NotNil(t, restored.WeightLoad)
	require.Zero(t, *restored.WeightLoad)
	require.Nil(t, restored.WeightPriority)

	cloned := restored.Clone()
	require.NotSame(t, restored.StickyWeightedEnabled, cloned.StickyWeightedEnabled)
	require.NotSame(t, restored.WeightLoad, cloned.WeightLoad)
}
