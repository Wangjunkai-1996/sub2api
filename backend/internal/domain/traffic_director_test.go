package domain

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTrafficDirectorSpecJSONContract(t *testing.T) {
	spec := TrafficDirectorSpec{
		SchemaVersion: TrafficDirectorSchemaVersion,
		HealthMode:    TrafficDirectorHealthModeOff,
		Pools: []TrafficDirectorPool{
			{
				Key:          "primary",
				WeightBPS:    TrafficDirectorWeightTotalBPS,
				AccountIDs:   []int64{1},
				MinAvailable: 1,
			},
		},
	}

	encoded, err := json.Marshal(spec)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"schema_version": 1,
		"health_mode": "off",
		"pools": [{
			"key": "primary",
			"weight_bps": 10000,
			"account_ids": [1],
			"min_available": 1
		}]
	}`, string(encoded))
}
