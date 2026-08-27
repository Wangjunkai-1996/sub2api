package domain

const (
	TrafficDirectorSchemaVersion = 1

	TrafficDirectorHealthModeOff     = "off"
	TrafficDirectorHealthModeObserve = "observe"
	TrafficDirectorHealthModeEnforce = "enforce"

	TrafficDirectorModeLegacy   = "legacy"
	TrafficDirectorModeShadow   = "shadow"
	TrafficDirectorModeEnforced = "enforced"

	TrafficDirectorMaxPools              = 32
	TrafficDirectorMaxAccountReferences  = 4096
	TrafficDirectorMaxCanonicalJSONBytes = 64 * 1024
	TrafficDirectorWeightTotalBPS        = 10000
)

// TrafficDirectorSpec is the versioned, group-scoped traffic routing policy.
type TrafficDirectorSpec struct {
	SchemaVersion int                   `json:"schema_version"`
	HealthMode    string                `json:"health_mode"`
	Pools         []TrafficDirectorPool `json:"pools"`
}

// TrafficDirectorPool defines a weighted home pool and its explicit overflow path.
type TrafficDirectorPool struct {
	Key             string  `json:"key"`
	WeightBPS       int     `json:"weight_bps"`
	AccountIDs      []int64 `json:"account_ids"`
	MinAvailable    int     `json:"min_available"`
	FallbackPoolKey string  `json:"fallback_pool_key,omitempty"`
}
