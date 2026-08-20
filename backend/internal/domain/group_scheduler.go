package domain

const (
	GroupSchedulerTypeInherit  = "inherit"
	GroupSchedulerTypeBasic    = "basic"
	GroupSchedulerTypeAdvanced = "advanced"
)

// AdvancedSchedulerOverrides contains sparse group-level overrides for the
// global OpenAI advanced scheduler configuration. Nil fields inherit the
// global value; non-nil false and zero values are intentional overrides.
type AdvancedSchedulerOverrides struct {
	StickyWeightedEnabled       *bool    `json:"sticky_weighted_enabled,omitempty"`
	SubscriptionPriorityEnabled *bool    `json:"subscription_priority_enabled,omitempty"`
	LBTopK                      *int     `json:"lb_top_k,omitempty"`
	WeightPriority              *float64 `json:"weight_priority,omitempty"`
	WeightLoad                  *float64 `json:"weight_load,omitempty"`
	WeightQueue                 *float64 `json:"weight_queue,omitempty"`
	WeightErrorRate             *float64 `json:"weight_error_rate,omitempty"`
	WeightTTFT                  *float64 `json:"weight_ttft,omitempty"`
	WeightReset                 *float64 `json:"weight_reset,omitempty"`
	WeightQuotaHeadroom         *float64 `json:"weight_quota_headroom,omitempty"`
	WeightUpstreamCost          *float64 `json:"weight_upstream_cost,omitempty"`
	WeightPreviousResponse      *float64 `json:"weight_previous_response,omitempty"`
	WeightSessionSticky         *float64 `json:"weight_session_sticky,omitempty"`
}

func cloneSchedulerOverridePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

// Clone returns a deep copy so cached or duplicated group configurations do
// not share mutable pointer fields.
func (o AdvancedSchedulerOverrides) Clone() AdvancedSchedulerOverrides {
	return AdvancedSchedulerOverrides{
		StickyWeightedEnabled:       cloneSchedulerOverridePointer(o.StickyWeightedEnabled),
		SubscriptionPriorityEnabled: cloneSchedulerOverridePointer(o.SubscriptionPriorityEnabled),
		LBTopK:                      cloneSchedulerOverridePointer(o.LBTopK),
		WeightPriority:              cloneSchedulerOverridePointer(o.WeightPriority),
		WeightLoad:                  cloneSchedulerOverridePointer(o.WeightLoad),
		WeightQueue:                 cloneSchedulerOverridePointer(o.WeightQueue),
		WeightErrorRate:             cloneSchedulerOverridePointer(o.WeightErrorRate),
		WeightTTFT:                  cloneSchedulerOverridePointer(o.WeightTTFT),
		WeightReset:                 cloneSchedulerOverridePointer(o.WeightReset),
		WeightQuotaHeadroom:         cloneSchedulerOverridePointer(o.WeightQuotaHeadroom),
		WeightUpstreamCost:          cloneSchedulerOverridePointer(o.WeightUpstreamCost),
		WeightPreviousResponse:      cloneSchedulerOverridePointer(o.WeightPreviousResponse),
		WeightSessionSticky:         cloneSchedulerOverridePointer(o.WeightSessionSticky),
	}
}
