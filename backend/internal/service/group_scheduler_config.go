package service

import (
	"fmt"
	"math"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/domain"
)

const (
	GroupSchedulerTypeInherit  = domain.GroupSchedulerTypeInherit
	GroupSchedulerTypeBasic    = domain.GroupSchedulerTypeBasic
	GroupSchedulerTypeAdvanced = domain.GroupSchedulerTypeAdvanced
)

type AdvancedSchedulerOverrides = domain.AdvancedSchedulerOverrides

// NormalizeGroupSchedulerType applies the legacy-compatible inherit default.
func NormalizeGroupSchedulerType(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return GroupSchedulerTypeInherit, nil
	}
	switch value {
	case GroupSchedulerTypeInherit, GroupSchedulerTypeBasic, GroupSchedulerTypeAdvanced:
		return value, nil
	default:
		return "", fmt.Errorf("scheduler_type must be one of inherit, basic, advanced")
	}
}

// ValidateAdvancedSchedulerOverrides validates only fields that are present.
// Weight zero is a valid explicit override; lb_top_k must remain positive.
func ValidateAdvancedSchedulerOverrides(overrides AdvancedSchedulerOverrides) error {
	if overrides.LBTopK != nil && *overrides.LBTopK <= 0 {
		return fmt.Errorf("advanced_scheduler_overrides.lb_top_k must be > 0")
	}
	weights := []struct {
		name  string
		value *float64
	}{
		{"weight_priority", overrides.WeightPriority},
		{"weight_load", overrides.WeightLoad},
		{"weight_queue", overrides.WeightQueue},
		{"weight_error_rate", overrides.WeightErrorRate},
		{"weight_ttft", overrides.WeightTTFT},
		{"weight_reset", overrides.WeightReset},
		{"weight_quota_headroom", overrides.WeightQuotaHeadroom},
		{"weight_upstream_cost", overrides.WeightUpstreamCost},
		{"weight_previous_response", overrides.WeightPreviousResponse},
		{"weight_session_sticky", overrides.WeightSessionSticky},
	}
	for _, weight := range weights {
		if weight.value == nil {
			continue
		}
		if *weight.value < 0 || math.IsNaN(*weight.value) || math.IsInf(*weight.value, 0) {
			return fmt.Errorf("advanced_scheduler_overrides.%s must be non-negative and finite", weight.name)
		}
	}
	return nil
}
