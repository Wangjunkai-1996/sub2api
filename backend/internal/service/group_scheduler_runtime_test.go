package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

func schedulerRuntimeTestPointer[T any](value T) *T { return &value }

func TestOpenAIAdvancedSchedulerEffectiveRuntimeSettings(t *testing.T) {
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)

	groupID := int64(8101)
	global := openAIAdvancedSchedulerRuntimeSettings{
		lowUpstreamRatePriorityEnabled: true,
		oauthSchedulingRateMultiplier:  0.75,
		enabled:                        true,
		stickyWeightedEnabled:          true,
		subscriptionPriorityEnabled:    true,
		lbTopKOverride:                 6,
		weightOverrides: map[string]float64{
			"priority": 2,
			"load":     3,
		},
	}

	tests := []struct {
		name         string
		platform     string
		group        *Group
		global       openAIAdvancedSchedulerRuntimeSettings
		wantEnabled  bool
		wantSticky   bool
		wantPriority bool
		wantTopK     int
		wantWeights  map[string]float64
	}{
		{
			name:     "inherit applies sparse overrides when global scheduler is advanced",
			platform: PlatformOpenAI,
			group: &Group{
				ID: groupID, Platform: PlatformOpenAI, Status: StatusActive, Hydrated: true,
				SchedulerType: GroupSchedulerTypeInherit,
				AdvancedSchedulerOverrides: AdvancedSchedulerOverrides{
					StickyWeightedEnabled: schedulerRuntimeTestPointer(false),
					LBTopK:                schedulerRuntimeTestPointer(2),
					WeightLoad:            schedulerRuntimeTestPointer(0.0),
				},
			},
			global: global, wantEnabled: true, wantSticky: false, wantPriority: true,
			wantTopK: 2, wantWeights: map[string]float64{"priority": 2, "load": 0},
		},
		{
			name:     "basic disables advanced scheduler",
			platform: PlatformOpenAI,
			group: &Group{
				ID: groupID, Platform: PlatformOpenAI, Status: StatusActive, Hydrated: true,
				SchedulerType: GroupSchedulerTypeBasic,
			},
			global: global, wantEnabled: false, wantSticky: true, wantPriority: true,
			wantTopK: 6, wantWeights: map[string]float64{"priority": 2, "load": 3},
		},
		{
			name:     "inherit ignores overrides when global scheduler is basic",
			platform: PlatformOpenAI,
			group: &Group{
				ID: groupID, Platform: PlatformOpenAI, Status: StatusActive, Hydrated: true,
				SchedulerType: GroupSchedulerTypeInherit,
				AdvancedSchedulerOverrides: AdvancedSchedulerOverrides{
					StickyWeightedEnabled: schedulerRuntimeTestPointer(true),
					LBTopK:                schedulerRuntimeTestPointer(2),
					WeightLoad:            schedulerRuntimeTestPointer(0.0),
				},
			},
			global: openAIAdvancedSchedulerRuntimeSettings{
				enabled:               false,
				lbTopKOverride:        6,
				weightOverrides:       map[string]float64{"priority": 2, "load": 3},
				stickyWeightedEnabled: false,
			},
			wantEnabled: false, wantSticky: false, wantPriority: false,
			wantTopK: 6, wantWeights: map[string]float64{"priority": 2, "load": 3},
		},
		{
			name:     "advanced enables scheduler and preserves explicit false and zero",
			platform: PlatformOpenAI,
			group: &Group{
				ID: groupID, Platform: PlatformOpenAI, Status: StatusActive, Hydrated: true,
				SchedulerType: GroupSchedulerTypeAdvanced,
				AdvancedSchedulerOverrides: AdvancedSchedulerOverrides{
					StickyWeightedEnabled:       schedulerRuntimeTestPointer(false),
					SubscriptionPriorityEnabled: schedulerRuntimeTestPointer(false),
					LBTopK:                      schedulerRuntimeTestPointer(2),
					WeightLoad:                  schedulerRuntimeTestPointer(0.0),
				},
			},
			global: openAIAdvancedSchedulerRuntimeSettings{
				enabled:                     false,
				stickyWeightedEnabled:       true,
				subscriptionPriorityEnabled: true,
				lbTopKOverride:              6,
				weightOverrides:             map[string]float64{"priority": 2, "load": 3},
			},
			wantEnabled: true, wantSticky: false, wantPriority: false,
			wantTopK: 2, wantWeights: map[string]float64{"priority": 2, "load": 0},
		},
		{
			name:     "non OpenAI request keeps global behavior",
			platform: PlatformGrok,
			group: &Group{
				ID: groupID, Platform: PlatformOpenAI, Status: StatusActive, Hydrated: true,
				SchedulerType: GroupSchedulerTypeBasic,
			},
			global: global, wantEnabled: true, wantSticky: true, wantPriority: true,
			wantTopK: 6, wantWeights: map[string]float64{"priority": 2, "load": 3},
		},
		{
			name:     "non OpenAI group keeps global behavior",
			platform: PlatformOpenAI,
			group: &Group{
				ID: groupID, Platform: PlatformGrok, Status: StatusActive, Hydrated: true,
				SchedulerType: GroupSchedulerTypeBasic,
			},
			global: global, wantEnabled: true, wantSticky: true, wantPriority: true,
			wantTopK: 6, wantWeights: map[string]float64{"priority": 2, "load": 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetOpenAIAdvancedSchedulerSettingCacheForTest()
			openAIAdvancedSchedulerSettingCache.Store(&cachedOpenAIAdvancedSchedulerSetting{
				lowUpstreamRatePriorityEnabled: tt.global.lowUpstreamRatePriorityEnabled,
				oauthSchedulingRateMultiplier:  tt.global.oauthSchedulingRateMultiplier,
				enabled:                        tt.global.enabled,
				stickyWeightedEnabled:          tt.global.stickyWeightedEnabled,
				subscriptionPriorityEnabled:    tt.global.subscriptionPriorityEnabled,
				lbTopKOverride:                 tt.global.lbTopKOverride,
				weightOverrides:                cloneOpenAIAdvancedSchedulerWeightOverrides(tt.global.weightOverrides),
				expiresAt:                      time.Now().Add(time.Hour).UnixNano(),
			})

			ctx := context.WithValue(context.Background(), ctxkey.Group, tt.group)
			got := (&OpenAIGatewayService{}).openAIAdvancedSchedulerEffectiveRuntimeSettings(ctx, &groupID, tt.platform)

			require.Equal(t, tt.wantEnabled, got.enabled)
			require.Equal(t, tt.wantSticky, got.stickyWeightedEnabled)
			require.Equal(t, tt.wantPriority, got.subscriptionPriorityEnabled)
			require.Equal(t, tt.wantTopK, got.lbTopKOverride)
			require.Equal(t, tt.wantWeights, got.weightOverrides)
		})
	}
}

func TestOpenAIAdvancedSchedulerRequestSettingsDriveRuntimeControls(t *testing.T) {
	t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	openAIAdvancedSchedulerSettingCache.Store(&cachedOpenAIAdvancedSchedulerSetting{
		lowUpstreamRatePriorityEnabled: true,
		enabled:                        false,
		stickyWeightedEnabled:          true,
		subscriptionPriorityEnabled:    true,
		lbTopKOverride:                 6,
		weightOverrides:                map[string]float64{"priority": 2, "load": 3},
		expiresAt:                      time.Now().Add(time.Hour).UnixNano(),
	})

	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.LBTopK = 9
	cfg.Gateway.OpenAIWS.SchedulerScoreWeights.Priority = 1
	cfg.Gateway.OpenAIWS.SchedulerScoreWeights.Load = 1
	groupID := int64(8201)
	group := &Group{
		ID: groupID, Platform: PlatformOpenAI, Status: StatusActive, Hydrated: true,
		SchedulerType: GroupSchedulerTypeAdvanced,
		AdvancedSchedulerOverrides: AdvancedSchedulerOverrides{
			StickyWeightedEnabled:       schedulerRuntimeTestPointer(false),
			SubscriptionPriorityEnabled: schedulerRuntimeTestPointer(false),
			LBTopK:                      schedulerRuntimeTestPointer(2),
			WeightPriority:              schedulerRuntimeTestPointer(0.0),
			WeightLoad:                  schedulerRuntimeTestPointer(0.0),
		},
	}
	svc := &OpenAIGatewayService{cfg: cfg}
	ctx := context.WithValue(context.Background(), ctxkey.Group, group)
	ctx = svc.withOpenAIAdvancedSchedulerRequestSettings(ctx, &groupID, PlatformOpenAI)

	require.NotNil(t, svc.getOpenAIAccountScheduler(ctx))
	require.False(t, svc.isOpenAILowUpstreamRatePriorityEnabled(ctx))
	require.False(t, svc.isOpenAIAdvancedSchedulerStickyWeightedEnabled(ctx))
	require.False(t, svc.isOpenAIAdvancedSchedulerSubscriptionPriorityEnabled(ctx))
	require.Equal(t, 2, svc.openAIWSLBTopKForRequest(ctx))
	weights := svc.openAIWSSchedulerWeightsForRequest(ctx)
	require.Zero(t, weights.Priority)
	require.Zero(t, weights.Load)

	ttft := 125
	svc.ReportOpenAIAccountScheduleResult(99, "gpt-5", true, &ttft)
	svc.RecordOpenAIAccountSwitch()
	metrics := svc.SnapshotOpenAIAccountSchedulerMetrics()
	require.Equal(t, int64(1), metrics.AccountSwitchTotal)
	require.Equal(t, 1, metrics.RuntimeStatsAccountCount)
}
