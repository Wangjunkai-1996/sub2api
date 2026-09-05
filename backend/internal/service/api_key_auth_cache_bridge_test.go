package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// legacyV24APIKeyAuthCacheEntry models the fields decoded by the production
// version being replaced. encoding/json must ignore the bridge sentinel and
// merged policy fields that did not exist in that binary.
type legacyV24APIKeyAuthCacheEntry struct {
	Snapshot *struct {
		Version  int   `json:"version"`
		APIKeyID int64 `json:"api_key_id"`
		Group    *struct {
			SchedulerType              string                     `json:"scheduler_type"`
			AdvancedSchedulerOverrides AdvancedSchedulerOverrides `json:"advanced_scheduler_overrides"`
		} `json:"group,omitempty"`
	} `json:"snapshot,omitempty"`
}

func TestAPIKeyAuthSnapshotBridgeRoundTrip(t *testing.T) {
	groupID := int64(50)
	stickyWeighted := false
	lbTopK := 7
	weightLoad := 0.35
	apiKey := &APIKey{
		ID:          82,
		UserID:      40,
		GroupID:     &groupID,
		Key:         "sk-v24-bridge",
		Status:      StatusActive,
		IPWhitelist: []string{"203.0.113.8"},
		IPBlacklist: []string{"198.51.100.0/24"},
		Quota:       120,
		QuotaUsed:   19,
		RateLimit5h: 5,
		RateLimit1d: 12,
		RateLimit7d: 45,
		User: &User{
			ID:                   40,
			Status:               StatusActive,
			Role:                 RoleUser,
			Balance:              88,
			Concurrency:          4,
			AllowedGroups:        []int64{groupID},
			RestrictPublicGroups: true,
		},
		Group: &Group{
			ID:                          groupID,
			Name:                        "bridge",
			Platform:                    PlatformOpenAI,
			Status:                      StatusActive,
			Hydrated:                    true,
			SubscriptionType:            SubscriptionTypeStandard,
			RateMultiplier:              0.8,
			ForceOpenAIFast:             true,
			FreeOpenAIFast:              true,
			MaxReasoningEffort:          "medium",
			MaxReasoningEffortOverLimit: ReasoningEffortOverLimitDeny,
			ReasoningEffortMappings: []ReasoningEffortMapping{
				{From: "max", To: "xhigh"},
			},
			ProfitControlEnabled: true,
			ProfitMinMargin:      0.2,
			ProfitSafetyBuffer:   0.05,
			SchedulerType:        GroupSchedulerTypeAdvanced,
			AdvancedSchedulerOverrides: AdvancedSchedulerOverrides{
				StickyWeightedEnabled: &stickyWeighted,
				LBTopK:                &lbTopK,
				WeightLoad:            &weightLoad,
			},
		},
	}

	svc := &APIKeyService{}
	snapshot := svc.snapshotFromAPIKey(context.Background(), apiKey)
	require.NotNil(t, snapshot)
	require.Equal(t, apiKeyAuthSnapshotBridgeWireVersion, snapshot.Version)
	require.Equal(t, apiKeyAuthSnapshotVersion, snapshot.CompletenessVersion)

	payload, err := json.Marshal(&APIKeyAuthCacheEntry{Snapshot: snapshot})
	require.NoError(t, err)

	var legacy legacyV24APIKeyAuthCacheEntry
	require.NoError(t, json.Unmarshal(payload, &legacy))
	require.NotNil(t, legacy.Snapshot)
	require.Equal(t, 24, legacy.Snapshot.Version)
	require.Equal(t, apiKey.ID, legacy.Snapshot.APIKeyID)
	require.NotNil(t, legacy.Snapshot.Group)
	require.Equal(t, GroupSchedulerTypeAdvanced, legacy.Snapshot.Group.SchedulerType)
	require.Equal(t, apiKey.Group.AdvancedSchedulerOverrides, legacy.Snapshot.Group.AdvancedSchedulerOverrides)

	var restored APIKeyAuthCacheEntry
	require.NoError(t, json.Unmarshal(payload, &restored))
	materialized, used, err := svc.applyAuthCacheEntry(apiKey.Key, &restored)
	require.NoError(t, err)
	require.True(t, used)
	require.NotNil(t, materialized)
	require.Equal(t, apiKey.IPWhitelist, materialized.IPWhitelist)
	require.Equal(t, apiKey.IPBlacklist, materialized.IPBlacklist)
	require.Equal(t, apiKey.Quota, materialized.Quota)
	require.Equal(t, apiKey.QuotaUsed, materialized.QuotaUsed)
	require.Equal(t, apiKey.RateLimit5h, materialized.RateLimit5h)
	require.Equal(t, apiKey.RateLimit1d, materialized.RateLimit1d)
	require.Equal(t, apiKey.RateLimit7d, materialized.RateLimit7d)
	require.True(t, materialized.User.RestrictPublicGroups)
	require.True(t, materialized.Group.ForceOpenAIFast)
	require.True(t, materialized.Group.FreeOpenAIFast)
	require.Equal(t, apiKey.Group.RateMultiplier, materialized.Group.RateMultiplier)
	require.True(t, materialized.Group.ProfitControlEnabled)
	require.Equal(t, apiKey.Group.ProfitMinMargin, materialized.Group.ProfitMinMargin)
	require.Equal(t, apiKey.Group.ProfitSafetyBuffer, materialized.Group.ProfitSafetyBuffer)
	require.Equal(t, apiKey.Group.MaxReasoningEffort, materialized.Group.MaxReasoningEffort)
	require.Equal(t, apiKey.Group.MaxReasoningEffortOverLimit, materialized.Group.MaxReasoningEffortOverLimit)
	require.Equal(t, apiKey.Group.ReasoningEffortMappings, materialized.Group.ReasoningEffortMappings)
	require.Equal(t, apiKey.Group.SchedulerType, materialized.Group.SchedulerType)
	require.Equal(t, apiKey.Group.AdvancedSchedulerOverrides, materialized.Group.AdvancedSchedulerOverrides)
}

func TestAPIKeyAuthSnapshotVersionCompatibility(t *testing.T) {
	tests := []struct {
		name                string
		wireVersion         int
		completenessVersion int
		wantUsed            bool
	}{
		{name: "native v25", wireVersion: 25, wantUsed: true},
		{name: "v25 bridge", wireVersion: 24, completenessVersion: 25, wantUsed: true},
		{name: "legacy v22", wireVersion: 22, wantUsed: false},
		{name: "legacy v23 without marker", wireVersion: 23, wantUsed: false},
		{name: "legacy v24 without marker", wireVersion: 24, wantUsed: false},
		{name: "v24 with old marker", wireVersion: 24, completenessVersion: 24, wantUsed: false},
		{name: "unknown wire version", wireVersion: 26, completenessVersion: 25, wantUsed: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry := &APIKeyAuthCacheEntry{Snapshot: &APIKeyAuthSnapshot{
				Version:             tt.wireVersion,
				CompletenessVersion: tt.completenessVersion,
			}}
			materialized, used, err := (&APIKeyService{}).applyAuthCacheEntry("sk-version", entry)
			require.NoError(t, err)
			require.Equal(t, tt.wantUsed, used)
			if tt.wantUsed {
				require.NotNil(t, materialized)
			} else {
				require.Nil(t, materialized)
			}
		})
	}
}
