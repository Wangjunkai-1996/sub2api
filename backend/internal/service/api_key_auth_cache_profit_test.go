package service

// 投影漏列回归（service 半程）：认证快照 build → L2 JSON 序列化
// → 反序列化 → 还原 apiKey.Group → 请求 ctx → 利润门解析，全链路保真。
// repository 半程（真实 GetByKeyForAuth 投影）见
// internal/repository/api_key_repo_profit_projection_integration_test.go。

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/stretchr/testify/require"
)

func profitAuthTestAPIKey() *APIKey {
	groupID := int64(50)
	return &APIKey{
		ID:      82,
		UserID:  40,
		GroupID: &groupID,
		Name:    "profit-auth-roundtrip",
		Status:  StatusActive,
		User: &User{
			ID:          40,
			Email:       "profit@test.local",
			Status:      StatusActive,
			Concurrency: 5,
		},
		Group: &Group{
			ID:                   groupID,
			Name:                 "VIP-roundtrip",
			Platform:             PlatformOpenAI,
			Status:               StatusActive,
			Hydrated:             true,
			RateMultiplier:       0.06,
			SubscriptionType:     SubscriptionTypeStandard,
			PeakRateEnabled:      false,
			ProfitControlEnabled: true,
			ProfitMinMargin:      0.2,
			ProfitSafetyBuffer:   0.05,
		},
	}
}

// 快照构建 → L2 JSON 往返 → 还原 → 装门：利润字段必须全程保真，阈值与
// 计费同源（0.06 × (1−0.25) = 0.045）。
func TestAPIKeyAuthSnapshotProfitControlRoundtrip(t *testing.T) {
	svc := &APIKeyService{}
	apiKey := profitAuthTestAPIKey()

	snapshot := svc.snapshotFromAPIKey(context.Background(), apiKey)
	require.NotNil(t, snapshot)
	require.Equal(t, apiKeyAuthSnapshotVersion, snapshot.Version)
	require.Equal(t, 23, snapshot.Version)

	// 模拟 L2 缓存的完整 JSON 往返（与 apiKeyCache.SetAuthCache/GetAuthCache 同构）。
	payload, err := json.Marshal(&APIKeyAuthCacheEntry{Snapshot: snapshot})
	require.NoError(t, err)
	var restored APIKeyAuthCacheEntry
	require.NoError(t, json.Unmarshal(payload, &restored))

	materialized, used, err := svc.applyAuthCacheEntry(apiKey.Key, &restored)
	require.NoError(t, err)
	require.True(t, used)
	require.NotNil(t, materialized.Group)
	require.True(t, materialized.Group.Hydrated)
	require.True(t, materialized.Group.ProfitControlEnabled)
	require.InDelta(t, 0.2, materialized.Group.ProfitMinMargin, 1e-12)
	require.InDelta(t, 0.05, materialized.Group.ProfitSafetyBuffer, 1e-12)
	require.InDelta(t, 0.06, materialized.Group.RateMultiplier, 1e-12)

	// 中间件语义：materialized.Group 进请求 ctx → 门必须按快照配置装上。
	ctx := context.WithValue(context.Background(), ctxkey.Group, materialized.Group)
	gwSvc := &OpenAIGatewayService{}
	gate := gwSvc.resolveOpenAIProfitControlGate(ctx, materialized.GroupID)
	require.NotNil(t, gate, "还原后的认证分组必须能装门（投影漏列时本断言最先失败）")
	require.InDelta(t, 0.06*(1-0.25), gate.threshold, 1e-12)
}

// 除当前 v23 与蓝绿过渡期的 v22 外，其余版本必须淘汰回源。
func TestAPIKeyAuthSnapshotUnsupportedVersionsEvicted(t *testing.T) {
	for _, version := range []int{16, 21, 24} {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			svc := &APIKeyService{}
			snapshot := svc.snapshotFromAPIKey(context.Background(), profitAuthTestAPIKey())
			require.NotNil(t, snapshot)
			snapshot.Version = version

			materialized, used, err := svc.applyAuthCacheEntry("sk-old", &APIKeyAuthCacheEntry{Snapshot: snapshot})
			require.NoError(t, err)
			require.False(t, used, "unsupported cache versions must be evicted and rebuilt")
			require.Nil(t, materialized)
		})
	}
}

func TestAPIKeyAuthV23PayloadOmitsTrafficDirectorFields(t *testing.T) {
	svc := &APIKeyService{}
	snapshot := svc.snapshotFromAPIKey(context.Background(), profitAuthTestAPIKey())
	require.NotNil(t, snapshot)
	payload, err := json.Marshal(&APIKeyAuthCacheEntry{Snapshot: snapshot})
	require.NoError(t, err)
	require.NotContains(t, string(payload), "traffic_director_mode")
	require.NotContains(t, string(payload), "traffic_director_version")
	require.NotContains(t, string(payload), "traffic_director_spec")
}

func TestAPIKeyAuthV22TrafficDirectorPayloadBridgesWithoutMaterializingRetiredFields(t *testing.T) {
	payload := `{"snapshot":{"version":22,"api_key_id":82,"user_id":40,"group_id":50,"name":"legacy-v22","status":"active","user":{"id":40,"status":"active","concurrency":5},"group":{"id":50,"name":"VIP-roundtrip","platform":"openai","status":"active","rate_multiplier":0.06,"profit_control_enabled":true,"profit_min_margin":0.2,"profit_safety_buffer":0.05,"traffic_director_mode":"enforced","traffic_director_version":7}}}`

	var entry APIKeyAuthCacheEntry
	require.NoError(t, json.Unmarshal([]byte(payload), &entry))
	canonical, err := json.Marshal(&entry)
	require.NoError(t, err)
	require.NotContains(t, string(canonical), "traffic_director_mode")
	require.NotContains(t, string(canonical), "traffic_director_version")

	materialized, used, err := (&APIKeyService{}).applyAuthCacheEntry("sk-legacy-v22", &entry)
	require.NoError(t, err)
	require.True(t, used, "v22 remains a read-only bridge during blue-green rollout")
	require.NotNil(t, materialized)
	require.NotNil(t, materialized.Group)
	require.True(t, materialized.Group.ProfitControlEnabled)
}
