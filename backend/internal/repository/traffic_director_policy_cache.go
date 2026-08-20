package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const trafficDirectorPolicyRedisKeyPrefix = "traffic_director:policy:v1"

type trafficDirectorPolicyRedisCache struct {
	rdb *redis.Client
}

func NewTrafficDirectorPolicyRedisCache(rdb *redis.Client) service.TrafficDirectorPolicyRedisCache {
	return &trafficDirectorPolicyRedisCache{rdb: rdb}
}

func (c *trafficDirectorPolicyRedisCache) GetTrafficDirectorPolicyVersion(
	ctx context.Context,
	groupID int64,
	version int64,
) (*service.TrafficDirectorVersion, error) {
	if c == nil || c.rdb == nil {
		return nil, fmt.Errorf("traffic director Redis cache is unavailable")
	}
	if err := validateTrafficDirectorPolicyRedisCoordinates(groupID, version); err != nil {
		return nil, err
	}

	data, err := c.rdb.Get(ctx, trafficDirectorPolicyRedisKey(groupID, version)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get traffic director policy from Redis: %w", err)
	}

	var policy service.TrafficDirectorVersion
	if err := json.Unmarshal(data, &policy); err != nil {
		return nil, fmt.Errorf("decode traffic director policy from Redis: %w", err)
	}
	if policy.GroupID != groupID || policy.Version != version {
		return nil, fmt.Errorf(
			"cached traffic director policy coordinates mismatch: requested group=%d version=%d, got group=%d version=%d",
			groupID,
			version,
			policy.GroupID,
			policy.Version,
		)
	}
	return &policy, nil
}

func (c *trafficDirectorPolicyRedisCache) SetTrafficDirectorPolicyVersion(
	ctx context.Context,
	policy *service.TrafficDirectorVersion,
	ttl time.Duration,
) error {
	if c == nil || c.rdb == nil {
		return fmt.Errorf("traffic director Redis cache is unavailable")
	}
	if policy == nil {
		return fmt.Errorf("traffic director policy must not be nil")
	}
	if err := validateTrafficDirectorPolicyRedisCoordinates(policy.GroupID, policy.Version); err != nil {
		return err
	}

	data, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("encode traffic director policy for Redis: %w", err)
	}
	if err := c.rdb.Set(
		ctx,
		trafficDirectorPolicyRedisKey(policy.GroupID, policy.Version),
		data,
		ttl,
	).Err(); err != nil {
		return fmt.Errorf("set traffic director policy in Redis: %w", err)
	}
	return nil
}

func trafficDirectorPolicyRedisKey(groupID, version int64) string {
	return fmt.Sprintf("%s:{%d}:%d", trafficDirectorPolicyRedisKeyPrefix, groupID, version)
}

func validateTrafficDirectorPolicyRedisCoordinates(groupID, version int64) error {
	if groupID <= 0 {
		return fmt.Errorf("traffic director group ID must be positive")
	}
	if version <= service.TrafficDirectorLegacyVersion {
		return fmt.Errorf("cached traffic director version must be positive")
	}
	return nil
}
