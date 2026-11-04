package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const openAIRecoveryAliasCachePrefix = "kkai:recovery:v1:alias:"
const openAIRecoveryAliasScopeLimit = 256
const openAIRecoveryAliasScopeBytesLimit = 64 << 20
const openAIRecoveryAliasGlobalBytesLimit = 256 << 20

var acquireOpenAIRecoverySnapshotWriteScript = redis.NewScript(`
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if redis.call('ZCARD', KEYS[1]) >= 32 then return 0 end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[3])
redis.call('PEXPIRE', KEYS[1], 10000)
return 1
`)

var publishOpenAIRecoveryAliasScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local function purge(index, sizes, total)
  local expired = redis.call('ZRANGEBYSCORE', index, '-inf', ARGV[6], 'LIMIT', 0, 256)
  for _, key in ipairs(expired) do
    local size = tonumber(redis.call('HGET', sizes, key) or '0')
    redis.call('ZREM', index, key)
    redis.call('HDEL', sizes, key)
    if size > 0 then redis.call('DECRBY', total, size) end
  end
end
purge(KEYS[2], KEYS[3], KEYS[4])
purge(KEYS[5], KEYS[6], KEYS[7])
local size = string.len(ARGV[2])
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[7]) then return -1 end
if tonumber(redis.call('GET', KEYS[4]) or '0') + size > tonumber(ARGV[8]) then return -1 end
if tonumber(redis.call('GET', KEYS[7]) or '0') + size > tonumber(ARGV[9]) then return -1 end
redis.call('HSET', KEYS[1], 'state', 'published', 'metadata', ARGV[1], 'snapshot', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
redis.call('ZADD', KEYS[2], ARGV[4], ARGV[5])
redis.call('HSET', KEYS[3], ARGV[5], size)
redis.call('INCRBY', KEYS[4], size)
if size > 0 then
  redis.call('ZADD', KEYS[5], ARGV[4], ARGV[5])
  redis.call('HSET', KEYS[6], ARGV[5], size)
  redis.call('INCRBY', KEYS[7], size)
end
for i = 2, 7 do
  if redis.call('EXISTS', KEYS[i]) == 1 then
    redis.call('PEXPIRE', KEYS[i], math.max(redis.call('PTTL', KEYS[i]), tonumber(ARGV[3])))
  end
end
return 1
`)

var deleteOpenAIRecoveryAliasScript = redis.NewScript(`
local function remove(index, sizes, total)
  local size = tonumber(redis.call('HGET', sizes, ARGV[1]) or '0')
  if redis.call('ZREM', index, ARGV[1]) == 1 then
    redis.call('HDEL', sizes, ARGV[1])
    if size > 0 then redis.call('DECRBY', total, size) end
  end
end
remove(KEYS[2], KEYS[3], KEYS[4])
remove(KEYS[5], KEYS[6], KEYS[7])
redis.call('DEL', KEYS[1])
return 1
`)

var _ service.OpenAIRecoveryAliasCache = (*gatewayCache)(nil)

func openAIRecoveryAliasCacheKey(scope, alias string) string {
	digest := sha256.Sum256([]byte(alias))
	return openAIRecoveryAliasCachePrefix + scope + ":" + hex.EncodeToString(digest[:])
}

func openAIRecoveryAliasScopeKey(scope string) string {
	return openAIRecoveryAliasCachePrefix + scope + ":active"
}

func openAIRecoveryAliasIndexKeys(scope string) []string {
	scopeKey := openAIRecoveryAliasScopeKey(scope)
	globalKey := openAIRecoveryAliasCachePrefix + "global:active"
	return []string{scopeKey, scopeKey + ":sizes", scopeKey + ":bytes", globalKey, globalKey + ":sizes", globalKey + ":bytes"}
}

func (c *gatewayCache) PublishOpenAIRecoveryAlias(ctx context.Context, scope, alias string, metadata, snapshot []byte, ttl time.Duration) (bool, error) {
	if c == nil || c.rdb == nil {
		return false, service.ErrOpenAIRecoveryAliasUnavailable
	}
	if ttl < time.Millisecond || len(metadata) > 4<<10 || len(snapshot) > 8<<20 {
		return false, service.ErrOpenAIRecoveryAliasInvalid
	}
	aliasKey := openAIRecoveryAliasCacheKey(scope, alias)
	cacheCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if len(snapshot) > 0 {
		lease, err := service.NewOpenAIRecoveryAliasID()
		if err != nil {
			return false, err
		}
		leaseKey := openAIRecoveryAliasCachePrefix + "global:writes"
		now := time.Now()
		acquired, err := acquireOpenAIRecoverySnapshotWriteScript.Run(cacheCtx, c.rdb, []string{leaseKey}, now.UnixMilli(), now.Add(10*time.Second).UnixMilli(), lease).Int64()
		if err != nil {
			return false, err
		}
		if acquired != 1 {
			return false, service.ErrOpenAIRecoveryAliasCapacity
		}
		defer func() {
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), time.Second)
			defer releaseCancel()
			_ = c.rdb.ZRem(releaseCtx, leaseKey, lease).Err()
		}()
	}
	keys := append([]string{aliasKey}, openAIRecoveryAliasIndexKeys(scope)...)
	now := time.Now()
	result, err := publishOpenAIRecoveryAliasScript.Run(cacheCtx, c.rdb,
		keys,
		metadata, snapshot, ttl.Milliseconds(), now.Add(ttl).UnixMilli(), aliasKey, now.UnixMilli(), openAIRecoveryAliasScopeLimit, openAIRecoveryAliasScopeBytesLimit, openAIRecoveryAliasGlobalBytesLimit,
	).Int64()
	if err != nil {
		return false, err
	}
	if result < 0 {
		return false, service.ErrOpenAIRecoveryAliasCapacity
	}
	return result == 1, nil
}

func (c *gatewayCache) GetOpenAIRecoveryAlias(ctx context.Context, scope, alias string) ([]byte, []byte, error) {
	if c == nil || c.rdb == nil {
		return nil, nil, service.ErrOpenAIRecoveryAliasUnavailable
	}
	cacheCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	values, err := c.rdb.HMGet(cacheCtx, openAIRecoveryAliasCacheKey(scope, alias), "state", "metadata", "snapshot").Result()
	if err != nil {
		return nil, nil, err
	}
	if len(values) != 3 || values[0] == nil {
		return nil, nil, nil
	}
	state, ok := values[0].(string)
	if !ok {
		return nil, nil, service.ErrOpenAIRecoveryAliasInvalid
	}
	if state == "pending" {
		return []byte("pending"), nil, nil
	}
	if state != "published" || values[1] == nil {
		return nil, nil, service.ErrOpenAIRecoveryAliasInvalid
	}
	metadata, ok := values[1].(string)
	if !ok {
		return nil, nil, service.ErrOpenAIRecoveryAliasInvalid
	}
	var snapshot []byte
	if values[2] != nil {
		stored, ok := values[2].(string)
		if !ok {
			return nil, nil, service.ErrOpenAIRecoveryAliasInvalid
		}
		snapshot = []byte(stored)
	}
	return []byte(metadata), snapshot, nil
}

func (c *gatewayCache) DeleteOpenAIRecoveryAlias(ctx context.Context, scope, alias string) error {
	if c == nil || c.rdb == nil {
		return service.ErrOpenAIRecoveryAliasUnavailable
	}
	aliasKey := openAIRecoveryAliasCacheKey(scope, alias)
	cacheCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	keys := append([]string{aliasKey}, openAIRecoveryAliasIndexKeys(scope)...)
	err := deleteOpenAIRecoveryAliasScript.Run(cacheCtx, c.rdb, keys, aliasKey).Err()
	return err
}
