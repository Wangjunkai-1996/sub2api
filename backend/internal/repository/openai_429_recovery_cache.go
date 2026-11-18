package repository

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

// One hash holds the generation, cooldown round and probe token. Redis time
// and a single script make admission and state changes atomic across instances.
var openAI429RecoveryScript = redis.NewScript(`
redis.replicate_commands()
local key = KEYS[1]
local op = ARGV[1]
local generation = ARGV[2]
local token = ARGV[3]
local nextGeneration = ARGV[4]
local duration = tonumber(ARGV[5])
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
local current = redis.call('HGET', key, 'generation')
local retention = 86400000

if op == 'acquire' then
    if not current then
        current = token
        redis.call('HSET', key, 'generation', current, 'round', 0, 'until', 0)
        redis.call('PEXPIRE', key, retention)
    end
    local untilAt = tonumber(redis.call('HGET', key, 'until') or '0')
    if untilAt == 0 then return {1, current, 0, 0} end
    if untilAt > now then return {0, current, 0, untilAt - now} end
    local probeUntil = tonumber(redis.call('HGET', key, 'probe_until') or '0')
    if probeUntil > now then return {0, current, 0, probeUntil - now} end
    redis.call('HSET', key, 'probe', token, 'probe_until', now + duration)
    redis.call('PEXPIRE', key, retention)
    return {1, current, 1, 0}
end

if not current or current ~= generation then return 0 end
local untilAt = tonumber(redis.call('HGET', key, 'until') or '0')
local probe = redis.call('HGET', key, 'probe')
local probeUntil = tonumber(redis.call('HGET', key, 'probe_until') or '0')
local ownsProbe = probe == token and probeUntil > now

if op == 'fail' then
    if untilAt ~= 0 and not ownsProbe then return 0 end
    local round = math.min(4, tonumber(redis.call('HGET', key, 'round') or '0') + 1)
    local delays = {2000, 4000, 8000, 15000}
    local delay = math.max(delays[round], duration)
    redis.call('HSET', key, 'generation', nextGeneration, 'round', round, 'until', now + delay)
    redis.call('HDEL', key, 'probe', 'probe_until')
    redis.call('PEXPIRE', key, math.max(retention, delay + retention))
    return 1
end
if not ownsProbe then return 0 end
if op == 'accept' then
    redis.call('HSET', key, 'generation', nextGeneration, 'round', 0, 'until', 0)
    redis.call('HDEL', key, 'probe', 'probe_until')
    redis.call('PEXPIRE', key, retention)
    return 1
end
if op == 'refresh' then
    redis.call('HSET', key, 'probe_until', now + duration)
    redis.call('PEXPIRE', key, retention)
    return 1
end
if op == 'release' then
    redis.call('HDEL', key, 'probe', 'probe_until')
    return 1
end
return 0
`)

var _ service.OpenAI429RecoveryCache = (*concurrencyCache)(nil)

func openAI429RecoveryKey(accountID int64, model string) string {
	modelHash := sha256.Sum256([]byte(strings.TrimSpace(model)))
	return fmt.Sprintf("concurrency:openai_429:{%d}:%x", accountID, modelHash[:])
}

func (c *concurrencyCache) AcquireOpenAI429Attempt(ctx context.Context, accountID int64, model, token string, ttl time.Duration) (service.OpenAI429Admission, error) {
	values, err := openAI429RecoveryScript.Run(ctx, c.rdb, []string{openAI429RecoveryKey(accountID, model)}, "acquire", "", token, "", ttl.Milliseconds()).Slice()
	if err != nil {
		return service.OpenAI429Admission{}, err
	}
	if len(values) != 4 {
		return service.OpenAI429Admission{}, fmt.Errorf("invalid openai 429 admission result")
	}
	allowed, allowedOK := values[0].(int64)
	generation, generationOK := values[1].(string)
	probe, probeOK := values[2].(int64)
	wait, waitOK := values[3].(int64)
	if !allowedOK || !generationOK || !probeOK || !waitOK || generation == "" {
		return service.OpenAI429Admission{}, fmt.Errorf("invalid openai 429 admission fields")
	}
	return service.OpenAI429Admission{Allowed: allowed == 1, Generation: generation, Probe: probe == 1, RetryAfter: time.Duration(wait) * time.Millisecond}, nil
}

func (c *concurrencyCache) FailOpenAI429Attempt(ctx context.Context, accountID int64, model, generation, token, nextGeneration string, retryAfter time.Duration) (bool, error) {
	return c.updateOpenAI429Recovery(ctx, accountID, model, "fail", generation, token, nextGeneration, retryAfter)
}

func (c *concurrencyCache) AcceptOpenAI429Attempt(ctx context.Context, accountID int64, model, generation, token, nextGeneration string) (bool, error) {
	return c.updateOpenAI429Recovery(ctx, accountID, model, "accept", generation, token, nextGeneration, 0)
}

func (c *concurrencyCache) RefreshOpenAI429Probe(ctx context.Context, accountID int64, model, generation, token string, ttl time.Duration) (bool, error) {
	return c.updateOpenAI429Recovery(ctx, accountID, model, "refresh", generation, token, "", ttl)
}

func (c *concurrencyCache) ReleaseOpenAI429Probe(ctx context.Context, accountID int64, model, generation, token string) (bool, error) {
	return c.updateOpenAI429Recovery(ctx, accountID, model, "release", generation, token, "", 0)
}

func (c *concurrencyCache) updateOpenAI429Recovery(ctx context.Context, accountID int64, model, operation, generation, token, nextGeneration string, duration time.Duration) (bool, error) {
	result, err := openAI429RecoveryScript.Run(ctx, c.rdb, []string{openAI429RecoveryKey(accountID, model)}, operation, generation, token, nextGeneration, strconv.FormatInt(duration.Milliseconds(), 10)).Int64()
	return result == 1, err
}
