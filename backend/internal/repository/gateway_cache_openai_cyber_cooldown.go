package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const openAICyberAccountCooldownPrefix = "openai:cyber_account_cooldown:"

var recordOpenAICyberAccountCooldownStrikeScript = redis.NewScript(`
	local state_key = KEYS[1]
	local event_key = KEYS[2]
	local now = tonumber(ARGV[1])
	local window = tonumber(ARGV[2])
	local strikes = tonumber(redis.call('HGET', state_key, 'strikes')) or 0

	if redis.call('EXISTS', event_key) == 1 then
		if strikes < 1 then
			strikes = 1
		end
		return {strikes, 1}
	end

	local last_at = tonumber(redis.call('HGET', state_key, 'last_at')) or 0
	if last_at == 0 or (now - last_at) >= window then
		strikes = 1
	else
		strikes = strikes + 1
	end

	redis.call('HSET', state_key, 'strikes', strikes, 'last_at', now)
	redis.call('EXPIRE', state_key, window)
	redis.call('SET', event_key, '1', 'EX', window)
	return {strikes, 0}
`)

var _ service.OpenAICyberAccountCooldownStore = (*gatewayCache)(nil)

func openAICyberAccountCooldownKeys(accountID int64, eventDigest string) (string, string) {
	stateKey := fmt.Sprintf("%s%d", openAICyberAccountCooldownPrefix, accountID)
	eventSum := sha256.Sum256([]byte(eventDigest))
	eventKey := stateKey + ":event:" + hex.EncodeToString(eventSum[:])
	return stateKey, eventKey
}

func (c *gatewayCache) RecordOpenAICyberAccountCooldownStrike(
	ctx context.Context,
	accountID int64,
	eventDigest string,
	window time.Duration,
	now time.Time,
) (service.OpenAICyberAccountCooldownStrike, error) {
	if accountID <= 0 || eventDigest == "" || window < time.Second {
		return service.OpenAICyberAccountCooldownStrike{}, fmt.Errorf("invalid OpenAI Cyber cooldown strike input")
	}
	stateKey, eventKey := openAICyberAccountCooldownKeys(accountID, eventDigest)
	values, err := recordOpenAICyberAccountCooldownStrikeScript.Run(
		ctx,
		c.rdb,
		[]string{stateKey, eventKey},
		now.UTC().Unix(),
		int64(window/time.Second),
	).Int64Slice()
	if err != nil {
		return service.OpenAICyberAccountCooldownStrike{}, err
	}
	if len(values) != 2 || values[0] <= 0 {
		return service.OpenAICyberAccountCooldownStrike{}, fmt.Errorf("invalid OpenAI Cyber cooldown strike result")
	}
	return service.OpenAICyberAccountCooldownStrike{
		Strikes:   int(values[0]),
		Duplicate: values[1] == 1,
	}, nil
}
