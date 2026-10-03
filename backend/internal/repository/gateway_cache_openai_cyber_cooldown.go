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
	local events_key = KEYS[2]
	local now = tonumber(ARGV[1])
	local window = tonumber(ARGV[2])
	local first_duration = tonumber(ARGV[3])
	local escalated_duration = tonumber(ARGV[4])
	local event_field = ARGV[5]
	local strikes = tonumber(redis.call('HGET', state_key, 'strikes')) or 0
	local blocked_until = tonumber(redis.call('HGET', state_key, 'blocked_until')) or 0

	local event = redis.call('HGET', events_key, event_field)
	if event then
		local event_strikes, event_recorded_at, event_deadline = string.match(event, '^(%d+):(%d+):(%d+)$')
		event_strikes = tonumber(event_strikes)
		event_recorded_at = tonumber(event_recorded_at)
		event_deadline = tonumber(event_deadline)
		if not event_strikes or event_strikes < 1 or not event_recorded_at or event_recorded_at < 1 or not event_deadline or event_deadline < 1 then
			return redis.error_reply('invalid OpenAI Cyber cooldown event state')
		end
		if event_deadline > blocked_until then
			blocked_until = event_deadline
			redis.call('HSET', state_key, 'blocked_until', blocked_until)
		end
		local ttl = window
		local remaining = blocked_until - now
		if remaining > ttl then
			ttl = remaining
		end
		redis.call('EXPIRE', state_key, ttl)
		redis.call('EXPIRE', events_key, ttl)
		return {event_strikes, 1, event_recorded_at, event_deadline, blocked_until}
	end

	local last_at = tonumber(redis.call('HGET', state_key, 'last_at')) or 0
	if last_at == 0 or (now - last_at) >= window then
		strikes = 1
	else
		strikes = strikes + 1
	end

	local duration = escalated_duration
	if strikes == 1 then
		duration = first_duration
	end
	local event_deadline = now + duration
	if event_deadline > blocked_until then
		blocked_until = event_deadline
	end

	redis.call('HSET', state_key, 'strikes', strikes, 'last_at', now, 'blocked_until', blocked_until)
	redis.call('HSET', events_key, event_field, tostring(strikes) .. ':' .. tostring(now) .. ':' .. tostring(event_deadline))
	local ttl = window
	local remaining = blocked_until - now
	if remaining > ttl then
		ttl = remaining
	end
	redis.call('EXPIRE', state_key, ttl)
	redis.call('EXPIRE', events_key, ttl)
	return {strikes, 0, now, event_deadline, blocked_until}
`)

var getOpenAICyberAccountCooldownDeadlineScript = redis.NewScript(`
	local state_key = KEYS[1]
	if redis.call('EXISTS', state_key) == 0 then
		return 0
	end
	local blocked_until = tonumber(redis.call('HGET', state_key, 'blocked_until'))
	if not blocked_until or blocked_until < 1 then
		return redis.error_reply('invalid OpenAI Cyber cooldown account state')
	end
	return blocked_until
`)

var _ service.OpenAICyberAccountCooldownStore = (*gatewayCache)(nil)

func openAICyberAccountCooldownKeys(accountID int64, eventDigest string) (string, string, string) {
	stateKey := fmt.Sprintf("%s%d", openAICyberAccountCooldownPrefix, accountID)
	eventSum := sha256.Sum256([]byte(eventDigest))
	eventsKey := stateKey + ":events"
	return stateKey, eventsKey, hex.EncodeToString(eventSum[:])
}

func (c *gatewayCache) RecordOpenAICyberAccountCooldownStrike(
	ctx context.Context,
	accountID int64,
	eventDigest string,
	window time.Duration,
	firstDuration time.Duration,
	escalatedDuration time.Duration,
	now time.Time,
) (service.OpenAICyberAccountCooldownStrike, error) {
	if accountID <= 0 || eventDigest == "" || window < time.Second || firstDuration < time.Second || escalatedDuration < firstDuration {
		return service.OpenAICyberAccountCooldownStrike{}, fmt.Errorf("invalid OpenAI Cyber cooldown strike input")
	}
	stateKey, eventsKey, eventField := openAICyberAccountCooldownKeys(accountID, eventDigest)
	values, err := recordOpenAICyberAccountCooldownStrikeScript.Run(
		ctx,
		c.rdb,
		[]string{stateKey, eventsKey},
		now.UTC().Unix(),
		int64(window/time.Second),
		int64(firstDuration/time.Second),
		int64(escalatedDuration/time.Second),
		eventField,
	).Int64Slice()
	if err != nil {
		return service.OpenAICyberAccountCooldownStrike{}, err
	}
	if len(values) != 5 || values[0] <= 0 || values[2] <= 0 || values[3] <= 0 || values[4] <= 0 {
		return service.OpenAICyberAccountCooldownStrike{}, fmt.Errorf("invalid OpenAI Cyber cooldown strike result")
	}
	return service.OpenAICyberAccountCooldownStrike{
		Strikes:              int(values[0]),
		Duplicate:            values[1] == 1,
		EventRecordedAt:      time.Unix(values[2], 0).UTC(),
		EventCooldownUntil:   time.Unix(values[3], 0).UTC(),
		AccountCooldownUntil: time.Unix(values[4], 0).UTC(),
	}, nil
}

func (c *gatewayCache) GetOpenAICyberAccountCooldownDeadline(ctx context.Context, accountID int64) (time.Time, error) {
	if accountID <= 0 {
		return time.Time{}, fmt.Errorf("invalid OpenAI Cyber cooldown account ID")
	}
	stateKey, _, _ := openAICyberAccountCooldownKeys(accountID, "deadline")
	deadlineUnix, err := getOpenAICyberAccountCooldownDeadlineScript.Run(ctx, c.rdb, []string{stateKey}).Int64()
	if err != nil {
		return time.Time{}, err
	}
	if deadlineUnix == 0 {
		return time.Time{}, nil
	}
	if deadlineUnix < 0 {
		return time.Time{}, fmt.Errorf("invalid OpenAI Cyber cooldown deadline")
	}
	return time.Unix(deadlineUnix, 0).UTC(), nil
}
