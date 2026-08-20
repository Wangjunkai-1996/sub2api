package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const trafficDirectorHealthRedisKeyPrefix = "traffic_director:health:v1"

const trafficDirectorHealthCheckScriptSource = `
local key = KEYS[1]
local model = ARGV[1]
local now = tonumber(ARGV[2])
local streak_ttl = tonumber(ARGV[3])
local acquire = ARGV[4] == '1'
local probe_token = ARGV[5]
local probe_lease = tonumber(ARGV[6])

if redis.call('EXISTS', key) == 0 then
  return {0, 0, 0, 0, 0, 0}
end
if redis.call('HGET', key, 'model') ~= model then
  return redis.error_reply('traffic director health model mismatch')
end

local state = redis.call('HGET', key, 'state')
local streak = tonumber(redis.call('HGET', key, 'streak')) or 0
local last_failure = tonumber(redis.call('HGET', key, 'last_failure')) or 0
local open_until = tonumber(redis.call('HGET', key, 'open_until')) or 0
local stored_probe_token = redis.call('HGET', key, 'probe_token') or ''
local probe_until = tonumber(redis.call('HGET', key, 'probe_until')) or 0
if state ~= 'suspect' and state ~= 'open' and state ~= 'half_open' then
  return redis.error_reply('invalid traffic director health state')
end
if state == 'open' and open_until < 1 then
  return redis.error_reply('invalid traffic director health open deadline')
end
if state == 'half_open' and (stored_probe_token == '' or probe_until < 1) then
  return redis.error_reply('invalid traffic director health probe state')
end
if streak < 1 or last_failure < 1 then
  return redis.error_reply('invalid traffic director health streak')
end
if now < last_failure or now - last_failure >= streak_ttl then
  redis.call('DEL', key)
  return {0, 0, 0, 0, 0, 0}
end

if state == 'suspect' then
  return {1, streak, last_failure, 0, 0, 0}
end

if state == 'open' and now < open_until then
  return {2, streak, last_failure, open_until, 0, 0}
end

-- An open deadline has elapsed, or an existing probe lease is being observed.
if stored_probe_token ~= '' and probe_until > now then
  return {3, streak, last_failure, open_until, 0, probe_until}
end
if not acquire then
  return {3, streak, last_failure, open_until, 0, 0}
end
if probe_token == '' or probe_lease < 1 then
  return redis.error_reply('invalid traffic director health probe lease')
end

probe_until = now + probe_lease
redis.call('HSET', key, 'state', 'half_open', 'probe_token', probe_token, 'probe_until', probe_until)
return {3, streak, last_failure, open_until, 1, probe_until}
`

const trafficDirectorHealthRecordFailureScriptSource = `
local key = KEYS[1]
local model = ARGV[1]
local now = tonumber(ARGV[2])
local streak_ttl = tonumber(ARGV[3])
local probe_token = ARGV[4]
local short_open = tonumber(ARGV[5])
local long_open = tonumber(ARGV[6])

if redis.call('EXISTS', key) == 0 then
  redis.call('HSET', key,
    'model', model,
    'state', 'suspect',
    'streak', 1,
    'last_failure', now,
    'open_until', 0,
    'probe_token', '',
    'probe_until', 0)
  redis.call('PEXPIRE', key, streak_ttl)
  return {1, 1, now, 0, 0, 1}
end
if redis.call('HGET', key, 'model') ~= model then
  return redis.error_reply('traffic director health model mismatch')
end

local state = redis.call('HGET', key, 'state')
local streak = tonumber(redis.call('HGET', key, 'streak')) or 0
local last_failure = tonumber(redis.call('HGET', key, 'last_failure')) or 0
local open_until = tonumber(redis.call('HGET', key, 'open_until')) or 0
local stored_probe_token = redis.call('HGET', key, 'probe_token') or ''
local probe_until = tonumber(redis.call('HGET', key, 'probe_until')) or 0
if state ~= 'suspect' and state ~= 'open' and state ~= 'half_open' then
  return redis.error_reply('invalid traffic director health state')
end
if state == 'open' and open_until < 1 then
  return redis.error_reply('invalid traffic director health open deadline')
end
if state == 'half_open' and (stored_probe_token == '' or probe_until < 1) then
  return redis.error_reply('invalid traffic director health probe state')
end
if streak < 1 or last_failure < 1 then
  return redis.error_reply('invalid traffic director health streak')
end

if now < last_failure or now - last_failure >= streak_ttl then
  state = 'healthy'
  streak = 0
  open_until = 0
  stored_probe_token = ''
  probe_until = 0
end

-- A non-empty token identifies a real half-open probe. Never let a stale probe
-- overwrite a newer owner; an observe request without a token cannot interfere
-- while a real lease is active.
if state == 'half_open' then
  if probe_token ~= '' then
    if probe_token ~= stored_probe_token then
      return {3, streak, last_failure, open_until, probe_until, 0}
    end
  elseif probe_until > now then
    return {3, streak, last_failure, open_until, probe_until, 0}
  end
elseif probe_token ~= '' then
  return redis.error_reply('stale traffic director health probe token')
end

local half_open_failure = state == 'half_open'
if state == 'open' and open_until > 0 and now >= open_until then
  half_open_failure = true
end

streak = streak + 1
last_failure = now
if half_open_failure then
  if streak < 3 then
    streak = 3
  end
  state = 'open'
  open_until = now + long_open
else
  if streak == 1 then
    state = 'suspect'
    open_until = 0
  else
    state = 'open'
    open_until = now + short_open
  end
end

redis.call('HSET', key,
  'model', model,
  'state', state,
  'streak', streak,
  'last_failure', last_failure,
  'open_until', open_until,
  'probe_token', '',
  'probe_until', 0)
redis.call('PEXPIRE', key, streak_ttl)
local state_code = 1
if state == 'open' then
  state_code = 2
end
return {state_code, streak, last_failure, open_until, 0, 1}
`

const trafficDirectorHealthRecordSuccessScriptSource = `
local key = KEYS[1]
local model = ARGV[1]
local probe_token = ARGV[2]
local allow_observe_recovery = ARGV[3] == '1'
if redis.call('EXISTS', key) == 0 then
  return 0
end
if redis.call('HGET', key, 'model') ~= model then
  return redis.error_reply('traffic director health model mismatch')
end
local state = redis.call('HGET', key, 'state')
local stored_probe_token = redis.call('HGET', key, 'probe_token') or ''
if state ~= 'suspect' and state ~= 'open' and state ~= 'half_open' then
  return redis.error_reply('invalid traffic director health state')
end
if state == 'open' and (tonumber(redis.call('HGET', key, 'open_until')) or 0) < 1 then
  return redis.error_reply('invalid traffic director health open deadline')
end
if state == 'half_open' then
  if probe_token == '' or probe_token ~= stored_probe_token then
    return 0
  end
elseif state == 'open' then
  -- A request admitted before the breaker opened cannot close it early.
  -- Observe-mode traffic never takes a probe, so a later real success is its
  -- only recovery signal. Enforce mode must still wait for the unique probe.
  if not allow_observe_recovery or probe_token ~= '' then
    return 0
  end
elseif probe_token ~= '' then
  return 0
end
redis.call('DEL', key)
return 1
`

const trafficDirectorHealthRenewProbeScriptSource = `
local key = KEYS[1]
local model = ARGV[1]
local now = tonumber(ARGV[2])
local probe_token = ARGV[3]
local probe_lease = tonumber(ARGV[4])
if redis.call('EXISTS', key) == 0 then
  return 0
end
if redis.call('HGET', key, 'model') ~= model then
  return redis.error_reply('traffic director health model mismatch')
end
if redis.call('HGET', key, 'state') ~= 'half_open' then
  return 0
end
if redis.call('HGET', key, 'probe_token') ~= probe_token then
  return 0
end
if probe_token == '' or probe_lease < 1 then
  return redis.error_reply('invalid traffic director health probe lease')
end
redis.call('HSET', key, 'probe_until', now + probe_lease)
return 1
`

const trafficDirectorHealthReleaseProbeScriptSource = `
local key = KEYS[1]
local model = ARGV[1]
local probe_token = ARGV[2]
if redis.call('EXISTS', key) == 0 then
  return 0
end
if redis.call('HGET', key, 'model') ~= model then
  return redis.error_reply('traffic director health model mismatch')
end
if redis.call('HGET', key, 'state') ~= 'half_open' then
  return 0
end
if probe_token == '' or redis.call('HGET', key, 'probe_token') ~= probe_token then
  return 0
end
local open_until = tonumber(redis.call('HGET', key, 'open_until')) or 0
if open_until < 1 then
  return redis.error_reply('invalid traffic director health open deadline')
end
redis.call('HSET', key, 'state', 'open', 'probe_token', '', 'probe_until', 0)
return 1
`

var (
	trafficDirectorHealthCheckScript         = redis.NewScript(trafficDirectorHealthCheckScriptSource)
	trafficDirectorHealthRecordFailureScript = redis.NewScript(trafficDirectorHealthRecordFailureScriptSource)
	trafficDirectorHealthRecordSuccessScript = redis.NewScript(trafficDirectorHealthRecordSuccessScriptSource)
	trafficDirectorHealthRenewProbeScript    = redis.NewScript(trafficDirectorHealthRenewProbeScriptSource)
	trafficDirectorHealthReleaseProbeScript  = redis.NewScript(trafficDirectorHealthReleaseProbeScriptSource)
)

type trafficDirectorHealthRedisStore struct {
	rdb *redis.Client
}

func NewTrafficDirectorHealthRedisStore(rdb *redis.Client) service.TrafficDirectorHealthStore {
	return &trafficDirectorHealthRedisStore{rdb: rdb}
}

func (s *trafficDirectorHealthRedisStore) CheckTrafficDirectorHealth(
	ctx context.Context,
	request service.TrafficDirectorHealthStoreCheckRequest,
) (service.TrafficDirectorHealthSnapshot, error) {
	if err := validateTrafficDirectorHealthStoreCheckRequest(request); err != nil {
		return service.TrafficDirectorHealthSnapshot{}, err
	}
	if s == nil || s.rdb == nil {
		return service.TrafficDirectorHealthSnapshot{}, fmt.Errorf("traffic director health Redis store is unavailable")
	}
	values, err := trafficDirectorHealthCheckScript.Run(
		ctx,
		s.rdb,
		[]string{trafficDirectorHealthRedisKey(request.AccountID, request.NormalizedModel)},
		request.NormalizedModel,
		request.Now.UnixMilli(),
		request.FailureStreakTTL.Milliseconds(),
		boolArg(request.AcquireProbe),
		request.ProbeToken,
		request.ProbeLease.Milliseconds(),
	).Int64Slice()
	if err != nil {
		return service.TrafficDirectorHealthSnapshot{}, err
	}
	return trafficDirectorHealthSnapshotFromScript(values, false)
}

func (s *trafficDirectorHealthRedisStore) RecordTrafficDirectorHealthFailure(
	ctx context.Context,
	request service.TrafficDirectorHealthStoreFailureRequest,
) (service.TrafficDirectorHealthSnapshot, error) {
	if err := validateTrafficDirectorHealthStoreFailureRequest(request); err != nil {
		return service.TrafficDirectorHealthSnapshot{}, err
	}
	if s == nil || s.rdb == nil {
		return service.TrafficDirectorHealthSnapshot{}, fmt.Errorf("traffic director health Redis store is unavailable")
	}
	values, err := trafficDirectorHealthRecordFailureScript.Run(
		ctx,
		s.rdb,
		[]string{trafficDirectorHealthRedisKey(request.AccountID, request.NormalizedModel)},
		request.NormalizedModel,
		request.Now.UnixMilli(),
		request.FailureStreakTTL.Milliseconds(),
		request.ProbeToken,
		request.ShortOpen.Milliseconds(),
		request.LongOpen.Milliseconds(),
	).Int64Slice()
	if err != nil {
		return service.TrafficDirectorHealthSnapshot{}, err
	}
	return trafficDirectorHealthSnapshotFromScript(values, true)
}

func (s *trafficDirectorHealthRedisStore) RecordTrafficDirectorHealthSuccess(
	ctx context.Context,
	request service.TrafficDirectorHealthStoreSuccessRequest,
) (bool, error) {
	if err := validateTrafficDirectorHealthStoreSuccessRequest(request); err != nil {
		return false, err
	}
	if s == nil || s.rdb == nil {
		return false, fmt.Errorf("traffic director health Redis store is unavailable")
	}
	result, err := trafficDirectorHealthRecordSuccessScript.Run(
		ctx,
		s.rdb,
		[]string{trafficDirectorHealthRedisKey(request.AccountID, request.NormalizedModel)},
		request.NormalizedModel,
		request.ProbeToken,
		boolArg(request.AllowObserveRecovery),
	).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (s *trafficDirectorHealthRedisStore) RenewTrafficDirectorHealthProbe(
	ctx context.Context,
	request service.TrafficDirectorHealthStoreProbeRequest,
) (bool, error) {
	if err := validateTrafficDirectorHealthStoreProbeRequest(request); err != nil {
		return false, err
	}
	if s == nil || s.rdb == nil {
		return false, fmt.Errorf("traffic director health Redis store is unavailable")
	}
	result, err := trafficDirectorHealthRenewProbeScript.Run(
		ctx,
		s.rdb,
		[]string{trafficDirectorHealthRedisKey(request.AccountID, request.NormalizedModel)},
		request.NormalizedModel,
		request.Now.UnixMilli(),
		request.ProbeToken,
		request.ProbeLease.Milliseconds(),
	).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func (s *trafficDirectorHealthRedisStore) ReleaseTrafficDirectorHealthProbe(
	ctx context.Context,
	request service.TrafficDirectorHealthStoreProbeReleaseRequest,
) (bool, error) {
	if err := validateTrafficDirectorHealthStoreProbeReleaseRequest(request); err != nil {
		return false, err
	}
	if s == nil || s.rdb == nil {
		return false, fmt.Errorf("traffic director health Redis store is unavailable")
	}
	result, err := trafficDirectorHealthReleaseProbeScript.Run(
		ctx,
		s.rdb,
		[]string{trafficDirectorHealthRedisKey(request.AccountID, request.NormalizedModel)},
		request.NormalizedModel,
		request.ProbeToken,
	).Int64()
	if err != nil {
		return false, err
	}
	return result == 1, nil
}

func trafficDirectorHealthRedisKey(accountID int64, normalizedModel string) string {
	sum := sha256.Sum256([]byte(normalizedModel))
	return fmt.Sprintf("%s:{%d}:%s", trafficDirectorHealthRedisKeyPrefix, accountID, hex.EncodeToString(sum[:]))
}

func validateTrafficDirectorHealthStoreCheckRequest(request service.TrafficDirectorHealthStoreCheckRequest) error {
	if err := validateTrafficDirectorHealthStoreCoordinates(request.AccountID, request.NormalizedModel); err != nil {
		return err
	}
	if err := validateTrafficDirectorHealthStoreTime(request.Now); err != nil {
		return err
	}
	if request.FailureStreakTTL <= 0 || request.FailureStreakTTL.Milliseconds() <= 0 {
		return fmt.Errorf("invalid traffic director health check streak duration")
	}
	if request.AcquireProbe && (request.ProbeLease <= 0 || request.ProbeLease.Milliseconds() <= 0) {
		return fmt.Errorf("invalid traffic director health probe lease duration")
	}
	if request.AcquireProbe && (strings.TrimSpace(request.ProbeToken) == "" || len(request.ProbeToken) > 256) {
		return fmt.Errorf("invalid traffic director health probe token")
	}
	return nil
}

func validateTrafficDirectorHealthStoreFailureRequest(request service.TrafficDirectorHealthStoreFailureRequest) error {
	if err := validateTrafficDirectorHealthStoreCoordinates(request.AccountID, request.NormalizedModel); err != nil {
		return err
	}
	if err := validateTrafficDirectorHealthStoreTime(request.Now); err != nil {
		return err
	}
	if request.FailureStreakTTL <= 0 || request.ShortOpen <= 0 || request.LongOpen <= 0 || request.LongOpen < request.ShortOpen ||
		request.FailureStreakTTL.Milliseconds() <= 0 || request.ShortOpen.Milliseconds() <= 0 || request.LongOpen.Milliseconds() <= 0 {
		return fmt.Errorf("invalid traffic director health failure durations")
	}
	if len(request.ProbeToken) > 256 {
		return fmt.Errorf("invalid traffic director health probe token")
	}
	return nil
}

func validateTrafficDirectorHealthStoreSuccessRequest(request service.TrafficDirectorHealthStoreSuccessRequest) error {
	if err := validateTrafficDirectorHealthStoreCoordinates(request.AccountID, request.NormalizedModel); err != nil {
		return err
	}
	if len(request.ProbeToken) > 256 {
		return fmt.Errorf("invalid traffic director health probe token")
	}
	if request.AllowObserveRecovery && request.ProbeToken != "" {
		return fmt.Errorf("observe recovery cannot carry a traffic director health probe token")
	}
	return nil
}

func validateTrafficDirectorHealthStoreProbeRequest(request service.TrafficDirectorHealthStoreProbeRequest) error {
	if err := validateTrafficDirectorHealthStoreCoordinates(request.AccountID, request.NormalizedModel); err != nil {
		return err
	}
	if err := validateTrafficDirectorHealthStoreTime(request.Now); err != nil {
		return err
	}
	if request.ProbeToken == "" || len(request.ProbeToken) > 256 || request.ProbeLease <= 0 || request.ProbeLease.Milliseconds() <= 0 {
		return fmt.Errorf("invalid traffic director health probe request")
	}
	return nil
}

func validateTrafficDirectorHealthStoreProbeReleaseRequest(request service.TrafficDirectorHealthStoreProbeReleaseRequest) error {
	if err := validateTrafficDirectorHealthStoreCoordinates(request.AccountID, request.NormalizedModel); err != nil {
		return err
	}
	if request.ProbeToken == "" || request.ProbeToken != strings.TrimSpace(request.ProbeToken) || len(request.ProbeToken) > 256 {
		return fmt.Errorf("invalid traffic director health probe release request")
	}
	return nil
}

func validateTrafficDirectorHealthStoreCoordinates(accountID int64, normalizedModel string) error {
	if accountID <= 0 || service.NormalizeTrafficDirectorHealthModel(normalizedModel) != normalizedModel {
		return fmt.Errorf("invalid traffic director health account or normalized model")
	}
	return nil
}

func validateTrafficDirectorHealthStoreTime(now time.Time) error {
	if now.IsZero() || now.UnixMilli() <= 0 {
		return fmt.Errorf("invalid traffic director health clock")
	}
	return nil
}

func trafficDirectorHealthSnapshotFromScript(values []int64, mutationResult bool) (service.TrafficDirectorHealthSnapshot, error) {
	if len(values) != 6 {
		return service.TrafficDirectorHealthSnapshot{}, fmt.Errorf("invalid traffic director health script result length")
	}
	state, err := trafficDirectorHealthStateFromCode(values[0])
	if err != nil {
		return service.TrafficDirectorHealthSnapshot{}, err
	}
	if values[1] < 0 || values[2] < 0 || values[3] < 0 || values[4] < 0 || values[5] < 0 {
		return service.TrafficDirectorHealthSnapshot{}, fmt.Errorf("invalid traffic director health script result values")
	}
	snapshot := service.TrafficDirectorHealthSnapshot{
		State:           state,
		FailureStreak:   int(values[1]),
		LastFailureAt:   trafficDirectorHealthUnixMillis(values[2]),
		OpenUntil:       trafficDirectorHealthUnixMillis(values[3]),
		ProbeAcquired:   values[4] == 1,
		ProbeUntil:      trafficDirectorHealthUnixMillis(values[5]),
		MutationApplied: false,
	}
	if mutationResult {
		// Failure script uses the final slot for mutation status, not probe state.
		snapshot.MutationApplied = values[5] == 1
		snapshot.ProbeUntil = trafficDirectorHealthUnixMillis(values[4])
		// The failure script's fifth slot is probe_until and final slot is applied.
		snapshot.ProbeAcquired = false
	}
	return snapshot, nil
}

func trafficDirectorHealthStateFromCode(code int64) (string, error) {
	switch code {
	case 0:
		return service.TrafficDirectorHealthStateHealthy, nil
	case 1:
		return service.TrafficDirectorHealthStateSuspect, nil
	case 2:
		return service.TrafficDirectorHealthStateOpen, nil
	case 3:
		return service.TrafficDirectorHealthStateHalfOpen, nil
	default:
		return "", fmt.Errorf("invalid traffic director health state code %d", code)
	}
}

func trafficDirectorHealthUnixMillis(value int64) time.Time {
	if value <= 0 {
		return time.Time{}
	}
	return time.UnixMilli(value).UTC()
}

func boolArg(value bool) string {
	if value {
		return "1"
	}
	return "0"
}
