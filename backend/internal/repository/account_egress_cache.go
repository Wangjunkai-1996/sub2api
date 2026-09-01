package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/redis/go-redis/v9"
)

const accountEgressKeyPrefix = "concurrency:egress:"

// Every account-egress key uses one {acct:N} hash tag. Legacy account and Live
// admissions maintain tagged mirror ZSETs so the pool allocator never mixes
// historical, untagged concurrency keys into one Lua invocation.

var (
	accountEgressSyncConfigScript = redis.NewScript(`
		redis.replicate_commands()
		local key = KEYS[1]
		local incomingVersion = tonumber(ARGV[1])
		local incomingDigest = ARGV[2]
		local currentVersionRaw = redis.call('HGET', key, 'version')
		if currentVersionRaw ~= false then
			local currentVersion = tonumber(currentVersionRaw)
			if currentVersion > incomingVersion then
				return 'CONFIG_STALE'
			end
			if currentVersion == incomingVersion then
				if redis.call('HGET', key, 'digest') ~= incomingDigest then
					return 'CONFIG_CONFLICT'
				end
				return 'OK'
			end
		end

		redis.call('DEL', key)
		redis.call('HSET', key,
			'version', ARGV[1],
			'digest', incomingDigest,
			'limit', ARGV[3],
			'max_waiting', ARGV[4])
		local candidateCount = tonumber(ARGV[5])
		local offset = 6
		for i = 1, candidateCount do
			redis.call('HSET', key, 'binding:' .. ARGV[offset], ARGV[offset + 1])
			offset = offset + 2
		end
		return 'OK'
	`)

	accountEgressAcquireScript = redis.NewScript(`
		redis.replicate_commands()
		local configKey = KEYS[1]
		local waitersKey = KEYS[2]
		local rrKey = KEYS[3]
		local exclusiveKey = KEYS[4]
		local metadataKey = KEYS[5]
		local modeKey = KEYS[6]
		local poolTotalKey = KEYS[7]
		local legacyRegularKey = KEYS[8]
		local legacyLiveKey = KEYS[9]

		local expectedVersion = tonumber(ARGV[1])
		local expectedDigest = ARGV[2]
		local limit = tonumber(ARGV[3])
		local maxWaiting = tonumber(ARGV[4])
		local leaseTTL = tonumber(ARGV[5])
		local waiterTTL = tonumber(ARGV[6])
		local leaseID = ARGV[7]
		local leaseMember = ARGV[8]
		local requiredBindingHash = ARGV[9]
		local preferredBindingHash = ARGV[10]
		local candidateCount = tonumber(ARGV[11])
		local legacyRegularTTL = tonumber(ARGV[12])
		local legacyLiveTTL = tonumber(ARGV[13])

		local storedVersionRaw = redis.call('HGET', configKey, 'version')
		if storedVersionRaw == false then
			return {'CONFIG_UNAVAILABLE', '', '0', '', 0, 0, 0, 0, expectedVersion}
		end
		local storedVersion = tonumber(storedVersionRaw)
		if storedVersion ~= expectedVersion or redis.call('HGET', configKey, 'digest') ~= expectedDigest then
			return {'CONFIG_STALE', '', '0', '', 0, 0, 0, 0, storedVersion}
		end
		if tonumber(redis.call('HGET', configKey, 'limit')) ~= limit then
			return {'CONFIG_STALE', '', '0', '', 0, 0, 0, 0, storedVersion}
		end

		local timeResult = redis.call('TIME')
		local nowSeconds = tonumber(timeResult[1])
		local nowMicrosPart = tonumber(timeResult[2])
		local nowMicros = nowSeconds * 1000000 + nowMicrosPart
		local nowMillis = math.floor(nowMicros / 1000)
		local leaseCutoff = nowMillis - leaseTTL
		local waiterCutoff = nowMicros - waiterTTL * 1000

		redis.call('ZREMRANGEBYSCORE', waitersKey, '-inf', waiterCutoff)
		redis.call('ZREMRANGEBYSCORE', poolTotalKey, '-inf', leaseCutoff)
		redis.call('ZREMRANGEBYSCORE', legacyRegularKey, '-inf', nowSeconds - legacyRegularTTL)
		redis.call('ZREMRANGEBYSCORE', legacyLiveKey, '-inf', nowSeconds - legacyLiveTTL)
		local identityKeyCount = #KEYS - 9
		if identityKeyCount % 3 ~= 0 then
			return {'CONFIG_STALE', '', '0', '', 0, 0, 0, nowMillis, storedVersion}
		end
		local identityCount = identityKeyCount / 3
		local identityLoads = {}
		local legacyIdentityLoads = {}
		local activeTotal = 0
		local mappedLegacyTotal = 0
		for identityIndex = 1, identityCount do
			local identityKey = KEYS[9 + identityIndex]
			local legacyRegularIdentityKey = KEYS[9 + identityCount + identityIndex]
			local legacyLiveIdentityKey = KEYS[9 + identityCount * 2 + identityIndex]
			redis.call('ZREMRANGEBYSCORE', identityKey, '-inf', leaseCutoff)
			redis.call('ZREMRANGEBYSCORE', legacyRegularIdentityKey, '-inf', nowSeconds - legacyRegularTTL)
			redis.call('ZREMRANGEBYSCORE', legacyLiveIdentityKey, '-inf', nowSeconds - legacyLiveTTL)
			local load = redis.call('ZCARD', identityKey)
			local legacyLoad = redis.call('ZCARD', legacyRegularIdentityKey) + redis.call('ZCARD', legacyLiveIdentityKey)
			identityLoads[identityIndex] = load
			legacyIdentityLoads[identityIndex] = legacyLoad
			activeTotal = activeTotal + load
			mappedLegacyTotal = mappedLegacyTotal + legacyLoad
		end
		local candidates = {}
		local eligibleIdentities = {}
		local effectiveCapacity = 0
		local primaryIdentityIndex = 0
		local offset = 14
		for candidateIndex = 1, candidateCount do
			local candidate = {
				bindingID = ARGV[offset],
				bindingHash = ARGV[offset + 1],
				routeID = ARGV[offset + 2],
				identityID = ARGV[offset + 3],
				identityHash = ARGV[offset + 4],
				position = tonumber(ARGV[offset + 5]),
				primary = tonumber(ARGV[offset + 6]),
				healthy = tonumber(ARGV[offset + 7]),
				identityIndex = tonumber(ARGV[offset + 8])
			}
			local expectedMapping = candidate.identityHash .. ':' .. ARGV[offset + 7] .. ':' .. candidate.routeID
			if redis.call('HGET', configKey, 'binding:' .. candidate.bindingHash) ~= expectedMapping then
				return {'CONFIG_STALE', '', '0', '', activeTotal, effectiveCapacity, redis.call('ZCARD', waitersKey), nowMillis, storedVersion}
			end
			candidates[candidateIndex] = candidate
			if candidate.primary == 1 then
				primaryIdentityIndex = candidate.identityIndex
			end
			if candidate.healthy == 1 and candidate.identityIndex > 0 then
				eligibleIdentities[candidate.identityIndex] = true
			end
			offset = offset + 9
		end
		local eligibleIdentityCount = 0
		for identityIndex = 1, identityCount do
			if eligibleIdentities[identityIndex] == true then
				eligibleIdentityCount = eligibleIdentityCount + 1
			end
		end
		effectiveCapacity = eligibleIdentityCount * limit

		local function result(status, candidate)
			local waiting = redis.call('ZCARD', waitersKey)
			if candidate == nil then
				return {status, '', '0', '', activeTotal, effectiveCapacity, waiting, nowMillis, storedVersion}
			end
			return {status, candidate.bindingID, candidate.routeID, candidate.identityID, activeTotal, effectiveCapacity, waiting, nowMillis, storedVersion}
		end

		local function enqueue(status)
			if redis.call('ZSCORE', waitersKey, leaseMember) ~= false then
				return result(status, nil)
			end
			if maxWaiting <= 0 then
				return result(status, nil)
			end
			if redis.call('ZCARD', waitersKey) >= maxWaiting then
				return result('QUEUE_FULL', nil)
			end
			local lastScore = tonumber(redis.call('HGET', rrKey, 'wait_score') or '0')
			local nextScore = nowMicros
			if lastScore >= nextScore then
				nextScore = lastScore + 1
			end
			redis.call('HSET', rrKey, 'wait_score', nextScore)
			redis.call('ZADD', waitersKey, 'NX', nextScore, leaseMember)
			redis.call('PEXPIRE', waitersKey, waiterTTL * 2)
			return result(status, nil)
		end

		local poolTotalCount = redis.call('ZCARD', poolTotalKey)
		if poolTotalCount ~= activeTotal then
			return result('CONFIG_STALE', nil)
		end
		local legacyActive = redis.call('ZCARD', legacyRegularKey) + redis.call('ZCARD', legacyLiveKey)
		local mode = redis.call('GET', modeKey)
		local blockNewPool = false
		if mode == false then
			if poolTotalCount > 0 then
				redis.call('SET', modeKey, 'pool')
				mode = 'pool'
			elseif legacyActive > 0 then
				redis.call('SET', modeKey, 'transition')
				mode = 'transition'
			else
				redis.call('SET', modeKey, 'pool')
				mode = 'pool'
			end
		end
		if mode == 'legacy' then
			if poolTotalCount > 0 then
				return result('CONFIG_STALE', nil)
			end
			if legacyActive > 0 then
				redis.call('SET', modeKey, 'transition')
				mode = 'transition'
			else
				redis.call('SET', modeKey, 'pool')
				mode = 'pool'
			end
		elseif mode == 'transition' then
			if legacyActive == 0 then
				redis.call('SET', modeKey, 'pool')
				mode = 'pool'
			end
		elseif mode == 'to_legacy' then
			blockNewPool = true
		elseif mode == 'pool' then
			if legacyActive > 0 then
				return result('CONFIG_STALE', nil)
			end
		else
			return result('CONFIG_STALE', nil)
		end
		if mode == 'transition' and legacyActive > 0 then
			if primaryIdentityIndex <= 0 then
				return result('CONFIG_STALE', nil)
			end
			if mappedLegacyTotal ~= legacyActive then
				return enqueue('LEGACY_DRAINING')
			end
			for identityIndex = 1, identityCount do
				identityLoads[identityIndex] = identityLoads[identityIndex] + legacyIdentityLoads[identityIndex]
			end
			activeTotal = activeTotal + legacyActive
		end

		local metadataBindingHash = redis.call('HGET', metadataKey, 'binding_hash')
		if metadataBindingHash ~= false then
			if tonumber(redis.call('HGET', metadataKey, 'version') or '0') ~= expectedVersion then
				return result('CONFIG_STALE', nil)
			end
			local metadataIdentityHash = redis.call('HGET', metadataKey, 'identity_hash')
			local metadataCandidate = nil
			for _, candidate in ipairs(candidates) do
				if candidate.bindingHash == metadataBindingHash and candidate.identityHash == metadataIdentityHash then
					metadataCandidate = candidate
					break
				end
			end
			if metadataCandidate == nil or metadataCandidate.identityIndex <= 0 then
				return result('CONFIG_STALE', nil)
			end
			local identityKey = KEYS[9 + metadataCandidate.identityIndex]
			if redis.call('ZSCORE', identityKey, leaseMember) ~= false and redis.call('ZSCORE', poolTotalKey, leaseMember) ~= false then
				redis.call('ZADD', identityKey, 'XX', nowMillis, leaseMember)
				redis.call('ZADD', poolTotalKey, 'XX', nowMillis, leaseMember)
				redis.call('PEXPIRE', identityKey, leaseTTL * 2)
				redis.call('PEXPIRE', poolTotalKey, leaseTTL * 2)
				redis.call('PEXPIRE', metadataKey, leaseTTL * 2)
				redis.call('ZREM', waitersKey, leaseMember)
				return result('ACQUIRED', metadataCandidate)
			end
			redis.call('DEL', metadataKey)
		end

		for identityIndex = 1, identityCount do
			if redis.call('ZSCORE', KEYS[9 + identityIndex], leaseMember) ~= false then
				return result('CONFIG_STALE', nil)
			end
		end
		if redis.call('ZSCORE', poolTotalKey, leaseMember) ~= false then
			return result('CONFIG_STALE', nil)
		end
		if blockNewPool then
			redis.call('ZREM', waitersKey, leaseMember)
			return result('LEGACY_DRAINING', nil)
		end

		local waiterScore = redis.call('ZSCORE', waitersKey, leaseMember)
		local head = redis.call('ZRANGE', waitersKey, 0, 0)[1]
		if waiterScore ~= false and head ~= leaseMember then
			return result('NOT_QUEUE_HEAD', nil)
		end
		if waiterScore == false and head ~= nil then
			return enqueue('NOT_QUEUE_HEAD')
		end
		if redis.call('EXISTS', exclusiveKey) == 1 then
			return enqueue('EXCLUSIVE')
		end

		local selected = nil
		if requiredBindingHash ~= '' then
			local requiredCandidate = nil
			for _, candidate in ipairs(candidates) do
				if candidate.bindingHash == requiredBindingHash then
					requiredCandidate = candidate
					break
				end
			end
			if requiredCandidate == nil or requiredCandidate.healthy ~= 1 or requiredCandidate.identityIndex <= 0 then
				redis.call('ZREM', waitersKey, leaseMember)
				return result('REQUIRED_BINDING_UNAVAILABLE', nil)
			end
			if identityLoads[requiredCandidate.identityIndex] >= limit then
				return enqueue('FULL')
			end
			selected = requiredCandidate
		end

		if selected == nil and preferredBindingHash ~= '' then
			for _, candidate in ipairs(candidates) do
				if candidate.bindingHash == preferredBindingHash and candidate.healthy == 1 and candidate.identityIndex > 0 and identityLoads[candidate.identityIndex] < limit then
					selected = candidate
					break
				end
			end
		end

		if selected == nil then
			local minLoad = nil
			local tiedIdentities = {}
			for identityIndex = 1, identityCount do
				local load = identityLoads[identityIndex]
				if eligibleIdentities[identityIndex] == true and load < limit then
					if minLoad == nil or load < minLoad then
						minLoad = load
						tiedIdentities = {identityIndex}
					elseif load == minLoad then
						table.insert(tiedIdentities, identityIndex)
					end
				end
			end
			if #tiedIdentities == 0 then
				if eligibleIdentityCount == 0 then
					redis.call('ZREM', waitersKey, leaseMember)
					return result('NO_ELIGIBLE_EGRESS', nil)
				end
				return enqueue('FULL')
			end
			local rr = redis.call('HINCRBY', rrKey, 'selection', 1)
			local selectedIdentity = tiedIdentities[((rr - 1) % #tiedIdentities) + 1]
			for _, candidate in ipairs(candidates) do
				if candidate.healthy == 1 and candidate.identityIndex == selectedIdentity then
					selected = candidate
					break
				end
			end
		end

		if selected == nil then
			redis.call('ZREM', waitersKey, leaseMember)
			return result('NO_ELIGIBLE_EGRESS', nil)
		end
		local selectedIdentityKey = KEYS[9 + selected.identityIndex]
		if redis.call('ZADD', selectedIdentityKey, 'NX', nowMillis, leaseMember) ~= 1 then
			return result('CONFIG_STALE', nil)
		end
		if redis.call('ZADD', poolTotalKey, 'NX', nowMillis, leaseMember) ~= 1 then
			redis.call('ZREM', selectedIdentityKey, leaseMember)
			return result('CONFIG_STALE', nil)
		end
		redis.call('PEXPIRE', selectedIdentityKey, leaseTTL * 2)
		redis.call('PEXPIRE', poolTotalKey, leaseTTL * 2)
		redis.call('HSET', metadataKey,
			'lease_id', leaseID,
			'lease_member', leaseMember,
			'binding_id', selected.bindingID,
			'binding_hash', selected.bindingHash,
			'route_id', selected.routeID,
			'identity_id', selected.identityID,
			'identity_hash', selected.identityHash,
			'version', storedVersion)
		redis.call('PEXPIRE', metadataKey, leaseTTL * 2)
		redis.call('ZREM', waitersKey, leaseMember)
		if redis.call('ZCARD', waitersKey) == 0 then
			redis.call('DEL', waitersKey)
		end
		identityLoads[selected.identityIndex] = identityLoads[selected.identityIndex] + 1
		activeTotal = activeTotal + 1
		return result('ACQUIRED', selected)
	`)

	accountEgressRefreshScript = redis.NewScript(`
		redis.replicate_commands()
		local metadataKey = KEYS[1]
		local identityKey = KEYS[2]
		local poolTotalKey = KEYS[3]
		local modeKey = KEYS[4]
		local leaseMember = ARGV[1]
		local expectedIdentityHash = ARGV[2]
		local ttl = tonumber(ARGV[3])
		local timeResult = redis.call('TIME')
		local nowMillis = tonumber(timeResult[1]) * 1000 + math.floor(tonumber(timeResult[2]) / 1000)
		redis.call('ZREMRANGEBYSCORE', identityKey, '-inf', nowMillis - ttl)
		redis.call('ZREMRANGEBYSCORE', poolTotalKey, '-inf', nowMillis - ttl)
		local mode = redis.call('GET', modeKey)
		if mode ~= 'pool' and mode ~= 'transition' and mode ~= 'to_legacy' then
			return 0
		end
		if redis.call('HGET', metadataKey, 'lease_member') ~= leaseMember then
			return 0
		end
		if redis.call('HGET', metadataKey, 'identity_hash') ~= expectedIdentityHash then
			return 0
		end
		if redis.call('ZSCORE', identityKey, leaseMember) == false or redis.call('ZSCORE', poolTotalKey, leaseMember) == false then
			return 0
		end
		redis.call('ZADD', identityKey, 'XX', nowMillis, leaseMember)
		redis.call('ZADD', poolTotalKey, 'XX', nowMillis, leaseMember)
		redis.call('PEXPIRE', identityKey, ttl * 2)
		redis.call('PEXPIRE', poolTotalKey, ttl * 2)
		redis.call('PEXPIRE', metadataKey, ttl * 2)
		return 1
	`)

	accountEgressReleaseScript = redis.NewScript(`
		local metadataKey = KEYS[1]
		local identityKey = KEYS[2]
		local poolTotalKey = KEYS[3]
		local modeKey = KEYS[4]
		local leaseMember = ARGV[1]
		local expectedIdentityHash = ARGV[2]
		local metadataMember = redis.call('HGET', metadataKey, 'lease_member')
		if metadataMember ~= false then
			if metadataMember ~= leaseMember or redis.call('HGET', metadataKey, 'identity_hash') ~= expectedIdentityHash then
				return 0
			end
		end
		redis.call('ZREM', identityKey, leaseMember)
		redis.call('ZREM', poolTotalKey, leaseMember)
		redis.call('DEL', metadataKey)
		if redis.call('GET', modeKey) == 'to_legacy' and redis.call('ZCARD', poolTotalKey) == 0 then
			redis.call('SET', modeKey, 'legacy')
		end
		return 1
	`)

	accountEgressBeginPoolTransitionScript = redis.NewScript(`
		local modeKey = KEYS[1]
		local poolTotalKey = KEYS[2]
		local legacyRegularKey = KEYS[3]
		local legacyLiveKey = KEYS[4]
		local poolTTL = tonumber(ARGV[1])
		local legacyRegularTTL = tonumber(ARGV[2])
		local legacyLiveTTL = tonumber(ARGV[3])
		local timeResult = redis.call('TIME')
		local nowSeconds = tonumber(timeResult[1])
		local nowMillis = nowSeconds * 1000 + math.floor(tonumber(timeResult[2]) / 1000)
		redis.call('ZREMRANGEBYSCORE', poolTotalKey, '-inf', nowMillis - poolTTL)
		redis.call('ZREMRANGEBYSCORE', legacyRegularKey, '-inf', nowSeconds - legacyRegularTTL)
		redis.call('ZREMRANGEBYSCORE', legacyLiveKey, '-inf', nowSeconds - legacyLiveTTL)
		local legacyActive = redis.call('ZCARD', legacyRegularKey) + redis.call('ZCARD', legacyLiveKey)
		if legacyActive > 0 then
			redis.call('SET', modeKey, 'transition')
			return 'transition'
		end
		redis.call('SET', modeKey, 'pool')
		return 'pool'
	`)

	accountEgressBeginLegacyTransitionScript = redis.NewScript(`
		local modeKey = KEYS[1]
		local poolTotalKey = KEYS[2]
		local poolTTL = tonumber(ARGV[1])
		local timeResult = redis.call('TIME')
		local nowMillis = tonumber(timeResult[1]) * 1000 + math.floor(tonumber(timeResult[2]) / 1000)
		redis.call('ZREMRANGEBYSCORE', poolTotalKey, '-inf', nowMillis - poolTTL)
		if redis.call('ZCARD', poolTotalKey) > 0 then
			redis.call('SET', modeKey, 'to_legacy')
			return 'to_legacy'
		end
		redis.call('SET', modeKey, 'legacy')
		return 'legacy'
	`)

	accountEgressLoadSnapshotScript = redis.NewScript(`
		redis.replicate_commands()
		local configKey = KEYS[1]
		local poolTotalKey = KEYS[2]
		local legacyRegularKey = KEYS[3]
		local legacyLiveKey = KEYS[4]
		local modeKey = KEYS[5]
		local waitersKey = KEYS[6]
		local exclusiveKey = KEYS[7]
		local leaseTTL = tonumber(ARGV[1])
		local waiterTTL = tonumber(ARGV[2])
		local legacyRegularTTL = tonumber(ARGV[3])
		local legacyLiveTTL = tonumber(ARGV[4])

		local timeResult = redis.call('TIME')
		local nowSeconds = tonumber(timeResult[1])
		local nowMicros = nowSeconds * 1000000 + tonumber(timeResult[2])
		local nowMillis = math.floor(nowMicros / 1000)
		redis.call('ZREMRANGEBYSCORE', poolTotalKey, '-inf', nowMillis - leaseTTL)
		redis.call('ZREMRANGEBYSCORE', legacyRegularKey, '-inf', nowSeconds - legacyRegularTTL)
		redis.call('ZREMRANGEBYSCORE', legacyLiveKey, '-inf', nowSeconds - legacyLiveTTL)
		redis.call('ZREMRANGEBYSCORE', waitersKey, '-inf', nowMicros - waiterTTL * 1000)

		local legacyRegularCount = redis.call('ZCARD', legacyRegularKey)
		local legacyLiveCount = redis.call('ZCARD', legacyLiveKey)
		local mode = redis.call('GET', modeKey) or ''
		if mode == 'transition' and legacyRegularCount + legacyLiveCount == 0 then
			redis.call('SET', modeKey, 'pool')
			mode = 'pool'
		elseif mode == 'to_legacy' and redis.call('ZCARD', poolTotalKey) == 0 then
			redis.call('SET', modeKey, 'legacy')
			mode = 'legacy'
		end

		local result = {
			redis.call('HGET', configKey, 'version') or '',
			redis.call('HGET', configKey, 'digest') or '',
			redis.call('HGET', configKey, 'limit') or '',
			mode,
			redis.call('ZCARD', poolTotalKey),
			legacyRegularCount,
			legacyLiveCount,
			redis.call('ZCARD', waitersKey),
			redis.call('EXISTS', exclusiveKey)
		}
		local identityKeyCount = #KEYS - 7
		if identityKeyCount % 3 ~= 0 then return result end
		local identityCount = identityKeyCount / 3
		for identityIndex = 1, identityCount do
			local identityKey = KEYS[7 + identityIndex]
			redis.call('ZREMRANGEBYSCORE', identityKey, '-inf', nowMillis - leaseTTL)
			table.insert(result, redis.call('ZCARD', identityKey))
		end
		for identityIndex = 1, identityCount do
			local identityKey = KEYS[7 + identityCount + identityIndex]
			redis.call('ZREMRANGEBYSCORE', identityKey, '-inf', nowSeconds - legacyRegularTTL)
			table.insert(result, redis.call('ZCARD', identityKey))
		end
		for identityIndex = 1, identityCount do
			local identityKey = KEYS[7 + identityCount * 2 + identityIndex]
			redis.call('ZREMRANGEBYSCORE', identityKey, '-inf', nowSeconds - legacyLiveTTL)
			table.insert(result, redis.call('ZCARD', identityKey))
		end
		return result
	`)
)

func NewAccountEgressCache(rdb *redis.Client) service.AccountEgressCache {
	return &concurrencyCache{
		rdb:                 rdb,
		slotTTLSeconds:      defaultSlotTTLMinutes * 60,
		waitQueueTTLSeconds: defaultSlotTTLMinutes * 60,
	}
}

var _ service.AccountEgressCache = (*concurrencyCache)(nil)

func accountEgressHashTag(accountID int64) string {
	return "{acct:" + strconv.FormatInt(accountID, 10) + "}"
}

func accountEgressBaseKey(accountID int64) string {
	return accountEgressKeyPrefix + accountEgressHashTag(accountID)
}

func accountEgressConfigKey(accountID int64) string {
	return accountEgressBaseKey(accountID) + ":config"
}

func accountEgressWaitersKey(accountID int64) string {
	return accountEgressBaseKey(accountID) + ":waiters"
}

func accountEgressRRKey(accountID int64) string {
	return accountEgressBaseKey(accountID) + ":rr"
}

func accountEgressExclusiveKey(accountID int64) string {
	return accountEgressBaseKey(accountID) + ":exclusive"
}

func accountEgressModeKey(accountID int64) string {
	return accountEgressBaseKey(accountID) + ":mode"
}

func accountEgressTotalKey(accountID int64) string {
	return accountEgressBaseKey(accountID) + ":total"
}

func accountEgressLegacyRegularKey(accountID int64) string {
	return accountEgressBaseKey(accountID) + ":legacy:regular"
}

func accountEgressLegacyLiveKey(accountID int64) string {
	return accountEgressBaseKey(accountID) + ":legacy:live"
}

func accountEgressLegacyRegularIdentityKey(accountID int64, identityID string) string {
	return accountEgressBaseKey(accountID) + ":legacy:identity:" + accountEgressIDHash(identityID) + ":regular"
}

func accountEgressLegacyLiveIdentityKey(accountID int64, identityID string) string {
	return accountEgressBaseKey(accountID) + ":legacy:identity:" + accountEgressIDHash(identityID) + ":live"
}

func accountEgressIdentityKey(accountID int64, identityID string) string {
	return accountEgressBaseKey(accountID) + ":identity:" + accountEgressIDHash(identityID) + ":leases"
}

func accountEgressLeaseKey(accountID int64, leaseID string) string {
	return accountEgressBaseKey(accountID) + ":lease:" + accountEgressIDHash(leaseID)
}

func accountEgressIDHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func accountEgressDurationMilliseconds(duration time.Duration) int64 {
	milliseconds := duration.Milliseconds()
	if duration%time.Millisecond != 0 {
		milliseconds++
	}
	if milliseconds <= 0 {
		return 1
	}
	return milliseconds
}

func accountEgressBool(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func accountEgressBindingMapping(candidate service.AccountEgressCandidate) string {
	return accountEgressIDHash(candidate.IdentityID) + ":" + accountEgressBool(candidate.Healthy) + ":" + strconv.FormatInt(candidate.RouteID, 10)
}

func (c *concurrencyCache) SyncAccountEgressConfigs(
	ctx context.Context,
	configs []service.AccountEgressPoolConfig,
) (map[int64]service.AccountEgressConfigSyncStatus, error) {
	results := make(map[int64]service.AccountEgressConfigSyncStatus, len(configs))
	if len(configs) == 0 {
		return results, nil
	}
	if c == nil || c.rdb == nil {
		return nil, errors.New("account egress redis cache is unavailable")
	}

	pipe := c.rdb.Pipeline()
	type syncCommand struct {
		accountID int64
		cmd       *redis.Cmd
	}
	commands := make([]syncCommand, 0, len(configs))
	seen := make(map[int64]struct{}, len(configs))
	for _, config := range configs {
		if _, exists := seen[config.AccountID]; exists {
			return nil, fmt.Errorf("duplicate account egress config for account %d", config.AccountID)
		}
		seen[config.AccountID] = struct{}{}
		digest, err := config.Digest()
		if err != nil {
			return nil, err
		}
		candidates := config.SortedCandidates()
		args := make([]any, 0, 5+len(candidates)*2)
		args = append(args, config.Version, digest, config.PerIdentityConcurrency, config.MaxWaiting, len(candidates))
		for _, candidate := range candidates {
			args = append(args, accountEgressIDHash(candidate.BindingID), accountEgressBindingMapping(candidate))
		}
		commands = append(commands, syncCommand{
			accountID: config.AccountID,
			// A pipelined EVALSHA cannot observe NOSCRIPT until Exec, which is too
			// late for Script.Run's fallback. EVAL keeps cold starts atomic.
			cmd: accountEgressSyncConfigScript.Eval(ctx, pipe, []string{accountEgressConfigKey(config.AccountID)}, args...),
		})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("sync account egress configs: %w", err)
	}
	for _, command := range commands {
		status, err := command.cmd.Text()
		if err != nil {
			return nil, fmt.Errorf("read account %d egress config sync result: %w", command.accountID, err)
		}
		results[command.accountID] = service.AccountEgressConfigSyncStatus(status)
	}
	return results, nil
}

func (c *concurrencyCache) AcquireAccountEgress(
	ctx context.Context,
	request service.AccountEgressCacheAcquireRequest,
) (service.AccountEgressAcquireResult, error) {
	if c == nil || c.rdb == nil {
		return service.AccountEgressAcquireResult{}, errors.New("account egress redis cache is unavailable")
	}
	if err := request.Config.Validate(); err != nil {
		return service.AccountEgressAcquireResult{}, err
	}
	if request.LeaseID == "" {
		return service.AccountEgressAcquireResult{}, errors.New("account egress lease id is required")
	}
	digest, err := request.Config.Digest()
	if err != nil {
		return service.AccountEgressAcquireResult{}, err
	}
	candidates := request.Config.SortedCandidates()

	identityIndexes := make(map[string]int, len(candidates))
	identities := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if _, exists := identityIndexes[candidate.IdentityID]; !exists {
			identities = append(identities, candidate.IdentityID)
			identityIndexes[candidate.IdentityID] = len(identities)
		}
	}

	keys := []string{
		accountEgressConfigKey(request.Config.AccountID),
		accountEgressWaitersKey(request.Config.AccountID),
		accountEgressRRKey(request.Config.AccountID),
		accountEgressExclusiveKey(request.Config.AccountID),
		accountEgressLeaseKey(request.Config.AccountID, request.LeaseID),
		accountEgressModeKey(request.Config.AccountID),
		accountEgressTotalKey(request.Config.AccountID),
		accountEgressLegacyRegularKey(request.Config.AccountID),
		accountEgressLegacyLiveKey(request.Config.AccountID),
	}
	for _, identityID := range identities {
		keys = append(keys, accountEgressIdentityKey(request.Config.AccountID, identityID))
	}
	for _, identityID := range identities {
		keys = append(keys, accountEgressLegacyRegularIdentityKey(request.Config.AccountID, identityID))
	}
	for _, identityID := range identities {
		keys = append(keys, accountEgressLegacyLiveIdentityKey(request.Config.AccountID, identityID))
	}

	args := make([]any, 0, 13+len(candidates)*9)
	args = append(args,
		request.Config.Version,
		digest,
		request.Config.PerIdentityConcurrency,
		request.Config.MaxWaiting,
		accountEgressDurationMilliseconds(request.LeaseTTL),
		accountEgressDurationMilliseconds(request.WaiterTTL),
		request.LeaseID,
		accountEgressIDHash(request.LeaseID),
		accountEgressIDHashOrEmpty(request.RequiredBindingID),
		accountEgressIDHashOrEmpty(request.PreferredBindingID),
		len(candidates),
		c.slotTTLSeconds,
		liveLeaseTTLSeconds,
	)
	for _, candidate := range candidates {
		identityIndex := identityIndexes[candidate.IdentityID]
		args = append(args,
			candidate.BindingID,
			accountEgressIDHash(candidate.BindingID),
			candidate.RouteID,
			candidate.IdentityID,
			accountEgressIDHash(candidate.IdentityID),
			candidate.Position,
			accountEgressBool(candidate.Primary),
			accountEgressBool(candidate.Healthy),
			identityIndex,
		)
	}

	raw, err := accountEgressAcquireScript.Run(ctx, c.rdb, keys, args...).Result()
	if err != nil {
		return service.AccountEgressAcquireResult{}, err
	}
	return parseAccountEgressAcquireResult(request.LeaseID, raw)
}

func accountEgressIDHashOrEmpty(value string) string {
	if value == "" {
		return ""
	}
	return accountEgressIDHash(value)
}

func parseAccountEgressAcquireResult(leaseID string, raw any) (service.AccountEgressAcquireResult, error) {
	values, ok := raw.([]any)
	if !ok || len(values) != 9 {
		return service.AccountEgressAcquireResult{}, fmt.Errorf("invalid account egress acquire result: %#v", raw)
	}
	intAt := func(index int) (int64, error) {
		return strconv.ParseInt(fmt.Sprint(values[index]), 10, 64)
	}
	routeID, err := intAt(2)
	if err != nil {
		return service.AccountEgressAcquireResult{}, fmt.Errorf("parse route id: %w", err)
	}
	activeTotal, err := intAt(4)
	if err != nil {
		return service.AccountEgressAcquireResult{}, fmt.Errorf("parse active total: %w", err)
	}
	effectiveCapacity, err := intAt(5)
	if err != nil {
		return service.AccountEgressAcquireResult{}, fmt.Errorf("parse effective capacity: %w", err)
	}
	waitingCount, err := intAt(6)
	if err != nil {
		return service.AccountEgressAcquireResult{}, fmt.Errorf("parse waiting count: %w", err)
	}
	redisMillis, err := intAt(7)
	if err != nil {
		return service.AccountEgressAcquireResult{}, fmt.Errorf("parse redis time: %w", err)
	}
	version, err := intAt(8)
	if err != nil {
		return service.AccountEgressAcquireResult{}, fmt.Errorf("parse config version: %w", err)
	}
	return service.AccountEgressAcquireResult{
		Status:            service.AccountEgressStatus(fmt.Sprint(values[0])),
		LeaseID:           leaseID,
		BindingID:         fmt.Sprint(values[1]),
		RouteID:           routeID,
		IdentityID:        fmt.Sprint(values[3]),
		ActiveTotal:       int(activeTotal),
		EffectiveCapacity: int(effectiveCapacity),
		WaitingCount:      int(waitingCount),
		RedisTime:         time.UnixMilli(redisMillis),
		ConfigVersion:     version,
	}, nil
}

func (c *concurrencyCache) RemoveAccountEgressWaiter(ctx context.Context, accountID int64, leaseID string) error {
	if c == nil || c.rdb == nil || accountID <= 0 || leaseID == "" {
		return nil
	}
	return c.rdb.ZRem(ctx, accountEgressWaitersKey(accountID), accountEgressIDHash(leaseID)).Err()
}

func (c *concurrencyCache) RefreshAccountEgressLeases(
	ctx context.Context,
	leases []service.AccountEgressLeaseRef,
	ttl time.Duration,
) (map[string]bool, error) {
	results := make(map[string]bool, len(leases))
	if len(leases) == 0 {
		return results, nil
	}
	if c == nil || c.rdb == nil {
		return nil, errors.New("account egress redis cache is unavailable")
	}
	pipe := c.rdb.Pipeline()
	type refreshCommand struct {
		key string
		cmd *redis.Cmd
	}
	commands := make([]refreshCommand, 0, len(leases))
	for _, lease := range leases {
		if lease.AccountID <= 0 || lease.ID == "" || lease.IdentityID == "" {
			return nil, errors.New("invalid account egress lease reference")
		}
		member := accountEgressIDHash(lease.ID)
		commands = append(commands, refreshCommand{
			key: lease.Key(),
			cmd: accountEgressRefreshScript.Eval(ctx, pipe, []string{
				accountEgressLeaseKey(lease.AccountID, lease.ID),
				accountEgressIdentityKey(lease.AccountID, lease.IdentityID),
				accountEgressTotalKey(lease.AccountID),
				accountEgressModeKey(lease.AccountID),
			}, member, accountEgressIDHash(lease.IdentityID), accountEgressDurationMilliseconds(ttl)),
		})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("refresh account egress leases: %w", err)
	}
	for _, command := range commands {
		value, err := command.cmd.Int()
		if err != nil {
			return nil, err
		}
		results[command.key] = value == 1
	}
	return results, nil
}

func (c *concurrencyCache) ReleaseAccountEgressLease(ctx context.Context, lease service.AccountEgressLeaseRef) error {
	if c == nil || c.rdb == nil || lease.AccountID <= 0 || lease.ID == "" || lease.IdentityID == "" {
		return nil
	}
	member := accountEgressIDHash(lease.ID)
	return accountEgressReleaseScript.Run(ctx, c.rdb, []string{
		accountEgressLeaseKey(lease.AccountID, lease.ID),
		accountEgressIdentityKey(lease.AccountID, lease.IdentityID),
		accountEgressTotalKey(lease.AccountID),
		accountEgressModeKey(lease.AccountID),
	}, member, accountEgressIDHash(lease.IdentityID)).Err()
}

// BeginAccountEgressPoolTransition fences new legacy admissions before pool
// selection starts. Existing legacy leases remain refreshable while their
// admission-time identity mirrors are drained or bridged.
func (c *concurrencyCache) BeginAccountEgressPoolTransition(ctx context.Context, accountID int64) (string, error) {
	if c == nil || c.rdb == nil || accountID <= 0 {
		return "", errors.New("account egress redis cache is unavailable")
	}
	return accountEgressBeginPoolTransitionScript.Run(ctx, c.rdb, []string{
		accountEgressModeKey(accountID),
		accountEgressTotalKey(accountID),
		accountEgressLegacyRegularKey(accountID),
		accountEgressLegacyLiveKey(accountID),
	}, accountEgressDurationMilliseconds(service.AccountEgressLeaseTTL), c.slotTTLSeconds, liveLeaseTTLSeconds).Text()
}

// BeginAccountEgressLegacyTransition fences new pool and legacy admissions
// until existing pool leases drain. The final pool release advances to legacy.
func (c *concurrencyCache) BeginAccountEgressLegacyTransition(ctx context.Context, accountID int64) (string, error) {
	if c == nil || c.rdb == nil || accountID <= 0 {
		return "", errors.New("account egress redis cache is unavailable")
	}
	return accountEgressBeginLegacyTransitionScript.Run(ctx, c.rdb, []string{
		accountEgressModeKey(accountID),
		accountEgressTotalKey(accountID),
	}, accountEgressDurationMilliseconds(service.AccountEgressLeaseTTL)).Text()
}

func (c *concurrencyCache) GetAccountEgressLoadsBatch(
	ctx context.Context,
	configs []service.AccountEgressPoolConfig,
	leaseTTL time.Duration,
	waiterTTL time.Duration,
) (map[int64]service.AccountEgressLoadInfo, error) {
	results := make(map[int64]service.AccountEgressLoadInfo, len(configs))
	if len(configs) == 0 {
		return results, nil
	}
	if c == nil || c.rdb == nil {
		return nil, errors.New("account egress redis cache is unavailable")
	}

	type loadCommands struct {
		config      service.AccountEgressPoolConfig
		digest      string
		identityIDs []string
		snapshotCmd *redis.Cmd
	}
	pipe := c.rdb.Pipeline()
	commands := make([]loadCommands, 0, len(configs))
	seen := make(map[int64]struct{}, len(configs))
	for _, config := range configs {
		if _, exists := seen[config.AccountID]; exists {
			return nil, fmt.Errorf("duplicate account egress config for account %d", config.AccountID)
		}
		seen[config.AccountID] = struct{}{}
		digest, err := config.Digest()
		if err != nil {
			return nil, err
		}
		identityIDs := make(map[string]struct{}, len(config.Candidates))
		for _, candidate := range config.Candidates {
			identityIDs[candidate.IdentityID] = struct{}{}
		}
		orderedIdentities := make([]string, 0, len(identityIDs))
		for identityID := range identityIDs {
			orderedIdentities = append(orderedIdentities, identityID)
		}
		sort.Strings(orderedIdentities)
		keys := []string{
			accountEgressConfigKey(config.AccountID),
			accountEgressTotalKey(config.AccountID),
			accountEgressLegacyRegularKey(config.AccountID),
			accountEgressLegacyLiveKey(config.AccountID),
			accountEgressModeKey(config.AccountID),
			accountEgressWaitersKey(config.AccountID),
			accountEgressExclusiveKey(config.AccountID),
		}
		for _, identityID := range orderedIdentities {
			keys = append(keys, accountEgressIdentityKey(config.AccountID, identityID))
		}
		for _, identityID := range orderedIdentities {
			keys = append(keys, accountEgressLegacyRegularIdentityKey(config.AccountID, identityID))
		}
		for _, identityID := range orderedIdentities {
			keys = append(keys, accountEgressLegacyLiveIdentityKey(config.AccountID, identityID))
		}
		commands = append(commands, loadCommands{
			config:      config,
			digest:      digest,
			identityIDs: orderedIdentities,
			// EVAL keeps cleanup and every count in one account-scoped atomic snapshot.
			snapshotCmd: accountEgressLoadSnapshotScript.Eval(ctx, pipe, keys,
				accountEgressDurationMilliseconds(leaseTTL),
				accountEgressDurationMilliseconds(waiterTTL),
				c.slotTTLSeconds,
				liveLeaseTTLSeconds,
			),
		})
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("load account egress pipeline: %w", err)
	}

	for _, command := range commands {
		values, err := command.snapshotCmd.Slice()
		if err != nil {
			return nil, fmt.Errorf("read account %d egress load snapshot: %w", command.config.AccountID, err)
		}
		if len(values) != 9+len(command.identityIDs)*3 {
			return nil, fmt.Errorf("invalid account %d egress load snapshot length: got %d, want %d", command.config.AccountID, len(values), 9+len(command.identityIDs)*3)
		}
		if fmt.Sprint(values[0]) == "" {
			results[command.config.AccountID] = service.AccountEgressLoadInfo{
				AccountID:     command.config.AccountID,
				Status:        service.AccountEgressStatusConfigUnavailable,
				ConfigVersion: command.config.Version,
			}
			continue
		}
		storedVersion, versionErr := strconv.ParseInt(fmt.Sprint(values[0]), 10, 64)
		storedLimit, limitErr := strconv.Atoi(fmt.Sprint(values[2]))
		if versionErr != nil || limitErr != nil || storedVersion != command.config.Version || fmt.Sprint(values[1]) != command.digest || storedLimit != command.config.PerIdentityConcurrency {
			results[command.config.AccountID] = service.AccountEgressLoadInfo{
				AccountID:     command.config.AccountID,
				Status:        service.AccountEgressStatusConfigStale,
				ConfigVersion: command.config.Version,
			}
			continue
		}
		intAt := func(index int, label string) (int, error) {
			value, parseErr := strconv.Atoi(fmt.Sprint(values[index]))
			if parseErr != nil {
				return 0, fmt.Errorf("parse account %d egress %s: %w", command.config.AccountID, label, parseErr)
			}
			return value, nil
		}
		poolActiveTotal := 0
		identityLoads := make(map[string]int, len(command.identityIDs))
		for index, identityID := range command.identityIDs {
			active, parseErr := intAt(9+index, "identity load")
			if parseErr != nil {
				return nil, parseErr
			}
			identityLoads[identityID] = active
			poolActiveTotal += active
		}
		totalCount, err := intAt(4, "total count")
		if err != nil {
			return nil, err
		}
		if totalCount != poolActiveTotal {
			results[command.config.AccountID] = service.AccountEgressLoadInfo{
				AccountID:     command.config.AccountID,
				Status:        service.AccountEgressStatusConfigStale,
				ActiveTotal:   poolActiveTotal,
				IdentityLoads: identityLoads,
				ConfigVersion: command.config.Version,
			}
			continue
		}
		waitingCount, err := intAt(7, "waiting count")
		if err != nil {
			return nil, err
		}
		legacyRegular, err := intAt(5, "legacy regular count")
		if err != nil {
			return nil, err
		}
		legacyLive, err := intAt(6, "legacy live count")
		if err != nil {
			return nil, err
		}
		legacyActive := legacyRegular + legacyLive
		activeTotal := poolActiveTotal + legacyActive
		mappedLegacyTotal := 0
		identityCount := len(command.identityIDs)
		for index, identityID := range command.identityIDs {
			legacyRegularLoad, parseErr := intAt(9+identityCount+index, "legacy regular identity load")
			if parseErr != nil {
				return nil, parseErr
			}
			legacyLiveLoad, parseErr := intAt(9+identityCount*2+index, "legacy live identity load")
			if parseErr != nil {
				return nil, parseErr
			}
			legacyIdentityLoad := legacyRegularLoad + legacyLiveLoad
			identityLoads[identityID] += legacyIdentityLoad
			mappedLegacyTotal += legacyIdentityLoad
		}
		effectiveCapacity := command.config.EffectiveCapacity()
		status := service.AccountEgressStatusAcquired
		loadRate := 0
		if effectiveCapacity == 0 {
			status = service.AccountEgressStatusNoEligibleEgress
		} else {
			loadRate = (activeTotal + waitingCount) * 100 / effectiveCapacity
		}
		exclusiveCount, err := intAt(8, "exclusive count")
		if err != nil {
			return nil, err
		}
		if exclusiveCount > 0 {
			status = service.AccountEgressStatusExclusive
			if loadRate < 100 {
				loadRate = 100
			}
		}
		mode := fmt.Sprint(values[3])
		if mode != "" && mode != "legacy" && mode != "transition" && mode != "to_legacy" && mode != "pool" {
			status = service.AccountEgressStatusConfigStale
		} else if (mode == "pool" && legacyActive > 0) || (mode == "legacy" && poolActiveTotal > 0) {
			status = service.AccountEgressStatusConfigStale
		} else if mappedLegacyTotal != legacyActive {
			status = service.AccountEgressStatusLegacyDraining
			if loadRate < 100 {
				loadRate = 100
			}
		}
		results[command.config.AccountID] = service.AccountEgressLoadInfo{
			AccountID:         command.config.AccountID,
			Status:            status,
			ActiveTotal:       activeTotal,
			IdentityLoads:     identityLoads,
			WaitingCount:      waitingCount,
			EffectiveCapacity: effectiveCapacity,
			LoadRate:          loadRate,
			ConfigVersion:     storedVersion,
		}
	}
	return results, nil
}
