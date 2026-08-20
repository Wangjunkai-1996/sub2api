package service

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"golang.org/x/sync/singleflight"
)

const (
	defaultTrafficDirectorPolicyL1Capacity = 256
	defaultTrafficDirectorPolicyRedisTTL   = 24 * time.Hour
	// Redis is an accelerator, so one unhealthy L2 command must not consume the
	// five-second PostgreSQL fallback budget used by an enforced request.
	defaultTrafficDirectorPolicyRedisTimeout = 100 * time.Millisecond
	defaultTrafficDirectorPolicyLoadTimeout  = 5 * time.Second

	TrafficDirectorPolicySourceL1     = "l1"
	TrafficDirectorPolicySourceL2     = "l2"
	TrafficDirectorPolicySourceDB     = "db"
	TrafficDirectorPolicySourceLegacy = "legacy"
)

// TrafficDirectorPolicyCache resolves an immutable policy version for a group head.
type TrafficDirectorPolicyCache interface {
	GetTrafficDirectorPolicy(ctx context.Context, head TrafficDirectorHead) (TrafficDirectorResolvedPolicy, error)
	Stats() TrafficDirectorPolicyCacheStats
}

// TrafficDirectorCompiledPolicyCache is the internal fast path used by the
// OpenAI scheduler. Implementations return an immutable compiled policy so the
// request path does not clone or revalidate a large spec on every hit.
type TrafficDirectorCompiledPolicyCache interface {
	GetTrafficDirectorCompiledPolicy(ctx context.Context, head TrafficDirectorHead) (TrafficDirectorResolvedPolicy, error)
}

// TrafficDirectorPolicyRedisCache is deliberately limited to immutable policy
// versions so the service owns fallback and enforcement semantics.
type TrafficDirectorPolicyRedisCache interface {
	GetTrafficDirectorPolicyVersion(ctx context.Context, groupID, version int64) (*TrafficDirectorVersion, error)
	SetTrafficDirectorPolicyVersion(ctx context.Context, policy *TrafficDirectorVersion, ttl time.Duration) error
}

// TrafficDirectorResolvedPolicy records both the selected immutable policy and
// whether resolution had to fail open to the synthetic legacy policy.
type TrafficDirectorResolvedPolicy struct {
	Version  TrafficDirectorVersion `json:"version"`
	Degraded bool                   `json:"degraded"`
	Source   string                 `json:"source"`
	compiled *trafficDirectorCompiledPolicy
}

// TrafficDirectorPolicyCacheStats is an atomic snapshot suitable for runtime
// status reporting. DBLoads counts actual store calls, including failed calls.
type TrafficDirectorPolicyCacheStats struct {
	L1Hits            uint64 `json:"l1_hits"`
	L2Hits            uint64 `json:"l2_hits"`
	DBLoads           uint64 `json:"db_loads"`
	DegradedFallbacks uint64 `json:"degraded_fallbacks"`
	Errors            uint64 `json:"errors"`
}

type trafficDirectorPolicyCache struct {
	store    TrafficDirectorPolicyStore
	redis    TrafficDirectorPolicyRedisCache
	redisTTL time.Duration
	l1       *trafficDirectorPolicyLRU
	loads    singleflight.Group

	l1Hits            atomic.Uint64
	l2Hits            atomic.Uint64
	dbLoads           atomic.Uint64
	degradedFallbacks atomic.Uint64
	errors            atomic.Uint64
}

type trafficDirectorPolicyLoadResult struct {
	version  TrafficDirectorVersion
	compiled *trafficDirectorCompiledPolicy
	source   string
}

// NewTrafficDirectorPolicyCache creates the process-LRU -> Redis -> PostgreSQL
// read chain. Non-positive limits use conservative defaults.
func NewTrafficDirectorPolicyCache(
	store TrafficDirectorPolicyStore,
	redisCache TrafficDirectorPolicyRedisCache,
	l1Capacity int,
	redisTTL time.Duration,
) TrafficDirectorPolicyCache {
	if l1Capacity <= 0 {
		l1Capacity = defaultTrafficDirectorPolicyL1Capacity
	}
	if redisTTL <= 0 {
		redisTTL = defaultTrafficDirectorPolicyRedisTTL
	}
	return &trafficDirectorPolicyCache{
		store:    store,
		redis:    redisCache,
		redisTTL: redisTTL,
		l1:       newTrafficDirectorPolicyLRU(l1Capacity),
	}
}

func (c *trafficDirectorPolicyCache) GetTrafficDirectorPolicy(
	ctx context.Context,
	head TrafficDirectorHead,
) (TrafficDirectorResolvedPolicy, error) {
	resolved, err := c.GetTrafficDirectorCompiledPolicy(ctx, head)
	if err != nil {
		return TrafficDirectorResolvedPolicy{}, err
	}
	// Keep the public cache API defensive. The OpenAI scheduler uses the
	// compiled fast path below and does not pay this large-spec clone cost.
	resolved.Version = cloneTrafficDirectorPolicyVersion(resolved.Version)
	resolved.compiled = nil
	return resolved, nil
}

func (c *trafficDirectorPolicyCache) GetTrafficDirectorCompiledPolicy(
	ctx context.Context,
	head TrafficDirectorHead,
) (TrafficDirectorResolvedPolicy, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if c == nil {
		return TrafficDirectorResolvedPolicy{}, ErrTrafficDirectorPolicyUnavailable
	}
	if head.GroupID <= 0 {
		return c.unavailable(head, "", fmt.Errorf("traffic director group ID must be positive"))
	}
	if head.Version < TrafficDirectorLegacyVersion {
		return c.unavailable(head, "", fmt.Errorf("traffic director version must not be negative"))
	}
	mode, ok := normalizeTrafficDirectorPolicyMode(head.Mode)
	if !ok {
		return c.unavailable(head, "", fmt.Errorf("invalid traffic director head mode %q", head.Mode))
	}
	if head.Version == TrafficDirectorLegacyVersion {
		if mode != domain.TrafficDirectorModeLegacy {
			return c.unavailable(
				head,
				mode,
				fmt.Errorf("traffic director version zero requires legacy mode"),
			)
		}
		return TrafficDirectorResolvedPolicy{
			Version: syntheticTrafficDirectorLegacyPolicy(head.GroupID),
			Source:  TrafficDirectorPolicySourceLegacy,
		}, nil
	}
	key := trafficDirectorPolicyKey{groupID: head.GroupID, version: head.Version}

	if entry, hit := c.l1.get(key); hit {
		if trafficDirectorPolicyEntryMatches(entry, head.GroupID, head.Version, mode) {
			c.l1Hits.Add(1)
			return resolvedCompiledTrafficDirectorPolicy(entry, TrafficDirectorPolicySourceL1), nil
		}
		c.l1.remove(key)
	}

	if c.redis != nil {
		redisCtx, cancel := context.WithTimeout(ctx, defaultTrafficDirectorPolicyRedisTimeout)
		cached, err := c.redis.GetTrafficDirectorPolicyVersion(redisCtx, head.GroupID, head.Version)
		cancel()
		if err == nil && cached != nil {
			validated, compiled, validationErr := compileValidatedTrafficDirectorPolicy(
				*cached,
				head.GroupID,
				head.Version,
				mode,
			)
			if validationErr == nil {
				c.l1.add(key, validated, compiled)
				c.l2Hits.Add(1)
				return TrafficDirectorResolvedPolicy{
					Version:  validated,
					Source:   TrafficDirectorPolicySourceL2,
					compiled: compiled,
				}, nil
			}
		}
	}

	loadResult := c.loads.DoChan(trafficDirectorPolicySingleflightKey(key, mode), func() (any, error) {
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultTrafficDirectorPolicyLoadTimeout)
		defer cancel()
		// A peer may have populated L1 after this caller's initial lookup.
		if entry, hit := c.l1.get(key); hit {
			if trafficDirectorPolicyEntryMatches(entry, head.GroupID, head.Version, mode) {
				c.l1Hits.Add(1)
				return trafficDirectorPolicyLoadResult{
					version:  entry.version,
					compiled: entry.compiled,
					source:   TrafficDirectorPolicySourceL1,
				}, nil
			}
			c.l1.remove(key)
		}
		if c.store == nil {
			return nil, fmt.Errorf("traffic director policy store is unavailable")
		}

		c.dbLoads.Add(1)
		stored, loadErr := c.store.GetTrafficDirectorVersion(loadCtx, head.GroupID, head.Version)
		if loadErr != nil {
			return nil, loadErr
		}
		if stored == nil {
			return nil, fmt.Errorf("traffic director policy store returned a nil version")
		}
		validated, compiled, validationErr := compileValidatedTrafficDirectorPolicy(
			*stored,
			head.GroupID,
			head.Version,
			mode,
		)
		if validationErr != nil {
			return nil, validationErr
		}

		c.l1.add(key, validated, compiled)
		if c.redis != nil {
			policy := cloneTrafficDirectorPolicyVersion(validated)
			redisCtx, cancel := context.WithTimeout(loadCtx, defaultTrafficDirectorPolicyRedisTimeout)
			_ = c.redis.SetTrafficDirectorPolicyVersion(redisCtx, &policy, c.redisTTL)
			cancel()
		}
		return trafficDirectorPolicyLoadResult{
			version:  validated,
			compiled: compiled,
			source:   TrafficDirectorPolicySourceDB,
		}, nil
	})
	var loaded any
	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case result := <-loadResult:
		loaded = result.Val
		err = result.Err
	}
	if err != nil {
		return c.unavailable(head, mode, err)
	}

	result, ok := loaded.(trafficDirectorPolicyLoadResult)
	if !ok {
		return c.unavailable(head, mode, fmt.Errorf("traffic director policy load returned an invalid result"))
	}
	// Singleflight is keyed by immutable coordinates. Check the cheap head
	// coordinates/mode here in case a malformed caller supplied a different mode.
	if !trafficDirectorPolicyEntryMatches(
		trafficDirectorPolicyCacheEntry{version: result.version, compiled: result.compiled},
		head.GroupID,
		head.Version,
		mode,
	) {
		return c.unavailable(head, mode, fmt.Errorf("traffic director policy load returned mismatched coordinates"))
	}
	return TrafficDirectorResolvedPolicy{
		Version:  result.version,
		Source:   result.source,
		compiled: result.compiled,
	}, nil
}

func (c *trafficDirectorPolicyCache) Stats() TrafficDirectorPolicyCacheStats {
	if c == nil {
		return TrafficDirectorPolicyCacheStats{}
	}
	return TrafficDirectorPolicyCacheStats{
		L1Hits:            c.l1Hits.Load(),
		L2Hits:            c.l2Hits.Load(),
		DBLoads:           c.dbLoads.Load(),
		DegradedFallbacks: c.degradedFallbacks.Load(),
		Errors:            c.errors.Load(),
	}
}

func (c *trafficDirectorPolicyCache) unavailable(
	head TrafficDirectorHead,
	mode string,
	cause error,
) (TrafficDirectorResolvedPolicy, error) {
	if mode == domain.TrafficDirectorModeShadow || mode == domain.TrafficDirectorModeLegacy {
		c.degradedFallbacks.Add(1)
		return TrafficDirectorResolvedPolicy{
			Version:  syntheticTrafficDirectorLegacyPolicy(head.GroupID),
			Degraded: true,
			Source:   TrafficDirectorPolicySourceLegacy,
		}, nil
	}

	c.errors.Add(1)
	return TrafficDirectorResolvedPolicy{}, ErrTrafficDirectorPolicyUnavailable.
		WithCause(cause).
		WithMetadata(map[string]string{
			"group_id": fmt.Sprintf("%d", head.GroupID),
			"version":  fmt.Sprintf("%d", head.Version),
		})
}

func normalizeTrafficDirectorPolicyMode(mode string) (string, bool) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch mode {
	case domain.TrafficDirectorModeLegacy,
		domain.TrafficDirectorModeShadow,
		domain.TrafficDirectorModeEnforced:
		return mode, true
	default:
		return "", false
	}
}

func validateResolvedTrafficDirectorPolicy(
	policy TrafficDirectorVersion,
	groupID int64,
	version int64,
	expectedMode string,
) (TrafficDirectorVersion, error) {
	if policy.GroupID != groupID || policy.Version != version {
		return TrafficDirectorVersion{}, fmt.Errorf(
			"traffic director policy coordinates mismatch: requested group=%d version=%d, got group=%d version=%d",
			groupID,
			version,
			policy.GroupID,
			policy.Version,
		)
	}

	mode, ok := normalizeTrafficDirectorPolicyMode(policy.Mode)
	if !ok {
		return TrafficDirectorVersion{}, fmt.Errorf("invalid traffic director policy mode %q", policy.Mode)
	}
	if mode != expectedMode {
		return TrafficDirectorVersion{}, fmt.Errorf(
			"traffic director policy mode mismatch: head=%s version=%s",
			expectedMode,
			mode,
		)
	}

	validated := cloneTrafficDirectorPolicyVersion(policy)
	validated.Mode = mode
	var checksum string
	switch mode {
	case domain.TrafficDirectorModeLegacy:
		if validated.Spec != nil {
			return TrafficDirectorVersion{}, fmt.Errorf("legacy traffic director policy must not include a spec")
		}
		checksum = TrafficDirectorLegacyChecksum()
	case domain.TrafficDirectorModeShadow, domain.TrafficDirectorModeEnforced:
		if validated.Spec == nil {
			return TrafficDirectorVersion{}, fmt.Errorf("%s traffic director policy requires a spec", mode)
		}
		normalized, canonical, err := normalizeTrafficDirectorSpec(*validated.Spec)
		if err != nil {
			return TrafficDirectorVersion{}, fmt.Errorf("validate traffic director policy spec: %w", err)
		}
		checksum = trafficDirectorCanonicalChecksum(canonical)
		validated.Spec = &normalized
	}

	if len(validated.Checksum) != sha256.Size*2 {
		return TrafficDirectorVersion{}, fmt.Errorf("traffic director policy checksum must be a 64-character SHA-256 hex value")
	}
	if _, err := hex.DecodeString(validated.Checksum); err != nil {
		return TrafficDirectorVersion{}, fmt.Errorf("traffic director policy checksum must be valid hex")
	}
	if !strings.EqualFold(validated.Checksum, checksum) {
		return TrafficDirectorVersion{}, fmt.Errorf("traffic director policy checksum mismatch")
	}
	validated.Checksum = checksum
	return validated, nil
}

func compileValidatedTrafficDirectorPolicy(
	policy TrafficDirectorVersion,
	groupID int64,
	version int64,
	expectedMode string,
) (TrafficDirectorVersion, *trafficDirectorCompiledPolicy, error) {
	validated, err := validateResolvedTrafficDirectorPolicy(policy, groupID, version, expectedMode)
	if err != nil {
		return TrafficDirectorVersion{}, nil, err
	}
	if validated.Spec == nil {
		return validated, nil, nil
	}
	compiled, err := compileNormalizedTrafficDirectorSpec(*validated.Spec)
	if err != nil {
		return TrafficDirectorVersion{}, nil, fmt.Errorf("compile traffic director policy: %w", err)
	}
	return validated, compiled, nil
}

func trafficDirectorCanonicalChecksum(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

func syntheticTrafficDirectorLegacyPolicy(groupID int64) TrafficDirectorVersion {
	return TrafficDirectorVersion{
		GroupID:  groupID,
		Version:  TrafficDirectorLegacyVersion,
		Mode:     domain.TrafficDirectorModeLegacy,
		Checksum: TrafficDirectorLegacyChecksum(),
	}
}

func resolvedCompiledTrafficDirectorPolicy(
	entry trafficDirectorPolicyCacheEntry,
	source string,
) TrafficDirectorResolvedPolicy {
	return TrafficDirectorResolvedPolicy{
		Version:  entry.version,
		Source:   source,
		compiled: entry.compiled,
	}
}

func cloneTrafficDirectorPolicyVersion(policy TrafficDirectorVersion) TrafficDirectorVersion {
	cloned := policy
	cloned.Spec = cloneTrafficDirectorSpec(policy.Spec)
	if policy.OperatorID != nil {
		operatorID := *policy.OperatorID
		cloned.OperatorID = &operatorID
	}
	if policy.RollbackFromVersion != nil {
		rollbackFromVersion := *policy.RollbackFromVersion
		cloned.RollbackFromVersion = &rollbackFromVersion
	}
	return cloned
}

type trafficDirectorPolicyKey struct {
	groupID int64
	version int64
}

func trafficDirectorPolicyEntryMatches(
	entry trafficDirectorPolicyCacheEntry,
	groupID int64,
	version int64,
	expectedMode string,
) bool {
	if entry.version.GroupID != groupID || entry.version.Version != version {
		return false
	}
	mode, ok := normalizeTrafficDirectorPolicyMode(entry.version.Mode)
	if !ok || mode != expectedMode {
		return false
	}
	if mode != domain.TrafficDirectorModeLegacy && entry.compiled == nil {
		return false
	}
	return true
}

func trafficDirectorPolicySingleflightKey(key trafficDirectorPolicyKey, mode string) string {
	return strconv.FormatInt(key.groupID, 10) + ":" + strconv.FormatInt(key.version, 10) + ":" + mode
}

type trafficDirectorPolicyLRU struct {
	mu       sync.Mutex
	capacity int
	items    map[trafficDirectorPolicyKey]*list.Element
	order    *list.List
}

type trafficDirectorPolicyLRUEntry struct {
	key      trafficDirectorPolicyKey
	version  TrafficDirectorVersion
	compiled *trafficDirectorCompiledPolicy
}

type trafficDirectorPolicyCacheEntry struct {
	version  TrafficDirectorVersion
	compiled *trafficDirectorCompiledPolicy
}

func newTrafficDirectorPolicyLRU(capacity int) *trafficDirectorPolicyLRU {
	return &trafficDirectorPolicyLRU{
		capacity: capacity,
		items:    make(map[trafficDirectorPolicyKey]*list.Element, capacity),
		order:    list.New(),
	}
}

func (l *trafficDirectorPolicyLRU) get(key trafficDirectorPolicyKey) (trafficDirectorPolicyCacheEntry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	element, ok := l.items[key]
	if !ok {
		return trafficDirectorPolicyCacheEntry{}, false
	}
	l.order.MoveToFront(element)
	entry := element.Value.(trafficDirectorPolicyLRUEntry)
	return trafficDirectorPolicyCacheEntry{version: entry.version, compiled: entry.compiled}, true
}

func (l *trafficDirectorPolicyLRU) add(
	key trafficDirectorPolicyKey,
	version TrafficDirectorVersion,
	compiled *trafficDirectorCompiledPolicy,
) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if element, ok := l.items[key]; ok {
		element.Value = trafficDirectorPolicyLRUEntry{
			key:      key,
			version:  cloneTrafficDirectorPolicyVersion(version),
			compiled: compiled,
		}
		l.order.MoveToFront(element)
		return
	}

	element := l.order.PushFront(trafficDirectorPolicyLRUEntry{
		key:      key,
		version:  cloneTrafficDirectorPolicyVersion(version),
		compiled: compiled,
	})
	l.items[key] = element
	if l.order.Len() <= l.capacity {
		return
	}

	oldest := l.order.Back()
	entry := oldest.Value.(trafficDirectorPolicyLRUEntry)
	delete(l.items, entry.key)
	l.order.Remove(oldest)
}

func (l *trafficDirectorPolicyLRU) remove(key trafficDirectorPolicyKey) {
	l.mu.Lock()
	defer l.mu.Unlock()
	element, ok := l.items[key]
	if !ok {
		return
	}
	delete(l.items, key)
	l.order.Remove(element)
}
