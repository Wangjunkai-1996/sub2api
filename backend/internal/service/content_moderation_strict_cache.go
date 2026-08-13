package service

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	strictModerationResultCacheTTL      = 2 * time.Minute
	strictModerationResultCacheCapacity = 2048
	strictModerationPoolCooldown        = time.Minute
)

var (
	errStrictModerationRateLimited = errors.New("strict moderation local rate limit reached")
	errStrictModerationCircuitOpen = errors.New("strict moderation endpoint circuit is open")
)

type strictModerationCircuitState uint8

const (
	strictModerationCircuitClosed strictModerationCircuitState = iota
	strictModerationCircuitOpen
	strictModerationCircuitHalfOpen
)

type strictModerationPool struct {
	mu           sync.Mutex
	sem          chan struct{}
	requestTimes []time.Time
	circuit      strictModerationCircuitState
	openUntil    time.Time
	probeActive  bool
	probeDone    chan struct{}
	now          func() time.Time
}

type strictModerationPoolLease struct {
	pool     *strictModerationPool
	probe    bool
	trackRPM bool
	once     sync.Once
}

func (l *strictModerationPoolLease) release() {
	if l == nil || l.pool == nil {
		return
	}
	l.once.Do(func() { <-l.pool.sem })
}

type strictModerationResultCache struct {
	mu       sync.Mutex
	ttl      time.Duration
	capacity int
	now      func() time.Time
	entries  map[[sha256.Size]byte]*strictModerationResultCacheEntry
	lru      list.List
}

type strictModerationResultCacheEntry struct {
	key       [sha256.Size]byte
	results   []moderationAPIResult
	expiresAt time.Time
	lru       *list.Element
}

func newStrictModerationResultCache(ttl time.Duration, capacity int) *strictModerationResultCache {
	if ttl <= 0 {
		ttl = strictModerationResultCacheTTL
	}
	if capacity <= 0 {
		capacity = strictModerationResultCacheCapacity
	}
	return &strictModerationResultCache{
		ttl: ttl, capacity: capacity, now: time.Now,
		entries: make(map[[sha256.Size]byte]*strictModerationResultCacheEntry, capacity),
	}
}

func (s *ContentModerationService) strictModerationCache() *strictModerationResultCache {
	if s == nil {
		return nil
	}
	if cache := s.strictResultCache.Load(); cache != nil {
		return cache
	}
	cache := newStrictModerationResultCache(strictModerationResultCacheTTL, strictModerationResultCacheCapacity)
	if s.strictResultCache.CompareAndSwap(nil, cache) {
		return cache
	}
	return s.strictResultCache.Load()
}

func strictModerationResultCacheKey(cfg *ContentModerationConfig, batch strictModerationBatch, states ...*strictModerationKeyState) ([sha256.Size]byte, bool) {
	var zero [sha256.Size]byte
	if cfg == nil || batch.expectedResults <= 0 || len(states) == 0 || states[0] == nil {
		return zero, false
	}
	state := states[0]
	if state.clientAPIKeyID <= 0 || !state.groupIDPresent || strings.TrimSpace(state.requestEndpoint) == "" {
		return zero, false
	}
	text, ok := batch.input.(string)
	if !ok || strings.TrimSpace(text) == "" {
		return zero, false
	}
	endpoint, err := url.JoinPath(strings.TrimRight(cfg.BaseURL, "/"), "/v1/moderations")
	if err != nil {
		return zero, false
	}

	textHash := sha256.Sum256([]byte(normalizeContentModerationText(text)))
	hasher := sha256.New()
	writeStrictModerationCacheHashField(hasher, "strict-moderation-result-v3")
	writeStrictModerationCacheHashField(hasher, "api-key-id:"+strconv.FormatInt(state.clientAPIKeyID, 10))
	writeStrictModerationCacheHashField(hasher, "group-id:"+strconv.FormatInt(state.groupID, 10))
	writeStrictModerationCacheHashField(hasher, "request-endpoint:"+strings.TrimSpace(state.requestEndpoint))
	writeStrictModerationCacheHashField(hasher, endpoint)
	writeStrictModerationCacheHashField(hasher, cfg.Model)
	if cfg.ProxyID == nil {
		writeStrictModerationCacheHashField(hasher, "proxy:direct")
	} else {
		writeStrictModerationCacheHashField(hasher, "proxy:"+strconv.FormatInt(*cfg.ProxyID, 10))
	}
	writeStrictModerationCacheHashField(hasher, strconv.Itoa(batch.expectedResults))
	writeStrictModerationCacheHashBytes(hasher, textHash[:])

	credentialHasher := sha256.New()
	for _, apiKey := range cfg.apiKeys() {
		writeStrictModerationCacheHashField(credentialHasher, moderationAPIKeyHash(apiKey))
	}
	writeStrictModerationCacheHashBytes(hasher, credentialHasher.Sum(nil))

	var key [sha256.Size]byte
	copy(key[:], hasher.Sum(nil))
	return key, true
}

func writeStrictModerationCacheHashField(hasher hash.Hash, value string) {
	writeStrictModerationCacheHashBytes(hasher, []byte(value))
}

func writeStrictModerationCacheHashBytes(hasher hash.Hash, value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = hasher.Write(size[:])
	_, _ = hasher.Write(value)
}

func (c *strictModerationResultCache) get(key [sha256.Size]byte) ([]moderationAPIResult, bool) {
	if c == nil {
		return nil, false
	}
	now := c.currentTime()
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil {
		return nil, false
	}
	if !now.Before(entry.expiresAt) {
		c.removeLocked(entry)
		return nil, false
	}
	c.lru.MoveToFront(entry.lru)
	return cloneStrictModerationResults(entry.results), true
}

func (c *strictModerationResultCache) put(key [sha256.Size]byte, results []moderationAPIResult) {
	if c == nil || len(results) == 0 {
		return
	}
	now := c.currentTime()
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing := c.entries[key]; existing != nil {
		existing.results = cloneStrictModerationResults(results)
		existing.expiresAt = now.Add(c.ttl)
		c.lru.MoveToFront(existing.lru)
		return
	}
	for len(c.entries) >= c.capacity {
		oldest := c.lru.Back()
		if oldest == nil {
			break
		}
		c.removeLocked(oldest.Value.(*strictModerationResultCacheEntry))
	}
	entry := &strictModerationResultCacheEntry{key: key, results: cloneStrictModerationResults(results), expiresAt: now.Add(c.ttl)}
	entry.lru = c.lru.PushFront(entry)
	c.entries[key] = entry
}

func (c *strictModerationResultCache) currentTime() time.Time {
	if c != nil && c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *strictModerationResultCache) removeLocked(entry *strictModerationResultCacheEntry) {
	if entry == nil {
		return
	}
	if entry.lru != nil {
		c.lru.Remove(entry.lru)
		entry.lru = nil
	}
	if current := c.entries[entry.key]; current == entry {
		delete(c.entries, entry.key)
	}
}

func (s *ContentModerationService) acquireStrictModerationPool(ctx context.Context, cfg *ContentModerationConfig) (*strictModerationPoolLease, error) {
	if s == nil || cfg == nil {
		return nil, errors.New("strict moderation config is unavailable")
	}
	pool, err := s.strictModerationPool(cfg)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		select {
		case pool.sem <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}

		now := pool.currentTime()
		pool.mu.Lock()
		trackRPM := cfg.MaxRPM > 0
		if trackRPM {
			pool.pruneRequestsLocked(now)
		}
		switch pool.circuit {
		case strictModerationCircuitOpen:
			if now.Before(pool.openUntil) {
				until := pool.openUntil
				pool.mu.Unlock()
				<-pool.sem
				return nil, fmt.Errorf("%w until %s", errStrictModerationCircuitOpen, until.UTC().Format(time.RFC3339))
			}
			pool.circuit = strictModerationCircuitHalfOpen
			pool.openUntil = time.Time{}
		case strictModerationCircuitHalfOpen:
			if pool.probeActive {
				probeDone := pool.probeDone
				pool.mu.Unlock()
				<-pool.sem
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-probeDone:
					continue
				}
			}
		}
		if trackRPM && len(pool.requestTimes) >= cfg.MaxRPM {
			pool.mu.Unlock()
			<-pool.sem
			return nil, errStrictModerationRateLimited
		}
		probe := pool.circuit == strictModerationCircuitHalfOpen
		if probe {
			pool.probeActive = true
			pool.probeDone = make(chan struct{})
		}
		pool.mu.Unlock()
		return &strictModerationPoolLease{pool: pool, probe: probe, trackRPM: trackRPM}, nil
	}
}

func (l *strictModerationPoolLease) recordDispatch() {
	if l == nil || l.pool == nil || !l.trackRPM {
		return
	}
	now := l.pool.currentTime()
	l.pool.mu.Lock()
	l.pool.pruneRequestsLocked(now)
	l.pool.requestTimes = append(l.pool.requestTimes, now)
	l.pool.mu.Unlock()
}

func (l *strictModerationPoolLease) recordResult(resultErr error) {
	if l == nil || l.pool == nil {
		return
	}
	pool := l.pool
	now := pool.currentTime()
	pool.mu.Lock()
	defer pool.mu.Unlock()
	var apiErr *moderationAPIError
	is429 := errors.As(resultErr, &apiErr) && apiErr.StatusCode == http.StatusTooManyRequests
	if is429 {
		if l.probe {
			pool.circuit = strictModerationCircuitClosed
			pool.openUntil = time.Time{}
		}
	} else if l.probe {
		if resultErr == nil {
			pool.circuit = strictModerationCircuitClosed
			pool.openUntil = time.Time{}
		} else {
			pool.circuit = strictModerationCircuitOpen
			pool.openUntil = now.Add(strictModerationPoolCooldown)
		}
	}
	if l.probe && pool.probeActive {
		pool.probeActive = false
		if pool.probeDone != nil {
			close(pool.probeDone)
			pool.probeDone = nil
		}
	}
}

func (p *strictModerationPool) currentTime() time.Time {
	if p != nil && p.now != nil {
		return p.now()
	}
	return time.Now()
}

func (p *strictModerationPool) pruneRequestsLocked(now time.Time) {
	cutoff := now.Add(-time.Minute)
	first := 0
	for first < len(p.requestTimes) && !p.requestTimes[first].After(cutoff) {
		first++
	}
	if first > 0 {
		p.requestTimes = append(p.requestTimes[:0], p.requestTimes[first:]...)
	}
}

func (s *ContentModerationService) strictModerationPool(cfg *ContentModerationConfig) (*strictModerationPool, error) {
	key, err := strictModerationPoolKey(cfg)
	if err != nil {
		return nil, err
	}
	s.strictPoolMu.Lock()
	defer s.strictPoolMu.Unlock()
	if s.strictPools == nil {
		s.strictPools = make(map[string]*strictModerationPool)
	}
	pool := s.strictPools[key]
	if pool == nil {
		pool = &strictModerationPool{sem: make(chan struct{}, 1), now: time.Now}
		s.strictPools[key] = pool
	}
	return pool, nil
}

func strictModerationPoolKey(cfg *ContentModerationConfig) (string, error) {
	if cfg == nil {
		return "", errors.New("strict moderation config is unavailable")
	}
	endpoint, err := url.JoinPath(strings.TrimRight(cfg.BaseURL, "/"), "/v1/moderations")
	if err != nil {
		return "", err
	}
	proxy := "direct"
	if cfg.ProxyID != nil {
		proxy = strconv.FormatInt(*cfg.ProxyID, 10)
	}
	digest := sha256.Sum256([]byte(endpoint + "\x00" + cfg.Model + "\x00" + proxy))
	return fmt.Sprintf("%x", digest[:]), nil
}

func cloneStrictModerationResults(results []moderationAPIResult) []moderationAPIResult {
	if results == nil {
		return nil
	}
	cloned := make([]moderationAPIResult, len(results))
	for index := range results {
		cloned[index] = results[index]
		if results[index].CategoryScores != nil {
			cloned[index].CategoryScores = make(map[string]float64, len(results[index].CategoryScores))
			for category, score := range results[index].CategoryScores {
				cloned[index].CategoryScores[category] = score
			}
		}
	}
	return cloned
}

type moderationAPIResponseDiagnostics struct {
	StatusCode             int
	KeyHash                string
	RequestID              string
	LimitRequests          string
	RemainingRequests      string
	ResetRequests          string
	LimitTokens            string
	RemainingTokens        string
	ResetTokens            string
	LimitProjectTokens     string
	RemainingProjectTokens string
	ResetProjectTokens     string
}

func captureModerationAPIResponseDiagnostics(resp *http.Response, apiKey string) moderationAPIResponseDiagnostics {
	diagnostics := moderationAPIResponseDiagnostics{KeyHash: moderationAPIKeyHash(apiKey)}
	if resp == nil {
		return diagnostics
	}
	diagnostics.StatusCode = resp.StatusCode
	diagnostics.RequestID = safeModerationHeader(resp.Header.Get("x-request-id"))
	diagnostics.LimitRequests = safeModerationHeader(resp.Header.Get("x-ratelimit-limit-requests"))
	diagnostics.RemainingRequests = safeModerationHeader(resp.Header.Get("x-ratelimit-remaining-requests"))
	diagnostics.ResetRequests = safeModerationHeader(resp.Header.Get("x-ratelimit-reset-requests"))
	diagnostics.LimitTokens = safeModerationHeader(resp.Header.Get("x-ratelimit-limit-tokens"))
	diagnostics.RemainingTokens = safeModerationHeader(resp.Header.Get("x-ratelimit-remaining-tokens"))
	diagnostics.ResetTokens = safeModerationHeader(resp.Header.Get("x-ratelimit-reset-tokens"))
	diagnostics.LimitProjectTokens = safeModerationHeader(resp.Header.Get("x-ratelimit-limit-project-tokens"))
	diagnostics.RemainingProjectTokens = safeModerationHeader(resp.Header.Get("x-ratelimit-remaining-project-tokens"))
	diagnostics.ResetProjectTokens = safeModerationHeader(resp.Header.Get("x-ratelimit-reset-project-tokens"))
	return diagnostics
}

func moderationAPISuccessLogFields(diagnostics moderationAPIResponseDiagnostics) []any {
	fields := []any{"moderation_http_status", diagnostics.StatusCode, "moderation_key_hash", diagnostics.KeyHash}
	appendField := func(key, value string) {
		if value != "" {
			fields = append(fields, key, value)
		}
	}
	appendField("openai_request_id", diagnostics.RequestID)
	appendField("ratelimit_limit_requests", diagnostics.LimitRequests)
	appendField("ratelimit_remaining_requests", diagnostics.RemainingRequests)
	appendField("ratelimit_reset_requests", diagnostics.ResetRequests)
	appendField("ratelimit_limit_tokens", diagnostics.LimitTokens)
	appendField("ratelimit_remaining_tokens", diagnostics.RemainingTokens)
	appendField("ratelimit_reset_tokens", diagnostics.ResetTokens)
	appendField("ratelimit_limit_project_tokens", diagnostics.LimitProjectTokens)
	appendField("ratelimit_remaining_project_tokens", diagnostics.RemainingProjectTokens)
	appendField("ratelimit_reset_project_tokens", diagnostics.ResetProjectTokens)
	return fields
}
