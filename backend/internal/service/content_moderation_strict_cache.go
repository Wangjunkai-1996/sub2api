package service

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
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
)

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
	ready     chan struct{}
	loading   bool
	cancel    context.CancelFunc
	results   []moderationAPIResult
	err       error
	expiresAt time.Time
	lru       *list.Element
	waiters   int
}

type strictModerationCacheOutcome uint8

const (
	strictModerationCacheOutcomeMiss strictModerationCacheOutcome = iota
	strictModerationCacheOutcomeHit
	strictModerationCacheOutcomeShared
)

var (
	errStrictModerationCacheSaturated = errors.New("strict moderation result cache is saturated")
	errStrictModerationCacheLoadPanic = errors.New("strict moderation shared load panicked")
)

func newStrictModerationResultCache(ttl time.Duration, capacity int) *strictModerationResultCache {
	if ttl <= 0 {
		ttl = strictModerationResultCacheTTL
	}
	if capacity <= 0 {
		capacity = strictModerationResultCacheCapacity
	}
	return &strictModerationResultCache{
		ttl:      ttl,
		capacity: capacity,
		now:      time.Now,
		entries:  make(map[[sha256.Size]byte]*strictModerationResultCacheEntry, capacity),
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

func strictModerationResultCacheKey(cfg *ContentModerationConfig, batch strictModerationBatch) ([sha256.Size]byte, bool) {
	var zero [sha256.Size]byte
	if cfg == nil || batch.expectedResults <= 0 {
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

	hasher := sha256.New()
	writeStrictModerationCacheHashField(hasher, "strict-moderation-result-v1")
	writeStrictModerationCacheHashField(hasher, endpoint)
	writeStrictModerationCacheHashField(hasher, cfg.Model)
	if cfg.ProxyID == nil {
		writeStrictModerationCacheHashField(hasher, "proxy:direct")
	} else {
		writeStrictModerationCacheHashField(hasher, "proxy:"+strconv.FormatInt(*cfg.ProxyID, 10))
	}
	writeStrictModerationCacheHashField(hasher, strconv.Itoa(batch.expectedResults))
	writeStrictModerationCacheHashField(hasher, text)

	// Invalidate entries when the configured credential set changes without
	// retaining any credential material in the cache key or entry.
	credentialHasher := sha256.New()
	for _, apiKey := range cfg.apiKeys() {
		writeStrictModerationCacheHashField(credentialHasher, apiKey)
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

func (c *strictModerationResultCache) do(
	ctx context.Context,
	key [sha256.Size]byte,
	newLoadContext func() (context.Context, context.CancelFunc),
	load func(context.Context) ([]moderationAPIResult, error),
) ([]moderationAPIResult, error, strictModerationCacheOutcome) {
	if c == nil || newLoadContext == nil || load == nil {
		return nil, errors.New("strict moderation result cache is unavailable"), strictModerationCacheOutcomeMiss
	}
	if ctx == nil {
		ctx = context.Background()
	}
	now := c.currentTime()
	c.mu.Lock()
	if entry := c.entries[key]; entry != nil {
		if entry.loading {
			entry.waiters++
			ready := entry.ready
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				c.releaseWaiter(entry, true)
				return nil, ctx.Err(), strictModerationCacheOutcomeShared
			case <-ready:
				c.releaseWaiter(entry, false)
				c.mu.Lock()
				results := cloneStrictModerationResults(entry.results)
				err := entry.err
				c.mu.Unlock()
				return results, err, strictModerationCacheOutcomeShared
			}
		}
		if now.Before(entry.expiresAt) {
			c.lru.MoveToFront(entry.lru)
			results := cloneStrictModerationResults(entry.results)
			c.mu.Unlock()
			return results, nil, strictModerationCacheOutcomeHit
		}
		c.removeLocked(entry)
	}
	if !c.reserveSlotLocked(now) {
		c.mu.Unlock()
		return nil, errStrictModerationCacheSaturated, strictModerationCacheOutcomeMiss
	}
	loadCtx, cancel := newLoadContext()
	entry := &strictModerationResultCacheEntry{
		key: key, ready: make(chan struct{}), loading: true, cancel: cancel, waiters: 1,
	}
	c.entries[key] = entry
	ready := entry.ready
	c.mu.Unlock()
	go c.loadEntry(key, entry, loadCtx, load)

	select {
	case <-ctx.Done():
		c.releaseWaiter(entry, true)
		return nil, ctx.Err(), strictModerationCacheOutcomeMiss
	case <-ready:
		c.releaseWaiter(entry, false)
		c.mu.Lock()
		results := cloneStrictModerationResults(entry.results)
		err := entry.err
		c.mu.Unlock()
		return results, err, strictModerationCacheOutcomeMiss
	}
}

func (c *strictModerationResultCache) loadEntry(
	key [sha256.Size]byte,
	entry *strictModerationResultCacheEntry,
	loadCtx context.Context,
	load func(context.Context) ([]moderationAPIResult, error),
) {
	var results []moderationAPIResult
	var err error
	defer func() {
		if recover() != nil {
			err = errStrictModerationCacheLoadPanic
		}
		c.finishLoad(key, entry, results, err)
	}()
	results, err = load(loadCtx)
}

func (c *strictModerationResultCache) finishLoad(
	key [sha256.Size]byte,
	entry *strictModerationResultCacheEntry,
	results []moderationAPIResult,
	err error,
) {
	stored := cloneStrictModerationResults(results)
	c.mu.Lock()
	ready := entry.ready
	cancel := entry.cancel
	entry.cancel = nil
	entry.loading = false
	entry.results = stored
	entry.err = err
	if current := c.entries[key]; current == entry {
		if err != nil {
			delete(c.entries, key)
		} else {
			entry.expiresAt = c.currentTime().Add(c.ttl)
			entry.ready = nil
			entry.lru = c.lru.PushFront(entry)
		}
	}
	close(ready)
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *strictModerationResultCache) releaseWaiter(entry *strictModerationResultCacheEntry, canceled bool) {
	if c == nil || entry == nil {
		return
	}
	var cancel context.CancelFunc
	c.mu.Lock()
	if entry.waiters > 0 {
		entry.waiters--
	}
	if canceled && entry.loading && entry.waiters == 0 {
		if current := c.entries[entry.key]; current == entry {
			delete(c.entries, entry.key)
		}
		cancel = entry.cancel
		entry.cancel = nil
	}
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func strictModerationSharedLoadContext(ctx context.Context, cfg *ContentModerationConfig) (context.Context, context.CancelFunc) {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	timeout := time.Duration(defaultContentModerationTimeoutMS) * time.Millisecond
	if cfg != nil && cfg.TimeoutMS > 0 {
		timeout = time.Duration(cfg.TimeoutMS) * time.Millisecond
	}
	maxTimeout := time.Duration(maxContentModerationTimeoutMS) * time.Millisecond
	if timeout > maxTimeout {
		timeout = maxTimeout
	}
	return context.WithTimeout(base, timeout)
}

func (c *strictModerationResultCache) currentTime() time.Time {
	if c != nil && c.now != nil {
		return c.now()
	}
	return time.Now()
}

func (c *strictModerationResultCache) reserveSlotLocked(now time.Time) bool {
	if len(c.entries) < c.capacity {
		return true
	}
	for element := c.lru.Back(); element != nil; {
		previous := element.Prev()
		entry := element.Value.(*strictModerationResultCacheEntry)
		if !now.Before(entry.expiresAt) {
			c.removeLocked(entry)
		}
		element = previous
	}
	if len(c.entries) < c.capacity {
		return true
	}
	if element := c.lru.Back(); element != nil {
		c.removeLocked(element.Value.(*strictModerationResultCacheEntry))
	}
	return len(c.entries) < c.capacity
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
	fields := []any{
		"moderation_http_status", diagnostics.StatusCode,
		"moderation_key_hash", diagnostics.KeyHash,
	}
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
