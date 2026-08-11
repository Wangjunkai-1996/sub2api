package service

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func strictModerationCacheTestScores(score float64) map[string]float64 {
	scores := make(map[string]float64, len(contentModerationCategoryOrder))
	for _, category := range contentModerationCategoryOrder {
		scores[category] = score
	}
	return scores
}

func writeStrictModerationCacheTestResponse(w http.ResponseWriter, flagged bool, score float64) {
	_ = json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{
		Flagged: flagged, CategoryScores: strictModerationCacheTestScores(score),
	}}})
}

func strictModerationCacheTestConfig(baseURL, model string, apiKeys ...string) *ContentModerationConfig {
	cfg := defaultContentModerationConfig()
	cfg.BaseURL = baseURL
	cfg.Model = model
	cfg.APIKeys = append([]string(nil), apiKeys...)
	cfg.RetryCount = 0
	cfg.TimeoutMS = 2000
	return cfg
}

func strictModerationCacheTestService(client *http.Client, cache *strictModerationResultCache) *ContentModerationService {
	svc := NewContentModerationService(nil, nil, nil, nil, nil, nil, nil, nil)
	svc.httpClient = client
	if cache != nil {
		svc.strictResultCache.Store(cache)
	}
	return svc
}

type strictModerationCacheCallResult struct {
	result *moderationAPIResult
	state  *strictModerationKeyState
	err    error
}

func callStrictModerationCacheTest(
	ctx context.Context,
	svc *ContentModerationService,
	cfg *ContentModerationConfig,
	text string,
) strictModerationCacheCallResult {
	state := newStrictModerationKeyState(cfg)
	result, err := svc.callModerationStrictBatches(ctx, cfg, []strictModerationBatch{{
		input: text, expectedResults: 1,
	}}, state)
	return strictModerationCacheCallResult{result: result, state: state, err: err}
}

func TestStrictModerationResultCacheSerialHitCachesFlaggedAndUnflagged(t *testing.T) {
	for _, flagged := range []bool{false, true} {
		t.Run(fmt.Sprintf("flagged_%t", flagged), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				writeStrictModerationCacheTestResponse(w, flagged, 0.01)
			}))
			defer server.Close()

			cache := newStrictModerationResultCache(time.Minute, 8)
			svc := strictModerationCacheTestService(server.Client(), cache)
			cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")

			first := callStrictModerationCacheTest(context.Background(), svc, cfg, "normalized current user text")
			require.NoError(t, first.err)
			require.Equal(t, flagged, first.result.Flagged)
			require.Equal(t, 1, strictModerationCallCount(first.state))
			require.False(t, strictModerationCacheHit(first.state))
			require.False(t, strictModerationShared(first.state))
			first.result.CategoryScores["hate"] = 0.99

			second := callStrictModerationCacheTest(context.Background(), svc, cfg, "normalized current user text")
			require.NoError(t, second.err)
			require.Equal(t, flagged, second.result.Flagged)
			require.Equal(t, 0.01, second.result.CategoryScores["hate"])
			require.Zero(t, strictModerationCallCount(second.state))
			require.True(t, strictModerationCacheHit(second.state))
			require.False(t, strictModerationShared(second.state))
			require.EqualValues(t, 1, calls.Load())
		})
	}
}

func TestStrictModerationResultCacheMergesConcurrentIdenticalCalls(t *testing.T) {
	var calls atomic.Int32
	upstreamStarted := make(chan struct{})
	releaseUpstream := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(upstreamStarted)
		}
		<-releaseUpstream
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	cache := newStrictModerationResultCache(time.Minute, 8)
	svc := strictModerationCacheTestService(server.Client(), cache)
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")
	batch := strictModerationBatch{input: "same current user text", expectedResults: 1}
	cacheKey, ok := strictModerationResultCacheKey(cfg, batch)
	require.True(t, ok)

	leaderDone := make(chan strictModerationCacheCallResult, 1)
	go func() {
		leaderDone <- callStrictModerationCacheTest(context.Background(), svc, cfg, batch.input.(string))
	}()
	<-upstreamStarted

	waiterDone := make(chan strictModerationCacheCallResult, 1)
	go func() {
		waiterDone <- callStrictModerationCacheTest(context.Background(), svc, cfg, batch.input.(string))
	}()
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		entry := cache.entries[cacheKey]
		return entry != nil && entry.waiters == 2
	}, time.Second, time.Millisecond)
	close(releaseUpstream)

	leader := <-leaderDone
	waiter := <-waiterDone
	require.NoError(t, leader.err)
	require.NoError(t, waiter.err)
	require.EqualValues(t, 1, calls.Load())
	require.Equal(t, 1, strictModerationCallCount(leader.state))
	require.False(t, strictModerationCacheHit(leader.state))
	require.False(t, strictModerationShared(leader.state))
	require.Zero(t, strictModerationCallCount(waiter.state))
	require.False(t, strictModerationCacheHit(waiter.state))
	require.True(t, strictModerationShared(waiter.state))
}

func TestStrictModerationResultCacheLeaderCancellationDoesNotCancelSharedLoad(t *testing.T) {
	var calls atomic.Int32
	upstreamStarted := make(chan struct{})
	releaseUpstream := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		close(upstreamStarted)
		<-releaseUpstream
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	cache := newStrictModerationResultCache(time.Minute, 8)
	svc := strictModerationCacheTestService(server.Client(), cache)
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")
	text := "same text survives leader cancellation"
	cacheKey, ok := strictModerationResultCacheKey(cfg, strictModerationBatch{input: text, expectedResults: 1})
	require.True(t, ok)

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan strictModerationCacheCallResult, 1)
	go func() {
		leaderDone <- callStrictModerationCacheTest(leaderCtx, svc, cfg, text)
	}()
	<-upstreamStarted

	waiterDone := make(chan strictModerationCacheCallResult, 1)
	go func() {
		waiterDone <- callStrictModerationCacheTest(context.Background(), svc, cfg, text)
	}()
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		entry := cache.entries[cacheKey]
		return entry != nil && entry.waiters == 2
	}, time.Second, time.Millisecond)

	cancelLeader()
	leader := <-leaderDone
	require.ErrorIs(t, leader.err, context.Canceled)
	require.Equal(t, 1, strictModerationCallCount(leader.state))
	select {
	case result := <-waiterDone:
		require.FailNow(t, "shared waiter returned before upstream release", "result: %+v", result)
	default:
	}

	close(releaseUpstream)
	waiter := <-waiterDone
	require.NoError(t, waiter.err)
	require.True(t, strictModerationShared(waiter.state))
	require.Zero(t, strictModerationCallCount(waiter.state))
	require.EqualValues(t, 1, calls.Load())

	hit := callStrictModerationCacheTest(context.Background(), svc, cfg, text)
	require.NoError(t, hit.err)
	require.True(t, strictModerationCacheHit(hit.state))
	require.Zero(t, strictModerationCallCount(hit.state))
}

func TestStrictModerationResultCacheCancelsLoadAfterAllWaitersLeave(t *testing.T) {
	type cacheCallResult struct {
		results []moderationAPIResult
		err     error
	}

	cache := newStrictModerationResultCache(time.Minute, 1)
	key := sha256.Sum256([]byte("all waiters cancel"))
	loadStarted := make(chan struct{})
	loadCanceled := make(chan struct{})
	load := func(ctx context.Context) ([]moderationAPIResult, error) {
		close(loadStarted)
		<-ctx.Done()
		close(loadCanceled)
		return nil, ctx.Err()
	}
	newLoadContext := func() (context.Context, context.CancelFunc) {
		return context.WithCancel(context.Background())
	}

	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	ownerDone := make(chan cacheCallResult, 1)
	go func() {
		results, err, _ := cache.do(ownerCtx, key, newLoadContext, load)
		ownerDone <- cacheCallResult{results: results, err: err}
	}()
	<-loadStarted

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterDone := make(chan cacheCallResult, 1)
	go func() {
		results, err, _ := cache.do(waiterCtx, key, newLoadContext, load)
		waiterDone <- cacheCallResult{results: results, err: err}
	}()
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		entry := cache.entries[key]
		return entry != nil && entry.waiters == 2
	}, time.Second, time.Millisecond)

	cancelOwner()
	require.ErrorIs(t, (<-ownerDone).err, context.Canceled)
	select {
	case <-loadCanceled:
		require.FailNow(t, "shared load canceled while a waiter remained")
	default:
	}

	cancelWaiter()
	require.ErrorIs(t, (<-waiterDone).err, context.Canceled)
	select {
	case <-loadCanceled:
	case <-time.After(time.Second):
		require.FailNow(t, "shared load was not canceled after all waiters left")
	}
	require.Eventually(t, func() bool {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		return len(cache.entries) == 0
	}, time.Second, time.Millisecond)

	reloaded, err, outcome := cache.do(context.Background(), key, newLoadContext, func(context.Context) ([]moderationAPIResult, error) {
		return []moderationAPIResult{{Flagged: false}}, nil
	})
	require.NoError(t, err)
	require.Equal(t, strictModerationCacheOutcomeMiss, outcome)
	require.Len(t, reloaded, 1)
}

func TestStrictModerationResultCacheRecoversLoaderPanicAndAllowsReload(t *testing.T) {
	cache := newStrictModerationResultCache(time.Minute, 1)
	key := sha256.Sum256([]byte("panic cleanup"))
	newLoadContext := func() (context.Context, context.CancelFunc) {
		return context.WithCancel(context.Background())
	}

	_, err, outcome := cache.do(context.Background(), key, newLoadContext, func(context.Context) ([]moderationAPIResult, error) {
		panic("test panic")
	})
	require.ErrorIs(t, err, errStrictModerationCacheLoadPanic)
	require.Equal(t, strictModerationCacheOutcomeMiss, outcome)
	cache.mu.Lock()
	require.Empty(t, cache.entries)
	cache.mu.Unlock()

	reloaded, err, outcome := cache.do(context.Background(), key, newLoadContext, func(context.Context) ([]moderationAPIResult, error) {
		return []moderationAPIResult{{Flagged: true}}, nil
	})
	require.NoError(t, err)
	require.Equal(t, strictModerationCacheOutcomeMiss, outcome)
	require.Len(t, reloaded, 1)
	require.True(t, reloaded[0].Flagged)
}

func TestStrictModerationResultCacheRejectsDistinctLoadWhenInflightCapacityIsFull(t *testing.T) {
	cache := newStrictModerationResultCache(time.Minute, 1)
	firstKey := sha256.Sum256([]byte("first inflight key"))
	secondKey := sha256.Sum256([]byte("second inflight key"))
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)
	newLoadContext := func() (context.Context, context.CancelFunc) {
		return context.WithCancel(context.Background())
	}

	go func() {
		_, err, _ := cache.do(context.Background(), firstKey, newLoadContext, func(context.Context) ([]moderationAPIResult, error) {
			close(firstStarted)
			<-releaseFirst
			return []moderationAPIResult{{Flagged: false}}, nil
		})
		firstDone <- err
	}()
	<-firstStarted

	var secondContextCalls atomic.Int32
	var secondLoadCalls atomic.Int32
	_, err, outcome := cache.do(context.Background(), secondKey, func() (context.Context, context.CancelFunc) {
		secondContextCalls.Add(1)
		return context.WithCancel(context.Background())
	}, func(context.Context) ([]moderationAPIResult, error) {
		secondLoadCalls.Add(1)
		return []moderationAPIResult{{Flagged: false}}, nil
	})
	require.ErrorIs(t, err, errStrictModerationCacheSaturated)
	require.Equal(t, strictModerationCacheOutcomeMiss, outcome)
	require.Zero(t, secondContextCalls.Load())
	require.Zero(t, secondLoadCalls.Load())
	cache.mu.Lock()
	_, firstPresent := cache.entries[firstKey]
	_, secondPresent := cache.entries[secondKey]
	require.True(t, firstPresent)
	require.False(t, secondPresent)
	require.Len(t, cache.entries, 1)
	cache.mu.Unlock()

	close(releaseFirst)
	require.NoError(t, <-firstDone)
}

func TestStrictModerationResultCacheDoesNotCacheErrors(t *testing.T) {
	t.Run("429", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if calls.Add(1) == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded"}}`))
				return
			}
			writeStrictModerationCacheTestResponse(w, false, 0.01)
		}))
		defer server.Close()

		cache := newStrictModerationResultCache(time.Minute, 8)
		svc := strictModerationCacheTestService(server.Client(), cache)
		cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a", "sk-cache-b")

		first := callStrictModerationCacheTest(context.Background(), svc, cfg, "retry after upstream error")
		require.Error(t, first.err)
		require.Contains(t, first.err.Error(), "status 429")
		second := callStrictModerationCacheTest(context.Background(), svc, cfg, "retry after upstream error")
		require.NoError(t, second.err)
		require.Equal(t, 1, strictModerationCallCount(second.state))
		require.False(t, strictModerationCacheHit(second.state))
		require.EqualValues(t, 2, calls.Load())

		third := callStrictModerationCacheTest(context.Background(), svc, cfg, "retry after upstream error")
		require.NoError(t, third.err)
		require.True(t, strictModerationCacheHit(third.state))
		require.EqualValues(t, 2, calls.Load())
	})

	t.Run("malformed success", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if calls.Add(1) == 1 {
				_ = json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{
					Flagged: false, CategoryScores: map[string]float64{"hate": 0.01},
				}}})
				return
			}
			writeStrictModerationCacheTestResponse(w, false, 0.01)
		}))
		defer server.Close()

		cache := newStrictModerationResultCache(time.Minute, 8)
		svc := strictModerationCacheTestService(server.Client(), cache)
		cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a", "sk-cache-b")

		first := callStrictModerationCacheTest(context.Background(), svc, cfg, "retry malformed verdict")
		require.Error(t, first.err)
		require.Contains(t, first.err.Error(), "missing category score")
		second := callStrictModerationCacheTest(context.Background(), svc, cfg, "retry malformed verdict")
		require.NoError(t, second.err)
		require.Equal(t, 1, strictModerationCallCount(second.state))
		require.False(t, strictModerationCacheHit(second.state))
		require.EqualValues(t, 2, calls.Load())
	})
}

func TestStrictModerationResultCacheExpiresWithDeterministicClock(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	now := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	cache := newStrictModerationResultCache(5*time.Second, 8)
	cache.now = func() time.Time { return now }
	svc := strictModerationCacheTestService(server.Client(), cache)
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")

	first := callStrictModerationCacheTest(context.Background(), svc, cfg, "expires")
	require.NoError(t, first.err)
	second := callStrictModerationCacheTest(context.Background(), svc, cfg, "expires")
	require.NoError(t, second.err)
	require.True(t, strictModerationCacheHit(second.state))
	now = now.Add(6 * time.Second)
	third := callStrictModerationCacheTest(context.Background(), svc, cfg, "expires")
	require.NoError(t, third.err)
	require.False(t, strictModerationCacheHit(third.state))
	require.Equal(t, 1, strictModerationCallCount(third.state))
	require.EqualValues(t, 2, calls.Load())
}

func TestStrictModerationResultCacheSeparatesTextAndModel(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	cache := newStrictModerationResultCache(time.Minute, 8)
	svc := strictModerationCacheTestService(server.Client(), cache)
	modelA := strictModerationCacheTestConfig(server.URL, "moderation-a", "sk-cache-a")
	modelB := strictModerationCacheTestConfig(server.URL, "moderation-b", "sk-cache-a")

	for _, call := range []struct {
		cfg  *ContentModerationConfig
		text string
	}{
		{cfg: modelA, text: "text-a"},
		{cfg: modelA, text: "text-b"},
		{cfg: modelB, text: "text-a"},
	} {
		result := callStrictModerationCacheTest(context.Background(), svc, call.cfg, call.text)
		require.NoError(t, result.err)
		require.False(t, strictModerationCacheHit(result.state))
	}
	hit := callStrictModerationCacheTest(context.Background(), svc, modelA, "text-a")
	require.NoError(t, hit.err)
	require.True(t, strictModerationCacheHit(hit.state))
	require.EqualValues(t, 3, calls.Load())
}

func TestStrictModerationResultCacheKeySeparatesRoutingAndCredentials(t *testing.T) {
	baseCfg := strictModerationCacheTestConfig("https://moderation-a.example", "moderation-a", "sk-cache-a")
	baseBatch := strictModerationBatch{input: "same text", expectedResults: 1}
	baseKey, ok := strictModerationResultCacheKey(baseCfg, baseBatch)
	require.True(t, ok)

	endpointCfg := *baseCfg
	endpointCfg.BaseURL = "https://moderation-b.example"
	proxyCfg := *baseCfg
	proxyID := int64(42)
	proxyCfg.ProxyID = &proxyID
	credentialsCfg := *baseCfg
	credentialsCfg.APIKeys = []string{"sk-cache-b"}

	for _, testCase := range []struct {
		name  string
		cfg   *ContentModerationConfig
		batch strictModerationBatch
	}{
		{name: "endpoint", cfg: &endpointCfg, batch: baseBatch},
		{name: "proxy", cfg: &proxyCfg, batch: baseBatch},
		{name: "credentials", cfg: &credentialsCfg, batch: baseBatch},
		{name: "expected result count", cfg: baseCfg, batch: strictModerationBatch{input: "same text", expectedResults: 2}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			key, cacheable := strictModerationResultCacheKey(testCase.cfg, testCase.batch)
			require.True(t, cacheable)
			require.NotEqual(t, baseKey, key)
		})
	}
}

func TestStrictModerationResultCacheCapacityIsHardBound(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	cache := newStrictModerationResultCache(time.Minute, 2)
	svc := strictModerationCacheTestService(server.Client(), cache)
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")
	for _, text := range []string{"one", "two", "three", "four"} {
		result := callStrictModerationCacheTest(context.Background(), svc, cfg, text)
		require.NoError(t, result.err)
		cache.mu.Lock()
		require.LessOrEqual(t, len(cache.entries), 2)
		cache.mu.Unlock()
	}
	require.EqualValues(t, 4, calls.Load())
}

func TestStrictModerationResultCacheRejectsNonTextWithoutUpstreamCall(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	svc := strictModerationCacheTestService(server.Client(), newStrictModerationResultCache(time.Minute, 8))
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")
	_, err := svc.callModerationStrictBatch(context.Background(), cfg, strictModerationBatch{
		input: []any{map[string]any{"type": "image_url", "image_url": "not-read"}}, expectedResults: 1,
	}, newStrictModerationKeyState(cfg))
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be text")
	require.Zero(t, calls.Load())
}

func TestModerationAPISuccessLogFieldsAreHeaderOnlyAndSanitized(t *testing.T) {
	response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header)}
	response.Header.Set("x-request-id", "req-safe\r\n")
	response.Header.Set("x-ratelimit-limit-requests", "100")
	response.Header.Set("x-ratelimit-remaining-requests", "99")
	diagnostics := captureModerationAPIResponseDiagnostics(response, "sk-secret-value")
	fields := moderationAPISuccessLogFields(diagnostics)
	require.Zero(t, len(fields)%2)

	logged := make(map[string]any, len(fields)/2)
	for index := 0; index < len(fields); index += 2 {
		key, ok := fields[index].(string)
		require.True(t, ok)
		logged[key] = fields[index+1]
	}
	require.Equal(t, http.StatusOK, logged["moderation_http_status"])
	require.Equal(t, moderationAPIKeyHash("sk-secret-value"), logged["moderation_key_hash"])
	require.Equal(t, "req-safe", logged["openai_request_id"])
	require.Equal(t, "100", logged["ratelimit_limit_requests"])
	require.Equal(t, "99", logged["ratelimit_remaining_requests"])
	require.NotContains(t, logged, "ratelimit_reset_requests")
	serialized := fmt.Sprint(fields)
	require.False(t, strings.Contains(serialized, "sk-secret-value"))
	require.False(t, strings.Contains(serialized, "user text"))
	require.False(t, strings.Contains(serialized, "message"))
}

func TestStrictModerationSharedLoadContextIgnoresCallerCancellationAndKeepsTimeout(t *testing.T) {
	caller, cancelCaller := context.WithCancel(context.Background())
	cfg := strictModerationCacheTestConfig("https://api.openai.com", "omni-moderation-latest", "sk-cache-a")
	cfg.TimeoutMS = 250
	shared, cancelShared := strictModerationSharedLoadContext(caller, cfg)
	defer cancelShared()
	cancelCaller()

	select {
	case <-shared.Done():
		require.FailNow(t, "shared load inherited caller cancellation", shared.Err())
	default:
	}
	deadline, ok := shared.Deadline()
	require.True(t, ok)
	require.WithinDuration(t, time.Now().Add(250*time.Millisecond), deadline, 50*time.Millisecond)
	require.False(t, errors.Is(shared.Err(), context.Canceled))
}
