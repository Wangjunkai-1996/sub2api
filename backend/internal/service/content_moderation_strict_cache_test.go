package service

import (
	"context"
	"encoding/json"
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
	cfg.MaxRPM = 100
	return cfg
}

func strictModerationCacheTestIdentity() ContentModerationCheckInput {
	groupID := int64(22)
	return ContentModerationCheckInput{
		APIKeyID: 101,
		GroupID:  &groupID,
		Endpoint: "/v1/chat/completions",
	}
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
	identities ...ContentModerationCheckInput,
) strictModerationCacheCallResult {
	identity := strictModerationCacheTestIdentity()
	if len(identities) > 0 {
		identity = identities[0]
	}
	state := newStrictModerationKeyState(cfg, identity)
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
	text := "same current user text"

	leaderDone := make(chan strictModerationCacheCallResult, 1)
	go func() {
		leaderDone <- callStrictModerationCacheTest(context.Background(), svc, cfg, text)
	}()
	<-upstreamStarted

	waiterEntered := make(chan struct{})
	waiterDone := make(chan strictModerationCacheCallResult, 1)
	go func() {
		close(waiterEntered)
		waiterDone <- callStrictModerationCacheTest(context.Background(), svc, cfg, text)
	}()
	<-waiterEntered
	select {
	case result := <-waiterDone:
		require.FailNow(t, "identical waiter returned while the first request held the serial pool", "result: %+v", result)
	case <-time.After(20 * time.Millisecond):
	}
	require.EqualValues(t, 1, calls.Load())
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
	require.True(t, strictModerationCacheHit(waiter.state))
	require.True(t, strictModerationShared(waiter.state))
}

func TestStrictModerationResultCacheDoesNotCacheErrors(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"type":"invalid_request_error","code":"invalid_input"}}`))
			return
		}
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	cache := newStrictModerationResultCache(time.Minute, 8)
	svc := strictModerationCacheTestService(server.Client(), cache)
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")

	first := callStrictModerationCacheTest(context.Background(), svc, cfg, "retry after upstream error")
	require.Error(t, first.err)
	require.Contains(t, first.err.Error(), "status 400")
	require.False(t, strictModerationCacheHit(first.state))

	second := callStrictModerationCacheTest(context.Background(), svc, cfg, "retry after upstream error")
	require.NoError(t, second.err)
	require.Equal(t, 1, strictModerationCallCount(second.state))
	require.False(t, strictModerationCacheHit(second.state))
	require.EqualValues(t, 2, calls.Load())

	third := callStrictModerationCacheTest(context.Background(), svc, cfg, "retry after upstream error")
	require.NoError(t, third.err)
	require.True(t, strictModerationCacheHit(third.state))
	require.EqualValues(t, 2, calls.Load())
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

func TestStrictModerationResultCacheEvictsLeastRecentlyUsed(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	cache := newStrictModerationResultCache(time.Minute, 2)
	svc := strictModerationCacheTestService(server.Client(), cache)
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")

	for _, text := range []string{"one", "two"} {
		result := callStrictModerationCacheTest(context.Background(), svc, cfg, text)
		require.NoError(t, result.err)
	}
	refresh := callStrictModerationCacheTest(context.Background(), svc, cfg, "one")
	require.NoError(t, refresh.err)
	require.True(t, strictModerationCacheHit(refresh.state))
	third := callStrictModerationCacheTest(context.Background(), svc, cfg, "three")
	require.NoError(t, third.err)

	evicted := callStrictModerationCacheTest(context.Background(), svc, cfg, "two")
	require.NoError(t, evicted.err)
	require.False(t, strictModerationCacheHit(evicted.state))
	require.Equal(t, 1, strictModerationCallCount(evicted.state))
	require.EqualValues(t, 4, calls.Load())
	cache.mu.Lock()
	require.Len(t, cache.entries, 2)
	cache.mu.Unlock()
}

func TestStrictModerationResultCacheSeparatesClientIdentity(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	svc := strictModerationCacheTestService(server.Client(), newStrictModerationResultCache(time.Minute, 8))
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")
	base := strictModerationCacheTestIdentity()
	differentAPIKey := base
	differentAPIKey.APIKeyID++
	differentGroup := base
	groupID := int64(23)
	differentGroup.GroupID = &groupID
	differentEndpoint := base
	differentEndpoint.Endpoint = "/v1/responses"

	for _, identity := range []ContentModerationCheckInput{base, differentAPIKey, differentGroup, differentEndpoint} {
		result := callStrictModerationCacheTest(context.Background(), svc, cfg, "same text", identity)
		require.NoError(t, result.err)
		require.False(t, strictModerationCacheHit(result.state))
	}
	hit := callStrictModerationCacheTest(context.Background(), svc, cfg, "same text", base)
	require.NoError(t, hit.err)
	require.True(t, strictModerationCacheHit(hit.state))
	require.EqualValues(t, 4, calls.Load())
}

func TestStrictModerationResultCacheDoesNotCacheWithoutCompleteClientIdentity(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	svc := strictModerationCacheTestService(server.Client(), newStrictModerationResultCache(time.Minute, 8))
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")
	testCases := []struct {
		name   string
		mutate func(*ContentModerationCheckInput)
	}{
		{name: "api key", mutate: func(input *ContentModerationCheckInput) { input.APIKeyID = 0 }},
		{name: "group", mutate: func(input *ContentModerationCheckInput) { input.GroupID = nil }},
		{name: "endpoint", mutate: func(input *ContentModerationCheckInput) { input.Endpoint = "" }},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			identity := strictModerationCacheTestIdentity()
			testCase.mutate(&identity)
			for range 2 {
				result := callStrictModerationCacheTest(context.Background(), svc, cfg, "same text", identity)
				require.NoError(t, result.err)
				require.False(t, strictModerationCacheHit(result.state))
				require.Equal(t, 1, strictModerationCallCount(result.state))
			}
		})
	}
	require.EqualValues(t, 6, calls.Load())
}

func TestStrictModerationResultCacheKeySeparatesRoutingAndCredentialSet(t *testing.T) {
	baseCfg := strictModerationCacheTestConfig("https://moderation-a.example", "moderation-a", "sk-cache-a")
	baseBatch := strictModerationBatch{input: "same text", expectedResults: 1}
	baseState := newStrictModerationKeyState(baseCfg, strictModerationCacheTestIdentity())
	baseKey, ok := strictModerationResultCacheKey(baseCfg, baseBatch, baseState)
	require.True(t, ok)

	endpointCfg := *baseCfg
	endpointCfg.BaseURL = "https://moderation-b.example"
	modelCfg := *baseCfg
	modelCfg.Model = "moderation-b"
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
		{name: "upstream endpoint", cfg: &endpointCfg, batch: baseBatch},
		{name: "model", cfg: &modelCfg, batch: baseBatch},
		{name: "proxy", cfg: &proxyCfg, batch: baseBatch},
		{name: "credential set", cfg: &credentialsCfg, batch: baseBatch},
		{name: "expected result count", cfg: baseCfg, batch: strictModerationBatch{input: "same text", expectedResults: 2}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			state := newStrictModerationKeyState(testCase.cfg, strictModerationCacheTestIdentity())
			key, cacheable := strictModerationResultCacheKey(testCase.cfg, testCase.batch, state)
			require.True(t, cacheable)
			require.NotEqual(t, baseKey, key)
		})
	}
}

func TestStrictModerationResultCacheKeyPreservesChunkOrder(t *testing.T) {
	cfg := strictModerationCacheTestConfig("https://moderation.example", "moderation-a", "sk-cache-a")
	identity := strictModerationCacheTestIdentity()
	forward := strictModerationBatch{input: []string{"first", "second"}, expectedResults: 2}
	reversed := strictModerationBatch{input: []string{"second", "first"}, expectedResults: 2}

	forwardKey, ok := strictModerationResultCacheKey(cfg, forward, newStrictModerationKeyState(cfg, identity))
	require.True(t, ok)
	reversedKey, ok := strictModerationResultCacheKey(cfg, reversed, newStrictModerationKeyState(cfg, identity))
	require.True(t, ok)
	require.NotEqual(t, forwardKey, reversedKey)
}

func TestStrictModerationResultCacheCredentialOrderChangesKey(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	svc := strictModerationCacheTestService(server.Client(), newStrictModerationResultCache(time.Minute, 8))
	forward := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a", "sk-cache-b")
	reversed := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-b", "sk-cache-a")
	identity := strictModerationCacheTestIdentity()
	batch := strictModerationBatch{input: "same text", expectedResults: 1}
	forwardKey, ok := strictModerationResultCacheKey(forward, batch, newStrictModerationKeyState(forward, identity))
	require.True(t, ok)
	reversedKey, ok := strictModerationResultCacheKey(reversed, batch, newStrictModerationKeyState(reversed, identity))
	require.True(t, ok)
	require.NotEqual(t, forwardKey, reversedKey)

	first := callStrictModerationCacheTest(context.Background(), svc, forward, "same text", identity)
	require.NoError(t, first.err)
	second := callStrictModerationCacheTest(context.Background(), svc, reversed, "same text", identity)
	require.NoError(t, second.err)
	require.False(t, strictModerationCacheHit(second.state))
	require.EqualValues(t, 2, calls.Load())
}

func TestStrictModerationPoolEnforcesRollingMaxRPM(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	svc := strictModerationCacheTestService(server.Client(), newStrictModerationResultCache(time.Minute, 8))
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")
	cfg.MaxRPM = 2
	pool, err := svc.strictModerationPool(cfg)
	require.NoError(t, err)
	now := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	pool.now = func() time.Time { return now }

	first := callStrictModerationCacheTest(context.Background(), svc, cfg, "rpm-1")
	require.NoError(t, first.err)
	now = now.Add(30 * time.Second)
	second := callStrictModerationCacheTest(context.Background(), svc, cfg, "rpm-2")
	require.NoError(t, second.err)
	now = now.Add(29 * time.Second)
	blocked := callStrictModerationCacheTest(context.Background(), svc, cfg, "rpm-blocked-1")
	require.ErrorIs(t, blocked.err, errStrictModerationRateLimited)
	require.Zero(t, strictModerationCallCount(blocked.state))
	require.EqualValues(t, 2, calls.Load())

	now = now.Add(time.Second)
	third := callStrictModerationCacheTest(context.Background(), svc, cfg, "rpm-3")
	require.NoError(t, third.err)
	now = now.Add(29 * time.Second)
	blocked = callStrictModerationCacheTest(context.Background(), svc, cfg, "rpm-blocked-2")
	require.ErrorIs(t, blocked.err, errStrictModerationRateLimited)
	require.EqualValues(t, 3, calls.Load())

	now = now.Add(time.Second)
	fourth := callStrictModerationCacheTest(context.Background(), svc, cfg, "rpm-4")
	require.NoError(t, fourth.err)
	require.EqualValues(t, 4, calls.Load())
}

func TestStrictModerationPoolZeroMaxRPMIsUnlimited(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	svc := strictModerationCacheTestService(server.Client(), newStrictModerationResultCache(time.Minute, 8))
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")
	cfg.MaxRPM = 0

	for i := 0; i < 3; i++ {
		result := callStrictModerationCacheTest(context.Background(), svc, cfg, fmt.Sprintf("unlimited-%d", i))
		require.NoError(t, result.err)
	}
	require.EqualValues(t, 3, calls.Load())
	pool, err := svc.strictModerationPool(cfg)
	require.NoError(t, err)
	pool.mu.Lock()
	require.Empty(t, pool.requestTimes)
	pool.mu.Unlock()
}

func TestStrictModerationPoolWaitHonorsContextCancellation(t *testing.T) {
	svc := strictModerationCacheTestService(http.DefaultClient, nil)
	cfg := strictModerationCacheTestConfig("https://moderation.example", "omni-moderation-latest", "sk-cache-a")
	holder, err := svc.acquireStrictModerationPool(context.Background(), cfg)
	require.NoError(t, err)
	defer holder.release()

	type acquireResult struct {
		lease *strictModerationPoolLease
		err   error
	}
	waiterStarted := make(chan struct{})
	waiterDone := make(chan acquireResult, 1)
	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	go func() {
		close(waiterStarted)
		lease, acquireErr := svc.acquireStrictModerationPool(waiterCtx, cfg)
		waiterDone <- acquireResult{lease: lease, err: acquireErr}
	}()
	<-waiterStarted
	select {
	case result := <-waiterDone:
		require.FailNow(t, "pool waiter returned while the only lease was held", "result: %+v", result)
	case <-time.After(20 * time.Millisecond):
	}

	cancelWaiter()
	result := <-waiterDone
	require.Nil(t, result.lease)
	require.ErrorIs(t, result.err, context.Canceled)
}

func TestStrictModerationPool429FailsOverWithoutOpeningSharedCircuit(t *testing.T) {
	var calls atomic.Int32
	var authorizations []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorizations = append(authorizations, r.Header.Get("Authorization"))
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "60")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded"}}`))
			return
		}
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	svc := strictModerationCacheTestService(server.Client(), newStrictModerationResultCache(time.Minute, 8))
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a", "sk-cache-b")
	startedAt := time.Now()
	first := callStrictModerationCacheTest(context.Background(), svc, cfg, "fail over")
	require.NoError(t, first.err)
	require.Equal(t, 2, strictModerationCallCount(first.state))

	second := callStrictModerationCacheTest(context.Background(), svc, cfg, "skip frozen key")
	require.NoError(t, second.err)
	require.Equal(t, 1, strictModerationCallCount(second.state))
	require.EqualValues(t, 3, calls.Load())
	require.Equal(t, []string{"Bearer sk-cache-a", "Bearer sk-cache-b", "Bearer sk-cache-b"}, authorizations)

	status := svc.apiKeyStatusForHash(0, moderationAPIKeyHash("sk-cache-a"), maskSecretTail("sk-cache-a"), true)
	require.NotNil(t, status.FrozenUntil)
	require.False(t, status.FrozenUntil.Before(startedAt.Add(59*time.Second)))
	pool, err := svc.strictModerationPool(cfg)
	require.NoError(t, err)
	pool.mu.Lock()
	require.Equal(t, strictModerationCircuitClosed, pool.circuit)
	pool.mu.Unlock()
}

func TestStrictModeration429FailoverReacquiresRPMGate(t *testing.T) {
	var authorizations []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authorizations = append(authorizations, r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded"}}`))
	}))
	defer server.Close()

	svc := strictModerationCacheTestService(server.Client(), newStrictModerationResultCache(time.Minute, 8))
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a", "sk-cache-b")
	cfg.MaxRPM = 1
	result := callStrictModerationCacheTest(context.Background(), svc, cfg, "respect rpm on failover")

	require.ErrorIs(t, result.err, errStrictModerationRateLimited)
	require.Equal(t, 1, strictModerationCallCount(result.state))
	require.Equal(t, []string{"Bearer sk-cache-a"}, authorizations)
}

func TestStrictModerationPoolHalfOpenAllowsSingleProbeAndClosesOnSuccess(t *testing.T) {
	var calls atomic.Int32
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			close(probeStarted)
			<-releaseProbe
		}
		writeStrictModerationCacheTestResponse(w, false, 0.01)
	}))
	defer server.Close()

	svc := strictModerationCacheTestService(server.Client(), newStrictModerationResultCache(time.Minute, 8))
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")
	pool, err := svc.strictModerationPool(cfg)
	require.NoError(t, err)
	now := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	pool.now = func() time.Time { return now }
	pool.mu.Lock()
	pool.circuit = strictModerationCircuitOpen
	pool.openUntil = now.Add(-time.Second)
	pool.mu.Unlock()

	probeDone := make(chan strictModerationCacheCallResult, 1)
	go func() {
		probeDone <- callStrictModerationCacheTest(context.Background(), svc, cfg, "half-open probe")
	}()
	<-probeStarted
	pool.mu.Lock()
	require.Equal(t, strictModerationCircuitHalfOpen, pool.circuit)
	require.True(t, pool.probeActive)
	pool.mu.Unlock()

	waiterEntered := make(chan struct{})
	waiterDone := make(chan strictModerationCacheCallResult, 1)
	go func() {
		close(waiterEntered)
		waiterDone <- callStrictModerationCacheTest(context.Background(), svc, cfg, "after successful probe")
	}()
	<-waiterEntered
	select {
	case result := <-waiterDone:
		require.FailNow(t, "a second request dispatched while the half-open probe was active", "result: %+v", result)
	case <-time.After(20 * time.Millisecond):
	}
	require.EqualValues(t, 1, calls.Load())
	close(releaseProbe)

	probe := <-probeDone
	waiter := <-waiterDone
	require.NoError(t, probe.err)
	require.NoError(t, waiter.err)
	require.EqualValues(t, 2, calls.Load())
	pool.mu.Lock()
	require.Equal(t, strictModerationCircuitClosed, pool.circuit)
	require.False(t, pool.probeActive)
	pool.mu.Unlock()
}

func TestStrictModerationPoolHalfOpenFailureReopensCircuit(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"type":"server_error","code":"upstream_failure"}}`))
	}))
	defer server.Close()

	svc := strictModerationCacheTestService(server.Client(), newStrictModerationResultCache(time.Minute, 8))
	cfg := strictModerationCacheTestConfig(server.URL, "omni-moderation-latest", "sk-cache-a")
	pool, err := svc.strictModerationPool(cfg)
	require.NoError(t, err)
	now := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	pool.now = func() time.Time { return now }
	pool.mu.Lock()
	pool.circuit = strictModerationCircuitOpen
	pool.openUntil = now.Add(-time.Second)
	pool.mu.Unlock()

	probe := callStrictModerationCacheTest(context.Background(), svc, cfg, "failing half-open probe")
	require.Error(t, probe.err)
	require.Contains(t, probe.err.Error(), "status 500")
	require.EqualValues(t, 1, calls.Load())
	pool.mu.Lock()
	require.Equal(t, strictModerationCircuitOpen, pool.circuit)
	require.Equal(t, now.Add(strictModerationPoolCooldown), pool.openUntil)
	require.False(t, pool.probeActive)
	pool.mu.Unlock()

	blocked := callStrictModerationCacheTest(context.Background(), svc, cfg, "must stay blocked")
	require.ErrorIs(t, blocked.err, errStrictModerationCircuitOpen)
	require.Zero(t, strictModerationCallCount(blocked.state))
	require.EqualValues(t, 1, calls.Load())
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
	}, newStrictModerationKeyState(cfg, strictModerationCacheTestIdentity()))
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
