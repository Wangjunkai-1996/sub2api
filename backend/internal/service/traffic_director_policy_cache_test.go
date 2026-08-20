package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestTrafficDirectorPolicyCache_L1HitAvoidsRedisAndDB(t *testing.T) {
	policy := newTrafficDirectorPolicyCacheTestVersion(t, 41, 7, domain.TrafficDirectorModeEnforced)
	store := newTrafficDirectorPolicyStoreStub(policy)
	redisCache := newTrafficDirectorPolicyRedisStub()
	cache := NewTrafficDirectorPolicyCache(store, redisCache, 8, time.Hour)
	head := TrafficDirectorHead{GroupID: 41, Version: 7, Mode: domain.TrafficDirectorModeEnforced}

	first, err := cache.GetTrafficDirectorPolicy(context.Background(), head)
	require.NoError(t, err)
	require.Equal(t, TrafficDirectorPolicySourceDB, first.Source)

	second, err := cache.GetTrafficDirectorPolicy(context.Background(), head)
	require.NoError(t, err)
	require.Equal(t, TrafficDirectorPolicySourceL1, second.Source)
	require.Equal(t, int64(1), store.totalCalls.Load())
	require.Equal(t, int64(1), redisCache.getCalls.Load())
	require.Equal(t, int64(1), redisCache.setCalls.Load())
	require.Equal(t, TrafficDirectorPolicyCacheStats{L1Hits: 1, DBLoads: 1}, cache.Stats())
}

func TestTrafficDirectorPolicyCache_RedisHitPopulatesL1(t *testing.T) {
	policy := newTrafficDirectorPolicyCacheTestVersion(t, 42, 8, domain.TrafficDirectorModeShadow)
	redisCache := newTrafficDirectorPolicyRedisStub(policy)
	store := newTrafficDirectorPolicyStoreStub()
	cache := NewTrafficDirectorPolicyCache(store, redisCache, 8, time.Hour)
	head := TrafficDirectorHead{GroupID: 42, Version: 8, Mode: domain.TrafficDirectorModeShadow}

	first, err := cache.GetTrafficDirectorPolicy(context.Background(), head)
	require.NoError(t, err)
	require.Equal(t, TrafficDirectorPolicySourceL2, first.Source)

	second, err := cache.GetTrafficDirectorPolicy(context.Background(), head)
	require.NoError(t, err)
	require.Equal(t, TrafficDirectorPolicySourceL1, second.Source)
	require.Zero(t, store.totalCalls.Load())
	require.Equal(t, int64(1), redisCache.getCalls.Load())
	require.Equal(t, TrafficDirectorPolicyCacheStats{L1Hits: 1, L2Hits: 1}, cache.Stats())
}

func TestTrafficDirectorPolicyCache_RedisFailureFallsThroughToDB(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*trafficDirectorPolicyRedisStub)
	}{
		{name: "miss"},
		{
			name: "read error",
			configure: func(cache *trafficDirectorPolicyRedisStub) {
				cache.getErr = errors.New("redis unavailable")
			},
		},
		{
			name: "mismatched cached coordinates",
			configure: func(cache *trafficDirectorPolicyRedisStub) {
				wrong := newTrafficDirectorPolicyCacheTestVersion(t, 999, 9, domain.TrafficDirectorModeEnforced)
				cache.policies[trafficDirectorPolicyKey{groupID: 43, version: 9}] = wrong
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := newTrafficDirectorPolicyCacheTestVersion(t, 43, 9, domain.TrafficDirectorModeEnforced)
			store := newTrafficDirectorPolicyStoreStub(policy)
			redisCache := newTrafficDirectorPolicyRedisStub()
			redisCache.setErr = errors.New("best-effort write failed")
			if tt.configure != nil {
				tt.configure(redisCache)
			}
			cache := NewTrafficDirectorPolicyCache(store, redisCache, 8, time.Hour)

			resolved, err := cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
				GroupID: 43,
				Version: 9,
				Mode:    domain.TrafficDirectorModeEnforced,
			})
			require.NoError(t, err)
			require.Equal(t, TrafficDirectorPolicySourceDB, resolved.Source)
			require.Equal(t, int64(1), store.totalCalls.Load())
			require.Equal(t, int64(1), redisCache.setCalls.Load())
		})
	}
}

func TestTrafficDirectorPolicyCache_RedisLatencyCannotConsumeDBFallbackBudget(t *testing.T) {
	policy := newTrafficDirectorPolicyCacheTestVersion(t, 430, 9, domain.TrafficDirectorModeEnforced)
	store := newTrafficDirectorPolicyStoreStub(policy)
	redisCache := newTrafficDirectorPolicyRedisStub()
	redisCache.blockGet = true
	redisCache.blockSet = true
	cache := NewTrafficDirectorPolicyCache(store, redisCache, 8, time.Hour)

	started := time.Now()
	resolved, err := cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
		GroupID: 430,
		Version: 9,
		Mode:    domain.TrafficDirectorModeEnforced,
	})
	require.NoError(t, err)
	require.Equal(t, TrafficDirectorPolicySourceDB, resolved.Source)
	require.Less(t, time.Since(started), time.Second)
	require.Equal(t, int64(1), store.totalCalls.Load())
	require.Equal(t, int64(1), redisCache.getCalls.Load())
	require.Equal(t, int64(1), redisCache.setCalls.Load())
}

func TestTrafficDirectorPolicyCache_DBLoadUsesSingleflight(t *testing.T) {
	policy := newTrafficDirectorPolicyCacheTestVersion(t, 44, 10, domain.TrafficDirectorModeEnforced)
	store := newTrafficDirectorPolicyStoreStub(policy)
	store.delay = 80 * time.Millisecond
	cache := NewTrafficDirectorPolicyCache(store, nil, 8, time.Hour)
	head := TrafficDirectorHead{GroupID: 44, Version: 10, Mode: domain.TrafficDirectorModeEnforced}

	const callers = 24
	start := make(chan struct{})
	errorsCh := make(chan error, callers)
	versionsCh := make(chan int64, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resolved, err := cache.GetTrafficDirectorPolicy(context.Background(), head)
			errorsCh <- err
			versionsCh <- resolved.Version.Version
		}()
	}
	close(start)
	wg.Wait()
	close(errorsCh)
	close(versionsCh)

	for err := range errorsCh {
		require.NoError(t, err)
	}
	for version := range versionsCh {
		require.Equal(t, int64(10), version)
	}
	require.Equal(t, int64(1), store.totalCalls.Load())
	require.Equal(t, uint64(1), cache.Stats().DBLoads)
}

func TestTrafficDirectorPolicyCache_CanceledCallerDoesNotCancelSharedLoad(t *testing.T) {
	policy := newTrafficDirectorPolicyCacheTestVersion(t, 440, 10, domain.TrafficDirectorModeEnforced)
	store := newTrafficDirectorPolicyStoreStub(policy)
	store.delay = 80 * time.Millisecond
	cache := NewTrafficDirectorPolicyCache(store, nil, 8, time.Hour)
	head := TrafficDirectorHead{GroupID: 440, Version: 10, Mode: domain.TrafficDirectorModeEnforced}

	leaderCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	leaderErr := make(chan error, 1)
	go func() {
		_, err := cache.GetTrafficDirectorPolicy(leaderCtx, head)
		leaderErr <- err
	}()
	require.Eventually(t, func() bool { return store.totalCalls.Load() == 1 }, time.Second, time.Millisecond)

	follower, followerErr := cache.GetTrafficDirectorPolicy(context.Background(), head)
	require.NoError(t, followerErr)
	require.Equal(t, int64(10), follower.Version.Version)
	require.ErrorIs(t, <-leaderErr, ErrTrafficDirectorPolicyUnavailable)
	require.Equal(t, int64(1), store.totalCalls.Load())
}

func TestTrafficDirectorPolicyCache_ImmutableVersionsDoNotCollide(t *testing.T) {
	versionOne := newTrafficDirectorPolicyCacheTestVersion(t, 45, 1, domain.TrafficDirectorModeEnforced)
	versionTwo := newTrafficDirectorPolicyCacheTestVersion(t, 45, 2, domain.TrafficDirectorModeEnforced)
	store := newTrafficDirectorPolicyStoreStub(versionOne, versionTwo)
	cache := NewTrafficDirectorPolicyCache(store, nil, 8, time.Hour)

	first, err := cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
		GroupID: 45,
		Version: 1,
		Mode:    domain.TrafficDirectorModeEnforced,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), first.Version.Version)

	latest, err := cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
		GroupID: 45,
		Version: 2,
		Mode:    domain.TrafficDirectorModeEnforced,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), latest.Version.Version)

	again, err := cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
		GroupID: 45,
		Version: 1,
		Mode:    domain.TrafficDirectorModeEnforced,
	})
	require.NoError(t, err)
	require.Equal(t, TrafficDirectorPolicySourceL1, again.Source)
	require.Equal(t, 1, store.callCount(45, 1))
	require.Equal(t, 1, store.callCount(45, 2))
}

func TestTrafficDirectorPolicyCache_RejectsStoreCoordinateMismatch(t *testing.T) {
	wrong := newTrafficDirectorPolicyCacheTestVersion(t, 47, 11, domain.TrafficDirectorModeEnforced)
	store := newTrafficDirectorPolicyStoreStub()
	store.defaultPolicy = &wrong
	cache := NewTrafficDirectorPolicyCache(store, nil, 8, time.Hour)

	_, err := cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
		GroupID: 46,
		Version: 11,
		Mode:    domain.TrafficDirectorModeEnforced,
	})
	require.ErrorIs(t, err, ErrTrafficDirectorPolicyUnavailable)
	require.Equal(t, uint64(1), cache.Stats().Errors)
}

func TestTrafficDirectorPolicyCache_RejectsInvalidModeSpecAndChecksum(t *testing.T) {
	valid := newTrafficDirectorPolicyCacheTestVersion(t, 47, 12, domain.TrafficDirectorModeEnforced)
	tests := []struct {
		name   string
		policy TrafficDirectorVersion
	}{
		{
			name: "invalid mode",
			policy: func() TrafficDirectorVersion {
				policy := cloneTrafficDirectorPolicyVersion(valid)
				policy.Mode = "invalid"
				return policy
			}(),
		},
		{
			name: "missing enforced spec",
			policy: func() TrafficDirectorVersion {
				policy := cloneTrafficDirectorPolicyVersion(valid)
				policy.Spec = nil
				return policy
			}(),
		},
		{
			name: "invalid spec",
			policy: func() TrafficDirectorVersion {
				policy := cloneTrafficDirectorPolicyVersion(valid)
				policy.Spec.Pools[0].WeightBPS = 1
				policy.Checksum = ""
				return policy
			}(),
		},
		{
			name: "missing checksum",
			policy: func() TrafficDirectorVersion {
				policy := cloneTrafficDirectorPolicyVersion(valid)
				policy.Checksum = ""
				return policy
			}(),
		},
		{
			name: "checksum mismatch",
			policy: func() TrafficDirectorVersion {
				policy := cloneTrafficDirectorPolicyVersion(valid)
				policy.Checksum = "not-the-policy-checksum"
				return policy
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newTrafficDirectorPolicyStoreStub(tt.policy)
			cache := NewTrafficDirectorPolicyCache(store, nil, 8, time.Hour)
			_, err := cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
				GroupID: 47,
				Version: 12,
				Mode:    domain.TrafficDirectorModeEnforced,
			})
			require.ErrorIs(t, err, ErrTrafficDirectorPolicyUnavailable)
		})
	}
}

func TestTrafficDirectorPolicyCache_UnavailableFallbackDependsOnHeadMode(t *testing.T) {
	tests := []struct {
		mode          string
		wantError     bool
		wantDegraded  bool
		wantVersion   int64
		wantErrorStat uint64
	}{
		{mode: domain.TrafficDirectorModeEnforced, wantError: true, wantErrorStat: 1},
		{mode: domain.TrafficDirectorModeShadow, wantDegraded: true, wantVersion: 0},
		{mode: domain.TrafficDirectorModeLegacy, wantDegraded: true, wantVersion: 0},
	}

	for _, tt := range tests {
		t.Run(tt.mode, func(t *testing.T) {
			store := newTrafficDirectorPolicyStoreStub()
			store.defaultErr = errors.New("database unavailable")
			cache := NewTrafficDirectorPolicyCache(store, nil, 8, time.Hour)
			resolved, err := cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
				GroupID: 48,
				Version: 12,
				Mode:    tt.mode,
			})

			if tt.wantError {
				require.ErrorIs(t, err, ErrTrafficDirectorPolicyUnavailable)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.wantDegraded, resolved.Degraded)
				require.Equal(t, tt.wantVersion, resolved.Version.Version)
				require.Equal(t, int64(48), resolved.Version.GroupID)
				require.Equal(t, domain.TrafficDirectorModeLegacy, resolved.Version.Mode)
			}
			require.Equal(t, tt.wantErrorStat, cache.Stats().Errors)
		})
	}
}

func TestTrafficDirectorPolicyCache_LegacyVersionBypassesStores(t *testing.T) {
	store := newTrafficDirectorPolicyStoreStub()
	store.defaultErr = errors.New("must not be called")
	redisCache := newTrafficDirectorPolicyRedisStub()
	redisCache.getErr = errors.New("must not be called")
	cache := NewTrafficDirectorPolicyCache(store, redisCache, 8, time.Hour)

	resolved, err := cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
		GroupID: 49,
		Version: TrafficDirectorLegacyVersion,
		Mode:    domain.TrafficDirectorModeLegacy,
	})
	require.NoError(t, err)
	require.False(t, resolved.Degraded)
	require.Equal(t, TrafficDirectorPolicySourceLegacy, resolved.Source)
	require.Equal(t, int64(49), resolved.Version.GroupID)
	require.Equal(t, TrafficDirectorLegacyVersion, resolved.Version.Version)
	require.Equal(t, domain.TrafficDirectorModeLegacy, resolved.Version.Mode)
	require.Zero(t, store.totalCalls.Load())
	require.Zero(t, redisCache.getCalls.Load())
}

func TestTrafficDirectorPolicyCache_LegacyVersionRejectsEnforcedHead(t *testing.T) {
	store := newTrafficDirectorPolicyStoreStub()
	redisCache := newTrafficDirectorPolicyRedisStub()
	cache := NewTrafficDirectorPolicyCache(store, redisCache, 8, time.Hour)

	_, err := cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
		GroupID: 49,
		Version: TrafficDirectorLegacyVersion,
		Mode:    domain.TrafficDirectorModeEnforced,
	})
	require.ErrorIs(t, err, ErrTrafficDirectorPolicyUnavailable)
	require.Zero(t, store.totalCalls.Load())
	require.Zero(t, redisCache.getCalls.Load())
}

func TestTrafficDirectorPolicyCache_L1WorksWithoutRedis(t *testing.T) {
	policy := newTrafficDirectorPolicyCacheTestVersion(t, 50, 13, domain.TrafficDirectorModeEnforced)
	store := newTrafficDirectorPolicyStoreStub(policy)
	cache := NewTrafficDirectorPolicyCache(store, nil, 8, time.Hour)
	head := TrafficDirectorHead{GroupID: 50, Version: 13, Mode: domain.TrafficDirectorModeEnforced}

	_, err := cache.GetTrafficDirectorPolicy(context.Background(), head)
	require.NoError(t, err)
	second, err := cache.GetTrafficDirectorPolicy(context.Background(), head)
	require.NoError(t, err)
	require.Equal(t, TrafficDirectorPolicySourceL1, second.Source)
	require.Equal(t, int64(1), store.totalCalls.Load())
}

func TestTrafficDirectorPolicyCache_LRUEvictsLeastRecentlyUsedDeterministically(t *testing.T) {
	policies := []TrafficDirectorVersion{
		newTrafficDirectorPolicyCacheTestVersion(t, 51, 1, domain.TrafficDirectorModeEnforced),
		newTrafficDirectorPolicyCacheTestVersion(t, 51, 2, domain.TrafficDirectorModeEnforced),
		newTrafficDirectorPolicyCacheTestVersion(t, 51, 3, domain.TrafficDirectorModeEnforced),
	}
	store := newTrafficDirectorPolicyStoreStub(policies...)
	cache := NewTrafficDirectorPolicyCache(store, nil, 2, time.Hour)
	load := func(version int64) TrafficDirectorResolvedPolicy {
		resolved, err := cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
			GroupID: 51,
			Version: version,
			Mode:    domain.TrafficDirectorModeEnforced,
		})
		require.NoError(t, err)
		return resolved
	}

	load(1)
	load(2)
	require.Equal(t, TrafficDirectorPolicySourceL1, load(1).Source)
	load(3) // Version 2 is now the least recently used entry and is evicted.
	require.Equal(t, TrafficDirectorPolicySourceDB, load(2).Source)
	require.Equal(t, 1, store.callCount(51, 1))
	require.Equal(t, 2, store.callCount(51, 2))
	require.Equal(t, 1, store.callCount(51, 3))
}

func TestTrafficDirectorPolicyCache_StatsDistinguishResolutionPaths(t *testing.T) {
	redisPolicy := newTrafficDirectorPolicyCacheTestVersion(t, 52, 1, domain.TrafficDirectorModeShadow)
	dbPolicy := newTrafficDirectorPolicyCacheTestVersion(t, 52, 2, domain.TrafficDirectorModeShadow)
	redisCache := newTrafficDirectorPolicyRedisStub(redisPolicy)
	store := newTrafficDirectorPolicyStoreStub(dbPolicy)
	store.errorsByKey[trafficDirectorPolicyKey{groupID: 53, version: 1}] = errors.New("missing shadow")
	store.errorsByKey[trafficDirectorPolicyKey{groupID: 54, version: 1}] = errors.New("missing enforced")
	cache := NewTrafficDirectorPolicyCache(store, redisCache, 8, time.Hour)

	_, err := cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
		GroupID: 52, Version: 1, Mode: domain.TrafficDirectorModeShadow,
	})
	require.NoError(t, err)
	_, err = cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
		GroupID: 52, Version: 2, Mode: domain.TrafficDirectorModeShadow,
	})
	require.NoError(t, err)
	_, err = cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
		GroupID: 52, Version: 2, Mode: domain.TrafficDirectorModeShadow,
	})
	require.NoError(t, err)
	degraded, err := cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
		GroupID: 53, Version: 1, Mode: domain.TrafficDirectorModeShadow,
	})
	require.NoError(t, err)
	require.True(t, degraded.Degraded)
	_, err = cache.GetTrafficDirectorPolicy(context.Background(), TrafficDirectorHead{
		GroupID: 54, Version: 1, Mode: domain.TrafficDirectorModeEnforced,
	})
	require.ErrorIs(t, err, ErrTrafficDirectorPolicyUnavailable)

	require.Equal(t, TrafficDirectorPolicyCacheStats{
		L1Hits:            1,
		L2Hits:            1,
		DBLoads:           3,
		DegradedFallbacks: 1,
		Errors:            1,
	}, cache.Stats())
}

func TestTrafficDirectorPolicyCache_ReturnedPolicyDoesNotMutateL1(t *testing.T) {
	policy := newTrafficDirectorPolicyCacheTestVersion(t, 55, 1, domain.TrafficDirectorModeEnforced)
	store := newTrafficDirectorPolicyStoreStub(policy)
	cache := NewTrafficDirectorPolicyCache(store, nil, 8, time.Hour)
	head := TrafficDirectorHead{GroupID: 55, Version: 1, Mode: domain.TrafficDirectorModeEnforced}

	first, err := cache.GetTrafficDirectorPolicy(context.Background(), head)
	require.NoError(t, err)
	first.Version.Spec.Pools[0].Key = "mutated"
	first.Version.Spec.Pools[0].AccountIDs[0] = 999

	second, err := cache.GetTrafficDirectorPolicy(context.Background(), head)
	require.NoError(t, err)
	require.Equal(t, "primary", second.Version.Spec.Pools[0].Key)
	require.Equal(t, int64(101), second.Version.Spec.Pools[0].AccountIDs[0])
}

type trafficDirectorPolicyStoreStub struct {
	mu            sync.Mutex
	policies      map[trafficDirectorPolicyKey]TrafficDirectorVersion
	errorsByKey   map[trafficDirectorPolicyKey]error
	callsByKey    map[trafficDirectorPolicyKey]int
	defaultPolicy *TrafficDirectorVersion
	defaultErr    error
	delay         time.Duration
	totalCalls    atomic.Int64
}

func newTrafficDirectorPolicyStoreStub(policies ...TrafficDirectorVersion) *trafficDirectorPolicyStoreStub {
	stub := &trafficDirectorPolicyStoreStub{
		policies:    make(map[trafficDirectorPolicyKey]TrafficDirectorVersion, len(policies)),
		errorsByKey: make(map[trafficDirectorPolicyKey]error),
		callsByKey:  make(map[trafficDirectorPolicyKey]int),
	}
	for _, policy := range policies {
		stub.policies[trafficDirectorPolicyKey{groupID: policy.GroupID, version: policy.Version}] = policy
	}
	return stub
}

func (s *trafficDirectorPolicyStoreStub) GetTrafficDirectorVersion(
	ctx context.Context,
	groupID int64,
	version int64,
) (*TrafficDirectorVersion, error) {
	s.totalCalls.Add(1)
	key := trafficDirectorPolicyKey{groupID: groupID, version: version}
	s.mu.Lock()
	s.callsByKey[key]++
	delay := s.delay
	err := s.errorsByKey[key]
	defaultErr := s.defaultErr
	policy, ok := s.policies[key]
	defaultPolicy := s.defaultPolicy
	s.mu.Unlock()

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err != nil {
		return nil, err
	}
	if defaultErr != nil {
		return nil, defaultErr
	}
	if !ok {
		if defaultPolicy == nil {
			return nil, errors.New("policy not found")
		}
		cloned := cloneTrafficDirectorPolicyVersion(*defaultPolicy)
		return &cloned, nil
	}
	cloned := cloneTrafficDirectorPolicyVersion(policy)
	return &cloned, nil
}

func (s *trafficDirectorPolicyStoreStub) callCount(groupID, version int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.callsByKey[trafficDirectorPolicyKey{groupID: groupID, version: version}]
}

type trafficDirectorPolicyRedisStub struct {
	mu       sync.Mutex
	policies map[trafficDirectorPolicyKey]TrafficDirectorVersion
	getErr   error
	setErr   error
	blockGet bool
	blockSet bool
	getCalls atomic.Int64
	setCalls atomic.Int64
}

func newTrafficDirectorPolicyRedisStub(policies ...TrafficDirectorVersion) *trafficDirectorPolicyRedisStub {
	stub := &trafficDirectorPolicyRedisStub{
		policies: make(map[trafficDirectorPolicyKey]TrafficDirectorVersion, len(policies)),
	}
	for _, policy := range policies {
		stub.policies[trafficDirectorPolicyKey{groupID: policy.GroupID, version: policy.Version}] = policy
	}
	return stub
}

func (s *trafficDirectorPolicyRedisStub) GetTrafficDirectorPolicyVersion(
	ctx context.Context,
	groupID int64,
	version int64,
) (*TrafficDirectorVersion, error) {
	s.getCalls.Add(1)
	if s.blockGet {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getErr != nil {
		return nil, s.getErr
	}
	policy, ok := s.policies[trafficDirectorPolicyKey{groupID: groupID, version: version}]
	if !ok {
		return nil, nil
	}
	cloned := cloneTrafficDirectorPolicyVersion(policy)
	return &cloned, nil
}

func (s *trafficDirectorPolicyRedisStub) SetTrafficDirectorPolicyVersion(
	ctx context.Context,
	policy *TrafficDirectorVersion,
	_ time.Duration,
) error {
	s.setCalls.Add(1)
	if s.blockSet {
		<-ctx.Done()
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.setErr != nil {
		return s.setErr
	}
	cloned := cloneTrafficDirectorPolicyVersion(*policy)
	s.policies[trafficDirectorPolicyKey{groupID: policy.GroupID, version: policy.Version}] = cloned
	return nil
}

func newTrafficDirectorPolicyCacheTestVersion(
	t *testing.T,
	groupID int64,
	version int64,
	mode string,
) TrafficDirectorVersion {
	t.Helper()
	policy := TrafficDirectorVersion{
		GroupID: groupID,
		Version: version,
		Mode:    mode,
	}
	if mode == domain.TrafficDirectorModeLegacy {
		policy.Checksum = TrafficDirectorLegacyChecksum()
		return policy
	}

	spec := domain.TrafficDirectorSpec{
		SchemaVersion: domain.TrafficDirectorSchemaVersion,
		HealthMode:    domain.TrafficDirectorHealthModeOff,
		Pools: []domain.TrafficDirectorPool{
			{
				Key:          "primary",
				WeightBPS:    domain.TrafficDirectorWeightTotalBPS,
				AccountIDs:   []int64{101},
				MinAvailable: 1,
			},
		},
	}
	checksum, err := TrafficDirectorSpecChecksum(spec)
	require.NoError(t, err)
	policy.Spec = &spec
	policy.Checksum = checksum
	return policy
}
