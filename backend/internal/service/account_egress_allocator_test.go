package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type accountEgressCacheStub struct {
	mu sync.Mutex

	syncStatus AccountEgressConfigSyncStatus
	syncErr    error

	acquireResults []AccountEgressAcquireResult
	acquireErr     error
	acquireFn      func(context.Context, AccountEgressCacheAcquireRequest, int) (AccountEgressAcquireResult, error)
	acquireCalls   int
	lastAcquire    AccountEgressCacheAcquireRequest

	refreshOwned bool
	refreshErr   error
	refreshCalls int

	releaseCalls int
	removeCalls  int
}

func (s *accountEgressCacheStub) SyncAccountEgressConfigs(_ context.Context, configs []AccountEgressPoolConfig) (map[int64]AccountEgressConfigSyncStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.syncErr != nil {
		return nil, s.syncErr
	}
	status := s.syncStatus
	if status == "" {
		status = AccountEgressConfigSyncOK
	}
	result := make(map[int64]AccountEgressConfigSyncStatus, len(configs))
	for _, config := range configs {
		result[config.AccountID] = status
	}
	return result, nil
}

func (s *accountEgressCacheStub) AcquireAccountEgress(ctx context.Context, request AccountEgressCacheAcquireRequest) (AccountEgressAcquireResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastAcquire = request
	index := s.acquireCalls
	s.acquireCalls++
	if s.acquireFn != nil {
		return s.acquireFn(ctx, request, index)
	}
	if s.acquireErr != nil {
		return AccountEgressAcquireResult{}, s.acquireErr
	}
	if len(s.acquireResults) == 0 {
		return AccountEgressAcquireResult{Status: AccountEgressStatusFull, LeaseID: request.LeaseID}, nil
	}
	if index >= len(s.acquireResults) {
		index = len(s.acquireResults) - 1
	}
	result := s.acquireResults[index]
	if result.LeaseID == "" {
		result.LeaseID = request.LeaseID
	}
	return result, nil
}

func (s *accountEgressCacheStub) RemoveAccountEgressWaiter(context.Context, int64, string) error {
	s.mu.Lock()
	s.removeCalls++
	s.mu.Unlock()
	return nil
}

func (s *accountEgressCacheStub) RefreshAccountEgressLeases(_ context.Context, leases []AccountEgressLeaseRef, _ time.Duration) (map[string]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.refreshCalls++
	if s.refreshErr != nil {
		return nil, s.refreshErr
	}
	result := make(map[string]bool, len(leases))
	for _, lease := range leases {
		result[lease.Key()] = s.refreshOwned
	}
	return result, nil
}

func (s *accountEgressCacheStub) ReleaseAccountEgressLease(context.Context, AccountEgressLeaseRef) error {
	s.mu.Lock()
	s.releaseCalls++
	s.mu.Unlock()
	return nil
}

func (s *accountEgressCacheStub) GetAccountEgressLoadsBatch(_ context.Context, configs []AccountEgressPoolConfig, _, _ time.Duration) (map[int64]AccountEgressLoadInfo, error) {
	result := make(map[int64]AccountEgressLoadInfo, len(configs))
	for _, config := range configs {
		result[config.AccountID] = AccountEgressLoadInfo{
			AccountID:         config.AccountID,
			Status:            AccountEgressStatusAcquired,
			EffectiveCapacity: config.EffectiveCapacity(),
			ConfigVersion:     config.Version,
		}
	}
	return result, nil
}

func (s *accountEgressCacheStub) counts() (acquire, refresh, release, remove int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.acquireCalls, s.refreshCalls, s.releaseCalls, s.removeCalls
}

func accountEgressAllocatorTestConfig(maxWaiting int) AccountEgressPoolConfig {
	return AccountEgressPoolConfig{
		AccountID:              42,
		Version:                7,
		PerIdentityConcurrency: 2,
		MaxWaiting:             maxWaiting,
		Candidates: []AccountEgressCandidate{
			{BindingID: "route:101", RouteID: 101, IdentityID: "ip:192.0.2.1", Position: 0, Primary: true, Healthy: true},
			{BindingID: "route:102", RouteID: 102, IdentityID: "ip:192.0.2.1", Position: 1, Healthy: true},
			{BindingID: "route:103", RouteID: 103, IdentityID: "ip:192.0.2.2", Position: 2, Healthy: true},
		},
	}
}

func accountEgressAllocatorAcquiredResult() AccountEgressAcquireResult {
	return AccountEgressAcquireResult{
		Status:            AccountEgressStatusAcquired,
		BindingID:         "route:101",
		RouteID:           101,
		IdentityID:        "ip:192.0.2.1",
		ActiveTotal:       1,
		EffectiveCapacity: 4,
		ConfigVersion:     7,
	}
}

func TestAccountEgressConfigValidationAndCapacity(t *testing.T) {
	config := accountEgressAllocatorTestConfig(3)
	require.NoError(t, config.Validate(), "two bindings may intentionally share one identity")
	require.Equal(t, 4, config.EffectiveCapacity(), "capacity is unique healthy identities times the shared limit")
	digest, err := config.Digest()
	require.NoError(t, err)
	require.Len(t, digest, 64)

	tests := []struct {
		name   string
		mutate func(*AccountEgressPoolConfig)
	}{
		{name: "invalid account", mutate: func(c *AccountEgressPoolConfig) { c.AccountID = 0 }},
		{name: "invalid version", mutate: func(c *AccountEgressPoolConfig) { c.Version = 0 }},
		{name: "invalid limit", mutate: func(c *AccountEgressPoolConfig) { c.PerIdentityConcurrency = 0 }},
		{name: "duplicate binding", mutate: func(c *AccountEgressPoolConfig) { c.Candidates[1].BindingID = c.Candidates[0].BindingID }},
		{name: "duplicate route", mutate: func(c *AccountEgressPoolConfig) { c.Candidates[1].RouteID = c.Candidates[0].RouteID }},
		{name: "duplicate position", mutate: func(c *AccountEgressPoolConfig) { c.Candidates[1].Position = c.Candidates[0].Position }},
		{name: "multiple primary", mutate: func(c *AccountEgressPoolConfig) { c.Candidates[1].Primary = true }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := accountEgressAllocatorTestConfig(3)
			test.mutate(&invalid)
			require.Error(t, invalid.Validate())
		})
	}
}

func TestAccountEgressAllocatorFailsClosed(t *testing.T) {
	config := accountEgressAllocatorTestConfig(0)
	_, err := NewAccountEgressAllocator(nil).Acquire(context.Background(), AccountEgressAcquireRequest{Config: config})
	require.ErrorIs(t, err, ErrAccountEgressUnavailable)

	cache := &accountEgressCacheStub{syncErr: errors.New("redis unavailable")}
	_, err = NewAccountEgressAllocator(cache).Acquire(context.Background(), AccountEgressAcquireRequest{Config: config})
	require.ErrorIs(t, err, ErrAccountEgressUnavailable)

	cache = &accountEgressCacheStub{acquireErr: errors.New("uncertain redis response")}
	_, err = NewAccountEgressAllocator(cache).Acquire(context.Background(), AccountEgressAcquireRequest{Config: config})
	require.ErrorIs(t, err, ErrAccountEgressUnavailable)
}

func TestAccountEgressAllocatorResolvesAndReleasesIdempotently(t *testing.T) {
	cache := &accountEgressCacheStub{
		acquireResults: []AccountEgressAcquireResult{accountEgressAllocatorAcquiredResult()},
		refreshOwned:   true,
	}
	allocator := newAccountEgressAllocatorWithTiming(cache, time.Second, time.Hour, time.Second, time.Minute, time.Now)
	defer allocator.Close()
	request := AccountEgressAcquireRequest{
		Config:             accountEgressAllocatorTestConfig(0),
		LeaseID:            "lease-1",
		RequiredBindingID:  "route:101",
		PreferredBindingID: "route:103",
	}
	resolved, err := allocator.Acquire(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "route:101", resolved.BindingID)
	require.Equal(t, int64(101), resolved.RouteID)
	require.Equal(t, "ip:192.0.2.1", resolved.IdentityID)
	require.Equal(t, request.RequiredBindingID, cache.lastAcquire.RequiredBindingID)
	require.Equal(t, "lease-1", resolved.Lease.ID)

	resolved.Lease.Release()
	resolved.Lease.Release()
	_, _, releaseCalls, removeCalls := cache.counts()
	require.Equal(t, 1, releaseCalls)
	require.Equal(t, 1, removeCalls)
}

func TestAccountEgressAllocatorPollsFIFOAndAlwaysRemovesWaiter(t *testing.T) {
	cache := &accountEgressCacheStub{
		acquireResults: []AccountEgressAcquireResult{
			{Status: AccountEgressStatusFull},
			{Status: AccountEgressStatusNotQueueHead},
			accountEgressAllocatorAcquiredResult(),
		},
		refreshOwned: true,
	}
	allocator := newAccountEgressAllocatorWithTiming(cache, time.Second, time.Hour, time.Second, time.Minute, time.Now)
	defer allocator.Close()
	resolved, err := allocator.Acquire(context.Background(), AccountEgressAcquireRequest{
		Config:  accountEgressAllocatorTestConfig(3),
		LeaseID: "fifo-lease",
	})
	require.NoError(t, err)
	resolved.Lease.Release()
	acquireCalls, _, _, removeCalls := cache.counts()
	require.Equal(t, 3, acquireCalls)
	require.Equal(t, 1, removeCalls)

	timeoutCache := &accountEgressCacheStub{acquireResults: []AccountEgressAcquireResult{{Status: AccountEgressStatusFull}}}
	timeoutAllocator := newAccountEgressAllocatorWithTiming(timeoutCache, time.Second, time.Hour, time.Second, time.Minute, time.Now)
	defer timeoutAllocator.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()
	_, err = timeoutAllocator.Acquire(ctx, AccountEgressAcquireRequest{Config: accountEgressAllocatorTestConfig(3), LeaseID: "timeout-waiter"})
	require.ErrorIs(t, err, ErrAccountEgressCapacityFull)
	_, _, _, removeCalls = timeoutCache.counts()
	require.Equal(t, 1, removeCalls)
}

func TestAccountEgressAllocatorBoundsWaitWithoutCallerDeadline(t *testing.T) {
	cache := &accountEgressCacheStub{
		acquireResults: []AccountEgressAcquireResult{{Status: AccountEgressStatusLegacyDraining}},
	}
	allocator := newAccountEgressAllocatorWithTiming(
		cache,
		time.Second,
		time.Hour,
		time.Second,
		30*time.Millisecond,
		time.Now,
	)
	defer allocator.Close()

	startedAt := time.Now()
	_, err := allocator.Acquire(context.Background(), AccountEgressAcquireRequest{
		Config:  accountEgressAllocatorTestConfig(3),
		LeaseID: "locally-bounded-waiter",
	})
	require.ErrorIs(t, err, ErrAccountEgressCapacityFull)
	require.Less(t, time.Since(startedAt), 500*time.Millisecond)
	acquireCalls, _, _, removeCalls := cache.counts()
	require.GreaterOrEqual(t, acquireCalls, 1)
	require.LessOrEqual(t, acquireCalls, 2)
	require.Equal(t, 1, removeCalls)
}

func TestAccountEgressAllocatorPreservesCallerCancellationWhileWaiting(t *testing.T) {
	cache := &accountEgressCacheStub{
		acquireResults: []AccountEgressAcquireResult{{Status: AccountEgressStatusLegacyDraining}},
	}
	allocator := newAccountEgressAllocatorWithTiming(
		cache,
		time.Second,
		time.Hour,
		time.Second,
		time.Second,
		time.Now,
	)
	defer allocator.Close()
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)

	_, err := allocator.Acquire(ctx, AccountEgressAcquireRequest{
		Config:  accountEgressAllocatorTestConfig(3),
		LeaseID: "canceled-waiter",
	})
	require.ErrorIs(t, err, context.Canceled)
	_, _, _, removeCalls := cache.counts()
	require.Equal(t, 1, removeCalls)
}

func TestAccountEgressAllocatorMapsLocalDeadlineDuringAcquireToCapacityFull(t *testing.T) {
	cache := &accountEgressCacheStub{}
	cache.acquireFn = func(ctx context.Context, request AccountEgressCacheAcquireRequest, call int) (AccountEgressAcquireResult, error) {
		if call == 0 {
			return AccountEgressAcquireResult{Status: AccountEgressStatusLegacyDraining, LeaseID: request.LeaseID}, nil
		}
		<-ctx.Done()
		return AccountEgressAcquireResult{}, ctx.Err()
	}
	allocator := newAccountEgressAllocatorWithTiming(
		cache,
		time.Second,
		time.Hour,
		time.Second,
		80*time.Millisecond,
		time.Now,
	)
	defer allocator.Close()

	_, err := allocator.Acquire(context.Background(), AccountEgressAcquireRequest{
		Config:  accountEgressAllocatorTestConfig(3),
		LeaseID: "deadline-during-acquire",
	})
	require.ErrorIs(t, err, ErrAccountEgressCapacityFull)
	require.NotErrorIs(t, err, ErrAccountEgressUnavailable)
	acquireCalls, _, _, removeCalls := cache.counts()
	require.Equal(t, 2, acquireCalls)
	require.Equal(t, 1, removeCalls)
}

func TestAccountEgressLeaseManagerRefreshAndLoss(t *testing.T) {
	t.Run("central manager refreshes", func(t *testing.T) {
		cache := &accountEgressCacheStub{
			acquireResults: []AccountEgressAcquireResult{accountEgressAllocatorAcquiredResult()},
			refreshOwned:   true,
		}
		allocator := newAccountEgressAllocatorWithTiming(cache, 100*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond, time.Minute, time.Now)
		resolved, err := allocator.Acquire(context.Background(), AccountEgressAcquireRequest{Config: accountEgressAllocatorTestConfig(0), LeaseID: "refreshing"})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			_, refreshCalls, _, _ := cache.counts()
			return refreshCalls > 0
		}, 200*time.Millisecond, 5*time.Millisecond)
		require.NoError(t, resolved.Lease.Context().Err())
		allocator.Close()
	})

	t.Run("missing member cancels immediately", func(t *testing.T) {
		cache := &accountEgressCacheStub{
			acquireResults: []AccountEgressAcquireResult{accountEgressAllocatorAcquiredResult()},
			refreshOwned:   false,
		}
		allocator := newAccountEgressAllocatorWithTiming(cache, time.Second, 10*time.Millisecond, 20*time.Millisecond, time.Minute, time.Now)
		defer allocator.Close()
		resolved, err := allocator.Acquire(context.Background(), AccountEgressAcquireRequest{Config: accountEgressAllocatorTestConfig(0), LeaseID: "missing"})
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			return errors.Is(context.Cause(resolved.Lease.Context()), ErrAccountEgressLeaseLost)
		}, 200*time.Millisecond, 5*time.Millisecond)
	})
}

func TestAccountEgressLeaseManagerToleratesTransientErrorOnlyUntilTTL(t *testing.T) {
	cache := &accountEgressCacheStub{refreshErr: errors.New("redis unavailable")}
	base := time.Now()
	allocator := newAccountEgressAllocatorWithTiming(cache, 100*time.Millisecond, time.Hour, time.Second, time.Minute, func() time.Time { return base })
	leaseCtx, cancel := context.WithCancelCause(context.Background())
	lease := &AccountEgressLease{
		ID: "transient", BindingID: "route:101", IdentityID: "ip:192.0.2.1", ConfigVersion: 7,
		accountID: 42, ctx: leaseCtx, cancel: cancel, allocator: allocator,
	}
	require.True(t, allocator.register(lease))
	ref := lease.ref()

	allocator.applyRefreshResult([]AccountEgressLeaseRef{ref}, nil, cache.refreshErr, base.Add(99*time.Millisecond))
	require.NoError(t, lease.Context().Err())
	allocator.applyRefreshResult([]AccountEgressLeaseRef{ref}, nil, cache.refreshErr, base.Add(100*time.Millisecond))
	require.ErrorIs(t, context.Cause(lease.Context()), ErrAccountEgressLeaseLost)
	allocator.Close()
}

func TestAccountEgressAllocatorCloseCancelsAndReleasesLeases(t *testing.T) {
	cache := &accountEgressCacheStub{
		acquireResults: []AccountEgressAcquireResult{accountEgressAllocatorAcquiredResult()},
		refreshOwned:   true,
	}
	allocator := newAccountEgressAllocatorWithTiming(cache, time.Second, time.Hour, time.Second, time.Minute, time.Now)
	resolved, err := allocator.Acquire(context.Background(), AccountEgressAcquireRequest{Config: accountEgressAllocatorTestConfig(0), LeaseID: "close-me"})
	require.NoError(t, err)
	allocator.Close()
	require.Error(t, resolved.Lease.Context().Err())
	_, _, releaseCalls, _ := cache.counts()
	require.Equal(t, 1, releaseCalls)
	_, err = allocator.Acquire(context.Background(), AccountEgressAcquireRequest{Config: accountEgressAllocatorTestConfig(0), LeaseID: "after-close"})
	require.ErrorIs(t, err, ErrAccountEgressUnavailable)
}

func TestAccountEgressLeaseDetachSurvivesRequestCancellationAndStillFailsOnLeaseLoss(t *testing.T) {
	cache := &accountEgressCacheStub{
		acquireResults: []AccountEgressAcquireResult{accountEgressAllocatorAcquiredResult()},
		refreshOwned:   true,
	}
	allocator := newAccountEgressAllocatorWithTiming(cache, 200*time.Millisecond, 10*time.Millisecond, 20*time.Millisecond, time.Minute, time.Now)
	defer allocator.Close()
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	resolved, err := allocator.Acquire(requestCtx, AccountEgressAcquireRequest{
		Config:  accountEgressAllocatorTestConfig(0),
		LeaseID: "detached-live",
	})
	require.NoError(t, err)
	originalLeaseCtx := resolved.Lease.Context()
	require.True(t, resolved.Lease.Detach())
	detachedLeaseCtx := resolved.Lease.Context()
	require.NotEqual(t, originalLeaseCtx, detachedLeaseCtx)

	cancelRequest()
	require.Eventually(t, func() bool { return originalLeaseCtx.Err() != nil }, time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool {
		_, refreshCalls, _, _ := cache.counts()
		return refreshCalls > 0
	}, time.Second, 5*time.Millisecond)
	require.NoError(t, detachedLeaseCtx.Err(), "detached lease must not inherit the HTTP request cancellation")

	cache.mu.Lock()
	cache.refreshOwned = false
	cache.mu.Unlock()
	require.Eventually(t, func() bool {
		return errors.Is(context.Cause(detachedLeaseCtx), ErrAccountEgressLeaseLost)
	}, time.Second, 5*time.Millisecond)

	resolved.Lease.Release()
	resolved.Lease.Release()
	_, _, releaseCalls, _ := cache.counts()
	require.Equal(t, 1, releaseCalls)
}

func TestAccountEgressLeaseAbandonStopsLocalRefreshWithoutReleasingRedis(t *testing.T) {
	cache := &accountEgressCacheStub{
		acquireResults: []AccountEgressAcquireResult{accountEgressAllocatorAcquiredResult()},
		refreshOwned:   true,
	}
	allocator := newAccountEgressAllocatorWithTiming(cache, time.Second, time.Hour, time.Second, time.Minute, time.Now)
	defer allocator.Close()
	resolved, err := allocator.Acquire(context.Background(), AccountEgressAcquireRequest{
		Config:  accountEgressAllocatorTestConfig(0),
		LeaseID: "transferred-live",
	})
	require.NoError(t, err)
	require.True(t, resolved.Lease.Detach())
	leaseCtx := resolved.Lease.Context()

	resolved.Lease.Abandon()
	require.ErrorIs(t, context.Cause(leaseCtx), context.Canceled)
	allocator.mu.Lock()
	require.Empty(t, allocator.leases)
	allocator.mu.Unlock()
	_, _, releaseCalls, _ := cache.counts()
	require.Zero(t, releaseCalls, "abandon must not delete a lease used by another process")

	resolved.Lease.Release()
	_, _, releaseCalls, _ = cache.counts()
	require.Zero(t, releaseCalls, "release after ownership transfer must remain local-only")
}

func TestAccountEgressLeaseRefreshWithinSafetyWindow(t *testing.T) {
	cache := &accountEgressCacheStub{
		acquireResults: []AccountEgressAcquireResult{accountEgressAllocatorAcquiredResult()},
		refreshOwned:   true,
	}
	base := time.Now()
	now := base
	allocator := newAccountEgressAllocatorWithTiming(cache, 100*time.Millisecond, time.Hour, time.Second, time.Minute, func() time.Time {
		return now
	})
	defer allocator.Close()
	resolved, err := allocator.Acquire(context.Background(), AccountEgressAcquireRequest{
		Config:  accountEgressAllocatorTestConfig(0),
		LeaseID: "live-safety-window",
	})
	require.NoError(t, err)
	cache.mu.Lock()
	cache.refreshErr = errors.New("redis unavailable")
	cache.mu.Unlock()

	now = base.Add(99 * time.Millisecond)
	require.NoError(t, resolved.Lease.RefreshWithinSafetyWindow(context.Background()))
	require.NoError(t, resolved.Lease.Context().Err())

	now = base.Add(100 * time.Millisecond)
	require.ErrorIs(t, resolved.Lease.RefreshWithinSafetyWindow(context.Background()), ErrAccountEgressLeaseLost)
	require.ErrorIs(t, context.Cause(resolved.Lease.Context()), ErrAccountEgressLeaseLost)
}

func TestRestoreAccountEgressLeaseValidatesAndProvesExactOwnership(t *testing.T) {
	cache := &accountEgressCacheStub{refreshOwned: true}
	allocator := newAccountEgressAllocatorWithTiming(cache, time.Second, time.Hour, time.Second, time.Minute, time.Now)
	defer allocator.Close()
	service := &ConcurrencyService{accountEgressAllocator: allocator}
	ref := AccountEgressLeaseRef{
		AccountID:     42,
		ID:            "persisted-live-egress",
		BindingID:     StableAccountEgressBindingID(42, 101),
		IdentityID:    "501",
		ConfigVersion: 7,
	}

	restoreCtx, cancelRestore := context.WithCancel(context.Background())
	lease, err := service.RestoreAccountEgressLease(restoreCtx, ref)
	require.NoError(t, err)
	require.Equal(t, ref, lease.ref())
	cancelRestore()
	require.NoError(t, lease.Context().Err(), "restored leases must use a detached context")
	_, refreshCalls, _, _ := cache.counts()
	require.Equal(t, 1, refreshCalls, "restore must prove Redis ownership before returning")

	same, err := service.RestoreAccountEgressLease(context.Background(), ref)
	require.NoError(t, err)
	require.Same(t, lease, same)

	invalid := []AccountEgressLeaseRef{
		{AccountID: 42, ID: ref.ID, BindingID: StableAccountEgressBindingID(99, 101), IdentityID: ref.IdentityID, ConfigVersion: ref.ConfigVersion},
		{AccountID: 42, ID: ref.ID, BindingID: ref.BindingID, IdentityID: "not-an-id", ConfigVersion: ref.ConfigVersion},
		{AccountID: 42, ID: ref.ID, BindingID: ref.BindingID, IdentityID: ref.IdentityID, ConfigVersion: 0},
		{AccountID: 42, ID: ref.ID, BindingID: StableAccountEgressBindingID(42, 102), IdentityID: ref.IdentityID, ConfigVersion: ref.ConfigVersion},
	}
	for _, invalidRef := range invalid {
		_, restoreErr := service.RestoreAccountEgressLease(context.Background(), invalidRef)
		require.Error(t, restoreErr)
	}

	lease.Release()
}
