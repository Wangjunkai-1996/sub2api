package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type openAIWSDetachedWriteCache struct {
	GatewayCache
	mu            sync.Mutex
	accountID     int64
	ctxValue      any
	hasDeadline   bool
	deadlineDelta time.Duration
}

type openAIWSResponseRoutingPairFailureStore struct {
	OpenAIWSStateStore
	egressStore openAIWSEgressStateStore

	bindAccountErr   error
	bindEgressErr    error
	deleteAccountErr error

	bindAccountCalls    int
	bindEgressCalls     int
	deleteAccountCalls  int
	deleteAccountCtxErr error
}

func newOpenAIWSResponseRoutingPairFailureStore() *openAIWSResponseRoutingPairFailureStore {
	base := NewOpenAIWSStateStore(nil)
	return &openAIWSResponseRoutingPairFailureStore{
		OpenAIWSStateStore: base,
		egressStore:        base.(openAIWSEgressStateStore),
	}
}

func (s *openAIWSResponseRoutingPairFailureStore) BindResponseAccount(
	ctx context.Context,
	groupID int64,
	responseID string,
	accountID int64,
	ttl time.Duration,
) error {
	s.bindAccountCalls++
	if err := s.OpenAIWSStateStore.BindResponseAccount(ctx, groupID, responseID, accountID, ttl); err != nil {
		return err
	}
	return s.bindAccountErr
}

func (s *openAIWSResponseRoutingPairFailureStore) DeleteResponseAccount(ctx context.Context, groupID int64, responseID string) error {
	s.deleteAccountCalls++
	s.deleteAccountCtxErr = ctx.Err()
	if err := s.OpenAIWSStateStore.DeleteResponseAccount(ctx, groupID, responseID); err != nil {
		return err
	}
	return s.deleteAccountErr
}

func (s *openAIWSResponseRoutingPairFailureStore) BindResponseEgress(
	ctx context.Context,
	groupID int64,
	responseID string,
	bindingID string,
	ttl time.Duration,
) error {
	s.bindEgressCalls++
	if s.bindEgressErr != nil {
		return s.bindEgressErr
	}
	if err := s.egressStore.BindResponseEgress(ctx, groupID, responseID, bindingID, ttl); err != nil {
		return err
	}
	return nil
}

func (s *openAIWSResponseRoutingPairFailureStore) GetResponseEgress(ctx context.Context, groupID int64, responseID string) (string, bool) {
	return s.egressStore.GetResponseEgress(ctx, groupID, responseID)
}

func (s *openAIWSResponseRoutingPairFailureStore) BindSessionEgress(ctx context.Context, groupID int64, sessionHash, bindingID string, ttl time.Duration) error {
	return s.egressStore.BindSessionEgress(ctx, groupID, sessionHash, bindingID, ttl)
}

func (s *openAIWSResponseRoutingPairFailureStore) GetSessionEgress(ctx context.Context, groupID int64, sessionHash string) (string, bool) {
	return s.egressStore.GetSessionEgress(ctx, groupID, sessionHash)
}

func (s *openAIWSResponseRoutingPairFailureStore) DeleteSessionEgress(ctx context.Context, groupID int64, sessionHash string) error {
	return s.egressStore.DeleteSessionEgress(ctx, groupID, sessionHash)
}

type openAIHTTPInvalidMarkerCache struct {
	*stubGatewayCache
	mu     sync.Mutex
	values map[string]openAIHTTPInvalidMarkerValue
	setErr error
}

type openAIHTTPInvalidMarkerValue struct {
	value     int64
	expiresAt time.Time
}

func (c *openAIHTTPInvalidMarkerCache) markerKey(groupID int64, sessionHash string) string {
	return fmt.Sprintf("%d:%s", groupID, sessionHash)
}

func (c *openAIHTTPInvalidMarkerCache) GetSessionAccountID(_ context.Context, groupID int64, sessionHash string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	value, ok := c.values[c.markerKey(groupID, sessionHash)]
	if !ok || !time.Now().Before(value.expiresAt) {
		if ok {
			delete(c.values, c.markerKey(groupID, sessionHash))
		}
		return 0, ErrStickySessionNotFound
	}
	return value.value, nil
}

func (c *openAIHTTPInvalidMarkerCache) SetSessionAccountID(ctx context.Context, groupID int64, sessionHash string, accountID int64, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.setErr != nil {
		return c.setErr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.values == nil {
		c.values = make(map[string]openAIHTTPInvalidMarkerValue)
	}
	c.values[c.markerKey(groupID, sessionHash)] = openAIHTTPInvalidMarkerValue{
		value: accountID, expiresAt: time.Now().Add(ttl),
	}
	return nil
}

func (c *openAIHTTPInvalidMarkerCache) DeleteSessionAccountID(_ context.Context, groupID int64, sessionHash string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.values, c.markerKey(groupID, sessionHash))
	return nil
}

func (c *openAIWSDetachedWriteCache) SetSessionAccountID(ctx context.Context, _ int64, _ string, accountID int64, _ time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.accountID = accountID
	c.ctxValue = ctx.Value(openAIWSDetachedWriteContextKey{})
	if deadline, ok := ctx.Deadline(); ok {
		c.hasDeadline = true
		c.deadlineDelta = time.Until(deadline)
	}
	c.mu.Unlock()
	return nil
}

func (c *openAIWSDetachedWriteCache) GetSessionAccountID(context.Context, int64, string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.accountID <= 0 {
		return 0, ErrStickySessionNotFound
	}
	return c.accountID, nil
}

type openAIWSDetachedWriteContextKey struct{}

func TestOpenAIWSStateStore_BindGetDeleteResponseAccount(t *testing.T) {
	cache := &stubGatewayCache{}
	store := NewOpenAIWSStateStore(cache)
	ctx := context.Background()
	groupID := int64(7)

	require.NoError(t, store.BindResponseAccount(ctx, groupID, "resp_abc", 101, time.Minute))

	accountID, err := store.GetResponseAccount(ctx, groupID, "resp_abc")
	require.NoError(t, err)
	require.Equal(t, int64(101), accountID)

	require.NoError(t, store.DeleteResponseAccount(ctx, groupID, "resp_abc"))
	accountID, err = store.GetResponseAccount(ctx, groupID, "resp_abc")
	require.NoError(t, err)
	require.Zero(t, accountID)
}

func TestBindOpenAIWSResponseRoutingPairPreservesFailClosedMarkersOnWriteFailure(t *testing.T) {
	ctx := context.Background()
	const (
		groupID    = int64(17)
		accountID  = int64(101)
		responseID = "resp_pair_rollback"
	)
	bindingID := StableAccountEgressBindingID(accountID, 11)

	assertPairMissing := func(t *testing.T, store *openAIWSResponseRoutingPairFailureStore) {
		t.Helper()
		gotAccountID, err := store.GetResponseAccount(ctx, groupID, responseID)
		require.NoError(t, err)
		require.Zero(t, gotAccountID)
		_, found := store.GetResponseEgress(ctx, groupID, responseID)
		require.False(t, found)
	}

	t.Run("empty route keeps legacy account marker", func(t *testing.T) {
		store := newOpenAIWSResponseRoutingPairFailureStore()

		err := bindOpenAIWSResponseRoutingPair(store, ctx, groupID, responseID, accountID, "", time.Minute)

		require.NoError(t, err)
		require.Zero(t, store.bindEgressCalls)
		require.Zero(t, store.deleteAccountCalls)
		gotAccountID, getErr := store.GetResponseAccount(ctx, groupID, responseID)
		require.NoError(t, getErr)
		require.Equal(t, accountID, gotAccountID)
		_, found := store.GetResponseEgress(ctx, groupID, responseID)
		require.False(t, found)
	})

	t.Run("mismatched route account writes neither marker", func(t *testing.T) {
		store := newOpenAIWSResponseRoutingPairFailureStore()
		mismatchedBindingID := StableAccountEgressBindingID(accountID+1, 11)

		err := bindOpenAIWSResponseRoutingPair(store, ctx, groupID, responseID, accountID, mismatchedBindingID, time.Minute)

		require.ErrorIs(t, err, ErrAccountEgressConfigStale)
		require.Zero(t, store.bindAccountCalls)
		require.Zero(t, store.bindEgressCalls)
		require.Zero(t, store.deleteAccountCalls)
		assertPairMissing(t, store)
	})

	t.Run("account write failure preserves partial account fence", func(t *testing.T) {
		bindErr := errors.New("account write failed")
		store := newOpenAIWSResponseRoutingPairFailureStore()
		store.bindAccountErr = bindErr

		err := bindOpenAIWSResponseRoutingPair(store, ctx, groupID, responseID, accountID, bindingID, time.Minute)

		require.ErrorIs(t, err, bindErr)
		require.Zero(t, store.bindEgressCalls)
		require.Zero(t, store.deleteAccountCalls)
		gotAccountID, getErr := store.GetResponseAccount(ctx, groupID, responseID)
		require.NoError(t, getErr)
		require.Equal(t, accountID, gotAccountID)
		_, found := store.GetResponseEgress(ctx, groupID, responseID)
		require.False(t, found)
	})

	t.Run("route write failure preserves account-only fence", func(t *testing.T) {
		bindErr := errors.New("route write failed")
		store := newOpenAIWSResponseRoutingPairFailureStore()
		store.bindEgressErr = bindErr

		err := bindOpenAIWSResponseRoutingPair(store, ctx, groupID, responseID, accountID, bindingID, time.Minute)

		require.ErrorIs(t, err, bindErr)
		require.Equal(t, 1, store.bindEgressCalls)
		require.Zero(t, store.deleteAccountCalls)
		gotAccountID, getErr := store.GetResponseAccount(ctx, groupID, responseID)
		require.NoError(t, getErr)
		require.Equal(t, accountID, gotAccountID)
		_, found := store.GetResponseEgress(ctx, groupID, responseID)
		require.False(t, found)
	})

	t.Run("route write failure preserves an existing hard fence", func(t *testing.T) {
		bindErr := errors.New("route write failed")
		store := newOpenAIWSResponseRoutingPairFailureStore()
		require.NoError(t, store.egressStore.BindResponseEgress(ctx, groupID, responseID, bindingID, time.Minute))
		store.bindEgressErr = bindErr

		err := bindOpenAIWSResponseRoutingPair(store, ctx, groupID, responseID, accountID, bindingID, time.Minute)

		require.ErrorIs(t, err, bindErr)
		require.Zero(t, store.deleteAccountCalls)
		gotBindingID, found := store.GetResponseEgress(ctx, groupID, responseID)
		require.True(t, found)
		require.Equal(t, bindingID, gotBindingID)
	})

	t.Run("canceled request does not erase partial markers", func(t *testing.T) {
		bindErr := errors.New("route write failed")
		store := newOpenAIWSResponseRoutingPairFailureStore()
		store.bindEgressErr = bindErr
		canceledCtx, cancel := context.WithCancel(ctx)
		cancel()

		err := bindOpenAIWSResponseRoutingPair(store, canceledCtx, groupID, responseID, accountID, bindingID, time.Minute)

		require.ErrorIs(t, err, bindErr)
		require.Zero(t, store.deleteAccountCalls)
		gotAccountID, getErr := store.GetResponseAccount(ctx, groupID, responseID)
		require.NoError(t, getErr)
		require.Equal(t, accountID, gotAccountID)
	})

	t.Run("write error is returned without invoking configured deletes", func(t *testing.T) {
		bindErr := errors.New("account write failed")
		store := newOpenAIWSResponseRoutingPairFailureStore()
		store.bindAccountErr = bindErr
		store.deleteAccountErr = errors.New("account delete must not run")

		err := bindOpenAIWSResponseRoutingPair(store, ctx, groupID, responseID, accountID, bindingID, time.Minute)

		require.ErrorIs(t, err, bindErr)
		require.Zero(t, store.deleteAccountCalls)
	})
}

func TestOpenAIWSStateStore_ResponseEgressIsolatedByGroup(t *testing.T) {
	ctx := context.Background()
	store := NewOpenAIWSStateStore(nil).(openAIWSEgressStateStore)
	responseID := "resp_shared_across_groups"

	require.NoError(t, store.BindResponseEgress(ctx, 101, responseID, StableAccountEgressBindingID(1001, 11), time.Minute))
	require.NoError(t, store.BindResponseEgress(ctx, 202, responseID, StableAccountEgressBindingID(2002, 22), time.Minute))

	binding, found := store.GetResponseEgress(ctx, 101, responseID)
	require.True(t, found)
	require.Equal(t, StableAccountEgressBindingID(1001, 11), binding)
	binding, found = store.GetResponseEgress(ctx, 202, responseID)
	require.True(t, found)
	require.Equal(t, StableAccountEgressBindingID(2002, 22), binding)

	// Deleting the response in one group must not remove another group's fence
	// when response IDs happen to be identical.
	defaultStore := store.(*defaultOpenAIWSStateStore)
	require.NoError(t, defaultStore.BindResponseAccount(ctx, 101, responseID, 1001, time.Minute))
	require.NoError(t, defaultStore.BindResponseAccount(ctx, 202, responseID, 2002, time.Minute))
	require.NoError(t, defaultStore.DeleteResponseAccount(ctx, 101, responseID))
	_, found = store.GetResponseEgress(ctx, 101, responseID)
	require.False(t, found)
	binding, found = store.GetResponseEgress(ctx, 202, responseID)
	require.True(t, found)
	require.Equal(t, StableAccountEgressBindingID(2002, 22), binding)
}

func TestOpenAIWSStateStore_HTTPResponseOwnerPersistsAcrossStoreInstances(t *testing.T) {
	cache := &stubGatewayCache{}
	ctx := context.Background()
	groupID := int64(8)
	writer := NewOpenAIWSStateStore(cache)

	require.NoError(t, writer.BindHTTPResponseOwner(ctx, groupID, "resp_owned", 201, 301, time.Minute))
	userID, apiKeyID, found, err := writer.GetHTTPResponseOwner(ctx, groupID, "resp_owned")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(201), userID)
	require.Equal(t, int64(301), apiKeyID)

	reader := NewOpenAIWSStateStore(cache)
	userID, apiKeyID, found, err = reader.GetHTTPResponseOwner(ctx, groupID, "resp_owned")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(201), userID)
	require.Equal(t, int64(301), apiKeyID)
}

func TestOpenAIWSStateStore_BindResponseAccountPersistsAfterParentCanceled(t *testing.T) {
	cache := &openAIWSDetachedWriteCache{}
	writer := NewOpenAIWSStateStore(cache)
	parent := context.WithValue(context.Background(), openAIWSDetachedWriteContextKey{}, "request-value")
	canceled, cancel := context.WithCancel(parent)
	cancel()

	require.NoError(t, writer.BindResponseAccount(canceled, 12, "resp_after_cancel", 10601, time.Hour))

	reader := NewOpenAIWSStateStore(cache)
	accountID, err := reader.GetResponseAccount(context.Background(), 12, "resp_after_cancel")
	require.NoError(t, err)
	require.Equal(t, int64(10601), accountID, "a fresh process must resolve the persisted response binding")
	require.Equal(t, "request-value", cache.ctxValue, "detaching cancellation must preserve request context values")
	require.True(t, cache.hasDeadline)
	require.Greater(t, cache.deadlineDelta, 2*time.Second)
	require.LessOrEqual(t, cache.deadlineDelta, openAIWSStateStoreRedisTimeout)
}

func TestOpenAIWSStateStore_ResponseConnTTL(t *testing.T) {
	store := NewOpenAIWSStateStore(nil)
	store.BindResponseConn("resp_conn", "conn_1", 30*time.Millisecond)

	connID, ok := store.GetResponseConn("resp_conn")
	require.True(t, ok)
	require.Equal(t, "conn_1", connID)

	time.Sleep(60 * time.Millisecond)
	_, ok = store.GetResponseConn("resp_conn")
	require.False(t, ok)
}

func TestOpenAIWSStateStore_SessionTurnStateTTL(t *testing.T) {
	store := NewOpenAIWSStateStore(nil)
	store.BindSessionTurnState(9, "session_hash_1", "turn_state_1", 30*time.Millisecond)

	state, ok := store.GetSessionTurnState(9, "session_hash_1")
	require.True(t, ok)
	require.Equal(t, "turn_state_1", state)

	// group 隔离
	_, ok = store.GetSessionTurnState(10, "session_hash_1")
	require.False(t, ok)

	time.Sleep(60 * time.Millisecond)
	_, ok = store.GetSessionTurnState(9, "session_hash_1")
	require.False(t, ok)
}

func TestOpenAIWSStateStore_SessionConnTTL(t *testing.T) {
	store := NewOpenAIWSStateStore(nil)
	store.BindSessionConn(9, "session_hash_conn_1", "conn_1", 30*time.Millisecond)

	connID, ok := store.GetSessionConn(9, "session_hash_conn_1")
	require.True(t, ok)
	require.Equal(t, "conn_1", connID)

	// group 隔离
	_, ok = store.GetSessionConn(10, "session_hash_conn_1")
	require.False(t, ok)

	time.Sleep(60 * time.Millisecond)
	_, ok = store.GetSessionConn(9, "session_hash_conn_1")
	require.False(t, ok)
}

func TestOpenAIWSStateStore_GetResponseAccount_NoStaleAfterCacheMiss(t *testing.T) {
	cache := &stubGatewayCache{sessionBindings: map[string]int64{}}
	store := NewOpenAIWSStateStore(cache)
	ctx := context.Background()
	groupID := int64(17)
	responseID := "resp_cache_stale"
	cacheKey := openAIWSResponseAccountCacheKey(responseID)

	cache.sessionBindings[cacheKey] = 501
	accountID, err := store.GetResponseAccount(ctx, groupID, responseID)
	require.NoError(t, err)
	require.Equal(t, int64(501), accountID)

	delete(cache.sessionBindings, cacheKey)
	accountID, err = store.GetResponseAccount(ctx, groupID, responseID)
	require.NoError(t, err)
	require.Zero(t, accountID, "上游缓存失效后不应继续命中本地陈旧映射")
}

func TestOpenAIWSStateStore_MaybeCleanupRemovesExpiredIncrementally(t *testing.T) {
	raw := NewOpenAIWSStateStore(nil)
	store, ok := raw.(*defaultOpenAIWSStateStore)
	require.True(t, ok)

	expiredAt := time.Now().Add(-time.Minute)
	total := 2048
	store.responseToConnMu.Lock()
	for i := 0; i < total; i++ {
		store.responseToConn[fmt.Sprintf("resp_%d", i)] = openAIWSConnBinding{
			connID:    "conn_incremental",
			expiresAt: expiredAt,
		}
	}
	store.responseToConnMu.Unlock()

	store.lastCleanupUnixNano.Store(time.Now().Add(-2 * openAIWSStateStoreCleanupInterval).UnixNano())
	store.maybeCleanup()

	store.responseToConnMu.RLock()
	remainingAfterFirst := len(store.responseToConn)
	store.responseToConnMu.RUnlock()
	require.Less(t, remainingAfterFirst, total, "单轮 cleanup 应至少有进展")
	require.Greater(t, remainingAfterFirst, 0, "增量清理不要求单轮清空全部键")

	for i := 0; i < 8; i++ {
		store.lastCleanupUnixNano.Store(time.Now().Add(-2 * openAIWSStateStoreCleanupInterval).UnixNano())
		store.maybeCleanup()
	}

	store.responseToConnMu.RLock()
	remaining := len(store.responseToConn)
	store.responseToConnMu.RUnlock()
	require.Zero(t, remaining, "多轮 cleanup 后应逐步清空全部过期键")
}

func TestEnsureBindingCapacity_EvictsOneWhenMapIsFull(t *testing.T) {
	bindings := map[string]int{
		"a": 1,
		"b": 2,
	}

	ensureBindingCapacity(bindings, "c", 2)
	bindings["c"] = 3

	require.Len(t, bindings, 2)
	require.Equal(t, 3, bindings["c"])
}

func TestEnsureBindingCapacity_DoesNotEvictWhenUpdatingExistingKey(t *testing.T) {
	bindings := map[string]int{
		"a": 1,
		"b": 2,
	}

	ensureBindingCapacity(bindings, "a", 2)
	bindings["a"] = 9

	require.Len(t, bindings, 2)
	require.Equal(t, 9, bindings["a"])
}

type openAIWSStateStoreTimeoutProbeCache struct {
	setHasDeadline    bool
	getHasDeadline    bool
	deleteHasDeadline bool
	setDeadlineDelta  time.Duration
	getDeadlineDelta  time.Duration
	delDeadlineDelta  time.Duration
}

func (c *openAIWSStateStoreTimeoutProbeCache) GetSessionAccountID(ctx context.Context, _ int64, _ string) (int64, error) {
	if deadline, ok := ctx.Deadline(); ok {
		c.getHasDeadline = true
		c.getDeadlineDelta = time.Until(deadline)
	}
	return 123, nil
}

func (c *openAIWSStateStoreTimeoutProbeCache) SetSessionAccountID(ctx context.Context, _ int64, _ string, _ int64, _ time.Duration) error {
	if deadline, ok := ctx.Deadline(); ok {
		c.setHasDeadline = true
		c.setDeadlineDelta = time.Until(deadline)
	}
	return errors.New("set failed")
}

func (c *openAIWSStateStoreTimeoutProbeCache) RefreshSessionTTL(context.Context, int64, string, time.Duration) error {
	return nil
}

func (c *openAIWSStateStoreTimeoutProbeCache) DeleteSessionAccountID(ctx context.Context, _ int64, _ string) error {
	if deadline, ok := ctx.Deadline(); ok {
		c.deleteHasDeadline = true
		c.delDeadlineDelta = time.Until(deadline)
	}
	return nil
}

func (c *openAIWSStateStoreTimeoutProbeCache) SetGrokVideoPendingBilling(_ context.Context, _ string, _ []byte, _ time.Duration) error {
	return nil
}
func (c *openAIWSStateStoreTimeoutProbeCache) GetGrokVideoPendingBilling(_ context.Context, _ string) ([]byte, error) {
	return nil, nil
}
func (c *openAIWSStateStoreTimeoutProbeCache) ClaimGrokVideoBilled(_ context.Context, _ string, _ time.Duration) (bool, error) {
	return true, nil
}

func (c *openAIWSStateStoreTimeoutProbeCache) ReleaseGrokVideoBilled(_ context.Context, _ string) error {
	return nil
}

func (c *openAIWSStateStoreTimeoutProbeCache) SetReasoningContent(_ context.Context, _ string, _ string, _ time.Duration) error {
	return nil
}
func (c *openAIWSStateStoreTimeoutProbeCache) GetReasoningContent(_ context.Context, _ string) (string, error) {
	return "", ErrReasoningContentNotFound
}

func TestOpenAIWSStateStore_RedisOpsUseShortTimeout(t *testing.T) {
	probe := &openAIWSStateStoreTimeoutProbeCache{}
	store := NewOpenAIWSStateStore(probe)
	ctx := context.Background()
	groupID := int64(5)

	err := store.BindResponseAccount(ctx, groupID, "resp_timeout_probe", 11, time.Minute)
	require.Error(t, err)

	accountID, getErr := store.GetResponseAccount(ctx, groupID, "resp_timeout_probe")
	require.NoError(t, getErr)
	require.Equal(t, int64(11), accountID, "本地缓存命中应优先返回已绑定账号")

	require.NoError(t, store.DeleteResponseAccount(ctx, groupID, "resp_timeout_probe"))

	require.True(t, probe.setHasDeadline, "SetSessionAccountID 应携带独立超时上下文")
	require.True(t, probe.deleteHasDeadline, "DeleteSessionAccountID 应携带独立超时上下文")
	require.False(t, probe.getHasDeadline, "GetSessionAccountID 本用例应由本地缓存命中，不触发 Redis 读取")
	require.Greater(t, probe.setDeadlineDelta, 2*time.Second)
	require.LessOrEqual(t, probe.setDeadlineDelta, 3*time.Second)
	require.Greater(t, probe.delDeadlineDelta, 2*time.Second)
	require.LessOrEqual(t, probe.delDeadlineDelta, 3*time.Second)

	probe2 := &openAIWSStateStoreTimeoutProbeCache{}
	store2 := NewOpenAIWSStateStore(probe2)
	accountID2, err2 := store2.GetResponseAccount(ctx, groupID, "resp_cache_only")
	require.NoError(t, err2)
	require.Equal(t, int64(123), accountID2)
	require.True(t, probe2.getHasDeadline, "GetSessionAccountID 在缓存未命中时应携带独立超时上下文")
	require.Greater(t, probe2.getDeadlineDelta, 2*time.Second)
	require.LessOrEqual(t, probe2.getDeadlineDelta, 3*time.Second)
}

func TestOpenAIWSStateStore_HTTPResponseInvalidMarkerPersistsAcrossInstancesAndExpires(t *testing.T) {
	cache := &openAIHTTPInvalidMarkerCache{stubGatewayCache: &stubGatewayCache{}}
	ctx := context.Background()
	groupID := int64(41)
	responseID := "resp_invalid_marker"

	writer := NewOpenAIWSStateStore(cache)
	invalidWriter, ok := writer.(openAIHTTPResponseInvalidStateStore)
	require.True(t, ok)
	require.NoError(t, invalidWriter.MarkHTTPResponseInvalid(ctx, groupID, responseID, OpenAISessionBlockedReason, 30*time.Millisecond))

	reader := NewOpenAIWSStateStore(cache)
	reason, found, err := reader.(openAIHTTPResponseInvalidStateStore).GetHTTPResponseInvalidReason(ctx, groupID, responseID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, OpenAISessionBlockedReason, reason)

	time.Sleep(50 * time.Millisecond)
	readerAfterExpiry := NewOpenAIWSStateStore(cache)
	reason, found, err = readerAfterExpiry.(openAIHTTPResponseInvalidStateStore).GetHTTPResponseInvalidReason(ctx, groupID, responseID)
	require.NoError(t, err)
	require.False(t, found)
	require.Empty(t, reason)
}

func TestOpenAIWSStateStore_HTTPResponseInvalidMarkerWriteFailureIsReturned(t *testing.T) {
	cache := &openAIHTTPInvalidMarkerCache{
		stubGatewayCache: &stubGatewayCache{},
		setErr:           errors.New("redis unavailable"),
	}
	store := NewOpenAIWSStateStore(cache).(openAIHTTPResponseInvalidStateStore)
	err := store.MarkHTTPResponseInvalid(context.Background(), 42, "resp_write_failed", OpenAISessionBlockedReason, time.Minute)
	require.Error(t, err)
}

func TestInvalidateOpenAIHTTPContinuationClearsLocalTurnStateWithoutCache(t *testing.T) {
	store := NewOpenAIWSStateStore(nil)
	store.BindSessionTurnState(43, "session_to_clear", "old-turn-state", time.Minute)
	svc := &OpenAIGatewayService{openaiWSStateStore: store}

	require.NoError(t, svc.InvalidateOpenAIHTTPContinuation(
		context.Background(), nil, 43, "", "session_to_clear", 101,
	))
	_, found := store.GetSessionTurnState(43, "session_to_clear")
	require.False(t, found)
}

func TestWithOpenAIWSStateStoreRedisTimeout_WithParentContext(t *testing.T) {
	ctx, cancel := withOpenAIWSStateStoreRedisTimeout(context.Background())
	defer cancel()
	require.NotNil(t, ctx)
	_, ok := ctx.Deadline()
	require.True(t, ok, "应附加短超时")

	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()
	canceledCtx, cancelCanceledCtx := withOpenAIWSStateStoreRedisTimeout(parent)
	defer cancelCanceledCtx()
	require.ErrorIs(t, canceledCtx.Err(), context.Canceled, "读取和删除使用的通用 helper 必须继续继承父取消")
}
