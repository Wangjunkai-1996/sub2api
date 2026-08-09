package service

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newCyberBlockTestCtx(headers map[string]string, body string) (*gin.Context, []byte) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest("POST", "/openai/v1/responses", strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	c.Request = req
	return c, []byte(body)
}

// TestCyberSessionBlockKey verifies F5a key derivation: explicit session signals
// only (header session_id/conversation_id or body prompt_cache_key), apiKey
// isolated, and EMPTY when no explicit signal (no content-derived fallback —
// "不退化" decision).
func TestCyberSessionBlockKey(t *testing.T) {
	c1, b1 := newCyberBlockTestCtx(map[string]string{"session_id": "sess-abc"}, `{}`)
	k1 := CyberSessionBlockKey(101, c1, b1)
	require.NotEmpty(t, k1)

	// Same session, different apiKey → different key (isolation).
	c2, b2 := newCyberBlockTestCtx(map[string]string{"session_id": "sess-abc"}, `{}`)
	require.NotEqual(t, k1, CyberSessionBlockKey(202, c2, b2))

	// Same session + same apiKey → stable key.
	c3, b3 := newCyberBlockTestCtx(map[string]string{"session_id": "sess-abc"}, `{}`)
	require.Equal(t, k1, CyberSessionBlockKey(101, c3, b3))

	// prompt_cache_key in body counts as explicit.
	c4, b4 := newCyberBlockTestCtx(nil, `{"prompt_cache_key":"pck-1"}`)
	require.NotEmpty(t, CyberSessionBlockKey(101, c4, b4))

	// No explicit signal → empty key → caller must skip blocking entirely.
	c5, b5 := newCyberBlockTestCtx(nil, `{"input":"hello world"}`)
	require.Empty(t, CyberSessionBlockKey(101, c5, b5))

	// conversation_id header counts as explicit; key is stable and non-empty.
	c6, b6 := newCyberBlockTestCtx(map[string]string{"conversation_id": "conv-xyz"}, `{}`)
	k6 := CyberSessionBlockKey(101, c6, b6)
	require.NotEmpty(t, k6)
	c6b, b6b := newCyberBlockTestCtx(map[string]string{"conversation_id": "conv-xyz"}, `{}`)
	require.Equal(t, k6, CyberSessionBlockKey(101, c6b, b6b), "conversation_id key must be stable")
}

// --- fakes ---

type fakeCyberBlockStore struct {
	blocked    map[string]bool
	readCalls  int
	writeCalls int
	readErr    error
	writeErr   error
	lastTTL    time.Duration
}

var _ CyberSessionBlockStore = (*fakeCyberBlockStore)(nil)

func (f *fakeCyberBlockStore) SetCyberSessionBlocked(_ context.Context, key string, ttl time.Duration) error {
	f.writeCalls++
	f.lastTTL = ttl
	if f.writeErr != nil {
		return f.writeErr
	}
	if f.blocked == nil {
		f.blocked = map[string]bool{}
	}
	f.blocked[key] = true
	return nil
}

func (f *fakeCyberBlockStore) IsCyberSessionBlocked(_ context.Context, key string) (bool, error) {
	f.readCalls++
	if f.readErr != nil {
		return false, f.readErr
	}
	return f.blocked[key], nil
}

// fakeSettingRepo is a minimal SettingRepository stub for unit tests.
type fakeSettingRepo struct {
	vals map[string]string
	errs map[string]error
}

func (r *fakeSettingRepo) GetValue(_ context.Context, key string) (string, error) {
	if err := r.errs[key]; err != nil {
		return "", err
	}
	v, ok := r.vals[key]
	if !ok {
		return "", ErrSettingNotFound
	}
	return v, nil
}
func (r *fakeSettingRepo) Get(_ context.Context, _ string) (*Setting, error) {
	panic("fakeSettingRepo.Get not implemented")
}
func (r *fakeSettingRepo) Set(_ context.Context, _, _ string) error {
	panic("fakeSettingRepo.Set not implemented")
}
func (r *fakeSettingRepo) GetMultiple(_ context.Context, keys []string) (map[string]string, error) {
	result := make(map[string]string, len(keys))
	for _, key := range keys {
		if err := r.errs[key]; err != nil {
			return nil, err
		}
		if value, ok := r.vals[key]; ok {
			result[key] = value
		}
	}
	return result, nil
}
func (r *fakeSettingRepo) SetMultiple(_ context.Context, _ map[string]string) error {
	panic("fakeSettingRepo.SetMultiple not implemented")
}
func (r *fakeSettingRepo) GetAll(_ context.Context) (map[string]string, error) {
	panic("fakeSettingRepo.GetAll not implemented")
}
func (r *fakeSettingRepo) Delete(_ context.Context, _ string) error {
	panic("fakeSettingRepo.Delete not implemented")
}

var _ SettingRepository = (*fakeSettingRepo)(nil)

// comboCacheAndStore implements both GatewayCache (no-op stubs) and
// CyberSessionBlockStore (delegates to fakeCyberBlockStore) so it can be
// injected as s.cache and successfully type-asserted to CyberSessionBlockStore.
type comboCacheAndStore struct {
	store fakeCyberBlockStore
}

var _ GatewayCache = (*comboCacheAndStore)(nil)
var _ CyberSessionBlockStore = (*comboCacheAndStore)(nil)

func (c *comboCacheAndStore) GetSessionAccountID(_ context.Context, _ int64, _ string) (int64, error) {
	return 0, errors.New("stub")
}
func (c *comboCacheAndStore) SetSessionAccountID(_ context.Context, _ int64, _ string, _ int64, _ time.Duration) error {
	return nil
}
func (c *comboCacheAndStore) RefreshSessionTTL(_ context.Context, _ int64, _ string, _ time.Duration) error {
	return nil
}
func (c *comboCacheAndStore) DeleteSessionAccountID(_ context.Context, _ int64, _ string) error {
	return nil
}
func (c *comboCacheAndStore) SetCyberSessionBlocked(ctx context.Context, key string, ttl time.Duration) error {
	return c.store.SetCyberSessionBlocked(ctx, key, ttl)
}
func (c *comboCacheAndStore) IsCyberSessionBlocked(ctx context.Context, key string) (bool, error) {
	return c.store.IsCyberSessionBlocked(ctx, key)
}

// --- tests ---

// TestIsCyberSessionBlocked_EmptyKeyAndNilService covers the fail-open paths:
// empty key, nil service, store missing → always false / no panic.
func TestIsCyberSessionBlocked_EmptyKeyAndNilService(t *testing.T) {
	var nilSvc *OpenAIGatewayService
	require.False(t, nilSvc.IsCyberSessionBlocked(context.Background(), nil, "k"))
	require.NotPanics(t, func() { nilSvc.MarkCyberSessionBlocked(context.Background(), nil, "k") })

	svc := &OpenAIGatewayService{}
	require.False(t, svc.IsCyberSessionBlocked(context.Background(), nil, ""))
	require.False(t, svc.IsCyberSessionBlocked(context.Background(), nil, "k"), "no store + no settings → fail-open false")
}

// TestCyberSessionBlock_RoundTrip exercises the type-assertion success path:
// mark a session blocked via a combo cache+store, then confirm IsCyberSessionBlocked
// returns true, and an unrelated key returns false.
func TestCyberSessionBlock_RoundTrip(t *testing.T) {
	// SettingService with only settingRepo set — GetCyberSessionBlockRuntime needs
	// nothing else (cfg/proxyRepo/etc. are not touched by this code path).
	settingSvc := &SettingService{
		settingRepo: &fakeSettingRepo{
			vals: map[string]string{
				SettingKeyCyberSessionBlockEnabled:    "true",
				SettingKeyCyberSessionBlockTTLSeconds: "60",
			},
		},
	}

	combo := &comboCacheAndStore{}
	svc := &OpenAIGatewayService{
		cache:          combo,
		settingService: settingSvc,
	}

	ctx := context.Background()
	const testKey = "deadbeef1234"

	groupID := int64(12)

	// Before marking: not blocked. Missing scope keys retain the legacy all-groups behavior.
	require.False(t, svc.IsCyberSessionBlocked(ctx, &groupID, testKey))

	// Mark as blocked.
	svc.MarkCyberSessionBlocked(ctx, &groupID, testKey)

	// After marking: blocked.
	require.True(t, svc.IsCyberSessionBlocked(ctx, &groupID, testKey))

	// Different key: still not blocked.
	require.False(t, svc.IsCyberSessionBlocked(ctx, &groupID, "other-key"))
	require.Equal(t, time.Minute, combo.store.lastTTL)
}

func TestCyberSessionBlock_AllGroupsIncludesGroupedAndUngroupedKeys(t *testing.T) {
	settingSvc := &SettingService{settingRepo: &fakeSettingRepo{vals: map[string]string{
		SettingKeyCyberSessionBlockEnabled:    "true",
		SettingKeyCyberSessionBlockTTLSeconds: "60",
		SettingKeyCyberSessionBlockAllGroups:  "true",
		SettingKeyCyberSessionBlockGroupIDs:   `[]`,
	}}}
	combo := &comboCacheAndStore{}
	svc := &OpenAIGatewayService{cache: combo, settingService: settingSvc}

	group12 := int64(12)
	group13 := int64(13)
	for i, groupID := range []*int64{&group12, &group13, nil} {
		key := []string{"group-12", "group-13", "ungrouped"}[i]
		svc.MarkCyberSessionBlocked(context.Background(), groupID, key)
		require.True(t, svc.IsCyberSessionBlocked(context.Background(), groupID, key))
	}
	require.Equal(t, 3, combo.store.writeCalls)
	require.Equal(t, 3, combo.store.readCalls)
}

func TestCyberSessionBlock_ScopedGroupSkipsRedisForOutOfScopeKeys(t *testing.T) {
	settingSvc := &SettingService{settingRepo: &fakeSettingRepo{vals: map[string]string{
		SettingKeyCyberSessionBlockEnabled:    "true",
		SettingKeyCyberSessionBlockTTLSeconds: "60",
		SettingKeyCyberSessionBlockAllGroups:  "false",
		SettingKeyCyberSessionBlockGroupIDs:   `[12]`,
	}}}
	combo := &comboCacheAndStore{store: fakeCyberBlockStore{blocked: map[string]bool{
		"plus-existing": true,
	}}}
	svc := &OpenAIGatewayService{cache: combo, settingService: settingSvc}
	group12 := int64(12)
	group13 := int64(13)

	require.True(t, svc.CyberSessionBlockEnabledForGroup(context.Background(), &group12))
	require.False(t, svc.CyberSessionBlockEnabledForGroup(context.Background(), &group13))
	require.False(t, svc.CyberSessionBlockEnabledForGroup(context.Background(), nil))

	svc.MarkCyberSessionBlocked(context.Background(), &group13, "plus-new")
	svc.MarkCyberSessionBlocked(context.Background(), nil, "ungrouped-new")
	require.Zero(t, combo.store.writeCalls, "out-of-scope groups must not write Redis")

	require.False(t, svc.IsCyberSessionBlocked(context.Background(), &group13, "plus-existing"))
	require.False(t, svc.IsCyberSessionBlocked(context.Background(), nil, "plus-existing"))
	require.Zero(t, combo.store.readCalls, "out-of-scope groups must not query Redis, even for existing keys")

	svc.MarkCyberSessionBlocked(context.Background(), &group12, "pro-new")
	require.True(t, svc.IsCyberSessionBlocked(context.Background(), &group12, "pro-new"))
	require.Equal(t, 1, combo.store.writeCalls)
	require.Equal(t, 1, combo.store.readCalls)
}

func TestCyberSessionBlock_SettingReadFailureFailsOpenBeforeRedis(t *testing.T) {
	dbErr := errors.New("settings unavailable")
	settingSvc := &SettingService{settingRepo: &fakeSettingRepo{
		vals: map[string]string{},
		errs: map[string]error{SettingKeyCyberSessionBlockEnabled: dbErr},
	}}
	combo := &comboCacheAndStore{store: fakeCyberBlockStore{blocked: map[string]bool{"existing": true}}}
	svc := &OpenAIGatewayService{cache: combo, settingService: settingSvc}
	groupID := int64(12)

	svc.MarkCyberSessionBlocked(context.Background(), &groupID, "new")
	require.False(t, svc.IsCyberSessionBlocked(context.Background(), &groupID, "existing"))
	require.Zero(t, combo.store.writeCalls)
	require.Zero(t, combo.store.readCalls)
}

func TestCyberSessionBlock_RedisFailureFailsOpen(t *testing.T) {
	settingSvc := &SettingService{settingRepo: &fakeSettingRepo{vals: map[string]string{
		SettingKeyCyberSessionBlockEnabled:    "true",
		SettingKeyCyberSessionBlockTTLSeconds: "60",
		SettingKeyCyberSessionBlockAllGroups:  "true",
	}}}
	redisErr := errors.New("redis unavailable")
	combo := &comboCacheAndStore{store: fakeCyberBlockStore{readErr: redisErr, writeErr: redisErr}}
	svc := &OpenAIGatewayService{cache: combo, settingService: settingSvc}
	groupID := int64(12)

	require.NotPanics(t, func() {
		svc.MarkCyberSessionBlocked(context.Background(), &groupID, "new")
	})
	require.False(t, svc.IsCyberSessionBlocked(context.Background(), &groupID, "existing"))
	require.Equal(t, 1, combo.store.writeCalls)
	require.Equal(t, 1, combo.store.readCalls)
}
