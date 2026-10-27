package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newCyberBlockTestCtx(headers map[string]string, body string, userID, tokenID int64) (*gin.Context, []byte) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	req := httptest.NewRequest("POST", "/openai/v1/responses", strings.NewReader(body))
	for name, value := range headers {
		req.Header.Set(name, value)
	}
	if userID > 0 {
		ctx := context.WithValue(req.Context(), ctxkey.NewAPIUserID, userID)
		ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, tokenID)
		ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "https://newapi.internal")
		req = req.WithContext(ctx)
	}
	c.Request = req
	return c, []byte(body)
}

func TestCyberSessionBlockKeyRequiresVerifiedIdentityAndConversation(t *testing.T) {
	headers := map[string]string{"X-Session-Id": "conversation-1"}
	c, body := newCyberBlockTestCtx(headers, `{}`, 42, 77)
	key := CyberSessionExplicitBlockKey(21, c, body)
	require.NotEmpty(t, key)
	require.Equal(t, key, CyberSessionExplicitBlockKey(21, c, body))
	c.Request.Header.Set("Session_Id", "different-session")
	c.Request.Header.Set("Conversation_Id", "another-session")
	require.Equal(t, key, CyberSessionExplicitBlockKey(21, c, []byte(`{"prompt_cache_key":"third-session"}`)))

	for _, test := range []struct {
		name    string
		apiKey  int64
		user    int64
		token   int64
		headers map[string]string
		body    string
	}{
		{name: "another user", apiKey: 21, user: 43, token: 77, headers: headers, body: `{}`},
		{name: "another token", apiKey: 21, user: 42, token: 78, headers: headers, body: `{}`},
		{name: "another conversation", apiKey: 21, user: 42, token: 77, headers: map[string]string{"X-Session-Id": "conversation-2"}, body: `{}`},
		{name: "another ingress key", apiKey: 22, user: 42, token: 77, headers: headers, body: `{}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			other, payload := newCyberBlockTestCtx(test.headers, test.body, test.user, test.token)
			require.NotEqual(t, key, CyberSessionExplicitBlockKey(test.apiKey, other, payload))
		})
	}
	for _, test := range []struct {
		name    string
		user    int64
		token   int64
		headers map[string]string
		body    string
	}{
		{name: "unsigned ingress", headers: headers, body: `{}`},
		{name: "no token", user: 42, headers: headers, body: `{}`},
		{name: "no conversation", user: 42, token: 77, body: `{"input":"hello"}`},
		{name: "prompt cache is not a conversation", user: 42, token: 77, body: `{"prompt_cache_key":"shared-template"}`},
		{name: "affinity hint is not a conversation", user: 42, token: 77, headers: map[string]string{"X-Session-Affinity": "shared"}, body: `{}`},
		{name: "legacy session hint is not a conversation", user: 42, token: 77, headers: map[string]string{"Session_Id": "conversation-1"}, body: `{}`},
		{name: "conversation hint is not a conversation", user: 42, token: 77, headers: map[string]string{"Conversation_Id": "conversation-1"}, body: `{}`},
		{name: "invalid conversation header", user: 42, token: 77, headers: map[string]string{"X-Session-Id": "bad\tvalue"}, body: `{}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			other, payload := newCyberBlockTestCtx(test.headers, test.body, test.user, test.token)
			require.Empty(t, CyberSessionExplicitBlockKey(21, other, payload))
		})
	}
}

type fakeCyberBlockStore struct {
	blocked   map[string]bool
	findCalls int
}

var _ CyberSessionBlockStore = (*fakeCyberBlockStore)(nil)

func (f *fakeCyberBlockStore) SetCyberSessionBlocked(_ context.Context, key string, _ time.Duration) error {
	if f.blocked == nil {
		f.blocked = make(map[string]bool)
	}
	f.blocked[key] = true
	return nil
}

func (f *fakeCyberBlockStore) FindCyberSessionBlocked(_ context.Context, key string) (bool, error) {
	f.findCalls++
	return f.blocked[key], nil
}

type fakeSettingRepo struct {
	vals map[string]string
}

func (r *fakeSettingRepo) GetValue(_ context.Context, key string) (string, error) {
	v, ok := r.vals[key]
	if !ok {
		return "", ErrSettingNotFound
	}
	return v, nil
}
func (r *fakeSettingRepo) Get(_ context.Context, _ string) (*Setting, error) { panic("unused") }
func (r *fakeSettingRepo) Set(_ context.Context, _, _ string) error          { panic("unused") }
func (r *fakeSettingRepo) GetMultiple(_ context.Context, _ []string) (map[string]string, error) {
	panic("unused")
}
func (r *fakeSettingRepo) SetMultiple(_ context.Context, _ map[string]string) error { panic("unused") }
func (r *fakeSettingRepo) GetAll(_ context.Context) (map[string]string, error)      { panic("unused") }
func (r *fakeSettingRepo) Delete(_ context.Context, _ string) error                 { panic("unused") }

var _ SettingRepository = (*fakeSettingRepo)(nil)

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
func (c *comboCacheAndStore) SetGrokVideoPendingBilling(_ context.Context, _ string, _ []byte, _ time.Duration) error {
	return nil
}
func (c *comboCacheAndStore) GetGrokVideoPendingBilling(_ context.Context, _ string) ([]byte, error) {
	return nil, nil
}
func (c *comboCacheAndStore) ClaimGrokVideoBilled(_ context.Context, _ string, _ time.Duration) (bool, error) {
	return true, nil
}
func (c *comboCacheAndStore) ReleaseGrokVideoBilled(_ context.Context, _ string) error { return nil }
func (c *comboCacheAndStore) SetReasoningContent(_ context.Context, _ string, _ string, _ time.Duration) error {
	return nil
}
func (c *comboCacheAndStore) GetReasoningContent(_ context.Context, _ string) (string, error) {
	return "", ErrReasoningContentNotFound
}
func (c *comboCacheAndStore) SetCyberSessionBlocked(ctx context.Context, key string, ttl time.Duration) error {
	return c.store.SetCyberSessionBlocked(ctx, key, ttl)
}
func (c *comboCacheAndStore) FindCyberSessionBlocked(ctx context.Context, key string) (bool, error) {
	return c.store.FindCyberSessionBlocked(ctx, key)
}

func TestCyberSessionBlockIsolatedUnderSharedIngress(t *testing.T) {
	settingSvc := &SettingService{settingRepo: &fakeSettingRepo{vals: map[string]string{
		SettingKeyCyberSessionBlockEnabled: "true", SettingKeyCyberSessionBlockTTLSeconds: "60",
	}}}
	combo := &comboCacheAndStore{}
	svc := &OpenAIGatewayService{cache: combo, settingService: settingSvc}
	ctx := context.Background()
	request := func(user, token int64, session string) (*gin.Context, []byte) {
		headers := map[string]string{"X-Session-Id": session, "User-Agent": "Codex CLI 1.2", "X-Forwarded-For": "203.0.113.20"}
		return newCyberBlockTestCtx(headers, `{"input":"same transcript"}`, user, token)
	}
	a, body := request(42, 77, "conversation-1")
	key := CyberSessionExplicitBlockKey(21, a, body)
	legacy := sha256.Sum256([]byte(isolateOpenAISessionID(21, "conversation-1")))
	combo.store.blocked = map[string]bool{hex.EncodeToString(legacy[:]): true}
	require.Empty(t, svc.FindCyberSessionBlockedForRequest(ctx, 21, a, body))
	svc.MarkCyberSessionBlocked(ctx, key)
	require.Equal(t, key, svc.FindCyberSessionBlockedForRequest(ctx, 21, a, body))
	for _, test := range []struct {
		user, token int64
		session     string
	}{
		{43, 77, "conversation-1"}, {42, 78, "conversation-1"}, {42, 77, "conversation-2"},
	} {
		other, payload := request(test.user, test.token, test.session)
		require.Empty(t, svc.FindCyberSessionBlockedForRequest(ctx, 21, other, payload))
	}
	missing, payload := newCyberBlockTestCtx(nil, `{"prompt_cache_key":"conversation-1","input":"same transcript"}`, 42, 77)
	before := combo.store.findCalls
	require.Empty(t, svc.FindCyberSessionBlockedForRequest(ctx, 21, missing, payload))
	require.Equal(t, before, combo.store.findCalls)
	combo.store.blocked["transcript_lookup_limit_exceeded"] = true
	require.Empty(t, svc.FindCyberSessionBlockedForRequest(ctx, 21, missing, payload))
}

func TestCyberSessionBlockMissingStoreFailsOpen(t *testing.T) {
	var nilSvc *OpenAIGatewayService
	require.Empty(t, nilSvc.FindCyberSessionBlockedForRequest(context.Background(), 1, nil, nil))
	require.NotPanics(t, func() { nilSvc.MarkCyberSessionBlocked(context.Background(), "key") })
	require.Empty(t, (&OpenAIGatewayService{}).FindCyberSessionBlockedForRequest(context.Background(), 1, nil, nil))
}
