package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// newTestGinContext builds a bare gin.Context backed by an httptest recorder.
func newTestGinContext() *gin.Context {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	return c
}

type cyberHandlerSettingRepo struct {
	service.SettingRepository
	values map[string]string
}

func (r *cyberHandlerSettingRepo) GetValue(_ context.Context, key string) (string, error) {
	if value, ok := r.values[key]; ok {
		return value, nil
	}
	return "", service.ErrSettingNotFound
}

func (r *cyberHandlerSettingRepo) GetMultiple(_ context.Context, keys []string) (map[string]string, error) {
	result := make(map[string]string, len(keys))
	for _, key := range keys {
		if value, ok := r.values[key]; ok {
			result[key] = value
		}
	}
	return result, nil
}

type cyberHandlerGatewayCache struct {
	service.GatewayCache
	blocked             map[string]bool
	readCalls           int
	writeCalls          int
	stickyReadCalls     int
	cooldownStrike      service.OpenAICyberAccountCooldownStrike
	cooldownStrikeCalls int
	cooldownDeadline    time.Time
	cooldownReadCalls   int
}

func (c *cyberHandlerGatewayCache) SetCyberSessionBlocked(_ context.Context, key string, _ time.Duration) error {
	c.writeCalls++
	if c.blocked == nil {
		c.blocked = make(map[string]bool)
	}
	c.blocked[key] = true
	return nil
}

func (c *cyberHandlerGatewayCache) IsCyberSessionBlocked(_ context.Context, key string) (bool, error) {
	c.readCalls++
	return c.blocked[key], nil
}

func (c *cyberHandlerGatewayCache) GetSessionAccountID(context.Context, int64, string) (int64, error) {
	c.stickyReadCalls++
	return 0, service.ErrStickySessionNotFound
}

func (c *cyberHandlerGatewayCache) RecordOpenAICyberAccountCooldownStrike(
	_ context.Context,
	_ int64,
	_ string,
	_ time.Duration,
	firstDuration time.Duration,
	escalatedDuration time.Duration,
	now time.Time,
) (service.OpenAICyberAccountCooldownStrike, error) {
	c.cooldownStrikeCalls++
	strike := c.cooldownStrike
	if strike.EventRecordedAt.IsZero() {
		strike.EventRecordedAt = now.UTC()
	}
	duration := escalatedDuration
	if strike.Strikes == 1 {
		duration = firstDuration
	}
	if strike.EventCooldownUntil.IsZero() {
		strike.EventCooldownUntil = strike.EventRecordedAt.Add(duration)
	}
	if strike.AccountCooldownUntil.IsZero() {
		strike.AccountCooldownUntil = strike.EventCooldownUntil
	}
	c.cooldownDeadline = strike.AccountCooldownUntil
	return strike, nil
}

func (c *cyberHandlerGatewayCache) GetOpenAICyberAccountCooldownDeadline(context.Context, int64) (time.Time, error) {
	c.cooldownReadCalls++
	return c.cooldownDeadline, nil
}

type cyberHandlerAccountRepo struct {
	service.AccountRepository
	tempUnschedulableCalls int
}

func (r *cyberHandlerAccountRepo) SetTempUnschedulable(context.Context, int64, time.Time, string) error {
	r.tempUnschedulableCalls++
	return nil
}

func newCyberScopedHandler(groupIDs string) (*OpenAIGatewayHandler, *cyberHandlerGatewayCache) {
	settingSvc := service.NewSettingService(&cyberHandlerSettingRepo{values: map[string]string{
		service.SettingKeyCyberSessionBlockEnabled:    "true",
		service.SettingKeyCyberSessionBlockTTLSeconds: "60",
		service.SettingKeyCyberSessionBlockAllGroups:  "false",
		service.SettingKeyCyberSessionBlockGroupIDs:   groupIDs,
	}}, nil)
	cache := &cyberHandlerGatewayCache{blocked: make(map[string]bool)}
	gatewaySvc := service.NewOpenAIGatewayService(
		nil, nil, nil, nil, nil, nil,
		cache, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		settingSvc, nil,
	)
	return &OpenAIGatewayHandler{gatewayService: gatewaySvc}, cache
}

// TestRecordCyberPolicyIfMarked_NoMark verifies that when no cyber mark is set,
// the function returns immediately and does NOT set the recorded flag.
func TestRecordCyberPolicyIfMarked_NoMark(t *testing.T) {
	c := newTestGinContext()
	h := &OpenAIGatewayHandler{}

	h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", true, "", service.ChannelUsageFields{}, "")

	// Flag must NOT be set when there was no mark.
	require.False(t, c.GetBool(cyberPolicyRecordedKey),
		"cyberPolicyRecordedKey must remain false when no cyber mark is present")
}

// TestRecordCyberPolicyIfMarked_WithMark verifies that:
//  1. When a cyber mark is present, the recorded flag is set (guard activated).
//  2. A second call is a no-op (idempotent guard).
//  3. Nil services do not panic.
func TestRecordCyberPolicyIfMarked_WithMark(t *testing.T) {
	c := newTestGinContext()
	service.MarkOpsCyberPolicy(c, service.CyberPolicyMark{
		Message:        "flagged",
		Body:           `{"error":{"code":"cyber_policy"}}`,
		UpstreamStatus: 400,
	})

	h := &OpenAIGatewayHandler{} // nil services — must not panic

	// First call: should set the flag.
	require.NotPanics(t, func() {
		h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", true, "", service.ChannelUsageFields{}, "")
	})
	require.True(t, c.GetBool(cyberPolicyRecordedKey),
		"cyberPolicyRecordedKey must be true after first call with a mark")

	// Second call: flag already set — must be a no-op (idempotent).
	require.NotPanics(t, func() {
		h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", false, "", service.ChannelUsageFields{}, "")
	})
	// Flag should still be true (not toggled or cleared).
	require.True(t, c.GetBool(cyberPolicyRecordedKey),
		"cyberPolicyRecordedKey must remain true after second call (guard)")
}

func TestRecordCyberPolicyIfMarkedAppliesAccountCooldownOnlyForRealMark(t *testing.T) {
	settingSvc := service.NewSettingService(&cyberHandlerSettingRepo{values: map[string]string{
		service.SettingKeyOpenAICyberAccountCooldownEnabled:          "true",
		service.SettingKeyOpenAICyberAccountCooldownWindowSeconds:    "86400",
		service.SettingKeyOpenAICyberAccountCooldownFirstSeconds:     "3600",
		service.SettingKeyOpenAICyberAccountCooldownEscalatedSeconds: "86400",
	}}, nil)
	cache := &cyberHandlerGatewayCache{cooldownStrike: service.OpenAICyberAccountCooldownStrike{Strikes: 1}}
	repo := &cyberHandlerAccountRepo{}
	gatewaySvc := service.NewOpenAIGatewayService(
		repo, nil, nil, nil, nil, nil,
		cache, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
		settingSvc, nil,
	)
	h := &OpenAIGatewayHandler{gatewayService: gatewaySvc}
	account := &service.Account{ID: 10616, Platform: service.PlatformOpenAI}

	withoutMark := newTestGinContext()
	h.recordCyberPolicyIfMarked(withoutMark, nil, account, nil, "gpt-5", false, "", service.ChannelUsageFields{}, "hash-a")
	require.Zero(t, cache.cooldownStrikeCalls)
	require.Zero(t, repo.tempUnschedulableCalls)

	withMark := newTestGinContext()
	service.MarkOpsCyberPolicy(withMark, service.CyberPolicyMark{Message: "blocked", UpstreamStatus: 400})
	h.recordCyberPolicyIfMarked(withMark, nil, account, nil, "gpt-5", false, "", service.ChannelUsageFields{}, "hash-b")
	require.Equal(t, 1, cache.cooldownStrikeCalls)
	require.Equal(t, 1, repo.tempUnschedulableCalls)

	// The request-level guard prevents duplicate event/cooldown writes.
	h.recordCyberPolicyIfMarked(withMark, nil, account, nil, "gpt-5", false, "", service.ChannelUsageFields{}, "hash-b")
	require.Equal(t, 1, cache.cooldownStrikeCalls)
	require.Equal(t, 1, repo.tempUnschedulableCalls)
}

// TestRecordCyberPolicyIfMarked_ForwardSuccessSkipsUsageLog verifies the semantic:
// when forwardErrored=false the function still sets the guard flag (mark present),
// but the cyber usage row is NOT requested (only RecordCyberPolicyEvent fires).
// Since services are nil here we only verify the guard flag and no panic.
func TestRecordCyberPolicyIfMarked_ForwardSuccessSkipsUsageLog(t *testing.T) {
	c := newTestGinContext()
	service.MarkOpsCyberPolicy(c, service.CyberPolicyMark{
		Message:        "flagged",
		UpstreamStatus: 200,
	})

	h := &OpenAIGatewayHandler{}

	require.NotPanics(t, func() {
		h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", false /* forwardErrored=false */, "", service.ChannelUsageFields{}, "")
	})
	require.True(t, c.GetBool(cyberPolicyRecordedKey))
}

// TestClearCyberPolicyTurnState verifies F1 at the handler level: after a turn
// is finalized, both the mark and the recorded guard are reset so the next WS
// turn detects/records independently.
func TestClearCyberPolicyTurnState(t *testing.T) {
	c := newTestGinContext()
	h := &OpenAIGatewayHandler{}

	service.MarkOpsCyberPolicy(c, service.CyberPolicyMark{Message: "turn1", UpstreamStatus: 200})
	h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", false, "", service.ChannelUsageFields{}, "")
	require.True(t, c.GetBool(cyberPolicyRecordedKey))

	clearCyberPolicyTurnState(c)
	require.Nil(t, service.GetOpsCyberPolicy(c))
	require.False(t, c.GetBool(cyberPolicyRecordedKey))

	// turn2: a fresh cyber hit must be recordable again.
	service.MarkOpsCyberPolicy(c, service.CyberPolicyMark{Message: "turn2", UpstreamStatus: 200})
	h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", false, "", service.ChannelUsageFields{}, "")
	require.True(t, c.GetBool(cyberPolicyRecordedKey))
	require.Equal(t, "turn2", service.GetOpsCyberPolicy(c).Message)
}

// TestBuildCyberSessionBlockedOpsEntry verifies the locally-rejected request is
// auditable: 403 / phase=request / type=cyber_policy_session_blocked — distinct
// from upstream cyber_policy hits, and it must NOT touch moderation/violation.
func TestBuildCyberSessionBlockedOpsEntry(t *testing.T) {
	entry := buildCyberSessionBlockedOpsEntry(cyberPolicyOpsErrorMeta{
		RequestID: "req-9", Model: "gpt-5", RequestPath: "/openai/v1/responses",
	})
	require.Equal(t, 403, entry.StatusCode)
	require.Equal(t, "cyber_policy_session_blocked", entry.ErrorType)
	require.Equal(t, "request", entry.ErrorPhase)
	require.True(t, entry.IsBusinessLimited)
	require.Equal(t, "gateway_local", entry.ErrorSource)
	require.Equal(t, "platform", entry.ErrorOwner)
	require.Empty(t, entry.ErrorBody, "no session block key → ErrorBody must be empty")

	entryWithKey := buildCyberSessionBlockedOpsEntry(cyberPolicyOpsErrorMeta{
		RequestID: "req-9", Model: "gpt-5", RequestPath: "/openai/v1/responses",
		SessionBlockKey: "abc123",
	})
	require.Equal(t, "session_block_key=abc123", entryWithKey.ErrorBody)
}

// TestRejectIfCyberSessionBlocked_FailOpen verifies fail-open paths: nil handler
// services, no explicit session signal, and (implicitly) disabled switch all
// pass the request through.
func TestRejectIfCyberSessionBlocked_FailOpen(t *testing.T) {
	c := newTestGinContext()
	c.Request = httptest.NewRequest("POST", "/openai/v1/responses", strings.NewReader(`{}`))

	h := &OpenAIGatewayHandler{}
	require.False(t, h.rejectIfCyberSessionBlocked(c, nil, []byte(`{}`), "gpt-5", cyberBlockFormatResponses), "nil apiKey → pass")

	h2 := &OpenAIGatewayHandler{gatewayService: nil}
	key := &service.APIKey{ID: 1}
	require.False(t, h2.rejectIfCyberSessionBlocked(c, key, []byte(`{}`), "gpt-5", cyberBlockFormatResponses), "nil gateway service → pass")
}

func TestRejectIfCyberSessionBlocked_GroupScopeAcrossHTTPFormats(t *testing.T) {
	h, cache := newCyberScopedHandler(`[12]`)
	group12 := int64(12)
	group13 := int64(13)

	plusRecorder := httptest.NewRecorder()
	plusCtx, _ := gin.CreateTestContext(plusRecorder)
	plusCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{}`))
	plusCtx.Request.Header.Set("session_id", "plus-existing-session")
	plusKey := &service.APIKey{ID: 2, GroupID: &group13}
	cache.blocked[service.CyberSessionBlockKey(plusKey.ID, plusCtx, nil)] = true

	require.False(t, h.rejectIfCyberSessionBlocked(plusCtx, plusKey, nil, "gpt-5", cyberBlockFormatResponses))
	require.Zero(t, cache.readCalls, "out-of-scope requests must not query Redis")
	require.Equal(t, http.StatusOK, plusRecorder.Code)

	for _, tc := range []struct {
		name      string
		path      string
		format    cyberSessionBlockFormat
		anthropic bool
	}{
		{name: "responses", path: "/v1/responses", format: cyberBlockFormatResponses},
		{name: "chat_completions", path: "/v1/chat/completions", format: cyberBlockFormatChat},
		{name: "messages", path: "/v1/messages", format: cyberBlockFormatAnthropic, anthropic: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(`{}`))
			c.Request.Header.Set("session_id", "pro-session-"+tc.name)
			apiKey := &service.APIKey{ID: 7, GroupID: &group12}
			cache.blocked[service.CyberSessionBlockKey(apiKey.ID, c, nil)] = true

			require.True(t, h.rejectIfCyberSessionBlocked(c, apiKey, nil, "gpt-5", tc.format))
			require.Equal(t, http.StatusForbidden, recorder.Code)
			if tc.anthropic {
				require.Contains(t, recorder.Body.String(), `"type":"error"`)
				require.NotContains(t, recorder.Body.String(), `session_blocked_by_cyber_policy`)
			} else {
				require.Contains(t, recorder.Body.String(), `"code":"session_blocked_by_cyber_policy"`)
			}
		})
	}
	require.Equal(t, 3, cache.readCalls)
}

func TestCyberSessionBlockWebSocketScopeAndConnectionState(t *testing.T) {
	h, cache := newCyberScopedHandler(`[12]`)
	group12 := int64(12)
	group13 := int64(13)

	c := newTestGinContext()
	c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	c.Request.Header.Set("session_id", "shared-ws-session")

	proKey := &service.APIKey{ID: 7, GroupID: &group12}
	plusKey := &service.APIKey{ID: 2, GroupID: &group13}
	require.NotEmpty(t, h.cyberSessionBlockKeyForAPIKey(c, proKey, nil))
	require.Empty(t, h.cyberSessionBlockKeyForAPIKey(c, plusKey, nil), "out-of-scope WS handshake must not derive a block key")
	require.Zero(t, cache.readCalls)

	cyberBlockedThisConn := false
	cyberBlockedThisConn = nextCyberBlockedThisConn(cyberBlockedThisConn, true, h.gatewayService.CyberSessionBlockEnabledForGroup(c.Request.Context(), plusKey.GroupID))
	require.False(t, cyberBlockedThisConn, "Plus cyber hit must not block a later turn on the same connection")

	cyberBlockedThisConn = nextCyberBlockedThisConn(cyberBlockedThisConn, true, h.gatewayService.CyberSessionBlockEnabledForGroup(c.Request.Context(), proKey.GroupID))
	require.True(t, cyberBlockedThisConn, "in-scope cyber hit must block a later turn on the same connection")
	require.True(t, nextCyberBlockedThisConn(cyberBlockedThisConn, false, false), "connection-local block remains sticky")
}

func TestRecordCyberPolicyIfMarked_OutOfScopeStillActivatesAuditGuard(t *testing.T) {
	c := newTestGinContext()
	service.MarkOpsCyberPolicy(c, service.CyberPolicyMark{Message: "flagged", UpstreamStatus: http.StatusForbidden})
	group13 := int64(13)
	h := &OpenAIGatewayHandler{}

	require.NotPanics(t, func() {
		h.recordCyberPolicyIfMarked(c, &service.APIKey{ID: 2, GroupID: &group13}, nil, nil, "gpt-5", true, "", service.ChannelUsageFields{}, "")
	})
	require.True(t, c.GetBool(cyberPolicyRecordedKey), "scope must not suppress real cyber audit handling")
}

// TestRecordCyberPolicyIfMarked_BlockKeyPlumbed verifies the 6th param is
// accepted and a non-empty key with nil gateway service does not panic
// (write-side guards live in the service layer).
func TestRecordCyberPolicyIfMarked_BlockKeyPlumbed(t *testing.T) {
	c := newTestGinContext()
	service.MarkOpsCyberPolicy(c, service.CyberPolicyMark{Message: "x", UpstreamStatus: 400})
	h := &OpenAIGatewayHandler{}
	require.NotPanics(t, func() {
		h.recordCyberPolicyIfMarked(c, nil, nil, nil, "gpt-5", true, "deadbeef", service.ChannelUsageFields{}, "")
	})
}

// TestBuildCyberPolicyOpsErrorEntry_StatusCode verifies F6: the ops error log
// records the status the codex client actually received (400 non-stream / 200 stream),
// not a hardcoded 403.
func TestBuildCyberPolicyOpsErrorEntry_StatusCode(t *testing.T) {
	for _, tc := range []struct {
		name           string
		upstreamStatus int
	}{
		{"non_stream_400", 400},
		{"stream_200", 200},
		{"zero_value", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mark := &service.CyberPolicyMark{
				Code:           "cyber_policy",
				Message:        "blocked",
				UpstreamStatus: tc.upstreamStatus,
			}
			entry := buildCyberPolicyOpsErrorEntry(cyberPolicyOpsErrorMeta{
				RequestID: "req-1", Model: "gpt-5", RequestPath: "/openai/v1/responses",
			}, mark)
			require.Equal(t, tc.upstreamStatus, entry.StatusCode)
			require.Equal(t, "cyber_policy", entry.ErrorType)
			require.Equal(t, "request", entry.ErrorPhase)
		})
	}
}
