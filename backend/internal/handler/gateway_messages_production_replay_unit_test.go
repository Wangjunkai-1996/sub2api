//go:build unit

package handler

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	middleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type gatewayMessagesReplayUpstream struct {
	mu         sync.Mutex
	accountIDs []int64
}

type gatewayMessagesReplayConcurrencyCache struct {
	*fakeConcurrencyCache
	mu       sync.Mutex
	released []int64
}

func newGatewayMessagesReplayConcurrencyCache() *gatewayMessagesReplayConcurrencyCache {
	return &gatewayMessagesReplayConcurrencyCache{fakeConcurrencyCache: &fakeConcurrencyCache{}}
}

func (c *gatewayMessagesReplayConcurrencyCache) ReleaseAccountSlot(
	_ context.Context,
	accountID int64,
	_ string,
) error {
	c.mu.Lock()
	c.released = append(c.released, accountID)
	c.mu.Unlock()
	return nil
}

func (c *gatewayMessagesReplayConcurrencyCache) releaseCount(accountID int64) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, releasedID := range c.released {
		if releasedID == accountID {
			count++
		}
	}
	return count
}

func (u *gatewayMessagesReplayUpstream) Do(
	req *http.Request,
	_ string,
	accountID int64,
	_ int,
) (*http.Response, error) {
	return u.respond(req, accountID)
}

func (u *gatewayMessagesReplayUpstream) DoWithTLS(
	req *http.Request,
	_ string,
	accountID int64,
	_ int,
	_ *tlsfingerprint.Profile,
) (*http.Response, error) {
	return u.respond(req, accountID)
}

func (u *gatewayMessagesReplayUpstream) respond(req *http.Request, accountID int64) (*http.Response, error) {
	if req != nil && req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
	}
	u.mu.Lock()
	u.accountIDs = append(u.accountIDs, accountID)
	u.mu.Unlock()

	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
			"X-Request-Id": []string{"req_gateway_messages_replay"},
		},
		Body: io.NopCloser(strings.NewReader(
			`{"type":"message","id":"msg_gateway_messages_replay","role":"assistant","model":"claude-sonnet-4-5","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":1,"output_tokens":1}}`,
		)),
	}, nil
}

func (u *gatewayMessagesReplayUpstream) calls() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int64(nil), u.accountIDs...)
}

func newGatewayMessagesReplayHandler(
	t *testing.T,
	group *service.Group,
	accounts []service.Account,
	upstream service.HTTPUpstream,
	engine securityaudit.PromptEngine,
) *GatewayHandler {
	t.Helper()
	handler, _ := newGatewayMessagesReplayHandlerWithFreshAccounts(
		t, group, accounts, accounts, upstream, engine,
	)
	return handler
}

func newGatewayMessagesReplayHandlerWithFreshAccounts(
	t *testing.T,
	group *service.Group,
	snapshotAccounts []service.Account,
	freshAccounts []service.Account,
	upstream service.HTTPUpstream,
	engine securityaudit.PromptEngine,
) (*GatewayHandler, *gatewayMessagesReplayConcurrencyCache) {
	t.Helper()

	accountPointers := make([]*service.Account, len(snapshotAccounts))
	for i := range snapshotAccounts {
		account := snapshotAccounts[i]
		accountPointers[i] = &account
	}
	schedulerSnapshot := service.NewSchedulerSnapshotService(
		&fakeSchedulerCache{accounts: accountPointers}, nil, nil, nil, nil,
	)
	accountRepo := openAIImagesFailoverAccountRepo{accounts: freshAccounts}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	cfg.Gateway.Scheduling.LoadBatchEnabled = true
	cfg.Gateway.Scheduling.StickySessionMaxWaiting = 3
	cfg.Gateway.Scheduling.StickySessionWaitTimeout = 100 * time.Millisecond
	cfg.Gateway.Scheduling.FallbackWaitTimeout = 100 * time.Millisecond
	cfg.Gateway.Scheduling.FallbackMaxWaiting = 10

	concurrencyCache := newGatewayMessagesReplayConcurrencyCache()
	concurrencyService := service.NewConcurrencyService(concurrencyCache)
	gatewayService := service.NewGatewayService(
		accountRepo,
		&fakeGroupRepo{group: group},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		cfg,
		schedulerSnapshot,
		concurrencyService,
		nil,
		nil,
		nil,
		nil,
		upstream,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	openAIGatewayService := service.NewOpenAIGatewayService(
		accountRepo,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		cfg,
		nil,
		concurrencyService,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	billingCacheService := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billingCacheService.Stop)

	return &GatewayHandler{
		gatewayService:           gatewayService,
		openAIGatewayService:     openAIGatewayService,
		billingCacheService:      billingCacheService,
		concurrencyHelper:        NewConcurrencyHelper(concurrencyService, SSEPingFormatClaude, 0),
		securityAuditCoordinator: securityaudit.NewCoordinator(nil, engine),
		maxAccountSwitches:       10,
		maxAccountSwitchesGemini: 3,
		cfg:                      cfg,
	}, concurrencyCache
}

func gatewayMessagesReplayContext(
	t *testing.T,
	group *service.Group,
	body []byte,
) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodPost, EndpointMessages, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "claude-cli/2.1.237 (external, local-agent, agent-sdk/0.3.237)")
	req = req.WithContext(context.WithValue(req.Context(), ctxkey.Group, group))
	c.Request = req

	groupID := group.ID
	apiKey := &service.APIKey{
		ID:      3,
		UserID:  1,
		GroupID: &groupID,
		Status:  service.StatusActive,
		User: &service.User{
			ID:          1,
			Concurrency: 10,
			Balance:     100,
		},
		Group: group,
	}
	c.Set(string(middleware.ContextKeyAPIKey), apiKey)
	c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1, Concurrency: 10})
	return c, recorder
}

func exactGatewayMessagesReplayBody(t *testing.T, base string, bodyBytes int) []byte {
	t.Helper()
	require.True(t, strings.HasSuffix(base, "}"))
	require.LessOrEqual(t, len(base), bodyBytes)
	padding := strings.Repeat(" ", bodyBytes-len(base))
	body := []byte(base[:len(base)-1] + padding + "}")
	require.Len(t, body, bodyBytes)
	return body
}

func gatewayMessagesReplayAccounts(groupID int64) []service.Account {
	return []service.Account{
		{
			ID: 1, Name: "unverified-anthropic", Platform: service.PlatformAnthropic,
			Type: "future", Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 0,
			Credentials:   map[string]any{"access_token": "must-not-dispatch"},
			AccountGroups: []service.AccountGroup{{AccountID: 1, GroupID: groupID}},
		},
		{
			ID: 4, Name: "verified-anthropic-api-key", Platform: service.PlatformAnthropic,
			Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 1,
			Credentials:   map[string]any{"api_key": "verified-test-key"},
			AccountGroups: []service.AccountGroup{{AccountID: 4, GroupID: groupID}},
		},
	}
}

func gatewayMessagesReplayVerifiedSnapshotAccounts(groupID int64) []service.Account {
	return []service.Account{
		{
			ID: 1, Name: "snapshot-verified-1", Platform: service.PlatformAnthropic,
			Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 0,
			Credentials:   map[string]any{"api_key": "snapshot-key-1"},
			AccountGroups: []service.AccountGroup{{AccountID: 1, GroupID: groupID}},
		},
		{
			ID: 2, Name: "snapshot-verified-2", Platform: service.PlatformAnthropic,
			Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 1,
			Credentials:   map[string]any{"api_key": "snapshot-key-2"},
			AccountGroups: []service.AccountGroup{{AccountID: 2, GroupID: groupID}},
		},
	}
}

func gatewayMessagesReplayFreshAccounts(
	snapshot []service.Account,
	incompatibleIDs ...int64,
) []service.Account {
	incompatible := make(map[int64]struct{}, len(incompatibleIDs))
	for _, accountID := range incompatibleIDs {
		incompatible[accountID] = struct{}{}
	}
	fresh := make([]service.Account, len(snapshot))
	for i := range snapshot {
		fresh[i] = snapshot[i]
		fresh[i].Credentials = map[string]any{"api_key": "fresh-verified-key"}
		if _, reject := incompatible[fresh[i].ID]; reject {
			fresh[i].Type = "future"
			fresh[i].Credentials = map[string]any{"access_token": "fresh-unverified-token"}
		}
	}
	return fresh
}

func TestGatewayHandlerMessages_ReplaysProductionUninspectableMetadata(t *testing.T) {
	// Production intentionally did not retain request bodies. Each fixture is
	// metadata-equivalent: same endpoint, classification reason, and exact byte
	// length as the structured log record identified by requestID.
	unknownRoleBase := `{"model":"claude-sonnet-4-5","stream":false,"max_tokens":16,"messages":[{"role":"future","content":"x"}]}`
	textLimitBase := `{"model":"claude-sonnet-4-5","stream":false,"max_tokens":16,"messages":[{"role":"user","content":"` +
		strings.Repeat("x", securityadmission.DefaultMaxTextRunes+1) + `"}]}`
	tests := []struct {
		requestIDs []string
		name       string
		bodyBytes  int
		base       string
		reason     securityadmission.ReasonCode
	}{
		{
			requestIDs: []string{
				"25bd4a4a-051d-4921-a3b0-f15d72380934",
				"44c49da3-c7c9-4fd3-80be-ff26e659e277",
				"545877ef-9ee0-473e-aae8-0413b1834ac3",
				"707bb979-a288-473b-9afe-f760ff82ab37",
				"73270d95-9c7e-4207-8d3f-dd9bbd4ce9e3",
				"836d9c9b-ea93-41d3-9d8e-ff0b95e4bf30",
				"de1034ad-12a5-4810-a8ab-01000c0e227e",
				"f33249a8-d6d6-41ad-baa3-53efcef59d6f",
				"fcd6926b-f941-46e9-8719-0da1f4bc53f5",
			},
			name: "unknown-role-97840", bodyBytes: 97_840, base: unknownRoleBase, reason: securityadmission.ReasonUnknownRole,
		},
		{
			requestIDs: []string{
				"05889632-a344-4e70-97b2-5fff77b39fa7",
				"36939c13-78a5-4957-bb8c-4202b268e002",
				"5931e914-b31a-44d5-a6a1-c1ed514676d9",
				"d3c497d5-7297-4afa-a00c-8cc30787612c",
				"fbb5c576-3657-40f9-97c2-87d1fe69c5a4",
			},
			name: "text-limit-183425", bodyBytes: 183_425, base: textLimitBase, reason: securityadmission.ReasonTextLimit,
		},
		{
			requestIDs: []string{
				"3670cfe4-51cc-4b5a-a201-0378ee5790a6",
				"4717a307-848b-4968-89f7-2d1d240c59bc",
				"5168f6f0-b3ce-46e4-91d1-ffd85fe2626b",
				"778ec8ee-c386-41c8-bbde-a02d8ab97919",
				"8e1dce41-8e8e-48ec-904a-03827e7edf48",
				"b35601d8-b1bd-4f9d-839e-03e18cb4b071",
				"cdebbb06-1b5b-4ed2-9555-83c86f949232",
				"d0200c9f-3445-4e98-91a8-95fb696bf68f",
			},
			name: "text-limit-353401", bodyBytes: 353_401, base: textLimitBase, reason: securityadmission.ReasonTextLimit,
		},
		{requestIDs: []string{"edf51e92-2105-4b3c-a5bb-c2a345b12640"}, name: "unknown-role-67936", bodyBytes: 67_936, base: unknownRoleBase, reason: securityadmission.ReasonUnknownRole},
		{requestIDs: []string{"cee0665a-2cff-47c8-8347-28464621090a"}, name: "unknown-role-81594", bodyBytes: 81_594, base: unknownRoleBase, reason: securityadmission.ReasonUnknownRole},
		{requestIDs: []string{"96ae57cf-e2d2-480f-9883-df8b8538579d"}, name: "unknown-role-69367", bodyBytes: 69_367, base: unknownRoleBase, reason: securityadmission.ReasonUnknownRole},
		{requestIDs: []string{"777c50fe-5644-4088-9bc3-d5dc95c82f41"}, name: "text-limit-249815", bodyBytes: 249_815, base: textLimitBase, reason: securityadmission.ReasonTextLimit},
		{requestIDs: []string{"ce744af2-d39b-48d2-855d-fceea183fb05"}, name: "unknown-role-63435", bodyBytes: 63_435, base: unknownRoleBase, reason: securityadmission.ReasonUnknownRole},
	}

	replayed := 0
	for _, test := range tests {
		for _, requestID := range test.requestIDs {
			replayed++
			t.Run(requestID+"/"+test.name, func(t *testing.T) {
				body := exactGatewayMessagesReplayBody(t, test.base, test.bodyBytes)
				admission, err := securityadmission.Classify(
					string(securityadmission.ProtocolAnthropicMessages),
					body,
					securityadmission.Options{Lineage: securityadmission.LineageUntrusted},
				)
				require.NoError(t, err)
				require.Equal(t, securityadmission.RequestUninspectable, admission.Class())
				require.Equal(t, test.reason, admission.Reason())
				require.Equal(t, securityadmission.AccountRequirementAuditExempt, admission.Requirement())
				require.Equal(t, test.bodyBytes, admission.BodyBytes())

				const groupID = int64(23)
				group := &service.Group{
					ID: groupID, Hydrated: true, Platform: service.PlatformAnthropic,
					Status: service.StatusActive,
				}
				upstream := &gatewayMessagesReplayUpstream{}
				engine, scannerCalls := newSelectedAccountAuditPromptEngine(
					t, http.StatusOK, "Safety: Safe\nCategories: None",
				)
				handler := newGatewayMessagesReplayHandler(
					t, group, gatewayMessagesReplayAccounts(groupID), upstream, engine,
				)
				c, recorder := gatewayMessagesReplayContext(t, group, body)

				handler.Messages(c)

				require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
				require.Equal(t, []int64{4}, upstream.calls(), "unverified account must never dispatch")
				require.Empty(t, scannerCalls(), "uninspectable production signature must not enter the prompt scanner")
				selected, ok := c.Get(opsAccountIDKey)
				require.True(t, ok)
				require.Equal(t, int64(4), selected)
				state := openAISecurityAdmissionFromContext(c)
				require.NotNil(t, state)
				require.Equal(t, securityadmission.RequestUninspectable, state.admission.Class())
				require.Equal(t, test.reason, state.admission.Reason())
				require.Equal(t, securityadmission.AccountRequirementAuditExempt,
					service.OpenAIAccountRequirementFromContext(c.Request.Context()))
				terminal := service.OpenAIAccountTerminalAdmissionFromContext(c.Request.Context())
				require.NotNil(t, terminal)
				require.NotNil(t, terminal.Selected)
				require.Equal(t, int64(4), terminal.Selected.ID)
				require.Equal(t, securityadmission.AccountAuditExemptVerified, terminal.AccountClass)
			})
		}
	}
	require.Equal(t, 27, replayed, "production metadata corpus must account for every observed generic Messages 503")
}

func TestGatewayHandlerMessages_OversizeContinuesThroughAuditExemptAdmission(t *testing.T) {
	body := []byte(`{"model":"claude-sonnet-4-5","stream":false,"max_tokens":16,"messages":[{"role":"user","content":"` +
		strings.Repeat("x", securityadmission.DefaultBodyCapBytes) + `"}]}`)
	admission, err := securityadmission.Classify(
		string(securityadmission.ProtocolAnthropicMessages),
		body,
		securityadmission.Options{Lineage: securityadmission.LineageUntrusted},
	)
	require.NoError(t, err)
	require.Equal(t, securityadmission.RequestUninspectable, admission.Class())
	require.Equal(t, securityadmission.ReasonLargeBody, admission.Reason())

	const groupID = int64(23)
	group := &service.Group{
		ID: groupID, Hydrated: true, Platform: service.PlatformAnthropic,
		Status: service.StatusActive,
	}
	upstream := &gatewayMessagesReplayUpstream{}
	engine, scannerCalls := newSelectedAccountAuditPromptEngine(
		t, http.StatusOK, "Safety: Safe\nCategories: None",
	)
	handler := newGatewayMessagesReplayHandler(
		t, group, gatewayMessagesReplayAccounts(groupID), upstream, engine,
	)
	c, recorder := gatewayMessagesReplayContext(t, group, body)

	handler.Messages(c)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, []int64{4}, upstream.calls())
	require.Empty(t, scannerCalls())
}

func TestGatewayHandlerMessages_TerminalCredentialDriftReselectsVerifiedAccount(t *testing.T) {
	const groupID = int64(23)
	group := &service.Group{
		ID: groupID, Hydrated: true, Platform: service.PlatformAnthropic,
		Status: service.StatusActive,
	}
	snapshotAccounts := gatewayMessagesReplayVerifiedSnapshotAccounts(groupID)
	freshAccounts := gatewayMessagesReplayFreshAccounts(snapshotAccounts, 1)
	body := exactGatewayMessagesReplayBody(t,
		`{"model":"claude-sonnet-4-5","stream":false,"max_tokens":16,"messages":[{"role":"future","content":"x"}]}`,
		67_936,
	)
	upstream := &gatewayMessagesReplayUpstream{}
	engine, scannerCalls := newSelectedAccountAuditPromptEngine(
		t, http.StatusOK, "Safety: Safe\nCategories: None",
	)
	handler, concurrencyCache := newGatewayMessagesReplayHandlerWithFreshAccounts(
		t, group, snapshotAccounts, freshAccounts, upstream, engine,
	)
	c, recorder := gatewayMessagesReplayContext(t, group, body)

	handler.Messages(c)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, []int64{2}, upstream.calls(), "drifted account must be excluded before dispatch")
	require.GreaterOrEqual(t, concurrencyCache.releaseCount(1), 1, "rejected account slot must be released")
	require.Empty(t, scannerCalls())
	selected, ok := c.Get(opsAccountIDKey)
	require.True(t, ok)
	require.Equal(t, int64(2), selected)
	terminal := service.OpenAIAccountTerminalAdmissionFromContext(c.Request.Context())
	require.NotNil(t, terminal)
	require.NotNil(t, terminal.Selected)
	require.Equal(t, int64(2), terminal.Selected.ID)
	require.Equal(t, securityadmission.AccountAuditExemptVerified, terminal.AccountClass)
}

func TestGatewayHandlerMessages_AllTerminalAccountsIncompatibleReturnControlled503(t *testing.T) {
	const groupID = int64(23)
	group := &service.Group{
		ID: groupID, Hydrated: true, Platform: service.PlatformAnthropic,
		Status: service.StatusActive,
	}
	snapshotAccounts := gatewayMessagesReplayVerifiedSnapshotAccounts(groupID)
	freshAccounts := gatewayMessagesReplayFreshAccounts(snapshotAccounts, 1, 2)
	body := exactGatewayMessagesReplayBody(t,
		`{"model":"claude-sonnet-4-5","stream":false,"max_tokens":16,"messages":[{"role":"future","content":"x"}]}`,
		67_936,
	)
	upstream := &gatewayMessagesReplayUpstream{}
	engine, scannerCalls := newSelectedAccountAuditPromptEngine(
		t, http.StatusOK, "Safety: Safe\nCategories: None",
	)
	handler, concurrencyCache := newGatewayMessagesReplayHandlerWithFreshAccounts(
		t, group, snapshotAccounts, freshAccounts, upstream, engine,
	)
	c, recorder := gatewayMessagesReplayContext(t, group, body)

	handler.Messages(c)

	require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "Account security admission is temporarily unavailable")
	require.Empty(t, upstream.calls(), "no incompatible account may dispatch")
	require.GreaterOrEqual(t, concurrencyCache.releaseCount(1), 1)
	require.GreaterOrEqual(t, concurrencyCache.releaseCount(2), 1)
	require.Empty(t, scannerCalls())
}

func TestGatewayHandlerMessages_TerminalAdmissionPrecedesBothForwardLoops(t *testing.T) {
	source := stripGoComments(goFunctionSource(t, "gateway_handler.go", "Messages"))
	firstAdmission := strings.Index(source, "h.admitGatewaySecurityAccount(")
	secondAdmission := strings.LastIndex(source, "h.admitGatewaySecurityAccount(")
	firstProfitCheck := strings.Index(source, "h.gatewayService.GatewayProfitControlVetoLatest(")
	secondProfitCheck := strings.LastIndex(source, "h.gatewayService.GatewayProfitControlVetoLatest(")
	antigravityGeminiForward := strings.Index(source, "h.antigravityGatewayService.ForwardGemini(")
	nativeGeminiForward := strings.Index(source, "h.geminiCompatService.Forward(")
	anthropicForward := strings.Index(source, "h.gatewayService.Forward(")

	require.NotEqual(t, -1, firstAdmission)
	require.NotEqual(t, firstAdmission, secondAdmission, "both Messages scheduling loops need terminal admission")
	require.Less(t, firstProfitCheck, firstAdmission)
	require.Less(t, firstAdmission, antigravityGeminiForward)
	require.Less(t, firstAdmission, nativeGeminiForward)
	require.Less(t, secondProfitCheck, secondAdmission)
	require.Less(t, secondAdmission, anthropicForward)
}
