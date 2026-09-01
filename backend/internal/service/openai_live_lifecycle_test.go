package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/stretchr/testify/require"
)

type liveTestFrame struct {
	messageType coderws.MessageType
	payload     []byte
	err         error
}

type liveTestFrameConn struct {
	reads     chan liveTestFrame
	writes    chan liveTestFrame
	closed    chan struct{}
	closeOnce sync.Once
}

func newLiveTestFrameConn() *liveTestFrameConn {
	return &liveTestFrameConn{
		reads:  make(chan liveTestFrame, 8),
		writes: make(chan liveTestFrame, 8),
		closed: make(chan struct{}),
	}
}

func (c *liveTestFrameConn) ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error) {
	select {
	case frame := <-c.reads:
		return frame.messageType, frame.payload, frame.err
	case <-c.closed:
		return coderws.MessageText, nil, coderws.CloseError{Code: coderws.StatusNormalClosure}
	case <-ctx.Done():
		return coderws.MessageText, nil, context.Cause(ctx)
	}
}

func (c *liveTestFrameConn) WriteFrame(ctx context.Context, messageType coderws.MessageType, payload []byte) error {
	frame := liveTestFrame{messageType: messageType, payload: append([]byte(nil), payload...)}
	select {
	case c.writes <- frame:
		return nil
	case <-c.closed:
		return errors.New("connection closed")
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (c *liveTestFrameConn) WriteJSON(ctx context.Context, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.WriteFrame(ctx, coderws.MessageText, payload)
}

func (c *liveTestFrameConn) ReadMessage(ctx context.Context) ([]byte, error) {
	_, payload, err := c.ReadFrame(ctx)
	return payload, err
}

func (c *liveTestFrameConn) Ping(context.Context) error { return nil }

func (c *liveTestFrameConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed) })
	return nil
}

type liveTestDialer struct {
	conn    *liveTestFrameConn
	url     string
	headers http.Header
	proxy   string
}

func (d *liveTestDialer) Dial(
	_ context.Context,
	wsURL string,
	headers http.Header,
	proxyURL string,
) (openAIWSClientConn, int, http.Header, error) {
	d.url = wsURL
	d.headers = headers.Clone()
	d.proxy = proxyURL
	return d.conn, http.StatusSwitchingProtocols, nil, nil
}

type liveTestAccountRepo struct {
	AccountRepository
	account *Account
}

func (r *liveTestAccountRepo) GetByID(context.Context, int64) (*Account, error) {
	return r.account, nil
}

type liveTestStore struct {
	GatewayCache
	mu     sync.Mutex
	record *LiveCallRecord
	// 注入 store 故障（模拟 Redis 抖动），区别于 ErrLiveCallNotFound。
	claimErr         error
	claimAllowed     *bool
	getCallErr       error
	getControllerErr error
	markClosedErr    error
	markClosedFirst  *bool
	markClosedCalls  int
}

func (s *liveTestStore) SaveLiveCall(_ context.Context, record *LiveCallRecord, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy := *record
	s.record = &copy
	return nil
}

func (s *liveTestStore) GetLiveCall(_ context.Context, callHash string) (*LiveCallRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getCallErr != nil {
		return nil, s.getCallErr
	}
	if s.record == nil || s.record.CallHash != callHash {
		return nil, ErrLiveCallNotFound
	}
	copy := *s.record
	return &copy, nil
}

func (s *liveTestStore) ClaimLiveController(_ context.Context, callHash, controller, owner string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimErr != nil {
		return false, s.claimErr
	}
	if s.claimAllowed != nil && !*s.claimAllowed {
		return false, nil
	}
	if s.record == nil || s.record.CallHash != callHash || s.record.Controller == LiveControllerClosed {
		return false, nil
	}
	if controller == LiveControllerObserver && s.record.Controller != LiveControllerPending {
		return false, nil
	}
	if controller == LiveControllerProxy && s.record.Controller != LiveControllerPending && s.record.Controller != LiveControllerObserver {
		return false, nil
	}
	s.record.Controller = controller
	s.record.ControllerOwner = owner
	return true, nil
}

func (s *liveTestStore) ReleaseLiveController(_ context.Context, callHash, owner string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.record == nil || s.record.CallHash != callHash || s.record.ControllerOwner != owner {
		return false, nil
	}
	s.record.Controller = LiveControllerPending
	s.record.ControllerOwner = ""
	return true, nil
}

func (s *liveTestStore) GetLiveController(_ context.Context, callHash string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.getControllerErr != nil {
		return "", s.getControllerErr
	}
	if s.record == nil || s.record.CallHash != callHash {
		return "", ErrLiveCallNotFound
	}
	return s.record.Controller, nil
}

func (s *liveTestStore) MarkLiveCallClosed(_ context.Context, callHash string, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markClosedCalls++
	if s.markClosedErr != nil {
		return false, s.markClosedErr
	}
	if s.markClosedFirst != nil && !*s.markClosedFirst {
		return false, nil
	}
	if s.record == nil || s.record.CallHash != callHash || s.record.Controller == LiveControllerClosed {
		return false, nil
	}
	s.record.Controller = LiveControllerClosed
	s.record.ControllerOwner = ""
	return true, nil
}

type liveTestConcurrencyCache struct {
	ConcurrencyCache
	mu                     sync.Mutex
	releases               int
	egressOwned            bool
	egressRefreshErr       error
	egressAcquires         int
	egressRefreshes        int
	egressLiveReleases     int
	legacyOwned            bool
	legacyAcquires         int
	legacyRefreshes        int
	legacyLiveReleases     int
	legacyAccountID        int64
	legacyAccountMax       int
	legacyUserID           int64
	legacyUserMax          int
	legacyAPIKeyID         int64
	legacyLeaseID          string
	legacyIdentityID       string
	legacyReplacingRegular bool
}

func (c *liveTestConcurrencyCache) AcquireLiveLease(
	context.Context,
	int64,
	int,
	int64,
	int,
	int64,
	string,
	bool,
) (bool, error) {
	return true, nil
}

func (c *liveTestConcurrencyCache) RefreshLiveLease(
	context.Context,
	int64,
	int64,
	int64,
	string,
) (bool, error) {
	return true, nil
}

func (c *liveTestConcurrencyCache) ReleaseLiveLease(
	context.Context,
	int64,
	int64,
	int64,
	string,
) error {
	c.mu.Lock()
	c.releases++
	c.mu.Unlock()
	return nil
}

func (c *liveTestConcurrencyCache) AcquireLiveLeaseForEgress(
	context.Context,
	AccountEgressLeaseRef,
	int64,
	int,
	int64,
	string,
	bool,
) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.egressAcquires++
	return c.egressOwned, nil
}

func (c *liveTestConcurrencyCache) RefreshLiveLeaseForEgress(
	context.Context,
	AccountEgressLeaseRef,
	int64,
	int64,
	string,
) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.egressRefreshes++
	return c.egressOwned, c.egressRefreshErr
}

func (c *liveTestConcurrencyCache) ReleaseLiveLeaseForEgress(
	context.Context,
	AccountEgressLeaseRef,
	int64,
	int64,
	string,
) error {
	c.mu.Lock()
	c.egressLiveReleases++
	c.mu.Unlock()
	return nil
}

func (c *liveTestConcurrencyCache) AcquireLiveLeaseForLegacyEgress(
	_ context.Context,
	accountID int64,
	accountMax int,
	userID int64,
	userMax int,
	apiKeyID int64,
	leaseID string,
	identityID string,
	replacingRegularSlots bool,
) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.legacyAcquires++
	c.legacyAccountID = accountID
	c.legacyAccountMax = accountMax
	c.legacyUserID = userID
	c.legacyUserMax = userMax
	c.legacyAPIKeyID = apiKeyID
	c.legacyLeaseID = leaseID
	c.legacyIdentityID = identityID
	c.legacyReplacingRegular = replacingRegularSlots
	return c.legacyOwned, nil
}

func (c *liveTestConcurrencyCache) RefreshLiveLeaseForLegacyEgress(
	_ context.Context,
	accountID int64,
	userID int64,
	apiKeyID int64,
	leaseID string,
	identityID string,
) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.legacyRefreshes++
	c.legacyAccountID = accountID
	c.legacyUserID = userID
	c.legacyAPIKeyID = apiKeyID
	c.legacyLeaseID = leaseID
	c.legacyIdentityID = identityID
	return c.legacyOwned, nil
}

func (c *liveTestConcurrencyCache) ReleaseLiveLeaseForLegacyEgress(
	_ context.Context,
	accountID int64,
	userID int64,
	apiKeyID int64,
	leaseID string,
	identityID string,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.legacyLiveReleases++
	c.legacyAccountID = accountID
	c.legacyUserID = userID
	c.legacyAPIKeyID = apiKeyID
	c.legacyLeaseID = leaseID
	c.legacyIdentityID = identityID
	return nil
}

type liveTestPoolConcurrencyCache struct {
	*liveTestConcurrencyCache
	*accountEgressCacheStub
}

type liveTestUsageRepo struct {
	UsageLogRepository
	mu   sync.Mutex
	logs []*UsageLog
}

func (r *liveTestUsageRepo) Create(_ context.Context, log *UsageLog) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	copy := *log
	r.logs = append(r.logs, &copy)
	return true, nil
}

func TestRunLiveControllerClosesExpiredSession(t *testing.T) {
	upstream := newLiveTestFrameConn()
	record := &LiveCallRecord{ExpiresAt: time.Now().Add(20 * time.Millisecond)}
	service := &OpenAIGatewayService{}

	err := service.runLiveController(context.Background(), record, upstream, make(chan error))
	require.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case frame := <-upstream.writes:
		require.Equal(t, coderws.MessageText, frame.messageType)
		require.JSONEq(t, `{"type":"session.close"}`, string(frame.payload))
	case <-time.After(time.Second):
		t.Fatal("没有向上游发送 session.close")
	}
}

func TestFinalizeLiveCallIsIdempotentAndWritesZeroUsage(t *testing.T) {
	record := &LiveCallRecord{
		CallID:          "call_secret",
		CallHash:        hashLiveCallID("call_secret"),
		AccountID:       11,
		APIKeyID:        22,
		UserID:          33,
		GroupID:         44,
		LeaseID:         "lease-1",
		Model:           "gpt-live-test",
		CreatedAt:       time.Now().Add(-time.Second),
		ExpiresAt:       time.Now().Add(time.Hour),
		Controller:      LiveControllerPending,
		InboundEndpoint: "/v1/live",
	}
	store := &liveTestStore{}
	require.NoError(t, store.SaveLiveCall(context.Background(), record, time.Hour))
	concurrencyCache := &liveTestConcurrencyCache{}
	usageRepo := &liveTestUsageRepo{}
	service := &OpenAIGatewayService{
		cache:              store,
		concurrencyService: NewConcurrencyService(concurrencyCache),
		usageLogRepo:       usageRepo,
	}

	service.finalizeLiveCall(record)
	service.finalizeLiveCall(record)

	concurrencyCache.mu.Lock()
	require.Equal(t, 2, concurrencyCache.releases,
		"every terminal attempt repeats the idempotent lease cleanup even when the closed marker already exists")
	concurrencyCache.mu.Unlock()
	usageRepo.mu.Lock()
	require.Len(t, usageRepo.logs, 1)
	log := usageRepo.logs[0]
	usageRepo.mu.Unlock()
	require.Equal(t, RequestTypeLive, log.RequestType)
	require.Equal(t, record.CallHash, log.RequestID)
	require.NotEqual(t, record.CallID, log.RequestID)
	require.NotNil(t, log.DurationMs)
	require.Zero(t, log.InputTokens)
	require.Zero(t, log.OutputTokens)
	require.Zero(t, log.TotalCost)
	require.Zero(t, log.ActualCost)
}

func liveTestPoolRecord(callID string) *LiveCallRecord {
	return &LiveCallRecord{
		CallID:              callID,
		CallHash:            hashLiveCallID(callID),
		AccountID:           42,
		APIKeyID:            22,
		UserID:              33,
		LeaseID:             "live-lease-" + callID,
		EgressBindingID:     StableAccountEgressBindingID(42, 101),
		EgressLeaseID:       "egress-lease-" + callID,
		EgressRouteID:       101,
		EgressIdentityID:    "501",
		EgressConfigVersion: 7,
		CreatedAt:           time.Now().Add(-time.Second),
		ExpiresAt:           time.Now().Add(time.Hour),
		Controller:          LiveControllerPending,
	}
}

func liveTestLegacyEgressRecord(t *testing.T, callID string) *LiveCallRecord {
	t.Helper()
	account := legacyEgressTestAccount()
	admission, err := resolveLegacyAccountEgressAdmission(account)
	require.NoError(t, err)
	return &LiveCallRecord{
		CallID:                    callID,
		CallHash:                  hashLiveCallID(callID),
		AccountID:                 account.ID,
		APIKeyID:                  22,
		UserID:                    33,
		LeaseID:                   "live-lease-" + callID,
		LegacyEgressBindingID:     admission.BindingID,
		LegacyEgressRouteID:       admission.RouteID,
		LegacyEgressIdentityID:    admission.IdentityID,
		LegacyEgressConfigVersion: admission.ConfigVersion,
		CreatedAt:                 time.Now().Add(-time.Second),
		ExpiresAt:                 time.Now().Add(time.Hour),
		Controller:                LiveControllerPending,
	}
}

func TestLegacyLiveLeaseRefreshAndReleaseUsePersistedIdentity(t *testing.T) {
	record := liveTestLegacyEgressRecord(t, "legacy-refresh-release")
	cache := &liveTestConcurrencyCache{legacyOwned: true}
	service := &OpenAIGatewayService{concurrencyService: NewConcurrencyService(cache)}

	require.True(t, service.refreshLiveLease(record))
	service.releaseLiveConcurrencyLease(record)

	cache.mu.Lock()
	defer cache.mu.Unlock()
	require.Equal(t, 1, cache.legacyRefreshes)
	require.Equal(t, 1, cache.legacyLiveReleases)
	require.Zero(t, cache.releases)
	require.Equal(t, record.AccountID, cache.legacyAccountID)
	require.Equal(t, record.UserID, cache.legacyUserID)
	require.Equal(t, record.APIKeyID, cache.legacyAPIKeyID)
	require.Equal(t, record.LeaseID, cache.legacyLeaseID)
	require.Equal(t, record.LegacyEgressIdentityID, cache.legacyIdentityID)
}

func TestLiveObserverRestoresPersistedLegacyEgress(t *testing.T) {
	account := legacyEgressTestAccount()
	account.Credentials = map[string]any{
		"access_token":       "test-access-token",
		"chatgpt_account_id": "acct_test",
	}
	record := liveTestLegacyEgressRecord(t, "legacy-observer")
	attestationCipher := newLiveAttestationCipher(&config.Config{
		JWT: config.JWTConfig{Secret: "legacy-live-observer-secret"},
	})
	var err error
	record.AttestationCiphertext, err = attestationCipher.Encrypt(`{"v":1,"s":0,"t":"v1.sideband"}`)
	require.NoError(t, err)
	store := &liveTestStore{}
	require.NoError(t, store.SaveLiveCall(context.Background(), record, time.Hour))
	upstream := newLiveTestFrameConn()
	upstream.reads <- liveTestFrame{
		messageType: coderws.MessageText,
		payload:     []byte(`{"type":"session.ended"}`),
	}
	dialer := &liveTestDialer{conn: upstream}
	cache := &liveTestConcurrencyCache{legacyOwned: true}
	service := &OpenAIGatewayService{
		accountRepo:               &liveTestAccountRepo{account: account},
		cache:                     store,
		concurrencyService:        NewConcurrencyService(cache),
		openaiWSPassthroughDialer: dialer,
		liveAttestationCipher:     attestationCipher,
	}

	service.observeLiveCall(record)

	require.Equal(t, account.Proxy.URL(), dialer.proxy)
	require.Equal(t, "wss://chatgpt.com/backend-api/codex/legacy-observer", dialer.url)
	cache.mu.Lock()
	require.Equal(t, 1, cache.legacyLiveReleases)
	require.Equal(t, record.LegacyEgressIdentityID, cache.legacyIdentityID)
	cache.mu.Unlock()
}

func TestLiveSidebandRejectsChangedLegacyEgressConfig(t *testing.T) {
	record := liveTestLegacyEgressRecord(t, "legacy-config-changed")
	changed := legacyEgressTestAccount()
	changed.EgressRevision++
	dialer := &liveTestDialer{conn: newLiveTestFrameConn()}
	service := &OpenAIGatewayService{
		accountRepo:               &liveTestAccountRepo{account: changed},
		openaiWSPassthroughDialer: dialer,
	}

	_, err := service.dialLiveSideband(context.Background(), record)
	require.ErrorIs(t, err, ErrLiveUnavailable)
	require.Empty(t, dialer.url)
}

func TestLiveControllerClaimFailureDoesNotRestoreEgressLease(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*OpenAIGatewayService, *LiveCallRecord) error
	}{
		{
			name: "observer",
			run: func(service *OpenAIGatewayService, record *LiveCallRecord) error {
				service.observeLiveCall(record)
				return nil
			},
		},
		{
			name: "proxy",
			run: func(service *OpenAIGatewayService, record *LiveCallRecord) error {
				return service.ProxyLiveSideband(context.Background(), record, new(coderws.Conn))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			claimAllowed := false
			record := liveTestPoolRecord("claim-loser-" + tc.name)
			store := &liveTestStore{claimAllowed: &claimAllowed}
			require.NoError(t, store.SaveLiveCall(context.Background(), record, time.Hour))
			egressCache := &accountEgressCacheStub{refreshOwned: true}
			poolCache := &liveTestPoolConcurrencyCache{
				liveTestConcurrencyCache: &liveTestConcurrencyCache{egressOwned: true},
				accountEgressCacheStub:   egressCache,
			}
			concurrencyService := NewConcurrencyService(poolCache)
			t.Cleanup(concurrencyService.accountEgressAllocator.Close)
			service := &OpenAIGatewayService{cache: store, concurrencyService: concurrencyService}

			err := tc.run(service, record)
			if tc.name == "proxy" {
				require.ErrorIs(t, err, ErrLiveControllerChanged)
			} else {
				require.NoError(t, err)
			}
			_, refreshCalls, releaseCalls, _ := egressCache.counts()
			require.Zero(t, refreshCalls, "a claim loser must not restore or refresh the persisted egress lease")
			require.Zero(t, releaseCalls, "a claim loser must not delete another controller's egress lease")
			service.liveEgressLeaseMu.Lock()
			require.Empty(t, service.liveEgressLeases)
			service.liveEgressLeaseMu.Unlock()
		})
	}
}

func TestFinalizeLiveCallAlwaysCleansPoolLeases(t *testing.T) {
	for _, tc := range []struct {
		name      string
		configure func(*liveTestStore)
	}{
		{
			name: "closed marker error",
			configure: func(store *liveTestStore) {
				store.markClosedErr = errors.New("redis: response lost")
			},
		},
		{
			name: "closed marker already exists",
			configure: func(store *liveTestStore) {
				first := false
				store.markClosedFirst = &first
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := liveTestPoolRecord("finalize-" + tc.name)
			store := &liveTestStore{}
			require.NoError(t, store.SaveLiveCall(context.Background(), record, time.Hour))
			tc.configure(store)
			egressCache := &accountEgressCacheStub{refreshOwned: true}
			liveCache := &liveTestConcurrencyCache{egressOwned: true}
			poolCache := &liveTestPoolConcurrencyCache{
				liveTestConcurrencyCache: liveCache,
				accountEgressCacheStub:   egressCache,
			}
			concurrencyService := NewConcurrencyService(poolCache)
			t.Cleanup(concurrencyService.accountEgressAllocator.Close)
			lease, err := concurrencyService.RestoreAccountEgressLease(context.Background(), AccountEgressLeaseRef{
				AccountID:     record.AccountID,
				ID:            record.EgressLeaseID,
				BindingID:     record.EgressBindingID,
				IdentityID:    record.EgressIdentityID,
				ConfigVersion: record.EgressConfigVersion,
			})
			require.NoError(t, err)
			record.EgressLease = lease
			service := &OpenAIGatewayService{cache: store, concurrencyService: concurrencyService}
			require.NoError(t, service.rememberLiveEgressLease(record, lease, time.Now()))

			service.finalizeLiveCall(record)

			require.ErrorIs(t, context.Cause(lease.Context()), context.Canceled)
			_, _, egressReleases, _ := egressCache.counts()
			require.Equal(t, 1, egressReleases)
			liveCache.mu.Lock()
			require.Equal(t, 1, liveCache.egressLiveReleases)
			liveCache.mu.Unlock()
			service.liveEgressLeaseMu.Lock()
			require.Empty(t, service.liveEgressLeases)
			service.liveEgressLeaseMu.Unlock()
		})
	}
}

func TestRefreshLiveEgressLeaseToleratesOnlyIndeterminateErrorsInsideSafetyWindow(t *testing.T) {
	record := liveTestPoolRecord("refresh-safety")
	egressCache := &accountEgressCacheStub{refreshOwned: true}
	liveCache := &liveTestConcurrencyCache{
		egressOwned:      true,
		egressRefreshErr: errors.New("redis: i/o timeout"),
	}
	poolCache := &liveTestPoolConcurrencyCache{
		liveTestConcurrencyCache: liveCache,
		accountEgressCacheStub:   egressCache,
	}
	concurrencyService := NewConcurrencyService(poolCache)
	t.Cleanup(concurrencyService.accountEgressAllocator.Close)
	lease, err := concurrencyService.RestoreAccountEgressLease(context.Background(), AccountEgressLeaseRef{
		AccountID:     record.AccountID,
		ID:            record.EgressLeaseID,
		BindingID:     record.EgressBindingID,
		IdentityID:    record.EgressIdentityID,
		ConfigVersion: record.EgressConfigVersion,
	})
	require.NoError(t, err)
	record.EgressLease = lease
	service := &OpenAIGatewayService{concurrencyService: concurrencyService}
	require.NoError(t, service.rememberLiveEgressLease(record, lease, time.Now()))

	require.True(t, service.refreshLiveLease(record), "a single indeterminate Redis error remains inside the safety window")
	service.liveEgressLeaseMu.Lock()
	service.liveEgressLeases[record.CallHash].liveLastConfirmed = time.Now().Add(-liveLeaseTransientSafetyWindow)
	service.liveEgressLeaseMu.Unlock()
	require.False(t, service.refreshLiveLease(record), "the controller must stop before Redis can expire its Live slots")

	liveCache.mu.Lock()
	liveCache.egressRefreshErr = nil
	liveCache.egressOwned = false
	liveCache.mu.Unlock()
	service.liveEgressLeaseMu.Lock()
	service.liveEgressLeases[record.CallHash].liveLastConfirmed = time.Now()
	service.liveEgressLeaseMu.Unlock()
	require.False(t, service.refreshLiveLease(record), "a definitive missing lease is terminal even inside the safety window")
	service.releaseLiveEgressLease(record)
}

func TestGetLiveCallForIdentityRejectsMismatchedCaller(t *testing.T) {
	groupID := int64(44)
	record := &LiveCallRecord{
		CallID:     "call_identity",
		CallHash:   hashLiveCallID("call_identity"),
		APIKeyID:   22,
		UserID:     33,
		GroupID:    groupID,
		Controller: LiveControllerPending,
	}
	store := &liveTestStore{}
	require.NoError(t, store.SaveLiveCall(context.Background(), record, time.Hour))
	service := &OpenAIGatewayService{cache: store}

	_, err := service.GetLiveCallForIdentity(context.Background(), record.CallID, LiveCallIdentity{
		APIKeyID: 99,
		UserID:   record.UserID,
		GroupID:  &groupID,
	})
	require.ErrorIs(t, err, ErrLiveIdentityMismatch)

	loaded, err := service.GetLiveCallForIdentity(context.Background(), record.CallID, LiveCallIdentity{
		APIKeyID: record.APIKeyID,
		UserID:   record.UserID,
		GroupID:  &groupID,
	})
	require.NoError(t, err)
	require.Equal(t, record.AccountID, loaded.AccountID)
}

func TestProxyLiveSidebandForwardsTextAndBinary(t *testing.T) {
	account := &Account{
		ID:          11,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Concurrency: 2,
		Credentials: map[string]any{
			"access_token":       "test-access-token",
			"chatgpt_account_id": "acct_test",
		},
	}
	record := &LiveCallRecord{
		CallID:     "call_proxy",
		CallHash:   hashLiveCallID("call_proxy"),
		AccountID:  account.ID,
		APIKeyID:   22,
		UserID:     33,
		LeaseID:    "lease-1",
		CreatedAt:  time.Now(),
		ExpiresAt:  time.Now().Add(time.Minute),
		Controller: LiveControllerPending,
	}
	attestationCipher := newLiveAttestationCipher(&config.Config{
		JWT: config.JWTConfig{Secret: "live-sideband-test-secret"},
	})
	var err error
	record.AttestationCiphertext, err = attestationCipher.Encrypt(`{"v":1,"s":0,"t":"v1.sideband"}`)
	require.NoError(t, err)
	store := &liveTestStore{}
	require.NoError(t, store.SaveLiveCall(context.Background(), record, time.Hour))
	upstream := newLiveTestFrameConn()
	dialer := &liveTestDialer{conn: upstream}
	service := &OpenAIGatewayService{
		accountRepo:               &liveTestAccountRepo{account: account},
		cache:                     store,
		openaiWSPassthroughDialer: dialer,
		liveAttestationCipher:     attestationCipher,
	}
	proxyResult := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		downstream, err := coderws.Accept(writer, request, nil)
		if err != nil {
			proxyResult <- err
			return
		}
		defer func() { _ = downstream.CloseNow() }()
		proxyResult <- service.ProxyLiveSideband(request.Context(), record, downstream)
	}))
	defer server.Close()

	client, _, err := coderws.Dial(
		context.Background(),
		"ws"+strings.TrimPrefix(server.URL, "http"),
		nil,
	)
	require.NoError(t, err)
	defer func() { _ = client.CloseNow() }()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"client.text"}`)))
	clientText := <-upstream.writes
	require.Equal(t, coderws.MessageText, clientText.messageType)
	require.JSONEq(t, `{"type":"client.text"}`, string(clientText.payload))

	require.NoError(t, client.Write(ctx, coderws.MessageBinary, []byte{1, 2, 3}))
	clientBinary := <-upstream.writes
	require.Equal(t, coderws.MessageBinary, clientBinary.messageType)
	require.Equal(t, []byte{1, 2, 3}, clientBinary.payload)

	upstream.reads <- liveTestFrame{messageType: coderws.MessageText, payload: []byte(`{"type":"server.text"}`)}
	messageType, payload, err := client.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, coderws.MessageText, messageType)
	require.JSONEq(t, `{"type":"server.text"}`, string(payload))

	upstream.reads <- liveTestFrame{messageType: coderws.MessageBinary, payload: []byte{4, 5, 6}}
	messageType, payload, err = client.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, coderws.MessageBinary, messageType)
	require.Equal(t, []byte{4, 5, 6}, payload)

	require.Equal(t, "wss://chatgpt.com/backend-api/codex/call_proxy", dialer.url)
	require.Equal(t, "Bearer test-access-token", dialer.headers.Get("Authorization"))
	require.Equal(t, "acct_test", dialer.headers.Get("Chatgpt-Account-Id"))
	require.Equal(t, `{"v":1,"s":0,"t":"v1.sideband"}`, dialer.headers.Get(liveAttestationHeader))
	upstream.reads <- liveTestFrame{err: coderws.CloseError{Code: coderws.StatusNormalClosure}}
	require.ErrorIs(t, <-proxyResult, ErrLiveCallNotFound)
}

// TestLiveSessionEndedTreatsLeaseLossAsTerminal 锁定：租约续租失败（ErrLiveUnavailable）
// 必须判为会话终结。RefreshLiveLease 的 Lua 在 leaseID 被 GC 后不会重新写入，若把它
// 当临时错误交给 observer 重连，会话会空转到 ExpiresAt 且不计入任何并发限制。
func TestLiveSessionEndedTreatsLeaseLossAsTerminal(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"租约丢失", ErrLiveUnavailable, true},
		{"租约丢失（被包装）", fmt.Errorf("refresh live lease: %w", ErrLiveUnavailable), true},
		{"上游报告会话已关闭", ErrLiveCallNotFound, true},
		{"到达会话时长上限", context.DeadlineExceeded, true},
		{"控制权被他人接管", ErrLiveControllerChanged, false},
		{"临时读错误", errors.New("unexpected EOF"), false},
		{"无错误", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, liveSessionEnded(tc.err))
		})
	}
}

// TestWaitForLiveObserverRetryLeavesExpiryToLoopFinalize 锁定：已过期但控制权仍在
// observer 手上时返回 true，让调用方回到 observeLiveCall 循环顶部的过期分支去
// finalize（写 usage log + 释放租约）。在此处直接返回 false 会让会话静默结束、不留记录。
func TestWaitForLiveObserverRetryLeavesExpiryToLoopFinalize(t *testing.T) {
	record := &LiveCallRecord{
		CallID:     "call_expired",
		CallHash:   hashLiveCallID("call_expired"),
		Controller: LiveControllerObserver,
		ExpiresAt:  time.Now().Add(-time.Minute),
	}
	store := &liveTestStore{}
	require.NoError(t, store.SaveLiveCall(context.Background(), record, time.Hour))
	svc := &OpenAIGatewayService{cache: store}

	require.True(t, svc.waitForLiveObserverRetry(record),
		"过期判定必须留给循环顶部，否则不会写 usage log")

	// 控制权已被他人接管时仍必须停止重试，避免与新控制者抢同一个 call。
	require.NoError(t, store.SaveLiveCall(context.Background(), &LiveCallRecord{
		CallID:     record.CallID,
		CallHash:   record.CallHash,
		Controller: LiveControllerProxy,
		ExpiresAt:  time.Now().Add(time.Hour),
	}, time.Hour))
	require.False(t, svc.waitForLiveObserverRetry(record))
}

// TestWaitForLiveObserverRetryTreatsStoreErrorAsRetryable 锁定：store 报错（Redis
// 抖动）不等于控制权被接管，必须返回 true 交回 observeLiveCall 循环顶部，由它做
// 有限次重试与 ExpiresAt 兜底 finalize；记录确实不存在时才停止重试。
func TestWaitForLiveObserverRetryTreatsStoreErrorAsRetryable(t *testing.T) {
	record := &LiveCallRecord{
		CallID:     "call_flaky_store",
		CallHash:   hashLiveCallID("call_flaky_store"),
		Controller: LiveControllerObserver,
		ExpiresAt:  time.Now().Add(time.Hour),
	}
	store := &liveTestStore{getControllerErr: errors.New("redis: connection refused")}
	require.NoError(t, store.SaveLiveCall(context.Background(), record, time.Hour))
	svc := &OpenAIGatewayService{cache: store}

	require.True(t, svc.waitForLiveObserverRetry(record),
		"store 报错必须继续重试，否则 Redis 抖动会让会话静默结束、不留记录")

	// 记录已被清理（ErrLiveCallNotFound）不是故障，应停止重试。
	require.False(t, (&OpenAIGatewayService{cache: &liveTestStore{}}).waitForLiveObserverRetry(record))
}

// TestObserveLiveCallStoreOutageFallsBackToExpiryFinalize 锁定：observer 遇到持续
// store 报错时不能静默退出，必须按 record.ExpiresAt 兜底 finalize（写 usage log +
// 释放租约）。
func TestObserveLiveCallStoreOutageFallsBackToExpiryFinalize(t *testing.T) {
	restore := liveObserverStoreRetryInterval
	liveObserverStoreRetryInterval = time.Millisecond
	t.Cleanup(func() { liveObserverStoreRetryInterval = restore })

	cases := []struct {
		name   string
		inject func(*liveTestStore)
	}{
		{"GetLiveCall 持续报错", func(s *liveTestStore) { s.getCallErr = errors.New("redis: i/o timeout") }},
		{"ClaimLiveController 报错", func(s *liveTestStore) { s.claimErr = errors.New("redis: i/o timeout") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			record := &LiveCallRecord{
				CallID:     "call_store_outage",
				CallHash:   hashLiveCallID("call_store_outage"),
				AccountID:  11,
				APIKeyID:   22,
				UserID:     33,
				LeaseID:    "lease-1",
				Model:      "gpt-live-test",
				CreatedAt:  time.Now().Add(-time.Minute),
				ExpiresAt:  time.Now().Add(-time.Second), // 已到期：兜底无需等待
				Controller: LiveControllerPending,
			}
			store := &liveTestStore{}
			require.NoError(t, store.SaveLiveCall(context.Background(), record, time.Hour))
			tc.inject(store)
			concurrencyCache := &liveTestConcurrencyCache{}
			usageRepo := &liveTestUsageRepo{}
			svc := &OpenAIGatewayService{
				cache:              store,
				concurrencyService: NewConcurrencyService(concurrencyCache),
				usageLogRepo:       usageRepo,
			}

			svc.observeLiveCall(record)

			concurrencyCache.mu.Lock()
			require.Equal(t, 1, concurrencyCache.releases, "store 故障时租约释放不能丢")
			concurrencyCache.mu.Unlock()
			usageRepo.mu.Lock()
			require.Len(t, usageRepo.logs, 1, "store 故障时 usage log 不能丢")
			require.Equal(t, RequestTypeLive, usageRepo.logs[0].RequestType)
			usageRepo.mu.Unlock()
		})
	}
}

type liveTestBestEffortUsageRepo struct {
	liveTestUsageRepo
	bestEffortErr   error
	bestEffortCalls int
}

func (r *liveTestBestEffortUsageRepo) CreateBestEffort(_ context.Context, _ *UsageLog) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bestEffortCalls++
	return r.bestEffortErr
}

// TestFinalizeLiveCallUsageLogFallsBackToSyncCreate 锁定：finalize 是该会话唯一一次
// 落库机会（MarkLiveCallClosed 已标记 first），best-effort 写入失败必须走同步 Create
// 兜底，而不是丢弃错误。
func TestFinalizeLiveCallUsageLogFallsBackToSyncCreate(t *testing.T) {
	record := &LiveCallRecord{
		CallID:     "call_usage_fallback",
		CallHash:   hashLiveCallID("call_usage_fallback"),
		AccountID:  11,
		APIKeyID:   22,
		UserID:     33,
		LeaseID:    "lease-1",
		Model:      "gpt-live-test",
		CreatedAt:  time.Now().Add(-time.Second),
		ExpiresAt:  time.Now().Add(time.Hour),
		Controller: LiveControllerPending,
	}
	store := &liveTestStore{}
	require.NoError(t, store.SaveLiveCall(context.Background(), record, time.Hour))
	usageRepo := &liveTestBestEffortUsageRepo{bestEffortErr: errors.New("usage log queue dropped")}
	svc := &OpenAIGatewayService{
		cache:              store,
		concurrencyService: NewConcurrencyService(&liveTestConcurrencyCache{}),
		usageLogRepo:       usageRepo,
	}

	svc.finalizeLiveCall(record)

	usageRepo.mu.Lock()
	defer usageRepo.mu.Unlock()
	require.Equal(t, 1, usageRepo.bestEffortCalls)
	require.Len(t, usageRepo.logs, 1, "best-effort 失败后必须同步兜底落库")
	require.Equal(t, record.CallHash, usageRepo.logs[0].RequestID)
}
