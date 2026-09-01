package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	coderws "github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

const (
	defaultLiveMaxSessionDuration = time.Hour
	liveLeaseRefreshInterval      = 20 * time.Second
	liveRedisOperationTimeout     = 3 * time.Second
	liveClosedRecordTTL           = 24 * time.Hour
	liveObserverPollInterval      = 250 * time.Millisecond
	liveObserverStoreRetryLimit   = 5
	liveUpstreamBodyLimit         = 2 << 20
	// Stop before Redis can expire the 60-second user/API-key Live slots. This
	// permits one missed 20-second heartbeat while preserving a full heartbeat
	// interval as the no-overcommit margin.
	liveLeaseTransientSafetyWindow = LiveConcurrencyLeaseTTL - liveLeaseRefreshInterval
)

// liveObserverStoreRetryInterval 是 var 以便测试缩短 store 报错的重试等待。
var liveObserverStoreRetryInterval = time.Second

var (
	chatGPTLiveCallsURL        = "https://chatgpt.com/backend-api/codex/realtime/calls?intent=quicksilver&architecture=avas"
	chatGPTLiveSidebandBaseURL = "wss://chatgpt.com/backend-api/codex"
)

type liveFrameConn interface {
	ReadFrame(ctx context.Context) (coderws.MessageType, []byte, error)
	WriteFrame(ctx context.Context, msgType coderws.MessageType, payload []byte) error
	Close() error
}

func liveSidebandReadError(err error) error {
	if coderws.CloseStatus(err) == coderws.StatusNormalClosure {
		return ErrLiveCallNotFound
	}
	return err
}

func hashLiveCallID(callID string) string {
	sum := sha256.Sum256([]byte(callID))
	return hex.EncodeToString(sum[:])
}

func liveGroupID(groupID *int64) int64 {
	if groupID == nil {
		return 0
	}
	return *groupID
}

func liveOptionalID(value int64) *int64 {
	if value <= 0 {
		return nil
	}
	result := value
	return &result
}

func (s *OpenAIGatewayService) liveStore() (LiveCallStore, error) {
	if s == nil || s.cache == nil {
		return nil, ErrLiveUnavailable
	}
	store, ok := s.cache.(LiveCallStore)
	if !ok {
		return nil, ErrLiveUnavailable
	}
	return store, nil
}

func (s *OpenAIGatewayService) liveConcurrencyCache() (LiveConcurrencyCache, error) {
	if s == nil || s.concurrencyService == nil || s.concurrencyService.cache == nil {
		return nil, ErrLiveUnavailable
	}
	cache, ok := s.concurrencyService.cache.(LiveConcurrencyCache)
	if !ok {
		return nil, ErrLiveUnavailable
	}
	return cache, nil
}

func (s *OpenAIGatewayService) liveEgressConcurrencyCache() (LiveEgressConcurrencyCache, error) {
	if s == nil || s.concurrencyService == nil || s.concurrencyService.cache == nil {
		return nil, ErrLiveUnavailable
	}
	cache, ok := s.concurrencyService.cache.(LiveEgressConcurrencyCache)
	if !ok {
		return nil, ErrLiveUnavailable
	}
	return cache, nil
}

func liveCallEgressRef(record *LiveCallRecord) (AccountEgressLeaseRef, bool, error) {
	if record == nil {
		return AccountEgressLeaseRef{}, false, ErrLiveUnavailable
	}
	hasEgress := strings.TrimSpace(record.EgressBindingID) != "" ||
		strings.TrimSpace(record.EgressLeaseID) != "" ||
		record.EgressRouteID > 0 || strings.TrimSpace(record.EgressIdentityID) != "" ||
		record.EgressConfigVersion > 0
	if !hasEgress {
		return AccountEgressLeaseRef{}, false, nil
	}
	ref := AccountEgressLeaseRef{
		AccountID:     record.AccountID,
		ID:            strings.TrimSpace(record.EgressLeaseID),
		BindingID:     strings.TrimSpace(record.EgressBindingID),
		IdentityID:    strings.TrimSpace(record.EgressIdentityID),
		ConfigVersion: record.EgressConfigVersion,
	}
	accountID, routeID, ok := parseStableAccountEgressBindingID(ref.BindingID)
	if !ok || accountID != record.AccountID || routeID != record.EgressRouteID {
		return AccountEgressLeaseRef{}, true, ErrLiveUnavailable
	}
	if err := validateRestoredAccountEgressLeaseRef(ref); err != nil {
		return AccountEgressLeaseRef{}, true, ErrLiveUnavailable
	}
	return ref, true, nil
}

func accountEgressLeaseMatchesRef(lease *AccountEgressLease, ref AccountEgressLeaseRef) bool {
	return lease != nil && lease.ref() == ref
}

type liveEgressLeaseState struct {
	lease             *AccountEgressLease
	owners            map[string]struct{}
	liveLastConfirmed time.Time
}

// rememberLiveEgressLease records a lease acquired while creating a Live call.
// At that point both the egress slot and the user/API-key Live slots were
// confirmed by one Redis script, so the transient-error safety window can begin.
func (s *OpenAIGatewayService) rememberLiveEgressLease(
	record *LiveCallRecord,
	lease *AccountEgressLease,
	confirmedAt time.Time,
) error {
	ref, poolMode, err := liveCallEgressRef(record)
	if err != nil || !poolMode || !accountEgressLeaseMatchesRef(lease, ref) || context.Cause(lease.Context()) != nil {
		return ErrLiveUnavailable
	}
	s.liveEgressLeaseMu.Lock()
	defer s.liveEgressLeaseMu.Unlock()
	if s.liveEgressLeases == nil {
		s.liveEgressLeases = make(map[string]*liveEgressLeaseState)
	}
	if state := s.liveEgressLeases[record.CallHash]; state != nil {
		if !accountEgressLeaseMatchesRef(state.lease, ref) || context.Cause(state.lease.Context()) != nil {
			return ErrLiveUnavailable
		}
		if state.liveLastConfirmed.Before(confirmedAt) {
			state.liveLastConfirmed = confirmedAt
		}
		record.EgressLease = state.lease
		return nil
	}
	s.liveEgressLeases[record.CallHash] = &liveEgressLeaseState{
		lease:             lease,
		owners:            make(map[string]struct{}),
		liveLastConfirmed: confirmedAt,
	}
	record.EgressLease = lease
	return nil
}

// ensureLiveEgressLease returns the one process-local lease for the persisted
// binding. A different process restores it only after its controller claim and
// only when Redis confirms the exact lease metadata and identity membership.
func (s *OpenAIGatewayService) ensureLiveEgressLease(record *LiveCallRecord) (*AccountEgressLease, error) {
	ref, poolMode, err := liveCallEgressRef(record)
	if err != nil || !poolMode {
		return nil, err
	}
	if s == nil || s.concurrencyService == nil {
		return nil, ErrLiveUnavailable
	}

	s.liveEgressLeaseMu.Lock()
	defer s.liveEgressLeaseMu.Unlock()
	if s.liveEgressLeases == nil {
		s.liveEgressLeases = make(map[string]*liveEgressLeaseState)
	}
	if state := s.liveEgressLeases[record.CallHash]; state != nil {
		if !accountEgressLeaseMatchesRef(state.lease, ref) || context.Cause(state.lease.Context()) != nil {
			return nil, ErrLiveUnavailable
		}
		record.EgressLease = state.lease
		return state.lease, nil
	}
	if record.EgressLease != nil {
		if !accountEgressLeaseMatchesRef(record.EgressLease, ref) || context.Cause(record.EgressLease.Context()) != nil {
			return nil, ErrLiveUnavailable
		}
		s.liveEgressLeases[record.CallHash] = &liveEgressLeaseState{
			lease:  record.EgressLease,
			owners: make(map[string]struct{}),
		}
		return record.EgressLease, nil
	}

	restoreCtx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	lease, restoreErr := s.concurrencyService.RestoreAccountEgressLease(restoreCtx, ref)
	cancel()
	if restoreErr != nil || lease == nil {
		logger.L().Warn("openai_live_egress_restore_failed",
			zap.Int64("account_id", record.AccountID),
			zap.String("call_hash", record.CallHash),
			zap.String("egress_binding_id", record.EgressBindingID),
			zap.Error(restoreErr),
		)
		return nil, ErrLiveUnavailable
	}
	record.EgressLease = lease
	s.liveEgressLeases[record.CallHash] = &liveEgressLeaseState{
		lease:  lease,
		owners: make(map[string]struct{}),
	}
	return lease, nil
}

func (s *OpenAIGatewayService) activateLiveEgressOwner(record *LiveCallRecord, owner string) error {
	if strings.TrimSpace(owner) == "" {
		return ErrLiveUnavailable
	}
	lease, err := s.ensureLiveEgressLease(record)
	if err != nil {
		return err
	}
	s.liveEgressLeaseMu.Lock()
	state := s.liveEgressLeases[record.CallHash]
	if state == nil || state.lease != lease {
		s.liveEgressLeaseMu.Unlock()
		return ErrLiveUnavailable
	}
	state.owners[owner] = struct{}{}
	s.liveEgressLeaseMu.Unlock()
	if !s.refreshLiveLease(record) {
		s.releaseLiveEgressOwner(record, owner)
		return ErrLiveUnavailable
	}
	return nil
}

// releaseLiveEgressOwner relinquishes only this process-local controller's
// ownership. The last local owner abandons its refresher without deleting the
// Redis lease, which may already belong to a controller in another process.
func (s *OpenAIGatewayService) releaseLiveEgressOwner(record *LiveCallRecord, owner string) {
	if s == nil || record == nil || strings.TrimSpace(owner) == "" {
		return
	}
	s.liveEgressLeaseMu.Lock()
	if state := s.liveEgressLeases[record.CallHash]; state != nil {
		if _, owned := state.owners[owner]; owned {
			delete(state.owners, owner)
			if len(state.owners) == 0 {
				delete(s.liveEgressLeases, record.CallHash)
				if record.EgressLease == state.lease {
					record.EgressLease = nil
				}
				// Keep the registry lock until the allocator unregister completes;
				// otherwise a new local owner could restore this doomed object.
				state.lease.Abandon()
			}
		}
	}
	s.liveEgressLeaseMu.Unlock()
}

// abandonUnownedLiveEgressLease cleans up the creation-process lease when that
// process loses or cannot confirm the controller claim. Existing local owners
// are left untouched, and Redis is never modified by this operation.
func (s *OpenAIGatewayService) abandonUnownedLiveEgressLease(record *LiveCallRecord) {
	if s == nil || record == nil {
		return
	}
	s.liveEgressLeaseMu.Lock()
	if state := s.liveEgressLeases[record.CallHash]; state != nil && len(state.owners) == 0 {
		delete(s.liveEgressLeases, record.CallHash)
		if record.EgressLease == state.lease {
			record.EgressLease = nil
		}
		state.lease.Abandon()
	} else if state == nil && record.EgressLease != nil {
		record.EgressLease.Abandon()
		record.EgressLease = nil
	}
	s.liveEgressLeaseMu.Unlock()
}

func (s *OpenAIGatewayService) markLiveEgressLeaseConfirmed(record *LiveCallRecord, lease *AccountEgressLease) {
	s.liveEgressLeaseMu.Lock()
	if state := s.liveEgressLeases[record.CallHash]; state != nil && state.lease == lease {
		state.liveLastConfirmed = time.Now()
	}
	s.liveEgressLeaseMu.Unlock()
}

func (s *OpenAIGatewayService) liveEgressLeaseInsideSafetyWindow(record *LiveCallRecord, lease *AccountEgressLease) bool {
	s.liveEgressLeaseMu.Lock()
	defer s.liveEgressLeaseMu.Unlock()
	state := s.liveEgressLeases[record.CallHash]
	return state != nil && state.lease == lease && !state.liveLastConfirmed.IsZero() &&
		time.Now().Before(state.liveLastConfirmed.Add(liveLeaseTransientSafetyWindow))
}

func (s *OpenAIGatewayService) liveMaxSessionDuration() time.Duration {
	if s != nil && s.cfg != nil && s.cfg.Gateway.Live.MaxSessionDurationSeconds > 0 {
		return time.Duration(s.cfg.Gateway.Live.MaxSessionDurationSeconds) * time.Second
	}
	return defaultLiveMaxSessionDuration
}

func ValidateLiveCallRequest(request *LiveCallRequest) error {
	if request == nil || strings.TrimSpace(request.SDP) == "" {
		return errors.New("sdp is required")
	}
	if len(request.Session) == 0 || !json.Valid(request.Session) {
		return errors.New("session must be valid JSON")
	}
	var sessionObject map[string]json.RawMessage
	if err := json.Unmarshal(request.Session, &sessionObject); err != nil {
		return errors.New("session must be a JSON object")
	}
	if sessionObject == nil {
		return errors.New("session must be a JSON object")
	}
	return nil
}

// CreateLiveCall 创建 Frameless 会话。调用方须在调用期间持有普通用户槽位；
// 调度器持有的普通账号槽位会被同一个 Live 租约原子接替。
func (s *OpenAIGatewayService) CreateLiveCall(
	ctx context.Context,
	request *LiveCallRequest,
	identity LiveCallIdentity,
	userMaxConcurrency int,
) (*LiveCallCreated, error) {
	if err := ValidateLiveCallRequest(request); err != nil {
		return nil, err
	}
	store, err := s.liveStore()
	if err != nil {
		return nil, err
	}
	liveCache, err := s.liveConcurrencyCache()
	if err != nil {
		return nil, err
	}
	attestation, attestationCiphertext, err := s.prepareLiveAttestation(ctx)
	if err != nil {
		return nil, err
	}

	excluded := make(map[int64]struct{})
	// Live 按通话时长计费，不属于 token 利润门的语义范围：显式豁免，避免
	// 防御性装门按文本 D 过滤 Live 账号池且门与计费时刻不同源。
	ctx = WithOpenAIProfitControlSuppressed(ctx)
	var lastErr error
	for attempt := 0; attempt <= 3; attempt++ {
		selection, _, selectErr := s.SelectAccountWithSchedulerForCapability(
			ctx,
			identity.GroupID,
			"",
			uuid.NewString(),
			"",
			excluded,
			OpenAIUpstreamTransportHTTPSSE,
			OpenAIEndpointCapabilityLive,
			false,
			false,
			false,
		)
		if selectErr != nil {
			if lastErr != nil {
				return nil, lastErr
			}
			return nil, selectErr
		}
		if selection == nil || selection.Account == nil || !selection.Acquired {
			if selection != nil && selection.ReleaseFunc != nil {
				selection.ReleaseFunc()
			}
			return nil, ErrLiveConcurrencyFull
		}

		account := selection.Account
		leaseID := generateRequestID()
		provisionalRecord := &LiveCallRecord{
			AccountID: account.ID,
			APIKeyID:  identity.APIKeyID,
			UserID:    identity.UserID,
			LeaseID:   leaseID,
		}
		poolMode := selection.Egress != nil
		var acquired bool
		var acquireErr error
		if poolMode {
			if selection.Egress.Lease == nil {
				selection.ReleaseFunc()
				return nil, ErrLiveUnavailable
			}
			egressRef := selection.Egress.Lease.ref()
			provisionalRecord.EgressBindingID = selection.Egress.BindingID
			provisionalRecord.EgressLeaseID = egressRef.ID
			provisionalRecord.EgressRouteID = selection.Egress.RouteID
			provisionalRecord.EgressIdentityID = selection.Egress.IdentityID
			provisionalRecord.EgressConfigVersion = selection.Egress.ConfigVersion
			provisionalRecord.EgressLease = selection.Egress.Lease
			if _, _, refErr := liveCallEgressRef(provisionalRecord); refErr != nil ||
				!accountEgressLeaseMatchesRef(selection.Egress.Lease, AccountEgressLeaseRef{
					AccountID:     account.ID,
					ID:            provisionalRecord.EgressLeaseID,
					BindingID:     provisionalRecord.EgressBindingID,
					IdentityID:    provisionalRecord.EgressIdentityID,
					ConfigVersion: provisionalRecord.EgressConfigVersion,
				}) {
				selection.ReleaseFunc()
				return nil, ErrLiveUnavailable
			}
			egressCache, cacheErr := s.liveEgressConcurrencyCache()
			if cacheErr != nil {
				selection.ReleaseFunc()
				return nil, cacheErr
			}
			acquired, acquireErr = egressCache.AcquireLiveLeaseForEgress(
				ctx,
				egressRef,
				identity.UserID,
				userMaxConcurrency,
				identity.APIKeyID,
				leaseID,
				true,
			)
		} else {
			acquired, acquireErr = liveCache.AcquireLiveLease(
				ctx,
				account.ID,
				account.Concurrency,
				identity.UserID,
				userMaxConcurrency,
				identity.APIKeyID,
				leaseID,
				true,
			)
		}
		if acquireErr != nil || !acquired {
			selection.ReleaseFunc()
			if acquireErr != nil {
				return nil, acquireErr
			}
			return nil, ErrLiveConcurrencyFull
		}
		liveLeaseConfirmedAt := time.Now()

		created, createErr := s.createUpstreamLiveCall(ctx, account, request, attestation)
		if createErr != nil {
			s.releaseLiveConcurrencyLease(provisionalRecord)
			selection.ReleaseFunc()
			if !s.shouldFailoverLiveCreateError(createErr) {
				return nil, createErr
			}
			excluded[account.ID] = struct{}{}
			lastErr = createErr
			continue
		}
		if poolMode {
			if !selection.Egress.Lease.Detach() {
				s.releaseLiveConcurrencyLease(provisionalRecord)
				selection.ReleaseFunc()
				return nil, ErrLiveUnavailable
			}
		} else {
			selection.ReleaseFunc()
		}

		now := time.Now()
		model := strings.TrimSpace(gjson.GetBytes(request.Session, "model").String())
		if model == "" {
			model = "gpt-live"
		}
		record := &LiveCallRecord{
			CallID:                created.CallID,
			CallHash:              hashLiveCallID(created.CallID),
			AccountID:             account.ID,
			APIKeyID:              identity.APIKeyID,
			UserID:                identity.UserID,
			GroupID:               liveGroupID(identity.GroupID),
			SubscriptionID:        liveGroupID(identity.SubscriptionID),
			LeaseID:               leaseID,
			Model:                 model,
			CreatedAt:             now,
			ExpiresAt:             now.Add(s.liveMaxSessionDuration()),
			Controller:            LiveControllerPending,
			UserAgent:             identity.UserAgent,
			IPAddress:             identity.IPAddress,
			InboundEndpoint:       identity.InboundEndpoint,
			AttestationCiphertext: attestationCiphertext,
		}
		if poolMode {
			record.EgressBindingID = provisionalRecord.EgressBindingID
			record.EgressLeaseID = provisionalRecord.EgressLeaseID
			record.EgressRouteID = provisionalRecord.EgressRouteID
			record.EgressIdentityID = provisionalRecord.EgressIdentityID
			record.EgressConfigVersion = provisionalRecord.EgressConfigVersion
			record.EgressLease = provisionalRecord.EgressLease
			if rememberErr := s.rememberLiveEgressLease(record, record.EgressLease, liveLeaseConfirmedAt); rememberErr != nil {
				s.releaseLiveCallLeases(record)
				return nil, rememberErr
			}
		}
		mappingTTL := s.liveMaxSessionDuration() + 5*time.Minute
		if saveErr := store.SaveLiveCall(ctx, record, mappingTTL); saveErr != nil {
			s.releaseLiveCallLeases(record)
			return nil, fmt.Errorf("save live call mapping: %w", saveErr)
		}
		created.Account = account
		go s.observeLiveCall(record)
		return created, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, ErrLiveUnavailable
}

func (s *OpenAIGatewayService) shouldFailoverLiveCreateError(err error) bool {
	var upstreamErr *UpstreamFailoverError
	if !errors.As(err, &upstreamErr) {
		// 凭证读取和网络传输错误都可能只影响当前账号或代理。
		return true
	}
	return s.shouldFailoverOpenAIUpstreamResponse(
		upstreamErr.StatusCode,
		"",
		upstreamErr.ResponseBody,
	)
}

func (s *OpenAIGatewayService) createUpstreamLiveCall(
	ctx context.Context,
	account *Account,
	request *LiveCallRequest,
	attestation string,
) (*LiveCallCreated, error) {
	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		logLiveCreateStageFailure(ctx, account.ID, "access_token", err)
		return nil, err
	}
	body, err := json.Marshal(struct {
		SDP     string          `json:"sdp"`
		Session json.RawMessage `json:"session"`
	}{
		SDP:     request.SDP,
		Session: request.Session,
	})
	if err != nil {
		return nil, err
	}
	reqCtx := WithHTTPUpstreamRedirectsDisabled(WithHTTPUpstreamProfile(ctx, HTTPUpstreamProfileOpenAI))
	reqCtx = ContextWithSelectedAccountEgress(reqCtx, account)
	upstreamReq, err := http.NewRequestWithContext(reqCtx, http.MethodPost, chatGPTLiveCallsURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	authHeaders, err := s.buildOpenAIAuthenticationHeaders(ctx, account, token)
	if err != nil {
		logLiveCreateStageFailure(ctx, account.ID, "authentication_headers", err)
		return nil, err
	}
	for key, values := range authHeaders {
		for _, value := range values {
			upstreamReq.Header.Add(key, value)
		}
	}
	upstreamReq.Host = "chatgpt.com"
	if err := resolveAndSetOpenAIChatGPTAccountHeaders(ctx, s.accountRepo, upstreamReq.Header, account); err != nil {
		logLiveCreateStageFailure(ctx, account.ID, "account_headers", err)
		return nil, err
	}
	upstreamReq.Header.Set("Content-Type", "application/json")
	upstreamReq.Header.Set("Accept", "application/sdp")
	upstreamReq.Header.Set(liveAttestationHeader, attestation)
	applyLiveUpstreamIdentityHeaders(upstreamReq.Header)

	resp, err := s.doOpenAIUpstream(upstreamReq, resolveAccountProxyURL(account), account)
	if err != nil {
		logLiveCreateStageFailure(ctx, account.ID, "upstream_transport", err)
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, liveUpstreamBodyLimit+1))
	if readErr != nil {
		return nil, readErr
	}
	if len(responseBody) > liveUpstreamBodyLimit {
		return nil, errors.New("live upstream response is too large")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logLiveUpstreamFailure(ctx, account.ID, resp.StatusCode, resp.Header, responseBody)
		return nil, &UpstreamFailoverError{
			StatusCode:      resp.StatusCode,
			ResponseBody:    responseBody,
			ResponseHeaders: resp.Header.Clone(),
		}
	}
	callID, err := liveCallIDFromLocation(resp.Header.Get("Location"))
	if err != nil {
		return nil, err
	}
	return &LiveCallCreated{
		SDP:      responseBody,
		CallID:   callID,
		Location: resp.Header.Get("Location"),
	}, nil
}

func logLiveCreateStageFailure(ctx context.Context, accountID int64, stage string, err error) {
	logger.FromContext(ctx).Warn(
		"OpenAI Live 创建阶段失败",
		zap.Int64("account_id", accountID),
		zap.String("stage", stage),
		zap.String("error_type", fmt.Sprintf("%T", err)),
	)
}

func logLiveUpstreamFailure(
	ctx context.Context,
	accountID int64,
	statusCode int,
	headers http.Header,
	body []byte,
) {
	errorType := strings.TrimSpace(gjson.GetBytes(body, "error.type").String())
	errorCode := strings.TrimSpace(gjson.GetBytes(body, "error.code").String())
	errorMessage := strings.TrimSpace(gjson.GetBytes(body, "error.message").String())
	if errorType == "" {
		errorType = strings.TrimSpace(gjson.GetBytes(body, "type").String())
	}
	if errorCode == "" {
		errorCode = strings.TrimSpace(gjson.GetBytes(body, "code").String())
	}
	if errorMessage == "" {
		errorMessage = strings.TrimSpace(gjson.GetBytes(body, "message").String())
	}
	if errorMessage == "" {
		errorMessage = strings.TrimSpace(gjson.GetBytes(body, "detail").String())
	}

	logger.FromContext(ctx).Warn(
		"OpenAI Live 上游拒绝请求",
		zap.Int64("account_id", accountID),
		zap.Int("upstream_status_code", statusCode),
		zap.String("upstream_error_type", truncateOpenAIWSLogValue(errorType, 120)),
		zap.String("upstream_error_code", truncateOpenAIWSLogValue(errorCode, 120)),
		zap.String("upstream_error_message", truncateOpenAIWSLogValue(errorMessage, 300)),
		zap.String("upstream_content_type", truncateOpenAIWSLogValue(headers.Get("Content-Type"), 120)),
		zap.String("upstream_server", truncateOpenAIWSLogValue(headers.Get("Server"), 120)),
		zap.String("upstream_cf_mitigated", truncateOpenAIWSLogValue(headers.Get("Cf-Mitigated"), 120)),
		zap.String("upstream_cf_ray", truncateOpenAIWSLogValue(headers.Get("Cf-Ray"), 120)),
		zap.String("upstream_request_id", truncateOpenAIWSLogValue(headers.Get("X-Request-Id"), 120)),
	)
}

func liveCallIDFromLocation(location string) (string, error) {
	location = strings.TrimSpace(location)
	if location == "" {
		return "", errors.New("live upstream response has no Location")
	}
	parsed, err := url.Parse(location)
	if err != nil {
		return "", fmt.Errorf("parse live Location: %w", err)
	}
	callID := strings.TrimSpace(path.Base(strings.TrimSuffix(parsed.Path, "/")))
	if callID == "" || callID == "." || callID == "codex" {
		return "", errors.New("live upstream Location has no call id")
	}
	return callID, nil
}

func applyLiveUpstreamIdentityHeaders(headers http.Header) {
	headers.Set("OpenAI-Alpha", "quicksilver=v2")
	ensureCodexIdentityHeaders(headers)
	enforceCodexIdentityHeaders(headers)
	if strings.TrimSpace(headers.Get("session-id")) == "" {
		headers.Set("session-id", uuid.NewString())
	}
	if strings.TrimSpace(headers.Get("thread-id")) == "" {
		headers.Set("thread-id", uuid.NewString())
	}
	// Realtime/Live 不使用 Responses 的实验头。
	headers.Del("OpenAI-Beta")
}

func (s *OpenAIGatewayService) liveSidebandHeaders(
	ctx context.Context,
	account *Account,
	record *LiveCallRecord,
) (http.Header, error) {
	token, _, err := s.GetAccessToken(ctx, account)
	if err != nil {
		return nil, err
	}
	headers, err := s.buildOpenAIAuthenticationHeaders(ctx, account, token)
	if err != nil {
		return nil, err
	}
	if err := resolveAndSetOpenAIChatGPTAccountHeaders(ctx, s.accountRepo, headers, account); err != nil {
		return nil, err
	}
	attestation, err := s.decryptLiveAttestation(record)
	if err != nil {
		return nil, err
	}
	headers.Set(liveAttestationHeader, attestation)
	applyLiveUpstreamIdentityHeaders(headers)
	return headers, nil
}

func (s *OpenAIGatewayService) dialLiveSideband(ctx context.Context, record *LiveCallRecord) (liveFrameConn, error) {
	account, err := s.accountRepo.GetByID(ctx, record.AccountID)
	if err != nil {
		return nil, err
	}
	if account == nil || !account.SupportsOpenAIEndpointCapability(OpenAIEndpointCapabilityLive) {
		return nil, ErrLiveUnavailable
	}
	if _, poolMode, egressErr := liveCallEgressRef(record); egressErr != nil {
		return nil, egressErr
	} else if poolMode {
		lease, leaseErr := s.ensureLiveEgressLease(record)
		if leaseErr != nil {
			return nil, leaseErr
		}
		resolved := &ResolvedAccountEgress{
			BindingID:     record.EgressBindingID,
			RouteID:       record.EgressRouteID,
			IdentityID:    record.EgressIdentityID,
			Lease:         lease,
			ConfigVersion: record.EgressConfigVersion,
		}
		account, err = WithResolvedAccountEgress(account, resolved)
		if err != nil {
			return nil, ErrLiveUnavailable
		}
		ctx = ContextWithSelectedAccountEgress(ctx, account)
	}
	headers, err := s.liveSidebandHeaders(ctx, account, record)
	if err != nil {
		return nil, err
	}
	target := strings.TrimRight(chatGPTLiveSidebandBaseURL, "/") + "/" + url.PathEscape(record.CallID)
	conn, status, _, err := s.getOpenAIWSPassthroughDialer().Dial(ctx, target, headers, resolveAccountProxyURL(account))
	if err != nil {
		return nil, fmt.Errorf("dial live sideband (status %d): %w", status, err)
	}
	raw, ok := conn.(liveFrameConn)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("live sideband transport does not support raw frames")
	}
	return raw, nil
}

func (s *OpenAIGatewayService) GetLiveCallForIdentity(
	ctx context.Context,
	callID string,
	identity LiveCallIdentity,
) (*LiveCallRecord, error) {
	store, err := s.liveStore()
	if err != nil {
		return nil, err
	}
	record, err := store.GetLiveCall(ctx, hashLiveCallID(callID))
	if err != nil {
		return nil, err
	}
	if record.CallID != callID ||
		record.APIKeyID != identity.APIKeyID ||
		record.UserID != identity.UserID ||
		record.GroupID != liveGroupID(identity.GroupID) {
		return nil, ErrLiveIdentityMismatch
	}
	if record.Controller == LiveControllerClosed {
		return nil, ErrLiveCallNotFound
	}
	return record, nil
}

// ProxyLiveSideband 让认证后的客户端接管控制连接；媒体始终不经过这里。
func (s *OpenAIGatewayService) ProxyLiveSideband(
	ctx context.Context,
	record *LiveCallRecord,
	downstream *coderws.Conn,
) error {
	if record == nil || downstream == nil {
		return ErrLiveCallNotFound
	}
	store, err := s.liveStore()
	if err != nil {
		s.abandonUnownedLiveEgressLease(record)
		return err
	}
	owner := uuid.NewString()
	claimed, err := store.ClaimLiveController(ctx, record.CallHash, LiveControllerProxy, owner)
	if err != nil {
		s.abandonUnownedLiveEgressLease(record)
		return err
	}
	if !claimed {
		s.abandonUnownedLiveEgressLease(record)
		return ErrLiveControllerChanged
	}
	_, poolMode, egressErr := liveCallEgressRef(record)
	if egressErr != nil {
		_, _ = store.ReleaseLiveController(context.Background(), record.CallHash, owner)
		s.finalizeLiveCall(record)
		return egressErr
	}
	if poolMode {
		if ownerErr := s.activateLiveEgressOwner(record, owner); ownerErr != nil {
			_, _ = store.ReleaseLiveController(context.Background(), record.CallHash, owner)
			s.finalizeLiveCall(record)
			return ownerErr
		}
		defer s.releaseLiveEgressOwner(record, owner)
	}

	// observer 轮询到接管状态后会关闭旧控制连接；同一个 call 可重新加入。
	time.Sleep(liveObserverPollInterval)
	upstream, err := s.dialLiveSideband(ctx, record)
	if err != nil {
		_, _ = store.ReleaseLiveController(context.Background(), record.CallHash, owner)
		if liveSessionEnded(err) {
			s.finalizeLiveCall(record)
			return err
		}
		go s.observeLiveCall(record)
		return err
	}
	defer func() { _ = upstream.Close() }()
	downstream.SetReadLimit(openAIWSMessageReadLimitBytes)

	proxyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 2)
	go func() {
		for {
			messageType, payload, readErr := downstream.Read(proxyCtx)
			if readErr != nil {
				errCh <- readErr
				return
			}
			if writeErr := upstream.WriteFrame(proxyCtx, messageType, payload); writeErr != nil {
				errCh <- writeErr
				return
			}
		}
	}()
	go func() {
		for {
			messageType, payload, readErr := upstream.ReadFrame(proxyCtx)
			if readErr != nil {
				errCh <- liveSidebandReadError(readErr)
				return
			}
			if writeErr := downstream.Write(proxyCtx, messageType, payload); writeErr != nil {
				errCh <- writeErr
				return
			}
			if messageType == coderws.MessageText {
				eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
				if eventType == "session.closed" || eventType == "session.ended" {
					errCh <- ErrLiveCallNotFound
					return
				}
			}
		}
	}()

	runErr := s.runLiveController(proxyCtx, record, upstream, errCh)
	cancel()
	_, _ = store.ReleaseLiveController(context.Background(), record.CallHash, owner)
	if liveSessionEnded(runErr) || !time.Now().Before(record.ExpiresAt) {
		s.finalizeLiveCall(record)
		return runErr
	}
	go s.observeLiveCall(record)
	return runErr
}

// liveSessionEnded 判断控制连接的退出原因是否意味着会话已终结（应 finalize：写
// usage log 并释放租约），而不是可以交给 observer 重连的临时错误。
//
// ErrLiveUnavailable 在控制循环里只会来自租约续租失败。RefreshLiveLease 的 Lua 在
// leaseID 被 GC 后不会重新写入，重连也拿不回并发槽 —— 若按临时错误重试，会话会以
// 约 1 秒一轮的节奏空转到 ExpiresAt，期间持着上游连接却不计入任何并发限制。
func liveSessionEnded(err error) bool {
	return errors.Is(err, ErrLiveCallNotFound) ||
		errors.Is(err, ErrLiveUnavailable) ||
		errors.Is(err, context.DeadlineExceeded)
}

func (s *OpenAIGatewayService) runLiveController(
	ctx context.Context,
	record *LiveCallRecord,
	upstream liveFrameConn,
	errCh <-chan error,
) error {
	refreshTicker := time.NewTicker(liveLeaseRefreshInterval)
	defer refreshTicker.Stop()
	maxTimer := time.NewTimer(time.Until(record.ExpiresAt))
	defer maxTimer.Stop()
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case err := <-errCh:
			return err
		case <-maxTimer.C:
			closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = upstream.WriteFrame(closeCtx, coderws.MessageText, []byte(`{"type":"session.close"}`))
			cancel()
			return context.DeadlineExceeded
		case <-refreshTicker.C:
			if !s.refreshLiveLease(record) {
				return ErrLiveUnavailable
			}
		}
	}
}

func (s *OpenAIGatewayService) observeLiveCall(record *LiveCallRecord) {
	if record == nil {
		return
	}
	store, err := s.liveStore()
	if err != nil {
		s.abandonUnownedLiveEgressLease(record)
		return
	}
	owner := uuid.NewString()
	claimed, claimErr := store.ClaimLiveController(context.Background(), record.CallHash, LiveControllerObserver, owner)
	if claimErr != nil {
		s.abandonUnownedLiveEgressLease(record)
		// store 报错时无法确认控制权归属，不能静默退出：若 claim 实际已生效而
		// observer 消失，租约与 usage log 都会丢。兜底 finalize 是幂等的，即使
		// 控制权在他人手上也只会在到期后落一次库。
		s.finalizeLiveCallAfterExpiry(record)
		return
	}
	if !claimed {
		s.abandonUnownedLiveEgressLease(record)
		return
	}
	_, poolMode, egressErr := liveCallEgressRef(record)
	if egressErr != nil {
		_, _ = store.ReleaseLiveController(context.Background(), record.CallHash, owner)
		s.finalizeLiveCall(record)
		return
	}
	if poolMode {
		if ownerErr := s.activateLiveEgressOwner(record, owner); ownerErr != nil {
			_, _ = store.ReleaseLiveController(context.Background(), record.CallHash, owner)
			s.finalizeLiveCall(record)
			return
		}
		defer s.releaseLiveEgressOwner(record, owner)
	}
	storeErrStreak := 0
	for {
		latest, getErr := store.GetLiveCall(context.Background(), record.CallHash)
		if getErr != nil {
			// 记录已被清理（closed TTL 到期）不是故障，直接退出。
			if errors.Is(getErr, ErrLiveCallNotFound) {
				return
			}
			// store 抖动不等于控制权被接管：有限次重试；仍失败则按
			// record.ExpiresAt 兜底 finalize，保证 usage log 与租约释放不丢。
			storeErrStreak++
			if storeErrStreak >= liveObserverStoreRetryLimit {
				s.finalizeLiveCallAfterExpiry(record)
				return
			}
			time.Sleep(liveObserverStoreRetryInterval)
			continue
		}
		storeErrStreak = 0
		record = latest
		if record.Controller != LiveControllerObserver {
			return
		}
		if !time.Now().Before(record.ExpiresAt) {
			s.finalizeLiveCall(record)
			return
		}
		upstream, dialErr := s.dialLiveSideband(context.Background(), record)
		if dialErr != nil {
			if liveSessionEnded(dialErr) {
				s.finalizeLiveCall(record)
				return
			}
			if !s.waitForLiveObserverRetry(record) {
				return
			}
			continue
		}
		runErr := s.runLiveObserverConnection(record, upstream)
		_ = upstream.Close()
		if errors.Is(runErr, ErrLiveControllerChanged) {
			return
		}
		if liveSessionEnded(runErr) {
			s.finalizeLiveCall(record)
			return
		}
		if !s.waitForLiveObserverRetry(record) {
			return
		}
	}
}

func (s *OpenAIGatewayService) runLiveObserverConnection(record *LiveCallRecord, upstream liveFrameConn) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	frameCh := make(chan []byte, 1)
	errCh := make(chan error, 1)
	go func() {
		for {
			messageType, payload, err := upstream.ReadFrame(ctx)
			if err != nil {
				select {
				case errCh <- liveSidebandReadError(err):
				case <-ctx.Done():
				}
				return
			}
			if messageType == coderws.MessageText {
				select {
				case frameCh <- payload:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	refreshTicker := time.NewTicker(liveLeaseRefreshInterval)
	defer refreshTicker.Stop()
	controllerTicker := time.NewTicker(liveObserverPollInterval)
	defer controllerTicker.Stop()
	maxTimer := time.NewTimer(time.Until(record.ExpiresAt))
	defer maxTimer.Stop()
	store, _ := s.liveStore()
	for {
		select {
		case payload := <-frameCh:
			eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
			if eventType == "session.closed" || eventType == "session.ended" {
				return ErrLiveCallNotFound
			}
		case err := <-errCh:
			return err
		case <-controllerTicker.C:
			controller, err := store.GetLiveController(context.Background(), record.CallHash)
			if err != nil {
				return err
			}
			if controller != LiveControllerObserver {
				return ErrLiveControllerChanged
			}
		case <-refreshTicker.C:
			if !s.refreshLiveLease(record) {
				return ErrLiveUnavailable
			}
		case <-maxTimer.C:
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = upstream.WriteFrame(closeCtx, coderws.MessageText, []byte(`{"type":"session.close"}`))
			closeCancel()
			return context.DeadlineExceeded
		}
	}
}

func (s *OpenAIGatewayService) waitForLiveObserverRetry(record *LiveCallRecord) bool {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	<-timer.C
	store, err := s.liveStore()
	if err != nil {
		return false
	}
	controller, getErr := store.GetLiveController(context.Background(), record.CallHash)
	if getErr != nil && !errors.Is(getErr, ErrLiveCallNotFound) {
		// store 报错不等于控制权被接管：返回 true 交回 observeLiveCall 循环顶部，
		// 由它对 store 故障做有限次重试与 ExpiresAt 兜底 finalize。在这里返回
		// false 会让 Redis 抖动时会话静默结束、不留记录。
		return true
	}
	// 过期不在此处判定：返回 true 让调用方回到循环顶部的过期分支，由它 finalize
	// （写 usage log + 释放租约）。在这里直接返回 false 会让会话静默结束、不留记录。
	return getErr == nil && controller == LiveControllerObserver
}

// finalizeLiveCallAfterExpiry 是 store 持续报错、observer 无法继续观察时的兜底：
// 等到会话最长时限 ExpiresAt 再 finalize，保证 usage log 与租约释放最迟在会话到期
// 时完成。MarkLiveCallClosed 的 first 语义保证与其他恢复路径不会重复落库。
func (s *OpenAIGatewayService) finalizeLiveCallAfterExpiry(record *LiveCallRecord) {
	if record == nil {
		return
	}
	if wait := time.Until(record.ExpiresAt); wait > 0 {
		time.Sleep(wait)
	}
	s.finalizeLiveCall(record)
}

func (s *OpenAIGatewayService) refreshLiveLease(record *LiveCallRecord) bool {
	ref, poolMode, err := liveCallEgressRef(record)
	if err != nil {
		return false
	}
	if poolMode {
		lease, leaseErr := s.ensureLiveEgressLease(record)
		if leaseErr != nil || !accountEgressLeaseMatchesRef(lease, ref) {
			return false
		}
		egressCtx, egressCancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
		egressErr := lease.RefreshWithinSafetyWindow(egressCtx)
		egressCancel()
		if egressErr != nil {
			return false
		}
		cache, cacheErr := s.liveEgressConcurrencyCache()
		if cacheErr != nil {
			return false
		}
		liveCtx, liveCancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
		refreshed, refreshErr := cache.RefreshLiveLeaseForEgress(
			liveCtx,
			ref,
			record.UserID,
			record.APIKeyID,
			record.LeaseID,
		)
		liveCancel()
		if refreshErr == nil && refreshed {
			s.markLiveEgressLeaseConfirmed(record, lease)
			return true
		}
		if refreshErr != nil {
			return s.liveEgressLeaseInsideSafetyWindow(record, lease)
		}
		return false
	}

	cache, cacheErr := s.liveConcurrencyCache()
	if cacheErr != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	refreshed, refreshErr := cache.RefreshLiveLease(ctx, record.AccountID, record.UserID, record.APIKeyID, record.LeaseID)
	cancel()
	return refreshErr == nil && refreshed
}

func (s *OpenAIGatewayService) releaseLiveConcurrencyLease(record *LiveCallRecord) {
	if record == nil {
		return
	}
	ref, poolMode, refErr := liveCallEgressRef(record)
	if refErr == nil && poolMode {
		if cache, err := s.liveEgressConcurrencyCache(); err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
			_ = cache.ReleaseLiveLeaseForEgress(ctx, ref, record.UserID, record.APIKeyID, record.LeaseID)
			cancel()
			return
		}
	}
	cache, err := s.liveConcurrencyCache()
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	_ = cache.ReleaseLiveLease(ctx, record.AccountID, record.UserID, record.APIKeyID, record.LeaseID)
	cancel()
}

func (s *OpenAIGatewayService) releaseLiveEgressLease(record *LiveCallRecord) {
	if record == nil || s == nil {
		return
	}
	var lease *AccountEgressLease
	s.liveEgressLeaseMu.Lock()
	if state := s.liveEgressLeases[record.CallHash]; state != nil {
		lease = state.lease
		delete(s.liveEgressLeases, record.CallHash)
	}
	if lease == nil {
		lease = record.EgressLease
	}
	record.EgressLease = nil
	s.liveEgressLeaseMu.Unlock()
	if lease != nil {
		lease.Release()
		return
	}

	ref, poolMode, err := liveCallEgressRef(record)
	if err != nil || !poolMode {
		return
	}
	if s.concurrencyService == nil || s.concurrencyService.accountEgressAllocator == nil ||
		s.concurrencyService.accountEgressAllocator.cache == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	_ = s.concurrencyService.accountEgressAllocator.cache.ReleaseAccountEgressLease(ctx, ref)
	cancel()
}

func (s *OpenAIGatewayService) releaseLiveCallLeases(record *LiveCallRecord) {
	s.releaseLiveConcurrencyLease(record)
	s.releaseLiveEgressLease(record)
}

func (s *OpenAIGatewayService) finalizeLiveCall(record *LiveCallRecord) {
	if record == nil {
		return
	}
	// Resource cleanup is independent from the closed-record idempotency gate.
	// Even when Redis applied MarkLiveCallClosed but its response was lost, this
	// process must stop renewing and make both releases; all Redis removals are
	// exact-ID and idempotent.
	s.releaseLiveCallLeases(record)
	store, err := s.liveStore()
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), liveRedisOperationTimeout)
	first, err := store.MarkLiveCallClosed(ctx, record.CallHash, liveClosedRecordTTL)
	cancel()
	if err != nil || !first {
		return
	}
	if s.usageLogRepo == nil {
		return
	}
	duration := int(time.Since(record.CreatedAt).Milliseconds())
	if duration < 0 {
		duration = 0
	}
	inboundEndpoint := record.InboundEndpoint
	upstreamEndpoint := "/backend-api/codex/realtime/calls"
	userAgent := record.UserAgent
	ipAddress := record.IPAddress
	billingType := int8(BillingTypeBalance)
	if record.SubscriptionID > 0 {
		billingType = BillingTypeSubscription
	}
	// TODO(billing): Live 会话目前不计费：TotalCost/ActualCost 恒为 0，完全绕过
	// recordUsageCore/applyUsageBilling，余额模式下极低余额也能反复开启最长
	// liveMaxSessionDuration 的会话。若确认按时长计费，应在此接入计费管道；
	// 若确认有意免费，删除本注释即可（零值行为由
	// TestFinalizeLiveCallIsIdempotentAndWritesZeroUsage 锁定）。
	//
	// 这是该会话唯一一次落库机会（MarkLiveCallClosed 已标记 first），失败即永久
	// 丢失，因此走带日志与同步兜底的 writeUsageLogBestEffort（issue #3656）。
	writeUsageLogBestEffort(context.Background(), s.usageLogRepo, &UsageLog{
		UserID:           record.UserID,
		APIKeyID:         record.APIKeyID,
		AccountID:        record.AccountID,
		RequestID:        record.CallHash,
		Model:            record.Model,
		RequestedModel:   record.Model,
		GroupID:          liveOptionalID(record.GroupID),
		SubscriptionID:   liveOptionalID(record.SubscriptionID),
		RateMultiplier:   1,
		BillingType:      billingType,
		RequestType:      RequestTypeLive,
		DurationMs:       &duration,
		UserAgent:        &userAgent,
		IPAddress:        &ipAddress,
		InboundEndpoint:  &inboundEndpoint,
		UpstreamEndpoint: &upstreamEndpoint,
		CreatedAt:        record.CreatedAt,
	}, "service.openai_live")
}
