package service

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

const (
	LiveControllerPending  = "pending"
	LiveControllerObserver = "observer"
	LiveControllerProxy    = "proxy"
	LiveControllerClosed   = "closed"
	// LiveConcurrencyLeaseTTL is shared with the Redis implementation so the
	// controller's transient-error safety window cannot outlive its slots.
	LiveConcurrencyLeaseTTL = time.Minute
)

var (
	ErrLiveUnavailable       = errors.New("live is unavailable")
	ErrLiveConcurrencyFull   = errors.New("live concurrency is full")
	ErrLiveCallNotFound      = errors.New("live call not found")
	ErrLiveIdentityMismatch  = errors.New("live call identity mismatch")
	ErrLiveControllerChanged = errors.New("live controller changed")
)

type LiveAttestationUnavailableError struct {
	Reason string
}

func (e *LiveAttestationUnavailableError) Error() string {
	if e == nil || e.Reason == "" {
		return "Live attestation is unavailable"
	}
	return "Live attestation is unavailable: " + e.Reason
}

// LiveCallRequest 是两个下游创建协议归一后的请求。Session 不做结构改写。
type LiveCallRequest struct {
	SDP     string          `json:"sdp"`
	Session json.RawMessage `json:"session"`
}

type LiveCallIdentity struct {
	APIKeyID        int64
	UserID          int64
	GroupID         *int64
	SubscriptionID  *int64
	UserAgent       string
	IPAddress       string
	InboundEndpoint string
}

type LiveCallRecord struct {
	CallID         string
	CallHash       string
	AccountID      int64
	APIKeyID       int64
	UserID         int64
	GroupID        int64
	SubscriptionID int64
	LeaseID        string
	// Egress fields are persisted so an observer in another process can restore
	// the exact public route instead of selecting the account's current route.
	EgressBindingID     string
	EgressLeaseID       string
	EgressRouteID       int64
	EgressIdentityID    string
	EgressConfigVersion int64
	EgressLease         *AccountEgressLease `json:"-"`
	Model               string
	CreatedAt           time.Time
	ExpiresAt           time.Time
	Controller          string
	ControllerOwner     string
	UserAgent           string
	IPAddress           string
	InboundEndpoint     string
	// AttestationCiphertext 仅用于让同一会话的 Sideband 复用创建时的证明。
	AttestationCiphertext string
}

type LiveCallCreated struct {
	SDP      []byte
	CallID   string
	Location string
	Account  *Account
}

// LiveCallStore 由 GatewayCache 的 Redis 实现可选提供，避免扩大旧缓存接口。
type LiveCallStore interface {
	SaveLiveCall(ctx context.Context, record *LiveCallRecord, ttl time.Duration) error
	GetLiveCall(ctx context.Context, callHash string) (*LiveCallRecord, error)
	ClaimLiveController(ctx context.Context, callHash, controller, owner string) (bool, error)
	ReleaseLiveController(ctx context.Context, callHash, owner string) (bool, error)
	GetLiveController(ctx context.Context, callHash string) (string, error)
	MarkLiveCallClosed(ctx context.Context, callHash string, ttl time.Duration) (bool, error)
}

type LiveConcurrencyCache interface {
	AcquireLiveLease(
		ctx context.Context,
		accountID int64,
		accountMax int,
		userID int64,
		userMax int,
		apiKeyID int64,
		leaseID string,
		replacingRegularSlots bool,
	) (bool, error)
	RefreshLiveLease(ctx context.Context, accountID, userID, apiKeyID int64, leaseID string) (bool, error)
	ReleaseLiveLease(ctx context.Context, accountID, userID, apiKeyID int64, leaseID string) error
}

// LiveEgressConcurrencyCache is the pool-mode companion to LiveConcurrencyCache.
// The account capacity is already represented by AccountEgressLease; this
// capability atomically reserves only user/API-key live slots while fencing the
// reservation against that exact egress lease.
type LiveEgressConcurrencyCache interface {
	AcquireLiveLeaseForEgress(
		ctx context.Context,
		egress AccountEgressLeaseRef,
		userID int64,
		userMax int,
		apiKeyID int64,
		leaseID string,
		replacingRegularSlots bool,
	) (bool, error)
	RefreshLiveLeaseForEgress(
		ctx context.Context,
		egress AccountEgressLeaseRef,
		userID int64,
		apiKeyID int64,
		leaseID string,
	) (bool, error)
	ReleaseLiveLeaseForEgress(
		ctx context.Context,
		egress AccountEgressLeaseRef,
		userID int64,
		apiKeyID int64,
		leaseID string,
	) error
}
