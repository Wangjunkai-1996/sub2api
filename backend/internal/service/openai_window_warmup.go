package service

// This file contains the domain port and orchestration logic for the OpenAI
// Codex five-hour window warmup.  It deliberately has no Gin, Ent, or plugin
// dependencies: HTTP/TLS and credential recovery are supplied by the outbound
// adapter, while durable state and correctness are supplied by the repository.

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/google/uuid"
)

const (
	// OpenAICodexWarmupPolicyExtraKey is the canonical account configuration key.
	// The two aliases are read for backwards compatibility with early clients.
	OpenAICodexWarmupPolicyExtraKey  = "openai_codex_warmup_policy"
	CodexWarmupPolicyExtraKey        = "codex_warmup_policy"
	OpenAIWindowWarmupPolicyExtraKey = "openai_window_warmup_policy"

	OpenAIWindowWarmupQuotaScopeGlobal = "global"

	OpenAIWindowWarmupPolicyOff         = "off"
	OpenAIWindowWarmupPolicyInitialOnce = "initial_once"
	OpenAIWindowWarmupPolicyContinuous  = "continuous"

	OpenAIWindowWarmupStatePending       = "pending"
	OpenAIWindowWarmupStateArmed         = "armed"
	OpenAIWindowWarmupStateDue           = "due"
	OpenAIWindowWarmupStateRunning       = "running"
	OpenAIWindowWarmupStateRetrying      = "retrying"
	OpenAIWindowWarmupStateUncertain     = "uncertain"
	OpenAIWindowWarmupStatePossiblySent  = "possibly_sent"
	OpenAIWindowWarmupStatePaused        = "paused"
	OpenAIWindowWarmupStateBlocked       = "blocked"
	OpenAIWindowWarmupStateBlockedConfig = "blocked_config"
	OpenAIWindowWarmupStateFailed        = "failed"
	OpenAIWindowWarmupStateCompleted     = "completed"

	OpenAIWindowWarmupErrorFiveHourWindowUnsupported = "five_hour_window_unsupported"

	OpenAIWindowWarmupTriggerImport    = "import"
	OpenAIWindowWarmupTriggerReset     = "reset"
	OpenAIWindowWarmupTriggerReconcile = "reconcile"
	OpenAIWindowWarmupTriggerManual    = "manual"
	AuditActionOpenAIWindowWarmup      = "system.openai.window_warmup"

	// The defaults are intentionally conservative.  They can be overridden by
	// deployment configuration without changing the state machine.
	openAIWindowWarmupDefaultScanInterval          = 30 * time.Second
	openAIWindowWarmupDefaultLease                 = 120 * time.Second
	openAIWindowWarmupDefaultGrace                 = 90 * time.Second
	openAIWindowWarmupDefaultJitter                = 30 * time.Second
	openAIWindowWarmupDefaultTimeout               = 45 * time.Second
	openAIWindowWarmupDefaultBatch                 = 20
	openAIWindowWarmupDefaultMaxAttempts           = 8
	openAIWindowWarmupClaimInterval                = 5 * time.Second
	openAIWindowWarmupReconcileInterval            = 10 * time.Minute
	openAIWindowWarmupUncertainObservationInterval = time.Minute
	openAIWindowWarmupResetStabilityTolerance      = 5 * time.Second
)

// Public aliases make the API pleasant for callers that use the shorter
// WarmupPolicy spelling while keeping the canonical constants discoverable.
const (
	OpenAIWindowWarmupPolicyOnce = OpenAIWindowWarmupPolicyInitialOnce
	OpenAIWindowWarmupPolicyOn   = OpenAIWindowWarmupPolicyContinuous
)

// OpenAIWindowWarmupPolicy is persisted as a small, validated string.
type OpenAIWindowWarmupPolicy string

func NormalizeOpenAIWindowWarmupPolicy(raw string) OpenAIWindowWarmupPolicy {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case OpenAIWindowWarmupPolicyInitialOnce:
		return OpenAIWindowWarmupPolicyInitialOnce
	case OpenAIWindowWarmupPolicyContinuous:
		return OpenAIWindowWarmupPolicyContinuous
	default:
		return OpenAIWindowWarmupPolicyOff
	}
}

func (p OpenAIWindowWarmupPolicy) Enabled() bool {
	return p == OpenAIWindowWarmupPolicyInitialOnce || p == OpenAIWindowWarmupPolicyContinuous
}

// OpenAIWindowWarmupPolicyForAccount reads the canonical key first, then the
// aliases.  Invalid or missing values fail closed to off.
func OpenAIWindowWarmupPolicyForAccount(account *Account) OpenAIWindowWarmupPolicy {
	if account == nil || account.Extra == nil {
		return OpenAIWindowWarmupPolicyOff
	}
	for _, key := range []string{
		OpenAICodexWarmupPolicyExtraKey,
		CodexWarmupPolicyExtraKey,
		OpenAIWindowWarmupPolicyExtraKey,
	} {
		if value, ok := account.Extra[key]; ok {
			if raw, ok := value.(string); ok && strings.TrimSpace(raw) != "" {
				return NormalizeOpenAIWindowWarmupPolicy(raw)
			}
		}
	}
	return OpenAIWindowWarmupPolicyOff
}

// SetOpenAIWindowWarmupPolicy updates only the strategy field in Extra.  The
// caller persists the returned map through AccountRepository.UpdateExtra.
func SetOpenAIWindowWarmupPolicy(account *Account, policy OpenAIWindowWarmupPolicy) map[string]any {
	updates := map[string]any{OpenAICodexWarmupPolicyExtraKey: string(NormalizeOpenAIWindowWarmupPolicy(string(policy)))}
	if account != nil && account.Extra != nil {
		// Preserve aliases in memory for older UI clients, but only the canonical
		// key is written by normal service paths.
		account.Extra[OpenAICodexWarmupPolicyExtraKey] = updates[OpenAICodexWarmupPolicyExtraKey]
	}
	return updates
}

// OpenAIOutboundRequest is the narrow request contract between the warmup
// service and an OpenAI transport adapter.  Payload is a fixed minimal
// Responses JSON body; callers must never put credentials or user content in it.
type OpenAIOutboundRequest struct {
	Account            *Account
	IdentityGeneration int64
	SendGuard          OpenAIWindowWarmupSendGuard
	Model              string
	Payload            []byte
	Headers            http.Header
	Timeout            time.Duration
	Endpoint           string
}

type OpenAIWindowWarmupSendGuard interface {
	Check(context.Context, int64) error
}

type OpenAIWindowWarmupSendGuardFunc func(context.Context, int64) error

func (f OpenAIWindowWarmupSendGuardFunc) Check(ctx context.Context, accountID int64) error {
	if f == nil {
		return nil
	}
	return f(ctx, accountID)
}

// OpenAIWindowWarmupIdentityLeaseRepository linearizes a guarded outbound POST
// with durable identity, policy, and business-use updates. The lease must hold a
// database row lock until the transport returns so a conflicting update either
// completes first and rejects the send, or waits for the existing send to finish.
type OpenAIWindowWarmupIdentityLeaseRepository interface {
	AcquireOpenAIWindowWarmupIdentityLease(
		context.Context,
		int64,
		int64,
		OpenAIWindowWarmupPolicy,
		*time.Time,
		map[string]any,
		*int64,
		*Proxy,
	) (release func(), acquired bool, err error)
}

type openAIWindowWarmupSendGuardContextKey struct{}

func withOpenAIWindowWarmupSendGuard(ctx context.Context, guard OpenAIWindowWarmupSendGuard) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, openAIWindowWarmupSendGuardContextKey{}, guard)
}

func openAIWindowWarmupSendGuardFromContext(ctx context.Context) OpenAIWindowWarmupSendGuard {
	if ctx == nil {
		return nil
	}
	guard, _ := ctx.Value(openAIWindowWarmupSendGuardContextKey{}).(OpenAIWindowWarmupSendGuard)
	return guard
}

// OpenAIOutboundResult is intentionally bounded and metadata-only.  Body may
// contain a short in-memory SSE sample for parsing, but is never persisted.
type OpenAIOutboundResult struct {
	StatusCode   int
	Headers      http.Header
	Body         []byte
	Terminal     bool
	TerminalType string
	ResetAt      *time.Time
	Started      bool
	EOF          bool
	RequestID    string
	AuthFailure  *OpenAIWindowWarmupAuthFailure
}

// OpenAIWindowWarmupTokenUsage is the bounded metering evidence emitted by a
// completed Responses request. It intentionally excludes token details and
// response content.
type OpenAIWindowWarmupTokenUsage struct {
	InputTokens  int64
	OutputTokens int64
	TotalTokens  int64
}

// Positive reports whether the complete token triplet is internally
// consistent and proves that upstream accounted for this request.
func (u *OpenAIWindowWarmupTokenUsage) Positive() bool {
	if u == nil || u.InputTokens <= 0 || u.OutputTokens < 0 || u.TotalTokens <= 0 {
		return false
	}
	if u.InputTokens > math.MaxInt64-u.OutputTokens {
		return false
	}
	return u.InputTokens+u.OutputTokens == u.TotalTokens
}

// OpenAIWindowWarmupAuthDisposition is bounded metadata describing how an
// authentication failure behaved. It never contains credential or response
// content and lets the account-state adapter reuse existing 401/403 policy
// without logging a warmup response body.
type OpenAIWindowWarmupAuthDisposition string

const (
	OpenAIWindowWarmupAuthNotRefreshable    OpenAIWindowWarmupAuthDisposition = "not_refreshable"
	OpenAIWindowWarmupAuthReplayRejected    OpenAIWindowWarmupAuthDisposition = "replay_rejected"
	OpenAIWindowWarmupAuthRefreshTerminal   OpenAIWindowWarmupAuthDisposition = "refresh_terminal"
	OpenAIWindowWarmupAuthRefreshTransient  OpenAIWindowWarmupAuthDisposition = "refresh_transient"
	OpenAIWindowWarmupAuthRefreshInProgress OpenAIWindowWarmupAuthDisposition = "refresh_in_progress"
	OpenAIWindowWarmupAuthForbiddenHTML     OpenAIWindowWarmupAuthDisposition = "forbidden_html"
	OpenAIWindowWarmupAuthForbidden         OpenAIWindowWarmupAuthDisposition = "forbidden"
)

type OpenAIWindowWarmupAuthFailure struct {
	AccountID           int64
	StatusCode          int
	Disposition         OpenAIWindowWarmupAuthDisposition
	ExpectedCredentials map[string]any `json:"-"`
}

type openAIWindowWarmupAuthError struct {
	err     error
	failure *OpenAIWindowWarmupAuthFailure
}

func (e *openAIWindowWarmupAuthError) Error() string { return e.err.Error() }
func (e *openAIWindowWarmupAuthError) Unwrap() error { return e.err }

func withOpenAIWindowWarmupAuthFailure(err error, failure *OpenAIWindowWarmupAuthFailure) error {
	if err == nil || failure == nil {
		return err
	}
	return &openAIWindowWarmupAuthError{err: err, failure: cloneOpenAIWindowWarmupAuthFailure(failure)}
}

func openAIWindowWarmupAuthFailureFromError(err error) *OpenAIWindowWarmupAuthFailure {
	var authErr *openAIWindowWarmupAuthError
	if !errors.As(err, &authErr) || authErr == nil {
		return nil
	}
	return cloneOpenAIWindowWarmupAuthFailure(authErr.failure)
}

func cloneOpenAIWindowWarmupAuthFailure(failure *OpenAIWindowWarmupAuthFailure) *OpenAIWindowWarmupAuthFailure {
	if failure == nil {
		return nil
	}
	cloned := *failure
	cloned.ExpectedCredentials = shallowCopyMap(failure.ExpectedCredentials)
	return &cloned
}

// OpenAIOutboundExecutor is implemented by the built-in HTTP/TLS adapter and
// by the optional OpenAI OAuth transport plugin bridge.
type OpenAIOutboundExecutor interface {
	Execute(context.Context, OpenAIOutboundRequest) (*OpenAIOutboundResult, error)
}

// OpenAIWindowProbeResult is the probe's sanitized result.  ResetAt must come
// from upstream headers/SSE/usage; the warmup service never derives it by
// adding five hours to local time.
type OpenAIWindowProbeResult struct {
	StatusCode      int
	Headers         http.Header
	Body            []byte
	Terminal        bool
	TerminalType    string
	ResetAt         *time.Time
	ObservedResetAt *time.Time
	Started         bool
	EOF             bool
	Outcome         string
	AuthFailure     *OpenAIWindowWarmupAuthFailure
	Usage           *OpenAIWindowWarmupTokenUsage
	ResponseID      string
	RequestID       string
	// resetFromRelativeHeader distinguishes x-codex reset-after evidence from an
	// explicit absolute reset carried by a response event. An idle now+5h
	// projection naturally moves forward while the request is in flight.
	resetFromRelativeHeader bool
}

// OpenAIWindowProbe is the probe port consumed by the service.  A concrete
// OpenAICodexWindowProbe lives in the adapter file and can transparently use
// Agent Identity, OAuth refresh, proxy and plugin transport paths.
type OpenAIWindowProbe interface {
	Probe(context.Context, *Account, *time.Time) (*OpenAIWindowProbeResult, error)
}

// OpenAIWindowWarmupUsageReconciler is used after timeout/EOF/ambiguous 2xx to
// query authoritative usage before allowing another request.
type OpenAIWindowWarmupUsageReconciler interface {
	QueryUsage(context.Context, int64) (*OpenAIQuotaUsage, error)
}

type OpenAIWindowWarmupUsageReconcilerFunc func(context.Context, int64) (*OpenAIQuotaUsage, error)

func (f OpenAIWindowWarmupUsageReconcilerFunc) QueryUsage(ctx context.Context, accountID int64) (*OpenAIQuotaUsage, error) {
	if f == nil {
		return nil, errors.New("warmup usage reconciler is unavailable")
	}
	return f(ctx, accountID)
}

type OpenAIWindowWarmupAuthFailureHandler interface {
	HandleOpenAIWindowWarmupAuthFailure(context.Context, OpenAIWindowWarmupAuthFailure) error
}

// OpenAIWindowWarmupKillSwitch is deliberately tiny so settings can be read
// through a cached service and tests can inject an atomic switch.
type OpenAIWindowWarmupKillSwitch interface {
	Enabled(context.Context) (bool, error)
}

type OpenAIWindowWarmupKillSwitchFunc func(context.Context) (bool, error)

func (f OpenAIWindowWarmupKillSwitchFunc) Enabled(ctx context.Context) (bool, error) {
	if f == nil {
		return true, nil
	}
	return f(ctx)
}

// OpenAIWindowWarmupAllowlist is read on every scan. An empty list selects all
// otherwise eligible accounts; a non-empty list narrows that set. Read errors
// still fail closed.
type OpenAIWindowWarmupAllowlist interface {
	AccountIDs(context.Context) ([]int64, error)
}

type OpenAIWindowWarmupAllowlistFunc func(context.Context) ([]int64, error)

func (f OpenAIWindowWarmupAllowlistFunc) AccountIDs(ctx context.Context) ([]int64, error) {
	if f == nil {
		return nil, nil
	}
	return f(ctx)
}

// OpenAIWindowWarmupJob is the durable state projection returned to admin/UI.
// Error fields are deliberately redacted codes/messages only.
type OpenAIWindowWarmupJob struct {
	ID                 int64      `json:"id"`
	AccountID          int64      `json:"account_id"`
	QuotaScope         string     `json:"quota_scope"`
	State              string     `json:"state"`
	Trigger            string     `json:"trigger"`
	CycleKey           string     `json:"cycle_key"`
	CycleGeneration    int64      `json:"cycle_generation"`
	IdentityGeneration int64      `json:"-"`
	ObservedResetAt    *time.Time `json:"observed_reset_at,omitempty"`
	// Preflight* records the authoritative five-hour observation immediately
	// before the current/last synthetic send. It is attempt evidence rather than
	// the cycle baseline exposed to operators.
	PreflightResetAt    *time.Time `json:"-"`
	PreflightObservedAt *time.Time `json:"-"`
	// UncertainObserved* is internal durable evidence used to distinguish a
	// fixed active reset from /wham/usage's idle rolling now+5h projection.
	UncertainObservedResetAt  *time.Time `json:"-"`
	UncertainObservedAt       *time.Time `json:"-"`
	UncertainTerminalObserved bool       `json:"-"`
	NextAttemptAt             time.Time  `json:"next_attempt_at"`
	AttemptCount              int        `json:"attempt_count"`
	SentAt                    *time.Time `json:"sent_at,omitempty"`
	LeaseOwner                string     `json:"-"`
	LeaseToken                string     `json:"-"`
	LeaseUntil                *time.Time `json:"-"`
	LastAttemptAt             *time.Time `json:"last_attempt_at,omitempty"`
	LastSuccessAt             *time.Time `json:"last_success_at,omitempty"`
	StatusCode                *int       `json:"status_code,omitempty"`
	LastErrorCode             string     `json:"last_error_code,omitempty"`
	LastError                 string     `json:"last_error,omitempty"`
	CreatedAt                 time.Time  `json:"created_at"`
	UpdatedAt                 time.Time  `json:"updated_at"`
}

// OpenAIWindowWarmupEnqueue describes one idempotent cycle insertion.
type OpenAIWindowWarmupEnqueue struct {
	AccountID          int64
	QuotaScope         string
	CycleKey           string
	CycleGeneration    int64
	IdentityGeneration int64
	Trigger            string
	ObservedResetAt    *time.Time
	NextAttemptAt      time.Time
}

// OpenAIWindowWarmupClaim is returned by a repository after an atomic lease
// claim.  The token is a random fencing value and must accompany every write.
type OpenAIWindowWarmupClaim struct {
	Job        *OpenAIWindowWarmupJob
	Owner      string
	LeaseToken string
	LeaseUntil time.Time
	// PreviousState is the state atomically observed before ClaimDue changed it
	// to running. It lets a lease takeover distinguish an interrupted send from
	// a reconciled uncertain job without trusting process-local memory.
	PreviousState      string
	PreviousProjection OpenAIWindowWarmupProjectionSnapshot
}

// OpenAIWindowWarmupProjectionSnapshot is the job evidence changed by
// MarkStarted and restored if its local reservation is cancelled before the
// outbound transport begins.
type OpenAIWindowWarmupProjectionSnapshot struct {
	AttemptCount              int
	SentAt                    *time.Time
	LastAttemptAt             *time.Time
	PreflightResetAt          *time.Time
	PreflightObservedAt       *time.Time
	UncertainObservedResetAt  *time.Time
	UncertainObservedAt       *time.Time
	UncertainTerminalObserved bool
}

// OpenAIWindowWarmupSendReservation identifies the exact local send
// reservation made by MarkStarted. The previous projection is captured by
// ClaimDue under the job row lock and is restored only when the same attempt
// is still the current fenced started row.
type OpenAIWindowWarmupSendReservation struct {
	PreviousProjection OpenAIWindowWarmupProjectionSnapshot
	StartedAt          time.Time
}

// OpenAIWindowWarmupStartEvidence is the final passive usage observation that
// authorizes MarkStarted. The repository combines it with the durable account
// row so a business request racing the service cannot be hidden by a stale
// in-memory snapshot.
type OpenAIWindowWarmupStartEvidence struct {
	Authoritative bool
	UsedPercent   float64
	ResetAt       *time.Time
}

// OpenAIWindowWarmupUncertainEvidence records one authoritative passive usage
// observation after a request with an ambiguous outcome. ResetAt may be nil;
// Authoritative distinguishes that evidence from a failed observation.
type OpenAIWindowWarmupUncertainEvidence struct {
	Authoritative bool
	ResetAt       *time.Time
	Terminal      bool
}

// openAIWindowWarmupSuccessEvidence is the only response-derived metadata
// emitted in the structured success log. It never contains body text, prompt
// content, headers, or credentials.
type openAIWindowWarmupSuccessEvidence struct {
	Usage      *OpenAIWindowWarmupTokenUsage
	ResponseID string
	RequestID  string
}

// OpenAIWindowWarmupAuthStateRetry identifies one completed blocked transition
// that may be rearmed when its account-state side effect failed. The evidence
// is deliberately credential-free and lets the repository reject stale owners,
// later cycles, later attempts, and administrator state changes.
type OpenAIWindowWarmupAuthStateRetry struct {
	JobID           int64
	CycleGeneration int64
	AttemptCount    int
	BlockedState    string
	StatusCode      int
	ErrorCode       string
	RetryCode       string
	NextAttemptAt   time.Time
}

// OpenAIWindowWarmupRepository is the durable port. Implementations must use
// PostgreSQL's DB clock and SELECT ... FOR UPDATE SKIP LOCKED in ClaimDue.
type OpenAIWindowWarmupRepository interface {
	Enqueue(context.Context, OpenAIWindowWarmupEnqueue) (*OpenAIWindowWarmupJob, bool, error)
	ClaimDue(context.Context, string, time.Duration, int, []int64) ([]OpenAIWindowWarmupClaim, error)
	QueueStats(context.Context, []int64) (OpenAIWindowWarmupQueueStats, error)
	CleanupExpiredAttempts(context.Context, int) (int64, error)
	CleanupSupersededTerminalJobs(context.Context, int) (int64, error)
	ReserveGlobalSend(context.Context, time.Duration, time.Duration) (string, bool, error)
	ReleaseGlobalSend(context.Context, string) (bool, error)
	RenewLease(context.Context, int64, string, string, time.Duration) (bool, error)
	MarkStarted(context.Context, int64, string, string, time.Time, OpenAIWindowWarmupStartEvidence) (bool, error)
	CancelStartedBeforeSend(context.Context, int64, string, string, OpenAIWindowWarmupSendReservation, time.Time, string) (bool, error)
	MarkSuccess(context.Context, int64, string, string, time.Time, *time.Time, int, string) (bool, error)
	ProjectSuccessReset(context.Context, int64, int64, time.Time, time.Time) (bool, error)
	MarkSuppressed(context.Context, int64, string, string, time.Time, *time.Time, string) (bool, error)
	MarkRetry(context.Context, int64, string, string, time.Time, time.Time, int, string, string) (bool, error)
	MarkObservationFailure(context.Context, int64, string, string, time.Time, time.Time, string, int, string, string) (bool, error)
	MarkRateLimited(context.Context, int64, string, string, time.Time, time.Time, *time.Time, int, string) (bool, error)
	MarkUncertain(context.Context, int64, string, string, time.Time, time.Time, int, string, string, OpenAIWindowWarmupUncertainEvidence) (bool, error)
	MarkBlocked(context.Context, int64, string, string, time.Time, int, string, string) (bool, error)
	MarkUnsupportedFiveHourWindow(context.Context, int64, string, string, time.Time) (bool, error)
	ClearUnsupportedFiveHourWindow(context.Context, int64, int64) (bool, error)
	RequeueLegacyPreflightLimit(context.Context, int64, int64, int, time.Time) (bool, error)
	RequeueAuthStateUpdateFailure(context.Context, OpenAIWindowWarmupAuthStateRetry) (bool, error)
	MarkPaused(context.Context, int64, string, string, time.Time, string) (bool, error)
	Reschedule(context.Context, int64, string, string, time.Time, string, *time.Time) (bool, error)
	GetByID(context.Context, int64) (*OpenAIWindowWarmupJob, error)
	GetCurrent(context.Context, int64, string) (*OpenAIWindowWarmupJob, error)
	GetCurrentForAccounts(context.Context, []int64, string) (map[int64]*OpenAIWindowWarmupJob, error)
	ListPage(context.Context, OpenAIWindowWarmupListOptions) ([]*OpenAIWindowWarmupJob, int64, error)
	UnblockAccount(context.Context, int64, time.Time, *time.Time) (*OpenAIWindowWarmupJob, bool, error)
}

type OpenAIWindowWarmupListOptions struct {
	AccountID int64
	States    []string
	Limit     int
	Offset    int
}

// OpenAIWindowWarmupQueueStats is computed with PostgreSQL's clock so gauges
// remain meaningful across app instances and hosts with clock skew.
type OpenAIWindowWarmupQueueStats struct {
	Enqueued            int64
	Due                 int64
	OldestDueAgeSeconds int64
	Inflight            int64
	ResetLagSeconds     int64
}

// OpenAIWindowWarmupMetrics is process-local observability. Labels intentionally
// exclude account IDs.
type OpenAIWindowWarmupMetrics struct {
	Enqueued, Started, Success, Failed, Retry, Uncertain      int64
	RealTrafficSuppressed, Due, Inflight, DuplicateSuppressed int64
	OldestDueAgeSeconds, ResetLagSeconds                      int64
}

type openAIWindowWarmupMetricCounters struct {
	enqueued     atomic.Int64
	started      atomic.Int64
	success      atomic.Int64
	failed       atomic.Int64
	retry        atomic.Int64
	uncertain    atomic.Int64
	suppressed   atomic.Int64
	due          atomic.Int64
	inflight     atomic.Int64
	duplicates   atomic.Int64
	oldestDueAge atomic.Int64
	resetLag     atomic.Int64
}

func (m *openAIWindowWarmupMetricCounters) Snapshot() OpenAIWindowWarmupMetrics {
	if m == nil {
		return OpenAIWindowWarmupMetrics{}
	}
	return OpenAIWindowWarmupMetrics{
		Enqueued: m.enqueued.Load(), Started: m.started.Load(), Success: m.success.Load(),
		Failed: m.failed.Load(), Retry: m.retry.Load(), Uncertain: m.uncertain.Load(),
		RealTrafficSuppressed: m.suppressed.Load(), Due: m.due.Load(), Inflight: m.inflight.Load(),
		DuplicateSuppressed: m.duplicates.Load(), OldestDueAgeSeconds: m.oldestDueAge.Load(),
		ResetLagSeconds: m.resetLag.Load(),
	}
}

// OpenAIWindowWarmupOptions controls worker safety limits.
type OpenAIWindowWarmupOptions struct {
	WorkerConcurrency int
	GlobalQPS         float64
	BatchSize         int
	ScanInterval      time.Duration
	RequestTimeout    time.Duration
	LeaseDuration     time.Duration
	ResetGrace        time.Duration
	ResetGraceSet     bool
	Jitter            time.Duration
	MaxAttempts       int
	Model             string
	KillSwitch        OpenAIWindowWarmupKillSwitch
	Allowlist         OpenAIWindowWarmupAllowlist
	Concurrency       interface {
		TryAcquireAccountExclusive(context.Context, int64, time.Duration) (*AccountExclusiveLease, bool, error)
	}
	UsageReconciler    OpenAIWindowWarmupUsageReconciler
	AuthFailureHandler OpenAIWindowWarmupAuthFailureHandler
	Now                func() time.Time
	RandomJitter       func(string, time.Duration) time.Duration
}

func (o OpenAIWindowWarmupOptions) withDefaults() OpenAIWindowWarmupOptions {
	if o.WorkerConcurrency <= 0 {
		o.WorkerConcurrency = 1
	}
	if o.GlobalQPS <= 0 {
		o.GlobalQPS = 0.2
	}
	if o.BatchSize <= 0 {
		o.BatchSize = openAIWindowWarmupDefaultBatch
	}
	if o.ScanInterval <= 0 {
		o.ScanInterval = openAIWindowWarmupDefaultScanInterval
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = openAIWindowWarmupDefaultTimeout
	}
	if o.LeaseDuration <= o.RequestTimeout {
		o.LeaseDuration = openAIWindowWarmupDefaultLease
	}
	if o.ResetGrace < 0 {
		o.ResetGrace = 0
	}
	if o.ResetGrace == 0 && !o.ResetGraceSet {
		o.ResetGrace = openAIWindowWarmupDefaultGrace
	}
	if o.Jitter <= 0 {
		o.Jitter = openAIWindowWarmupDefaultJitter
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = openAIWindowWarmupDefaultMaxAttempts
	}
	if strings.TrimSpace(o.Model) == "" {
		o.Model = "codex-auto-review"
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	if o.KillSwitch == nil {
		o.KillSwitch = OpenAIWindowWarmupKillSwitchFunc(func(context.Context) (bool, error) { return true, nil })
	}
	if o.RandomJitter == nil {
		o.RandomJitter = deterministicWarmupJitter
	}
	return o
}

// OpenAIWindowWarmupService owns policy, scheduling, lease processing and
// outcome transitions. It is safe to run in multiple application instances.
type OpenAIWindowWarmupService struct {
	repo           OpenAIWindowWarmupRepository
	accounts       AccountRepository
	executor       OpenAIOutboundExecutor
	probe          OpenAIWindowProbe
	audit          *AuditLogService
	options        OpenAIWindowWarmupOptions
	metrics        openAIWindowWarmupMetricCounters
	workerInflight atomic.Int64
	cohortMu       sync.RWMutex
	cohortIDs      []int64
	owner          string
	ctx            context.Context
	cancel         context.CancelFunc
	startOnce      sync.Once
	stopOnce       sync.Once
	wg             sync.WaitGroup
	limiter        *warmupRateLimiter
	claimInterval  time.Duration
}

func NewOpenAIWindowWarmupService(repo OpenAIWindowWarmupRepository, accounts AccountRepository, executor OpenAIOutboundExecutor, probe OpenAIWindowProbe, audit *AuditLogService, options OpenAIWindowWarmupOptions) *OpenAIWindowWarmupService {
	options = options.withDefaults()
	if configurable, ok := probe.(interface{ SetRequestTimeout(time.Duration) }); ok {
		configurable.SetRequestTimeout(options.RequestTimeout)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &OpenAIWindowWarmupService{
		repo: repo, accounts: accounts, executor: executor, probe: probe, audit: audit,
		options: options, owner: "warmup-" + uuid.NewString(), ctx: ctx, cancel: cancel,
		limiter: newWarmupRateLimiter(options.GlobalQPS), claimInterval: openAIWindowWarmupClaimInterval,
	}
}

func (s *OpenAIWindowWarmupService) Metrics() OpenAIWindowWarmupMetrics {
	if s == nil {
		return OpenAIWindowWarmupMetrics{}
	}
	return s.metrics.Snapshot()
}

func (s *OpenAIWindowWarmupService) Start() {
	if s == nil || s.repo == nil || s.accounts == nil || s.probe == nil {
		return
	}
	s.startOnce.Do(func() {
		s.wg.Add(1)
		go s.runScanner()
	})
}

func (s *OpenAIWindowWarmupService) Stop() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() { s.cancel(); s.wg.Wait() })
}

func (s *OpenAIWindowWarmupService) runScanner() {
	defer s.wg.Done()
	timer := time.NewTimer(10 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return
	case <-timer.C:
		s.reconcileAccounts(s.ctx)
		s.claimDueOnce(s.ctx)
		s.refreshCachedQueueStats(s.ctx)
	}
	claimInterval := s.claimInterval
	if claimInterval <= 0 {
		claimInterval = openAIWindowWarmupClaimInterval
	}
	claimTicker := time.NewTicker(claimInterval)
	defer claimTicker.Stop()
	cohortTicker := time.NewTicker(s.options.ScanInterval)
	defer cohortTicker.Stop()
	reconcileTicker := time.NewTicker(openAIWindowWarmupReconcileInterval)
	defer reconcileTicker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-claimTicker.C:
			s.claimDueOnce(s.ctx)
		case <-cohortTicker.C:
			s.refreshCohortAndQueueStats(s.ctx)
		case <-reconcileTicker.C:
			s.reconcileAccounts(s.ctx)
		}
	}
}

func (s *OpenAIWindowWarmupService) reconcileAccounts(ctx context.Context) {
	if s == nil || s.accounts == nil || s.repo == nil {
		return
	}
	_, _ = s.repo.CleanupExpiredAttempts(ctx, 500)
	_, _ = s.repo.CleanupSupersededTerminalJobs(ctx, 500)
	accounts, accountIDs, ok := s.refreshWarmupCohort(ctx)
	if !ok || len(accountIDs) == 0 {
		return
	}
	currentByAccount, err := s.repo.GetCurrentForAccounts(ctx, accountIDs, OpenAIWindowWarmupQuotaScopeGlobal)
	if err != nil {
		return
	}
	for index := range accounts {
		account := &accounts[index]
		current := currentByAccount[account.ID]
		if current == nil {
			_, _, _ = s.scheduleAccountWarmup(ctx, account, OpenAIWindowWarmupTriggerReconcile, false)
			continue
		}
		if current.State == OpenAIWindowWarmupStatePaused &&
			(OpenAIWindowWarmupPolicyForAccount(account) == OpenAIWindowWarmupPolicyContinuous || strings.HasPrefix(current.CycleKey, "initial:")) {
			_, _, _ = s.scheduleAccountWarmup(ctx, account, OpenAIWindowWarmupTriggerReconcile, false)
			continue
		}
		if current.State == OpenAIWindowWarmupStateBlocked && current.SentAt == nil &&
			current.LastErrorCode == "attempt_limit_preflight" {
			// Older workers exhausted weekly-only accounts before capability
			// classification existed. Rearm exactly once for a passive check; the
			// retained attempt cap still prevents any synthetic send.
			_, _ = s.repo.RequeueLegacyPreflightLimit(
				ctx, current.ID, current.IdentityGeneration, current.AttemptCount, s.now(),
			)
			continue
		}
		if current.State == OpenAIWindowWarmupStateCompleted &&
			OpenAIWindowWarmupPolicyForAccount(account) == OpenAIWindowWarmupPolicyContinuous &&
			current.ObservedResetAt != nil {
			s.enqueueNextContinuousCycle(ctx, account, current.ObservedResetAt)
		}
	}
}

func (s *OpenAIWindowWarmupService) scanOnce(ctx context.Context) {
	s.refreshWarmupCohort(ctx)
	s.claimDueOnce(ctx)
	s.refreshCachedQueueStats(ctx)
}

func (s *OpenAIWindowWarmupService) claimDueOnce(ctx context.Context) {
	cohortAccountIDs := s.cachedWarmupCohortIDs()
	if len(cohortAccountIDs) == 0 {
		return
	}
	// A disabled worker may still report backlog, but must never claim a lease.
	if !s.killSwitchEnabled(ctx) {
		return
	}
	available := s.options.WorkerConcurrency - int(s.workerInflight.Load())
	if available <= 0 {
		return
	}
	limit := s.options.BatchSize
	if available < limit {
		limit = available
	}
	claims, err := s.repo.ClaimDue(ctx, s.owner, s.options.LeaseDuration, limit, cohortAccountIDs)
	if err != nil {
		return
	}
	for _, claim := range claims {
		if claim.Job == nil {
			continue
		}
		s.workerInflight.Add(1)
		s.wg.Add(1)
		go func(c OpenAIWindowWarmupClaim) {
			defer s.wg.Done()
			defer s.workerInflight.Add(-1)
			s.processClaim(ctx, c)
		}(claim)
	}
}

func (s *OpenAIWindowWarmupService) refreshCohortAndQueueStats(ctx context.Context) {
	s.refreshWarmupCohort(ctx)
	s.refreshCachedQueueStats(ctx)
}

func (s *OpenAIWindowWarmupService) refreshCachedQueueStats(ctx context.Context) {
	cohortAccountIDs := s.cachedWarmupCohortIDs()
	if len(cohortAccountIDs) == 0 {
		s.applyQueueStats(OpenAIWindowWarmupQueueStats{})
		return
	}
	s.refreshQueueStats(ctx, cohortAccountIDs)
}

func (s *OpenAIWindowWarmupService) refreshWarmupCohort(ctx context.Context) ([]Account, []int64, bool) {
	if s == nil {
		return nil, nil, false
	}
	accounts, accountIDs, ok := s.warmupCohort(ctx)
	if !ok {
		accountIDs = nil
	}
	s.cohortMu.Lock()
	s.cohortIDs = append(s.cohortIDs[:0], accountIDs...)
	s.cohortMu.Unlock()
	return accounts, accountIDs, ok
}

func (s *OpenAIWindowWarmupService) cachedWarmupCohortIDs() []int64 {
	if s == nil {
		return nil
	}
	s.cohortMu.RLock()
	defer s.cohortMu.RUnlock()
	return append([]int64(nil), s.cohortIDs...)
}

// ScheduleAccountWarmup persists the strategy and creates the first/current
// cycle. It is safe to call after every import/update; the unique cycle key
// suppresses duplicate jobs.
func (s *OpenAIWindowWarmupService) ScheduleAccountWarmup(ctx context.Context, account *Account, trigger string) (*OpenAIWindowWarmupJob, bool, error) {
	return s.scheduleAccountWarmup(ctx, account, trigger, true)
}

func (s *OpenAIWindowWarmupService) scheduleAccountWarmup(ctx context.Context, account *Account, trigger string, enforceCohort bool) (*OpenAIWindowWarmupJob, bool, error) {
	if s == nil || s.repo == nil || account == nil {
		return nil, false, errors.New("warmup service is not configured")
	}
	if !warmupAccountEligibleAt(account, s.now()) {
		return nil, false, nil
	}
	policy := OpenAIWindowWarmupPolicyForAccount(account)
	if !policy.Enabled() {
		return nil, false, nil
	}
	if enforceCohort && !s.accountAllowed(ctx, account.ID) {
		return nil, false, nil
	}
	if trigger == "" {
		trigger = OpenAIWindowWarmupTriggerImport
	}
	cycleGeneration := warmupAccountGeneration(account)
	now := s.now()
	resetAt := accountCodexGlobalResetAt(account)
	cycleKey := warmupInitialCycleKey(account, cycleGeneration)
	next := now
	waitForReset := warmupAccountShouldWaitForReset(account, resetAt, now)
	if waitForReset {
		next = s.warmupDueAt(cycleKey, resetAt)
	}
	if s.options.Jitter > 0 && !waitForReset {
		next = next.Add(s.options.RandomJitter(cycleKey, s.options.Jitter))
	}
	current, currentErr := s.repo.GetCurrent(ctx, account.ID, OpenAIWindowWarmupQuotaScopeGlobal)
	if currentErr != nil && !errors.Is(currentErr, sql.ErrNoRows) {
		return nil, false, currentErr
	}
	if current != nil && current.IdentityGeneration == account.OpenAIWarmupIdentityGeneration &&
		current.State == OpenAIWindowWarmupStatePaused {
		rearmed, changed, rearmErr := s.repo.UnblockAccount(ctx, account.ID, next, resetAt)
		if rearmErr != nil {
			return nil, false, rearmErr
		}
		if changed {
			s.recordWarmupAudit(rearmed, rearmed.State, 0, "policy_rearmed", resetAt)
		}
		return rearmed, changed, nil
	}
	job, inserted, err := s.repo.Enqueue(ctx, OpenAIWindowWarmupEnqueue{
		AccountID: account.ID, QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: cycleKey, CycleGeneration: cycleGeneration,
		IdentityGeneration: account.OpenAIWarmupIdentityGeneration, Trigger: trigger,
		ObservedResetAt: resetAt, NextAttemptAt: next,
	})
	if err != nil {
		return nil, inserted, err
	}
	if !inserted && job != nil && job.State == OpenAIWindowWarmupStatePaused {
		rearmed, changed, rearmErr := s.repo.UnblockAccount(ctx, account.ID, next, resetAt)
		if rearmErr != nil {
			return nil, false, rearmErr
		}
		if changed {
			s.recordWarmupAudit(rearmed, rearmed.State, 0, "policy_rearmed", resetAt)
			return rearmed, true, nil
		}
	}
	if !inserted {
		// The account trigger creates the initial job in the import transaction.
		// Seeing that durable row immediately afterward is the normal success
		// path, not duplicate suppression. Do not emit another enqueue audit here:
		// repeated idempotent imports project the same trigger-created row. Reserve
		// the duplicate counter for competing Core enqueue attempts and
		// manual/reset cycles.
		triggerBackedInitial := job != nil && job.Trigger == OpenAIWindowWarmupTriggerImport &&
			strings.HasPrefix(job.CycleKey, "initial:") &&
			(trigger == OpenAIWindowWarmupTriggerImport || trigger == OpenAIWindowWarmupTriggerReconcile)
		if !triggerBackedInitial {
			s.metrics.duplicates.Add(1)
		}
	} else {
		s.metrics.enqueued.Add(1)
		if job != nil {
			s.recordWarmupAudit(job, job.State, 0, "enqueued", job.ObservedResetAt)
		}
	}
	return job, inserted, nil
}

// Enqueue is a convenience for callers that already know the account and
// cycle. It never derives a reset timestamp.
func (s *OpenAIWindowWarmupService) Enqueue(ctx context.Context, account *Account, trigger string) (*OpenAIWindowWarmupJob, bool, error) {
	return s.ScheduleAccountWarmup(ctx, account, trigger)
}

func (s *OpenAIWindowWarmupService) GetJob(ctx context.Context, id int64) (*OpenAIWindowWarmupJob, error) {
	if s == nil || s.repo == nil {
		return nil, errors.New("warmup service is not configured")
	}
	return s.repo.GetByID(ctx, id)
}

func (s *OpenAIWindowWarmupService) ListJobsPage(ctx context.Context, options OpenAIWindowWarmupListOptions) ([]*OpenAIWindowWarmupJob, int64, error) {
	if s == nil || s.repo == nil {
		return nil, 0, errors.New("warmup service is not configured")
	}
	return s.repo.ListPage(ctx, options)
}

func (s *OpenAIWindowWarmupService) CurrentJobsForAccounts(ctx context.Context, accountIDs []int64) (map[int64]*OpenAIWindowWarmupJob, error) {
	if s == nil || s.repo == nil {
		return nil, errors.New("warmup service is not configured")
	}
	return s.repo.GetCurrentForAccounts(ctx, accountIDs, OpenAIWindowWarmupQuotaScopeGlobal)
}

// RequeueAccount rearms the current cycle when possible. A completed account
// gets one stable manual cycle derived from the latest durable job ID; the
// active-cycle unique index suppresses concurrent admin requests.
func (s *OpenAIWindowWarmupService) RequeueAccount(ctx context.Context, accountID int64) (*OpenAIWindowWarmupJob, bool, error) {
	if s == nil || s.repo == nil || s.accounts == nil {
		return nil, false, errors.New("warmup service is not configured")
	}
	account, err := s.accounts.GetByID(ctx, accountID)
	if err != nil || account == nil {
		return nil, false, err
	}
	if !warmupAccountEligibleAt(account, s.now()) {
		return nil, false, errors.New("account is not eligible for OpenAI window warmup")
	}
	if !s.accountAllowed(ctx, accountID) {
		return nil, false, errors.New("account is outside the configured OpenAI window warmup cohort")
	}
	current, currentErr := s.repo.GetCurrent(ctx, accountID, OpenAIWindowWarmupQuotaScopeGlobal)
	if currentErr != nil && !errors.Is(currentErr, sql.ErrNoRows) {
		return nil, false, currentErr
	}
	capabilityRecheck := false
	if current != nil && current.IdentityGeneration == account.OpenAIWarmupIdentityGeneration &&
		openAIWindowWarmupFiveHourUnsupportedJob(current) {
		cleared, clearErr := s.repo.ClearUnsupportedFiveHourWindow(
			ctx, accountID, account.OpenAIWarmupIdentityGeneration,
		)
		if clearErr != nil {
			return nil, false, clearErr
		}
		capabilityRecheck = cleared
		if cleared {
			current, currentErr = s.repo.GetCurrent(ctx, accountID, OpenAIWindowWarmupQuotaScopeGlobal)
			if currentErr != nil && !errors.Is(currentErr, sql.ErrNoRows) {
				return nil, false, currentErr
			}
		}
	}
	if current != nil && current.IdentityGeneration == account.OpenAIWarmupIdentityGeneration {
		if IsOpenAIWindowWarmupStateActive(current.State) {
			return current, capabilityRecheck, nil
		}
		if current.State == OpenAIWindowWarmupStateBlocked || current.State == OpenAIWindowWarmupStateBlockedConfig || current.State == OpenAIWindowWarmupStatePaused {
			resetAt := accountCodexGlobalResetAt(account)
			next := s.now()
			if warmupAccountShouldWaitForReset(account, resetAt, next) {
				next = s.warmupDueAt(current.CycleKey, resetAt)
			}
			return s.repo.UnblockAccount(ctx, accountID, next, resetAt)
		}
	}
	resetAt := accountCodexGlobalResetAt(account)
	now := s.now()
	next := now
	if warmupAccountShouldWaitForReset(account, resetAt, now) {
		next = s.warmupDueAt(warmupResetCycleKey(*resetAt), resetAt)
	}
	manualSeed := warmupAccountGeneration(account)
	if current != nil {
		manualSeed = current.ID
	}
	cycleGeneration := warmupAccountGeneration(account)
	job, inserted, enqueueErr := s.repo.Enqueue(ctx, OpenAIWindowWarmupEnqueue{
		AccountID: account.ID, QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey:           fmt.Sprintf("manual:%d:%d", account.OpenAIWarmupIdentityGeneration, manualSeed),
		CycleGeneration:    cycleGeneration,
		IdentityGeneration: account.OpenAIWarmupIdentityGeneration,
		Trigger:            OpenAIWindowWarmupTriggerManual, ObservedResetAt: resetAt,
		NextAttemptAt: next,
	})
	return job, inserted || capabilityRecheck, enqueueErr
}

func openAIWindowWarmupFiveHourUnsupportedJob(job *OpenAIWindowWarmupJob) bool {
	return job != nil && job.State == OpenAIWindowWarmupStateFailed &&
		job.LastErrorCode == OpenAIWindowWarmupErrorFiveHourWindowUnsupported
}

// IsOpenAIWindowWarmupStateActive reports whether a job can still be leased or
// is currently held by a worker.
func IsOpenAIWindowWarmupStateActive(state string) bool {
	switch state {
	case OpenAIWindowWarmupStatePending, OpenAIWindowWarmupStateArmed, OpenAIWindowWarmupStateDue,
		OpenAIWindowWarmupStateRunning, OpenAIWindowWarmupStateRetrying,
		OpenAIWindowWarmupStateUncertain, OpenAIWindowWarmupStatePossiblySent:
		return true
	default:
		return false
	}
}

// UnblockAccount releases the latest blocked/paused job without creating a
// duplicate cycle. A no-op returns the current row with changed=false.
func (s *OpenAIWindowWarmupService) UnblockAccount(ctx context.Context, accountID int64) (*OpenAIWindowWarmupJob, bool, error) {
	if s == nil || s.repo == nil || s.accounts == nil {
		return nil, false, errors.New("warmup service is not configured")
	}
	account, err := s.accounts.GetByID(ctx, accountID)
	if err != nil || account == nil {
		return nil, false, err
	}
	if !s.accountAllowed(ctx, accountID) {
		return nil, false, errors.New("account is outside the configured OpenAI window warmup cohort")
	}
	current, currentErr := s.repo.GetCurrent(ctx, accountID, OpenAIWindowWarmupQuotaScopeGlobal)
	if currentErr != nil && !errors.Is(currentErr, sql.ErrNoRows) {
		return nil, false, currentErr
	}
	if current == nil || current.IdentityGeneration != account.OpenAIWarmupIdentityGeneration {
		return s.scheduleAccountWarmup(ctx, account, OpenAIWindowWarmupTriggerManual, false)
	}
	resetAt := accountCodexGlobalResetAt(account)
	next := s.now()
	if resetAt != nil && resetAt.After(next) {
		next = s.warmupDueAt(warmupResetCycleKey(*resetAt), resetAt)
	}
	return s.repo.UnblockAccount(ctx, accountID, next, resetAt)
}

func (s *OpenAIWindowWarmupService) processClaim(parent context.Context, claim OpenAIWindowWarmupClaim) {
	job := claim.Job
	ctx, cancel := context.WithTimeout(parent, s.options.RequestTimeout+s.options.LeaseDuration)
	defer cancel()
	if !s.killSwitchEnabled(ctx) {
		s.reschedule(ctx, claim, s.now().Add(s.options.ScanInterval), OpenAIWindowWarmupStatePending, job.ObservedResetAt)
		return
	}
	account, err := s.accounts.GetByID(ctx, job.AccountID)
	if err != nil {
		if errors.Is(err, ErrAccountNotFound) || errors.Is(err, sql.ErrNoRows) {
			s.markBlocked(ctx, claim, s.now(), 0, "account_not_found", "account unavailable")
			return
		}
		s.markRetry(ctx, claim, s.now(), s.now().Add(time.Minute), 0, "account_read_failed", "transient account lookup failed")
		return
	}
	if account == nil {
		s.markBlocked(ctx, claim, s.now(), 0, "account_not_found", "account unavailable")
		return
	}
	policy := OpenAIWindowWarmupPolicyForAccount(account)
	if !warmupAccountEligibleAt(account, s.now()) || !policy.Enabled() {
		s.markPaused(ctx, claim, s.now(), "account_ineligible")
		return
	}
	if policy == OpenAIWindowWarmupPolicyInitialOnce && !strings.HasPrefix(job.CycleKey, "initial:") {
		s.markPaused(ctx, claim, s.now(), "policy_cycle_disabled")
		return
	}
	if !s.accountAllowed(ctx, account.ID) {
		s.reschedule(ctx, claim, s.now().Add(s.options.ScanInterval), OpenAIWindowWarmupStatePending, job.ObservedResetAt)
		return
	}
	// A real business request may have advanced the window after the job was
	// created. A newer reset alone is not activity evidence: /wham/usage exposes
	// a rolling now+5h reset for an unused 0% window. For a reset-backed cycle,
	// only activity after the observed reset belongs to the new window.
	if latestReset := accountCodexGlobalResetAt(account); latestReset != nil && latestReset.After(s.now()) {
		if warmupResetAdvanced(latestReset, job.ObservedResetAt, s.now()) {
			if warmupAccountUsedSinceCycle(account, job) {
				s.completeSuppressedCycle(ctx, claim, account, latestReset, "real_traffic_suppressed")
				return
			}
		} else if !warmupInitialCycleHasIdleSnapshot(account, job) {
			s.reschedule(ctx, claim, s.warmupDueAt(job.CycleKey, latestReset), OpenAIWindowWarmupStateArmed, latestReset)
			return
		}
	}
	// A timed-out sender may have reached upstream. A fixed reset on two
	// separated passive observations fences replay; a reset that advances with
	// the observation interval proves that /wham is still reporting an idle
	// rolling projection and permits another bounded attempt.
	// sent_at is the durable send fence. It must take precedence over the
	// previous state: a retrying/armed projection can still represent a request
	// whose response was lost, so another POST is unsafe until passive usage
	// reconciliation has completed.
	if job.SentAt != nil {
		now := s.now()
		reset, authoritative, active, reconcileErr := s.reconcileFiveHourObservation(ctx, account.ID, job)
		if reconcileErr != nil {
			if errors.Is(reconcileErr, errWarmupFiveHourWindowUnsupported) {
				s.markUnsupportedFiveHourWindow(ctx, claim, now)
				return
			}
			s.handleUsageObservationFailure(ctx, claim, now, OpenAIWindowWarmupStateUncertain, "reconcile", reconcileErr)
			return
		}
		if authoritative && active && reset != nil && reset.After(now) {
			if job.UncertainTerminalObserved && warmupAttemptResetAdvanced(reset, job, now) {
				if s.markSuccess(ctx, claim, now, reset, warmupJobStatus(job), "completed_reconciled", openAIWindowWarmupSuccessEvidence{}) {
					s.enqueueNextContinuousCycle(ctx, account, reset)
				}
			} else {
				s.completeSuppressedCycle(ctx, claim, account, reset, "lease_takeover_reconciled")
			}
			return
		}
		if authoritative && active {
			s.markUncertain(ctx, claim, now, nextWarmupUncertainObservation(job, now), warmupJobStatus(job),
				warmupUncertainCode(job), "active usage has no future reset evidence",
				OpenAIWindowWarmupUncertainEvidence{Authoritative: true, ResetAt: reset, Terminal: job.UncertainTerminalObserved})
			return
		}
		switch classifyWarmupUncertainObservation(job, reset, now) {
		case warmupUncertainRecordObservation:
			s.markUncertain(ctx, claim, now, nextWarmupUncertainObservation(job, now), warmupJobStatus(job),
				warmupUncertainCode(job), "ambiguous warmup requires another passive observation",
				OpenAIWindowWarmupUncertainEvidence{Authoritative: authoritative, ResetAt: reset, Terminal: job.UncertainTerminalObserved})
			return
		case warmupUncertainWait:
			s.markUncertain(ctx, claim, now, nextWarmupUncertainObservation(job, now), warmupJobStatus(job),
				warmupUncertainCode(job), "ambiguous warmup observation interval has not converged",
				OpenAIWindowWarmupUncertainEvidence{Terminal: job.UncertainTerminalObserved})
			return
		case warmupUncertainFixedReset:
			if job.UncertainTerminalObserved && warmupAttemptResetAdvanced(reset, job, now) {
				if s.markSuccess(ctx, claim, now, reset, warmupJobStatus(job), "completed_reconciled", openAIWindowWarmupSuccessEvidence{}) {
					s.enqueueNextContinuousCycle(ctx, account, reset)
				}
			} else {
				s.completeSuppressedCycle(ctx, claim, account, reset, "possibly_sent_reconciled")
			}
			return
		case warmupUncertainRollingReset:
			// The prior request did not establish a fixed five-hour window. The
			// normal preflight and attempt cap below decide whether to retry.
		default:
			s.markUncertain(ctx, claim, now, nextWarmupUncertainObservation(job, now), warmupJobStatus(job),
				warmupUncertainCode(job), "authoritative usage unavailable",
				OpenAIWindowWarmupUncertainEvidence{Terminal: job.UncertainTerminalObserved})
			return
		}
	}
	// Rows that exhausted the legacy preflight retry budget get one final
	// passive capability classification. This fixes already-blocked weekly-only
	// accounts without allowing another synthetic send or depending on the
	// exclusive send gate.
	if job.AttemptCount >= s.options.MaxAttempts {
		if job.SentAt == nil {
			usage, usageErr := s.queryWarmupUsage(ctx, account.ID)
			if usageErr == nil && warmupFiveHourCapabilityForUsage(usage) == warmupFiveHourUnsupported {
				s.markUnsupportedFiveHourWindow(ctx, claim, s.now())
				return
			}
		}
		s.markBlocked(ctx, claim, s.now(), 0, "attempt_limit", "warmup attempt limit reached")
		return
	}
	if s.options.Concurrency == nil {
		s.markRetry(ctx, claim, s.now(), s.now().Add(time.Minute), 0, "concurrency_gate_unavailable", "exclusive account gate unavailable")
		return
	}
	exclusive, acquired, acquireErr := s.options.Concurrency.TryAcquireAccountExclusive(ctx, account.ID, s.options.LeaseDuration)
	if acquireErr != nil || !acquired || exclusive == nil {
		next := s.now().Add(time.Minute)
		s.reschedule(ctx, claim, next, OpenAIWindowWarmupStateRetrying, job.ObservedResetAt)
		return
	}
	defer exclusive.Release()

	// This is the final authoritative check after the account becomes
	// exclusive. Account reset snapshots are persisted asynchronously on the
	// business path, so the database row alone cannot close this race.
	usage, usageErr := s.queryWarmupUsage(ctx, account.ID)
	if usageErr != nil || usage == nil {
		if usageErr == nil {
			usageErr = errors.New("warmup usage reconciler returned an empty response")
		}
		s.handleUsageObservationFailure(ctx, claim, s.now(), OpenAIWindowWarmupStateRetrying, "preflight", usageErr)
		return
	}
	if warmupFiveHourCapabilityForUsage(usage) == warmupFiveHourUnsupported {
		s.markUnsupportedFiveHourWindow(ctx, claim, s.now())
		return
	}
	fiveHour, authoritative := warmupFiveHourObservation(usage)
	if !authoritative {
		s.handleUsageObservationFailure(ctx, claim, s.now(), OpenAIWindowWarmupStateRetrying, "preflight", errors.New("authoritative five-hour usage window unavailable"))
		return
	}
	fiveHourReset := fiveHour.ResetAt
	weekly, weeklyAuthoritative := warmupBlockedWeeklyObservation(usage)
	if !weeklyAuthoritative {
		s.handleUsageObservationFailure(ctx, claim, s.now(), OpenAIWindowWarmupStateRetrying, "preflight", errors.New("authoritative weekly usage window unavailable"))
		return
	}
	if weekly.Blocked {
		blockedUntil := cloneWarmupTime(weekly.ResetAt)
		if fiveHourReset != nil && (blockedUntil == nil || fiveHourReset.After(*blockedUntil)) {
			blockedUntil = cloneWarmupTime(fiveHourReset)
		}
		if blockedUntil == nil || !blockedUntil.After(s.now()) {
			s.handleUsageObservationFailure(ctx, claim, s.now(), OpenAIWindowWarmupStateRetrying, "preflight", errors.New("blocked weekly window has no future reset"))
			return
		}
		next := s.warmupDueAt(warmupResetCycleKey(*blockedUntil), blockedUntil)
		s.markRateLimited(ctx, claim, s.now(), next, blockedUntil, 0, "weekly_limit_preflight")
		return
	}
	latestAccount, latestErr := s.accounts.GetByID(ctx, account.ID)
	if latestErr != nil || latestAccount == nil {
		if latestErr == nil {
			latestErr = errors.New("account unavailable after usage preflight")
		}
		s.handleUsageObservationFailure(ctx, claim, s.now(), OpenAIWindowWarmupStateRetrying, "preflight_account", latestErr)
		return
	}
	account = latestAccount
	if job.IdentityGeneration <= 0 || account.OpenAIWarmupIdentityGeneration != job.IdentityGeneration {
		s.markPaused(ctx, claim, s.now(), "credential_identity_superseded")
		_, _, _ = s.scheduleAccountWarmup(ctx, account, OpenAIWindowWarmupTriggerReconcile, false)
		return
	}
	if !warmupAccountEligibleAt(account, s.now()) || !OpenAIWindowWarmupPolicyForAccount(account).Enabled() {
		s.markPaused(ctx, claim, s.now(), "account_ineligible")
		return
	}
	activeUsage := fiveHour.UsedPercent > 0 || warmupAccountUsedSinceCycle(account, job)
	if fiveHourReset != nil && fiveHourReset.After(s.now()) {
		if warmupResetAdvanced(fiveHourReset, job.ObservedResetAt, s.now()) {
			if activeUsage {
				s.completeSuppressedCycle(ctx, claim, account, fiveHourReset, "usage_preflight_suppressed")
				return
			}
		} else if !warmupInitialCycleHasIdleObservation(job, fiveHour.UsedPercent) {
			next := s.warmupDueAt(warmupResetCycleKey(*fiveHourReset), fiveHourReset)
			s.reschedule(ctx, claim, next, OpenAIWindowWarmupStateArmed, fiveHourReset)
			return
		}
	}
	if activeUsage && (fiveHourReset == nil || !fiveHourReset.After(s.now())) {
		s.handleUsageObservationFailure(ctx, claim, s.now(), OpenAIWindowWarmupStateRetrying, "preflight", errors.New("active five-hour usage has no future reset"))
		return
	}
	if !s.limiter.Allow(s.now()) {
		next := s.now().Add(5 * time.Second)
		s.reschedule(ctx, claim, next, OpenAIWindowWarmupStateRetrying, job.ObservedResetAt)
		return
	}
	permitToken, reserved, reserveErr := s.repo.ReserveGlobalSend(ctx, s.limiter.interval, s.options.LeaseDuration)
	if reserveErr != nil || !reserved {
		s.reschedule(ctx, claim, s.now().Add(s.limiter.interval), OpenAIWindowWarmupStateRetrying, job.ObservedResetAt)
		return
	}
	defer s.releaseGlobalSend(permitToken)
	if guardErr := s.checkWarmupSendGuard(ctx, account.ID); guardErr != nil {
		s.reschedule(ctx, claim, s.now().Add(s.options.ScanInterval), OpenAIWindowWarmupStatePending, job.ObservedResetAt)
		return
	}
	if ok, err := exclusive.Refresh(ctx); err != nil || !ok {
		s.reschedule(ctx, claim, s.now().Add(time.Minute), OpenAIWindowWarmupStateRetrying, job.ObservedResetAt)
		return
	}
	// Refresh the lease immediately before the send CAS. Preflight checks may
	// have consumed most of the original lease; a stale owner must stop here.
	if ok, err := s.repo.RenewLease(ctx, job.ID, claim.Owner, claim.LeaseToken, s.options.LeaseDuration); err != nil || !ok {
		return
	}
	startEvidence := OpenAIWindowWarmupStartEvidence{
		Authoritative: true,
		UsedPercent:   fiveHour.UsedPercent,
		ResetAt:       cloneWarmupTime(fiveHour.ResetAt),
	}
	startedAt := s.now()
	if !s.markStarted(ctx, &claim, startedAt, startEvidence) {
		if latest, latestErr := s.accounts.GetByID(ctx, job.AccountID); latestErr == nil && latest != nil {
			if reset := accountCodexGlobalResetAt(latest); reset != nil && reset.After(s.now()) {
				if warmupResetAdvanced(reset, job.ObservedResetAt, s.now()) {
					if warmupAccountUsedSinceCycle(latest, job) {
						s.completeSuppressedCycle(ctx, claim, latest, reset, "mark_started_reset_cas")
					} else {
						s.reschedule(ctx, claim, s.now().Add(s.options.ScanInterval), OpenAIWindowWarmupStateRetrying, nil)
					}
				} else {
					s.reschedule(ctx, claim, s.warmupDueAt(job.CycleKey, reset), OpenAIWindowWarmupStateArmed, reset)
				}
				return
			}
		}
		s.reschedule(ctx, claim, s.now().Add(s.options.ScanInterval), OpenAIWindowWarmupStateRetrying, nil)
		return
	}
	probeCtx, probeCancel := context.WithTimeout(ctx, s.options.RequestTimeout)
	probeCtx = withOpenAIWindowWarmupSendGuard(probeCtx, OpenAIWindowWarmupSendGuardFunc(s.checkWarmupSendGuard))
	result, probeErr := s.probe.Probe(probeCtx, account, job.PreflightResetAt)
	probeCancel()
	s.handleProbeResultWithPreflight(ctx, claim, account, result, probeErr, fiveHour.UsedPercent <= 0)
}

func (s *OpenAIWindowWarmupService) handleProbeResult(ctx context.Context, claim OpenAIWindowWarmupClaim, account *Account, result *OpenAIWindowProbeResult, probeErr error) {
	s.handleProbeResultWithPreflight(ctx, claim, account, result, probeErr, false)
}

func (s *OpenAIWindowWarmupService) handleProbeResultWithPreflight(ctx context.Context, claim OpenAIWindowWarmupClaim, account *Account, result *OpenAIWindowProbeResult, probeErr error, idlePreflight bool) {
	job := claim.Job
	now := s.now()
	statusCode := 0
	if result != nil {
		statusCode = result.StatusCode
	}
	if errors.Is(probeErr, ErrOpenAIWindowWarmupSendGuardClosed) {
		if result == nil || !result.Started {
			s.cancelStartedBeforeSend(ctx, claim, now, "send_guard_closed")
		} else {
			s.markRetry(ctx, claim, now, now.Add(s.options.ScanInterval), statusCode,
				"send_guard_closed", "dynamic warmup guard closed before replay")
		}
		return
	}
	if errors.Is(probeErr, ErrOpenAIWindowWarmupCredentialsChanged) {
		// The identity fence can trip before the outbound transport is entered
		// (for example, while refreshing the OAuth token). In that case the
		// durable MarkStarted record is only a local send reservation and may be
		// cancelled. Once the transport reports Started, the request may have
		// reached upstream; retain the send evidence and force passive
		// reconciliation instead of manufacturing a suppressed/failed outcome.
		if result == nil || !result.Started {
			s.cancelStartedBeforeSend(ctx, claim, now, "credential_identity_superseded")
		} else {
			s.markPaused(ctx, claim, now, "credential_identity_superseded")
		}
		if s.accounts != nil && account != nil {
			if latest, err := s.accounts.GetByID(ctx, account.ID); err == nil && latest != nil {
				_, _, _ = s.scheduleAccountWarmup(ctx, latest, OpenAIWindowWarmupTriggerReconcile, false)
			}
		}
		return
	}
	authFailure := openAIWindowWarmupProbeAuthFailure(result, probeErr)
	if authFailure != nil && (authFailure.Disposition == OpenAIWindowWarmupAuthRefreshTransient ||
		authFailure.Disposition == OpenAIWindowWarmupAuthRefreshInProgress) {
		if job.AttemptCount >= s.options.MaxAttempts {
			// The durable job is terminal at the attempt cap, but a transient
			// OAuth failure still needs the account-level cooldown so real traffic
			// does not immediately select the same credential. Apply that side
			// effect only after the fenced job transition succeeds.
			if s.markBlocked(ctx, claim, now, statusCode, "attempt_limit", "warmup attempt limit reached") {
				_ = s.handleAuthFailure(ctx, authFailure)
			}
			return
		}
		if s.markRetry(ctx, claim, now, now.Add(warmupBackoff(job.AttemptCount-1)), statusCode,
			string(authFailure.Disposition), "transient credential recovery failure") {
			_ = s.handleAuthFailure(ctx, authFailure)
		}
		return
	}
	if statusCode == http.StatusForbidden && warmupAuthFailureIsRetryableForbidden(authFailure) {
		// HTML/proxy 403s are not credential evidence, while structured 403s are
		// only a temporary account cooldown until the shared 403 policy reaches
		// its threshold. Keep the durable cycle retryable so it can resume after
		// that cooldown; a later eligibility check pauses it if the account was
		// permanently quarantined by the shared policy.
		if job.AttemptCount >= s.options.MaxAttempts {
			if s.markBlocked(ctx, claim, now, statusCode, "attempt_limit", "warmup attempt limit reached") {
				_ = s.handleAuthFailure(ctx, authFailure)
			}
			return
		}
		if s.markRetry(ctx, claim, now, now.Add(warmupBackoff(job.AttemptCount-1)), statusCode,
			string(authFailure.Disposition), "retryable forbidden response") {
			_ = s.handleAuthFailure(ctx, authFailure)
		}
		return
	}
	if statusCode == http.StatusTooManyRequests {
		reset := result.ResetAt
		if reset == nil {
			reset = result.ObservedResetAt
		}
		authoritativeReset, _, reconcileErr := s.reconcileBlockedReset(ctx, account.ID)
		if errors.Is(reconcileErr, errWarmupFiveHourWindowUnsupported) {
			s.markUnsupportedFiveHourWindow(ctx, claim, now)
			return
		}
		if authoritativeReset != nil && (reset == nil || authoritativeReset.After(*reset)) {
			reset = authoritativeReset
		}
		if job.AttemptCount >= s.options.MaxAttempts {
			s.markBlocked(ctx, claim, now, statusCode, "attempt_limit", "warmup attempt limit reached")
			return
		}
		if reset != nil && reset.After(now) {
			next := s.warmupDueAt(warmupResetCycleKey(*reset), reset)
			s.markRateLimited(ctx, claim, now, next, reset, statusCode, "rate_limited")
			return
		}
		s.markRetry(ctx, claim, now, now.Add(warmupBackoff(job.AttemptCount-1)), statusCode, warmupStatusCode(statusCode), "rate limit reset unavailable")
		return
	}
	if isWarmupBlockedError(probeErr, statusCode) {
		code := warmupBlockedCode(probeErr, statusCode)
		if s.markBlocked(ctx, claim, now, statusCode, code, "warmup blocked") {
			s.handleTerminalAuthFailure(ctx, claim, authFailure, statusCode, code)
		}
		return
	}
	if reset, ok := warmupUsageConfirmedSuccess(result, now); ok {
		if s.markSuccess(ctx, claim, now, reset, statusCode, "completed", warmupSuccessEvidence(result)) {
			s.enqueueNextContinuousCycle(ctx, account, reset)
		}
		return
	}
	if probeErr != nil {
		// A 5xx is an HTTP response, but it does not prove that the upstream
		// failed before executing the POST.  OpenAI may have accepted the
		// request and then failed while producing the response.  Route every
		// server error through the same passive reconciliation/fencing path as
		// timeout and EOF; retrying it directly can consume the window twice.
		if statusCode >= 500 && statusCode <= 599 {
			s.handleAmbiguousProbe(ctx, claim, account, result, statusCode, now)
			return
		}
		// A received non-2xx status is a definitive rejection, even if reading
		// the bounded response body later failed. Only status-less and 2xx
		// transport ambiguity can be possibly_sent.
		if statusCode > 0 && (statusCode < 200 || statusCode >= 300) {
			if job.AttemptCount >= s.options.MaxAttempts {
				s.markBlocked(ctx, claim, now, statusCode, "attempt_limit", "warmup attempt limit reached")
				return
			}
			s.markRetry(ctx, claim, now, now.Add(warmupBackoff(job.AttemptCount-1)), statusCode, warmupStatusCode(statusCode), "transient upstream failure")
			return
		}
		if isWarmupUncertainError(probeErr, result) {
			s.handleAmbiguousProbe(ctx, claim, account, result, statusCode, now)
			return
		}
		code := warmupErrorCode(probeErr)
		if job.AttemptCount >= s.options.MaxAttempts {
			s.markBlocked(ctx, claim, now, statusCode, "attempt_limit", "warmup attempt limit reached")
			return
		}
		s.markRetry(ctx, claim, now, now.Add(warmupBackoff(job.AttemptCount-1)), statusCode, code, "transient upstream failure")
		return
	}
	if result == nil {
		s.handleAmbiguousProbe(ctx, claim, account, nil, 0, now)
		return
	}
	statusCode = result.StatusCode
	if statusCode >= 500 && statusCode <= 599 {
		// Keep the no-error test-double path consistent with the real probe path:
		// any received 5xx is potentially post-acceptance and needs passive
		// reconciliation before another synthetic POST is considered.
		s.handleAmbiguousProbe(ctx, claim, account, result, statusCode, now)
		return
	}
	reset := result.ResetAt
	if reset == nil {
		reset = result.ObservedResetAt
	}
	if statusCode >= 200 && statusCode < 300 && warmupTerminalSucceeded(result) && warmupAttemptResetAdvanced(reset, job, now) {
		if idlePreflight && result.resetFromRelativeHeader {
			// A relative reset is reconstructed at response-read time. For an idle
			// rolling now+5h projection it will therefore be slightly later than the
			// preflight reset even when this model did not start the target window.
			// Reuse the passive fixed-vs-rolling observation fence before success.
			s.handleAmbiguousProbe(ctx, claim, account, result, statusCode, now)
			return
		}
		ok := s.markSuccess(ctx, claim, now, reset, statusCode, "completed", warmupSuccessEvidence(result))
		if ok {
			s.enqueueNextContinuousCycle(ctx, account, reset)
		}
		return
	}
	if (statusCode >= 200 && statusCode < 300) || result.EOF || !result.Terminal {
		s.handleAmbiguousProbe(ctx, claim, account, result, statusCode, now)
		return
	}
	if job.AttemptCount >= s.options.MaxAttempts {
		s.markBlocked(ctx, claim, now, statusCode, "attempt_limit", "warmup attempt limit reached")
		return
	}
	s.markRetry(ctx, claim, now, now.Add(warmupBackoff(job.AttemptCount-1)), statusCode, warmupStatusCode(statusCode), "upstream response")
}

func (s *OpenAIWindowWarmupService) handleAmbiguousProbe(ctx context.Context, claim OpenAIWindowWarmupClaim, account *Account, result *OpenAIWindowProbeResult, statusCode int, now time.Time) {
	job := claim.Job
	terminal := statusCode >= 200 && statusCode < 300 && warmupTerminalSucceeded(result)
	reset, authoritative, active, reconcileErr := s.reconcileFiveHourObservation(ctx, account.ID, job)
	if errors.Is(reconcileErr, errWarmupFiveHourWindowUnsupported) {
		s.markUnsupportedFiveHourWindow(ctx, claim, now)
		return
	}
	if reconcileErr == nil && authoritative && active && reset != nil && reset.After(now) {
		if terminal && warmupAttemptResetAdvanced(reset, job, now) {
			if s.markSuccess(ctx, claim, now, reset, statusCode, "completed_reconciled", warmupSuccessEvidence(result)) {
				s.enqueueNextContinuousCycle(ctx, account, reset)
			}
		} else {
			// Activity proves replay is unsafe, but without both terminal and
			// advanced-reset evidence it must not count as warmup success.
			s.completeSuppressedCycle(ctx, claim, account, reset, "possibly_sent_reconciled")
		}
		return
	}
	if job.AttemptCount >= s.options.MaxAttempts {
		s.markBlocked(ctx, claim, now, statusCode, "attempt_limit_uncertain", "ambiguous warmup reached attempt limit")
		return
	}
	code := "possibly_sent"
	if terminal {
		code = "completed_reset_unconfirmed"
	}
	next := now.Add(warmupBackoff(job.AttemptCount - 1))
	minimumNext := now.Add(openAIWindowWarmupUncertainObservationInterval)
	if next.Before(minimumNext) {
		next = minimumNext
	}
	s.markUncertain(ctx, claim, now, next, statusCode, code, "upstream outcome requires passive reset reconciliation",
		OpenAIWindowWarmupUncertainEvidence{
			Authoritative: reconcileErr == nil && authoritative,
			ResetAt:       reset,
			Terminal:      terminal,
		})
}

func (s *OpenAIWindowWarmupService) markStarted(ctx context.Context, claim *OpenAIWindowWarmupClaim, at time.Time, evidence OpenAIWindowWarmupStartEvidence) bool {
	if claim == nil || claim.Job == nil {
		return false
	}
	ok, err := s.repo.MarkStarted(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, at, evidence)
	if err != nil || !ok {
		return false
	}
	startedAt := at.UTC()
	claim.Job.AttemptCount++
	claim.Job.SentAt = &startedAt
	claim.Job.LastAttemptAt = &startedAt
	claim.Job.PreflightResetAt = cloneWarmupTime(evidence.ResetAt)
	claim.Job.PreflightObservedAt = &startedAt
	s.metrics.started.Add(1)
	s.recordWarmupAudit(claim.Job, OpenAIWindowWarmupStateRunning, 0, "started", claim.Job.ObservedResetAt)
	return true
}

func (s *OpenAIWindowWarmupService) markSuccess(ctx context.Context, claim OpenAIWindowWarmupClaim, at time.Time, resetAt *time.Time, status int, code string, evidence openAIWindowWarmupSuccessEvidence) bool {
	ok, err := s.repo.MarkSuccess(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, at, resetAt, status, code)
	if err != nil || !ok {
		return false
	}
	if evidence.Usage.Positive() {
		slog.Info("openai_window_warmup_usage_confirmed",
			"job_id", claim.Job.ID,
			"account_id", claim.Job.AccountID,
			"input_tokens", evidence.Usage.InputTokens,
			"output_tokens", evidence.Usage.OutputTokens,
			"total_tokens", evidence.Usage.TotalTokens,
			"request_id", evidence.RequestID,
			"response_id", evidence.ResponseID,
		)
	}
	if resetAt != nil {
		_, _ = s.repo.ProjectSuccessReset(ctx, claim.Job.AccountID, claim.Job.IdentityGeneration, at, *resetAt)
	}
	s.metrics.success.Add(1)
	s.recordWarmupAudit(claim.Job, OpenAIWindowWarmupStateCompleted, status, code, resetAt)
	return true
}

func (s *OpenAIWindowWarmupService) cancelStartedBeforeSend(ctx context.Context, claim OpenAIWindowWarmupClaim, at time.Time, reason string) bool {
	if s == nil || s.repo == nil || claim.Job == nil {
		return false
	}
	startedAt := time.Time{}
	if claim.Job.LastAttemptAt != nil {
		startedAt = claim.Job.LastAttemptAt.UTC()
	}
	reservation := OpenAIWindowWarmupSendReservation{
		PreviousProjection: cloneWarmupProjectionSnapshot(claim.PreviousProjection),
		StartedAt:          startedAt,
	}
	ok, err := s.repo.CancelStartedBeforeSend(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, reservation, at, reason)
	if err != nil || !ok {
		return false
	}
	previous := reservation.PreviousProjection
	claim.Job.AttemptCount = previous.AttemptCount
	claim.Job.SentAt = cloneWarmupTime(previous.SentAt)
	claim.Job.LastAttemptAt = cloneWarmupTime(previous.LastAttemptAt)
	claim.Job.PreflightResetAt = cloneWarmupTime(previous.PreflightResetAt)
	claim.Job.PreflightObservedAt = cloneWarmupTime(previous.PreflightObservedAt)
	claim.Job.UncertainObservedResetAt = cloneWarmupTime(previous.UncertainObservedResetAt)
	claim.Job.UncertainObservedAt = cloneWarmupTime(previous.UncertainObservedAt)
	claim.Job.UncertainTerminalObserved = previous.UncertainTerminalObserved
	s.recordWarmupAudit(claim.Job, OpenAIWindowWarmupStatePending, 0, reason, claim.Job.ObservedResetAt)
	return true
}

func cloneWarmupProjectionSnapshot(value OpenAIWindowWarmupProjectionSnapshot) OpenAIWindowWarmupProjectionSnapshot {
	value.SentAt = cloneWarmupTime(value.SentAt)
	value.LastAttemptAt = cloneWarmupTime(value.LastAttemptAt)
	value.PreflightResetAt = cloneWarmupTime(value.PreflightResetAt)
	value.PreflightObservedAt = cloneWarmupTime(value.PreflightObservedAt)
	value.UncertainObservedResetAt = cloneWarmupTime(value.UncertainObservedResetAt)
	value.UncertainObservedAt = cloneWarmupTime(value.UncertainObservedAt)
	return value
}

func (s *OpenAIWindowWarmupService) markRetry(ctx context.Context, claim OpenAIWindowWarmupClaim, at, next time.Time, status int, code, message string) bool {
	ok, err := s.repo.MarkRetry(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, at, next, status, code, message)
	if err != nil || !ok {
		return false
	}
	s.metrics.retry.Add(1)
	s.recordWarmupAudit(claim.Job, OpenAIWindowWarmupStateRetrying, status, code, claim.Job.ObservedResetAt)
	return true
}

func (s *OpenAIWindowWarmupService) handleUsageObservationFailure(ctx context.Context, claim OpenAIWindowWarmupClaim, at time.Time, retryState, phase string, observationErr error) bool {
	status, code, terminalState := classifyWarmupUsageObservationError(observationErr)
	authFailure := openAIWindowWarmupAuthFailureFromError(observationErr)
	if status == http.StatusForbidden && warmupAuthFailureIsRetryableForbidden(authFailure) {
		terminalState = ""
		code = string(authFailure.Disposition)
	}
	if code == "usage_observation_failed" {
		code = "usage_" + phase + "_failed"
	}
	state := retryState
	if terminalState != "" {
		state = terminalState
	} else if claim.Job.AttemptCount+1 >= s.options.MaxAttempts {
		state = OpenAIWindowWarmupStateBlocked
		code = "attempt_limit_" + phase
	}
	next := at
	if state == OpenAIWindowWarmupStateRetrying || state == OpenAIWindowWarmupStateUncertain {
		next = at.Add(warmupBackoff(claim.Job.AttemptCount))
	}
	message := "authoritative usage " + phase + " failed"
	ok, err := s.repo.MarkObservationFailure(
		ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, at, next, state, status, code, message,
	)
	if err != nil || !ok {
		return false
	}
	claim.Job.AttemptCount++
	switch state {
	case OpenAIWindowWarmupStateBlocked, OpenAIWindowWarmupStateBlockedConfig:
		s.metrics.failed.Add(1)
	case OpenAIWindowWarmupStateUncertain:
		s.metrics.uncertain.Add(1)
	default:
		s.metrics.retry.Add(1)
	}
	s.recordWarmupAudit(claim.Job, state, status, code, claim.Job.ObservedResetAt)
	if state == OpenAIWindowWarmupStateBlocked || state == OpenAIWindowWarmupStateBlockedConfig {
		s.handleTerminalAuthFailure(ctx, claim, authFailure, status, code)
	} else {
		_ = s.handleAuthFailure(ctx, authFailure)
	}
	return true
}

func openAIWindowWarmupProbeAuthFailure(result *OpenAIWindowProbeResult, err error) *OpenAIWindowWarmupAuthFailure {
	if result != nil && result.AuthFailure != nil {
		return cloneOpenAIWindowWarmupAuthFailure(result.AuthFailure)
	}
	return openAIWindowWarmupAuthFailureFromError(err)
}

func warmupAuthFailureIsRetryableForbidden(failure *OpenAIWindowWarmupAuthFailure) bool {
	return failure != nil && failure.StatusCode == http.StatusForbidden &&
		(failure.Disposition == OpenAIWindowWarmupAuthForbidden ||
			failure.Disposition == OpenAIWindowWarmupAuthForbiddenHTML)
}

func (s *OpenAIWindowWarmupService) handleAuthFailure(ctx context.Context, failure *OpenAIWindowWarmupAuthFailure) error {
	if s == nil || failure == nil || s.options.AuthFailureHandler == nil {
		return nil
	}
	if err := s.options.AuthFailureHandler.HandleOpenAIWindowWarmupAuthFailure(ctx, *cloneOpenAIWindowWarmupAuthFailure(failure)); err != nil {
		if errors.Is(err, ErrOpenAIWindowWarmupCredentialsChanged) {
			return err
		}
		slog.Warn("openai_window_warmup_auth_state_update_failed",
			"account_id", failure.AccountID,
			"status", failure.StatusCode,
			"disposition", failure.Disposition,
			"error", err,
		)
		return err
	}
	return nil
}

func (s *OpenAIWindowWarmupService) handleTerminalAuthFailure(
	ctx context.Context,
	claim OpenAIWindowWarmupClaim,
	failure *OpenAIWindowWarmupAuthFailure,
	status int,
	code string,
) {
	if failure == nil || claim.Job == nil {
		return
	}
	authStateErr := s.handleAuthFailure(ctx, failure)
	if authStateErr == nil {
		return
	}
	// A definitive 401/403 was rejected before producing content, so one
	// bounded retry is safe. The repository CAS below refuses to rearm a job
	// changed by an administrator, a later attempt, or a later cycle.
	if claim.Job.AttemptCount >= s.options.MaxAttempts {
		return
	}
	blockedState := OpenAIWindowWarmupStateBlocked
	if status == http.StatusBadRequest || status == http.StatusNotFound || code == "blocked_config" {
		blockedState = OpenAIWindowWarmupStateBlockedConfig
	}
	next := s.now().Add(warmupBackoff(claim.Job.AttemptCount))
	retryCode := "account_state_update_failed"
	if errors.Is(authStateErr, ErrOpenAIWindowWarmupCredentialsChanged) {
		retryCode = "credentials_changed"
	}
	ok, err := s.repo.RequeueAuthStateUpdateFailure(ctx, OpenAIWindowWarmupAuthStateRetry{
		JobID:           claim.Job.ID,
		CycleGeneration: claim.Job.CycleGeneration,
		AttemptCount:    claim.Job.AttemptCount,
		BlockedState:    blockedState,
		StatusCode:      status,
		ErrorCode:       code,
		RetryCode:       retryCode,
		NextAttemptAt:   next,
	})
	if err != nil {
		slog.Warn("openai_window_warmup_auth_state_retry_failed",
			"job_id", claim.Job.ID,
			"cycle_generation", claim.Job.CycleGeneration,
			"error", err,
		)
		return
	}
	if ok {
		s.metrics.retry.Add(1)
		s.recordWarmupAudit(claim.Job, OpenAIWindowWarmupStateRetrying, status, retryCode, claim.Job.ObservedResetAt)
	}
}

func classifyWarmupUsageObservationError(err error) (status int, code, terminalState string) {
	status = infraerrors.Code(err)
	switch status {
	case http.StatusUnauthorized:
		return status, "needs_reauth", OpenAIWindowWarmupStateBlocked
	case http.StatusForbidden:
		return status, "blocked", OpenAIWindowWarmupStateBlocked
	case http.StatusBadRequest, http.StatusNotFound:
		return status, "blocked_config", OpenAIWindowWarmupStateBlockedConfig
	default:
		return status, "usage_observation_failed", ""
	}
}

func (s *OpenAIWindowWarmupService) markRateLimited(ctx context.Context, claim OpenAIWindowWarmupClaim, at, next time.Time, resetAt *time.Time, status int, code string) bool {
	ok, err := s.repo.MarkRateLimited(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, at, next, resetAt, status, code)
	if err != nil || !ok {
		return false
	}
	s.metrics.retry.Add(1)
	s.recordWarmupAudit(claim.Job, OpenAIWindowWarmupStateArmed, status, code, resetAt)
	return true
}

func (s *OpenAIWindowWarmupService) markUncertain(ctx context.Context, claim OpenAIWindowWarmupClaim, at, next time.Time, status int, code, message string, evidence OpenAIWindowWarmupUncertainEvidence) bool {
	ok, err := s.repo.MarkUncertain(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, at, next, status, code, message, evidence)
	if err != nil || !ok {
		return false
	}
	s.metrics.uncertain.Add(1)
	s.recordWarmupAudit(claim.Job, OpenAIWindowWarmupStateUncertain, status, code, claim.Job.ObservedResetAt)
	return true
}

func (s *OpenAIWindowWarmupService) markBlocked(ctx context.Context, claim OpenAIWindowWarmupClaim, at time.Time, status int, code, message string) bool {
	ok, err := s.repo.MarkBlocked(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, at, status, code, message)
	if err != nil || !ok {
		return false
	}
	s.metrics.failed.Add(1)
	state := OpenAIWindowWarmupStateBlocked
	if status == http.StatusBadRequest || status == http.StatusNotFound || code == "blocked_config" {
		state = OpenAIWindowWarmupStateBlockedConfig
	}
	s.recordWarmupAudit(claim.Job, state, status, code, claim.Job.ObservedResetAt)
	return true
}

func (s *OpenAIWindowWarmupService) markUnsupportedFiveHourWindow(ctx context.Context, claim OpenAIWindowWarmupClaim, at time.Time) bool {
	ok, err := s.repo.MarkUnsupportedFiveHourWindow(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, at)
	if err != nil || !ok {
		return false
	}
	s.recordWarmupAudit(claim.Job, OpenAIWindowWarmupStateFailed, 0, OpenAIWindowWarmupErrorFiveHourWindowUnsupported, nil)
	return true
}

func (s *OpenAIWindowWarmupService) markPaused(ctx context.Context, claim OpenAIWindowWarmupClaim, at time.Time, reason string) bool {
	ok, err := s.repo.MarkPaused(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, at, reason)
	if err != nil || !ok {
		return false
	}
	s.recordWarmupAudit(claim.Job, OpenAIWindowWarmupStatePaused, 0, reason, claim.Job.ObservedResetAt)
	return true
}

func (s *OpenAIWindowWarmupService) reschedule(ctx context.Context, claim OpenAIWindowWarmupClaim, next time.Time, state string, resetAt *time.Time) bool {
	ok, err := s.repo.Reschedule(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, next, state, resetAt)
	if err != nil || !ok {
		return false
	}
	s.recordWarmupAudit(claim.Job, state, 0, "rescheduled", resetAt)
	return true
}

func (s *OpenAIWindowWarmupService) recordWarmupAudit(job *OpenAIWindowWarmupJob, state string, upstreamStatus int, errorCode string, resetAt *time.Time) {
	if s == nil || s.audit == nil || job == nil {
		return
	}
	auditStatus := http.StatusOK
	switch state {
	case OpenAIWindowWarmupStateRetrying, OpenAIWindowWarmupStateUncertain, OpenAIWindowWarmupStatePossiblySent:
		auditStatus = http.StatusAccepted
	case OpenAIWindowWarmupStateBlocked, OpenAIWindowWarmupStateBlockedConfig:
		auditStatus = http.StatusConflict
	}
	extra := map[string]any{
		"account_id":       job.AccountID,
		"job_id":           job.ID,
		"quota_scope":      job.QuotaScope,
		"cycle_key":        job.CycleKey,
		"cycle_generation": job.CycleGeneration,
		"trigger":          job.Trigger,
		"state":            state,
		"status_code":      upstreamStatus,
		"error_code":       errorCode,
	}
	if resetAt != nil {
		extra["reset_at"] = resetAt.UTC().Format(time.RFC3339Nano)
	}
	s.audit.Record(&AuditLog{
		ActorEmail: "system", ActorRole: "system", AuthMethod: "system",
		Action: AuditActionOpenAIWindowWarmup, Method: "SYSTEM",
		Path:       fmt.Sprintf("/system/openai/accounts/%d/window-warmup/jobs/%d", job.AccountID, job.ID),
		StatusCode: auditStatus, Extra: extra,
	})
}

func (s *OpenAIWindowWarmupService) reconcileFiveHourObservation(ctx context.Context, accountID int64, job *OpenAIWindowWarmupJob) (*time.Time, bool, bool, error) {
	usage, err := s.queryWarmupUsage(ctx, accountID)
	if err != nil {
		return nil, false, false, err
	}
	if warmupFiveHourCapabilityForUsage(usage) == warmupFiveHourUnsupported {
		return nil, false, false, errWarmupFiveHourWindowUnsupported
	}
	observation, authoritative := warmupFiveHourObservation(usage)
	if !authoritative {
		return nil, false, false, errors.New("authoritative five-hour usage window unavailable")
	}
	account, err := s.accounts.GetByID(ctx, accountID)
	if err != nil || account == nil {
		if err == nil {
			err = errors.New("account unavailable during usage reconciliation")
		}
		return nil, false, false, err
	}
	active := observation.UsedPercent > 0 || warmupAccountUsedSinceCycle(account, job)
	return observation.ResetAt, true, active, nil
}

func (s *OpenAIWindowWarmupService) reconcileBlockedReset(ctx context.Context, accountID int64) (*time.Time, bool, error) {
	usage, err := s.queryWarmupUsage(ctx, accountID)
	if err != nil {
		return nil, false, err
	}
	if warmupFiveHourCapabilityForUsage(usage) == warmupFiveHourUnsupported {
		return nil, false, errWarmupFiveHourWindowUnsupported
	}
	fiveHour, fiveHourAuthoritative := warmupFiveHourObservation(usage)
	weekly, weeklyAuthoritative := warmupBlockedWeeklyObservation(usage)
	if !fiveHourAuthoritative || !weeklyAuthoritative {
		return nil, false, errors.New("authoritative blocked-window usage unavailable")
	}
	reset := cloneWarmupTime(fiveHour.ResetAt)
	if weekly.Blocked && weekly.ResetAt != nil && (reset == nil || weekly.ResetAt.After(*reset)) {
		reset = cloneWarmupTime(weekly.ResetAt)
	}
	return reset, true, nil
}

func (s *OpenAIWindowWarmupService) queryWarmupUsage(ctx context.Context, accountID int64) (*OpenAIQuotaUsage, error) {
	if s.options.UsageReconciler == nil {
		return nil, errors.New("warmup usage reconciler is not configured")
	}
	usage, err := s.options.UsageReconciler.QueryUsage(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if usage == nil {
		return nil, errors.New("warmup usage reconciler returned an empty response")
	}
	return usage, nil
}

func (s *OpenAIWindowWarmupService) completeSuppressedCycle(ctx context.Context, claim OpenAIWindowWarmupClaim, account *Account, reset *time.Time, code string) {
	if reset == nil {
		return
	}
	ok, err := s.repo.MarkSuppressed(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, s.now(), reset, code)
	if err == nil && ok {
		if isRealTrafficWarmupSuppression(code) {
			s.metrics.suppressed.Add(1)
		}
		s.recordWarmupAudit(claim.Job, OpenAIWindowWarmupStateCompleted, 0, code, reset)
	}
	if ok {
		s.enqueueNextContinuousCycle(ctx, account, reset)
	}
}

func isRealTrafficWarmupSuppression(code string) bool {
	switch strings.TrimSpace(code) {
	case "real_traffic_suppressed", "usage_preflight_suppressed", "mark_started_reset_cas":
		return true
	default:
		return false
	}
}

func (s *OpenAIWindowWarmupService) enqueueNextContinuousCycle(ctx context.Context, account *Account, reset *time.Time) {
	if account == nil || reset == nil {
		return
	}
	if s.accounts != nil {
		latest, err := s.accounts.GetByID(ctx, account.ID)
		if err != nil || latest == nil {
			return
		}
		account = latest
	}
	if !warmupAccountEligibleAt(account, s.now()) || OpenAIWindowWarmupPolicyForAccount(account) != OpenAIWindowWarmupPolicyContinuous {
		return
	}
	if !s.accountAllowed(ctx, account.ID) {
		return
	}
	cycleGeneration := warmupAccountGeneration(account)
	cycleKey := warmupResetCycleKey(*reset, cycleGeneration)
	job, inserted, err := s.repo.Enqueue(ctx, OpenAIWindowWarmupEnqueue{
		AccountID: account.ID, QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: cycleKey, CycleGeneration: cycleGeneration,
		IdentityGeneration: account.OpenAIWarmupIdentityGeneration,
		Trigger:            OpenAIWindowWarmupTriggerReset, ObservedResetAt: reset,
		NextAttemptAt: s.warmupDueAt(cycleKey, reset),
	})
	_ = job
	if err == nil {
		if inserted {
			s.metrics.enqueued.Add(1)
			if job != nil {
				s.recordWarmupAudit(job, job.State, 0, "enqueued", job.ObservedResetAt)
			}
		} else {
			s.metrics.duplicates.Add(1)
		}
	}
}

type warmupWindowObservation struct {
	ResetAt     *time.Time
	UsedPercent float64
}

type warmupFiveHourCapability uint8

const (
	warmupFiveHourUnknown warmupFiveHourCapability = iota
	warmupFiveHourAvailable
	warmupFiveHourUnsupported
)

var errWarmupFiveHourWindowUnsupported = errors.New(OpenAIWindowWarmupErrorFiveHourWindowUnsupported)

// warmupFiveHourCapabilityForUsage only excludes the established weekly-only
// response shape. Missing or ambiguous fields remain unknown and continue
// through the existing fail-closed observation path.
func warmupFiveHourCapabilityForUsage(usage *OpenAIQuotaUsage) warmupFiveHourCapability {
	if usage == nil || usage.RateLimit == nil {
		return warmupFiveHourUnknown
	}
	rateLimit := usage.RateLimit
	if rateLimit.primaryWindowPresent && rateLimit.PrimaryWindow == nil {
		return warmupFiveHourAvailable
	}
	windows := []*OpenAIRateLimitWindow{rateLimit.PrimaryWindow, rateLimit.SecondaryWindow}
	for _, window := range windows {
		if window != nil && window.LimitWindowSeconds > 0 && window.LimitWindowSeconds <= 6*60*60 {
			return warmupFiveHourAvailable
		}
	}
	if !rateLimit.primaryWindowPresent || rateLimit.PrimaryWindow == nil || rateLimit.PrimaryWindow.LimitWindowSeconds <= 6*60*60 {
		return warmupFiveHourUnknown
	}
	for _, window := range windows {
		if window == nil {
			continue
		}
		if window.LimitWindowSeconds <= 6*60*60 || !warmupUsagePercentAuthoritative(window) {
			return warmupFiveHourUnknown
		}
		if window.UsedPercent >= 100 {
			reset, valid := warmupUsageWindowResetAt(usage, window)
			if !valid || reset == nil {
				return warmupFiveHourUnknown
			}
		}
	}
	return warmupFiveHourUnsupported
}

// warmupFiveHourObservation distinguishes an authoritative empty 5h slot from
// an unknown/schema-drift response. A 0% window may still carry a rolling
// now+5h reset; activity is decided separately using used_percent and the
// durable account last_used_at.
func warmupFiveHourObservation(usage *OpenAIQuotaUsage) (warmupWindowObservation, bool) {
	if usage == nil || usage.RateLimit == nil {
		return warmupWindowObservation{}, false
	}
	window, found := warmupUsageWindow(usage.RateLimit, true)
	if !found {
		return warmupWindowObservation{}, warmupUsageWindowExplicitlyEmpty(usage.RateLimit, true)
	}
	if !warmupUsagePercentAuthoritative(window) {
		return warmupWindowObservation{}, false
	}
	reset, valid := warmupUsageWindowResetAt(usage, window)
	if !valid {
		return warmupWindowObservation{}, false
	}
	return warmupWindowObservation{ResetAt: reset, UsedPercent: window.UsedPercent}, true
}

type warmupWeeklyBlockObservation struct {
	Blocked bool
	ResetAt *time.Time
}

func warmupBlockedWeeklyObservation(usage *OpenAIQuotaUsage) (warmupWeeklyBlockObservation, bool) {
	if usage == nil || usage.RateLimit == nil {
		return warmupWeeklyBlockObservation{}, false
	}
	window, found := warmupUsageWindow(usage.RateLimit, false)
	if !found {
		// secondary_window is optional. Missing and explicit null both mean that
		// this response exposes no weekly dimension; a valid primary window may
		// still authorize the probe. A present but malformed weekly window remains
		// fail-closed below.
		return warmupWeeklyBlockObservation{}, !usage.RateLimit.secondaryWindowPresent ||
			warmupUsageWindowExplicitlyEmpty(usage.RateLimit, false)
	}
	if !warmupUsagePercentAuthoritative(window) {
		return warmupWeeklyBlockObservation{}, false
	}
	if window.UsedPercent < 100 {
		return warmupWeeklyBlockObservation{}, true
	}
	reset, valid := warmupUsageWindowResetAt(usage, window)
	if !valid || reset == nil {
		return warmupWeeklyBlockObservation{}, false
	}
	return warmupWeeklyBlockObservation{Blocked: true, ResetAt: reset}, true
}

func warmupResetFromUsage(usage *OpenAIQuotaUsage, includeBlockedSevenDay bool) *time.Time {
	fiveHour, authoritative := warmupFiveHourObservation(usage)
	if !authoritative {
		return nil
	}
	if !includeBlockedSevenDay {
		return fiveHour.ResetAt
	}
	weekly, weeklyAuthoritative := warmupBlockedWeeklyObservation(usage)
	if !weeklyAuthoritative {
		return nil
	}
	if weekly.Blocked && weekly.ResetAt != nil && (fiveHour.ResetAt == nil || weekly.ResetAt.After(*fiveHour.ResetAt)) {
		return weekly.ResetAt
	}
	return fiveHour.ResetAt
}

func warmupUsagePercentAuthoritative(window *OpenAIRateLimitWindow) bool {
	return window != nil && window.UsedPercent >= 0 && (window.usedPercentPresent || window.UsedPercent != 0)
}

func warmupUsageWindowExplicitlyEmpty(rateLimit *OpenAIRateLimit, fiveHour bool) bool {
	if rateLimit == nil {
		return false
	}
	if fiveHour {
		return rateLimit.primaryWindowPresent && rateLimit.PrimaryWindow == nil
	}
	return rateLimit.secondaryWindowPresent && rateLimit.SecondaryWindow == nil
}

func warmupUsageWindow(rateLimit *OpenAIRateLimit, fiveHour bool) (*OpenAIRateLimitWindow, bool) {
	if rateLimit == nil {
		return nil, false
	}
	for index, window := range []*OpenAIRateLimitWindow{rateLimit.PrimaryWindow, rateLimit.SecondaryWindow} {
		if window == nil {
			continue
		}
		isFiveHour := window.LimitWindowSeconds > 0 && window.LimitWindowSeconds <= 6*60*60
		isSevenDay := window.LimitWindowSeconds > 6*60*60
		if window.LimitWindowSeconds == 0 {
			// Older upstream responses omitted duration but consistently placed the
			// five-hour window first and the weekly window second.
			isFiveHour = index == 0
			isSevenDay = index == 1
		}
		if (fiveHour && isFiveHour) || (!fiveHour && isSevenDay) {
			return window, true
		}
	}
	return nil, false
}

func warmupUsageWindowResetAt(usage *OpenAIQuotaUsage, window *OpenAIRateLimitWindow) (*time.Time, bool) {
	if window == nil {
		return nil, true
	}
	if window.ResetAt > 0 {
		reset := time.Unix(window.ResetAt, 0).UTC()
		return &reset, true
	}
	if window.ResetAfterSeconds > 0 {
		if usage == nil || usage.FetchedAt <= 0 {
			return nil, false
		}
		reset := time.Unix(usage.FetchedAt, 0).UTC().Add(time.Duration(window.ResetAfterSeconds) * time.Second)
		return &reset, true
	}
	if window.resetAtPresent || window.resetAfterSecondsPresent {
		return nil, true
	}
	return nil, false
}

func warmupTerminalSucceeded(result *OpenAIWindowProbeResult) bool {
	if result == nil || !result.Terminal {
		return false
	}
	switch strings.TrimSpace(result.TerminalType) {
	case "response.completed", "response.done":
		return true
	default:
		return false
	}
}

func warmupUsageConfirmedSuccess(result *OpenAIWindowProbeResult, now time.Time) (*time.Time, bool) {
	if result == nil || result.StatusCode < http.StatusOK || result.StatusCode >= http.StatusMultipleChoices ||
		!warmupTerminalSucceeded(result) || !result.Usage.Positive() {
		return nil, false
	}
	reset := result.ResetAt
	if reset == nil {
		reset = result.ObservedResetAt
	}
	if reset == nil || !reset.After(now) {
		return nil, false
	}
	return reset, true
}

func warmupSuccessEvidence(result *OpenAIWindowProbeResult) openAIWindowWarmupSuccessEvidence {
	if result == nil {
		return openAIWindowWarmupSuccessEvidence{}
	}
	return openAIWindowWarmupSuccessEvidence{
		Usage:      cloneOpenAIWindowWarmupTokenUsage(result.Usage),
		ResponseID: result.ResponseID,
		RequestID:  result.RequestID,
	}
}

func warmupResetAdvanced(reset, expected *time.Time, now time.Time) bool {
	if reset == nil || !reset.After(now) {
		return false
	}
	return expected == nil || reset.After(*expected)
}

// warmupAttemptResetAdvanced compares post-send evidence with the exact
// authoritative preflight observation. Rows created before that evidence was
// persisted may fall back to their durable cycle reset, but a row with neither
// baseline cannot claim success merely because the response contains a future
// reset.
func warmupAttemptResetAdvanced(reset *time.Time, job *OpenAIWindowWarmupJob, now time.Time) bool {
	if job == nil {
		return false
	}
	if job.PreflightObservedAt != nil {
		return warmupResetAdvanced(reset, job.PreflightResetAt, now)
	}
	if job.ObservedResetAt == nil {
		return false
	}
	return warmupResetAdvanced(reset, job.ObservedResetAt, now)
}

func warmupAccountUsedSinceCycle(account *Account, job *OpenAIWindowWarmupJob) bool {
	if account == nil || account.LastUsedAt == nil || job == nil {
		return false
	}
	boundary := job.CreatedAt
	if job.ObservedResetAt != nil && (boundary.IsZero() || job.ObservedResetAt.After(boundary)) {
		boundary = *job.ObservedResetAt
	}
	return boundary.IsZero() || account.LastUsedAt.After(boundary)
}

func (s *OpenAIWindowWarmupService) killSwitchEnabled(ctx context.Context) bool {
	if s.options.KillSwitch == nil {
		return true
	}
	ok, err := s.options.KillSwitch.Enabled(ctx)
	return err == nil && ok
}

func (s *OpenAIWindowWarmupService) checkWarmupSendGuard(ctx context.Context, accountID int64) error {
	if s == nil || !s.killSwitchEnabled(ctx) {
		return ErrOpenAIWindowWarmupSendGuardClosed
	}
	if !s.accountAllowed(ctx, accountID) {
		return ErrOpenAIWindowWarmupSendGuardClosed
	}
	return nil
}

func (s *OpenAIWindowWarmupService) refreshQueueStats(ctx context.Context, accountIDs []int64) {
	if s == nil || s.repo == nil {
		return
	}
	stats, err := s.repo.QueueStats(ctx, accountIDs)
	if err == nil {
		s.applyQueueStats(stats)
	}
}

func (s *OpenAIWindowWarmupService) applyQueueStats(stats OpenAIWindowWarmupQueueStats) {
	if s == nil {
		return
	}
	// PostgreSQL is authoritative for this total so trigger-created import jobs
	// are visible even though no in-process enqueue callback ran.
	s.metrics.enqueued.Store(stats.Enqueued)
	s.metrics.due.Store(stats.Due)
	s.metrics.oldestDueAge.Store(stats.OldestDueAgeSeconds)
	s.metrics.resetLag.Store(stats.ResetLagSeconds)
	// This is the database-wide leased count for the configured cohort, not the
	// process-local goroutine count used for worker capacity above.
	s.metrics.inflight.Store(stats.Inflight)
}

func (s *OpenAIWindowWarmupService) configuredWarmupAllowlist(ctx context.Context) (map[int64]struct{}, bool, bool) {
	if s == nil || s.options.Allowlist == nil {
		return nil, false, false
	}
	accountIDs, err := s.options.Allowlist.AccountIDs(ctx)
	if err != nil {
		return nil, false, false
	}
	allowAll := len(accountIDs) == 0
	configured := make(map[int64]struct{}, len(accountIDs))
	for _, accountID := range accountIDs {
		if accountID <= 0 {
			continue
		}
		configured[accountID] = struct{}{}
	}
	return configured, allowAll, true
}

// warmupCohort is the single account-selection path used by reconciliation,
// queue metrics, and claiming. This prevents reconciliation from creating jobs
// that the scanner can never lease.
func (s *OpenAIWindowWarmupService) warmupCohort(ctx context.Context) ([]Account, []int64, bool) {
	configured, allowAll, ok := s.configuredWarmupAllowlist(ctx)
	if !ok || s.accounts == nil {
		return nil, nil, false
	}
	accounts, err := s.accounts.ListSchedulableByPlatform(ctx, PlatformOpenAI)
	if err != nil {
		return nil, nil, false
	}
	now := s.now()
	cohort := make([]Account, 0, len(accounts))
	accountIDs := make([]int64, 0, len(accounts))
	for index := range accounts {
		account := &accounts[index]
		if !warmupAccountEligibleAt(account, now) || !OpenAIWindowWarmupPolicyForAccount(account).Enabled() {
			continue
		}
		if !allowAll {
			if _, allowed := configured[account.ID]; !allowed {
				continue
			}
		}
		cohort = append(cohort, *account)
		accountIDs = append(accountIDs, account.ID)
	}
	return cohort, accountIDs, true
}

func (s *OpenAIWindowWarmupService) accountAllowed(ctx context.Context, accountID int64) bool {
	configured, allowAll, ok := s.configuredWarmupAllowlist(ctx)
	if !ok {
		return false
	}
	if allowAll {
		return true
	}
	_, allowed := configured[accountID]
	return allowed
}

func (s *OpenAIWindowWarmupService) releaseGlobalSend(permitToken string) {
	if s == nil || s.repo == nil || strings.TrimSpace(permitToken) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _ = s.repo.ReleaseGlobalSend(ctx, permitToken)
}

func (s *OpenAIWindowWarmupService) now() time.Time {
	if s == nil || s.options.Now == nil {
		return time.Now().UTC()
	}
	return s.options.Now().UTC()
}

func (s *OpenAIWindowWarmupService) warmupDueAt(cycleKey string, resetAt *time.Time) time.Time {
	if resetAt == nil {
		return s.now()
	}
	due := resetAt.UTC().Add(s.options.ResetGrace)
	if s.options.Jitter > 0 {
		due = due.Add(s.options.RandomJitter(cycleKey, s.options.Jitter))
	}
	return due
}

// OpenAIWindowWarmupTriggerResetWithCycle allows callers to retain the exact
// reset evidence in an audit-friendly trigger string without exposing tokens.
func OpenAIWindowWarmupTriggerResetWithCycle(resetAt time.Time) string {
	return OpenAIWindowWarmupTriggerReset + ":" + resetAt.UTC().Format(time.RFC3339)
}

func warmupAccountEligible(a *Account) bool {
	return warmupAccountEligibleAt(a, time.Now().UTC())
}

func warmupAccountEligibleAt(a *Account, now time.Time) bool {
	return a != nil && a.ID > 0 && a.Platform == PlatformOpenAI && a.Type == AccountTypeOAuth &&
		a.OpenAIWarmupIdentityGeneration > 0 && openAIWindowWarmupIdentityKey(a) != "" &&
		a.ParentAccountID == nil && a.QuotaDimensionOrDefault() == QuotaDimensionGlobal &&
		a.IsActive() && a.Schedulable && !a.IsShadow() && !accountExpiredAt(a, now) &&
		(a.TempUnschedulableUntil == nil || !now.Before(*a.TempUnschedulableUntil))
}

func openAIWindowWarmupIdentityKey(account *Account) string {
	if account == nil || !account.IsOpenAIOAuthLike() {
		return ""
	}
	if account.IsOpenAIAgentIdentity() {
		runtimeID := strings.TrimSpace(account.GetCredential("agent_runtime_id"))
		if runtimeID == "" {
			return ""
		}
		return "agent:" + runtimeID
	}
	if account.IsOpenAIPersonalAccessToken() {
		token := strings.TrimSpace(account.GetOpenAIAccessToken())
		if token == "" {
			return ""
		}
		digest := sha256.Sum256([]byte("openai-warmup-pat:" + token))
		return "pat:" + hex.EncodeToString(digest[:])
	}
	accountID := strings.TrimSpace(account.GetChatGPTAccountID())
	if accountID == "" {
		return ""
	}
	identity := "chatgpt:" + accountID
	if userID := strings.TrimSpace(account.GetCredential("chatgpt_user_id")); userID != "" {
		identity += ":user:" + userID
	}
	return identity
}

func accountExpiredAt(a *Account, now time.Time) bool {
	return a != nil && a.ExpiresAt != nil && !now.Before(*a.ExpiresAt)
}

func warmupAccountGeneration(a *Account) int64 {
	if a == nil {
		return 0
	}
	if a.OpenAIWarmupIdentityGeneration > 0 {
		return a.OpenAIWarmupIdentityGeneration
	}
	if !a.CreatedAt.IsZero() {
		// PostgreSQL stores TIMESTAMPTZ at microsecond precision and rounds
		// sub-microsecond input. Match that representation so an Ent-created
		// in-memory account and the enqueue trigger derive the same cycle key.
		return a.CreatedAt.UTC().Round(time.Microsecond).UnixNano()
	}
	return a.ID
}

func warmupInitialCycleKey(a *Account, generation int64) string {
	return fmt.Sprintf("initial:%d", generation)
}

func warmupResetCycleKey(resetAt time.Time, generation ...int64) string {
	if len(generation) > 0 && generation[0] > 0 {
		return fmt.Sprintf("reset:%d:%s", generation[0], resetAt.UTC().Format(time.RFC3339Nano))
	}
	return "reset:" + resetAt.UTC().Format(time.RFC3339Nano)
}

func accountCodexGlobalResetAt(a *Account) *time.Time {
	if a == nil || a.Extra == nil {
		return nil
	}
	var latest *time.Time
	for _, key := range []string{"codex_5h_reset_at", "codex_global_5h_reset_at"} {
		if value, ok := a.Extra[key]; ok {
			t := parseWarmupTime(value)
			if !t.IsZero() && (latest == nil || t.After(*latest)) {
				candidate := t.UTC()
				latest = &candidate
			}
		}
	}
	return latest
}

// A 0% five-hour snapshot can carry an idle rolling now+5h projection before
// any Codex request has started the real window. Such a reset is retained as
// preflight evidence but must not delay the initial durable job. Missing usage
// remains conservative and waits for a future reset.
func warmupAccountShouldWaitForReset(a *Account, resetAt *time.Time, now time.Time) bool {
	if resetAt == nil || !resetAt.After(now) {
		return false
	}
	if a == nil {
		return true
	}
	usedPercent, present := resolveAccountExtraNumber(a.Extra, "codex_5h_used_percent")
	return !present || usedPercent > 0
}

func warmupInitialCycleHasIdleSnapshot(account *Account, job *OpenAIWindowWarmupJob) bool {
	if account == nil || job == nil || !strings.HasPrefix(job.CycleKey, "initial:") {
		return false
	}
	usedPercent, present := resolveAccountExtraNumber(account.Extra, "codex_5h_used_percent")
	return present && usedPercent <= 0
}

func warmupInitialCycleHasIdleObservation(job *OpenAIWindowWarmupJob, usedPercent float64) bool {
	return job != nil && strings.HasPrefix(job.CycleKey, "initial:") && usedPercent <= 0
}

func parseWarmupTime(value any) time.Time {
	switch v := value.(type) {
	case time.Time:
		return v
	case *time.Time:
		if v != nil {
			return *v
		}
	case string:
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if t, err := time.Parse(layout, strings.TrimSpace(v)); err == nil {
				return t
			}
		}
	case int64:
		return time.Unix(v, 0)
	case int:
		return time.Unix(int64(v), 0)
	case float64:
		return time.Unix(int64(v), 0)
	case fmt.Stringer:
		if t, err := time.Parse(time.RFC3339Nano, v.String()); err == nil {
			return t
		}
	}
	return time.Time{}
}

func deterministicWarmupJitter(key string, max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	// A stable hash prevents a fleet restart from moving every account at once.
	h := uint64(1469598103934665603)
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return time.Duration(h % uint64(max))
}

type warmupUncertainObservationDecision uint8

const (
	warmupUncertainUnknown warmupUncertainObservationDecision = iota
	warmupUncertainRecordObservation
	warmupUncertainWait
	warmupUncertainFixedReset
	warmupUncertainRollingReset
)

func classifyWarmupUncertainObservation(job *OpenAIWindowWarmupJob, reset *time.Time, now time.Time) warmupUncertainObservationDecision {
	if job == nil || job.UncertainObservedAt == nil {
		return warmupUncertainRecordObservation
	}
	elapsed := now.Sub(*job.UncertainObservedAt)
	if elapsed < openAIWindowWarmupUncertainObservationInterval {
		return warmupUncertainWait
	}
	previous := job.UncertainObservedResetAt
	if previous == nil {
		if reset == nil {
			return warmupUncertainRollingReset
		}
		return warmupUncertainRecordObservation
	}
	if reset == nil || !reset.After(now) {
		return warmupUncertainRollingReset
	}
	delta := reset.Sub(*previous)
	if delta >= -openAIWindowWarmupResetStabilityTolerance && delta <= openAIWindowWarmupResetStabilityTolerance {
		return warmupUncertainFixedReset
	}
	if delta >= elapsed/2 {
		return warmupUncertainRollingReset
	}
	// A partially cached or quantized value is not enough evidence either way.
	// Record it as the next baseline and require another separated observation.
	return warmupUncertainRecordObservation
}

func nextWarmupUncertainObservation(job *OpenAIWindowWarmupJob, now time.Time) time.Time {
	next := now.Add(openAIWindowWarmupUncertainObservationInterval)
	if job == nil || job.UncertainObservedAt == nil {
		return next
	}
	minimum := job.UncertainObservedAt.Add(openAIWindowWarmupUncertainObservationInterval)
	if minimum.After(now) && minimum.Before(next) {
		return minimum
	}
	return next
}

func warmupJobStatus(job *OpenAIWindowWarmupJob) int {
	if job != nil && job.StatusCode != nil {
		return *job.StatusCode
	}
	if job != nil && job.UncertainTerminalObserved {
		return http.StatusOK
	}
	return 0
}

func warmupUncertainCode(job *OpenAIWindowWarmupJob) string {
	if job != nil && job.UncertainTerminalObserved {
		return "completed_reset_unconfirmed"
	}
	return "possibly_sent"
}

func warmupBackoff(attempt int) time.Duration {
	steps := []time.Duration{time.Minute, 2 * time.Minute, 5 * time.Minute, 15 * time.Minute, 30 * time.Minute, time.Hour}
	if attempt < 0 {
		attempt = 0
	}
	if attempt >= len(steps) {
		attempt = len(steps) - 1
	}
	return steps[attempt] + deterministicWarmupJitter(strconv.Itoa(attempt), 15*time.Second)
}

func isWarmupUncertainError(err error, result *OpenAIWindowProbeResult) bool {
	if result != nil && (result.EOF || (result.StatusCode >= 200 && result.StatusCode < 300 && !result.Terminal)) {
		return true
	}
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "timeout") || strings.Contains(text, "eof") || strings.Contains(text, "possibly_sent") || strings.Contains(text, "uncertain")
}

func isWarmupBlockedError(err error, status int) bool {
	if status == http.StatusUnauthorized || status == http.StatusForbidden || status == http.StatusBadRequest || status == http.StatusNotFound {
		return true
	}
	if err == nil {
		return false
	}
	if errors.Is(err, ErrOpenAIWindowWarmupNeedsReauth) ||
		errors.Is(err, ErrOpenAIWindowWarmupBlocked) ||
		errors.Is(err, ErrOpenAIWindowWarmupBlockedConfig) {
		return true
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "blocked") || strings.Contains(text, "needs_reauth") || strings.Contains(text, "invalid model") || strings.Contains(text, "configuration")
}

func warmupErrorCode(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, ErrOpenAIWindowWarmupNeedsReauth):
		return "needs_reauth"
	case errors.Is(err, ErrOpenAIWindowWarmupBlockedConfig):
		return "blocked_config"
	case errors.Is(err, ErrOpenAIWindowWarmupBlocked):
		return "blocked"
	}
	text := strings.TrimSpace(err.Error())
	if len(text) > 96 {
		text = text[:96]
	}
	for _, sep := range []string{" ", ":", "\n"} {
		if idx := strings.Index(text, sep); idx > 0 {
			text = text[:idx]
			break
		}
	}
	return strings.ToLower(strings.ReplaceAll(text, " ", "_"))
}

func warmupBlockedCode(err error, status int) string {
	switch status {
	case http.StatusUnauthorized:
		return "needs_reauth"
	case http.StatusForbidden:
		return "blocked"
	case http.StatusBadRequest, http.StatusNotFound:
		return "blocked_config"
	}
	code := warmupErrorCode(err)
	if code == "" {
		return warmupStatusCode(status)
	}
	return code
}

func warmupStatusCode(status int) string {
	if status <= 0 {
		return "upstream_error"
	}
	return "http_" + strconv.Itoa(status)
}

func warmupMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// warmupRateLimiter is a process-local token bucket. Correctness does not rely
// on it; PostgreSQL leases remain the authority across instances.
type warmupRateLimiter struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func newWarmupRateLimiter(qps float64) *warmupRateLimiter {
	if qps <= 0 {
		qps = 0.2
	}
	return &warmupRateLimiter{interval: time.Duration(float64(time.Second) / qps)}
}

func (l *warmupRateLimiter) Allow(now time.Time) bool {
	if l == nil {
		return true
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.next.IsZero() || !now.Before(l.next) {
		l.next = now.Add(l.interval)
		return true
	}
	return false
}

// NewWarmupLeaseToken is exported for repository implementations and tests.
func NewWarmupLeaseToken() string {
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err == nil {
		return hex.EncodeToString(raw[:])
	}
	return strings.ReplaceAll(uuid.NewString(), "-", "")
}
