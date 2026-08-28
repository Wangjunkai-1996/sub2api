package service

// This file contains the domain port and orchestration logic for the OpenAI
// Codex five-hour window warmup.  It deliberately has no Gin, Ent, or plugin
// dependencies: HTTP/TLS and credential recovery are supplied by the outbound
// adapter, while durable state and correctness are supplied by the repository.

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
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
	OpenAIWindowWarmupStateCompleted     = "completed"

	OpenAIWindowWarmupTriggerImport    = "import"
	OpenAIWindowWarmupTriggerReset     = "reset"
	OpenAIWindowWarmupTriggerReconcile = "reconcile"
	OpenAIWindowWarmupTriggerManual    = "manual"
	AuditActionOpenAIWindowWarmup      = "system.openai.window_warmup"

	// The defaults are intentionally conservative.  They can be overridden by
	// deployment configuration without changing the state machine.
	openAIWindowWarmupDefaultScanInterval = 30 * time.Second
	openAIWindowWarmupDefaultLease        = 120 * time.Second
	openAIWindowWarmupDefaultGrace        = 90 * time.Second
	openAIWindowWarmupDefaultJitter       = 30 * time.Second
	openAIWindowWarmupDefaultTimeout      = 45 * time.Second
	openAIWindowWarmupDefaultBatch        = 20
	openAIWindowWarmupDefaultMaxAttempts  = 8
	openAIWindowWarmupReconcileInterval   = 10 * time.Minute
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
	Account  *Account
	Model    string
	Payload  []byte
	Headers  http.Header
	Timeout  time.Duration
	Endpoint string
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
	EOF             bool
	Outcome         string
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

// OpenAIWindowWarmupAllowlist is read on every scan. Returning an empty list
// disables all accounts, and any read error fails closed.
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
	ID              int64      `json:"id"`
	AccountID       int64      `json:"account_id"`
	QuotaScope      string     `json:"quota_scope"`
	State           string     `json:"state"`
	Trigger         string     `json:"trigger"`
	CycleKey        string     `json:"cycle_key"`
	CycleGeneration int64      `json:"cycle_generation"`
	ObservedResetAt *time.Time `json:"observed_reset_at,omitempty"`
	NextAttemptAt   time.Time  `json:"next_attempt_at"`
	AttemptCount    int        `json:"attempt_count"`
	SentAt          *time.Time `json:"sent_at,omitempty"`
	LeaseOwner      string     `json:"-"`
	LeaseToken      string     `json:"-"`
	LeaseUntil      *time.Time `json:"-"`
	LastAttemptAt   *time.Time `json:"last_attempt_at,omitempty"`
	LastSuccessAt   *time.Time `json:"last_success_at,omitempty"`
	StatusCode      *int       `json:"status_code,omitempty"`
	LastErrorCode   string     `json:"last_error_code,omitempty"`
	LastError       string     `json:"last_error,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

// OpenAIWindowWarmupEnqueue describes one idempotent cycle insertion.
type OpenAIWindowWarmupEnqueue struct {
	AccountID       int64
	QuotaScope      string
	CycleKey        string
	CycleGeneration int64
	Trigger         string
	ObservedResetAt *time.Time
	NextAttemptAt   time.Time
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
	PreviousState string
}

// OpenAIWindowWarmupRepository is the durable port. Implementations must use
// PostgreSQL's DB clock and SELECT ... FOR UPDATE SKIP LOCKED in ClaimDue.
type OpenAIWindowWarmupRepository interface {
	Enqueue(context.Context, OpenAIWindowWarmupEnqueue) (*OpenAIWindowWarmupJob, bool, error)
	ClaimDue(context.Context, string, time.Duration, int, []int64) ([]OpenAIWindowWarmupClaim, error)
	QueueStats(context.Context, []int64) (OpenAIWindowWarmupQueueStats, error)
	CleanupExpiredAttempts(context.Context, int) (int64, error)
	ReserveGlobalSend(context.Context, time.Duration, time.Duration) (string, bool, error)
	ReleaseGlobalSend(context.Context, string) (bool, error)
	RenewLease(context.Context, int64, string, string, time.Duration) (bool, error)
	MarkStarted(context.Context, int64, string, string, time.Time) (bool, error)
	MarkSuccess(context.Context, int64, string, string, time.Time, *time.Time, int, string) (bool, error)
	MarkSuppressed(context.Context, int64, string, string, time.Time, *time.Time, string) (bool, error)
	MarkRetry(context.Context, int64, string, string, time.Time, time.Time, int, string, string) (bool, error)
	MarkObservationFailure(context.Context, int64, string, string, time.Time, time.Time, string, int, string, string) (bool, error)
	MarkRateLimited(context.Context, int64, string, string, time.Time, time.Time, *time.Time, int, string) (bool, error)
	MarkUncertain(context.Context, int64, string, string, time.Time, time.Time, int, string, string) (bool, error)
	MarkBlocked(context.Context, int64, string, string, time.Time, int, string, string) (bool, error)
	MarkPaused(context.Context, int64, string, string, time.Time, string) (bool, error)
	Reschedule(context.Context, int64, string, string, time.Time, string, *time.Time) (bool, error)
	GetByID(context.Context, int64) (*OpenAIWindowWarmupJob, error)
	GetCurrent(context.Context, int64, string) (*OpenAIWindowWarmupJob, error)
	GetCurrentForAccounts(context.Context, []int64, string) (map[int64]*OpenAIWindowWarmupJob, error)
	List(context.Context, OpenAIWindowWarmupListOptions) ([]*OpenAIWindowWarmupJob, error)
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
	UsageReconciler OpenAIWindowWarmupUsageReconciler
	Now             func() time.Time
	RandomJitter    func(string, time.Duration) time.Duration
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
	owner          string
	ctx            context.Context
	cancel         context.CancelFunc
	startOnce      sync.Once
	stopOnce       sync.Once
	wg             sync.WaitGroup
	limiter        *warmupRateLimiter
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
		limiter: newWarmupRateLimiter(options.GlobalQPS),
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
		s.scanOnce(s.ctx)
	}
	ticker := time.NewTicker(s.options.ScanInterval)
	defer ticker.Stop()
	reconcileTicker := time.NewTicker(openAIWindowWarmupReconcileInterval)
	defer reconcileTicker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.scanOnce(s.ctx)
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
	accounts, err := s.accounts.ListSchedulableByPlatform(ctx, PlatformOpenAI)
	if err != nil {
		return
	}
	for index := range accounts {
		account := &accounts[index]
		if !warmupAccountEligibleAt(account, s.now()) || !OpenAIWindowWarmupPolicyForAccount(account).Enabled() {
			continue
		}
		current, currentErr := s.repo.GetCurrent(ctx, account.ID, OpenAIWindowWarmupQuotaScopeGlobal)
		if currentErr != nil && !errors.Is(currentErr, sql.ErrNoRows) {
			continue
		}
		if current == nil {
			_, _, _ = s.ScheduleAccountWarmup(ctx, account, OpenAIWindowWarmupTriggerReconcile)
			continue
		}
		if current.State == OpenAIWindowWarmupStatePaused &&
			(OpenAIWindowWarmupPolicyForAccount(account) == OpenAIWindowWarmupPolicyContinuous || strings.HasPrefix(current.CycleKey, "initial:")) {
			_, _, _ = s.ScheduleAccountWarmup(ctx, account, OpenAIWindowWarmupTriggerReconcile)
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
	allowedAccountIDs, ok := s.allowedAccountIDs(ctx)
	if !ok {
		s.applyQueueStats(OpenAIWindowWarmupQueueStats{})
		return
	}
	// A disabled worker may still report backlog, but must never claim a lease.
	if !s.killSwitchEnabled(ctx) {
		s.refreshQueueStats(ctx, allowedAccountIDs)
		return
	}
	available := s.options.WorkerConcurrency - int(s.workerInflight.Load())
	if available <= 0 {
		s.refreshQueueStats(ctx, allowedAccountIDs)
		return
	}
	limit := s.options.BatchSize
	if available < limit {
		limit = available
	}
	claims, err := s.repo.ClaimDue(ctx, s.owner, s.options.LeaseDuration, limit, allowedAccountIDs)
	if err != nil {
		return
	}
	s.refreshQueueStats(ctx, allowedAccountIDs)
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

// ScheduleAccountWarmup persists the strategy and creates the first/current
// cycle. It is safe to call after every import/update; the unique cycle key
// suppresses duplicate jobs.
func (s *OpenAIWindowWarmupService) ScheduleAccountWarmup(ctx context.Context, account *Account, trigger string) (*OpenAIWindowWarmupJob, bool, error) {
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
	if trigger == "" {
		trigger = OpenAIWindowWarmupTriggerImport
	}
	cycleGeneration := warmupAccountGeneration(account)
	now := s.now()
	resetAt := accountCodexGlobalResetAt(account)
	cycleKey := warmupInitialCycleKey(account, cycleGeneration)
	next := now
	if resetAt != nil && resetAt.After(now) {
		next = s.warmupDueAt(cycleKey, resetAt)
	}
	if s.options.Jitter > 0 && (resetAt == nil || !resetAt.After(now)) {
		next = next.Add(s.options.RandomJitter(cycleKey, s.options.Jitter))
	}
	current, currentErr := s.repo.GetCurrent(ctx, account.ID, OpenAIWindowWarmupQuotaScopeGlobal)
	if currentErr != nil && !errors.Is(currentErr, sql.ErrNoRows) {
		return nil, false, currentErr
	}
	if current != nil && current.State == OpenAIWindowWarmupStatePaused {
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
		CycleKey: cycleKey, CycleGeneration: cycleGeneration, Trigger: trigger,
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

func (s *OpenAIWindowWarmupService) ListJobs(ctx context.Context, options OpenAIWindowWarmupListOptions) ([]*OpenAIWindowWarmupJob, error) {
	if s == nil || s.repo == nil {
		return nil, errors.New("warmup service is not configured")
	}
	return s.repo.List(ctx, options)
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
	current, currentErr := s.repo.GetCurrent(ctx, accountID, OpenAIWindowWarmupQuotaScopeGlobal)
	if currentErr != nil && !errors.Is(currentErr, sql.ErrNoRows) {
		return nil, false, currentErr
	}
	if current != nil {
		if warmupActiveState(current.State) {
			return current, false, nil
		}
		if current.State == OpenAIWindowWarmupStateBlocked || current.State == OpenAIWindowWarmupStateBlockedConfig || current.State == OpenAIWindowWarmupStatePaused {
			resetAt := accountCodexGlobalResetAt(account)
			next := s.now()
			if resetAt != nil && resetAt.After(next) {
				next = s.warmupDueAt(current.CycleKey, resetAt)
			}
			return s.repo.UnblockAccount(ctx, accountID, next, resetAt)
		}
	}
	resetAt := accountCodexGlobalResetAt(account)
	now := s.now()
	next := now
	if resetAt != nil && resetAt.After(now) {
		next = s.warmupDueAt(warmupResetCycleKey(*resetAt), resetAt)
	}
	manualSeed := warmupAccountGeneration(account)
	if current != nil {
		manualSeed = current.ID
	}
	return s.repo.Enqueue(ctx, OpenAIWindowWarmupEnqueue{
		AccountID: account.ID, QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey:        fmt.Sprintf("manual:%d", manualSeed),
		CycleGeneration: warmupAccountGeneration(account),
		Trigger:         OpenAIWindowWarmupTriggerManual, ObservedResetAt: resetAt,
		NextAttemptAt: next,
	})
}

func warmupActiveState(state string) bool {
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
	// claimed. Re-read the authoritative account snapshot and skip the probe.
	// If the snapshot still carries the reset observed by this cycle, keep this
	// cycle armed until that authoritative reset rather than treating it as a
	// completed synthetic attempt.
	if latestReset := accountCodexGlobalResetAt(account); latestReset != nil && latestReset.After(s.now()) {
		if warmupResetAdvanced(latestReset, job.ObservedResetAt, s.now()) {
			s.completeSuppressedCycle(ctx, claim, account, latestReset, "real_traffic_suppressed")
		} else {
			s.reschedule(ctx, claim, s.warmupDueAt(job.CycleKey, latestReset), OpenAIWindowWarmupStateArmed, latestReset)
		}
		return
	}
	// A timed-out sender may have reached upstream. The first takeover always
	// performs a passive usage query and defers if evidence is still unchanged.
	// A subsequent `uncertain` claim may send only after another authoritative
	// query confirms the reset still has not advanced.
	if job.SentAt != nil && (claim.PreviousState == OpenAIWindowWarmupStateRunning ||
		claim.PreviousState == OpenAIWindowWarmupStateUncertain ||
		claim.PreviousState == OpenAIWindowWarmupStatePossiblySent) {
		reset, authoritative, reconcileErr := s.reconcileFiveHourReset(ctx, account.ID)
		if warmupResetAdvanced(reset, job.ObservedResetAt, s.now()) {
			s.completeSuppressedCycle(ctx, claim, account, reset, "lease_takeover_reconciled")
			return
		}
		if reconcileErr != nil {
			s.handleUsageObservationFailure(ctx, claim, s.now(), OpenAIWindowWarmupStateUncertain, "reconcile", reconcileErr)
			return
		}
		if claim.PreviousState != OpenAIWindowWarmupStateUncertain && claim.PreviousState != OpenAIWindowWarmupStatePossiblySent {
			s.markUncertain(ctx, claim, s.now(), s.now().Add(warmupBackoff(job.AttemptCount)), 0, "possibly_sent", "expired sender lease requires another usage observation")
			return
		}
		if !authoritative {
			s.markUncertain(ctx, claim, s.now(), s.now().Add(warmupBackoff(job.AttemptCount)), 0, "usage_unavailable", "authoritative usage unavailable")
			return
		}
	}
	// An uncertain sender gets its required passive observations above before
	// the cap is enforced. No cycle may start another synthetic request once its
	// durable attempt budget has been consumed.
	if job.AttemptCount >= s.options.MaxAttempts {
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
	fiveHourReset := warmupResetFromUsage(usage, false)
	blockedReset := warmupResetFromUsage(usage, true)
	if blockedReset != nil && (fiveHourReset == nil || blockedReset.After(*fiveHourReset)) && blockedReset.After(s.now()) {
		next := s.warmupDueAt(warmupResetCycleKey(*blockedReset), blockedReset)
		s.markRateLimited(ctx, claim, s.now(), next, blockedReset, 0, "weekly_limit_preflight")
		return
	}
	if fiveHourReset != nil && fiveHourReset.After(s.now()) {
		if warmupResetAdvanced(fiveHourReset, job.ObservedResetAt, s.now()) {
			s.completeSuppressedCycle(ctx, claim, account, fiveHourReset, "usage_preflight_suppressed")
		} else {
			next := s.warmupDueAt(warmupResetCycleKey(*fiveHourReset), fiveHourReset)
			s.reschedule(ctx, claim, next, OpenAIWindowWarmupStateArmed, fiveHourReset)
		}
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
	if !s.killSwitchEnabled(ctx) {
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
	if !s.markStarted(ctx, claim, s.now()) {
		if latest, latestErr := s.accounts.GetByID(ctx, job.AccountID); latestErr == nil && latest != nil {
			if reset := accountCodexGlobalResetAt(latest); reset != nil && reset.After(s.now()) {
				if warmupResetAdvanced(reset, job.ObservedResetAt, s.now()) {
					s.completeSuppressedCycle(ctx, claim, latest, reset, "mark_started_reset_cas")
				} else {
					s.reschedule(ctx, claim, s.warmupDueAt(job.CycleKey, reset), OpenAIWindowWarmupStateArmed, reset)
				}
			}
		}
		return
	}
	job.AttemptCount++
	sentAt := s.now()
	job.SentAt = &sentAt
	probeCtx, probeCancel := context.WithTimeout(ctx, s.options.RequestTimeout)
	result, probeErr := s.probe.Probe(probeCtx, account, job.ObservedResetAt)
	probeCancel()
	s.handleProbeResult(ctx, claim, account, result, probeErr)
}

func (s *OpenAIWindowWarmupService) handleProbeResult(ctx context.Context, claim OpenAIWindowWarmupClaim, account *Account, result *OpenAIWindowProbeResult, probeErr error) {
	job := claim.Job
	now := s.now()
	statusCode := 0
	if result != nil {
		statusCode = result.StatusCode
	}
	if statusCode == http.StatusTooManyRequests {
		if job.AttemptCount >= s.options.MaxAttempts {
			s.markBlocked(ctx, claim, now, statusCode, "attempt_limit", "warmup attempt limit reached")
			return
		}
		reset := result.ResetAt
		if reset == nil {
			reset = result.ObservedResetAt
		}
		if authoritative, _, _ := s.reconcileBlockedReset(ctx, account.ID); authoritative != nil && (reset == nil || authoritative.After(*reset)) {
			reset = authoritative
		}
		if reset != nil && reset.After(now) {
			next := s.warmupDueAt(warmupResetCycleKey(*reset), reset)
			s.markRateLimited(ctx, claim, now, next, reset, statusCode, "rate_limited")
			return
		}
		s.markRetry(ctx, claim, now, now.Add(warmupBackoff(job.AttemptCount-1)), statusCode, warmupStatusCode(statusCode), "rate limit reset unavailable")
		return
	}
	if probeErr != nil {
		if isWarmupUncertainError(probeErr, result) {
			if reset, authoritative, _ := s.reconcileFiveHourReset(ctx, account.ID); authoritative && warmupResetAdvanced(reset, job.ObservedResetAt, now) {
				// Usage reconciliation proves that the window moved, but it does
				// not prove that this request produced a completed Responses
				// terminal event. Keep it out of the success counter and record a
				// fenced suppression so the cycle cannot be replayed.
				s.completeSuppressedCycle(ctx, claim, account, reset, "possibly_sent_reconciled")
				return
			}
			if job.AttemptCount >= s.options.MaxAttempts {
				s.markBlocked(ctx, claim, now, statusCode, "attempt_limit_uncertain", "ambiguous warmup reached attempt limit")
				return
			}
			schedule := now.Add(warmupBackoff(job.AttemptCount - 1))
			s.markUncertain(ctx, claim, now, schedule, statusCode, "possibly_sent", "upstream outcome requires usage reconciliation")
			return
		}
		code := warmupErrorCode(probeErr)
		if isWarmupBlockedError(probeErr, statusCode) {
			s.markBlocked(ctx, claim, now, statusCode, code, "warmup blocked")
			return
		}
		if job.AttemptCount >= s.options.MaxAttempts {
			s.markBlocked(ctx, claim, now, statusCode, "attempt_limit", "warmup attempt limit reached")
			return
		}
		s.markRetry(ctx, claim, now, now.Add(warmupBackoff(job.AttemptCount-1)), statusCode, code, "transient upstream failure")
		return
	}
	if result == nil {
		if job.AttemptCount >= s.options.MaxAttempts {
			s.markBlocked(ctx, claim, now, 0, "attempt_limit_uncertain", "ambiguous warmup reached attempt limit")
			return
		}
		s.markUncertain(ctx, claim, now, now.Add(warmupBackoff(job.AttemptCount-1)), 0, "possibly_sent", "empty probe result")
		return
	}
	statusCode = result.StatusCode
	reset := result.ResetAt
	if reset == nil {
		reset = result.ObservedResetAt
	}
	if statusCode >= 200 && statusCode < 300 && warmupTerminalSucceeded(result) && warmupResetAdvanced(reset, job.ObservedResetAt, now) {
		ok := s.markSuccess(ctx, claim, now, reset, statusCode, "completed")
		if ok {
			s.enqueueNextContinuousCycle(ctx, account, reset)
		}
		return
	}
	if (statusCode >= 200 && statusCode < 300) || result.EOF || !result.Terminal {
		if reconciled, authoritative, _ := s.reconcileFiveHourReset(ctx, account.ID); authoritative && warmupResetAdvanced(reconciled, job.ObservedResetAt, now) {
			// A passive usage observation can suppress a duplicate replay, but
			// cannot upgrade an ambiguous response into a successful warmup.
			s.completeSuppressedCycle(ctx, claim, account, reconciled, "possibly_sent_reconciled")
			return
		}
		if job.AttemptCount >= s.options.MaxAttempts {
			s.markBlocked(ctx, claim, now, statusCode, "attempt_limit_uncertain", "ambiguous warmup reached attempt limit")
			return
		}
		s.markUncertain(ctx, claim, now, now.Add(warmupBackoff(job.AttemptCount-1)), statusCode, "possibly_sent", "terminal/reset evidence missing")
		return
	}
	if isWarmupBlockedError(nil, statusCode) {
		s.markBlocked(ctx, claim, now, statusCode, warmupStatusCode(statusCode), "warmup blocked")
		return
	}
	if job.AttemptCount >= s.options.MaxAttempts {
		s.markBlocked(ctx, claim, now, statusCode, "attempt_limit", "warmup attempt limit reached")
		return
	}
	s.markRetry(ctx, claim, now, now.Add(warmupBackoff(job.AttemptCount-1)), statusCode, warmupStatusCode(statusCode), "upstream response")
}

func (s *OpenAIWindowWarmupService) markStarted(ctx context.Context, claim OpenAIWindowWarmupClaim, at time.Time) bool {
	ok, err := s.repo.MarkStarted(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, at)
	if err != nil || !ok {
		return false
	}
	s.metrics.started.Add(1)
	s.recordWarmupAudit(claim.Job, OpenAIWindowWarmupStateRunning, 0, "started", claim.Job.ObservedResetAt)
	return true
}

func (s *OpenAIWindowWarmupService) markSuccess(ctx context.Context, claim OpenAIWindowWarmupClaim, at time.Time, resetAt *time.Time, status int, code string) bool {
	ok, err := s.repo.MarkSuccess(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, at, resetAt, status, code)
	if err != nil || !ok {
		return false
	}
	s.metrics.success.Add(1)
	s.recordWarmupAudit(claim.Job, OpenAIWindowWarmupStateCompleted, status, code, resetAt)
	return true
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
	return true
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

func (s *OpenAIWindowWarmupService) markUncertain(ctx context.Context, claim OpenAIWindowWarmupClaim, at, next time.Time, status int, code, message string) bool {
	ok, err := s.repo.MarkUncertain(ctx, claim.Job.ID, claim.Owner, claim.LeaseToken, at, next, status, code, message)
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

func (s *OpenAIWindowWarmupService) reconcileFiveHourReset(ctx context.Context, accountID int64) (*time.Time, bool, error) {
	usage, err := s.queryWarmupUsage(ctx, accountID)
	if err != nil {
		return nil, false, err
	}
	reset := warmupResetFromUsage(usage, false)
	// A successful usage response with no primary window is authoritative: it
	// means no five-hour window is currently active. This distinction lets an
	// uncertain cycle replay only after a second passive observation, while a
	// failed or empty usage response remains fail-closed.
	return reset, true, nil
}

func (s *OpenAIWindowWarmupService) reconcileBlockedReset(ctx context.Context, accountID int64) (*time.Time, bool, error) {
	usage, err := s.queryWarmupUsage(ctx, accountID)
	if err != nil {
		return nil, false, err
	}
	reset := warmupResetFromUsage(usage, true)
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
	cycleKey := warmupResetCycleKey(*reset)
	job, inserted, err := s.repo.Enqueue(ctx, OpenAIWindowWarmupEnqueue{
		AccountID: account.ID, QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: cycleKey, CycleGeneration: warmupAccountGeneration(account),
		Trigger: OpenAIWindowWarmupTriggerReset, ObservedResetAt: reset,
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

func warmupResetFromUsage(usage *OpenAIQuotaUsage, includeBlockedSevenDay bool) *time.Time {
	if usage == nil || usage.RateLimit == nil {
		return nil
	}
	windows := []*OpenAIRateLimitWindow{usage.RateLimit.PrimaryWindow, usage.RateLimit.SecondaryWindow}
	var fiveHour *time.Time
	var blockedSevenDay *time.Time
	for index, window := range windows {
		if window == nil || window.ResetAt <= 0 {
			continue
		}
		reset := time.Unix(window.ResetAt, 0).UTC()
		isFiveHour := window.LimitWindowSeconds > 0 && window.LimitWindowSeconds <= 6*60*60
		isSevenDay := window.LimitWindowSeconds > 6*60*60
		if window.LimitWindowSeconds <= 0 {
			// Older upstream responses omitted the duration but consistently placed
			// the five-hour window first. Keep this fallback scoped to missing data.
			isFiveHour = index == 0
			isSevenDay = index == 1
		}
		if isFiveHour && (fiveHour == nil || reset.After(*fiveHour)) {
			candidate := reset
			fiveHour = &candidate
		}
		if includeBlockedSevenDay && isSevenDay && window.UsedPercent >= 100 &&
			(blockedSevenDay == nil || reset.After(*blockedSevenDay)) {
			candidate := reset
			blockedSevenDay = &candidate
		}
	}
	if blockedSevenDay != nil && (fiveHour == nil || blockedSevenDay.After(*fiveHour)) {
		return blockedSevenDay
	}
	return fiveHour
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

func warmupResetAdvanced(reset, expected *time.Time, now time.Time) bool {
	if reset == nil || !reset.After(now) {
		return false
	}
	return expected == nil || reset.After(*expected)
}

func (s *OpenAIWindowWarmupService) killSwitchEnabled(ctx context.Context) bool {
	if s.options.KillSwitch == nil {
		return true
	}
	ok, err := s.options.KillSwitch.Enabled(ctx)
	return err == nil && ok
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

func (s *OpenAIWindowWarmupService) allowedAccountIDs(ctx context.Context) ([]int64, bool) {
	if s == nil || s.options.Allowlist == nil {
		return nil, false
	}
	accountIDs, err := s.options.Allowlist.AccountIDs(ctx)
	if err != nil || len(accountIDs) == 0 {
		return nil, false
	}
	unique := make(map[int64]struct{}, len(accountIDs))
	result := make([]int64, 0, len(accountIDs))
	for _, accountID := range accountIDs {
		if accountID <= 0 {
			continue
		}
		if _, exists := unique[accountID]; exists {
			continue
		}
		unique[accountID] = struct{}{}
		result = append(result, accountID)
	}
	return result, len(result) > 0
}

func (s *OpenAIWindowWarmupService) accountAllowed(ctx context.Context, accountID int64) bool {
	accountIDs, ok := s.allowedAccountIDs(ctx)
	if !ok {
		return false
	}
	for _, allowedID := range accountIDs {
		if allowedID == accountID {
			return true
		}
	}
	return false
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
		a.ParentAccountID == nil && a.QuotaDimensionOrDefault() == QuotaDimensionGlobal &&
		a.IsActive() && a.Schedulable && !a.IsShadow() && !accountExpiredAt(a, now) &&
		(a.TempUnschedulableUntil == nil || !now.Before(*a.TempUnschedulableUntil))
}

func accountExpiredAt(a *Account, now time.Time) bool {
	return a != nil && a.ExpiresAt != nil && !now.Before(*a.ExpiresAt)
}

func warmupAccountGeneration(a *Account) int64 {
	if a == nil {
		return 0
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

func warmupResetCycleKey(resetAt time.Time) string {
	return "reset:" + resetAt.UTC().Format(time.RFC3339Nano)
}

func accountCodexGlobalResetAt(a *Account) *time.Time {
	if a == nil || a.Extra == nil {
		return nil
	}
	for _, key := range []string{"codex_5h_reset_at", "codex_global_5h_reset_at"} {
		if value, ok := a.Extra[key]; ok {
			t := parseWarmupTime(value)
			if !t.IsZero() {
				t = t.UTC()
				return &t
			}
		}
	}
	return nil
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
