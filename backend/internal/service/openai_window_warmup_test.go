package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

type warmupRepositorySpy struct {
	OpenAIWindowWarmupRepository
	mu sync.Mutex

	claims            []OpenAIWindowWarmupClaim
	claimCalls        int
	claimAccountIDs   []int64
	enqueues          []OpenAIWindowWarmupEnqueue
	cycles            map[string]*OpenAIWindowWarmupJob
	currentBatchCalls int
	action            string
	state             string
	code              string
	status            int
	next              time.Time
	reset             *time.Time
	startEvidence     OpenAIWindowWarmupStartEvidence
	uncertainEvidence OpenAIWindowWarmupUncertainEvidence
	started           int
	cancelStarted     int
	successCalls      int
	projectionCalls   int
	projectedAccount  int64
	projectedIdentity int64
	projectedAt       time.Time
	projectedReset    time.Time
	suppressedCalls   int
	reserve           bool
	rejectSuccess     bool
	rejectStarted     bool
	rejectRenew       bool
	rejectBlocked     bool
	rejectAuthRetry   bool
	authRetryCalls    int
	authRetry         OpenAIWindowWarmupAuthStateRetry
	consumeClaims     bool
	onMarkStarted     func()
	queueStats        OpenAIWindowWarmupQueueStats
	queueStatsCalls   int
}

func (r *warmupRepositorySpy) Enqueue(_ context.Context, in OpenAIWindowWarmupEnqueue) (*OpenAIWindowWarmupJob, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cycles == nil {
		r.cycles = make(map[string]*OpenAIWindowWarmupJob)
	}
	key := in.QuotaScope + ":" + in.CycleKey
	if existing := r.cycles[key]; existing != nil {
		return existing, false, nil
	}
	job := &OpenAIWindowWarmupJob{
		ID: int64(len(r.cycles) + 1), AccountID: in.AccountID, QuotaScope: in.QuotaScope,
		State: OpenAIWindowWarmupStatePending, Trigger: in.Trigger, CycleKey: in.CycleKey,
		CycleGeneration: in.CycleGeneration, IdentityGeneration: in.IdentityGeneration,
		ObservedResetAt: cloneWarmupTestTime(in.ObservedResetAt),
		NextAttemptAt:   in.NextAttemptAt,
	}
	r.cycles[key] = job
	r.enqueues = append(r.enqueues, in)
	return job, true, nil
}

func (r *warmupRepositorySpy) ClaimDue(_ context.Context, _ string, _ time.Duration, limit int, accountIDs []int64) ([]OpenAIWindowWarmupClaim, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claimCalls++
	r.claimAccountIDs = append([]int64(nil), accountIDs...)
	count := len(r.claims)
	if limit < count {
		count = limit
	}
	claims := append([]OpenAIWindowWarmupClaim(nil), r.claims[:count]...)
	if r.consumeClaims {
		r.claims = append([]OpenAIWindowWarmupClaim(nil), r.claims[count:]...)
	}
	return claims, nil
}

func (r *warmupRepositorySpy) QueueStats(context.Context, []int64) (OpenAIWindowWarmupQueueStats, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queueStatsCalls++
	return r.queueStats, nil
}

func (r *warmupRepositorySpy) CleanupExpiredAttempts(context.Context, int) (int64, error) {
	return 0, nil
}

func (r *warmupRepositorySpy) CleanupSupersededTerminalJobs(context.Context, int) (int64, error) {
	return 0, nil
}

func (r *warmupRepositorySpy) ReserveGlobalSend(context.Context, time.Duration, time.Duration) (string, bool, error) {
	if !r.reserve {
		return "", false, nil
	}
	return "permit", true, nil
}

func (r *warmupRepositorySpy) ReleaseGlobalSend(context.Context, string) (bool, error) {
	return true, nil
}

func (r *warmupRepositorySpy) RenewLease(context.Context, int64, string, string, time.Duration) (bool, error) {
	return !r.rejectRenew, nil
}

func (r *warmupRepositorySpy) MarkStarted(_ context.Context, _ int64, _, _ string, _ time.Time, evidence OpenAIWindowWarmupStartEvidence) (bool, error) {
	r.mu.Lock()
	r.started++
	r.startEvidence = evidence
	r.action = "started"
	onMarkStarted := r.onMarkStarted
	rejectStarted := r.rejectStarted
	r.mu.Unlock()
	if onMarkStarted != nil {
		onMarkStarted()
	}
	return !rejectStarted, nil
}

func (r *warmupRepositorySpy) CancelStartedBeforeSend(_ context.Context, _ int64, _, _ string, _ time.Time, _ string) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancelStarted++
	r.action = "cancel_started"
	return true, nil
}

func (r *warmupRepositorySpy) MarkSuccess(_ context.Context, _ int64, _, _ string, _ time.Time, reset *time.Time, status int, code string) (bool, error) {
	if r.rejectSuccess {
		return false, nil
	}
	r.mu.Lock()
	r.successCalls++
	r.mu.Unlock()
	r.capture("success", OpenAIWindowWarmupStateCompleted, status, code, time.Time{}, reset)
	return true, nil
}

func (r *warmupRepositorySpy) ProjectSuccessReset(_ context.Context, accountID, identityGeneration int64, at, resetAt time.Time) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.projectionCalls++
	r.projectedAccount = accountID
	r.projectedIdentity = identityGeneration
	r.projectedAt = at
	r.projectedReset = resetAt
	return true, nil
}

func (r *warmupRepositorySpy) MarkSuppressed(_ context.Context, _ int64, _, _ string, _ time.Time, reset *time.Time, code string) (bool, error) {
	r.mu.Lock()
	r.suppressedCalls++
	r.mu.Unlock()
	r.capture("success", OpenAIWindowWarmupStateCompleted, 0, code, time.Time{}, reset)
	return true, nil
}

func (r *warmupRepositorySpy) MarkRetry(_ context.Context, _ int64, _, _ string, _, next time.Time, status int, code, _ string) (bool, error) {
	r.capture("retry", OpenAIWindowWarmupStateRetrying, status, code, next, nil)
	return true, nil
}

func (r *warmupRepositorySpy) MarkObservationFailure(_ context.Context, _ int64, _, _ string, _, next time.Time, state string, status int, code, _ string) (bool, error) {
	action := "retry"
	if state == OpenAIWindowWarmupStateUncertain {
		action = "uncertain"
	} else if state == OpenAIWindowWarmupStateBlocked || state == OpenAIWindowWarmupStateBlockedConfig {
		action = "blocked"
	}
	r.capture(action, state, status, code, next, nil)
	return true, nil
}

func (r *warmupRepositorySpy) MarkRateLimited(_ context.Context, _ int64, _, _ string, _, next time.Time, reset *time.Time, status int, code string) (bool, error) {
	r.capture("retry", OpenAIWindowWarmupStateArmed, status, code, next, reset)
	return true, nil
}

func (r *warmupRepositorySpy) MarkUncertain(_ context.Context, _ int64, _, _ string, _, next time.Time, status int, code, _ string, evidence OpenAIWindowWarmupUncertainEvidence) (bool, error) {
	r.mu.Lock()
	r.uncertainEvidence = evidence
	r.mu.Unlock()
	r.capture("uncertain", OpenAIWindowWarmupStateUncertain, status, code, next, nil)
	return true, nil
}

func (r *warmupRepositorySpy) MarkBlocked(_ context.Context, _ int64, _, _ string, _ time.Time, status int, code, _ string) (bool, error) {
	if r.rejectBlocked {
		return false, nil
	}
	state := OpenAIWindowWarmupStateBlocked
	if status == http.StatusBadRequest || status == http.StatusNotFound || code == "blocked_config" {
		state = OpenAIWindowWarmupStateBlockedConfig
	}
	r.capture("blocked", state, status, code, time.Time{}, nil)
	return true, nil
}

func (r *warmupRepositorySpy) RequeueAuthStateUpdateFailure(_ context.Context, retry OpenAIWindowWarmupAuthStateRetry) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.rejectAuthRetry {
		return false, nil
	}
	r.authRetryCalls++
	r.authRetry = retry
	r.action = "auth_state_retry"
	r.state = OpenAIWindowWarmupStateRetrying
	r.code = retry.RetryCode
	r.next = retry.NextAttemptAt
	return true, nil
}

type warmupAuthFailureHandlerSpy struct {
	mu       sync.Mutex
	failures []OpenAIWindowWarmupAuthFailure
	err      error
}

func (s *warmupAuthFailureHandlerSpy) HandleOpenAIWindowWarmupAuthFailure(_ context.Context, failure OpenAIWindowWarmupAuthFailure) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failures = append(s.failures, *cloneOpenAIWindowWarmupAuthFailure(&failure))
	return s.err
}

func (r *warmupRepositorySpy) MarkPaused(_ context.Context, _ int64, _, _ string, _ time.Time, reason string) (bool, error) {
	r.capture("paused", OpenAIWindowWarmupStatePaused, 0, reason, time.Time{}, nil)
	return true, nil
}

func (r *warmupRepositorySpy) Reschedule(_ context.Context, _ int64, _, _ string, next time.Time, state string, reset *time.Time) (bool, error) {
	r.capture("reschedule", state, 0, "", next, reset)
	return true, nil
}

func (r *warmupRepositorySpy) UnblockAccount(_ context.Context, accountID int64, next time.Time, reset *time.Time) (*OpenAIWindowWarmupJob, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, job := range r.cycles {
		if job.AccountID != accountID || (job.State != OpenAIWindowWarmupStatePaused && job.State != OpenAIWindowWarmupStateBlocked && job.State != OpenAIWindowWarmupStateBlockedConfig) {
			continue
		}
		if job.State == OpenAIWindowWarmupStatePaused && job.SentAt != nil {
			job.State = OpenAIWindowWarmupStateUncertain
			job.NextAttemptAt = next
			return job, true, nil
		}
		job.State = OpenAIWindowWarmupStatePending
		if reset != nil {
			job.State = OpenAIWindowWarmupStateArmed
		}
		job.NextAttemptAt = next
		job.ObservedResetAt = cloneWarmupTestTime(reset)
		return job, true, nil
	}
	return nil, false, nil
}

func (r *warmupRepositorySpy) GetCurrent(_ context.Context, accountID int64, _ string) (*OpenAIWindowWarmupJob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var current *OpenAIWindowWarmupJob
	for _, job := range r.cycles {
		if job.AccountID == accountID && (current == nil || job.ID > current.ID) {
			current = job
		}
	}
	if current == nil {
		return nil, sql.ErrNoRows
	}
	return current, nil
}

func (r *warmupRepositorySpy) GetCurrentForAccounts(_ context.Context, accountIDs []int64, _ string) (map[int64]*OpenAIWindowWarmupJob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentBatchCalls++
	requested := make(map[int64]struct{}, len(accountIDs))
	for _, accountID := range accountIDs {
		requested[accountID] = struct{}{}
	}
	currentByAccount := make(map[int64]*OpenAIWindowWarmupJob, len(accountIDs))
	for _, job := range r.cycles {
		if job == nil {
			continue
		}
		if _, ok := requested[job.AccountID]; !ok {
			continue
		}
		current := currentByAccount[job.AccountID]
		if current == nil || job.ID > current.ID {
			currentByAccount[job.AccountID] = job
		}
	}
	return currentByAccount, nil
}

func (r *warmupRepositorySpy) capture(action, state string, status int, code string, next time.Time, reset *time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.action = action
	r.state = state
	r.status = status
	r.code = code
	r.next = next
	r.reset = cloneWarmupTestTime(reset)
}

type warmupAccountRepositoryStub struct {
	AccountRepository
	mu        sync.Mutex
	account   *Account
	accounts  map[int64]*Account
	getCalls  int
	listCalls int
	err       error
	updates   []map[string]any
}

func (r *warmupAccountRepositoryStub) GetByID(_ context.Context, id int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.getCalls++
	if r.err != nil {
		return nil, r.err
	}
	if account := r.accounts[id]; account != nil {
		return account, nil
	}
	if r.account == nil || r.account.ID != id {
		return nil, errors.New("account not found")
	}
	return r.account, nil
}

func (r *warmupAccountRepositoryStub) ListSchedulableByPlatform(context.Context, string) ([]Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listCalls++
	if r.err != nil {
		return nil, r.err
	}
	if len(r.accounts) > 0 {
		accounts := make([]Account, 0, len(r.accounts))
		for _, account := range r.accounts {
			accounts = append(accounts, *account)
		}
		return accounts, nil
	}
	if r.account == nil {
		return nil, nil
	}
	return []Account{*r.account}, nil
}

func (r *warmupAccountRepositoryStub) UpdateExtra(_ context.Context, id int64, updates map[string]any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	account := r.accounts[id]
	if account == nil && r.account != nil && r.account.ID == id {
		account = r.account
	}
	if account == nil {
		return errors.New("account not found")
	}
	copy := make(map[string]any, len(updates))
	for key, value := range updates {
		copy[key] = value
		if account.Extra == nil {
			account.Extra = make(map[string]any)
		}
		account.Extra[key] = value
	}
	r.updates = append(r.updates, copy)
	return nil
}

type warmupProbeStub struct {
	mu             sync.Mutex
	calls          int
	result         *OpenAIWindowProbeResult
	err            error
	results        []*OpenAIWindowProbeResult
	errs           []error
	expectedResets []*time.Time
}

type warmupProbeFunc func(context.Context, *Account, *time.Time) (*OpenAIWindowProbeResult, error)

func (f warmupProbeFunc) Probe(ctx context.Context, account *Account, expectedReset *time.Time) (*OpenAIWindowProbeResult, error) {
	return f(ctx, account, expectedReset)
}

func (p *warmupProbeStub) Probe(_ context.Context, _ *Account, expectedReset *time.Time) (*OpenAIWindowProbeResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	index := p.calls
	p.calls++
	p.expectedResets = append(p.expectedResets, cloneWarmupTestTime(expectedReset))
	if index < len(p.results) {
		var err error
		if index < len(p.errs) {
			err = p.errs[index]
		}
		return p.results[index], err
	}
	return p.result, p.err
}

type warmupUsageStub struct {
	mu     sync.Mutex
	calls  int
	usage  *OpenAIQuotaUsage
	err    error
	usages []*OpenAIQuotaUsage
	errs   []error
}

func (u *warmupUsageStub) QueryUsage(context.Context, int64) (*OpenAIQuotaUsage, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	index := u.calls
	u.calls++
	if index < len(u.errs) && u.errs[index] != nil {
		return nil, u.errs[index]
	}
	if index < len(u.usages) {
		return u.usages[index], nil
	}
	return u.usage, u.err
}

type warmupExclusiveCacheStub struct {
	mu           sync.Mutex
	refresh      bool
	refreshCalls int
	releaseCalls int
}

func (c *warmupExclusiveCacheStub) AcquireAccountExclusive(context.Context, int64, string, time.Duration) (bool, error) {
	return true, nil
}

func (c *warmupExclusiveCacheStub) RefreshAccountExclusive(context.Context, int64, string, time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.refreshCalls++
	return c.refresh, nil
}

func (c *warmupExclusiveCacheStub) ReleaseAccountExclusive(context.Context, int64, string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseCalls++
	return true, nil
}

type warmupConcurrencyStub struct {
	mu       sync.Mutex
	acquired bool
	err      error
	calls    int
	lease    *warmupExclusiveCacheStub
}

func (c *warmupConcurrencyStub) TryAcquireAccountExclusive(_ context.Context, accountID int64, ttl time.Duration) (*AccountExclusiveLease, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.err != nil || !c.acquired {
		return nil, c.acquired, c.err
	}
	return &AccountExclusiveLease{
		cache: c.lease, accountID: accountID, token: "warmup-exclusive-test", ttl: ttl,
	}, true, nil
}

func TestOpenAIWindowWarmupScheduleUsesStableInitialCycle(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	reset := now.Add(2 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Extra["codex_5h_reset_at"] = reset.Format(time.RFC3339)
	repo := &warmupRepositorySpy{}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	service.audit = NewAuditLogService(nil, nil)

	first, inserted, err := service.ScheduleAccountWarmup(context.Background(), account, OpenAIWindowWarmupTriggerImport)
	require.NoError(t, err)
	require.True(t, inserted)
	require.Equal(t, "initial:1", first.CycleKey)
	require.Equal(t, reset.Add(openAIWindowWarmupDefaultGrace), first.NextAttemptAt)
	require.Equal(t, reset, *first.ObservedResetAt)

	second, inserted, err := service.ScheduleAccountWarmup(context.Background(), account, OpenAIWindowWarmupTriggerImport)
	require.NoError(t, err)
	require.False(t, inserted)
	require.Equal(t, first.ID, second.ID)
	require.Len(t, repo.enqueues, 1)
	require.Len(t, service.audit.queue, 1, "idempotent import must not duplicate the initial enqueue audit")
}

func TestOpenAIWindowWarmupScheduleDoesNotDelayIdleRollingReset(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	rollingReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Extra["codex_5h_used_percent"] = float64(0)
	account.Extra["codex_5h_reset_at"] = rollingReset.Format(time.RFC3339)
	repo := &warmupRepositorySpy{}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)

	job, inserted, err := service.ScheduleAccountWarmup(context.Background(), account, OpenAIWindowWarmupTriggerImport)

	require.NoError(t, err)
	require.True(t, inserted)
	require.Equal(t, now, job.NextAttemptAt)
	require.Equal(t, rollingReset, *job.ObservedResetAt)
}

func TestOpenAIWindowWarmupScheduleRearmsPausedPolicyCycle(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	cycleKey := warmupInitialCycleKey(account, warmupAccountGeneration(account))
	repo := &warmupRepositorySpy{cycles: map[string]*OpenAIWindowWarmupJob{
		OpenAIWindowWarmupQuotaScopeGlobal + ":" + cycleKey: {
			ID: 1, AccountID: account.ID, QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal,
			CycleKey: cycleKey, State: OpenAIWindowWarmupStatePaused,
		},
	}}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)

	job, changed, err := service.ScheduleAccountWarmup(context.Background(), account, OpenAIWindowWarmupTriggerReconcile)

	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, OpenAIWindowWarmupStatePending, job.State)
	require.Equal(t, cycleKey, job.CycleKey)
}

func TestOpenAIWindowWarmupScheduleRearmsPausedSentCycleForPassiveObservation(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	oldReset := now.Add(-time.Hour)
	rollingReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Extra["codex_5h_reset_at"] = rollingReset.Format(time.RFC3339)
	account.Extra["codex_5h_used_percent"] = float64(0)
	cycleKey := warmupInitialCycleKey(account, warmupAccountGeneration(account))
	sentAt := now.Add(-time.Minute)
	repo := &warmupRepositorySpy{cycles: map[string]*OpenAIWindowWarmupJob{
		OpenAIWindowWarmupQuotaScopeGlobal + ":" + cycleKey: {
			ID: 1, AccountID: account.ID, QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal,
			CycleKey: cycleKey, State: OpenAIWindowWarmupStatePaused,
			ObservedResetAt: &oldReset, SentAt: &sentAt, AttemptCount: 1,
		},
	}}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{usage: warmupUsage(rollingReset, 0, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, probe, usage, now, true)

	job, changed, err := service.ScheduleAccountWarmup(context.Background(), account, OpenAIWindowWarmupTriggerReconcile)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, OpenAIWindowWarmupStateUncertain, job.State)
	require.Equal(t, oldReset, *job.ObservedResetAt, "reenable must preserve the pre-send cycle baseline")

	claim := warmupTestClaim(account.ID, oldReset)
	claim.Job = job
	claim.Job.State = OpenAIWindowWarmupStateRunning
	claim.Owner = "reenabled-worker"
	claim.LeaseToken = "1:reenabled"
	claim.PreviousState = OpenAIWindowWarmupStateUncertain
	service.processClaim(context.Background(), claim)

	require.Equal(t, 1, usage.calls)
	require.Equal(t, "uncertain", repo.action)
	require.Zero(t, repo.started)
	require.Zero(t, probe.calls, "the first post-reenable pass must remain passive")
}

func TestOpenAIWindowWarmupScheduleRearmsLatestPausedResetCycle(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	initialKey := warmupInitialCycleKey(account, warmupAccountGeneration(account))
	resetKey := warmupResetCycleKey(now.Add(-time.Hour))
	repo := &warmupRepositorySpy{cycles: map[string]*OpenAIWindowWarmupJob{
		OpenAIWindowWarmupQuotaScopeGlobal + ":" + initialKey: {
			ID: 1, AccountID: account.ID, QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal,
			CycleKey: initialKey, State: OpenAIWindowWarmupStateCompleted,
		},
		OpenAIWindowWarmupQuotaScopeGlobal + ":" + resetKey: {
			ID: 2, AccountID: account.ID, QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal,
			CycleKey: resetKey, State: OpenAIWindowWarmupStatePaused,
		},
	}}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)

	job, changed, err := service.ScheduleAccountWarmup(context.Background(), account, OpenAIWindowWarmupTriggerReconcile)

	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, int64(2), job.ID)
	require.Equal(t, resetKey, job.CycleKey)
}

func TestOpenAIWindowWarmupReconcilerRearmsPausedContinuousResetCycle(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	resetKey := warmupResetCycleKey(now.Add(-time.Hour))
	repo := &warmupRepositorySpy{cycles: map[string]*OpenAIWindowWarmupJob{
		OpenAIWindowWarmupQuotaScopeGlobal + ":" + resetKey: {
			ID: 2, AccountID: account.ID, QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal,
			CycleKey: resetKey, State: OpenAIWindowWarmupStatePaused,
		},
	}}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)

	service.reconcileAccounts(context.Background())

	job, err := repo.GetCurrent(context.Background(), account.ID, OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, OpenAIWindowWarmupStatePending, job.State)
}

func TestOpenAIWindowWarmupReconcilerDoesNotRearmOnceResetCycle(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyInitialOnce)
	resetKey := warmupResetCycleKey(now.Add(-time.Hour))
	repo := &warmupRepositorySpy{cycles: map[string]*OpenAIWindowWarmupJob{
		OpenAIWindowWarmupQuotaScopeGlobal + ":" + resetKey: {
			ID: 2, AccountID: account.ID, QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal,
			CycleKey: resetKey, State: OpenAIWindowWarmupStatePaused,
		},
	}}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)

	service.reconcileAccounts(context.Background())

	job, err := repo.GetCurrent(context.Background(), account.ID, OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, OpenAIWindowWarmupStatePaused, job.State)
}

func TestOpenAIWindowWarmupScanHonorsKillSwitchBeforeClaim(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	repo := &warmupRepositorySpy{}
	service := newWarmupTestService(repo, warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous), &warmupProbeStub{}, nil, now, false)

	service.scanOnce(context.Background())

	require.Zero(t, repo.claimCalls)
}

func TestOpenAIWindowWarmupScanReportsDatabaseQueueStatsWhileDisabled(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	repo := &warmupRepositorySpy{queueStats: OpenAIWindowWarmupQueueStats{
		Enqueued: 11, Due: 7, OldestDueAgeSeconds: 91, Inflight: 2, ResetLagSeconds: 123,
	}}
	service := newWarmupTestService(repo, warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous), &warmupProbeStub{}, nil, now, false)

	service.scanOnce(context.Background())

	metrics := service.Metrics()
	require.Equal(t, int64(11), metrics.Enqueued)
	require.Equal(t, int64(7), metrics.Due)
	require.Equal(t, int64(91), metrics.OldestDueAgeSeconds)
	require.Equal(t, int64(2), metrics.Inflight)
	require.Equal(t, int64(123), metrics.ResetLagSeconds)
	require.Zero(t, repo.claimCalls)
}

func TestOpenAIWindowWarmupScanEmptyAllowlistSelectsAllEligibleAccounts(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	repo := &warmupRepositorySpy{}
	first := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	second := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyInitialOnce)
	second.ID++
	off := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyOff)
	off.ID += 2
	unschedulable := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	unschedulable.ID += 3
	unschedulable.Schedulable = false
	inactive := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	inactive.ID += 4
	inactive.Status = StatusDisabled
	shadow := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	shadow.ID += 5
	shadow.ParentAccountID = &first.ID
	apiKey := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	apiKey.ID += 6
	apiKey.Type = AccountTypeAPIKey
	spark := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	spark.ID += 7
	spark.QuotaDimension = QuotaDimensionSpark
	service := newWarmupTestService(repo, first, &warmupProbeStub{}, nil, now, true)
	service.accounts = &warmupAccountRepositoryStub{accounts: map[int64]*Account{
		first.ID: first, second.ID: second, off.ID: off, unschedulable.ID: unschedulable,
		inactive.ID: inactive, shadow.ID: shadow, apiKey.ID: apiKey, spark.ID: spark,
	}}
	service.options.Allowlist = OpenAIWindowWarmupAllowlistFunc(func(context.Context) ([]int64, error) { return nil, nil })

	service.scanOnce(context.Background())

	require.Equal(t, 1, repo.claimCalls)
	require.ElementsMatch(t, []int64{first.ID, second.ID}, repo.claimAccountIDs)
}

func TestOpenAIWindowWarmupReconcileAndScanShareExplicitCohort(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	first := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	second := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	second.ID++
	repo := &warmupRepositorySpy{}
	service := newWarmupTestService(repo, first, &warmupProbeStub{}, nil, now, true)
	service.accounts = &warmupAccountRepositoryStub{accounts: map[int64]*Account{first.ID: first, second.ID: second}}
	service.options.Allowlist = OpenAIWindowWarmupAllowlistFunc(func(context.Context) ([]int64, error) {
		return []int64{second.ID}, nil
	})

	service.reconcileAccounts(context.Background())
	service.scanOnce(context.Background())

	require.Equal(t, 1, repo.currentBatchCalls)
	require.Len(t, repo.enqueues, 1)
	require.Equal(t, second.ID, repo.enqueues[0].AccountID)
	require.Equal(t, []int64{second.ID}, repo.claimAccountIDs)
}

func TestOpenAIWindowWarmupScheduleSkipsAccountOutsideExplicitCohort(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	service.options.Allowlist = OpenAIWindowWarmupAllowlistFunc(func(context.Context) ([]int64, error) {
		return []int64{account.ID + 1}, nil
	})

	job, inserted, err := service.ScheduleAccountWarmup(context.Background(), account, OpenAIWindowWarmupTriggerImport)

	require.NoError(t, err)
	require.Nil(t, job)
	require.False(t, inserted)
	require.Empty(t, repo.enqueues)
}

func TestOpenAIWindowWarmupCohortReadErrorFailsClosed(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	accounts := &warmupAccountRepositoryStub{account: account}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	service.accounts = accounts
	service.options.Allowlist = OpenAIWindowWarmupAllowlistFunc(func(context.Context) ([]int64, error) {
		return nil, errors.New("settings unavailable")
	})

	service.reconcileAccounts(context.Background())
	service.scanOnce(context.Background())

	require.Zero(t, accounts.listCalls)
	require.Zero(t, repo.currentBatchCalls)
	require.Zero(t, repo.claimCalls)
	require.Empty(t, repo.enqueues)
}

func TestOpenAIWindowWarmupRunScannerClaimsFromCachedCohort(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	accounts := service.accounts.(*warmupAccountRepositoryStub)
	service.options.ScanInterval = time.Hour
	service.claimInterval = 10 * time.Millisecond

	service.Start()
	require.Eventually(t, func() bool {
		repo.mu.Lock()
		defer repo.mu.Unlock()
		return repo.claimCalls >= 3
	}, time.Second, 5*time.Millisecond)
	service.Stop()

	accounts.mu.Lock()
	listCalls := accounts.listCalls
	accounts.mu.Unlock()
	repo.mu.Lock()
	claimCalls := repo.claimCalls
	queueStatsCalls := repo.queueStatsCalls
	repo.mu.Unlock()
	require.GreaterOrEqual(t, claimCalls, 3)
	require.Equal(t, 1, listCalls, "fast claim ticks must reuse the cached cohort")
	require.Equal(t, 1, queueStatsCalls, "fast claim ticks must not rescan queue metrics")
}

func TestOpenAIWindowWarmupCohortRefreshFailureClearsClaimSnapshot(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	accounts := service.accounts.(*warmupAccountRepositoryStub)

	service.refreshCohortAndQueueStats(context.Background())
	require.Equal(t, []int64{account.ID}, service.cachedWarmupCohortIDs())
	accounts.err = errors.New("account store unavailable")
	service.refreshCohortAndQueueStats(context.Background())
	service.claimDueOnce(context.Background())

	require.Empty(t, service.cachedWarmupCohortIDs())
	require.Zero(t, repo.claimCalls, "a failed cohort refresh must fail closed")
}

func TestOpenAIWindowWarmupSuppressesProbeWhenBusinessAdvancedReset(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	oldReset := now.Add(-time.Minute)
	newReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Extra["codex_5h_reset_at"] = newReset.Format(time.RFC3339)
	lastUsed := oldReset.Add(time.Second)
	account.LastUsedAt = &lastUsed
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	service := newWarmupTestService(repo, account, probe, nil, now, true)
	claim := warmupTestClaim(account.ID, oldReset)

	service.processClaim(context.Background(), claim)

	require.Equal(t, "success", repo.action)
	require.Equal(t, newReset, *repo.reset)
	require.Zero(t, probe.calls)
	require.Len(t, repo.enqueues, 1)
	require.Equal(t, warmupResetCycleKey(newReset), repo.enqueues[0].CycleKey)
	require.Equal(t, int64(1), service.Metrics().RealTrafficSuppressed)
}

func TestOpenAIWindowWarmupAmbiguousReconcileSuppressesWithoutSuccess(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	observedReset := now.Add(-time.Minute)
	newReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	usage := &warmupUsageStub{usage: warmupUsage(newReset, 1, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, usage, now, true)
	claim := warmupTestClaim(account.ID, observedReset)
	claim.Job.AttemptCount = 1

	service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
		StatusCode: http.StatusOK,
	}, nil)

	require.Equal(t, "success", repo.action, "the durable cycle is completed, but as suppressed evidence")
	require.Equal(t, "possibly_sent_reconciled", repo.code)
	require.Equal(t, 1, repo.suppressedCalls)
	require.Zero(t, repo.successCalls)
	require.Zero(t, service.Metrics().Success)
	require.Zero(t, service.Metrics().RealTrafficSuppressed)
	require.Len(t, repo.enqueues, 1)
	require.Equal(t, warmupResetCycleKey(newReset), repo.enqueues[0].CycleKey)
}

func TestOpenAIWindowWarmupAmbiguousIdleRollingResetRecordsEvidenceBeforeReplay(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	observedReset := now.Add(-time.Minute)
	rollingReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	usage := &warmupUsageStub{usage: warmupUsage(rollingReset, 0, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, usage, now, true)
	claim := warmupTestClaim(account.ID, observedReset)
	claim.Job.AttemptCount = 1

	service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
		StatusCode: http.StatusOK,
	}, nil)

	require.Equal(t, "uncertain", repo.action)
	require.Equal(t, OpenAIWindowWarmupStateUncertain, repo.state)
	require.Equal(t, "possibly_sent", repo.code)
	require.True(t, repo.uncertainEvidence.Authoritative)
	require.Equal(t, rollingReset, *repo.uncertainEvidence.ResetAt)
	require.Zero(t, repo.suppressedCalls)
	require.Zero(t, repo.successCalls)
	require.Empty(t, repo.enqueues)
}

func TestOpenAIWindowWarmupCompletedTerminalCanConfirmResetFromUsage(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	observedReset := now.Add(-time.Minute)
	newReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	usage := &warmupUsageStub{usage: warmupUsage(newReset, 1, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, usage, now, true)
	claim := warmupTestClaim(account.ID, observedReset)
	claim.Job.AttemptCount = 1

	service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
		StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.completed",
	}, errors.New("possibly_sent: terminal response omitted reset metadata"))

	require.Equal(t, 1, repo.successCalls)
	require.Zero(t, repo.suppressedCalls)
	require.Equal(t, "completed_reconciled", repo.code)
	require.Len(t, repo.enqueues, 1)
	require.Equal(t, warmupResetCycleKey(newReset), repo.enqueues[0].CycleKey)
}

func TestOpenAIWindowWarmupWaitsWhenClaimStillHasCurrentFutureReset(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	reset := now.Add(time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Extra["codex_5h_reset_at"] = reset.Format(time.RFC3339)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	service := newWarmupTestService(repo, account, probe, nil, now, true)

	service.processClaim(context.Background(), warmupTestClaim(account.ID, reset))

	require.Equal(t, "reschedule", repo.action)
	require.Equal(t, OpenAIWindowWarmupStateArmed, repo.state)
	require.Equal(t, reset, *repo.reset)
	require.Equal(t, reset.Add(openAIWindowWarmupDefaultGrace), repo.next)
	require.Zero(t, probe.calls)
	require.Zero(t, repo.suppressedCalls)
}

func TestOpenAIWindowWarmupUsagePreflightSuppressesStaleAccountSnapshot(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	oldReset := now.Add(-time.Minute)
	newReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	usage := &warmupUsageStub{usage: warmupUsage(newReset, 1, now.Add(24*time.Hour), 1)}
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	service := newWarmupTestService(repo, account, probe, usage, now, true)

	service.processClaim(context.Background(), warmupTestClaim(account.ID, oldReset))

	require.Equal(t, "success", repo.action)
	require.Equal(t, "usage_preflight_suppressed", repo.code)
	require.Equal(t, newReset, *repo.reset)
	require.Equal(t, 1, usage.calls)
	require.Zero(t, probe.calls)
	require.Equal(t, int64(1), service.Metrics().RealTrafficSuppressed)
}

func TestOpenAIWindowWarmupUsagePreflightSendsForIdleRollingReset(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	oldReset := now.Add(-time.Minute)
	rollingReset := now.Add(5 * time.Hour)
	confirmedReset := rollingReset.Add(time.Minute)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Extra["codex_5h_used_percent"] = float64(0)
	account.Extra["codex_5h_reset_at"] = rollingReset.Format(time.RFC3339)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{result: &OpenAIWindowProbeResult{
		StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.completed", ResetAt: &confirmedReset,
	}}
	usage := &warmupUsageStub{usage: warmupUsage(rollingReset, 0, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, probe, usage, now, true)

	claim := warmupTestClaim(account.ID, oldReset)
	service.processClaim(context.Background(), claim)

	require.Equal(t, 1, usage.calls)
	require.Equal(t, 1, probe.calls)
	require.Equal(t, 1, repo.started)
	require.True(t, repo.startEvidence.Authoritative)
	require.Zero(t, repo.startEvidence.UsedPercent)
	require.Equal(t, rollingReset, *repo.startEvidence.ResetAt)
	require.Len(t, probe.expectedResets, 1)
	require.Equal(t, rollingReset, *probe.expectedResets[0])
	require.Equal(t, rollingReset, *claim.Job.PreflightResetAt)
	require.Equal(t, now, *claim.Job.PreflightObservedAt)
	require.Equal(t, 1, repo.successCalls)
	require.Equal(t, "completed", repo.code)
	require.Len(t, repo.enqueues, 1)
	require.Equal(t, warmupResetCycleKey(confirmedReset), repo.enqueues[0].CycleKey)
	require.Equal(t, 1, repo.projectionCalls)
	require.Equal(t, account.ID, repo.projectedAccount)
	require.Equal(t, account.OpenAIWarmupIdentityGeneration, repo.projectedIdentity)
	require.Equal(t, confirmedReset, repo.projectedReset)
}

func TestOpenAIWindowWarmupIdleRelativeResetRequiresPassiveConfirmation(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	oldReset := now.Add(-time.Minute)
	rollingReset := now.Add(5 * time.Hour)
	// This is the same rolling reset-after projection reconstructed one second
	// later, not evidence that the target five-hour window became active.
	parsedLater := rollingReset.Add(time.Second)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Extra["codex_5h_used_percent"] = float64(0)
	account.Extra["codex_5h_reset_at"] = rollingReset.Format(time.RFC3339)
	parsed := ParseOpenAIWindowWarmupResult(&OpenAIOutboundResult{
		StatusCode:   http.StatusOK,
		Terminal:     true,
		TerminalType: "response.completed",
		ResetAt:      &parsedLater,
		Headers: http.Header{
			"X-Codex-Secondary-Window-Minutes":      {"300"},
			"X-Codex-Secondary-Reset-After-Seconds": {"18000"},
		},
	}, &rollingReset)
	require.True(t, parsed.resetFromRelativeHeader)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{result: parsed}
	usage := &warmupUsageStub{usages: []*OpenAIQuotaUsage{
		warmupUsage(rollingReset, 0, now.Add(24*time.Hour), 1),
		warmupUsage(parsedLater, 0, now.Add(24*time.Hour), 1),
	}}
	service := newWarmupTestService(repo, account, probe, usage, now, true)

	service.processClaim(context.Background(), warmupTestClaim(account.ID, oldReset))

	require.Equal(t, 2, usage.calls)
	require.Equal(t, 1, probe.calls)
	require.Equal(t, 1, repo.started)
	require.Zero(t, repo.successCalls)
	require.Equal(t, "uncertain", repo.action)
	require.Equal(t, "completed_reset_unconfirmed", repo.code)
	require.True(t, repo.uncertainEvidence.Terminal)
	require.Equal(t, parsedLater, *repo.uncertainEvidence.ResetAt)
	require.Empty(t, repo.enqueues)
}

func TestOpenAIWindowWarmupPositiveUsageConfirmsIdleRelativeReset(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	oldReset := now.Add(-time.Minute)
	rollingReset := now.Add(5 * time.Hour)
	parsedLater := rollingReset.Add(time.Second)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Extra["codex_5h_used_percent"] = float64(0)
	account.Extra["codex_5h_reset_at"] = rollingReset.Format(time.RFC3339)
	parsed := ParseOpenAIWindowWarmupResult(&OpenAIOutboundResult{
		StatusCode: http.StatusOK,
		Body:       []byte(`data: {"type":"response.completed","response":{"id":"resp_usage","status":"completed","usage":{"input_tokens":9,"output_tokens":1,"total_tokens":10}}}` + "\n\n"),
		ResetAt:    &parsedLater,
		RequestID:  "req_usage",
		Headers: http.Header{
			"X-Codex-Secondary-Window-Minutes":      {"300"},
			"X-Codex-Secondary-Reset-After-Seconds": {"18000"},
		},
	}, &rollingReset)
	require.True(t, parsed.resetFromRelativeHeader)
	require.True(t, parsed.Usage.Positive())
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{result: parsed}
	usage := &warmupUsageStub{usage: warmupUsage(rollingReset, 0, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, probe, usage, now, true)

	service.processClaim(context.Background(), warmupTestClaim(account.ID, oldReset))

	require.Equal(t, 1, usage.calls, "positive terminal usage avoids a second passive observation")
	require.Equal(t, 1, probe.calls)
	require.Equal(t, 1, repo.started)
	require.Equal(t, 1, repo.successCalls)
	require.Equal(t, "completed", repo.code)
	require.Len(t, repo.enqueues, 1)
	require.Equal(t, warmupResetCycleKey(parsedLater, account.OpenAIWarmupIdentityGeneration), repo.enqueues[0].CycleKey)
}

func TestOpenAIWindowWarmupInitialNilCycleBaselineRequiresPreflightAdvance(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	rollingReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Extra["codex_5h_used_percent"] = float64(0)
	account.Extra["codex_5h_reset_at"] = rollingReset.Format(time.RFC3339)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{result: &OpenAIWindowProbeResult{
		StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.completed", ResetAt: &rollingReset,
	}}
	usage := &warmupUsageStub{usage: warmupUsage(rollingReset, 0, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
	claim.Job.ObservedResetAt = nil
	claim.Job.CycleKey = warmupInitialCycleKey(account, warmupAccountGeneration(account))

	service.processClaim(context.Background(), claim)

	require.Equal(t, 1, repo.started)
	require.Len(t, probe.expectedResets, 1)
	require.Equal(t, rollingReset, *probe.expectedResets[0])
	require.Equal(t, rollingReset, *claim.Job.PreflightResetAt)
	require.Equal(t, now, *claim.Job.PreflightObservedAt)
	require.Zero(t, repo.successCalls, "a reset equal to the persisted preflight baseline is not advancement")
	require.Equal(t, "uncertain", repo.action)
	require.Equal(t, "completed_reset_unconfirmed", repo.code)
	require.Empty(t, repo.enqueues)
}

func TestWarmupAttemptResetAdvancedFailsClosedWithoutDurableBaseline(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	reset := now.Add(5 * time.Hour)
	job := &OpenAIWindowWarmupJob{}

	require.False(t, warmupAttemptResetAdvanced(&reset, job, now))

	observedAt := now.Add(-time.Second)
	job.PreflightObservedAt = &observedAt
	require.True(t, warmupAttemptResetAdvanced(&reset, job, now),
		"an authoritative preflight with no reset may establish the first future reset")
}

func TestOpenAIWindowWarmupFreshIdleInitialCycleDoesNotRearmEqualReset(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	rollingReset := now.Add(5 * time.Hour)
	confirmedReset := rollingReset.Add(time.Minute)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Extra["codex_5h_used_percent"] = float64(0)
	account.Extra["codex_5h_reset_at"] = rollingReset.Format(time.RFC3339)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{result: &OpenAIWindowProbeResult{
		StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.completed", ResetAt: &confirmedReset,
	}}
	usage := &warmupUsageStub{usage: warmupUsage(rollingReset, 0, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, rollingReset)
	claim.Job.CycleKey = warmupInitialCycleKey(account, warmupAccountGeneration(account))

	service.processClaim(context.Background(), claim)

	require.Equal(t, 1, usage.calls)
	require.Equal(t, 1, repo.started)
	require.Equal(t, 1, probe.calls)
	require.Equal(t, 1, repo.successCalls)
	require.Equal(t, "completed", repo.code)
}

func TestOpenAIWindowWarmupMarkStartedCASRejectsNewerIdleRollingResetWithoutSuppressing(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	oldReset := now.Add(-time.Minute)
	preflightReset := now.Add(5 * time.Hour)
	racedReset := preflightReset.Add(time.Minute)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{rejectStarted: true}
	repo.onMarkStarted = func() {
		account.Extra["codex_5h_used_percent"] = float64(0)
		account.Extra["codex_5h_reset_at"] = racedReset.Format(time.RFC3339Nano)
	}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{usage: warmupUsage(preflightReset, 0, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, probe, usage, now, true)

	service.processClaim(context.Background(), warmupTestClaim(account.ID, oldReset))

	require.Equal(t, 1, usage.calls)
	require.Equal(t, 1, repo.started, "the final repository CAS was attempted")
	require.Equal(t, "reschedule", repo.action)
	require.Equal(t, OpenAIWindowWarmupStateRetrying, repo.state)
	require.Equal(t, now.Add(service.options.ScanInterval), repo.next)
	require.Nil(t, repo.reset)
	require.Zero(t, repo.suppressedCalls)
	require.Zero(t, probe.calls)
	require.Empty(t, repo.enqueues)
}

func TestOpenAIWindowWarmupIdleRollingResetHonorsRecentBusinessUse(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	oldReset := now.Add(-time.Minute)
	rollingReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Extra["codex_5h_reset_at"] = rollingReset.Format(time.RFC3339)
	claim := warmupTestClaim(account.ID, oldReset)
	lastUsed := oldReset.Add(time.Second)
	account.LastUsedAt = &lastUsed
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{usage: warmupUsage(rollingReset, 0, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, probe, usage, now, true)

	service.processClaim(context.Background(), claim)

	require.Equal(t, "real_traffic_suppressed", repo.code)
	require.Zero(t, usage.calls)
	require.Zero(t, probe.calls)
	require.Zero(t, repo.started)
}

func TestOpenAIWindowWarmupIdleRollingResetIgnoresUseBeforeObservedReset(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	oldReset := now.Add(-time.Minute)
	rollingReset := now.Add(5 * time.Hour)
	confirmedReset := rollingReset.Add(time.Minute)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Extra["codex_5h_reset_at"] = rollingReset.Format(time.RFC3339)
	claim := warmupTestClaim(account.ID, oldReset)
	lastUsed := oldReset.Add(-time.Second)
	account.LastUsedAt = &lastUsed
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{result: &OpenAIWindowProbeResult{
		StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.done", ResetAt: &confirmedReset,
	}}
	usage := &warmupUsageStub{usage: warmupUsage(rollingReset, 0, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, probe, usage, now, true)

	service.processClaim(context.Background(), claim)

	require.Equal(t, 1, probe.calls)
	require.Equal(t, 1, repo.started)
	require.Equal(t, 1, repo.successCalls)
}

func TestOpenAIWindowWarmupUncertainIdleRollingResetFirstObservationNeverReplays(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	observedReset := now.Add(-time.Minute)
	rollingReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{usage: warmupUsage(rollingReset, 0, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, observedReset)
	sentAt := now.Add(-time.Minute)
	claim.Job.SentAt = &sentAt
	claim.Job.AttemptCount = 1
	claim.PreviousState = OpenAIWindowWarmupStateUncertain

	service.processClaim(context.Background(), claim)

	require.Equal(t, "uncertain", repo.action)
	require.Equal(t, "possibly_sent", repo.code)
	require.Equal(t, 1, usage.calls)
	require.True(t, repo.uncertainEvidence.Authoritative)
	require.Equal(t, rollingReset, *repo.uncertainEvidence.ResetAt)
	require.Zero(t, repo.suppressedCalls)
	require.Zero(t, repo.successCalls)
	require.Zero(t, repo.started)
	require.Zero(t, probe.calls)
	require.Empty(t, repo.enqueues)
}

func TestOpenAIWindowWarmupUncertainRollingResetAllowsReplayAfterSecondObservation(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	observedReset := now.Add(-time.Minute)
	firstRollingReset := now.Add(5*time.Hour - 2*time.Minute)
	secondRollingReset := now.Add(5 * time.Hour)
	confirmedReset := secondRollingReset.Add(time.Minute)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{result: &OpenAIWindowProbeResult{
		StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.completed", ResetAt: &confirmedReset,
	}}
	usage := &warmupUsageStub{usages: []*OpenAIQuotaUsage{
		warmupUsage(secondRollingReset, 0, now.Add(24*time.Hour), 1),
		warmupUsage(secondRollingReset, 0, now.Add(24*time.Hour), 1),
	}}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, observedReset)
	sentAt := now.Add(-3 * time.Minute)
	firstObservedAt := now.Add(-2 * time.Minute)
	claim.Job.SentAt = &sentAt
	claim.Job.AttemptCount = 1
	claim.Job.UncertainObservedResetAt = &firstRollingReset
	claim.Job.UncertainObservedAt = &firstObservedAt
	claim.PreviousState = OpenAIWindowWarmupStateUncertain

	service.processClaim(context.Background(), claim)

	require.Equal(t, 2, usage.calls)
	require.Equal(t, 1, probe.calls)
	require.Equal(t, 1, repo.started)
	require.Equal(t, 1, repo.successCalls)
	require.Zero(t, repo.suppressedCalls)
	require.Len(t, repo.enqueues, 1)
	require.Equal(t, warmupResetCycleKey(confirmedReset), repo.enqueues[0].CycleKey)
}

func TestOpenAIWindowWarmupUncertainFixedResetSuppressesWithoutReplay(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	observedReset := now.Add(-time.Minute)
	fixedReset := now.Add(4 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{usage: warmupUsage(fixedReset, 0, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, observedReset)
	sentAt := now.Add(-3 * time.Minute)
	firstObservedAt := now.Add(-2 * time.Minute)
	claim.Job.SentAt = &sentAt
	claim.Job.AttemptCount = 1
	claim.Job.UncertainObservedResetAt = &fixedReset
	claim.Job.UncertainObservedAt = &firstObservedAt
	claim.PreviousState = OpenAIWindowWarmupStateUncertain

	service.processClaim(context.Background(), claim)

	require.Equal(t, 1, usage.calls)
	require.Zero(t, probe.calls)
	require.Zero(t, repo.started)
	require.Zero(t, repo.successCalls)
	require.Equal(t, 1, repo.suppressedCalls)
	require.Equal(t, "possibly_sent_reconciled", repo.code)
	require.Len(t, repo.enqueues, 1)
	require.Equal(t, warmupResetCycleKey(fixedReset), repo.enqueues[0].CycleKey)
}

func TestOpenAIWindowWarmupUncertainTerminalFixedResetCompletesSuccess(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	observedReset := now.Add(-time.Minute)
	fixedReset := now.Add(4 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	usage := &warmupUsageStub{usage: warmupUsage(fixedReset, 0, now.Add(24*time.Hour), 1)}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, usage, now, true)
	claim := warmupTestClaim(account.ID, observedReset)
	sentAt := now.Add(-3 * time.Minute)
	firstObservedAt := now.Add(-2 * time.Minute)
	status := http.StatusOK
	claim.Job.SentAt = &sentAt
	claim.Job.AttemptCount = 1
	claim.Job.StatusCode = &status
	claim.Job.UncertainObservedResetAt = &fixedReset
	claim.Job.UncertainObservedAt = &firstObservedAt
	claim.Job.UncertainTerminalObserved = true
	claim.PreviousState = OpenAIWindowWarmupStateUncertain

	service.processClaim(context.Background(), claim)

	require.Equal(t, 1, usage.calls)
	require.Equal(t, 1, repo.successCalls)
	require.Zero(t, repo.suppressedCalls)
	require.Equal(t, "completed_reconciled", repo.code)
	require.Len(t, repo.enqueues, 1)
}

func TestClassifyWarmupUncertainObservationDoesNotTreatResetRegressionAsFixed(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	observedAt := now.Add(-2 * time.Minute)
	previousReset := now.Add(4 * time.Hour)
	job := &OpenAIWindowWarmupJob{
		UncertainObservedAt:      &observedAt,
		UncertainObservedResetAt: &previousReset,
	}

	regressedReset := previousReset.Add(-time.Hour)
	require.Equal(t, warmupUncertainRecordObservation,
		classifyWarmupUncertainObservation(job, &regressedReset, now))

	stableReset := previousReset.Add(-openAIWindowWarmupResetStabilityTolerance / 2)
	require.Equal(t, warmupUncertainFixedReset,
		classifyWarmupUncertainObservation(job, &stableReset, now))
}

func TestOpenAIWindowWarmupDefinitiveHTTPStatusPrecedesTransportAmbiguity(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name, wantAction, wantState, wantCode string
		status                                int
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, wantAction: "blocked", wantState: OpenAIWindowWarmupStateBlocked, wantCode: "needs_reauth"},
		{name: "forbidden", status: http.StatusForbidden, wantAction: "blocked", wantState: OpenAIWindowWarmupStateBlocked, wantCode: "blocked"},
		{name: "bad request", status: http.StatusBadRequest, wantAction: "blocked", wantState: OpenAIWindowWarmupStateBlockedConfig, wantCode: "blocked_config"},
		{name: "not found", status: http.StatusNotFound, wantAction: "blocked", wantState: OpenAIWindowWarmupStateBlockedConfig, wantCode: "blocked_config"},
		// A 5xx response does not prove that the upstream rejected the POST;
		// it must be fenced through passive usage reconciliation first.
		{name: "server error", status: http.StatusServiceUnavailable, wantAction: "uncertain", wantState: OpenAIWindowWarmupStateUncertain, wantCode: "possibly_sent"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
			repo := &warmupRepositorySpy{}
			usage := &warmupUsageStub{}
			service := newWarmupTestService(repo, account, &warmupProbeStub{}, usage, now, true)
			claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
			claim.Job.AttemptCount = 1

			service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
				StatusCode: test.status,
			}, errors.New("possibly_sent: unexpected EOF while reading response"))

			require.Equal(t, test.wantAction, repo.action)
			require.Equal(t, test.wantState, repo.state)
			require.Equal(t, test.wantCode, repo.code)
			if test.status >= 500 && test.status <= 599 {
				require.Equal(t, 1, usage.calls)
			} else {
				require.Zero(t, usage.calls)
			}
			require.Zero(t, repo.suppressedCalls)
		})
	}
}

func TestOpenAIWindowWarmupAuthStateSideEffectRequiresFencedJobTransition(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Credentials = map[string]any{"access_token": "rejected", "refresh_token": "refresh"}
	failure := &OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusUnauthorized,
		Disposition:         OpenAIWindowWarmupAuthReplayRejected,
		ExpectedCredentials: shallowCopyMap(account.Credentials),
	}

	for _, test := range []struct {
		name          string
		rejectBlocked bool
		wantCalls     int
	}{
		{name: "current owner", wantCalls: 1},
		{name: "stale owner", rejectBlocked: true, wantCalls: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := &warmupRepositorySpy{rejectBlocked: test.rejectBlocked}
			handler := &warmupAuthFailureHandlerSpy{}
			service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
			service.options.AuthFailureHandler = handler
			claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
			claim.Job.AttemptCount = 1

			service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
				StatusCode: http.StatusUnauthorized, AuthFailure: failure,
			}, ErrOpenAIWindowWarmupNeedsReauth)

			require.Len(t, handler.failures, test.wantCalls)
			require.Zero(t, repo.authRetryCalls)
		})
	}
}

func TestOpenAIWindowWarmupAuthStateWriteFailureRequeuesOnlyExactFencedTransition(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Credentials = map[string]any{"access_token": "rejected", "refresh_token": "refresh"}
	failure := &OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusUnauthorized,
		Disposition:         OpenAIWindowWarmupAuthReplayRejected,
		ExpectedCredentials: shallowCopyMap(account.Credentials),
	}

	for _, test := range []struct {
		name          string
		rejectBlocked bool
		attemptCount  int
		handlerErr    error
		retryCode     string
		wantHandler   int
		wantRetry     int
	}{
		{name: "current owner retries account state", attemptCount: 1, handlerErr: errors.New("database unavailable"), retryCode: "account_state_update_failed", wantHandler: 1, wantRetry: 1},
		{name: "credentials changed rearms cycle", attemptCount: 1, handlerErr: ErrOpenAIWindowWarmupCredentialsChanged, retryCode: "credentials_changed", wantHandler: 1, wantRetry: 1},
		{name: "stale owner cannot retry", rejectBlocked: true, attemptCount: 1, handlerErr: errors.New("database unavailable")},
		{name: "attempt limit remains blocked", attemptCount: openAIWindowWarmupDefaultMaxAttempts, handlerErr: errors.New("database unavailable"), wantHandler: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := &warmupRepositorySpy{rejectBlocked: test.rejectBlocked}
			handler := &warmupAuthFailureHandlerSpy{err: test.handlerErr}
			service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
			service.options.AuthFailureHandler = handler
			claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
			claim.Job.AttemptCount = test.attemptCount

			service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
				StatusCode: http.StatusUnauthorized, AuthFailure: failure,
			}, ErrOpenAIWindowWarmupNeedsReauth)

			require.Len(t, handler.failures, test.wantHandler)
			require.Equal(t, test.wantRetry, repo.authRetryCalls)
			if test.wantRetry == 1 {
				require.Equal(t, claim.Job.ID, repo.authRetry.JobID)
				require.Equal(t, claim.Job.CycleGeneration, repo.authRetry.CycleGeneration)
				require.Equal(t, claim.Job.AttemptCount, repo.authRetry.AttemptCount)
				require.Equal(t, OpenAIWindowWarmupStateBlocked, repo.authRetry.BlockedState)
				require.Equal(t, http.StatusUnauthorized, repo.authRetry.StatusCode)
				require.Equal(t, "needs_reauth", repo.authRetry.ErrorCode)
				require.Equal(t, test.retryCode, repo.authRetry.RetryCode)
			}
		})
	}
}

func TestOpenAIWindowWarmupRefreshTransientRetriesAndTemporarilyIsolates(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	handler := &warmupAuthFailureHandlerSpy{}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	service.options.AuthFailureHandler = handler
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
	claim.Job.AttemptCount = 1
	failure := &OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusUnauthorized,
		Disposition:         OpenAIWindowWarmupAuthRefreshTransient,
		ExpectedCredentials: map[string]any{"access_token": "rejected"},
	}

	service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
		StatusCode: http.StatusUnauthorized, AuthFailure: failure,
	}, errors.New("possibly_sent: refresh failed"))

	require.Equal(t, "retry", repo.action)
	require.Equal(t, string(OpenAIWindowWarmupAuthRefreshTransient), repo.code)
	require.Len(t, handler.failures, 1)
}

func TestOpenAIWindowWarmupRefreshTransientAtAttemptLimitStillIsolates(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	handler := &warmupAuthFailureHandlerSpy{}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	service.options.AuthFailureHandler = handler
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
	claim.Job.AttemptCount = service.options.MaxAttempts
	failure := &OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusUnauthorized,
		Disposition:         OpenAIWindowWarmupAuthRefreshTransient,
		ExpectedCredentials: map[string]any{"access_token": "rejected"},
	}

	service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
		StatusCode: http.StatusUnauthorized, AuthFailure: failure,
	}, errors.New("possibly_sent: refresh failed"))

	require.Equal(t, "blocked", repo.action)
	require.Equal(t, OpenAIWindowWarmupStateBlocked, repo.state)
	require.Len(t, handler.failures, 1)
	require.Equal(t, OpenAIWindowWarmupAuthRefreshTransient, handler.failures[0].Disposition)
}

func TestOpenAIWindowWarmupForbiddenAuthUsesBoundedRetry(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)

	for _, disposition := range []OpenAIWindowWarmupAuthDisposition{
		OpenAIWindowWarmupAuthForbidden,
		OpenAIWindowWarmupAuthForbiddenHTML,
	} {
		t.Run(string(disposition), func(t *testing.T) {
			repo := &warmupRepositorySpy{}
			handler := &warmupAuthFailureHandlerSpy{}
			service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
			service.options.AuthFailureHandler = handler
			claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
			claim.Job.AttemptCount = 1
			failure := &OpenAIWindowWarmupAuthFailure{
				AccountID: account.ID, StatusCode: http.StatusForbidden,
				Disposition: disposition, ExpectedCredentials: map[string]any{"access_token": "rejected"},
			}

			service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
				StatusCode: http.StatusForbidden, AuthFailure: failure,
			}, ErrOpenAIWindowWarmupBlocked)

			require.Equal(t, "retry", repo.action)
			require.Equal(t, OpenAIWindowWarmupStateRetrying, repo.state)
			require.Equal(t, string(disposition), repo.code)
			require.Len(t, handler.failures, 1)
		})
	}
}

func TestOpenAIWindowWarmupForbiddenAuthAtAttemptLimitBlocks(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	handler := &warmupAuthFailureHandlerSpy{}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	service.options.AuthFailureHandler = handler
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
	claim.Job.AttemptCount = service.options.MaxAttempts
	failure := &OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusForbidden,
		Disposition: OpenAIWindowWarmupAuthForbiddenHTML,
	}

	service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
		StatusCode: http.StatusForbidden, AuthFailure: failure,
	}, ErrOpenAIWindowWarmupBlocked)

	require.Equal(t, "blocked", repo.action)
	require.Equal(t, "attempt_limit", repo.code)
	require.Len(t, handler.failures, 1)
}

func TestOpenAIWindowWarmupUsageAuthFailureUpdatesStateAfterDurableOutcome(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	handler := &warmupAuthFailureHandlerSpy{}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	service.options.AuthFailureHandler = handler
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
	failure := &OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusUnauthorized,
		Disposition:         OpenAIWindowWarmupAuthNotRefreshable,
		ExpectedCredentials: map[string]any{"access_token": "pat"},
	}
	err := withOpenAIWindowWarmupAuthFailure(
		infraerrors.New(http.StatusUnauthorized, "OPENAI_QUOTA_UPSTREAM_ERROR", "upstream returned 401"), failure,
	)

	service.handleUsageObservationFailure(context.Background(), claim, now, OpenAIWindowWarmupStateRetrying, "preflight", err)

	require.Equal(t, "blocked", repo.action)
	require.Len(t, handler.failures, 1)
	require.Equal(t, OpenAIWindowWarmupAuthNotRefreshable, handler.failures[0].Disposition)
}

func TestOpenAIWindowWarmupUsageAuthStateWriteFailureDurablyRetries(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	handler := &warmupAuthFailureHandlerSpy{err: errors.New("database unavailable")}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	service.options.AuthFailureHandler = handler
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
	failure := &OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusUnauthorized,
		Disposition:         OpenAIWindowWarmupAuthNotRefreshable,
		ExpectedCredentials: map[string]any{"access_token": "pat"},
	}
	err := withOpenAIWindowWarmupAuthFailure(
		infraerrors.New(http.StatusUnauthorized, "OPENAI_QUOTA_UPSTREAM_ERROR", "upstream returned 401"), failure,
	)

	service.handleUsageObservationFailure(context.Background(), claim, now, OpenAIWindowWarmupStateRetrying, "preflight", err)

	require.Equal(t, 1, repo.authRetryCalls)
	require.Equal(t, 1, repo.authRetry.AttemptCount)
	require.Equal(t, OpenAIWindowWarmupStateBlocked, repo.authRetry.BlockedState)
	require.Equal(t, "needs_reauth", repo.authRetry.ErrorCode)
	require.Equal(t, "account_state_update_failed", repo.authRetry.RetryCode)
}

func TestOpenAIWindowWarmupScanContinuesAfterAuthStateWriteFailure(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	first := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	first.Credentials = map[string]any{"access_token": "rejected", "refresh_token": "refresh"}
	second := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	second.ID = first.ID + 1
	second.Name = "warmup-next-account"

	firstClaim := warmupTestClaim(first.ID, now.Add(-time.Minute))
	secondClaim := warmupTestClaim(second.ID, now.Add(-time.Minute))
	secondClaim.Job.ID = firstClaim.Job.ID + 1
	newReset := now.Add(5 * time.Hour)
	authFailure := &OpenAIWindowWarmupAuthFailure{
		AccountID: first.ID, StatusCode: http.StatusUnauthorized,
		Disposition:         OpenAIWindowWarmupAuthReplayRejected,
		ExpectedCredentials: shallowCopyMap(first.Credentials),
	}
	repo := &warmupRepositorySpy{
		claims: []OpenAIWindowWarmupClaim{firstClaim, secondClaim}, consumeClaims: true,
	}
	probe := &warmupProbeStub{
		results: []*OpenAIWindowProbeResult{
			{StatusCode: http.StatusUnauthorized, AuthFailure: authFailure},
			{StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.completed", ResetAt: &newReset},
		},
		errs: []error{ErrOpenAIWindowWarmupNeedsReauth, nil},
	}
	service := newWarmupTestService(repo, first, probe, nil, now, true)
	service.accounts = &warmupAccountRepositoryStub{accounts: map[int64]*Account{first.ID: first, second.ID: second}}
	service.options.Allowlist = OpenAIWindowWarmupAllowlistFunc(func(context.Context) ([]int64, error) {
		return []int64{first.ID, second.ID}, nil
	})
	service.options.AuthFailureHandler = &warmupAuthFailureHandlerSpy{err: errors.New("database unavailable")}

	service.scanOnce(context.Background())
	require.Eventually(t, func() bool { return service.workerInflight.Load() == 0 }, time.Second, time.Millisecond)
	service.limiter.mu.Lock()
	service.limiter.next = time.Time{}
	service.limiter.mu.Unlock()
	service.scanOnce(context.Background())
	require.Eventually(t, func() bool { return service.workerInflight.Load() == 0 }, time.Second, time.Millisecond)

	require.Equal(t, 2, probe.calls)
	require.Equal(t, 1, repo.authRetryCalls)
	require.Equal(t, 1, repo.successCalls)
}

func TestOpenAIWindowWarmupSuppressionMetricClassification(t *testing.T) {
	for _, code := range []string{"real_traffic_suppressed", "usage_preflight_suppressed", "mark_started_reset_cas"} {
		require.True(t, isRealTrafficWarmupSuppression(code), code)
	}
	for _, code := range []string{"possibly_sent_reconciled", "lease_takeover_reconciled", ""} {
		require.False(t, isRealTrafficWarmupSuppression(code), code)
	}
}

func TestOpenAIWindowWarmupUsagePreflightFailsClosed(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{err: errors.New("usage temporarily unavailable")}
	service := newWarmupTestService(repo, account, probe, usage, now, true)

	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
	service.processClaim(context.Background(), claim)

	require.Equal(t, "retry", repo.action)
	require.Equal(t, "usage_preflight_failed", repo.code)
	require.Equal(t, 1, claim.Job.AttemptCount)
	require.Zero(t, repo.started)
	require.Zero(t, probe.calls)
}

func TestOpenAIWindowWarmupUsagePreflightRejectsUnknownSchema(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{usage: &OpenAIQuotaUsage{}}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))

	service.processClaim(context.Background(), claim)

	require.Equal(t, "retry", repo.action)
	require.Equal(t, "usage_preflight_failed", repo.code)
	require.Zero(t, repo.started)
	require.Zero(t, probe.calls)
}

func TestOpenAIWindowWarmupUsagePreflightAcceptsOptionalWeeklyWindow(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	rollingReset := now.Add(5 * time.Hour)
	confirmedReset := rollingReset.Add(time.Minute)
	for _, secondary := range []struct {
		name, field string
	}{
		{name: "omitted"},
		{name: "null", field: `,"secondary_window":null`},
	} {
		t.Run(secondary.name, func(t *testing.T) {
			var usageValue OpenAIQuotaUsage
			body := fmt.Sprintf(`{"fetched_at":%d,"rate_limit":{"allowed":true,"primary_window":{"used_percent":0,"limit_window_seconds":18000,"reset_at":%d}%s}}`,
				now.Unix(), rollingReset.Unix(), secondary.field)
			require.NoError(t, json.Unmarshal([]byte(body), &usageValue))
			account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
			repo := &warmupRepositorySpy{}
			probe := &warmupProbeStub{result: &OpenAIWindowProbeResult{
				StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.completed", ResetAt: &confirmedReset,
			}}
			usage := &warmupUsageStub{usage: &usageValue}
			service := newWarmupTestService(repo, account, probe, usage, now, true)

			service.processClaim(context.Background(), warmupTestClaim(account.ID, now.Add(-time.Minute)))

			require.Equal(t, 1, probe.calls)
			require.Equal(t, 1, repo.started)
			require.Equal(t, 1, repo.successCalls)
		})
	}
}

func TestOpenAIWindowWarmupUsagePreflightPermanentErrorsBlock(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name, code, wantState string
		status                int
	}{
		{name: "unauthorized", status: http.StatusUnauthorized, code: "needs_reauth", wantState: OpenAIWindowWarmupStateBlocked},
		{name: "forbidden", status: http.StatusForbidden, code: "blocked", wantState: OpenAIWindowWarmupStateBlocked},
		{name: "bad request", status: http.StatusBadRequest, code: "blocked_config", wantState: OpenAIWindowWarmupStateBlockedConfig},
		{name: "not found", status: http.StatusNotFound, code: "blocked_config", wantState: OpenAIWindowWarmupStateBlockedConfig},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
			repo := &warmupRepositorySpy{}
			probe := &warmupProbeStub{}
			usage := &warmupUsageStub{err: infraerrors.New(test.status, "UPSTREAM", "sanitized")}
			service := newWarmupTestService(repo, account, probe, usage, now, true)

			service.processClaim(context.Background(), warmupTestClaim(account.ID, now.Add(-time.Minute)))

			require.Equal(t, "blocked", repo.action)
			require.Equal(t, test.wantState, repo.state)
			require.Equal(t, test.status, repo.status)
			require.Equal(t, test.code, repo.code)
			require.Zero(t, repo.started)
			require.Zero(t, probe.calls)
		})
	}
}

func TestOpenAIWindowWarmupUsagePreflightRetryableForbiddenDoesNotPermanentlyBlock(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	handler := &warmupAuthFailureHandlerSpy{}
	failure := &OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusForbidden,
		Disposition:         OpenAIWindowWarmupAuthForbidden,
		ExpectedCredentials: shallowCopyMap(account.Credentials),
	}
	usage := &warmupUsageStub{err: withOpenAIWindowWarmupAuthFailure(
		infraerrors.New(http.StatusForbidden, "OPENAI_QUOTA_UPSTREAM_ERROR", "upstream returned 403"), failure,
	)}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, usage, now, true)
	service.options.AuthFailureHandler = handler

	service.processClaim(context.Background(), warmupTestClaim(account.ID, now.Add(-time.Minute)))

	require.Equal(t, "retry", repo.action)
	require.Equal(t, OpenAIWindowWarmupStateRetrying, repo.state)
	require.Equal(t, string(OpenAIWindowWarmupAuthForbidden), repo.code)
	require.Len(t, handler.failures, 1)
}

func TestOpenAIWindowWarmupUsagePreflightFailureHonorsAttemptLimit(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{err: errors.New("usage temporarily unavailable")}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
	claim.Job.AttemptCount = service.options.MaxAttempts - 1

	service.processClaim(context.Background(), claim)

	require.Equal(t, "blocked", repo.action)
	require.Equal(t, OpenAIWindowWarmupStateBlocked, repo.state)
	require.Equal(t, "attempt_limit_preflight", repo.code)
	require.Zero(t, repo.started)
	require.Zero(t, probe.calls)
}

func TestOpenAIWindowWarmupUsageRefreshContentionRetries(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{err: infraerrors.New(
		http.StatusServiceUnavailable,
		"OPENAI_QUOTA_REFRESH_IN_PROGRESS",
		"openai oauth refresh is in progress",
	)}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))

	service.processClaim(context.Background(), claim)

	require.Equal(t, "retry", repo.action)
	require.Equal(t, OpenAIWindowWarmupStateRetrying, repo.state)
	require.Equal(t, http.StatusServiceUnavailable, repo.status)
	require.Equal(t, "usage_preflight_failed", repo.code)
	require.Equal(t, 1, claim.Job.AttemptCount)
	require.Zero(t, probe.calls)
}

func TestOpenAIWindowWarmupUsagePreflightWaitsForBlockedSevenDayReset(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	fiveHourReset := now.Add(-time.Minute)
	sevenDayReset := now.Add(24 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{usage: warmupUsage(fiveHourReset, 100, sevenDayReset, 100)}
	service := newWarmupTestService(repo, account, probe, usage, now, true)

	service.processClaim(context.Background(), warmupTestClaim(account.ID, fiveHourReset))

	require.Equal(t, "retry", repo.action)
	require.Equal(t, OpenAIWindowWarmupStateArmed, repo.state)
	require.Equal(t, "weekly_limit_preflight", repo.code)
	require.Equal(t, sevenDayReset, *repo.reset)
	require.Equal(t, sevenDayReset.Add(openAIWindowWarmupDefaultGrace), repo.next)
	require.Zero(t, probe.calls)
}

func TestOpenAIWindowWarmupWaitsWhenBlockedWeeklyResetIsEarlierThanIdleFiveHourReset(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	fiveHourReset := now.Add(5 * time.Hour)
	sevenDayReset := now.Add(time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{usage: warmupUsage(fiveHourReset, 0, sevenDayReset, 100)}
	service := newWarmupTestService(repo, account, probe, usage, now, true)

	service.processClaim(context.Background(), warmupTestClaim(account.ID, now.Add(-time.Minute)))

	require.Equal(t, "retry", repo.action)
	require.Equal(t, OpenAIWindowWarmupStateArmed, repo.state)
	require.Equal(t, "weekly_limit_preflight", repo.code)
	require.Equal(t, fiveHourReset, *repo.reset)
	require.Equal(t, fiveHourReset.Add(openAIWindowWarmupDefaultGrace), repo.next)
	require.Zero(t, probe.calls)
}

func TestOpenAIWindowWarmupFailsClosedWhenBlockedWeeklyResetIsMissing(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usageValue := warmupUsage(now.Add(5*time.Hour), 0, now.Add(time.Hour), 100)
	usageValue.RateLimit.SecondaryWindow.ResetAt = 0
	usageValue.RateLimit.SecondaryWindow.ResetAfterSeconds = 0
	usage := &warmupUsageStub{usage: usageValue}
	service := newWarmupTestService(repo, account, probe, usage, now, true)

	service.processClaim(context.Background(), warmupTestClaim(account.ID, now.Add(-time.Minute)))

	require.Equal(t, "retry", repo.action)
	require.Equal(t, "usage_preflight_failed", repo.code)
	require.Zero(t, repo.started)
	require.Zero(t, probe.calls)
}

func TestOpenAIWindowWarmupBusyAccountNeverQueriesUsageOrSends(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{usage: &OpenAIQuotaUsage{RateLimit: &OpenAIRateLimit{
		primaryWindowPresent: true, secondaryWindowPresent: true,
	}}}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	service.options.Concurrency = &warmupConcurrencyStub{acquired: false, lease: &warmupExclusiveCacheStub{refresh: true}}

	service.processClaim(context.Background(), warmupTestClaim(account.ID, now.Add(-time.Minute)))

	require.Equal(t, "reschedule", repo.action)
	require.Equal(t, OpenAIWindowWarmupStateRetrying, repo.state)
	require.Zero(t, usage.calls)
	require.Zero(t, probe.calls)
}

func TestOpenAIWindowWarmupLostExclusiveLeaseStopsBeforeSend(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	service := newWarmupTestService(repo, account, probe, nil, now, true)
	service.options.Concurrency = &warmupConcurrencyStub{acquired: true, lease: &warmupExclusiveCacheStub{refresh: false}}

	service.processClaim(context.Background(), warmupTestClaim(account.ID, now.Add(-time.Minute)))

	require.Equal(t, "reschedule", repo.action)
	require.Zero(t, repo.started)
	require.Zero(t, probe.calls)
}

func TestOpenAIWindowWarmupAccountReadErrorsAreClassified(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	tests := []struct {
		name       string
		err        error
		wantAction string
		wantCode   string
	}{
		{name: "transient", err: errors.New("database connection reset"), wantAction: "retry", wantCode: "account_read_failed"},
		{name: "not found", err: ErrAccountNotFound, wantAction: "blocked", wantCode: "account_not_found"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo := &warmupRepositorySpy{}
			probe := &warmupProbeStub{}
			service := newWarmupTestService(repo, account, probe, nil, now, true)
			service.accounts = &warmupAccountRepositoryStub{err: test.err}

			service.processClaim(context.Background(), warmupTestClaim(account.ID, now.Add(-time.Minute)))

			require.Equal(t, test.wantAction, repo.action)
			require.Equal(t, test.wantCode, repo.code)
			require.Zero(t, probe.calls)
		})
	}
}

func TestOpenAIWindowWarmupStopsBeforeSendWhenLeaseRenewalIsRejected(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{rejectRenew: true}
	probe := &warmupProbeStub{}
	service := newWarmupTestService(repo, account, probe, nil, now, true)

	service.processClaim(context.Background(), warmupTestClaim(account.ID, now.Add(-time.Minute)))

	require.Zero(t, repo.started)
	require.Zero(t, probe.calls)
}

func TestOpenAIWindowWarmupCancelsStartedAttemptWhenSendGuardClosesBeforeProbe(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	guardOpen := true
	service.options.KillSwitch = OpenAIWindowWarmupKillSwitchFunc(func(context.Context) (bool, error) {
		return guardOpen, nil
	})
	repo.onMarkStarted = func() { guardOpen = false }
	probeCalls := 0
	service.probe = warmupProbeFunc(func(ctx context.Context, probeAccount *Account, _ *time.Time) (*OpenAIWindowProbeResult, error) {
		probeCalls++
		guard := openAIWindowWarmupSendGuardFromContext(ctx)
		require.NotNil(t, guard)
		return &OpenAIWindowProbeResult{}, guard.Check(ctx, probeAccount.ID)
	})
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))

	service.processClaim(context.Background(), claim)

	require.Equal(t, 1, repo.started)
	require.Equal(t, 1, probeCalls)
	require.Equal(t, 1, repo.cancelStarted)
	require.Equal(t, "cancel_started", repo.action)
	require.Zero(t, claim.Job.AttemptCount)
	require.Nil(t, claim.Job.SentAt)
}

func TestOpenAIWindowWarmupOncePolicyPausesContinuousResetCycle(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyInitialOnce)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	service := newWarmupTestService(repo, account, probe, nil, now, true)

	service.processClaim(context.Background(), warmupTestClaim(account.ID, now.Add(-time.Minute)))

	require.Equal(t, "paused", repo.action)
	require.Equal(t, "policy_cycle_disabled", repo.code)
	require.Zero(t, repo.started)
	require.Zero(t, probe.calls)
}

func TestOpenAIWindowWarmupContinuousSuccessEnqueuesExactlyOneNextCycle(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	newReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
	claim.Job.AttemptCount = 1
	result := &OpenAIWindowProbeResult{
		StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.completed", ResetAt: &newReset,
	}

	service.handleProbeResult(context.Background(), claim, account, result, nil)
	service.handleProbeResult(context.Background(), claim, account, result, nil)

	require.Equal(t, "success", repo.action)
	require.Len(t, repo.enqueues, 1)
	require.Equal(t, warmupResetCycleKey(newReset), repo.enqueues[0].CycleKey)
}

func TestOpenAIWindowWarmupStaleSuccessOwnerCannotEnqueueNextCycle(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	newReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{rejectSuccess: true}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
	claim.Job.AttemptCount = 1

	service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
		StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.completed", ResetAt: &newReset,
	}, nil)

	require.Empty(t, repo.enqueues)
}

func TestOpenAIWindowWarmupReconcilerRepairsCompletedContinuousNextCycle(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	reset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	initialKey := warmupInitialCycleKey(account, warmupAccountGeneration(account))
	repo := &warmupRepositorySpy{cycles: map[string]*OpenAIWindowWarmupJob{
		OpenAIWindowWarmupQuotaScopeGlobal + ":" + initialKey: {
			ID: 1, AccountID: account.ID, QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal,
			CycleKey: initialKey, State: OpenAIWindowWarmupStateCompleted, ObservedResetAt: &reset,
		},
	}}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)

	service.reconcileAccounts(context.Background())

	require.Len(t, repo.enqueues, 1)
	require.Equal(t, warmupResetCycleKey(reset), repo.enqueues[0].CycleKey)
}

func TestOpenAIWindowWarmupOnceSuccessDoesNotEnqueueNextCycle(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	newReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyInitialOnce)
	repo := &warmupRepositorySpy{}
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, nil, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
	claim.Job.AttemptCount = 1

	service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
		StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.done", ResetAt: &newReset,
	}, nil)

	require.Equal(t, "success", repo.action)
	require.Empty(t, repo.enqueues)
}

func TestOpenAIWindowWarmup429UsesAuthoritativeResetWithProbeError(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	probeReset := now.Add(30 * time.Minute)
	fiveHourReset := now.Add(time.Hour)
	sevenDayReset := now.Add(24 * time.Hour)
	usage := &warmupUsageStub{usage: warmupUsage(fiveHourReset, 100, sevenDayReset, 100)}
	repo := &warmupRepositorySpy{}
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, usage, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
	claim.Job.AttemptCount = 1

	service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
		StatusCode: http.StatusTooManyRequests, ResetAt: &probeReset,
	}, errors.New("upstream returned 429"))

	require.Equal(t, "retry", repo.action)
	require.Equal(t, OpenAIWindowWarmupStateArmed, repo.state)
	require.Equal(t, sevenDayReset, *repo.reset)
	require.Equal(t, sevenDayReset.Add(openAIWindowWarmupDefaultGrace), repo.next)
	require.Equal(t, 1, usage.calls)
	require.Equal(t, int64(1), service.Metrics().Retry)
}

func TestOpenAIWindowWarmup429WithoutHeaderQueriesUsageOnce(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	fiveHourReset := now.Add(time.Hour)
	sevenDayReset := now.Add(24 * time.Hour)
	usage := &warmupUsageStub{usage: warmupUsage(fiveHourReset, 100, sevenDayReset, 80)}
	repo := &warmupRepositorySpy{}
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	service := newWarmupTestService(repo, account, &warmupProbeStub{}, usage, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
	claim.Job.AttemptCount = 1

	service.handleProbeResult(context.Background(), claim, account, &OpenAIWindowProbeResult{
		StatusCode: http.StatusTooManyRequests,
	}, errors.New("upstream returned 429"))

	require.Equal(t, "retry", repo.action)
	require.Equal(t, fiveHourReset, *repo.reset)
	require.Equal(t, 1, usage.calls)
}

func TestOpenAIWindowWarmupAmbiguousResponsesBecomeUncertain(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	futureReset := now.Add(5 * time.Hour)
	tests := []struct {
		name   string
		result *OpenAIWindowProbeResult
		err    error
	}{
		{name: "timeout", err: errors.New("request timeout")},
		{name: "sse eof", result: &OpenAIWindowProbeResult{StatusCode: http.StatusOK, EOF: true}, err: errors.New("EOF")},
		{name: "200 without terminal", result: &OpenAIWindowProbeResult{StatusCode: http.StatusOK, ResetAt: &futureReset}},
		{name: "200 without reset", result: &OpenAIWindowProbeResult{StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.completed"}},
		{name: "failed terminal", result: &OpenAIWindowProbeResult{StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.failed", ResetAt: &futureReset}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo := &warmupRepositorySpy{}
			account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
			service := newWarmupTestService(repo, account, &warmupProbeStub{}, &warmupUsageStub{}, now, true)
			claim := warmupTestClaim(account.ID, now.Add(-time.Minute))
			claim.Job.AttemptCount = 1

			service.handleProbeResult(context.Background(), claim, account, test.result, test.err)

			require.Equal(t, "uncertain", repo.action)
			require.Equal(t, OpenAIWindowWarmupStateUncertain, repo.state)
		})
	}
}

func TestOpenAIWindowWarmupExpiredLeaseRequiresPassiveReconcileBeforeReplay(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Hour))
	sent := now.Add(-2 * time.Minute)
	claim.Job.SentAt = &sent
	claim.PreviousState = OpenAIWindowWarmupStateRunning

	service.processClaim(context.Background(), claim)

	require.Equal(t, "uncertain", repo.action)
	require.Zero(t, probe.calls)
	require.Equal(t, 1, usage.calls)
}

func TestOpenAIWindowWarmupUncertainReplayRequiresAuthoritativeUsage(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	tests := []struct {
		name  string
		usage *warmupUsageStub
	}{
		{name: "missing usage", usage: &warmupUsageStub{}},
		{name: "usage error", usage: &warmupUsageStub{err: errors.New("usage unavailable")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repo := &warmupRepositorySpy{}
			probe := &warmupProbeStub{}
			service := newWarmupTestService(repo, account, probe, test.usage, now, true)
			claim := warmupTestClaim(account.ID, now.Add(-time.Hour))
			sent := now.Add(-2 * time.Minute)
			claim.Job.SentAt = &sent
			claim.Job.AttemptCount = 1
			claim.PreviousState = OpenAIWindowWarmupStateUncertain

			service.processClaim(context.Background(), claim)

			require.Equal(t, "uncertain", repo.action)
			require.Zero(t, probe.calls)
			require.Equal(t, 1, test.usage.calls)
		})
	}
}

func TestOpenAIWindowWarmupUncertainReplayAllowsAuthoritativeEmptyWindow(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{result: &OpenAIWindowProbeResult{
		StatusCode:   http.StatusInternalServerError,
		Terminal:     true,
		TerminalType: "response.failed",
	}}
	usage := &warmupUsageStub{usage: &OpenAIQuotaUsage{RateLimit: &OpenAIRateLimit{
		primaryWindowPresent: true, secondaryWindowPresent: true,
	}}}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Hour))
	sent := now.Add(-2 * time.Minute)
	claim.Job.SentAt = &sent
	claim.Job.AttemptCount = 1
	firstObservedAt := now.Add(-time.Minute)
	claim.Job.UncertainObservedAt = &firstObservedAt
	claim.PreviousState = OpenAIWindowWarmupStateUncertain

	service.processClaim(context.Background(), claim)

	require.Equal(t, 3, usage.calls, "takeover reconciliation, final preflight, and post-5xx fencing must all be passive")
	require.Equal(t, 1, probe.calls)
	require.Equal(t, 1, repo.started)
	require.Equal(t, "uncertain", repo.action)
}

func TestOpenAIWindowWarmupUncertainReplayStopsAtAttemptLimit(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{usage: &OpenAIQuotaUsage{RateLimit: &OpenAIRateLimit{
		primaryWindowPresent: true, secondaryWindowPresent: true,
	}}}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Hour))
	sent := now.Add(-2 * time.Minute)
	claim.Job.SentAt = &sent
	claim.Job.AttemptCount = service.options.MaxAttempts
	firstObservedAt := now.Add(-time.Minute)
	claim.Job.UncertainObservedAt = &firstObservedAt
	claim.PreviousState = OpenAIWindowWarmupStateUncertain

	service.processClaim(context.Background(), claim)

	require.Equal(t, "blocked", repo.action)
	require.Equal(t, OpenAIWindowWarmupStateBlocked, repo.state)
	require.Equal(t, "attempt_limit", repo.code)
	require.Equal(t, 1, usage.calls)
	require.Zero(t, probe.calls)
	require.Zero(t, repo.started)
}

func TestOpenAIWindowWarmupUncertainReconcileFailureHonorsAttemptLimit(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{err: errors.New("usage temporarily unavailable")}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Hour))
	sent := now.Add(-2 * time.Minute)
	claim.Job.SentAt = &sent
	claim.Job.AttemptCount = service.options.MaxAttempts - 1
	claim.PreviousState = OpenAIWindowWarmupStateUncertain

	service.processClaim(context.Background(), claim)

	require.Equal(t, "blocked", repo.action)
	require.Equal(t, OpenAIWindowWarmupStateBlocked, repo.state)
	require.Equal(t, "attempt_limit_reconcile", repo.code)
	require.Equal(t, service.options.MaxAttempts, claim.Job.AttemptCount)
	require.Equal(t, 1, usage.calls)
	require.Zero(t, probe.calls)
}

func TestWarmupResetFromUsageSeparatesFiveHourAndBlockedSevenDay(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	fiveHourReset := now.Add(time.Hour)
	sevenDayReset := now.Add(24 * time.Hour)
	usage := warmupUsage(fiveHourReset, 100, sevenDayReset, 100)

	require.Equal(t, fiveHourReset, *warmupResetFromUsage(usage, false))
	require.Equal(t, sevenDayReset, *warmupResetFromUsage(usage, true))
	usage.RateLimit.SecondaryWindow.UsedPercent = 99
	require.Equal(t, fiveHourReset, *warmupResetFromUsage(usage, true))
}

func TestWarmupFiveHourObservationUsesFetchedAtForRelativeReset(t *testing.T) {
	fetchedAt := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	usage := &OpenAIQuotaUsage{
		FetchedAt: fetchedAt.Unix(),
		RateLimit: &OpenAIRateLimit{primaryWindowPresent: true, PrimaryWindow: &OpenAIRateLimitWindow{
			UsedPercent: 0, LimitWindowSeconds: 5 * 60 * 60, ResetAfterSeconds: 18000,
			usedPercentPresent: true, resetAfterSecondsPresent: true,
		}},
	}

	observation, authoritative := warmupFiveHourObservation(usage)

	require.True(t, authoritative)
	require.Zero(t, observation.UsedPercent)
	require.Equal(t, fetchedAt.Add(5*time.Hour), *observation.ResetAt)
}

func TestWarmupFiveHourObservationDistinguishesEmptyAndUnknown(t *testing.T) {
	var explicitEmpty OpenAIQuotaUsage
	require.NoError(t, json.Unmarshal([]byte(`{"rate_limit":{"primary_window":null,"secondary_window":null}}`), &explicitEmpty))
	observation, authoritative := warmupFiveHourObservation(&explicitEmpty)
	require.True(t, authoritative)
	require.Nil(t, observation.ResetAt)

	_, authoritative = warmupFiveHourObservation(&OpenAIQuotaUsage{})
	require.False(t, authoritative)
	_, authoritative = warmupFiveHourObservation(&OpenAIQuotaUsage{RateLimit: &OpenAIRateLimit{}})
	require.False(t, authoritative, "missing window fields are schema-unknown, not explicitly empty")

	_, authoritative = warmupFiveHourObservation(&OpenAIQuotaUsage{RateLimit: &OpenAIRateLimit{
		primaryWindowPresent: true,
		PrimaryWindow: &OpenAIRateLimitWindow{
			LimitWindowSeconds: 5 * 60 * 60, ResetAfterSeconds: 18000,
			usedPercentPresent: true, resetAfterSecondsPresent: true,
		},
	}})
	require.False(t, authoritative, "relative reset without an upstream observation time is unknown")

	var missingUtilization OpenAIQuotaUsage
	require.NoError(t, json.Unmarshal([]byte(`{"fetched_at":1787900000,"rate_limit":{"primary_window":{"limit_window_seconds":18000,"reset_after_seconds":18000},"secondary_window":null}}`), &missingUtilization))
	_, authoritative = warmupFiveHourObservation(&missingUtilization)
	require.False(t, authoritative, "missing used_percent must not become an implicit zero")
}

func TestAccountCodexGlobalResetAtUsesLatestValidCandidate(t *testing.T) {
	older := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	newer := older.Add(5 * time.Hour)
	account := &Account{Extra: map[string]any{
		"codex_5h_reset_at":        older.Format(time.RFC3339),
		"codex_global_5h_reset_at": newer.Format(time.RFC3339),
	}}

	require.Equal(t, newer, *accountCodexGlobalResetAt(account))
}

func TestWarmupEligibilityUsesInjectedClock(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	expires := now.Add(time.Minute)
	account.AutoPauseOnExpired = true
	account.ExpiresAt = &expires

	require.True(t, warmupAccountEligibleAt(account, now))
	require.False(t, warmupAccountEligibleAt(account, expires))
	account.AutoPauseOnExpired = false
	require.False(t, warmupAccountEligibleAt(account, expires), "expired accounts stay ineligible even when auto-pause is disabled")
}

func TestOpenAIWindowWarmupOptionsPreserveExplicitZeroGrace(t *testing.T) {
	options := (OpenAIWindowWarmupOptions{ResetGraceSet: true}).withDefaults()
	require.Zero(t, options.ResetGrace)
	require.Equal(t, openAIWindowWarmupDefaultGrace, (OpenAIWindowWarmupOptions{}).withDefaults().ResetGrace)
}

func newWarmupTestService(repo *warmupRepositorySpy, account *Account, probe *warmupProbeStub, usage *warmupUsageStub, now time.Time, enabled bool) *OpenAIWindowWarmupService {
	repo.reserve = true
	if usage == nil {
		usage = &warmupUsageStub{usage: &OpenAIQuotaUsage{RateLimit: &OpenAIRateLimit{
			primaryWindowPresent: true, secondaryWindowPresent: true,
		}}}
	}
	accounts := &warmupAccountRepositoryStub{account: account}
	exclusiveCache := &warmupExclusiveCacheStub{refresh: true}
	return NewOpenAIWindowWarmupService(repo, accounts, nil, probe, nil, OpenAIWindowWarmupOptions{
		WorkerConcurrency: 1,
		GlobalQPS:         1000,
		BatchSize:         20,
		RequestTimeout:    45 * time.Second,
		LeaseDuration:     2 * time.Minute,
		ResetGrace:        openAIWindowWarmupDefaultGrace,
		UsageReconciler:   usage,
		Concurrency:       &warmupConcurrencyStub{acquired: true, lease: exclusiveCache},
		KillSwitch: OpenAIWindowWarmupKillSwitchFunc(func(context.Context) (bool, error) {
			return enabled, nil
		}),
		Allowlist: OpenAIWindowWarmupAllowlistFunc(func(context.Context) ([]int64, error) {
			if account == nil {
				return nil, nil
			}
			return []int64{account.ID}, nil
		}),
		Now:          func() time.Time { return now },
		RandomJitter: func(string, time.Duration) time.Duration { return 0 },
	})
}

func warmupEligibleAccount(now time.Time, policy string) *Account {
	return &Account{
		ID: 42, Name: "warmup-test", Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1, CreatedAt: now.Add(-24 * time.Hour),
		OpenAIWarmupIdentityGeneration: 1,
		Credentials:                    map[string]any{"chatgpt_account_id": "warmup-account-42"},
		Extra:                          map[string]any{OpenAICodexWarmupPolicyExtraKey: policy},
	}
}

func warmupTestClaim(accountID int64, observed time.Time) OpenAIWindowWarmupClaim {
	return OpenAIWindowWarmupClaim{
		Job: &OpenAIWindowWarmupJob{
			ID: 7, AccountID: accountID, State: OpenAIWindowWarmupStateRunning,
			QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal, CycleKey: warmupResetCycleKey(observed),
			CycleGeneration: 1, IdentityGeneration: 1,
			ObservedResetAt: &observed, CreatedAt: observed.Add(-time.Hour),
		},
		Owner: "worker-a", LeaseToken: "1:test", LeaseUntil: observed.Add(2 * time.Minute),
		PreviousState: OpenAIWindowWarmupStatePending,
	}
}

func warmupUsage(fiveHourReset time.Time, fiveHourUsed float64, sevenDayReset time.Time, sevenDayUsed float64) *OpenAIQuotaUsage {
	return &OpenAIQuotaUsage{RateLimit: &OpenAIRateLimit{
		LimitReached:         fiveHourUsed >= 100 || sevenDayUsed >= 100,
		primaryWindowPresent: true, secondaryWindowPresent: true,
		PrimaryWindow: &OpenAIRateLimitWindow{
			UsedPercent: fiveHourUsed, LimitWindowSeconds: 5 * 60 * 60, ResetAt: fiveHourReset.Unix(),
			usedPercentPresent: true, resetAtPresent: true,
		},
		SecondaryWindow: &OpenAIRateLimitWindow{
			UsedPercent: sevenDayUsed, LimitWindowSeconds: 7 * 24 * 60 * 60, ResetAt: sevenDayReset.Unix(),
			usedPercentPresent: true, resetAtPresent: true,
		},
	}}
}

func cloneWarmupTestTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
