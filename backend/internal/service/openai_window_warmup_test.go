package service

import (
	"context"
	"database/sql"
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

	claims          []OpenAIWindowWarmupClaim
	claimCalls      int
	enqueues        []OpenAIWindowWarmupEnqueue
	cycles          map[string]*OpenAIWindowWarmupJob
	action          string
	state           string
	code            string
	status          int
	next            time.Time
	reset           *time.Time
	started         int
	successCalls    int
	suppressedCalls int
	reserve         bool
	rejectSuccess   bool
	rejectRenew     bool
	queueStats      OpenAIWindowWarmupQueueStats
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
		CycleGeneration: in.CycleGeneration, ObservedResetAt: cloneWarmupTestTime(in.ObservedResetAt),
		NextAttemptAt: in.NextAttemptAt,
	}
	r.cycles[key] = job
	r.enqueues = append(r.enqueues, in)
	return job, true, nil
}

func (r *warmupRepositorySpy) ClaimDue(_ context.Context, _ string, _ time.Duration, limit int, _ []int64) ([]OpenAIWindowWarmupClaim, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.claimCalls++
	if limit < len(r.claims) {
		return append([]OpenAIWindowWarmupClaim(nil), r.claims[:limit]...), nil
	}
	return append([]OpenAIWindowWarmupClaim(nil), r.claims...), nil
}

func (r *warmupRepositorySpy) QueueStats(context.Context, []int64) (OpenAIWindowWarmupQueueStats, error) {
	return r.queueStats, nil
}

func (r *warmupRepositorySpy) CleanupExpiredAttempts(context.Context, int) (int64, error) {
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

func (r *warmupRepositorySpy) MarkStarted(_ context.Context, _ int64, _, _ string, _ time.Time) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started++
	r.action = "started"
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

func (r *warmupRepositorySpy) MarkUncertain(_ context.Context, _ int64, _, _ string, _, next time.Time, status int, code, _ string) (bool, error) {
	r.capture("uncertain", OpenAIWindowWarmupStateUncertain, status, code, next, nil)
	return true, nil
}

func (r *warmupRepositorySpy) MarkBlocked(_ context.Context, _ int64, _, _ string, _ time.Time, status int, code, _ string) (bool, error) {
	state := OpenAIWindowWarmupStateBlocked
	if status == http.StatusBadRequest || status == http.StatusNotFound || code == "blocked_config" {
		state = OpenAIWindowWarmupStateBlockedConfig
	}
	r.capture("blocked", state, status, code, time.Time{}, nil)
	return true, nil
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
	mu       sync.Mutex
	account  *Account
	getCalls int
	err      error
}

func (r *warmupAccountRepositoryStub) GetByID(_ context.Context, id int64) (*Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.getCalls++
	if r.err != nil {
		return nil, r.err
	}
	if r.account == nil || r.account.ID != id {
		return nil, errors.New("account not found")
	}
	return r.account, nil
}

func (r *warmupAccountRepositoryStub) ListSchedulableByPlatform(context.Context, string) ([]Account, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return nil, r.err
	}
	if r.account == nil {
		return nil, nil
	}
	return []Account{*r.account}, nil
}

type warmupProbeStub struct {
	mu     sync.Mutex
	calls  int
	result *OpenAIWindowProbeResult
	err    error
}

func (p *warmupProbeStub) Probe(context.Context, *Account, *time.Time) (*OpenAIWindowProbeResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
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
	require.Equal(t, fmt.Sprintf("initial:%d", account.CreatedAt.UnixNano()), first.CycleKey)
	require.Equal(t, reset.Add(openAIWindowWarmupDefaultGrace), first.NextAttemptAt)
	require.Equal(t, reset, *first.ObservedResetAt)

	second, inserted, err := service.ScheduleAccountWarmup(context.Background(), account, OpenAIWindowWarmupTriggerImport)
	require.NoError(t, err)
	require.False(t, inserted)
	require.Equal(t, first.ID, second.ID)
	require.Len(t, repo.enqueues, 1)
	require.Len(t, service.audit.queue, 1, "idempotent import must not duplicate the initial enqueue audit")
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

func TestOpenAIWindowWarmupScanFailsClosedWithEmptyAllowlist(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	repo := &warmupRepositorySpy{}
	service := newWarmupTestService(repo, warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous), &warmupProbeStub{}, nil, now, true)
	service.options.Allowlist = OpenAIWindowWarmupAllowlistFunc(func(context.Context) ([]int64, error) { return nil, nil })

	service.scanOnce(context.Background())

	require.Zero(t, repo.claimCalls)
}

func TestOpenAIWindowWarmupSuppressesProbeWhenBusinessAdvancedReset(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	oldReset := now.Add(-time.Minute)
	newReset := now.Add(5 * time.Hour)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	account.Extra["codex_5h_reset_at"] = newReset.Format(time.RFC3339)
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

func TestOpenAIWindowWarmupBusyAccountNeverQueriesUsageOrSends(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{usage: &OpenAIQuotaUsage{RateLimit: &OpenAIRateLimit{}}}
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
	usage := &warmupUsageStub{usage: &OpenAIQuotaUsage{RateLimit: &OpenAIRateLimit{}}}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Hour))
	sent := now.Add(-2 * time.Minute)
	claim.Job.SentAt = &sent
	claim.Job.AttemptCount = 1
	claim.PreviousState = OpenAIWindowWarmupStateUncertain

	service.processClaim(context.Background(), claim)

	require.Equal(t, 2, usage.calls, "takeover reconciliation and final preflight must both be passive")
	require.Equal(t, 1, probe.calls)
	require.Equal(t, 1, repo.started)
	require.Equal(t, "retry", repo.action)
}

func TestOpenAIWindowWarmupUncertainReplayStopsAtAttemptLimit(t *testing.T) {
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)
	account := warmupEligibleAccount(now, OpenAIWindowWarmupPolicyContinuous)
	repo := &warmupRepositorySpy{}
	probe := &warmupProbeStub{}
	usage := &warmupUsageStub{usage: &OpenAIQuotaUsage{RateLimit: &OpenAIRateLimit{}}}
	service := newWarmupTestService(repo, account, probe, usage, now, true)
	claim := warmupTestClaim(account.ID, now.Add(-time.Hour))
	sent := now.Add(-2 * time.Minute)
	claim.Job.SentAt = &sent
	claim.Job.AttemptCount = service.options.MaxAttempts
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
		usage = &warmupUsageStub{usage: &OpenAIQuotaUsage{RateLimit: &OpenAIRateLimit{}}}
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
		Extra: map[string]any{OpenAICodexWarmupPolicyExtraKey: policy},
	}
}

func warmupTestClaim(accountID int64, observed time.Time) OpenAIWindowWarmupClaim {
	return OpenAIWindowWarmupClaim{
		Job: &OpenAIWindowWarmupJob{
			ID: 7, AccountID: accountID, State: OpenAIWindowWarmupStateRunning,
			QuotaScope: OpenAIWindowWarmupQuotaScopeGlobal, CycleKey: warmupResetCycleKey(observed),
			CycleGeneration: 1, ObservedResetAt: &observed,
		},
		Owner: "worker-a", LeaseToken: "1:test", LeaseUntil: observed.Add(2 * time.Minute),
		PreviousState: OpenAIWindowWarmupStatePending,
	}
}

func warmupUsage(fiveHourReset time.Time, fiveHourUsed float64, sevenDayReset time.Time, sevenDayUsed float64) *OpenAIQuotaUsage {
	return &OpenAIQuotaUsage{RateLimit: &OpenAIRateLimit{
		LimitReached: fiveHourUsed >= 100 || sevenDayUsed >= 100,
		PrimaryWindow: &OpenAIRateLimitWindow{
			UsedPercent: fiveHourUsed, LimitWindowSeconds: 5 * 60 * 60, ResetAt: fiveHourReset.Unix(),
		},
		SecondaryWindow: &OpenAIRateLimitWindow{
			UsedPercent: sevenDayUsed, LimitWindowSeconds: 7 * 24 * 60 * 60, ResetAt: sevenDayReset.Unix(),
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
