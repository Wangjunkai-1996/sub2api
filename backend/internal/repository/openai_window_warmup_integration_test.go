//go:build integration

package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	appmigrations "github.com/Wei-Shaw/sub2api/migrations"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWindowWarmupRepositoryUniqueCycle(t *testing.T) {
	accountID := createWarmupIntegrationAccount(t)
	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	in := service.OpenAIWindowWarmupEnqueue{
		AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "initial:1", CycleGeneration: 1,
		Trigger: service.OpenAIWindowWarmupTriggerImport, NextAttemptAt: time.Now().UTC().Add(time.Minute),
	}

	first, inserted, err := repo.Enqueue(context.Background(), in)
	require.NoError(t, err)
	require.True(t, inserted)
	require.Equal(t, service.OpenAIWindowWarmupStatePending, first.State, "jitter without reset must not be projected as armed")
	second, inserted, err := repo.Enqueue(context.Background(), in)
	require.NoError(t, err)
	require.False(t, inserted)
	require.Equal(t, first.ID, second.ID)
	active, inserted, err := repo.Enqueue(context.Background(), service.OpenAIWindowWarmupEnqueue{
		AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "manual:other", CycleGeneration: 2,
		Trigger: service.OpenAIWindowWarmupTriggerManual, NextAttemptAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.False(t, inserted)
	require.Equal(t, first.ID, active.ID)

	var count int
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM openai_window_warmup_jobs
		WHERE account_id = $1 AND quota_scope = $2 AND cycle_key = $3`,
		accountID, service.OpenAIWindowWarmupQuotaScopeGlobal, in.CycleKey).Scan(&count))
	require.Equal(t, 1, count)
}

func TestOpenAIWindowWarmupMigrationEnqueuesWithAccountTransaction(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	reset := now.Add(2 * time.Hour)
	createdAt := time.Date(2026, time.August, 28, 1, 2, 3, 123456789, time.UTC)
	account := mustCreateAccount(t, integrationEntClient, &service.Account{
		Name: "warmup-trigger-" + uuid.NewString(), Platform: service.PlatformOpenAI,
		Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
		CreatedAt: createdAt,
		Extra: map[string]any{
			service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
			"codex_5h_reset_at":                     reset.Format(time.RFC3339),
		},
	})
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM accounts WHERE id = $1`, account.ID)
	})

	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	job, err := repo.GetCurrent(context.Background(), account.ID, service.OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, service.OpenAIWindowWarmupStateArmed, job.State)
	// PostgreSQL rounds TIMESTAMPTZ to microseconds. The >=500ns tail makes
	// this fail if the Go side ever regresses to UnixNano or Truncate.
	require.Equal(t, 789, account.CreatedAt.Nanosecond()%1000)
	expectedGeneration := account.CreatedAt.UTC().Round(time.Microsecond).UnixNano()
	require.Equal(t, expectedGeneration, job.CycleGeneration)
	require.Equal(t, fmt.Sprintf("initial:%d", expectedGeneration), job.CycleKey)
	require.WithinDuration(t, reset, *job.ObservedResetAt, time.Second)

	// Remove the trigger-created row, then schedule with the original Ent
	// object so the runtime helper is checked against the same expected value.
	_, err = integrationDB.ExecContext(context.Background(),
		`DELETE FROM openai_window_warmup_jobs WHERE id = $1`, job.ID)
	require.NoError(t, err)
	warmupService := service.NewOpenAIWindowWarmupService(repo, nil, nil, nil, nil, service.OpenAIWindowWarmupOptions{
		Now: func() time.Time { return now },
	})
	runtimeJob, inserted, err := warmupService.ScheduleAccountWarmup(
		context.Background(), account, service.OpenAIWindowWarmupTriggerImport,
	)
	require.NoError(t, err)
	require.True(t, inserted)
	require.Equal(t, expectedGeneration, runtimeJob.CycleGeneration)
	require.Equal(t, fmt.Sprintf("initial:%d", expectedGeneration), runtimeJob.CycleKey)
}

func TestOpenAIWindowWarmupMigrationCurrentJobIndexMatchesLookupOrder(t *testing.T) {
	var definition string
	err := integrationDB.QueryRowContext(context.Background(), `
		SELECT indexdef
		FROM pg_indexes
		WHERE schemaname = 'public' AND indexname = 'idx_openai_window_warmup_jobs_account'`).Scan(&definition)
	require.NoError(t, err)
	require.Contains(t, definition, "(account_id, quota_scope, id DESC)")
	require.NotContains(t, definition, "updated_at")
}

func TestOpenAIWindowWarmupMigrationReconcilesEarlyDuplicateActiveJobs(t *testing.T) {
	ctx := context.Background()
	migrationSQL, err := appmigrations.FS.ReadFile("231_openai_window_warmup.sql")
	require.NoError(t, err)

	accountIDs := []int64{
		createWarmupIntegrationAccount(t),
		createWarmupIntegrationAccount(t),
		createWarmupIntegrationAccount(t),
		createWarmupIntegrationAccount(t),
	}
	const (
		firstReplay  = "991_test_openai_window_warmup_deduplicate.sql"
		secondReplay = "992_test_openai_window_warmup_deduplicate_idempotent.sql"
	)
	t.Cleanup(func() {
		for _, accountID := range accountIDs {
			_, _ = integrationDB.ExecContext(context.Background(),
				`DELETE FROM openai_window_warmup_jobs WHERE account_id = $1`, accountID)
		}
		_, _ = integrationDB.ExecContext(context.Background(),
			`DELETE FROM schema_migrations WHERE filename IN ($1, $2)`, firstReplay, secondReplay)
		_, _ = integrationDB.ExecContext(context.Background(),
			`DROP INDEX IF EXISTS idx_openai_window_warmup_jobs_one_active`)
		_, _ = integrationDB.ExecContext(context.Background(), `
			CREATE UNIQUE INDEX IF NOT EXISTS idx_openai_window_warmup_jobs_one_active
			ON openai_window_warmup_jobs (account_id, quota_scope)
			WHERE state IN ('pending', 'armed', 'due', 'running', 'retrying', 'uncertain', 'possibly_sent')`)
	})

	_, err = integrationDB.ExecContext(ctx, `DROP INDEX idx_openai_window_warmup_jobs_one_active`)
	require.NoError(t, err)
	// A same-named non-unique draft index must not cause IF NOT EXISTS to leave
	// the active-job invariant unenforced.
	_, err = integrationDB.ExecContext(ctx, `
		CREATE INDEX idx_openai_window_warmup_jobs_one_active
		ON openai_window_warmup_jobs (account_id, quota_scope)
		WHERE state = 'pending'`)
	require.NoError(t, err)

	seedJob := func(accountID int64, state, cycleKey string, withLease bool) {
		t.Helper()
		_, seedErr := integrationDB.ExecContext(ctx, `
			INSERT INTO openai_window_warmup_jobs
			    (account_id, quota_scope, state, trigger, cycle_key, cycle_generation,
			     next_attempt_at, lease_owner, lease_token, lease_until, created_at, updated_at)
			VALUES
			    ($1, 'global', $2, 'import', $3, 1, NOW(),
			     CASE WHEN $4 THEN 'legacy-owner' END,
			     CASE WHEN $4 THEN 'legacy-token' END,
			     CASE WHEN $4 THEN NOW() + INTERVAL '5 minutes' END,
			     NOW(), NOW())`, accountID, state, cycleKey, withLease)
		require.NoError(t, seedErr)
	}

	seedJob(accountIDs[0], "possibly_sent", "keep-possibly-sent", false)
	seedJob(accountIDs[0], "uncertain", "block-uncertain", true)
	seedJob(accountIDs[0], "running", "block-running", true)
	seedJob(accountIDs[0], "pending", "block-newest-pending", true)

	seedJob(accountIDs[1], "uncertain", "keep-uncertain", false)
	seedJob(accountIDs[1], "running", "block-newer-running", true)
	seedJob(accountIDs[1], "due", "block-newest-due", true)

	seedJob(accountIDs[2], "running", "keep-running", true)
	seedJob(accountIDs[2], "retrying", "block-newer-retrying", true)

	seedJob(accountIDs[3], "armed", "block-older-armed", true)
	seedJob(accountIDs[3], "pending", "keep-newest-pending", false)

	replayFS := fstest.MapFS{
		firstReplay:  &fstest.MapFile{Data: migrationSQL},
		secondReplay: &fstest.MapFile{Data: migrationSQL},
	}
	require.NoError(t, applyMigrationsFS(ctx, integrationDB, replayFS))
	require.NoError(t, applyMigrationsFS(ctx, integrationDB, replayFS))

	expected := []struct {
		cycleKey   string
		state      string
		totalRows  int
		keepsLease bool
	}{
		{cycleKey: "keep-possibly-sent", state: "possibly_sent", totalRows: 4},
		{cycleKey: "keep-uncertain", state: "uncertain", totalRows: 3},
		{cycleKey: "keep-running", state: "running", totalRows: 2, keepsLease: true},
		{cycleKey: "keep-newest-pending", state: "pending", totalRows: 2},
	}
	for i, want := range expected {
		var (
			activeCount, blockedCount, cleanBlockedCount int
			cycleKey, state                              string
		)
		err = integrationDB.QueryRowContext(ctx, `
			SELECT COUNT(*), MIN(cycle_key), MIN(state)
			FROM openai_window_warmup_jobs
			WHERE account_id = $1
			  AND state IN ('pending', 'armed', 'due', 'running', 'retrying', 'uncertain', 'possibly_sent')`,
			accountIDs[i]).Scan(&activeCount, &cycleKey, &state)
		require.NoError(t, err)
		require.Equal(t, 1, activeCount)
		require.Equal(t, want.cycleKey, cycleKey)
		require.Equal(t, want.state, state)
		if want.keepsLease {
			var keepsLease bool
			err = integrationDB.QueryRowContext(ctx, `
				SELECT lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_until > NOW()
				FROM openai_window_warmup_jobs
				WHERE account_id = $1 AND cycle_key = $2`, accountIDs[i], want.cycleKey).Scan(&keepsLease)
			require.NoError(t, err)
			require.True(t, keepsLease)
		}

		err = integrationDB.QueryRowContext(ctx, `
			SELECT COUNT(*),
			       COUNT(*) FILTER (
			           WHERE lease_owner IS NULL AND lease_token IS NULL AND lease_until IS NULL
			             AND last_error_code = 'migration_duplicate_active_job'
			             AND last_error = 'Superseded duplicate active warmup job during migration 231'
			       )
			FROM openai_window_warmup_jobs
			WHERE account_id = $1 AND state = 'blocked'`, accountIDs[i]).Scan(&blockedCount, &cleanBlockedCount)
		require.NoError(t, err)
		require.Equal(t, want.totalRows-1, blockedCount)
		require.Equal(t, blockedCount, cleanBlockedCount)
	}

	var (
		isUnique, isValid bool
		indexDefinition   string
	)
	err = integrationDB.QueryRowContext(ctx, `
		SELECT i.indisunique, i.indisvalid, pg_get_indexdef(i.indexrelid)
		FROM pg_index i
		JOIN pg_class idx ON idx.oid = i.indexrelid
		JOIN pg_namespace ns ON ns.oid = idx.relnamespace
		WHERE ns.nspname = 'public'
		  AND idx.relname = 'idx_openai_window_warmup_jobs_one_active'`).Scan(
		&isUnique, &isValid, &indexDefinition,
	)
	require.NoError(t, err)
	require.True(t, isUnique)
	require.True(t, isValid)
	for _, state := range []string{"pending", "armed", "due", "running", "retrying", "uncertain", "possibly_sent"} {
		require.Contains(t, indexDefinition, state)
	}

	result, err := integrationDB.ExecContext(ctx, `
		INSERT INTO openai_window_warmup_jobs
		    (account_id, quota_scope, state, trigger, cycle_key, cycle_generation, next_attempt_at)
		VALUES ($1, 'global', 'pending', 'manual', $2, 2, NOW())
		ON CONFLICT DO NOTHING`, accountIDs[0], "collision:"+uuid.NewString())
	require.NoError(t, err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(t, err)
	require.Zero(t, rowsAffected)
}

func TestOpenAIWindowWarmupMigrationFallsBackFromMalformedPrimaryReset(t *testing.T) {
	reset := time.Now().UTC().Truncate(time.Second).Add(2 * time.Hour)
	for _, tc := range []struct {
		name    string
		primary string
	}{
		{name: "invalid", primary: "not-a-timestamp"},
		{name: "relative", primary: "now"},
		{name: "infinite", primary: "infinity"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := mustCreateAccount(t, integrationEntClient, &service.Account{
				Name: "warmup-reset-fallback-" + uuid.NewString(), Platform: service.PlatformOpenAI,
				Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
				Extra: map[string]any{
					service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
					"codex_5h_reset_at":                     tc.primary,
					"codex_global_5h_reset_at":              reset.Format(time.RFC3339Nano),
				},
			})
			t.Cleanup(func() {
				_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM accounts WHERE id = $1`, account.ID)
			})

			job, err := NewOpenAIWindowWarmupRepository(integrationDB).GetCurrent(
				context.Background(), account.ID, service.OpenAIWindowWarmupQuotaScopeGlobal,
			)
			require.NoError(t, err)
			require.Equal(t, service.OpenAIWindowWarmupStateArmed, job.State)
			require.NotNil(t, job.ObservedResetAt)
			require.WithinDuration(t, reset, *job.ObservedResetAt, time.Microsecond)
		})
	}
}

func TestOpenAIWindowWarmupMigrationFallsBackFromInvalidPrimaryPolicy(t *testing.T) {
	for _, tc := range []struct {
		name    string
		primary any
	}{
		{name: "blank", primary: "   "},
		{name: "non-string", primary: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := mustCreateAccount(t, integrationEntClient, &service.Account{
				Name: "warmup-policy-alias-" + uuid.NewString(), Platform: service.PlatformOpenAI,
				Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
				Extra: map[string]any{
					service.OpenAICodexWarmupPolicyExtraKey: tc.primary,
					service.CodexWarmupPolicyExtraKey:       service.OpenAIWindowWarmupPolicyContinuous,
				},
			})
			t.Cleanup(func() {
				_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM accounts WHERE id = $1`, account.ID)
			})

			job, err := NewOpenAIWindowWarmupRepository(integrationDB).GetCurrent(
				context.Background(), account.ID, service.OpenAIWindowWarmupQuotaScopeGlobal,
			)
			require.NoError(t, err)
			require.Equal(t, service.OpenAIWindowWarmupPolicy(service.OpenAIWindowWarmupPolicyContinuous), service.OpenAIWindowWarmupPolicyForAccount(account))
			require.Equal(t, service.OpenAIWindowWarmupStatePending, job.State)
		})
	}
}

func TestOpenAIWindowWarmupMigrationSkipsTemporarilyUnschedulableAccount(t *testing.T) {
	until := time.Now().UTC().Add(time.Hour)
	account, err := integrationEntClient.Account.Create().
		SetName("warmup-temp-unschedulable-" + uuid.NewString()).
		SetPlatform(service.PlatformOpenAI).
		SetType(service.AccountTypeOAuth).
		SetStatus(service.StatusActive).
		SetSchedulable(true).
		SetTempUnschedulableUntil(until).
		SetExtra(map[string]any{service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous}).
		Save(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM accounts WHERE id = $1`, account.ID)
	})

	var count int
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM openai_window_warmup_jobs WHERE account_id = $1`, account.ID).Scan(&count))
	require.Zero(t, count)
}

func TestOpenAIWindowWarmupRepositoryMarkStartedPolicyAndResetCAS(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	olderReset := now.Add(-time.Hour)
	newerReset := now.Add(2 * time.Hour)
	tests := []struct {
		name          string
		policy        string
		policyExtra   map[string]any
		observedReset *time.Time
		accountReset  *time.Time
		wantStarted   bool
	}{
		{name: "policy off fails closed", wantStarted: false},
		{name: "enabled policy starts", policy: service.OpenAIWindowWarmupPolicyContinuous, wantStarted: true},
		{
			name: "blank canonical falls back to alias",
			policyExtra: map[string]any{
				service.OpenAICodexWarmupPolicyExtraKey: "   ",
				service.CodexWarmupPolicyExtraKey:       service.OpenAIWindowWarmupPolicyContinuous,
			},
			wantStarted: true,
		},
		{
			name: "non-string canonical falls back to alias",
			policyExtra: map[string]any{
				service.OpenAICodexWarmupPolicyExtraKey: false,
				service.CodexWarmupPolicyExtraKey:       service.OpenAIWindowWarmupPolicyContinuous,
			},
			wantStarted: true,
		},
		{
			name: "newer authoritative reset suppresses stale claim", policy: service.OpenAIWindowWarmupPolicyContinuous,
			observedReset: &olderReset, accountReset: &newerReset, wantStarted: false,
		},
		{
			name: "any future authoritative reset fails closed", policy: service.OpenAIWindowWarmupPolicyContinuous,
			observedReset: &newerReset, accountReset: &newerReset, wantStarted: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			accountID := createWarmupIntegrationAccount(t)
			repo := NewOpenAIWindowWarmupRepository(integrationDB)
			job, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
				AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
				CycleKey: "cas:" + uuid.NewString(), CycleGeneration: 41,
				Trigger: service.OpenAIWindowWarmupTriggerReset, ObservedResetAt: tc.observedReset,
				NextAttemptAt: now.Add(-time.Minute),
			})
			require.NoError(t, err)
			require.True(t, inserted)

			if tc.policy != "" || tc.policyExtra != nil || tc.accountReset != nil {
				extra := tc.policyExtra
				if extra == nil {
					extra = map[string]any{}
				}
				if tc.policy != "" {
					extra[service.OpenAICodexWarmupPolicyExtraKey] = tc.policy
				}
				if tc.accountReset != nil {
					extra["codex_5h_reset_at"] = tc.accountReset.Format(time.RFC3339Nano)
				}
				mergeWarmupIntegrationAccountExtra(t, accountID, extra)
				if tc.policy != "" {
					assertWarmupIntegrationAccountEligible(t, accountID, tc.policy)
				}
			}

			claims, err := repo.ClaimDue(ctx, "cas-owner-"+uuid.NewString(), 2*time.Minute, 1, []int64{accountID})
			require.NoError(t, err)
			require.Len(t, claims, 1)
			started, err := repo.MarkStarted(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken, now)
			require.NoError(t, err)
			require.Equal(t, tc.wantStarted, started)
		})
	}
}

func TestOpenAIWindowWarmupRepositoryGlobalPermitAndCircuitBreaker(t *testing.T) {
	ctx := context.Background()
	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	_, err := integrationDB.ExecContext(ctx, `
		UPDATE openai_window_warmup_runtime
		SET next_send_at = to_timestamp(0), permit_token = NULL, inflight_until = NULL
		WHERE gate_key = 'global_send'`)
	require.NoError(t, err)

	type permitResult struct {
		token string
		ok    bool
		err   error
	}
	results := make(chan permitResult, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			token, ok, reserveErr := repo.ReserveGlobalSend(ctx, 5*time.Second, 2*time.Minute)
			results <- permitResult{token: token, ok: ok, err: reserveErr}
		}()
	}
	wg.Wait()
	close(results)
	granted := 0
	permitToken := ""
	for result := range results {
		require.NoError(t, result.err)
		if result.ok {
			granted++
			permitToken = result.token
		}
	}
	require.Equal(t, 1, granted)
	require.NotEmpty(t, permitToken)
	released, err := repo.ReleaseGlobalSend(ctx, permitToken)
	require.NoError(t, err)
	require.True(t, released)
	_, grantedImmediately, err := repo.ReserveGlobalSend(ctx, 5*time.Second, 2*time.Minute)
	require.NoError(t, err)
	require.False(t, grantedImmediately)

	accountID := createWarmupIntegrationAccount(t)
	job, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
		AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "circuit:1", CycleGeneration: 1,
		Trigger: service.OpenAIWindowWarmupTriggerManual, NextAttemptAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	for attempt := 1; attempt <= 10; attempt++ {
		_, err = integrationDB.ExecContext(ctx, `
			INSERT INTO openai_window_warmup_attempts
			(job_id, attempt_no, outcome, started_at, finished_at)
			VALUES ($1, $2, 'retry', NOW(), NOW())`, job.ID, attempt)
		require.NoError(t, err)
	}
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE openai_window_warmup_runtime
		SET next_send_at = to_timestamp(0), permit_token = NULL, inflight_until = NULL
		WHERE gate_key = 'global_send'`)
	require.NoError(t, err)
	_, grantedDuringCircuit, err := repo.ReserveGlobalSend(ctx, 5*time.Second, 2*time.Minute)
	require.NoError(t, err)
	require.False(t, grantedDuringCircuit)
}

func TestOpenAIWindowWarmupRepositoryEightWorkersClaimOnce(t *testing.T) {
	accountID := createWarmupIntegrationAccount(t)
	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	_, inserted, err := repo.Enqueue(context.Background(), service.OpenAIWindowWarmupEnqueue{
		AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "initial:8", CycleGeneration: 8,
		Trigger: service.OpenAIWindowWarmupTriggerImport, NextAttemptAt: time.Now().UTC().Add(-time.Second),
	})
	require.NoError(t, err)
	require.True(t, inserted)

	type claimResult struct {
		claims []service.OpenAIWindowWarmupClaim
		err    error
	}
	const workers = 8
	start := make(chan struct{})
	results := make(chan claimResult, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			claims, claimErr := repo.ClaimDue(context.Background(), fmt.Sprintf("worker-%d", worker), 2*time.Minute, 1, []int64{accountID})
			results <- claimResult{claims: claims, err: claimErr}
		}(worker)
	}
	close(start)
	wg.Wait()
	close(results)

	total := 0
	owners := make(map[string]struct{})
	for result := range results {
		require.NoError(t, result.err)
		total += len(result.claims)
		for _, claim := range result.claims {
			owners[claim.Owner] = struct{}{}
			require.Equal(t, service.OpenAIWindowWarmupStatePending, claim.PreviousState)
			require.Contains(t, claim.LeaseToken, "8:")
		}
	}
	require.Equal(t, 1, total)
	require.Len(t, owners, 1)
}

func TestOpenAIWindowWarmupRepositoryQueueStatsUsesDatabaseClock(t *testing.T) {
	ctx := context.Background()
	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	dueAccountID := createWarmupIntegrationAccount(t)
	inflightAccountID := createWarmupIntegrationAccount(t)
	completedAccountID := createWarmupIntegrationAccount(t)

	dueJob, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
		AccountID: dueAccountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "stats:due", CycleGeneration: 51,
		Trigger: service.OpenAIWindowWarmupTriggerReset, NextAttemptAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE openai_window_warmup_jobs
		SET state = 'pending', next_attempt_at = NOW() - INTERVAL '2 minutes',
		    observed_reset_at = NOW() - INTERVAL '3 minutes'
		WHERE id = $1`, dueJob.ID)
	require.NoError(t, err)

	_, inserted, err = repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
		AccountID: inflightAccountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "stats:inflight", CycleGeneration: 52,
		Trigger: service.OpenAIWindowWarmupTriggerReset, NextAttemptAt: time.Now().UTC().Add(-time.Minute),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	claims, err := repo.ClaimDue(ctx, "stats-owner", 2*time.Minute, 1, []int64{inflightAccountID})
	require.NoError(t, err)
	require.Len(t, claims, 1)

	completedJob, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
		AccountID: completedAccountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "stats:completed", CycleGeneration: 53,
		Trigger: service.OpenAIWindowWarmupTriggerReset, NextAttemptAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE openai_window_warmup_jobs
		SET state = 'completed', next_attempt_at = NOW() - INTERVAL '1 day',
		    observed_reset_at = NOW() - INTERVAL '1 day'
		WHERE id = $1`, completedJob.ID)
	require.NoError(t, err)

	stats, err := repo.QueueStats(ctx, []int64{dueAccountID, inflightAccountID, completedAccountID})
	require.NoError(t, err)
	require.EqualValues(t, 3, stats.Enqueued)
	require.EqualValues(t, 1, stats.Due)
	require.EqualValues(t, 1, stats.Inflight)
	require.GreaterOrEqual(t, stats.OldestDueAgeSeconds, int64(119))
	require.Less(t, stats.OldestDueAgeSeconds, int64(5*60))
	require.GreaterOrEqual(t, stats.ResetLagSeconds, int64(179))
	require.Less(t, stats.ResetLagSeconds, int64(6*60))
}

func TestOpenAIWindowWarmupRepositoryExpiredLeaseTakeoverFencesOldOwner(t *testing.T) {
	ctx := context.Background()
	accountID := createWarmupIntegrationAccount(t)
	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	job, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
		AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "initial:17", CycleGeneration: 17,
		Trigger: service.OpenAIWindowWarmupTriggerImport, NextAttemptAt: time.Now().UTC().Add(-time.Second),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	// Enable policy after the explicit enqueue. The active-cycle unique index
	// suppresses the trigger's initial insert while MarkStarted sees the policy.
	mergeWarmupIntegrationAccountExtra(t, accountID, map[string]any{
		service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
	})
	assertWarmupIntegrationAccountEligible(t, accountID, service.OpenAIWindowWarmupPolicyContinuous)

	first, err := repo.ClaimDue(ctx, "old-owner", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, first, 1)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE openai_window_warmup_jobs SET lease_until = NOW() - INTERVAL '1 second' WHERE id = $1`, job.ID)
	require.NoError(t, err)

	second, err := repo.ClaimDue(ctx, "new-owner", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, service.OpenAIWindowWarmupStateRunning, second[0].PreviousState)
	require.NotEqual(t, first[0].LeaseToken, second[0].LeaseToken)
	require.Contains(t, second[0].LeaseToken, "17:")

	attemptOneAt := time.Now().UTC()
	ok, err := repo.MarkStarted(ctx, job.ID, first[0].Owner, first[0].LeaseToken, attemptOneAt)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = repo.MarkStarted(ctx, job.ID, second[0].Owner, second[0].LeaseToken, attemptOneAt)
	require.NoError(t, err)
	require.True(t, ok)

	next := time.Now().UTC().Add(-time.Second)
	ok, err = repo.MarkRetry(ctx, job.ID, second[0].Owner, second[0].LeaseToken,
		attemptOneAt.Add(time.Second), next, 503, "upstream_5xx", "sanitized retry")
	require.NoError(t, err)
	require.True(t, ok)
	assertWarmupIntegrationAttempt(t, job.ID, 1, "retry", 503, "upstream_5xx")
	retryingJob, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, service.OpenAIWindowWarmupStateRetrying, retryingJob.State)
	require.Equal(t, 1, retryingJob.AttemptCount)
	require.Empty(t, retryingJob.LeaseOwner)
	require.Empty(t, retryingJob.LeaseToken)

	// A transition that released the lease cannot be followed by another write
	// with either the reclaimed token or the stale pre-takeover token.
	resetAfterRetry := time.Now().UTC().Add(5 * time.Hour)
	ok, err = repo.MarkSuccess(ctx, job.ID, first[0].Owner, first[0].LeaseToken,
		time.Now().UTC(), &resetAfterRetry, 200, "")
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = repo.MarkSuccess(ctx, job.ID, second[0].Owner, second[0].LeaseToken,
		time.Now().UTC(), &resetAfterRetry, 200, "")
	require.NoError(t, err)
	require.False(t, ok)

	finalClaim, err := repo.ClaimDue(ctx, "final-owner", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, finalClaim, 1)
	require.Equal(t, service.OpenAIWindowWarmupStateRetrying, finalClaim[0].PreviousState)
	attemptTwoAt := time.Now().UTC()
	ok, err = repo.MarkStarted(ctx, job.ID, finalClaim[0].Owner, finalClaim[0].LeaseToken, attemptTwoAt)
	require.NoError(t, err)
	require.True(t, ok)
	resetAfterSuccess := time.Now().UTC().Add(5 * time.Hour)
	ok, err = repo.MarkSuccess(ctx, job.ID, finalClaim[0].Owner, finalClaim[0].LeaseToken,
		attemptTwoAt.Add(time.Second), &resetAfterSuccess, 200, "")
	require.NoError(t, err)
	require.True(t, ok)
	assertWarmupIntegrationAttempt(t, job.ID, 2, "success", 200, "")

	// Final states are immutable through all lease-fenced worker methods.
	ok, err = repo.MarkRetry(ctx, job.ID, finalClaim[0].Owner, finalClaim[0].LeaseToken,
		time.Now().UTC(), time.Now().UTC().Add(time.Minute), 500, "late_retry", "ignored")
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = repo.Reschedule(ctx, job.ID, finalClaim[0].Owner, finalClaim[0].LeaseToken,
		time.Now().UTC(), service.OpenAIWindowWarmupStatePending, nil)
	require.NoError(t, err)
	require.False(t, ok)

	finalJob, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, service.OpenAIWindowWarmupStateCompleted, finalJob.State)
	require.Equal(t, 2, finalJob.AttemptCount)
	require.Empty(t, finalJob.LeaseOwner)
	require.Empty(t, finalJob.LeaseToken)
	var attemptCount int
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM openai_window_warmup_attempts WHERE job_id = $1`, job.ID).Scan(&attemptCount))
	require.Equal(t, 2, attemptCount)
}

func TestOpenAIWindowWarmupRepositoryObservationFailureIsCountedAndFenced(t *testing.T) {
	ctx := context.Background()
	accountID := createWarmupIntegrationAccount(t)
	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	job, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
		AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "observation:" + uuid.NewString(), CycleGeneration: 71,
		Trigger: service.OpenAIWindowWarmupTriggerReset, NextAttemptAt: time.Now().UTC().Add(-time.Second),
	})
	require.NoError(t, err)
	require.True(t, inserted)

	claims, err := repo.ClaimDue(ctx, "observation-owner", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, claims, 1)
	at := time.Now().UTC()
	next := at.Add(time.Minute)
	ok, err := repo.MarkObservationFailure(ctx, job.ID, claims[0].Owner, "71:stale", at, next,
		service.OpenAIWindowWarmupStateRetrying, 502, "usage_observation_failed", "sanitized")
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = repo.MarkObservationFailure(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken, at, next,
		service.OpenAIWindowWarmupStateRetrying, 502, "usage_observation_failed", "sanitized")
	require.NoError(t, err)
	require.True(t, ok)

	updated, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, service.OpenAIWindowWarmupStateRetrying, updated.State)
	require.Equal(t, 1, updated.AttemptCount)
	require.Nil(t, updated.SentAt)
	assertWarmupIntegrationAttempt(t, job.ID, 1, "retry", 502, "usage_observation_failed")
}

func TestOpenAIWindowWarmupRepositorySuppressionUsesRealAttemptNumber(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	accountID := createWarmupIntegrationAccount(t)
	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	job, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
		AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "suppressed-sent:" + uuid.NewString(), CycleGeneration: 73,
		Trigger: service.OpenAIWindowWarmupTriggerReset, NextAttemptAt: now.Add(-time.Minute),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	mergeWarmupIntegrationAccountExtra(t, accountID, map[string]any{
		service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
	})
	claims, err := repo.ClaimDue(ctx, "suppressed-sender", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, claims, 1)
	started, err := repo.MarkStarted(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken, now)
	require.NoError(t, err)
	require.True(t, started)
	_, err = integrationDB.ExecContext(ctx,
		`UPDATE openai_window_warmup_jobs SET lease_until = NOW() - INTERVAL '1 second' WHERE id = $1`, job.ID)
	require.NoError(t, err)
	takeover, err := repo.ClaimDue(ctx, "suppressed-reconciler", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, takeover, 1)
	reset := now.Add(5 * time.Hour)
	ok, err := repo.MarkSuppressed(ctx, job.ID, takeover[0].Owner, takeover[0].LeaseToken, now.Add(time.Second), &reset, "lease_takeover_reconciled")
	require.NoError(t, err)
	require.True(t, ok)

	updated, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, 1, updated.AttemptCount)
	assertWarmupIntegrationAttempt(t, job.ID, 1, "suppressed", 0, "lease_takeover_reconciled")
	var attempts int
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM openai_window_warmup_attempts WHERE job_id = $1`, job.ID).Scan(&attempts))
	require.Equal(t, 1, attempts)
}

func TestOpenAIWindowWarmupRepositorySuppressionBeforeSendDoesNotInventAttempt(t *testing.T) {
	ctx := context.Background()
	accountID := createWarmupIntegrationAccount(t)
	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	job, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
		AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "suppressed-unsent:" + uuid.NewString(), CycleGeneration: 74,
		Trigger: service.OpenAIWindowWarmupTriggerReset, NextAttemptAt: time.Now().UTC().Add(-time.Minute),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	claims, err := repo.ClaimDue(ctx, "suppressed-business", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, claims, 1)
	reset := time.Now().UTC().Add(5 * time.Hour)
	ok, err := repo.MarkSuppressed(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken, time.Now().UTC(), &reset, "real_traffic_suppressed")
	require.NoError(t, err)
	require.True(t, ok)

	updated, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Zero(t, updated.AttemptCount)
	var attempts int
	require.NoError(t, integrationDB.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM openai_window_warmup_attempts WHERE job_id = $1`, job.ID).Scan(&attempts))
	require.Zero(t, attempts)
}

func TestOpenAIWindowWarmupRepositorySuppressionFinalizesUncertainEvidence(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	accountID := createWarmupIntegrationAccount(t)
	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	job, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
		AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "suppressed-uncertain:" + uuid.NewString(), CycleGeneration: 75,
		Trigger: service.OpenAIWindowWarmupTriggerReset, NextAttemptAt: now.Add(-time.Minute),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	mergeWarmupIntegrationAccountExtra(t, accountID, map[string]any{
		service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
	})
	claims, err := repo.ClaimDue(ctx, "uncertain-sender", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, claims, 1)
	started, err := repo.MarkStarted(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken, now)
	require.NoError(t, err)
	require.True(t, started)
	uncertain, err := repo.MarkUncertain(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken,
		now.Add(time.Second), now.Add(-time.Second), 200, "possibly_sent", "sanitized")
	require.NoError(t, err)
	require.True(t, uncertain)
	assertWarmupIntegrationAttempt(t, job.ID, 1, "uncertain", 200, "possibly_sent")

	takeover, err := repo.ClaimDue(ctx, "uncertain-reconciler", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, takeover, 1)
	reset := now.Add(5 * time.Hour)
	ok, err := repo.MarkSuppressed(ctx, job.ID, takeover[0].Owner, takeover[0].LeaseToken,
		now.Add(2*time.Second), &reset, "lease_takeover_reconciled")
	require.NoError(t, err)
	require.True(t, ok)
	assertWarmupIntegrationAttempt(t, job.ID, 1, "suppressed", 200, "lease_takeover_reconciled")
}

func TestOpenAIWindowWarmupRepositoryPreSendSuppressionPreservesFinalizedRetry(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	accountID := createWarmupIntegrationAccount(t)
	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	job, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
		AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "suppressed-after-retry:" + uuid.NewString(), CycleGeneration: 76,
		Trigger: service.OpenAIWindowWarmupTriggerReset, NextAttemptAt: now.Add(-time.Minute),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	mergeWarmupIntegrationAccountExtra(t, accountID, map[string]any{
		service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
	})
	claims, err := repo.ClaimDue(ctx, "retry-sender", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, claims, 1)
	started, err := repo.MarkStarted(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken, now)
	require.NoError(t, err)
	require.True(t, started)
	retried, err := repo.MarkRetry(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken,
		now.Add(time.Second), now.Add(-time.Second), 503, "upstream_5xx", "sanitized")
	require.NoError(t, err)
	require.True(t, retried)
	assertWarmupIntegrationAttempt(t, job.ID, 1, "retry", 503, "upstream_5xx")

	nextClaim, err := repo.ClaimDue(ctx, "retry-suppressor", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, nextClaim, 1)
	reset := now.Add(5 * time.Hour)
	ok, err := repo.MarkSuppressed(ctx, job.ID, nextClaim[0].Owner, nextClaim[0].LeaseToken,
		now.Add(2*time.Second), &reset, "real_traffic_suppressed")
	require.NoError(t, err)
	require.True(t, ok)
	assertWarmupIntegrationAttempt(t, job.ID, 1, "retry", 503, "upstream_5xx")
	updated, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, 1, updated.AttemptCount)
}

func TestOpenAIWindowWarmupRepositoryAccountDeleteCleanup(t *testing.T) {
	for _, tc := range []struct {
		name   string
		delete string
	}{
		{name: "soft delete trigger", delete: `UPDATE accounts SET deleted_at = NOW() WHERE id = $1`},
		{name: "hard delete foreign key", delete: `DELETE FROM accounts WHERE id = $1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			accountID := createWarmupIntegrationAccount(t)
			repo := NewOpenAIWindowWarmupRepository(integrationDB)
			job, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
				AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
				CycleKey: "delete:" + uuid.NewString(), CycleGeneration: 29,
				Trigger: service.OpenAIWindowWarmupTriggerImport, NextAttemptAt: time.Now().UTC(),
			})
			require.NoError(t, err)
			require.True(t, inserted)
			_, err = integrationDB.ExecContext(ctx, `
				INSERT INTO openai_window_warmup_attempts
				    (job_id, attempt_no, outcome, started_at, finished_at)
				VALUES ($1, 1, 'retry', NOW(), NOW())`, job.ID)
			require.NoError(t, err)

			_, err = integrationDB.ExecContext(ctx, tc.delete, accountID)
			require.NoError(t, err)
			assertWarmupIntegrationRowsRemoved(t, accountID, job.ID)
		})
	}
}

func createWarmupIntegrationAccount(t *testing.T) int64 {
	t.Helper()
	account := mustCreateAccount(t, integrationEntClient, &service.Account{
		Name: "warmup-" + uuid.NewString(), Platform: service.PlatformOpenAI,
		Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
	})
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM accounts WHERE id = $1`, account.ID)
	})
	return account.ID
}

func mergeWarmupIntegrationAccountExtra(t *testing.T, accountID int64, extra map[string]any) {
	t.Helper()
	payload, err := json.Marshal(extra)
	require.NoError(t, err)
	_, err = integrationDB.ExecContext(context.Background(), `
		UPDATE accounts
		SET extra = COALESCE(extra, '{}'::jsonb) || $2::jsonb
		WHERE id = $1`, accountID, string(payload))
	require.NoError(t, err)
}

func assertWarmupIntegrationAccountEligible(t *testing.T, accountID int64, policy string) {
	t.Helper()
	var (
		platform, accountType, status, quota, gotPolicy string
		parentNull, schedulable, notDeleted             bool
		notExpired, notTemporarilyUnschedulable         bool
	)
	err := integrationDB.QueryRowContext(context.Background(), `
		SELECT platform::text, type::text, status::text, quota_dimension::text,
		       extra ->> 'openai_codex_warmup_policy', parent_account_id IS NULL,
		       schedulable, deleted_at IS NULL,
		       expires_at IS NULL OR expires_at > NOW(),
		       temp_unschedulable_until IS NULL OR temp_unschedulable_until <= NOW()
		FROM accounts WHERE id = $1`, accountID).Scan(
		&platform, &accountType, &status, &quota, &gotPolicy, &parentNull,
		&schedulable, &notDeleted, &notExpired, &notTemporarilyUnschedulable,
	)
	require.NoError(t, err)
	require.Equal(t, service.PlatformOpenAI, platform)
	require.Equal(t, service.AccountTypeOAuth, accountType)
	require.Equal(t, service.StatusActive, status)
	require.Equal(t, service.OpenAIWindowWarmupQuotaScopeGlobal, quota)
	require.Equal(t, policy, gotPolicy)
	require.True(t, parentNull)
	require.True(t, schedulable)
	require.True(t, notDeleted)
	require.True(t, notExpired)
	require.True(t, notTemporarilyUnschedulable)
}

func assertWarmupIntegrationAttempt(t *testing.T, jobID int64, attemptNo int, outcome string, status int, code string) {
	t.Helper()
	var gotOutcome string
	var gotStatus *int
	var gotCode *string
	err := integrationDB.QueryRowContext(context.Background(), `
		SELECT outcome, status_code, error_code
		FROM openai_window_warmup_attempts
		WHERE job_id = $1 AND attempt_no = $2`, jobID, attemptNo).Scan(&gotOutcome, &gotStatus, &gotCode)
	require.NoError(t, err)
	require.Equal(t, outcome, gotOutcome)
	if status == 0 {
		require.Nil(t, gotStatus)
	} else {
		require.NotNil(t, gotStatus)
		require.Equal(t, status, *gotStatus)
	}
	if code == "" {
		require.Nil(t, gotCode)
	} else {
		require.NotNil(t, gotCode)
		require.Equal(t, code, *gotCode)
	}
}

func assertWarmupIntegrationRowsRemoved(t *testing.T, accountID, jobID int64) {
	t.Helper()
	var jobCount, attemptCount int
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM openai_window_warmup_jobs WHERE account_id = $1`, accountID).Scan(&jobCount))
	require.NoError(t, integrationDB.QueryRowContext(context.Background(), `
		SELECT COUNT(*) FROM openai_window_warmup_attempts WHERE job_id = $1`, jobID).Scan(&attemptCount))
	require.Zero(t, jobCount)
	require.Zero(t, attemptCount)
}
