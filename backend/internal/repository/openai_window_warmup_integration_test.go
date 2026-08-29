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

func TestOpenAIWindowWarmupMigrationDoesNotArmIdleRollingReset(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	reset := now.Add(5 * time.Hour)
	account := mustCreateAccount(t, integrationEntClient, &service.Account{
		Name: "warmup-idle-trigger-" + uuid.NewString(), Platform: service.PlatformOpenAI,
		Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
		Extra: map[string]any{
			service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
			"codex_5h_used_percent":                 float64(0),
			"codex_5h_reset_at":                     reset.Format(time.RFC3339),
		},
	})
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM accounts WHERE id = $1`, account.ID)
	})

	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	job, err := repo.GetCurrent(
		ctx, account.ID, service.OpenAIWindowWarmupQuotaScopeGlobal,
	)
	require.NoError(t, err)
	require.Equal(t, service.OpenAIWindowWarmupStatePending, job.State)
	require.WithinDuration(t, reset, *job.ObservedResetAt, time.Second)
	require.False(t, job.NextAttemptAt.Before(now))
	require.LessOrEqual(t, job.NextAttemptAt.Sub(now), 31*time.Second)

	_, err = integrationDB.ExecContext(ctx,
		`UPDATE openai_window_warmup_jobs SET next_attempt_at = NOW() WHERE id = $1`, job.ID)
	require.NoError(t, err)
	claims, err := repo.ClaimDue(ctx, "idle-initial-"+uuid.NewString(), 2*time.Minute, 1, []int64{account.ID})
	require.NoError(t, err)
	require.Len(t, claims, 1)
	started, err := repo.MarkStarted(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken, now,
		service.OpenAIWindowWarmupStartEvidence{Authoritative: true, UsedPercent: 0, ResetAt: &reset})
	require.NoError(t, err)
	require.True(t, started, "a fresh initial 0%% projection must reach the probe without a second account refresh")
}

func TestOpenAIWindowWarmupMigrationBackfillsOnlyUntouchedIdleArmedJobs(t *testing.T) {
	ctx := context.Background()
	migrationSQL, err := appmigrations.FS.ReadFile("232_openai_window_warmup_latest_reset.sql")
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Second)
	reset := now.Add(5 * time.Hour)
	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	type seededJob struct {
		name          string
		extra         map[string]any
		expectTrigger bool
		attemptCount  int
		sent          bool
		wantPending   bool
		accountID     int64
		jobID         int64
		nextBefore    time.Time
	}
	jobs := []seededJob{
		{
			name: "eligible numeric zero", expectTrigger: true, wantPending: true,
			extra: map[string]any{
				service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
				"codex_5h_used_percent":                 float64(0),
				"codex_5h_reset_at":                     reset.Format(time.RFC3339Nano),
			},
		},
		{
			name: "active usage", expectTrigger: true,
			extra: map[string]any{
				service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
				"codex_5h_used_percent":                 float64(1),
				"codex_5h_reset_at":                     reset.Format(time.RFC3339Nano),
			},
		},
		{
			name: "missing usage", expectTrigger: true,
			extra: map[string]any{
				service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
				"codex_5h_reset_at":                     reset.Format(time.RFC3339Nano),
			},
		},
		{
			name: "string zero", expectTrigger: true,
			extra: map[string]any{
				service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
				"codex_5h_used_percent":                 "0",
				"codex_5h_reset_at":                     reset.Format(time.RFC3339Nano),
			},
		},
		{
			name: "policy off",
			extra: map[string]any{
				service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyOff,
				"codex_5h_used_percent":                 float64(0),
				"codex_5h_reset_at":                     reset.Format(time.RFC3339Nano),
			},
		},
		{
			name: "already attempted", expectTrigger: true, attemptCount: 1,
			extra: map[string]any{
				service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
				"codex_5h_used_percent":                 float64(0),
				"codex_5h_reset_at":                     reset.Format(time.RFC3339Nano),
			},
		},
		{
			name: "already sent", expectTrigger: true, sent: true,
			extra: map[string]any{
				service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
				"codex_5h_used_percent":                 float64(0),
				"codex_5h_reset_at":                     reset.Format(time.RFC3339Nano),
			},
		},
	}

	for i := range jobs {
		account := mustCreateAccount(t, integrationEntClient, &service.Account{
			Name: "warmup-backfill-" + uuid.NewString(), Platform: service.PlatformOpenAI,
			Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
			Extra: jobs[i].extra,
		})
		jobs[i].accountID = account.ID
		t.Cleanup(func() {
			_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM accounts WHERE id = $1`, account.ID)
		})

		var job *service.OpenAIWindowWarmupJob
		if jobs[i].expectTrigger {
			job, err = repo.GetCurrent(ctx, account.ID, service.OpenAIWindowWarmupQuotaScopeGlobal)
			require.NoError(t, err, jobs[i].name)
		} else {
			var inserted bool
			job, inserted, err = repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
				AccountID: account.ID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
				CycleKey: "backfill:" + uuid.NewString(), CycleGeneration: int64(i + 1),
				Trigger: service.OpenAIWindowWarmupTriggerImport, NextAttemptAt: reset,
			})
			require.NoError(t, err, jobs[i].name)
			require.True(t, inserted, jobs[i].name)
		}
		jobs[i].jobID = job.ID
		jobs[i].nextBefore = reset.Add(time.Duration(i+1) * time.Minute)
		_, err = integrationDB.ExecContext(ctx, `
			UPDATE openai_window_warmup_jobs
			SET state = 'armed', observed_reset_at = $2, next_attempt_at = $3,
			    attempt_count = $4, sent_at = CASE WHEN $5 THEN NOW() END
			WHERE id = $1`, job.ID, reset, jobs[i].nextBefore, jobs[i].attemptCount, jobs[i].sent)
		require.NoError(t, err, jobs[i].name)
	}

	const (
		firstReplay  = "994_test_openai_window_warmup_idle_backfill.sql"
		secondReplay = "995_test_openai_window_warmup_idle_backfill_idempotent.sql"
	)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(),
			`DELETE FROM schema_migrations WHERE filename IN ($1, $2)`, firstReplay, secondReplay)
	})
	var dbBefore time.Time
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT NOW()`).Scan(&dbBefore))
	replayFS := fstest.MapFS{
		firstReplay:  &fstest.MapFile{Data: migrationSQL},
		secondReplay: &fstest.MapFile{Data: migrationSQL},
	}
	require.NoError(t, applyMigrationsFS(ctx, integrationDB, replayFS))
	var dbAfter time.Time
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT NOW()`).Scan(&dbAfter))

	for _, seeded := range jobs {
		var (
			state         string
			nextAttemptAt time.Time
			observedReset time.Time
		)
		err = integrationDB.QueryRowContext(ctx, `
			SELECT state, next_attempt_at, observed_reset_at
			FROM openai_window_warmup_jobs WHERE id = $1`, seeded.jobID).Scan(
			&state, &nextAttemptAt, &observedReset,
		)
		require.NoError(t, err, seeded.name)
		require.WithinDuration(t, reset, observedReset, time.Microsecond, seeded.name)
		if seeded.wantPending {
			require.Equal(t, service.OpenAIWindowWarmupStatePending, state, seeded.name)
			require.False(t, nextAttemptAt.Before(dbBefore), seeded.name)
			require.LessOrEqual(t, nextAttemptAt.Sub(dbAfter), 30*time.Second, seeded.name)
			continue
		}
		require.Equal(t, service.OpenAIWindowWarmupStateArmed, state, seeded.name)
		require.WithinDuration(t, seeded.nextBefore, nextAttemptAt, time.Microsecond, seeded.name)
	}
}

func TestOpenAIWindowWarmupMigrationPreservesIdleResetBaselineAcrossAccountRefresh(t *testing.T) {
	ctx := context.Background()
	previousSQL, err := appmigrations.FS.ReadFile("232_openai_window_warmup_latest_reset.sql")
	require.NoError(t, err)
	fixedSQL, err := appmigrations.FS.ReadFile("233_openai_window_warmup_idle_reset_baseline.sql")
	require.NoError(t, err)
	numericGuardSQL, err := appmigrations.FS.ReadFile("234_openai_window_warmup_numeric_guard.sql")
	require.NoError(t, err)

	const (
		previousReplay = "996_test_openai_window_warmup_previous_idle_trigger.sql"
		firstReplay    = "997_test_openai_window_warmup_idle_baseline.sql"
		secondReplay   = "998_test_openai_window_warmup_idle_baseline_idempotent.sql"
	)
	t.Cleanup(func() {
		// This test intentionally replays migration 233 to reproduce the old
		// baseline behavior. Restore the current 234 trigger afterward so later
		// tests observe the same schema that TestMain applied initially.
		_, _ = integrationDB.ExecContext(context.Background(), string(numericGuardSQL))
		_, _ = integrationDB.ExecContext(context.Background(),
			`DELETE FROM schema_migrations WHERE filename IN ($1, $2, $3)`,
			previousReplay, firstReplay, secondReplay)
	})
	require.NoError(t, applyMigrationsFS(ctx, integrationDB, fstest.MapFS{
		previousReplay: &fstest.MapFile{Data: previousSQL},
	}))

	now := time.Now().UTC().Truncate(time.Second)
	resetOne := now.Add(5 * time.Hour)
	account := mustCreateAccount(t, integrationEntClient, &service.Account{
		Name: "warmup-idle-baseline-" + uuid.NewString(), Platform: service.PlatformOpenAI,
		Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
		Extra: map[string]any{
			service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
			"codex_5h_used_percent":                 float64(0),
			"codex_5h_reset_at":                     resetOne.Format(time.RFC3339Nano),
		},
	})
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM accounts WHERE id = $1`, account.ID)
	})

	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	job, err := repo.GetCurrent(ctx, account.ID, service.OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, service.OpenAIWindowWarmupStatePending, job.State)
	require.WithinDuration(t, resetOne, *job.ObservedResetAt, time.Microsecond)
	attemptedAccount := mustCreateAccount(t, integrationEntClient, &service.Account{
		Name: "warmup-idle-baseline-attempted-" + uuid.NewString(), Platform: service.PlatformOpenAI,
		Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
		Extra: map[string]any{
			service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
			"codex_5h_used_percent":                 float64(0),
			"codex_5h_reset_at":                     resetOne.Format(time.RFC3339Nano),
		},
	})
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM accounts WHERE id = $1`, attemptedAccount.ID)
	})
	attemptedJob, err := repo.GetCurrent(ctx, attemptedAccount.ID, service.OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)
	sentAccount := mustCreateAccount(t, integrationEntClient, &service.Account{
		Name: "warmup-idle-baseline-sent-" + uuid.NewString(), Platform: service.PlatformOpenAI,
		Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
		Extra: map[string]any{
			service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
			"codex_5h_used_percent":                 float64(0),
			"codex_5h_reset_at":                     resetOne.Format(time.RFC3339Nano),
		},
	})
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(), `DELETE FROM accounts WHERE id = $1`, sentAccount.ID)
	})
	sentJob, err := repo.GetCurrent(ctx, sentAccount.ID, service.OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)

	resetTwo := resetOne.Add(2 * time.Minute)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE accounts
		SET extra = jsonb_set(
			jsonb_set(extra, '{codex_5h_used_percent}', '0'::jsonb, true),
			'{codex_5h_reset_at}', to_jsonb($2::text), true
		)
		WHERE id = $1`, account.ID, resetTwo.Format(time.RFC3339Nano))
	require.NoError(t, err)
	job, err = repo.GetCurrent(ctx, account.ID, service.OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)
	// This assertion proves the test first reproduced the migration-232 bug.
	require.WithinDuration(t, resetTwo, *job.ObservedResetAt, time.Microsecond)

	staleNext := resetTwo.Add(time.Hour)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE openai_window_warmup_jobs
		SET state = 'armed', next_attempt_at = $2
		WHERE id = $1`, job.ID, staleNext)
	require.NoError(t, err)
	attemptedNext := staleNext.Add(time.Minute)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE openai_window_warmup_jobs
		SET state = 'armed', next_attempt_at = $2, attempt_count = 1
		WHERE id = $1`, attemptedJob.ID, attemptedNext)
	require.NoError(t, err)
	sentNext := attemptedNext.Add(time.Minute)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE openai_window_warmup_jobs
		SET state = 'armed', next_attempt_at = $2, sent_at = NOW()
		WHERE id = $1`, sentJob.ID, sentNext)
	require.NoError(t, err)

	replayFS := fstest.MapFS{
		firstReplay:  &fstest.MapFile{Data: fixedSQL},
		secondReplay: &fstest.MapFile{Data: fixedSQL},
	}
	var dbBefore time.Time
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT NOW()`).Scan(&dbBefore))
	require.NoError(t, applyMigrationsFS(ctx, integrationDB, replayFS))
	var dbAfter time.Time
	require.NoError(t, integrationDB.QueryRowContext(ctx, `SELECT NOW()`).Scan(&dbAfter))

	job, err = repo.GetCurrent(ctx, account.ID, service.OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, service.OpenAIWindowWarmupStatePending, job.State)
	require.WithinDuration(t, resetTwo, *job.ObservedResetAt, time.Microsecond)
	require.False(t, job.NextAttemptAt.Before(dbBefore))
	require.LessOrEqual(t, job.NextAttemptAt.Sub(dbAfter), 30*time.Second)
	attemptedJob, err = repo.GetCurrent(ctx, attemptedAccount.ID, service.OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, service.OpenAIWindowWarmupStateArmed, attemptedJob.State)
	require.Equal(t, 1, attemptedJob.AttemptCount)
	require.WithinDuration(t, attemptedNext, attemptedJob.NextAttemptAt, time.Microsecond,
		"an attempted cycle must retain its state and schedule")
	sentJob, err = repo.GetCurrent(ctx, sentAccount.ID, service.OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, service.OpenAIWindowWarmupStateArmed, sentJob.State)
	require.NotNil(t, sentJob.SentAt)
	require.WithinDuration(t, sentNext, sentJob.NextAttemptAt, time.Microsecond,
		"a sent cycle must retain its state and schedule")

	resetThree := resetTwo.Add(2 * time.Minute)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE accounts
		SET extra = jsonb_set(
			jsonb_set(extra, '{codex_5h_used_percent}', '0'::jsonb, true),
			'{codex_5h_reset_at}', to_jsonb($2::text), true
		)
		WHERE id = $1`, account.ID, resetThree.Format(time.RFC3339Nano))
	require.NoError(t, err)
	job, err = repo.GetCurrent(ctx, account.ID, service.OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, service.OpenAIWindowWarmupStatePending, job.State)
	require.WithinDuration(t, resetTwo, *job.ObservedResetAt, time.Microsecond,
		"a later rolling 0%% reset must not replace the durable comparison baseline")

	resetActive := resetThree.Add(2 * time.Minute)
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE accounts
		SET extra = jsonb_set(
			jsonb_set(extra, '{codex_5h_used_percent}', '1'::jsonb, true),
			'{codex_5h_reset_at}', to_jsonb($2::text), true
		)
		WHERE id = $1`, account.ID, resetActive.Format(time.RFC3339Nano))
	require.NoError(t, err)
	job, err = repo.GetCurrent(ctx, account.ID, service.OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, service.OpenAIWindowWarmupStateArmed, job.State)
	require.WithinDuration(t, resetActive, *job.ObservedResetAt, time.Microsecond,
		"positive usage must still replace the baseline with the authoritative reset")
	require.True(t, job.NextAttemptAt.After(resetActive.Add(89*time.Second)))
	require.LessOrEqual(t, job.NextAttemptAt.Sub(resetActive), 120*time.Second)
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

func TestOpenAIWindowWarmupRepositoryCurrentPrefersActiveOverNewerBlocked(t *testing.T) {
	ctx := context.Background()
	accountID := createWarmupIntegrationAccount(t)
	repo := NewOpenAIWindowWarmupRepository(integrationDB)

	active, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
		AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "active:" + uuid.NewString(), CycleGeneration: 1,
		Trigger: service.OpenAIWindowWarmupTriggerImport, NextAttemptAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	require.True(t, inserted)

	// A legacy 231 cleanup can leave a newer quarantined row behind the real
	// active cycle. Read/update APIs must continue to project the live row.
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO openai_window_warmup_jobs
		    (account_id, quota_scope, state, trigger, cycle_key, cycle_generation,
		     next_attempt_at, last_error_code, last_error, created_at, updated_at)
		VALUES ($1, 'global', 'blocked', 'migration', $2, 2, NOW(),
		        'migration_duplicate_active_job', 'legacy duplicate', NOW(), NOW())`,
		accountID, "blocked:"+uuid.NewString())
	require.NoError(t, err)

	current, err := repo.GetCurrent(ctx, accountID, service.OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)
	require.Equal(t, active.ID, current.ID)
	require.Equal(t, service.OpenAIWindowWarmupStatePending, current.State)

	byAccount, err := repo.GetCurrentForAccounts(ctx, []int64{accountID}, service.OpenAIWindowWarmupQuotaScopeGlobal)
	require.NoError(t, err)
	require.Contains(t, byAccount, accountID)
	require.Equal(t, active.ID, byAccount[accountID].ID)

	// Unblock is a no-op while an active row exists; it must not try to mutate
	// the newer blocked row (or collide with the partial unique index).
	unblocked, changed, err := repo.UnblockAccount(ctx, accountID, time.Now().UTC(), nil)
	require.NoError(t, err)
	require.False(t, changed)
	require.Equal(t, active.ID, unblocked.ID)
}

func TestOpenAIWindowWarmupNumericGuardHandlesOversizedJSONNumbers(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		number    string
		wantState string
		wantIdle  bool
	}{
		{name: "oversized positive", number: "1e100000", wantState: service.OpenAIWindowWarmupStateArmed, wantIdle: false},
		{name: "oversized negative", number: "-1e100000", wantState: service.OpenAIWindowWarmupStatePending, wantIdle: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accountID := createWarmupIntegrationAccount(t)
			reset := time.Now().UTC().Add(2 * time.Hour).Format(time.RFC3339Nano)
			_, err := integrationDB.ExecContext(ctx, fmt.Sprintf(`
				UPDATE accounts
				SET extra = '{"openai_codex_warmup_policy":"continuous",
				               "codex_5h_used_percent":%s,
				               "codex_5h_reset_at":"%s"}'::jsonb
				WHERE id = $1`, tc.number, reset), accountID)
			require.NoError(t, err, "the trigger must not cast the oversized number")

			job, err := NewOpenAIWindowWarmupRepository(integrationDB).GetCurrent(
				ctx, accountID, service.OpenAIWindowWarmupQuotaScopeGlobal,
			)
			require.NoError(t, err)
			require.Equal(t, tc.wantState, job.State)
			if tc.wantIdle {
				require.NotNil(t, job.ObservedResetAt)
				require.LessOrEqual(t, job.NextAttemptAt.Sub(time.Now().UTC()), 31*time.Second)
			} else {
				require.NotNil(t, job.ObservedResetAt)
				require.True(t, job.NextAttemptAt.After(*job.ObservedResetAt))
			}
		})
	}
}

func TestOpenAIWindowWarmupNumericGuardDoesNotRearmResetCycle(t *testing.T) {
	ctx := context.Background()
	numericGuardSQL, err := appmigrations.FS.ReadFile("234_openai_window_warmup_numeric_guard.sql")
	require.NoError(t, err)
	const replay = "995_test_openai_window_warmup_numeric_guard_reset_cycle.sql"

	accountID := createWarmupIntegrationAccount(t)
	mergeWarmupIntegrationAccountExtra(t, accountID, map[string]any{
		service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
		"codex_5h_used_percent":                 float64(0),
		"codex_5h_reset_at":                     time.Now().UTC().Add(2 * time.Hour).Format(time.RFC3339Nano),
	})
	// Remove the trigger-created initial row so the synthetic reset-cycle row can
	// be isolated from the partial unique active-job index.
	_, err = integrationDB.ExecContext(ctx, `
		DELETE FROM openai_window_warmup_jobs WHERE account_id = $1`, accountID)
	require.NoError(t, err)
	reset := time.Now().UTC().Add(90 * time.Minute).Truncate(time.Microsecond)
	next := reset.Add(90 * time.Second)
	cycleKey := "reset:" + reset.Format(time.RFC3339Nano)
	_, err = integrationDB.ExecContext(ctx, `
		INSERT INTO openai_window_warmup_jobs
		    (account_id, quota_scope, state, trigger, cycle_key, cycle_generation,
		     observed_reset_at, next_attempt_at, attempt_count, created_at, updated_at)
		VALUES ($1, 'global', 'armed', 'reset', $2, 1, $3, $4, 0, NOW(), NOW())`,
		accountID, cycleKey, reset, next)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = integrationDB.ExecContext(context.Background(),
			`DELETE FROM schema_migrations WHERE filename = $1`, replay)
	})

	require.NoError(t, applyMigrationsFS(ctx, integrationDB, fstest.MapFS{
		replay: &fstest.MapFile{Data: numericGuardSQL},
	}))

	var state string
	var gotNext, gotReset time.Time
	require.NoError(t, integrationDB.QueryRowContext(ctx, `
		SELECT state, next_attempt_at, observed_reset_at
		FROM openai_window_warmup_jobs
		WHERE account_id = $1 AND cycle_key = $2`, accountID, cycleKey).Scan(&state, &gotNext, &gotReset))
	require.Equal(t, service.OpenAIWindowWarmupStateArmed, state)
	require.WithinDuration(t, next, gotNext, time.Microsecond,
		"a completed continuous reset cycle must remain armed until its authoritative reset")
	require.WithinDuration(t, reset, gotReset, time.Microsecond)
}

func TestOpenAIWindowWarmupMigrationReconcilesEarlyDuplicateActiveJobs(t *testing.T) {
	ctx := context.Background()
	migrationSQL, err := appmigrations.FS.ReadFile("231_openai_window_warmup.sql")
	require.NoError(t, err)
	latestResetSQL, err := appmigrations.FS.ReadFile("232_openai_window_warmup_latest_reset.sql")
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
		latestReplay = "993_test_openai_window_warmup_latest_reset.sql"
	)
	t.Cleanup(func() {
		for _, accountID := range accountIDs {
			_, _ = integrationDB.ExecContext(context.Background(),
				`DELETE FROM openai_window_warmup_jobs WHERE account_id = $1`, accountID)
		}
		_, _ = integrationDB.ExecContext(context.Background(),
			`DELETE FROM schema_migrations WHERE filename IN ($1, $2, $3)`, firstReplay, secondReplay, latestReplay)
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
		latestReplay: &fstest.MapFile{Data: latestResetSQL},
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

func TestOpenAIWindowWarmupMigrationSelectsLatestValidReset(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	primaryReset := now.Add(time.Hour)
	globalReset := now.Add(2 * time.Hour)
	account := mustCreateAccount(t, integrationEntClient, &service.Account{
		Name: "warmup-latest-reset-" + uuid.NewString(), Platform: service.PlatformOpenAI,
		Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
		Extra: map[string]any{
			service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
			"codex_5h_reset_at":                     primaryReset.Format(time.RFC3339Nano),
			"codex_global_5h_reset_at":              globalReset.Format(time.RFC3339Nano),
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
	require.WithinDuration(t, globalReset, *job.ObservedResetAt, time.Microsecond)
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
	jobCreatedBeforeReset := olderReset.Add(-time.Hour)
	usedBeforeObservedReset := olderReset.Add(-time.Minute)
	usedAfterObservedReset := olderReset.Add(time.Minute)
	recentUse := now.Add(time.Minute)
	tests := []struct {
		name          string
		policy        string
		policyExtra   map[string]any
		initialCycle  bool
		observedReset *time.Time
		accountReset  *time.Time
		evidence      service.OpenAIWindowWarmupStartEvidence
		jobCreatedAt  *time.Time
		lastUsedAt    *time.Time
		wantStarted   bool
	}{
		{name: "policy off fails closed", evidence: service.OpenAIWindowWarmupStartEvidence{Authoritative: true}, wantStarted: false},
		{name: "enabled policy starts", policy: service.OpenAIWindowWarmupPolicyContinuous, evidence: service.OpenAIWindowWarmupStartEvidence{Authoritative: true}, wantStarted: true},
		{
			name: "blank canonical falls back to alias",
			policyExtra: map[string]any{
				service.OpenAICodexWarmupPolicyExtraKey: "   ",
				service.CodexWarmupPolicyExtraKey:       service.OpenAIWindowWarmupPolicyContinuous,
			},
			evidence: service.OpenAIWindowWarmupStartEvidence{Authoritative: true}, wantStarted: true,
		},
		{
			name: "non-string canonical falls back to alias",
			policyExtra: map[string]any{
				service.OpenAICodexWarmupPolicyExtraKey: false,
				service.CodexWarmupPolicyExtraKey:       service.OpenAIWindowWarmupPolicyContinuous,
			},
			evidence: service.OpenAIWindowWarmupStartEvidence{Authoritative: true}, wantStarted: true,
		},
		{
			name: "active usage rejects a newer reset", policy: service.OpenAIWindowWarmupPolicyContinuous,
			observedReset: &olderReset, accountReset: &newerReset,
			evidence: service.OpenAIWindowWarmupStartEvidence{Authoritative: true, UsedPercent: 1, ResetAt: &newerReset}, wantStarted: false,
		},
		{
			name: "same future reset stays armed", policy: service.OpenAIWindowWarmupPolicyContinuous,
			observedReset: &newerReset, accountReset: &newerReset,
			evidence: service.OpenAIWindowWarmupStartEvidence{Authoritative: true, ResetAt: &newerReset}, wantStarted: false,
		},
		{
			name: "initial idle equal reset starts", policy: service.OpenAIWindowWarmupPolicyContinuous,
			initialCycle: true, observedReset: &newerReset, accountReset: &newerReset,
			evidence: service.OpenAIWindowWarmupStartEvidence{Authoritative: true, ResetAt: &newerReset}, wantStarted: true,
		},
		{
			name: "initial active equal reset stays armed", policy: service.OpenAIWindowWarmupPolicyContinuous,
			initialCycle: true, observedReset: &newerReset, accountReset: &newerReset,
			evidence: service.OpenAIWindowWarmupStartEvidence{Authoritative: true, UsedPercent: 1, ResetAt: &newerReset}, wantStarted: false,
		},
		{
			name: "idle rolling reset allows start", policy: service.OpenAIWindowWarmupPolicyContinuous,
			observedReset: &olderReset, accountReset: &newerReset,
			evidence: service.OpenAIWindowWarmupStartEvidence{Authoritative: true, ResetAt: &newerReset}, wantStarted: true,
		},
		{
			name: "recent business use rejects idle-looking reset", policy: service.OpenAIWindowWarmupPolicyContinuous,
			observedReset: &olderReset, accountReset: &newerReset, lastUsedAt: &recentUse,
			evidence: service.OpenAIWindowWarmupStartEvidence{Authoritative: true, ResetAt: &newerReset}, wantStarted: false,
		},
		{
			name: "business use before observed reset belongs to old window", policy: service.OpenAIWindowWarmupPolicyContinuous,
			observedReset: &olderReset, accountReset: &newerReset,
			jobCreatedAt: &jobCreatedBeforeReset, lastUsedAt: &usedBeforeObservedReset,
			evidence: service.OpenAIWindowWarmupStartEvidence{Authoritative: true, ResetAt: &newerReset}, wantStarted: true,
		},
		{
			name: "business use after observed reset suppresses send", policy: service.OpenAIWindowWarmupPolicyContinuous,
			observedReset: &olderReset, accountReset: &newerReset,
			jobCreatedAt: &jobCreatedBeforeReset, lastUsedAt: &usedAfterObservedReset,
			evidence: service.OpenAIWindowWarmupStartEvidence{Authoritative: true, ResetAt: &newerReset}, wantStarted: false,
		},
		{
			name: "non-authoritative evidence fails closed", policy: service.OpenAIWindowWarmupPolicyContinuous,
			evidence: service.OpenAIWindowWarmupStartEvidence{}, wantStarted: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			accountID := createWarmupIntegrationAccount(t)
			repo := NewOpenAIWindowWarmupRepository(integrationDB)
			cycleKey := "cas:" + uuid.NewString()
			if tc.initialCycle {
				cycleKey = "initial:41"
			}
			job, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
				AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
				CycleKey: cycleKey, CycleGeneration: 41,
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
				if tc.initialCycle {
					extra["codex_5h_used_percent"] = tc.evidence.UsedPercent
				}
				if tc.accountReset != nil {
					extra["codex_5h_reset_at"] = tc.accountReset.Format(time.RFC3339Nano)
				}
				mergeWarmupIntegrationAccountExtra(t, accountID, extra)
				if tc.policy != "" {
					assertWarmupIntegrationAccountEligible(t, accountID, tc.policy)
				}
			}
			if tc.jobCreatedAt != nil {
				_, err = integrationDB.ExecContext(ctx,
					`UPDATE openai_window_warmup_jobs SET created_at = $2 WHERE id = $1`, job.ID, tc.jobCreatedAt.UTC())
				require.NoError(t, err)
			}
			if tc.lastUsedAt != nil {
				_, err = integrationDB.ExecContext(ctx,
					`UPDATE accounts SET last_used_at = $2 WHERE id = $1`, accountID, tc.lastUsedAt.UTC())
				require.NoError(t, err)
			}

			claims, err := repo.ClaimDue(ctx, "cas-owner-"+uuid.NewString(), 2*time.Minute, 1, []int64{accountID})
			require.NoError(t, err)
			require.Len(t, claims, 1)
			started, err := repo.MarkStarted(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken, now, tc.evidence)
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
			(job_id, attempt_no, outcome, status_code, error_code, started_at, finished_at)
			VALUES ($1, $2, 'failed', 401, 'needs_reauth', NOW(), NOW())`, job.ID, attempt)
		require.NoError(t, err)
	}
	_, err = integrationDB.ExecContext(ctx, `
		UPDATE openai_window_warmup_runtime
		SET next_send_at = to_timestamp(0), permit_token = NULL, inflight_until = NULL
		WHERE gate_key = 'global_send'`)
	require.NoError(t, err)
	localFailurePermit, grantedAfterLocalFailures, err := repo.ReserveGlobalSend(ctx, 5*time.Second, 2*time.Minute)
	require.NoError(t, err)
	require.True(t, grantedAfterLocalFailures, "account-local 401 failures must not trip the global circuit")
	released, err = repo.ReleaseGlobalSend(ctx, localFailurePermit)
	require.NoError(t, err)
	require.True(t, released)

	_, err = integrationDB.ExecContext(ctx, `DELETE FROM openai_window_warmup_attempts WHERE job_id = $1`, job.ID)
	require.NoError(t, err)
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
	ok, err := repo.MarkStarted(ctx, job.ID, first[0].Owner, first[0].LeaseToken, attemptOneAt,
		service.OpenAIWindowWarmupStartEvidence{Authoritative: true})
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = repo.MarkStarted(ctx, job.ID, second[0].Owner, second[0].LeaseToken, attemptOneAt,
		service.OpenAIWindowWarmupStartEvidence{Authoritative: true})
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
	ok, err = repo.MarkStarted(ctx, job.ID, finalClaim[0].Owner, finalClaim[0].LeaseToken, attemptTwoAt,
		service.OpenAIWindowWarmupStartEvidence{Authoritative: true})
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

func TestOpenAIWindowWarmupRepositoryAuthStateRetryUsesExactTransitionCAS(t *testing.T) {
	ctx := context.Background()
	accountID := createWarmupIntegrationAccount(t)
	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	job, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
		AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "auth-state-retry:" + uuid.NewString(), CycleGeneration: 91,
		Trigger: service.OpenAIWindowWarmupTriggerReset, NextAttemptAt: time.Now().UTC().Add(-time.Second),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	mergeWarmupIntegrationAccountExtra(t, accountID, map[string]any{
		service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
	})

	claims, err := repo.ClaimDue(ctx, "auth-state-owner", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, claims, 1)
	startedAt := time.Now().UTC()
	started, err := repo.MarkStarted(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken, startedAt,
		service.OpenAIWindowWarmupStartEvidence{Authoritative: true})
	require.NoError(t, err)
	require.True(t, started)
	blocked, err := repo.MarkBlocked(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken,
		startedAt.Add(time.Second), 401, "needs_reauth", "warmup blocked")
	require.NoError(t, err)
	require.True(t, blocked)

	next := time.Now().UTC().Add(time.Minute)
	exact := service.OpenAIWindowWarmupAuthStateRetry{
		JobID: job.ID, CycleGeneration: 91, AttemptCount: 1,
		BlockedState: service.OpenAIWindowWarmupStateBlocked,
		StatusCode:   401, ErrorCode: "needs_reauth",
		RetryCode: "account_state_update_failed", NextAttemptAt: next,
	}
	for _, stale := range []service.OpenAIWindowWarmupAuthStateRetry{
		func() service.OpenAIWindowWarmupAuthStateRetry { value := exact; value.CycleGeneration++; return value }(),
		func() service.OpenAIWindowWarmupAuthStateRetry { value := exact; value.AttemptCount++; return value }(),
		func() service.OpenAIWindowWarmupAuthStateRetry {
			value := exact
			value.ErrorCode = "blocked"
			return value
		}(),
	} {
		updated, retryErr := repo.RequeueAuthStateUpdateFailure(ctx, stale)
		require.NoError(t, retryErr)
		require.False(t, updated)
	}

	updated, err := repo.RequeueAuthStateUpdateFailure(ctx, exact)
	require.NoError(t, err)
	require.True(t, updated)
	retrying, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Equal(t, service.OpenAIWindowWarmupStateRetrying, retrying.State)
	require.Equal(t, 1, retrying.AttemptCount)
	require.NotNil(t, retrying.StatusCode)
	require.Equal(t, 401, *retrying.StatusCode)
	require.Equal(t, "account_state_update_failed", retrying.LastErrorCode)
	require.WithinDuration(t, next, retrying.NextAttemptAt, time.Microsecond)
	require.Empty(t, retrying.LeaseOwner)
	require.Empty(t, retrying.LeaseToken)
	require.Nil(t, retrying.LeaseUntil)

	updated, err = repo.RequeueAuthStateUpdateFailure(ctx, exact)
	require.NoError(t, err)
	require.False(t, updated, "a delayed owner must not rearm a job that already left the blocked transition")
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
	started, err := repo.MarkStarted(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken, now,
		service.OpenAIWindowWarmupStartEvidence{Authoritative: true})
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
	started, err := repo.MarkStarted(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken, now,
		service.OpenAIWindowWarmupStartEvidence{Authoritative: true})
	require.NoError(t, err)
	require.True(t, started)
	uncertain, err := repo.MarkUncertain(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken,
		now.Add(time.Second), now.Add(-time.Second), 200, "possibly_sent", "sanitized",
		service.OpenAIWindowWarmupUncertainEvidence{Authoritative: true, ResetAt: &now, Terminal: true})
	require.NoError(t, err)
	require.True(t, uncertain)
	assertWarmupIntegrationAttempt(t, job.ID, 1, "uncertain", 200, "possibly_sent")
	uncertainJob, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.NotNil(t, uncertainJob.UncertainObservedAt)
	require.NotNil(t, uncertainJob.UncertainObservedResetAt)
	require.WithinDuration(t, now, *uncertainJob.UncertainObservedResetAt, time.Millisecond)
	require.True(t, uncertainJob.UncertainTerminalObserved)

	takeover, err := repo.ClaimDue(ctx, "uncertain-reconciler", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, takeover, 1)
	require.NotNil(t, takeover[0].Job.UncertainObservedAt)
	require.NotNil(t, takeover[0].Job.UncertainObservedResetAt)
	require.WithinDuration(t, now, *takeover[0].Job.UncertainObservedResetAt, time.Millisecond)
	require.True(t, takeover[0].Job.UncertainTerminalObserved)
	reset := now.Add(5 * time.Hour)
	ok, err := repo.MarkSuppressed(ctx, job.ID, takeover[0].Owner, takeover[0].LeaseToken,
		now.Add(2*time.Second), &reset, "lease_takeover_reconciled")
	require.NoError(t, err)
	require.True(t, ok)
	assertWarmupIntegrationAttempt(t, job.ID, 1, "suppressed", 200, "lease_takeover_reconciled")
	completedJob, err := repo.GetByID(ctx, job.ID)
	require.NoError(t, err)
	require.Nil(t, completedJob.UncertainObservedAt)
	require.Nil(t, completedJob.UncertainObservedResetAt)
	require.False(t, completedJob.UncertainTerminalObserved)
}

func TestOpenAIWindowWarmupRepositorySuccessFinalizesUncertainAttempt(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	accountID := createWarmupIntegrationAccount(t)
	repo := NewOpenAIWindowWarmupRepository(integrationDB)
	job, inserted, err := repo.Enqueue(ctx, service.OpenAIWindowWarmupEnqueue{
		AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		CycleKey: "success-uncertain:" + uuid.NewString(), CycleGeneration: 77,
		Trigger: service.OpenAIWindowWarmupTriggerReset, NextAttemptAt: now.Add(-time.Minute),
	})
	require.NoError(t, err)
	require.True(t, inserted)
	mergeWarmupIntegrationAccountExtra(t, accountID, map[string]any{
		service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
	})
	claims, err := repo.ClaimDue(ctx, "uncertain-success-sender", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, claims, 1)
	started, err := repo.MarkStarted(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken, now,
		service.OpenAIWindowWarmupStartEvidence{Authoritative: true})
	require.NoError(t, err)
	require.True(t, started)
	uncertain, err := repo.MarkUncertain(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken,
		now.Add(time.Second), now.Add(-time.Second), 200, "possibly_sent", "sanitized",
		service.OpenAIWindowWarmupUncertainEvidence{Authoritative: true, ResetAt: &now, Terminal: true})
	require.NoError(t, err)
	require.True(t, uncertain)
	assertWarmupIntegrationAttempt(t, job.ID, 1, "uncertain", 200, "possibly_sent")

	takeover, err := repo.ClaimDue(ctx, "uncertain-success-reconciler", 2*time.Minute, 1, []int64{accountID})
	require.NoError(t, err)
	require.Len(t, takeover, 1)
	reset := now.Add(5 * time.Hour)
	succeeded, err := repo.MarkSuccess(ctx, job.ID, takeover[0].Owner, takeover[0].LeaseToken,
		now.Add(2*time.Second), &reset, 200, "")
	require.NoError(t, err)
	require.True(t, succeeded)
	assertWarmupIntegrationAttempt(t, job.ID, 1, "success", 200, "")
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
	started, err := repo.MarkStarted(ctx, job.ID, claims[0].Owner, claims[0].LeaseToken, now,
		service.OpenAIWindowWarmupStartEvidence{Authoritative: true})
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
