package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

// openAIWindowWarmupSQL intentionally uses the small SQL port shared by the
// repository package.  Keeping this adapter on raw SQL avoids generating Ent
// code for a rapidly evolving worker state machine and lets integration tests
// exercise PostgreSQL locking semantics directly.
type openAIWindowWarmupSQL interface {
	sqlExecutor
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

type openAIWindowWarmupRepository struct {
	db openAIWindowWarmupSQL
}

// NewOpenAIWindowWarmupRepository constructs the durable warmup repository.
func NewOpenAIWindowWarmupRepository(db *sql.DB) service.OpenAIWindowWarmupRepository {
	return &openAIWindowWarmupRepository{db: db}
}

const warmupJobSelectColumns = `
    id, account_id, quota_scope, state, trigger, cycle_key, cycle_generation,
    observed_reset_at, uncertain_observed_reset_at, uncertain_observed_at,
    uncertain_terminal_observed,
    next_attempt_at, attempt_count, sent_at, lease_owner, lease_token,
    lease_until, last_attempt_at, last_success_at, status_code, last_error_code,
    last_error, created_at, updated_at`

func (r *openAIWindowWarmupRepository) Enqueue(ctx context.Context, in service.OpenAIWindowWarmupEnqueue) (*service.OpenAIWindowWarmupJob, bool, error) {
	if r == nil || r.db == nil {
		return nil, false, errors.New("nil openai warmup repository")
	}
	if in.AccountID <= 0 || strings.TrimSpace(in.CycleKey) == "" {
		return nil, false, errors.New("account_id and cycle_key are required")
	}
	scope := strings.TrimSpace(in.QuotaScope)
	if scope == "" {
		scope = service.OpenAIWindowWarmupQuotaScopeGlobal
	}
	trigger := strings.TrimSpace(in.Trigger)
	if trigger == "" {
		trigger = service.OpenAIWindowWarmupTriggerImport
	}
	next := in.NextAttemptAt
	if next.IsZero() {
		next = time.Now().UTC()
	}
	// The state is selected with the database clock, not the application clock.
	// This matters when an import request and a worker run on hosts with skew.
	insertSQL := `
INSERT INTO openai_window_warmup_jobs
    (account_id, quota_scope, state, trigger, cycle_key, cycle_generation,
     observed_reset_at, next_attempt_at, attempt_count, created_at, updated_at)
VALUES ($1, $2,
			CASE WHEN $6::timestamptz IS NOT NULL AND $6::timestamptz > NOW() THEN 'armed' ELSE 'pending' END,
	        $3, $4, $5, $6::timestamptz, $7, 0, NOW(), NOW())
ON CONFLICT DO NOTHING
RETURNING ` + warmupJobSelectColumns
	job, err := scanWarmupJob(r.db.QueryRowContext(ctx, insertSQL,
		in.AccountID, scope, trigger, in.CycleKey, in.CycleGeneration,
		nullWarmupTime(in.ObservedResetAt), next.UTC()))
	if err == nil {
		return job, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	// A duplicate is a successful idempotent enqueue. Return the existing row so
	// callers can expose its current state without an additional race-prone API.
	selectSQL := `SELECT ` + warmupJobSelectColumns + `
	FROM openai_window_warmup_jobs
	WHERE account_id = $1 AND quota_scope = $2
	  AND (cycle_key = $3 OR state IN ('pending', 'armed', 'due', 'running', 'retrying', 'uncertain', 'possibly_sent'))
	ORDER BY (cycle_key = $3) DESC, id DESC
	LIMIT 1`
	job, err = scanWarmupJob(r.db.QueryRowContext(ctx, selectSQL, in.AccountID, scope, in.CycleKey))
	if err != nil {
		return nil, false, err
	}
	return job, false, nil
}

// ClaimDue atomically claims due rows. The CTE lock and update happen in one
// statement, so two workers cannot observe the same cycle. Expired leases are
// reclaimable, while all subsequent writes remain fenced by lease_token.
func (r *openAIWindowWarmupRepository) ClaimDue(ctx context.Context, owner string, lease time.Duration, limit int, allowedAccountIDs []int64) ([]service.OpenAIWindowWarmupClaim, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("nil openai warmup repository")
	}
	owner = strings.TrimSpace(owner)
	if owner == "" {
		return nil, errors.New("lease owner is required")
	}
	if lease <= 0 {
		lease = 2 * time.Minute
	}
	if limit <= 0 {
		limit = 20
	}
	if len(allowedAccountIDs) == 0 {
		return []service.OpenAIWindowWarmupClaim{}, nil
	}
	query := `
	WITH candidates AS MATERIALIZED (
	    SELECT id, state AS previous_state
	    FROM openai_window_warmup_jobs
	    WHERE account_id = ANY($4) AND ((
	        state IN ('pending', 'armed', 'due', 'retrying', 'uncertain', 'possibly_sent')
	        AND next_attempt_at <= NOW()
	    ) OR (
	        state = 'running' AND lease_until IS NOT NULL AND lease_until <= NOW()
	    ))
    ORDER BY next_attempt_at ASC, id ASC
    LIMIT $2
    FOR UPDATE SKIP LOCKED
	), claimed AS (
	    UPDATE openai_window_warmup_jobs AS j
	    SET state = 'running', lease_owner = $1,
	        lease_token = j.cycle_generation::text || ':' ||
	            replace(gen_random_uuid()::text, '-', '') ||
	            replace(gen_random_uuid()::text, '-', ''),
	        lease_until = NOW() + ($3 * INTERVAL '1 microsecond'),
	        updated_at = NOW()
	    FROM candidates c
	    WHERE j.id = c.id
	    RETURNING j.*, c.previous_state
	)
	SELECT ` + warmupJobSelectColumns + `, previous_state
	FROM claimed
ORDER BY next_attempt_at ASC, id ASC`
	// PostgreSQL interval multiplication accepts microseconds as a numeric
	// value and avoids converting a Go duration to a wall-clock timestamp.
	rows, err := r.db.QueryContext(ctx, query, owner, limit, lease.Microseconds(), pq.Array(allowedAccountIDs))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	claims := make([]service.OpenAIWindowWarmupClaim, 0, limit)
	for rows.Next() {
		var (
			job                                                                          service.OpenAIWindowWarmupJob
			observed, uncertainReset, uncertainAt, sent, until, lastAttempt, lastSuccess sql.NullTime
			leaseOwner, leaseToken, errorCode, lastError                                 sql.NullString
			statusCode                                                                   sql.NullInt64
			previousState                                                                string
		)
		if err := rows.Scan(
			&job.ID, &job.AccountID, &job.QuotaScope, &job.State, &job.Trigger,
			&job.CycleKey, &job.CycleGeneration, &observed, &uncertainReset, &uncertainAt,
			&job.UncertainTerminalObserved, &job.NextAttemptAt,
			&job.AttemptCount, &sent, &leaseOwner, &leaseToken, &until,
			&lastAttempt, &lastSuccess, &statusCode, &errorCode, &lastError,
			&job.CreatedAt, &job.UpdatedAt, &previousState,
		); err != nil {
			return nil, err
		}
		warmupAssignNullable(&job, observed, sent, until, lastAttempt, lastSuccess, statusCode, leaseOwner, leaseToken, lastError, errorCode)
		if uncertainReset.Valid {
			v := uncertainReset.Time.UTC()
			job.UncertainObservedResetAt = &v
		}
		if uncertainAt.Valid {
			v := uncertainAt.Time.UTC()
			job.UncertainObservedAt = &v
		}
		claims = append(claims, service.OpenAIWindowWarmupClaim{
			Job: &job, Owner: owner, LeaseToken: job.LeaseToken,
			LeaseUntil: nullableTime(until), PreviousState: previousState,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return claims, nil
}

func (r *openAIWindowWarmupRepository) QueueStats(ctx context.Context, allowedAccountIDs []int64) (service.OpenAIWindowWarmupQueueStats, error) {
	var stats service.OpenAIWindowWarmupQueueStats
	if r == nil || r.db == nil {
		return stats, errors.New("nil openai warmup repository")
	}
	if len(allowedAccountIDs) == 0 {
		return stats, nil
	}
	query := `
		WITH relevant AS MATERIALIZED (
		    SELECT next_attempt_at, observed_reset_at, state, lease_until,
		           ((state IN ('pending', 'armed', 'due', 'retrying', 'uncertain', 'possibly_sent')
		             AND next_attempt_at <= NOW())
		            OR (state = 'running' AND lease_until IS NOT NULL AND lease_until <= NOW())) AS is_due
		    FROM openai_window_warmup_jobs
		    WHERE account_id = ANY($1)
		)
		SELECT
		    COUNT(*),
		    COUNT(*) FILTER (WHERE is_due),
		    COALESCE(GREATEST(0, FLOOR(EXTRACT(EPOCH FROM NOW() -
		        MIN(next_attempt_at) FILTER (WHERE is_due)))), 0)::BIGINT,
		    COUNT(*) FILTER (WHERE state = 'running' AND lease_until > NOW()),
		    COALESCE(GREATEST(0, FLOOR(MAX(EXTRACT(EPOCH FROM NOW() - observed_reset_at))
		        FILTER (WHERE is_due AND observed_reset_at IS NOT NULL AND observed_reset_at <= NOW()))), 0)::BIGINT
		FROM relevant`
	err := r.db.QueryRowContext(ctx, query, pq.Array(allowedAccountIDs)).Scan(
		&stats.Enqueued, &stats.Due, &stats.OldestDueAgeSeconds, &stats.Inflight, &stats.ResetLagSeconds,
	)
	return stats, err
}

func (r *openAIWindowWarmupRepository) CleanupExpiredAttempts(ctx context.Context, limit int) (int64, error) {
	if r == nil || r.db == nil {
		return 0, errors.New("nil openai warmup repository")
	}
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	result, err := r.db.ExecContext(ctx, `
		DELETE FROM openai_window_warmup_attempts
		WHERE id IN (
		    SELECT id FROM openai_window_warmup_attempts
		    WHERE expires_at <= NOW()
		    ORDER BY expires_at, id
		    LIMIT $1
		)`, limit)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (r *openAIWindowWarmupRepository) ReserveGlobalSend(ctx context.Context, minInterval, inflightLease time.Duration) (string, bool, error) {
	if r == nil || r.db == nil {
		return "", false, errors.New("nil openai warmup repository")
	}
	if minInterval <= 0 {
		minInterval = 5 * time.Second
	}
	if inflightLease <= 0 {
		inflightLease = 2 * time.Minute
	}
	if _, err := r.db.ExecContext(ctx, `
		INSERT INTO openai_window_warmup_runtime (gate_key, next_send_at, updated_at)
		VALUES ('global_send', to_timestamp(0), NOW())
		ON CONFLICT (gate_key) DO NOTHING`); err != nil {
		return "", false, err
	}
	var permitToken string
	err := r.db.QueryRowContext(ctx, `
		UPDATE openai_window_warmup_runtime
		SET next_send_at = NOW() + ($1 * INTERVAL '1 microsecond'),
		    permit_token = replace(gen_random_uuid()::text, '-', ''),
		    inflight_until = NOW() + ($2 * INTERVAL '1 microsecond'),
		    updated_at = NOW()
		WHERE gate_key = 'global_send'
		  AND next_send_at <= NOW()
		  AND (inflight_until IS NULL OR inflight_until <= NOW())
		  AND NOT EXISTS (
		      SELECT 1
		      FROM (
		          SELECT COUNT(*) FILTER (WHERE outcome IN ('success', 'failed', 'retry', 'uncertain')) AS total,
		                 COUNT(*) FILTER (WHERE outcome IN ('failed', 'retry', 'uncertain')) AS bad
		          FROM openai_window_warmup_attempts
		          WHERE finished_at >= NOW() - INTERVAL '10 minutes'
		            AND (status_code IS NULL OR status_code NOT IN (400, 401, 403, 404))
		            AND lower(trim(COALESCE(error_code, ''))) NOT IN (
		                'needs_reauth', 'blocked', 'blocked_config',
		                'account_not_found', 'account_ineligible', 'policy_cycle_disabled'
		            )
		      ) recent
		      WHERE recent.total >= 10 AND recent.bad * 2 >= recent.total
		  )
		RETURNING permit_token`, minInterval.Microseconds(), inflightLease.Microseconds()).Scan(&permitToken)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return permitToken, err == nil, err
}

func (r *openAIWindowWarmupRepository) ReleaseGlobalSend(ctx context.Context, permitToken string) (bool, error) {
	if strings.TrimSpace(permitToken) == "" {
		return false, nil
	}
	return r.fencedUpdate(ctx, `
		UPDATE openai_window_warmup_runtime
		SET permit_token = NULL, inflight_until = NULL, updated_at = NOW()
		WHERE gate_key = 'global_send' AND permit_token = $1`, permitToken)
}

func (r *openAIWindowWarmupRepository) RenewLease(ctx context.Context, id int64, owner, token string, lease time.Duration) (bool, error) {
	if lease <= 0 {
		lease = 2 * time.Minute
	}
	return r.fencedUpdate(ctx, `
UPDATE openai_window_warmup_jobs
SET lease_until = NOW() + ($4 * INTERVAL '1 microsecond'), updated_at = NOW()
	WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $3
	  AND split_part(lease_token, ':', 1) = cycle_generation::text
	  AND lease_until IS NOT NULL AND lease_until > NOW()`, id, owner, token, lease.Microseconds())
}

func (r *openAIWindowWarmupRepository) MarkStarted(ctx context.Context, id int64, owner, token string, at time.Time, evidence service.OpenAIWindowWarmupStartEvidence) (bool, error) {
	return r.fencedUpdate(ctx, `
	UPDATE openai_window_warmup_jobs AS j
	SET last_attempt_at = COALESCE($4, NOW()), attempt_count = attempt_count + 1,
	    sent_at = COALESCE($4, NOW()), uncertain_observed_reset_at = NULL,
	    uncertain_observed_at = NULL, uncertain_terminal_observed = FALSE,
	    updated_at = NOW()
	WHERE j.id = $1 AND j.state = 'running' AND j.lease_owner = $2 AND j.lease_token = $3
	  AND split_part(lease_token, ':', 1) = cycle_generation::text
	  AND lease_until IS NOT NULL AND lease_until > NOW()
		  AND EXISTS (
			      SELECT 1
			      FROM accounts a
			      CROSS JOIN LATERAL (
				          SELECT MAX(candidate) AS reset_at
				          FROM (VALUES
				              (public.openai_window_warmup_parse_reset(a.extra ->> 'codex_5h_reset_at')),
				              (public.openai_window_warmup_parse_reset(a.extra ->> 'codex_global_5h_reset_at'))
				          ) AS resets(candidate)
			      ) latest
	      CROSS JOIN LATERAL (
	          SELECT lower(trim(COALESCE(
	              CASE WHEN jsonb_typeof(a.extra -> 'openai_codex_warmup_policy') = 'string'
	                  THEN NULLIF(trim(a.extra ->> 'openai_codex_warmup_policy'), '') END,
	              CASE WHEN jsonb_typeof(a.extra -> 'codex_warmup_policy') = 'string'
	                  THEN NULLIF(trim(a.extra ->> 'codex_warmup_policy'), '') END,
	              CASE WHEN jsonb_typeof(a.extra -> 'openai_window_warmup_policy') = 'string'
	                  THEN NULLIF(trim(a.extra ->> 'openai_window_warmup_policy'), '') END,
	              'off'
	          ))) AS policy
		      ) warmup
		      WHERE a.id = j.account_id
	        AND a.platform::text = 'openai'
	        AND a.type::text = 'oauth'
	        AND a.parent_account_id IS NULL
	        AND COALESCE(a.quota_dimension::text, 'global') = 'global'
	        AND a.status::text = 'active'
	        AND a.schedulable
	        AND a.deleted_at IS NULL
	        AND (a.expires_at IS NULL OR a.expires_at > NOW())
		        AND (a.temp_unschedulable_until IS NULL OR a.temp_unschedulable_until <= NOW())
			        AND warmup.policy IN ('initial_once', 'continuous')
			        AND (warmup.policy = 'continuous' OR j.cycle_key LIKE 'initial:%')
		        AND $5::boolean
		        AND $6::double precision <= 0
		        -- Only activity in the current durable window suppresses a send.
		        -- Usage before an observed reset belongs to the preceding window.
		        AND (a.last_used_at IS NULL OR NOT (
		            a.last_used_at > GREATEST(j.created_at, COALESCE(j.observed_reset_at, j.created_at))
		        ))
		        -- A still-current observed reset remains armed. A newer 0% reset may
		        -- be /wham's natural rolling reset and is allowed only when it is the
		        -- same or newer than the account snapshot seen by this final CAS.
		        AND ($7::timestamptz IS NULL OR $7 <= NOW() OR
		             j.observed_reset_at IS NULL OR $7 > j.observed_reset_at)
		        AND (latest.reset_at IS NULL OR latest.reset_at <= NOW() OR
		             ($7::timestamptz IS NOT NULL AND latest.reset_at <= $7))
			  )`, id, owner, token, nullableTimeValue(at), evidence.Authoritative, evidence.UsedPercent, nullWarmupTime(evidence.ResetAt))
}

func (r *openAIWindowWarmupRepository) MarkSuccess(ctx context.Context, id int64, owner, token string, at time.Time, resetAt *time.Time, status int, code string) (bool, error) {
	return r.fencedQuery(ctx, `
		WITH updated AS (
		    UPDATE openai_window_warmup_jobs
			    SET state = 'completed', sent_at = COALESCE(sent_at, $4),
			        last_success_at = $4, observed_reset_at = COALESCE($5, observed_reset_at),
			        uncertain_observed_reset_at = NULL, uncertain_observed_at = NULL,
			        uncertain_terminal_observed = FALSE,
			        status_code = NULLIF($6, 0), last_error_code = NULL, last_error = NULL,
		        lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = NOW()
		    WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $3
		      AND split_part(lease_token, ':', 1) = cycle_generation::text
		      AND lease_until IS NOT NULL AND lease_until > NOW()
		    RETURNING id, attempt_count, last_attempt_at
		), attempt AS (
		    INSERT INTO openai_window_warmup_attempts
		        (job_id, attempt_no, outcome, status_code, error_code, observed_reset_at, started_at, finished_at)
			    SELECT id, attempt_count, 'success', NULLIF($6, 0), NULLIF($7, ''), $5,
			           COALESCE(last_attempt_at, $4), $4
			    FROM updated WHERE attempt_count > 0
			    ON CONFLICT (job_id, attempt_no) DO UPDATE
			    SET outcome = EXCLUDED.outcome,
			        status_code = EXCLUDED.status_code,
			        error_code = EXCLUDED.error_code,
			        observed_reset_at = EXCLUDED.observed_reset_at,
			        finished_at = EXCLUDED.finished_at
			    WHERE openai_window_warmup_attempts.outcome IN ('started', 'uncertain')
		)
		SELECT EXISTS(SELECT 1 FROM updated)`, id, owner, token,
		nullableTimeValue(at), nullWarmupTime(resetAt), status, code)
}

func (r *openAIWindowWarmupRepository) MarkSuppressed(ctx context.Context, id int64, owner, token string, at time.Time, resetAt *time.Time, code string) (bool, error) {
	return r.fencedQuery(ctx, `
		WITH updated AS (
		    UPDATE openai_window_warmup_jobs
			    SET state = 'completed', observed_reset_at = COALESCE($4, observed_reset_at),
			        uncertain_observed_reset_at = NULL, uncertain_observed_at = NULL,
			        uncertain_terminal_observed = FALSE,
			        status_code = NULL, last_error_code = NULL, last_error = NULL,
		        lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = NOW()
		    WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $3
		      AND split_part(lease_token, ':', 1) = cycle_generation::text
		      AND lease_until IS NOT NULL AND lease_until > NOW()
		    RETURNING id, attempt_count, last_attempt_at
		), attempt AS (
		    INSERT INTO openai_window_warmup_attempts
		        (job_id, attempt_no, outcome, error_code, observed_reset_at, started_at, finished_at)
		    SELECT id, attempt_count, 'suppressed', NULLIF($5, ''), $4,
		           COALESCE(last_attempt_at, $6), $6
		    FROM updated WHERE attempt_count > 0
		    ON CONFLICT (job_id, attempt_no) DO UPDATE
		    SET outcome = EXCLUDED.outcome,
		        error_code = EXCLUDED.error_code,
		        observed_reset_at = EXCLUDED.observed_reset_at,
		        finished_at = EXCLUDED.finished_at
		    WHERE openai_window_warmup_attempts.outcome IN ('started', 'uncertain')
		)
		SELECT EXISTS(SELECT 1 FROM updated)`, id, owner, token, nullWarmupTime(resetAt), code, nullableTimeValue(at))
}

func (r *openAIWindowWarmupRepository) MarkRetry(ctx context.Context, id int64, owner, token string, at, next time.Time, status int, code, message string) (bool, error) {
	return r.markNonterminal(ctx, id, owner, token, service.OpenAIWindowWarmupStateRetrying, at, next, status, code, message, nil, service.OpenAIWindowWarmupUncertainEvidence{})
}

// MarkObservationFailure records a failed authoritative usage check without
// claiming that a synthetic request was sent. It still consumes one durable
// per-cycle attempt and is fenced by the current lease owner/token.
func (r *openAIWindowWarmupRepository) MarkObservationFailure(ctx context.Context, id int64, owner, token string, at, next time.Time, state string, status int, code, message string) (bool, error) {
	state = normalizeWarmupState(state)
	switch state {
	case service.OpenAIWindowWarmupStateRetrying, service.OpenAIWindowWarmupStateUncertain,
		service.OpenAIWindowWarmupStateBlocked, service.OpenAIWindowWarmupStateBlockedConfig:
	default:
		state = service.OpenAIWindowWarmupStateRetrying
	}
	return r.fencedQuery(ctx, `
		WITH updated AS (
		    UPDATE openai_window_warmup_jobs
		    SET state = $4, next_attempt_at = $5, attempt_count = attempt_count + 1,
		        last_attempt_at = $6, status_code = NULLIF($7, 0),
		        last_error_code = NULLIF($8, ''), last_error = NULLIF($9, ''),
		        lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = NOW()
		    WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $3
		      AND split_part(lease_token, ':', 1) = cycle_generation::text
		      AND lease_until IS NOT NULL AND lease_until > NOW()
		    RETURNING id, attempt_count, last_attempt_at
		), attempt AS (
		    INSERT INTO openai_window_warmup_attempts
		        (job_id, attempt_no, outcome, status_code, error_code, started_at, finished_at)
		    SELECT id, attempt_count, $10, NULLIF($7, 0), NULLIF($8, ''), last_attempt_at, $6
		    FROM updated
		    ON CONFLICT (job_id, attempt_no) DO NOTHING
		)
		SELECT EXISTS(SELECT 1 FROM updated)`, id, owner, token, state, next.UTC(),
		nullableTimeValue(at), status, code, message, normalizeAttemptOutcome(state))
}

func (r *openAIWindowWarmupRepository) MarkRateLimited(ctx context.Context, id int64, owner, token string, at, next time.Time, resetAt *time.Time, status int, code string) (bool, error) {
	return r.markNonterminal(ctx, id, owner, token, service.OpenAIWindowWarmupStateArmed, at, next, status, code, "rate limited until authoritative reset", resetAt, service.OpenAIWindowWarmupUncertainEvidence{})
}

func (r *openAIWindowWarmupRepository) MarkUncertain(ctx context.Context, id int64, owner, token string, at, next time.Time, status int, code, message string, evidence service.OpenAIWindowWarmupUncertainEvidence) (bool, error) {
	return r.markNonterminal(ctx, id, owner, token, service.OpenAIWindowWarmupStateUncertain, at, next, status, code, message, nil, evidence)
}

func (r *openAIWindowWarmupRepository) MarkBlocked(ctx context.Context, id int64, owner, token string, at time.Time, status int, code, message string) (bool, error) {
	return r.fencedQuery(ctx, `
		WITH updated AS (
		    UPDATE openai_window_warmup_jobs
			    SET state = CASE WHEN $4 IN (400, 404) OR $5 = 'blocked_config' THEN 'blocked_config' ELSE 'blocked' END,
			        status_code = NULLIF($4, 0), last_error_code = NULLIF($5, ''),
			        last_error = NULLIF($6, ''), last_attempt_at = COALESCE(last_attempt_at, $7),
			        uncertain_observed_reset_at = NULL, uncertain_observed_at = NULL,
			        uncertain_terminal_observed = FALSE,
			        lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = NOW()
		    WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $3
		      AND split_part(lease_token, ':', 1) = cycle_generation::text
		      AND lease_until IS NOT NULL AND lease_until > NOW()
		    RETURNING id, attempt_count, last_attempt_at
		), attempt AS (
		    INSERT INTO openai_window_warmup_attempts
		        (job_id, attempt_no, outcome, status_code, error_code, started_at, finished_at)
		    SELECT id, attempt_count, 'failed', NULLIF($4, 0), NULLIF($5, ''),
		           COALESCE(last_attempt_at, $7), $7
		    FROM updated WHERE attempt_count > 0
		    ON CONFLICT (job_id, attempt_no) DO NOTHING
		)
		SELECT EXISTS(SELECT 1 FROM updated)`, id, owner, token, status, code, message, nullableTimeValue(at))
}

func (r *openAIWindowWarmupRepository) MarkPaused(ctx context.Context, id int64, owner, token string, at time.Time, reason string) (bool, error) {
	return r.fencedQuery(ctx, `
		WITH updated AS (
		    UPDATE openai_window_warmup_jobs
			    SET state = 'paused', last_error_code = NULLIF($4, ''), last_error = NULL,
			        uncertain_observed_reset_at = NULL, uncertain_observed_at = NULL,
			        uncertain_terminal_observed = FALSE,
			        lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = NOW()
		    WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $3
		      AND split_part(lease_token, ':', 1) = cycle_generation::text
		      AND lease_until IS NOT NULL AND lease_until > NOW()
		    RETURNING id, attempt_count, last_attempt_at
		), attempt AS (
		    INSERT INTO openai_window_warmup_attempts
		        (job_id, attempt_no, outcome, error_code, started_at, finished_at)
		    SELECT id, attempt_count, 'suppressed', NULLIF($4, ''), COALESCE(last_attempt_at, $5), $5
		    FROM updated WHERE attempt_count > 0
		    ON CONFLICT (job_id, attempt_no) DO NOTHING
		)
		SELECT EXISTS(SELECT 1 FROM updated)`, id, owner, token, reason, nullableTimeValue(at))
}

func (r *openAIWindowWarmupRepository) Reschedule(ctx context.Context, id int64, owner, token string, next time.Time, state string, resetAt *time.Time) (bool, error) {
	state = normalizeWarmupState(state)
	return r.fencedUpdate(ctx, `
UPDATE openai_window_warmup_jobs
SET state = $4::varchar, next_attempt_at = $5, observed_reset_at = COALESCE($6, observed_reset_at),
	uncertain_observed_reset_at = CASE WHEN $4::varchar = 'uncertain' THEN uncertain_observed_reset_at ELSE NULL END,
	uncertain_observed_at = CASE WHEN $4::varchar = 'uncertain' THEN uncertain_observed_at ELSE NULL END,
	uncertain_terminal_observed = CASE WHEN $4::varchar = 'uncertain' THEN uncertain_terminal_observed ELSE FALSE END,
    lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = NOW()
	WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $3
	  AND split_part(lease_token, ':', 1) = cycle_generation::text
	  AND lease_until IS NOT NULL AND lease_until > NOW()`, id, owner, token, state, next.UTC(), nullWarmupTime(resetAt))
}

func (r *openAIWindowWarmupRepository) markNonterminal(ctx context.Context, id int64, owner, token, state string, at, next time.Time, status int, code, message string, resetAt *time.Time, uncertain service.OpenAIWindowWarmupUncertainEvidence) (bool, error) {
	return r.fencedQuery(ctx, `
			WITH updated AS (
			    UPDATE openai_window_warmup_jobs
				    SET state = $4::varchar, next_attempt_at = $5, status_code = NULLIF($6, 0),
			        last_error_code = NULLIF($7, ''), last_error = NULLIF($8, ''),
			        observed_reset_at = COALESCE($9, observed_reset_at),
			        uncertain_observed_reset_at = CASE
				            WHEN $4::varchar = 'uncertain' AND $10::boolean THEN $11::timestamptz
				            WHEN $4::varchar = 'uncertain' THEN uncertain_observed_reset_at
			            ELSE NULL
			        END,
			        uncertain_observed_at = CASE
				            WHEN $4::varchar = 'uncertain' AND $10::boolean THEN NOW()
				            WHEN $4::varchar = 'uncertain' THEN uncertain_observed_at
			            ELSE NULL
			        END,
			        uncertain_terminal_observed = CASE
				            WHEN $4::varchar = 'uncertain' THEN uncertain_terminal_observed OR $12::boolean
			            ELSE FALSE
			        END,
			        lease_owner = NULL, lease_token = NULL, lease_until = NULL, updated_at = NOW()
		    WHERE id = $1 AND state = 'running' AND lease_owner = $2 AND lease_token = $3
		      AND split_part(lease_token, ':', 1) = cycle_generation::text
		      AND lease_until IS NOT NULL AND lease_until > NOW()
		    RETURNING id, attempt_count, last_attempt_at
			), attempt AS (
			    INSERT INTO openai_window_warmup_attempts
			        (job_id, attempt_no, outcome, status_code, error_code, observed_reset_at, started_at, finished_at)
			    SELECT id, attempt_count, $14, NULLIF($6, 0), NULLIF($7, ''), COALESCE($9, $11),
			           COALESCE(last_attempt_at, $13), $13
			    FROM updated WHERE attempt_count > 0
			    ON CONFLICT (job_id, attempt_no) DO NOTHING
			)
			SELECT EXISTS(SELECT 1 FROM updated)`, id, owner, token, state, next.UTC(), status, code, message,
		nullWarmupTime(resetAt), uncertain.Authoritative, nullWarmupTime(uncertain.ResetAt),
		uncertain.Terminal, nullableTimeValue(at), normalizeAttemptOutcome(state))
}

func (r *openAIWindowWarmupRepository) GetByID(ctx context.Context, id int64) (*service.OpenAIWindowWarmupJob, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("nil openai warmup repository")
	}
	return scanWarmupJob(r.db.QueryRowContext(ctx, `SELECT `+warmupJobSelectColumns+` FROM openai_window_warmup_jobs WHERE id = $1`, id))
}

func (r *openAIWindowWarmupRepository) GetCurrent(ctx context.Context, accountID int64, scope string) (*service.OpenAIWindowWarmupJob, error) {
	if scope == "" {
		scope = service.OpenAIWindowWarmupQuotaScopeGlobal
	}
	return scanWarmupJob(r.db.QueryRowContext(ctx, `
SELECT `+warmupJobSelectColumns+` FROM openai_window_warmup_jobs
WHERE account_id = $1 AND quota_scope = $2
ORDER BY id DESC LIMIT 1`, accountID, scope))
}

func (r *openAIWindowWarmupRepository) GetCurrentForAccounts(ctx context.Context, accountIDs []int64, scope string) (map[int64]*service.OpenAIWindowWarmupJob, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("nil openai warmup repository")
	}
	jobs := make(map[int64]*service.OpenAIWindowWarmupJob, len(accountIDs))
	if len(accountIDs) == 0 {
		return jobs, nil
	}
	if scope == "" {
		scope = service.OpenAIWindowWarmupQuotaScopeGlobal
	}
	rows, err := r.db.QueryContext(ctx, `
	SELECT DISTINCT ON (account_id) `+warmupJobSelectColumns+`
	FROM openai_window_warmup_jobs
	WHERE account_id = ANY($1) AND quota_scope = $2
	ORDER BY account_id, id DESC`, pq.Array(accountIDs), scope)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		job, scanErr := scanWarmupJobRows(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		jobs[job.AccountID] = job
	}
	return jobs, rows.Err()
}

func (r *openAIWindowWarmupRepository) List(ctx context.Context, options service.OpenAIWindowWarmupListOptions) ([]*service.OpenAIWindowWarmupJob, error) {
	if r == nil || r.db == nil {
		return nil, errors.New("nil openai warmup repository")
	}
	limit := options.Limit
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	offset := options.Offset
	if offset < 0 {
		offset = 0
	}
	args := make([]any, 0, 4)
	where := []string{"1=1"}
	if options.AccountID > 0 {
		args = append(args, options.AccountID)
		where = append(where, fmt.Sprintf("account_id = $%d", len(args)))
	}
	if len(options.States) > 0 {
		args = append(args, pq.Array(options.States))
		where = append(where, fmt.Sprintf("state = ANY($%d)", len(args)))
	}
	args = append(args, limit, offset)
	query := `SELECT ` + warmupJobSelectColumns + ` FROM openai_window_warmup_jobs WHERE ` + strings.Join(where, " AND ") +
		fmt.Sprintf(" ORDER BY next_attempt_at ASC, id ASC LIMIT $%d OFFSET $%d", len(args)-1, len(args))
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]*service.OpenAIWindowWarmupJob, 0, limit)
	for rows.Next() {
		job, err := scanWarmupJobRows(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (r *openAIWindowWarmupRepository) UnblockAccount(ctx context.Context, accountID int64, next time.Time, resetAt *time.Time) (*service.OpenAIWindowWarmupJob, bool, error) {
	if r == nil || r.db == nil {
		return nil, false, errors.New("nil openai warmup repository")
	}
	query := `
	WITH target AS MATERIALIZED (
	    SELECT id
	    FROM openai_window_warmup_jobs
	    WHERE account_id = $1
	    ORDER BY id DESC
	    LIMIT 1
	    FOR UPDATE
	), updated AS (
	    UPDATE openai_window_warmup_jobs AS j
	    SET state = CASE WHEN $2 > NOW() THEN 'armed' ELSE 'pending' END,
	        next_attempt_at = $2, observed_reset_at = COALESCE($3, observed_reset_at),
	        lease_owner = NULL, lease_token = NULL, lease_until = NULL,
	        last_error_code = NULL, last_error = NULL, updated_at = NOW()
	    FROM target
	    WHERE j.id = target.id
	      AND j.state IN ('paused', 'blocked', 'blocked_config')
	    RETURNING j.*
	)
	SELECT ` + warmupJobSelectColumns + ` FROM updated`
	job, err := scanWarmupJob(r.db.QueryRowContext(ctx, query, accountID, next.UTC(), nullWarmupTime(resetAt)))
	if err == nil {
		return job, true, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, false, err
	}
	job, err = r.GetCurrent(ctx, accountID, service.OpenAIWindowWarmupQuotaScopeGlobal)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	return job, false, err
}

func (r *openAIWindowWarmupRepository) fencedUpdate(ctx context.Context, query string, args ...any) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("nil openai warmup repository")
	}
	result, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n > 0, err
}

func (r *openAIWindowWarmupRepository) fencedQuery(ctx context.Context, query string, args ...any) (bool, error) {
	if r == nil || r.db == nil {
		return false, errors.New("nil openai warmup repository")
	}
	var updated bool
	err := r.db.QueryRowContext(ctx, query, args...).Scan(&updated)
	return updated, err
}

type warmupRowScanner interface{ Scan(...any) error }

func scanWarmupJob(row warmupRowScanner) (*service.OpenAIWindowWarmupJob, error) {
	job := &service.OpenAIWindowWarmupJob{}
	var observed, uncertainReset, uncertainAt, sent, until, lastAttempt, lastSuccess sql.NullTime
	var owner, token, errorCode, lastError sql.NullString
	var statusCode sql.NullInt64
	err := row.Scan(
		&job.ID, &job.AccountID, &job.QuotaScope, &job.State, &job.Trigger,
		&job.CycleKey, &job.CycleGeneration, &observed, &uncertainReset, &uncertainAt,
		&job.UncertainTerminalObserved, &job.NextAttemptAt,
		&job.AttemptCount, &sent, &owner, &token, &until, &lastAttempt,
		&lastSuccess, &statusCode, &errorCode, &lastError, &job.CreatedAt, &job.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	warmupAssignNullable(job, observed, sent, until, lastAttempt, lastSuccess, statusCode, owner, token, lastError, errorCode)
	if uncertainReset.Valid {
		v := uncertainReset.Time.UTC()
		job.UncertainObservedResetAt = &v
	}
	if uncertainAt.Valid {
		v := uncertainAt.Time.UTC()
		job.UncertainObservedAt = &v
	}
	return job, nil
}

func scanWarmupJobRows(rows interface{ Scan(...any) error }) (*service.OpenAIWindowWarmupJob, error) {
	return scanWarmupJob(rows)
}

func warmupAssignNullable(job *service.OpenAIWindowWarmupJob, observed, sent, until, lastAttempt, lastSuccess sql.NullTime, statusCode sql.NullInt64, owner, token, lastError, errorCode sql.NullString) {
	if observed.Valid {
		v := observed.Time.UTC()
		job.ObservedResetAt = &v
	}
	if sent.Valid {
		v := sent.Time.UTC()
		job.SentAt = &v
	}
	if until.Valid {
		v := until.Time.UTC()
		job.LeaseUntil = &v
	}
	if lastAttempt.Valid {
		v := lastAttempt.Time.UTC()
		job.LastAttemptAt = &v
	}
	if lastSuccess.Valid {
		v := lastSuccess.Time.UTC()
		job.LastSuccessAt = &v
	}
	if statusCode.Valid {
		v := int(statusCode.Int64)
		job.StatusCode = &v
	}
	if owner.Valid {
		job.LeaseOwner = owner.String
	}
	if token.Valid {
		job.LeaseToken = token.String
	}
	if errorCode.Valid {
		job.LastErrorCode = errorCode.String
	}
	if lastError.Valid {
		job.LastError = lastError.String
	}
}

func nullableTime(v sql.NullTime) time.Time {
	if v.Valid {
		return v.Time.UTC()
	}
	return time.Time{}
}

func nullWarmupTime(v *time.Time) any {
	if v == nil {
		return nil
	}
	return v.UTC()
}

func nullableTimeValue(v time.Time) any {
	if v.IsZero() {
		return nil
	}
	return v.UTC()
}

func normalizeWarmupState(state string) string {
	switch strings.TrimSpace(state) {
	case service.OpenAIWindowWarmupStatePending, service.OpenAIWindowWarmupStateArmed,
		service.OpenAIWindowWarmupStateDue, service.OpenAIWindowWarmupStateRetrying,
		service.OpenAIWindowWarmupStateUncertain, service.OpenAIWindowWarmupStatePossiblySent,
		service.OpenAIWindowWarmupStatePaused, service.OpenAIWindowWarmupStateBlocked,
		service.OpenAIWindowWarmupStateBlockedConfig, service.OpenAIWindowWarmupStateCompleted:
		return strings.TrimSpace(state)
	default:
		return service.OpenAIWindowWarmupStateRetrying
	}
}

func normalizeAttemptOutcome(outcome string) string {
	switch outcome {
	case service.OpenAIWindowWarmupStateCompleted:
		return "success"
	case service.OpenAIWindowWarmupStateRetrying:
		return "retry"
	case service.OpenAIWindowWarmupStateArmed:
		return "retry"
	case service.OpenAIWindowWarmupStateUncertain, service.OpenAIWindowWarmupStatePossiblySent:
		return "uncertain"
	case service.OpenAIWindowWarmupStatePaused:
		return "suppressed"
	case service.OpenAIWindowWarmupStateBlocked, service.OpenAIWindowWarmupStateBlockedConfig:
		return "failed"
	default:
		return "started"
	}
}
