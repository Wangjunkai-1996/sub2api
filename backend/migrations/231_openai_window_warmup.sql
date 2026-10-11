-- Durable OpenAI Codex five-hour window warmup jobs.
--
-- This migration is intentionally additive and idempotent.  Runtime state is
-- kept out of accounts.extra so account snapshots remain suitable for routing
-- and old application versions can continue to read accounts safely.
CREATE TABLE IF NOT EXISTS openai_window_warmup_jobs (
    id                  BIGSERIAL PRIMARY KEY,
    account_id          BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    quota_scope         VARCHAR(32) NOT NULL DEFAULT 'global',
    state               VARCHAR(32) NOT NULL DEFAULT 'pending',
    trigger             VARCHAR(32) NOT NULL DEFAULT 'import',
    cycle_key           VARCHAR(255) NOT NULL,
    cycle_generation    BIGINT NOT NULL DEFAULT 0,
    observed_reset_at   TIMESTAMPTZ,
    next_attempt_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    sent_at             TIMESTAMPTZ,
    lease_owner         VARCHAR(128),
    lease_token         VARCHAR(128),
    lease_until         TIMESTAMPTZ,
    last_attempt_at     TIMESTAMPTZ,
    last_success_at     TIMESTAMPTZ,
    status_code         INTEGER,
    last_error_code     VARCHAR(96),
    last_error          TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT openai_window_warmup_jobs_scope_check
        CHECK (quota_scope IN ('global')),
    CONSTRAINT openai_window_warmup_jobs_state_check
        CHECK (state IN (
            'pending', 'armed', 'due', 'running', 'retrying', 'uncertain',
            'possibly_sent', 'paused', 'blocked', 'blocked_config', 'failed', 'completed'
        )),
    CONSTRAINT openai_window_warmup_jobs_attempt_count_check
        CHECK (attempt_count >= 0),
    CONSTRAINT openai_window_warmup_jobs_cycle_unique
        UNIQUE (account_id, quota_scope, cycle_key)
);

CREATE INDEX IF NOT EXISTS idx_openai_window_warmup_jobs_due
    ON openai_window_warmup_jobs (next_attempt_at, id)
    WHERE state IN ('pending', 'armed', 'due', 'retrying', 'uncertain', 'possibly_sent');

CREATE INDEX IF NOT EXISTS idx_openai_window_warmup_jobs_lease
    ON openai_window_warmup_jobs (lease_until, id)
    WHERE state = 'running';

-- Replace early development variants that ordered by updated_at or used the
-- temporary _latest name. GetCurrent and GetCurrentForAccounts order by id.
DROP INDEX IF EXISTS idx_openai_window_warmup_jobs_account;
DROP INDEX IF EXISTS idx_openai_window_warmup_jobs_account_latest;
CREATE INDEX IF NOT EXISTS idx_openai_window_warmup_jobs_account
    ON openai_window_warmup_jobs (account_id, quota_scope, id DESC);

-- Early development versions of this migration did not enforce the active-job
-- invariant, and some installations may also have a same-named non-unique or
-- differently-predicated index. Preserve the row with the strongest evidence
-- that a send may already be in flight, then retain the newest remaining row.
DROP INDEX IF EXISTS idx_openai_window_warmup_jobs_one_active;
WITH ranked_active_jobs AS (
    SELECT id,
           row_number() OVER (
               PARTITION BY account_id, quota_scope
               ORDER BY CASE state
                   WHEN 'possibly_sent' THEN 0
                   WHEN 'uncertain' THEN 1
                   WHEN 'running' THEN 2
                   ELSE 3
               END,
               id DESC
           ) AS active_rank
    FROM openai_window_warmup_jobs
    WHERE state IN ('pending', 'armed', 'due', 'running', 'retrying', 'uncertain', 'possibly_sent')
)
UPDATE openai_window_warmup_jobs AS job
SET state = 'blocked',
    lease_owner = NULL,
    lease_token = NULL,
    lease_until = NULL,
    last_error_code = 'migration_duplicate_active_job',
    last_error = 'Superseded duplicate active warmup job during migration 231',
    updated_at = NOW()
FROM ranked_active_jobs AS ranked
WHERE job.id = ranked.id
  AND ranked.active_rank > 1;

CREATE UNIQUE INDEX idx_openai_window_warmup_jobs_one_active
    ON openai_window_warmup_jobs (account_id, quota_scope)
    WHERE state IN ('pending', 'armed', 'due', 'running', 'retrying', 'uncertain', 'possibly_sent');

-- A single database-clock gate bounds aggregate send rate across all app
-- instances. Process-local limiters remain an optimization only.
CREATE TABLE IF NOT EXISTS openai_window_warmup_runtime (
    gate_key       VARCHAR(32) PRIMARY KEY,
    next_send_at   TIMESTAMPTZ NOT NULL DEFAULT to_timestamp(0),
    permit_token   VARCHAR(64),
    inflight_until TIMESTAMPTZ,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT openai_window_warmup_runtime_gate_check CHECK (gate_key IN ('global_send'))
);

ALTER TABLE openai_window_warmup_runtime
    ADD COLUMN IF NOT EXISTS permit_token VARCHAR(64),
    ADD COLUMN IF NOT EXISTS inflight_until TIMESTAMPTZ;

INSERT INTO openai_window_warmup_runtime (gate_key, next_send_at)
VALUES ('global_send', to_timestamp(0))
ON CONFLICT (gate_key) DO NOTHING;

-- Keep a bounded, token-free audit trail for attempts.  Response bodies,
-- prompts, credential material and generated text are deliberately absent.
CREATE TABLE IF NOT EXISTS openai_window_warmup_attempts (
    id              BIGSERIAL PRIMARY KEY,
    job_id          BIGINT NOT NULL REFERENCES openai_window_warmup_jobs(id) ON DELETE CASCADE,
    attempt_no      INTEGER NOT NULL,
    outcome         VARCHAR(32) NOT NULL,
    status_code     INTEGER,
    error_code      VARCHAR(96),
    observed_reset_at TIMESTAMPTZ,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT openai_window_warmup_attempts_attempt_no_check CHECK (attempt_no > 0),
    CONSTRAINT openai_window_warmup_attempts_outcome_check CHECK (
        outcome IN ('started', 'success', 'failed', 'retry', 'uncertain', 'suppressed')
    ),
    CONSTRAINT openai_window_warmup_attempts_unique UNIQUE (job_id, attempt_no)
);

CREATE INDEX IF NOT EXISTS idx_openai_window_warmup_attempts_job
    ON openai_window_warmup_attempts (job_id, attempt_no DESC);

-- Keep attempt evidence for a finite window without retaining response data.
ALTER TABLE openai_window_warmup_attempts
    ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '90 days');

CREATE INDEX IF NOT EXISTS idx_openai_window_warmup_attempts_expiry
    ON openai_window_warmup_attempts (expires_at, id);

CREATE INDEX IF NOT EXISTS idx_openai_window_warmup_attempts_finished_outcome
    ON openai_window_warmup_attempts (finished_at DESC, outcome);

CREATE OR REPLACE FUNCTION public.openai_window_warmup_parse_reset(value TEXT)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    parsed TIMESTAMPTZ;
BEGIN
    IF value IS NULL OR trim(value) = '' THEN
        RETURN NULL;
    END IF;
	IF trim(value) !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,9})?(Z|[+-][0-9]{2}:[0-9]{2})$' THEN
		RETURN NULL;
	END IF;
	parsed := value::TIMESTAMPTZ;
	IF NOT isfinite(parsed) THEN
		RETURN NULL;
	END IF;
	RETURN parsed;
EXCEPTION WHEN OTHERS THEN
    RETURN NULL;
END;
$$;

-- Account import/update and the initial durable job commit in the same
-- PostgreSQL transaction. Handler scheduling remains useful for an immediate
-- response projection, while this trigger is the reliability boundary.
CREATE OR REPLACE FUNCTION public.openai_window_warmup_enqueue_initial()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    policy_value TEXT;
    reset_at_value TIMESTAMPTZ;
    generation_value BIGINT;
    cycle_value TEXT;
    next_value TIMESTAMPTZ;
    state_value TEXT;
    jitter_seconds INTEGER;
    grace_seconds INTEGER := 90;
BEGIN
    -- Match OpenAIWindowWarmupPolicyForAccount: blank canonical values do
    -- not mask a usable backwards-compatible alias, and non-string JSON
    -- values are ignored rather than coerced through ->>.
    policy_value := lower(trim(COALESCE(
        CASE WHEN jsonb_typeof(NEW.extra -> 'openai_codex_warmup_policy') = 'string'
            THEN NULLIF(trim(NEW.extra ->> 'openai_codex_warmup_policy'), '') END,
        CASE WHEN jsonb_typeof(NEW.extra -> 'codex_warmup_policy') = 'string'
            THEN NULLIF(trim(NEW.extra ->> 'codex_warmup_policy'), '') END,
        CASE WHEN jsonb_typeof(NEW.extra -> 'openai_window_warmup_policy') = 'string'
            THEN NULLIF(trim(NEW.extra ->> 'openai_window_warmup_policy'), '') END,
        'off'
    )));

    IF NEW.platform::text <> 'openai'
       OR NEW.type::text <> 'oauth'
       OR NEW.parent_account_id IS NOT NULL
       OR COALESCE(NEW.quota_dimension::text, 'global') <> 'global'
       OR NEW.status::text <> 'active'
       OR NOT NEW.schedulable
       OR (NEW.temp_unschedulable_until IS NOT NULL AND NEW.temp_unschedulable_until > NOW())
       OR NEW.deleted_at IS NOT NULL
       OR (NEW.expires_at IS NOT NULL AND NEW.expires_at <= NOW())
       OR policy_value NOT IN ('initial_once', 'continuous') THEN
        RETURN NEW;
    END IF;

	reset_at_value := COALESCE(
		public.openai_window_warmup_parse_reset(NEW.extra ->> 'codex_5h_reset_at'),
		public.openai_window_warmup_parse_reset(NEW.extra ->> 'codex_global_5h_reset_at')
	);

    -- TIMESTAMPTZ is stored at microsecond precision. Convert the stored
    -- microseconds to nanoseconds exactly so this matches the Go generation.
    generation_value := (extract(epoch FROM NEW.created_at) * 1000000)::BIGINT * 1000;
    cycle_value := 'initial:' || generation_value::TEXT;
    -- Match the runtime's bounded reset grace while keeping the trigger
    -- reliable when the setting is absent or malformed.
    IF to_regclass('public.settings') IS NOT NULL THEN
        BEGIN
            EXECUTE $query$
                SELECT LEAST(900, GREATEST(0, trim(value)::INTEGER))
                FROM public.settings
                WHERE key = 'openai_window_warmup_reset_grace_seconds'
            $query$ INTO grace_seconds;
            IF grace_seconds IS NULL THEN
                grace_seconds := 90;
            END IF;
        EXCEPTION WHEN invalid_text_representation OR numeric_value_out_of_range THEN
            grace_seconds := 90;
        END;
    END IF;
    -- Mask the signed hash rather than abs(INT_MIN), which can overflow and
    -- must never abort the surrounding account import transaction.
    jitter_seconds := mod((hashtextextended(cycle_value, 0) & 2147483647::BIGINT), 30)::INTEGER;
    IF reset_at_value IS NOT NULL AND reset_at_value > NOW() THEN
        next_value := reset_at_value + (grace_seconds * INTERVAL '1 second') + (jitter_seconds * INTERVAL '1 second');
        state_value := 'armed';
    ELSE
        next_value := NOW() + (jitter_seconds * INTERVAL '1 second');
        state_value := 'pending';
    END IF;

    UPDATE openai_window_warmup_jobs
    SET observed_reset_at = reset_at_value,
        next_attempt_at = next_value,
        state = state_value,
        updated_at = NOW()
    WHERE account_id = NEW.id
      AND quota_scope = 'global'
      AND cycle_key = cycle_value
      AND attempt_count = 0
      AND sent_at IS NULL
      AND state IN ('pending', 'armed', 'due', 'retrying');

    IF NOT FOUND THEN
        INSERT INTO openai_window_warmup_jobs
        (account_id, quota_scope, state, trigger, cycle_key, cycle_generation,
         observed_reset_at, next_attempt_at, attempt_count, created_at, updated_at)
        VALUES
            (NEW.id, 'global', state_value, 'import', cycle_value, generation_value,
             reset_at_value, next_value, 0, NOW(), NOW())
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_openai_window_warmup_enqueue_initial ON accounts;
CREATE TRIGGER trg_openai_window_warmup_enqueue_initial
    AFTER INSERT OR UPDATE OF extra, credentials, status, schedulable, expires_at,
        auto_pause_on_expired, parent_account_id, quota_dimension, temp_unschedulable_until
    ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION public.openai_window_warmup_enqueue_initial();

-- Account deletion in this application is normally a soft delete.  Remove
-- durable warmup state immediately when an account is soft-deleted as well;
-- hard deletes are covered by the FK above.
CREATE OR REPLACE FUNCTION public.openai_window_warmup_cleanup_deleted_account()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.deleted_at IS NOT NULL AND OLD.deleted_at IS NULL THEN
        DELETE FROM openai_window_warmup_jobs WHERE account_id = NEW.id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_openai_window_warmup_cleanup_deleted_account ON accounts;
CREATE TRIGGER trg_openai_window_warmup_cleanup_deleted_account
    AFTER UPDATE OF deleted_at ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION public.openai_window_warmup_cleanup_deleted_account();
