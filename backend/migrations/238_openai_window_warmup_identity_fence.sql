-- Fence durable warmup work to the logical OpenAI credential identity that
-- created it. Token refreshes and Agent Identity task rotation intentionally do
-- not advance this generation.
SET LOCAL lock_timeout = '3s';

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS openai_warmup_identity_generation BIGINT NOT NULL DEFAULT 1;

-- The jobs column is also the first-run marker. Ent may already have added the
-- account column before SQL migrations run, so use the jobs column to ensure
-- the legacy generation backfill happens exactly once on replay-safe installs.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'openai_window_warmup_jobs'
          AND column_name = 'identity_generation'
    ) THEN
        UPDATE accounts
        SET openai_warmup_identity_generation =
            (extract(epoch FROM created_at) * 1000000)::BIGINT * 1000
        WHERE openai_warmup_identity_generation = 1;
    END IF;
END;
$$;

ALTER TABLE openai_window_warmup_jobs
    ADD COLUMN IF NOT EXISTS identity_generation BIGINT;

UPDATE openai_window_warmup_jobs AS job
SET identity_generation = account.openai_warmup_identity_generation
FROM accounts AS account
WHERE account.id = job.account_id
  AND job.identity_generation IS NULL;

ALTER TABLE openai_window_warmup_jobs
    ALTER COLUMN identity_generation SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'openai_window_warmup_jobs_identity_generation_check'
          AND conrelid = 'openai_window_warmup_jobs'::regclass
    ) THEN
        ALTER TABLE openai_window_warmup_jobs
            ADD CONSTRAINT openai_window_warmup_jobs_identity_generation_check
            CHECK (identity_generation > 0);
    END IF;
END;
$$;

COMMENT ON COLUMN accounts.openai_warmup_identity_generation IS
    'Monotonic logical OpenAI warmup identity generation';
COMMENT ON COLUMN openai_window_warmup_jobs.identity_generation IS
    'Account logical identity generation authorized to execute this warmup job';

CREATE OR REPLACE FUNCTION public.openai_window_warmup_identity_key(
    platform_value TEXT,
    type_value TEXT,
    credentials_value JSONB
)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    auth_mode_value TEXT;
    legacy_auth_mode_value TEXT;
    account_id_value TEXT;
    user_id_value TEXT;
    runtime_id_value TEXT;
    access_token_value TEXT;
BEGIN
    IF lower(trim(COALESCE(platform_value, ''))) <> 'openai'
       OR lower(trim(COALESCE(type_value, ''))) <> 'oauth' THEN
        RETURN NULL;
    END IF;

    credentials_value := COALESCE(credentials_value, '{}'::jsonb);
    auth_mode_value := lower(trim(COALESCE(credentials_value ->> 'auth_mode', '')));
    legacy_auth_mode_value := lower(trim(COALESCE(credentials_value ->> 'openai_auth_mode', '')));

    IF auth_mode_value = 'agentidentity' THEN
        runtime_id_value := trim(COALESCE(credentials_value ->> 'agent_runtime_id', ''));
        IF runtime_id_value = '' THEN
            RETURN NULL;
        END IF;
        RETURN 'agent:' || runtime_id_value;
    END IF;

    IF auth_mode_value IN ('personalaccesstoken', 'personal_access_token')
       OR legacy_auth_mode_value IN ('personalaccesstoken', 'personal_access_token') THEN
        access_token_value := trim(COALESCE(credentials_value ->> 'access_token', ''));
        IF access_token_value = '' THEN
            RETURN NULL;
        END IF;
        -- md5 is used only as a non-reversible equality fingerprint for a
        -- high-entropy token; no token material is retained in the key.
        RETURN 'pat:' || md5('openai-warmup-pat:' || access_token_value);
    END IF;

    account_id_value := trim(COALESCE(credentials_value ->> 'chatgpt_account_id', ''));
    IF account_id_value = '' THEN
        RETURN NULL;
    END IF;
    user_id_value := trim(COALESCE(credentials_value ->> 'chatgpt_user_id', ''));
    IF user_id_value = '' THEN
        RETURN 'chatgpt:' || account_id_value;
    END IF;
    RETURN 'chatgpt:' || account_id_value || ':user:' || user_id_value;
END;
$$;

CREATE OR REPLACE FUNCTION public.openai_window_warmup_fence_account_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    old_identity TEXT;
    new_identity TEXT;
    initial_generation BIGINT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF COALESCE(NEW.openai_warmup_identity_generation, 1) <= 1
           AND NEW.created_at IS NOT NULL THEN
            initial_generation :=
                (extract(epoch FROM NEW.created_at) * 1000000)::BIGINT * 1000;
            NEW.openai_warmup_identity_generation := GREATEST(initial_generation, 1);
        ELSE
            NEW.openai_warmup_identity_generation :=
                GREATEST(COALESCE(NEW.openai_warmup_identity_generation, 1), 1);
        END IF;
        RETURN NEW;
    END IF;

    old_identity := public.openai_window_warmup_identity_key(
        OLD.platform::text, OLD.type::text, OLD.credentials
    );
    new_identity := public.openai_window_warmup_identity_key(
        NEW.platform::text, NEW.type::text, NEW.credentials
    );

    IF old_identity IS NOT DISTINCT FROM new_identity THEN
        NEW.openai_warmup_identity_generation := OLD.openai_warmup_identity_generation;
        RETURN NEW;
    END IF;

    NEW.openai_warmup_identity_generation :=
        GREATEST(COALESCE(OLD.openai_warmup_identity_generation, 1), 1) + 1;
    NEW.extra := COALESCE(NEW.extra, '{}'::jsonb)
        - 'codex_usage_updated_at'
        - 'codex_5h_used_percent'
        - 'codex_5h_reset_after_seconds'
        - 'codex_5h_window_minutes'
        - 'codex_5h_reset_at'
        - 'codex_global_5h_used_percent'
        - 'codex_global_5h_reset_after_seconds'
        - 'codex_global_5h_window_minutes'
        - 'codex_global_5h_reset_at';

    UPDATE openai_window_warmup_jobs
    SET state = 'paused',
        next_attempt_at = NOW(),
        last_error_code = 'credential_identity_superseded',
        last_error = NULL,
        lease_owner = NULL,
        lease_token = NULL,
        lease_until = NULL,
        updated_at = NOW()
    WHERE account_id = OLD.id
      AND identity_generation = OLD.openai_warmup_identity_generation
      AND state IN ('pending', 'armed', 'due', 'running', 'retrying', 'uncertain', 'possibly_sent');

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_openai_window_warmup_fence_account_identity ON accounts;
CREATE TRIGGER trg_openai_window_warmup_fence_account_identity
    BEFORE INSERT OR UPDATE OF platform, type, credentials, created_at
    ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION public.openai_window_warmup_fence_account_identity();

-- Older rollback binaries omit identity_generation from their INSERT. Fill it
-- from the account row before NOT NULL and the worker fence are evaluated.
CREATE OR REPLACE FUNCTION public.openai_window_warmup_fill_job_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.identity_generation IS NULL OR NEW.identity_generation <= 0 THEN
        SELECT openai_warmup_identity_generation
        INTO NEW.identity_generation
        FROM accounts
        WHERE id = NEW.account_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_openai_window_warmup_fill_job_identity ON openai_window_warmup_jobs;
CREATE TRIGGER trg_openai_window_warmup_fill_job_identity
    BEFORE INSERT ON openai_window_warmup_jobs
    FOR EACH ROW
    EXECUTE FUNCTION public.openai_window_warmup_fill_job_identity();

-- Preserve migration 234's overflow-safe idle/reset behavior while binding the
-- initial cycle and job row to the current logical credential identity.
CREATE OR REPLACE FUNCTION public.openai_window_warmup_enqueue_initial()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    policy_value TEXT;
    identity_value TEXT;
    reset_at_value TIMESTAMPTZ;
    generation_value BIGINT;
    cycle_value TEXT;
    next_value TIMESTAMPTZ;
    state_value TEXT;
    idle_five_hour_snapshot BOOLEAN := FALSE;
    jitter_seconds INTEGER;
    grace_seconds INTEGER := 90;
BEGIN
    policy_value := lower(trim(COALESCE(
        CASE WHEN jsonb_typeof(NEW.extra -> 'openai_codex_warmup_policy') = 'string'
            THEN NULLIF(trim(NEW.extra ->> 'openai_codex_warmup_policy'), '') END,
        CASE WHEN jsonb_typeof(NEW.extra -> 'codex_warmup_policy') = 'string'
            THEN NULLIF(trim(NEW.extra ->> 'codex_warmup_policy'), '') END,
        CASE WHEN jsonb_typeof(NEW.extra -> 'openai_window_warmup_policy') = 'string'
            THEN NULLIF(trim(NEW.extra ->> 'openai_window_warmup_policy'), '') END,
        'off'
    )));
    identity_value := public.openai_window_warmup_identity_key(
        NEW.platform::text, NEW.type::text, NEW.credentials
    );

    IF NEW.platform::text <> 'openai'
       OR NEW.type::text <> 'oauth'
       OR identity_value IS NULL
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

    SELECT MAX(candidate)
    INTO reset_at_value
    FROM (VALUES
        (public.openai_window_warmup_parse_reset(NEW.extra ->> 'codex_5h_reset_at')),
        (public.openai_window_warmup_parse_reset(NEW.extra ->> 'codex_global_5h_reset_at'))
    ) AS reset_candidates(candidate);

    idle_five_hour_snapshot := public.openai_window_warmup_json_number_nonpositive(
        NEW.extra -> 'codex_5h_used_percent'
    );

    generation_value := NEW.openai_warmup_identity_generation;
    cycle_value := 'initial:' || generation_value::TEXT;
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
    jitter_seconds := mod((hashtextextended(cycle_value, 0) & 2147483647::BIGINT), 30)::INTEGER;
    IF reset_at_value IS NOT NULL AND reset_at_value > NOW() AND NOT idle_five_hour_snapshot THEN
        next_value := reset_at_value + (grace_seconds * INTERVAL '1 second') + (jitter_seconds * INTERVAL '1 second');
        state_value := 'armed';
    ELSE
        next_value := NOW() + (jitter_seconds * INTERVAL '1 second');
        state_value := 'pending';
    END IF;

    UPDATE openai_window_warmup_jobs
    SET observed_reset_at = CASE
            WHEN idle_five_hour_snapshot
                THEN COALESCE(openai_window_warmup_jobs.observed_reset_at, reset_at_value)
            ELSE reset_at_value
        END,
        next_attempt_at = next_value,
        state = state_value,
        updated_at = NOW()
    WHERE account_id = NEW.id
      AND quota_scope = 'global'
      AND cycle_key = cycle_value
      AND identity_generation = NEW.openai_warmup_identity_generation
      AND attempt_count = 0
      AND sent_at IS NULL
      AND state IN ('pending', 'armed', 'due', 'retrying');

    IF NOT FOUND THEN
        INSERT INTO openai_window_warmup_jobs
        (account_id, quota_scope, state, trigger, cycle_key, cycle_generation,
         identity_generation, observed_reset_at, next_attempt_at, attempt_count,
         created_at, updated_at)
        VALUES
        (NEW.id, 'global', state_value, 'import', cycle_value, generation_value,
         NEW.openai_warmup_identity_generation, reset_at_value, next_value, 0,
         NOW(), NOW())
        ON CONFLICT DO NOTHING;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_openai_window_warmup_enqueue_initial ON accounts;
CREATE TRIGGER trg_openai_window_warmup_enqueue_initial
    AFTER INSERT OR UPDATE OF extra, credentials, platform, type, status,
        schedulable, expires_at, auto_pause_on_expired, parent_account_id,
        quota_dimension, temp_unschedulable_until
    ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION public.openai_window_warmup_enqueue_initial();
