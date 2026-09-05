-- Guard the warmup trigger and worker CAS against oversized JSON numbers.
-- PostgreSQL JSONB stores NUMERIC values, while the previous migrations cast
-- codex_5h_used_percent through DOUBLE PRECISION/NUMERIC. A value such as
-- 1e100000 is valid JSONB but overflows DOUBLE PRECISION and can abort an
-- otherwise unrelated account import or update. The warmup decision only needs
-- to know whether the value is non-positive, so determine that from the scalar
-- JSON spelling without converting its magnitude.
CREATE OR REPLACE FUNCTION public.openai_window_warmup_json_number_nonpositive(value JSONB)
RETURNS BOOLEAN
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    text_value TEXT;
BEGIN
    IF value IS NULL OR jsonb_typeof(value) <> 'number' THEN
        RETURN FALSE;
    END IF;

    text_value := trim(value #>> '{}');
    IF text_value = '' THEN
        RETURN FALSE;
    END IF;

    -- JSON numbers have no positive sign. A leading minus is sufficient for
    -- every negative value (including an arbitrarily large exponent); the
    -- regular expression covers zero with optional decimal/exponent syntax.
    IF left(text_value, 1) = '-' THEN
        RETURN TRUE;
    END IF;
    RETURN text_value ~ '^0+(\.0*)?([eE][+-]?[0-9]+)?$';
EXCEPTION WHEN OTHERS THEN
    -- A malformed/unsupported value must never abort the account transaction.
    RETURN FALSE;
END;
$$;

-- Keep the trigger's existing transaction and eligibility semantics, but use
-- the overflow-safe predicate above for the idle snapshot decision.
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

    SELECT MAX(candidate)
    INTO reset_at_value
    FROM (VALUES
        (public.openai_window_warmup_parse_reset(NEW.extra ->> 'codex_5h_reset_at')),
        (public.openai_window_warmup_parse_reset(NEW.extra ->> 'codex_global_5h_reset_at'))
    ) AS reset_candidates(candidate);

    idle_five_hour_snapshot := public.openai_window_warmup_json_number_nonpositive(
        NEW.extra -> 'codex_5h_used_percent'
    );

    generation_value := (extract(epoch FROM NEW.created_at) * 1000000)::BIGINT * 1000;
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

-- Repair untouched rows if this migration follows an older 232/233 run. The
-- helper makes this update safe even when the stored value exceeds float64.
UPDATE openai_window_warmup_jobs AS job
SET state = 'pending',
    next_attempt_at = NOW()
        + (mod((hashtextextended(job.cycle_key, 0) & 2147483647::BIGINT), 30) * INTERVAL '1 second'),
    updated_at = NOW()
FROM accounts AS account
WHERE job.account_id = account.id
  AND job.quota_scope = 'global'
  AND job.cycle_key LIKE 'initial:%'
  AND job.state = 'armed'
  AND job.attempt_count = 0
  AND job.sent_at IS NULL
  AND account.platform::text = 'openai'
  AND account.type::text = 'oauth'
  AND account.parent_account_id IS NULL
  AND COALESCE(account.quota_dimension::text, 'global') = 'global'
  AND account.status::text = 'active'
  AND account.schedulable
  AND (account.temp_unschedulable_until IS NULL OR account.temp_unschedulable_until <= NOW())
  AND account.deleted_at IS NULL
  AND (account.expires_at IS NULL OR account.expires_at > NOW())
  AND lower(trim(COALESCE(
      CASE WHEN jsonb_typeof(account.extra -> 'openai_codex_warmup_policy') = 'string'
          THEN NULLIF(trim(account.extra ->> 'openai_codex_warmup_policy'), '') END,
      CASE WHEN jsonb_typeof(account.extra -> 'codex_warmup_policy') = 'string'
          THEN NULLIF(trim(account.extra ->> 'codex_warmup_policy'), '') END,
      CASE WHEN jsonb_typeof(account.extra -> 'openai_window_warmup_policy') = 'string'
          THEN NULLIF(trim(account.extra ->> 'openai_window_warmup_policy'), '') END,
      'off'
  ))) IN ('initial_once', 'continuous')
  AND public.openai_window_warmup_json_number_nonpositive(
      account.extra -> 'codex_5h_used_percent'
  );
