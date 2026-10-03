-- Keep post-send usage observations separate from the cycle reset. Two
-- observations are required to distinguish a fixed active reset from the idle
-- rolling now+5h projection returned by /wham/usage.
ALTER TABLE openai_window_warmup_jobs
    ADD COLUMN IF NOT EXISTS uncertain_observed_reset_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS uncertain_observed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS uncertain_terminal_observed BOOLEAN NOT NULL DEFAULT FALSE;

COMMENT ON COLUMN openai_window_warmup_jobs.uncertain_observed_reset_at IS
    'Authoritative passive reset observed after an ambiguous warmup send';
COMMENT ON COLUMN openai_window_warmup_jobs.uncertain_observed_at IS
    'Database transition time of the passive observation used for replay fencing';
COMMENT ON COLUMN openai_window_warmup_jobs.uncertain_terminal_observed IS
    'Whether response.completed or response.done was observed before reset reconciliation';

-- Select the latest valid five-hour reset exposed by the account snapshot.
-- Replacing the function is additive, idempotent, and updates the existing
-- account trigger without changing its transaction or enqueue semantics.
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

    SELECT MAX(candidate)
    INTO reset_at_value
    FROM (VALUES
        (public.openai_window_warmup_parse_reset(NEW.extra ->> 'codex_5h_reset_at')),
        (public.openai_window_warmup_parse_reset(NEW.extra ->> 'codex_global_5h_reset_at'))
    ) AS reset_candidates(candidate);

    idle_five_hour_snapshot := CASE
        WHEN jsonb_typeof(NEW.extra -> 'codex_5h_used_percent') = 'number'
            THEN (NEW.extra ->> 'codex_5h_used_percent')::DOUBLE PRECISION <= 0
        ELSE FALSE
    END;

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
    IF reset_at_value IS NOT NULL AND reset_at_value > NOW() AND NOT idle_five_hour_snapshot THEN
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

-- Releases before this migration armed an unused account until the rolling
-- now+5h projection reported by /wham/usage. Move only untouched, eligible
-- jobs with explicit numeric 0% evidence into the near-term queue. Preserve
-- observed_reset_at as evidence; the worker still performs its fenced usage
-- preflight before sending.
UPDATE openai_window_warmup_jobs AS job
SET state = 'pending',
    next_attempt_at = NOW()
        + (mod((hashtextextended(job.cycle_key, 0) & 2147483647::BIGINT), 30) * INTERVAL '1 second'),
    updated_at = NOW()
FROM accounts AS account
WHERE job.account_id = account.id
  AND job.quota_scope = 'global'
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
  AND jsonb_typeof(account.extra -> 'codex_5h_used_percent') = 'number'
  AND (account.extra ->> 'codex_5h_used_percent')::NUMERIC <= 0;
