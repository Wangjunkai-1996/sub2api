-- Remove the four retired KKAI features after every runnable application
-- version has stopped depending on their persisted schema.
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '90s';

-- The surviving groups trigger must no longer read the columns dropped below.
CREATE OR REPLACE FUNCTION enqueue_group_auth_cache_invalidation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    target_group_id BIGINT;
BEGIN
    target_group_id := OLD.id;
    IF TG_OP = 'UPDATE'
       AND OLD.status IS NOT DISTINCT FROM NEW.status
       AND OLD.is_exclusive IS NOT DISTINCT FROM NEW.is_exclusive
       AND OLD.allow_image_generation IS NOT DISTINCT FROM NEW.allow_image_generation
       AND OLD.platform IS NOT DISTINCT FROM NEW.platform
       AND OLD.subscription_type IS NOT DISTINCT FROM NEW.subscription_type
       AND OLD.rate_multiplier IS NOT DISTINCT FROM NEW.rate_multiplier
       AND OLD.peak_rate_enabled IS NOT DISTINCT FROM NEW.peak_rate_enabled
       AND OLD.peak_start IS NOT DISTINCT FROM NEW.peak_start
       AND OLD.peak_end IS NOT DISTINCT FROM NEW.peak_end
       AND OLD.peak_rate_multiplier IS NOT DISTINCT FROM NEW.peak_rate_multiplier
       AND OLD.profit_control_enabled IS NOT DISTINCT FROM NEW.profit_control_enabled
       AND OLD.profit_min_margin IS NOT DISTINCT FROM NEW.profit_min_margin
       AND OLD.profit_safety_buffer IS NOT DISTINCT FROM NEW.profit_safety_buffer
       AND OLD.deleted_at IS NOT DISTINCT FROM NEW.deleted_at THEN
        RETURN NEW;
    END IF;

    INSERT INTO auth_cache_invalidation_outbox (cache_key)
    SELECT encode(sha256(convert_to(k.key, 'UTF8')), 'hex')
    FROM api_keys AS k
    WHERE k.group_id = target_group_id
      AND k.deleted_at IS NULL
      AND k.key <> '';
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_openai_window_warmup_cleanup_deleted_account ON accounts;
DROP TRIGGER IF EXISTS trg_openai_window_warmup_enqueue_initial ON accounts;
DROP TRIGGER IF EXISTS trg_openai_window_warmup_fence_account_identity ON accounts;
DROP TRIGGER IF EXISTS trg_openai_window_warmup_sync_policy_state ON accounts;
DO $$
BEGIN
    IF to_regclass('public.openai_window_warmup_jobs') IS NOT NULL THEN
        EXECUTE 'DROP TRIGGER IF EXISTS trg_openai_window_warmup_fill_job_identity ON openai_window_warmup_jobs';
    END IF;
END;
$$;

DROP TABLE IF EXISTS openai_window_warmup_attempts;
DROP TABLE IF EXISTS openai_window_warmup_runtime;
DROP TABLE IF EXISTS openai_window_warmup_jobs;

DROP FUNCTION IF EXISTS public.openai_window_warmup_cleanup_deleted_account();
DROP FUNCTION IF EXISTS public.openai_window_warmup_enqueue_initial();
DROP FUNCTION IF EXISTS public.openai_window_warmup_fence_account_identity();
DROP FUNCTION IF EXISTS public.openai_window_warmup_fill_job_identity();
DROP FUNCTION IF EXISTS public.openai_window_warmup_sync_policy_state();
DROP FUNCTION IF EXISTS public.openai_window_warmup_identity_key(TEXT, TEXT, JSONB);
DROP FUNCTION IF EXISTS public.openai_window_warmup_json_number_nonpositive(JSONB);
DROP FUNCTION IF EXISTS public.openai_window_warmup_parse_reset(TEXT);

ALTER TABLE accounts
    DROP COLUMN IF EXISTS openai_warmup_identity_generation;

ALTER TABLE groups
    DROP CONSTRAINT IF EXISTS groups_scheduler_type_check,
    DROP CONSTRAINT IF EXISTS groups_advanced_scheduler_overrides_object_check,
    DROP COLUMN IF EXISTS scheduler_type,
    DROP COLUMN IF EXISTS advanced_scheduler_overrides;
