-- Group-scoped scheduler selection and sparse advanced scheduler overrides.
-- Existing groups inherit the global switch and therefore retain legacy behavior.

ALTER TABLE groups
    ADD COLUMN IF NOT EXISTS scheduler_type VARCHAR(16) NOT NULL DEFAULT 'inherit',
    ADD COLUMN IF NOT EXISTS advanced_scheduler_overrides JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE groups
    ADD CONSTRAINT groups_scheduler_type_check
    CHECK (scheduler_type IN ('inherit', 'basic', 'advanced')),
    ADD CONSTRAINT groups_advanced_scheduler_overrides_object_check
    CHECK (jsonb_typeof(advanced_scheduler_overrides) = 'object');

COMMENT ON COLUMN groups.scheduler_type IS
    'inherit = global openai_advanced_scheduler_enabled; basic = force legacy scheduler; advanced = force advanced scheduler';
COMMENT ON COLUMN groups.advanced_scheduler_overrides IS
    'Sparse advanced scheduler overrides. Missing fields inherit global values; explicit false and zero are preserved.';

-- Keep the durable API-key auth cache invalidation backstop aligned with the
-- latest function body from migration 193. Scheduler settings are consumed
-- from the auth snapshot on the request hot path.
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
       AND OLD.scheduler_type IS NOT DISTINCT FROM NEW.scheduler_type
       AND OLD.advanced_scheduler_overrides IS NOT DISTINCT FROM NEW.advanced_scheduler_overrides
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
