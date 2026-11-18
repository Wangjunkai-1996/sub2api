-- Traffic Director has been retired from the request path. Remove its durable
-- publication state after restoring the group auth-cache invalidation function
-- to the latest non-Traffic-Director contract from migration 227.
--
-- Replacing the function first is required: the existing groups trigger keeps
-- this function installed, and the body written by migration 228 reads the
-- three columns removed below.
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

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

-- DROP TRIGGER requires its relation to exist. Use guarded dynamic SQL so the
-- migration also remains safe to replay after the history table is gone.
DO $cleanup$
BEGIN
    IF to_regclass('traffic_director_versions') IS NOT NULL THEN
        EXECUTE 'DROP TRIGGER IF EXISTS traffic_director_versions_immutable ON traffic_director_versions';
    END IF;
END;
$cleanup$;

DROP TABLE IF EXISTS traffic_director_versions;
DROP FUNCTION IF EXISTS prevent_traffic_director_version_mutation();

ALTER TABLE groups
    DROP CONSTRAINT IF EXISTS groups_traffic_director_version_check,
    DROP CONSTRAINT IF EXISTS groups_traffic_director_policy_check,
    DROP COLUMN IF EXISTS traffic_director_mode,
    DROP COLUMN IF EXISTS traffic_director_version,
    DROP COLUMN IF EXISTS traffic_director_spec;
