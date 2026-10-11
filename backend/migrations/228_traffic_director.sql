-- Traffic Director group head and immutable publication history.
-- Version zero is synthetic legacy state and is intentionally not persisted.

ALTER TABLE groups
    ADD COLUMN IF NOT EXISTS traffic_director_mode VARCHAR(16) NOT NULL DEFAULT 'legacy',
    ADD COLUMN IF NOT EXISTS traffic_director_version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS traffic_director_spec JSONB;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'groups_traffic_director_version_check'
          AND conrelid = 'groups'::regclass
    ) THEN
        ALTER TABLE groups
            ADD CONSTRAINT groups_traffic_director_version_check
            CHECK (traffic_director_version >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'groups_traffic_director_policy_check'
          AND conrelid = 'groups'::regclass
    ) THEN
        ALTER TABLE groups
            ADD CONSTRAINT groups_traffic_director_policy_check
            CHECK (
                (traffic_director_version > 0 OR traffic_director_mode = 'legacy')
                AND (
                    (traffic_director_mode = 'legacy' AND traffic_director_spec IS NULL)
                    OR (
                        traffic_director_mode IN ('shadow', 'enforced')
                        AND traffic_director_spec IS NOT NULL
                        AND jsonb_typeof(traffic_director_spec) = 'object'
                    )
                )
            );
    END IF;
END;
$$;

COMMENT ON COLUMN groups.traffic_director_mode IS
    'Published Traffic Director mode: legacy, shadow, or enforced';
COMMENT ON COLUMN groups.traffic_director_version IS
    'Published Traffic Director version; zero is synthetic legacy state';
COMMENT ON COLUMN groups.traffic_director_spec IS
    'Canonical published Traffic Director policy; NULL in legacy mode';

CREATE TABLE IF NOT EXISTS traffic_director_versions (
    id BIGSERIAL PRIMARY KEY,
    group_id BIGINT NOT NULL,
    version BIGINT NOT NULL,
    mode VARCHAR(16) NOT NULL,
    spec JSONB,
    checksum VARCHAR(64) NOT NULL,
    operation_key VARCHAR(200) NOT NULL,
    request_fingerprint VARCHAR(64) NOT NULL,
    operator_id BIGINT,
    note TEXT NOT NULL DEFAULT '',
    rollback_from_version BIGINT,
    unassigned_account_ids BIGINT[] NOT NULL DEFAULT '{}'::BIGINT[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT traffic_director_versions_group_fk
        FOREIGN KEY (group_id) REFERENCES groups(id) ON DELETE CASCADE,
    CONSTRAINT traffic_director_versions_group_version_key
        UNIQUE (group_id, version),
    CONSTRAINT traffic_director_versions_group_operation_key
        UNIQUE (group_id, operation_key),
    CONSTRAINT traffic_director_versions_version_check
        CHECK (version > 0),
    CONSTRAINT traffic_director_versions_mode_spec_check
        CHECK (
            (mode = 'legacy' AND spec IS NULL)
            OR (
                mode IN ('shadow', 'enforced')
                AND spec IS NOT NULL
                AND jsonb_typeof(spec) = 'object'
            )
        ),
    CONSTRAINT traffic_director_versions_checksum_check
        CHECK (checksum ~ '^[0-9a-f]{64}$'),
    CONSTRAINT traffic_director_versions_fingerprint_check
        CHECK (request_fingerprint ~ '^[0-9a-f]{64}$'),
    CONSTRAINT traffic_director_versions_rollback_check
        CHECK (rollback_from_version IS NULL OR rollback_from_version >= 0)
);

COMMENT ON TABLE traffic_director_versions IS
    'Immutable Traffic Director publications; version zero is synthesized by the application';
COMMENT ON COLUMN traffic_director_versions.unassigned_account_ids IS
    'Authoritative group-account IDs outside all pools at publication time';

-- Direct history mutation is forbidden. Deletes are only allowed for a real
-- foreign-key cascade, or for the Group lifecycle transaction after it has
-- marked the parent deleted and set the transaction-local cleanup marker.
-- Checking only deleted_at would let an arbitrary caller soft-delete a Group
-- and then erase its immutable history in a separate transaction.
CREATE OR REPLACE FUNCTION prevent_traffic_director_version_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        RAISE EXCEPTION 'traffic_director_versions rows are immutable'
            USING ERRCODE = '55000';
    END IF;

    IF TG_OP = 'DELETE' THEN
        -- PostgreSQL invokes the child trigger at a deeper trigger depth for
        -- the FK ON DELETE CASCADE path. This is the only hard-delete path
        -- that may remove history without an application marker.
        IF pg_trigger_depth() > 1
           AND NOT EXISTS (
               SELECT 1
               FROM groups
               WHERE id = OLD.group_id
           ) THEN
            RETURN OLD;
        END IF;

        -- Group soft-delete cleanup must happen in the same transaction and
        -- explicitly opt in with a local setting. The parent check prevents
        -- the marker from authorizing deletion for an active Group.
        IF current_setting('sub2api.traffic_director_history_cleanup', true) = 'on'
           AND EXISTS (
               SELECT 1
               FROM groups
               WHERE id = OLD.group_id
                 AND deleted_at IS NOT NULL
           ) THEN
            RETURN OLD;
        END IF;
    END IF;

    RAISE EXCEPTION 'traffic_director_versions rows are immutable'
        USING ERRCODE = '55000';
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS traffic_director_versions_immutable ON traffic_director_versions;
CREATE TRIGGER traffic_director_versions_immutable
BEFORE UPDATE OR DELETE ON traffic_director_versions
FOR EACH ROW
EXECUTE FUNCTION prevent_traffic_director_version_mutation();

-- Keep durable API-key auth snapshots aligned with Traffic Director head
-- changes. This replaces the latest function body from migration 227.
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
       AND OLD.traffic_director_mode IS NOT DISTINCT FROM NEW.traffic_director_mode
       AND OLD.traffic_director_version IS NOT DISTINCT FROM NEW.traffic_director_version
       AND OLD.traffic_director_spec IS NOT DISTINCT FROM NEW.traffic_director_spec
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
