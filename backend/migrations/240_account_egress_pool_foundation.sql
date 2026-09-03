-- Durable public egress identities, transport routes, and account selection.
-- A route is not a capacity unit: multiple routes may resolve to one identity.
-- Existing accounts remain legacy until explicitly switched to pool mode.
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

CREATE TABLE IF NOT EXISTS egress_identities (
    id          BIGSERIAL PRIMARY KEY,
    public_ip   inet NOT NULL UNIQUE,
    status      VARCHAR(16) NOT NULL DEFAULT 'active',
    created_at  timestamptz NOT NULL DEFAULT NOW(),
    updated_at  timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT egress_identities_status_check
        CHECK (status IN ('active', 'retired')),
    CONSTRAINT egress_identities_public_ip_host_check
        CHECK ((family(public_ip) = 4 AND masklen(public_ip) = 32)
            OR (family(public_ip) = 6 AND masklen(public_ip) = 128))
);

CREATE TABLE IF NOT EXISTS egress_routes (
    id                    BIGSERIAL PRIMARY KEY,
    kind                  VARCHAR(16) NOT NULL,
    proxy_id              BIGINT REFERENCES proxies(id) ON DELETE RESTRICT,
    runtime_scope         VARCHAR(128),
    expected_identity_id  BIGINT REFERENCES egress_identities(id) ON DELETE RESTRICT,
    state                 VARCHAR(32) NOT NULL DEFAULT 'pending_verification',
    last_observed_ip      inet,
    last_probed_at        timestamptz,
    verified_at           timestamptz,
    revision              BIGINT NOT NULL DEFAULT 1,
    last_error            TEXT,
    created_at            timestamptz NOT NULL DEFAULT NOW(),
    updated_at            timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT egress_routes_kind_check
        CHECK (kind IN ('proxy', 'direct')),
    CONSTRAINT egress_routes_shape_check
        CHECK ((kind = 'proxy' AND proxy_id IS NOT NULL AND runtime_scope IS NULL)
            OR (kind = 'direct' AND proxy_id IS NULL AND runtime_scope IS NOT NULL)),
    CONSTRAINT egress_routes_state_check
        CHECK (state IN ('pending_verification', 'active', 'inactive', 'expired',
                         'identity_mismatch', 'retired')),
    CONSTRAINT egress_routes_active_identity_check
        CHECK (state <> 'active' OR expected_identity_id IS NOT NULL),
    CONSTRAINT egress_routes_observed_ip_host_check
        CHECK (last_observed_ip IS NULL
            OR (family(last_observed_ip) = 4 AND masklen(last_observed_ip) = 32)
            OR (family(last_observed_ip) = 6 AND masklen(last_observed_ip) = 128)),
    CONSTRAINT egress_routes_revision_check
        CHECK (revision > 0),
    CONSTRAINT egress_routes_proxy_unique UNIQUE (proxy_id),
    CONSTRAINT egress_routes_runtime_scope_unique UNIQUE (runtime_scope)
);

ALTER TABLE accounts
    ADD COLUMN IF NOT EXISTS egress_mode VARCHAR(16) NOT NULL DEFAULT 'legacy',
    ADD COLUMN IF NOT EXISTS egress_revision BIGINT NOT NULL DEFAULT 1;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'accounts_egress_mode_check'
          AND conrelid = 'accounts'::regclass
    ) THEN
        ALTER TABLE accounts
            ADD CONSTRAINT accounts_egress_mode_check
            CHECK (egress_mode IN ('legacy', 'pool')) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'accounts_egress_revision_check'
          AND conrelid = 'accounts'::regclass
    ) THEN
        ALTER TABLE accounts
            ADD CONSTRAINT accounts_egress_revision_check
            CHECK (egress_revision > 0) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'accounts_pool_concurrency_check'
          AND conrelid = 'accounts'::regclass
    ) THEN
        ALTER TABLE accounts
            ADD CONSTRAINT accounts_pool_concurrency_check
            CHECK (egress_mode <> 'pool' OR concurrency BETWEEN 1 AND 10000) NOT VALID;
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS account_egress_bindings (
    account_id  BIGINT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    route_id    BIGINT NOT NULL REFERENCES egress_routes(id) ON DELETE RESTRICT,
    position    INTEGER NOT NULL DEFAULT 0,
    is_primary  BOOLEAN NOT NULL DEFAULT FALSE,
    status      VARCHAR(16) NOT NULL DEFAULT 'active',
    created_at  timestamptz NOT NULL DEFAULT NOW(),
    updated_at  timestamptz NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, route_id),
    CONSTRAINT account_egress_bindings_position_check CHECK (position >= 0),
    CONSTRAINT account_egress_bindings_status_check CHECK (status IN ('active', 'draining')),
    CONSTRAINT account_egress_bindings_account_position_unique UNIQUE (account_id, position)
);

CREATE INDEX IF NOT EXISTS egress_routes_identity_idx
    ON egress_routes (expected_identity_id);
CREATE INDEX IF NOT EXISTS egress_routes_state_identity_idx
    ON egress_routes (state, expected_identity_id);
CREATE INDEX IF NOT EXISTS account_egress_bindings_route_idx
    ON account_egress_bindings (route_id);
CREATE UNIQUE INDEX IF NOT EXISTS account_egress_bindings_one_primary_idx
    ON account_egress_bindings (account_id)
    WHERE is_primary;

COMMENT ON TABLE egress_identities IS
    'Immutable normalized public IP identities used as per-account capacity units';
COMMENT ON TABLE egress_routes IS
    'Proxy or deployment-scoped direct paths to a verified public egress identity';
COMMENT ON COLUMN egress_routes.runtime_scope IS
    'Deployment egress scope for direct routes; all workers in a scope must observe the same IP';
COMMENT ON COLUMN egress_routes.revision IS
    'Monotonic CAS revision for probe/identity confirmation';
COMMENT ON COLUMN accounts.egress_mode IS
    'legacy uses accounts.proxy_id; pool uses account_egress_bindings';
COMMENT ON COLUMN accounts.egress_revision IS
    'Monotonic egress configuration/cache fence';
COMMENT ON COLUMN accounts.concurrency IS
    'Legacy account concurrency, or per-distinct-egress concurrency while egress_mode=pool';
COMMENT ON TABLE account_egress_bindings IS
    'Configured account routes; runtime capacity is deduplicated by resolved egress identity';

INSERT INTO settings (key, value, updated_at)
VALUES ('account_egress_pool_rollout_mode', 'off', NOW())
ON CONFLICT (key) DO NOTHING;
