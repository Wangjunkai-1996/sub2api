-- Make the deployment default direct route a valid pool egress and move the
-- sys1-ipv4 pool bindings off the 3proxy path.
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

-- Serialize the route/account rewrite with every egress writer.  The
-- migration runner already holds the global migration advisory lock.
LOCK TABLE egress_routes, accounts, account_egress_bindings
    IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE account_egress_direct_target (
    direct_route_id BIGINT PRIMARY KEY,
    proxy_route_id  BIGINT NOT NULL UNIQUE,
    proxy_id        BIGINT NOT NULL
) ON COMMIT DROP;

-- The proxy name is the stable operator-facing identity.  Prefer an active,
-- recently verified match, with the known production id as a tie breaker for
-- duplicate historical rows.
INSERT INTO account_egress_direct_target (direct_route_id, proxy_route_id, proxy_id)
SELECT direct_route.id, candidate.route_id, candidate.proxy_id
FROM egress_routes direct_route
CROSS JOIN LATERAL (
    SELECT proxy_route.id AS route_id, proxy_route.proxy_id
    FROM egress_routes proxy_route
    JOIN proxies proxy ON proxy.id = proxy_route.proxy_id
    WHERE proxy_route.kind = 'proxy'
      AND proxy.name = 'sys1-ipv4'
    ORDER BY (proxy_route.state = 'active') DESC,
             proxy_route.verified_at DESC NULLS LAST,
             (proxy.id = 9) DESC,
             proxy.id,
             proxy_route.id
    LIMIT 1
) candidate
WHERE direct_route.kind = 'direct'
  AND direct_route.runtime_scope = 'default';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM egress_routes
        WHERE kind = 'direct' AND runtime_scope = 'default'
    ) THEN
        RAISE EXCEPTION 'default direct egress route is required before enabling direct pool mode';
    END IF;

    -- Do not silently remove the proxy-only contract when the deployment
    -- route is missing.  The direct route must inherit a real, verified
    -- sys1 identity in every environment that applies this migration.
    IF NOT EXISTS (SELECT 1 FROM account_egress_direct_target) THEN
        RAISE EXCEPTION 'sys1-ipv4 proxy route is required before migrating direct pool accounts';
    END IF;
END;
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM account_egress_direct_target target
        JOIN egress_routes proxy_route
          ON proxy_route.id = target.proxy_route_id
        LEFT JOIN egress_identities identity
          ON identity.id = proxy_route.expected_identity_id
        WHERE proxy_route.state <> 'active'
           OR proxy_route.expected_identity_id IS NULL
           OR proxy_route.last_observed_ip IS NULL
           OR proxy_route.verified_at IS NULL
           OR proxy_route.verified_at < NOW() - INTERVAL '5 minutes'
           OR identity.status <> 'active'
    ) THEN
        RAISE EXCEPTION 'sys1-ipv4 proxy route must have an active verified identity before direct migration';
    END IF;
END;
$$;

-- Copy the verified sys1 route evidence into the direct route.  The direct
-- route therefore uses the identity actually observed by the proxy probe,
-- without manufacturing an environment-specific IP or probe timestamp.
UPDATE egress_routes route
SET expected_identity_id = proxy_route.expected_identity_id,
    state = proxy_route.state,
    last_observed_ip = proxy_route.last_observed_ip,
    last_probed_at = proxy_route.last_probed_at,
    verified_at = proxy_route.verified_at,
    revision = route.revision + 1,
    last_error = proxy_route.last_error,
    updated_at = NOW()
FROM account_egress_direct_target target
JOIN egress_routes proxy_route ON proxy_route.id = target.proxy_route_id
WHERE route.id = target.direct_route_id;

-- An empty value means "no explicit proxy".  New OAuth accounts therefore
-- inherit the verified deployment direct route instead of sys1's 3proxy path.
UPDATE settings
SET value = '', updated_at = NOW()
WHERE key = 'openai_oauth_default_proxy_id';

-- Migration 244 installed a proxy-only contract.  Direct routes are now a
-- supported pool route, so remove those obsolete guards before inserting the
-- direct bindings below.
DROP TRIGGER IF EXISTS account_egress_bindings_proxy_only ON account_egress_bindings;
DROP TRIGGER IF EXISTS accounts_egress_pool_proxy_only ON accounts;
DROP TRIGGER IF EXISTS egress_routes_proxy_only_for_pool_accounts ON egress_routes;
DROP FUNCTION IF EXISTS enforce_pool_binding_proxy_only();
DROP FUNCTION IF EXISTS enforce_pool_account_proxy_only();
DROP FUNCTION IF EXISTS enforce_route_proxy_only_for_pool_accounts();

CREATE TEMP TABLE account_egress_direct_migration (
    account_id      BIGINT PRIMARY KEY,
    old_route_id    BIGINT NOT NULL,
    old_position    INTEGER NOT NULL,
    old_is_primary  BOOLEAN NOT NULL,
    old_status      VARCHAR(16) NOT NULL,
    position_shift  BIGINT NOT NULL
) ON COMMIT DROP;

INSERT INTO account_egress_direct_migration (
    account_id, old_route_id, old_position, old_is_primary, old_status, position_shift
)
SELECT binding.account_id,
       binding.route_id,
       binding.position,
       binding.is_primary,
       binding.status,
       MAX(all_bindings.position)::BIGINT + 1
FROM account_egress_bindings binding
JOIN account_egress_direct_target target
  ON target.proxy_route_id = binding.route_id
JOIN accounts account ON account.id = binding.account_id
JOIN account_egress_bindings all_bindings
  ON all_bindings.account_id = binding.account_id
WHERE account.egress_mode = 'pool'
  AND account.deleted_at IS NULL
GROUP BY binding.account_id, binding.route_id, binding.position, binding.is_primary, binding.status;

-- Move every affected binding above its old positions first.  The subsequent
-- route replacement can therefore never collide with the position unique key.
UPDATE account_egress_bindings binding
SET position = (binding.position::BIGINT + migration.position_shift)::INTEGER,
    updated_at = NOW()
FROM account_egress_direct_migration migration
WHERE binding.account_id = migration.account_id;

DELETE FROM account_egress_bindings binding
USING account_egress_direct_migration migration
WHERE binding.account_id = migration.account_id
  AND binding.route_id = migration.old_route_id;

INSERT INTO account_egress_bindings (
    account_id, route_id, position, is_primary, status, created_at, updated_at
)
SELECT migration.account_id,
       target.direct_route_id,
       (migration.old_position::BIGINT + migration.position_shift)::INTEGER,
       migration.old_is_primary,
       migration.old_status,
       NOW(),
       NOW()
FROM account_egress_direct_migration migration
JOIN account_egress_direct_target target ON TRUE
ON CONFLICT (account_id, route_id) DO UPDATE
SET position = EXCLUDED.position,
    is_primary = account_egress_bindings.is_primary OR EXCLUDED.is_primary,
    status = EXCLUDED.status,
    updated_at = NOW();

-- Compact positions after the replacement.  All current positions are above
-- the old maximum, so assigning 0..N-1 cannot collide with a live position.
WITH ranked AS (
    SELECT binding.account_id,
           binding.route_id,
           ROW_NUMBER() OVER (
               PARTITION BY binding.account_id
               ORDER BY binding.position, binding.route_id
           ) - 1 AS final_position
    FROM account_egress_bindings binding
    JOIN account_egress_direct_migration migration
      ON migration.account_id = binding.account_id
)
UPDATE account_egress_bindings binding
SET position = ranked.final_position,
    updated_at = NOW()
FROM ranked
WHERE binding.account_id = ranked.account_id
  AND binding.route_id = ranked.route_id;

CREATE TEMP TABLE account_egress_direct_root_state (
    account_id        BIGINT PRIMARY KEY,
    primary_proxy_id  BIGINT
) ON COMMIT DROP;

INSERT INTO account_egress_direct_root_state (account_id, primary_proxy_id)
SELECT migration.account_id, primary_route.proxy_id
FROM account_egress_direct_migration migration
LEFT JOIN LATERAL (
    SELECT route.proxy_id
    FROM account_egress_bindings binding
    JOIN egress_routes route ON route.id = binding.route_id
    WHERE binding.account_id = migration.account_id
      AND binding.is_primary
    ORDER BY binding.position, binding.route_id
    LIMIT 1
) primary_route ON TRUE;

UPDATE accounts account
SET proxy_id = state.primary_proxy_id,
    proxy_fallback_origin_id = NULL,
    egress_revision = account.egress_revision + 1,
    updated_at = NOW()
FROM account_egress_direct_root_state state
WHERE account.id = state.account_id;

-- Shadow accounts inherit the root's selected proxy for legacy callers.  A
-- direct primary route therefore clears that compatibility field as well.
UPDATE accounts shadow
SET proxy_id = state.primary_proxy_id,
    proxy_fallback_origin_id = NULL,
    egress_revision = shadow.egress_revision + 1,
    updated_at = NOW()
FROM account_egress_direct_root_state state
WHERE shadow.parent_account_id = state.account_id
  AND shadow.deleted_at IS NULL;

WITH affected AS (
    SELECT account_id AS id
    FROM account_egress_direct_migration
    UNION
    SELECT shadow.id
    FROM accounts shadow
    JOIN account_egress_direct_migration migration
      ON migration.account_id = shadow.parent_account_id
    WHERE shadow.deleted_at IS NULL
), payload AS (
    SELECT jsonb_build_object('account_ids', to_jsonb(array_agg(id ORDER BY id))) AS body
    FROM affected
    HAVING COUNT(*) > 0
)
INSERT INTO scheduler_outbox (event_type, payload)
SELECT 'account_bulk_changed', body
FROM payload;
