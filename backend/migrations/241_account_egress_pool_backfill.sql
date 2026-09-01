-- Build pending route records for all existing proxy paths plus the deployment
-- default direct path. Backfill one binding per non-shadow account so rollback
-- and later opt-in are deterministic, but deliberately keep every account in
-- legacy mode until an administrator confirms identities and enables its pool.
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

INSERT INTO egress_routes (kind, proxy_id, state, revision, created_at, updated_at)
SELECT 'proxy', p.id, 'pending_verification', 1, NOW(), NOW()
FROM proxies AS p
ON CONFLICT (proxy_id) DO NOTHING;

INSERT INTO egress_routes (kind, runtime_scope, state, revision, created_at, updated_at)
VALUES ('direct', 'default', 'pending_verification', 1, NOW(), NOW())
ON CONFLICT (runtime_scope) DO NOTHING;

INSERT INTO account_egress_bindings (
    account_id, route_id, position, is_primary, status, created_at, updated_at
)
SELECT
    a.id,
    CASE WHEN a.proxy_id IS NULL THEN direct_route.id ELSE proxy_route.id END,
    0,
    TRUE,
    'active',
    NOW(),
    NOW()
FROM accounts AS a
LEFT JOIN egress_routes AS proxy_route
    ON proxy_route.kind = 'proxy' AND proxy_route.proxy_id = a.proxy_id
JOIN egress_routes AS direct_route
    ON direct_route.kind = 'direct' AND direct_route.runtime_scope = 'default'
WHERE a.parent_account_id IS NULL
  AND a.deleted_at IS NULL
  AND (a.proxy_id IS NULL OR proxy_route.id IS NOT NULL)
ON CONFLICT (account_id, route_id) DO NOTHING;
