-- Remove migration 243's broad future-object grants and establish the
-- proxy-only account-pool contract. Existing legacy dormant direct bindings
-- remain available for rollback; only pool-mode data is repaired/rejected.
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

DO $$
DECLARE
    runtime_role name;
    default_acl_owners name[];
BEGIN
    SELECT role.rolname
      INTO runtime_role
      FROM pg_class relation
      JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
      JOIN pg_roles role ON role.oid = relation.relowner
     WHERE namespace.nspname = 'public'
       AND relation.relname = 'accounts'
       AND relation.relkind IN ('r', 'p');

    IF runtime_role IS NULL THEN
        RAISE EXCEPTION 'cannot resolve runtime role from public.accounts owner';
    END IF;

    SELECT array_agg(DISTINCT owner.rolname ORDER BY owner.rolname)
      INTO default_acl_owners
      FROM pg_default_acl defaults
      JOIN pg_namespace namespace ON namespace.oid = defaults.defaclnamespace
      JOIN pg_roles owner ON owner.oid = defaults.defaclrole
      CROSS JOIN LATERAL aclexplode(defaults.defaclacl) privilege
     WHERE namespace.nspname = 'public'
       AND defaults.defaclobjtype IN ('r', 'S')
       AND privilege.grantee = (SELECT oid FROM pg_roles WHERE rolname = runtime_role);

    IF default_acl_owners IS NOT NULL
       AND default_acl_owners <> ARRAY[current_user::name] THEN
        RAISE EXCEPTION
            'refusing to revoke default privileges owned by %, current migration owner is %',
            default_acl_owners, current_user;
    END IF;

    EXECUTE format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
        'REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM %I',
        current_user,
        runtime_role
    );
    EXECUTE format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
        'REVOKE USAGE, SELECT, UPDATE ON SEQUENCES FROM %I',
        current_user,
        runtime_role
    );

    IF EXISTS (
        SELECT 1
          FROM pg_default_acl defaults
          JOIN pg_namespace namespace ON namespace.oid = defaults.defaclnamespace
          CROSS JOIN LATERAL aclexplode(defaults.defaclacl) privilege
         WHERE namespace.nspname = 'public'
           AND defaults.defaclrole = (SELECT oid FROM pg_roles WHERE rolname = current_user)
           AND defaults.defaclobjtype IN ('r', 'S')
           AND privilege.grantee = (SELECT oid FROM pg_roles WHERE rolname = runtime_role)
    ) THEN
        RAISE EXCEPTION 'runtime role still has public-schema future object privileges';
    END IF;
END;
$$;

-- Keep repair, invariant verification, and trigger installation in one DML
-- exclusion window. The order matches every runtime egress writer.
LOCK TABLE egress_routes, accounts, account_egress_bindings
IN SHARE ROW EXCLUSIVE MODE;

CREATE TEMP TABLE account_egress_direct_repair (
    account_id BIGINT PRIMARY KEY,
    repair_kind TEXT NOT NULL,
    primary_route_id BIGINT,
    primary_proxy_id BIGINT
) ON COMMIT DROP;

INSERT INTO account_egress_direct_repair (
    account_id, repair_kind, primary_route_id, primary_proxy_id
)
SELECT
    account.id,
    CASE WHEN selected.route_id IS NULL THEN 'direct_only' ELSE 'mixed' END,
    selected.route_id,
    selected.proxy_id
FROM accounts account
LEFT JOIN LATERAL (
    SELECT binding.route_id, route.proxy_id
    FROM account_egress_bindings binding
    JOIN egress_routes route ON route.id = binding.route_id
    WHERE binding.account_id = account.id
      AND route.kind = 'proxy'
    ORDER BY binding.is_primary DESC, binding.position, binding.route_id
    LIMIT 1
) selected ON TRUE
WHERE account.egress_mode = 'pool'
  AND account.deleted_at IS NULL
  AND EXISTS (
      SELECT 1
      FROM account_egress_bindings binding
      JOIN egress_routes route ON route.id = binding.route_id
      WHERE binding.account_id = account.id
        AND route.kind = 'direct'
  );

-- Global lifecycle order: routes first, then repaired roots and shadows by ID.
SELECT route.id
FROM egress_routes route
WHERE route.id IN (
    SELECT binding.route_id
    FROM account_egress_bindings binding
    JOIN account_egress_direct_repair repair ON repair.account_id = binding.account_id
)
ORDER BY route.id
FOR UPDATE;

SELECT account.id
FROM accounts account
WHERE account.id IN (SELECT account_id FROM account_egress_direct_repair)
   OR account.parent_account_id IN (SELECT account_id FROM account_egress_direct_repair)
ORDER BY account.id
FOR UPDATE;

DELETE FROM account_egress_bindings binding
USING account_egress_direct_repair repair, egress_routes route
WHERE repair.account_id = binding.account_id
  AND repair.repair_kind = 'mixed'
  AND route.id = binding.route_id
  AND route.kind = 'direct';

UPDATE account_egress_bindings binding
SET is_primary = FALSE,
    updated_at = NOW()
FROM account_egress_direct_repair repair
WHERE repair.account_id = binding.account_id
  AND repair.repair_kind = 'mixed';

UPDATE account_egress_bindings binding
SET is_primary = TRUE,
    updated_at = NOW()
FROM account_egress_direct_repair repair
WHERE repair.account_id = binding.account_id
  AND repair.repair_kind = 'mixed'
  AND repair.primary_route_id = binding.route_id;

-- Move positions above every old value before compacting to avoid immediate
-- uniqueness checks observing a transient account_id/position collision.
WITH staged AS (
    SELECT
        binding.account_id,
        binding.route_id,
        MAX(binding.position) OVER (PARTITION BY binding.account_id)
            + ROW_NUMBER() OVER (
                PARTITION BY binding.account_id
                ORDER BY binding.position, binding.route_id
              ) AS temporary_position
    FROM account_egress_bindings binding
    JOIN account_egress_direct_repair repair ON repair.account_id = binding.account_id
    WHERE repair.repair_kind = 'mixed'
)
UPDATE account_egress_bindings binding
SET position = staged.temporary_position,
    updated_at = NOW()
FROM staged
WHERE binding.account_id = staged.account_id
  AND binding.route_id = staged.route_id;

WITH ranked AS (
    SELECT
        binding.account_id,
        binding.route_id,
        ROW_NUMBER() OVER (
            PARTITION BY binding.account_id
            ORDER BY binding.position, binding.route_id
        ) - 1 AS final_position
    FROM account_egress_bindings binding
    JOIN account_egress_direct_repair repair ON repair.account_id = binding.account_id
    WHERE repair.repair_kind = 'mixed'
)
UPDATE account_egress_bindings binding
SET position = ranked.final_position,
    updated_at = NOW()
FROM ranked
WHERE binding.account_id = ranked.account_id
  AND binding.route_id = ranked.route_id;

UPDATE accounts account
SET egress_mode = CASE
        WHEN repair.repair_kind = 'direct_only' THEN 'legacy'
        ELSE account.egress_mode
    END,
    proxy_id = repair.primary_proxy_id,
    proxy_fallback_origin_id = NULL,
    egress_revision = account.egress_revision + 1,
    updated_at = NOW()
FROM account_egress_direct_repair repair
WHERE account.id = repair.account_id;

UPDATE accounts shadow
SET proxy_id = repair.primary_proxy_id,
    proxy_fallback_origin_id = NULL,
    egress_revision = shadow.egress_revision + 1,
    updated_at = NOW()
FROM account_egress_direct_repair repair
WHERE shadow.parent_account_id = repair.account_id
  AND shadow.deleted_at IS NULL;

WITH affected AS (
    SELECT repair.account_id AS id
    FROM account_egress_direct_repair repair
    UNION
    SELECT shadow.id
    FROM accounts shadow
    JOIN account_egress_direct_repair repair
      ON repair.account_id = shadow.parent_account_id
    WHERE shadow.deleted_at IS NULL
), payload AS (
    SELECT jsonb_build_object('account_ids', to_jsonb(array_agg(id ORDER BY id))) AS body
    FROM affected
    HAVING COUNT(*) > 0
)
INSERT INTO scheduler_outbox (event_type, payload)
SELECT 'account_bulk_changed', body
FROM payload;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM accounts account
        JOIN account_egress_bindings binding ON binding.account_id = account.id
        JOIN egress_routes route ON route.id = binding.route_id
        WHERE account.egress_mode = 'pool'
          AND account.deleted_at IS NULL
          AND route.kind = 'direct'
    ) THEN
        RAISE EXCEPTION 'proxy-only egress repair incomplete: pool direct binding remains';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_pool_binding_proxy_only()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    route_ids BIGINT[] := ARRAY[NEW.route_id];
    account_ids BIGINT[] := ARRAY[NEW.account_id];
    locked_route_kind TEXT;
    locked_account_mode TEXT;
    locked_account_deleted_at TIMESTAMPTZ;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        route_ids := route_ids || OLD.route_id;
        account_ids := account_ids || OLD.account_id;
    END IF;

    -- Canonical constraint order for binding writers: route, then account.
    -- The final SELECT runs after both conflicting writers have serialized.
    PERFORM route.id
    FROM egress_routes route
    WHERE route.id = ANY(route_ids)
    ORDER BY route.id
    FOR SHARE;

    PERFORM account.id
    FROM accounts account
    WHERE account.id = ANY(account_ids)
    ORDER BY account.id
    FOR NO KEY UPDATE;

    SELECT route.kind, account.egress_mode, account.deleted_at
      INTO locked_route_kind, locked_account_mode, locked_account_deleted_at
      FROM egress_routes route, accounts account
     WHERE route.id = NEW.route_id
       AND account.id = NEW.account_id;

    IF locked_account_mode = 'pool'
       AND locked_account_deleted_at IS NULL
       AND locked_route_kind = 'direct' THEN
        RAISE EXCEPTION 'pool account % cannot bind direct route %', NEW.account_id, NEW.route_id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_pool_account_proxy_only()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- UPDATE already owns NEW.id. Binding and route writers that might have
    -- raced this check must acquire that same account lock before committing.
    IF NEW.egress_mode = 'pool'
       AND NEW.deleted_at IS NULL
       AND EXISTS (
        SELECT 1
        FROM account_egress_bindings binding
        JOIN egress_routes route ON route.id = binding.route_id
        WHERE binding.account_id = NEW.id
          AND route.kind = 'direct'
    ) THEN
        RAISE EXCEPTION 'account % cannot enter pool mode with a direct binding', NEW.id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_route_proxy_only_for_pool_accounts()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    -- The route row is already locked by UPDATE. Lock every referencing active
    -- account in ID order so route and binding writers share route->account.
    PERFORM account.id
    FROM accounts account
    JOIN account_egress_bindings binding ON binding.account_id = account.id
    WHERE binding.route_id = OLD.id
    ORDER BY account.id
    FOR NO KEY UPDATE OF account;

    IF NEW.kind = 'direct' AND EXISTS (
        SELECT 1
        FROM account_egress_bindings binding
        JOIN accounts account ON account.id = binding.account_id
        WHERE binding.route_id = OLD.id
          AND account.egress_mode = 'pool'
          AND account.deleted_at IS NULL
    ) THEN
        RAISE EXCEPTION 'route % cannot become direct while referenced by a pool account', OLD.id
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS account_egress_bindings_proxy_only ON account_egress_bindings;
CREATE TRIGGER account_egress_bindings_proxy_only
BEFORE INSERT OR UPDATE OF account_id, route_id ON account_egress_bindings
FOR EACH ROW EXECUTE FUNCTION enforce_pool_binding_proxy_only();

DROP TRIGGER IF EXISTS accounts_egress_pool_proxy_only ON accounts;
CREATE TRIGGER accounts_egress_pool_proxy_only
BEFORE INSERT OR UPDATE OF egress_mode, deleted_at ON accounts
FOR EACH ROW EXECUTE FUNCTION enforce_pool_account_proxy_only();

DROP TRIGGER IF EXISTS egress_routes_proxy_only_for_pool_accounts ON egress_routes;
CREATE TRIGGER egress_routes_proxy_only_for_pool_accounts
BEFORE UPDATE OF kind, proxy_id, runtime_scope ON egress_routes
FOR EACH ROW EXECUTE FUNCTION enforce_route_proxy_only_for_pool_accounts();
