-- Manual production migrations may run as a dedicated migrator instead of the
-- application role. Grant the runtime role that owns the existing accounts
-- table access to the egress objects, and preserve that policy for future
-- objects created by the same migrator.
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

DO $$
DECLARE
    runtime_role name;
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

    EXECUTE format(
        'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE '
        'public.egress_identities, public.egress_routes, public.account_egress_bindings TO %I',
        runtime_role
    );
    EXECUTE format(
        'GRANT USAGE, SELECT, UPDATE ON SEQUENCE '
        'public.egress_identities_id_seq, public.egress_routes_id_seq TO %I',
        runtime_role
    );

    EXECUTE format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
        'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %I',
        current_user,
        runtime_role
    );
    EXECUTE format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public '
        'GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO %I',
        current_user,
        runtime_role
    );
END;
$$;
