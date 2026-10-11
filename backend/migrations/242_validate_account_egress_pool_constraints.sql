-- Validate the account checks separately from their low-lock installation.
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE accounts VALIDATE CONSTRAINT accounts_egress_mode_check;
ALTER TABLE accounts VALIDATE CONSTRAINT accounts_egress_revision_check;
ALTER TABLE accounts VALIDATE CONSTRAINT accounts_pool_concurrency_check;
