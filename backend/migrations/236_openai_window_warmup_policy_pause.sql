-- Synchronize durable warmup jobs with explicit account policy changes.
-- Unsent work can be paused immediately, while a row with evidence that a
-- synthetic POST may have started must retain the uncertain replay fence.
SET LOCAL lock_timeout = '3s';

CREATE OR REPLACE FUNCTION public.openai_window_warmup_sync_policy_state()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    old_policy TEXT;
    new_policy TEXT;
BEGIN
    old_policy := lower(trim(COALESCE(
        CASE WHEN jsonb_typeof(OLD.extra -> 'openai_codex_warmup_policy') = 'string'
            THEN NULLIF(trim(OLD.extra ->> 'openai_codex_warmup_policy'), '') END,
        CASE WHEN jsonb_typeof(OLD.extra -> 'codex_warmup_policy') = 'string'
            THEN NULLIF(trim(OLD.extra ->> 'codex_warmup_policy'), '') END,
        CASE WHEN jsonb_typeof(OLD.extra -> 'openai_window_warmup_policy') = 'string'
            THEN NULLIF(trim(OLD.extra ->> 'openai_window_warmup_policy'), '') END,
        'off'
    )));
    new_policy := lower(trim(COALESCE(
        CASE WHEN jsonb_typeof(NEW.extra -> 'openai_codex_warmup_policy') = 'string'
            THEN NULLIF(trim(NEW.extra ->> 'openai_codex_warmup_policy'), '') END,
        CASE WHEN jsonb_typeof(NEW.extra -> 'codex_warmup_policy') = 'string'
            THEN NULLIF(trim(NEW.extra ->> 'codex_warmup_policy'), '') END,
        CASE WHEN jsonb_typeof(NEW.extra -> 'openai_window_warmup_policy') = 'string'
            THEN NULLIF(trim(NEW.extra ->> 'openai_window_warmup_policy'), '') END,
        'off'
    )));

    IF new_policy NOT IN ('initial_once', 'continuous') THEN
        UPDATE openai_window_warmup_jobs
        SET state = CASE
                WHEN sent_at IS NOT NULL OR state IN ('uncertain', 'possibly_sent') THEN 'uncertain'
                ELSE 'paused'
            END,
            next_attempt_at = NOW(),
            last_error_code = CASE
                WHEN sent_at IS NOT NULL OR state IN ('uncertain', 'possibly_sent')
                    THEN COALESCE(NULLIF(last_error_code, ''), 'policy_disabled_after_send')
                ELSE 'policy_disabled'
            END,
            last_error = NULL,
            uncertain_observed_reset_at = CASE
                WHEN sent_at IS NOT NULL OR state IN ('uncertain', 'possibly_sent')
                    THEN uncertain_observed_reset_at
                ELSE NULL
            END,
            uncertain_observed_at = CASE
                WHEN sent_at IS NOT NULL OR state IN ('uncertain', 'possibly_sent')
                    THEN uncertain_observed_at
                ELSE NULL
            END,
            uncertain_terminal_observed = CASE
                WHEN sent_at IS NOT NULL OR state IN ('uncertain', 'possibly_sent')
                    THEN uncertain_terminal_observed
                ELSE FALSE
            END,
            lease_owner = NULL,
            lease_token = NULL,
            lease_until = NULL,
            updated_at = NOW()
        WHERE account_id = NEW.id
          AND state IN ('pending', 'armed', 'due', 'running', 'retrying', 'uncertain', 'possibly_sent');
        RETURN NEW;
    END IF;

    -- The enqueue trigger handles new cycles. On an explicit off -> enabled
    -- transition, revive at most one older paused cycle. A paused row with send
    -- evidence becomes uncertain and is due only for passive reconciliation.
    IF old_policy NOT IN ('initial_once', 'continuous')
       AND NEW.platform::text = 'openai'
       AND NEW.type::text = 'oauth'
       AND NEW.parent_account_id IS NULL
       AND COALESCE(NEW.quota_dimension::text, 'global') = 'global'
       AND NEW.status::text = 'active'
       AND NEW.schedulable
       AND (NEW.temp_unschedulable_until IS NULL OR NEW.temp_unschedulable_until <= NOW())
       AND NEW.deleted_at IS NULL
       AND (NEW.expires_at IS NULL OR NEW.expires_at > NOW()) THEN
        WITH target AS MATERIALIZED (
            SELECT id, sent_at IS NOT NULL AS may_have_sent
            FROM openai_window_warmup_jobs
            WHERE account_id = NEW.id
              AND quota_scope = 'global'
              AND state = 'paused'
            ORDER BY (sent_at IS NOT NULL) DESC, id DESC
            LIMIT 1
            FOR UPDATE
        )
        UPDATE openai_window_warmup_jobs AS job
        SET state = CASE WHEN target.may_have_sent THEN 'uncertain' ELSE 'pending' END,
            next_attempt_at = NOW(),
            last_error_code = CASE
                WHEN target.may_have_sent
                    THEN COALESCE(NULLIF(job.last_error_code, ''), 'policy_reenabled_after_send')
                ELSE NULL
            END,
            last_error = NULL,
            uncertain_observed_reset_at = CASE
                WHEN target.may_have_sent THEN job.uncertain_observed_reset_at ELSE NULL
            END,
            uncertain_observed_at = CASE
                WHEN target.may_have_sent THEN job.uncertain_observed_at ELSE NULL
            END,
            uncertain_terminal_observed = CASE
                WHEN target.may_have_sent THEN job.uncertain_terminal_observed ELSE FALSE
            END,
            lease_owner = NULL,
            lease_token = NULL,
            lease_until = NULL,
            updated_at = NOW()
        FROM target
        WHERE job.id = target.id
          AND NOT EXISTS (
              SELECT 1
              FROM openai_window_warmup_jobs AS active
              WHERE active.account_id = NEW.id
                AND active.quota_scope = 'global'
                AND active.id <> job.id
                AND active.state IN ('pending', 'armed', 'due', 'running', 'retrying', 'uncertain', 'possibly_sent')
          );
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_openai_window_warmup_sync_policy_state ON accounts;
CREATE TRIGGER trg_openai_window_warmup_sync_policy_state
    AFTER UPDATE OF extra ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION public.openai_window_warmup_sync_policy_state();

-- Bring rows created before this trigger under the same policy fence. The
-- partial unique index guarantees at most one active row per account/scope.
UPDATE openai_window_warmup_jobs AS job
SET state = CASE
        WHEN job.sent_at IS NOT NULL OR job.state IN ('uncertain', 'possibly_sent') THEN 'uncertain'
        ELSE 'paused'
    END,
    next_attempt_at = NOW(),
    last_error_code = CASE
        WHEN job.sent_at IS NOT NULL OR job.state IN ('uncertain', 'possibly_sent')
            THEN COALESCE(NULLIF(job.last_error_code, ''), 'policy_disabled_after_send')
        ELSE 'policy_disabled'
    END,
    last_error = NULL,
    uncertain_observed_reset_at = CASE
        WHEN job.sent_at IS NOT NULL OR job.state IN ('uncertain', 'possibly_sent')
            THEN job.uncertain_observed_reset_at
        ELSE NULL
    END,
    uncertain_observed_at = CASE
        WHEN job.sent_at IS NOT NULL OR job.state IN ('uncertain', 'possibly_sent')
            THEN job.uncertain_observed_at
        ELSE NULL
    END,
    uncertain_terminal_observed = CASE
        WHEN job.sent_at IS NOT NULL OR job.state IN ('uncertain', 'possibly_sent')
            THEN job.uncertain_terminal_observed
        ELSE FALSE
    END,
    lease_owner = NULL,
    lease_token = NULL,
    lease_until = NULL,
    updated_at = NOW()
FROM accounts AS account
WHERE job.account_id = account.id
  AND job.state IN ('pending', 'armed', 'due', 'running', 'retrying', 'uncertain', 'possibly_sent')
  AND lower(trim(COALESCE(
      CASE WHEN jsonb_typeof(account.extra -> 'openai_codex_warmup_policy') = 'string'
          THEN NULLIF(trim(account.extra ->> 'openai_codex_warmup_policy'), '') END,
      CASE WHEN jsonb_typeof(account.extra -> 'codex_warmup_policy') = 'string'
          THEN NULLIF(trim(account.extra ->> 'codex_warmup_policy'), '') END,
      CASE WHEN jsonb_typeof(account.extra -> 'openai_window_warmup_policy') = 'string'
          THEN NULLIF(trim(account.extra ->> 'openai_window_warmup_policy'), '') END,
      'off'
  ))) NOT IN ('initial_once', 'continuous');

-- Older binaries could pause a row after MarkStarted. Repair such rows only
-- when no other active cycle exists, preserving the one-active-row invariant.
UPDATE openai_window_warmup_jobs AS job
SET state = 'uncertain',
    next_attempt_at = NOW(),
    last_error_code = COALESCE(NULLIF(job.last_error_code, ''), 'policy_disabled_after_send'),
    last_error = NULL,
    lease_owner = NULL,
    lease_token = NULL,
    lease_until = NULL,
    updated_at = NOW()
WHERE job.state = 'paused'
  AND job.sent_at IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM openai_window_warmup_jobs AS active
      WHERE active.account_id = job.account_id
        AND active.quota_scope = job.quota_scope
        AND active.id <> job.id
        AND active.state IN ('pending', 'armed', 'due', 'running', 'retrying', 'uncertain', 'possibly_sent')
  );
