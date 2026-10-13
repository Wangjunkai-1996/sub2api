-- Purge retired private account material separately from table-locking DDL.
SET LOCAL lock_timeout = '3s';
SET LOCAL statement_timeout = '90s';

UPDATE accounts AS account
SET extra = account.extra - ARRAY(
    SELECT key
    FROM jsonb_object_keys(CASE WHEN jsonb_typeof(account.extra) = 'object'
        THEN account.extra ELSE '{}'::jsonb END) AS key
    WHERE starts_with(key, 'codex_turn_ticket:')
       OR key IN (
           'codex_ticket_config', 'codex_ticket_watchdog',
           'codex_harvest_proxy_url', 'openai_codex_warmup_policy',
           'codex_warmup_policy', 'openai_window_warmup_policy'
       )
)
WHERE jsonb_typeof(account.extra) = 'object'
  AND (account.extra ?| ARRAY[
    'codex_ticket_config', 'codex_ticket_watchdog',
    'codex_harvest_proxy_url', 'openai_codex_warmup_policy',
    'codex_warmup_policy', 'openai_window_warmup_policy'
] OR EXISTS (
    SELECT 1 FROM jsonb_object_keys(CASE WHEN jsonb_typeof(account.extra) = 'object'
        THEN account.extra ELSE '{}'::jsonb END) AS key
    WHERE starts_with(key, 'codex_turn_ticket:')
));

DELETE FROM settings
WHERE key IN (
    'openai_codex_ticket_332_enabled',
    'openai_codex_ticket_enabled',
    'openai_codex_ticket_harvest_proxy_url',
    'openai_codex_ticket_v2_enabled',
    'openai_window_warmup_allowlist',
    'openai_window_warmup_batch_size',
    'openai_window_warmup_default_policy',
    'openai_window_warmup_enabled',
    'openai_window_warmup_global_qps',
    'openai_window_warmup_lease_seconds',
    'openai_window_warmup_probe_model',
    'openai_window_warmup_request_timeout_seconds',
    'openai_window_warmup_reset_grace_seconds',
    'openai_window_warmup_scan_seconds',
    'openai_window_warmup_worker_concurrency'
);
