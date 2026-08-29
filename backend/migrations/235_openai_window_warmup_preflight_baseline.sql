-- Persist the authoritative five-hour observation that immediately precedes
-- each synthetic warmup send. These nullable columns are additive so older
-- binaries continue to operate during a migration-first rolling deployment.
ALTER TABLE openai_window_warmup_jobs
    ADD COLUMN IF NOT EXISTS preflight_reset_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS preflight_observed_at TIMESTAMPTZ;

COMMENT ON COLUMN openai_window_warmup_jobs.preflight_reset_at IS
    'Authoritative five-hour reset observed immediately before the current or last synthetic send';

COMMENT ON COLUMN openai_window_warmup_jobs.preflight_observed_at IS
    'Timestamp at which the current or last synthetic send passed its fenced preflight check';
