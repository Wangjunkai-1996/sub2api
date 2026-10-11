-- Retain warmup attempt evidence for seven days after completion. Keep an
-- earlier explicit expiry intact while tightening rows created under the old
-- 90-day default.
SET LOCAL lock_timeout = '3s';

ALTER TABLE openai_window_warmup_attempts
    ALTER COLUMN expires_at SET DEFAULT (NOW() + INTERVAL '7 days');

UPDATE openai_window_warmup_attempts
SET expires_at = finished_at + INTERVAL '7 days'
WHERE expires_at > finished_at + INTERVAL '7 days';
