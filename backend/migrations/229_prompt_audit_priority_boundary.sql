-- Keep the audit chunk-priority boundary out of client-controlled prompt text.
-- Existing jobs default to zero and are scanned without a priority boundary.
ALTER TABLE IF EXISTS prompt_audit_jobs
    ADD COLUMN IF NOT EXISTS priority_prefix_runes INT NOT NULL DEFAULT 0;

ALTER TABLE IF EXISTS prompt_audit_jobs
    DROP CONSTRAINT IF EXISTS chk_prompt_audit_jobs_priority_prefix_runes;

ALTER TABLE IF EXISTS prompt_audit_jobs
    ADD CONSTRAINT chk_prompt_audit_jobs_priority_prefix_runes
    CHECK (priority_prefix_runes >= 0 AND priority_prefix_runes <= prompt_length);
