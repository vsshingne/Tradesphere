ALTER TABLE trade_outbox
    ADD COLUMN IF NOT EXISTS publish_attempts INTEGER NOT NULL DEFAULT 0;

ALTER TABLE trade_outbox
    ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMP NOT NULL DEFAULT NOW();

ALTER TABLE trade_outbox
    ADD COLUMN IF NOT EXISTS claimed_by TEXT NULL;

ALTER TABLE trade_outbox
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP NULL;

ALTER TABLE trade_outbox
    ADD COLUMN IF NOT EXISTS last_error TEXT NULL;

CREATE INDEX IF NOT EXISTS idx_trade_outbox_publishable
    ON trade_outbox (published_at, next_attempt_at, created_at);
