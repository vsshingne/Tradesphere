CREATE TABLE IF NOT EXISTS order_outbox (
    id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    event_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP NULL,
    publish_attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMP NOT NULL DEFAULT NOW(),
    claimed_by TEXT NULL,
    claimed_at TIMESTAMP NULL,
    last_error TEXT NULL
);

CREATE INDEX IF NOT EXISTS idx_order_outbox_unpublished
    ON order_outbox (published_at, created_at);

CREATE INDEX IF NOT EXISTS idx_order_outbox_publishable
    ON order_outbox (published_at, next_attempt_at, created_at);
