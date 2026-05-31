-- =====================================================
-- EXTENSIONS
-- =====================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- =====================================================
-- USERS
-- Monetary values stored in smallest unit
-- Example:
-- ₹100.25 = 10025 paise
-- =====================================================

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,

    email TEXT UNIQUE,
    password_hash TEXT,
    role TEXT DEFAULT 'user',

    balance BIGINT NOT NULL DEFAULT 0,
    reserved_balance BIGINT NOT NULL DEFAULT 0,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT users_balance_non_negative
        CHECK (balance >= 0),

    CONSTRAINT users_reserved_balance_non_negative
        CHECK (reserved_balance >= 0)
);

-- =====================================================
-- REFRESH TOKENS
-- =====================================================

CREATE TABLE IF NOT EXISTS refresh_tokens (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    token_hash TEXT NOT NULL UNIQUE,
    expires_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMP,

    CONSTRAINT fk_refresh_tokens_user
        FOREIGN KEY (user_id)
        REFERENCES users(id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user_id ON refresh_tokens(user_id);
CREATE INDEX IF NOT EXISTS idx_refresh_tokens_hash ON refresh_tokens(token_hash);

-- =====================================================
-- POSITIONS
-- Quantity uses fixed precision:
-- 1.5 => 150000000
-- =====================================================

CREATE TABLE IF NOT EXISTS positions (
    user_id UUID NOT NULL,
    symbol TEXT NOT NULL,

    quantity BIGINT NOT NULL DEFAULT 0,
    reserved_quantity BIGINT NOT NULL DEFAULT 0,

    PRIMARY KEY (user_id, symbol),

    CONSTRAINT fk_positions_user
        FOREIGN KEY (user_id)
        REFERENCES users(id)
        ON DELETE CASCADE,

    CONSTRAINT positions_quantity_non_negative
        CHECK (quantity >= 0),

    CONSTRAINT positions_reserved_quantity_non_negative
        CHECK (reserved_quantity >= 0)
);

-- =====================================================
-- ORDERS
-- price stored in paise
-- quantity stored in precision units
-- =====================================================

CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,

    user_id UUID NOT NULL,
    symbol TEXT NOT NULL,

    side TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'LIMIT',

    price BIGINT NOT NULL,
    quantity BIGINT NOT NULL,
    remaining_quantity BIGINT NOT NULL,

    reserved_amount BIGINT NOT NULL DEFAULT 0,

    status TEXT NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_orders_user
        FOREIGN KEY (user_id)
        REFERENCES users(id)
        ON DELETE CASCADE,

    CONSTRAINT orders_price_positive
        CHECK (price >= 0),

    CONSTRAINT orders_quantity_positive
        CHECK (quantity > 0),

    CONSTRAINT orders_remaining_quantity_non_negative
        CHECK (remaining_quantity >= 0),

    CONSTRAINT orders_reserved_amount_non_negative
        CHECK (reserved_amount >= 0),

    CONSTRAINT orders_status_valid
        CHECK (
            status IN (
                'NEW',
                'PARTIALLY_FILLED',
                'FILLED',
                'CANCELLED'
            )
        ),

    CONSTRAINT orders_side_valid
        CHECK (
            side IN (
                'BUY',
                'SELL'
            )
        ),

    CONSTRAINT orders_type_valid
        CHECK (
            type IN (
                'LIMIT',
                'MARKET'
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_orders_symbol
ON orders(symbol);

CREATE INDEX IF NOT EXISTS idx_orders_status
ON orders(status);

-- =====================================================
-- TRADES
-- =====================================================

CREATE TABLE IF NOT EXISTS trades (
    id UUID PRIMARY KEY,

    symbol TEXT NOT NULL,

    buyer_user_id UUID NOT NULL,
    seller_user_id UUID NOT NULL,

    buy_order_id UUID NOT NULL,
    sell_order_id UUID NOT NULL,

    price BIGINT NOT NULL,
    quantity BIGINT NOT NULL,

    executed_at TIMESTAMP NOT NULL,

    CONSTRAINT fk_trades_buyer
        FOREIGN KEY (buyer_user_id)
        REFERENCES users(id),

    CONSTRAINT fk_trades_seller
        FOREIGN KEY (seller_user_id)
        REFERENCES users(id),

    CONSTRAINT fk_trades_buy_order
        FOREIGN KEY (buy_order_id)
        REFERENCES orders(id),

    CONSTRAINT fk_trades_sell_order
        FOREIGN KEY (sell_order_id)
        REFERENCES orders(id),

    CONSTRAINT trades_price_positive
        CHECK (price > 0),

    CONSTRAINT trades_quantity_positive
        CHECK (quantity > 0)
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol
ON trades(symbol);

CREATE INDEX IF NOT EXISTS idx_trades_executed_at
ON trades(executed_at);

-- =====================================================
-- ORDER OUTBOX
-- =====================================================

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
ON order_outbox(published_at, created_at);

CREATE INDEX IF NOT EXISTS idx_order_outbox_publishable
    ON order_outbox (published_at, next_attempt_at, created_at);

-- =====================================================
-- TRADE OUTBOX
-- =====================================================

CREATE TABLE IF NOT EXISTS trade_outbox (
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

CREATE INDEX IF NOT EXISTS idx_trade_outbox_unpublished
ON trade_outbox(published_at, created_at);

CREATE INDEX IF NOT EXISTS idx_trade_outbox_publishable
    ON trade_outbox (published_at, next_attempt_at, created_at);

-- =====================================================
-- IDEMPOTENCY
-- =====================================================

CREATE TABLE IF NOT EXISTS processed_events (
    consumer_group TEXT NOT NULL,
    event_id UUID NOT NULL,

    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (
        consumer_group,
        event_id
    )
);

-- =====================================================
-- SEED USERS
-- 10000000 = ₹100000.00
-- =====================================================

INSERT INTO users (
    id,
    email,
    password_hash,
    role,
    balance
)
VALUES
(
    '11111111-1111-1111-1111-111111111111',
    'user1@example.com',
    '$2a$10$Y3ScnMmM9dd9R5mY/Eolf.OBpHxk1htdXLqZFwv/UbGskYmr3njc2', -- 'password'
    'user',
    1000000000000000 -- 10,000,000.00000000
),
(
    '22222222-2222-2222-2222-222222222222',
    'user2@example.com',
    '$2a$10$Y3ScnMmM9dd9R5mY/Eolf.OBpHxk1htdXLqZFwv/UbGskYmr3njc2', -- 'password'
    'user',
    1000000000000000 -- 10,000,000.00000000
)
ON CONFLICT DO NOTHING;

INSERT INTO positions (
    user_id,
    symbol,
    quantity
)
VALUES
(
    '22222222-2222-2222-2222-222222222222',
    'BTC',
    10000000000 -- 100.00000000
)
ON CONFLICT DO NOTHING;