CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    balance DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS positions (
    user_id UUID NOT NULL,
    symbol TEXT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, symbol),
    CONSTRAINT fk_positions_user
        FOREIGN KEY (user_id)
        REFERENCES users(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS trades (
    id UUID PRIMARY KEY,
    symbol TEXT NOT NULL,
    buyer_user_id UUID NOT NULL,
    seller_user_id UUID NOT NULL,
    buy_order_id UUID NOT NULL,
    sell_order_id UUID NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    executed_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_trades_buyer
        FOREIGN KEY (buyer_user_id)
        REFERENCES users(id),
    CONSTRAINT fk_trades_seller
        FOREIGN KEY (seller_user_id)
        REFERENCES users(id)
);

INSERT INTO users (id, balance)
VALUES
('11111111-1111-1111-1111-111111111111', 100000),
('22222222-2222-2222-2222-222222222222', 100000)
ON CONFLICT DO NOTHING;

-- USERS
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    balance DOUBLE PRECISION NOT NULL DEFAULT 0,
    reserved_balance DOUBLE PRECISION NOT NULL DEFAULT 0
);

-- POSITIONS
CREATE TABLE IF NOT EXISTS positions (
    user_id UUID NOT NULL,
    symbol TEXT NOT NULL,
    quantity DOUBLE PRECISION NOT NULL DEFAULT 0,
    reserved_quantity DOUBLE PRECISION NOT NULL DEFAULT 0,
    PRIMARY KEY (user_id, symbol)
);

-- ORDERS
CREATE TABLE IF NOT EXISTS orders (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    remaining_quantity DOUBLE PRECISION NOT NULL,
    reserved_amount DOUBLE PRECISION NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- TRADES
CREATE TABLE IF NOT EXISTS trades (
    id UUID PRIMARY KEY,
    symbol TEXT NOT NULL,
    buyer_user_id UUID NOT NULL,
    seller_user_id UUID NOT NULL,
    buy_order_id UUID NOT NULL,
    sell_order_id UUID NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    quantity DOUBLE PRECISION NOT NULL,
    executed_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS trade_outbox (
    id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    event_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    published_at TIMESTAMP NULL
);

CREATE INDEX IF NOT EXISTS idx_trade_outbox_unpublished
    ON trade_outbox (published_at, created_at);

CREATE TABLE IF NOT EXISTS processed_events (
    consumer_group TEXT NOT NULL,
    event_id UUID NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, event_id)
);

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS reserved_balance DOUBLE PRECISION NOT NULL DEFAULT 0;

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS reserved_quantity DOUBLE PRECISION NOT NULL DEFAULT 0;

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS reserved_amount DOUBLE PRECISION NOT NULL DEFAULT 0;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'users_balance_non_negative') THEN
        ALTER TABLE users ADD CONSTRAINT users_balance_non_negative CHECK (balance >= 0);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'users_reserved_balance_non_negative') THEN
        ALTER TABLE users ADD CONSTRAINT users_reserved_balance_non_negative CHECK (reserved_balance >= 0);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'positions_quantity_non_negative') THEN
        ALTER TABLE positions ADD CONSTRAINT positions_quantity_non_negative CHECK (quantity >= 0);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'positions_reserved_quantity_non_negative') THEN
        ALTER TABLE positions ADD CONSTRAINT positions_reserved_quantity_non_negative CHECK (reserved_quantity >= 0);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'orders_remaining_quantity_non_negative') THEN
        ALTER TABLE orders ADD CONSTRAINT orders_remaining_quantity_non_negative CHECK (remaining_quantity >= 0);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'orders_reserved_amount_non_negative') THEN
        ALTER TABLE orders ADD CONSTRAINT orders_reserved_amount_non_negative CHECK (reserved_amount >= 0);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'orders_status_valid') THEN
        ALTER TABLE orders ADD CONSTRAINT orders_status_valid CHECK (status IN ('NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED'));
    END IF;
END $$;
