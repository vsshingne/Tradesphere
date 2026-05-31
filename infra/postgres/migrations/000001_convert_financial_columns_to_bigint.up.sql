DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'users'
          AND column_name = 'balance'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE users
            ALTER COLUMN balance TYPE BIGINT
            USING ROUND(balance * 100000000)::BIGINT;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'users'
          AND column_name = 'reserved_balance'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE users
            ALTER COLUMN reserved_balance TYPE BIGINT
            USING ROUND(reserved_balance * 100000000)::BIGINT;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'positions'
          AND column_name = 'quantity'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE positions
            ALTER COLUMN quantity TYPE BIGINT
            USING ROUND(quantity * 100000000)::BIGINT;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'positions'
          AND column_name = 'reserved_quantity'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE positions
            ALTER COLUMN reserved_quantity TYPE BIGINT
            USING ROUND(reserved_quantity * 100000000)::BIGINT;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'orders'
          AND column_name = 'price'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE orders
            ALTER COLUMN price TYPE BIGINT
            USING ROUND(price * 100000000)::BIGINT;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'orders'
          AND column_name = 'quantity'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE orders
            ALTER COLUMN quantity TYPE BIGINT
            USING ROUND(quantity * 100000000)::BIGINT;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'orders'
          AND column_name = 'remaining_quantity'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE orders
            ALTER COLUMN remaining_quantity TYPE BIGINT
            USING ROUND(remaining_quantity * 100000000)::BIGINT;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'orders'
          AND column_name = 'reserved_amount'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE orders
            ALTER COLUMN reserved_amount TYPE BIGINT
            USING ROUND(reserved_amount * 100000000)::BIGINT;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'trades'
          AND column_name = 'price'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE trades
            ALTER COLUMN price TYPE BIGINT
            USING ROUND(price * 100000000)::BIGINT;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'trades'
          AND column_name = 'quantity'
          AND data_type <> 'bigint'
    ) THEN
        ALTER TABLE trades
            ALTER COLUMN quantity TYPE BIGINT
            USING ROUND(quantity * 100000000)::BIGINT;
    END IF;
END $$;
