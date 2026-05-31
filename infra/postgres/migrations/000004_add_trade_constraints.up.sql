DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'trades_quantity_positive') THEN
        ALTER TABLE trades ADD CONSTRAINT trades_quantity_positive CHECK (quantity > 0);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'trades_price_positive') THEN
        ALTER TABLE trades ADD CONSTRAINT trades_price_positive CHECK (price > 0);
    END IF;
END $$;
