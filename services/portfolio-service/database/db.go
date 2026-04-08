package database

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"tradesphere/portfolio/model"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

var DB *sql.DB

var ErrEventAlreadyProcessed = errors.New("event already processed")

type OrderReservation struct {
	ID                uuid.UUID
	UserID            uuid.UUID
	Symbol            string
	Side              string
	Price             float64
	RemainingQuantity float64
	ReservedAmount    float64
}

func InitDB() {
	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "postgres"
	}
	connStr := fmt.Sprintf("host=%s port=5432 user=tradesphere password=tradesphere dbname=tradesphere sslmode=disable", host)

	var err error
	DB, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	if err = DB.Ping(); err != nil {
		log.Fatal(err)
	}
	DB.SetMaxOpenConns(10)
	DB.SetMaxIdleConns(5)
	DB.SetConnMaxLifetime(5 * time.Minute)

	_, err = DB.Exec(`
		CREATE TABLE IF NOT EXISTS positions (
			user_id UUID NOT NULL,
			symbol TEXT NOT NULL,
			quantity DOUBLE PRECISION NOT NULL DEFAULT 0,
			PRIMARY KEY (user_id, symbol)
		)
	`)
	if err != nil {
		log.Fatal("failed to ensure positions table:", err)
	}

	_, err = DB.Exec(`
		CREATE TABLE IF NOT EXISTS processed_events (
			consumer_group TEXT NOT NULL,
			event_id UUID NOT NULL,
			processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY (consumer_group, event_id)
		)
	`)
	if err != nil {
		log.Fatal("failed to ensure processed_events table:", err)
	}

	_, err = DB.Exec(`
		ALTER TABLE users
		ADD COLUMN IF NOT EXISTS reserved_balance DOUBLE PRECISION NOT NULL DEFAULT 0
	`)
	if err != nil {
		log.Fatal("failed to ensure users.reserved_balance:", err)
	}

	_, err = DB.Exec(`
		ALTER TABLE positions
		ADD COLUMN IF NOT EXISTS reserved_quantity DOUBLE PRECISION NOT NULL DEFAULT 0
	`)
	if err != nil {
		log.Fatal("failed to ensure positions.reserved_quantity:", err)
	}

	_, err = DB.Exec(`
		ALTER TABLE orders
		ADD COLUMN IF NOT EXISTS reserved_amount DOUBLE PRECISION NOT NULL DEFAULT 0
	`)
	if err != nil {
		log.Fatal("failed to ensure orders.reserved_amount:", err)
	}

	for _, stmt := range []string{
		`DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_constraint WHERE conname = 'users_balance_non_negative'
			) THEN
				ALTER TABLE users ADD CONSTRAINT users_balance_non_negative CHECK (balance >= 0);
			END IF;
		END $$;`,
		`DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_constraint WHERE conname = 'users_reserved_balance_non_negative'
			) THEN
				ALTER TABLE users ADD CONSTRAINT users_reserved_balance_non_negative CHECK (reserved_balance >= 0);
			END IF;
		END $$;`,
		`DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_constraint WHERE conname = 'positions_quantity_non_negative'
			) THEN
				ALTER TABLE positions ADD CONSTRAINT positions_quantity_non_negative CHECK (quantity >= 0);
			END IF;
		END $$;`,
		`DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_constraint WHERE conname = 'positions_reserved_quantity_non_negative'
			) THEN
				ALTER TABLE positions ADD CONSTRAINT positions_reserved_quantity_non_negative CHECK (reserved_quantity >= 0);
			END IF;
		END $$;`,
		`DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_constraint WHERE conname = 'orders_remaining_quantity_non_negative'
			) THEN
				ALTER TABLE orders ADD CONSTRAINT orders_remaining_quantity_non_negative CHECK (remaining_quantity >= 0);
			END IF;
		END $$;`,
		`DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_constraint WHERE conname = 'orders_reserved_amount_non_negative'
			) THEN
				ALTER TABLE orders ADD CONSTRAINT orders_reserved_amount_non_negative CHECK (reserved_amount >= 0);
			END IF;
		END $$;`,
		`DO $$
		BEGIN
			IF NOT EXISTS (
				SELECT 1 FROM pg_constraint WHERE conname = 'orders_status_valid'
			) THEN
				ALTER TABLE orders ADD CONSTRAINT orders_status_valid CHECK (status IN ('NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED'));
			END IF;
		END $$;`,
	} {
		if _, err = DB.Exec(stmt); err != nil {
			log.Fatal("failed to ensure check constraint:", err)
		}
	}

	log.Println("Portfolio DB connected")
}

func IsEventProcessed(tx *sql.Tx, consumerGroup string, eventID uuid.UUID) (bool, error) {
	var processed bool
	err := tx.QueryRow(`
		SELECT EXISTS(
			SELECT 1
			FROM processed_events
			WHERE consumer_group = $1 AND event_id = $2
		)
	`, consumerGroup, eventID).Scan(&processed)
	if err != nil {
		return false, err
	}
	return processed, nil
}

func MarkEventProcessed(tx *sql.Tx, consumerGroup string, eventID uuid.UUID) error {
	res, err := tx.Exec(`
		INSERT INTO processed_events (consumer_group, event_id)
		VALUES ($1, $2)
		ON CONFLICT DO NOTHING
	`, consumerGroup, eventID)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return ErrEventAlreadyProcessed
	}
	return nil
}

func LockAccount(tx *sql.Tx, userID uuid.UUID) error {
	var exists int
	err := tx.QueryRow(`
		SELECT 1
		FROM users
		WHERE id = $1
		FOR UPDATE
	`, userID).Scan(&exists)
	return err
}

func ReleaseOrderReservation(tx *sql.Tx, orderID uuid.UUID) error {
	order, err := LockOrderReservation(tx, orderID)
	if err != nil {
		return err
	}

	if order.ReservedAmount == 0 {
		return nil
	}

	if order.Side == "BUY" {
		res, err := tx.Exec(`
			UPDATE users
			SET reserved_balance = reserved_balance - $1
			WHERE id = $2
			  AND reserved_balance >= $1
		`, order.ReservedAmount, order.UserID)
		if err != nil {
			return err
		}
		if err := requireRowsAffected("cancel buyer reservation", orderID, res); err != nil {
			return err
		}
	} else {
		res, err := tx.Exec(`
			UPDATE positions
			SET reserved_quantity = reserved_quantity - $1
			WHERE user_id = $2
			  AND symbol = $3
			  AND reserved_quantity >= $1
		`, order.ReservedAmount, order.UserID, order.Symbol)
		if err != nil {
			return err
		}
		if err := requireRowsAffected("cancel seller reservation", orderID, res); err != nil {
			return err
		}
	}

	res, err := tx.Exec(`
		UPDATE orders
		SET reserved_amount = 0
		WHERE id = $1
	`, orderID)
	if err != nil {
		return err
	}
	if err := requireRowsAffected("cancel order reservation", orderID, res); err != nil {
		return err
	}

	return nil
}

func LockOrderReservation(tx *sql.Tx, orderID uuid.UUID) (*OrderReservation, error) {
	var order OrderReservation
	err := tx.QueryRow(`
		SELECT id, user_id, symbol, side, price, remaining_quantity, reserved_amount
		FROM orders
		WHERE id = $1
		FOR UPDATE
	`, orderID).Scan(
		&order.ID,
		&order.UserID,
		&order.Symbol,
		&order.Side,
		&order.Price,
		&order.RemainingQuantity,
		&order.ReservedAmount,
	)
	if err != nil {
		return nil, err
	}
	return &order, nil
}

func ApplyBuyerTrade(tx *sql.Tx, trade model.Trade) error {
	total := trade.Price * trade.Quantity
	order, err := LockOrderReservation(tx, trade.BuyOrderID)
	if err != nil {
		return err
	}
	if order.UserID != trade.BuyerUserID || order.Side != "BUY" {
		return errors.New("buyer order reservation mismatch")
	}

	nextReservedAmount := order.RemainingQuantity * order.Price
	if nextReservedAmount < 0 || order.ReservedAmount < nextReservedAmount {
		return errors.New("invalid buyer reserved amount state")
	}
	reservedRelease := order.ReservedAmount - nextReservedAmount
	if reservedRelease < 0 {
		return errors.New("invalid buyer reserved amount release")
	}

	balanceRes, err := tx.Exec(`
		UPDATE users
		SET balance = balance - $1,
		    reserved_balance = reserved_balance - $2
		WHERE id = $3
		  AND reserved_balance >= $2
		  AND balance >= $1
	`, total, reservedRelease, trade.BuyerUserID)
	if err != nil {
		return err
	}
	if err := requireRowsAffected("buyer balance", trade.ID, balanceRes); err != nil {
		return err
	}

	orderRes, err := tx.Exec(`
		UPDATE orders
		SET reserved_amount = $1
		WHERE id = $2
	`, nextReservedAmount, trade.BuyOrderID)
	if err != nil {
		return err
	}
	if err := requireRowsAffected("buyer order reservation", trade.ID, orderRes); err != nil {
		return err
	}

	positionRes, err := tx.Exec(`
		INSERT INTO positions (user_id, symbol, quantity, reserved_quantity)
		VALUES ($1, $2, $3, 0)
		ON CONFLICT (user_id, symbol)
		DO UPDATE SET quantity = positions.quantity + EXCLUDED.quantity
	`, trade.BuyerUserID, trade.Symbol, trade.Quantity)
	if err != nil {
		return err
	}
	if err := requireRowsAffected("buyer position", trade.ID, positionRes); err != nil {
		return err
	}

	return nil
}

func ApplySellerTrade(tx *sql.Tx, trade model.Trade) error {
	total := trade.Price * trade.Quantity
	order, err := LockOrderReservation(tx, trade.SellOrderID)
	if err != nil {
		return err
	}
	if order.UserID != trade.SellerUserID || order.Side != "SELL" {
		return errors.New("seller order reservation mismatch")
	}

	nextReservedAmount := order.RemainingQuantity
	if nextReservedAmount < 0 || order.ReservedAmount < nextReservedAmount {
		return errors.New("invalid seller reserved amount state")
	}
	reservedRelease := order.ReservedAmount - nextReservedAmount
	if reservedRelease < 0 {
		return errors.New("invalid seller reserved amount release")
	}

	positionRes, err := tx.Exec(`
		UPDATE positions
		SET quantity = quantity - $1,
		    reserved_quantity = reserved_quantity - $2
		WHERE user_id = $3
		  AND symbol = $4
		  AND quantity >= $1
		  AND reserved_quantity >= $2
	`, trade.Quantity, reservedRelease, trade.SellerUserID, trade.Symbol)
	if err != nil {
		return err
	}
	if err := requireRowsAffected("seller position", trade.ID, positionRes); err != nil {
		return err
	}

	orderRes, err := tx.Exec(`
		UPDATE orders
		SET reserved_amount = $1
		WHERE id = $2
	`, nextReservedAmount, trade.SellOrderID)
	if err != nil {
		return err
	}
	if err := requireRowsAffected("seller order reservation", trade.ID, orderRes); err != nil {
		return err
	}

	balanceRes, err := tx.Exec(`
		UPDATE users
		SET balance = balance + $1
		WHERE id = $2
	`, total, trade.SellerUserID)
	if err != nil {
		return err
	}
	if err := requireRowsAffected("seller balance", trade.ID, balanceRes); err != nil {
		return err
	}

	return nil
}

func requireRowsAffected(stage string, tradeID uuid.UUID, res sql.Result) error {
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return errors.New(stage + " update affected 0 rows for trade " + tradeID.String())
	}
	return nil
}
