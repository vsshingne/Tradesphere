package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"tradesphere/matching/model"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

var DB *sql.DB

type OutboxEvent struct {
	ID       string
	Topic    string
	EventKey string
	Payload  []byte
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
		log.Fatal("Failed to connect to DB:", err)
	}

	err = DB.Ping()
	if err != nil {
		log.Fatal("DB not reachable:", err)
	}

	_, err = DB.Exec(`
		CREATE TABLE IF NOT EXISTS trade_outbox (
			id UUID PRIMARY KEY,
			topic TEXT NOT NULL,
			event_key TEXT NOT NULL,
			payload JSONB NOT NULL,
			created_at TIMESTAMP NOT NULL DEFAULT NOW(),
			published_at TIMESTAMP NULL
		)
	`)
	if err != nil {
		log.Fatal("failed to ensure trade_outbox table:", err)
	}

	_, err = DB.Exec(`
		ALTER TABLE orders
		ADD COLUMN IF NOT EXISTS reserved_amount DOUBLE PRECISION NOT NULL DEFAULT 0
	`)
	if err != nil {
		log.Fatal("failed to ensure orders.reserved_amount:", err)
	}

	log.Println("Connected to PostgreSQL")
}

func PersistMatchResults(trades []model.Trade, orders []*model.Order) error {
	tx, err := DB.Begin()
	if err != nil {
		return err
	}

	for _, trade := range trades {
		payload, err := json.Marshal(trade)
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		_, err = tx.Exec(`
			INSERT INTO trades (
				id,
				symbol,
				buyer_user_id,
				seller_user_id,
				buy_order_id,
				sell_order_id,
				price,
				quantity,
				executed_at
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
		`,
			trade.ID,
			trade.Symbol,
			trade.BuyerUserID,
			trade.SellerUserID,
			trade.BuyOrderID,
			trade.SellOrderID,
			trade.Price,
			trade.Quantity,
			trade.ExecutedAt,
		)
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		if err := enqueueOutboxEvent(tx, trade.ID, "trades", trade.Symbol, payload); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	for _, order := range orders {
		_, err = tx.Exec(`
			UPDATE orders
			SET remaining_quantity = $1, status = $2
			WHERE id = $3
		`,
			order.RemainingQuantity,
			order.Status,
			order.ID,
		)
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		eventID := uuid.New()
		payload, err := json.Marshal(model.OrderEvent{
			ID:                eventID,
			Type:              model.OrderUpdateEvent,
			OrderID:           order.ID,
			UserID:            order.UserID,
			Symbol:            order.Symbol,
			Side:              order.Side,
			Status:            order.Status,
			RemainingQuantity: order.RemainingQuantity,
			ReservedAmount:    order.ReservedAmount,
			CreatedAt:         time.Now().UTC(),
		})
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		if err := enqueueOutboxEvent(tx, eventID, "order-events", order.Symbol, payload); err != nil {
			_ = tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

func PersistCancelledOrder(order *model.Order) error {
	tx, err := DB.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		UPDATE orders
		SET remaining_quantity = $1, status = $2
		WHERE id = $3
	`,
		order.RemainingQuantity,
		order.Status,
		order.ID,
	)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	eventID := uuid.New()
	payload, err := json.Marshal(model.OrderEvent{
		ID:                eventID,
		Type:              model.OrderCancelledEvent,
		OrderID:           order.ID,
		UserID:            order.UserID,
		Symbol:            order.Symbol,
		Side:              order.Side,
		Status:            order.Status,
		RemainingQuantity: order.RemainingQuantity,
		ReservedAmount:    order.ReservedAmount,
		CreatedAt:         time.Now().UTC(),
	})
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	if err := enqueueOutboxEvent(tx, eventID, "order-events", order.Symbol, payload); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func FetchPendingOutboxEvents(ctx context.Context, limit int) ([]OutboxEvent, error) {
	rows, err := DB.QueryContext(ctx, `
		SELECT id, topic, event_key, payload
		FROM trade_outbox
		WHERE published_at IS NULL
		ORDER BY created_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := make([]OutboxEvent, 0, limit)
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(&event.ID, &event.Topic, &event.EventKey, &event.Payload); err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func MarkOutboxEventPublished(eventID string) error {
	_, err := DB.Exec(`
		UPDATE trade_outbox
		SET published_at = NOW()
		WHERE id = $1 AND published_at IS NULL
	`, eventID)
	return err
}

func enqueueOutboxEvent(tx *sql.Tx, id uuid.UUID, topic, eventKey string, payload []byte) error {
	_, err := tx.Exec(`
		INSERT INTO trade_outbox (
			id,
			topic,
			event_key,
			payload,
			created_at
		) VALUES ($1,$2,$3,$4,$5)
	`,
		id,
		topic,
		eventKey,
		payload,
		time.Now().UTC(),
	)
	return err
}

func GetOrder(orderID uuid.UUID) (*model.Order, error) {
	row := DB.QueryRow(`
		SELECT id, user_id, symbol, side, price, quantity, remaining_quantity, reserved_amount, status, created_at
		FROM orders
		WHERE id = $1
	`, orderID)

	var order model.Order
	var side string
	var status string
	if err := row.Scan(
		&order.ID,
		&order.UserID,
		&order.Symbol,
		&side,
		&order.Price,
		&order.Quantity,
		&order.RemainingQuantity,
		&order.ReservedAmount,
		&status,
		&order.CreatedAt,
	); err != nil {
		return nil, err
	}

	order.Side = model.Side(side)
	order.Status = normalizeOrderStatus(status)
	return &order, nil
}

func UpdateOrderState(order *model.Order) error {
	_, err := DB.Exec(`
		UPDATE orders
		SET remaining_quantity = $1, status = $2
		WHERE id = $3
	`,
		order.RemainingQuantity,
		order.Status,
		order.ID,
	)
	return err
}

func normalizeOrderStatus(status string) model.OrderStatus {
	switch status {
	case "PARTIAL":
		return model.PartiallyFilled
	case "CANCELED":
		return model.Cancelled
	case "OPEN":
		return model.New
	default:
		return model.OrderStatus(status)
	}
}
