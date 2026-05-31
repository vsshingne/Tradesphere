package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"tradesphere/matching/model"
	"tradesphere/matching/telemetry"
	"tradesphere/money"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

var DB *sql.DB

var ErrEventAlreadyProcessed = errors.New("event already processed")
var dbQueryDuration = telemetry.Duration("db_query_duration_seconds", "Duration of matching-engine database operations.")

type OutboxEvent struct {
	ID              string
	Topic           string
	EventKey        string
	Payload         []byte
	PublishAttempts int
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

	log.Println("Connected to PostgreSQL")
}

func PersistMatchResults(trades []model.Trade, orders []*model.Order) error {
	start := time.Now()
	defer dbQueryDuration.Observe(time.Since(start))

	tx, err := DB.Begin()
	if err != nil {
		return err
	}

	if err := PersistMatchResultsTx(tx, trades, orders); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func PersistMatchResultsTx(tx *sql.Tx, trades []model.Trade, orders []*model.Order) error {
	for _, trade := range trades {
		payload, err := json.Marshal(trade)
		if err != nil {
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
			return err
		}

		if err := enqueueOutboxEvent(tx, trade.ID, "trades", trade.Symbol, payload); err != nil {
			return err
		}
	}

	for _, order := range orders {
		_, err := tx.Exec(`
			UPDATE orders
			SET remaining_quantity = $1, status = $2
			WHERE id = $3
		`,
			order.RemainingQuantity,
			order.Status,
			order.ID,
		)
		if err != nil {
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
			return err
		}

		if err := enqueueOutboxEvent(tx, eventID, "order-events", order.Symbol, payload); err != nil {
			return err
		}
	}

	return nil
}

func PersistCancelledOrder(order *model.Order) error {
	start := time.Now()
	defer dbQueryDuration.Observe(time.Since(start))

	tx, err := DB.Begin()
	if err != nil {
		return err
	}

	if err := PersistCancelledOrderTx(tx, order); err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func PersistCancelledOrderTx(tx *sql.Tx, order *model.Order) error {
	if order == nil {
		return errors.New("order is nil")
	}

	_, err := tx.Exec(`
		UPDATE orders
		SET remaining_quantity = $1, status = $2
		WHERE id = $3
	`,
		order.RemainingQuantity,
		order.Status,
		order.ID,
	)
	if err != nil {
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
		return err
	}

	if err := enqueueOutboxEvent(tx, eventID, "order-events", order.Symbol, payload); err != nil {
		return err
	}

	return nil
}

func ClaimPendingOutboxEvents(ctx context.Context, workerID string, limit int) ([]OutboxEvent, error) {
	start := time.Now()
	defer dbQueryDuration.Observe(time.Since(start))

	tx, err := DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	rows, err := tx.QueryContext(ctx, `
		WITH candidates AS (
			SELECT id
			FROM trade_outbox
			WHERE published_at IS NULL
			  AND next_attempt_at <= NOW()
			  AND (claimed_at IS NULL OR claimed_at < NOW() - INTERVAL '30 seconds')
			ORDER BY created_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE trade_outbox AS outbox
		SET claimed_by = $2,
		    claimed_at = NOW()
		FROM candidates
		WHERE outbox.id = candidates.id
		RETURNING outbox.id, outbox.topic, outbox.event_key, outbox.payload, outbox.publish_attempts
	`, limit, workerID)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	defer rows.Close()

	events := make([]OutboxEvent, 0, limit)
	for rows.Next() {
		var event OutboxEvent
		if err := rows.Scan(&event.ID, &event.Topic, &event.EventKey, &event.Payload, &event.PublishAttempts); err != nil {
			_ = tx.Rollback()
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return events, nil
}

func MarkOutboxEventPublished(eventID string) error {
	start := time.Now()
	defer dbQueryDuration.Observe(time.Since(start))

	_, err := DB.Exec(`
		UPDATE trade_outbox
		SET published_at = NOW(),
		    claimed_at = NULL,
		    claimed_by = NULL,
		    last_error = NULL
		WHERE id = $1 AND published_at IS NULL
	`, eventID)
	return err
}

func RecordOutboxPublishFailure(eventID string, nextAttemptAt time.Time, publishErr error) error {
	start := time.Now()
	defer dbQueryDuration.Observe(time.Since(start))

	errorText := ""
	if publishErr != nil {
		errorText = publishErr.Error()
	}

	_, err := DB.Exec(`
		UPDATE trade_outbox
		SET publish_attempts = publish_attempts + 1,
		    next_attempt_at = $2,
		    claimed_at = NULL,
		    claimed_by = NULL,
		    last_error = $3
		WHERE id = $1 AND published_at IS NULL
	`, eventID, nextAttemptAt.UTC(), errorText)
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

func LoadOpenOrders() ([]*model.Order, error) {
	start := time.Now()
	defer dbQueryDuration.Observe(time.Since(start))

	rows, err := DB.Query(`
		SELECT id, user_id, symbol, side, price, quantity, remaining_quantity, reserved_amount, status, created_at
		FROM orders
		WHERE remaining_quantity > 0
		  AND status IN ('NEW', 'PARTIALLY_FILLED', 'OPEN', 'PARTIAL')
		ORDER BY symbol ASC, created_at ASC, id ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	orders := make([]*model.Order, 0)
	for rows.Next() {
		var order model.Order
		var side string
		var status string
		var price int64
		var quantity int64
		var remainingQuantity int64
		var reservedAmount int64

		if err := rows.Scan(
			&order.ID,
			&order.UserID,
			&order.Symbol,
			&side,
			&price,
			&quantity,
			&remainingQuantity,
			&reservedAmount,
			&status,
			&order.CreatedAt,
		); err != nil {
			return nil, err
		}

		order.Price = money.Money(price)
		order.Quantity = money.Quantity(quantity)
		order.RemainingQuantity = money.Quantity(remainingQuantity)
		order.ReservedAmount = money.Money(reservedAmount)
		order.Side = model.Side(side)
		order.Type = model.Limit
		order.Status = normalizeOrderStatus(status)
		orders = append(orders, &order)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return orders, nil
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

func GetOrder(orderID uuid.UUID) (*model.Order, error) {
	start := time.Now()
	defer dbQueryDuration.Observe(time.Since(start))

	row := DB.QueryRow(`
		SELECT id, user_id, symbol, side, price, quantity, remaining_quantity, reserved_amount, status, created_at
		FROM orders
		WHERE id = $1
	`, orderID)

	var order model.Order
	var side string
	var status string
	var price int64
	var quantity int64
	var remainingQuantity int64
	var reservedAmount int64
	if err := row.Scan(
		&order.ID,
		&order.UserID,
		&order.Symbol,
		&side,
		&price,
		&quantity,
		&remainingQuantity,
		&reservedAmount,
		&status,
		&order.CreatedAt,
	); err != nil {
		return nil, err
	}

	order.Price = money.Money(price)
	order.Quantity = money.Quantity(quantity)
	order.RemainingQuantity = money.Quantity(remainingQuantity)
	order.ReservedAmount = money.Money(reservedAmount)
	order.Side = model.Side(side)
	order.Status = normalizeOrderStatus(status)
	return &order, nil
}

func GetOrderForUpdate(tx *sql.Tx, orderID uuid.UUID) (*model.Order, error) {
	start := time.Now()
	defer dbQueryDuration.Observe(time.Since(start))

	row := tx.QueryRow(`
		SELECT id, user_id, symbol, side, price, quantity, remaining_quantity, reserved_amount, status, created_at
		FROM orders
		WHERE id = $1
		FOR UPDATE
	`, orderID)

	var order model.Order
	var side string
	var status string
	var price int64
	var quantity int64
	var remainingQuantity int64
	var reservedAmount int64
	if err := row.Scan(
		&order.ID,
		&order.UserID,
		&order.Symbol,
		&side,
		&price,
		&quantity,
		&remainingQuantity,
		&reservedAmount,
		&status,
		&order.CreatedAt,
	); err != nil {
		return nil, err
	}

	order.Price = money.Money(price)
	order.Quantity = money.Quantity(quantity)
	order.RemainingQuantity = money.Quantity(remainingQuantity)
	order.ReservedAmount = money.Money(reservedAmount)
	order.Side = model.Side(side)
	order.Status = normalizeOrderStatus(status)
	order.Type = model.Limit
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
