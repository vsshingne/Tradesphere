package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"tradesphere/money"
	"tradesphere/order/model"
	"tradesphere/order/telemetry"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

var DB *sql.DB

var dbQueryDuration = telemetry.Duration("db_query_duration_seconds", "Duration of order-service database operations.")

func InitDB() {
	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "postgres"
	}

	connStr := fmt.Sprintf(
		"host=%s port=5432 user=tradesphere password=tradesphere dbname=tradesphere sslmode=disable",
		host,
	)

	var err error
	DB, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal("failed to connect to DB:", err)
	}

	if err = DB.Ping(); err != nil {
		log.Fatal("DB not reachable:", err)
	}

	DB.SetMaxOpenConns(10)
	DB.SetMaxIdleConns(5)
	DB.SetConnMaxLifetime(5 * time.Minute)

	log.Println("Order DB connected")
}

func InsertOrder(order model.Order) error {
	start := time.Now()
	defer dbQueryDuration.Observe(time.Since(start))

	_, err := DB.Exec(`
		INSERT INTO orders (
			id,
			user_id,
			symbol,
			side,
			price,
			quantity,
			remaining_quantity,
			reserved_amount,
			status,
			created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
	`,
		order.ID,
		order.UserID,
		order.Symbol,
		order.Side,
		order.Price,
		order.Quantity,
		order.RemainingQuantity,
		order.ReservedAmount,
		order.Status,
		order.CreatedAt,
	)

	return err
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
	order.Status = model.OrderStatus(status)
	return &order, nil
}
