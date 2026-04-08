package database

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"tradesphere/order/model"

	_ "github.com/lib/pq"
)

var DB *sql.DB

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

	_, err = DB.Exec(`
		ALTER TABLE orders
		ADD COLUMN IF NOT EXISTS reserved_amount DOUBLE PRECISION NOT NULL DEFAULT 0
	`)
	if err != nil {
		log.Fatal("failed to ensure orders.reserved_amount:", err)
	}

	log.Println("Order DB connected")
}

func InsertOrder(order model.Order) error {
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

func DeleteOrder(orderID string) error {
	_, err := DB.Exec(`DELETE FROM orders WHERE id = $1`, orderID)
	return err
}
