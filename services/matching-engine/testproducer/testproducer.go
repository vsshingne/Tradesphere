package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"tradesphere/matching/model"
)

func main() {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "orders",
	})

	// SELL order
	sell := model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             49000,
		Quantity:          1,
		RemainingQuantity: 1,
		CreatedAt:         time.Now(),
	}

	send(writer, sell)

	time.Sleep(2 * time.Second)

	// BUY order
	buy := model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Price:             50000,
		Quantity:          1,
		RemainingQuantity: 1,
		CreatedAt:         time.Now(),
	}

	send(writer, buy)
}

func send(writer *kafka.Writer, order model.Order) {
	data, _ := json.Marshal(order)

	writer.WriteMessages(context.Background(),
		kafka.Message{
			Value: data,
		},
	)
}
