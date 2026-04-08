package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"

	"tradesphere/matching/database"
	"tradesphere/matching/engine"
	"tradesphere/matching/model"

	"github.com/segmentio/kafka-go"
)

func StartOrderConsumer(ctx context.Context, me *engine.MatchingEngine) {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9092"
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "orders",
		GroupID: "matching-engine",
	})
	defer reader.Close()

	log.Printf("Kafka consumer started. topic=orders broker=%s", broker)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				log.Println("Order consumer stopped")
				return
			}
			log.Println("Error reading message:", err)
			continue
		}

		if len(msg.Value) == 0 {
			continue
		}

		var order model.Order
		if err := json.Unmarshal(msg.Value, &order); err != nil {
			log.Println("Invalid order format:", err)
			continue
		}

		log.Printf("Received order: %s %s @ %.2f (Qty: %.2f)", order.Side, order.Symbol, order.Price, order.Quantity)

		trades, updatedOrders := me.ProcessOrder(&order)
		if len(trades) > 0 {
			log.Printf("Executed %d trade(s)", len(trades))
		}

		if err := database.PersistMatchResults(trades, updatedOrders); err != nil {
			log.Println("Failed to persist match results:", err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Println("Failed to commit order message:", err)
			continue
		}
	}
}
