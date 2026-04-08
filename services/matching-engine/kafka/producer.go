package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
	"tradesphere/matching/database"
	"tradesphere/matching/model"
)

func getKafkaBroker() string {
	if broker := os.Getenv("KAFKA_BROKER"); broker != "" {
		return broker
	}
	return "kafka:9092"
}

var tradeWriter = kafka.NewWriter(kafka.WriterConfig{
	Brokers:  []string{getKafkaBroker()},
	Topic:    "trades",
	Balancer: &kafka.Hash{},
})

var orderEventWriter = kafka.NewWriter(kafka.WriterConfig{
	Brokers:  []string{getKafkaBroker()},
	Topic:    "order-events",
	Balancer: &kafka.Hash{},
})

func PublishTrade(trade model.Trade) error {
	data, err := json.Marshal(trade)
	if err != nil {
		return err
	}

	return tradeWriter.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(trade.Symbol),
		Value: data,
	})
}

func StartTradeOutboxPublisher(ctx context.Context) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := publishPendingTradeEvents(ctx); err != nil {
				if ctx.Err() == nil {
					log.Printf("trade outbox publish failed: %v", err)
					continue
				}
				return
			}
		}
	}
}

func publishPendingTradeEvents(ctx context.Context) error {
	events, err := database.FetchPendingOutboxEvents(ctx, 100)
	if err != nil {
		return err
	}

	for _, event := range events {
		writer, err := writerForTopic(event.Topic)
		if err != nil {
			return err
		}

		if err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.EventKey),
			Value: event.Payload,
		}); err != nil {
			return err
		}

		if err := database.MarkOutboxEventPublished(event.ID); err != nil {
			return err
		}
	}

	return nil
}

func writerForTopic(topic string) (*kafka.Writer, error) {
	switch topic {
	case "trades":
		return tradeWriter, nil
	case "order-events":
		return orderEventWriter, nil
	default:
		return nil, fmt.Errorf("unsupported outbox topic: %s", topic)
	}
}
