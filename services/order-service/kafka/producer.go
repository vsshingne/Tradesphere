package kafka

import (
	"context"
	"encoding/json"
	"os"

	"github.com/segmentio/kafka-go"
	"tradesphere/order/model"
)

func getKafkaBroker() string {
	if broker := os.Getenv("KAFKA_BROKER"); broker != "" {
		return broker
	}
	return "kafka:9092"
}

var writer = kafka.NewWriter(kafka.WriterConfig{
	Brokers:  []string{getKafkaBroker()},
	Topic:    "orders",
	Balancer: &kafka.Hash{},
})

func PublishOrder(order model.Order) error {
	data, err := json.Marshal(order)
	if err != nil {
		return err
	}

	return writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(order.Symbol),
		Value: data,
	})
}
