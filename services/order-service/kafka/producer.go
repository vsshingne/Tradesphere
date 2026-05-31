package kafka

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/google/uuid"
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
	command := model.OrderCommand{
		ID:        order.ID,
		Type:      model.CreateOrderCommand,
		Symbol:    order.Symbol,
		Order:     &order,
		CreatedAt: order.CreatedAt,
	}

	return publishCommand(command)
}

func PublishCancel(cancel model.CancelRequest) error {
	command := model.OrderCommand{
		ID:        uuid.New(),
		Type:      model.CancelOrderCommand,
		Symbol:    cancel.Symbol,
		Cancel:    &cancel,
		CreatedAt: time.Now().UTC(),
	}

	return publishCommand(command)
}

func publishCommand(command model.OrderCommand) error {
	data, err := json.Marshal(command)
	if err != nil {
		return err
	}

	return PublishRawMessage("orders", command.Symbol, data)
}

func PublishRawMessage(topic, key string, payload []byte) error {
	writer := writerForTopic(topic)
	return writer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(key),
		Value: payload,
	})
}

func writerForTopic(topic string) *kafka.Writer {
	switch topic {
	case "orders":
		return writer
	default:
		return writer
	}
}
