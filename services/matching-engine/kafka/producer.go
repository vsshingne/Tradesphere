package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"tradesphere/matching/database"
	"tradesphere/matching/telemetry"
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

var tradeDlqWriter = kafka.NewWriter(kafka.WriterConfig{
	Brokers:  []string{getKafkaBroker()},
	Topic:    "trades-dlq",
	Balancer: &kafka.Hash{},
})

var orderEventWriter = kafka.NewWriter(kafka.WriterConfig{
	Brokers:  []string{getKafkaBroker()},
	Topic:    "order-events",
	Balancer: &kafka.Hash{},
})

var orderEventDlqWriter = kafka.NewWriter(kafka.WriterConfig{
	Brokers:  []string{getKafkaBroker()},
	Topic:    "order-events-dlq",
	Balancer: &kafka.Hash{},
})

var outboxWorkerID = fmt.Sprintf("matching-outbox-%d", time.Now().UnixNano())
var (
	outboxPublishedTotal = telemetry.Counter("outbox_events_published_total", "Total outbox events published by matching-engine.")
	outboxRetryTotal     = telemetry.Counter("outbox_publish_retries_total", "Total outbox publish retries scheduled by matching-engine.")
)

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

const maxDLQAttempts = 10

func publishPendingTradeEvents(ctx context.Context) error {
	events, err := database.ClaimPendingOutboxEvents(ctx, outboxWorkerID, 100)
	if err != nil {
		return err
	}

	for _, event := range events {
		if event.PublishAttempts >= maxDLQAttempts {
			dlqWriter, dlqErr := writerForTopic(dlqTopic(event.Topic))
			if dlqErr != nil {
				log.Printf("DLQ writer not found for event %s: %v", event.ID, dlqErr)
			} else {
				if writeErr := dlqWriter.WriteMessages(ctx, kafka.Message{
					Key:   []byte(event.EventKey),
					Value: event.Payload,
				}); writeErr != nil {
					log.Printf("DLQ publish failed for event %s: %v", event.ID, writeErr)
				} else {
					log.Printf("Event %s sent to DLQ after %d attempts", event.ID, event.PublishAttempts)
					_ = database.MarkOutboxEventPublished(event.ID)
				}
			}
			continue
		}

		writer, err := writerForTopic(event.Topic)
		if err != nil {
			return err
		}

		if err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.EventKey),
			Value: event.Payload,
		}); err != nil {
			retryAt := time.Now().Add(outboxBackoff(event.PublishAttempts))
			outboxRetryTotal.Inc()
			if recordErr := database.RecordOutboxPublishFailure(event.ID, retryAt, err); recordErr != nil {
				return fmt.Errorf("publish failed: %v; record failure failed: %w", err, recordErr)
			}
			return err
		}

		if err := database.MarkOutboxEventPublished(event.ID); err != nil {
			return err
		}
		outboxPublishedTotal.Inc()
	}

	return nil
}

func writerForTopic(topic string) (*kafka.Writer, error) {
	switch topic {
	case "trades":
		return tradeWriter, nil
	case "trades-dlq":
		return tradeDlqWriter, nil
	case "order-events":
		return orderEventWriter, nil
	case "order-events-dlq":
		return orderEventDlqWriter, nil
	default:
		return nil, fmt.Errorf("unsupported outbox topic: %s", topic)
	}
}

func outboxBackoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	if attempt > 5 {
		attempt = 5
	}

	return time.Second * time.Duration(1<<attempt)
}

func dlqTopic(topic string) string {
	return topic + "-dlq"
}

func init() {
	if host := os.Getenv("HOSTNAME"); host != "" {
		outboxWorkerID = "matching-outbox-" + host
	}
	if pid := os.Getpid(); pid > 0 {
		outboxWorkerID += "-" + strconv.Itoa(pid)
	}
}
