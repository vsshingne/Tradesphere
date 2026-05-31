package kafka

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"tradesphere/money"
	"tradesphere/portfolio/database"
	"tradesphere/portfolio/model"
	"tradesphere/portfolio/telemetry"

	"github.com/google/uuid"
	kafkago "github.com/segmentio/kafka-go"
)

var (
	tradeEventsProcessedTotal = telemetry.Counter("trade_events_processed_total", "Total trade events processed by portfolio-service.")
	orderEventsProcessedTotal = telemetry.Counter("order_events_processed_total", "Total order events processed by portfolio-service.")
)

type PortfolioService struct{}

var portfolioService = PortfolioService{}

type OrderEvent struct {
	ID                uuid.UUID      `json:"id"`
	Type              string         `json:"type"`
	OrderID           uuid.UUID      `json:"order_id"`
	UserID            uuid.UUID      `json:"user_id"`
	Symbol            string         `json:"symbol"`
	Side              string         `json:"side"`
	Status            string         `json:"status"`
	RemainingQuantity money.Quantity `json:"remaining_quantity"`
	ReservedAmount    money.Money    `json:"reserved_amount"`
	CreatedAt         time.Time      `json:"created_at"`
}

func StartTradeConsumer(ctx context.Context) {
	broker, groupID := kafkaConfig()

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "trades",
		GroupID: groupID,
	})
	defer reader.Close()

	log.Printf("Portfolio consumer started. topic=trades broker=%s group_id=%s", broker, groupID)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				log.Println("Portfolio consumer stopped")
				return
			}
			log.Println("Error reading:", err)
			continue
		}

		var trade model.Trade
		if err := json.Unmarshal(msg.Value, &trade); err != nil {
			log.Printf("Invalid trade JSON: %v", err)
			continue
		}

		processed, err := processTrade(trade, groupID)
		if err != nil {
			log.Printf("Trade processing failed: %v", err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Commit failed for trade %s: %v", trade.ID, err)
			continue
		}

		if processed {
			tradeEventsProcessedTotal.Inc()
			telemetry.Info("trade_applied", map[string]interface{}{
				"trade_id":  trade.ID.String(),
				"buyer_id":  trade.BuyerUserID.String(),
				"seller_id": trade.SellerUserID.String(),
			})
			log.Println("Processed trade:", trade.ID)
		} else {
			log.Println("Skipped duplicate trade:", trade.ID)
		}
	}
}

func StartOrderEventConsumer(ctx context.Context) {
	broker, groupID := kafkaConfig()
	orderEventGroupID := groupID + "-order-events"

	reader := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: []string{broker},
		Topic:   "order-events",
		GroupID: orderEventGroupID,
	})
	defer reader.Close()

	log.Printf("Portfolio consumer started. topic=order-events broker=%s group_id=%s", broker, orderEventGroupID)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				log.Println("Order event consumer stopped")
				return
			}
			log.Println("Error reading order event:", err)
			continue
		}

		var event OrderEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("Invalid order event JSON: %v", err)
			continue
		}

		processed, err := processOrderEvent(event, orderEventGroupID)
		if err != nil {
			log.Printf("Order event processing failed: %v", err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("Commit failed for order event %s: %v", event.ID, err)
			continue
		}

		if processed {
			orderEventsProcessedTotal.Inc()
			telemetry.Info("order_event_applied", map[string]interface{}{
				"order_id": event.OrderID.String(),
				"user_id":  event.UserID.String(),
				"type":     event.Type,
			})
			log.Println("Processed order event:", event.ID)
		} else {
			log.Println("Skipped duplicate order event:", event.ID)
		}
	}
}

func processTrade(trade model.Trade, consumerGroup string) (bool, error) {
	tx, err := database.DB.Begin()
	if err != nil {
		return false, err
	}

	processed, err := database.IsEventProcessed(tx, consumerGroup, trade.ID)
	if err != nil {
		_ = tx.Rollback()
		return false, err
	}

	if processed {
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return false, nil
	}

	if err := portfolioService.ApplyTrade(tx, trade); err != nil {
		_ = tx.Rollback()
		return false, err
	}

	if err := database.MarkEventProcessed(tx, consumerGroup, trade.ID); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, database.ErrEventAlreadyProcessed) {
			return false, nil
		}
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	return true, nil
}

func processOrderEvent(event OrderEvent, consumerGroup string) (bool, error) {
	tx, err := database.DB.Begin()
	if err != nil {
		return false, err
	}

	processed, err := database.IsEventProcessed(tx, consumerGroup, event.ID)
	if err != nil {
		_ = tx.Rollback()
		return false, err
	}

	if processed {
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return false, nil
	}

	if err := portfolioService.ApplyOrderEvent(tx, event); err != nil {
		_ = tx.Rollback()
		return false, err
	}

	if err := database.MarkEventProcessed(tx, consumerGroup, event.ID); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, database.ErrEventAlreadyProcessed) {
			return false, nil
		}
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	return true, nil
}

func (PortfolioService) ApplyTrade(tx *sql.Tx, trade model.Trade) error {
	buyerID := trade.BuyerUserID.String()
	sellerID := trade.SellerUserID.String()

	if buyerID == sellerID {
		if err := database.LockAccount(tx, trade.BuyerUserID); err != nil {
			return err
		}
	} else if buyerID < sellerID {
		if err := database.LockAccount(tx, trade.BuyerUserID); err != nil {
			return err
		}
		if err := database.LockAccount(tx, trade.SellerUserID); err != nil {
			return err
		}
	} else {
		if err := database.LockAccount(tx, trade.SellerUserID); err != nil {
			return err
		}
		if err := database.LockAccount(tx, trade.BuyerUserID); err != nil {
			return err
		}
	}

	if err := database.ApplyBuyerTrade(tx, trade); err != nil {
		return err
	}
	if err := database.ApplySellerTrade(tx, trade); err != nil {
		return err
	}

	return nil
}

func (PortfolioService) ApplyOrderEvent(tx *sql.Tx, event OrderEvent) error {
	if event.Type != "ORDER_CANCELLED" {
		return nil
	}

	if err := database.LockAccount(tx, event.UserID); err != nil {
		return err
	}

	return database.ReleaseOrderReservation(tx, event.OrderID)
}

func kafkaConfig() (string, string) {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		broker = "kafka:9092"
	}
	groupID := os.Getenv("KAFKA_GROUP_ID")
	if groupID == "" {
		groupID = "portfolio-service"
	}
	return broker, groupID
}
