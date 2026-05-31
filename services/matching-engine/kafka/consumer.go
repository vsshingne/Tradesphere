package kafka

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"os"

	"tradesphere/matching/database"
	"tradesphere/matching/engine"
	"tradesphere/matching/model"
	"tradesphere/matching/telemetry"
	"tradesphere/money"

	"github.com/segmentio/kafka-go"
)

var (
	ordersProcessedTotal = telemetry.Counter("orders_processed_total", "Total order commands processed by matching-engine.")
	cancelProcessedTotal = telemetry.Counter("order_cancels_processed_total", "Total cancel commands processed by matching-engine.")
	tradesExecutedTotal  = telemetry.Counter("trades_executed_total", "Total trades executed by matching-engine.")
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

		command, err := decodeOrderCommand(msg.Value)
		if err != nil {
			log.Println("Invalid order command:", err)
			continue
		}

		switch command.Type {
		case model.CreateOrderCommand:
			order := command.Order
			if order == nil {
				log.Println("Create order command missing order payload")
				continue
			}
			if order.Type == "" {
				order.Type = model.Limit
			}
			if order.Type != model.Limit {
				log.Printf("Rejected unsupported order type: %s order_id=%s", order.Type, order.ID)
				telemetry.Error("unsupported_order_type", map[string]interface{}{
					"order_id": order.ID.String(),
					"type":     order.Type,
				})
				continue
			}

			log.Printf("Received order: %s %s @ %s (Qty: %s)", order.Side, order.Symbol, money.MoneyToDecimal(order.Price), money.QuantityToDecimal(order.Quantity))
			telemetry.Info("order_received", map[string]interface{}{
				"order_id": order.ID.String(),
				"user_id":  order.UserID.String(),
				"symbol":   order.Symbol,
				"side":     order.Side,
			})

			processed, err := processCreateCommand(me, command, "matching-engine-orders")
			if err != nil {
				log.Println("Failed to process create command:", err)
				continue
			}
			if processed {
				ordersProcessedTotal.Inc()
				telemetry.Info("create_command_processed", map[string]interface{}{
					"order_id": command.Order.ID.String(),
					"user_id":  command.Order.UserID.String(),
				})
				log.Printf("Processed create command: %s", command.ID)
			} else {
				log.Printf("Skipped duplicate create command: %s", command.ID)
			}
		case model.CancelOrderCommand:
			processed, err := processCancelCommand(me, command, "matching-engine-cancel")
			if err != nil {
				log.Println("Failed to process cancel command:", err)
				continue
			}
			if processed {
				cancelProcessedTotal.Inc()
				telemetry.Info("cancel_command_processed", map[string]interface{}{
					"order_id": command.Cancel.OrderID.String(),
					"user_id":  command.Cancel.UserID.String(),
				})
				log.Printf("Processed cancel command: %s", command.ID)
			} else {
				log.Printf("Skipped duplicate cancel command: %s", command.ID)
			}
		default:
			log.Printf("Unsupported order command type: %s", command.Type)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Println("Failed to commit order message:", err)
			continue
		}
	}
}

func decodeOrderCommand(raw []byte) (model.OrderCommand, error) {
	var command model.OrderCommand
	if err := json.Unmarshal(raw, &command); err == nil && isSupportedCommandType(command.Type) {
		return command, nil
	}

	var legacyOrder model.Order
	if err := json.Unmarshal(raw, &legacyOrder); err != nil {
		return model.OrderCommand{}, err
	}

	return model.OrderCommand{
		ID:        legacyOrder.ID,
		Type:      model.CreateOrderCommand,
		Symbol:    legacyOrder.Symbol,
		Order:     &legacyOrder,
		CreatedAt: legacyOrder.CreatedAt,
	}, nil
}

func processCreateCommand(me *engine.MatchingEngine, command model.OrderCommand, consumerGroup string) (bool, error) {
	if command.Order == nil {
		return false, errors.New("create command missing order payload")
	}

	tx, err := database.DB.Begin()
	if err != nil {
		return false, err
	}

	processed, err := database.IsEventProcessed(tx, consumerGroup, command.ID)
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

	snapshot := me.SnapshotSymbol(command.Order.Symbol)
	trades, updatedOrders := me.ProcessOrder(command.Order)

	if len(trades) > 0 {
		tradesExecutedTotal.Add(int64(len(trades)))
		for _, trade := range trades {
			telemetry.Info("trade_executed", map[string]interface{}{
				"trade_id": trade.ID.String(),
				"symbol":   trade.Symbol,
			})
		}
		log.Printf("Executed %d trade(s)", len(trades))
	}

	if err := database.PersistMatchResultsTx(tx, trades, updatedOrders); err != nil {
		me.RestoreSymbol(snapshot)
		_ = tx.Rollback()
		return false, err
	}

	if err := database.MarkEventProcessed(tx, consumerGroup, command.ID); err != nil {
		me.RestoreSymbol(snapshot)
		_ = tx.Rollback()
		if errors.Is(err, database.ErrEventAlreadyProcessed) {
			return false, nil
		}
		return false, err
	}

	if err := tx.Commit(); err != nil {
		me.RestoreSymbol(snapshot)
		return false, err
	}

	return true, nil
}

func isSupportedCommandType(commandType model.OrderCommandType) bool {
	switch commandType {
	case model.CreateOrderCommand, model.CancelOrderCommand:
		return true
	default:
		return false
	}
}

func processCancelCommand(me *engine.MatchingEngine, command model.OrderCommand, consumerGroup string) (bool, error) {
	if command.Cancel == nil {
		return false, errors.New("cancel command missing cancel payload")
	}

	tx, err := database.DB.Begin()
	if err != nil {
		return false, err
	}

	processed, err := database.IsEventProcessed(tx, consumerGroup, command.ID)
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

	order, err := database.GetOrderForUpdate(tx, command.Cancel.OrderID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			if err := database.MarkEventProcessed(tx, consumerGroup, command.ID); err != nil {
				_ = tx.Rollback()
				return false, err
			}
			if err := tx.Commit(); err != nil {
				return false, err
			}
			return true, nil
		}
		_ = tx.Rollback()
		return false, err
	}

	if order.Status != model.Filled && order.Status != model.Cancelled && order.RemainingQuantity > 0 {
		inMemoryCancelled := false
		memRemaining, memStatus, err := me.CancelOrder(order.ID)
		if err == nil {
			inMemoryCancelled = true
		} else if !errors.Is(err, engine.ErrOrderNotFound) {
			_ = tx.Rollback()
			return false, err
		}

		order.RemainingQuantity = 0
		order.Status = model.Cancelled
		if err := database.PersistCancelledOrderTx(tx, order); err != nil {
			if inMemoryCancelled {
				_ = me.RestoreOrder(order.ID, memRemaining, memStatus)
			}
			_ = tx.Rollback()
			return false, err
		}
	}

	if err := database.MarkEventProcessed(tx, consumerGroup, command.ID); err != nil {
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
