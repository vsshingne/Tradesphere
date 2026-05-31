package engine

import (
	"testing"
	"time"

	"tradesphere/matching/model"
	"tradesphere/money"

	"github.com/google/uuid"
)

func mustMoney(t *testing.T, value string) money.Money {
	t.Helper()

	parsed, err := money.MoneyFromDecimal(value)
	if err != nil {
		t.Fatalf("parse money %s: %v", value, err)
	}
	return parsed
}

func mustQuantity(t *testing.T, value string) money.Quantity {
	t.Helper()

	parsed, err := money.QuantityFromDecimal(value)
	if err != nil {
		t.Fatalf("parse quantity %s: %v", value, err)
	}
	return parsed
}

func TestOrderStatusLifecycleTransitions(t *testing.T) {
	me := NewMatchingEngine()

	buy := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Price:             mustMoney(t, "50000"),
		Quantity:          mustQuantity(t, "2"),
		RemainingQuantity: mustQuantity(t, "2"),
		Status:            model.New,
		CreatedAt:         time.Now().Add(-time.Second),
	}

	sell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             mustMoney(t, "49000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		Status:            model.New,
		CreatedAt:         time.Now(),
	}

	_, _ = me.ProcessOrder(buy)
	if buy.Status != model.New {
		t.Fatalf("expected resting order status NEW, got %s", buy.Status)
	}

	trades, _ := me.ProcessOrder(sell)
	if len(trades) != 1 {
		t.Fatalf("expected 1 trade, got %d", len(trades))
	}

	if buy.Status != model.PartiallyFilled {
		t.Fatalf("expected buy order to be PARTIALLY_FILLED, got %s", buy.Status)
	}

	if sell.Status != model.Filled {
		t.Fatalf("expected sell order to be FILLED, got %s", sell.Status)
	}
}

func TestRestoreOrdersRebuildsPriceTimePriority(t *testing.T) {
	me := NewMatchingEngine()
	now := time.Now()

	laterHigherBid := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Type:              model.Limit,
		Price:             mustMoney(t, "51000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		Status:            model.New,
		CreatedAt:         now,
	}

	earlierHigherBid := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Type:              model.Limit,
		Price:             mustMoney(t, "51000"),
		Quantity:          mustQuantity(t, "2"),
		RemainingQuantity: mustQuantity(t, "2"),
		Status:            model.PartiallyFilled,
		CreatedAt:         now.Add(-time.Second),
	}

	lowerBid := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Type:              model.Limit,
		Price:             mustMoney(t, "50000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		Status:            model.New,
		CreatedAt:         now.Add(-2 * time.Second),
	}

	if restored := me.RestoreOrders([]*model.Order{laterHigherBid, lowerBid, earlierHigherBid}); restored != 3 {
		t.Fatalf("expected 3 restored orders, got %d", restored)
	}

	incomingSell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Type:              model.Limit,
		Price:             mustMoney(t, "50000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		Status:            model.New,
		CreatedAt:         now.Add(time.Second),
	}

	trades, _ := me.ProcessOrder(incomingSell)
	if len(trades) != 1 {
		t.Fatalf("expected 1 trade after recovery, got %d", len(trades))
	}

	if trades[0].BuyOrderID != earlierHigherBid.ID {
		t.Fatalf("expected recovered earliest highest bid %s to match first, got %s", earlierHigherBid.ID, trades[0].BuyOrderID)
	}
}
