package orderbook

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"tradesphere/matching/model"
	"tradesphere/money"
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

func TestFullMatch(t *testing.T) {
	ob := NewOrderBook()

	buy := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Price:             mustMoney(t, "50000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		CreatedAt:         time.Now(),
	}

	sell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             mustMoney(t, "49000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		CreatedAt:         time.Now(),
	}

	ob.ProcessOrder(buy)
	trades := ob.ProcessOrder(sell)

	if len(trades) != 1 {
		t.Fatalf("Expected 1 trade, got %d", len(trades))
	}

	if trades[0].Quantity != mustQuantity(t, "1") {
		t.Fatalf("Expected trade quantity 1, got %s", money.QuantityToDecimal(trades[0].Quantity))
	}
}

func TestPartialMatch(t *testing.T) {
	ob := NewOrderBook()

	buy := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Price:             mustMoney(t, "50000"),
		Quantity:          mustQuantity(t, "2"),
		RemainingQuantity: mustQuantity(t, "2"),
		CreatedAt:         time.Now(),
	}

	sell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             mustMoney(t, "49000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		CreatedAt:         time.Now(),
	}

	ob.ProcessOrder(buy)
	trades := ob.ProcessOrder(sell)

	if len(trades) != 1 {
		t.Fatalf("Expected 1 trade, got %d", len(trades))
	}

	if buy.RemainingQuantity != mustQuantity(t, "1") {
		t.Fatalf("Expected remaining quantity 1, got %s", money.QuantityToDecimal(buy.RemainingQuantity))
	}
}

func TestNoMatch(t *testing.T) {
	ob := NewOrderBook()

	buy := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Price:             mustMoney(t, "45000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		CreatedAt:         time.Now(),
	}

	sell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             mustMoney(t, "49000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		CreatedAt:         time.Now(),
	}

	ob.ProcessOrder(buy)
	trades := ob.ProcessOrder(sell)

	if len(trades) != 0 {
		t.Fatalf("Expected 0 trades, got %d", len(trades))
	}
}

func TestIncomingSellUsesRestingBidPrice(t *testing.T) {
	ob := NewOrderBook()

	restingBuy := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Price:             mustMoney(t, "50000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		CreatedAt:         time.Now().Add(-time.Second),
	}

	incomingSell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             mustMoney(t, "49000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		CreatedAt:         time.Now(),
	}

	ob.ProcessOrder(restingBuy)
	trades := ob.ProcessOrder(incomingSell)

	if len(trades) != 1 {
		t.Fatalf("Expected 1 trade, got %d", len(trades))
	}

	if trades[0].Price != restingBuy.Price {
		t.Fatalf("Expected resting bid price %s, got %s", money.MoneyToDecimal(restingBuy.Price), money.MoneyToDecimal(trades[0].Price))
	}
}

func TestPriceTimePriorityOnSamePrice(t *testing.T) {
	ob := NewOrderBook()

	olderSell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             mustMoney(t, "49000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		CreatedAt:         time.Now().Add(-2 * time.Second),
	}

	newerSell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             mustMoney(t, "49000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		CreatedAt:         time.Now().Add(-time.Second),
	}

	incomingBuy := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Price:             mustMoney(t, "50000"),
		Quantity:          mustQuantity(t, "1"),
		RemainingQuantity: mustQuantity(t, "1"),
		CreatedAt:         time.Now(),
	}

	ob.ProcessOrder(newerSell)
	ob.ProcessOrder(olderSell)
	trades := ob.ProcessOrder(incomingBuy)

	if len(trades) != 1 {
		t.Fatalf("Expected 1 trade, got %d", len(trades))
	}

	if trades[0].SellOrderID != olderSell.ID {
		t.Fatalf("Expected older resting sell %s, got %s", olderSell.ID, trades[0].SellOrderID)
	}
}
