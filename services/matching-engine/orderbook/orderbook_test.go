package orderbook

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"tradesphere/matching/model"
)

func TestFullMatch(t *testing.T) {
	ob := NewOrderBook()

	buy := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Price:             50000,
		Quantity:          1,
		RemainingQuantity: 1,
		CreatedAt:         time.Now(),
	}

	sell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             49000,
		Quantity:          1,
		RemainingQuantity: 1,
		CreatedAt:         time.Now(),
	}

	ob.ProcessOrder(buy)
	trades := ob.ProcessOrder(sell)

	if len(trades) != 1 {
		t.Fatalf("Expected 1 trade, got %d", len(trades))
	}

	if trades[0].Quantity != 1 {
		t.Fatalf("Expected trade quantity 1, got %f", trades[0].Quantity)
	}
}

func TestPartialMatch(t *testing.T) {
	ob := NewOrderBook()

	buy := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Price:             50000,
		Quantity:          2,
		RemainingQuantity: 2,
		CreatedAt:         time.Now(),
	}

	sell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             49000,
		Quantity:          1,
		RemainingQuantity: 1,
		CreatedAt:         time.Now(),
	}

	ob.ProcessOrder(buy)
	trades := ob.ProcessOrder(sell)

	if len(trades) != 1 {
		t.Fatalf("Expected 1 trade, got %d", len(trades))
	}

	if buy.RemainingQuantity != 1 {
		t.Fatalf("Expected remaining quantity 1, got %f", buy.RemainingQuantity)
	}
}

func TestNoMatch(t *testing.T) {
	ob := NewOrderBook()

	buy := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Price:             45000,
		Quantity:          1,
		RemainingQuantity: 1,
		CreatedAt:         time.Now(),
	}

	sell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             49000,
		Quantity:          1,
		RemainingQuantity: 1,
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
		Price:             50000,
		Quantity:          1,
		RemainingQuantity: 1,
		CreatedAt:         time.Now().Add(-time.Second),
	}

	incomingSell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             49000,
		Quantity:          1,
		RemainingQuantity: 1,
		CreatedAt:         time.Now(),
	}

	ob.ProcessOrder(restingBuy)
	trades := ob.ProcessOrder(incomingSell)

	if len(trades) != 1 {
		t.Fatalf("Expected 1 trade, got %d", len(trades))
	}

	if trades[0].Price != restingBuy.Price {
		t.Fatalf("Expected resting bid price %.2f, got %.2f", restingBuy.Price, trades[0].Price)
	}
}

func TestPriceTimePriorityOnSamePrice(t *testing.T) {
	ob := NewOrderBook()

	olderSell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             49000,
		Quantity:          1,
		RemainingQuantity: 1,
		CreatedAt:         time.Now().Add(-2 * time.Second),
	}

	newerSell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             49000,
		Quantity:          1,
		RemainingQuantity: 1,
		CreatedAt:         time.Now().Add(-time.Second),
	}

	incomingBuy := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Price:             50000,
		Quantity:          1,
		RemainingQuantity: 1,
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
