package engine

import (
	"testing"
	"time"

	"tradesphere/matching/model"

	"github.com/google/uuid"
)

func TestOrderStatusLifecycleTransitions(t *testing.T) {
	me := NewMatchingEngine()

	buy := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Buy,
		Price:             50000,
		Quantity:          2,
		RemainingQuantity: 2,
		Status:            model.New,
		CreatedAt:         time.Now().Add(-time.Second),
	}

	sell := &model.Order{
		ID:                uuid.New(),
		UserID:            uuid.New(),
		Symbol:            "BTC",
		Side:              model.Sell,
		Price:             49000,
		Quantity:          1,
		RemainingQuantity: 1,
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
