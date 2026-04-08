package engine

import (
	"errors"
	"sync"

	"tradesphere/matching/model"
	"tradesphere/matching/orderbook"

	"github.com/google/uuid"
)

var ErrOrderNotFound = errors.New("order not found")

type MatchingEngine struct {
	orderBooks map[string]*orderbook.OrderBook
	orders     map[uuid.UUID]*model.Order
	mutex      sync.Mutex
}

func NewMatchingEngine() *MatchingEngine {
	return &MatchingEngine{
		orderBooks: make(map[string]*orderbook.OrderBook),
		orders:     make(map[uuid.UUID]*model.Order),
	}
}

func (me *MatchingEngine) getOrCreateOrderBook(symbol string) *orderbook.OrderBook {
	ob, exists := me.orderBooks[symbol]
	if !exists {
		ob = orderbook.NewOrderBook()
		me.orderBooks[symbol] = ob
	}
	return ob
}

func (me *MatchingEngine) ProcessOrder(order *model.Order) ([]model.Trade, []*model.Order) {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	me.orders[order.ID] = order

	ob := me.getOrCreateOrderBook(order.Symbol)
	trades := ob.ProcessOrder(order)

	changedOrders := map[uuid.UUID]*model.Order{
		order.ID: order,
	}

	for _, trade := range trades {
		if buyOrder, exists := me.orders[trade.BuyOrderID]; exists {
			changedOrders[buyOrder.ID] = buyOrder
		}
		if sellOrder, exists := me.orders[trade.SellOrderID]; exists {
			changedOrders[sellOrder.ID] = sellOrder
		}
	}

	updatedOrders := make([]*model.Order, 0, len(changedOrders))
	for _, candidate := range changedOrders {
		updateOrderStatus(candidate)
		updatedOrders = append(updatedOrders, candidate)
	}

	return trades, updatedOrders
}

func (me *MatchingEngine) GetOrderBookSnapshot(symbol string) *orderbook.OrderBook {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	ob, exists := me.orderBooks[symbol]
	if !exists {
		return nil
	}
	return ob.Clone()
}

func (me *MatchingEngine) CancelOrder(id uuid.UUID) (float64, model.OrderStatus, error) {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	order, exists := me.orders[id]
	if !exists {
		return 0, "", ErrOrderNotFound
	}

	previousRemaining := order.RemainingQuantity
	previousStatus := order.Status
	order.RemainingQuantity = 0
	order.Status = model.Cancelled

	return previousRemaining, previousStatus, nil
}

func (me *MatchingEngine) RestoreOrder(id uuid.UUID, remainingQuantity float64, status model.OrderStatus) error {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	order, exists := me.orders[id]
	if !exists {
		return ErrOrderNotFound
	}

	order.RemainingQuantity = remainingQuantity
	order.Status = status
	return nil
}

func updateOrderStatus(order *model.Order) {
	if order.Status == model.Cancelled {
		return
	}

	switch {
	case order.RemainingQuantity <= 0:
		order.RemainingQuantity = 0
		order.Status = model.Filled
	case order.RemainingQuantity < order.Quantity:
		order.Status = model.PartiallyFilled
	default:
		order.Status = model.New
	}
}
