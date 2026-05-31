package engine

import (
	"errors"
	"sync"

	"tradesphere/matching/model"
	"tradesphere/matching/orderbook"
	"tradesphere/money"
	"tradesphere/observability"

	"github.com/google/uuid"
)

var ErrOrderNotFound = errors.New("order not found")

type MatchingEngine struct {
	orderBooks map[string]*orderbook.OrderBook
	orders     map[uuid.UUID]*model.Order
	mutex      sync.Mutex
}

type SymbolSnapshot struct {
	Symbol string
	Orders []*model.Order
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
	if len(trades) > 0 {
		observability.TradesTotal.Add(float64(len(trades)))
	}

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

func (me *MatchingEngine) RestoreOrders(orders []*model.Order) int {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	me.orderBooks = make(map[string]*orderbook.OrderBook)
	me.orders = make(map[uuid.UUID]*model.Order)

	restored := 0
	for _, order := range orders {
		if order == nil || order.RemainingQuantity <= 0 {
			continue
		}

		me.orders[order.ID] = order
		me.getOrCreateOrderBook(order.Symbol).RestoreOrder(order)
		restored++
	}

	return restored
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

func (me *MatchingEngine) SnapshotSymbol(symbol string) *SymbolSnapshot {
	me.mutex.Lock()
	defer me.mutex.Unlock()

	snapshot := &SymbolSnapshot{
		Symbol: symbol,
		Orders: make([]*model.Order, 0),
	}

	for _, order := range me.orders {
		if order.Symbol != symbol {
			continue
		}
		snapshot.Orders = append(snapshot.Orders, cloneOrder(order))
	}

	return snapshot
}

func (me *MatchingEngine) RestoreSymbol(snapshot *SymbolSnapshot) {
	if snapshot == nil {
		return
	}

	me.mutex.Lock()
	defer me.mutex.Unlock()

	for id, order := range me.orders {
		if order.Symbol == snapshot.Symbol {
			delete(me.orders, id)
		}
	}
	delete(me.orderBooks, snapshot.Symbol)

	for _, order := range snapshot.Orders {
		orderCopy := cloneOrder(order)
		me.orders[orderCopy.ID] = orderCopy
		if orderCopy.RemainingQuantity > 0 && orderCopy.Status != model.Cancelled && orderCopy.Status != model.Filled {
			me.getOrCreateOrderBook(orderCopy.Symbol).RestoreOrder(orderCopy)
		}
	}
}

func (me *MatchingEngine) CancelOrder(id uuid.UUID) (money.Quantity, model.OrderStatus, error) {
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

func (me *MatchingEngine) RestoreOrder(id uuid.UUID, remainingQuantity money.Quantity, status model.OrderStatus) error {
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

func cloneOrder(order *model.Order) *model.Order {
	if order == nil {
		return nil
	}

	orderCopy := *order
	return &orderCopy
}
