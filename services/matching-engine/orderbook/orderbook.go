package orderbook

import (
	"container/heap"
	"time"

	"tradesphere/matching/model"
	"tradesphere/money"

	"github.com/google/uuid"
)

type OrderBook struct {
	BuyOrders  *MaxHeap
	SellOrders *MinHeap
}

func NewOrderBook() *OrderBook {
	buy := &MaxHeap{}
	sell := &MinHeap{}
	heap.Init(buy)
	heap.Init(sell)

	return &OrderBook{
		BuyOrders:  buy,
		SellOrders: sell,
	}
}

type MaxHeap []*model.Order

func (h MaxHeap) Len() int { return len(h) }
func (h MaxHeap) Less(i, j int) bool {
	if h[i].Price == h[j].Price {
		return h[i].CreatedAt.Before(h[j].CreatedAt)
	}
	return h[i].Price > h[j].Price
}

func (h MaxHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MaxHeap) Push(x interface{}) {
	*h = append(*h, x.(*model.Order))
}

func (h *MaxHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

type MinHeap []*model.Order

func (h MinHeap) Len() int { return len(h) }

func (h MinHeap) Less(i, j int) bool {
	if h[i].Price == h[j].Price {
		return h[i].CreatedAt.Before(h[j].CreatedAt)
	}
	return h[i].Price < h[j].Price
}

func (h MinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(*model.Order))
}

func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func (ob *OrderBook) ProcessOrder(order *model.Order) []model.Trade {
	var trades []model.Trade

	if order.RemainingQuantity <= 0 && order.Quantity > 0 {
		order.RemainingQuantity = order.Quantity
	}

	if order.Type == model.Market {
		if order.Side == model.Buy {
			trades = ob.matchBuyMarketOrder(order)
		} else {
			trades = ob.matchSellMarketOrder(order)
		}
		order.RemainingQuantity = 0
		return trades
	}

	if order.Side == model.Buy {
		trades = ob.matchBuyOrder(order)
	} else {
		trades = ob.matchSellOrder(order)
	}

	return trades
}

func (ob *OrderBook) RestoreOrder(order *model.Order) {
	if order == nil || order.RemainingQuantity <= 0 {
		return
	}

	if order.Side == model.Buy {
		heap.Push(ob.BuyOrders, order)
		return
	}

	heap.Push(ob.SellOrders, order)
}

func (ob *OrderBook) matchBuyMarketOrder(order *model.Order) []model.Trade {
	var trades []model.Trade

	for order.RemainingQuantity > 0 && ob.SellOrders.Len() > 0 {
		bestSell := (*ob.SellOrders)[0]

		tradeQty := min(order.RemainingQuantity, bestSell.RemainingQuantity)
		if tradeQty <= 0 {
			if bestSell.RemainingQuantity <= 0 {
				heap.Pop(ob.SellOrders)
				continue
			}
			break
		}

		trade := createTrade(bestSell, order, tradeQty)
		trades = append(trades, trade)

		order.RemainingQuantity -= tradeQty
		bestSell.RemainingQuantity -= tradeQty

		if bestSell.RemainingQuantity == 0 {
			heap.Pop(ob.SellOrders)
		}
	}

	return trades
}

func (ob *OrderBook) matchSellMarketOrder(order *model.Order) []model.Trade {
	var trades []model.Trade

	for order.RemainingQuantity > 0 && ob.BuyOrders.Len() > 0 {
		bestBuy := (*ob.BuyOrders)[0]

		tradeQty := min(order.RemainingQuantity, bestBuy.RemainingQuantity)
		if tradeQty <= 0 {
			if bestBuy.RemainingQuantity <= 0 {
				heap.Pop(ob.BuyOrders)
				continue
			}
			break
		}

		trade := createTrade(bestBuy, order, tradeQty)
		trades = append(trades, trade)

		order.RemainingQuantity -= tradeQty
		bestBuy.RemainingQuantity -= tradeQty

		if bestBuy.RemainingQuantity == 0 {
			heap.Pop(ob.BuyOrders)
		}
	}

	return trades
}

func (ob *OrderBook) matchBuyOrder(order *model.Order) []model.Trade {
	var trades []model.Trade

	for order.RemainingQuantity > 0 && ob.SellOrders.Len() > 0 {
		bestSell := (*ob.SellOrders)[0]

		if order.Price < bestSell.Price {
			break
		}

		tradeQty := min(order.RemainingQuantity, bestSell.RemainingQuantity)
		if tradeQty <= 0 {
			if bestSell.RemainingQuantity <= 0 {
				heap.Pop(ob.SellOrders)
				continue
			}
			break
		}

		trade := createTrade(bestSell, order, tradeQty)
		trades = append(trades, trade)

		order.RemainingQuantity -= tradeQty
		bestSell.RemainingQuantity -= tradeQty

		if bestSell.RemainingQuantity == 0 {
			heap.Pop(ob.SellOrders)
		}
	}

	if order.RemainingQuantity > 0 {
		heap.Push(ob.BuyOrders, order)
	}

	return trades
}

func (ob *OrderBook) matchSellOrder(order *model.Order) []model.Trade {
	var trades []model.Trade

	for order.RemainingQuantity > 0 && ob.BuyOrders.Len() > 0 {
		bestBuy := (*ob.BuyOrders)[0]

		if order.Price > bestBuy.Price {
			break
		}

		tradeQty := min(order.RemainingQuantity, bestBuy.RemainingQuantity)
		if tradeQty <= 0 {
			if bestBuy.RemainingQuantity <= 0 {
				heap.Pop(ob.BuyOrders)
				continue
			}
			break
		}

		trade := createTrade(bestBuy, order, tradeQty)
		trades = append(trades, trade)

		order.RemainingQuantity -= tradeQty
		bestBuy.RemainingQuantity -= tradeQty

		if bestBuy.RemainingQuantity == 0 {
			heap.Pop(ob.BuyOrders)
		}
	}

	if order.RemainingQuantity > 0 {
		heap.Push(ob.SellOrders, order)
	}

	return trades
}

func createTrade(restingOrder, incomingOrder *model.Order, qty money.Quantity) model.Trade {
	buyOrder := restingOrder
	sellOrder := incomingOrder

	if restingOrder.Side == model.Sell {
		buyOrder = incomingOrder
		sellOrder = restingOrder
	}

	return model.Trade{
		ID:           uuid.New(),
		Symbol:       restingOrder.Symbol,
		BuyerUserID:  buyOrder.UserID,
		SellerUserID: sellOrder.UserID,
		BuyOrderID:   buyOrder.ID,
		SellOrderID:  sellOrder.ID,
		Price:        restingOrder.Price,
		Quantity:     qty,
		ExecutedAt:   time.Now(),
	}
}

func min(a, b money.Quantity) money.Quantity {
	if a < b {
		return a
	}
	return b
}

type PriceLevel struct {
	Price    money.Money    `json:"price"`
	Quantity money.Quantity `json:"quantity"`
}

func (ob *OrderBook) Snapshot() (bids []PriceLevel, asks []PriceLevel) {
	for _, order := range *ob.BuyOrders {
		bids = append(bids, PriceLevel{Price: order.Price, Quantity: order.RemainingQuantity})
	}
	for _, order := range *ob.SellOrders {
		asks = append(asks, PriceLevel{Price: order.Price, Quantity: order.RemainingQuantity})
	}
	return
}

func (ob *OrderBook) Clone() *OrderBook {
	cloned := NewOrderBook()

	for _, order := range *ob.BuyOrders {
		orderCopy := *order
		*cloned.BuyOrders = append(*cloned.BuyOrders, &orderCopy)
	}

	for _, order := range *ob.SellOrders {
		orderCopy := *order
		*cloned.SellOrders = append(*cloned.SellOrders, &orderCopy)
	}

	return cloned
}
