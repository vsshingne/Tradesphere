package model

import (
	"time"

	"tradesphere/money"

	"github.com/google/uuid"
)

type Side string

const (
	Buy  Side = "BUY"
	Sell Side = "SELL"
)

type OrderType string

const (
	Limit  OrderType = "LIMIT"
	Market OrderType = "MARKET"
)

type OrderStatus string

const (
	New             OrderStatus = "NEW"
	PartiallyFilled OrderStatus = "PARTIALLY_FILLED"
	Filled          OrderStatus = "FILLED"
	Cancelled       OrderStatus = "CANCELLED"
)

type OrderCommandType string

const (
	CreateOrderCommand OrderCommandType = "CREATE_ORDER"
	CancelOrderCommand OrderCommandType = "CANCEL_ORDER"
)

type Order struct {
	ID                uuid.UUID      `json:"id"`
	UserID            uuid.UUID      `json:"user_id"`
	Symbol            string         `json:"symbol"`
	Side              Side           `json:"side"`
	Type              OrderType      `json:"type"`
	Price             money.Money    `json:"price"`
	Quantity          money.Quantity `json:"quantity"`
	RemainingQuantity money.Quantity `json:"remaining_quantity"`
	ReservedAmount    money.Money    `json:"reserved_amount"`
	Status            OrderStatus    `json:"status"`
	CreatedAt         time.Time      `json:"created_at"`
}

type CancelRequest struct {
	OrderID uuid.UUID `json:"order_id"`
	UserID  uuid.UUID `json:"user_id"`
	Symbol  string    `json:"symbol"`
	Side    Side      `json:"side"`
}

type OrderCommand struct {
	ID        uuid.UUID        `json:"id"`
	Type      OrderCommandType `json:"type"`
	Symbol    string           `json:"symbol"`
	Order     *Order           `json:"order,omitempty"`
	Cancel    *CancelRequest   `json:"cancel,omitempty"`
	CreatedAt time.Time        `json:"created_at"`
}
