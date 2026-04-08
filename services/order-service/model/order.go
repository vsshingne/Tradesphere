package model

import (
	"time"

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

type Order struct {
	ID                uuid.UUID   `json:"id"`
	UserID            uuid.UUID   `json:"user_id"`
	Symbol            string      `json:"symbol"`
	Side              Side        `json:"side"`
	Type              OrderType   `json:"type"`
	Price             float64     `json:"price"`
	Quantity          float64     `json:"quantity"`
	RemainingQuantity float64     `json:"remaining_quantity"`
	ReservedAmount    float64     `json:"reserved_amount"`
	Status            OrderStatus `json:"status"`
	CreatedAt         time.Time   `json:"created_at"`
}
