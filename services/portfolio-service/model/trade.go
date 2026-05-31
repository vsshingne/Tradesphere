package model

import (
	"time"

	"tradesphere/money"

	"github.com/google/uuid"
)

type Trade struct {
	ID           uuid.UUID      `json:"id"`
	Symbol       string         `json:"symbol"`
	BuyerUserID  uuid.UUID      `json:"buyer_user_id"`
	SellerUserID uuid.UUID      `json:"seller_user_id"`
	BuyOrderID   uuid.UUID      `json:"buy_order_id"`
	SellOrderID  uuid.UUID      `json:"sell_order_id"`
	Price        money.Money    `json:"price"`
	Quantity     money.Quantity `json:"quantity"`
	ExecutedAt   time.Time      `json:"executed_at"`
}
