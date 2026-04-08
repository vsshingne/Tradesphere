package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"tradesphere/order/database"
	"tradesphere/order/kafka"
	"tradesphere/order/model"

	"github.com/google/uuid"
)

type CreateOrderRequest struct {
	UserID   string  `json:"user_id"`
	Symbol   string  `json:"symbol"`
	Side     string  `json:"side"`
	Type     string  `json:"type"`
	Price    float64 `json:"price"`
	Quantity float64 `json:"quantity"`
}

type reservationRequest struct {
	OrderID        string  `json:"order_id,omitempty"`
	UserID         string  `json:"user_id"`
	Symbol         string  `json:"symbol"`
	Side           string  `json:"side"`
	Price          float64 `json:"price"`
	Quantity       float64 `json:"quantity"`
	ReservedAmount float64 `json:"reserved_amount"`
}

type portfolioErrorResponse struct {
	Error string `json:"error"`
}

var portfolioClient = &http.Client{Timeout: 3 * time.Second}

func main() {
	database.InitDB()

	http.HandleFunc("/orders", createOrderHandler)
	http.HandleFunc("/healthz", healthHandler)

	log.Println("Order Service running on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func createOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	userID, err := uuid.Parse(req.UserID)
	if err != nil {
		http.Error(w, "invalid user_id", http.StatusBadRequest)
		return
	}

	side := strings.ToUpper(strings.TrimSpace(req.Side))
	if side != string(model.Buy) && side != string(model.Sell) {
		http.Error(w, "side must be BUY or SELL", http.StatusBadRequest)
		return
	}

	orderType := model.Limit
	if req.Type != "" {
		orderType = model.OrderType(strings.ToUpper(strings.TrimSpace(req.Type)))
	}
	if orderType != model.Limit && orderType != model.Market {
		http.Error(w, "type must be LIMIT or MARKET", http.StatusBadRequest)
		return
	}

	symbol := strings.ToUpper(strings.TrimSpace(req.Symbol))
	if symbol == "" || req.Quantity <= 0 {
		http.Error(w, "symbol and quantity must be valid", http.StatusBadRequest)
		return
	}
	if orderType == model.Limit && req.Price <= 0 {
		http.Error(w, "price must be valid for LIMIT orders", http.StatusBadRequest)
		return
	}

	order := model.Order{
		ID:                uuid.New(),
		UserID:            userID,
		Symbol:            symbol,
		Side:              model.Side(side),
		Type:              orderType,
		Price:             req.Price,
		Quantity:          req.Quantity,
		RemainingQuantity: req.Quantity,
		ReservedAmount:    calculateReservedAmount(model.Side(side), req.Price, req.Quantity),
		Status:            model.New,
		CreatedAt:         time.Now(),
	}

	allowed, err := validateWithPortfolio(req.UserID, symbol, side, req.Price, req.Quantity)
	if err != nil {
		http.Error(w, "risk check unavailable", http.StatusInternalServerError)
		return
	}
	if !allowed {
		http.Error(w, "Risk check failed", http.StatusBadRequest)
		return
	}

	statusCode, message, err := callPortfolioReservation("reserve", order)
	if err != nil {
		http.Error(w, "reservation service unavailable", http.StatusInternalServerError)
		return
	}
	if statusCode != http.StatusOK {
		http.Error(w, message, statusCode)
		return
	}

	if err := database.InsertOrder(order); err != nil {
		if releaseErr := rollbackReservation(order); releaseErr != nil {
			log.Printf("failed to release reservation for order %s after DB error: %v", order.ID, releaseErr)
		}
		http.Error(w, "failed to persist order", http.StatusInternalServerError)
		return
	}

	if err := kafka.PublishOrder(order); err != nil {
		if releaseErr := rollbackReservation(order); releaseErr != nil {
			log.Printf("failed to release reservation for order %s after Kafka publish error: %v", order.ID, releaseErr)
		}
		if deleteErr := database.DeleteOrder(order.ID.String()); deleteErr != nil {
			log.Printf("failed to delete order %s after Kafka publish error: %v", order.ID, deleteErr)
		}
		http.Error(w, "failed to publish order", http.StatusInternalServerError)
		return
	}

	log.Printf("Order published: %s | %s %s %s @ %.2f Qty: %.2f", order.ID, order.Type, order.Side, order.Symbol, order.Price, order.Quantity)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(order)
}

func validateWithPortfolio(userID, symbol, side string, price, qty float64) (bool, error) {
	url := fmt.Sprintf(
		"http://portfolio-service:8081/validate?user_id=%s&symbol=%s&side=%s&price=%f&quantity=%f",
		userID, symbol, side, price, qty,
	)

	resp, err := portfolioClient.Get(url)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("validate returned status %d", resp.StatusCode)
	}

	var result map[string]bool
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return false, err
	}

	return result["allowed"], nil
}

func rollbackReservation(order model.Order) error {
	statusCode, message, err := callPortfolioReservation("release", order)
	if err != nil {
		return err
	}
	if statusCode != http.StatusOK {
		return fmt.Errorf("release failed with status %d: %s", statusCode, message)
	}
	return nil
}

func callPortfolioReservation(path string, order model.Order) (int, string, error) {
	reqBody := reservationRequest{
		OrderID:        order.ID.String(),
		UserID:         order.UserID.String(),
		Symbol:         order.Symbol,
		Side:           string(order.Side),
		Price:          order.Price,
		Quantity:       order.Quantity,
		ReservedAmount: order.ReservedAmount,
	}

	payload, err := json.Marshal(reqBody)
	if err != nil {
		return http.StatusInternalServerError, "", err
	}

	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("http://portfolio-service:8081/%s", path),
		bytes.NewReader(payload),
	)
	if err != nil {
		return http.StatusInternalServerError, "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := portfolioClient.Do(req)
	if err != nil {
		return http.StatusInternalServerError, "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return resp.StatusCode, "", nil
	}

	var portfolioErr portfolioErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&portfolioErr); err != nil || portfolioErr.Error == "" {
		portfolioErr.Error = "portfolio request failed"
	}

	return resp.StatusCode, portfolioErr.Error, nil
}

func calculateReservedAmount(side model.Side, price, quantity float64) float64 {
	if side == model.Buy {
		return price * quantity
	}
	return quantity
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "order-service"})
}
