package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"tradesphere/money"
	"tradesphere/order/database"
	"tradesphere/order/kafka"
	"tradesphere/order/model"
	"tradesphere/order/telemetry"
	"tradesphere/auth"
	"tradesphere/observability"
	"golang.org/x/time/rate"
	"github.com/google/uuid"
)

type CreateOrderRequest struct {
	UserID   string         `json:"user_id"`
	Symbol   string         `json:"symbol"`
	Side     string         `json:"side"`
	Type     string         `json:"type"`
	Price    money.Money    `json:"price"`
	Quantity money.Quantity `json:"quantity"`
}

type reservationRequest struct {
	OrderID        string         `json:"order_id,omitempty"`
	UserID         string         `json:"user_id"`
	Symbol         string         `json:"symbol"`
	Side           string         `json:"side"`
	Price          money.Money    `json:"price"`
	Quantity       money.Quantity `json:"quantity"`
	ReservedAmount money.Money    `json:"reserved_amount"`
}

type portfolioErrorResponse struct {
	Error string `json:"error"`
}

var portfolioClient = &http.Client{Timeout: 3 * time.Second}

var (
	httpRequestsTotal     = telemetry.Counter("http_requests_total", "Total HTTP requests handled by order-service.")
	ordersCreatedTotal    = telemetry.Counter("orders_created_total", "Total orders successfully created.")
	cancelRequestsTotal   = telemetry.Counter("order_cancel_requests_total", "Total cancel requests accepted by order-service.")
	portfolioCallDuration = telemetry.Duration("portfolio_http_duration_seconds", "Duration of portfolio-service HTTP calls from order-service.")
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	database.InitDB()
	
	shutdown := observability.Init("order-service")
	defer shutdown(context.Background())

	go kafka.StartOrderOutboxPublisher(ctx)

	authMiddleware := func(next http.Handler) http.Handler {
		return auth.RateLimit(rate.Limit(10), 20)(auth.RequireAuth("user")(next))
	}

	mux := http.NewServeMux()
	mux.Handle("/orders", authMiddleware(http.HandlerFunc(createOrderHandler)))
	mux.Handle("/orders/", authMiddleware(http.HandlerFunc(orderActionHandler)))
	mux.HandleFunc("/healthz", healthHandler)
	mux.Handle("/internal-metrics", telemetry.Handler())
	mux.Handle("/metrics", observability.MetricsHandler())

	// Wrap mux with observability
	handler := observability.HTTPMiddleware(telemetry.RequestIDMiddleware(mux))

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("order-service shutdown error: %v", err)
		}
	}()

	log.Println("Order Service running on :8080")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func createOrderHandler(w http.ResponseWriter, r *http.Request) {
	httpRequestsTotal.Inc()
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

	userIDStr, ok := r.Context().Value(auth.UserIDKey).(string)
	if !ok || req.UserID != userIDStr {
		http.Error(w, "user_id in request does not match token", http.StatusUnauthorized)
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
	if orderType != model.Limit {
		http.Error(w, "only LIMIT orders are supported", http.StatusBadRequest)
		return
	}

	symbol := strings.ToUpper(strings.TrimSpace(req.Symbol))
	if symbol == "" || req.Quantity <= 0 {
		http.Error(w, "symbol and quantity must be valid", http.StatusBadRequest)
		return
	}
	if req.Price <= 0 {
		http.Error(w, "price must be valid for LIMIT orders", http.StatusBadRequest)
		return
	}

	reservedAmount, err := calculateReservedAmount(model.Side(side), req.Price, req.Quantity)
	if err != nil {
		http.Error(w, "price and quantity cannot be represented exactly", http.StatusBadRequest)
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
		ReservedAmount:    reservedAmount,
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

	if err := database.InsertOrderWithOutbox(order); err != nil {
		if releaseErr := rollbackReservation(order); releaseErr != nil {
			log.Printf("failed to release reservation for order %s after DB error: %v", order.ID, releaseErr)
		}
		http.Error(w, "failed to persist order", http.StatusInternalServerError)
		return
	}

	log.Printf("Order accepted: %s | %s %s %s @ %s Qty: %s", order.ID, order.Type, order.Side, order.Symbol, money.MoneyToDecimal(order.Price), money.QuantityToDecimal(order.Quantity))
	ordersCreatedTotal.Inc()
	observability.OrdersTotal.Inc()
	telemetry.Info("order_created", map[string]interface{}{
		"request_id": telemetry.RequestIDFromContext(r.Context()),
		"user_id":    order.UserID.String(),
		"order_id":   order.ID.String(),
		"symbol":     order.Symbol,
		"side":       order.Side,
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(order)
}

func orderActionHandler(w http.ResponseWriter, r *http.Request) {
	httpRequestsTotal.Inc()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/orders/")
	if !strings.HasSuffix(path, "/cancel") {
		http.NotFound(w, r)
		return
	}

	orderIDStr := strings.TrimSuffix(path, "/cancel")
	orderIDStr = strings.TrimSuffix(orderIDStr, "/")
	orderID, err := uuid.Parse(strings.TrimSpace(orderIDStr))
	if err != nil {
		http.Error(w, "invalid order_id", http.StatusBadRequest)
		return
	}

	order, err := database.GetOrder(orderID)
	if err != nil {
		http.Error(w, "order not found", http.StatusNotFound)
		return
	}

	userIDStr, ok := r.Context().Value(auth.UserIDKey).(string)
	if !ok || order.UserID.String() != userIDStr {
		http.Error(w, "forbidden: order belongs to another user", http.StatusForbidden)
		return
	}

	if order.Status == model.Filled || order.Status == model.Cancelled || order.RemainingQuantity <= 0 {
		http.Error(w, "order is not cancelable", http.StatusBadRequest)
		return
	}

	if err := database.EnqueueCancelCommand(model.CancelRequest{
		OrderID: order.ID,
		UserID:  order.UserID,
		Symbol:  order.Symbol,
		Side:    order.Side,
	}); err != nil {
		http.Error(w, "failed to enqueue cancel request", http.StatusInternalServerError)
		return
	}
	cancelRequestsTotal.Inc()
	telemetry.Info("order_cancel_requested", map[string]interface{}{
		"request_id": telemetry.RequestIDFromContext(r.Context()),
		"user_id":    order.UserID.String(),
		"order_id":   order.ID.String(),
		"symbol":     order.Symbol,
	})

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]string{
		"order_id": order.ID.String(),
		"status":   "cancel_requested",
	})
}

func validateWithPortfolio(userID, symbol, side string, price money.Money, qty money.Quantity) (bool, error) {
	start := time.Now()
	defer portfolioCallDuration.Observe(time.Since(start))

	url := fmt.Sprintf(
		"http://portfolio-service:8081/validate?user_id=%s&symbol=%s&side=%s&price=%s&quantity=%s",
		userID, symbol, side, money.MoneyToDecimal(price), money.QuantityToDecimal(qty),
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
	start := time.Now()
	defer portfolioCallDuration.Observe(time.Since(start))

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

func calculateReservedAmount(side model.Side, price money.Money, quantity money.Quantity) (money.Money, error) {
	if side == model.Buy {
		return money.CostFor(price, quantity)
	}
	return money.Money(quantity), nil
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	httpRequestsTotal.Inc()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "order-service"})
}
