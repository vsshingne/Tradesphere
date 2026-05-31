package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"tradesphere/money"
	"tradesphere/portfolio/database"
	"tradesphere/portfolio/kafka"
	"tradesphere/portfolio/telemetry"

	"tradesphere/auth"
	"tradesphere/observability"
	"golang.org/x/time/rate"
	"github.com/google/uuid"
)

type Position struct {
	Symbol   string         `json:"symbol"`
	Quantity money.Quantity `json:"quantity"`
}

var (
	httpRequestsTotal  = telemetry.Counter("http_requests_total", "Total HTTP requests handled by portfolio-service.")
	reservationsTotal  = telemetry.Counter("reservations_total", "Total successful reservations handled by portfolio-service.")
	releasesTotal      = telemetry.Counter("releases_total", "Total successful reservation releases handled by portfolio-service.")
	validationDuration = telemetry.Duration("validation_duration_seconds", "Duration of validation requests in portfolio-service.")
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	database.InitDB()
	
	shutdown := observability.Init("portfolio-service")
	defer shutdown(context.Background())

	go kafka.StartTradeConsumer(ctx)
	go kafka.StartOrderEventConsumer(ctx)
	go startReconciliationWorker(ctx)

	startHTTPServer(ctx)
}

func startHTTPServer(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/validate", validateHandler)
	mux.HandleFunc("/reserve", reserveHandler)
	mux.HandleFunc("/release", releaseHandler)

	authMiddleware := func(next http.Handler) http.Handler {
		return auth.RateLimit(rate.Limit(10), 20)(auth.RequireAuth("user")(next))
	}
	mux.Handle("/portfolio/", authMiddleware(http.HandlerFunc(portfolioHandler)))

	mux.Handle("/metrics", observability.MetricsHandler())
	mux.Handle("/internal-metrics", telemetry.Handler()) // keep existing one

	// Wrap with observability middleware
	handler := observability.HTTPMiddleware(telemetry.RequestIDMiddleware(mux))

	srv := &http.Server{Addr: ":8081", Handler: handler}

	go func() {
		<-ctx.Done()
		log.Println("Portfolio API shutdown initiated")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("Portfolio API shutdown error: %v", err)
		}
	}()

	log.Println("Portfolio API running on :8081")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	} else if err == http.ErrServerClosed {
		log.Println("Portfolio API server stopped")
	}
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	httpRequestsTotal.Inc()
	if err := database.DB.Ping(); err != nil {
		writeJSONError(w, "db unreachable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "portfolio-service"})
}

func portfolioHandler(w http.ResponseWriter, r *http.Request) {
	httpRequestsTotal.Inc()
	path := strings.TrimPrefix(r.URL.Path, "/portfolio/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		http.Error(w, "missing user id", http.StatusBadRequest)
		return
	}

	userID := parts[0]
	userIDStr, ok := r.Context().Value(auth.UserIDKey).(string)
	if !ok || userID != userIDStr {
		http.Error(w, "forbidden: cannot view other user's portfolio", http.StatusForbidden)
		return
	}

	if len(parts) == 1 {
		getFullPortfolio(w, userID)
		return
	}
	if len(parts) == 2 {
		switch parts[1] {
		case "balance":
			getBalance(w, userID)
		case "positions":
			getPositions(w, userID)
		default:
			http.NotFound(w, r)
		}
		return
	}
	http.NotFound(w, r)
}

func getBalance(w http.ResponseWriter, userID string) {
	var balance int64
	err := database.DB.QueryRow("SELECT balance FROM users WHERE id = $1", userID).Scan(&balance)
	if err != nil {
		http.Error(w, "User not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"user_id": userID, "balance": money.Money(balance)})
}

func getPositions(w http.ResponseWriter, userID string) {
	rows, err := database.DB.Query("SELECT symbol, quantity FROM positions WHERE user_id = $1", userID)
	if err != nil {
		http.Error(w, "Error fetching positions", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	positions := make([]Position, 0)
	for rows.Next() {
		var p Position
		var quantity int64
		if err := rows.Scan(&p.Symbol, &quantity); err != nil {
			http.Error(w, "Error scanning positions", http.StatusInternalServerError)
			return
		}
		p.Quantity = money.Quantity(quantity)
		positions = append(positions, p)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "Error fetching positions", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"user_id": userID, "positions": positions})
}

func getFullPortfolio(w http.ResponseWriter, userID string) {
	var balance int64
	err := database.DB.QueryRow("SELECT balance FROM users WHERE id = $1", userID).Scan(&balance)
	if err != nil {
		http.Error(w, "User not found", http.StatusNotFound)
		return
	}

	rows, err := database.DB.Query("SELECT symbol, quantity FROM positions WHERE user_id = $1", userID)
	if err != nil {
		http.Error(w, "Error fetching positions", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	positions := make([]Position, 0)
	for rows.Next() {
		var p Position
		var quantity int64
		if err := rows.Scan(&p.Symbol, &quantity); err != nil {
			http.Error(w, "Error scanning positions", http.StatusInternalServerError)
			return
		}
		p.Quantity = money.Quantity(quantity)
		positions = append(positions, p)
	}
	if err := rows.Err(); err != nil {
		http.Error(w, "Error fetching positions", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"user_id":   userID,
		"balance":   money.Money(balance),
		"positions": positions,
	})
}

func validateHandler(w http.ResponseWriter, r *http.Request) {
	httpRequestsTotal.Inc()
	start := time.Now()
	defer validationDuration.Observe(time.Since(start))

	w.Header().Set("Content-Type", "application/json")

	userID := r.URL.Query().Get("user_id")
	symbol := r.URL.Query().Get("symbol")
	side := strings.ToUpper(r.URL.Query().Get("side"))
	priceStr := r.URL.Query().Get("price")
	qtyStr := r.URL.Query().Get("quantity")

	if userID == "" || symbol == "" || side == "" || priceStr == "" || qtyStr == "" {
		writeJSONError(w, "missing required parameters", http.StatusBadRequest)
		return
	}

	if _, err := uuid.Parse(userID); err != nil {
		log.Printf("Validation failed: invalid user_id=%q", userID)
		writeJSONError(w, "invalid user_id", http.StatusBadRequest)
		return
	}

	price, err := money.MoneyFromDecimal(priceStr)
	if err != nil || price <= 0 {
		log.Printf("Validation failed: invalid price=%q", priceStr)
		writeJSONError(w, "invalid price", http.StatusBadRequest)
		return
	}

	qty, err := money.QuantityFromDecimal(qtyStr)
	if err != nil || qty <= 0 {
		log.Printf("Validation failed: invalid quantity=%q", qtyStr)
		writeJSONError(w, "invalid quantity", http.StatusBadRequest)
		return
	}

	allowed := false

	switch side {
	case "BUY":
		var balance, reserved int64
		err := database.DB.QueryRow(
			`SELECT balance, reserved_balance FROM users WHERE id = $1`,
			userID,
		).Scan(&balance, &reserved)

		if err == sql.ErrNoRows {
			log.Printf("Validation failed: user not found user_id=%s", userID)
		}
		cost, costErr := money.CostFor(price, qty)
		if costErr == nil && err == nil && money.Money(balance)-money.Money(reserved) >= cost {
			allowed = true
		}

	case "SELL":
		var position, reserved int64
		err := database.DB.QueryRow(
			`SELECT quantity, reserved_quantity FROM positions WHERE user_id = $1 AND symbol = $2`,
			userID, symbol,
		).Scan(&position, &reserved)

		if err == sql.ErrNoRows {
			log.Printf("Validation failed: position not found user_id=%s symbol=%s", userID, symbol)
		}
		if err == nil && money.Quantity(position)-money.Quantity(reserved) >= qty {
			allowed = true
		}

	default:
		log.Printf("Validation failed: invalid side=%q", side)
		writeJSONError(w, "invalid side (must be BUY or SELL)", http.StatusBadRequest)
		return
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"allowed": allowed,
	})
}

func writeJSONError(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

func reserveHandler(w http.ResponseWriter, r *http.Request) {
	httpRequestsTotal.Inc()
	if r.Method != http.MethodPost {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req struct {
		OrderID        string         `json:"order_id,omitempty"`
		UserID         string         `json:"user_id"`
		Symbol         string         `json:"symbol"`
		Side           string         `json:"side"`
		Price          money.Money    `json:"price"`
		Quantity       money.Quantity `json:"quantity"`
		ReservedAmount money.Money    `json:"reserved_amount"`
	}

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeJSONError(w, "invalid request", http.StatusBadRequest)
		return
	}
	req.Side = strings.ToUpper(strings.TrimSpace(req.Side))
	if req.Side != "BUY" && req.Side != "SELL" {
		writeJSONError(w, "invalid side", http.StatusBadRequest)
		return
	}

	tx, err := database.DB.Begin()
	if err != nil {
		writeJSONError(w, "db error", http.StatusInternalServerError)
		return
	}

	if req.Side == "BUY" {

		var balance, reserved int64

		err := tx.QueryRow(`
		SELECT balance, reserved_balance
		FROM users
		WHERE id = $1
		FOR UPDATE
		`, req.UserID).Scan(&balance, &reserved)

		if err != nil {
			tx.Rollback()
			writeJSONError(w, "user not found", http.StatusBadRequest)
			return
		}

		cost := req.ReservedAmount
		if cost <= 0 {
			cost, err = money.CostFor(req.Price, req.Quantity)
			if err != nil {
				tx.Rollback()
				writeJSONError(w, "price and quantity cannot be represented exactly", http.StatusBadRequest)
				return
			}
		}
		available := money.Money(balance) - money.Money(reserved)

		if available < cost {
			tx.Rollback()
			observability.ReservationFailuresTotal.Inc()
			writeJSONError(w, "insufficient balance", http.StatusBadRequest)
			return
		}

		_, err = tx.Exec(`
		UPDATE users
		SET reserved_balance = reserved_balance + $1
		WHERE id = $2
		`, cost, req.UserID)

		if err != nil {
			tx.Rollback()
			writeJSONError(w, "reservation failed", http.StatusInternalServerError)
			return
		}

	} else {

		var qty, reserved int64

		err := tx.QueryRow(`
		SELECT quantity, reserved_quantity
		FROM positions
		WHERE user_id = $1 AND symbol = $2
		FOR UPDATE
		`, req.UserID, req.Symbol).Scan(&qty, &reserved)

		if err != nil {
			tx.Rollback()
			writeJSONError(w, "position not found", http.StatusBadRequest)
			return
		}

		available := money.Quantity(qty) - money.Quantity(reserved)

		releaseQty := money.Quantity(req.ReservedAmount)
		if releaseQty <= 0 {
			releaseQty = req.Quantity
		}
		if available < releaseQty {
			tx.Rollback()
			observability.ReservationFailuresTotal.Inc()
			writeJSONError(w, "insufficient position", http.StatusBadRequest)
			return
		}

		_, err = tx.Exec(`
		UPDATE positions
		SET reserved_quantity = reserved_quantity + $1
		WHERE user_id = $2 AND symbol = $3
		`, releaseQty, req.UserID, req.Symbol)

		if err != nil {
			tx.Rollback()
			writeJSONError(w, "reservation failed", http.StatusInternalServerError)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		writeJSONError(w, "reservation commit failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"reserved"}`))
	reservationsTotal.Inc()
}

func releaseHandler(w http.ResponseWriter, r *http.Request) {
	httpRequestsTotal.Inc()
	if r.Method != http.MethodPost {
		writeJSONError(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	defer r.Body.Close()

	var req struct {
		OrderID        string         `json:"order_id,omitempty"`
		UserID         string         `json:"user_id"`
		Symbol         string         `json:"symbol"`
		Side           string         `json:"side"`
		Price          money.Money    `json:"price"`
		Quantity       money.Quantity `json:"quantity"`
		ReservedAmount money.Money    `json:"reserved_amount"`
	}

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		writeJSONError(w, "invalid request", http.StatusBadRequest)
		return
	}
	req.Side = strings.ToUpper(strings.TrimSpace(req.Side))
	if req.Side != "BUY" && req.Side != "SELL" {
		writeJSONError(w, "invalid side", http.StatusBadRequest)
		return
	}

	tx, err := database.DB.Begin()
	if err != nil {
		writeJSONError(w, "db error", http.StatusInternalServerError)
		return
	}

	if req.Side == "BUY" {
		var reserved int64
		err := tx.QueryRow(`
		SELECT reserved_balance
		FROM users
		WHERE id = $1
		FOR UPDATE
		`, req.UserID).Scan(&reserved)
		if err != nil {
			tx.Rollback()
			writeJSONError(w, "user not found", http.StatusBadRequest)
			return
		}

		cost := req.ReservedAmount
		if cost <= 0 {
			cost, err = money.CostFor(req.Price, req.Quantity)
			if err != nil {
				tx.Rollback()
				writeJSONError(w, "price and quantity cannot be represented exactly", http.StatusBadRequest)
				return
			}
		}
		if req.OrderID != "" {
			orderID, parseErr := uuid.Parse(req.OrderID)
			if parseErr != nil {
				tx.Rollback()
				writeJSONError(w, "invalid order_id", http.StatusBadRequest)
				return
			}
			row, orderErr := database.LockOrderReservation(tx, orderID)
			if orderErr == nil {
				cost = row.ReservedAmount
				_, err = tx.Exec(`
				UPDATE orders
				SET reserved_amount = 0
				WHERE id = $1
				`, orderID)
				if err != nil {
					tx.Rollback()
					writeJSONError(w, "release failed", http.StatusInternalServerError)
					return
				}
			}
		}
		if money.Money(reserved) < cost {
			tx.Rollback()
			writeJSONError(w, "insufficient reserved balance", http.StatusBadRequest)
			return
		}

		_, err = tx.Exec(`
		UPDATE users
		SET reserved_balance = reserved_balance - $1
		WHERE id = $2
		`, cost, req.UserID)
		if err != nil {
			tx.Rollback()
			writeJSONError(w, "release failed", http.StatusInternalServerError)
			return
		}
	} else {
		var reserved int64
		orderReleaseQty := money.Quantity(req.ReservedAmount)
		err := tx.QueryRow(`
		SELECT reserved_quantity
		FROM positions
		WHERE user_id = $1 AND symbol = $2
		FOR UPDATE
		`, req.UserID, req.Symbol).Scan(&reserved)
		if err != nil {
			tx.Rollback()
			writeJSONError(w, "position not found", http.StatusBadRequest)
			return
		}

		if req.OrderID != "" {
			orderID, parseErr := uuid.Parse(req.OrderID)
			if parseErr != nil {
				tx.Rollback()
				writeJSONError(w, "invalid order_id", http.StatusBadRequest)
				return
			}
			row, orderErr := database.LockOrderReservation(tx, orderID)
			if orderErr == nil {
				orderReleaseQty = money.Quantity(row.ReservedAmount)
				_, err = tx.Exec(`
				UPDATE orders
				SET reserved_amount = 0
				WHERE id = $1
				`, orderID)
				if err != nil {
					tx.Rollback()
					writeJSONError(w, "release failed", http.StatusInternalServerError)
					return
				}
			}
		}
		if orderReleaseQty <= 0 {
			orderReleaseQty = req.Quantity
		}
		if money.Quantity(reserved) < orderReleaseQty {
			tx.Rollback()
			writeJSONError(w, "insufficient reserved quantity", http.StatusBadRequest)
			return
		}

		_, err = tx.Exec(`
		UPDATE positions
		SET reserved_quantity = reserved_quantity - $1
		WHERE user_id = $2 AND symbol = $3
		`, orderReleaseQty, req.UserID, req.Symbol)
		if err != nil {
			tx.Rollback()
			writeJSONError(w, "release failed", http.StatusInternalServerError)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		writeJSONError(w, "release commit failed", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(`{"status":"released"}`))
	releasesTotal.Inc()
}
