package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"

	"tradesphere/matching/database"
	"tradesphere/matching/engine"
	"tradesphere/matching/kafka"
	"tradesphere/matching/telemetry"
	"tradesphere/observability"
)

var httpRequestsTotal = telemetry.Counter("http_requests_total", "Total HTTP requests handled by matching-engine.")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	database.InitDB()
	
	shutdown := observability.Init("matching-engine")
	defer shutdown(context.Background())

	me := engine.NewMatchingEngine()

	openOrders, err := database.LoadOpenOrders()
	if err != nil {
		log.Fatalf("failed to load open orders for recovery: %v", err)
	}
	restored := me.RestoreOrders(openOrders)

	telemetry.Info("matching_engine_starting", map[string]interface{}{"restored_open_orders": restored})
	go kafka.StartOrderConsumer(ctx, me)
	go kafka.StartTradeOutboxPublisher(ctx)

	startHTTPServer(me)
}

func startHTTPServer(me *engine.MatchingEngine) {
	mux := http.NewServeMux()
	mux.HandleFunc("/orderbook/", func(w http.ResponseWriter, r *http.Request) {
		httpRequestsTotal.Inc()
		symbol := strings.TrimPrefix(r.URL.Path, "/orderbook/")
		ob := me.GetOrderBookSnapshot(symbol)

		if ob == nil {
			http.Error(w, "Symbol not found", http.StatusNotFound)
			return
		}

		bids, asks := ob.Snapshot()

		json.NewEncoder(w).Encode(map[string]interface{}{
			"symbol": symbol,
			"bids":   bids,
			"asks":   asks,
		})
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		httpRequestsTotal.Inc()
		if err := database.DB.Ping(); err != nil {
			http.Error(w, "db unreachable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "matching-engine"})
	})
	mux.Handle("/internal-metrics", telemetry.Handler())
	mux.Handle("/metrics", observability.MetricsHandler())
	
	handler := observability.HTTPMiddleware(telemetry.RequestIDMiddleware(mux))
	log.Println("OrderBook API running on :8082")
	log.Fatal(http.ListenAndServe(":8082", handler))
}
