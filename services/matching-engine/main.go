package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"strings"
	"syscall"

	"tradesphere/matching/database"
	"tradesphere/matching/engine"
	"tradesphere/matching/kafka"
	"tradesphere/matching/model"

	"github.com/google/uuid"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	me := engine.NewMatchingEngine()
	database.InitDB()

	log.Println("Starting Matching Engine...")
	go kafka.StartOrderConsumer(ctx, me)
	go kafka.StartTradeOutboxPublisher(ctx)

	startHTTPServer(me)
}

func startHTTPServer(me *engine.MatchingEngine) {

	http.HandleFunc("/orderbook/", func(w http.ResponseWriter, r *http.Request) {
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

	http.HandleFunc("/cancel/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		idStr := strings.TrimPrefix(r.URL.Path, "/cancel/")
		id, err := uuid.Parse(idStr)
		if err != nil {
			http.Error(w, "Invalid order ID", http.StatusBadRequest)
			return
		}

		order, err := database.GetOrder(id)
		if err != nil {
			http.Error(w, "order not found", http.StatusNotFound)
			return
		}
		if order.Status == model.Filled || order.Status == model.Cancelled || order.RemainingQuantity <= 0 {
			http.Error(w, "order is not cancelable", http.StatusBadRequest)
			return
		}

		inMemoryCancelled := false
		memRemaining, memStatus, err := me.CancelOrder(id)
		if err == nil {
			inMemoryCancelled = true
		} else if !errors.Is(err, engine.ErrOrderNotFound) {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		order.RemainingQuantity = 0
		order.Status = model.Cancelled
		if err := database.PersistCancelledOrder(order); err != nil {
			if inMemoryCancelled {
				_ = me.RestoreOrder(id, memRemaining, memStatus)
			}
			http.Error(w, "failed to persist cancel", http.StatusInternalServerError)
			return
		}

		w.Write([]byte("Order canceled"))
	})

	log.Println("OrderBook API running on :8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}
