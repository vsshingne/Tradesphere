package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

var (
	clients   = make(map[*websocket.Conn]bool)
	clientsMu sync.Mutex
	upgrader  = websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
)

type tradeMessage struct {
	ID     string `json:"id"`
	Symbol string `json:"symbol"`
}

type orderEvent struct {
	ID     string `json:"id"`
	Type   string `json:"type"`
	Symbol string `json:"symbol"`
}

type outboundMessage struct {
	Type   string          `json:"type"`
	Symbol string          `json:"symbol"`
	Data   json.RawMessage `json:"data"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go consumeTrades(ctx)
	go consumeOrderEvents(ctx)

	http.HandleFunc("/ws", handleConnections)
	http.HandleFunc("/healthz", healthHandler)

	log.Println("WebSocket service running on :8083")
	log.Fatal(http.ListenAndServe(":8083", nil))
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "websocket-service"})
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	clientsMu.Lock()
	clients[ws] = true
	clientsMu.Unlock()

	go func() {
		defer removeClient(ws)
		for {
			if _, _, err := ws.ReadMessage(); err != nil {
				return
			}
		}
	}()
}

func consumeTrades(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{getKafkaBroker()},
		Topic:   "trades",
		GroupID: "websocket-service-trades",
	})
	defer r.Close()

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				log.Println("WebSocket trade consumer stopped")
				return
			}
			log.Println("Kafka error:", err)
			continue
		}

		payload, err := buildTradeMessage(m.Value)
		if err != nil {
			log.Println("Invalid trade payload:", err)
			continue
		}

		broadcast(payload)

		if err := r.CommitMessages(ctx, m); err != nil {
			log.Println("Trade commit failed:", err)
			continue
		}
	}
}

func consumeOrderEvents(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{getKafkaBroker()},
		Topic:   "order-events",
		GroupID: "websocket-service-order-events",
	})
	defer r.Close()

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				log.Println("WebSocket order event consumer stopped")
				return
			}
			log.Println("Kafka order event error:", err)
			continue
		}

		payload, err := buildOrderEventMessage(m.Value)
		if err != nil {
			log.Println("Invalid order event payload:", err)
			continue
		}
		if payload == nil {
			if err := r.CommitMessages(ctx, m); err != nil {
				log.Println("Order event commit failed:", err)
			}
			continue
		}

		broadcast(payload)

		if err := r.CommitMessages(ctx, m); err != nil {
			log.Println("Order event commit failed:", err)
			continue
		}
	}
}

func broadcast(message []byte) {
	clientsMu.Lock()
	defer clientsMu.Unlock()

	for client := range clients {
		_ = client.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if err := client.WriteMessage(websocket.TextMessage, message); err != nil {
			_ = client.Close()
			delete(clients, client)
		}
	}
}

func buildTradeMessage(raw []byte) ([]byte, error) {
	var trade tradeMessage
	if err := json.Unmarshal(raw, &trade); err != nil {
		return nil, err
	}

	return json.Marshal(outboundMessage{
		Type:   "TRADE",
		Symbol: trade.Symbol,
		Data:   json.RawMessage(raw),
	})
}

func buildOrderEventMessage(raw []byte) ([]byte, error) {
	var event orderEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return nil, err
	}

	messageType := ""
	switch event.Type {
	case "ORDER_UPDATE":
		messageType = "ORDER_UPDATE"
	case "ORDER_CANCELLED":
		messageType = "CANCEL"
	default:
		return nil, nil
	}

	return json.Marshal(outboundMessage{
		Type:   messageType,
		Symbol: event.Symbol,
		Data:   json.RawMessage(raw),
	})
}

func getKafkaBroker() string {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		return "kafka:9092"
	}
	return broker
}

func removeClient(ws *websocket.Conn) {
	clientsMu.Lock()
	delete(clients, ws)
	clientsMu.Unlock()
	_ = ws.Close()
}
