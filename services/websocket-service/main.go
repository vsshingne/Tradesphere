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

	"tradesphere/websocket/database"
	"tradesphere/websocket/telemetry"
	"tradesphere/observability"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/segmentio/kafka-go"
)

var (
	clients   = make(map[*wsClient]bool)
	clientsMu sync.Mutex
	upgrader  = websocket.Upgrader{CheckOrigin: func(_ *http.Request) bool { return true }}
)

var (
	httpRequestsTotal         = telemetry.Counter("http_requests_total", "Total HTTP requests handled by websocket-service.")
	websocketConnectionsGauge = telemetry.Gauge("websocket_connections", "Current active websocket connections.")
	messagesBroadcastTotal    = telemetry.Counter("websocket_messages_broadcast_total", "Total websocket messages broadcast.")
	kafkaEventsProcessedTotal = telemetry.Counter("kafka_events_processed_total", "Total Kafka events processed by websocket-service.")
)

const (
	writeTimeout = 5 * time.Second
	pongWait     = 60 * time.Second
	pingPeriod   = 30 * time.Second
)

type wsClient struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

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

	database.InitDB()
	
	shutdown := observability.Init("websocket-service")
	defer shutdown(context.Background())

	go consumeTrades(ctx)
	go consumeOrderEvents(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", handleConnections)
	mux.HandleFunc("/healthz", healthHandler)
	mux.Handle("/internal-metrics", telemetry.Handler())
	mux.Handle("/metrics", observability.MetricsHandler())
	
	handler := observability.HTTPMiddleware(telemetry.RequestIDMiddleware(mux))

	srv := &http.Server{
		Addr:              ":8083",
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Println("WebSocket service running on :8083")
	log.Fatal(srv.ListenAndServe())
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	httpRequestsTotal.Inc()
	if err := database.DB.Ping(); err != nil {
		http.Error(w, "db unreachable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "service": "websocket-service"})
}

func handleConnections(w http.ResponseWriter, r *http.Request) {
	httpRequestsTotal.Inc()
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	client := &wsClient{conn: ws}
	_ = ws.SetReadDeadline(time.Now().Add(pongWait))
	ws.SetReadLimit(1024)
	ws.SetPongHandler(func(string) error {
		return ws.SetReadDeadline(time.Now().Add(pongWait))
	})

	clientsMu.Lock()
	clients[client] = true
	clientsMu.Unlock()
	websocketConnectionsGauge.Inc()
	observability.WebsocketClientsActive.Inc()
	telemetry.Info("websocket_connected", map[string]interface{}{
		"request_id": telemetry.RequestIDFromContext(r.Context()),
	})

	go func() {
		ticker := time.NewTicker(pingPeriod)
		defer ticker.Stop()
		defer removeClient(client)

		go readLoop(client)

		for {
			<-ticker.C
			if err := writeControl(client, websocket.PingMessage); err != nil {
				return
			}
		}
	}()
}

func readLoop(client *wsClient) {
	for {
		if _, _, err := client.conn.ReadMessage(); err != nil {
			return
		}
	}
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

		payload, eventID, err := buildTradeMessage(m.Value)
		if err != nil {
			log.Println("Invalid trade payload:", err)
			continue
		}

		if err := processEvent("websocket-service-trades", eventID, payload); err != nil {
			log.Println("Trade event processing failed:", err)
			telemetry.Error("trade_event_processing_failed", map[string]interface{}{"trade_id": eventID})
			continue
		}

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

		payload, eventID, err := buildOrderEventMessage(m.Value)
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

		if err := processEvent("websocket-service-order-events", eventID, payload); err != nil {
			log.Println("Order event processing failed:", err)
			telemetry.Error("order_event_processing_failed", map[string]interface{}{"event_id": eventID})
			continue
		}

		if err := r.CommitMessages(ctx, m); err != nil {
			log.Println("Order event commit failed:", err)
			continue
		}
	}
}

func broadcast(message []byte) {
	// Snapshot the client list under a short lock.
	// Writing to WebSocket connections happens outside the lock so a slow
	// or stuck client cannot block delivery to every other client.
	clientsMu.Lock()
	snapshot := make([]*wsClient, 0, len(clients))
	for client := range clients {
		snapshot = append(snapshot, client)
	}
	clientsMu.Unlock()

	var failed []*wsClient
	for _, client := range snapshot {
		client.mu.Lock()
		_ = client.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
		err := client.conn.WriteMessage(websocket.TextMessage, message)
		client.mu.Unlock()
		if err != nil {
			_ = client.conn.Close()
			failed = append(failed, client)
		}
	}

	if len(failed) > 0 {
		clientsMu.Lock()
		for _, client := range failed {
			delete(clients, client)
		}
		websocketConnectionsGauge.Set(int64(len(clients)))
		observability.WebsocketClientsActive.Set(float64(len(clients)))
		clientsMu.Unlock()
	}

	messagesBroadcastTotal.Inc()
}

func buildTradeMessage(raw []byte) ([]byte, string, error) {
	var trade tradeMessage
	if err := json.Unmarshal(raw, &trade); err != nil {
		return nil, "", err
	}

	payload, err := json.Marshal(outboundMessage{
		Type:   "TRADE",
		Symbol: trade.Symbol,
		Data:   json.RawMessage(raw),
	})
	return payload, trade.ID, err
}

func buildOrderEventMessage(raw []byte) ([]byte, string, error) {
	var event orderEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return nil, "", err
	}

	messageType := ""
	switch event.Type {
	case "ORDER_UPDATE":
		messageType = "ORDER_UPDATE"
	case "ORDER_CANCELLED":
		messageType = "CANCEL"
	default:
		return nil, event.ID, nil
	}

	payload, err := json.Marshal(outboundMessage{
		Type:   messageType,
		Symbol: event.Symbol,
		Data:   json.RawMessage(raw),
	})
	return payload, event.ID, err
}

func getKafkaBroker() string {
	broker := os.Getenv("KAFKA_BROKER")
	if broker == "" {
		return "kafka:9092"
	}
	return broker
}

func removeClient(client *wsClient) {
	clientsMu.Lock()
	delete(clients, client)
	websocketConnectionsGauge.Set(int64(len(clients)))
	observability.WebsocketClientsActive.Set(float64(len(clients)))
	clientsMu.Unlock()
	_ = client.conn.Close()
}

func writeControl(client *wsClient, messageType int) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	_ = client.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return client.conn.WriteMessage(messageType, nil)
}

func processEvent(consumerGroup, eventID string, payload []byte) error {
	id, err := uuid.Parse(eventID)
	if err != nil {
		return err
	}

	tx, err := database.DB.Begin()
	if err != nil {
		return err
	}

	processed, err := database.IsEventProcessed(tx, consumerGroup, id)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	if processed {
		return tx.Commit()
	}

	broadcast(payload)

	if err := database.MarkEventProcessed(tx, consumerGroup, id); err != nil {
		_ = tx.Rollback()
		if errors.Is(err, database.ErrEventAlreadyProcessed) {
			// Another goroutine or consumer already handled this event.
			// This is not an error — skip silently.
			return nil
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	kafkaEventsProcessedTotal.Inc()
	telemetry.Info("websocket_event_processed", map[string]interface{}{"event_id": eventID})
	return nil
}
