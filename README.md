# TradeSphere — Production-Grade Event-Driven Trading Platform

TradeSphere is a high-performance, distributed, cloud-native trading platform engineered with Go, PostgreSQL, Apache Kafka, and Kubernetes (GKE). It implements event-driven microservices architecture, transactional outbox pattern, deterministic order book matching, idempotency tracking, and full observability.

---

## 🏛️ System Architecture Overview

```
                          ┌──────────────────────────┐
                          │   Client / Browser / UI  │
                          └─────────────┬────────────┘
                                        │
                         HTTP / REST    │    WebSocket (/ws)
                       ┌────────────────┴────────────────┐
                       ▼                                 ▼
           ┌───────────────────────┐         ┌───────────────────────┐
           │      API Gateway      │         │   WebSocket Service   │
           │  (Port 8000: Rate /   │         │ (Port 8083: Realtime  │
           │    JWT / Proxy)       │         │      Broadcast)       │
           └───────────┬───────────┘         └───────────▲───────────┘
                       │                                 │ Kafka Cons.
       ┌───────────────┼─────────────────┐               │
       ▼               ▼                 ▼               │
┌──────────────┐┌──────────────┐┌──────────────────┐     │
│ User Service ││ Order Service││Portfolio Service │     │
│ (Port 8084)  ││ (Port 8080)  ││   (Port 8081)    │     │
└──────┬───────┘└──────┬───────┘└────────┬─────────┘     │
       │               │                 │               │
       │ SQL           │ Outbox          │ Consumes      │
       ▼               ▼                 ▼               │
┌──────────────────────────────────────────────────┐     │
│                 PostgreSQL Database              │     │
│  (Users, Positions, Orders, Trades, Outboxes)    │     │
└──────────────────────────────────────────────────┘     │
                       │                                 │
                       │ Polled via Outbox Relay         │
                       ▼                                 │
┌──────────────────────────────────────────────────┐     │
│                   Apache Kafka                   │─────┘
│     Topics: orders (3p), trades (3p),            │
│     order-events (3p), DLQ topics                │
└──────────────────────┬───────────────────────────┘
                       │
                       │ Consumes Orders (Symbol Partitioned)
                       ▼
            ┌──────────────────────┐
            │   Matching Engine    │
            │ (Port 8082: Orderbook│
            │    + Trade Outbox)   │
            └──────────────────────┘
```

---

## 🚀 Microservices Breakdown

| Service | Port | Primary Responsibility | Data Store / Event Hub |
|---|---|---|---|
| **API Gateway** | 8000 | Reverse proxy, JWT validation, rate limiting | Stateless |
| **User Service** | 8084 | User registration, authentication, JWT tokens | PostgreSQL (`users`, `refresh_tokens`) |
| **Order Service** | 8080 | Pre-flight validation, balance reservation, Transactional Outbox | PostgreSQL (`orders`, `order_outbox`) + Kafka |
| **Portfolio Service** | 8081 | Balances, positions, trade reconciliation | PostgreSQL (`positions`, `users`) + Kafka |
| **Matching Engine** | 8082 | Price-time priority order matching, Trade Outbox | In-Memory Order Book + PostgreSQL (`trade_outbox`) + Kafka |
| **WebSocket Service** | 8083 | Real-time market data & order update streaming | Kafka Consumer + In-Memory Clients |

---

## 📦 Key Architectural Patterns

### 1. Transactional Outbox Pattern
To prevent dual-write anomalies between PostgreSQL and Kafka, `order-service` and `matching-engine` execute database transactions and insert event payloads into `order_outbox` / `trade_outbox` in the *same* database transaction. Background relay goroutines poll publishable outbox rows, emit them to Kafka, and mark them as `published_at = NOW()`.

### 2. Idempotency & Deduplication
Kafka consumers (`portfolio-service`, `websocket-service`) track processed events in the `processed_events` table using composite key `(consumer_group, event_id)`. Replayed or duplicate Kafka messages are safely ignored.

### 3. Balance & Position Reservation System
Orders are validated via synchronous HTTP pre-flight checks against `portfolio-service` before being accepted into `NEW` state, ensuring zero negative balance/position anomalies.

---

## ☸️ Kubernetes & Infrastructure Assets

The repository supports two complete deployment strategies:

### 1. Helm Chart (`infra/helm/tradesphere/`)
- Complete values parameterization (`values.yaml`).
- Pre-configured HPA (HorizontalPodAutoscaler) for stateless services.
- PodDisruptionBudgets (PDB) for zero-downtime maintenance.
- Post-install/upgrade hooks for Kafka topic creation (`kafka-init`) and database schema setup (`db-migrate`).

### 2. Kustomize / Raw Manifests (`k8s/`)
```
k8s/
├── namespace.yaml           # Dedicated namespace
├── configmap.yaml           # Non-sensitive app config
├── secret.yaml              # Secret templates
├── rbac.yaml                # Prometheus discovery RBAC
├── network-policy.yaml      # Least-privilege network isolation
├── infra.yaml               # Zookeeper, Kafka, Postgres, Kafka-init, DB-migrate Jobs
├── api-gateway.yaml         # Gateway deployment, service, HPA, PDB
├── services.yaml            # User, Order, Portfolio, Matching, WebSocket deployments
├── frontend.yaml            # Frontend deployment, service, HPA, PDB
├── ingress.yaml             # Ingress rules (/ws, /api, /)
├── monitoring/              # Prometheus & Grafana manifests
└── kustomization.yaml       # Kustomize entrypoint
```

---

## 📊 Observability & Monitoring

- **Prometheus Metrics**: Every Go service exposes `/metrics` powered by `prometheus/client_golang` and custom business counters (`tradesphere_orders_total`, `tradesphere_trades_total`, `tradesphere_reservation_failures_total`, `tradesphere_db_query_duration_seconds`).
- **Grafana Dashboards**: Custom dashboards located in `grafana/dashboards/`:
  - `trading-dashboard.json`: Order/trade throughput, WS client count, HTTP latency.
  - `kafka-dashboard.json`: Consumer group lag, topic partition offsets, DLQ counts.
  - `infrastructure-dashboard.json`: Pod CPU/memory, pod restart counts.
  - `database-dashboard.json`: DB query latency percentiles, Outbox publish duration.

---

## 🔄 CI/CD Pipelines

- **CI (`.github/workflows/ci.yml`)**: Lints Go modules (`golangci-lint`) and Frontend (`eslint`), runs Go unit/integration tests with race detector (`-race`), builds Docker images from repository root, runs Trivy vulnerability scanning, and validates Helm templates (`helm lint`, `kubeval`).
- **CD (`.github/workflows/cd.yml`)**: Triggered on push to `main`. Authenticates to GCP via Workload Identity, builds and pushes multi-stage Docker images to GCP Artifact Registry, runs atomic Helm upgrades (`helm upgrade --atomic --wait`), verifies rollout status, and automatically rolls back if health checks fail.

---

## 🛠️ Quickstart (Local Docker Compose)

```bash
# 1. Start all infrastructure and microservices
docker compose up -d --build

# 2. Check service health
docker compose ps

# 3. View logs
docker compose logs -f api-gateway
```
