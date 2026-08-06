# TradeSphere Repository Handbook

## 1. Executive summary

TradeSphere is a full-stack trading platform prototype that implements a simplified exchange workflow end to end. The repository is not just a UI app or a single backend service; it is a polyglot distributed system composed of:

- a Next.js frontend for user interaction,
- a Go-based API gateway,
- several Go microservices for auth, orders, portfolio management, matching, and websocket delivery,
- PostgreSQL for state persistence,
- Kafka for asynchronous event propagation,
- Docker Compose for local development,
- Helm charts for Kubernetes deployment,
- GitHub Actions for CI.

The main architectural idea is to separate concerns into independent services while preserving financial consistency through transactional updates and an outbox-based event flow.

The repository is deliberately built as a systems-design exercise. It uses a simplified but credible trading-domain architecture rather than a purely CRUD app. The core business cycle is:

1. The user submits an order through the frontend.
2. The order-service validates the request, checks risk with the portfolio-service, reserves funds/positions, and persists the order.
3. The order-service writes an outbox event into PostgreSQL.
4. A publisher relays the event to Kafka.
5. The matching-engine consumes order commands, matches them against the in-memory order book, persists trades and order updates, and emits outbox events.
6. The portfolio-service consumes trade and order events and updates balances and positions.
7. The websocket-service consumes those events and pushes real-time updates to clients.

This is a classic event-driven architecture with local transactional consistency and eventual consistency across services.

---

## 2. Architecture overview before services

### 2.1 System topology

The repository is organized around a distributed event-driven core:

- Order ingress: order-service + API gateway
- Financial state: portfolio-service + PostgreSQL
- Matching engine: matching-engine + in-memory order books
- Real-time delivery: websocket-service + Kafka
- Identity and auth: user-service + JWT
- Frontend: Next.js UI

The communication pattern is a mix of:

- synchronous HTTP between services for request/response flows,
- asynchronous Kafka topics for domain event propagation,
- PostgreSQL as the source of truth and durable transaction log,
- outbox tables to avoid losing events when a service writes to the database and then publishes to Kafka.

### 2.2 Why this architecture exists

The repository is designed to demonstrate a trading platform that must maintain correctness under concurrent activity. A naive single-service implementation would be simpler but would make it harder to show:

- independent scaling of matching and order intake,
- asynchronous propagation of trade events,
- real-time updates to clients,
- financial reconciliation and defensive state checks,
- resilience to downstream failures via outbox and retry logic.

The architecture therefore deliberately prioritizes correctness and event-driven decoupling over simplicity.

### 2.3 Design decisions that matter

1. Financial values are stored as integers with 8 decimal places.
   - The shared money package implements fixed-point scaling.
   - This avoids floating-point issues in a trading system.
   - See [pkg/money/money.go](pkg/money/money.go).

2. Reservation-based accounting is used before matching.
   - The portfolio-service reserves balances and positions before order execution.
   - This prevents double-spending and ensures an order cannot consume funds/positions that are already committed elsewhere.
   - See [services/portfolio-service/main.go](services/portfolio-service/main.go) and [services/order-service/main.go](services/order-service/main.go).

3. The system uses PostgreSQL outbox tables for reliability.
   - Order creation writes an outbox row in the order-service database.
   - Matching writes trade and order-event outbox rows in the matching-engine database.
   - A publisher worker claims rows and publishes them to Kafka with retry/backoff.
   - See [services/order-service/database/outbox.go](services/order-service/database/outbox.go) and [services/matching-engine/kafka/producer.go](services/matching-engine/kafka/producer.go).

4. The matching engine is stateful in memory.
   - It uses in-memory heaps to maintain buy/sell order books per symbol.
   - Orders are restored on startup from the database for recovery.
   - This makes matching fast and simple, but it means state is not persisted in a distributed way; recovery is only as good as the database snapshot plus replay.
   - See [services/matching-engine/engine/engine.go](services/matching-engine/engine/engine.go) and [services/matching-engine/orderbook/orderbook.go](services/matching-engine/orderbook/orderbook.go).

5. The frontend is intentionally thin and mostly presentational.
   - It consumes the gateway and websocket service.
   - It does not directly talk to the internal services.
   - That is an architectural boundary choice that reflects a production-like API gateway setup.

### 2.4 What tradeoffs the architecture makes

- Simplicity vs realism: the system is a credible prototype, but not a production-grade exchange. It uses a single PostgreSQL instance and a single Kafka broker for local deployment rather than a distributed cluster.
- Strong consistency vs availability: the portfolio reservation and matching logic uses transactions and locking, which improves correctness but can create contention under load.
- Eventual consistency vs synchronous consistency: portfolio state updates happen by consuming Kafka events after the initial order write; this is decoupled but introduces a temporary state lag window.
- In-memory matching vs durable matching: the engine is fast but not horizontally scalable without more careful partitioning.

---

## 3. Top-level directory breakdown

### [.] root

Why it exists:
- This is the workspace root and the integration boundary for local development and container orchestration.

Important files:
- [docker-compose.yml](docker-compose.yml): local orchestration for Postgres, Kafka, and all services.
- [go.mod](go.mod): shared Go module root for workspace usage.
- [go.work](go.work): multi-module Go workspace declaration.
- [test_hash.go](test_hash.go): small repository-level test artifact.
- [workspace_test.go](workspace_test.go): workspace-level test artifact.

How it interacts with others:
- The root Compose file wires together all directories under [services](services) and [infra](infra).
- The Go workspace ensures the local development environment can import packages from [pkg](pkg) and the service modules.

Design decisions and tradeoffs:
- The root acts as an orchestrator rather than containing business logic.
- The choice to use a single workspace root and go work is practical for local development rather than packaging.

---

## 4. Infrastructure and deployment

### [infra](infra)

Why it exists:
- This folder contains deployment artifacts for databases, messaging, and Kubernetes manifests.

Responsibilities:
- Provision PostgreSQL schema and migration files.
- Define Docker Compose environment for local simulation.
- Describe a Helm-based production-like deployment topology.

Important files:
- [infra/postgres/init.sql](infra/postgres/init.sql): initial schema, seed data, and financial constraints.
- [infra/postgres/migrations](infra/postgres/migrations): incremental DB schema evolution scripts.
- [infra/helm/tradesphere](infra/helm/tradesphere): Kubernetes deployment templates.

Interactions:
- The Compose file consumes the SQL init script for local DB startup.
- The Helm chart consumes the same schema ideas, but wraps them as ConfigMaps and StatefulSets.

Design decisions:
- Postgres is used for durable state, not just for ephemeral storage.
- The deployment includes explicit stateful services for Postgres and Kafka, reflecting the need for persistence and ordered event delivery.
- The Helm chart is moderately production-oriented with probes, PDBs, anti-affinity, and autoscaling.

Alternatives and tradeoffs:
- A simpler deployment could have used SQLite or a single process to reduce operational complexity, but that would undermine the distributed-systems story being demonstrated.

### [infra/postgres](infra/postgres)

Why it exists:
- This is the database bootstrap layer.

Responsibilities:
- Define the core relational schema.
- Seed demo users and initial positions.
- Support migration-based evolution.

Important files:
- [infra/postgres/init.sql](infra/postgres/init.sql): the main schema and seed data.
- [infra/postgres/migrations](infra/postgres/migrations): versioned schema changes.

Important implementation details:
- The schema uses UUID primary keys and checks on non-negative balances and quantities.
- The orders table stores price, quantity, remaining quantity, reserved amount, and status.
- The trades table records completed matches.
- The order_outbox and trade_outbox tables are central to reliable event publication.
- The processed_events table implements idempotency for event consumers.

Design decisions:
- The schema uses explicit monetary columns in integer form, plus checks, rather than floating-point.
- Outbox tables are part of the core database schema, showing that event publication is treated as a first-class reliability feature.

Tradeoffs:
- The schema is straightforward but not fully normalized for a large exchange. For example, it stores state in users, positions, orders, and trades directly rather than using a more elaborate ledger model.

### [infra/helm/tradesphere](infra/helm/tradesphere)

Why it exists:
- This provides a deployable Kubernetes representation of the system.

Responsibilities:
- Deploy the application services and their dependencies.
- Inject config and secrets.
- Provide autoscaling, ingress, and observability resources.

Important files:
- [infra/helm/tradesphere/Chart.yaml](infra/helm/tradesphere/Chart.yaml)
- [infra/helm/tradesphere/values.yaml](infra/helm/tradesphere/values.yaml)
- [infra/helm/tradesphere/templates](infra/helm/tradesphere/templates)

Design decisions:
- Stateful components like Postgres, Kafka, and Zookeeper are represented as StatefulSets.
- The matching-engine deployment is annotated with a scaling strategy that explicitly acknowledges Kafka partition constraints.
- Ingress routes /ws to the websocket-service, /api to the gateway, and / to the frontend.
- Prometheus, Grafana, Loki, Jaeger, and Promtail are included for observability.

Tradeoffs:
- The chart is broad but not fully production-hardening. It uses simple defaults and not all service dependencies are managed through init jobs or advanced operators.

---

## 5. Shared libraries and reusable infrastructure

### [pkg/auth](pkg/auth)

Why it exists:
- This package centralizes authentication and request security behavior for services and gateway middleware.

Responsibilities:
- Issue and validate JWT access tokens.
- Attach user identity into request context.
- Enforce authorization roles.
- Apply rate limiting.

Important files:
- [pkg/auth/jwt.go](pkg/auth/jwt.go)
- [pkg/auth/middleware.go](pkg/auth/middleware.go)

Important implementation details:
- JWTs are HMAC-signed with HS256.
- The token includes user_id, role, and standard registered claims.
- The middleware validates the token and stores the user ID in the request context under the UserIDKey constant.

Design decisions:
- The package is intentionally simple and local; there is no external identity provider.
- The rate limiter is in-memory and per-IP, which is appropriate for a demo but not for multi-instance production.

Tradeoffs:
- A shared JWT package is good for consistency, but a production system would likely use a dedicated auth service and a distributed session store.

### [pkg/money](pkg/money)

Why it exists:
- This package implements fixed-precision arithmetic for price and quantity values.

Responsibilities:
- Parse decimal strings into scaled integers.
- Format scaled integers back to decimal strings.
- Perform exact multiplication for price × quantity.

Important files:
- [pkg/money/money.go](pkg/money/money.go)
- [pkg/money/money_test.go](pkg/money/money_test.go)

Design decisions:
- The package uses $10^8$ scale, which is common in financial systems and explicitly documented.
- It rejects values that cannot be represented exactly at that scale.

Tradeoffs:
- Fixed precision is correct and deterministic, but it adds implementation complexity versus using decimal types.

### [pkg/observability](pkg/observability)

Why it exists:
- This package provides shared metrics and tracing setup for all services.

Responsibilities:
- Register Prometheus counters, gauges, and histograms.
- Initialize OpenTelemetry tracing with OTLP exporter.
- Provide HTTP middleware for metrics and tracing.

Important files:
- [pkg/observability/metrics.go](pkg/observability/metrics.go)

Design decisions:
- The package exposes a simple Init(serviceName) bootstrap that wires metrics and tracing into each service.
- The system uses the OpenTelemetry HTTP exporter to Jaeger by default.

Tradeoffs:
- The implementation is straightforward but not fully complete for all production-scale needs; it uses global prometheus registries and a basic tracing setup.

---

## 6. Services

### 6.1 [services/api-gateway](services/api-gateway)

Why it exists:
- The gateway is the public ingress for client traffic. It consolidates routing and cross-cutting concerns before requests hit the internal services.

Responsibilities:
- Expose a single external entry point on port 8000.
- Proxy requests to the user-service, order-service, portfolio-service, and websocket-service.
- Apply CORS, request IDs, logging, and rate limiting.
- Enforce auth for protected routes.

Important files:
- [services/api-gateway/main.go](services/api-gateway/main.go)

Important implementation details:
- The gateway mounts /api/auth/ to the user-service.
- It protects /api/orders and /api/portfolio/ routes with auth middleware.
- It also routes /api/trades/ to the portfolio service, although this is a somewhat odd path choice because trades are not actually served by the portfolio-service.

Design decisions:
- A reverse proxy is used rather than an API framework like Gin or Echo, which keeps dependencies minimal and demonstrates the architecture in a lightweight way.
- The gateway implements middleware composition, which is a good separation of concerns.

Alternatives and tradeoffs:
- A true API gateway framework would provide more advanced routing, auth, and observability features; this implementation is intentionally minimal.

### 6.2 [services/user-service](services/user-service)

Why it exists:
- This service owns identity, authentication, and session tokens.

Responsibilities:
- Create users.
- Authenticate users via email/password.
- Issue access tokens and refresh tokens.
- Return the authenticated user profile.

Important files:
- [services/user-service/main.go](services/user-service/main.go)
- [services/user-service/database/db.go](services/user-service/database/db.go)

Important implementation details:
- Signup uses bcrypt for password hashing.
- Login issues an access token and a refresh token.
- Refresh token rotation is implemented by revoking the old token and issuing a new one.
- The service stores refresh token hashes in the database to avoid persisting raw tokens.

Design decisions:
- The service is built as a plain HTTP server with handler functions rather than a web framework.
- The auth flow is simple and local, which fits the prototype scope.

Tradeoffs:
- The password and token management are reasonable but not sufficient for a production-grade identity platform.

### 6.3 [services/order-service](services/order-service)

Why it exists:
- This service is the order intake and command entry point for users.

Responsibilities:
- Accept order creation requests.
- Validate orders.
- Check whether the user can place the order by consulting the portfolio-service.
- Reserve balance or position state via the portfolio-service.
- Persist orders and write outbox events.
- Handle cancellation requests by enqueuing cancel commands.

Important files:
- [services/order-service/main.go](services/order-service/main.go)
- [services/order-service/database/db.go](services/order-service/database/db.go)
- [services/order-service/database/outbox.go](services/order-service/database/outbox.go)
- [services/order-service/kafka/producer.go](services/order-service/kafka/producer.go)

Important implementation details:
- Only LIMIT orders are supported; the handler rejects other order types.
- The order-service calls the portfolio-service twice:
  - first with /validate for pre-check,
  - then with /reserve to lock the balance/position state.
- It uses the database transaction to insert the order and enqueue the outbox event atomically.
- Cancel requests are not executed immediately; they are written as commands into the order outbox to be consumed by the matching-engine.

Design decisions:
- The service uses a synchronous reservation flow to protect against invalid orders before they are accepted.
- Outbox-based event publication ensures the order submission is durable even if Kafka is temporarily unavailable.

Tradeoffs:
- The service depends heavily on the portfolio-service for risk checks, which makes it more coupled than it might be in a larger system.
- The reservation flow is a simple synchronous check rather than a more sophisticated risk engine.

### 6.4 [services/portfolio-service](services/portfolio-service)

Why it exists:
- This service owns account and position state, including balances, reserved balances, and positions.

Responsibilities:
- Serve portfolio read APIs.
- Validate whether a proposed order is affordable or position-eligible.
- Reserve and release balance/position amounts.
- Apply trades from Kafka to update balances and positions.
- Apply order-cancel events to release reservations.
- Run reconciliation checks to diagnose drift.

Important files:
- [services/portfolio-service/main.go](services/portfolio-service/main.go)
- [services/portfolio-service/reconcile.go](services/portfolio-service/reconcile.go)
- [services/portfolio-service/database/db.go](services/portfolio-service/database/db.go)
- [services/portfolio-service/kafka/consumer.go](services/portfolio-service/kafka/consumer.go)

Important implementation details:
- The reserve and release handlers use SQL transactions and row-level locking with FOR UPDATE.
- The ApplyBuyerTrade and ApplySellerTrade functions use careful account and position updates to preserve the invariant that reserved amounts and balances stay consistent.
- The reconciliation worker checks for negative balances, over-reserved positions, and orphaned trades.

Design decisions:
- The service uses the database as the authoritative state store for portfolio state.
- The portfolio-service consumes Kafka events rather than being called directly by the matching-engine, which decouples the two and introduces replayability.

Tradeoffs:
- The logic is somewhat manual and domain-specific, and it is not a full ledger system. It is sufficient for a prototype but less robust than a true ledger or account processing platform.

### 6.5 [services/matching-engine](services/matching-engine)

Why it exists:
- This service is the exchange engine that turns resting orders into trades.

Responsibilities:
- Consume order commands from Kafka.
- Build and maintain per-symbol in-memory order books.
- Match incoming orders against resting orders.
- Persist trade results and updated order states.
- Emit trade and order-event outbox rows.
- Restore open orders from the database on startup.

Important files:
- [services/matching-engine/main.go](services/matching-engine/main.go)
- [services/matching-engine/orderbook/orderbook.go](services/matching-engine/orderbook/orderbook.go)
- [services/matching-engine/engine/engine.go](services/matching-engine/engine/engine.go)
- [services/matching-engine/kafka/consumer.go](services/matching-engine/kafka/consumer.go)
- [services/matching-engine/kafka/producer.go](services/matching-engine/kafka/producer.go)
- [services/matching-engine/database/db.go](services/matching-engine/database/db.go)

Important implementation details:
- The engine uses max-heaps and min-heaps to hold buy and sell orders by price/time priority.
- The matching logic is deterministic and straightforward.
- The engine’s RestoreOrders function rebuilds the in-memory state from database rows.
- The service processes cancellations by updating order state in memory and in the DB.

Design decisions:
- The matching engine is a single process with in-memory order books, which is fine for a prototype but limited for scale.
- The use of outbox tables and event idempotency is intentional and strong for a distributed system.

Tradeoffs:
- The engine replays from the database on restart, but it does not have a more advanced distributed consensus or state replication mechanism.

### 6.6 [services/websocket-service](services/websocket-service)

Why it exists:
- This service streams live events to clients over WebSocket.

Responsibilities:
- Accept WebSocket connections.
- Consume trade and order-event Kafka topics.
- Broadcast updates to all connected clients.
- Track connection counts and process events idempotently.

Important files:
- [services/websocket-service/main.go](services/websocket-service/main.go)

Important implementation details:
- The service uses a central clients map and a mutex to manage connected clients.
- It uses processed_events in the database to avoid duplicate event delivery.
- It maps trade events to a TRADE message type and order events to ORDER_UPDATE or CANCEL messages.

Design decisions:
- Websocket delivery is event-driven rather than polling-based, which matches the trading UX requirement.
- The service maintains a short heartbeat and ping/pong mechanism to keep connections alive.

Tradeoffs:
- It holds a simple in-memory connection list; this is acceptable for a prototype but not for large-scale multi-instance deployments without a pub/sub or shared state layer.

---

## 7. Microservice deep dive: each service, line by line

The earlier sections described the system at a high level. This section is more surgical. It walks through each microservice as it exists in the code, using the actual handlers, packages, structs, goroutines, database calls, Kafka consumers/producers, and middleware that are implemented in the repository.

### 7.1 API gateway

Purpose
- The gateway is the public ingress point for the browser and client apps.
- It is responsible for routing requests to the correct internal service and applying cross-cutting concerns such as CORS, request IDs, logging, rate limiting, and auth enforcement.

Folder structure
- [services/api-gateway](services/api-gateway)
- Files:
  - [services/api-gateway/main.go](services/api-gateway/main.go)
  - [services/api-gateway/Dockerfile](services/api-gateway/Dockerfile)
  - [services/api-gateway/go.mod](services/api-gateway/go.mod)

Main files
- [services/api-gateway/main.go](services/api-gateway/main.go)

Entry point
- The main function in [services/api-gateway/main.go](services/api-gateway/main.go) starts an HTTP server on port 8000.
- It initializes observability and wires the mux with middleware.

Main packages
- [pkg/auth](pkg/auth) for JWT validation and rate limiting
- [pkg/observability](pkg/observability) for metrics and tracing
- Standard library net/http, net/http/httputil, net/url

Important structs
- contextKey and the requestIDKey constant
- responseWriter, which captures the status code from the downstream handler

Methods and functions
- main()
- proxy(target, stripPrefix)
- corsMiddleware(next)
- requestIDMiddleware(next)
- loggingMiddleware(next)
- rateLimitMiddleware(next)

Request lifecycle
1. The browser sends a request to /api/...
2. The gateway applies CORS, rate limiting, request ID injection, logging, and observability middleware.
3. The router forwards the request to the target service through an httputil.ReverseProxy.
4. The request path is rewritten by stripping the /api prefix.
5. Auth context is passed along using headers and the shared auth middleware.

Business logic
- There is no domain business logic here; this service is a routing and policy layer.
- It decides which internal service owns the request and who is allowed to call it.
- The protected routes are /api/orders and /api/portfolio and /api/trades; /api/auth is left publicly routable.

Error handling
- Reverse-proxy errors are converted to a 502 Bad Gateway.
- The gateway does not perform deep business validation; it simply returns an upstream error if the target service fails.

Logging
- Logging middleware captures the request ID, method, path, status code, and duration.
- The gateway also logs reverse-proxy failures.

Configuration
- It uses the default port 8000.
- It assumes the internal service addresses are fixed DNS names: user-service, order-service, portfolio-service.
- JWT secret is expected from the environment via the shared auth package.

Dependencies
- Shared auth package for token validation and rate limiting
- Shared observability package
- Internal HTTP services

External services
- None directly, but it depends on the downstream services and the JWT secret.

Database usage
- None.

Kafka usage
- None.

Redis usage
- None.

Performance
- It uses a reverse proxy and lightweight middleware, so its overhead is low.
- It sets timeouts of 10 seconds for reads and writes.

Scalability
- It is easy to scale horizontally because it is mostly stateless.
- The Helm values explicitly set replicas 2 for this service.

Security
- It enforces JWT auth on protected routes through auth.RequireAuth("user").
- It sets permissive CORS headers for browser access. That is convenient but not production-hardening.

Thread safety
- The gateway uses no shared mutable business state; its middleware is stateless aside from the in-memory rate limiter in the shared auth package.
- The rate limiter uses a mutex to protect its client map.

Concurrency
- The server handles requests concurrently via the net/http server.
- Each request gets its own context and middleware chain.

Possible bottlenecks
- The gateway is a single choke point for all browser traffic.
- The in-memory rate limiter is not distributed across instances.
- The reverse-proxy path is synchronous and will block on downstream latency.

Possible improvements
- Add service discovery instead of hard-coded hostnames.
- Add request tracing to downstream services consistently.
- Introduce a more robust auth gateway with per-route RBAC and stripping logic.
- Put the gateway behind an actual load balancer and ingress controller.

Common interview questions
- Why is the gateway needed if the frontend can call services directly?
- What is the difference between routing and business logic in a gateway?
- Why are request IDs useful in distributed systems?

Senior interview questions
- How would you make this gateway resilient under cascading failures?
- How would you evolve this into a true API management layer with circuit breakers and traffic policies?

### 7.2 User service

Purpose
- The user-service owns identity, authentication, and refresh-session state.
- It handles signup, login, refresh, logout, and the /me profile endpoint.

Folder structure
- [services/user-service](services/user-service)
- Files:
  - [services/user-service/main.go](services/user-service/main.go)
  - [services/user-service/database/db.go](services/user-service/database/db.go)
  - [services/user-service/model](services/user-service/model)
  - [services/user-service/Dockerfile](services/user-service/Dockerfile)

Main files
- [services/user-service/main.go](services/user-service/main.go)
- [services/user-service/database/db.go](services/user-service/database/db.go)
- [services/user-service/model/user.go](services/user-service/model/user.go)

Entry point
- [services/user-service/main.go](services/user-service/main.go) starts an HTTP server on port 8084.

Main packages
- [pkg/auth](pkg/auth) for JWT generation and middleware
- [pkg/observability](pkg/observability) for metrics and tracing
- [services/user-service/database](services/user-service/database) for repository-like DB access
- [services/user-service/model](services/user-service/model) for DTOs

Important structs
- SignupRequest
- LoginRequest
- TokenResponse
- RefreshRequest
- User
- RefreshToken

Methods and functions
- main()
- signupHandler(w, r)
- loginHandler(w, r)
- refreshHandler(w, r)
- logoutHandler(w, r)
- meHandler(w, r)
- database.CreateUser
- database.GetUserByEmail
- database.GetUserByID
- database.StoreRefreshToken
- database.GetRefreshToken
- database.RevokeRefreshToken

Request lifecycle
1. The client calls /signup, /login, /refresh, /logout, or /me.
2. The handler parses JSON from the request body.
3. The service validates basic requirements such as email and password length.
4. It interacts with PostgreSQL to create or look up a user or refresh token.
5. It returns a JSON response or a JWT token.

Business logic
- Signup uses bcrypt.GenerateFromPassword with bcrypt.DefaultCost and persists the user in the users table.
- Login verifies a password against the stored hash and issues a JWT access token plus a random refresh token.
- Refresh token rotation is implemented by revoking the old token hash and replacing it with a new one.
- Logout simply revokes the refresh token hash.

Error handling
- Bad input returns 400.
- Invalid credentials return 401.
- Conflict on duplicate email returns 409.
- DB failures return 500.

Logging
- The service uses standard log.Printf in startup and error paths; it does not have a heavy logging abstraction.

Configuration
- DB host from DB_HOST, defaulting to localhost in the service’s DB init file.
- JWT secret from JWT_SECRET, defaulting to a local secret in the shared auth package.

Dependencies
- PostgreSQL
- Shared auth and observability packages
- bcrypt
- uuid

External services
- None directly beyond PostgreSQL.

Database usage
- Insert into users on signup
- Query users by email or UUID
- Insert and query refresh_tokens
- Update revoked_at on refresh token revocation

Kafka usage
- None.

Redis usage
- None.

Performance
- Very lightweight; mostly CRUD and hashing.
- Password hashing is the heaviest step in signup.

Scalability
- Stateless and easy to scale horizontally.
- It is not limited by in-memory state or a single process queue.

Security
- Passwords are hashed with bcrypt.
- JWTs are signed with HS256.
- The service uses the shared auth middleware to enforce roles.
- Refresh tokens are stored as hashes, not raw tokens.

Thread safety
- The service is mostly stateless and request-scoped.
- The database connection pool is shared by the process, which is safe for concurrent use.

Concurrency
- The HTTP server handles requests concurrently.
- The DB pool handles concurrent queries safely.

Possible bottlenecks
- Password hashing on signup is CPU-intensive.
- The service is a single point for token issuance and session validation.
- Refresh-token lookup is a DB hit.

Possible improvements
- Use a dedicated identity provider or OAuth/OIDC.
- Add token revocation support with a short-lived access-token strategy.
- Add rate limiting per account and IP.
- Move refresh token storage to a more robust session store if the system grows.

Common interview questions
- Why are refresh tokens rotated instead of simply reused?
- How do you prevent password hashing from becoming a bottleneck?
- Why store only hashes, not raw refresh tokens?

Senior interview questions
- How would you move this to a federated identity system without breaking the rest of the architecture?
- How would you design revocation and token introspection for a production scale system?

### 7.3 Order service

Purpose
- The order-service is the admission layer for trading commands.
- It accepts user orders, validates them, checks risk with the portfolio-service, reserves funds or positions, persists the order in PostgreSQL, and enqueues an outbox event so the matching engine can process it.

Folder structure
- [services/order-service](services/order-service)
- Files:
  - [services/order-service/main.go](services/order-service/main.go)
  - [services/order-service/database](services/order-service/database)
  - [services/order-service/kafka](services/order-service/kafka)
  - [services/order-service/model](services/order-service/model)
  - [services/order-service/money](services/order-service/money)
  - [services/order-service/telemetry](services/order-service/telemetry)

Main files
- [services/order-service/main.go](services/order-service/main.go)
- [services/order-service/database/outbox.go](services/order-service/database/outbox.go)
- [services/order-service/kafka/producer.go](services/order-service/kafka/producer.go)
- [services/order-service/model/order.go](services/order-service/model/order.go)

Entry point
- [services/order-service/main.go](services/order-service/main.go) initializes the database, starts the outbox publisher goroutine, registers handlers, and launches the HTTP server on port 8080.

Main packages
- [pkg/auth](pkg/auth) for JWT auth and rate limiting
- [pkg/observability](pkg/observability) for metrics and tracing
- [pkg/money](pkg/money) for fixed-point arithmetic
- [services/order-service/database](services/order-service/database)
- [services/order-service/kafka](services/order-service/kafka)
- [services/order-service/model](services/order-service/model)
- [services/order-service/telemetry](services/order-service/telemetry)

Important structs
- CreateOrderRequest
- reservationRequest
- portfolioErrorResponse
- model.Order
- model.OrderCommand
- model.CancelRequest

Methods and functions
- main()
- createOrderHandler(w, r)
- orderActionHandler(w, r)
- validateWithPortfolio(userID, symbol, side, price, qty)
- rollbackReservation(order)
- callPortfolioReservation(path, order)
- calculateReservedAmount(side, price, quantity)
- healthHandler(w, r)
- database.InsertOrderWithOutbox(order)
- database.EnqueueCancelCommand(cancel)
- database.GetOrder(orderID)
- kafka.PublishOrder(order)
- kafka.PublishCancel(cancel)

Request lifecycle
1. The client posts an order to /orders.
2. The handler validates the request body, JWT, and order shape.
3. It computes the reserve amount using money.CostFor for BUY orders and quantity for SELL orders.
4. It performs a risk check against the portfolio-service through /validate.
5. It calls /reserve on the portfolio-service.
6. It inserts the order and outbox event in one DB transaction.
7. The outbox publisher later publishes the order command to Kafka.
8. The client receives the created order JSON.

Business logic
- Only LIMIT orders are accepted. The handler rejects any other order type.
- BUY orders reserve the notional cost; SELL orders reserve the quantity.
- Orders are stored in the orders table with status NEW and remaining_quantity equal to the quantity.
- Cancellation is not executed immediately; it is converted into an order command written to the outbox tables.

Error handling
- Invalid JSON, invalid UUID, bad side, bad type, invalid quantity, or invalid price all return 400.
- If the portfolio-service returns a bad status or is unreachable, the service returns 500 or 400 depending on the failure mode.
- If the database insert fails after a reservation has been made, the service calls the portfolio-service release endpoint to undo the reservation.

Logging
- It logs successful order acceptance and telemetry events through the shared telemetry package.
- It also logs reservation rollback failures.

Configuration
- Port 8080.
- Kafka broker from KAFKA_BROKER, default kafka:9092.
- DB host from DB_HOST, default postgres from the service connection logic.
- JWT secret from JWT_SECRET.

Dependencies
- Portfolio-service over HTTP
- PostgreSQL
- Kafka
- Shared auth, observability, money, telemetry packages

External services
- Portfolio-service
- Kafka
- PostgreSQL

Database usage
- Insert order into orders
- Insert and claim outbox events in order_outbox
- Query the order by ID for cancel actions

Kafka usage
- The service uses Kafka only indirectly via its outbox publisher.
- It publishes order commands to the orders topic.
- The publishing function uses kafka-go.Writer and writes messages keyed by the symbol.

Redis usage
- None.

Performance
- The service does a synchronous HTTP call to the portfolio-service before persisting the order, so latency is partly governed by the portfolio-service response time.
- The outbox publisher uses a 500ms ticker and claims up to 100 events per cycle.

Scalability
- It is stateless and can be horizontally scaled.
- The outbox pattern makes it robust when multiple instances are running.

Security
- JWT auth is enforced on /orders and /orders/:id/cancel.
- The handler checks that the user_id in the request body matches the user ID in the JWT.
- The service avoids accepting orders for another user.

Thread safety
- The service does not maintain business state in memory; it uses request-scoped structs and shared DB connections.
- The outbox publisher uses a single goroutine and does not share mutable state beyond its writer objects.

Concurrency
- The HTTP server handles multiple requests concurrently.
- The outbox publisher runs as a separate goroutine.

Possible bottlenecks
- Synchronous calls to the portfolio-service can create latency and contention.
- The service uses a single Kafka writer object per process and a single outbox worker goroutine.
- The DB transaction and two upstream calls create a bottleneck under burst traffic.

Possible improvements
- Move the reservation and validation to a more dedicated risk engine or portfolio-coordination service.
- Use idempotency keys for duplicate order submissions.
- Add circuit breakers around the portfolio-service.
- Introduce partitioned Kafka keys and more advanced retries.

Common interview questions
- Why is the reservation made before the order is persisted?
- What is the difference between a validation call and a reservation call?
- Why use an outbox rather than publish immediately after the DB write?

Senior interview questions
- How would you make this service eventually consistent without losing correctness?
- How would you prevent duplicate order submission under retries and network partitions?

### 7.4 Portfolio service

Purpose
- The portfolio-service owns financial state: balances, reserved balances, positions, and reserved positions.
- It is the authority for whether a user can afford a buy order or hold a sell order.

Folder structure
- [services/portfolio-service](services/portfolio-service)
- Files:
  - [services/portfolio-service/main.go](services/portfolio-service/main.go)
  - [services/portfolio-service/database/db.go](services/portfolio-service/database/db.go)
  - [services/portfolio-service/kafka/consumer.go](services/portfolio-service/kafka/consumer.go)
  - [services/portfolio-service/reconcile.go](services/portfolio-service/reconcile.go)
  - [services/portfolio-service/model](services/portfolio-service/model)

Main files
- [services/portfolio-service/main.go](services/portfolio-service/main.go)
- [services/portfolio-service/database/db.go](services/portfolio-service/database/db.go)
- [services/portfolio-service/kafka/consumer.go](services/portfolio-service/kafka/consumer.go)
- [services/portfolio-service/reconcile.go](services/portfolio-service/reconcile.go)

Entry point
- [services/portfolio-service/main.go](services/portfolio-service/main.go) starts the HTTP server, starts Kafka consumers, and starts a reconciliation goroutine.

Main packages
- [pkg/auth](pkg/auth) for auth middleware
- [pkg/observability](pkg/observability) for metrics
- [pkg/money](pkg/money) for fixed-point math
- [services/portfolio-service/database](services/portfolio-service/database)
- [services/portfolio-service/kafka](services/portfolio-service/kafka)
- [services/portfolio-service/model](services/portfolio-service/model)
- [services/portfolio-service/telemetry](services/portfolio-service/telemetry)

Important structs
- Position
- OrderReservation
- OrderEvent in the Kafka consumer package
- reconciliationResult

Methods and functions
- main()
- startHTTPServer(ctx)
- healthHandler(w, r)
- portfolioHandler(w, r)
- getBalance(w, userID)
- getPositions(w, userID)
- getFullPortfolio(w, userID)
- validateHandler(w, r)
- reserveHandler(w, r)
- releaseHandler(w, r)
- startReconciliationWorker(ctx)
- runReconciliationOnce()
- collectReconciliationResults(db)
- buildReconciliationFindings(results)
- reconciliationInterval()
- kafka.StartTradeConsumer(ctx)
- kafka.StartOrderEventConsumer(ctx)
- kafka.processTrade(trade, consumerGroup)
- kafka.processOrderEvent(event, consumerGroup)
- database.LockAccount(tx, userID)
- database.ReleaseOrderReservation(tx, orderID)
- database.ApplyBuyerTrade(tx, trade)
- database.ApplySellerTrade(tx, trade)

Request lifecycle
1. A client calls /validate, /reserve, /release, or /portfolio/...
2. The handler parses parameters or JSON and validates basic input.
3. For reserve and release, the service opens a DB transaction and locks the relevant rows with FOR UPDATE.
4. It updates balances or positions and commits the transaction.
5. For reads, it queries the users and positions tables.
6. For event-driven updates, Kafka consumers apply trade and order events.

Business logic
- BUY validation checks that balance minus reserved_balance is at least the notional cost.
- SELL validation checks that position quantity minus reserved_quantity is at least the requested quantity.
- Reservation updates reserved_balance for BUY and reserved_quantity for SELL.
- Release undoes reservations and resets reserved amounts on the order row.
- Trade application updates both sides of the ledger and adjusts the reserved amounts to match the partially filled remainder.
- Order-cancel events release any outstanding reservation.

Error handling
- Invalid input returns 400.
- Missing user or position returns 400 or 404 depending on the handler.
- The service uses transactions and checks for row changes; if an update affects zero rows it returns an error.
- The reconciliation worker logs mismatches rather than crashing.

Logging
- It logs errors from validation, reservation, release, event processing, and reconciliation.
- It emits telemetry events such as trade_applied and order_event_applied.

Configuration
- Port 8081.
- DB host from DB_HOST, default postgres.
- Kafka group ID from KAFKA_GROUP_ID, default portfolio-service.
- Reconciliation interval from RECONCILIATION_INTERVAL, default 1m.

Dependencies
- PostgreSQL
- Kafka
- Shared auth and observability packages
- Shared money package

External services
- PostgreSQL
- Kafka

Database usage
- Reads from users and positions
- Writes to users and positions
- Uses transactions and row locking via FOR UPDATE
- Uses processed_events for idempotency

Kafka usage
- Consumes trades and order-events.
- Uses consumer groups so it can process events in a controlled way.
- Uses processed_events to avoid double application on redelivery.

Redis usage
- None.

Performance
- The service uses DB transactions and row locks, so it is sensitive to contention.
- It sets max open DB connections to 10, which is a practical tuning choice.
- The reconciliation worker runs periodically and can become expensive on large datasets.

Scalability
- It can scale horizontally because it is stateless except for the DB and Kafka consumers.
- However, locking and contention can become a bottleneck when many orders are processed.

Security
- The portfolio read handlers enforce user identity via auth middleware.
- A user cannot access another user’s portfolio data.
- The service assumes the caller is authenticated and trusted enough to operate through the order-service.

Thread safety
- The HTTP handlers are standard request handlers.
- The DB connection pool is concurrent-safe.
- The Kafka consumer loops are independent goroutines.
- The service does not use shared in-memory mutable state except the Prometheus metrics registry.

Concurrency
- The service has three independent goroutines at startup: trade consumer, order-event consumer, and reconciliation worker.
- Inside a transaction, it locks accounts or orders to prevent races.

Possible bottlenecks
- Row-level locking on users and orders under heavy matching volume.
- Reconciliation queries on large datasets can be expensive.
- A single DB can become a throughput bottleneck.

Possible improvements
- Use a dedicated ledger or account service rather than these direct updates.
- Partition accounts by user or symbol to reduce lock contention.
- Add dead-letter handling and replay tooling for Kafka.
- Use optimistic concurrency where possible.

Common interview questions
- Why does the portfolio-service use transactions and row locks?
- What is the purpose of processed_events?
- Why is reconciliation useful in a financial system?

Senior interview questions
- How would you evolve this into a true ledger with immutable accounting entries?
- How would you handle distributed transactions or saga-based compensation if the reservation and trade application fail at different points?

### 7.5 Matching engine

Purpose
- The matching-engine is the exchange core.
- It consumes order commands from Kafka, applies them to in-memory order books, matches resting orders, persists trade and order updates to PostgreSQL, and emits outbox events for downstream services.

Folder structure
- [services/matching-engine](services/matching-engine)
- Files:
  - [services/matching-engine/main.go](services/matching-engine/main.go)
  - [services/matching-engine/database/db.go](services/matching-engine/database/db.go)
  - [services/matching-engine/engine/engine.go](services/matching-engine/engine/engine.go)
  - [services/matching-engine/kafka/consumer.go](services/matching-engine/kafka/consumer.go)
  - [services/matching-engine/kafka/producer.go](services/matching-engine/kafka/producer.go)
  - [services/matching-engine/model](services/matching-engine/model)
  - [services/matching-engine/orderbook/orderbook.go](services/matching-engine/orderbook/orderbook.go)
  - [services/matching-engine/testproducer](services/matching-engine/testproducer)

Main files
- [services/matching-engine/main.go](services/matching-engine/main.go)
- [services/matching-engine/engine/engine.go](services/matching-engine/engine/engine.go)
- [services/matching-engine/orderbook/orderbook.go](services/matching-engine/orderbook/orderbook.go)
- [services/matching-engine/kafka/consumer.go](services/matching-engine/kafka/consumer.go)
- [services/matching-engine/kafka/producer.go](services/matching-engine/kafka/producer.go)
- [services/matching-engine/database/db.go](services/matching-engine/database/db.go)

Entry point
- [services/matching-engine/main.go](services/matching-engine/main.go) initializes the DB, rebuilds the in-memory engine from persisted open orders, starts Kafka consumers and outbox publisher goroutines, and serves HTTP endpoints.

Main packages
- [services/matching-engine/engine](services/matching-engine/engine)
- [services/matching-engine/orderbook](services/matching-engine/orderbook)
- [services/matching-engine/database](services/matching-engine/database)
- [services/matching-engine/kafka](services/matching-engine/kafka)
- [services/matching-engine/model](services/matching-engine/model)
- [pkg/observability](pkg/observability)
- [pkg/money](pkg/money)

Important structs
- MatchingEngine
- SymbolSnapshot
- OrderBook
- MaxHeap and MinHeap
- model.Order
- model.OrderEvent
- model.Trade
- model.OrderCommand

Methods and functions
- main()
- startHTTPServer(me)
- engine.NewMatchingEngine()
- engine.ProcessOrder(order)
- engine.RestoreOrders(orders)
- engine.GetOrderBookSnapshot(symbol)
- engine.SnapshotSymbol(symbol)
- engine.RestoreSymbol(snapshot)
- engine.CancelOrder(id)
- engine.RestoreOrder(id, remainingQty, status)
- orderbook.NewOrderBook()
- orderbook.ProcessOrder(order)
- orderbook.RestoreOrder(order)
- orderbook.matchBuyOrder(order)
- orderbook.matchSellOrder(order)
- orderbook.matchBuyMarketOrder(order)
- orderbook.matchSellMarketOrder(order)
- createTrade(restingOrder, incomingOrder, qty)
- kafka.StartOrderConsumer(ctx, me)
- kafka.processCreateCommand(me, command, consumerGroup)
- kafka.processCancelCommand(me, command, consumerGroup)
- kafka.StartTradeOutboxPublisher(ctx)
- database.PersistMatchResults(trades, orders)
- database.PersistMatchResultsTx(tx, trades, orders)
- database.PersistCancelledOrder(order)
- database.LoadOpenOrders()
- database.IsEventProcessed(tx, group, eventID)
- database.MarkEventProcessed(tx, group, eventID)

Request lifecycle
- There is no user-facing request lifecycle in the usual sense; the service receives Kafka messages rather than browser requests.
- The HTTP endpoints are limited to /orderbook/:symbol and /healthz for inspection.
- For a create order, the matching-engine receives an order command, processes it, persists its result, and publishes trade/order events.

Business logic
- The engine stores buy and sell orders in separate heaps and matches them by price-time priority.
- Buy orders prefer higher price; sell orders prefer lower price; ties are broken by creation time.
- Limit orders rest on the book until matched; market orders immediately consume the best opposite-side liquidity.
- The engine updates order status to PARTIALLY_FILLED or FILLED as quantities reduce.
- Cancellation sets the order remaining_quantity to 0 and status to CANCELLED.

Error handling
- Invalid or unsupported command types are logged and ignored.
- If persistence of match results fails, the engine restores the in-memory state from the snapshot it captured before the DB transaction.
- If a cancel command targets an order that no longer exists, the service marks the event processed and returns success.

Logging
- The service logs when it receives an order, when it executes trades, and when it processes commands.
- It uses telemetry for request tracing and counters.

Configuration
- Port 8082.
- Kafka broker from KAFKA_BROKER, default kafka:9092.
- DB host from DB_HOST, default postgres.

Dependencies
- PostgreSQL
- Kafka
- Shared observability and money packages

External services
- PostgreSQL
- Kafka

Database usage
- Load open orders on startup and replay state
- Persist trades and order status updates in transactions
- Insert outbox rows for trades and order-events
- Read order rows for cancellation logic

Kafka usage
- Consumes the orders topic
- Produces to trades and order-events topics
- Also uses DLQ topics for repeated failures

Redis usage
- None.

Performance
- The order book implementation is efficient because it uses heaps rather than scanning the entire order book.
- Matching is O(log n) for heap insert/pop plus proportional to the number of trades executed.

Scalability
- The core matching logic is simple, but the in-memory state makes horizontal scaling non-trivial.
- The Helm values keep matching-engine at one replica intentionally.

Security
- There is no user auth on the orderbook HTTP endpoint in the code shown.
- The service relies on the surrounding deployment and internal network rather than a strong auth layer for its internal API.

Thread safety
- The MatchingEngine struct uses a sync.Mutex to guard its maps and order books.
- The orderbook heaps are mutated only via the engine mutex.

Concurrency
- The service uses a goroutine for the Kafka consumer and a separate goroutine for the outbox publisher.
- The HTTP server also runs concurrently.

Possible bottlenecks
- In-memory state is not partitioned, so a very large book can become a single-threaded bottleneck.
- The service uses a single DB transaction per batch of trades and order updates.
- The orderbook is not replicated or sharded.

Possible improvements
- Partition order books by symbol and route orders to specific engine instances.
- Use a more durable and replicated matching state layer.
- Add a real market data service and a more sophisticated orderbook snapshot model.
- Use multiple worker pools for the outbox publisher.

Common interview questions
- Why are heaps used for the order book?
- How does price-time priority work in this implementation?
- Why is the matching engine stateful in memory and how does recovery work?

Senior interview questions
- How would you scale the matching engine beyond a single instance?
- What would change if you had to support thousands of symbols and millions of orders per second?

### 7.6 Websocket service

Purpose
- The websocket-service is the real-time delivery layer.
- It accepts WebSocket connections from browsers and broadcasts trade and order events to connected clients.

Folder structure
- [services/websocket-service](services/websocket-service)
- Files:
  - [services/websocket-service/main.go](services/websocket-service/main.go)
  - [services/websocket-service/database/db.go](services/websocket-service/database/db.go)
  - [services/websocket-service/telemetry](services/websocket-service/telemetry)

Main files
- [services/websocket-service/main.go](services/websocket-service/main.go)
- [services/websocket-service/database/db.go](services/websocket-service/database/db.go)

Entry point
- [services/websocket-service/main.go](services/websocket-service/main.go) starts the HTTP server on port 8083, starts Kafka consumers, and exposes /ws.

Main packages
- [services/websocket-service/database](services/websocket-service/database)
- [services/websocket-service/telemetry](services/websocket-service/telemetry)
- [pkg/observability](pkg/observability)
- github.com/gorilla/websocket
- github.com/segmentio/kafka-go

Important structs
- wsClient
- tradeMessage
- orderEvent
- outboundMessage

Methods and functions
- main()
- healthHandler(w, r)
- handleConnections(w, r)
- readLoop(client)
- consumeTrades(ctx)
- consumeOrderEvents(ctx)
- broadcast(message)
- buildTradeMessage(raw)
- buildOrderEventMessage(raw)
- processEvent(consumerGroup, eventID, payload)
- removeClient(client)
- writeControl(client, messageType)

Request lifecycle
1. The browser opens a WebSocket connection to /ws.
2. The server upgrades the connection and stores the client in an in-memory map.
3. The service starts a goroutine that sends periodic ping frames and another that reads incoming messages.
4. Kafka trade and order-event messages are consumed and transformed into outbound WebSocket payloads.
5. Each payload is broadcast to every connected client.

Business logic
- Trade events become a TRADE message with the symbol and raw trade payload.
- Order events become ORDER_UPDATE or CANCEL messages depending on the event type.
- The service uses processed_events to avoid duplicate delivery on Kafka redelivery.

Error handling
- Invalid Kafka payloads are logged and ignored.
- Broadcast failures remove the client from the active set.
- Duplicate event delivery is skipped via the idempotency table.

Logging
- The service logs connection creation, consumer errors, processing failures, and broadcast failures.

Configuration
- Port 8083.
- Kafka broker from KAFKA_BROKER, default kafka:9092.
- DB host from DB_HOST, default postgres.
- Heartbeats use 60-second pong deadlines and 30-second ping intervals.

Dependencies
- PostgreSQL for idempotency and health checks
- Kafka for event consumption
- Gorilla WebSocket and shared observability/telemetry packages

External services
- Kafka
- PostgreSQL

Database usage
- Checks DB health on /healthz
- Uses processed_events to avoid duplicate event delivery

Kafka usage
- Consumes from trades and order-events
- Does not publish back to Kafka

Redis usage
- None.

Performance
- The broadcast path iterates over all connected clients and writes to each one.
- The service uses a short write timeout to prevent slow clients from blocking indefinitely.

Scalability
- It is easy to scale to multiple instances for horizontal capacity, but the in-memory client map is not shared across instances.
- In a real deployment, a shared pub/sub or message bus for client fan-out would be needed.

Security
- The service does not enforce JWT auth on the websocket endpoint in the code shown.
- It relies on the upstream gateway and deployment context.

Thread safety
- The clients map is protected by a mutex.
- Each websocket client has its own mutex for writes.
- That prevents a slow client from blocking the entire connection set.

Concurrency
- One goroutine per connection handles pings and reads.
- Kafka consumers run in their own goroutines.
- Broadcast writes happen outside the central lock, which is a good pattern for avoiding head-of-line blocking.

Possible bottlenecks
- Broadcasting to many clients can be expensive.
- The in-memory connection list does not scale across multiple service instances.
- Slow clients can still introduce write latency and close/reconnect churn.

Possible improvements
- Use a shared pub/sub layer such as Redis Pub/Sub or NATS for multi-instance fan-out.
- Add backpressure and per-client quotas.
- Add a connection manager and sharding strategy.

Common interview questions
- Why does the websocket service need idempotency?
- How do you prevent one slow client from stalling the whole broadcast loop?
- Why is WebSocket preferred over polling here?

Senior interview questions
- How would you build a horizontally scalable live market feed across multiple websocket instances?
- What happens when the number of clients grows beyond the memory or CPU budget of one process?

---

## 8. Kafka architecture and design analysis

This repository uses Kafka as the asynchronous backbone for order propagation, trade execution, portfolio updates, and live UI delivery. The design is intentionally event-driven: synchronous HTTP is used for user-facing commands, while Kafka carries the downstream state changes.

### 8.1 What Kafka is used for

Kafka is not the entrypoint for the system. Instead, it is the event bus between the services that own different responsibilities:

- The order-service publishes order commands to the orders topic after an order is accepted and reservations are made.
- The matching-engine consumes those commands, executes matches, and publishes trade and order-event messages.
- The portfolio-service consumes those events to update balances, positions, and reservation state.
- The websocket-service consumes the same events to broadcast live updates to connected browser clients.

The key pattern is an outbox: the services do not publish directly after the user request completes. They persist the event in the database first, then a background worker publishes it to Kafka.

### 8.2 Topics in this repository

The Docker Compose bootstrap creates these topics:

- orders
  - Producer: [services/order-service/kafka/producer.go](services/order-service/kafka/producer.go)
  - Consumer: [services/matching-engine/kafka/consumer.go](services/matching-engine/kafka/consumer.go)
  - Purpose: carries create and cancel order commands from the order-service to the matching engine.

- trades
  - Producer: [services/matching-engine/kafka/producer.go](services/matching-engine/kafka/producer.go)
  - Consumers: [services/portfolio-service/kafka/consumer.go](services/portfolio-service/kafka/consumer.go) and [services/websocket-service/main.go](services/websocket-service/main.go)
  - Purpose: publishes matched trades to the portfolio-service and websocket-service.

- order-events
  - Producer: [services/matching-engine/kafka/producer.go](services/matching-engine/kafka/producer.go)
  - Consumers: [services/portfolio-service/kafka/consumer.go](services/portfolio-service/kafka/consumer.go) and [services/websocket-service/main.go](services/websocket-service/main.go)
  - Purpose: publishes order updates and cancellation events for downstream consumers.

- orders-dlq
  - Used by the order-service outbox publisher when publish attempts exceed the retry budget.

- trades-dlq
  - Used by the matching-engine outbox publisher when trade events fail repeatedly.

- order-events-dlq
  - Referenced by the matching-engine producer code, but not created in the Compose bootstrap. That is a small but important configuration gap in the repository.

### 8.3 Partitions

The bootstrap creates the main topics with three partitions:

- orders: 3 partitions
- trades: 3 partitions
- order-events: 3 partitions
- DLQ topics: 1 partition each

This is visible in [docker-compose.yml](docker-compose.yml). The producer code uses a hash-based partitioning strategy via the kafka-go balancer:

- [services/order-service/kafka/producer.go](services/order-service/kafka/producer.go)
- [services/matching-engine/kafka/producer.go](services/matching-engine/kafka/producer.go)

Because the producer uses a key, messages for the same symbol are routed to the same partition. That gives you a meaningful ordering guarantee per symbol without forcing all traffic through a single partition.

### 8.4 Replication

The repository is configured for a single-broker local development cluster:

- [docker-compose.yml](docker-compose.yml) sets KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR to 1.
- The Kafka container is a single broker with broker ID 1.
- The topics are also created with replication-factor 1.

That is appropriate for local development but not for production. In a real deployment, you would want at least three brokers and a replication factor of 3 for durability and failover.

### 8.5 Producers

The repository has two real producer paths:

1. Order-service producer
   - [services/order-service/kafka/producer.go](services/order-service/kafka/producer.go)
   - Publishes order create and cancel commands to the orders topic.
   - Uses JSON serialization and the order symbol as the message key.
   - The producer is simple: it marshals a Go struct into JSON and writes it with kafka-go.

2. Matching-engine producer
   - [services/matching-engine/kafka/producer.go](services/matching-engine/kafka/producer.go)
   - Publishes trade events to the trades topic and order updates to the order-events topic.
   - Also publishes failed events to DLQ topics after retry exhaustion.
   - Uses separate writers for each topic and the same hash-based balancer.

The outbox persistence layer is what makes these producers reliable:

- [services/order-service/database/outbox.go](services/order-service/database/outbox.go)
- [services/matching-engine/database/db.go](services/matching-engine/database/db.go)

Those files insert events into the database first and only then let the outbox worker publish them. That reduces the chance of a message being lost if the producer or the service crashes after the business write.

### 8.6 Consumers

The repository contains three consumer implementations:

- Matching-engine consumer
  - [services/matching-engine/kafka/consumer.go](services/matching-engine/kafka/consumer.go)
  - Reads the orders topic and processes create and cancel order commands.

- Portfolio-service consumers
  - [services/portfolio-service/kafka/consumer.go](services/portfolio-service/kafka/consumer.go)
  - One consumer reads trades, another reads order-events.
  - Both apply the state changes to balances, positions, and reservation state.

- Websocket-service consumers
  - [services/websocket-service/main.go](services/websocket-service/main.go)
  - One consumer reads trades, another reads order-events.
  - They transform the payloads into browser-friendly messages and broadcast them over WebSocket.

### 8.7 Consumer groups

The repository uses Kafka consumer groups to allow multiple consumers to share work and to manage replays safely:

- matching-engine uses the group matching-engine for orders.
- portfolio-service uses portfolio-service for trades and portfolio-service-order-events for order-events.
- websocket-service uses websocket-service-trades and websocket-service-order-events.

There is also a second layer of idempotency inside the application layer. The code uses the processed_events table with a composite key of consumer_group and event_id. That is separate from Kafka’s own consumer-group semantics and is used to prevent duplicate business effect when a message is redelivered.

### 8.8 Serialization

Serialization is intentionally simple:

- The message body is JSON from Go structs.
- The key is a string byte slice, usually the symbol or an event key.
- The code uses encoding/json directly in the producer and consumer paths.

Examples:

- [services/order-service/kafka/producer.go](services/order-service/kafka/producer.go)
- [services/matching-engine/kafka/consumer.go](services/matching-engine/kafka/consumer.go)
- [services/portfolio-service/kafka/consumer.go](services/portfolio-service/kafka/consumer.go)
- [services/websocket-service/main.go](services/websocket-service/main.go)

This is easy to understand and debug, but it is not a schema registry or versioned contract. In a production system, you would likely want Avro, Protobuf, or a versioned JSON schema layer.

### 8.9 Offsets and commits

The consumers commit offsets explicitly after processing successfully:

- [services/matching-engine/kafka/consumer.go](services/matching-engine/kafka/consumer.go)
- [services/portfolio-service/kafka/consumer.go](services/portfolio-service/kafka/consumer.go)
- [services/websocket-service/main.go](services/websocket-service/main.go)

The flow is:

1. Fetch a message.
2. Process it.
3. Commit it only if processing succeeded.

If the process crashes or throws an error before the commit, Kafka will redeliver the message. That is the core at-least-once behavior in this design.

### 8.10 Retries and backoff

Both the order-service and matching-engine outbox publishers implement retry logic.

- They claim pending outbox events from the database with FOR UPDATE SKIP LOCKED, which ensures that concurrent workers do not double-publish the same event.
- On publish failure they increment publish_attempts and schedule the next attempt at an exponential backoff.
- The retry schedule is capped and uses powers of two seconds.
- After the maximum attempts are reached, the event is redirected to a DLQ topic.

The relevant files are:

- [services/order-service/kafka/relay.go](services/order-service/kafka/relay.go)
- [services/matching-engine/kafka/producer.go](services/matching-engine/kafka/producer.go)

The retry budget is configured as maxDLQAttempts = 10 in both services.

### 8.11 Dead letter queues

The repository defines DLQ topics for failed events:

- orders-dlq
- trades-dlq
- order-events-dlq

The producers route events to the DLQ after retry exhaustion. The local bootstrap creates orders-dlq and trades-dlq but does not create order-events-dlq. That means the DLQ path is implemented in code but the local topic is only partially provisioned.

### 8.12 Ordering guarantees

Kafka provides ordering only within a partition. This repository uses that fact intentionally:

- messages with the same key go to the same partition because the producer uses a hash balancer.
- the event key is often the symbol, so messages for a given symbol stay ordered.

That means the system preserves per-symbol order, not global order across all symbols or topics.

It is important to separate this from the application-level matching logic. The matching engine itself enforces price-time priority in the in-memory order book in [services/matching-engine/orderbook/orderbook.go](services/matching-engine/orderbook/orderbook.go). Kafka is not the source of that business rule; Kafka only preserves the message order within a partition.

### 8.13 Idempotency and duplicate events

The system expects duplicates and actively handles them.

- The outbox tables record pending events and their publish attempts.
- Consumers check a processed_events table before applying business effects.
- The code uses the same event ID as the deduplication key.

The key files are:

- [services/matching-engine/kafka/consumer.go](services/matching-engine/kafka/consumer.go)
- [services/portfolio-service/kafka/consumer.go](services/portfolio-service/kafka/consumer.go)
- [services/websocket-service/main.go](services/websocket-service/main.go)
- [infra/postgres/init.sql](infra/postgres/init.sql)

This is a very strong pattern because it turns Kafka’s at-least-once delivery into an application-level exactly-once-ish effect for business mutations.

### 8.14 Exactly once vs at least once

This repository does not implement true Kafka exactly-once semantics.

What it has:

- at-least-once delivery from Kafka
- application-level idempotency using processed_events
- outbox-based reliability

What it does not have:

- atomic commit between Kafka offset commit and database commit
- transactional producer/consumer semantics across the message broker and the database

So the best description is: at-least-once delivery with application-level deduplication, not true exactly-once semantics.

### 8.15 Failure handling and recovery

Failure handling is built around retries and replay:

- If a consumer fails before it commits the offset, the message is retried.
- If a consumer processes the same event twice, the processed_events table prevents double application.
- If a matching-engine DB transaction fails, the engine restores the in-memory state from a snapshot.
- If the outbox publisher crashes, the event remains in the database and is retried later.

That makes the system resilient to process restarts and temporary network or broker issues.

### 8.16 Back pressure and scaling

The repository does not implement sophisticated Kafka back pressure tuning. Instead, it uses simple conservative controls:

- the outbox worker claims 100 events per tick
- the worker runs every 500ms
- consumers process one message at a time and commit after success

That is adequate for a prototype, but it is not a high-throughput production setup. Scaling Kafka itself is straightforward through partitions and consumer groups, but the matching-engine is still stateful and in-memory, so it is not horizontally scaled in the same way as the stateless consumers.

### 8.17 How messages travel through the system

The end-to-end flow is:

1. The order-service accepts an order and writes it to PostgreSQL.
2. The order-service outbox inserts a pending event into the order_outbox table.
3. The outbox worker publishes the event to the orders topic.
4. The matching-engine consumes the event and applies the order to the in-memory order book.
5. The matching-engine writes trade and order-event rows to the database and inserts outbox events for them.
6. The matching-engine outbox worker publishes those events to the trades and order-events topics.
7. The portfolio-service consumes them and updates balances and positions.
8. The websocket-service consumes them and broadcasts updates to the UI.

That is the core event-driven architecture of the repository.

### 8.18 Why Kafka was chosen here instead of RabbitMQ, Redis Streams, NATS, or gRPC

Kafka was chosen because this system needs durable, replayable, fan-out event propagation between services. That maps better to Kafka than to the alternatives:

- RabbitMQ: excellent for task queues and request/response-style workloads, but less natural for durable event streams with replay and partitioned fan-out.
- Redis Streams: very fast and simple, but less appropriate when you need a long-lived event backbone, multi-consumer replay, and strong stream semantics in a distributed system.
- NATS: great for low-latency messaging and simple pub/sub, but less natural for the large-scale event retention and replay pattern used here.
- gRPC: great for synchronous request/response, but it is not a pub/sub backbone and does not naturally support fan-out to many consumers or durable event replay.

Kafka fits because the repository needs:

- asynchronous event propagation
- multiple consumers of the same event
- replay after restart
- partitioned ordering by key
- a durable event backbone that can outlive a single process

### 8.19 Likely interview questions with answers

1. Why does this repository use Kafka instead of calling services directly?
   - Because the system needs asynchronous propagation of order and trade events to multiple downstream services, and Kafka decouples the producers from the consumers.

2. What is the difference between an outbox and a normal producer call?
   - An outbox writes the event to the database first so the event is not lost if the producer crashes. The background worker later publishes it.

3. Why are consumer groups important here?
   - They let multiple service instances share the load and allow partitions to be consumed independently while still preserving the ability to replay messages safely.

4. Is this system exactly once?
   - No. It is at-least-once delivery with application-level idempotency, not true exactly-once semantics.

5. Why is processed_events used if Kafka already has offsets?
   - Kafka offsets protect against redelivery, but the application still needs a business-level deduplication layer because the database commit and offset commit are separate operations.

6. Why are the topics partitioned?
   - To parallelize consumption and to preserve ordering within a partition by message key.

7. What is the main tradeoff of the current Kafka design?
   - It is simple and robust for a prototype, but it is not yet production-grade for replication, schema evolution, or high-throughput scaling.

8. Why is the matching-engine still single-instance in this repo even though Kafka is used?
   - Because its core state is in-memory and not partitioned or sharded. Kafka handles event delivery, but the matching engine itself still needs a single coherent order book.

9. What would you improve first in a production version?
   - Move to a multi-broker Kafka cluster, add schema evolution, strengthen DLQ handling and replay tooling, and make the matching engine partitioned or replicated.

10. Why is the message key important?
   - It controls partition assignment. Using the symbol as the key preserves per-symbol order and keeps related events together.

---

## 9. Database architecture and SQL analysis

This repository uses one primary database: PostgreSQL. All application services connect to the same PostgreSQL instance defined in [docker-compose.yml](docker-compose.yml), and the schema is initialized from [infra/postgres/init.sql](infra/postgres/init.sql) plus the migrations under [infra/postgres/migrations](infra/postgres/migrations).

There is no separate database per service in this repository. The services are logically separated, but the persistence layer is shared. That is important because many of the correctness properties come from the database, not from the services themselves.

### 9.1 Database inventory

- Primary database: PostgreSQL 15
  - Defined in [docker-compose.yml](docker-compose.yml)
- Service connection code:
  - [services/user-service/database/db.go](services/user-service/database/db.go)
  - [services/order-service/database/db.go](services/order-service/database/db.go)
  - [services/portfolio-service/database/db.go](services/portfolio-service/database/db.go)
  - [services/matching-engine/database/db.go](services/matching-engine/database/db.go)
  - [services/websocket-service/database/db.go](services/websocket-service/database/db.go)
- No Redis, MongoDB, or other database system is used for durable state in this repository.

### 9.2 Schema overview

The core schema is defined in [infra/postgres/init.sql](infra/postgres/init.sql) and then expanded by the SQL migrations.

#### Users

The users table stores account identity and balances:

- id: UUID primary key
- email: unique email used for authentication
- password_hash: bcrypt hash for login
- role: application role, currently user by default
- balance: current spendable balance
- reserved_balance: balance held by open reservations
- created_at: timestamp

This table is used by the user-service for sign-up and login and by the portfolio-service for reserve and release operations.

#### Refresh tokens

The refresh_tokens table stores server-side refresh tokens for session management:

- id: UUID primary key
- user_id: foreign key to users
- token_hash: hashed refresh token, not the raw token
- expires_at: expiry timestamp
- created_at: timestamp
- revoked_at: optional revocation timestamp

This table is used by [services/user-service/database/db.go](services/user-service/database/db.go) and the auth handlers in [services/user-service/main.go](services/user-service/main.go).

#### Positions

The positions table stores per-user, per-symbol inventory:

- user_id: foreign key to users
- symbol: ticker symbol
- quantity: current available quantity
- reserved_quantity: quantity currently reserved by pending orders

This is a composite primary key on (user_id, symbol). It is used by the portfolio-service for sell-side reservations and by the matching engine’s downstream financial updates.

#### Orders

The orders table is the durable record of admitted orders:

- id: UUID primary key
- user_id: owner of the order
- symbol: instrument symbol
- side: BUY or SELL
- type: LIMIT or MARKET
- price: price in fixed-point integer units
- quantity: original quantity
- remaining_quantity: remaining quantity after fills
- reserved_amount: amount reserved for the user
- status: NEW, PARTIALLY_FILLED, FILLED, or CANCELLED
- created_at: timestamp

This is the main business table for order admission and is read by the order-service, portfolio-service, and matching-engine.

#### Trades

The trades table stores executed trades:

- id: UUID primary key
- symbol
- buyer_user_id
- seller_user_id
- buy_order_id
- sell_order_id
- price
- quantity
- executed_at

This table is populated by the matching-engine after an order match and is then consumed by the portfolio-service and websocket-service.

#### Outbox tables

The repository uses two outbox tables:

- order_outbox: used by the order-service to persist outbound order commands before publication
- trade_outbox: used by the matching-engine to persist outbound trade and order-event messages

Each outbox row contains:

- id
- topic
- event_key
- payload
- created_at
- published_at
- publish_attempts
- next_attempt_at
- claimed_by
- claimed_at
- last_error

This is the reliability mechanism that makes Kafka publishing durable even if a process crashes.

#### Processed events

The processed_events table is used to make consumers idempotent:

- consumer_group: the logical consumer group name
- event_id: UUID of the event that has already been applied
- processed_at: when it was recorded

This is used by the matching-engine, portfolio-service, and websocket-service to prevent duplicate business effects after re-delivery.

### 9.3 Relationships

The schema is small but it encodes the main domain relationships clearly.

- users to refresh_tokens: one-to-many
  - The refresh token rows belong to a single user and are removed on cascade delete.
- users to positions: one-to-many, keyed by (user_id, symbol)
  - Each user can have zero or many positions by ticker.
- users to orders: one-to-many
  - Each order belongs to a single user.
- users to trades: two foreign-key relationships
  - buyer_user_id and seller_user_id both point to users.
- orders to trades: two foreign-key relationships
  - buy_order_id and sell_order_id both point to orders.
- outbox tables are independent of the business tables but are linked by the application flow
  - The order-service writes an outbox row alongside the order row.
  - The matching-engine writes trade and order-event rows and their outbox rows in the same transaction.

### 9.4 Indexes

The repository uses several indexes, mostly for common access patterns.

- refresh_tokens(user_id)
- refresh_tokens(token_hash)
- orders(symbol)
- orders(status)
- trades(symbol)
- trades(executed_at)
- idx_order_outbox_unpublished
- idx_order_outbox_publishable
- idx_trade_outbox_unpublished
- idx_trade_outbox_publishable
- idx_trade_outbox_publishable

These are visible in [infra/postgres/init.sql](infra/postgres/init.sql) and the migration files [infra/postgres/migrations/000002_extend_trade_outbox.up.sql](infra/postgres/migrations/000002_extend_trade_outbox.up.sql) and [infra/postgres/migrations/000003_add_order_outbox.up.sql](infra/postgres/migrations/000003_add_order_outbox.up.sql).

### 9.5 Constraints

The schema has a strong set of constraints that reflect the financial integrity requirements of the system.

- users balance and reserved_balance must be non-negative
- positions quantity and reserved_quantity must be non-negative
- orders price must be non-negative
- orders quantity must be strictly positive
- orders remaining_quantity must be non-negative
- orders reserved_amount must be non-negative
- orders side must be BUY or SELL
- orders type must be LIMIT or MARKET
- orders status must be one of NEW, PARTIALLY_FILLED, FILLED, CANCELLED
- trades price and quantity must be positive
- email is unique, and refresh tokens are unique by token hash

These checks are defined directly in [infra/postgres/init.sql](infra/postgres/init.sql) and the migration set.

### 9.6 Transactions

Transactions are central to this system. The database is treated as the source of truth and the code frequently uses explicit transactions to preserve consistency.

Examples:

- Order creation and outbox insertion are wrapped in a transaction in [services/order-service/database/outbox.go](services/order-service/database/outbox.go).
- Matching results and outbox persistence are wrapped in one transaction in [services/matching-engine/database/db.go](services/matching-engine/database/db.go).
- Reservation and release operations in [services/portfolio-service/database/db.go](services/portfolio-service/database/db.go) use transactions with row locks.
- Trade application in [services/portfolio-service/database/db.go](services/portfolio-service/database/db.go) locks the relevant account rows and updates balances and positions atomically.

### 9.7 Isolation levels

The repository does not explicitly set a transaction isolation level. The code relies on PostgreSQL’s default isolation level, which is Read Committed.

That is acceptable for this prototype because the application uses explicit row locking where correctness matters. In particular:

- SELECT ... FOR UPDATE is used around reservation updates and account locking.
- BEGIN/COMMIT transactions ensure that balance and position updates are atomic.

### 9.8 Locks

The project uses pessimistic locking deliberately for financial state.

#### Pessimistic locking

The portfolio-service uses SELECT ... FOR UPDATE in several places:

- [services/portfolio-service/database/db.go](services/portfolio-service/database/db.go)
- [services/portfolio-service/main.go](services/portfolio-service/main.go)

That protects the user balance row or the position row while a reservation or trade is being processed.

#### Optimistic locking

There is no optimistic locking implementation in the repository. There are no version columns, no compare-and-swap checks, and no retry-on-conflict logic. That is a deliberate simplification. The system relies on explicit DB transactions and row-level locks instead.

#### Deadlocks

Deadlocks are possible in any locking-based system, especially when two transactions touch related rows in different order. This repository partially mitigates that risk by locking buyer and seller accounts in a deterministic order when applying a trade in [services/portfolio-service/kafka/consumer.go](services/portfolio-service/kafka/consumer.go).

The locking order is based on comparing the two user IDs so that the same pair of accounts is always locked in the same order. That is an interview-worthy detail because it shows awareness of lock ordering as a deadlock-prevention technique.

### 9.9 Connection pooling

The repository uses standard Go database/sql connection pools.

- [services/order-service/database/db.go](services/order-service/database/db.go) sets MaxOpenConns(10), MaxIdleConns(5), and MaxConnLifetime(5 minutes).
- [services/portfolio-service/database/db.go](services/portfolio-service/database/db.go) uses the same pool settings.
- The user-service and matching-engine do not override the defaults, so they rely on the library defaults.

This is a practical choice for a prototype. It helps bound resource usage, but it is not a full production tuning story.

### 9.10 Queries and access patterns

The service code uses a small but important set of queries.

#### User-service queries

- INSERT INTO users
- SELECT user by email or ID
- INSERT refresh token
- UPDATE refresh token revocation

These are in [services/user-service/database/db.go](services/user-service/database/db.go).

#### Order-service queries

- INSERT INTO orders
- SELECT order by ID

These are in [services/order-service/database/db.go](services/order-service/database/db.go).

#### Portfolio-service queries

- SELECT users row FOR UPDATE
- SELECT positions row FOR UPDATE
- UPDATE users reserved balance
- UPDATE positions reserved quantity
- UPDATE orders reserved_amount
- INSERT INTO positions ... ON CONFLICT DO UPDATE

These are in [services/portfolio-service/database/db.go](services/portfolio-service/database/db.go) and [services/portfolio-service/main.go](services/portfolio-service/main.go).

#### Matching-engine queries

- SELECT open orders for replay
- INSERT trades
- UPDATE orders status/remaining_quantity
- INSERT outbox rows
- SELECT/INSERT processed_events

These are in [services/matching-engine/database/db.go](services/matching-engine/database/db.go).

### 9.11 Performance characteristics

The database design is deliberately simple and correct, not heavily denormalized.

Strengths:

- ACID transactions around critical financial updates
- constraints safeguard invalid state
- row-level locks protect reservations and trade application
- outbox tables make event publication durable

Limitations:

- the database is a single point of contention for financial updates
- row locks can create head-of-line blocking under load
- there are no partitions or read replicas yet
- there are no advanced indexes for very large history tables

### 9.12 Slow query possibilities

The most likely slow or expensive queries are:

- reconciliation queries in [services/portfolio-service/reconcile.go](services/portfolio-service/reconcile.go), especially if the orders and trades tables grow large
- replay of open orders on startup in [services/matching-engine/database/db.go](services/matching-engine/database/db.go)
- outbox claim queries that scan for unpublished events in [services/order-service/database/outbox.go](services/order-service/database/outbox.go) and [services/matching-engine/database/db.go](services/matching-engine/database/db.go)
- reservation and trade updates that acquire row locks on users, positions, and orders under concurrent load

### 9.13 Scaling

The current design scales only to a modest degree.

- The app can scale horizontally at the service layer, but the database remains shared.
- The matching-engine is still single-instance from a stateful perspective.
- The portfolio-service uses row locks, so contention can grow quickly with volume.
- The current schema is not partitioned.

In a production version, the next logical steps would be:

- PostgreSQL replication and failover
- partitioning for very large history tables such as trades and orders
- connection-pool tuning and statement-level monitoring
- a dedicated ledger or immutable accounting table

### 9.14 Partitioning

There is no partitioning implemented in the repository. That is notable because the trades and orders tables would likely become large in a real production environment. The schema is simple and correct, but it is not yet designed for very large historical volumes.

### 9.15 Replication

There is no replication configuration in the repository. The Compose file runs a single PostgreSQL container with a single data volume. That is enough for local development but not for availability or failover.

### 9.16 Backup strategy

The repository does not define a backup strategy beyond the named Docker volume used by Postgres in [docker-compose.yml](docker-compose.yml). That means:

- the data is persisted locally on disk,
- but there is no automated backup, restore, or snapshot pipeline in the codebase.

A production version would need:

- scheduled pg_dump or WAL-based backups,
- offsite storage,
- point-in-time recovery.

### 9.17 Recovery and crash tolerance

The repository has a fairly thoughtful recovery story despite the lack of formal backup automation.

- outbox rows remain in the database until they are published successfully
- consumers use processed_events to survive redelivery
- the matching-engine reloads open orders from PostgreSQL on startup
- the portfolio-service can reconcile after a crash because the financial state is stored in PostgreSQL and not only in memory

This is much stronger than a purely in-memory design and is one of the clearest architectural strengths of the project.

### 9.18 SQL files, one by one

#### [infra/postgres/init.sql](infra/postgres/init.sql)

This is the primary schema definition for the application. It creates the users, refresh_tokens, positions, orders, trades, order_outbox, trade_outbox, processed_events tables, plus indexes and constraints. It also seeds the initial users and positions. This file is the authoritative database blueprint for the project.

#### [infra/postgres/migrations/000001_convert_financial_columns_to_bigint.up.sql](infra/postgres/migrations/000001_convert_financial_columns_to_bigint.up.sql)

This migration converts financial columns from decimal-like types to BIGINT for fixed-point arithmetic. It is an important migration because the shared money package expects integer-based values for correctness.

#### [infra/postgres/migrations/000002_extend_trade_outbox.up.sql](infra/postgres/migrations/000002_extend_trade_outbox.up.sql)

This migration extends the trade outbox with publish_attempts, next_attempt_at, claimed_by, claimed_at, and last_error. Those fields are used by the outbox publisher to implement retry and claiming logic.

#### [infra/postgres/migrations/000003_add_order_outbox.up.sql](infra/postgres/migrations/000003_add_order_outbox.up.sql)

This migration creates the order_outbox table, which is used by the order-service to publish order commands reliably to Kafka through the outbox pattern.

#### [infra/postgres/migrations/000004_add_trade_constraints.up.sql](infra/postgres/migrations/000004_add_trade_constraints.up.sql)

This migration adds positive-value constraints to the trades table. That is a good example of adding data integrity rules after the fact.

#### [infra/postgres/migrations/000005_add_auth_schema.up.sql](infra/postgres/migrations/000005_add_auth_schema.up.sql)

This migration adds the authentication schema: the email and password fields on users and the refresh_tokens table. It also adds the foreign-key relationship and indexes for token lookup and revocation.

### 9.19 Likely interview questions with answers

1. Why does the repository use PostgreSQL instead of a simpler store?
   - Because the system needs transactions, constraints, row-level locks, and financial integrity around balances and positions.

2. What is the role of the outbox tables?
   - They make message publication durable by recording the intent to publish in the database before the Kafka producer runs.

3. Why are SELECT ... FOR UPDATE queries used?
   - They lock the relevant rows while the service updates balances or reservations so two concurrent transactions do not corrupt the state.

4. Is this system using optimistic locking?
   - No. It uses pessimistic locking via row locks instead of version numbers and retry-on-conflict.

5. What is the biggest database bottleneck in the current architecture?
   - The shared PostgreSQL instance and the row-level locking around financial state are the main bottlenecks.

6. Why is processed_events needed if Kafka already has offsets?
   - Because the app needs its own deduplication layer to avoid applying the same business event twice if a consumer crashes after the database write but before the offset commit.

7. Why are there both order_outbox and trade_outbox?
   - Because the order-service and matching-engine own different event streams and need separate outbox tables for their respective events.

8. What is missing for production readiness?
   - Replication, backup automation, partitioning, stronger monitoring, and more aggressive connection and query tuning.

9. Why is the schema written in SQL files instead of being generated by the app?
   - Because it makes deployment reproducible and keeps the persistence contract explicit and versioned.

10. Why do the financial columns use BIGINT and fixed-point integers?
   - To avoid floating-point errors in currency calculations and align with the money package in [pkg/money](pkg/money).

---

## 10. Frontend

### [frontend](frontend)

Why it exists:
- This is the user-facing experience for placing orders, viewing portfolio data, and seeing live market activity.

Responsibilities:
- Provide auth pages: login and signup.
- Provide a dashboard that allows users to place orders and view portfolio state.
- Use React Query for server state.
- Use Zustand for auth state persistence.
- Connect to the websocket-service for live updates.

Important files:
- [frontend/src/app/page.tsx](frontend/src/app/page.tsx)
- [frontend/src/app/(app)/dashboard/page.tsx](frontend/src/app/(app)/dashboard/page.tsx)
- [frontend/src/lib/api.ts](frontend/src/lib/api.ts)
- [frontend/src/lib/useWebSocket.ts](frontend/src/lib/useWebSocket.ts)
- [frontend/src/lib/store.ts](frontend/src/lib/store.ts)

Important implementation details:
- The dashboard calls /api/orders and /api/portfolio/ through the API layer.
- The useWebSocket hook connects to /ws and parses messages from the backend.
- Auth tokens are stored in browser storage via Zustand persist middleware.

Design decisions:
- The frontend is deliberately not tightly coupled to all internal services; it uses the gateway as its API boundary.
- It is a polished demo-style UI rather than a production-grade trading terminal.

Tradeoffs:
- The UI uses mock chart data and simplified order book handling. The live market feed is functional but still a simplified representation of a real exchange.

---

## 11. Testing and quality

### [services/.../tests](services)

Why they exist:
- These tests validate the core mechanics and catch regressions in matching, outbox behavior, and websocket flow.

Important files:
- [services/matching-engine/orderbook/orderbook_test.go](services/matching-engine/orderbook/orderbook_test.go)
- [services/matching-engine/engine/engine_test.go](services/matching-engine/engine/engine_test.go)
- [services/matching-engine/kafka/consumer_test.go](services/matching-engine/kafka/consumer_test.go)
- [services/order-service/kafka/relay_test.go](services/order-service/kafka/relay_test.go)
- [services/portfolio-service/reconcile_test.go](services/portfolio-service/reconcile_test.go)
- [services/websocket-service/main_test.go](services/websocket-service/main_test.go)
- [pkg/money/money_test.go](pkg/money/money_test.go)

Design decisions:
- The repository values correctness around matching and financial invariants and includes targeted tests for that.
- There is also an end-to-end shell script that exercises a realistic order flow against Docker Compose.

### [scripts](scripts)

Why it exists:
- This directory contains operational scripts for validating the system end to end.

Important files:
- [scripts/e2e.sh](scripts/e2e.sh)
- [scripts/e2e_test.sh](scripts/e2e_test.sh)

Design decisions:
- The script orchestrates Docker Compose, waits for health checks, places orders, and verifies database state. It is a strong demonstration of the system’s intended business flow.

---

## 12. CI/CD and engineering workflow

### [.github/workflows](.github/workflows)

Why it exists:
- This folder defines automated quality gates for every pull request.

Important files:
- [.github/workflows/ci.yml](.github/workflows/ci.yml)

Responsibilities:
- Lint Go code and frontend code.
- Run Go tests across services and shared packages.
- Build Docker images for each service and the frontend.
- Run Trivy vulnerability scanning.

Design decisions:
- The workflow is broad and pragmatic. It covers the main concerns of an engineering team: correctness, linting, and basic security scanning.
- It uses matrix builds for service images and one build path for the frontend.

Tradeoffs:
- It does not include deployment automation or advanced release management in the current repository.

---

## 13. What the repository is really trying to demonstrate

This repository is not a generic web app. It is trying to demonstrate a distributed trading platform with the following engineering themes:

- event-driven architecture,
- service separation,
- asynchronous communication,
- financial correctness via reservations and reconciliation,
- real-time user updates,
- operational readiness via containerization and Helm,
- an interview-friendly architecture that can be defended.

That is why the project includes both the runtime path and the supporting systems: Kafka topics, outbox tables, reconciliation tasks, real-time websockets, and deployment manifests. These make the repository suitable for discussing distributed systems tradeoffs.

---

## 14. The architecture, explained as if you built it from nothing

If you are learning this project from scratch, the best way to understand it is to build the mental model in this order:

1. Start with the user experience.
   - The browser uses the frontend to submit an order.
   - The frontend does not talk directly to the trading engine. It talks to one public API boundary, the gateway.

2. Understand the domain boundaries.
   - The order-service owns the order admission lifecycle.
   - The portfolio-service owns account and position state.
   - The matching-engine owns execution and price-time priority logic.
   - The websocket-service owns the live update channel.
   - The user-service owns identity and sessions.

3. Understand the reliability model.
   - The system is not built around one giant transaction across all services.
   - Instead, each service does local work, persists state, and publishes an event to Kafka through an outbox.
   - That is how the platform stays decoupled and recoverable.

### 11.1 High-level architecture

At the highest level, TradeSphere is a layered system:

- Presentation layer: Next.js frontend
- Edge layer: API gateway
- Domain services: order-service, user-service, portfolio-service, matching-engine, websocket-service
- Data layer: PostgreSQL
- Event layer: Kafka
- Platform layer: Docker Compose, Helm, Prometheus, Jaeger, Grafana

The architecture is built around one central idea: the user places an order, the system reserves capacity for it, the exchange matches it, the portfolio state is updated, and the UI is notified in real time.

### 11.2 Why microservices were chosen

Microservices were chosen because the project is not just a CRUD app. It is trying to model a real exchange workflow with separate responsibilities:

- order intake is a different concern from matching,
- portfolio state needs its own correctness logic,
- websocket delivery is a different runtime concern from order admission,
- auth is a separate security boundary,
- event-driven propagation is easier to reason about when each concern is a separate component.

If this were a monolith, the code would be simpler at first but harder to evolve. A monolith would make it harder to show:

- resilience to downstream failures,
- independent scaling of order admission versus matching,
- real-time broadcast logic separate from matching,
- clear ownership boundaries for financial correctness.

The system therefore chooses microservices not because microservices are always better, but because they make the domain boundaries visible and teachable.

### 11.3 Architectural boundaries in plain English

Each service is responsible for a specific business concern:

- user-service: identity, login, tokens, refresh sessions
- order-service: validates orders and creates admission records
- portfolio-service: owns balance, escrow/reservation, positions
- matching-engine: executes matches and updates the order book
- websocket-service: pushes state changes to browsers
- api-gateway: single ingress and request routing

This matches the real-world division of labor in an exchange-like system.

---

## 15. Request flow from browser to backend

The simplest request path is placing an order.

### 12.1 Step-by-step request flow

1. The browser loads the dashboard in the frontend.
2. The frontend calls the gateway at /api/orders.
3. The gateway checks CORS, rate limits, and request ID propagation.
4. The gateway forwards the request to the order-service.
5. The order-service validates the request body and the JWT from the browser.
6. The order-service verifies that the user ID from the token matches the body.
7. The order-service calls the portfolio-service /validate to ensure the user can afford the request or hold the asset.
8. The portfolio-service checks the users and positions tables and returns allowed or not allowed.
9. The order-service calls the portfolio-service /reserve to place a temporary reservation.
10. The portfolio-service uses a transaction and row-level locking to reserve the balance or position.
11. The order-service persists the order to the orders table and writes an outbox row in the order_outbox table.
12. A background publisher attempts to send the order command to Kafka.
13. The matching-engine consumes the command, applies it to the in-memory order book, and writes trade/order-event records.
14. The portfolio-service consumes those events and updates balances and positions.
15. The websocket-service consumes the same events and broadcasts them to browser clients.
16. The frontend receives the update through WebSocket and refreshes the order list and portfolio view.

### 12.2 ASCII sequence diagram: placing an order

```text
Browser -> Frontend: user clicks Buy/Sell
Frontend -> API Gateway: POST /api/orders
API Gateway -> Order Service: POST /orders
Order Service -> Portfolio Service: GET /validate?user_id=...&symbol=...&side=...&price=...&quantity=...
Portfolio Service -> PostgreSQL: read users/positions, compute allowance
Portfolio Service -> Order Service: 200 allowed / 400 denied
Order Service -> Portfolio Service: POST /reserve
Portfolio Service -> PostgreSQL: BEGIN; SELECT ... FOR UPDATE; UPDATE reserved_balance/reserved_quantity; COMMIT
Portfolio Service -> Order Service: 200 reservation accepted
Order Service -> PostgreSQL: BEGIN; INSERT order; INSERT order_outbox; COMMIT
Order Service -> Kafka: publish order command (background outbox worker)
Kafka -> Matching Engine: consume order command
Matching Engine -> PostgreSQL: persist order updates and trades
Matching Engine -> Kafka: publish trade/order-event
Kafka -> Portfolio Service: consume trade/order-event
Portfolio Service -> PostgreSQL: apply balance/position updates
Kafka -> Websocket Service: consume trade/order-event
Websocket Service -> Browser: push TRADE / ORDER_UPDATE / CANCEL
```

### 12.3 Why the flow is this way

The flow is intentionally split by concern:

- the gateway handles ingress,
- the order-service handles admission,
- the portfolio-service handles financial validity,
- the matching-engine handles execution,
- the websocket-service handles delivery.

That separation is the whole point of the architecture.

---

## 16. Event flow in detail

The system uses Kafka for asynchronous propagation. The important point is that Kafka is not the source of truth; PostgreSQL is. Kafka is the transport and propagation layer.

### 13.1 Kafka topics used by this repository

- orders
  - Produced by the order-service outbox publisher.
  - Consumed by the matching-engine.
  - Carries order commands such as CREATE_ORDER and CANCEL_ORDER.

- trades
  - Produced by the matching-engine outbox publisher.
  - Consumed by the portfolio-service and websocket-service.
  - Carries executed trade records.

- order-events
  - Produced by the matching-engine outbox publisher.
  - Consumed by the portfolio-service and websocket-service.
  - Carries order lifecycle updates such as ORDER_UPDATE and ORDER_CANCELLED.

- orders-dlq, trades-dlq, order-events-dlq
  - These are fallback topics for failed or excessive retries.

### 13.2 ASCII diagram: event propagation

```text
Order Service
  └─> PostgreSQL order_outbox
        └─> outbox publisher
              └─> Kafka topic: orders
                    └─> Matching Engine
                          └─> PostgreSQL orders/trades/trade_outbox
                                └─> outbox publisher
                                      └─> Kafka topic: trades
                                      └─> Kafka topic: order-events
                                            ├─> Portfolio Service
                                            └─> Websocket Service
```

### 13.3 The meaning of each Kafka event

1. Order commands on orders
   - Payload is a JSON object with fields such as id, type, symbol, order, cancel, created_at.
   - The matching-engine uses this to create or cancel orders.

2. Trade events on trades
   - Payload is a trade object with id, symbol, buyer_user_id, seller_user_id, buy_order_id, sell_order_id, price, quantity, executed_at.
   - The portfolio-service uses this to apply cash and position movement.
   - The websocket-service uses this to push a TRADE message to the browser.

3. Order events on order-events
   - Payload is an order event object with id, type, order_id, user_id, symbol, side, status, remaining_quantity, reserved_amount, created_at.
   - The portfolio-service uses ORDER_CANCELLED to release reservations.
   - The websocket-service uses the event to display order lifecycle changes.

### 13.4 Why Kafka was chosen

Kafka was chosen because the architecture needs asynchronous, replayable, decoupled propagation. It is a good fit for:

- event-driven services,
- eventual consistency,
- replayability,
- independent consumers.

Alternatives such as direct HTTP fan-out or RabbitMQ were possible, but Kafka fits the teaching goal of distributed systems and event propagation well.

---

## 17. Data flow and ownership

This project has one important rule: the database is the source of truth, while Kafka is the propagation mechanism.

### 14.1 Who owns what state

- user-service owns user identity and refresh tokens.
- portfolio-service owns balances, reserved balances, and positions.
- order-service owns order records and order admission state.
- matching-engine owns order book execution state and trade creation records.
- websocket-service does not own durable business state; it consumes state changes and broadcasts them.

### 14.2 The core data lifecycle

1. A user signs up.
   - user-service writes a user row and password hash.
2. A user places an order.
   - order-service writes an order row and outbox row.
3. The order is matched.
   - matching-engine updates the order rows and creates trade rows.
4. The portfolio is updated.
   - portfolio-service updates users and positions rows.
5. The browser receives a push.
   - websocket-service forwards the event to the browser.

### 14.3 Database interaction inventory

User-service database interactions:
- Create user
- Find user by email
- Find user by ID
- Store refresh token
- Revoke refresh token
- Lookup refresh token hash

Order-service database interactions:
- Insert order
- Fetch order by ID
- Insert order outbox event
- Enqueue cancel command
- Mark outbox published
- Record outbox retry

Matching-engine database interactions:
- Load open orders for replay
- Insert trade rows
- Update order state after match/cancel
- Insert trade outbox rows
- Insert order-event outbox rows
- Check processed-event idempotency

Portfolio-service database interactions:
- Validate balance and position availability
- Reserve balance and positions in transactions
- Release reservations on cancel
- Apply buyer trade updates
- Apply seller trade updates
- Record processed events for idempotency
- Run reconciliation queries

Websocket-service database interactions:
- Check database health
- Record processed events to avoid duplicate delivery

### 14.4 Why PostgreSQL was chosen

PostgreSQL was chosen because the system needs:

- transactional correctness,
- schemas and constraints,
- row-level locking,
- JSONB support for outbox payloads,
- mature container support and migration tooling.

Alternatives such as SQLite or MongoDB would be less suitable for the financial integrity and transaction requirements of this system.

---

## 18. API call catalog

Every API call in the repository is worth understanding because it shows the architecture in action.

### 15.1 Frontend to gateway

1. POST /api/auth/signup
   - Sent by the frontend to the gateway.
   - Routed to the user-service /signup.
   - Creates a user account.

2. POST /api/auth/login
   - Routed to user-service /login.
   - Returns an access token and refresh token.

3. POST /api/auth/refresh
   - Routed to user-service /refresh.
   - Reissues tokens after refresh rotation.

4. POST /api/auth/logout
   - Routed to user-service /logout.
   - Revokes the refresh token.

5. POST /api/orders
   - Protected by JWT auth.
   - Routed to order-service /orders.
   - Creates an order.

6. POST /api/orders/:id/cancel
   - Protected by JWT auth.
   - Routed to order-service /orders/:id/cancel.
   - Requests cancellation.

7. GET /api/portfolio/:userId
   - Protected.
   - Routed to portfolio-service /portfolio/:userId.
   - Returns balance and positions.

8. GET /api/portfolio/:userId/balance
   - Protected.
   - Returns just the balance.

9. GET /api/portfolio/:userId/positions
   - Protected.
   - Returns positions only.

10. GET /api/trades/:userId
    - Routed to the portfolio-service path, though the implementation is still portfolio-oriented rather than a dedicated trades service.

### 15.2 Internal service-to-service HTTP calls

1. order-service -> portfolio-service /validate
   - Used before reservation.
   - Checks if the user can place the order.

2. order-service -> portfolio-service /reserve
   - Used to place an actual reservation.
   - Uses a transaction to lock rows.

3. order-service -> portfolio-service /release
   - Used if the DB insert fails and the reservation must be released.

4. gateway -> user-service and order-service and portfolio-service
   - Used as a reverse proxy entry point.

### 15.3 WebSocket path

1. GET /ws
   - Upgraded from HTTP to WebSocket.
   - The browser connects to the websocket-service.
   - The service broadcasts match and order events to connected clients.

### 15.4 Why HTTP is used here

HTTP is used for synchronous request/response interactions because the UI needs immediate answers such as login success, order acceptance, or portfolio readback. Kafka is used for asynchronous, decoupled propagation.

---

## 16. Startup sequence

The startup sequence is crucial because it shows the dependency ordering in the system.

### 16.1 Local startup order from docker-compose

1. zookeeper starts.
2. kafka starts once zookeeper is healthy.
3. kafka-init creates topics.
4. postgres starts.
5. db-migrate applies SQL migrations.
6. order-service starts.
7. matching-engine starts.
8. portfolio-service starts.
9. websocket-service starts.
10. user-service starts.
11. api-gateway starts.

### 16.2 ASCII startup diagram

```text
docker-compose up
  └─ Zookeeper
  └─ Kafka
  └─ Kafka Topics Init
  └─ PostgreSQL
  └─ DB migration
  └─ Order Service
  └─ Matching Engine
  └─ Portfolio Service
  └─ Websocket Service
  └─ User Service
  └─ API Gateway
```

### 16.3 What happens when a service starts

- The service connects to PostgreSQL.
- It initializes telemetry and metrics.
- It may start background goroutines.
- It begins consuming Kafka topics or exposing HTTP endpoints.
- The matching-engine also recovers open orders from the DB.

### 16.4 Why this startup order exists

The services need dependencies first:

- Kafka topics must exist before consumers start.
- The database schema must exist before services touch it.
- The matching-engine needs existing order rows to reconstruct the in-memory order book.

---

## 17. Failure scenarios and recovery

A strong architecture is not one that never fails; it is one that fails predictably and recovers safely.

### 17.1 Scenario: Kafka is down when an order is created

What happens:
- The order-service still writes the order and outbox row in PostgreSQL.
- The outbox publisher will retry later.
- The order is not lost even though Kafka was unavailable.

Recovery:
- The outbox worker retries after backoff.
- If retries exceed the threshold, the event is routed to the DLQ topic.

### 17.2 Scenario: the matching-engine crashes

What happens:
- In-memory order books are lost.
- The engine restarts.
- It loads open orders from PostgreSQL and restores the order book state.

Recovery:
- RestoreOrders rebuilds the in-memory state from the database.

### 17.3 Scenario: the portfolio-service crashes after processing a trade

What happens:
- The trade may be re-delivered after restart.
- The portfolio-service uses processed_events to detect duplicates and avoid double-applying.

Recovery:
- It uses the same event ID to check whether it already processed the event.

### 17.4 Scenario: the websocket-service crashes

What happens:
- Clients disconnect.
- The service restarts.
- A new connection is required.

Recovery:
- The service does not need to rebuild the UI state from memory because the frontend reconnects and the next event stream resumes.

### 17.5 Scenario: reservation is placed but DB insert fails

What happens:
- The order-service calls the portfolio-service /reserve.
- The order insert fails later.
- The order-service calls /release to remove the reservation.

Recovery:
- The order-service rolls back the reservation to preserve consistency.

### 17.6 Scenario: cancellation arrives while order is already filled

What happens:
- The matching-engine sees that the order is no longer active and skips the cancel.

Recovery:
- The system reports a no-op or already-final state rather than corrupting the ledger.

---

## 18. Scaling and horizontal growth

The architecture is designed to scale in layers.

### 18.1 What can scale independently

- API gateway: can be horizontally scaled behind a load balancer.
- user-service: can be horizontally scaled.
- order-service: can be horizontally scaled.
- portfolio-service: can be horizontally scaled.
- websocket-service: can be horizontally scaled, though it needs a shared broadcast mechanism in a real production deployment.
- matching-engine: is harder to scale because the order book is in memory and per-process.

### 18.2 What the Helm chart says about scale

The Helm values show:

- api-gateway replicas: 2
- user-service replicas: 2
- order-service replicas: 2
- portfolio-service replicas: 2
- matching-engine replicas: 1
- websocket-service replicas: 1

The matching-engine is intentionally single-replica because the in-memory order book is not partitioned or replicated in this repository.

### 18.3 Scaling limits in this repository

The biggest limitation is not HTTP; it is the matching-engine state model. Since it maintains an in-memory order book, scaling horizontally would require sharding by symbol or partitioning orders across instances. That is not implemented here.

### 18.4 Why the system still scales reasonably well for a prototype

The project can handle enough traffic to demonstrate the architecture because:

- the services are stateless except for their local process memory,
- the database is the authority for durable state,
- Kafka decouples producer and consumer demands,
- the gateway and stateless services can be replicated behind load balancers.

---

## 19. Load balancing, service discovery, and networking

This repository uses simple network topology rather than a full service mesh.

### 19.1 Service discovery in the local environment

In Docker Compose, services discover one another by container name:

- gateway talks to user-service at http://user-service:8084
- gateway talks to order-service at http://order-service:8080
- gateway talks to portfolio-service at http://portfolio-service:8081
- services talk to Kafka at kafka:9092
- services talk to PostgreSQL at postgres:5432

That is effectively a simple service discovery mechanism based on fixed DNS names.

### 19.2 In Kubernetes

The Helm chart uses standard Kubernetes Services and ingress. The ingress routes paths to the correct service. The deployment values also assume a load balancer and a Kubernetes DNS-based service discovery model.

### 19.3 Load balancing strategy

In local Docker Compose, there is no explicit load balancer. In Kubernetes, the gateway and stateless services would be exposed behind Services and likely a load balancer or ingress controller.

### 19.4 Networking choices and tradeoffs

The repository uses simple internal networking and fixed hostnames. That is easy to reason about and good for a prototype, but it is less flexible than a full service mesh or a dynamic discovery platform.

### 19.5 Why this is acceptable here

The repository is teaching architecture, not trying to become a full production service networking stack.

---

## 20. Deployment architecture

The deployment architecture is intentionally simple but realistic.

### 20.1 Local deployment

The local deployment is defined by docker-compose:

- PostgreSQL as the durable state store
- Kafka and Zookeeper as the event backbone
- Each service as its own container
- API gateway exposed at port 8000
- Order service at 8080
- Portfolio service at 8081
- Matching engine at 8082
- Websocket service at 8083
- User service at 8084

### 20.2 Kubernetes deployment

The Helm chart defines a more production-like topology:

- StatefulSets for Postgres, Kafka, and Zookeeper
- Deployments for the stateless services
- Ingress routes for /, /api, and /ws
- HorizontalPodAutoscaler config for the gateway, user, order, and portfolio services
- Observability stack for Prometheus, Grafana, Loki, Jaeger, and Promtail

### 20.3 Why this deployment architecture makes sense

The architecture separates stateful infrastructure from stateless application services. That is a fundamental distributed systems pattern:

- stateful components: PostgreSQL, Kafka, Zookeeper
- stateless services: gateway, auth, order, portfolio, matching, websocket

This separation is critical because stateful services have different scaling, persistence, and recovery concerns than stateless ones.

---

## 21. Why each technology was selected and what alternatives exist

### Go
Why it was chosen:
- the repository is already built around Go services,
- Go is excellent for concurrent network code,
- it compiles to small binaries and works well in containers,
- the service code is simple and explicit.

Alternatives:
- Java, C#, Node.js, Rust.

Tradeoff:
- Go is simple and fast, but the code is more explicit and less expressive than languages with richer abstractions.

### Next.js and React
Why it was chosen:
- quick UI composition,
- good support for client-side state and modern app patterns,
- easy integration with a gateway and websocket endpoint.

Alternatives:
- Vite React, Svelte, plain HTML/JS.

Tradeoff:
- Next.js adds framework complexity that is not necessary for a simple demo, but it improves structure and developer ergonomics.

### PostgreSQL
Why it was chosen:
- transactional safety,
- strong constraints,
- row locking,
- JSONB support,
- good migration tooling.

Alternatives:
- MySQL, MariaDB, CockroachDB, SQLite.

Tradeoff:
- PostgreSQL is powerful but adds operational overhead compared with a lighter embedded database.

### Kafka
Why it was chosen:
- event-driven decoupling,
- durable messaging,
- consumer groups,
- replay and buffering characteristics.

Alternatives:
- RabbitMQ, NATS, direct HTTP fan-out.

Tradeoff:
- Kafka has more operational complexity than RabbitMQ or simple HTTP, but it better fits an event-driven exchange-style architecture.

### JWT
Why it was chosen:
- simple stateless authentication,
- easy to propagate through the gateway and services,
- no need for a distributed session store for this prototype.

Alternatives:
- session cookies, OAuth/OIDC, mTLS-based identity.

Tradeoff:
- JWT is simple but requires careful token handling and revocation strategy.

### Docker Compose and Helm
Why they were chosen:
- Compose is ideal for local development,
- Helm is ideal for Kubernetes deployment.

Alternatives:
- raw scripts, Terraform, Nomad, k3s-only manifests.

Tradeoff:
- Compose and Helm are good fits, but they do not fully replace a real deployment pipeline or full IaC platform.

---

## 22. The most important architectural lesson

The most important thing to understand about this repository is that it teaches distributed systems thinking through the lens of a trading platform.

The core pattern is:

- accept input,
- validate it,
- reserve financial capacity,
- persist state,
- emit an event,
- propagate that event,
- update downstream systems,
- stream the result to the user.

That pattern is far more important than the specific trading domain. The repository is really teaching you how to design a system that is:

- decoupled,
- observable,
- resilient,
- auditable,
- event-driven,
- and safe under concurrency.

If you understand that pattern, you understand the repository.

---

## 23. Final architectural defense points

If you need to defend the design in an interview, these are the strongest points to make:

1. The architecture is intentionally layered around the domain rather than around technical convenience.
   - Orders, portfolio state, matching, and delivery are separate concerns.

2. The project uses transactional integrity plus event-driven propagation.
   - The system does not rely on a single monolithic database transaction spanning all services; instead it uses local transactions and outbox events for reliability.

3. The financial model is explicit and robust.
   - Fixed-point arithmetic prevents rounding bugs; reservation logic prevents double spend.

4. The system is operationally visible.
   - Prometheus, Grafana, Jaeger, and health checks are included.

5. The design acknowledges failure modes.
   - Outbox retries, DLQ-like paths, reconciliation jobs, and idempotency all demonstrate an awareness of real-world distributed systems failure.

6. The current implementation is a solid prototype that can be argued for convincingly.
   - It is simple enough to understand but sophisticated enough to show architectural thinking.
