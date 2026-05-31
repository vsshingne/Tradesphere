#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BUYER_ID="11111111-1111-1111-1111-111111111111"
SELLER_ID="22222222-2222-2222-2222-222222222222"
SCALE="100000000"

cleanup() {
  docker compose down -v >/dev/null 2>&1 || true
}

wait_http() {
  local url="$1"
  local name="$2"
  for _ in $(seq 1 60); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for $name at $url" >&2
  exit 1
}

wait_kafka() {
  for _ in $(seq 1 60); do
    if docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for kafka" >&2
  exit 1
}

json_field() {
  local field="$1"
  python3 -c 'import json,sys; data=json.load(sys.stdin); print(data[sys.argv[1]])' "$field"
}

psql_value() {
  local sql="$1"
  docker compose exec -T postgres psql -U tradesphere -d tradesphere -t -A -c "$sql" | tr -d '\r'
}

wait_sql_value() {
  local sql="$1"
  local expected="$2"
  local label="$3"
  for _ in $(seq 1 60); do
    local actual
    actual="$(psql_value "$sql")"
    if [[ "$actual" == "$expected" ]]; then
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for $label to become $expected" >&2
  exit 1
}

assert_equals() {
  local actual="$1"
  local expected="$2"
  local label="$3"
  if [[ "$actual" != "$expected" ]]; then
    echo "assertion failed for $label: expected $expected got $actual" >&2
    exit 1
  fi
}

trap cleanup EXIT

docker compose down -v >/dev/null 2>&1 || true
docker compose up -d --build

wait_http "http://localhost:8080/healthz" "order-service"
wait_http "http://localhost:8081/healthz" "portfolio-service"
wait_http "http://localhost:8082/healthz" "matching-engine"
wait_http "http://localhost:8083/healthz" "websocket-service"

docker compose exec -T postgres psql -U tradesphere -d tradesphere -c "
INSERT INTO positions (user_id, symbol, quantity, reserved_quantity)
VALUES ('$SELLER_ID', 'BTC', 2 * $SCALE, 0)
ON CONFLICT (user_id, symbol)
DO UPDATE SET quantity = 2 * $SCALE, reserved_quantity = 0;
" >/dev/null

docker compose stop kafka >/dev/null

SELL_RESPONSE="$(curl -fsS -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d "{\"user_id\":\"$SELLER_ID\",\"symbol\":\"BTC\",\"side\":\"SELL\",\"type\":\"LIMIT\",\"price\":49000,\"quantity\":2}")"
SELL_ORDER_ID="$(printf '%s' "$SELL_RESPONSE" | json_field id)"

wait_sql_value "SELECT COUNT(*) FROM order_outbox WHERE id = '$SELL_ORDER_ID' AND published_at IS NULL" "1" "pending sell order outbox row"
wait_sql_value "SELECT reserved_quantity FROM positions WHERE user_id = '$SELLER_ID' AND symbol = 'BTC'" "$((2 * SCALE))" "seller reserved quantity while kafka is down"

docker compose start kafka >/dev/null
wait_kafka
wait_sql_value "SELECT COUNT(*) FROM order_outbox WHERE id = '$SELL_ORDER_ID' AND published_at IS NOT NULL" "1" "published sell order outbox row"

BUY_RESPONSE="$(curl -fsS -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d "{\"user_id\":\"$BUYER_ID\",\"symbol\":\"BTC\",\"side\":\"BUY\",\"type\":\"LIMIT\",\"price\":50000,\"quantity\":1}")"
BUY_ORDER_ID="$(printf '%s' "$BUY_RESPONSE" | json_field id)"

wait_sql_value "SELECT status FROM orders WHERE id = '$SELL_ORDER_ID'" "PARTIALLY_FILLED" "sell order partial fill"
wait_sql_value "SELECT status FROM orders WHERE id = '$BUY_ORDER_ID'" "FILLED" "buy order fill"
wait_sql_value "SELECT quantity FROM positions WHERE user_id = '$BUYER_ID' AND symbol = 'BTC'" "$SCALE" "buyer BTC quantity"
wait_sql_value "SELECT reserved_quantity FROM positions WHERE user_id = '$SELLER_ID' AND symbol = 'BTC'" "$SCALE" "seller reserved quantity before cancel"

curl -fsS -X POST "http://localhost:8080/orders/$SELL_ORDER_ID/cancel" >/dev/null

wait_sql_value "SELECT status FROM orders WHERE id = '$SELL_ORDER_ID'" "CANCELLED" "sell order cancellation"
wait_sql_value "SELECT reserved_quantity FROM positions WHERE user_id = '$SELLER_ID' AND symbol = 'BTC'" "0" "seller reserved quantity after cancel"

TRADE_PAYLOAD="$(psql_value "SELECT json_build_object(
  'id', id,
  'symbol', symbol,
  'buyer_user_id', buyer_user_id,
  'seller_user_id', seller_user_id,
  'buy_order_id', buy_order_id,
  'sell_order_id', sell_order_id,
  'price', price,
  'quantity', quantity,
  'executed_at', executed_at
)::text FROM trades ORDER BY executed_at ASC LIMIT 1;")"

printf '%s\n' "$TRADE_PAYLOAD" | docker compose exec -T kafka kafka-console-producer \
  --bootstrap-server kafka:9092 \
  --topic trades >/dev/null

sleep 5

assert_equals "$(psql_value "SELECT balance::bigint FROM users WHERE id = '$BUYER_ID'")" "$((51000 * SCALE))" "buyer balance after replay"
assert_equals "$(psql_value "SELECT balance::bigint FROM users WHERE id = '$SELLER_ID'")" "$((149000 * SCALE))" "seller balance after replay"
assert_equals "$(psql_value "SELECT quantity::bigint FROM positions WHERE user_id = '$BUYER_ID' AND symbol = 'BTC'")" "$SCALE" "buyer quantity after replay"
assert_equals "$(psql_value "SELECT quantity::bigint FROM positions WHERE user_id = '$SELLER_ID' AND symbol = 'BTC'")" "$SCALE" "seller quantity after replay"
assert_equals "$(psql_value "SELECT COUNT(*) FROM users WHERE balance < 0 OR reserved_balance < 0")" "0" "non-negative user balances"
assert_equals "$(psql_value "SELECT COUNT(*) FROM positions WHERE quantity < 0 OR reserved_quantity < 0")" "0" "non-negative positions"

echo "TradeSphere end-to-end checks passed"
