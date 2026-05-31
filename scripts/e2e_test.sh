#!/bin/bash
set -e

echo "Starting E2E test..."

USER_1="11111111-1111-1111-1111-111111111111"
USER_2="22222222-2222-2222-2222-222222222222"

cd "$(dirname "$0")/.."

echo "Building and starting environment..."
docker compose down -v
docker compose up -d --build

echo "Waiting 30 seconds for services to be healthy..."
sleep 30

echo "Logging in User 1..."
TOKEN_1=$(curl -s -X POST http://localhost:8000/api/auth/login -H "Content-Type: application/json" -d '{"email":"user1@example.com","password":"password"}' | jq -r .access_token)

echo "Logging in User 2..."
TOKEN_2=$(curl -s -X POST http://localhost:8000/api/auth/login -H "Content-Type: application/json" -d '{"email":"user2@example.com","password":"password"}' | jq -r .access_token)

echo "Placing BUY order for User 1..."
curl -s -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN_1" \
  -d "{
  \"user_id\": \"$USER_1\",
  \"symbol\": \"BTC\",
  \"side\": \"BUY\",
  \"type\": \"LIMIT\",
  \"price\": \"50000\",
  \"quantity\": \"1\"
}" | jq .

echo -e "\nPlacing SELL order for User 2..."
curl -s -X POST http://localhost:8000/api/orders \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN_2" \
  -d "{
  \"user_id\": \"$USER_2\",
  \"symbol\": \"BTC\",
  \"side\": \"SELL\",
  \"type\": \"LIMIT\",
  \"price\": \"50000\",
  \"quantity\": \"1\"
}" | jq .

echo -e "\nWaiting 15 seconds for matching and settlement..."
sleep 15

echo "Checking portfolio for User 1 (should have 1 BTC)..."
curl -s "http://localhost:8000/api/portfolio/$USER_1" -H "Authorization: Bearer $TOKEN_1" | jq .

echo "Checking portfolio for User 2 (should have sold 1 BTC)..."
curl -s "http://localhost:8000/api/portfolio/$USER_2" -H "Authorization: Bearer $TOKEN_2" | jq .

echo "E2E test script complete. Run 'docker compose down -v' to clean up."
