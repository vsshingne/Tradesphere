FROM golang:alpine AS builder

WORKDIR /src

COPY pkg/ pkg/
COPY services/order-service/ services/order-service/

RUN cd services/order-service && go mod download
RUN cd services/order-service && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/order-service .

FROM alpine:3.20

RUN adduser -D -H appuser && apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /out/order-service /app/order-service

USER appuser

EXPOSE 8080

HEALTHCHECK --interval=15s --timeout=3s --retries=3 CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null || exit 1

CMD ["/app/order-service"]
