FROM golang:alpine AS builder

WORKDIR /src

COPY pkg/ pkg/
COPY services/websocket-service/ services/websocket-service/

RUN cd services/websocket-service && go mod download
RUN cd services/websocket-service && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/websocket-service .

FROM alpine:3.20

RUN adduser -D -H appuser && apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /out/websocket-service /app/websocket-service

USER appuser

EXPOSE 8083

HEALTHCHECK --interval=15s --timeout=3s --retries=3 CMD wget -qO- http://127.0.0.1:8083/healthz >/dev/null || exit 1

CMD ["/app/websocket-service"]
