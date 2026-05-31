FROM golang:alpine AS builder

WORKDIR /src

COPY pkg/ pkg/
COPY services/matching-engine/ services/matching-engine/

RUN cd services/matching-engine && go mod download
RUN cd services/matching-engine && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/matching-engine .

FROM alpine:3.20

RUN adduser -D -H appuser && apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /out/matching-engine /app/matching-engine

USER appuser

EXPOSE 8082

HEALTHCHECK --interval=15s --timeout=3s --retries=3 CMD wget -qO- http://127.0.0.1:8082/healthz >/dev/null || exit 1

CMD ["/app/matching-engine"]
