FROM golang:alpine AS builder

WORKDIR /src

COPY pkg/ pkg/
COPY services/portfolio-service/ services/portfolio-service/

RUN cd services/portfolio-service && go mod download
RUN cd services/portfolio-service && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/portfolio-service .

FROM alpine:3.20

RUN adduser -D -H appuser && apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /out/portfolio-service /app/portfolio-service

USER appuser

EXPOSE 8081

HEALTHCHECK --interval=15s --timeout=3s --retries=3 CMD wget -qO- http://127.0.0.1:8081/healthz >/dev/null || exit 1

CMD ["/app/portfolio-service"]
