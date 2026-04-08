FROM golang:1.23-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o order-service .

EXPOSE 8080

CMD ["./order-service"]
