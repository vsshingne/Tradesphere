FROM golang:1.23-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -o websocket-service .

EXPOSE 8083

CMD ["./websocket-service"]
