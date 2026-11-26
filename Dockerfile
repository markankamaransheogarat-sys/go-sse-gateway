FROM golang:1.26-alpine AS builder
WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /sse-gateway ./cmd/gateway

FROM gcr.io/distroless/static-debian12:nonroot
USER nonroot:nonroot
COPY --from=builder /sse-gateway /sse-gateway

EXPOSE 8080
ENTRYPOINT ["/sse-gateway"]
