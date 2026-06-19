FROM golang:1.22-alpine AS builder
WORKDIR /app

# Cache module downloads separately from source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o drift-demo ./cmd/demo

# ── Runtime ────────────────────────────────────────────────────────────────
FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/drift-demo /usr/local/bin/drift-demo

EXPOSE 8080

ENTRYPOINT ["drift-demo"]
