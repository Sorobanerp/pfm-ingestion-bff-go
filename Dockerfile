# ============================================================================
# Stage 1: Build Stage (Golang 1.24+)
# ============================================================================
FROM golang:1.24-bookworm AS builder
WORKDIR /workspace

COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /workspace/bin/pfm-ingestion-bff-go ./cmd/server

# ============================================================================
# Stage 2: Runtime Stage (Google Distroless Static Debian 12 Non-Root)
# ============================================================================
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /

COPY --from=builder /workspace/bin/pfm-ingestion-bff-go /pfm-ingestion-bff-go

EXPOSE 8081
USER nonroot:nonroot

ENTRYPOINT ["/pfm-ingestion-bff-go"]
