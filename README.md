# `pfm-ingestion-bff-go`

> **PFM Statement Ingestion & Edge BFF Service — Golang, Hexagonal Architecture, Zero-Disk RAM PDF Unlock (ADR-006) & Distributed Resilience (ADR-004)**

- **Módulo Go**: `github.com/dantas-pfm/pfm-ingestion-bff-go`
- **Stack Homologada**: `shopspring/decimal`, `pdfcpu`, `segmentio/kafka-go`, `google.golang.org/grpc` e OpenTelemetry.
- **Arquitetura**: Ports & Adapters (Hexagonal), desbloqueio de faturas PDF protegidas por senha exclusivamente em memória RAM com zeramento determinístico via `defer` (`Zero-Persistence`), validação de Checksum (`DeclaredTotal == ComputedTotal`), cálculo determinístico de `idempotency_key` (`SHA-256`) e publicação no Kafka particionada por `tenant_id` (`statement.raw.extracted`).

---

## Topologia de Portas Locais & 12-Factor App

| Interface | Porta Host Local | Porta Container / K8s | Variável de Ambiente | Endpoint / Probe |
|---|:---:|:---:|---|---|
| **HTTP API / Health** | `18100` | `8080` | `SERVER_PORT` | `http://localhost:18100/health` |
| **Envoy Gateway Edge** | `18080` | `8080` | — | `http://localhost:18080/healthz` |
| **LocalStack S3/KMS** | `14566` | `4566` | `AWS_ENDPOINT_URL` | `http://localhost:14566` |
| **Kafka Backbone** | `19092` | `9092` | `KAFKA_BROKERS` | `localhost:19092` |
| **Core Ledger (Java)** | `18200` / `18210` | `8080` / `9090` | `CORE_LEDGER_HTTP_URL` / `CORE_LEDGER_GRPC_ADDR` | `localhost:18210` |
| **Forecast Engine (Go)** | `18110` | `9090` | `FORECAST_GRPC_ADDR` | `localhost:18110` |

---

## Como Executar os Testes

```bash
go mod tidy
go test -v -race ./...
```
