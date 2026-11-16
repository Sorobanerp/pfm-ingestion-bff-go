# `pfm-ingestion-bff-go`

> **PFM Statement Ingestion & Edge BFF Service — Golang, Hexagonal Architecture, Zero-Disk RAM PDF Unlock (ADR-006) & Distributed Resilience (ADR-004)**

- **Módulo Go**: `github.com/dantas-pfm/pfm-ingestion-bff-go`
- **Stack Homologada**: `shopspring/decimal`, `pdfcpu`, `segmentio/kafka-go`, `google.golang.org/grpc` e OpenTelemetry.
- **Arquitetura**: Ports & Adapters (Hexagonal), desbloqueio de faturas PDF protegidas por senha exclusivamente em memória RAM com zeramento determinístico via `defer` (`Zero-Persistence`), validação de Checksum (`DeclaredTotal == ComputedTotal`), cálculo determinístico de `idempotency_key` (`SHA-256`) e publicação no Kafka particionada por `tenant_id` (`statement.raw.extracted`).

---

## Como Executar os Testes

```bash
go mod tidy
go test -v -race ./...
```
