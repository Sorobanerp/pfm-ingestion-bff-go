# PFM Ingestion BFF Service — Golang 1.24 (PDF em RAM & NFC-e Scraper)

[![Gitflow](https://img.shields.io/badge/Gitflow-develop%20%7C%20main-indigo)](CONTRIBUTING.md)
[![SemVer](https://img.shields.io/badge/SemVer-2.0.0-blue)](CHANGELOG.md)
[![Docs](https://img.shields.io/badge/PFM%20Docs-V3.0-8CBA51)](https://sorobanerp.github.io/pfm-docs/)

---

## 1. Visão Geral & Papel na Arquitetura

API BFF de alta vazão em **Golang 1.24+**: exposição REST HTTP/2-HTTP/3, parsing de faturas PDF **100% em memória RAM (`Zero-Disk Persistence`)** com `memclr`, Scraper SEFAZ NFC-e com Circuit Breaker e publicação de eventos no Upstash Kafka.

- **Repositório:** `Sorobanerp/pfm-ingestion-bff-go`
- **Vínculo com `pfm-docs`:** `ADR-001, ADR-003, ADR-004, ADR-006 | US-06, US-08`

```mermaid
flowchart LR
    CLIENT["Web / Android"] -->|HTTP/3 REST| BFF["pfm-ingestion-bff-go (RAM Zero-Disk)"]
    BFF -->|gRPC Service Connect| LEDGER["pfm-core-ledger-java"]
    BFF -->|Produce Events| KAFKA[["Upstash Kafka"]]
    BFF -->|Circuit Breaker| SEFAZ["Portais SEFAZ"]
```

---

## 2. Pré-requisitos & Variáveis de Ambiente

### Pré-requisitos
- Go `1.24+`
- `golangci-lint` e `gosec` (opcional local, obrigatório no CI)

### Variáveis de Ambiente (Sem Segredos Hardcoded)
| Variável | Padrão Local | Descrição |
| :--- | :---: | :--- |
| `SERVER_PORT` | `18100` | Porta HTTP do BFF |
| `KAFKA_BROKERS` | `localhost:19092` | Endereço dos brokers Kafka |
| `REDIS_ADDR` | `localhost:16379` | Endereço Redis (Idempotência SHA-256 PDF) |
| `LEDGER_GRPC_ADDR` | `localhost:18210` | Endpoint gRPC do Core Ledger |

> **Segurança (Zero-Trust):** Nunca commite tokens, senhas, chaves AWS ou strings de conexão com credenciais reais. Utilize `.env` local (ignorado no `.gitignore`) ou AWS Secrets Manager / GitHub OIDC.

---

## 3. Como Executar Localmente

```bash
export SERVER_PORT=18100
go run ./cmd/...
# Healthcheck: curl -f http://localhost:18100/health
```

---

## 4. Como Testar e Validar

```bash
go test -v -race -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

---

## 5. Como Provisionar / Validar na Nuvem

1. Fluxo Gitflow: `feat/*` $\rightarrow$ `develop` $\rightarrow$ `release/vX.Y.Z` $\rightarrow$ `main`.
2. Build gera imagem OCI Distroless `linux/arm64` com `readonlyRootFilesystem` e implantação via `pfm-ingestion-bff-infra`.

Consulte [CONTRIBUTING.md](CONTRIBUTING.md) para regras de Conventional Commits e [CHANGELOG.md](CHANGELOG.md) para histórico de versões.
