# Manual de Homologacao Manual Passo a Passo — PFM Pulse (IDEs + Insomnia + PostgreSQL RLS + Kafka)

Este roteiro guia o **Arquiteto / QA** na execucao manual completa da plataforma **PFM Pulse**, abrindo cada um dos 4 microsservicos em sua respectiva IDE (**IntelliJ IDEA** e **VS Code**), importando a colecao oficial no **Insomnia** e validando **cada uma das 26 requisicoes (Sucesso e Falha)** com evidencias no **PostgreSQL 16 (Row-Level Security)** e no **Redpanda / Kafka Console (`http://localhost:8081`)**.

---

## Arquivos da Suite de Homologacao (`validation-suite/`)

- **Colecao Importavel do Insomnia v4 (26 Requisicoes em 8 Pastas):**
  `/home/dantas/workspace/pfm-platform/pfm-ingestion-bff-go/validation-suite/Insomnia_PFM_Pulse_Local_Homologation.json`
- **Fatura PDF de Amostra (Para Upload em RAM no Teste `03.1`):**
  `/home/dantas/workspace/pfm-platform/pfm-ingestion-bff-go/validation-suite/sample-fatura-itau.pdf`
- **Colecao REST Client (`.http` alternativo para VS Code / IntelliJ):**
  `/home/dantas/workspace/pfm-platform/pfm-ingestion-bff-go/validation-suite/journey-complete.http`

---

## PASSO 1 — Subir a Infraestrutura Base Local (`./init-local-development.sh`)

Abra um terminal na raiz do workspace e execute o script idempotente de inicializacao local:

```bash
cd /home/dantas/workspace/pfm-platform
./init-local-development.sh
```

### O que conferir ao final do Passo 1:
1. Todos os 5 containers Docker devem estar `Up (healthy)`:
   - `postgres-core-ledger` (`localhost:5432`)
   - `pfm-kafka` (`localhost:9092`)
   - `pfm-kafka-ui` (`http://localhost:8081`)
   - `pfm-redis` (`localhost:6379`)
   - `pfm-localstack` (`http://localhost:4566` — S3 + KMS `alias/pfm-master-key`)
2. Abra `http://localhost:8081/topics` no navegador para visualizar os 4 topicos Kafka criados:
   - `statement.raw.extracted`
   - `tx.categorized`
   - `ledger.entry.committed`
   - `tx.dlq`

---

## PASSO 2 — Iniciar Manualmente Cada Projeto nas IDEs (Na Ordem Abaixo)

### 2.1. IntelliJ IDEA — `pfm-core-ledger-java` (Java 25 LTS | gRPC `:9090` | HTTP `:8082`)
1. Abra o **IntelliJ IDEA** e selecione **File -> Open** apontando para:
   `/home/dantas/workspace/pfm-platform/pfm-core-ledger-java`
2. No canto superior direito, selecione a Run Configuration compartilhada:
   **`CoreLedgerApplication (local)`** (carrega `SPRING_PROFILES_ACTIVE=local`).
3. Clique em **Run (`Shift+F10`)** ou **Debug (`Shift+F9`)**.
4. **Conferencia no Console do IntelliJ:**
   - Verifique que o **Flyway** aplicou ou validou as migracoes `V1`, `V2`, `V3` (tabelas `ledger_entries` com `ENABLE ROW LEVEL SECURITY` e `FORCE ROW LEVEL SECURITY`);
   - Verifique a mensagem `gRPC Server started, listening on port 9090` e Tomcat HTTP na porta `8082`.

---

### 2.2. VS Code (Janela 1) — `pfm-forecast-engine-go` (Go | gRPC `:9091`)
1. Abra uma janela do **VS Code** na pasta:
   `/home/dantas/workspace/pfm-platform/pfm-forecast-engine-go`
2. Abra a aba **Run and Debug (`Ctrl+Shift+D`)** e selecione **`Launch Forecast Engine Go (local :9091)`**.
3. Pressione **`F5`**.
4. **Conferencia no Debug Console do VS Code:**
   - `pfm-forecast-engine-go listening on :9091 (HTTP/2 h2c & gRPC Server Reflection enabled)`

---

### 2.3. VS Code (Janela 2) — `pfm-ingestion-bff-go` (Go | HTTP/2 `:8080` + Swagger UI)
1. Abra uma segunda janela do **VS Code** na pasta:
   `/home/dantas/workspace/pfm-platform/pfm-ingestion-bff-go`
2. Abra a aba **Run and Debug (`Ctrl+Shift+D`)** e selecione **`Launch Ingestion BFF Go (local :8080)`**.
3. Pressione **`F5`**.
4. **Conferencia no Debug Console do VS Code:**
   - `pfm-ingestion-bff-go listening on :8080 (HTTP/2, Swagger UI at http://localhost:8080/swagger/index.html)`

---

### 2.4. VS Code (Janela 3) — `pfm-ai-worker-python` (Python 3.12 | Kafka Consumer + Health `:18400`)
1. Abra uma terceira janela do **VS Code** na pasta:
   `/home/dantas/workspace/pfm-platform/pfm-ai-worker-python`
2. Confirme que o interpretador Python selecionado no rodape do VS Code e **`.venv/bin/python`**.
3. Abra a aba **Run and Debug (`Ctrl+Shift+D`)** e selecione **`Run PFM AI Worker Python (APP_ENV=local)`** (ou pressione **`F5`**).
4. **Conferencia no Terminal/Debug Console do VS Code:**
   - `INFO pfm-ai-worker: pfm-ai-worker-python started and listening on statement.raw.extracted`

---

## PASSO 3 — Importar a Colecao no Insomnia e Selecionar o Ambiente

1. Abra o **Insomnia**.
2. Clique em **Import -> Select File** e escolha o arquivo:
   `/home/dantas/workspace/pfm-platform/pfm-ingestion-bff-go/validation-suite/Insomnia_PFM_Pulse_Local_Homologation.json`
3. Abra a colecao **`PFM Pulse — Colecao Oficial de Homologacao Local E2E`**.
4. No seletor de ambiente (canto superior esquerdo), selecione o sub-ambiente verde:
   **`Local Dev - PFM Pulse`**
   *(Todas as variaveis `base_url`, `tenant_a_id`, `tenant_b_id`, `admin_token`, `token_tenant_a`, `token_tenant_b` e `token_viewer_a` ja estao pre-configuradas e prontas para clique imediato!).*

---

## PASSO 4 — Roteiro de Execucao Requisicao por Requisicao (26 Cenarios nas 8 Pastas)

Execute cada item na ordem abaixo clicando em **Send** (ou **Invoke** nas chamadas gRPC):

| # | Pasta / Requisicao no Insomnia | Tipo | Status Esperado | Payload / Retorno Esperado & Onde Conferir Evidencia |
| :-: | :--- | :---: | :---: | :--- |
| **01.1** | `01.1 [SUCESSO] Criar Tenant A (Familia Dantas) - SaaS Admin` | `HTTP POST` | **`201 Created`** | Retorna `"tenant_id": "11111111-1111-1111-1111-111111111111"`, `"plan": "FAMILY_PLUS"`, `"rls_policy": "ENABLED_FAIL_CLOSED"`. |
| **01.2** | `01.2 [SUCESSO] Criar Tenant B (Familia Visitante) - SaaS Admin` | `HTTP POST` | **`201 Created`** | Retorna `"tenant_id": "22222222-2222-2222-2222-222222222222"`, `"plan": "INDIVIDUAL_PRO"`. |
| **01.3** | `01.3 [SUCESSO] Gerar / Validar Sessao & Token JWT Local` | `HTTP POST` | **`200 OK`** | Retorna `"access_token"` JWT assinado localmente com claims `sub`, `tenant_id` e `"role": "OWNER"`. |
| **01.4** | `01.4 [FALHA - 401] Acesso sem Token / Sem X-Tenant-ID` | `HTTP GET` | **`401 Unauthorized`** | Retorna `"error": "missing or invalid X-Tenant-ID UUID header (Zero-Trust ADR-006)"`. |
| **01.5** | `01.5 [FALHA - 403] Papel VIEWER tentando criar transacao` | `HTTP POST` | **`403 Forbidden`** | Com `Bearer dev-viewer-tenant-a`, retorna `"error": "forbidden: VIEWER role cannot create or mutate ledger transactions"`. |
| **02.1** | `02.1 [SUCESSO] Cadastrar Cartao de Credito (Final 5285 - Limite R$ 15.000,00)` | `HTTP POST` | **`201 Created`** | Cria o cartao `"card-5285-11111111"` com `"credit_limit": "15000.00"` e `"available_limit": "15000.00"`. |
| **02.2** | `02.2 [SUCESSO] Criar Lancamento a Vista (Supermercado Queiroz - R$ 189,40)` | `HTTP POST` | **`201 Created`** | Cria a transacao `"id": "99999999-1111-4111-8111-000000000001"` de `"189.40"` (`BRL`) e reduz o limite disponivel do cartao. |
| **02.3** | `02.3 [SUCESSO] Criar Compra Parcelada 10x (R$ 3.500,00 -> 01/10 a 10/10)` | `HTTP POST` | **`201 Created`** | Gera o array `"installment_schedule"` com 10 parcelas de `"350.00"` (`01/10` em `2026-11` ate `10/10` em `2027-08` — `RN02`). |
| **02.4** | `02.4 [SUCESSO] Listar Extrato & Conferir Isolamento RLS (Tenant A ve tudo)` | `HTTP GET` | **`200 OK`** | Retorna `"count": 2` (ou mais) contendo todas as compras da `Familia Dantas` (`Tenant A`). |
| **02.5** | `02.5 [FALHA - RLS] Tenant B tenta listar transacoes do Tenant A` | `HTTP GET` | **`200 OK` (`0 rows`)** | Retorna `"tenant_id": "22222222-..."`, `"count": 0`, `"items": []`. **Zero vazamento entre tenants!** |
| **02.6** | `02.6 [SUCESSO] Atualizar Categoria / Descricao da Transacao (PATCH/PUT)` | `HTTP PATCH` | **`200 OK`** | Atualiza a transacao `0001` para `"category_code": "ALIMENTACAO :: FEIRA_E_HORTIFRUTI"`. |
| **02.7** | `02.7 [SUCESSO] Registrar Estorno na Categoria Original (RN03)` | `HTTP POST` | **`201 Created`** | Cria lancamento imutavel `"entry_type": "REFUND_CREDIT"`, `"amount": "-189.40"` na mesma categoria original e recompoe o limite do cartao. |
| **02.8** | `02.8 [FALHA - 400/422] Compra com Valor Negativo ou Parcelas = 0` | `HTTP POST` | **`400 Bad Request`** | Rejeita o payload com `"error": "amount must be a strictly positive decimal string (RN06)"`. |
| **03.1** | `03.1 [SUCESSO] Upload Multipart de Fatura PDF (Processamento 100% RAM)` | `HTTP POST` | **`200 OK`** | Processa a fatura Itau (`R$ 750,90`) em RAM (`defer clear`), valida `"ChecksumValid": true`, `"checksum_diff": "0.00"`, arquiva cifrado no S3 LocalStack e publica no topico Kafka `statement.raw.extracted` (visivel em `http://localhost:8081`). *Dica: voce tambem pode anexar `sample-fatura-itau.pdf` via Multipart.* |
| **03.2** | `03.2 [FALHA - 400/415] Upload de Arquivo Invalido / Corrompido` | `HTTP POST` | **`415 Unsupported Media Type`** | Rejeita o binario `.exe` (`MZ`) imediatamente com `"error": "unsupported media type: only PDF / financial statement streams are allowed (ADR-006)"`. |
| **04.1** | `04.1 [SUCESSO] Listar Itens Pendentes na Inbox de Categorizacao IA` | `HTTP GET` | **`200 OK`** | Lista itens classificados pela IA com `"uncategorized_ratio_pct": "2.40"` (abaixo da meta `"target_max_others_pct": "5.00"`). |
| **04.2** | `04.2 [SUCESSO] Aprovar / Reclassificar Categoria de Transacao` | `HTTP POST` | **`200 OK`** | Retorna `"status": "CONFIRMED_AND_COMMITTED"` e `"learn_rule": true`. |
| **05.1** | `05.1 [SUCESSO] Submeter NFC-e #1 (Azeite 500ml - R$ 39,90)` | `HTTP POST` | **`200 OK`** | Registra a chave SEFAZ de 44 digitos `#1` e grava o preco base de `R$ 39,90` para o `EAN 7891234567890`. |
| **05.2** | `05.2 [SUCESSO] Submeter NFC-e #2 Mesmo EAN (Azeite 500ml - R$ 47,90 -> Alerta +20%)` | `HTTP POST` | **`200 OK`** | Detecta aumento de `R$ 39,90` para `R$ 47,90` no mesmo `EAN 7891234567890` e retorna `"inflation_alerts"` com `"inflation_pct": "+20.05%"`. |
| **05.3** | `05.3 [FALHA - 409] Reenviar Mesma Chave NFC-e (Bloqueio Idempotencia)` | `HTTP POST` | **`409 Conflict`** | Rejeita a mesma chave NFC-e de 44 digitos com `"error": "duplicate SEFAZ NFC-e 44-digit access_key already ingested for tenant"`. |
| **06.1** | `06.1 [SUCESSO - HTTP] Simular Amortizacao Extraordinaria SAC (R$ 20.000,00)` | `HTTP POST` | **`200 OK`** | Calcula reducao de prazo (`REDUCE_TERM`) e economia de juros (`interest_savings_amount`) em `shopspring/decimal`. |
| **06.2** | `06.2 [SUCESSO - gRPC] Chamada Direta gRPC ForecastService/SimulateAmortization (:9091)` | `gRPC Unary` | **`0 OK`** | No Insomnia, clique em **Click to Select Method -> Refresh Reflection (`localhost:9091`) -> `/pfm.v1.ForecastService/SimulateContractAmortization`** e clique em **Invoke**. Retorna a tabela de amortizacao SAC calculada em Go. |
| **06.3** | `06.3 [FALHA - 400] Amortizacao Maior que Saldo Devedor` | `HTTP POST` | **`400 Bad Request`** | Rejeita amortizacao extra de `R$ 400.000,00` sobre saldo de `R$ 320.000,00` com `"error": "extra_amortization_amount must be strictly less than outstanding_balance"`. |
| **07.1** | `07.1 [SUCESSO] Resumo Executivo TODAY` | `HTTP GET` | **`200 OK`** | Retorna `"horizon": "TODAY"` e o valor atualizado de `"free_to_spend_today"` em `BRL`. |
| **07.2** | `07.2 [SUCESSO] Resumo Executivo WEEK` | `HTTP GET` | **`200 OK`** | Retorna `"horizon": "WEEK"` e o valor atualizado de `"free_to_spend_week"` em `BRL`. |
| **07.3** | `07.3 [SUCESSO] Resumo Executivo MONTH` | `HTTP GET` | **`200 OK`** | Retorna `"horizon": "MONTH"`, `"free_to_spend_month"`, `"custom_transactions_competence_net"` e `"active_cards_count"`. |
| **08.1** | `08.1 [SUCESSO] Configurar Teto Orcamentario Mensal por Categoria` | `HTTP POST` | **`200 OK`** | Configura o teto de `R$ 1.500,00` (`threshold_percentage: 80`) para `Alimentacao Fora`. |
| **08.2** | `08.2 [SUCESSO] Solicitar Exportacao de Dados Pessoais (LGPD Art. 18)` | `HTTP POST` | **`202 Accepted`** | Agenda a exportacao portavel em formato `JSON` para o titular. |
| **08.3** | `08.3 [SUCESSO - gRPC] Consulta Direta Double-Entry no Core Ledger (:9090)` | `gRPC Unary` | **`0 OK`** | No Insomnia, clique em **Refresh Reflection (`localhost:9090`) -> `/pfm.v1.CoreLedgerService/GetTenantFinancialSummary`** e clique em **Invoke**. Retorna o resumo do Core Ledger Java 25 LTS. |

---

## PASSO 5 — Comprovacao Direta no PostgreSQL 16 (Row-Level Security) e Kafka UI

Apos rodar os testes no Insomnia, voce pode auditar diretamente o isolamento no banco e os eventos no Kafka:

1. **Conferir Isolamento RLS no PostgreSQL 16 via Terminal:**
   ```bash
   docker exec -it postgres-core-ledger psql -U pfm -d pfm_ledger -c \
     "BEGIN; SET LOCAL app.current_tenant_id = '22222222-2222-2222-2222-222222222222'; SELECT count(*) FROM ledger_entries WHERE tenant_id = '11111111-1111-1111-1111-111111111111'; COMMIT;"
   ```
   *(Resultado obrigatorio: `count = 0`)*.

2. **Conferir Mensagens Publicadas no Kafka / Redpanda Console:**
   - Acesse `http://localhost:8081/topics/statement.raw.extracted` no navegador para inspecionar o evento gerado pelo upload da fatura PDF no teste **`03.1`**.
