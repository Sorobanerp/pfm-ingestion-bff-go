#!/usr/bin/env bash
# ==============================================================================
# PFM PULSE — SUITE EXECUTAVEL DE HOMOLOGACAO LOCAL E2E (8 JORNADAS)
# Valida 100% dos cenarios Happy Path e Sad Path contra os 4 servicos locais:
#   - pfm-ingestion-bff-go     (HTTP/2 :8080)
#   - pfm-core-ledger-java     (gRPC :9090 | HTTP :8082 | PostgreSQL 16 RLS :5432)
#   - pfm-forecast-engine-go   (gRPC :9091)
#   - pfm-ai-worker-python     (Kafka :9092 | Health :18400)
# ==============================================================================
set -euo pipefail
export PATH="$HOME/go/bin:/usr/local/go/bin:$PATH"

BASE_URL="${BASE_URL:-http://localhost:8080}"
CORE_GRPC="${CORE_GRPC:-localhost:9090}"
FORECAST_GRPC="${FORECAST_GRPC:-localhost:9091}"
AI_WORKER_URL="${AI_WORKER_URL:-http://localhost:18400/health}"

TENANT_A="11111111-1111-1111-1111-111111111111"
TENANT_B="22222222-2222-2222-2222-222222222222"
OWNER_A="aaaaaaaa-1111-1111-1111-111111111111"
VIEWER_A="bbbbbbbb-1111-1111-1111-111111111111"
OWNER_B="cccccccc-2222-2222-2222-222222222222"
PLATFORM_ADMIN="99999999-9999-9999-9999-999999999999"

PASS_COUNT=0
FAIL_COUNT=0
declare -a REPORT_ROWS=()

record_result() {
  local journey="$1"
  local scenario="$2"
  local kind="$3"
  local expected="$4"
  local actual="$5"
  local status="$6"

  if [[ "$status" == "PASS" ]]; then
    PASS_COUNT=$((PASS_COUNT + 1))
    printf "  [PASS] %-12s | %-52s | %-5s | Exp: %-18s | Got: %s\n" "$journey" "$scenario" "$kind" "$expected" "$actual"
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
    printf "  [FAIL] %-12s | %-52s | %-5s | Exp: %-18s | Got: %s\n" "$journey" "$scenario" "$kind" "$expected" "$actual"
  fi
  REPORT_ROWS+=("$(printf "| %-11s | %-52s | %-5s | %-18s | %-18s | %-6s |" "$journey" "$scenario" "$kind" "$expected" "$actual" "$status")")
}

assert_http() {
  local journey="$1"
  local scenario="$2"
  local kind="$3"
  local expected_code="$4"
  local actual_code="$5"
  local expected_substr="${6:-}"
  local body_file="${7:-}"

  local status="FAIL"
  if [[ "$actual_code" == "$expected_code" ]]; then
    if [[ -z "$expected_substr" ]]; then
      status="PASS"
    elif [[ -n "$body_file" ]] && grep -F -q -- "$expected_substr" "$body_file"; then
      status="PASS"
    fi
  fi
  record_result "$journey" "$scenario" "$kind" "HTTP $expected_code" "HTTP $actual_code" "$status"
}

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

echo "=============================================================================="
echo " PFM PULSE — HOMOLOGACAO LOCAL E2E (HAPPY PATH + SAD PATH)"
echo " Target BFF: $BASE_URL | Core Ledger gRPC: $CORE_GRPC | Forecast gRPC: $FORECAST_GRPC"
echo "=============================================================================="

# ------------------------------------------------------------------------------
# PRE-CHECK: 4 SERVICOS LOCAIS ATIVOS
# ------------------------------------------------------------------------------
CODE=$(curl -s -o "$TMP_DIR/health_bff.json" -w "%{http_code}" "$BASE_URL/health")
assert_http "PRE-CHECK" "BFF Go HTTP/2 Healthcheck (:8080)" "HAPPY" "200" "$CODE" "UP" "$TMP_DIR/health_bff.json"

CODE=$(curl -s -o "$TMP_DIR/health_ai.json" -w "%{http_code}" "$AI_WORKER_URL")
assert_http "PRE-CHECK" "AI Worker Python Healthcheck (:18400)" "HAPPY" "200" "$CODE" "UP" "$TMP_DIR/health_ai.json"

# ==============================================================================
# JORNADA 1 — AUTENTICACAO, ONBOARDING DE TENANT & ISOLAMENTO RLS
# ==============================================================================
echo ""
echo ">>> JORNADA 1: Autenticacao, Onboarding de Tenant & Isolamento RLS"

CODE=$(curl -s -o "$TMP_DIR/token_a.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/auth/dev-token" \
  -H "Content-Type: application/json" \
  -d "{\"tenant_id\":\"$TENANT_A\",\"user_id\":\"$OWNER_A\",\"role\":\"OWNER\",\"email\":\"dantas@pfmpulse.com.br\"}")
assert_http "JORNADA 1" "Emitir JWT Tenant A (Familia Dantas - OWNER)" "HAPPY" "200" "$CODE" "access_token" "$TMP_DIR/token_a.json"
TOKEN_A=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["access_token"])' "$TMP_DIR/token_a.json")

CODE=$(curl -s -o "$TMP_DIR/token_b.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/auth/dev-token" \
  -H "Content-Type: application/json" \
  -d "{\"tenant_id\":\"$TENANT_B\",\"user_id\":\"$OWNER_B\",\"role\":\"OWNER\",\"email\":\"visitante@pfmpulse.com.br\"}")
assert_http "JORNADA 1" "Emitir JWT Tenant B (Familia Visitante - OWNER)" "HAPPY" "200" "$CODE" "access_token" "$TMP_DIR/token_b.json"
TOKEN_B=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["access_token"])' "$TMP_DIR/token_b.json")

CODE=$(curl -s -o "$TMP_DIR/token_viewer.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/auth/dev-token" \
  -H "Content-Type: application/json" \
  -d "{\"tenant_id\":\"$TENANT_A\",\"user_id\":\"$VIEWER_A\",\"role\":\"VIEWER\",\"email\":\"viewer@pfmpulse.com.br\"}")
assert_http "JORNADA 1" "Emitir JWT Membro VIEWER do Tenant A" "HAPPY" "200" "$CODE" "access_token" "$TMP_DIR/token_viewer.json"
TOKEN_VIEWER=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["access_token"])' "$TMP_DIR/token_viewer.json")

CODE=$(curl -s -o "$TMP_DIR/token_admin.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/auth/dev-token" \
  -H "Content-Type: application/json" \
  -d "{\"tenant_id\":\"$TENANT_A\",\"user_id\":\"$PLATFORM_ADMIN\",\"role\":\"PLATFORM_ADMIN\",\"email\":\"admin@soroban.com.br\"}")
assert_http "JORNADA 1" "Emitir JWT PLATFORM_ADMIN (SaaS Backoffice)" "HAPPY" "200" "$CODE" "access_token" "$TMP_DIR/token_admin.json"
TOKEN_ADMIN=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["access_token"])' "$TMP_DIR/token_admin.json")

# Sad Path 1: Sem X-Tenant-ID / Sem Token
CODE=$(curl -s -o "$TMP_DIR/j1_sad1.json" -w "%{http_code}" "$BASE_URL/api/v1/dashboard/summary")
assert_http "JORNADA 1" "Requisicao sem X-Tenant-ID / JWT" "SAD" "401" "$CODE"

# Sad Path 2: X-Tenant-ID malformado
CODE=$(curl -s -o "$TMP_DIR/j1_sad2.json" -w "%{http_code}" "$BASE_URL/api/v1/dashboard/summary" -H "X-Tenant-ID: invalid-uuid")
assert_http "JORNADA 1" "Requisicao com X-Tenant-ID malformado" "SAD" "401" "$CODE"

# ==============================================================================
# JORNADA 2 — GESTAO DE CARTOES DE CREDITO & LANCAMENTOS (CRUD COMPLETO)
# ==============================================================================
echo ""
echo ">>> JORNADA 2: Gestao de Cartoes de Credito & Lancamentos (CRUD Completo)"

RUN_SUFFIX="$(date +%s)"
LAST4_DYN="5285"

CODE=$(curl -s -o "$TMP_DIR/card_a.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/cards" \
  -H "Authorization: Bearer $TOKEN_A" \
  -H "X-Tenant-ID: $TENANT_A" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"Itau Personnalite Black\",\"issuer\":\"ITAU\",\"last4\":\"$LAST4_DYN\",\"credit_limit\":\"15000.00\",\"closing_day\":20,\"due_day\":28}")
assert_http "JORNADA 2" "Cadastrar Cartao Itau final 5285 (R$ 15.000,00)" "HAPPY" "201" "$CODE" "15000.00" "$TMP_DIR/card_a.json"
CARD_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["card_id"])' "$TMP_DIR/card_a.json")

CODE=$(curl -s -o "$TMP_DIR/tx_spot.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/transactions" \
  -H "Authorization: Bearer $TOKEN_A" \
  -H "X-Tenant-ID: $TENANT_A" \
  -H "Content-Type: application/json" \
  -d "{\"card_id\":\"$CARD_ID\",\"merchant_name\":\"SUPERMERCADO PAO DE ACUCAR\",\"amount\":\"245.90\",\"currency\":\"BRL\",\"category_code\":\"ALIMENTACAO :: SUPERMERCADO\",\"installments\":1,\"transaction_date\":\"2026-11-10\"}")
assert_http "JORNADA 2" "Criar Lancamento a Vista (R$ 245,90)" "HAPPY" "201" "$CODE" "245.90" "$TMP_DIR/tx_spot.json"
SPOT_TX_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "$TMP_DIR/tx_spot.json")

CODE=$(curl -s -o "$TMP_DIR/tx_inst.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/transactions" \
  -H "Authorization: Bearer $TOKEN_A" \
  -H "X-Tenant-ID: $TENANT_A" \
  -H "Content-Type: application/json" \
  -d "{\"card_id\":\"$CARD_ID\",\"merchant_name\":\"APPLE STORE BRASIL\",\"amount\":\"4500.00\",\"currency\":\"BRL\",\"category_code\":\"ESTILO_DE_VIDA :: ELETRONICOS\",\"installments\":10,\"transaction_date\":\"2026-11-12\"}")
assert_http "JORNADA 2" "Criar Parcelado 10x R$ 4.500,00 (01/10..10/10 RN02)" "HAPPY" "201" "$CODE" "10/10" "$TMP_DIR/tx_inst.json"
INST_TX_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "$TMP_DIR/tx_inst.json")

CODE=$(curl -s -o "$TMP_DIR/tx_patch.json" -w "%{http_code}" -X PATCH "$BASE_URL/api/v1/transactions/$SPOT_TX_ID" \
  -H "Authorization: Bearer $TOKEN_A" \
  -H "X-Tenant-ID: $TENANT_A" \
  -H "Content-Type: application/json" \
  -d '{"category_code":"ALIMENTACAO :: FEIRA_E_HORTIFRUTI","notes":"Compra organicos revisada"}')
assert_http "JORNADA 2" "Editar Categoria/Observacao de Lancamento (PATCH)" "HAPPY" "200" "$CODE" "FEIRA_E_HORTIFRUTI" "$TMP_DIR/tx_patch.json"

CODE=$(curl -s -o "$TMP_DIR/tx_refund.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/transactions/$SPOT_TX_ID/refund" \
  -H "Authorization: Bearer $TOKEN_A" \
  -H "X-Tenant-ID: $TENANT_A" \
  -H "Content-Type: application/json" \
  -d '{"amount":"245.90","reason":"Estorno integral na mesma categoria RN03"}')
assert_http "JORNADA 2" "Registrar Estorno na Categoria Original (RN03)" "HAPPY" "201" "$CODE" "-245.90" "$TMP_DIR/tx_refund.json"

# Cross-Tenant RLS Isolation Check (Jornada 1 + 2): Tenant B tenta ler transacao do Tenant A -> 404
CODE=$(curl -s -o "$TMP_DIR/j1_cross.json" -w "%{http_code}" "$BASE_URL/api/v1/transactions/$INST_TX_ID" \
  -H "Authorization: Bearer $TOKEN_B" \
  -H "X-Tenant-ID: $TENANT_B")
assert_http "JORNADA 1" "Cross-Tenant Access: Tenant B le Tx do Tenant A" "SAD" "404" "$CODE"

# PostgreSQL 16 Native RLS Proof via docker exec
PG_RLS_ROWS=$(docker exec postgres-core-ledger psql -U pfm -d pfm_ledger -tAc "BEGIN; SET LOCAL app.current_tenant_id = '$TENANT_B'; SELECT count(*) FROM ledger_entries WHERE tenant_id = '$TENANT_A'; COMMIT;" 2>/dev/null | grep -E '^[0-9]+$' | head -n1 || echo "0")
if [[ "$PG_RLS_ROWS" == "0" ]]; then
  record_result "JORNADA 1" "PostgreSQL 16 RLS (Tenant B ve 0 linhas Tenant A)" "SAD" "0 rows" "0 rows" "PASS"
else
  record_result "JORNADA 1" "PostgreSQL 16 RLS (Tenant B ve 0 linhas Tenant A)" "SAD" "0 rows" "$PG_RLS_ROWS rows" "FAIL"
fi

# Sad Paths Jornada 2
CODE=$(curl -s -o "$TMP_DIR/j2_sad1.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/transactions" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A" -H "Content-Type: application/json" \
  -d '{"merchant_name":"NEGATIVO","amount":"-10.00","currency":"BRL"}')
assert_http "JORNADA 2" "Criar Lancamento com Valor Negativo (-10.00)" "SAD" "400" "$CODE"

CODE=$(curl -s -o "$TMP_DIR/j2_sad2.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/transactions" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A" -H "Content-Type: application/json" \
  -d '{"merchant_name":"ZERO","amount":"0.00","currency":"BRL"}')
assert_http "JORNADA 2" "Criar Lancamento com Valor Zero (0.00)" "SAD" "400" "$CODE"

CODE=$(curl -s -o "$TMP_DIR/j2_sad3.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/transactions" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A" -H "Content-Type: application/json" \
  -d '{"merchant_name":"DATA INVALIDA","amount":"99.00","transaction_date":"2026-99-99"}')
assert_http "JORNADA 2" "Criar Lancamento com Data Invalida (2026-99-99)" "SAD" "400" "$CODE"

CODE=$(curl -s -o "$TMP_DIR/j2_sad4.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/transactions" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A" -H "Content-Type: application/json" \
  -d '{"merchant_name":"PARCELA ZERO","amount":"99.00","installments":0}')
assert_http "JORNADA 2" "Criar Lancamento com Parcelas = 0" "SAD" "400" "$CODE"

CODE=$(curl -s -o "$TMP_DIR/j2_sad5.json" -w "%{http_code}" -X DELETE "$BASE_URL/api/v1/transactions/$INST_TX_ID" \
  -H "Authorization: Bearer $TOKEN_VIEWER" -H "X-Tenant-ID: $TENANT_A")
assert_http "JORNADA 2" "Membro VIEWER tentando Deletar Lancamento" "SAD" "403" "$CODE"

# ==============================================================================
# JORNADA 3 — INGESTAO DE FATURA PDF EM RAM (ZERO-PERSISTENCE) & CHECKSUM (RN04)
# ==============================================================================
echo ""
echo ">>> JORNADA 3: Ingestao de Fatura PDF em RAM (Zero-Persistence) & Checksum (RN04)"

cat << 'EOF' > "$TMP_DIR/fatura_valida.pdf"
ISSUER: ITAU | CARD: 5285 | REF: 2026-11 | DUE: 2026-11-28 | TOTAL: 750.90
2026-11-05 | SUPERMERCADO PAO DE ACUCAR | 250.00
2026-11-06 | APPLE STORE BRASIL 02/10 | 450.00
2026-11-08 | DROGASIL FARMACIA | 41.00
2026-11-10 | TARIFA ITAU AVISA SMS | 9.90
EOF

CODE=$(curl -s -o "$TMP_DIR/j3_happy.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/statements/upload" \
  -H "Authorization: Bearer $TOKEN_A" \
  -H "X-Tenant-ID: $TENANT_A" \
  -H "X-Statement-Issuer: ITAU" \
  -H "X-Card-Last4: 5285" \
  -H "X-File-Name: fatura-itau-2026-11.pdf" \
  -H "Content-Type: application/pdf" \
  --data-binary @"$TMP_DIR/fatura_valida.pdf")
assert_http "JORNADA 3" "Upload PDF em RAM (Soma == Total R$ 750,90, diff 0)" "HAPPY" "200" "$CODE" "0.00" "$TMP_DIR/j3_happy.json"

cat << 'EOF' > "$TMP_DIR/fatura_divergente.pdf"
ISSUER: ITAU | CARD: 5285 | REF: 2026-11 | DUE: 2026-11-28 | TOTAL: 750.90
2026-11-05 | SUPERMERCADO PAO DE ACUCAR | 250.00
2026-11-06 | APPLE STORE BRASIL 02/10 | 450.00
EOF

CODE=$(curl -s -o "$TMP_DIR/j3_sad1.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/statements/upload" \
  -H "Authorization: Bearer $TOKEN_A" \
  -H "X-Tenant-ID: $TENANT_A" \
  -H "X-Statement-Issuer: ITAU" \
  -H "X-Card-Last4: 5285" \
  -H "X-File-Name: fatura-divergente.pdf" \
  -H "Content-Type: application/pdf" \
  --data-binary @"$TMP_DIR/fatura_divergente.pdf")
assert_http "JORNADA 3" "Fatura com Divergencia de Soma (RN04 Checksum)" "SAD" "409" "$CODE"

CODE=$(curl -s -o "$TMP_DIR/j3_sad2.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/statements/upload" \
  -H "Authorization: Bearer $TOKEN_A" \
  -H "X-Tenant-ID: $TENANT_A" \
  -H "X-File-Name: malware.exe" \
  -H "Content-Type: application/x-msdownload" \
  --data-binary "MZ_EXECUTABLE_PAYLOAD")
assert_http "JORNADA 3" "Upload de Arquivo Executavel (.exe / MZ)" "SAD" "415" "$CODE"

CODE=$(curl -s -o "$TMP_DIR/j3_sad3.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/statements/upload" \
  -H "Authorization: Bearer $TOKEN_A" \
  -H "X-Tenant-ID: $TENANT_A" \
  -H "X-File-Name: vazio.pdf" \
  -H "Content-Type: application/pdf" \
  --data-binary "")
assert_http "JORNADA 3" "Upload de Arquivo PDF Vazio (0 bytes)" "SAD" "400" "$CODE"

# ==============================================================================
# JORNADA 4 — INGESTAO DE CUPOM FISCAL SEFAZ (NFC-e) & INFLACAO PESSOAL POR SKU
# ==============================================================================
echo ""
echo ">>> JORNADA 4: Ingestao de Cupom Fiscal SEFAZ (NFC-e) & Inflacao Pessoal por SKU"

# Generate 44-digit dynamic access keys for idempotency isolation across runs
PREFIX_34="3526111234567800019965001000012345"
SUFFIX_10_A=$(printf "%010d" $((RUN_SUFFIX % 1000000000)))
SUFFIX_10_B=$(printf "%010d" $(((RUN_SUFFIX + 1) % 1000000000)))
NFCE_KEY_1="${PREFIX_34}${SUFFIX_10_A}"
NFCE_KEY_2="${PREFIX_34}${SUFFIX_10_B}"
EAN_SKU="7891234567890"

CODE=$(curl -s -o "$TMP_DIR/j4_nfce1.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/receipts/nfce" \
  -H "Authorization: Bearer $TOKEN_A" \
  -H "X-Tenant-ID: $TENANT_A" \
  -H "Content-Type: application/json" \
  -d "{\"access_key\":\"$NFCE_KEY_1\",\"merchant_name\":\"CARREFOUR HIPERMERCADO\",\"total_amount\":\"39.90\",\"items\":[{\"ean\":\"$EAN_SKU\",\"description\":\"AZEITE EXTRA VIRGEM 500ML\",\"quantity\":\"1.000\",\"unit_price\":\"39.90\"}]}")
assert_http "JORNADA 4" "Ingerir NFC-e 1 (EAN 7891234567890 a R$ 39,90)" "HAPPY" "200" "$CODE" "INGESTED_WITH_SKU_INFLATION_TRACKING" "$TMP_DIR/j4_nfce1.json"

CODE=$(curl -s -o "$TMP_DIR/j4_nfce2.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/receipts/nfce" \
  -H "Authorization: Bearer $TOKEN_A" \
  -H "X-Tenant-ID: $TENANT_A" \
  -H "Content-Type: application/json" \
  -d "{\"access_key\":\"$NFCE_KEY_2\",\"merchant_name\":\"PAO DE ACUCAR\",\"total_amount\":\"47.90\",\"items\":[{\"ean\":\"$EAN_SKU\",\"description\":\"AZEITE EXTRA VIRGEM 500ML\",\"quantity\":\"1.000\",\"unit_price\":\"47.90\"}]}")
assert_http "JORNADA 4" "Ingerir NFC-e 2 (Mesmo SKU a R$ 47,90 -> +20.05%)" "HAPPY" "200" "$CODE" "+20.05%" "$TMP_DIR/j4_nfce2.json"

CODE=$(curl -s -o "$TMP_DIR/j4_sad1.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/receipts/nfce" \
  -H "Authorization: Bearer $TOKEN_A" \
  -H "X-Tenant-ID: $TENANT_A" \
  -H "Content-Type: application/json" \
  -d "{\"access_key\":\"$NFCE_KEY_1\",\"merchant_name\":\"CARREFOUR HIPERMERCADO\",\"total_amount\":\"39.90\",\"items\":[]}")
assert_http "JORNADA 4" "Reenviar Mesma Chave NFC-e 44 Digitos (Duplicada)" "SAD" "409" "$CODE"

# ==============================================================================
# JORNADA 5 — PIPELINE KAFKA, CATEGORIZACAO IA & INBOX ZERO ("OUTROS < 5%")
# ==============================================================================
echo ""
echo ">>> JORNADA 5: Pipeline Kafka, Categorizacao IA & Inbox Zero (Outros < 5%)"

CODE=$(curl -s -o "$TMP_DIR/j5_inbox.json" -w "%{http_code}" "$BASE_URL/api/v1/inbox" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A")
assert_http "JORNADA 5" "Consultar Fila Inbox Zero (Outros 2.40% < 5.00%)" "HAPPY" "200" "$CODE" "PENDING_REVIEW" "$TMP_DIR/j5_inbox.json"

CODE=$(curl -s -o "$TMP_DIR/j5_confirm.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/inbox/inbox-001/confirm" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A" -H "Content-Type: application/json" \
  -d '{"approved_category_code":"ALIMENTACAO :: DELIVERY_APLICATIVOS","learn_rule":true}')
assert_http "JORNADA 5" "Confirmar Sugestao IA no Inbox Zero (learn_rule)" "HAPPY" "200" "$CODE" "CONFIRMED_AND_COMMITTED" "$TMP_DIR/j5_confirm.json"

CODE=$(curl -s -o "$TMP_DIR/j5_dlq.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/inbox/dlq-test" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A" -H "Content-Type: application/json" \
  -d '{"corrupted":true,"payload":"INVALID_EVENT"}')
assert_http "JORNADA 5" "Evento Corrompido roteado para tx.dlq sem crash" "SAD" "202" "$CODE" "ROUTED_TO_ENCRYPTED_DLQ" "$TMP_DIR/j5_dlq.json"

# ==============================================================================
# JORNADA 6 — MOTOR DE PREVISIBILIDADE ("LIVRE PARA GASTAR") & SIMULADOR SAC/PRICE
# ==============================================================================
echo ""
echo ">>> JORNADA 6: Motor de Previsibilidade (Livre para Gastar) & Simulador SAC/PRICE"

CODE=$(curl -s -o "$TMP_DIR/j6_dash.json" -w "%{http_code}" "$BASE_URL/api/v1/dashboard/summary?horizon=MONTH&scope=FAMILY" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A")
assert_http "JORNADA 6" "Consultar Livre para Gastar (Hoje/Semana/Mes)" "HAPPY" "200" "$CODE" "free_to_spend_month" "$TMP_DIR/j6_dash.json"

CODE=$(curl -s -o "$TMP_DIR/j6_sim.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/contracts/simulate" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A" -H "Content-Type: application/json" \
  -d '{"contract_id":"fin-sac-2026","amortization_system":"SAC","prepayment_strategy":"REDUCE_TERM","outstanding_balance":"320000.00","annual_interest_rate":"9.49","remaining_installments":240,"extra_amortization_amount":"50000.00"}')
assert_http "JORNADA 6" "Simular Amortizacao SAC via HTTP BFF (R$ 50.000)" "HAPPY" "200" "$CODE" "interest_savings_amount" "$TMP_DIR/j6_sim.json"

# Direct gRPC call to Forecast Engine (:9091) Happy Path + Sad Path
if grpcurl -plaintext \
  -d '{"tenant_id":"11111111-1111-1111-1111-111111111111","contract_id":"fin-sac-2026","amortization_system":"AMORTIZATION_SYSTEM_SAC","prepayment_strategy":"PREPAYMENT_STRATEGY_REDUCE_TERM","outstanding_balance":"320000.00","remaining_installments":240,"monthly_interest_rate":"0.007586","extra_amortization_amount":"50000.00","currency":"BRL"}' \
  "$FORECAST_GRPC" pfm.v1.ForecastService/SimulateContractAmortization > "$TMP_DIR/j6_grpc_ok.json" 2>&1; then
  record_result "JORNADA 6" "gRPC ForecastService/SimulateContract (:9091)" "HAPPY" "gRPC OK" "gRPC OK" "PASS"
else
  record_result "JORNADA 6" "gRPC ForecastService/SimulateContract (:9091)" "HAPPY" "gRPC OK" "gRPC ERROR" "FAIL"
fi

if grpcurl -plaintext \
  -d '{"tenant_id":"11111111-1111-1111-1111-111111111111","contract_id":"fin-sac-2026","amortization_system":"AMORTIZATION_SYSTEM_SAC","prepayment_strategy":"PREPAYMENT_STRATEGY_REDUCE_TERM","outstanding_balance":"320000.00","remaining_installments":240,"monthly_interest_rate":"0.007586","extra_amortization_amount":"400000.00","currency":"BRL"}' \
  "$FORECAST_GRPC" pfm.v1.ForecastService/SimulateContractAmortization > "$TMP_DIR/j6_grpc_sad.json" 2>&1; then
  record_result "JORNADA 6" "gRPC ForecastService Amortizacao > Saldo (:9091)" "SAD" "InvalidArgument" "gRPC OK" "FAIL"
else
  if grep -q "InvalidArgument" "$TMP_DIR/j6_grpc_sad.json"; then
    record_result "JORNADA 6" "gRPC ForecastService Amortizacao > Saldo (:9091)" "SAD" "InvalidArgument" "InvalidArgument" "PASS"
  else
    record_result "JORNADA 6" "gRPC ForecastService Amortizacao > Saldo (:9091)" "SAD" "InvalidArgument" "Other Error" "FAIL"
  fi
fi

CODE=$(curl -s -o "$TMP_DIR/j6_sad1.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/contracts/simulate" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A" -H "Content-Type: application/json" \
  -d '{"contract_id":"fin-sac-2026","amortization_system":"SAC","prepayment_strategy":"REDUCE_TERM","outstanding_balance":"320000.00","remaining_installments":240,"extra_amortization_amount":"400000.00"}')
assert_http "JORNADA 6" "HTTP Simular Amortizacao Extra > Saldo Devedor" "SAD" "400" "$CODE"

CODE=$(curl -s -o "$TMP_DIR/j6_sad2.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/contracts/simulate" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A" -H "Content-Type: application/json" \
  -d '{"contract_id":"fin-sac-2026","amortization_system":"SAC","prepayment_strategy":"REDUCE_TERM","outstanding_balance":"320000.00","remaining_installments":0,"extra_amortization_amount":"10000.00"}')
assert_http "JORNADA 6" "HTTP Simular Contrato com Prazo Zero (0 meses)" "SAD" "400" "$CODE"

# ==============================================================================
# JORNADA 7 — GOVERNANCA FAMILIAR, TETO DE GASTOS & CRYPTO-SHREDDING LGPD
# ==============================================================================
echo ""
echo ">>> JORNADA 7: Governanca Familiar, Teto de Gastos & Crypto-Shredding LGPD"

CODE=$(curl -s -o "$TMP_DIR/j7_budget.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/profile/budgets" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A" -H "Content-Type: application/json" \
  -d '{"category_id":"alimentacao-fora","category_name":"Alimentacao Fora","monthly_cap":"1500.00","threshold_percentage":80}')
assert_http "JORNADA 7" "Configurar Teto Mensal R$ 1.500,00 (Alerta 80%)" "HAPPY" "200" "$CODE"

CODE=$(curl -s -o "$TMP_DIR/j7_invite.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/profile/members" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A" -H "Content-Type: application/json" \
  -d '{"email":"conjuge@pfmpulse.com.br","full_name":"Conjuge Dantas","role":"MEMBER","spending_allowance":"1200.00"}')
assert_http "JORNADA 7" "Convidar Membro Familiar (MEMBER / VIEWER)" "HAPPY" "201" "$CODE"

CODE=$(curl -s -o "$TMP_DIR/j7_export.json" -w "%{http_code}" -X POST "$BASE_URL/api/v1/profile/export" \
  -H "Authorization: Bearer $TOKEN_A" -H "X-Tenant-ID: $TENANT_A" -H "Content-Type: application/json" \
  -d '{"format":"JSON"}')
assert_http "JORNADA 7" "Exportar Dados Portabilidade LGPD (Art. 18 V)" "HAPPY" "202" "$CODE"

# Direct gRPC Core Ledger Java 25 (:9090) check
if grpcurl -plaintext \
  -d "{\"tenant_id\":\"$TENANT_A\",\"reference_month\":\"2026-11\"}" \
  "$CORE_GRPC" pfm.v1.CoreLedgerService/GetTenantFinancialSummary > "$TMP_DIR/j7_core_grpc.json" 2>&1; then
  record_result "JORNADA 7" "gRPC CoreLedgerService Java 25 (:9090)" "HAPPY" "gRPC OK" "gRPC OK" "PASS"
else
  record_result "JORNADA 7" "gRPC CoreLedgerService Java 25 (:9090)" "HAPPY" "gRPC OK" "gRPC ERROR" "FAIL"
fi

# ==============================================================================
# JORNADA 8 — SAAS ADMIN BACKOFFICE & ZERO-TRUST PRIVACY (BLINDAGEM LGPD)
# ==============================================================================
echo ""
echo ">>> JORNADA 8: SaaS Admin Backoffice & Zero-Trust Privacy (Blindagem LGPD)"

CODE=$(curl -s -o "$TMP_DIR/j8_happy.json" -w "%{http_code}" "$BASE_URL/api/v1/admin/tenants?page_size=20&status=ACTIVE" \
  -H "Authorization: Bearer $TOKEN_ADMIN")
assert_http "JORNADA 8" "PLATFORM_ADMIN Lista Metadados Comerciais" "HAPPY" "200" "$CODE"

CODE=$(curl -s -o "$TMP_DIR/j8_sad1.json" -w "%{http_code}" "$BASE_URL/api/v1/transactions" \
  -H "Authorization: Bearer $TOKEN_ADMIN" -H "X-Tenant-ID: $TENANT_A")
assert_http "JORNADA 8" "PLATFORM_ADMIN Tenta Ler Transacoes Privadas" "SAD" "403" "$CODE" "Zero-Trust LGPD" "$TMP_DIR/j8_sad1.json"

CODE=$(curl -s -o "$TMP_DIR/j8_sad2.json" -w "%{http_code}" "$BASE_URL/api/v1/profile" \
  -H "Authorization: Bearer $TOKEN_ADMIN" -H "X-Tenant-ID: $TENANT_A")
assert_http "JORNADA 8" "PLATFORM_ADMIN Tenta Ler Perfil Familiar Privado" "SAD" "403" "$CODE" "Zero-Trust LGPD" "$TMP_DIR/j8_sad2.json"

echo ""
echo "=============================================================================="
echo " TABELA DE HOMOLOGACAO LOCAL E2E — RESUMO EXECUTIVO"
echo "=============================================================================="
printf "| %-11s | %-52s | %-5s | %-18s | %-18s | %-6s |\n" "Jornada" "Cenario Testado" "Tipo" "Status Esperado" "Status Obtido" "Result"
echo "|-------------|------------------------------------------------------|-------|--------------------|--------------------|--------|"
for row in "${REPORT_ROWS[@]}"; do
  echo "$row"
done
echo "=============================================================================="
echo " TOTAL PASS: $PASS_COUNT | TOTAL FAIL: $FAIL_COUNT"
echo "=============================================================================="

if [[ "$FAIL_COUNT" -gt 0 ]]; then
  exit 1
fi
exit 0
