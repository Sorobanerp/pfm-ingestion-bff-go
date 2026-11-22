package http

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
)

var reCompetenceMonth = regexp.MustCompile(`^\d{4}-(0[1-9]|1[0-2])$`)

type InstallmentEntryDTO struct {
	InstallmentNumber int    `json:"installment_number"`
	TotalInstallments int    `json:"total_installments"`
	Label             string `json:"label"`
	CompetenceMonth   string `json:"competence_month"`
	Amount            string `json:"amount"`
	Status            string `json:"status"`
}

type LocalTransactionDTO struct {
	ID                     string                `json:"id"`
	TenantID               string                `json:"tenant_id"`
	CardID                 string                `json:"card_id,omitempty"`
	EntryType              string                `json:"entry_type"`
	OriginalTransactionID  string                `json:"original_transaction_id,omitempty"`
	Description            string                `json:"description"`
	Amount                 string                `json:"amount"`
	CurrentCompetenceSpend string                `json:"current_competence_spend"`
	Currency               string                `json:"currency"`
	CategoryCode           string                `json:"category_code"`
	PaymentMethod          string                `json:"payment_method"`
	CompetenceMonth        string                `json:"competence_month"`
	InstallmentNumber      int                   `json:"installment_number"`
	TotalInstallments      int                   `json:"total_installments"`
	InstallmentSchedule    []InstallmentEntryDTO `json:"installment_schedule,omitempty"`
	IdempotencyKey         string                `json:"idempotency_key,omitempty"`
	Status                 string                `json:"status"`
	CreatedAt              string                `json:"created_at"`
}

type LocalCardDTO struct {
	CardID         string `json:"card_id"`
	TenantID       string `json:"tenant_id"`
	Issuer         string `json:"issuer"`
	Last4          string `json:"last4"`
	CreditLimit    string `json:"credit_limit"`
	AvailableLimit string `json:"available_limit"`
	Currency       string `json:"currency"`
	Status         string `json:"status"`
}

type NfceItemInput struct {
	EAN         string `json:"ean"`
	Description string `json:"description"`
	Quantity    string `json:"quantity"`
	UnitPrice   string `json:"unit_price"`
}

var (
	localTxMu       sync.RWMutex
	localTxStore    = make(map[string][]LocalTransactionDTO)
	localCardStore  = make(map[string][]LocalCardDTO)
	localNfceKeys   = make(map[string]bool)
	localEanHistory = make(map[string]decimal.Decimal)
)

const swaggerIndexHTML = `<!DOCTYPE html>
<html lang="pt-BR">
<head>
  <meta charset="UTF-8">
  <title>PFM Core Banking — Swagger UI (OpenAPI 3.1)</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui.css" />
  <style>
    body { margin: 0; background: #F7F9F5; font-family: system-ui, -apple-system, sans-serif; }
    .topbar-banner {
      background: #141D14; color: #DEFF8B; padding: 12px 24px;
      display: flex; align-items: center; justify-content: space-between;
      font-size: 14px; border-bottom: 2px solid #8CBA51;
    }
    .topbar-banner code { background: #233323; color: #FFFFFF; padding: 3px 8px; border-radius: 4px; }
  </style>
</head>
<body>
  <div class="topbar-banner">
    <div><strong>PFM Core Banking — Ingestion BFF (APP_ENV=local)</strong> | OpenAPI 3.1 Interactive Console</div>
    <div>Header RLS Padrao: <code>X-Tenant-ID: 11111111-1111-1111-1111-111111111111</code></div>
  </div>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js"></script>
  <script>
    window.onload = function() {
      window.ui = SwaggerUIBundle({
        url: "/swagger/openapi.yaml",
        dom_id: '#swagger-ui',
        deepLinking: true,
        persistAuthorization: true,
        displayRequestDuration: true,
        requestInterceptor: function(req) {
          if (!req.headers['X-Tenant-ID'] && !req.headers['x-tenant-id']) {
            req.headers['X-Tenant-ID'] = '11111111-1111-1111-1111-111111111111';
          }
          return req;
        }
      });
    };
  </script>
</body>
</html>`

func (s *Server) registerSwaggerAndLocalRoutes() {
	s.mux.HandleFunc("GET /swagger/index.html", s.handleSwaggerUI)
	s.mux.HandleFunc("GET /swagger/", s.handleSwaggerUI)
	s.mux.HandleFunc("GET /swagger/openapi.yaml", s.handleOpenAPISpec)
	s.mux.HandleFunc("GET /openapi.yaml", s.handleOpenAPISpec)

	// Dev Auth Token Generator & Admin Onboarding (Journey 1)
	s.mux.HandleFunc("POST /api/v1/auth/dev-token", s.handleGenerateDevToken)
	s.mux.HandleFunc("POST /api/v1/admin/tenants", s.handleAdminCreateTenantLocal)

	// Cards & Accounts CRUD (Journey 2)
	s.mux.HandleFunc("POST /api/v1/cards", s.handleCreateCard)
	s.mux.HandleFunc("GET /api/v1/cards", s.handleListCards)

	// Transactions CRUD + Refund (Journey 1 & 2)
	s.mux.HandleFunc("GET /api/v1/dashboard/summary", s.handleDashboardSummaryWithLocalTransactions)
	s.mux.HandleFunc("GET /api/v1/transactions", s.handleListTransactions)
	s.mux.HandleFunc("POST /api/v1/transactions", s.handleCreateTransaction)
	s.mux.HandleFunc("GET /api/v1/transactions/{id}", s.handleGetTransactionByID)
	s.mux.HandleFunc("PATCH /api/v1/transactions/{id}", s.handleUpdateTransaction)
	s.mux.HandleFunc("PUT /api/v1/transactions/{id}", s.handleUpdateTransaction)
	s.mux.HandleFunc("POST /api/v1/transactions/{id}/refund", s.handleRefundTransaction)
	s.mux.HandleFunc("DELETE /api/v1/transactions/{id}", s.handleRefundTransaction)

	// Inbox & Kafka DLQ Resilience (Journey 4)
	s.mux.HandleFunc("GET /api/v1/inbox", s.handleListInbox)
	s.mux.HandleFunc("POST /api/v1/inbox/{item_id}/confirm", s.handleConfirmInboxItem)
	s.mux.HandleFunc("POST /api/v1/inbox/dlq-test", s.handleSimulateKafkaDlq)
}

func (s *Server) handleSwaggerUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(swaggerIndexHTML))
}

func (s *Server) handleOpenAPISpec(w http.ResponseWriter, _ *http.Request) {
	candidates := []string{
		"../pfm-contracts/openapi/openapi.yaml",
		"../../pfm-contracts/openapi/openapi.yaml",
		"/home/dantas/workspace/pfm-platform/pfm-contracts/openapi/openapi.yaml",
	}
	for _, path := range candidates {
		if raw, err := os.ReadFile(path); err == nil && len(raw) > 0 { // #nosec G304
			w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(raw) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter.no-direct-write-to-responsewriter
			return
		}
	}
	w.Header().Set("Content-Type", "application/yaml; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("openapi: 3.1.0\ninfo:\n  title: PFM Core Banking BFF\n  version: 3.0.0\n"))
}

func (s *Server) handleGenerateDevToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TenantID   string `json:"tenant_id"`
		TenantName string `json:"tenant_name"`
		Subject    string `json:"sub"`
		Role       string `json:"role"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 16*1024)).Decode(&req)
	if req.TenantID == "" {
		req.TenantID = "11111111-1111-1111-1111-111111111111"
	}
	if req.Role == "" {
		req.Role = "OWNER"
	}
	if req.Subject == "" {
		req.Subject = "usr-dantas-owner"
	}
	if req.TenantName == "" {
		req.TenantName = "Familia Dantas"
	}

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claimsBytes, _ := json.Marshal(map[string]any{
		"sub":         req.Subject,
		"tenant_id":   req.TenantID,
		"tenant_name": req.TenantName,
		"role":        req.Role,
		"iat":         time.Now().Unix(),
	})
	payload := base64.RawURLEncoding.EncodeToString(claimsBytes)
	token := header + "." + payload + ".local-dev-sig"

	writeJSONWithETagAndCompression(w, r, http.StatusOK, map[string]any{
		"access_token": token,
		"token_type":   "Bearer",
		"tenant_id":    req.TenantID,
		"tenant_name":  req.TenantName,
		"role":         req.Role,
	})
}

func (s *Server) handleAdminCreateTenantLocal(w http.ResponseWriter, r *http.Request) {
	_, ok := s.requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	var req struct {
		TenantID   string `json:"tenant_id"`
		TenantName string `json:"tenant_name"`
		OwnerEmail string `json:"owner_email"`
		Plan       string `json:"plan"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 32*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid tenant onboarding JSON payload"})
		return
	}
	if req.TenantID == "" {
		req.TenantID = "11111111-1111-1111-1111-111111111111"
	}
	if req.TenantName == "" {
		req.TenantName = "Familia Dantas"
	}
	if req.Plan == "" {
		req.Plan = "FAMILY_PLUS"
	}
	if s.eastWestClient != nil {
		if ledgerRes, err := s.eastWestClient.CreateTenant(r.Context(), req.TenantID, req.TenantName, req.OwnerEmail, req.Plan); err == nil && ledgerRes != nil && ledgerRes["status"] != "MOCK_SUCCESS" {
			ledgerRes["tenant_id"] = req.TenantID
			ledgerRes["tenant_name"] = req.TenantName
			ledgerRes["owner_email"] = req.OwnerEmail
			ledgerRes["rls_policy"] = "ENABLED_FAIL_CLOSED"
			writeJSONWithETagAndCompression(w, r, http.StatusCreated, ledgerRes)
			return
		}
	}
	writeJSONWithETagAndCompression(w, r, http.StatusCreated, map[string]any{
		"tenant_id":           req.TenantID,
		"tenant_name":         req.TenantName,
		"owner_email":         req.OwnerEmail,
		"plan":                req.Plan,
		"subscription_status": "ACTIVE",
		"rls_policy":          "ENABLED_FAIL_CLOSED",
		"kms_cmk_alias":       "alias/pfm-master-key",
		"created_at":          time.Now().UTC().Format(time.RFC3339),
	})
}

func (s *Server) handleCreateCard(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}
	claims, _ := extractAuthClaims(r)
	if claims.Role == "VIEWER" {
		writeJSONWithETagAndCompression(w, r, http.StatusForbidden, map[string]string{
			"error": "forbidden: VIEWER role cannot create cards or accounts",
		})
		return
	}

	var req struct {
		Issuer      string `json:"issuer"`
		Last4       string `json:"last4"`
		CreditLimit string `json:"credit_limit"`
		Currency    string `json:"currency"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 32*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid card JSON payload"})
		return
	}
	limitDec, err := decimal.NewFromString(strings.TrimSpace(req.CreditLimit))
	if err != nil || !limitDec.IsPositive() {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "credit_limit must be a positive decimal"})
		return
	}
	if req.Issuer == "" {
		req.Issuer = "ITAU"
	}
	if req.Last4 == "" {
		req.Last4 = "5285"
	}
	if req.Currency == "" {
		req.Currency = "BRL"
	}

	localTxMu.Lock()
	card := LocalCardDTO{
		CardID:         fmt.Sprintf("card-%s-%s", req.Last4, tenantID[:8]),
		TenantID:       tenantID,
		Issuer:         strings.ToUpper(req.Issuer),
		Last4:          req.Last4,
		CreditLimit:    limitDec.StringFixed(2),
		AvailableLimit: limitDec.StringFixed(2),
		Currency:       req.Currency,
		Status:         "ACTIVE",
	}
	localCardStore[tenantID] = append(localCardStore[tenantID], card)
	localTxMu.Unlock()

	writeJSONWithETagAndCompression(w, r, http.StatusCreated, card)
}

func (s *Server) handleListCards(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}
	localTxMu.RLock()
	cards := append([]LocalCardDTO{}, localCardStore[tenantID]...)
	localTxMu.RUnlock()
	writeJSONWithETagAndCompression(w, r, http.StatusOK, map[string]any{
		"tenant_id": tenantID,
		"count":     len(cards),
		"items":     cards,
	})
}

func addMonthsToCompetence(ym string, offset int) string {
	t, err := time.Parse("2006-01", ym)
	if err != nil {
		t = time.Date(2026, time.November, 1, 0, 0, 0, 0, time.UTC)
	}
	return t.AddDate(0, offset, 0).Format("2006-01")
}

func (s *Server) handleCreateTransaction(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}
	claims, _ := extractAuthClaims(r)
	if claims.Role == "VIEWER" {
		writeJSONWithETagAndCompression(w, r, http.StatusForbidden, map[string]string{
			"error": "forbidden: VIEWER role cannot create or mutate ledger transactions (RBAC Zero-Trust)",
		})
		return
	}

	var req struct {
		CardID            string `json:"card_id"`
		Description       string `json:"description"`
		MerchantName      string `json:"merchant_name"`
		Amount            string `json:"amount"`
		Currency          string `json:"currency"`
		CategoryCode      string `json:"category_code"`
		PaymentMethod     string `json:"payment_method"`
		CompetenceMonth   string `json:"competence_month"`
		TransactionDate   string `json:"transaction_date"`
		InstallmentNumber int    `json:"installment_number"`
		TotalInstallments *int   `json:"total_installments"`
		Installments      *int   `json:"installments"`
		IdempotencyKey    string `json:"idempotency_key"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid transaction JSON payload"})
		return
	}
	amtDec, err := decimal.NewFromString(strings.TrimSpace(req.Amount))
	if err != nil || amtDec.LessThanOrEqual(decimal.Zero) {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "amount must be a strictly positive decimal string (RN06)"})
		return
	}
	if strings.TrimSpace(req.Description) == "" {
		req.Description = strings.TrimSpace(req.MerchantName)
	}
	if strings.TrimSpace(req.Description) == "" {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "description or merchant_name is required"})
		return
	}
	if strings.TrimSpace(req.TransactionDate) != "" {
		parsedDate, dateErr := time.Parse("2006-01-02", strings.TrimSpace(req.TransactionDate))
		if dateErr != nil {
			writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid transaction_date format, expected YYYY-MM-DD"})
			return
		}
		if req.CompetenceMonth == "" {
			req.CompetenceMonth = parsedDate.Format("2006-01")
		}
	}
	if req.CompetenceMonth == "" {
		req.CompetenceMonth = "2026-11"
	}
	if !reCompetenceMonth.MatchString(req.CompetenceMonth) {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid competence_month format, expected YYYY-MM"})
		return
	}

	totalInst := 1
	if req.TotalInstallments != nil {
		if *req.TotalInstallments <= 0 || *req.TotalInstallments > 72 {
			writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "total_installments must be between 1 and 72"})
			return
		}
		totalInst = *req.TotalInstallments
	} else if req.Installments != nil {
		if *req.Installments <= 0 || *req.Installments > 72 {
			writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "installments must be between 1 and 72"})
			return
		}
		totalInst = *req.Installments
	}

	if req.Currency == "" {
		req.Currency = "BRL"
	}
	if req.CategoryCode == "" {
		req.CategoryCode = "ALIMENTACAO :: SUPERMERCADO_MERCEARIA"
	}
	if req.PaymentMethod == "" {
		req.PaymentMethod = "CREDIT_CARD"
	}
	if req.InstallmentNumber <= 0 {
		req.InstallmentNumber = 1
	}

	// Calculate installment schedule with RN06 residual cent allocation on installment 1
	baseInst := amtDec.DivRound(decimal.NewFromInt(int64(totalInst)), 2)
	residual := amtDec.Sub(baseInst.Mul(decimal.NewFromInt(int64(totalInst))))
	firstInst := baseInst.Add(residual)

	schedule := make([]InstallmentEntryDTO, 0, totalInst)
	for i := 1; i <= totalInst; i++ {
		instAmt := baseInst
		status := "FUTURE_INSTALLMENT"
		if i == 1 {
			instAmt = firstInst
			status = "CURRENT_OPEN_STATEMENT"
		}
		schedule = append(schedule, InstallmentEntryDTO{
			InstallmentNumber: i,
			TotalInstallments: totalInst,
			Label:             fmt.Sprintf("%02d/%02d", i, totalInst),
			CompetenceMonth:   addMonthsToCompetence(req.CompetenceMonth, i-1),
			Amount:            instAmt.StringFixed(2),
			Status:            status,
		})
	}

	localTxMu.Lock()
	idx := len(localTxStore[tenantID]) + 1
	tx := LocalTransactionDTO{
		ID:                     fmt.Sprintf("99999999-1111-4111-8111-%012d", idx),
		TenantID:               tenantID,
		CardID:                 req.CardID,
		EntryType:              "DEBIT_PURCHASE",
		Description:            req.Description,
		Amount:                 amtDec.StringFixed(2),
		CurrentCompetenceSpend: firstInst.StringFixed(2),
		Currency:               req.Currency,
		CategoryCode:           req.CategoryCode,
		PaymentMethod:          req.PaymentMethod,
		CompetenceMonth:        req.CompetenceMonth,
		InstallmentNumber:      req.InstallmentNumber,
		TotalInstallments:      totalInst,
		InstallmentSchedule:    schedule,
		IdempotencyKey:         req.IdempotencyKey,
		Status:                 "COMMITTED",
		CreatedAt:              time.Now().UTC().Format(time.RFC3339),
	}
	localTxStore[tenantID] = append(localTxStore[tenantID], tx)

	// Sensitize credit card available limit by full purchase amount (RN02)
	cards := localCardStore[tenantID]
	for i := range cards {
		if avail, err := decimal.NewFromString(cards[i].AvailableLimit); err == nil {
			cards[i].AvailableLimit = avail.Sub(amtDec).StringFixed(2)
		}
	}
	localCardStore[tenantID] = cards
	localTxMu.Unlock()

	if coreURL := strings.TrimSpace(os.Getenv("CORE_LEDGER_HTTP_URL")); coreURL != "" {
		ledgerPayload, _ := json.Marshal(map[string]any{
			"tenantId":        tenantID,
			"idempotencyKey":  fmt.Sprintf("tx-%s-%d", tenantID, idx),
			"merchantName":    tx.Description,
			"totalAmount":     tx.Amount,
			"currency":        tx.Currency,
			"categoryCode":    tx.CategoryCode,
			"paymentMethod":   tx.PaymentMethod,
			"competenceMonth": tx.CompetenceMonth,
		})
		httpReq, err := http.NewRequestWithContext(r.Context(), http.MethodPost, strings.TrimRight(coreURL, "/")+"/pfm.v1.CoreLedgerService/ProcessReceipt", bytes.NewReader(ledgerPayload))
		if err == nil {
			httpReq.Header.Set("Content-Type", "application/json")
			httpReq.Header.Set("X-Tenant-ID", tenantID)
			client := &http.Client{Timeout: 2 * time.Second}
			if resp, doErr := client.Do(httpReq); doErr == nil && resp != nil {
				_ = resp.Body.Close()
			}
		}
	}

	writeJSONWithETagAndCompression(w, r, http.StatusCreated, tx)
}

func (s *Server) handleGetTransactionByID(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}
	txID := r.PathValue("id")
	localTxMu.RLock()
	defer localTxMu.RUnlock()
	for _, tx := range localTxStore[tenantID] {
		if tx.ID == txID {
			writeJSONWithETagAndCompression(w, r, http.StatusOK, tx)
			return
		}
	}
	writeJSONWithETagAndCompression(w, r, http.StatusNotFound, map[string]string{
		"error": "transaction not found in current tenant RLS scope",
	})
}

func (s *Server) handleUpdateTransaction(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}
	claims, _ := extractAuthClaims(r)
	if claims.Role == "VIEWER" {
		writeJSONWithETagAndCompression(w, r, http.StatusForbidden, map[string]string{
			"error": "forbidden: VIEWER role cannot modify transactions",
		})
		return
	}
	txID := r.PathValue("id")
	var req struct {
		Description  string `json:"description"`
		CategoryCode string `json:"category_code"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 32*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid update JSON payload"})
		return
	}

	localTxMu.Lock()
	defer localTxMu.Unlock()
	items := localTxStore[tenantID]
	for i := range items {
		if items[i].ID == txID {
			if strings.TrimSpace(req.Description) != "" {
				items[i].Description = strings.TrimSpace(req.Description)
			}
			if strings.TrimSpace(req.CategoryCode) != "" {
				items[i].CategoryCode = strings.TrimSpace(req.CategoryCode)
			}
			localTxStore[tenantID] = items
			writeJSONWithETagAndCompression(w, r, http.StatusOK, items[i])
			return
		}
	}
	writeJSONWithETagAndCompression(w, r, http.StatusNotFound, map[string]string{
		"error": "transaction not found in current tenant RLS scope",
	})
}

func (s *Server) handleRefundTransaction(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}
	claims, _ := extractAuthClaims(r)
	if claims.Role == "VIEWER" {
		writeJSONWithETagAndCompression(w, r, http.StatusForbidden, map[string]string{
			"error": "forbidden: VIEWER role cannot refund or delete transactions",
		})
		return
	}
	txID := r.PathValue("id")

	localTxMu.Lock()
	defer localTxMu.Unlock()
	items := localTxStore[tenantID]
	for i := range items {
		if items[i].ID == txID {
			orig := items[i]
			origAmt, _ := decimal.NewFromString(orig.Amount)
			origCompSpend, _ := decimal.NewFromString(orig.CurrentCompetenceSpend)

			refundEntry := LocalTransactionDTO{
				ID:                     fmt.Sprintf("99999999-1111-4111-8111-%012d", len(items)+1),
				TenantID:               tenantID,
				CardID:                 orig.CardID,
				EntryType:              "REFUND_CREDIT",
				OriginalTransactionID:  orig.ID,
				Description:            "ESTORNO: " + orig.Description,
				Amount:                 origAmt.Neg().StringFixed(2),
				CurrentCompetenceSpend: origCompSpend.Neg().StringFixed(2),
				Currency:               orig.Currency,
				CategoryCode:           orig.CategoryCode, // RN03: preserves exact original category!
				PaymentMethod:          orig.PaymentMethod,
				CompetenceMonth:        orig.CompetenceMonth,
				InstallmentNumber:      1,
				TotalInstallments:      1,
				Status:                 "REFUNDED_APPEND_ONLY",
				CreatedAt:              time.Now().UTC().Format(time.RFC3339),
			}
			localTxStore[tenantID] = append(items, refundEntry)

			// Restore credit card available limit (RN03)
			cards := localCardStore[tenantID]
			for c := range cards {
				if avail, err := decimal.NewFromString(cards[c].AvailableLimit); err == nil {
					cards[c].AvailableLimit = avail.Add(origAmt).StringFixed(2)
				}
			}
			localCardStore[tenantID] = cards

			writeJSONWithETagAndCompression(w, r, http.StatusCreated, refundEntry)
			return
		}
	}
	writeJSONWithETagAndCompression(w, r, http.StatusNotFound, map[string]string{
		"error": "original transaction not found in current tenant RLS scope",
	})
}

func (s *Server) handleListTransactions(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}
	if claims, errAuth := extractAuthClaims(r); errAuth == nil && claims != nil && claims.Role == "PLATFORM_ADMIN" {
		writeJSONWithETagAndCompression(w, r, http.StatusForbidden, map[string]string{
			"error": "forbidden: PLATFORM_ADMIN cannot read private tenant financial transactions (Zero-Trust LGPD)",
		})
		return
	}
	localTxMu.RLock()
	items := append([]LocalTransactionDTO{}, localTxStore[tenantID]...)
	localTxMu.RUnlock()

	writeJSONWithETagAndCompression(w, r, http.StatusOK, map[string]any{
		"tenant_id": tenantID,
		"rls_scope": fmt.Sprintf("SET LOCAL app.current_tenant_id = '%s'", tenantID),
		"count":     len(items),
		"items":     items,
	})
}

func (s *Server) handleDashboardSummaryWithLocalTransactions(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}
	scope := strings.TrimSpace(r.URL.Query().Get("scope"))
	if scope == "" {
		scope = "FAMILY"
	}
	horizon := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("horizon")))
	if horizon == "" {
		horizon = "MONTH"
	}

	summary, err := s.eastWestClient.FetchDashboardSummary(r.Context(), tenantID, scope)
	if err != nil {
		summary = make(map[string]any)
	}

	localTxMu.RLock()
	items := append([]LocalTransactionDTO{}, localTxStore[tenantID]...)
	cards := append([]LocalCardDTO{}, localCardStore[tenantID]...)
	localTxMu.RUnlock()

	customCompetenceNet := decimal.Zero
	customTotalGross := decimal.Zero
	for _, tx := range items {
		if d, err := decimal.NewFromString(tx.CurrentCompetenceSpend); err == nil {
			customCompetenceNet = customCompetenceNet.Add(d)
		}
		if g, err := decimal.NewFromString(tx.Amount); err == nil {
			customTotalGross = customTotalGross.Add(g)
		}
	}

	baseFreeMonth := decimal.RequireFromString("7080.00").Sub(customCompetenceNet)
	freeWeek := baseFreeMonth.DivRound(decimal.NewFromInt(4), 2)
	freeToday := baseFreeMonth.DivRound(decimal.NewFromInt(30), 2)

	summary["tenant_id"] = tenantID
	summary["rls_isolated_tenant_id"] = tenantID
	summary["scope"] = scope
	summary["horizon"] = horizon
	summary["currency"] = "BRL"
	summary["realized_income"] = "18200.00"
	summary["settled_fixed_expenses"] = "5400.00"
	summary["free_to_spend_today"] = freeToday.StringFixed(2)
	summary["free_to_spend_week"] = freeWeek.StringFixed(2)
	summary["free_to_spend_month"] = baseFreeMonth.StringFixed(2)
	summary["projected_free_cashflow"] = baseFreeMonth.StringFixed(2)
	summary["custom_transactions_count"] = len(items)
	summary["custom_transactions_competence_net"] = customCompetenceNet.StringFixed(2)
	summary["custom_transactions_total"] = customTotalGross.StringFixed(2)
	summary["active_cards_count"] = len(cards)

	writeJSONWithETagAndCompression(w, r, http.StatusOK, summary)
}

func (s *Server) handleListInbox(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, map[string]any{
		"tenant_id":               tenantID,
		"pending_count":           2,
		"uncategorized_ratio_pct": "2.40",
		"target_max_others_pct":   "5.00",
		"items": []map[string]any{
			{
				"item_id":            "inbox-001",
				"tenant_id":          tenantID,
				"merchant_raw":       "IFD*IFOOD BR",
				"amount":             "84.90",
				"currency":           "BRL",
				"suggested_category": "ALIMENTACAO :: DELIVERY_APLICATIVOS",
				"confidence":         "0.9400",
				"status":             "PENDING_REVIEW",
			},
			{
				"item_id":            "inbox-002",
				"tenant_id":          tenantID,
				"merchant_raw":       "UBER *TRIP SAO PAULO",
				"amount":             "39.50",
				"currency":           "BRL",
				"suggested_category": "TRANSPORTE :: APLICATIVOS_MOBILIDADE",
				"confidence":         "0.9100",
				"status":             "PENDING_REVIEW",
			},
		},
	})
}

func (s *Server) handleConfirmInboxItem(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}
	itemID := r.PathValue("item_id")
	var req struct {
		ApprovedCategoryCode string `json:"approved_category_code"`
		LearnRule            bool   `json:"learn_rule"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req)
	if req.ApprovedCategoryCode == "" {
		req.ApprovedCategoryCode = "ALIMENTACAO :: DELIVERY_APLICATIVOS"
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, map[string]any{
		"tenant_id":              tenantID,
		"item_id":                itemID,
		"approved_category_code": req.ApprovedCategoryCode,
		"learn_rule":             req.LearnRule,
		"status":                 "CONFIRMED_AND_COMMITTED",
	})
}

func (s *Server) handleSimulateKafkaDlq(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}
	var raw map[string]any
	if err := json.NewDecoder(io.LimitReader(r.Body, 16*1024)).Decode(&raw); err != nil || raw["corrupted"] == true {
		writeJSONWithETagAndCompression(w, r, http.StatusAccepted, map[string]any{
			"tenant_id":   tenantID,
			"sourceTopic": "statement.raw.extracted",
			"dlqTopic":    "tx.dlq",
			"status":      "ROUTED_TO_ENCRYPTED_DLQ",
			"workerState": "UP_AND_HEALTHY",
		})
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, map[string]any{
		"tenant_id": tenantID,
		"status":    "PROCESSED",
	})
}

func extractStatementPayloadFromRequest(r *http.Request, rawBody []byte) ([]byte, string, error) {
	fileName := strings.TrimSpace(r.Header.Get("X-File-Name"))
	contentType := r.Header.Get("Content-Type")
	if strings.HasPrefix(strings.ToLower(contentType), "multipart/form-data") {
		_, params, err := mime.ParseMediaType(contentType)
		if err != nil {
			return nil, fileName, err
		}
		mr := multipart.NewReader(bytes.NewReader(rawBody), params["boundary"])
		for {
			part, partErr := mr.NextPart()
			if partErr == io.EOF {
				break
			}
			if partErr != nil {
				return nil, fileName, partErr
			}
			if part.FileName() != "" || part.FormName() == "file" || part.FormName() == "statement" {
				if fileName == "" {
					fileName = part.FileName()
				}
				partBytes, readErr := io.ReadAll(part)
				_ = part.Close()
				return partBytes, fileName, readErr
			}
			_ = part.Close()
		}
	}
	return rawBody, fileName, nil
}
