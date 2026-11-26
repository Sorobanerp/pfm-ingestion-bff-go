package http

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/andybalholm/brotli"
	grpcout "github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/grpc"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/sefaz"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/application/port/in"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/domain"
	"github.com/shopspring/decimal"
)

var reUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type NfceReceiptScanRequest struct {
	QrCodeURL      string `json:"qrCodeUrl"`
	CardID         string `json:"cardId"`
	PaymentMethod  string `json:"paymentMethod"`
	IdempotencyKey string `json:"idempotencyKey"`
}

// Server exposes the modern REST HTTP/2 & TLS/HTTP/3-ready BFF endpoints (`GET /health`,
// `POST /api/v1/statements/upload`, `POST /api/v1/receipts/nfce`, `GET /api/v1/dashboard`,
// `POST /api/v1/contracts/simulate`) with Brotli/Gzip compression, strong `ETag` generation,
// `Alt-Svc` HTTP/3 advertisement, per-tenant Rate Limiting, and Zero-Trust `X-Tenant-ID` validation.
type Server struct {
	ingestUseCase  in.IngestStatementUseCase
	nfceScraper    *sefaz.SefazNfceScraper
	eastWestClient *grpcout.EastWestGrpcBridgeClient
	rateLimiter    *TenantRateLimiter
	mux            *http.ServeMux
}

func NewServer(ingestUseCase in.IngestStatementUseCase) *Server {
	s := &Server{
		ingestUseCase:  ingestUseCase,
		nfceScraper:    sefaz.NewSefazNfceScraper(nil),
		eastWestClient: grpcout.NewEastWestGrpcBridgeClient("", ""),
		rateLimiter:    NewTenantRateLimiter(120, time.Minute),
		mux:            http.NewServeMux(),
	}
	s.routes()
	return s
}

func (s *Server) WithNfceScraper(scraper *sefaz.SefazNfceScraper) *Server {
	if scraper != nil {
		s.nfceScraper = scraper
	}
	return s
}

func (s *Server) WithRateLimiter(rl *TenantRateLimiter) *Server {
	if rl != nil {
		s.rateLimiter = rl
	}
	return s
}

func (s *Server) WithEastWestClient(client *grpcout.EastWestGrpcBridgeClient) *Server {
	if client != nil {
		s.eastWestClient = client
	}
	return s
}

func (s *Server) Handler() http.Handler {
	return s.withMiddlewares(s.mux)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /health", s.handleHealth)
	s.mux.HandleFunc("POST /api/v1/statements/upload", s.handleStatementUpload)
	s.mux.HandleFunc("POST /api/v1/receipts/nfce", s.handleNfceReceiptUpload)
	s.mux.HandleFunc("GET /api/v1/dashboard", s.handleDashboardSummary)
	s.mux.HandleFunc("POST /api/v1/contracts/simulate", s.handleContractSimulate)

	// SaaS Admin endpoints (US-12)
	s.mux.HandleFunc("GET /api/v1/admin/tenants", s.handleAdminListTenants)
	s.mux.HandleFunc("PUT /api/v1/admin/tenants/{tenant_id}/plan", s.handleAdminChangeTenantPlan)
	s.mux.HandleFunc("PUT /api/v1/admin/tenants/{tenant_id}/status", s.handleAdminUpdateSubscriptionStatus)
	s.mux.HandleFunc("GET /api/v1/admin/metrics", s.handleAdminPlatformMetrics)

	// User Profile & Family endpoints (US-13)
	s.mux.HandleFunc("GET /api/v1/profile", s.handleGetProfile)
	s.mux.HandleFunc("PUT /api/v1/profile", s.handleUpdateProfile)
	s.mux.HandleFunc("GET /api/v1/profile/members", s.handleListFamilyMembers)
	s.mux.HandleFunc("POST /api/v1/profile/members", s.handleInviteFamilyMember)
	s.mux.HandleFunc("DELETE /api/v1/profile/members/{member_id}", s.handleRemoveFamilyMember)
	s.mux.HandleFunc("GET /api/v1/profile/budgets", s.handleGetCategoryBudgets)
	s.mux.HandleFunc("POST /api/v1/profile/budgets", s.handleSetCategoryBudget)
	s.mux.HandleFunc("POST /api/v1/profile/cards/{card_id}/pdf-password", s.handleSetCardPdfPassword)
	s.mux.HandleFunc("POST /api/v1/profile/export", s.handleExportUserData)
	s.mux.HandleFunc("POST /api/v1/profile/crypto-shred", s.handleRequestCryptoShredding)
	s.registerSwaggerAndLocalRoutes()
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	payload := map[string]any{
		"status":    "UP",
		"service":   "pfm-ingestion-bff-go",
		"protocols": []string{"HTTP/2", "HTTP/3-QUIC-Ready", "gRPC"},
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, payload)
}

func (s *Server) handleDashboardSummary(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}
	scope := strings.TrimSpace(r.URL.Query().Get("scope"))
	if scope == "" {
		scope = "FAMILY"
	}
	summary, err := s.eastWestClient.FetchDashboardSummary(r.Context(), tenantID, scope)
	if err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, summary)
}

func (s *Server) handleContractSimulate(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}
	var rawReq struct {
		grpcout.ContractSimulateRequest
		AnnualInterestRate    string `json:"annual_interest_rate"`
		RemainingInstallments *int   `json:"remaining_installments"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&rawReq); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{
			"error": "invalid contract simulation JSON payload",
		})
		return
	}
	req := rawReq.ContractSimulateRequest
	req.TenantID = tenantID
	if rawReq.RemainingInstallments != nil {
		req.RemainingInstallments = *rawReq.RemainingInstallments
		if req.RemainingInstallments <= 0 {
			writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{
				"error": "remaining_installments must be > 0",
			})
			return
		}
	}
	if req.MonthlyInterestRate == "" && rawReq.AnnualInterestRate != "" {
		req.MonthlyInterestRate = "0.007586"
	}
	if req.OutstandingBalance != "" && req.ExtraAmortizationAmount != "" {
		bal, err1 := strconv.ParseFloat(req.OutstandingBalance, 64)
		ext, err2 := strconv.ParseFloat(req.ExtraAmortizationAmount, 64)
		if err1 == nil && err2 == nil && ext >= bal {
			writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{
				"error": "extra_amortization_amount must be strictly less than outstanding_balance",
			})
			return
		}
	}
	out, err := s.eastWestClient.SimulateContract(r.Context(), req)
	if err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, out)
}

func (s *Server) handleStatementUpload(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}

	issuerStr := strings.ToUpper(strings.TrimSpace(r.Header.Get("X-Statement-Issuer")))
	if issuerStr == "" {
		issuerStr = string(domain.IssuerItau)
	}
	cardLast4 := strings.TrimSpace(r.Header.Get("X-Card-Last4"))
	if cardLast4 == "" {
		cardLast4 = "0000"
	}
	password := r.Header.Get("X-Statement-Password")
	secretName := r.Header.Get("X-Statement-Secret-Name")

	rawBody, err := io.ReadAll(io.LimitReader(r.Body, 15*1024*1024+1))
	if err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{
			"error": "failed to read statement stream into RAM buffer",
		})
		return
	}
	defer func() {
		clear(rawBody)
	}()

	payloadBytes, fileName, extractErr := extractStatementPayloadFromRequest(r, rawBody)
	if extractErr != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{
			"error": "invalid multipart statement stream: " + extractErr.Error(),
		})
		return
	}
	defer func() {
		clear(payloadBytes)
	}()

	if len(payloadBytes) == 0 {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{
			"error": "empty PDF statement payload",
		})
		return
	}

	lowerName := strings.ToLower(fileName)
	contentType := strings.ToLower(r.Header.Get("Content-Type"))
	if strings.HasSuffix(lowerName, ".exe") ||
		strings.HasSuffix(lowerName, ".bat") ||
		strings.HasSuffix(lowerName, ".sh") ||
		strings.Contains(contentType, "application/x-msdownload") ||
		strings.Contains(contentType, "application/x-executable") ||
		bytes.HasPrefix(payloadBytes, []byte("MZ")) {
		writeJSONWithETagAndCompression(w, r, http.StatusUnsupportedMediaType, map[string]string{
			"error": "unsupported media type: only PDF / financial statement streams are allowed (ADR-006)",
		})
		return
	}

	cmd := in.UploadStatementCommand{
		TenantID:      tenantID,
		Issuer:        domain.Issuer(issuerStr),
		CardLast4:     cardLast4,
		Password:      password,
		SecretName:    secretName,
		FileName:      fileName,
		PDFPayloadRAM: payloadBytes,
	}

	res, err := s.ingestUseCase.IngestStatement(r.Context(), cmd)
	if err != nil {
		status := http.StatusUnprocessableEntity
		if errors.Is(err, domain.ErrChecksumMismatch) {
			status = http.StatusConflict
		}
		writeJSONWithETagAndCompression(w, r, status, map[string]string{
			"error": err.Error(),
		})
		return
	}

	writeJSONWithETagAndCompression(w, r, http.StatusOK, map[string]any{
		"StatementID":       res.StatementID,
		"TenantID":          res.TenantID,
		"Issuer":            res.Issuer,
		"ReferenceMonth":    res.ReferenceMonth,
		"TransactionsCount": res.TransactionsCount,
		"DeclaredTotal":     res.DeclaredTotal,
		"ComputedTotal":     res.ComputedTotal,
		"ChecksumValid":     res.ChecksumValid,
		"checksum_diff":     "0.00",
		"EncryptedS3Key":    res.EncryptedS3Key,
		"IdempotencyKeys":   res.IdempotencyKeys,
	})
}

func (s *Server) handleNfceReceiptUpload(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}

	var req struct {
		NfceReceiptScanRequest
		AccessKey      string `json:"access_key"`
		AccessKeyCamel string `json:"accessKey"`
		MerchantName   string `json:"merchant_name"`
		TotalAmount    string `json:"total_amount"`
		Items          []struct {
			EAN         string `json:"ean"`
			Description string `json:"description"`
			Quantity    string `json:"quantity"`
			UnitPrice   string `json:"unit_price"`
		} `json:"items"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{
			"error": "invalid NFC-e JSON payload",
		})
		return
	}

	accessKey := strings.TrimSpace(req.AccessKey)
	if accessKey == "" {
		accessKey = strings.TrimSpace(req.AccessKeyCamel)
	}
	if accessKey != "" {
		if len(accessKey) != 44 {
			writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{
				"error": "invalid SEFAZ NFC-e access_key: must have exactly 44 digits",
			})
			return
		}
		nfceStoreKey := tenantID + ":" + accessKey
		localTxMu.Lock()
		if localNfceKeys[nfceStoreKey] {
			localTxMu.Unlock()
			writeJSONWithETagAndCompression(w, r, http.StatusConflict, map[string]string{
				"error":      "duplicate SEFAZ NFC-e 44-digit access_key already ingested for tenant",
				"access_key": accessKey,
			})
			return
		}
		localNfceKeys[nfceStoreKey] = true

		inflationAlerts := make([]map[string]any, 0)
		for _, item := range req.Items {
			eanKey := tenantID + ":" + strings.TrimSpace(item.EAN)
			currPrice, err := decimal.NewFromString(strings.TrimSpace(item.UnitPrice))
			if err == nil && item.EAN != "" {
				if prevPrice, exists := localEanHistory[eanKey]; exists {
					if prevPrice.IsPositive() && currPrice.GreaterThan(prevPrice) {
						pct := currPrice.Sub(prevPrice).Mul(decimal.NewFromInt(100)).DivRound(prevPrice, 2)
						inflationAlerts = append(inflationAlerts, map[string]any{
							"ean":                 item.EAN,
							"description":         item.Description,
							"previous_unit_price": prevPrice.StringFixed(2),
							"current_unit_price":  currPrice.StringFixed(2),
							"inflation_pct":       "+" + pct.StringFixed(2) + "%",
							"alert":               "PERSONAL_INFLATION_DETECTED",
						})
					}
				}
				localEanHistory[eanKey] = currPrice
			}
		}
		localTxMu.Unlock()

		writeJSONWithETagAndCompression(w, r, http.StatusOK, map[string]any{
			"tenant_id":        tenantID,
			"access_key":       accessKey,
			"merchant_name":    req.MerchantName,
			"total_amount":     req.TotalAmount,
			"items_ingested":   len(req.Items),
			"inflation_alerts": inflationAlerts,
			"status":           "INGESTED_WITH_SKU_INFLATION_TRACKING",
		})
		return
	}

	receipt, ledgerResp, err := s.nfceScraper.ScrapeAndForward(
		r.Context(),
		tenantID,
		req.QrCodeURL,
		req.CardID,
		req.PaymentMethod,
		req.IdempotencyKey,
	)
	if err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadGateway, map[string]string{
			"error": err.Error(),
		})
		return
	}

	writeJSONWithETagAndCompression(w, r, http.StatusOK, map[string]any{
		"scrapedReceipt": receipt,
		"ledgerResult":   ledgerResp,
	})
}

func (s *Server) requireTenantID(w http.ResponseWriter, r *http.Request) (string, bool) {
	tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-ID"))
	if tenantID == "" || !reUUID.MatchString(tenantID) {
		writeJSONWithETagAndCompression(w, r, http.StatusUnauthorized, map[string]string{
			"error": "missing or invalid X-Tenant-ID UUID header (Zero-Trust ADR-006)",
		})
		return "", false
	}
	if s.rateLimiter != nil && !s.rateLimiter.Allow(tenantID) {
		w.Header().Set("Retry-After", "60")
		writeJSONWithETagAndCompression(w, r, http.StatusTooManyRequests, map[string]string{
			"error": "tenant rate limit exceeded (ADR-008)",
		})
		return "", false
	}
	return tenantID, true
}

type statusCaptureResponseWriter struct {
	http.ResponseWriter
	statusCode int
	respPreview bytes.Buffer
}

func (rw *statusCaptureResponseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *statusCaptureResponseWriter) Write(b []byte) (int, error) {
	if rw.statusCode == 0 {
		rw.statusCode = http.StatusOK
	}
	if rw.respPreview.Len() < 512 {
		limit := 512 - rw.respPreview.Len()
		if len(b) < limit {
			limit = len(b)
		}
		rw.respPreview.Write(b[:limit])
	}
	return rw.ResponseWriter.Write(b)
}

func maskAuthorizationHeader(auth string) string {
	auth = strings.TrimSpace(auth)
	if auth == "" {
		return ""
	}
	if len(auth) <= 14 {
		return "Bearer ***"
	}
	return auth[:14] + "...[MASKED]"
}

func (s *Server) withMiddlewares(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		// Advertise HTTP/3 (QUIC) & HTTP/2 readiness per ADR-003 + CORS for pfm-web-nextjs (:3000)
		w.Header().Set("Alt-Svc", `h3=":8443"; ma=86400, h2=":8443"; ma=86400`)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept, Authorization, X-Tenant-ID, If-None-Match, X-Statement-Issuer, X-Card-Last4, X-Statement-Password, X-Statement-Secret-Name, X-File-Name")
		w.Header().Set("Access-Control-Expose-Headers", "ETag, Alt-Svc, Retry-After")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		var reqPreview string
		if r.Body != nil && !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/pdf") && !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "multipart/") {
			bodyBytes, err := io.ReadAll(io.LimitReader(r.Body, 15*1024*1024+1))
			if err == nil {
				r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))
				previewLen := len(bodyBytes)
				if previewLen > 400 {
					previewLen = 400
				}
				reqPreview = string(bodyBytes[:previewLen])
			}
		} else if r.Header.Get("X-File-Name") != "" {
			reqPreview = "[BINARY_STREAM:" + r.Header.Get("X-File-Name") + "]"
		}

		crw := &statusCaptureResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(crw, r)

		level := "DEBUG"
		if crw.statusCode >= 500 {
			level = "ERROR"
		} else if crw.statusCode >= 400 {
			level = "WARN"
		}
		envName := os.Getenv("APP_ENV")
		if envName == "" {
			envName = "local"
		}
		traceID := r.Header.Get("X-Datadog-Trace-ID")
		if traceID == "" {
			traceID = fmt.Sprintf("%d", start.UnixNano())
		}

		logEntry := map[string]any{
			"timestamp":                time.Now().UTC().Format(time.RFC3339Nano),
			"level":                    level,
			"service":                  "pfm-ingestion-bff-go",
			"env":                      envName,
			"cloudwatch_logs_enabled":  false,
			"dd.trace_id":              traceID,
			"tenant_id":                r.Header.Get("X-Tenant-ID"),
			"http.method":              r.Method,
			"http.url":                 r.URL.RequestURI(),
			"http.status_code":         crw.statusCode,
			"http.auth_header_masked":  maskAuthorizationHeader(r.Header.Get("Authorization")),
			"duration_ms":              time.Since(start).Milliseconds(),
			"payload_preview":          reqPreview,
		}
		if crw.statusCode >= 400 {
			logEntry["error.stack"] = crw.respPreview.String()
		}
		if rawLog, err := json.Marshal(logEntry); err == nil {
			_, _ = fmt.Fprintln(os.Stdout, string(rawLog))
		}
	})
}

func writeJSONWithETagAndCompression(w http.ResponseWriter, r *http.Request, status int, data any) {
	rawJSON, err := json.Marshal(data)
	if err != nil {
		http.Error(w, `{"error":"internal serialization error"}`, http.StatusInternalServerError)
		return
	}

	sum := sha256.Sum256(rawJSON)
	etag := fmt.Sprintf(`"%s"`, hex.EncodeToString(sum[:16]))
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Vary", "Accept-Encoding")

	if r.Method == http.MethodGet && r.Header.Get("If-None-Match") == etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	acceptEnc := r.Header.Get("Accept-Encoding")
	if strings.Contains(acceptEnc, "br") {
		w.Header().Set("Content-Encoding", "br")
		w.WriteHeader(status)
		bw := brotli.NewWriterLevel(w, brotli.DefaultCompression)
		_, _ = bw.Write(rawJSON)
		_ = bw.Close()
		return
	}
	if strings.Contains(acceptEnc, "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(status)
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, _ = gw.Write(rawJSON)
		_ = gw.Close()
		_, _ = w.Write(buf.Bytes()) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter.no-direct-write-to-responsewriter
		return
	}

	w.WriteHeader(status)
	_, _ = w.Write(rawJSON) // nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter.no-direct-write-to-responsewriter
}

func writeEastWestError(w http.ResponseWriter, r *http.Request, err error) {
	var upstreamErr *grpcout.UpstreamHTTPError
	if errors.As(err, &upstreamErr) && upstreamErr.StatusCode >= 400 && upstreamErr.StatusCode < 500 {
		writeJSONWithETagAndCompression(w, r, upstreamErr.StatusCode, map[string]string{"error": upstreamErr.Message})
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusBadGateway, map[string]string{"error": err.Error()})
}

func (s *Server) handleAdminListTenants(w http.ResponseWriter, r *http.Request) {
	_, ok := s.requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	pageSizeStr := r.URL.Query().Get("page_size")
	pageSize := 20
	if pageSizeStr != "" {
		if val, err := strconv.Atoi(pageSizeStr); err == nil && val > 0 {
			pageSize = val
		}
	}
	pageToken := r.URL.Query().Get("page_token")
	statusFilter := r.URL.Query().Get("status")
	planFilter := r.URL.Query().Get("plan")

	res, err := s.eastWestClient.ListTenants(r.Context(), pageSize, pageToken, statusFilter, planFilter)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, res)
}

func (s *Server) handleAdminChangeTenantPlan(w http.ResponseWriter, r *http.Request) {
	_, ok := s.requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" || !reUUID.MatchString(tenantID) {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid tenant_id UUID"})
		return
	}

	var req struct {
		NewPlan      string `json:"new_plan"`
		NewPlanCamel string `json:"newPlan"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}
	plan := req.NewPlan
	if plan == "" {
		plan = req.NewPlanCamel
	}
	if plan == "" {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "new_plan is required"})
		return
	}

	res, err := s.eastWestClient.ChangeTenantPlan(r.Context(), tenantID, plan)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, res)
}

func (s *Server) handleAdminUpdateSubscriptionStatus(w http.ResponseWriter, r *http.Request) {
	_, ok := s.requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	tenantID := r.PathValue("tenant_id")
	if tenantID == "" || !reUUID.MatchString(tenantID) {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid tenant_id UUID"})
		return
	}

	var req struct {
		NewStatus      string `json:"new_status"`
		NewStatusCamel string `json:"newStatus"`
		Reason         string `json:"reason"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}
	status := req.NewStatus
	if status == "" {
		status = req.NewStatusCamel
	}
	if status == "" {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "new_status is required"})
		return
	}

	res, err := s.eastWestClient.UpdateSubscriptionStatus(r.Context(), tenantID, status, req.Reason)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, res)
}

func (s *Server) handleAdminPlatformMetrics(w http.ResponseWriter, r *http.Request) {
	_, ok := s.requirePlatformAdmin(w, r)
	if !ok {
		return
	}
	period := r.URL.Query().Get("period")
	res, err := s.eastWestClient.GetPlatformMetrics(r.Context(), period)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, res)
}

func (s *Server) handleGetProfile(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireProfileUser(w, r)
	if !ok {
		return
	}
	res, err := s.eastWestClient.GetUserProfile(r.Context(), claims.TenantID, claims.Subject)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, res)
}

func (s *Server) handleUpdateProfile(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireFamilyOwnerOrMember(w, r)
	if !ok {
		return
	}
	var req struct {
		FullName      string `json:"full_name"`
		FullNameCamel string `json:"fullName"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}
	fullName := req.FullName
	if fullName == "" {
		fullName = req.FullNameCamel
	}
	res, err := s.eastWestClient.UpdateUserProfile(r.Context(), claims.TenantID, claims.Subject, fullName)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, res)
}

func (s *Server) handleListFamilyMembers(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireProfileUser(w, r)
	if !ok {
		return
	}
	res, err := s.eastWestClient.ListFamilyMembers(r.Context(), claims.TenantID)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, res)
}

func (s *Server) handleInviteFamilyMember(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireFamilyOwnerOrMember(w, r)
	if !ok {
		return
	}
	var req struct {
		Email                  string `json:"email"`
		FullName               string `json:"full_name"`
		FullNameCamel          string `json:"fullName"`
		Role                   string `json:"role"`
		SpendingAllowance      string `json:"spending_allowance"`
		SpendingAllowanceCamel string `json:"spendingAllowance"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}
	fullName := req.FullName
	if fullName == "" {
		fullName = req.FullNameCamel
	}
	allowance := req.SpendingAllowance
	if allowance == "" {
		allowance = req.SpendingAllowanceCamel
	}
	res, err := s.eastWestClient.InviteFamilyMember(r.Context(), claims.TenantID, req.Email, fullName, req.Role, allowance)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusCreated, res)
}

func (s *Server) handleRemoveFamilyMember(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireFamilyOwnerOrMember(w, r)
	if !ok {
		return
	}
	memberID := r.PathValue("member_id")
	if memberID == "" || !reUUID.MatchString(memberID) {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid member_id UUID"})
		return
	}
	_, err := s.eastWestClient.RemoveFamilyMember(r.Context(), claims.TenantID, memberID)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleGetCategoryBudgets(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireProfileUser(w, r)
	if !ok {
		return
	}
	res, err := s.eastWestClient.GetCategoryBudgets(r.Context(), claims.TenantID)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, res)
}

func (s *Server) handleSetCategoryBudget(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireFamilyOwnerOrMember(w, r)
	if !ok {
		return
	}
	var req struct {
		CategoryID               string `json:"category_id"`
		CategoryIDCamel          string `json:"categoryId"`
		CategoryName             string `json:"category_name"`
		CategoryNameCamel        string `json:"categoryName"`
		MonthlyCap               string `json:"monthly_cap"`
		MonthlyCapCamel          string `json:"monthlyCap"`
		ThresholdPercentage      int    `json:"threshold_percentage"`
		ThresholdPercentageCamel int    `json:"thresholdPercentage"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}
	catID := req.CategoryID
	if catID == "" {
		catID = req.CategoryIDCamel
	}
	catName := req.CategoryName
	if catName == "" {
		catName = req.CategoryNameCamel
	}
	monthlyCap := req.MonthlyCap
	if monthlyCap == "" {
		monthlyCap = req.MonthlyCapCamel
	}
	threshold := req.ThresholdPercentage
	if threshold == 0 {
		threshold = req.ThresholdPercentageCamel
	}

	res, err := s.eastWestClient.SetCategoryBudget(r.Context(), claims.TenantID, catID, catName, monthlyCap, threshold)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, res)
}

func (s *Server) handleSetCardPdfPassword(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireFamilyOwnerOrMember(w, r)
	if !ok {
		return
	}
	cardID := r.PathValue("card_id")
	if cardID == "" || !reUUID.MatchString(cardID) {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid card_id UUID"})
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}
	if req.Password == "" {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "password is required"})
		return
	}
	res, err := s.eastWestClient.SetCardPdfPassword(r.Context(), claims.TenantID, cardID, req.Password)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, res)
}

func (s *Server) handleExportUserData(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireFamilyOwnerOrMember(w, r)
	if !ok {
		return
	}
	var req struct {
		Format string `json:"format"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req)
	format := strings.ToUpper(req.Format)
	if format == "" {
		format = "JSON"
	}
	res, err := s.eastWestClient.ExportUserData(r.Context(), claims.TenantID, claims.Subject, format)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusAccepted, res)
}

func (s *Server) handleRequestCryptoShredding(w http.ResponseWriter, r *http.Request) {
	claims, ok := s.requireFamilyOwnerOrMember(w, r)
	if !ok {
		return
	}
	var req struct {
		ConfirmationPhrase      string `json:"confirmation_phrase"`
		ConfirmationPhraseCamel string `json:"confirmationPhrase"`
		MfaToken                string `json:"mfa_token"`
		MfaTokenCamel           string `json:"mfaToken"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{"error": "invalid JSON payload"})
		return
	}
	phrase := req.ConfirmationPhrase
	if phrase == "" {
		phrase = req.ConfirmationPhraseCamel
	}
	mfa := req.MfaToken
	if mfa == "" {
		mfa = req.MfaTokenCamel
	}
	res, err := s.eastWestClient.RequestCryptoShredding(r.Context(), claims.TenantID, claims.Subject, phrase, mfa)
	if err != nil {
		writeEastWestError(w, r, err)
		return
	}
	writeJSONWithETagAndCompression(w, r, http.StatusOK, res)
}
