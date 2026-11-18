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
	"regexp"
	"strings"
	"time"

	"github.com/andybalholm/brotli"
	grpcout "github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/grpc"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/sefaz"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/application/port/in"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/domain"
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
	var req grpcout.ContractSimulateRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{
			"error": "invalid contract simulation JSON payload",
		})
		return
	}
	req.TenantID = tenantID
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

	cmd := in.UploadStatementCommand{
		TenantID:      tenantID,
		Issuer:        domain.Issuer(issuerStr),
		CardLast4:     cardLast4,
		Password:      password,
		SecretName:    secretName,
		FileName:      r.Header.Get("X-File-Name"),
		PDFPayloadRAM: rawBody,
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

	writeJSONWithETagAndCompression(w, r, http.StatusOK, res)
}

func (s *Server) handleNfceReceiptUpload(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := s.requireTenantID(w, r)
	if !ok {
		return
	}

	var req NfceReceiptScanRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		writeJSONWithETagAndCompression(w, r, http.StatusBadRequest, map[string]string{
			"error": "invalid NFC-e JSON payload",
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

func (s *Server) withMiddlewares(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Advertise HTTP/3 (QUIC) & HTTP/2 readiness per ADR-003 + CORS for pfm-web-nextjs (:3000)
		w.Header().Set("Alt-Svc", `h3=":8443"; ma=86400, h2=":8443"; ma=86400`)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Accept, X-Tenant-ID, If-None-Match, X-Statement-Issuer, X-Card-Last4, X-Statement-Password, X-Statement-Secret-Name, X-File-Name")
		w.Header().Set("Access-Control-Expose-Headers", "ETag, Alt-Svc, Retry-After")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
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
