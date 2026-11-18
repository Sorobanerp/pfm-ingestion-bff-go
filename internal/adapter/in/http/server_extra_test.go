package http_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	httpin "github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/in/http"
	grpcout "github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/grpc"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/sefaz"
	in "github.com/dantas-pfm/pfm-ingestion-bff-go/internal/application/port/in"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/domain"
	"github.com/stretchr/testify/assert"
)

type mockIngestUseCase struct {
	err error
	res *in.UploadStatementResult
}

func (m *mockIngestUseCase) IngestStatement(_ context.Context, _ in.UploadStatementCommand) (*in.UploadStatementResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.res, nil
}

func TestServer_DashboardAndContractRoutes(t *testing.T) {
	eastWest := grpcout.NewEastWestGrpcBridgeClient("", "")
	scraper := sefaz.NewSefazNfceScraper(nil)
	limiter := httpin.NewTenantRateLimiter(100, time.Minute)

	srv := httpin.NewServer(nil).
		WithEastWestClient(eastWest).
		WithNfceScraper(scraper).
		WithRateLimiter(limiter)

	handler := srv.Handler()

	// 1. Health check
	reqHealth := httptest.NewRequest(http.MethodGet, "/health", nil)
	recHealth := httptest.NewRecorder()
	handler.ServeHTTP(recHealth, reqHealth)
	assert.Equal(t, http.StatusOK, recHealth.Code)

	// 2. Dashboard without tenant header -> 401
	reqDashNoAuth := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard", nil)
	recDashNoAuth := httptest.NewRecorder()
	handler.ServeHTTP(recDashNoAuth, reqDashNoAuth)
	assert.Equal(t, http.StatusUnauthorized, recDashNoAuth.Code)

	// 3. Dashboard with valid tenant
	reqDash := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard?scope=HOLDER_DANTAS", nil)
	reqDash.Header.Set("X-Tenant-ID", "11111111-1111-4111-8111-111111111111")
	reqDash.Header.Set("Accept-Encoding", "gzip")
	recDash := httptest.NewRecorder()
	handler.ServeHTTP(recDash, reqDash)
	assert.Equal(t, http.StatusOK, recDash.Code)

	// ETag match -> 304 Not Modified
	etag := recDash.Header().Get("ETag")
	if etag != "" {
		reqEtag := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard?scope=HOLDER_DANTAS", nil)
		reqEtag.Header.Set("X-Tenant-ID", "11111111-1111-4111-8111-111111111111")
		reqEtag.Header.Set("If-None-Match", etag)
		recEtag := httptest.NewRecorder()
		handler.ServeHTTP(recEtag, reqEtag)
		assert.Equal(t, http.StatusNotModified, recEtag.Code)
	}

	// 4. Contract simulate without tenant -> 401
	reqSimNoAuth := httptest.NewRequest(http.MethodPost, "/api/v1/contracts/simulate", bytes.NewReader([]byte("{}")))
	recSimNoAuth := httptest.NewRecorder()
	handler.ServeHTTP(recSimNoAuth, reqSimNoAuth)
	assert.Equal(t, http.StatusUnauthorized, recSimNoAuth.Code)

	// 5. Contract simulate with valid tenant and bad JSON -> 400
	reqSimBad := httptest.NewRequest(http.MethodPost, "/api/v1/contracts/simulate", bytes.NewReader([]byte("{invalid")))
	reqSimBad.Header.Set("X-Tenant-ID", "11111111-1111-4111-8111-111111111111")
	recSimBad := httptest.NewRecorder()
	handler.ServeHTTP(recSimBad, reqSimBad)
	assert.Equal(t, http.StatusBadRequest, recSimBad.Code)

	// 6. Contract simulate valid
	payload := `{"outstanding_balance":"100000.00","remaining_installments":120,"extra_amortization_amount":"10000.00"}`
	reqSimOk := httptest.NewRequest(http.MethodPost, "/api/v1/contracts/simulate", bytes.NewReader([]byte(payload)))
	reqSimOk.Header.Set("X-Tenant-ID", "11111111-1111-4111-8111-111111111111")
	recSimOk := httptest.NewRecorder()
	handler.ServeHTTP(recSimOk, reqSimOk)
	assert.Equal(t, http.StatusOK, recSimOk.Code)

	// 7. NFC-e upload invalid JSON
	reqNfceBad := httptest.NewRequest(http.MethodPost, "/api/v1/receipts/nfce", bytes.NewReader([]byte("{invalid")))
	reqNfceBad.Header.Set("X-Tenant-ID", "11111111-1111-4111-8111-111111111111")
	recNfceBad := httptest.NewRecorder()
	handler.ServeHTTP(recNfceBad, reqNfceBad)
	assert.Equal(t, http.StatusBadRequest, recNfceBad.Code)
}

func TestServer_UploadStatementErrorHandling(t *testing.T) {
	// 1. Checksum mismatch -> 409
	useCaseChecksumErr := &mockIngestUseCase{err: domain.ErrChecksumMismatch}
	srvChecksum := httpin.NewServer(useCaseChecksumErr)
	req1 := httptest.NewRequest(http.MethodPost, "/api/v1/statements/upload", bytes.NewReader([]byte("dummy")))
	req1.Header.Set("X-Tenant-ID", "22222222-2222-4222-8222-222222222222")
	rec1 := httptest.NewRecorder()
	srvChecksum.Handler().ServeHTTP(rec1, req1)
	assert.Equal(t, http.StatusConflict, rec1.Code)

	// 2. Generic error -> 422
	useCaseGenericErr := &mockIngestUseCase{err: errors.New("parse error")}
	srvGeneric := httpin.NewServer(useCaseGenericErr)
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/statements/upload", bytes.NewReader([]byte("dummy")))
	req2.Header.Set("X-Tenant-ID", "22222222-2222-4222-8222-222222222222")
	rec2 := httptest.NewRecorder()
	srvGeneric.Handler().ServeHTTP(rec2, req2)
	assert.Equal(t, http.StatusUnprocessableEntity, rec2.Code)

	// 3. Rate limit exceeded -> 429
	limiter := httpin.NewTenantRateLimiter(1, time.Minute)
	srvRate := httpin.NewServer(nil).WithRateLimiter(limiter)
	reqRate1 := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard", nil)
	reqRate1.Header.Set("X-Tenant-ID", "33333333-3333-4333-8333-333333333333")
	recRate1 := httptest.NewRecorder()
	srvRate.Handler().ServeHTTP(recRate1, reqRate1)
	assert.Equal(t, http.StatusOK, recRate1.Code)

	reqRate2 := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard", nil)
	reqRate2.Header.Set("X-Tenant-ID", "33333333-3333-4333-8333-333333333333")
	recRate2 := httptest.NewRecorder()
	srvRate.Handler().ServeHTTP(recRate2, reqRate2)
	assert.Equal(t, http.StatusTooManyRequests, recRate2.Code)
}

func TestServer_NfceAndStatementSuccessPaths(t *testing.T) {
	scraper := sefaz.NewSefazNfceScraper(nil)
	useCaseSuccess := &mockIngestUseCase{
		res: &in.UploadStatementResult{
			StatementID: "stmt-success-1",
		},
	}

	srv := httpin.NewServer(useCaseSuccess).WithNfceScraper(scraper)
	handler := srv.Handler()

	// NFC-e upload success via gov.br emulation
	nfcePayload := `{"qrCodeUrl":"https://www.sefaz.rs.gov.br/NFCE/NFCE-COM.aspx?p=432611","cardId":"card-1","paymentMethod":"CREDIT_CARD","idempotencyKey":"sha256-idemp-000000000000001"}`
	reqNfceOk := httptest.NewRequest(http.MethodPost, "/api/v1/receipts/nfce", bytes.NewReader([]byte(nfcePayload)))
	reqNfceOk.Header.Set("X-Tenant-ID", "44444444-4444-4444-8444-444444444444")
	recNfceOk := httptest.NewRecorder()
	handler.ServeHTTP(recNfceOk, reqNfceOk)
	assert.Equal(t, http.StatusOK, recNfceOk.Code)

	// Statement upload success
	reqStmtOk := httptest.NewRequest(http.MethodPost, "/api/v1/statements/upload", bytes.NewReader([]byte("dummy content")))
	reqStmtOk.Header.Set("X-Tenant-ID", "44444444-4444-4444-8444-444444444444")
	recStmtOk := httptest.NewRecorder()
	handler.ServeHTTP(recStmtOk, reqStmtOk)
	assert.Equal(t, http.StatusOK, recStmtOk.Code)
}

func TestServer_BrotliAndCorsAndNfceError(t *testing.T) {
	srv := httpin.NewServer(nil)
	handler := srv.Handler()

	// 1. Brotli compression header
	reqBr := httptest.NewRequest(http.MethodGet, "/health", nil)
	reqBr.Header.Set("Accept-Encoding", "br")
	recBr := httptest.NewRecorder()
	handler.ServeHTTP(recBr, reqBr)
	assert.Equal(t, http.StatusOK, recBr.Code)
	assert.Equal(t, "br", recBr.Header().Get("Content-Encoding"))

	// 2. CORS Origin header
	reqCors := httptest.NewRequest(http.MethodOptions, "/api/v1/dashboard", nil)
	reqCors.Header.Set("Origin", "http://localhost:3000")
	recCors := httptest.NewRecorder()
	handler.ServeHTTP(recCors, reqCors)
	assert.Equal(t, "*", recCors.Header().Get("Access-Control-Allow-Origin"))

	// 3. NFC-e error path (empty URL in scraper -> 502)
	reqNfceFail := httptest.NewRequest(http.MethodPost, "/api/v1/receipts/nfce", bytes.NewReader([]byte(`{"qrCodeUrl":""}`)))
	reqNfceFail.Header.Set("X-Tenant-ID", "77777777-7777-4777-8777-777777777777")
	recNfceFail := httptest.NewRecorder()
	handler.ServeHTTP(recNfceFail, reqNfceFail)
	assert.Equal(t, http.StatusBadGateway, recNfceFail.Code)
}

func TestServer_ContractSimulateErrorPath(t *testing.T) {
	srv := httpin.NewServer(nil)
	payload := `{"outstanding_balance":"0.00"}`
	req := httptest.NewRequest(http.MethodPost, "/api/v1/contracts/simulate", bytes.NewReader([]byte(payload)))
	req.Header.Set("X-Tenant-ID", "99999999-9999-4999-8999-999999999999")
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnprocessableEntity, rec.Code)
}

func TestServer_MissingTenantOnUploadsAndMarshalError(t *testing.T) {
	srv := httpin.NewServer(nil)
	handler := srv.Handler()

	// 1. Missing tenant on statement upload -> 401
	reqStmtNoAuth := httptest.NewRequest(http.MethodPost, "/api/v1/statements/upload", bytes.NewReader([]byte("bytes")))
	recStmtNoAuth := httptest.NewRecorder()
	handler.ServeHTTP(recStmtNoAuth, reqStmtNoAuth)
	assert.Equal(t, http.StatusUnauthorized, recStmtNoAuth.Code)

	// 2. Missing tenant on NFC-e upload -> 401
	reqNfceNoAuth := httptest.NewRequest(http.MethodPost, "/api/v1/receipts/nfce", bytes.NewReader([]byte("{}")))
	recNfceNoAuth := httptest.NewRecorder()
	handler.ServeHTTP(recNfceNoAuth, reqNfceNoAuth)
	assert.Equal(t, http.StatusUnauthorized, recNfceNoAuth.Code)
}

func TestTenantRateLimiter_DefaultsAndEmptyTenant(t *testing.T) {
	// Defaults when non-positive
	rl := httpin.NewTenantRateLimiter(0, 0)
	assert.NotNil(t, rl)
	assert.True(t, rl.Allow(""))

	var nilRl *httpin.TenantRateLimiter
	assert.True(t, nilRl.Allow("any"))
}
