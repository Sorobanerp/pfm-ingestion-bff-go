package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"

	httpin "github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/in/http"
	grpcout "github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/grpc"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/application/port/in"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type stubIngestUC struct{}

func (s *stubIngestUC) IngestStatement(_ context.Context, cmd in.UploadStatementCommand) (*in.UploadStatementResult, error) {
	return &in.UploadStatementResult{
		StatementID:    "stmt-1",
		TenantID:       cmd.TenantID,
		Issuer:         cmd.Issuer,
		ReferenceMonth: "2026-11",
		ChecksumValid:  true,
	}, nil
}

func TestSwaggerAndDevTokenSecurityLock(t *testing.T) {
	srv := httpin.NewServer(nil)
	handler := srv.Handler()

	// 1. Swagger UI & OpenAPI spec
	for _, path := range []string{"/swagger/index.html", "/swagger/openapi.yaml"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	}

	// 2. Outside APP_ENV=local/development, dev-token endpoint & dev-* tokens MUST be blocked
	t.Setenv("APP_ENV", "prod")
	reqBlocked := httptest.NewRequest(http.MethodPost, "/api/v1/auth/dev-token", bytes.NewBufferString(`{"role":"OWNER"}`))
	recBlocked := httptest.NewRecorder()
	handler.ServeHTTP(recBlocked, reqBlocked)
	assert.Equal(t, http.StatusForbidden, recBlocked.Code)

	reqDevTokenProd := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants", nil)
	reqDevTokenProd.Header.Set("Authorization", "Bearer dev-admin-token")
	recDevTokenProd := httptest.NewRecorder()
	handler.ServeHTTP(recDevTokenProd, reqDevTokenProd)
	assert.Equal(t, http.StatusForbidden, recDevTokenProd.Code)

	// 3. Inside APP_ENV=local, dev-token endpoint and deterministic dev-* tokens work
	t.Setenv("APP_ENV", "local")
	for _, body := range []string{
		`{"role":"OWNER"}`,
		`{"role":"PLATFORM_ADMIN"}`,
		`{"role":"VIEWER"}`,
		`{"tenant_id":"22222222-2222-2222-2222-222222222222"}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/api/v1/auth/dev-token", bytes.NewBufferString(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusOK, rec.Code)
	}
}

func TestLocalCrudInboxNfceAndStatementValidation(t *testing.T) {
	t.Setenv("APP_ENV", "local")
	srv := httpin.NewServer(nil)
	handler := srv.Handler()

	tenantA := "11111111-1111-1111-1111-111111111111"
	tenantB := "22222222-2222-2222-2222-222222222222"

	// 1. Admin Create Tenant local
	reqTenant := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants", bytes.NewBufferString(`{"tenant_id":"`+tenantA+`","tenant_name":"Familia Dantas","owner_email":"d@pfm.br","plan":"FAMILY_PLUS"}`))
	reqTenant.Header.Set("Authorization", "Bearer dev-admin-token")
	recTenant := httptest.NewRecorder()
	handler.ServeHTTP(recTenant, reqTenant)
	assert.Equal(t, http.StatusCreated, recTenant.Code)

	// 2. Cards CRUD
	reqCard := httptest.NewRequest(http.MethodPost, "/api/v1/cards", bytes.NewBufferString(`{"issuer":"ITAU","last4":"5285","credit_limit":"15000.00"}`))
	reqCard.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqCard.Header.Set("X-Tenant-ID", tenantA)
	recCard := httptest.NewRecorder()
	handler.ServeHTTP(recCard, reqCard)
	assert.Equal(t, http.StatusCreated, recCard.Code)

	reqCardBad := httptest.NewRequest(http.MethodPost, "/api/v1/cards", bytes.NewBufferString(`{"credit_limit":"0"}`))
	reqCardBad.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqCardBad.Header.Set("X-Tenant-ID", tenantA)
	recCardBad := httptest.NewRecorder()
	handler.ServeHTTP(recCardBad, reqCardBad)
	assert.Equal(t, http.StatusBadRequest, recCardBad.Code)

	reqListCards := httptest.NewRequest(http.MethodGet, "/api/v1/cards", nil)
	reqListCards.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqListCards.Header.Set("X-Tenant-ID", tenantA)
	recListCards := httptest.NewRecorder()
	handler.ServeHTTP(recListCards, reqListCards)
	assert.Equal(t, http.StatusOK, recListCards.Code)

	// 3. Transactions CRUD (10x installment + PATCH + Refund + Cross-Tenant 404 + Sad paths + VIEWER 403)
	reqTx := httptest.NewRequest(http.MethodPost, "/api/v1/transactions", bytes.NewBufferString(`{"description":"APPLE STORE","amount":"3500.00","installments":10,"transaction_date":"2026-11-12"}`))
	reqTx.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqTx.Header.Set("X-Tenant-ID", tenantA)
	recTx := httptest.NewRecorder()
	handler.ServeHTTP(recTx, reqTx)
	require.Equal(t, http.StatusCreated, recTx.Code)

	var txResp map[string]any
	require.NoError(t, json.Unmarshal(recTx.Body.Bytes(), &txResp))
	txID, _ := txResp["id"].(string)
	require.NotEmpty(t, txID)

	for _, badBody := range []string{
		`{"description":"X","amount":"-10.00"}`,
		`{"description":"","amount":"10.00"}`,
		`{"description":"X","amount":"10.00","transaction_date":"2026-99-99"}`,
		`{"description":"X","amount":"10.00","installments":0}`,
		`{bad`,
	} {
		reqBad := httptest.NewRequest(http.MethodPost, "/api/v1/transactions", bytes.NewBufferString(badBody))
		reqBad.Header.Set("Authorization", "Bearer dev-token-tenant-a")
		reqBad.Header.Set("X-Tenant-ID", tenantA)
		recBad := httptest.NewRecorder()
		handler.ServeHTTP(recBad, reqBad)
		assert.Equal(t, http.StatusBadRequest, recBad.Code)
	}

	reqViewerDel := httptest.NewRequest(http.MethodDelete, "/api/v1/transactions/"+txID, nil)
	reqViewerDel.Header.Set("Authorization", "Bearer dev-viewer-tenant-a")
	reqViewerDel.Header.Set("X-Tenant-ID", tenantA)
	recViewerDel := httptest.NewRecorder()
	handler.ServeHTTP(recViewerDel, reqViewerDel)
	assert.Equal(t, http.StatusForbidden, recViewerDel.Code)

	reqCross := httptest.NewRequest(http.MethodGet, "/api/v1/transactions/"+txID, nil)
	reqCross.Header.Set("Authorization", "Bearer dev-token-tenant-b")
	reqCross.Header.Set("X-Tenant-ID", tenantB)
	recCross := httptest.NewRecorder()
	handler.ServeHTTP(recCross, reqCross)
	assert.Equal(t, http.StatusNotFound, recCross.Code)

	reqGetTx := httptest.NewRequest(http.MethodGet, "/api/v1/transactions/"+txID, nil)
	reqGetTx.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqGetTx.Header.Set("X-Tenant-ID", tenantA)
	recGetTx := httptest.NewRecorder()
	handler.ServeHTTP(recGetTx, reqGetTx)
	assert.Equal(t, http.StatusOK, recGetTx.Code)

	reqPatch := httptest.NewRequest(http.MethodPatch, "/api/v1/transactions/"+txID, bytes.NewBufferString(`{"category_code":"ALIMENTACAO :: SUPERMERCADO"}`))
	reqPatch.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqPatch.Header.Set("X-Tenant-ID", tenantA)
	recPatch := httptest.NewRecorder()
	handler.ServeHTTP(recPatch, reqPatch)
	assert.Equal(t, http.StatusOK, recPatch.Code)

	reqRefund := httptest.NewRequest(http.MethodPost, "/api/v1/transactions/"+txID+"/refund", bytes.NewBufferString(`{}`))
	reqRefund.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqRefund.Header.Set("X-Tenant-ID", tenantA)
	recRefund := httptest.NewRecorder()
	handler.ServeHTTP(recRefund, reqRefund)
	assert.Equal(t, http.StatusCreated, recRefund.Code)

	reqListTx := httptest.NewRequest(http.MethodGet, "/api/v1/transactions", nil)
	reqListTx.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqListTx.Header.Set("X-Tenant-ID", tenantA)
	recListTx := httptest.NewRecorder()
	handler.ServeHTTP(recListTx, reqListTx)
	assert.Equal(t, http.StatusOK, recListTx.Code)

	reqAdminListTx := httptest.NewRequest(http.MethodGet, "/api/v1/transactions", nil)
	reqAdminListTx.Header.Set("Authorization", "Bearer dev-admin-token")
	reqAdminListTx.Header.Set("X-Tenant-ID", tenantA)
	recAdminListTx := httptest.NewRecorder()
	handler.ServeHTTP(recAdminListTx, reqAdminListTx)
	assert.Equal(t, http.StatusForbidden, recAdminListTx.Code)

	// 4. Dashboard summary, Inbox & DLQ
	reqDash := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/summary?horizon=TODAY", nil)
	reqDash.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqDash.Header.Set("X-Tenant-ID", tenantA)
	recDash := httptest.NewRecorder()
	handler.ServeHTTP(recDash, reqDash)
	assert.Equal(t, http.StatusOK, recDash.Code)

	reqInbox := httptest.NewRequest(http.MethodGet, "/api/v1/inbox", nil)
	reqInbox.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqInbox.Header.Set("X-Tenant-ID", tenantA)
	recInbox := httptest.NewRecorder()
	handler.ServeHTTP(recInbox, reqInbox)
	assert.Equal(t, http.StatusOK, recInbox.Code)

	reqConfirm := httptest.NewRequest(http.MethodPost, "/api/v1/inbox/inbox-001/confirm", bytes.NewBufferString(`{"learn_rule":true}`))
	reqConfirm.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqConfirm.Header.Set("X-Tenant-ID", tenantA)
	recConfirm := httptest.NewRecorder()
	handler.ServeHTTP(recConfirm, reqConfirm)
	assert.Equal(t, http.StatusOK, recConfirm.Code)

	reqDlq := httptest.NewRequest(http.MethodPost, "/api/v1/inbox/dlq-test", bytes.NewBufferString(`{"corrupted":true}`))
	reqDlq.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqDlq.Header.Set("X-Tenant-ID", tenantA)
	recDlq := httptest.NewRecorder()
	handler.ServeHTTP(recDlq, reqDlq)
	assert.Equal(t, http.StatusAccepted, recDlq.Code)

	// 5. NFC-e SKU Inflation + 409 Duplicate
	nfce1 := `{"access_key":"35261112345678000199650010000123451000888001","merchant_name":"M1","total_amount":"39.90","items":[{"ean":"7891234567890","description":"Azeite","unit_price":"39.90"}]}`
	nfce2 := `{"access_key":"35261112345678000199650010000123451000888002","merchant_name":"M2","total_amount":"47.90","items":[{"ean":"7891234567890","description":"Azeite","unit_price":"47.90"}]}`
	for i, body := range []string{nfce1, nfce2, nfce1} {
		reqN := httptest.NewRequest(http.MethodPost, "/api/v1/receipts/nfce", bytes.NewBufferString(body))
		reqN.Header.Set("Authorization", "Bearer dev-token-tenant-a")
		reqN.Header.Set("X-Tenant-ID", tenantA)
		recN := httptest.NewRecorder()
		handler.ServeHTTP(recN, reqN)
		if i == 2 {
			assert.Equal(t, http.StatusConflict, recN.Code)
		} else {
			assert.Equal(t, http.StatusOK, recN.Code)
		}
	}

	// 6. Statement upload .exe (415) & empty (400) & multipart
	reqExe := httptest.NewRequest(http.MethodPost, "/api/v1/statements/upload", bytes.NewBufferString("MZ_EXE"))
	reqExe.Header.Set("X-Tenant-ID", tenantA)
	reqExe.Header.Set("X-File-Name", "bad.exe")
	recExe := httptest.NewRecorder()
	handler.ServeHTTP(recExe, reqExe)
	assert.Equal(t, http.StatusUnsupportedMediaType, recExe.Code)

	var mpBuf bytes.Buffer
	mw := multipart.NewWriter(&mpBuf)
	fw, _ := mw.CreateFormFile("file", "empty.pdf")
	_, _ = fw.Write([]byte{})
	_ = mw.Close()
	reqEmpty := httptest.NewRequest(http.MethodPost, "/api/v1/statements/upload", &mpBuf)
	reqEmpty.Header.Set("X-Tenant-ID", tenantA)
	reqEmpty.Header.Set("Content-Type", mw.FormDataContentType())
	recEmpty := httptest.NewRecorder()
	handler.ServeHTTP(recEmpty, reqEmpty)
	assert.Equal(t, http.StatusBadRequest, recEmpty.Code)

	// 7. UpstreamHTTPError 4xx propagation
	err400 := &grpcout.UpstreamHTTPError{StatusCode: http.StatusBadRequest, Message: "invalid uuid"}
	assert.Contains(t, err400.Error(), "400")

	// 8. Contract simulate annual_interest_rate + 400 validation
	for _, tc := range []struct {
		body string
		code int
	}{
		{`{"outstanding_balance":"320000.00","annual_interest_rate":"9.49","remaining_installments":240,"extra_amortization_amount":"50000.00"}`, http.StatusOK},
		{`{"outstanding_balance":"320000.00","remaining_installments":240,"extra_amortization_amount":"400000.00"}`, http.StatusBadRequest},
		{`{"outstanding_balance":"320000.00","remaining_installments":0,"extra_amortization_amount":"1000.00"}`, http.StatusBadRequest},
	} {
		reqSim := httptest.NewRequest(http.MethodPost, "/api/v1/contracts/simulate", bytes.NewBufferString(tc.body))
		reqSim.Header.Set("X-Tenant-ID", tenantA)
		recSim := httptest.NewRecorder()
		handler.ServeHTTP(recSim, reqSim)
		assert.Equal(t, tc.code, recSim.Code)
	}

	// 9. Additional local branch coverage (VIEWER card 403, bad JSON, 404s, X-User-Role/Id in local)
	reqTenantDefault := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants", bytes.NewBufferString(`{}`))
	reqTenantDefault.Header.Set("Authorization", "Bearer dev-admin-token")
	recTenantDefault := httptest.NewRecorder()
	handler.ServeHTTP(recTenantDefault, reqTenantDefault)
	assert.Equal(t, http.StatusCreated, recTenantDefault.Code)

	reqTenantBad := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants", bytes.NewBufferString(`{bad`))
	reqTenantBad.Header.Set("Authorization", "Bearer dev-admin-token")
	recTenantBad := httptest.NewRecorder()
	handler.ServeHTTP(recTenantBad, reqTenantBad)
	assert.Equal(t, http.StatusBadRequest, recTenantBad.Code)

	reqCardViewer := httptest.NewRequest(http.MethodPost, "/api/v1/cards", bytes.NewBufferString(`{"credit_limit":"100.00"}`))
	reqCardViewer.Header.Set("Authorization", "Bearer dev-viewer-tenant-a")
	reqCardViewer.Header.Set("X-Tenant-ID", tenantA)
	recCardViewer := httptest.NewRecorder()
	handler.ServeHTTP(recCardViewer, reqCardViewer)
	assert.Equal(t, http.StatusForbidden, recCardViewer.Code)

	reqCardBadJson := httptest.NewRequest(http.MethodPost, "/api/v1/cards", bytes.NewBufferString(`{bad`))
	reqCardBadJson.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqCardBadJson.Header.Set("X-Tenant-ID", tenantA)
	recCardBadJson := httptest.NewRecorder()
	handler.ServeHTTP(recCardBadJson, reqCardBadJson)
	assert.Equal(t, http.StatusBadRequest, recCardBadJson.Code)

	reqPatchViewer := httptest.NewRequest(http.MethodPatch, "/api/v1/transactions/nonexistent", bytes.NewBufferString(`{}`))
	reqPatchViewer.Header.Set("Authorization", "Bearer dev-viewer-tenant-a")
	reqPatchViewer.Header.Set("X-Tenant-ID", tenantA)
	recPatchViewer := httptest.NewRecorder()
	handler.ServeHTTP(recPatchViewer, reqPatchViewer)
	assert.Equal(t, http.StatusForbidden, recPatchViewer.Code)

	reqPatchNotFound := httptest.NewRequest(http.MethodPatch, "/api/v1/transactions/nonexistent", bytes.NewBufferString(`{"description":"X"}`))
	reqPatchNotFound.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqPatchNotFound.Header.Set("X-Tenant-ID", tenantA)
	recPatchNotFound := httptest.NewRecorder()
	handler.ServeHTTP(recPatchNotFound, reqPatchNotFound)
	assert.Equal(t, http.StatusNotFound, recPatchNotFound.Code)

	reqRefundNotFound := httptest.NewRequest(http.MethodPost, "/api/v1/transactions/nonexistent/refund", bytes.NewBufferString(`{}`))
	reqRefundNotFound.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqRefundNotFound.Header.Set("X-Tenant-ID", tenantA)
	recRefundNotFound := httptest.NewRecorder()
	handler.ServeHTTP(recRefundNotFound, reqRefundNotFound)
	assert.Equal(t, http.StatusNotFound, recRefundNotFound.Code)

	reqOverride := httptest.NewRequest(http.MethodGet, "/api/v1/cards", nil)
	reqOverride.Header.Set("Authorization", "Bearer OWNER")
	reqOverride.Header.Set("X-User-Role", "OWNER")
	reqOverride.Header.Set("X-User-Id", "11111111-1111-1111-1111-111111111112")
	reqOverride.Header.Set("X-Tenant-ID", tenantA)
	recOverride := httptest.NewRecorder()
	handler.ServeHTTP(recOverride, reqOverride)
	assert.Equal(t, http.StatusOK, recOverride.Code)

	// 10. Cover dev-token-tenant-b, invalid 43-char NFC-e key, invalid multipart, and mock EastWest CreateTenant
	reqTenantB := httptest.NewRequest(http.MethodGet, "/api/v1/cards", nil)
	reqTenantB.Header.Set("Authorization", "Bearer dev-token-tenant-b")
	reqTenantB.Header.Set("X-Tenant-ID", tenantB)
	recTenantB := httptest.NewRecorder()
	handler.ServeHTTP(recTenantB, reqTenantB)
	assert.Equal(t, http.StatusOK, recTenantB.Code)

	reqNfceShort := httptest.NewRequest(http.MethodPost, "/api/v1/receipts/nfce", bytes.NewBufferString(`{"access_key":"12345"}`))
	reqNfceShort.Header.Set("X-Tenant-ID", tenantA)
	recNfceShort := httptest.NewRecorder()
	handler.ServeHTTP(recNfceShort, reqNfceShort)
	assert.Equal(t, http.StatusBadRequest, recNfceShort.Code)

	reqBadMp := httptest.NewRequest(http.MethodPost, "/api/v1/statements/upload", bytes.NewBufferString("--bad"))
	reqBadMp.Header.Set("X-Tenant-ID", tenantA)
	reqBadMp.Header.Set("Content-Type", "multipart/form-data; boundary=missing")
	recBadMp := httptest.NewRecorder()
	handler.ServeHTTP(recBadMp, reqBadMp)
	assert.Equal(t, http.StatusBadRequest, recBadMp.Code)

	reqPatchBadJson := httptest.NewRequest(http.MethodPatch, "/api/v1/transactions/id1", bytes.NewBufferString(`{bad`))
	reqPatchBadJson.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqPatchBadJson.Header.Set("X-Tenant-ID", tenantA)
	recPatchBadJson := httptest.NewRecorder()
	handler.ServeHTTP(recPatchBadJson, reqPatchBadJson)
	assert.Equal(t, http.StatusBadRequest, recPatchBadJson.Code)

	reqTxMerchant := httptest.NewRequest(http.MethodPost, "/api/v1/transactions", bytes.NewBufferString(`{"merchant_name":"LOJA","amount":"100.00","total_installments":2,"competence_month":"2026-11"}`))
	reqTxMerchant.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqTxMerchant.Header.Set("X-Tenant-ID", tenantA)
	recTxMerchant := httptest.NewRecorder()
	handler.ServeHTTP(recTxMerchant, reqTxMerchant)
	assert.Equal(t, http.StatusCreated, recTxMerchant.Code)

	reqTxBadInst := httptest.NewRequest(http.MethodPost, "/api/v1/transactions", bytes.NewBufferString(`{"merchant_name":"LOJA","amount":"100.00","total_installments":0}`))
	reqTxBadInst.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqTxBadInst.Header.Set("X-Tenant-ID", tenantA)
	recTxBadInst := httptest.NewRecorder()
	handler.ServeHTTP(recTxBadInst, reqTxBadInst)
	assert.Equal(t, http.StatusBadRequest, recTxBadInst.Code)

	reqTxBadComp := httptest.NewRequest(http.MethodPost, "/api/v1/transactions", bytes.NewBufferString(`{"merchant_name":"LOJA","amount":"100.00","competence_month":"invalid"}`))
	reqTxBadComp.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqTxBadComp.Header.Set("X-Tenant-ID", tenantA)
	recTxBadComp := httptest.NewRecorder()
	handler.ServeHTTP(recTxBadComp, reqTxBadComp)
	assert.Equal(t, http.StatusBadRequest, recTxBadComp.Code)

	mockLedger := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"tenantId":"` + tenantA + `","plan":"FAMILY_PLUS","status":"ACTIVE"}`))
	}))
	defer mockLedger.Close()
	t.Setenv("CORE_LEDGER_HTTP_URL", mockLedger.URL)
	srvWithLedger := httpin.NewServer(nil).WithEastWestClient(grpcout.NewEastWestGrpcBridgeClient(mockLedger.URL, ""))
	reqCreateWithLedger := httptest.NewRequest(http.MethodPost, "/api/v1/admin/tenants", bytes.NewBufferString(`{"tenant_id":"`+tenantA+`"}`))
	reqCreateWithLedger.Header.Set("Authorization", "Bearer dev-admin-token")
	recCreateWithLedger := httptest.NewRecorder()
	srvWithLedger.Handler().ServeHTTP(recCreateWithLedger, reqCreateWithLedger)
	assert.Equal(t, http.StatusCreated, recCreateWithLedger.Code)

	reqTxCore := httptest.NewRequest(http.MethodPost, "/api/v1/transactions", bytes.NewBufferString(`{"merchant_name":"MERCADO","amount":"99.00","installments":1}`))
	reqTxCore.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqTxCore.Header.Set("X-Tenant-ID", tenantA)
	recTxCore := httptest.NewRecorder()
	srvWithLedger.Handler().ServeHTTP(recTxCore, reqTxCore)
	assert.Equal(t, http.StatusCreated, recTxCore.Code)

	_, errCreate := grpcout.NewEastWestGrpcBridgeClient(mockLedger.URL, "").CreateTenant(t.Context(), tenantA, "Fam", "a@b.com", "FREE")
	assert.NoError(t, errCreate)

	// 11. Cover multipart PDF extraction, invalid JSONs on nfce/contract, and PUT transaction
	var mpPdf bytes.Buffer
	mwPdf := multipart.NewWriter(&mpPdf)
	fwPdf, _ := mwPdf.CreateFormFile("file", "fatura.pdf")
	_, _ = fwPdf.Write([]byte("ISSUER: ITAU | TOTAL: 100.00\n2026-11-05 | LOJA | 100.00"))
	_ = mwPdf.Close()
	srvWithIngest := httpin.NewServer(&stubIngestUC{})
	reqMpPdf := httptest.NewRequest(http.MethodPost, "/api/v1/statements/upload", &mpPdf)
	reqMpPdf.Header.Set("X-Tenant-ID", tenantA)
	reqMpPdf.Header.Set("Content-Type", mwPdf.FormDataContentType())
	recMpPdf := httptest.NewRecorder()
	srvWithIngest.Handler().ServeHTTP(recMpPdf, reqMpPdf)
	assert.Equal(t, http.StatusOK, recMpPdf.Code)

	for _, ep := range []string{"/api/v1/receipts/nfce", "/api/v1/contracts/simulate"} {
		reqBadJ := httptest.NewRequest(http.MethodPost, ep, bytes.NewBufferString(`{bad`))
		reqBadJ.Header.Set("X-Tenant-ID", tenantA)
		recBadJ := httptest.NewRecorder()
		handler.ServeHTTP(recBadJ, reqBadJ)
		assert.Equal(t, http.StatusBadRequest, recBadJ.Code)
	}

	reqDashWithLedger := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/summary?horizon=WEEK", nil)
	reqDashWithLedger.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqDashWithLedger.Header.Set("X-Tenant-ID", tenantA)
	recDashWithLedger := httptest.NewRecorder()
	srvWithLedger.Handler().ServeHTTP(recDashWithLedger, reqDashWithLedger)
	assert.Equal(t, http.StatusOK, recDashWithLedger.Code)

	// 12. Cover JWT without sub for PLATFORM_ADMIN / VIEWER / Tenant B, plus cross-tenant mismatch 403 and local X-Tenant-ID fallback
	for _, tc := range []struct {
		tok    string
		tenant string
		code   int
	}{
		{createTestJwt("", tenantA, "PLATFORM_ADMIN"), tenantA, http.StatusOK},
		{createTestJwt("", tenantA, "VIEWER"), tenantA, http.StatusOK},
		{createTestJwt("", tenantB, "OWNER"), tenantB, http.StatusOK},
		{createTestJwt("", tenantB, "OWNER"), tenantA, http.StatusForbidden},
	} {
		reqTok := httptest.NewRequest(http.MethodGet, "/api/v1/cards", nil)
		reqTok.Header.Set("Authorization", "Bearer "+tc.tok)
		reqTok.Header.Set("X-Tenant-ID", tc.tenant)
		recTok := httptest.NewRecorder()
		handler.ServeHTTP(recTok, reqTok)
		assert.Equal(t, tc.code, recTok.Code)
	}

	reqLocalNoAuth := httptest.NewRequest(http.MethodGet, "/api/v1/profile/budgets", nil)
	reqLocalNoAuth.Header.Set("X-Tenant-ID", tenantA)
	recLocalNoAuth := httptest.NewRecorder()
	srvWithLedger.Handler().ServeHTTP(recLocalNoAuth, reqLocalNoAuth)
	assert.Equal(t, http.StatusOK, recLocalNoAuth.Code)

	reqAdminNoAuthLocal := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants", nil)
	reqAdminNoAuthLocal.Header.Set("X-Tenant-ID", tenantA)
	recAdminNoAuthLocal := httptest.NewRecorder()
	srvWithLedger.Handler().ServeHTTP(recAdminNoAuthLocal, reqAdminNoAuthLocal)
	assert.Equal(t, http.StatusForbidden, recAdminNoAuthLocal.Code)

	reqDashOrig := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard?scope=FAMILY", nil)
	reqDashOrig.Header.Set("X-Tenant-ID", tenantA)
	recDashOrig := httptest.NewRecorder()
	srvWithLedger.Handler().ServeHTTP(recDashOrig, reqDashOrig)
	assert.Equal(t, http.StatusOK, recDashOrig.Code)

	// 13. Cover UpstreamHTTPError JSON error/message parsing and SimulateContract upstream 200
	errServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"message":"unprocessable entity"}`))
	}))
	defer errServer.Close()
	_, err422 := grpcout.NewEastWestGrpcBridgeClient(errServer.URL, "").CreateTenant(t.Context(), tenantA, "Fam", "a@b.com", "FREE")
	assert.Error(t, err422)

	errServerPlain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer errServerPlain.Close()
	_, err404 := grpcout.NewEastWestGrpcBridgeClient(errServerPlain.URL, "").CreateTenant(t.Context(), tenantA, "Fam", "a@b.com", "FREE")
	assert.Error(t, err404)

	simServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"contract_id":"c1","interest_savings_amount":"12000.00"}`))
	}))
	defer simServer.Close()
	simRes, errSim := grpcout.NewEastWestGrpcBridgeClient("", simServer.URL).SimulateContract(t.Context(), grpcout.ContractSimulateRequest{
		TenantID: tenantA, ContractID: "c1", OutstandingBalance: "100000.00",
	})
	assert.NoError(t, errSim)
	assert.Equal(t, "c1", simRes["contract_id"])

	srvErr := httpin.NewServer(nil).WithEastWestClient(grpcout.NewEastWestGrpcBridgeClient(errServer.URL, ""))
	reqProfErr := httptest.NewRequest(http.MethodGet, "/api/v1/profile", nil)
	reqProfErr.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqProfErr.Header.Set("X-Tenant-ID", tenantA)
	recProfErr := httptest.NewRecorder()
	srvErr.Handler().ServeHTTP(recProfErr, reqProfErr)
	assert.Equal(t, http.StatusUnprocessableEntity, recProfErr.Code)

	reqAdminTenantErr := httptest.NewRequest(http.MethodGet, "/api/v1/admin/tenants", nil)
	reqAdminTenantErr.Header.Set("Authorization", "Bearer dev-admin-token")
	recAdminTenantErr := httptest.NewRecorder()
	srvErr.Handler().ServeHTTP(recAdminTenantErr, reqAdminTenantErr)
	assert.Equal(t, http.StatusUnprocessableEntity, recAdminTenantErr.Code)

	reqPutTx := httptest.NewRequest(http.MethodPut, "/api/v1/transactions/"+txID, bytes.NewBufferString(`{"description":"UPDATED","category_code":"OUTROS :: DESPESAS_EVENTUAIS"}`))
	reqPutTx.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqPutTx.Header.Set("X-Tenant-ID", tenantA)
	recPutTx := httptest.NewRecorder()
	handler.ServeHTTP(recPutTx, reqPutTx)
	assert.Equal(t, http.StatusOK, recPutTx.Code)

	reqDashToday := httptest.NewRequest(http.MethodGet, "/api/v1/dashboard/summary?horizon=MONTH", nil)
	reqDashToday.Header.Set("X-Tenant-ID", tenantA)
	recDashToday := httptest.NewRecorder()
	handler.ServeHTTP(recDashToday, reqDashToday)
	assert.Equal(t, http.StatusOK, recDashToday.Code)

	reqDlqOk := httptest.NewRequest(http.MethodPost, "/api/v1/inbox/dlq-test", bytes.NewBufferString(`{"corrupted":false}`))
	reqDlqOk.Header.Set("X-Tenant-ID", tenantA)
	recDlqOk := httptest.NewRecorder()
	handler.ServeHTTP(recDlqOk, reqDlqOk)
	assert.Equal(t, http.StatusOK, recDlqOk.Code)

	reqSwagger := httptest.NewRequest(http.MethodGet, "/swagger/", nil)
	recSwagger := httptest.NewRecorder()
	handler.ServeHTTP(recSwagger, reqSwagger)
	assert.Equal(t, http.StatusOK, recSwagger.Code)

	reqCors := httptest.NewRequest(http.MethodOptions, "/health", nil)
	recCors := httptest.NewRecorder()
	handler.ServeHTTP(recCors, reqCors)
	assert.Equal(t, http.StatusNoContent, recCors.Code)

	reqExportErr := httptest.NewRequest(http.MethodPost, "/api/v1/profile/export", bytes.NewBufferString(`{"format":"JSON"}`))
	reqExportErr.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqExportErr.Header.Set("X-Tenant-ID", tenantA)
	recExportErr := httptest.NewRecorder()
	srvErr.Handler().ServeHTTP(recExportErr, reqExportErr)
	assert.Equal(t, http.StatusUnprocessableEntity, recExportErr.Code)

	srvErr2 := httpin.NewServer(nil).WithEastWestClient(grpcout.NewEastWestGrpcBridgeClient(errServer.URL, ""))
	reqShredErr := httptest.NewRequest(http.MethodPost, "/api/v1/profile/crypto-shred", bytes.NewBufferString(`{"confirmation_phrase":"EXCLUIR"}`))
	reqShredErr.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqShredErr.Header.Set("X-Tenant-ID", tenantA)
	recShredErr := httptest.NewRecorder()
	srvErr2.Handler().ServeHTTP(recShredErr, reqShredErr)
	assert.Equal(t, http.StatusUnprocessableEntity, recShredErr.Code)

	reqBudgetErr := httptest.NewRequest(http.MethodPost, "/api/v1/profile/budgets", bytes.NewBufferString(`{"category_id":"c1","monthly_cap":"100"}`))
	reqBudgetErr.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqBudgetErr.Header.Set("X-Tenant-ID", tenantA)
	recBudgetErr := httptest.NewRecorder()
	srvErr2.Handler().ServeHTTP(recBudgetErr, reqBudgetErr)
	assert.Equal(t, http.StatusUnprocessableEntity, recBudgetErr.Code)

	reqInviteErr := httptest.NewRequest(http.MethodPost, "/api/v1/profile/members", bytes.NewBufferString(`{"email":"a@b.com","role":"MEMBER"}`))
	reqInviteErr.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqInviteErr.Header.Set("X-Tenant-ID", tenantA)
	recInviteErr := httptest.NewRecorder()
	srvErr2.Handler().ServeHTTP(recInviteErr, reqInviteErr)
	assert.Equal(t, http.StatusUnprocessableEntity, recInviteErr.Code)

	reqGetMissingTx := httptest.NewRequest(http.MethodGet, "/api/v1/transactions/missing-id", nil)
	reqGetMissingTx.Header.Set("Authorization", "Bearer dev-token-tenant-a")
	reqGetMissingTx.Header.Set("X-Tenant-ID", tenantA)
	recGetMissingTx := httptest.NewRecorder()
	handler.ServeHTTP(recGetMissingTx, reqGetMissingTx)
	assert.Equal(t, http.StatusNotFound, recGetMissingTx.Code)

	for _, ep := range []struct{ m, p string }{
		{http.MethodGet, "/api/v1/inbox"},
		{http.MethodPost, "/api/v1/inbox/1/confirm"},
		{http.MethodPost, "/api/v1/inbox/dlq-test"},
		{http.MethodGet, "/api/v1/cards"},
		{http.MethodPost, "/api/v1/cards"},
		{http.MethodGet, "/api/v1/transactions"},
		{http.MethodPost, "/api/v1/transactions"},
		{http.MethodGet, "/api/v1/transactions/1"},
		{http.MethodPatch, "/api/v1/transactions/1"},
		{http.MethodPost, "/api/v1/transactions/1/refund"},
		{http.MethodGet, "/api/v1/dashboard"},
		{http.MethodGet, "/api/v1/dashboard/summary"},
	} {
		reqNoT := httptest.NewRequest(ep.m, ep.p, nil)
		recNoT := httptest.NewRecorder()
		handler.ServeHTTP(recNoT, reqNoT)
		assert.Equal(t, http.StatusUnauthorized, recNoT.Code)
	}
}
