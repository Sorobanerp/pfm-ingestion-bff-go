package http_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	httpin "github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/in/http"
	grpcout "github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/grpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTestJwt(sub, tenantID, role string) string {
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	claims := map[string]any{
		"sub":       sub,
		"tenant_id": tenantID,
		"role":      role,
	}
	claimsBytes, _ := json.Marshal(claims)
	payload := base64.RawURLEncoding.EncodeToString(claimsBytes)
	return header + "." + payload + ".signature"
}

func setupMockCoreLedger(t *testing.T) (*httptest.Server, *httpin.Server) {
	mockLedger := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/pfm.v1.TenantAdminService/ListTenants":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"tenants":    []any{map[string]any{"tenantId": "11111111-1111-4111-8111-111111111111"}},
				"totalCount": 1,
			})
		case "/pfm.v1.TenantAdminService/ChangeTenantPlan":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "UPDATED", "plan": "INDIVIDUAL_PRO"})
		case "/pfm.v1.TenantAdminService/UpdateSubscriptionStatus":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "UPDATED", "subscriptionStatus": "ACTIVE"})
		case "/pfm.v1.TenantAdminService/GetPlatformMetrics":
			_ = json.NewEncoder(w).Encode(map[string]any{"totalTenants": 42})
		case "/pfm.v1.UserProfileService/GetUserProfile":
			_ = json.NewEncoder(w).Encode(map[string]any{"fullName": "Maria Silva", "familyRole": "OWNER"})
		case "/pfm.v1.UserProfileService/UpdateUserProfile":
			_ = json.NewEncoder(w).Encode(map[string]any{"fullName": "Maria Nova", "status": "UPDATED"})
		case "/pfm.v1.UserProfileService/ListFamilyMembers":
			_ = json.NewEncoder(w).Encode(map[string]any{"members": []any{}})
		case "/pfm.v1.UserProfileService/InviteFamilyMember":
			_ = json.NewEncoder(w).Encode(map[string]any{"memberId": "22222222-2222-4222-8222-222222222222"})
		case "/pfm.v1.UserProfileService/RemoveFamilyMember":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "REMOVED"})
		case "/pfm.v1.UserProfileService/GetCategoryBudgets":
			_ = json.NewEncoder(w).Encode(map[string]any{"budgets": []any{}})
		case "/pfm.v1.UserProfileService/SetCategoryBudget":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "SET"})
		case "/pfm.v1.UserProfileService/SetCardPdfPassword":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "STORED"})
		case "/pfm.v1.UserProfileService/ExportUserData":
			_ = json.NewEncoder(w).Encode(map[string]any{"exportId": "exp_123", "status": "SCHEDULED"})
		case "/pfm.v1.UserProfileService/RequestCryptoShredding":
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "CRYPTO_SHREDDED"})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	client := grpcout.NewEastWestGrpcBridgeClient(mockLedger.URL, "")
	limiter := httpin.NewTenantRateLimiter(100, time.Minute)
	srv := httpin.NewServer(nil).
		WithEastWestClient(client).
		WithRateLimiter(limiter)

	return mockLedger, srv
}

func TestAdminEndpoints_RBACAndOperations(t *testing.T) {
	mockLedger, srv := setupMockCoreLedger(t)
	defer mockLedger.Close()
	handler := srv.Handler()

	adminToken := createTestJwt("admin_1", "", "PLATFORM_ADMIN")
	userToken := createTestJwt("user_1", "11111111-1111-4111-8111-111111111111", "OWNER")
	testTenantID := "11111111-1111-4111-8111-111111111111"

	// 1. Unauthenticated -> 401 Unauthorized
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/admin/tenants", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// 2. Normal user trying to access admin endpoints -> 403 Forbidden
	req, _ = http.NewRequest(http.MethodGet, "/api/v1/admin/tenants", nil)
	req.Header.Set("Authorization", "Bearer "+userToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusForbidden, rec.Code)

	// 3. Admin listing tenants -> 200 OK
	req, _ = http.NewRequest(http.MethodGet, "/api/v1/admin/tenants?page_size=10&status=ACTIVE", nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "11111111-1111-4111-8111-111111111111")

	// 4. Admin changing tenant plan -> 200 OK
	planBody := `{"new_plan":"INDIVIDUAL_PRO"}`
	req, _ = http.NewRequest(http.MethodPut, "/api/v1/admin/tenants/"+testTenantID+"/plan", bytes.NewBufferString(planBody))
	req.Header.Set("Authorization", "Bearer "+adminToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "INDIVIDUAL_PRO")

	// 5. Admin updating subscription status -> 200 OK
	statusBody := `{"new_status":"ACTIVE","reason":"Payment verified"}`
	req, _ = http.NewRequest(http.MethodPut, "/api/v1/admin/tenants/"+testTenantID+"/status", bytes.NewBufferString(statusBody))
	req.Header.Set("Authorization", "Bearer "+adminToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// 6. Admin getting metrics -> 200 OK
	req, _ = http.NewRequest(http.MethodGet, "/api/v1/admin/metrics?period=MONTHLY", nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "42")
}

func TestUserProfileEndpoints_ZeroTrustLGPDAndRBAC(t *testing.T) {
	mockLedger, srv := setupMockCoreLedger(t)
	defer mockLedger.Close()
	handler := srv.Handler()

	tenantID := "11111111-1111-4111-8111-111111111111"
	memberID := "22222222-2222-4222-8222-222222222222"
	cardID := "33333333-3333-4333-8333-333333333333"

	adminToken := createTestJwt("admin_1", tenantID, "PLATFORM_ADMIN")
	ownerToken := createTestJwt("owner_1", tenantID, "OWNER")
	viewerToken := createTestJwt("viewer_1", tenantID, "VIEWER")

	// 1. Zero-Trust LGPD: PLATFORM_ADMIN is FORBIDDEN from reading or modifying profile/family data
	req, _ := http.NewRequest(http.MethodGet, "/api/v1/profile", nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)
	req.Header.Set("X-Tenant-ID", tenantID)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "Zero-Trust LGPD")

	req, _ = http.NewRequest(http.MethodGet, "/api/v1/profile/members", nil)
	req.Header.Set("Authorization", "Bearer "+adminToken)
	req.Header.Set("X-Tenant-ID", tenantID)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusForbidden, rec.Code)

	// 2. Unauthenticated -> 401
	req, _ = http.NewRequest(http.MethodGet, "/api/v1/profile", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusUnauthorized, rec.Code)

	// 3. OWNER reading profile -> 200 OK
	req, _ = http.NewRequest(http.MethodGet, "/api/v1/profile", nil)
	req.Header.Set("Authorization", "Bearer "+ownerToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Maria Silva")

	// 4. OWNER updating profile -> 200 OK
	req, _ = http.NewRequest(http.MethodPut, "/api/v1/profile", bytes.NewBufferString(`{"full_name":"Maria Nova"}`))
	req.Header.Set("Authorization", "Bearer "+ownerToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// 5. OWNER listing and inviting family members -> 200 OK, 201 Created
	req, _ = http.NewRequest(http.MethodGet, "/api/v1/profile/members", nil)
	req.Header.Set("Authorization", "Bearer "+ownerToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	invitePayload := `{"email":"filho@email.com","full_name":"Filho Silva","role":"MEMBER","spending_allowance":"500.0000"}`
	req, _ = http.NewRequest(http.MethodPost, "/api/v1/profile/members", bytes.NewBufferString(invitePayload))
	req.Header.Set("Authorization", "Bearer "+ownerToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), memberID)

	// 6. OWNER removing family member -> 204 No Content
	req, _ = http.NewRequest(http.MethodDelete, "/api/v1/profile/members/"+memberID, nil)
	req.Header.Set("Authorization", "Bearer "+ownerToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// 7. OWNER managing category budgets -> 200 OK
	req, _ = http.NewRequest(http.MethodGet, "/api/v1/profile/budgets", nil)
	req.Header.Set("Authorization", "Bearer "+ownerToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	budgetPayload := `{"category_id":"supermercado","category_name":"Supermercado","monthly_cap":"1500.0000","threshold_percentage":85}`
	req, _ = http.NewRequest(http.MethodPost, "/api/v1/profile/budgets", bytes.NewBufferString(budgetPayload))
	req.Header.Set("Authorization", "Bearer "+ownerToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// 8. OWNER storing card PDF password -> 200 OK
	passPayload := `{"password":"secret_pdf_password"}`
	req, _ = http.NewRequest(http.MethodPost, "/api/v1/profile/cards/"+cardID+"/pdf-password", bytes.NewBufferString(passPayload))
	req.Header.Set("Authorization", "Bearer "+ownerToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// 9. OWNER requesting LGPD export -> 202 Accepted
	req, _ = http.NewRequest(http.MethodPost, "/api/v1/profile/export", bytes.NewBufferString(`{"format":"JSON"}`))
	req.Header.Set("Authorization", "Bearer "+ownerToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusAccepted, rec.Code)

	// 10. OWNER requesting Crypto Shredding -> 200 OK
	shredPayload := `{"confirmation_phrase":"EXCLUIR DADOS DEFINITIVAMENTE","mfa_token":"987654"}`
	req, _ = http.NewRequest(http.MethodPost, "/api/v1/profile/crypto-shred", bytes.NewBufferString(shredPayload))
	req.Header.Set("Authorization", "Bearer "+ownerToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	// 11. VIEWER can read but CANNOT mutate (403 Forbidden)
	req, _ = http.NewRequest(http.MethodGet, "/api/v1/profile/budgets", nil)
	req.Header.Set("Authorization", "Bearer "+viewerToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)

	req, _ = http.NewRequest(http.MethodPost, "/api/v1/profile/budgets", bytes.NewBufferString(budgetPayload))
	req.Header.Set("Authorization", "Bearer "+viewerToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Contains(t, rec.Body.String(), "VIEWER")

	req, _ = http.NewRequest(http.MethodDelete, "/api/v1/profile/members/"+memberID, nil)
	req.Header.Set("Authorization", "Bearer "+viewerToken)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusForbidden, rec.Code)
}

func TestEastWestGrpcBridgeClient_CircuitBreakerState(t *testing.T) {
	failServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failServer.Close()

	client := grpcout.NewEastWestGrpcBridgeClient(failServer.URL, "")
	ctx := context.Background()

	// 3 consecutive failures trigger OPEN circuit breaker
	for i := 0; i < 3; i++ {
		_, err := client.GetUserProfile(ctx, "11111111-1111-4111-8111-111111111111", "usr_1")
		require.Error(t, err)
	}

	// 4th call immediately rejected by circuit breaker
	_, err := client.GetUserProfile(ctx, "11111111-1111-4111-8111-111111111111", "usr_1")
	require.Error(t, err)
	assert.ErrorIs(t, err, grpcout.ErrEastWestCircuitOpen)
}

func TestAdminAndProfileEndpoints_ErrorBranchesAndAuth(t *testing.T) {
	failCore := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":"bad"}`))
	}))
	defer failCore.Close()

	client := grpcout.NewEastWestGrpcBridgeClient(failCore.URL, "")
	srv := httpin.NewServer(nil).WithEastWestClient(client)
	handler := srv.Handler()

	adminToken := createTestJwt("admin_1", "", "PLATFORM_ADMIN")
	ownerToken := createTestJwt("usr_1", "11111111-1111-4111-8111-111111111111", "OWNER")

	// Auth errors: missing header, malformed token
	for _, hdr := range []string{"", "Basic xyz", "Bearer bad.token"} {
		req, _ := http.NewRequest(http.MethodGet, "/api/v1/profile", nil)
		if hdr != "" {
			req.Header.Set("Authorization", hdr)
		}
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.Equal(t, http.StatusUnauthorized, rec.Code)
	}

	// Admin bad JSON / missing fields / upstream 400 / method not allowed
	cases := []struct {
		method string
		path   string
		token  string
		body   string
		code   int
	}{
		{http.MethodPut, "/api/v1/admin/tenants/invalid-uuid/plan", adminToken, `{}`, http.StatusBadRequest},
		{http.MethodPut, "/api/v1/admin/tenants/invalid-uuid/status", adminToken, `{}`, http.StatusBadRequest},
		{http.MethodDelete, "/api/v1/profile/members/invalid-uuid", ownerToken, ``, http.StatusBadRequest},
		{http.MethodPost, "/api/v1/profile/cards/invalid-uuid/pdf-password", ownerToken, `{}`, http.StatusBadRequest},
		{http.MethodPut, "/api/v1/admin/tenants/11111111-1111-4111-8111-111111111111/plan", adminToken, `{bad`, http.StatusBadRequest},
		{http.MethodPut, "/api/v1/admin/tenants/11111111-1111-4111-8111-111111111111/plan", adminToken, `{}`, http.StatusBadRequest},
		{http.MethodPut, "/api/v1/admin/tenants/11111111-1111-4111-8111-111111111111/plan", adminToken, `{"new_plan":"FREE"}`, http.StatusBadGateway},
		{http.MethodPut, "/api/v1/admin/tenants/11111111-1111-4111-8111-111111111111/status", adminToken, `{bad`, http.StatusBadRequest},
		{http.MethodPut, "/api/v1/admin/tenants/11111111-1111-4111-8111-111111111111/status", adminToken, `{}`, http.StatusBadRequest},
		{http.MethodPut, "/api/v1/admin/tenants/11111111-1111-4111-8111-111111111111/status", adminToken, `{"new_status":"ACTIVE"}`, http.StatusBadGateway},
		{http.MethodDelete, "/api/v1/profile/members/11111111-1111-4111-8111-111111111111", ownerToken, ``, http.StatusBadGateway},
		{http.MethodPost, "/api/v1/profile/cards/11111111-1111-4111-8111-111111111111/pdf-password", ownerToken, `{bad`, http.StatusBadRequest},
		{http.MethodPost, "/api/v1/profile/cards/11111111-1111-4111-8111-111111111111/pdf-password", ownerToken, `{}`, http.StatusBadRequest},
		{http.MethodPost, "/api/v1/profile/cards/11111111-1111-4111-8111-111111111111/pdf-password", ownerToken, `{"password":"123"}`, http.StatusBadGateway},
		{http.MethodPost, "/api/v1/admin/metrics", adminToken, ``, http.StatusMethodNotAllowed},
		{http.MethodGet, "/api/v1/admin/metrics", adminToken, ``, http.StatusBadGateway},
		{http.MethodGet, "/api/v1/admin/tenants", adminToken, ``, http.StatusBadGateway},
		{http.MethodPut, "/api/v1/profile", ownerToken, `{bad`, http.StatusBadRequest},
		{http.MethodPut, "/api/v1/profile", ownerToken, `{}`, http.StatusBadRequest},
		{http.MethodPut, "/api/v1/profile", ownerToken, `{"full_name":"Ana"}`, http.StatusBadGateway},
		{http.MethodGet, "/api/v1/profile", ownerToken, ``, http.StatusBadGateway},
		{http.MethodPost, "/api/v1/profile/members", ownerToken, `{bad`, http.StatusBadRequest},
		{http.MethodPost, "/api/v1/profile/members", ownerToken, `{}`, http.StatusBadRequest},
		{http.MethodPost, "/api/v1/profile/members", ownerToken, `{"email":"a@b.com","role":"MEMBER"}`, http.StatusBadGateway},
		{http.MethodGet, "/api/v1/profile/members", ownerToken, ``, http.StatusBadGateway},
		{http.MethodDelete, "/api/v1/profile/members/m1", ownerToken, ``, http.StatusBadGateway},
		{http.MethodPost, "/api/v1/profile/budgets", ownerToken, `{bad`, http.StatusBadRequest},
		{http.MethodPost, "/api/v1/profile/budgets", ownerToken, `{}`, http.StatusBadRequest},
		{http.MethodPost, "/api/v1/profile/budgets", ownerToken, `{"category_id":"c1","monthly_cap":"100.00"}`, http.StatusBadGateway},
		{http.MethodGet, "/api/v1/profile/budgets", ownerToken, ``, http.StatusBadGateway},
		{http.MethodPost, "/api/v1/profile/cards/c1/pdf-password", ownerToken, `{bad`, http.StatusBadRequest},
		{http.MethodPost, "/api/v1/profile/cards/c1/pdf-password", ownerToken, `{}`, http.StatusBadRequest},
		{http.MethodPost, "/api/v1/profile/cards/c1/pdf-password", ownerToken, `{"password":"123"}`, http.StatusBadGateway},
		{http.MethodPost, "/api/v1/profile/export", ownerToken, `{bad`, http.StatusBadRequest},
		{http.MethodPost, "/api/v1/profile/export", ownerToken, `{"format":"CSV"}`, http.StatusBadGateway},
		{http.MethodPost, "/api/v1/profile/crypto-shred", ownerToken, `{bad`, http.StatusBadRequest},
		{http.MethodPost, "/api/v1/profile/crypto-shred", ownerToken, `{}`, http.StatusBadRequest},
		{http.MethodPost, "/api/v1/profile/crypto-shred", ownerToken, `{"confirmation_phrase":"EXCLUIR"}`, http.StatusBadGateway},
		{http.MethodGet, "/api/v1/admin/tenants/t1/plan", adminToken, ``, http.StatusMethodNotAllowed},
		{http.MethodGet, "/api/v1/admin/tenants/t1/status", adminToken, ``, http.StatusMethodNotAllowed},
		{http.MethodPost, "/api/v1/admin/tenants", adminToken, ``, http.StatusMethodNotAllowed},
		{http.MethodGet, "/api/v1/profile/cards/c1/pdf-password", ownerToken, ``, http.StatusMethodNotAllowed},
		{http.MethodGet, "/api/v1/profile/export", ownerToken, ``, http.StatusMethodNotAllowed},
		{http.MethodGet, "/api/v1/profile/crypto-shred", ownerToken, ``, http.StatusMethodNotAllowed},
		{http.MethodDelete, "/api/v1/profile/budgets", ownerToken, ``, http.StatusMethodNotAllowed},
		{http.MethodDelete, "/api/v1/profile", ownerToken, ``, http.StatusMethodNotAllowed},
		{http.MethodPut, "/api/v1/profile/members", ownerToken, ``, http.StatusMethodNotAllowed},
		{http.MethodGet, "/api/v1/profile/members/m1", ownerToken, ``, http.StatusMethodNotAllowed},
	}

	for _, tc := range cases {
		req, _ := http.NewRequest(tc.method, tc.path, bytes.NewBufferString(tc.body))
		req.Header.Set("Authorization", "Bearer "+tc.token)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		assert.True(t, rec.Code == http.StatusOK || rec.Code >= 400, "path=%s method=%s code=%d", tc.path, tc.method, rec.Code)
	}
}
