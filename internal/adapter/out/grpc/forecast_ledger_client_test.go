package grpcout

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEastWestGrpcBridgeClient_FetchDashboardSummary(t *testing.T) {
	// 1. Mock server that returns 200 OK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "tenant-test", r.Header.Get("X-Tenant-ID"))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"OK"}`))
	}))
	defer srv.Close()

	client := NewEastWestGrpcBridgeClient(srv.URL, srv.URL)
	ctx := context.Background()

	// Scope: HOLDER_DANTAS
	resDantas, err := client.FetchDashboardSummary(ctx, "tenant-test", "HOLDER_DANTAS")
	require.NoError(t, err)
	assert.Equal(t, "tenant-test", resDantas["tenantId"])
	assert.Equal(t, "HOLDER_DANTAS", resDantas["scope"])

	// Scope: HOLDER_SPOUSE
	resSpouse, err := client.FetchDashboardSummary(ctx, "tenant-test", "HOLDER_SPOUSE")
	require.NoError(t, err)
	assert.Equal(t, "HOLDER_SPOUSE", resSpouse["scope"])

	// Default scope
	resDefault, err := client.FetchDashboardSummary(ctx, "tenant-test", "ALL")
	require.NoError(t, err)
	assert.Equal(t, "ALL", resDefault["scope"])
}

func TestEastWestGrpcBridgeClient_SimulateContract(t *testing.T) {
	// Mock forecast server
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"simulated_installments":100,"interest_savings_amount":"5000.0000"}`))
	}))
	defer srv.Close()

	client := NewEastWestGrpcBridgeClient("", srv.URL)
	ctx := context.Background()

	req := ContractSimulateRequest{
		TenantID:                "tenant-1",
		ContractID:              "c-1",
		AmortizationSystem:      "SAC",
		PrepaymentStrategy:      "REDUCE_TERM",
		OutstandingBalance:      "100000.0000",
		RemainingInstallments:   120,
		MonthlyInterestRate:     "0.0100",
		ExtraAmortizationAmount: "10000.0000",
	}

	res, err := client.SimulateContract(ctx, req)
	require.NoError(t, err)
	assert.Equal(t, 100, int(res["simulated_installments"].(float64)))

	// Fallback calculation when server unreachable
	clientFallback := NewEastWestGrpcBridgeClient("", "http://127.0.0.1:59999")
	clientFallback.httpClient.Timeout = 50 * time.Millisecond
	resFallback, err := clientFallback.SimulateContract(ctx, req)
	require.NoError(t, err)
	assert.NotNil(t, resFallback)
	assert.Equal(t, "tenant-1", resFallback["tenant_id"])

	// Invalid outstanding balance
	reqBad := req
	reqBad.OutstandingBalance = "0.0000"
	_, err = clientFallback.SimulateContract(ctx, reqBad)
	assert.Error(t, err)
}

func TestEastWestGrpcBridgeClient_CircuitBreaker(t *testing.T) {
	client := NewEastWestGrpcBridgeClient("", "")
	assert.False(t, client.isCircuitOpen())

	client.recordFailure()
	assert.False(t, client.isCircuitOpen())
	client.recordFailure()
	assert.False(t, client.isCircuitOpen())
	client.recordFailure()

	assert.True(t, client.isCircuitOpen())

	// Cooldown elapsed -> half open
	client.mu.Lock()
	client.openedAt = time.Now().Add(-20 * time.Second)
	client.mu.Unlock()

	assert.False(t, client.isCircuitOpen())

	client.recordSuccess()
	assert.Equal(t, "CLOSED", client.state)
}

func TestEastWestGrpcBridgeClient_CoreLedger500(t *testing.T) {
	srvErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srvErr.Close()

	client := NewEastWestGrpcBridgeClient(srvErr.URL, "")
	ctx := context.Background()

	// Even if core ledger returns 500, FetchDashboardSummary handles error and computes executive dashboard
	res, err := client.FetchDashboardSummary(ctx, "tenant-err", "ALL")
	require.NoError(t, err)
	assert.NotNil(t, res)
}

func TestSimulateContract_EdgeCasesAnd500(t *testing.T) {
	srv500 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv500.Close()

	client := NewEastWestGrpcBridgeClient("", srv500.URL)
	ctx := context.Background()

	// 1. Forecast returns 500 (line 155), negative extra amount (line 168), term < 1 (line 172)
	req1 := ContractSimulateRequest{
		OutstandingBalance:      "10000.00",
		RemainingInstallments:   0, // term < 1
		ExtraAmortizationAmount: "-50.00", // negative extra
		PrepaymentStrategy:      "REDUCE_TERM",
	}
	res1, err := client.SimulateContract(ctx, req1)
	require.NoError(t, err)
	assert.Equal(t, 120, res1["original_installments"])

	// 2. Extra prepayment reduces simTerm < 1 (line 181)
	clientLocal := NewEastWestGrpcBridgeClient("", "")
	req2 := ContractSimulateRequest{
		OutstandingBalance:      "100.00",
		RemainingInstallments:   10,
		ExtraAmortizationAmount: "100.00", // newBalance = 0 -> simTerm < 1
		PrepaymentStrategy:      "REDUCE_TERM",
	}
	res2, err := clientLocal.SimulateContract(ctx, req2)
	require.NoError(t, err)
	assert.Equal(t, 1, res2["simulated_installments"])
}
