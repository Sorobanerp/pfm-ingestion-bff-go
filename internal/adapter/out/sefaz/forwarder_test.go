package sefaz

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHttpGrpcBridgeForwarder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "tenant-sefaz", r.Header.Get("X-Tenant-ID"))
		assert.Equal(t, "/pfm.v1.CoreLedgerService/ProcessReceipt", r.URL.Path)

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"receiptId":"rcpt-1","reconciliationStatus":"RECONCILED"}`))
	}))
	defer srv.Close()

	forwarder := NewHttpGrpcBridgeForwarder(srv.URL)
	ctx := context.Background()

	receipt := ScrapedNfceReceipt{
		TenantID:      "tenant-sefaz",
		AccessKey:     "35260400000000000000000000000000000000000000",
		MerchantTaxID: "00000000000191",
		TotalAmount:   decimal.NewFromFloat(150.00),
		Items: []ScrapedReceiptItem{
			{Description: "Cafe", Quantity: decimal.NewFromFloat(1.0), UnitPrice: decimal.NewFromFloat(50.00), TotalPrice: decimal.NewFromFloat(50.00)},
			{Description: "Chocolate", Quantity: decimal.NewFromFloat(2.0), UnitPrice: decimal.NewFromFloat(50.00), TotalPrice: decimal.NewFromFloat(100.00)},
		},
	}

	resp, err := forwarder.ForwardReceipt(ctx, receipt)
	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "rcpt-1", resp.ReceiptID)

	// Server error
	srvErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srvErr.Close()

	forwarderErr := NewHttpGrpcBridgeForwarder(srvErr.URL)
	_, err = forwarderErr.ForwardReceipt(ctx, receipt)
	assert.Error(t, err)
}

func TestHttpGrpcBridgeForwarder_BadJSONAndNetworkError(t *testing.T) {
	// Bad JSON response
	srvBadJSON := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{invalid-json"))
	}))
	defer srvBadJSON.Close()

	forwarder := NewHttpGrpcBridgeForwarder(srvBadJSON.URL)
	receipt := ScrapedNfceReceipt{
		TenantID:    "t-sefaz-1",
		CardID:      "short", // triggers default cardId branch (line 367)
		TotalAmount: decimal.NewFromFloat(10.00),
	}

	_, err := forwarder.ForwardReceipt(context.Background(), receipt)
	assert.Error(t, err)

	// Network error
	forwarderNetErr := NewHttpGrpcBridgeForwarder("http://127.0.0.1:59998")
	_, err = forwarderNetErr.ForwardReceipt(context.Background(), receipt)
	assert.Error(t, err)
}
