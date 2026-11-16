package sefaz

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shopspring/decimal"
)

type spyLedgerForwarder struct {
	forwarded []ScrapedNfceReceipt
}

func (s *spyLedgerForwarder) ForwardReceipt(
	_ context.Context,
	receipt ScrapedNfceReceipt,
) (*CoreLedgerProcessReceiptResponse, error) {
	s.forwarded = append(s.forwarded, receipt)
	return &CoreLedgerProcessReceiptResponse{
		ReceiptID:            "rcpt-001",
		ReconciliationStatus: "MATCHED_STATEMENT_LINE",
		MatchedTransactionID: "tx-stmt-001",
		JournalEntryID:       "je-001",
		MatchConfidence:      "1.0000",
		HasInflationAlerts:   true,
	}, nil
}

func TestSefazNfceScraper_ExtractsItemsAndForwardsToCoreLedger(t *testing.T) {
	t.Parallel()

	sampleDANFEHTML := `
	<html>
	  <body>
	    <div id="u20">ATACADAO S.A. - LOJA ACU</div>
	    <div>CNPJ: 75.315.333/0001-09</div>
	    <div>Chave de acesso: 24261175315333000109650010000987651000987654</div>
	    <div>Emissão: 16/11/2026 14:30:00</div>
	    <table>
	      <tr id="Item 1">
	        <td>
	          <span class="txtTit">CAFE TORRADO E MOIDO 500G</span>
	          <span class="RCod">(EAN: 7891000100103)</span>
	          <span class="Rqtd">Qtde.: 2,000</span>
	          <span class="RUN">UN: UN</span>
	          <span class="RvlUnit">Vl. Unit.: 23,50</span>
	        </td>
	        <td><span class="valor">47,00</span></td>
	      </tr>
	      <tr id="Item 2">
	        <td>
	          <span class="txtTit">AZEITE EXTRA VIRGEM 500ML</span>
	          <span class="RCod">(EAN: 7894900011517)</span>
	          <span class="Rqtd">Qtde.: 1,000</span>
	          <span class="RUN">UN: UN</span>
	          <span class="RvlUnit">Vl. Unit.: 42,90</span>
	        </td>
	        <td><span class="valor">42,90</span></td>
	      </tr>
	    </table>
	    <div>Valor a pagar R$: <span class="totalNumb">89,90</span></div>
	  </body>
	</html>`

	sefazServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sampleDANFEHTML))
	}))
	defer sefazServer.Close()

	spy := &spyLedgerForwarder{}
	scraper := NewSefazNfceScraper(spy)

	receipt, ledgerResp, err := scraper.ScrapeAndForward(
		context.Background(),
		"dddddddd-dddd-4ddd-8ddd-dddddddddddd",
		sefazServer.URL+"/nfce?p=24261175315333000109650010000987651000987654|2|1|1",
		"22222222-dddd-4ddd-8ddd-222222222222",
		"CREDIT_CARD",
		"sha256-nfce-test-001",
	)
	if err != nil {
		t.Fatalf("unexpected error scraping NFC-e: %v", err)
	}

	if receipt.MerchantTaxID != "75315333000109" {
		t.Fatalf("expected CNPJ 75315333000109, got %s", receipt.MerchantTaxID)
	}
	if receipt.MerchantName != "ATACADAO S.A. - LOJA ACU" {
		t.Fatalf("expected merchant ATACADAO S.A. - LOJA ACU, got %s", receipt.MerchantName)
	}
	if !receipt.TotalAmount.Equal(decimal.RequireFromString("89.9000")) {
		t.Fatalf("expected total 89.9000, got %s", receipt.TotalAmount)
	}
	if len(receipt.Items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(receipt.Items))
	}
	if receipt.Items[0].EAN != "7891000100103" || !receipt.Items[0].UnitPrice.Equal(decimal.RequireFromString("23.5000")) {
		t.Fatalf("unexpected item 1: %+v", receipt.Items[0])
	}
	if ledgerResp.ReconciliationStatus != "MATCHED_STATEMENT_LINE" || !ledgerResp.HasInflationAlerts {
		t.Fatalf("unexpected ledger response: %+v", ledgerResp)
	}
	if len(spy.forwarded) != 1 {
		t.Fatalf("expected 1 forwarded receipt to Core Ledger, got %d", len(spy.forwarded))
	}
}

func TestSefazNfceScraper_CircuitBreakerOpensAfterConsecutiveFailures(t *testing.T) {
	t.Parallel()

	failingSefaz := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer failingSefaz.Close()

	scraper := NewSefazNfceScraper(nil)
	scraper.maxRetries = 1
	scraper.failureThreshold = 2

	for i := 0; i < 2; i++ {
		_, _, err := scraper.ScrapeAndForward(
			context.Background(),
			"dddddddd-dddd-4ddd-8ddd-dddddddddddd",
			failingSefaz.URL,
			"",
			"PIX",
			"",
		)
		if err == nil {
			t.Fatalf("expected error on attempt %d", i+1)
		}
	}

	if scraper.CircuitState() != "OPEN" {
		t.Fatalf("expected circuit breaker state OPEN, got %s", scraper.CircuitState())
	}
}
