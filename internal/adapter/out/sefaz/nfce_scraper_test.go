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

func TestSefazNfceScraper_CircuitBreakerTransitionsAndShortCircuit(t *testing.T) {
	scraper := NewSefazNfceScraper(nil)
	scraper.failureThreshold = 2

	scraper.recordFailure()
	if scraper.CircuitState() != "CLOSED" {
		t.Fatalf("expected CLOSED, got %s", scraper.CircuitState())
	}
	scraper.recordFailure()
	if scraper.CircuitState() != "OPEN" {
		t.Fatalf("expected OPEN, got %s", scraper.CircuitState())
	}

	// Short circuit
	_, _, err := scraper.ScrapeAndForward(context.Background(), "t1", "http://example.com", "", "PIX", "")
	if err != ErrSefazCircuitOpen {
		t.Fatalf("expected ErrSefazCircuitOpen, got %v", err)
	}

	// Half-open transition
	scraper.mu.Lock()
	scraper.openedAt = scraper.openedAt.Add(-20 * scraper.openCooldown)
	scraper.mu.Unlock()

	if scraper.CircuitState() != "HALF_OPEN" {
		t.Fatalf("expected HALF_OPEN, got %s", scraper.CircuitState())
	}
	if scraper.isCircuitOpen() {
		t.Fatalf("expected isCircuitOpen() to be false when half open")
	}

	scraper.recordSuccess()
	if scraper.CircuitState() != "CLOSED" {
		t.Fatalf("expected CLOSED after success, got %s", scraper.CircuitState())
	}
}

func TestSefazNfceScraper_GovBrAndValidation(t *testing.T) {
	scraperWithoutForwarder := NewSefazNfceScraper(nil)
	ctx := context.Background()

	// Empty URL
	_, _, err := scraperWithoutForwarder.ScrapeAndForward(ctx, "t1", "", "", "PIX", "")
	if err != ErrInvalidNfceURL {
		t.Fatalf("expected ErrInvalidNfceURL, got %v", err)
	}

	// .gov.br offline emulation without forwarder
	rcpt, ledgerResp, err := scraperWithoutForwarder.ScrapeAndForward(
		ctx,
		"t1",
		"https://www.sefaz.rs.gov.br/NFCE/NFCE-COM.aspx?p=432611",
		"card-1",
		"CREDIT_CARD",
		"sha256-idemp-gov-000000000001",
	)
	if err != nil {
		t.Fatalf("unexpected error on gov.br emulation: %v", err)
	}
	if rcpt == nil || ledgerResp == nil {
		t.Fatalf("expected non-nil receipt and ledgerResp")
	}

	// With forwarder returning error
	failingSpy := &failingLedgerForwarder{}
	scraperWithFail := NewSefazNfceScraper(failingSpy)
	_, _, err = scraperWithFail.ScrapeAndForward(
		ctx,
		"t1",
		"https://www.sefaz.rs.gov.br/NFCE/NFCE-COM.aspx?p=432611",
		"card-1",
		"CREDIT_CARD",
		"sha256-idemp-gov-000000000002",
	)
	if err == nil {
		t.Fatalf("expected error from forwarder")
	}
}

type failingLedgerForwarder struct{}

func (f *failingLedgerForwarder) ForwardReceipt(_ context.Context, _ ScrapedNfceReceipt) (*CoreLedgerProcessReceiptResponse, error) {
	return nil, ErrSefazCircuitOpen
}

func TestParseNfceHTML_EmptyIdempotencyAndNoTotalNumb(t *testing.T) {
	htmlWithoutTotalNumb := `
	<html><body>
	  <div id="u20">PADARIA DO BAIRRO</div>
	  <div>CNPJ: 11.222.333/0001-44</div>
	  <div>Chave de acesso: 35261111222333000144650010000000011000000001</div>
	  <div>Emissão: 16/11/2026 15:00:00</div>
	  <table>
	    <tr id="Item 1">
	      <td>
	        <span class="txtTit">PAO FRANCES KG</span>
	        <span class="RCod">(EAN: 7890000000001)</span>
	        <span class="Rqtd">Qtde.: 1,000</span>
	        <span class="RUN">UN: KG</span>
	        <span class="RvlUnit">Vl. Unit.: 18,50</span>
	      </td>
	      <td><span class="valor">18,50</span></td>
	    </tr>
	  </table>
	</body></html>`

	// Empty idempotency key triggers SHA-256 derivation and computed sum fallback
	rcpt, err := ParseNfceHTML("tenant-1", "http://example.com/nfce", "", "PIX", "", htmlWithoutTotalNumb)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rcpt.IdempotencyKey == "" {
		t.Fatalf("expected non-empty derived idempotency key")
	}
	if !rcpt.TotalAmount.Equal(decimal.RequireFromString("18.5000")) {
		t.Fatalf("expected computed sum 18.5000, got %s", rcpt.TotalAmount)
	}
}

func TestParseNfceHTML_AlternativeRegexBranches(t *testing.T) {
	htmlAlt := `
	<html><body>
	  <h4 class="merchant">SUPERMERCADO ALTERNATIVO</h4>
	  <div>Emissão: 2026-11-16T14:30:00Z</div>
	  <div>Total: <span class="totalNumb">100,50</span></div>
	</body></html>`

	rcpt, err := ParseNfceHTML("t1", "http://example.com/nfce", "", "PIX", "sha256-idemp-0000000000000003", htmlAlt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rcpt.MerchantName != "SUPERMERCADO ALTERNATIVO" {
		t.Fatalf("expected SUPERMERCADO ALTERNATIVO, got %s", rcpt.MerchantName)
	}
	if !rcpt.TotalAmount.Equal(decimal.RequireFromString("100.5000")) {
		t.Fatalf("expected 100.5000, got %s", rcpt.TotalAmount)
	}
}
