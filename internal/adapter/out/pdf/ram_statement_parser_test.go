package pdf_test

import (
	"context"
	"errors"
	"testing"

	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/pdf"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/domain"
)

func TestZeroPersistencePDFParser_ExactChecksumHiddenFeeRefundAndRAMWipe(t *testing.T) {
	t.Parallel()

	// Statement containing:
	// 1. Spot purchase: R$ 250.00
	// 2. Installment purchase (03/12): R$ 400.00
	// 3. Negative Refund (ESTORNO): -R$ 45.50
	// 4. Hidden Bank Fee (TARIFA ITAU AVISA SMS): R$ 9.90
	// Exact Declared Total: 250.00 + 400.00 - 45.50 + 9.90 = 614.40
	rawStatement := `
# FATURA CARTAO ITAU PERSONALITE
REF: 2026-11
VENCIMENTO: 2026-11-25
TOTAL_FATURA: R$ 614,40
2026-11-03 | SUPERMERCADO PAO DE ACUCAR | 250,00
2026-11-05 | APPLE STORE BRASIL 03/12 | 400,00
2026-11-08 | ESTORNO UBER TRIP | -45,50
2026-11-10 | TARIFA ITAU AVISA SMS | 9,90
`
	payloadRAM := []byte(rawStatement)

	var internalBufferWiped bool
	parser := pdf.NewZeroPersistencePDFParser()
	parser.OnBufferWiped = func(wipedSlice []byte) {
		allZero := true
		for _, b := range wipedSlice {
			if b != 0 {
				allZero = false
				break
			}
		}
		internalBufferWiped = allZero
	}

	tenantID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	stmt, err := parser.ParseStatementInRAM(
		context.Background(),
		tenantID,
		domain.IssuerItau,
		"4532",
		"123456",
		payloadRAM,
	)
	if err != nil {
		t.Fatalf("expected successful statement parse, got error: %v", err)
	}

	// 1. Verify Zero-Persistence RAM Cleanup (both internal buffer and input slice wiped via defer)
	if !internalBufferWiped {
		t.Fatalf("expected internal working RAM buffer to be completely zeroed out via defer")
	}
	for i, b := range payloadRAM {
		if b != 0 {
			t.Fatalf("expected caller RAM slice byte at index %d to be zeroed, found %d", i, b)
		}
	}

	// 2. Verify Exact Checksum (DeclaredTotal == ComputedTotal == 614.4000)
	if !stmt.Checksum.IsValid {
		t.Fatalf("expected valid checksum report")
	}
	if stmt.Checksum.DeclaredTotal.Fixed4() != "614.4000" {
		t.Fatalf("expected DeclaredTotal 614.4000, got %s", stmt.Checksum.DeclaredTotal.Fixed4())
	}
	if stmt.Checksum.ComputedTotal.Fixed4() != "614.4000" {
		t.Fatalf("expected ComputedTotal 614.4000, got %s", stmt.Checksum.ComputedTotal.Fixed4())
	}

	// 3. Verify all 4 transaction kinds (SPOT, INSTALLMENT, REFUND, BANK_FEE)
	if len(stmt.Transactions) != 4 {
		t.Fatalf("expected 4 extracted transactions, got %d", len(stmt.Transactions))
	}

	spotTx := stmt.Transactions[0]
	if spotTx.Kind != domain.TransactionKindSpot || spotTx.Amount.Fixed4() != "250.0000" {
		t.Fatalf("unexpected spot transaction: %+v", spotTx)
	}

	instTx := stmt.Transactions[1]
	if instTx.Kind != domain.TransactionKindInstallment || instTx.CurrentInstallment != 3 || instTx.TotalInstallments != 12 {
		t.Fatalf("unexpected installment transaction: %+v", instTx)
	}

	refundTx := stmt.Transactions[2]
	if refundTx.Kind != domain.TransactionKindRefund || !refundTx.Amount.IsNegative() || refundTx.Amount.Fixed4() != "-45.5000" {
		t.Fatalf("unexpected refund transaction: %+v", refundTx)
	}

	feeTx := stmt.Transactions[3]
	if feeTx.Kind != domain.TransactionKindBankFee || feeTx.Amount.Fixed4() != "9.9000" {
		t.Fatalf("unexpected bank fee transaction: %+v", feeTx)
	}

	// 4. Reimport same statement and verify 100% identical SHA-256 idempotency keys
	reimportRAM := []byte(rawStatement)
	stmtReimported, err := parser.ParseStatementInRAM(
		context.Background(),
		tenantID,
		domain.IssuerItau,
		"4532",
		"123456",
		reimportRAM,
	)
	if err != nil {
		t.Fatalf("unexpected error on reimport: %v", err)
	}
	for i := range stmt.Transactions {
		if stmt.Transactions[i].IdempotencyKey != stmtReimported.Transactions[i].IdempotencyKey {
			t.Fatalf("idempotency key mismatch at index %d on reimport", i)
		}
	}
}

func TestZeroPersistencePDFParser_ChecksumMismatchStillWipesRAM(t *testing.T) {
	t.Parallel()

	corruptedStatement := `
REF: 2026-11
VENCIMENTO: 2026-11-25
TOTAL_FATURA: R$ 500,00
2026-11-03 | SUPERMERCADO PAO DE ACUCAR | 250,00
`
	buf := []byte(corruptedStatement)
	parser := pdf.NewZeroPersistencePDFParser()
	_, err := parser.ParseStatementInRAM(
		context.Background(),
		"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		domain.IssuerBB,
		"9876",
		"",
		buf,
	)
	if !errors.Is(err, domain.ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
	for i, b := range buf {
		if b != 0 {
			t.Fatalf("expected RAM buffer to be zeroed even on checksum error at byte %d", i)
		}
	}
}
