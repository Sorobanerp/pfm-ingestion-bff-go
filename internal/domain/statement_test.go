package domain_test

import (
	"errors"
	"testing"

	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/domain"
)

func TestComputeIdempotencyKey_DeterministicSHA256OnReimport(t *testing.T) {
	t.Parallel()

	amt, err := domain.ParseExactDecimal("450.75")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	key1 := domain.ComputeIdempotencyKey(
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"4532",
		"2026-11-10",
		amt,
		"  Apple   Store   Brasil ",
		2,
		10,
	)

	// Reimport of the exact same transaction with different whitespace/casing must yield identical SHA-256
	key2 := domain.ComputeIdempotencyKey(
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"4532",
		"2026-11-10",
		amt,
		"APPLE STORE BRASIL",
		2,
		10,
	)

	if key1 != key2 {
		t.Fatalf("expected identical idempotency key on reimport, got %s vs %s", key1, key2)
	}
	if len(key1) != 64 {
		t.Fatalf("expected 64-char SHA-256 hex key, got length %d (%s)", len(key1), key1)
	}

	// Changing installment number must produce a distinct SHA-256 hash
	keyNextInstallment := domain.ComputeIdempotencyKey(
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		"4532",
		"2026-11-10",
		amt,
		"APPLE STORE BRASIL",
		3,
		10,
	)
	if key1 == keyNextInstallment {
		t.Fatalf("expected distinct idempotency key for different installment number")
	}
}

func TestStatementChecksumValidation(t *testing.T) {
	t.Parallel()

	d1, _ := domain.ParseExactDecimal("300.00")
	d2, _ := domain.ParseExactDecimal("-50.00")
	d3, _ := domain.ParseExactDecimal("9.90")
	declared, _ := domain.ParseExactDecimal("259.90")

	txs := []domain.RawTransaction{
		{CompetenceDate: "2026-11-01", RawDescription: "COMPRA SPOT", Amount: d1, Kind: domain.TransactionKindSpot},
		{CompetenceDate: "2026-11-02", RawDescription: "ESTORNO COMPRA", Amount: d2, Kind: domain.TransactionKindRefund},
		{CompetenceDate: "2026-11-03", RawDescription: "ITAU AVISA SMS", Amount: d3, Kind: domain.TransactionKindBankFee},
	}

	stmt, err := domain.NewStatement(
		"stmt-01",
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		domain.IssuerItau,
		"4532",
		"2026-11",
		"2026-11-25",
		"BRL",
		declared,
		txs,
	)
	if err != nil {
		t.Fatalf("expected valid checksum, got error: %v", err)
	}
	if !stmt.Checksum.IsValid {
		t.Fatalf("expected Checksum.IsValid == true")
	}

	wrongDeclared, _ := domain.ParseExactDecimal("999.99")
	_, err = domain.NewStatement(
		"stmt-01",
		"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		domain.IssuerItau,
		"4532",
		"2026-11",
		"2026-11-25",
		"BRL",
		wrongDeclared,
		txs,
	)
	if !errors.Is(err, domain.ErrChecksumMismatch) {
		t.Fatalf("expected ErrChecksumMismatch, got %v", err)
	}
}
