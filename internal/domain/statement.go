package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
)

// ScaleFactor represents 10^4 (4 decimal places, matching PostgreSQL NUMERIC(19,4) - RN06).
const ScaleFactor int64 = 10000

var (
	ErrChecksumMismatch = errors.New("statement checksum mismatch: declared total does not equal computed total")
	ErrEmptyTenantID    = errors.New("tenant_id must not be empty")
)

// TransactionKind classifies the nature of an extracted credit card statement line.
type TransactionKind string

const (
	TransactionKindSpot        TransactionKind = "SPOT"
	TransactionKindInstallment TransactionKind = "INSTALLMENT"
	TransactionKindRefund      TransactionKind = "REFUND"
	TransactionKindBankFee     TransactionKind = "BANK_FEE"
)

// Issuer identifies the supported banking institution statement layout.
type Issuer string

const (
	IssuerItau   Issuer = "ITAU"
	IssuerBB     Issuer = "BANCO_DO_BRASIL"
	IssuerMarisa Issuer = "MARISA"
)

// ExactDecimal is a pure-Go exact decimal representation scaled at 4 decimal places (NUMERIC(19,4))
// with zero external library imports in the domain layer (RN06).
type ExactDecimal struct {
	scaled int64 // value * 10000
}

// ParseExactDecimal parses a canonical or Brazilian formatted decimal string into an ExactDecimal (scale 4).
func ParseExactDecimal(raw string) (ExactDecimal, error) {
	cleaned := strings.TrimSpace(raw)
	if cleaned == "" {
		return ExactDecimal{}, errors.New("empty decimal string")
	}
	// Handle Brazilian format "1.234,56" vs canonical "1234.56"
	if strings.Contains(cleaned, ",") && strings.Contains(cleaned, ".") {
		cleaned = strings.ReplaceAll(cleaned, ".", "")
		cleaned = strings.ReplaceAll(cleaned, ",", ".")
	} else if strings.Contains(cleaned, ",") {
		cleaned = strings.ReplaceAll(cleaned, ",", ".")
	}

	r := new(big.Rat)
	if _, ok := r.SetString(cleaned); !ok {
		return ExactDecimal{}, fmt.Errorf("invalid decimal string: %q", raw)
	}
	scaledRat := new(big.Rat).Mul(r, big.NewRat(ScaleFactor, 1))
	// Banker's rounding / exact integer check
	num := scaledRat.Num()
	den := scaledRat.Denom()
	quo, rem := new(big.Int).QuoRem(num, den, new(big.Int))
	// Half-even rounding if needed
	twiceRem := new(big.Int).Mul(rem, big.NewInt(2))
	cmp := twiceRem.CmpAbs(den)
	if cmp > 0 || (cmp == 0 && quo.Bit(0) == 1) {
		if num.Sign() >= 0 {
			quo.Add(quo, big.NewInt(1))
		} else {
			quo.Sub(quo, big.NewInt(1))
		}
	}
	return ExactDecimal{scaled: quo.Int64()}, nil
}

func NewExactDecimalFromScaled(scaled int64) ExactDecimal {
	return ExactDecimal{scaled: scaled}
}

func (d ExactDecimal) Add(other ExactDecimal) ExactDecimal {
	return ExactDecimal{scaled: d.scaled + other.scaled}
}

func (d ExactDecimal) Sub(other ExactDecimal) ExactDecimal {
	return ExactDecimal{scaled: d.scaled - other.scaled}
}

func (d ExactDecimal) Neg() ExactDecimal {
	return ExactDecimal{scaled: -d.scaled}
}

func (d ExactDecimal) Scaled() int64 {
	return d.scaled
}

func (d ExactDecimal) IsNegative() bool {
	return d.scaled < 0
}

func (d ExactDecimal) IsZero() bool {
	return d.scaled == 0
}

func (d ExactDecimal) Equal(other ExactDecimal) bool {
	return d.scaled == other.scaled
}

// Fixed4 returns the canonical 4-decimal string representation (e.g., "1450.9900" or "-45.9000").
func (d ExactDecimal) Fixed4() string {
	sign := ""
	val := d.scaled
	if val < 0 {
		sign = "-"
		val = -val
	}
	integerPart := val / ScaleFactor
	fractionalPart := val % ScaleFactor
	return fmt.Sprintf("%s%d.%04d", sign, integerPart, fractionalPart)
}

// RawTransaction represents an individual line item extracted from a credit card statement.
type RawTransaction struct {
	TenantID           string
	StatementID        string
	CardLast4          string
	CompetenceDate     string // YYYY-MM-DD
	RawDescription     string
	MerchantNormalized string
	Amount             ExactDecimal
	Currency           string
	CurrentInstallment int
	TotalInstallments  int
	Kind               TransactionKind
	IdempotencyKey     string
}

// ComputeIdempotencyKey calculates the deterministic SHA-256 hash required by ADR-004 / RN04:
// SHA-256("tenant_id|card_last4|competence_date|amount_fixed_4|merchant_normalized|parcela_atual/total")
func ComputeIdempotencyKey(
	tenantID string,
	cardLast4 string,
	competenceDate string,
	amount ExactDecimal,
	merchantNormalized string,
	currentInstallment int,
	totalInstallments int,
) string {
	instCurrent := currentInstallment
	instTotal := totalInstallments
	if instCurrent <= 0 {
		instCurrent = 1
	}
	if instTotal <= 0 {
		instTotal = 1
	}
	canonical := fmt.Sprintf(
		"%s|%s|%s|%s|%s|%02d/%02d",
		strings.TrimSpace(tenantID),
		strings.TrimSpace(cardLast4),
		strings.TrimSpace(competenceDate),
		amount.Fixed4(),
		NormalizeMerchantName(merchantNormalized),
		instCurrent,
		instTotal,
	)
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

// NormalizeMerchantName normalizes whitespace and casing for deterministic idempotency hashing.
func NormalizeMerchantName(raw string) string {
	fields := strings.Fields(strings.ToUpper(strings.TrimSpace(raw)))
	return strings.Join(fields, " ")
}

// ChecksumReport holds the reconciliation audit between the PDF header's declared invoice total
// and the arithmetic sum of all extracted transactions (purchases + fees - refunds).
type ChecksumReport struct {
	DeclaredTotal ExactDecimal
	ComputedTotal ExactDecimal
	Difference    ExactDecimal
	IsValid       bool
}

// Statement is the root aggregate of an extracted credit card invoice.
type Statement struct {
	StatementID    string
	TenantID       string
	Issuer         Issuer
	CardLast4      string
	ReferenceMonth string // YYYY-MM
	DueDate        string // YYYY-MM-DD
	Currency       string
	DeclaredTotal  ExactDecimal
	Transactions   []RawTransaction
	Checksum       ChecksumReport
}

// NewStatement constructs a Statement aggregate, populates deterministic idempotency keys on all lines,
// and computes the ChecksumReport.
func NewStatement(
	statementID string,
	tenantID string,
	issuer Issuer,
	cardLast4 string,
	referenceMonth string,
	dueDate string,
	currency string,
	declaredTotal ExactDecimal,
	txs []RawTransaction,
) (*Statement, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, ErrEmptyTenantID
	}
	if currency == "" {
		currency = "BRL"
	}

	var computed ExactDecimal
	enriched := make([]RawTransaction, len(txs))
	for i, tx := range txs {
		tx.TenantID = tenantID
		tx.StatementID = statementID
		if tx.CardLast4 == "" {
			tx.CardLast4 = cardLast4
		}
		if tx.Currency == "" {
			tx.Currency = currency
		}
		tx.MerchantNormalized = NormalizeMerchantName(tx.MerchantNormalized)
		if tx.MerchantNormalized == "" {
			tx.MerchantNormalized = NormalizeMerchantName(tx.RawDescription)
		}
		if tx.CurrentInstallment <= 0 {
			tx.CurrentInstallment = 1
		}
		if tx.TotalInstallments <= 0 {
			tx.TotalInstallments = 1
		}
		tx.IdempotencyKey = ComputeIdempotencyKey(
			tx.TenantID,
			tx.CardLast4,
			tx.CompetenceDate,
			tx.Amount,
			tx.MerchantNormalized,
			tx.CurrentInstallment,
			tx.TotalInstallments,
		)
		computed = computed.Add(tx.Amount)
		enriched[i] = tx
	}

	diff := declaredTotal.Sub(computed)
	report := ChecksumReport{
		DeclaredTotal: declaredTotal,
		ComputedTotal: computed,
		Difference:    diff,
		IsValid:       declaredTotal.Equal(computed),
	}

	stmt := &Statement{
		StatementID:    statementID,
		TenantID:       tenantID,
		Issuer:         issuer,
		CardLast4:      cardLast4,
		ReferenceMonth: referenceMonth,
		DueDate:        dueDate,
		Currency:       currency,
		DeclaredTotal:  declaredTotal,
		Transactions:   enriched,
		Checksum:       report,
	}

	if !report.IsValid {
		return stmt, fmt.Errorf(
			"%w: declared=%s computed=%s diff=%s",
			ErrChecksumMismatch,
			declaredTotal.Fixed4(),
			computed.Fixed4(),
			diff.Fixed4(),
		)
	}
	return stmt, nil
}
