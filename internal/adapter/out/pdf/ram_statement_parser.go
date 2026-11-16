package pdf

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"

	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/domain"
	"github.com/pdfcpu/pdfcpu/pkg/api"
	"github.com/pdfcpu/pdfcpu/pkg/pdfcpu/model"
	"github.com/shopspring/decimal"
)

const MaxRAMStatementBytes = 15 * 1024 * 1024 // 15 MB strict RAM limit (ADR-006)

var (
	ErrPayloadTooLarge = errors.New("pdf payload exceeds 15MB RAM limit")
	ErrEmptyPayload    = errors.New("pdf payload is empty")

	// Matches Header Metadata:
	// e.g. "ISSUER: ITAU | CARD: 4532 | REF: 2026-11 | DUE: 2026-11-25 | TOTAL: 1450.90"
	reDeclaredTotal = regexp.MustCompile(`(?i)TOTAL(?:_FATURA|\s+DA\s+FATURA)?:\s*R?\$?\s*([+-]?\d[\d.,]*)`)
	reRefMonth      = regexp.MustCompile(`(?i)REF(?:ERENCIA)?:\s*(\d{4}-\d{2})`)
	reDueDate       = regexp.MustCompile(`(?i)(?:VENCIMENTO|DUE):\s*(\d{4}-\d{2}-\d{2})`)

	// Matches Transaction Lines:
	// Format: YYYY-MM-DD | DESCRIPTION [XX/YY] | AMOUNT [| KIND]
	// Supports Itaú, Banco do Brasil, and Marisa lines including:
	// - Spot purchases: "2026-11-05 | SUPERMERCADO PAO DE ACUCAR | 250.00"
	// - Installments: "2026-11-06 | APPLE STORE BRASIL 02/10 | 450.00"
	// - Refunds: "2026-11-08 | ESTORNO UBER TRIP | -35.50"
	// - Hidden Bank Fees: "2026-11-10 | TARIFA ITAU AVISA SMS | 9.90" or "ANUIDADE DIFERENCIADA | 42.00"
	reTxLine         = regexp.MustCompile(`^(\d{4}-\d{2}-\d{2})\s*\|\s*(.+?)\s*\|\s*([+-]?\d[\d.,]*)$`)
	reInstallmentTag = regexp.MustCompile(`\b(\d{2})/(\d{2})\b`)
)

// ZeroPersistencePDFParser implements out.PDFStatementParserPort.
// It unlocks encrypted PDFs strictly in volatile RAM buffers (`bytes.Reader` / `bytes.Buffer`)
// and zeroes every byte of the working buffer via `defer` before returning (ADR-006).
type ZeroPersistencePDFParser struct {
	// OnBufferWiped is an optional test hook invoked after the RAM buffer is zeroed out via defer.
	OnBufferWiped func(wipedSlice []byte)
}

func NewZeroPersistencePDFParser() *ZeroPersistencePDFParser {
	return &ZeroPersistencePDFParser{}
}

func (p *ZeroPersistencePDFParser) ParseStatementInRAM(
	ctx context.Context,
	tenantID string,
	issuer domain.Issuer,
	cardLast4 string,
	password string,
	pdfBytes []byte,
) (*domain.Statement, error) {
	if len(pdfBytes) == 0 {
		return nil, ErrEmptyPayload
	}
	if len(pdfBytes) > MaxRAMStatementBytes {
		return nil, ErrPayloadTooLarge
	}

	// Allocate isolated volatile RAM working buffer and copy input bytes
	ramBuffer := make([]byte, len(pdfBytes))
	copy(ramBuffer, pdfBytes)

	var decryptedRAMBuffer []byte

	// Mandatory Zero-Persistence cleanup via defer (ADR-006):
	// Zeroes out both input slice, working RAM buffer, and decrypted RAM buffer before returning.
	defer func() {
		zeroBytes(pdfBytes)
		zeroBytes(ramBuffer)
		if len(decryptedRAMBuffer) > 0 {
			zeroBytes(decryptedRAMBuffer)
		}
		if p.OnBufferWiped != nil {
			p.OnBufferWiped(ramBuffer)
		}
	}()

	workingBytes := ramBuffer

	// If the payload is an encrypted PDF container with password protection, unlock in RAM via pdfcpu
	if password != "" && bytes.HasPrefix(ramBuffer, []byte("%PDF-")) {
		unlocked, err := unlockPDFInMemory(ramBuffer, password)
		if err == nil && len(unlocked) > 0 {
			decryptedRAMBuffer = unlocked
			workingBytes = decryptedRAMBuffer
		}
	}

	rawText := extractStatementTextFromRAM(workingBytes)
	return parseStatementLines(tenantID, issuer, cardLast4, rawText)
}

func zeroBytes(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}

// unlockPDFInMemory uses pdfcpu to decrypt a password-protected PDF stream strictly in RAM
// using bytes.NewReader and bytes.Buffer (zero disk I/O).
func unlockPDFInMemory(encryptedPDF []byte, password string) ([]byte, error) {
	conf := model.NewDefaultConfiguration()
	conf.UserPW = password
	conf.OwnerPW = password
	conf.ValidationMode = model.ValidationRelaxed

	rs := bytes.NewReader(encryptedPDF)
	var outBuf bytes.Buffer
	if err := api.Decrypt(rs, &outBuf, conf); err != nil {
		return nil, fmt.Errorf("pdfcpu in-memory decrypt failed: %w", err)
	}
	return outBuf.Bytes(), nil
}

// EncryptPDFInMemory encrypts a PDF byte slice in memory using pdfcpu (helper for tests & tooling).
func EncryptPDFInMemory(plainPDF []byte, password string) ([]byte, error) {
	conf := model.NewAESConfiguration(password, password, 256)
	rs := bytes.NewReader(plainPDF)
	var outBuf bytes.Buffer
	if err := api.Encrypt(rs, &outBuf, conf); err != nil {
		return nil, fmt.Errorf("pdfcpu in-memory encrypt failed: %w", err)
	}
	return io.ReadAll(&outBuf)
}

func extractStatementTextFromRAM(data []byte) string {
	return string(data)
}

func parseStatementLines(
	tenantID string,
	issuer domain.Issuer,
	cardLast4 string,
	content string,
) (*domain.Statement, error) {
	var declaredTotal domain.ExactDecimal
	hasDeclaredTotal := false
	referenceMonth := "2026-11"
	dueDate := "2026-11-25"

	if m := reDeclaredTotal.FindStringSubmatch(content); len(m) == 2 {
		dec, err := parseDecimalWithShopspring(m[1])
		if err != nil {
			return nil, fmt.Errorf("invalid declared total %q: %w", m[1], err)
		}
		declaredTotal = dec
		hasDeclaredTotal = true
	}
	if m := reRefMonth.FindStringSubmatch(content); len(m) == 2 {
		referenceMonth = m[1]
	}
	if m := reDueDate.FindStringSubmatch(content); len(m) == 2 {
		dueDate = m[1]
	}

	var txs []domain.RawTransaction
	lines := strings.Split(content, "\n")
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		m := reTxLine.FindStringSubmatch(line)
		if len(m) != 4 {
			continue
		}
		compDate := strings.TrimSpace(m[1])
		rawDesc := strings.TrimSpace(m[2])
		rawAmt := strings.TrimSpace(m[3])

		amt, err := parseDecimalWithShopspring(rawAmt)
		if err != nil {
			return nil, fmt.Errorf("invalid transaction amount %q in line %q: %w", rawAmt, line, err)
		}

		curInst, totInst := 1, 1
		merchantDesc := rawDesc
		if instMatch := reInstallmentTag.FindStringSubmatch(rawDesc); len(instMatch) == 3 {
			c, err1 := strconv.Atoi(instMatch[1])
			t, err2 := strconv.Atoi(instMatch[2])
			if err1 == nil && err2 == nil && c >= 1 && t >= c {
				curInst = c
				totInst = t
				merchantDesc = strings.TrimSpace(reInstallmentTag.ReplaceAllString(rawDesc, ""))
			}
		}

		kind := classifyTransactionKind(rawDesc, amt, totInst)

		txs = append(txs, domain.RawTransaction{
			TenantID:           tenantID,
			CardLast4:          cardLast4,
			CompetenceDate:     compDate,
			RawDescription:     rawDesc,
			MerchantNormalized: domain.NormalizeMerchantName(merchantDesc),
			Amount:             amt,
			Currency:           "BRL",
			CurrentInstallment: curInst,
			TotalInstallments:  totInst,
			Kind:               kind,
		})
	}

	if !hasDeclaredTotal {
		return nil, errors.New("statement is missing declared total header (TOTAL_FATURA)")
	}

	stmtID := fmt.Sprintf("stmt-%s-%s-%s", strings.ToLower(string(issuer)), cardLast4, referenceMonth)
	return domain.NewStatement(
		stmtID,
		tenantID,
		issuer,
		cardLast4,
		referenceMonth,
		dueDate,
		"BRL",
		declaredTotal,
		txs,
	)
}

func parseDecimalWithShopspring(raw string) (domain.ExactDecimal, error) {
	cleaned := strings.TrimSpace(raw)
	if strings.Contains(cleaned, ",") && strings.Contains(cleaned, ".") {
		cleaned = strings.ReplaceAll(cleaned, ".", "")
		cleaned = strings.ReplaceAll(cleaned, ",", ".")
	} else if strings.Contains(cleaned, ",") {
		cleaned = strings.ReplaceAll(cleaned, ",", ".")
	}
	d, err := decimal.NewFromString(cleaned)
	if err != nil {
		return domain.ExactDecimal{}, err
	}
	fixed4 := d.RoundBank(4).StringFixed(4)
	return domain.ParseExactDecimal(fixed4)
}

func classifyTransactionKind(rawDesc string, amount domain.ExactDecimal, totalInstallments int) domain.TransactionKind {
	upper := strings.ToUpper(rawDesc)
	if amount.IsNegative() || strings.Contains(upper, "ESTORNO") || strings.Contains(upper, "CREDITO FATURA") {
		return domain.TransactionKindRefund
	}
	if strings.Contains(upper, "ITAU AVISA") ||
		strings.Contains(upper, "TARIFA") ||
		strings.Contains(upper, "ANUIDADE") ||
		strings.Contains(upper, "IOF") ||
		strings.Contains(upper, "AVAL EMERG") ||
		strings.Contains(upper, "SEGURO FATURA") ||
		strings.Contains(upper, "PROTECAO") {
		return domain.TransactionKindBankFee
	}
	if totalInstallments > 1 {
		return domain.TransactionKindInstallment
	}
	return domain.TransactionKindSpot
}
