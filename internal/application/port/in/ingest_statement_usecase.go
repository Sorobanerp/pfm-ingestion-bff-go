package in

import (
	"context"

	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/domain"
)

// UploadStatementCommand encapsulates an incoming statement upload request.
type UploadStatementCommand struct {
	TenantID      string
	Issuer        domain.Issuer
	CardLast4     string
	Password      string
	SecretName    string
	FileName      string
	PDFPayloadRAM []byte
}

// UploadStatementResult summarizes the outcome of a statement ingestion workflow.
type UploadStatementResult struct {
	StatementID       string
	TenantID          string
	Issuer            domain.Issuer
	ReferenceMonth    string
	TransactionsCount int
	DeclaredTotal     string
	ComputedTotal     string
	ChecksumValid     bool
	EncryptedS3Key    string
	IdempotencyKeys   []string
}

// IngestStatementUseCase defines the Inbound Port for processing PDF statements in RAM,
// archiving encrypted payloads, and emitting domain events to Kafka.
type IngestStatementUseCase interface {
	IngestStatement(ctx context.Context, cmd UploadStatementCommand) (*UploadStatementResult, error)
}
