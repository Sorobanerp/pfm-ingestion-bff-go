package out

import (
	"context"

	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/domain"
)

// PDFStatementParserPort unlocks password-protected PDFs strictly in RAM (wiping buffer via defer)
// and extracts the Statement aggregate with Checksum verification.
type PDFStatementParserPort interface {
	ParseStatementInRAM(
		ctx context.Context,
		tenantID string,
		issuer domain.Issuer,
		cardLast4 string,
		password string,
		pdfBytes []byte,
	) (*domain.Statement, error)
}

// SecretVaultPort interacts with AWS KMS & Secrets Manager (or LocalStack on port 4566)
// for Envelope Encryption and secret retrieval.
type SecretVaultPort interface {
	EncryptEnvelope(ctx context.Context, tenantID string, keyAlias string, plaintext []byte) ([]byte, error)
	GetSecretValue(ctx context.Context, secretID string) (string, error)
}

// ObjectStoragePort uploads KMS-encrypted statement blobs to S3-compatible object storage.
type ObjectStoragePort interface {
	PutEncryptedStatement(ctx context.Context, bucket string, objectKey string, encryptedPayload []byte) error
}

// EventPublisherPort publishes extracted transactions to Apache Kafka partitioned by tenant_id.
type EventPublisherPort interface {
	PublishStatementTransactions(ctx context.Context, statement *domain.Statement) error
}
