package service

import (
	"context"
	"fmt"

	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/application/port/in"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/application/port/out"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/domain"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

const (
	DefaultStatementsBucket = "pfm-statements-encrypted-local"
	DefaultKMSKeyAlias      = "alias/pfm-tenant-master-key"
)

// StatementIngestionService implements in.IngestStatementUseCase orchestrating RAM-only PDF parsing,
// KMS envelope encryption, S3 archival, and Kafka event emission.
type StatementIngestionService struct {
	pdfParser      out.PDFStatementParserPort
	secretVault    out.SecretVaultPort
	objectStorage  out.ObjectStoragePort
	eventPublisher out.EventPublisherPort
	bucketName     string
	kmsKeyAlias    string
}

func NewStatementIngestionService(
	pdfParser out.PDFStatementParserPort,
	secretVault out.SecretVaultPort,
	objectStorage out.ObjectStoragePort,
	eventPublisher out.EventPublisherPort,
) *StatementIngestionService {
	return &StatementIngestionService{
		pdfParser:      pdfParser,
		secretVault:    secretVault,
		objectStorage:  objectStorage,
		eventPublisher: eventPublisher,
		bucketName:     DefaultStatementsBucket,
		kmsKeyAlias:    DefaultKMSKeyAlias,
	}
}

func (s *StatementIngestionService) IngestStatement(
	ctx context.Context,
	cmd in.UploadStatementCommand,
) (*in.UploadStatementResult, error) {
	tracer := otel.Tracer("pfm-ingestion-bff-go/application")
	ctx, span := tracer.Start(ctx, "StatementIngestionService.IngestStatement")
	defer span.End()

	// Zero out caller's slice on exit as an additional defense-in-depth layer (ADR-006)
	defer func() {
		clear(cmd.PDFPayloadRAM)
	}()

	if cmd.TenantID == "" {
		return nil, domain.ErrEmptyTenantID
	}

	span.SetAttributes(
		attribute.String("pfm.tenant_id", cmd.TenantID),
		attribute.String("pfm.issuer", string(cmd.Issuer)),
	)

	password := cmd.Password
	if password == "" && cmd.SecretName != "" && s.secretVault != nil {
		resolved, err := s.secretVault.GetSecretValue(ctx, cmd.SecretName)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve PDF unlock secret from vault: %w", err)
		}
		password = resolved
	}

	// Copy payload for optional encrypted archival before parser zeroes its working RAM buffer
	var encryptedCopy []byte
	var s3Key string
	if s.secretVault != nil && s.objectStorage != nil {
		ciphertext, err := s.secretVault.EncryptEnvelope(ctx, cmd.TenantID, s.kmsKeyAlias, cmd.PDFPayloadRAM)
		if err != nil {
			return nil, fmt.Errorf("kms envelope encryption failed: %w", err)
		}
		encryptedCopy = ciphertext
	}

	stmt, err := s.pdfParser.ParseStatementInRAM(
		ctx,
		cmd.TenantID,
		cmd.Issuer,
		cmd.CardLast4,
		password,
		cmd.PDFPayloadRAM,
	)
	if err != nil {
		return nil, err
	}

	if len(encryptedCopy) > 0 && s.objectStorage != nil {
		s3Key = fmt.Sprintf("tenants/%s/statements/%s/%s.pdf.enc", cmd.TenantID, stmt.ReferenceMonth, stmt.StatementID)
		if err := s.objectStorage.PutEncryptedStatement(ctx, s.bucketName, s3Key, encryptedCopy); err != nil {
			return nil, fmt.Errorf("failed to store encrypted statement in object storage: %w", err)
		}
	}

	if s.eventPublisher != nil {
		if err := s.eventPublisher.PublishStatementTransactions(ctx, stmt); err != nil {
			return nil, fmt.Errorf("failed to publish extracted transactions to kafka: %w", err)
		}
	}

	keys := make([]string, len(stmt.Transactions))
	for i, tx := range stmt.Transactions {
		keys[i] = tx.IdempotencyKey
	}

	return &in.UploadStatementResult{
		StatementID:       stmt.StatementID,
		TenantID:          stmt.TenantID,
		Issuer:            stmt.Issuer,
		ReferenceMonth:    stmt.ReferenceMonth,
		TransactionsCount: len(stmt.Transactions),
		DeclaredTotal:     stmt.Checksum.DeclaredTotal.Fixed4(),
		ComputedTotal:     stmt.Checksum.ComputedTotal.Fixed4(),
		ChecksumValid:     stmt.Checksum.IsValid,
		EncryptedS3Key:    s3Key,
		IdempotencyKeys:   keys,
	}, nil
}
