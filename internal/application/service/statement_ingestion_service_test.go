package service

import (
	"context"
	"errors"
	"testing"

	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/application/port/in"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/domain"
)

type fakeParser struct {
	stmt     *domain.Statement
	err      error
	password string
}

func (f *fakeParser) ParseStatementInRAM(_ context.Context, _ string, _ domain.Issuer, _ string, password string, _ []byte) (*domain.Statement, error) {
	f.password = password
	return f.stmt, f.err
}

type fakeVault struct {
	secret    string
	secretErr error
	encErr    error
	cipher    []byte
	gotSecret string
}

func (f *fakeVault) EncryptEnvelope(context.Context, string, string, []byte) ([]byte, error) {
	return f.cipher, f.encErr
}
func (f *fakeVault) GetSecretValue(_ context.Context, id string) (string, error) {
	f.gotSecret = id
	return f.secret, f.secretErr
}

type fakeStorage struct {
	err    error
	bucket string
	key    string
	data   []byte
}

func (f *fakeStorage) PutEncryptedStatement(_ context.Context, b, k string, d []byte) error {
	f.bucket, f.key, f.data = b, k, d
	return f.err
}

type fakePublisher struct {
	err   error
	calls int
}

func (f *fakePublisher) PublishStatementTransactions(context.Context, *domain.Statement) error {
	f.calls++
	return f.err
}

func sampleStatement(t *testing.T) *domain.Statement {
	t.Helper()
	amt := domain.NewExactDecimalFromScaled(12345)
	st, err := domain.NewStatement("stmt-1", "tenant-1", domain.IssuerItau, "1234", "2025-01", "2025-02-01", "BRL", amt,
		[]domain.RawTransaction{{RawDescription: "LOJA", Amount: amt, CompetenceDate: "2025-01-10"}})
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestIngestStatement(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name        string
		cmd         in.UploadStatementCommand
		parser      *fakeParser
		vault       *fakeVault
		storage     *fakeStorage
		pub         *fakePublisher
		nilVault    bool
		nilStorage  bool
		nilPub      bool
		wantErr     error
		wantKey     string
		wantPwd     string
		wantPubCall int
	}{
		{name: "empty tenant", cmd: in.UploadStatementCommand{}, parser: &fakeParser{}, wantErr: domain.ErrEmptyTenantID, nilVault: true, nilStorage: true, nilPub: true},
		{name: "secret resolve error", cmd: in.UploadStatementCommand{TenantID: "tenant-1", SecretName: "s"}, parser: &fakeParser{}, vault: &fakeVault{secretErr: boom}, nilStorage: true, nilPub: true, wantErr: boom},
		{name: "secret resolved and used", cmd: in.UploadStatementCommand{TenantID: "tenant-1", SecretName: "s"}, parser: &fakeParser{stmt: sampleStatement(t)}, vault: &fakeVault{secret: "pw"}, nilStorage: true, nilPub: true, wantPwd: "pw"},
		{name: "explicit password wins", cmd: in.UploadStatementCommand{TenantID: "tenant-1", SecretName: "s", Password: "own"}, parser: &fakeParser{stmt: sampleStatement(t)}, vault: &fakeVault{secret: "pw"}, nilStorage: true, nilPub: true, wantPwd: "own"},
		{name: "encrypt error", cmd: in.UploadStatementCommand{TenantID: "tenant-1"}, parser: &fakeParser{}, vault: &fakeVault{encErr: boom}, storage: &fakeStorage{}, nilPub: true, wantErr: boom},
		{name: "parser error", cmd: in.UploadStatementCommand{TenantID: "tenant-1"}, parser: &fakeParser{err: boom}, nilVault: true, nilStorage: true, nilPub: true, wantErr: boom},
		{name: "storage error", cmd: in.UploadStatementCommand{TenantID: "tenant-1"}, parser: &fakeParser{stmt: sampleStatement(t)}, vault: &fakeVault{cipher: []byte("enc")}, storage: &fakeStorage{err: boom}, nilPub: true, wantErr: boom},
		{name: "publish error", cmd: in.UploadStatementCommand{TenantID: "tenant-1"}, parser: &fakeParser{stmt: sampleStatement(t)}, nilVault: true, nilStorage: true, pub: &fakePublisher{err: boom}, wantErr: boom, wantPubCall: 1},
		{name: "full success", cmd: in.UploadStatementCommand{TenantID: "tenant-1", PDFPayloadRAM: []byte("pdf")}, parser: &fakeParser{stmt: sampleStatement(t)}, vault: &fakeVault{cipher: []byte("enc")}, storage: &fakeStorage{}, pub: &fakePublisher{}, wantKey: "tenants/tenant-1/statements/2025-01/stmt-1.pdf.enc", wantPubCall: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var vault *fakeVault
			svc := NewStatementIngestionService(tc.parser, nil, nil, nil)
			if !tc.nilVault {
				vault = tc.vault
				svc.secretVault = vault
			}
			if !tc.nilStorage {
				svc.objectStorage = tc.storage
			}
			if !tc.nilPub {
				svc.eventPublisher = tc.pub
			}
			res, err := svc.IngestStatement(context.Background(), tc.cmd)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("err=%v want %v", err, tc.wantErr)
				}
				if res != nil {
					t.Fatal("result must be nil on error")
				}
			} else if err != nil {
				t.Fatal(err)
			} else {
				if res.StatementID != "stmt-1" || res.TransactionsCount != 1 || !res.ChecksumValid ||
					res.DeclaredTotal != "1.2345" || res.ComputedTotal != "1.2345" ||
					len(res.IdempotencyKeys) != 1 || res.IdempotencyKeys[0] == "" || res.EncryptedS3Key != tc.wantKey {
					t.Fatalf("unexpected result %+v", res)
				}
			}
			if tc.wantPwd != "" && tc.parser.password != tc.wantPwd {
				t.Fatalf("password=%q want %q", tc.parser.password, tc.wantPwd)
			}
			if tc.pub != nil && tc.pub.calls != tc.wantPubCall {
				t.Fatalf("publish calls=%d", tc.pub.calls)
			}
			if tc.wantKey != "" && (tc.storage.key != tc.wantKey || tc.storage.bucket != DefaultStatementsBucket || string(tc.storage.data) != "enc") {
				t.Fatalf("storage got %+v", tc.storage)
			}
		})
	}
}

func TestIngestStatement_ZeroesCallerPayload(t *testing.T) {
	payload := []byte("secret-pdf")
	svc := NewStatementIngestionService(&fakeParser{stmt: sampleStatement(t)}, nil, nil, nil)
	if _, err := svc.IngestStatement(context.Background(), in.UploadStatementCommand{TenantID: "t", PDFPayloadRAM: payload}); err != nil {
		t.Fatal(err)
	}
	for _, b := range payload {
		if b != 0 {
			t.Fatal("payload not wiped")
		}
	}
}
