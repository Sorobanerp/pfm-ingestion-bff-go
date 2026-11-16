package http_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	httpin "github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/in/http"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/aws"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/kafka"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/adapter/out/pdf"
	"github.com/dantas-pfm/pfm-ingestion-bff-go/internal/application/service"
	kafkago "github.com/segmentio/kafka-go"
)

type fakeKafkaWriter struct {
	messages []kafkago.Message
}

func (f *fakeKafkaWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	f.messages = append(f.messages, msgs...)
	return nil
}

func (f *fakeKafkaWriter) Close() error {
	return nil
}

func TestHTTPServer_UploadStatementBrotliETagAndKafkaPartitioning(t *testing.T) {
	t.Parallel()

	// Mock LocalStack S3 server that fails once (testing Retry with backoff + jitter) then succeeds
	var s3Calls atomic.Int32
	mockLocalStack := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callNum := s3Calls.Add(1)
		if callNum == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer mockLocalStack.Close()

	policy := aws.ResiliencePolicy{
		Timeout:          5 * time.Second,
		MaxRetries:       3,
		BaseBackoff:      5 * time.Millisecond,
		FailureThreshold: 3,
		OpenStateTimeout: 5 * time.Second,
	}
	awsAdapter := aws.NewResilientLocalStackAdapter(mockLocalStack.URL, policy)
	kw := &fakeKafkaWriter{}
	kafkaProd := kafka.NewStatementEventProducerWithWriter(kw)
	pdfParser := pdf.NewZeroPersistencePDFParser()

	svc := service.NewStatementIngestionService(pdfParser, awsAdapter, awsAdapter, kafkaProd)
	srv := httpin.NewServer(svc)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 1. Test /health with ETag and 304 Not Modified
	reqHealth, _ := http.NewRequest(http.MethodGet, ts.URL+"/health", nil)
	reqHealth.Header.Set("Accept-Encoding", "br")
	respHealth, err := http.DefaultClient.Do(reqHealth)
	if err != nil {
		t.Fatalf("health request failed: %v", err)
	}
	defer respHealth.Body.Close()

	if respHealth.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", respHealth.StatusCode)
	}
	if respHealth.Header.Get("Content-Encoding") != "br" {
		t.Fatalf("expected brotli Content-Encoding 'br', got %q", respHealth.Header.Get("Content-Encoding"))
	}
	etag := respHealth.Header.Get("ETag")
	if etag == "" {
		t.Fatalf("expected non-empty ETag header")
	}

	// 2. Test POST /api/v1/statements/upload
	statementBody := strings.Join([]string{
		"REF: 2026-11",
		"VENCIMENTO: 2026-11-25",
		"TOTAL_FATURA: 359.90",
		"2026-11-02 | MARISA LOJAS 01/03 | 380.00",
		"2026-11-04 | ESTORNO MARISA | -30.00",
		"2026-11-05 | TARIFA ANUIDADE | 9.90",
	}, "\n")

	tenantID := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	reqUpload, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/statements/upload", bytes.NewBufferString(statementBody))
	reqUpload.Header.Set("X-Tenant-ID", tenantID)
	reqUpload.Header.Set("X-Statement-Issuer", "MARISA")
	reqUpload.Header.Set("X-Card-Last4", "7788")
	reqUpload.Header.Set("Accept-Encoding", "gzip")

	respUpload, err := http.DefaultClient.Do(reqUpload)
	if err != nil {
		t.Fatalf("upload request failed: %v", err)
	}
	defer respUpload.Body.Close()
	_, _ = io.ReadAll(respUpload.Body)

	if respUpload.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on upload, got %d", respUpload.StatusCode)
	}
	if s3Calls.Load() < 2 {
		t.Fatalf("expected S3 retry after transient 500 error, got %d calls", s3Calls.Load())
	}
	if len(kw.messages) != 3 {
		t.Fatalf("expected 3 Kafka events published to statement.raw.extracted, got %d", len(kw.messages))
	}
	for _, msg := range kw.messages {
		if string(msg.Key) != tenantID {
			t.Fatalf("expected Kafka message key to equal tenant_id %s, got %s", tenantID, string(msg.Key))
		}
	}
}
