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

	var s3Calls atomic.Int32
	mockLocalStack := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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

	// 1. Test /health with ETag and Brotli
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

func TestHTTPServer_RateLimiterDashboardETag304AndContractSimulate(t *testing.T) {
	t.Parallel()

	pdfParser := pdf.NewZeroPersistencePDFParser()
	svc := service.NewStatementIngestionService(pdfParser, nil, nil, nil)
	// Allow at most 2 requests per minute per tenant
	rl := httpin.NewTenantRateLimiter(2, time.Minute)
	srv := httpin.NewServer(svc).WithRateLimiter(rl)
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	tenantID := "dddddddd-dddd-4ddd-8ddd-dddddddddddd"

	// 1. First call to GET /api/v1/dashboard -> 200 OK + ETag
	reqDash, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/dashboard?scope=FAMILY", nil)
	reqDash.Header.Set("X-Tenant-ID", tenantID)
	respDash, err := http.DefaultClient.Do(reqDash)
	if err != nil {
		t.Fatalf("dashboard request failed: %v", err)
	}
	defer respDash.Body.Close()
	if respDash.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on dashboard, got %d", respDash.StatusCode)
	}
	etag := respDash.Header.Get("ETag")
	if etag == "" {
		t.Fatalf("expected ETag on dashboard response")
	}

	// 2. Second call with If-None-Match -> 304 Not Modified
	reqDash304, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/dashboard?scope=FAMILY", nil)
	reqDash304.Header.Set("X-Tenant-ID", tenantID)
	reqDash304.Header.Set("If-None-Match", etag)
	respDash304, err := http.DefaultClient.Do(reqDash304)
	if err != nil {
		t.Fatalf("dashboard 304 request failed: %v", err)
	}
	defer respDash304.Body.Close()
	if respDash304.StatusCode != http.StatusNotModified {
		t.Fatalf("expected 304 Not Modified, got %d", respDash304.StatusCode)
	}

	// 3. Third call for the same tenant exceeds maxRequests=2 -> 429 Too Many Requests + Retry-After
	reqRateLimited, _ := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/dashboard?scope=FAMILY", nil)
	reqRateLimited.Header.Set("X-Tenant-ID", tenantID)
	respRateLimited, err := http.DefaultClient.Do(reqRateLimited)
	if err != nil {
		t.Fatalf("rate-limited request failed: %v", err)
	}
	defer respRateLimited.Body.Close()
	if respRateLimited.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 Too Many Requests, got %d", respRateLimited.StatusCode)
	}
	if respRateLimited.Header.Get("Retry-After") != "60" {
		t.Fatalf("expected Retry-After: 60 header")
	}

	// 4. Separate tenant is isolated and can call POST /api/v1/contracts/simulate
	otherTenant := "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	simPayload := `{"contract_id":"sac-01","amortization_system":"SAC","prepayment_strategy":"REDUCE_TERM","outstanding_balance":"300000.0000","remaining_installments":120,"extra_amortization_amount":"60000.0000"}`
	reqSim, _ := http.NewRequest(http.MethodPost, ts.URL+"/api/v1/contracts/simulate", bytes.NewBufferString(simPayload))
	reqSim.Header.Set("X-Tenant-ID", otherTenant)
	respSim, err := http.DefaultClient.Do(reqSim)
	if err != nil {
		t.Fatalf("contract simulate request failed: %v", err)
	}
	defer respSim.Body.Close()
	if respSim.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK on contract simulate, got %d", respSim.StatusCode)
	}
}
