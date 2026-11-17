package aws

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func fastPolicy() ResiliencePolicy {
	return ResiliencePolicy{Timeout: time.Second, MaxRetries: 3, BaseBackoff: time.Millisecond, FailureThreshold: 3, OpenStateTimeout: time.Hour}
}

func TestDefaultResiliencePolicy(t *testing.T) {
	p := DefaultResiliencePolicy()
	if p.Timeout != DefaultRequestTimeout || p.MaxRetries != 3 || p.FailureThreshold != 3 || p.BaseBackoff != 50*time.Millisecond || p.OpenStateTimeout != 10*time.Second {
		t.Fatalf("unexpected defaults %+v", p)
	}
}

func TestNewResilientLocalStackAdapter(t *testing.T) {
	tests := []struct {
		name         string
		endpoint     string
		timeout      time.Duration
		wantEndpoint string
		wantTimeout  time.Duration
	}{
		{"blank uses default", "  ", 0, DefaultLocalStackEndpoint, DefaultRequestTimeout},
		{"trailing slash trimmed", "http://x:1///", 2 * time.Second, "http://x:1", 2 * time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := NewResilientLocalStackAdapter(tc.endpoint, ResiliencePolicy{Timeout: tc.timeout})
			if a.endpoint != tc.wantEndpoint || a.policy.Timeout != tc.wantTimeout || a.httpClient.Timeout != tc.wantTimeout || a.state != StateClosed || a.region != DefaultAWSRegion {
				t.Fatalf("unexpected adapter %+v", a)
			}
		})
	}
}

func TestEncryptEnvelope_RoundTripWithTenantAAD(t *testing.T) {
	a := NewResilientLocalStackAdapter("", fastPolicy())
	plain := []byte("statement-bytes")
	ct, err := a.EncryptEnvelope(context.Background(), "tenant-1", "alias/k", plain)
	if err != nil {
		t.Fatal(err)
	}
	dek := sha256.Sum256([]byte("alias/k:tenant-1"))
	block, _ := aes.NewCipher(dek[:])
	gcm, _ := cipher.NewGCM(block)
	ns := gcm.NonceSize()
	got, err := gcm.Open(nil, ct[:ns], ct[ns:], []byte("tenant-1"))
	if err != nil || string(got) != string(plain) {
		t.Fatalf("decrypt failed: %v %q", err, got)
	}
	if _, err := gcm.Open(nil, ct[:ns], ct[ns:], []byte("other")); err == nil {
		t.Fatal("AAD mismatch must fail")
	}
	ct2, _ := a.EncryptEnvelope(context.Background(), "tenant-1", "alias/k", plain)
	if string(ct) == string(ct2) {
		t.Fatal("nonce must be random")
	}
}

func TestGetSecretValue(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    string
		wantErr bool
	}{
		{"ok", func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			if r.Header.Get("X-Amz-Target") != "secretsmanager.GetSecretValue" || string(b) != `{"SecretId":"my"}` {
				w.WriteHeader(http.StatusTeapot)
				return
			}
			_, _ = w.Write([]byte(`{"SecretString":"pw123"}`))
		}, "pw123", false},
		{"client error", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(404) }, "", true},
		{"server error", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }, "", true},
		{"bad json", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("{")) }, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()
			a := NewResilientLocalStackAdapter(srv.URL, fastPolicy())
			got, err := a.GetSecretValue(context.Background(), "my")
			if (err != nil) != tc.wantErr || got != tc.want {
				t.Fatalf("got=%q err=%v", got, err)
			}
		})
	}
}

func TestPutEncryptedStatement(t *testing.T) {
	var gotPath, gotSSE, gotSum string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotSSE, gotSum = r.URL.Path, r.Header.Get("X-Amz-Server-Side-Encryption"), r.Header.Get("X-Amz-Checksum-Sha256")
		gotBody, _ = io.ReadAll(r.Body)
		if r.Method != http.MethodPut {
			w.WriteHeader(405)
		}
	}))
	defer srv.Close()
	a := NewResilientLocalStackAdapter(srv.URL, fastPolicy())
	payload := []byte("enc")
	if err := a.PutEncryptedStatement(context.Background(), "bkt", "/a/b.enc", payload); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	if gotPath != "/bkt/a/b.enc" || gotSSE != "aws:kms" || gotSum != base64.StdEncoding.EncodeToString(sum[:]) || string(gotBody) != "enc" {
		t.Fatalf("bad request path=%s sse=%s sum=%s body=%s", gotPath, gotSSE, gotSum, gotBody)
	}

	for _, code := range []int{400, 500} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }))
		b := NewResilientLocalStackAdapter(s.URL, fastPolicy())
		if err := b.PutEncryptedStatement(context.Background(), "b", "k", payload); err == nil {
			t.Fatalf("expected error for %d", code)
		}
		s.Close()
	}
}

func TestRequestBuildAndTransportErrors(t *testing.T) {
	bad := NewResilientLocalStackAdapter("://bad", fastPolicy())
	if _, err := bad.GetSecretValue(context.Background(), "x"); err == nil {
		t.Fatal("expected request build error (secret)")
	}
	if err := bad.PutEncryptedStatement(context.Background(), "b", "k", nil); err == nil {
		t.Fatal("expected request build error (put)")
	}
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	closed := NewResilientLocalStackAdapter(url, fastPolicy())
	if _, err := closed.GetSecretValue(context.Background(), "x"); err == nil {
		t.Fatal("expected transport error (secret)")
	}
	closed2 := NewResilientLocalStackAdapter(url, fastPolicy())
	if err := closed2.PutEncryptedStatement(context.Background(), "b", "k", nil); err == nil {
		t.Fatal("expected transport error (put)")
	}
}

func TestExecuteWithResilience_RetryAndSuccess(t *testing.T) {
	a := NewResilientLocalStackAdapter("", ResiliencePolicy{MaxRetries: 3, BaseBackoff: time.Millisecond, FailureThreshold: 10, OpenStateTimeout: time.Hour})
	var calls int32
	err := a.executeWithResilience(context.Background(), func(context.Context) error {
		if atomic.AddInt32(&calls, 1) < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil || calls != 3 || a.consecutiveFailures != 0 || a.state != StateClosed {
		t.Fatalf("err=%v calls=%d failures=%d", err, calls, a.consecutiveFailures)
	}
}

func TestExecuteWithResilience_ExhaustsAndMinOneAttempt(t *testing.T) {
	a := NewResilientLocalStackAdapter("", ResiliencePolicy{MaxRetries: 0, FailureThreshold: 10, OpenStateTimeout: time.Hour})
	boom := errors.New("boom")
	var calls int
	if err := a.executeWithResilience(context.Background(), func(context.Context) error { calls++; return boom }); !errors.Is(err, boom) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestCircuitBreaker_OpensRejectsAndHalfOpens(t *testing.T) {
	a := NewResilientLocalStackAdapter("", ResiliencePolicy{MaxRetries: 5, BaseBackoff: time.Millisecond, FailureThreshold: 2, OpenStateTimeout: 20 * time.Millisecond})
	boom := errors.New("boom")
	var calls int
	err := a.executeWithResilience(context.Background(), func(context.Context) error { calls++; return boom })
	if !errors.Is(err, ErrCircuitOpen) || calls != 2 || a.state != StateOpen {
		t.Fatalf("err=%v calls=%d state=%v", err, calls, a.state)
	}
	if err := a.executeWithResilience(context.Background(), func(context.Context) error { calls++; return nil }); !errors.Is(err, ErrCircuitOpen) || calls != 2 {
		t.Fatalf("open circuit must reject, err=%v", err)
	}
	time.Sleep(30 * time.Millisecond)
	if !a.allowRequest() || a.state != StateHalfOpen {
		t.Fatal("expected half-open after timeout")
	}
	a.recordFailure()
	if a.state != StateOpen {
		t.Fatal("failure in half-open must reopen")
	}
	time.Sleep(30 * time.Millisecond)
	if err := a.executeWithResilience(context.Background(), func(context.Context) error { return nil }); err != nil || a.state != StateClosed {
		t.Fatalf("probe success must close, err=%v state=%v", err, a.state)
	}
}

func TestExecuteWithResilience_ContextCancelledDuringBackoff(t *testing.T) {
	a := NewResilientLocalStackAdapter("", ResiliencePolicy{MaxRetries: 3, BaseBackoff: time.Hour, FailureThreshold: 100, OpenStateTimeout: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	err := a.executeWithResilience(ctx, func(context.Context) error { cancel(); return errors.New("x") })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
}

func TestComputeBackoffWithFullJitter(t *testing.T) {
	tests := []struct {
		name    string
		base    time.Duration
		attempt int
		max     time.Duration
	}{
		{"zero base falls back", 0, 1, 20 * time.Millisecond},
		{"exponential cap attempt 3", 10 * time.Millisecond, 3, 40 * time.Millisecond},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for i := 0; i < 50; i++ {
				d := computeBackoffWithFullJitter(tc.base, tc.attempt)
				if d < 0 || d >= tc.max {
					t.Fatalf("d=%v outside [0,%v)", d, tc.max)
				}
			}
		})
	}
	// overflow of nanoseconds yields non-positive max -> returns base
	if d := computeBackoffWithFullJitter(time.Hour, 64); d != time.Hour {
		t.Fatalf("overflow guard returned %v", d)
	}
}
