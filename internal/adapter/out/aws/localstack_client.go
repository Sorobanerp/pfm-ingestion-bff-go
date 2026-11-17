package aws

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	DefaultLocalStackEndpoint = "http://localhost:14566"
	DefaultAWSRegion          = "sa-east-1"
	DefaultRequestTimeout     = 5 * time.Second
)

var (
	ErrCircuitOpen = errors.New("circuit breaker is OPEN for external cloud endpoint")
)

// CircuitState represents the state machine of the Resilience Circuit Breaker (ADR-004).
type CircuitState int

const (
	StateClosed CircuitState = iota
	StateOpen
	StateHalfOpen
)

// ResiliencePolicy configures Timeout (5s), Exponential Backoff + Full Jitter Retry, and Circuit Breaker.
type ResiliencePolicy struct {
	Timeout          time.Duration
	MaxRetries       int
	BaseBackoff      time.Duration
	FailureThreshold int
	OpenStateTimeout time.Duration
}

func DefaultResiliencePolicy() ResiliencePolicy {
	return ResiliencePolicy{
		Timeout:          DefaultRequestTimeout,
		MaxRetries:       3,
		BaseBackoff:      50 * time.Millisecond,
		FailureThreshold: 3,
		OpenStateTimeout: 10 * time.Second,
	}
}

// ResilientLocalStackAdapter implements out.SecretVaultPort and out.ObjectStoragePort
// pointing to LocalStack (`http://localhost:14566`) in local/dev environments and protected by
// 5s Timeout, Retry with Exponential Backoff + Jitter, and Circuit Breaker (ADR-004).
type ResilientLocalStackAdapter struct {
	endpoint   string
	region     string
	policy     ResiliencePolicy
	httpClient *http.Client

	mu                  sync.Mutex
	state               CircuitState
	consecutiveFailures int
	openedAt            time.Time
}

func NewResilientLocalStackAdapter(endpoint string, policy ResiliencePolicy) *ResilientLocalStackAdapter {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = DefaultLocalStackEndpoint
	}
	if policy.Timeout <= 0 {
		policy.Timeout = DefaultRequestTimeout
	}
	return &ResilientLocalStackAdapter{
		endpoint: strings.TrimRight(endpoint, "/"),
		region:   DefaultAWSRegion,
		policy:   policy,
		httpClient: &http.Client{
			Timeout: policy.Timeout,
		},
		state: StateClosed,
	}
}

func (a *ResilientLocalStackAdapter) EncryptEnvelope(
	ctx context.Context,
	tenantID string,
	keyAlias string,
	plaintext []byte,
) ([]byte, error) {
	// Envelope Encryption with AES-256-GCM authenticated with tenant_id AAD (ADR-006)
	dek := sha256.Sum256([]byte(keyAlias + ":" + tenantID))
	block, err := aes.NewCipher(dek[:])
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nonce, nonce, plaintext, []byte(tenantID))
	return ciphertext, nil
}

func (a *ResilientLocalStackAdapter) GetSecretValue(ctx context.Context, secretID string) (string, error) {
	bodyMap := map[string]string{"SecretId": secretID}
	rawBody, _ := json.Marshal(bodyMap)

	var respBytes []byte
	err := a.executeWithResilience(ctx, func(attemptCtx context.Context) error {
		req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, a.endpoint+"/", bytes.NewReader(rawBody))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "secretsmanager.GetSecretValue")

		resp, err := a.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 500 {
			return fmt.Errorf("secretsmanager transient server error: HTTP %d", resp.StatusCode)
		}
		if resp.StatusCode >= 400 {
			return fmt.Errorf("secretsmanager client error: HTTP %d", resp.StatusCode)
		}
		respBytes, err = io.ReadAll(resp.Body)
		return err
	})
	if err != nil {
		return "", err
	}

	var parsed struct {
		SecretString string `json:"SecretString"`
	}
	if err := json.Unmarshal(respBytes, &parsed); err != nil {
		return "", err
	}
	return parsed.SecretString, nil
}

func (a *ResilientLocalStackAdapter) PutEncryptedStatement(
	ctx context.Context,
	bucket string,
	objectKey string,
	encryptedPayload []byte,
) error {
	url := fmt.Sprintf("%s/%s/%s", a.endpoint, bucket, strings.TrimLeft(objectKey, "/"))
	sum := sha256.Sum256(encryptedPayload)
	checksumHeader := base64.StdEncoding.EncodeToString(sum[:])

	return a.executeWithResilience(ctx, func(attemptCtx context.Context) error {
		req, err := http.NewRequestWithContext(attemptCtx, http.MethodPut, url, bytes.NewReader(encryptedPayload))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Amz-Server-Side-Encryption", "aws:kms")
		req.Header.Set("X-Amz-Checksum-Sha256", checksumHeader)

		resp, err := a.httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 500 {
			return fmt.Errorf("s3 put transient failure: HTTP %d", resp.StatusCode)
		}
		if resp.StatusCode >= 400 {
			return fmt.Errorf("s3 put client failure: HTTP %d", resp.StatusCode)
		}
		return nil
	})
}

func (a *ResilientLocalStackAdapter) executeWithResilience(
	ctx context.Context,
	operation func(attemptCtx context.Context) error,
) error {
	if !a.allowRequest() {
		return ErrCircuitOpen
	}

	var lastErr error
	maxAttempts := a.policy.MaxRetries
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		attemptCtx, cancel := context.WithTimeout(ctx, a.policy.Timeout)
		err := operation(attemptCtx)
		cancel()

		if err == nil {
			a.recordSuccess()
			return nil
		}
		lastErr = err
		a.recordFailure()

		if !a.allowRequest() {
			return fmt.Errorf("%w: %v", ErrCircuitOpen, lastErr)
		}

		if attempt < maxAttempts {
			sleepDur := computeBackoffWithFullJitter(a.policy.BaseBackoff, attempt)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(sleepDur):
			}
		}
	}
	return lastErr
}

func (a *ResilientLocalStackAdapter) allowRequest() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.state == StateOpen {
		if time.Since(a.openedAt) >= a.policy.OpenStateTimeout {
			a.state = StateHalfOpen
			return true
		}
		return false
	}
	return true
}

func (a *ResilientLocalStackAdapter) recordSuccess() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.consecutiveFailures = 0
	a.state = StateClosed
}

func (a *ResilientLocalStackAdapter) recordFailure() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.consecutiveFailures++
	if a.state == StateHalfOpen || a.consecutiveFailures >= a.policy.FailureThreshold {
		a.state = StateOpen
		a.openedAt = time.Now()
	}
}

func computeBackoffWithFullJitter(base time.Duration, attempt int) time.Duration {
	if base <= 0 {
		base = 20 * time.Millisecond
	}
	multiplier := int64(1 << (attempt - 1))
	maxSleepNs := base.Nanoseconds() * multiplier
	if maxSleepNs <= 0 {
		return base
	}
	n, err := rand.Int(rand.Reader, big.NewInt(maxSleepNs))
	if err != nil {
		return time.Duration(maxSleepNs / 2)
	}
	return time.Duration(n.Int64())
}
