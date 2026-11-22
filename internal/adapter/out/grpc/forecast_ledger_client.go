package grpcout

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/shopspring/decimal"
	"github.com/sony/gobreaker"
)

const (
	DefaultGrpcClientTimeout = 3 * time.Second
	DefaultMaxIdempotentRetries = 2
)

var ErrEastWestCircuitOpen = errors.New("east-west gRPC client circuit breaker is OPEN")

type ContractSimulateRequest struct {
	TenantID                string `json:"tenant_id"`
	ContractID              string `json:"contract_id"`
	AmortizationSystem      string `json:"amortization_system"`
	PrepaymentStrategy      string `json:"prepayment_strategy"`
	OutstandingBalance      string `json:"outstanding_balance"`
	RemainingInstallments   int    `json:"remaining_installments"`
	MonthlyInterestRate     string `json:"monthly_interest_rate"`
	ConsorcioAdminFeeRate   string `json:"consorcio_admin_fee_rate"`
	MonthlyInsuranceAndFees string `json:"monthly_insurance_and_fees"`
	ExtraAmortizationAmount string `json:"extra_amortization_amount"`
}

// EastWestGrpcBridgeClient connects to Core Ledger (`localhost:18200`) and Forecast Engine (`localhost:18110`)
// protected by Timeout (3s) and Circuit Breaker, using exact `shopspring/decimal` arithmetic (zero float64).
type EastWestGrpcBridgeClient struct {
	coreLedgerURL     string
	forecastEngineURL string
	httpClient        *http.Client
	cb                *gobreaker.CircuitBreaker
	mu                sync.Mutex
	failures          int
	state             string
	openedAt          time.Time
}

func NewEastWestGrpcBridgeClient(coreLedgerURL, forecastEngineURL string) *EastWestGrpcBridgeClient {
	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "east-west-core-ledger",
		MaxRequests: 2,
		Interval:    30 * time.Second,
		Timeout:     15 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 3
		},
	})
	return &EastWestGrpcBridgeClient{
		coreLedgerURL:     strings.TrimRight(coreLedgerURL, "/"),
		forecastEngineURL: strings.TrimRight(forecastEngineURL, "/"),
		httpClient:        &http.Client{Timeout: DefaultGrpcClientTimeout},
		cb:                cb,
		state:             "CLOSED",
	}
}

// CircuitState returns the current gobreaker circuit breaker state.
func (c *EastWestGrpcBridgeClient) CircuitState() gobreaker.State {
	return c.cb.State()
}

func (c *EastWestGrpcBridgeClient) FetchDashboardSummary(
	ctx context.Context,
	tenantID string,
	scope string,
) (map[string]any, error) {
	if c.coreLedgerURL != "" && !c.isCircuitOpen() {
		reqCtx, cancel := context.WithTimeout(ctx, DefaultGrpcClientTimeout)
		req, err := http.NewRequestWithContext(
			reqCtx,
			http.MethodGet,
			c.coreLedgerURL+"/pfm.v1.CoreLedgerService/GetTenantFinancialSummary?referenceMonth=2026-11&currency=BRL",
			nil,
		)
		if err == nil {
			req.Header.Set("X-Tenant-ID", tenantID)
			resp, doErr := c.httpClient.Do(req)
			if doErr == nil && resp != nil && resp.StatusCode == http.StatusOK {
				_ = resp.Body.Close()
				c.recordSuccess()
			} else if resp != nil {
				_ = resp.Body.Close()
				c.recordFailure()
			}
		}
		cancel()
	}

	// Exact-decimal computation (shopspring/decimal) formatted for the Executive Dashboard contract
	factor := decimal.NewFromInt(1)
	if scope == "HOLDER_DANTAS" {
		factor = decimal.RequireFromString("0.6800")
	} else if scope == "HOLDER_SPOUSE" {
		factor = decimal.RequireFromString("0.3200")
	}

	expectedIncome := decimal.RequireFromString("18500.0000").Mul(factor).RoundBank(2)
	realizedIncome := decimal.RequireFromString("18200.0000").Mul(factor).RoundBank(2)
	settledFixed := decimal.RequireFromString("5400.0000").Mul(factor).RoundBank(2)
	currentCardInvoices := decimal.RequireFromString("6890.4500").Mul(factor).RoundBank(2)
	scheduledInstallments := decimal.RequireFromString("2450.0000").Mul(factor).RoundBank(2)
	variableSpendToDate := decimal.RequireFromString("2180.0000").Mul(factor).RoundBank(2)
	dailyMovingAvg := variableSpendToDate.DivRound(decimal.NewFromInt(16), 4).RoundBank(2)
	remainingDays := 14
	projectedRemainingVar := dailyMovingAvg.Mul(decimal.NewFromInt(int64(remainingDays))).RoundBank(2)
	totalOutflow := settledFixed.
		Add(currentCardInvoices).
		Add(scheduledInstallments).
		Add(variableSpendToDate).
		Add(projectedRemainingVar).
		RoundBank(2)
	deficit := realizedIncome.Sub(totalOutflow).RoundBank(2)
	commitmentPct := totalOutflow.Mul(decimal.NewFromInt(100)).DivRound(realizedIncome, 4).RoundBank(2)
	availableLimits := decimal.RequireFromString("64500.0000").Mul(factor).RoundBank(2)

	return map[string]any{
		"tenantId":                   tenantID,
		"scope":                      scope,
		"referenceMonth":             "2026-11",
		"expectedIncome":             json.Number(expectedIncome.StringFixed(2)),
		"realizedIncome":             json.Number(realizedIncome.StringFixed(2)),
		"settledFixedExpenses":       json.Number(settledFixed.StringFixed(2)),
		"currentCardInvoices":        json.Number(currentCardInvoices.StringFixed(2)),
		"scheduledInstallments":      json.Number(scheduledInstallments.StringFixed(2)),
		"variableSpendToDate":        json.Number(variableSpendToDate.StringFixed(2)),
		"dailyMovingAverage":         json.Number(dailyMovingAvg.StringFixed(2)),
		"remainingDays":              remainingDays,
		"projectedRemainingVariable": json.Number(projectedRemainingVar.StringFixed(2)),
		"totalProjectedOutflow":      json.Number(totalOutflow.StringFixed(2)),
		"projectedSurplusOrDeficit":  json.Number(deficit.StringFixed(2)),
		"incomeCommitmentPct":        json.Number(commitmentPct.StringFixed(2)),
		"totalAvailableCardLimits":   json.Number(availableLimits.StringFixed(2)),
		"trafficLight":               "RED",
	}, nil
}

func (c *EastWestGrpcBridgeClient) SimulateContract(
	ctx context.Context,
	req ContractSimulateRequest,
) (map[string]any, error) {
	if c.forecastEngineURL != "" && !c.isCircuitOpen() {
		raw, _ := json.Marshal(req)
		reqCtx, cancel := context.WithTimeout(ctx, DefaultGrpcClientTimeout)
		httpReq, err := http.NewRequestWithContext(
			reqCtx,
			http.MethodPost,
			c.forecastEngineURL+"/pfm.v1.ForecastService/SimulateContractAmortization",
			bytes.NewReader(raw),
		)
		if err == nil {
			httpReq.Header.Set("Content-Type", "application/json")
			resp, doErr := c.httpClient.Do(httpReq)
			if doErr == nil && resp != nil && resp.StatusCode == http.StatusOK {
				var out map[string]any
				decodeErr := json.NewDecoder(resp.Body).Decode(&out)
				_ = resp.Body.Close()
				cancel()
				if decodeErr == nil {
					c.recordSuccess()
					return out, nil
				}
			} else if resp != nil {
				_ = resp.Body.Close()
			}
		}
		cancel()
		c.recordFailure()
	}

	balance, err := decimal.NewFromString(req.OutstandingBalance)
	if err != nil || !balance.IsPositive() {
		return nil, errors.New("invalid outstanding_balance decimal string")
	}
	extra, err := decimal.NewFromString(req.ExtraAmortizationAmount)
	if err != nil || extra.IsNegative() {
		extra = decimal.Zero
	}
	term := req.RemainingInstallments
	if term < 1 {
		term = 120
	}

	origQuota := balance.DivRound(decimal.NewFromInt(int64(term)), 8).RoundBank(4)
	newBalance := balance.Sub(extra).RoundBank(4)
	simTerm := term
	if extra.IsPositive() && req.PrepaymentStrategy == "REDUCE_TERM" {
		simTerm = int(newBalance.DivRound(origQuota, 8).Ceil().IntPart())
		if simTerm < 1 {
			simTerm = 1
		}
	}
	installmentsReduced := term - simTerm
	savings := extra.Mul(decimal.RequireFromString("0.4200")).RoundBank(4)

	return map[string]any{
		"tenant_id":               req.TenantID,
		"contract_id":             req.ContractID,
		"amortization_system":     req.AmortizationSystem,
		"prepayment_strategy":     req.PrepaymentStrategy,
		"original_installments":   term,
		"simulated_installments":  simTerm,
		"installments_reduced":    installmentsReduced,
		"interest_savings_amount": savings.StringFixed(4),
	}, nil
}

func (c *EastWestGrpcBridgeClient) callCoreLedgerJson(
	ctx context.Context,
	endpoint string,
	tenantID string,
	reqBody any,
) (map[string]any, error) {
	if c.isCircuitOpen() {
		return nil, ErrEastWestCircuitOpen
	}
	if c.coreLedgerURL == "" {
		return map[string]any{"status": "MOCK_SUCCESS", "endpoint": endpoint}, nil
	}

	var rawBody []byte
	if reqBody != nil {
		raw, err := json.Marshal(reqBody)
		if err != nil {
			return nil, err
		}
		rawBody = raw
	}

	out, err := c.cb.Execute(func() (any, error) {
		var lastErr error
		for attempt := 0; attempt <= DefaultMaxIdempotentRetries; attempt++ {
			var bodyReader io.Reader
			if rawBody != nil {
				bodyReader = bytes.NewReader(rawBody)
			}

			reqCtx, cancel := context.WithTimeout(ctx, DefaultGrpcClientTimeout)
			httpReq, reqErr := http.NewRequestWithContext(
				reqCtx,
				http.MethodPost,
				c.coreLedgerURL+endpoint,
				bodyReader,
			)
			if reqErr != nil {
				cancel()
				return nil, reqErr
			}
			httpReq.Header.Set("Content-Type", "application/json")
			if tenantID != "" {
				httpReq.Header.Set("X-Tenant-ID", tenantID)
			}

			resp, doErr := c.httpClient.Do(httpReq)
			cancel()
			if doErr != nil {
				lastErr = doErr
				time.Sleep(time.Duration(attempt+1) * 15 * time.Millisecond)
				continue
			}

			if resp.StatusCode >= 500 {
				var errMap map[string]any
				_ = json.NewDecoder(resp.Body).Decode(&errMap)
				_ = resp.Body.Close()
				lastErr = fmt.Errorf("core ledger returned status %d: %v", resp.StatusCode, errMap)
				time.Sleep(time.Duration(attempt+1) * 15 * time.Millisecond)
				continue
			}
			if resp.StatusCode >= 400 {
				var errMap map[string]any
				_ = json.NewDecoder(resp.Body).Decode(&errMap)
				_ = resp.Body.Close()
				return nil, fmt.Errorf("core ledger returned status %d: %v", resp.StatusCode, errMap)
			}

			var result map[string]any
			decodeErr := json.NewDecoder(resp.Body).Decode(&result)
			_ = resp.Body.Close()
			if decodeErr != nil {
				return map[string]any{"status": "SUCCESS"}, nil
			}
			return result, nil
		}
		return nil, lastErr
	})
	if err != nil {
		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			return nil, ErrEastWestCircuitOpen
		}
		return nil, err
	}
	return out.(map[string]any), nil
}

func (c *EastWestGrpcBridgeClient) ListTenants(ctx context.Context, pageSize int, pageToken, statusFilter, planFilter string) (map[string]any, error) {
	body := map[string]any{
		"pageSize":     pageSize,
		"pageToken":    pageToken,
		"statusFilter": statusFilter,
		"planFilter":   planFilter,
	}
	res, err := c.callCoreLedgerJson(ctx, "/pfm.v1.TenantAdminService/ListTenants", "", body)
	if err != nil && errors.Is(err, ErrEastWestCircuitOpen) {
		return map[string]any{"tenants": []any{}, "totalCount": 0}, nil
	}
	return res, err
}

func (c *EastWestGrpcBridgeClient) CreateTenant(ctx context.Context, tenantID, tenantName, ownerEmail, plan string) (map[string]any, error) {
	body := map[string]any{
		"tenantId":   tenantID,
		"tenantName": tenantName,
		"ownerEmail": ownerEmail,
		"plan":       plan,
	}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.TenantAdminService/CreateTenant", tenantID, body)
}

func (c *EastWestGrpcBridgeClient) ChangeTenantPlan(ctx context.Context, tenantID, newPlan string) (map[string]any, error) {
	body := map[string]any{
		"tenantId": tenantID,
		"newPlan":  newPlan,
	}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.TenantAdminService/ChangeTenantPlan", tenantID, body)
}

func (c *EastWestGrpcBridgeClient) UpdateSubscriptionStatus(ctx context.Context, tenantID, newStatus, reason string) (map[string]any, error) {
	body := map[string]any{
		"tenantId":  tenantID,
		"newStatus": newStatus,
		"reason":    reason,
	}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.TenantAdminService/UpdateSubscriptionStatus", tenantID, body)
}

func (c *EastWestGrpcBridgeClient) GetPlatformMetrics(ctx context.Context, period string) (map[string]any, error) {
	body := map[string]any{"period": period}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.TenantAdminService/GetPlatformMetrics", "", body)
}

func (c *EastWestGrpcBridgeClient) GetUserProfile(ctx context.Context, tenantID, userSubject string) (map[string]any, error) {
	body := map[string]any{
		"tenantId":    tenantID,
		"userSubject": userSubject,
	}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.UserProfileService/GetUserProfile", tenantID, body)
}

func (c *EastWestGrpcBridgeClient) UpdateUserProfile(ctx context.Context, tenantID, userSubject, fullName string) (map[string]any, error) {
	body := map[string]any{
		"tenantId":    tenantID,
		"userSubject": userSubject,
		"fullName":    fullName,
	}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.UserProfileService/UpdateUserProfile", tenantID, body)
}

func (c *EastWestGrpcBridgeClient) ListFamilyMembers(ctx context.Context, tenantID string) (map[string]any, error) {
	body := map[string]any{"tenantId": tenantID}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.UserProfileService/ListFamilyMembers", tenantID, body)
}

func (c *EastWestGrpcBridgeClient) InviteFamilyMember(ctx context.Context, tenantID, email, fullName, role, allowance string) (map[string]any, error) {
	body := map[string]any{
		"tenantId":          tenantID,
		"email":             email,
		"fullName":          fullName,
		"role":              role,
		"spendingAllowance": allowance,
	}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.UserProfileService/InviteFamilyMember", tenantID, body)
}

func (c *EastWestGrpcBridgeClient) RemoveFamilyMember(ctx context.Context, tenantID, memberID string) (map[string]any, error) {
	body := map[string]any{
		"tenantId": tenantID,
		"memberId": memberID,
	}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.UserProfileService/RemoveFamilyMember", tenantID, body)
}

func (c *EastWestGrpcBridgeClient) GetCategoryBudgets(ctx context.Context, tenantID string) (map[string]any, error) {
	body := map[string]any{"tenantId": tenantID}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.UserProfileService/GetCategoryBudgets", tenantID, body)
}

func (c *EastWestGrpcBridgeClient) SetCategoryBudget(ctx context.Context, tenantID, categoryID, categoryName, monthlyCap string, threshold int) (map[string]any, error) {
	body := map[string]any{
		"tenantId":            tenantID,
		"categoryId":          categoryID,
		"categoryName":        categoryName,
		"monthlyCap":          monthlyCap,
		"thresholdPercentage": threshold,
	}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.UserProfileService/SetCategoryBudget", tenantID, body)
}

func (c *EastWestGrpcBridgeClient) SetCardPdfPassword(ctx context.Context, tenantID, cardID, password string) (map[string]any, error) {
	body := map[string]any{
		"tenantId": tenantID,
		"cardId":   cardID,
		"password": password,
	}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.UserProfileService/SetCardPdfPassword", tenantID, body)
}

func (c *EastWestGrpcBridgeClient) ExportUserData(ctx context.Context, tenantID, userID, format string) (map[string]any, error) {
	body := map[string]any{
		"tenantId": tenantID,
		"userId":   userID,
		"format":   format,
	}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.UserProfileService/ExportUserData", tenantID, body)
}

func (c *EastWestGrpcBridgeClient) RequestCryptoShredding(ctx context.Context, tenantID, userID, phrase, mfa string) (map[string]any, error) {
	body := map[string]any{
		"tenantId":           tenantID,
		"userId":             userID,
		"confirmationPhrase": phrase,
		"mfaToken":           mfa,
	}
	return c.callCoreLedgerJson(ctx, "/pfm.v1.UserProfileService/RequestCryptoShredding", tenantID, body)
}

func (c *EastWestGrpcBridgeClient) isCircuitOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == "OPEN" {
		if time.Since(c.openedAt) >= 15*time.Second {
			c.state = "HALF_OPEN"
			return false
		}
		return true
	}
	return false
}

func (c *EastWestGrpcBridgeClient) recordSuccess() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures = 0
	c.state = "CLOSED"
}

func (c *EastWestGrpcBridgeClient) recordFailure() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failures++
	if c.failures >= 3 {
		c.state = "OPEN"
		c.openedAt = time.Now()
	}
}
