package http

import (
	"sync"
	"time"
)

// TenantRateLimiter implements a thread-safe per-tenant Sliding Window / Token Bucket Rate Limiter (ADR-008).
type TenantRateLimiter struct {
	mu          sync.Mutex
	maxRequests int
	window      time.Duration
	counters    map[string]*tenantWindow
}

type tenantWindow struct {
	count     int
	resetTime time.Time
}

func NewTenantRateLimiter(maxRequests int, window time.Duration) *TenantRateLimiter {
	if maxRequests <= 0 {
		maxRequests = 120
	}
	if window <= 0 {
		window = time.Minute
	}
	return &TenantRateLimiter{
		maxRequests: maxRequests,
		window:      window,
		counters:    make(map[string]*tenantWindow),
	}
}

func (rl *TenantRateLimiter) Allow(tenantID string) bool {
	if rl == nil || tenantID == "" {
		return true
	}
	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()

	entry, exists := rl.counters[tenantID]
	if !exists || now.After(entry.resetTime) {
		rl.counters[tenantID] = &tenantWindow{
			count:     1,
			resetTime: now.Add(rl.window),
		}
		return true
	}

	if entry.count >= rl.maxRequests {
		return false
	}
	entry.count++
	return true
}
