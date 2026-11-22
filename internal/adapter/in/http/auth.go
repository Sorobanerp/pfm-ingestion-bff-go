package http

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"strings"
)

type AuthClaims struct {
	Subject  string   `json:"sub"`
	TenantID string   `json:"tenant_id"`
	Role     string   `json:"role"`
	Roles    []string `json:"roles"`
	Email    string   `json:"email"`
}

func extractAuthClaims(r *http.Request) (*AuthClaims, error) {
	authHeader := r.Header.Get("Authorization")
	claims := &AuthClaims{}

	if authHeader != "" && strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
		token := strings.TrimSpace(authHeader[7:])
		parts := strings.Split(token, ".")
		if len(parts) == 3 {
			// standard JWT payload is parts[1]
			payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
			if err == nil {
				_ = json.Unmarshal(payloadBytes, claims)
			}
		} else {
			// Support test tokens or simple role name
			switch strings.ToLower(token) {
			case "dev-admin-token":
				claims.Role = "PLATFORM_ADMIN"
				claims.Subject = "usr-platform-admin"
			case "dev-token-tenant-a":
				claims.Role = "OWNER"
				claims.TenantID = "11111111-1111-1111-1111-111111111111"
				claims.Subject = "usr-dantas-owner"
			case "dev-token-tenant-b":
				claims.Role = "OWNER"
				claims.TenantID = "22222222-2222-2222-2222-222222222222"
				claims.Subject = "usr-visitante-owner"
			case "dev-viewer-tenant-a":
				claims.Role = "VIEWER"
				claims.TenantID = "11111111-1111-1111-1111-111111111111"
				claims.Subject = "usr-dantas-viewer"
			default:
				claims.Role = token
			}
		}
	}

	if r.Header.Get("X-User-Role") != "" {
		claims.Role = r.Header.Get("X-User-Role")
	}
	if claims.Role == "" && len(claims.Roles) > 0 {
		claims.Role = claims.Roles[0]
	}
	if r.Header.Get("X-User-Id") != "" {
		claims.Subject = r.Header.Get("X-User-Id")
	}
	if claims.TenantID == "" {
		claims.TenantID = r.Header.Get("X-Tenant-ID")
	}
	if claims.Subject == "" {
		claims.Subject = "usr-anonymous"
	}

	return claims, nil
}

func (s *Server) requirePlatformAdmin(w http.ResponseWriter, r *http.Request) (*AuthClaims, bool) {
	claims, _ := extractAuthClaims(r)
	if claims.Role == "" && r.Header.Get("Authorization") == "" && r.Header.Get("X-User-Role") == "" {
		if strings.EqualFold(strings.TrimSpace(os.Getenv("APP_ENV")), "local") && r.Header.Get("X-Tenant-ID") != "" {
			claims.Role = "OWNER"
		} else {
			writeJSONWithETagAndCompression(w, r, http.StatusUnauthorized, map[string]string{
				"error": "missing or invalid authorization header (Bearer token required)",
			})
			return nil, false
		}
	}

	if claims.Role != "PLATFORM_ADMIN" {
		writeJSONWithETagAndCompression(w, r, http.StatusForbidden, map[string]string{
			"error": "forbidden: requires PLATFORM_ADMIN role",
		})
		return nil, false
	}

	return claims, true
}

func (s *Server) requireProfileUser(w http.ResponseWriter, r *http.Request) (*AuthClaims, bool) {
	claims, _ := extractAuthClaims(r)

	// Zero-Trust LGPD: Platform admin cannot read or write user personal/family data
	if claims.Role == "PLATFORM_ADMIN" {
		writeJSONWithETagAndCompression(w, r, http.StatusForbidden, map[string]string{
			"error": "forbidden: platform administrators cannot access private user profile or family data (Zero-Trust LGPD)",
		})
		return nil, false
	}

	if claims.Role == "" && r.Header.Get("Authorization") == "" && r.Header.Get("X-User-Role") == "" {
		if strings.EqualFold(strings.TrimSpace(os.Getenv("APP_ENV")), "local") && r.Header.Get("X-Tenant-ID") != "" {
			claims.Role = "OWNER"
		} else {
			writeJSONWithETagAndCompression(w, r, http.StatusUnauthorized, map[string]string{
				"error": "missing or invalid authorization header (Bearer token required)",
			})
			return nil, false
		}
	}

	tenantID := claims.TenantID
	if tenantID == "" {
		tenantID = r.Header.Get("X-Tenant-ID")
	}
	if tenantID == "" || !reUUID.MatchString(tenantID) {
		writeJSONWithETagAndCompression(w, r, http.StatusUnauthorized, map[string]string{
			"error": "missing or invalid X-Tenant-ID UUID header (Zero-Trust ADR-006)",
		})
		return nil, false
	}
	claims.TenantID = tenantID

	if s.rateLimiter != nil && !s.rateLimiter.Allow(tenantID) {
		w.Header().Set("Retry-After", "60")
		writeJSONWithETagAndCompression(w, r, http.StatusTooManyRequests, map[string]string{
			"error": "tenant rate limit exceeded (ADR-008)",
		})
		return nil, false
	}

	return claims, true
}

func (s *Server) requireFamilyOwnerOrMember(w http.ResponseWriter, r *http.Request) (*AuthClaims, bool) {
	claims, ok := s.requireProfileUser(w, r)
	if !ok {
		return nil, false
	}

	if claims.Role == "VIEWER" {
		writeJSONWithETagAndCompression(w, r, http.StatusForbidden, map[string]string{
			"error": "forbidden: VIEWER role cannot modify profile or family settings",
		})
		return nil, false
	}

	return claims, true
}
