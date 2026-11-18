package observability

import (
	"net/http"
	"strings"
	"testing"
)

func TestObservabilityAndLgpdSanitizer(t *testing.T) {
	cfg := InitOtlpConfig(func(string) string { return "" }, "pfm-ingestion-bff-go")
	if !cfg.Noop || cfg.ServiceName != "pfm-ingestion-bff-go" || !cfg.LogsInjection {
		t.Fatalf("unexpected default config: %+v", cfg)
	}

	cfgLive := InitOtlpConfig(func(k string) string {
		switch k {
		case "OTEL_SERVICE_NAME":
			return "custom-svc"
		case "OTEL_EXPORTER_OTLP_ENDPOINT":
			return DefaultOtlpEndpoint
		case "DD_LOGS_INJECTION":
			return "true"
		default:
			return ""
		}
	}, "pfm-ingestion-bff-go")
	if cfgLive.Noop || cfgLive.ServiceName != "custom-svc" || cfgLive.Endpoint != DefaultOtlpEndpoint {
		t.Fatalf("unexpected live config: %+v", cfgLive)
	}

	validTP := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	if got := EnsureTraceparent(validTP); got != validTP {
		t.Fatalf("expected %s, got %s", validTP, got)
	}
	genTP := EnsureTraceparent("bad")
	if len(genTP) != 55 || !strings.HasPrefix(genTP, "00-") {
		t.Fatalf("invalid generated traceparent: %s", genTP)
	}

	req, _ := http.NewRequest(http.MethodGet, "http://localhost/test", nil)
	req.Header.Set("traceparent", validTP)
	outHdr := http.Header{}
	if got := PropagateTraceHeaders(req, outHdr); got != validTP || outHdr.Get("traceparent") != validTP {
		t.Fatalf("traceparent not propagated: %v", outHdr)
	}
	if got := PropagateTraceHeaders(nil, nil); len(got) != 55 {
		t.Fatalf("expected generated traceparent on nil request: %s", got)
	}

	if MaskSensitivePII("") != "" {
		t.Fatal("expected empty string")
	}
	masked := MaskSensitivePII(`cpf=123.456.789-01 card=4532 1122 3344 5566 "pdfPassword":"secret"`)
	if strings.Contains(masked, "123.456.789-01") || strings.Contains(masked, "4532 1122") || strings.Contains(masked, "secret") {
		t.Fatalf("PII leaked: %s", masked)
	}

	logLine := FormatStructuredJSONLog("pfm-ingestion-bff-go", "", "CPF 11122233344 password=abc", validTP)
	if !strings.Contains(logLine, "[CPF_MASKED]") || !strings.Contains(logLine, "[REDACTED]") || !strings.Contains(logLine, "4bf92f3577b34da6a3ce929d0e0e4736") {
		t.Fatalf("unexpected structured log: %s", logLine)
	}
}
