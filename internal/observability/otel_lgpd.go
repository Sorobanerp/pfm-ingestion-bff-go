package observability

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
)

const DefaultOtlpEndpoint = "http://datadog-agent.monitoring.svc.cluster.local:4318"

var (
	reCPF        = regexp.MustCompile(`\b\d{3}\.?\d{3}\.?\d{3}-\d{2}\b|\b\d{11}\b`)
	rePAN        = regexp.MustCompile(`\b(?:\d[ -]*?){13,16}\b`)
	rePasswordKV = regexp.MustCompile(`(?i)("?(?:pdf_?password|password)"?\s*[:=]\s*"?)([^"\s,}]+)("?)`)
	reW3CTrace   = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)
)

type OtlpRuntimeConfig struct {
	ServiceName   string
	Endpoint      string
	Noop          bool
	LogsInjection bool
}

func InitOtlpConfig(getenv func(string) string, defaultService string) OtlpRuntimeConfig {
	svc := strings.TrimSpace(getenv("OTEL_SERVICE_NAME"))
	if svc == "" {
		svc = defaultService
	}
	ep := strings.TrimSpace(getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	noop := false
	if ep == "" || strings.EqualFold(ep, "noop") {
		noop = true
		ep = DefaultOtlpEndpoint
	}
	logsInj := !strings.EqualFold(strings.TrimSpace(getenv("DD_LOGS_INJECTION")), "false")
	return OtlpRuntimeConfig{
		ServiceName:   svc,
		Endpoint:      ep,
		Noop:          noop,
		LogsInjection: logsInj,
	}
}

func EnsureTraceparent(incoming string) string {
	trimmed := strings.TrimSpace(incoming)
	if reW3CTrace.MatchString(trimmed) {
		return trimmed
	}
	var traceBytes [16]byte
	var spanBytes [8]byte
	_, _ = rand.Read(traceBytes[:])
	_, _ = rand.Read(spanBytes[:])
	return "00-" + hex.EncodeToString(traceBytes[:]) + "-" + hex.EncodeToString(spanBytes[:]) + "-01"
}

func PropagateTraceHeaders(inReq *http.Request, outHeaders http.Header) string {
	var incoming string
	if inReq != nil {
		incoming = inReq.Header.Get("traceparent")
	}
	tp := EnsureTraceparent(incoming)
	if outHeaders != nil {
		outHeaders.Set("traceparent", tp)
	}
	return tp
}

func MaskSensitivePII(raw string) string {
	if raw == "" {
		return ""
	}
	masked := rePasswordKV.ReplaceAllString(raw, "${1}[REDACTED]${3}")
	masked = rePAN.ReplaceAllString(masked, "[PAN_MASKED]")
	masked = reCPF.ReplaceAllString(masked, "[CPF_MASKED]")
	return masked
}

func FormatStructuredJSONLog(service, level, msg, traceparent string) string {
	tp := EnsureTraceparent(traceparent)
	parts := strings.Split(tp, "-")
	lvl := strings.ToUpper(strings.TrimSpace(level))
	if lvl == "" {
		lvl = "INFO"
	}
	payload := map[string]string{
		"level":       lvl,
		"dd.service":  service,
		"dd.env":      "production",
		"dd.version":  "0.1.0",
		"dd.trace_id": parts[1],
		"dd.span_id":  parts[2],
		"traceparent": tp,
		"message":     MaskSensitivePII(msg),
	}
	b, _ := json.Marshal(payload)
	return string(b)
}
