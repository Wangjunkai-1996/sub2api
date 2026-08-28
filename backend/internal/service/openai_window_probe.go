package service

// The warmup probe is intentionally kept separate from the account test and
// gateway paths.  It owns only the fixed, content-free Responses request and
// interpretation of the upstream terminal/reset evidence.  Credential
// material and the actual HTTP/TLS/plugin call belong to OpenAIOutboundExecutor.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	openaiPkg "github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/tidwall/gjson"
)

const (
	openAIWindowWarmupEndpoint = chatgptCodexURL
	// Keep enough of an SSE response to identify terminal/reset evidence while
	// making accidental retention of generated text unlikely.  The executor is
	// expected to enforce the same bound before returning a result.
	openAIWindowWarmupMaxBodyBytes      = 128 << 10
	openAIWindowWarmupOutcomeCompleted  = "completed"
	openAIWindowWarmupOutcomeIncomplete = "incomplete"
	openAIWindowWarmupOutcomeFailed     = "failed"
	openAIWindowWarmupOutcomeUncertain  = "uncertain"
	openAIWindowWarmupOutcomeBlocked    = "blocked"
	openAIWindowWarmupMaxModelBytes     = 128
)

var (
	ErrOpenAIWindowWarmupNeedsReauth   = errors.New("needs_reauth")
	ErrOpenAIWindowWarmupBlocked       = errors.New("blocked")
	ErrOpenAIWindowWarmupBlockedConfig = errors.New("blocked_config")
)

// OpenAICodexWindowProbe creates the one-shot request used to advance a
// subscription's five-hour Codex window.  The executor is deliberately
// injectable so production can select the built-in HTTP/TLS path or the
// installed OAuth transport plugin without changing probe semantics.
type OpenAICodexWindowProbe struct {
	executor OpenAIOutboundExecutor
	model    string
	modelErr error
	endpoint string
	timeout  time.Duration
}

// NewOpenAICodexWindowProbe constructs a probe.  An omitted/blank model uses
// the controlled CodexUsageProbeModel rather than a client-supplied model.
func NewOpenAICodexWindowProbe(executor OpenAIOutboundExecutor, model ...string) *OpenAICodexWindowProbe {
	selected := openaiPkg.CodexUsageProbeModel
	if len(model) > 0 && strings.TrimSpace(model[0]) != "" {
		selected = strings.TrimSpace(model[0])
	}
	normalized, modelErr := NormalizeOpenAIWindowWarmupProbeModel(selected)
	if modelErr == nil {
		selected = normalized
	}
	return &OpenAICodexWindowProbe{
		executor: executor,
		model:    selected,
		modelErr: modelErr,
		endpoint: openAIWindowWarmupEndpoint,
		timeout:  openAIWindowWarmupDefaultTimeout,
	}
}

func (p *OpenAICodexWindowProbe) SetRequestTimeout(timeout time.Duration) {
	if p == nil {
		return
	}
	if timeout <= 0 {
		timeout = openAIWindowWarmupDefaultTimeout
	}
	p.timeout = timeout
}

// NormalizeOpenAIWindowWarmupProbeModel accepts only the repository's
// controlled Codex subscription text-model catalog. Spark and image models
// have independent quota semantics and therefore fail closed.
func NormalizeOpenAIWindowWarmupProbeModel(raw string) (string, error) {
	model := strings.TrimSpace(raw)
	if model == "" {
		return "", fmt.Errorf("%w: model is required", ErrOpenAIWindowWarmupBlockedConfig)
	}
	if len(model) > openAIWindowWarmupMaxModelBytes {
		return "", fmt.Errorf("%w: model identifier is too long", ErrOpenAIWindowWarmupBlockedConfig)
	}
	if strings.Contains(strings.ToLower(model), "spark") {
		return "", fmt.Errorf("%w: Spark quota is excluded", ErrOpenAIWindowWarmupBlockedConfig)
	}
	for _, candidate := range openaiPkg.DefaultModels {
		if candidate.ID == model && (model == openaiPkg.CodexUsageProbeModel || strings.HasPrefix(model, "gpt-5")) {
			return model, nil
		}
	}
	return "", fmt.Errorf("%w: model is outside the controlled Codex catalog", ErrOpenAIWindowWarmupBlockedConfig)
}

// Model returns the controlled model used by the probe.
func (p *OpenAICodexWindowProbe) Model() string {
	if p == nil || strings.TrimSpace(p.model) == "" {
		return openaiPkg.CodexUsageProbeModel
	}
	return p.model
}

// Endpoint returns the upstream Responses endpoint.  It is exposed for
// diagnostics/tests, while callers should continue to use Probe.
func (p *OpenAICodexWindowProbe) Endpoint() string {
	if p == nil || strings.TrimSpace(p.endpoint) == "" {
		return openAIWindowWarmupEndpoint
	}
	return p.endpoint
}

func (p *OpenAICodexWindowProbe) RequestTimeout() time.Duration {
	if p == nil || p.timeout <= 0 {
		return openAIWindowWarmupDefaultTimeout
	}
	return p.timeout
}

// Probe performs one fixed minimal Responses request and returns only bounded
// metadata.  expectedResetAt is used as an evidence guard: a successful HTTP
// response without a *future* reset is never reported as completed.
func (p *OpenAICodexWindowProbe) Probe(ctx context.Context, account *Account, expectedResetAt *time.Time) (*OpenAIWindowProbeResult, error) {
	if p == nil || p.executor == nil {
		return nil, errors.New("warmup executor is not configured")
	}
	if p.modelErr != nil {
		return nil, p.modelErr
	}
	if !warmupProbeAccountEligible(account) {
		return nil, errors.New("warmup account is not an eligible OpenAI OAuth account")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	payload, err := buildOpenAIWindowWarmupPayload(p.Model())
	if err != nil {
		return nil, fmt.Errorf("build warmup payload: %w", err)
	}
	headers := buildOpenAIWindowWarmupHeaders(account)
	request := OpenAIOutboundRequest{
		Account:  account,
		Model:    p.Model(),
		Payload:  payload,
		Headers:  headers,
		Timeout:  p.RequestTimeout(),
		Endpoint: p.Endpoint(),
	}
	result, executeErr := p.executor.Execute(ctx, request)
	parsed := parseOpenAIWindowWarmupResult(result, expectedResetAt)
	if executeErr != nil {
		// Preserve the executor's bounded result so the service can distinguish a
		// request that may have reached upstream from one that failed pre-send.
		if parsed == nil {
			parsed = &OpenAIWindowProbeResult{Outcome: openAIWindowWarmupOutcomeUncertain}
		}
		if isWarmupProbePossiblySent(result, executeErr) {
			return parsed, fmt.Errorf("possibly_sent: %w", executeErr)
		}
		return parsed, executeErr
	}
	if parsed == nil {
		return nil, errors.New("warmup executor returned an empty result")
	}
	if err := validateOpenAIWindowWarmupOutcome(parsed, expectedResetAt); err != nil {
		return parsed, err
	}
	return parsed, nil
}

func warmupProbeAccountEligible(account *Account) bool {
	return account != nil && account.Platform == PlatformOpenAI &&
		account.Type == AccountTypeOAuth && account.ParentAccountID == nil &&
		!account.IsShadow() && account.IsActive() && account.Schedulable
}

// buildOpenAIWindowWarmupPayload is intentionally stable and tiny.  In
// particular it has no instructions, tools, previous_response_id, or user
// content from an inbound request.  `stream=true` is required by the Codex
// internal endpoint, and `store=false` prevents server-side response storage.
func buildOpenAIWindowWarmupPayload(model string) ([]byte, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		model = openaiPkg.CodexUsageProbeModel
	}
	payload := map[string]any{
		"model": model,
		"input": []map[string]any{
			{
				"role": "user",
				"content": []map[string]any{
					{"type": "input_text", "text": "ping"},
				},
			},
		},
		"stream": true,
		"store":  false,
	}
	return json.Marshal(payload)
}

// BuildOpenAIWindowWarmupPayload is an exported, read-only helper for adapter
// and contract tests.  It returns a fresh byte slice on every invocation.
func BuildOpenAIWindowWarmupPayload(model string) []byte {
	payload, _ := buildOpenAIWindowWarmupPayload(model)
	return payload
}

func buildOpenAIWindowWarmupHeaders(account *Account) http.Header {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	headers.Set("Accept", "text/event-stream")
	headers.Set("OpenAI-Beta", "responses=experimental")
	// Reuse the same identity normalization as ordinary Codex OAuth traffic.
	// The executor is responsible for adding Authorization (Bearer or Agent
	// Assertion) after refreshing/resolving credentials.
	applyOpenAICodexProbeHeaders(headers)
	if account != nil {
		setOpenAIChatGPTAccountHeaders(headers, account)
		if customUA := strings.TrimSpace(account.GetOpenAIUserAgent()); customUA != "" {
			headers.Set("User-Agent", customUA)
			enforceCodexIdentityHeadersWithUA(headers, customUA)
		}
		account.ApplyHeaderOverrides(headers)
	}
	return headers
}

// ParseOpenAIWindowWarmupResult interprets a bounded executor result.  It is
// exported so repository/service tests can exercise terminal and reset guards
// without making an upstream request.
func ParseOpenAIWindowWarmupResult(result *OpenAIOutboundResult, expectedResetAt *time.Time) *OpenAIWindowProbeResult {
	return parseOpenAIWindowWarmupResult(result, expectedResetAt)
}

func parseOpenAIWindowWarmupResult(result *OpenAIOutboundResult, expectedResetAt *time.Time) *OpenAIWindowProbeResult {
	if result == nil {
		return nil
	}
	headers := cloneWarmupHeaders(result.Headers)
	body := boundedWarmupBody(result.Body)
	resetAt := cloneWarmupTime(result.ResetAt)
	if resetAt == nil {
		resetAt = warmupResetFromHeaders(headers)
	}
	terminal := result.Terminal
	terminalType := strings.TrimSpace(result.TerminalType)
	if bodyTerminal, bodyType, bodyReset := parseWarmupSSEEvidence(body); bodyTerminal {
		terminal = true
		if terminalType == "" {
			terminalType = bodyType
		}
		if resetAt == nil {
			resetAt = bodyReset
		}
	}
	if terminalType == "" {
		terminalType = responseTerminalType(body)
	}
	outcome := openAIWindowWarmupOutcomeUncertain
	switch {
	case terminalType == "response.completed" || terminalType == "response.done":
		outcome = openAIWindowWarmupOutcomeCompleted
	case terminalType == "response.incomplete":
		outcome = openAIWindowWarmupOutcomeIncomplete
	case terminalType == "response.failed" || terminalType == "response.cancelled" || terminalType == "response.canceled":
		outcome = openAIWindowWarmupOutcomeFailed
	case result.StatusCode == http.StatusForbidden || result.StatusCode == http.StatusBadRequest || result.StatusCode == http.StatusNotFound:
		outcome = openAIWindowWarmupOutcomeBlocked
	}
	return &OpenAIWindowProbeResult{
		StatusCode:      result.StatusCode,
		Headers:         headers,
		Body:            body,
		Terminal:        terminal,
		TerminalType:    terminalType,
		ResetAt:         cloneWarmupTime(resetAt),
		ObservedResetAt: cloneWarmupTime(resetAt),
		EOF:             result.EOF,
		Outcome:         outcome,
	}
}

func validateOpenAIWindowWarmupOutcome(result *OpenAIWindowProbeResult, expectedResetAt *time.Time) error {
	if result == nil {
		return errors.New("warmup executor returned an empty result")
	}
	status := result.StatusCode
	if status == http.StatusUnauthorized {
		return fmt.Errorf("%w: upstream returned 401", ErrOpenAIWindowWarmupNeedsReauth)
	}
	if status == http.StatusForbidden {
		return fmt.Errorf("%w: upstream returned 403", ErrOpenAIWindowWarmupBlocked)
	}
	if status == http.StatusBadRequest || status == http.StatusNotFound {
		return fmt.Errorf("%w: upstream returned %d", ErrOpenAIWindowWarmupBlockedConfig, status)
	}
	if status == http.StatusTooManyRequests {
		return errors.New("rate_limited: warmup upstream returned 429")
	}
	if status < 200 || status >= 300 {
		return fmt.Errorf("upstream_status_%d: warmup upstream returned %d", status, status)
	}
	if !result.Terminal || (result.TerminalType != "response.completed" && result.TerminalType != "response.done") {
		return errors.New("possibly_sent: warmup response has no completed terminal event")
	}
	if result.ResetAt == nil || !result.ResetAt.After(time.Now()) {
		return errors.New("possibly_sent: warmup response has no future reset evidence")
	}
	if expectedResetAt != nil && !result.ResetAt.After(*expectedResetAt) {
		return errors.New("possibly_sent: warmup did not advance the observed reset")
	}
	return nil
}

func isWarmupProbePossiblySent(result *OpenAIOutboundResult, err error) bool {
	if result != nil && (result.Started || result.EOF || result.StatusCode > 0) {
		return true
	}
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "timeout") || strings.Contains(text, "eof") || strings.Contains(text, "possibly_sent")
}

func cloneWarmupHeaders(headers http.Header) http.Header {
	if headers == nil {
		return make(http.Header)
	}
	copy := make(http.Header, len(headers))
	for key, values := range headers {
		copy[key] = append([]string(nil), values...)
	}
	return copy
}

func boundedWarmupBody(body []byte) []byte {
	if len(body) <= openAIWindowWarmupMaxBodyBytes {
		return append([]byte(nil), body...)
	}
	return append([]byte(nil), body[:openAIWindowWarmupMaxBodyBytes]...)
}

func cloneWarmupTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := value.UTC()
	return &copy
}

// warmupResetFromHeaders converts the authoritative reset-after signal into
// an absolute timestamp at observation time.  It never assumes a five-hour
// duration.  The 5h/7d mapping follows ParseCodexRateLimitHeaders.Normalize.
func warmupResetFromHeaders(headers http.Header) *time.Time {
	snapshot := ParseCodexRateLimitHeaders(headers)
	if snapshot == nil {
		return nil
	}
	normalized := snapshot.Normalize()
	if normalized == nil || normalized.Reset5hSeconds == nil {
		return nil
	}
	seconds := *normalized.Reset5hSeconds
	if seconds < 0 {
		return nil
	}
	reset := time.Now().UTC().Add(time.Duration(seconds) * time.Second)
	return &reset
}

// parseWarmupSSEEvidence accepts both standard `data:` SSE frames and a
// non-stream JSON response returned by a compatible proxy.  It intentionally
// extracts only event type/status/reset metadata; response text is not parsed
// or logged.
func parseWarmupSSEEvidence(body []byte) (bool, string, *time.Time) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return false, "", nil
	}
	var terminal bool
	terminalType := ""
	var resetAt *time.Time
	forEachOpenAISSEDataPayload(string(trimmed), func(payload []byte) {
		typeName := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
		if typeName == "" {
			return
		}
		switch typeName {
		case "response.completed", "response.done", "response.incomplete", "response.failed", "response.cancelled", "response.canceled":
			terminal = true
			terminalType = typeName
		}
		if candidate := resetAtFromJSON(payload); candidate != nil {
			if resetAt == nil || candidate.After(*resetAt) {
				resetAt = candidate
			}
		}
	})
	if !terminal && gjson.Valid(string(trimmed)) {
		typeName := strings.TrimSpace(gjson.GetBytes(trimmed, "type").String())
		if typeName == "response" {
			typeName = strings.TrimSpace(gjson.GetBytes(trimmed, "status").String())
			if typeName == "completed" {
				typeName = "response.completed"
			}
		}
		if typeName == "response.completed" || typeName == "response.done" {
			terminal, terminalType = true, typeName
		}
		if candidate := resetAtFromJSON(trimmed); candidate != nil {
			resetAt = candidate
		}
	}
	return terminal, terminalType, resetAt
}

func responseTerminalType(body []byte) string {
	_, typeName, _ := parseWarmupSSEEvidence(body)
	return typeName
}

func resetAtFromJSON(payload []byte) *time.Time {
	// A successful warmup requires evidence for the Codex five-hour window,
	// never a generic or weekly reset. Relative reset signals are handled only
	// by the normalized Codex rate-limit header parser.
	for _, path := range []string{
		"codex_5h_reset_at",
		"metadata.codex_5h_reset_at",
		"response.codex_5h_reset_at",
		"response.metadata.codex_5h_reset_at",
		"response.rate_limit.codex_5h_reset_at",
		"response.usage.codex_5h_reset_at",
	} {
		value := gjson.GetBytes(payload, path)
		if !value.Exists() {
			continue
		}
		if t := parseWarmupTime(value.Value()); !t.IsZero() {
			t = t.UTC()
			return &t
		}
	}
	return nil
}
