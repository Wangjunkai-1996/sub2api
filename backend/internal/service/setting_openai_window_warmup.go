package service

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

const (
	defaultOpenAIWindowWarmupWorkerConcurrency     = 1
	defaultOpenAIWindowWarmupGlobalQPS             = 0.2
	defaultOpenAIWindowWarmupBatchSize             = 20
	defaultOpenAIWindowWarmupScanSeconds           = 30
	defaultOpenAIWindowWarmupRequestTimeoutSeconds = 45
	defaultOpenAIWindowWarmupLeaseSeconds          = 120
	defaultOpenAIWindowWarmupResetGraceSeconds     = 90
)

func applyDefaultOpenAIWindowWarmupSettings(settings *SystemSettings) {
	if settings == nil {
		return
	}
	settings.OpenAIWindowWarmupDefaultPolicy = OpenAIWindowWarmupPolicyOff
	settings.OpenAIWindowWarmupAllowlist = []int64{}
	settings.OpenAIWindowWarmupProbeModel = "codex-auto-review"
	settings.OpenAIWindowWarmupWorkerConcurrency = defaultOpenAIWindowWarmupWorkerConcurrency
	settings.OpenAIWindowWarmupGlobalQPS = defaultOpenAIWindowWarmupGlobalQPS
	settings.OpenAIWindowWarmupBatchSize = defaultOpenAIWindowWarmupBatchSize
	settings.OpenAIWindowWarmupScanSeconds = defaultOpenAIWindowWarmupScanSeconds
	settings.OpenAIWindowWarmupRequestTimeoutSeconds = defaultOpenAIWindowWarmupRequestTimeoutSeconds
	settings.OpenAIWindowWarmupLeaseSeconds = defaultOpenAIWindowWarmupLeaseSeconds
	settings.OpenAIWindowWarmupResetGraceSeconds = defaultOpenAIWindowWarmupResetGraceSeconds
}

func openAIWindowWarmupSettingsUnset(settings *SystemSettings) bool {
	return settings != nil &&
		strings.TrimSpace(settings.OpenAIWindowWarmupDefaultPolicy) == "" &&
		strings.TrimSpace(settings.OpenAIWindowWarmupProbeModel) == "" &&
		settings.OpenAIWindowWarmupWorkerConcurrency == 0 &&
		settings.OpenAIWindowWarmupGlobalQPS == 0 &&
		settings.OpenAIWindowWarmupBatchSize == 0 &&
		settings.OpenAIWindowWarmupScanSeconds == 0 &&
		settings.OpenAIWindowWarmupRequestTimeoutSeconds == 0 &&
		settings.OpenAIWindowWarmupLeaseSeconds == 0 &&
		settings.OpenAIWindowWarmupResetGraceSeconds == 0 &&
		len(settings.OpenAIWindowWarmupAllowlist) == 0 &&
		!settings.OpenAIWindowWarmupEnabled
}

func applyOpenAIWindowWarmupSettings(result *SystemSettings, values map[string]string) {
	if result == nil {
		return
	}
	result.OpenAIWindowWarmupEnabled = values[SettingKeyOpenAIWindowWarmupEnabled] == "true"
	result.OpenAIWindowWarmupDefaultPolicy = string(NormalizeOpenAIWindowWarmupPolicy(values[SettingKeyOpenAIWindowWarmupDefaultPolicy]))
	result.OpenAIWindowWarmupAllowlist = parseOpenAIWindowWarmupAllowlist(values[SettingKeyOpenAIWindowWarmupAllowlist])
	result.OpenAIWindowWarmupProbeModel = strings.TrimSpace(values[SettingKeyOpenAIWindowWarmupProbeModel])
	if result.OpenAIWindowWarmupProbeModel == "" {
		result.OpenAIWindowWarmupProbeModel = "codex-auto-review"
	}
	result.OpenAIWindowWarmupWorkerConcurrency = parseWarmupInt(values[SettingKeyOpenAIWindowWarmupWorkerConcurrency], defaultOpenAIWindowWarmupWorkerConcurrency)
	result.OpenAIWindowWarmupGlobalQPS = parseWarmupFloat(values[SettingKeyOpenAIWindowWarmupGlobalQPS], defaultOpenAIWindowWarmupGlobalQPS)
	result.OpenAIWindowWarmupBatchSize = parseWarmupInt(values[SettingKeyOpenAIWindowWarmupBatchSize], defaultOpenAIWindowWarmupBatchSize)
	result.OpenAIWindowWarmupScanSeconds = parseWarmupInt(values[SettingKeyOpenAIWindowWarmupScanSeconds], defaultOpenAIWindowWarmupScanSeconds)
	result.OpenAIWindowWarmupRequestTimeoutSeconds = parseWarmupInt(values[SettingKeyOpenAIWindowWarmupRequestTimeoutSeconds], defaultOpenAIWindowWarmupRequestTimeoutSeconds)
	result.OpenAIWindowWarmupLeaseSeconds = parseWarmupInt(values[SettingKeyOpenAIWindowWarmupLeaseSeconds], defaultOpenAIWindowWarmupLeaseSeconds)
	result.OpenAIWindowWarmupResetGraceSeconds = parseWarmupInt(values[SettingKeyOpenAIWindowWarmupResetGraceSeconds], defaultOpenAIWindowWarmupResetGraceSeconds)
	if validateOpenAIWindowWarmupSettings(result) != nil {
		result.OpenAIWindowWarmupEnabled = false
		result.OpenAIWindowWarmupWorkerConcurrency = defaultOpenAIWindowWarmupWorkerConcurrency
		result.OpenAIWindowWarmupGlobalQPS = defaultOpenAIWindowWarmupGlobalQPS
		result.OpenAIWindowWarmupBatchSize = defaultOpenAIWindowWarmupBatchSize
		result.OpenAIWindowWarmupScanSeconds = defaultOpenAIWindowWarmupScanSeconds
		result.OpenAIWindowWarmupRequestTimeoutSeconds = defaultOpenAIWindowWarmupRequestTimeoutSeconds
		result.OpenAIWindowWarmupLeaseSeconds = defaultOpenAIWindowWarmupLeaseSeconds
		result.OpenAIWindowWarmupResetGraceSeconds = defaultOpenAIWindowWarmupResetGraceSeconds
	}
}

func parseOpenAIWindowWarmupAllowlist(raw string) []int64 {
	ids, err := parseOpenAIWindowWarmupAllowlistStrict(raw)
	if err != nil {
		return []int64{}
	}
	return ids
}

func parseOpenAIWindowWarmupAllowlistStrict(raw string) ([]int64, error) {
	var ids []int64
	if err := json.Unmarshal([]byte(raw), &ids); err != nil {
		return nil, fmt.Errorf("decode OpenAI window warmup allowlist: %w", err)
	}
	if ids == nil {
		return nil, fmt.Errorf("OpenAI window warmup allowlist must be a JSON array")
	}
	for _, id := range ids {
		if id <= 0 {
			return nil, fmt.Errorf("OpenAI window warmup allowlist IDs must be positive")
		}
	}
	return normalizeWarmupAccountIDs(ids), nil
}

func parseWarmupInt(raw string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fallback
	}
	return value
}

func parseWarmupFloat(raw string, fallback float64) float64 {
	value, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return fallback
	}
	return value
}

func normalizeWarmupAccountIDs(ids []int64) []int64 {
	seen := make(map[int64]struct{}, len(ids))
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func validateOpenAIWindowWarmupSettings(settings *SystemSettings) error {
	if settings == nil {
		return infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_SETTINGS_REQUIRED", "OpenAI window warmup settings are required")
	}
	// Legacy service tests and internal callers may still construct a complete
	// SystemSettings value without the newly added block. Treat only the wholly
	// absent zero-value block as omitted; explicit partial/invalid values remain
	// subject to strict validation.
	if openAIWindowWarmupSettingsUnset(settings) {
		applyDefaultOpenAIWindowWarmupSettings(settings)
	}
	policy := NormalizeOpenAIWindowWarmupPolicy(settings.OpenAIWindowWarmupDefaultPolicy)
	if strings.TrimSpace(settings.OpenAIWindowWarmupDefaultPolicy) != string(policy) {
		return infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_POLICY_INVALID", "OpenAI window warmup default policy is invalid")
	}
	settings.OpenAIWindowWarmupDefaultPolicy = string(policy)
	for _, accountID := range settings.OpenAIWindowWarmupAllowlist {
		if accountID <= 0 {
			return infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_ALLOWLIST_INVALID", "OpenAI window warmup allowlist IDs must be positive")
		}
	}
	settings.OpenAIWindowWarmupAllowlist = normalizeWarmupAccountIDs(settings.OpenAIWindowWarmupAllowlist)
	model, err := NormalizeOpenAIWindowWarmupProbeModel(settings.OpenAIWindowWarmupProbeModel)
	if err != nil {
		return infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_MODEL_INVALID", "OpenAI window warmup probe model must be a controlled non-Spark Codex model")
	}
	settings.OpenAIWindowWarmupProbeModel = model
	checks := []struct {
		name       string
		value, min int
		max        int
	}{
		{"worker_concurrency", settings.OpenAIWindowWarmupWorkerConcurrency, 1, 8},
		{"batch_size", settings.OpenAIWindowWarmupBatchSize, 1, 100},
		{"scan_seconds", settings.OpenAIWindowWarmupScanSeconds, 5, 3600},
		{"request_timeout_seconds", settings.OpenAIWindowWarmupRequestTimeoutSeconds, 5, 300},
		{"lease_seconds", settings.OpenAIWindowWarmupLeaseSeconds, 10, 600},
		{"reset_grace_seconds", settings.OpenAIWindowWarmupResetGraceSeconds, 0, 900},
	}
	for _, check := range checks {
		if check.value < check.min || check.value > check.max {
			return infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_PARAMETER_INVALID", fmt.Sprintf("OpenAI window warmup %s must be between %d and %d", check.name, check.min, check.max))
		}
	}
	if settings.OpenAIWindowWarmupGlobalQPS <= 0 || settings.OpenAIWindowWarmupGlobalQPS > 0.2 || math.IsNaN(settings.OpenAIWindowWarmupGlobalQPS) || math.IsInf(settings.OpenAIWindowWarmupGlobalQPS, 0) {
		return infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_QPS_INVALID", "OpenAI window warmup global_qps must be greater than 0 and at most 0.2")
	}
	if settings.OpenAIWindowWarmupLeaseSeconds <= settings.OpenAIWindowWarmupRequestTimeoutSeconds {
		return infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_LEASE_INVALID", "OpenAI window warmup lease must be longer than request timeout")
	}
	return nil
}
