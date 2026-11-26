package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	OpenAICyberAccountCooldownMinSeconds = 60
	OpenAICyberAccountCooldownMaxSeconds = 604800

	defaultOpenAICyberAccountCooldownWindowSeconds    = 86400
	defaultOpenAICyberAccountCooldownFirstSeconds     = 3600
	defaultOpenAICyberAccountCooldownEscalatedSeconds = 86400

	openAICyberAccountCooldownRuntimeCacheTTL  = 60 * time.Second
	openAICyberAccountCooldownRuntimeErrorTTL  = 5 * time.Second
	openAICyberAccountCooldownRuntimeDBTimeout = 5 * time.Second
	openAICyberAccountCooldownRuntimeSFKey     = "openai_cyber_account_cooldown_runtime"
)

var defaultOpenAICyberAccountCooldownGroupIDs = []int64{12}

type OpenAICyberAccountCooldownPolicy struct {
	enabled         bool
	window          time.Duration
	first           time.Duration
	escalated       time.Duration
	groupIDs        map[int64]struct{}
	orderedGroupIDs []int64
}

func normalizeOpenAICyberAccountCooldownGroupIDs(groupIDs []int64) []int64 {
	seen := make(map[int64]struct{}, len(groupIDs))
	normalized := make([]int64, 0, len(groupIDs))
	for _, groupID := range groupIDs {
		if groupID <= 0 {
			continue
		}
		if _, exists := seen[groupID]; exists {
			continue
		}
		seen[groupID] = struct{}{}
		normalized = append(normalized, groupID)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i] < normalized[j] })
	return normalized
}

func parseOpenAICyberAccountCooldownGroupIDs(raw string) ([]int64, error) {
	if strings.TrimSpace(raw) == "" {
		return append([]int64(nil), defaultOpenAICyberAccountCooldownGroupIDs...), nil
	}
	var groupIDs []int64
	if err := json.Unmarshal([]byte(raw), &groupIDs); err != nil {
		return nil, fmt.Errorf("%s must be a JSON integer array: %w", SettingKeyOpenAICyberAccountCooldownGroupIDs, err)
	}
	groupIDs = normalizeOpenAICyberAccountCooldownGroupIDs(groupIDs)
	if len(groupIDs) == 0 {
		return nil, fmt.Errorf("%s must contain at least one positive group ID", SettingKeyOpenAICyberAccountCooldownGroupIDs)
	}
	return groupIDs, nil
}

func newOpenAICyberAccountCooldownPolicy(enabled bool, windowSeconds, firstSeconds, escalatedSeconds int, groupIDs []int64) OpenAICyberAccountCooldownPolicy {
	if windowSeconds < OpenAICyberAccountCooldownMinSeconds || windowSeconds > OpenAICyberAccountCooldownMaxSeconds {
		windowSeconds = defaultOpenAICyberAccountCooldownWindowSeconds
	}
	if firstSeconds < OpenAICyberAccountCooldownMinSeconds || firstSeconds > OpenAICyberAccountCooldownMaxSeconds {
		firstSeconds = defaultOpenAICyberAccountCooldownFirstSeconds
	}
	if escalatedSeconds < OpenAICyberAccountCooldownMinSeconds || escalatedSeconds > OpenAICyberAccountCooldownMaxSeconds {
		escalatedSeconds = defaultOpenAICyberAccountCooldownEscalatedSeconds
	}
	if escalatedSeconds < firstSeconds {
		escalatedSeconds = firstSeconds
	}
	groupIDs = normalizeOpenAICyberAccountCooldownGroupIDs(groupIDs)
	if len(groupIDs) == 0 {
		groupIDs = append([]int64(nil), defaultOpenAICyberAccountCooldownGroupIDs...)
	}
	groupSet := make(map[int64]struct{}, len(groupIDs))
	for _, groupID := range groupIDs {
		groupSet[groupID] = struct{}{}
	}
	return OpenAICyberAccountCooldownPolicy{
		enabled:         enabled,
		window:          time.Duration(windowSeconds) * time.Second,
		first:           time.Duration(firstSeconds) * time.Second,
		escalated:       time.Duration(escalatedSeconds) * time.Second,
		groupIDs:        groupSet,
		orderedGroupIDs: groupIDs,
	}
}

func (p OpenAICyberAccountCooldownPolicy) Enabled() bool                    { return p.enabled }
func (p OpenAICyberAccountCooldownPolicy) Window() time.Duration            { return p.window }
func (p OpenAICyberAccountCooldownPolicy) FirstDuration() time.Duration     { return p.first }
func (p OpenAICyberAccountCooldownPolicy) EscalatedDuration() time.Duration { return p.escalated }
func (p OpenAICyberAccountCooldownPolicy) GroupIDs() []int64 {
	return append([]int64(nil), p.orderedGroupIDs...)
}
func (p OpenAICyberAccountCooldownPolicy) IncludesGroup(groupID int64) bool {
	_, exists := p.groupIDs[groupID]
	return exists
}

type cachedOpenAICyberAccountCooldownRuntime struct {
	policy    OpenAICyberAccountCooldownPolicy
	expiresAt int64
}

func parseOpenAICyberAccountCooldownSeconds(raw string, fallback int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || parsed < OpenAICyberAccountCooldownMinSeconds || parsed > OpenAICyberAccountCooldownMaxSeconds {
		return fallback
	}
	return parsed
}

func ValidateOpenAICyberAccountCooldownDurations(windowSeconds, firstSeconds, escalatedSeconds int) error {
	for _, field := range []struct {
		key   string
		value int
	}{
		{key: SettingKeyOpenAICyberAccountCooldownWindowSeconds, value: windowSeconds},
		{key: SettingKeyOpenAICyberAccountCooldownFirstSeconds, value: firstSeconds},
		{key: SettingKeyOpenAICyberAccountCooldownEscalatedSeconds, value: escalatedSeconds},
	} {
		if field.value < OpenAICyberAccountCooldownMinSeconds || field.value > OpenAICyberAccountCooldownMaxSeconds {
			return fmt.Errorf("%s must be between %d and %d", field.key, OpenAICyberAccountCooldownMinSeconds, OpenAICyberAccountCooldownMaxSeconds)
		}
	}
	if escalatedSeconds < firstSeconds {
		return fmt.Errorf("%s must be greater than or equal to %s", SettingKeyOpenAICyberAccountCooldownEscalatedSeconds, SettingKeyOpenAICyberAccountCooldownFirstSeconds)
	}
	return nil
}

func normalizeOpenAICyberAccountCooldownSettings(settings *SystemSettings) error {
	settings.OpenAICyberAccountCooldownGroupIDs = normalizeOpenAICyberAccountCooldownGroupIDs(settings.OpenAICyberAccountCooldownGroupIDs)
	if len(settings.OpenAICyberAccountCooldownGroupIDs) == 0 {
		settings.OpenAICyberAccountCooldownGroupIDs = append([]int64(nil), defaultOpenAICyberAccountCooldownGroupIDs...)
	}
	if settings.OpenAICyberAccountCooldownWindowSeconds == 0 {
		settings.OpenAICyberAccountCooldownWindowSeconds = defaultOpenAICyberAccountCooldownWindowSeconds
	}
	if settings.OpenAICyberAccountCooldownFirstSeconds == 0 {
		settings.OpenAICyberAccountCooldownFirstSeconds = defaultOpenAICyberAccountCooldownFirstSeconds
	}
	if settings.OpenAICyberAccountCooldownEscalatedSeconds == 0 {
		settings.OpenAICyberAccountCooldownEscalatedSeconds = defaultOpenAICyberAccountCooldownEscalatedSeconds
	}
	return ValidateOpenAICyberAccountCooldownDurations(
		settings.OpenAICyberAccountCooldownWindowSeconds,
		settings.OpenAICyberAccountCooldownFirstSeconds,
		settings.OpenAICyberAccountCooldownEscalatedSeconds,
	)
}

func conservativeOpenAICyberAccountCooldownPolicy() OpenAICyberAccountCooldownPolicy {
	return newOpenAICyberAccountCooldownPolicy(
		true,
		defaultOpenAICyberAccountCooldownWindowSeconds,
		defaultOpenAICyberAccountCooldownEscalatedSeconds,
		defaultOpenAICyberAccountCooldownEscalatedSeconds,
		defaultOpenAICyberAccountCooldownGroupIDs,
	)
}

func (s *SettingService) GetOpenAICyberAccountCooldownRuntime(ctx context.Context) OpenAICyberAccountCooldownPolicy {
	if s == nil || s.settingRepo == nil {
		return conservativeOpenAICyberAccountCooldownPolicy()
	}
	if cached, ok := s.openAICyberAccountCooldownRuntimeCache.Load().(*cachedOpenAICyberAccountCooldownRuntime); ok && cached != nil && time.Now().UnixNano() < cached.expiresAt {
		return cached.policy
	}

	result, _, _ := s.openAICyberAccountCooldownRuntimeSF.Do(openAICyberAccountCooldownRuntimeSFKey, func() (any, error) {
		if cached, ok := s.openAICyberAccountCooldownRuntimeCache.Load().(*cachedOpenAICyberAccountCooldownRuntime); ok && cached != nil && time.Now().UnixNano() < cached.expiresAt {
			return cached, nil
		}
		dbCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), openAICyberAccountCooldownRuntimeDBTimeout)
		defer cancel()
		values, err := s.settingRepo.GetMultiple(dbCtx, []string{
			SettingKeyOpenAICyberAccountCooldownEnabled,
			SettingKeyOpenAICyberAccountCooldownWindowSeconds,
			SettingKeyOpenAICyberAccountCooldownFirstSeconds,
			SettingKeyOpenAICyberAccountCooldownEscalatedSeconds,
			SettingKeyOpenAICyberAccountCooldownGroupIDs,
		})
		if err != nil {
			policy := conservativeOpenAICyberAccountCooldownPolicy()
			if stale, ok := s.openAICyberAccountCooldownRuntimeCache.Load().(*cachedOpenAICyberAccountCooldownRuntime); ok && stale != nil {
				policy = stale.policy
			}
			slog.Warn("failed to get OpenAI Cyber account cooldown policy; retaining last policy", "error", err)
			entry := &cachedOpenAICyberAccountCooldownRuntime{policy: policy, expiresAt: time.Now().Add(openAICyberAccountCooldownRuntimeErrorTTL).UnixNano()}
			s.openAICyberAccountCooldownRuntimeCache.Store(entry)
			return entry, nil
		}

		enabled := strings.TrimSpace(values[SettingKeyOpenAICyberAccountCooldownEnabled]) == "true"
		window := parseOpenAICyberAccountCooldownSeconds(values[SettingKeyOpenAICyberAccountCooldownWindowSeconds], defaultOpenAICyberAccountCooldownWindowSeconds)
		first := parseOpenAICyberAccountCooldownSeconds(values[SettingKeyOpenAICyberAccountCooldownFirstSeconds], defaultOpenAICyberAccountCooldownFirstSeconds)
		escalated := parseOpenAICyberAccountCooldownSeconds(values[SettingKeyOpenAICyberAccountCooldownEscalatedSeconds], defaultOpenAICyberAccountCooldownEscalatedSeconds)
		groupIDs, parseErr := parseOpenAICyberAccountCooldownGroupIDs(values[SettingKeyOpenAICyberAccountCooldownGroupIDs])
		if parseErr != nil {
			policy := conservativeOpenAICyberAccountCooldownPolicy()
			if stale, ok := s.openAICyberAccountCooldownRuntimeCache.Load().(*cachedOpenAICyberAccountCooldownRuntime); ok && stale != nil {
				policy = stale.policy
			}
			slog.Warn("invalid OpenAI Cyber account cooldown group IDs; retaining last policy", "error", parseErr)
			entry := &cachedOpenAICyberAccountCooldownRuntime{policy: policy, expiresAt: time.Now().Add(openAICyberAccountCooldownRuntimeErrorTTL).UnixNano()}
			s.openAICyberAccountCooldownRuntimeCache.Store(entry)
			return entry, nil
		}
		entry := &cachedOpenAICyberAccountCooldownRuntime{
			policy:    newOpenAICyberAccountCooldownPolicy(enabled, window, first, escalated, groupIDs),
			expiresAt: time.Now().Add(openAICyberAccountCooldownRuntimeCacheTTL).UnixNano(),
		}
		s.openAICyberAccountCooldownRuntimeCache.Store(entry)
		return entry, nil
	})
	if entry, ok := result.(*cachedOpenAICyberAccountCooldownRuntime); ok && entry != nil {
		return entry.policy
	}
	return conservativeOpenAICyberAccountCooldownPolicy()
}
