package service

import (
	"maps"
	"strings"
)

// Retired fork metadata must not be restored by old backups or exposed in exports.
func IsRetiredAccountExtraKey(key string) bool {
	return strings.HasPrefix(key, "codex_turn_ticket:") || key == "codex_ticket_config" ||
		key == "codex_ticket_watchdog" || key == "codex_harvest_proxy_url" ||
		key == "openai_codex_warmup_policy" || key == "codex_warmup_policy" || key == "openai_window_warmup_policy"
}

func FilterRetiredAccountExtra(extra map[string]any) map[string]any {
	result := maps.Clone(extra)
	for key := range result {
		if IsRetiredAccountExtraKey(key) {
			delete(result, key)
		}
	}
	return result
}
