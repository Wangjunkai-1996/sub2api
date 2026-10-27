package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetiredAccountExtraDoesNotLeakOrMutateSource(t *testing.T) {
	extra := map[string]any{"ordinary": "retained"}
	for _, key := range []string{
		"codex_ticket_config", "codex_ticket_watchdog", "codex_harvest_proxy_url",
		"codex_turn_ticket:gpt-6-astra", "codex_turn_ticket:v2:config", "codex_turn_ticket:v2:lease",
		"openai_codex_warmup_policy", "codex_warmup_policy", "openai_window_warmup_policy",
	} {
		extra[key] = "private-retired-value"
	}
	require.Equal(t, map[string]any{"ordinary": "retained"}, FilterRetiredAccountExtra(extra))
	require.Len(t, extra, 10)
	for key := range extra {
		if key != "ordinary" {
			require.True(t, isAuditSensitiveBodyKey(key), key)
		}
	}
	require.Nil(t, FilterRetiredAccountExtra(nil))
}
