package config

import (
	"bytes"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAIRecoveryConfigLegacyRetryWindowWarnsOnceWhenExplicit(t *testing.T) {
	resetViperWithJWTSecret(t)
	t.Setenv("GATEWAY_OPENAI_RETRY_BUDGET_SECONDS", "")
	require.NoError(t, os.Unsetenv("GATEWAY_OPENAI_RETRY_BUDGET_SECONDS"))
	previousWarning := openAIRetryBudgetDeprecationWarned.Swap(false)
	t.Cleanup(func() { openAIRetryBudgetDeprecationWarned.Store(previousWarning) })
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(previousLogger) })
	_, err := Load()
	require.NoError(t, err)
	assert.NotContains(t, logs.String(), "deprecated for managed OpenAI recovery")
	t.Setenv("GATEWAY_OPENAI_RETRY_BUDGET_SECONDS", "300")
	for range 2 {
		cfg, err := Load()
		require.NoError(t, err)
		assert.Equal(t, 300, cfg.Gateway.OpenAIRetryBudgetSeconds)
	}
	assert.Equal(t, 1, strings.Count(logs.String(), "deprecated for managed OpenAI recovery"))
}
