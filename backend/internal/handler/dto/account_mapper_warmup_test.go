package dto

import (
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestAccountListItemFromAccountPreservesOpenAIWarmupPolicy(t *testing.T) {
	src := &Account{OpenAICodexWarmupPolicy: string(service.OpenAIWindowWarmupPolicyContinuous)}

	got := AccountListItemFromAccount(src)

	require.Equal(t, string(service.OpenAIWindowWarmupPolicyContinuous), got.OpenAICodexWarmupPolicy)
}
