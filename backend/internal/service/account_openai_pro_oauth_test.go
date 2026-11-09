package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsOpenAIProOAuthAccount(t *testing.T) {
	tests := []struct {
		name    string
		account *Account
		want    bool
	}{
		{name: "nil account"},
		{name: "openai pro oauth", account: &Account{
			ID: 1, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
			Credentials: map[string]any{"plan_type": "pro"},
		}, want: true},
		{name: "plan type ignores case and surrounding whitespace", account: &Account{
			ID: 2, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
			Credentials: map[string]any{"plan_type": " Pro "},
		}, want: true},
		{name: "classification does not require a persisted id", account: &Account{
			Platform: PlatformOpenAI, Type: AccountTypeOAuth,
			Credentials: map[string]any{"plan_type": "pro"},
		}, want: true},
		{name: "plus plan", account: &Account{
			ID: 3, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
			Credentials: map[string]any{"plan_type": "plus"},
		}},
		{name: "plan alias is not exact pro", account: &Account{
			ID: 4, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
			Credentials: map[string]any{"plan_type": "ChatGPTPro"},
		}},
		{name: "missing plan type", account: &Account{
			ID: 5, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		}},
		{name: "openai api key", account: &Account{
			ID: 6, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
			Credentials: map[string]any{"plan_type": "pro"},
		}},
		{name: "openai setup token", account: &Account{
			ID: 7, Platform: PlatformOpenAI, Type: AccountTypeSetupToken,
			Credentials: map[string]any{"plan_type": "pro"},
		}},
		{name: "non openai oauth", account: &Account{
			ID: 8, Platform: PlatformGrok, Type: AccountTypeOAuth,
			Credentials: map[string]any{"plan_type": "pro"},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsOpenAIProOAuthAccount(tt.account))
		})
	}
}
