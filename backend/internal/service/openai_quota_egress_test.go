package service

import (
	"context"
	"net/http"
	"testing"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestOpenAIQuotaPoolAccountWithoutEgressResolverFailsClosed(t *testing.T) {
	proxyID := int64(71)
	account := &Account{
		ID:         101,
		Platform:   PlatformOpenAI,
		Type:       AccountTypeOAuth,
		EgressMode: EgressModePool,
		ProxyID:    &proxyID,
		Proxy: &Proxy{
			ID:       proxyID,
			Protocol: "http",
			Host:     "legacy.example.test",
			Port:     3128,
			Status:   StatusActive,
		},
		Credentials: map[string]any{
			"chatgpt_account_id": "account-101",
		},
	}
	repo := &stubQuotaAccountRepo{accounts: map[int64]*Account{account.ID: account}}
	tokenProvider := NewOpenAITokenProvider(repo, &stubQuotaTokenCache{tokens: map[string]string{
		OpenAITokenCacheKey(account): "access-token",
	}}, nil)
	svc := NewOpenAIQuotaService(repo, nil, tokenProvider, nil)

	_, _, _, _, err := svc.prepareUpstreamCall(context.Background(), account.ID)

	require.Error(t, err)
	require.Equal(t, http.StatusServiceUnavailable, infraerrors.Code(err))
	require.Equal(t, "OPENAI_QUOTA_EGRESS_UNAVAILABLE", infraerrors.Reason(err))
}
