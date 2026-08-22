//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/stretchr/testify/require"
)

type openAISecurityStickyCacheProbe struct {
	*schedulerTestGatewayCache
	setCalls     int
	refreshCalls int
}

func (p *openAISecurityStickyCacheProbe) SetSessionAccountID(
	ctx context.Context,
	groupID int64,
	sessionHash string,
	accountID int64,
	ttl time.Duration,
) error {
	p.setCalls++
	return p.schedulerTestGatewayCache.SetSessionAccountID(ctx, groupID, sessionHash, accountID, ttl)
}

func (p *openAISecurityStickyCacheProbe) RefreshSessionTTL(
	ctx context.Context,
	groupID int64,
	sessionHash string,
	ttl time.Duration,
) error {
	p.refreshCalls++
	return p.schedulerTestGatewayCache.RefreshSessionTTL(ctx, groupID, sessionHash, ttl)
}

func TestOpenAIBindStickySessionAfterSecurityAdmissionRequiresVerifiedTerminal(t *testing.T) {
	account := &Account{ID: 7101, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	cache := &openAISecurityStickyCacheProbe{
		schedulerTestGatewayCache: &schedulerTestGatewayCache{sessionBindings: make(map[string]int64)},
	}
	svc := &OpenAIGatewayService{cache: cache}
	gatedCtx := WithOpenAIAccountRequirement(context.Background(), securityadmission.AccountRequirementAuditExempt)

	err := svc.BindStickySessionAfterProfitAdmission(gatedCtx, nil, "security-sticky", account.ID)
	require.ErrorIs(t, err, ErrOpenAIAccountAdmissionUnavailable)
	require.Zero(t, cache.setCalls, "security-gated requests must not bind before terminal admission")

	terminalCtx := WithOpenAIAccountTerminalAdmission(gatedCtx, &OpenAIAccountRequirementAdmission{
		Selected:                 account,
		EffectiveCredentialOwner: account,
		Requirement:              securityadmission.AccountRequirementAuditExempt,
		AccountClass:             securityadmission.AccountAuditExemptVerified,
	})
	err = svc.BindStickySessionAfterProfitAdmission(terminalCtx, nil, "security-sticky", account.ID)
	require.NoError(t, err)
	require.Greater(t, cache.setCalls, 0, "verified terminal admission should bind the successful account")
	require.Equal(t, account.ID, cache.sessionBindings["openai:security-sticky"])
}
