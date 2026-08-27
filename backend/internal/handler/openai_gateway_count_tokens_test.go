package handler

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestAcquireCountTokensAccountSlotMaterializesWaitPlan(t *testing.T) {
	cache := &helperConcurrencyCacheStub{accountSeq: []bool{false, true}}
	h := &OpenAIGatewayHandler{
		gatewayService:    &service.OpenAIGatewayService{},
		concurrencyHelper: NewConcurrencyHelper(service.NewConcurrencyService(cache), SSEPingFormatNone, time.Millisecond),
	}
	c, _ := newHelperTestContext(http.MethodPost, "/v1/messages/count_tokens")
	groupID := int64(42)
	selection := &service.AccountSelectionResult{
		Account: &service.Account{ID: 101, Concurrency: 1},
		WaitPlan: &service.AccountWaitPlan{
			AccountID:      101,
			MaxConcurrency: 1,
			Timeout:        time.Second,
			MaxWaiting:     1,
		},
	}

	release, retry, err := h.acquireCountTokensAccountSlot(c, &groupID, "session", selection, zap.NewNop())
	require.NoError(t, err)
	require.False(t, retry)
	require.NotNil(t, release)

	cache.mu.Lock()
	require.Equal(t, 2, cache.accountAcquireCalls, "WaitPlan must poll until it owns a real account slot")
	cache.mu.Unlock()

	release()
	cache.mu.Lock()
	require.Equal(t, 1, cache.accountReleaseCalls)
	cache.mu.Unlock()
}

func TestCountTokensUpstreamEvidenceRequiresForwardMarker(t *testing.T) {
	c, _ := newHelperTestContext(http.MethodPost, "/v1/messages/count_tokens")
	require.False(t, countTokensHasUpstreamEvidence(c),
		"a local timeout/EOF has no account health evidence until Forward marks an upstream attempt")

	c.Set(service.OpsUpstreamErrorMessageKey, "upstream connection reset")
	require.True(t, countTokensHasUpstreamEvidence(c))

	c, _ = newHelperTestContext(http.MethodPost, "/v1/messages/count_tokens")
	c.Set(service.OpsUpstreamStatusCodeKey, http.StatusBadGateway)
	require.True(t, countTokensHasUpstreamEvidence(c))

	c, _ = newHelperTestContext(http.MethodPost, "/v1/messages/count_tokens")
	c.Set(service.OpsUpstreamStatusCodeKey, http.StatusOK)
	require.True(t, countTokensHasUpstreamEvidence(c))
	require.NoError(t, countTokensHealthReportError(c, nil, true))
}

func TestCountTokensHTTP200StreamMarkerIsHealthFailure(t *testing.T) {
	c, _ := newHelperTestContext(http.MethodPost, "/v1/messages/count_tokens")
	c.Set(service.OpsUpstreamStatusCodeKey, int64(http.StatusOK))
	c.Set(service.OpsUpstreamErrorMessageKey, "stream error: upstream response missing input_tokens")

	reportErr := countTokensHealthReportError(c, errors.New("input_tokens response missing input_tokens field"), true)
	classification := service.ClassifyTrafficDirectorHealthFailure(service.TrafficDirectorHealthFailure{
		StatusCode:    http.StatusOK,
		Err:           reportErr,
		AccountScoped: true,
	})

	require.True(t, classification.Eligible)
	require.Equal(t, service.TrafficDirectorHealthFailureKindStream, classification.Kind)
}
