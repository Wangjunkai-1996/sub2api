//go:build unit

package handler

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type inputTokensAdmissionCache struct {
	*helperConcurrencyCacheStub
	service.OpenAI429RecoveryCache
	delays   map[int64][]time.Duration
	stateErr error
	attempts []int64
}

func (c *inputTokensAdmissionCache) AcquireOpenAI429Attempt(_ context.Context, accountID int64, _, token string, _ time.Duration) (service.OpenAI429Admission, error) {
	c.attempts = append(c.attempts, accountID)
	if c.stateErr != nil {
		return service.OpenAI429Admission{}, c.stateErr
	}
	if delays := c.delays[accountID]; len(delays) > 0 {
		c.delays[accountID] = delays[1:]
		return service.OpenAI429Admission{RetryAfter: delays[0]}, nil
	}
	return service.OpenAI429Admission{Allowed: true, Generation: token}, nil
}

type inputTokensRecoveryUpstream struct {
	service.HTTPUpstream
	accounts []int64
}

func (u *inputTokensRecoveryUpstream) Do(_ *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.accounts = append(u.accounts, accountID)
	return &http.Response{
		StatusCode: http.StatusOK, Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(`{"object":"response.input_tokens","input_tokens":17}`)),
	}, nil
}

func newRecoveryAdmissionTestHandler(t *testing.T, cache service.ConcurrencyCache, accounts []service.Account, upstream service.HTTPUpstream) *OpenAIGatewayHandler {
	t.Helper()
	concurrency := service.NewConcurrencyService(cache)
	cfg := &config.Config{RunMode: config.RunModeSimple}
	gateway := service.NewOpenAIGatewayService(
		openAIImagesFailoverAccountRepo{accounts: accounts}, nil, nil, nil, nil, nil, nil, cfg,
		nil, concurrency, nil, nil, nil, upstream, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	return NewOpenAIGatewayHandler(gateway, concurrency, billing, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
}

func TestOpenAI429InputTokensHandlerRecoversOrReturnsRetryHint(t *testing.T) {
	for _, tc := range []struct {
		name          string
		delays        map[int64][]time.Duration
		stateErr      error
		accountCount  int
		wantStatus    int
		wantRetry     string
		wantCode      string
		wantUpstream  []int64
		wantAdmission []int64
	}{
		{name: "healthy alternate", accountCount: 2, delays: map[int64][]time.Duration{1: {90 * time.Second}}, wantStatus: 200, wantUpstream: []int64{2}, wantAdmission: []int64{1, 2}},
		{name: "pool cooldown", accountCount: 2, delays: map[int64][]time.Duration{1: {90 * time.Second}, 2: {4 * time.Second}}, wantStatus: 429, wantRetry: "4", wantCode: "account_pool_rate_limited", wantAdmission: []int64{1, 2}},
		{name: "near recovery", accountCount: 1, delays: map[int64][]time.Duration{1: {time.Nanosecond}}, wantStatus: 200, wantUpstream: []int64{1}, wantAdmission: []int64{1, 1}},
		{name: "cache unavailable", accountCount: 1, stateErr: errors.New("cache offline"), wantStatus: 503, wantRetry: "2", wantCode: "account_recovery_unavailable", wantAdmission: []int64{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := &inputTokensAdmissionCache{helperConcurrencyCacheStub: &helperConcurrencyCacheStub{}, delays: tc.delays, stateErr: tc.stateErr}
			accounts := make([]service.Account, tc.accountCount)
			for i := range accounts {
				accounts[i] = service.Account{
					ID: int64(i + 1), Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
					Status: service.StatusActive, Schedulable: true, Priority: i, Concurrency: 1,
					Credentials: map[string]any{"access_token": "access-token"},
				}
			}
			upstream := &inputTokensRecoveryUpstream{}
			h := newRecoveryAdmissionTestHandler(t, cache, accounts, upstream)
			c, recorder := newOpenAIResponsesFailoverTestContext(t, nil)
			c.Request.URL.Path = "/v1/responses/input_tokens"

			h.ResponsesInputTokens(c)

			require.Equal(t, tc.wantStatus, recorder.Code, recorder.Body.String())
			assert.Equal(t, tc.wantRetry, recorder.Header().Get("Retry-After"))
			if tc.wantCode != "" {
				assert.Contains(t, recorder.Body.String(), `"code":"`+tc.wantCode+`"`)
			}
			assert.Equal(t, tc.wantUpstream, upstream.accounts)
			assert.Equal(t, tc.wantAdmission, cache.attempts)
			assert.Zero(t, cache.accountAcquireCalls, "native token counting must not acquire a generation slot")
		})
	}
}
