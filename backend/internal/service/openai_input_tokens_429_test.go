//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type inputTokensRecoveryTestCache struct {
	selectionRecoveryTestCache
	accepted int
	released int
	failed   int
}

func (c *inputTokensRecoveryTestCache) AcceptOpenAI429Attempt(context.Context, int64, string, string, string, string) (bool, error) {
	c.accepted++
	return true, nil
}

func (c *inputTokensRecoveryTestCache) ReleaseOpenAI429Probe(context.Context, int64, string, string, string) (bool, error) {
	c.released++
	return true, nil
}

func (c *inputTokensRecoveryTestCache) FailOpenAI429Attempt(context.Context, int64, string, string, string, string, time.Duration) (bool, error) {
	c.failed++
	return true, nil
}

func TestOpenAI429InputTokensRespectsAdmissionWithoutGenerationSlots(t *testing.T) {
	for _, tc := range []struct {
		name       string
		delay      time.Duration
		stateErr   error
		status     int
		body       string
		wantFailed int
	}{
		{name: "cooling account is not contacted", delay: 90 * time.Second},
		{name: "cache outage fails closed", stateErr: errors.New("cache offline")},
		{name: "token count success does not reopen generation", status: http.StatusOK, body: `{"object":"response.input_tokens","input_tokens":17}`},
		{name: "upstream 429 advances shared recovery", status: http.StatusTooManyRequests, body: `{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"Too many requests"}}`, wantFailed: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var generationSlots []int64
			cache := &inputTokensRecoveryTestCache{selectionRecoveryTestCache: selectionRecoveryTestCache{
				schedulerTestConcurrencyCache: schedulerTestConcurrencyCache{acquiredIDs: &generationSlots},
				deferred:                      map[int64]time.Duration{7101: tc.delay}, stateErr: tc.stateErr, probe: true,
			}}
			upstream := &httpUpstreamRecorder{resp: &http.Response{
				StatusCode: tc.status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(tc.body)),
			}}
			svc := &OpenAIGatewayService{
				cfg: &config.Config{}, httpUpstream: upstream, concurrencyService: NewConcurrencyService(cache),
			}
			account := &Account{
				ID: 7101, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive,
				Concurrency: 1, Credentials: map[string]any{"access_token": "access-token"},
			}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/input_tokens", nil)

			err := svc.ForwardResponsesInputTokens(c.Request.Context(), c, account, []byte(`{"model":"gpt-5.1","input":"hello"}`))

			switch {
			case tc.delay > 0:
				var cooldown *OpenAI429CooldownError
				require.ErrorAs(t, err, &cooldown)
				assert.Equal(t, tc.delay, cooldown.RetryAfter)
				assert.Nil(t, upstream.lastReq)
				assert.False(t, c.Writer.Written(), "handler must be able to choose another account")
			case tc.stateErr != nil:
				require.ErrorIs(t, err, ErrOpenAI429RecoveryUnavailable)
				assert.Nil(t, upstream.lastReq)
				assert.False(t, c.Writer.Written())
			case tc.wantFailed > 0:
				var failover *UpstreamFailoverError
				require.ErrorAs(t, err, &failover)
				assert.Equal(t, http.StatusTooManyRequests, failover.StatusCode)
				assert.False(t, failover.RetryableOnSameAccount)
			default:
				require.NoError(t, err)
				assert.Equal(t, tc.body, recorder.Body.String())
			}
			assert.Empty(t, generationSlots, "token counting must not take generation capacity")
			assert.Nil(t, account.OpenAI429Attempt, "shared account snapshot must not retain a request permit")
			assert.Equal(t, []int64{account.ID}, cache.attempts)
			assert.Zero(t, cache.accepted, "counting tokens is not evidence that generation recovered")
			assert.Equal(t, tc.wantFailed, cache.failed)
			if tc.delay == 0 && tc.stateErr == nil {
				assert.Equal(t, 1, cache.released, "completed upstream request must release its recovery probe")
			}
		})
	}
}
