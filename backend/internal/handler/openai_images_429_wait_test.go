//go:build unit

package handler

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type imagesQueuedRecoveryCache struct {
	*inputTokensAdmissionCache
	failed []int64
}

func (c *imagesQueuedRecoveryCache) FailOpenAI429Attempt(_ context.Context, accountID int64, _, _, _, _ string, _ time.Duration) (bool, error) {
	c.failed = append(c.failed, accountID)
	return true, nil
}

type imagesQueuedRateLimitUpstream struct {
	service.HTTPUpstream
	calls int
}

func (u *imagesQueuedRateLimitUpstream) Do(*http.Request, string, int64, int) (*http.Response, error) {
	u.calls++
	return &http.Response{
		StatusCode: http.StatusTooManyRequests, Header: make(http.Header),
		Body: io.NopCloser(strings.NewReader(`{"error":{"type":"rate_limit_error","code":"rate_limit_exceeded","message":"Too many requests"}}`)),
	}, nil
}

func TestOpenAI429ImagesWaitPlanPreservesRecoveryAttempt(t *testing.T) {
	cache := &imagesQueuedRecoveryCache{inputTokensAdmissionCache: &inputTokensAdmissionCache{
		helperConcurrencyCacheStub: &helperConcurrencyCacheStub{accountSeq: []bool{false, true}},
	}}
	account := service.Account{
		ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Status: service.StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{"access_token": "access-token"},
	}
	upstream := &imagesQueuedRateLimitUpstream{}
	h := newRecoveryAdmissionTestHandler(t, cache, []service.Account{account}, upstream)
	c, recorder := newOpenAIResponsesFailoverTestContext(t, nil)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"gpt-image-2","prompt":"a blue square"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	require.True(t, ok)
	apiKey.Group.AllowImageGeneration = true

	h.Images(c)

	assert.Equal(t, http.StatusTooManyRequests, recorder.Code, recorder.Body.String())
	assert.Equal(t, 1, upstream.calls)
	assert.Equal(t, []int64{account.ID}, cache.attempts)
	assert.Equal(t, []int64{account.ID}, cache.failed, "429 after a WaitPlan must update shared recovery on the admitted account copy")
	assert.Equal(t, 2, cache.accountAcquireCalls)
	assert.Equal(t, 1, cache.accountReleaseCalls)
}
