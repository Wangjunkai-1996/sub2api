package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

func TestOpenAIManagedRecoveryRequiresVerifiedAttribution(t *testing.T) {
	for _, tc := range []struct {
		name, version, origin string
		user, token           int64
		want                  bool
	}{
		{"verified", "1", "newapi", 1, 2, true},
		{"header alone", "1", "", 0, 0, false},
		{"missing token", "1", "newapi", 1, 0, false},
		{"unknown protocol", "2", "newapi", 1, 2, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, tc.user)
			ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, tc.token)
			ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, tc.origin)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
			c.Request.Header.Set(openAIRecoveryProtocolHeader, tc.version)
			assert.Equal(t, tc.want, openAIManagedRecoveryRequested(c))
			assert.Empty(t, c.Request.Header.Get(openAIRecoveryProtocolHeader))
		})
	}
}

func TestOpenAIManagedDispatchDeadlineDoesNotCancelHealthyAttempt(t *testing.T) {
	deadline := time.Now().Add(-time.Second)
	ctx := context.WithValue(context.Background(), openAIManagedDispatchDeadlineKey{}, deadline)
	ctx = service.WithOpenAIManagedRecovery(ctx)
	assert.False(t, openAIManagedCanDispatch(ctx))
	assert.NoError(t, ctx.Err())
	_, hasDeadline := ctx.Deadline()
	assert.False(t, hasDeadline)
}

func TestOpenAIManagedExclusionsExpireWithoutRemovingHardFailures(t *testing.T) {
	now := time.Now()
	rateLimit := now.Add(20 * time.Second)
	overload := now.Add(40 * time.Second)
	account := &service.Account{RateLimitResetAt: &rateLimit, OverloadUntil: &overload}
	failure := &service.UpstreamFailoverError{StatusCode: 429, ResponseHeaders: http.Header{"Retry-After": {"30"}}}
	assert.Equal(t, overload, openAIManagedAccountRetryAt(account, failure, now))
	excluded := map[int64]struct{}{1: {}, 2: {}, 3: {}}
	retryAt := map[int64]time.Time{1: now.Add(-time.Second), 2: now.Add(time.Second)}
	openAIManagedReleaseExpiredExclusions(excluded, retryAt, now)
	assert.Equal(t, map[int64]struct{}{2: {}, 3: {}}, excluded)
	assert.Equal(t, map[int64]time.Time{2: now.Add(time.Second)}, retryAt)
	assert.True(t, openAIManagedAccountRetryAt(nil, &service.UpstreamFailoverError{StatusCode: 401}, now).IsZero())
}

func TestOpenAIManagedRecoveryStopsBeforeDeliveryReserve(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	defer cancel()
	ctx = service.WithOpenAIManagedRecovery(ctx)
	assert.False(t, openAIManagedCanDispatch(ctx))
	assert.False(t, openAIManagedWaitForSelection(ctx, &service.OpenAI429CooldownError{RetryAfter: time.Second}, nil))
}

func TestOpenAIManagedSelectionDoesNotWaitOnInternalError(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancel()
	ctx = service.WithOpenAIManagedRecovery(ctx)
	assert.False(t, openAIManagedWaitForSelection(ctx, errors.New("scheduler database unavailable"), map[int64]time.Time{1: time.Now().Add(10 * time.Second)}))
}

func TestOpenAIManagedTextCapabilityGates(t *testing.T) {
	assert.True(t, openAIManagedResponsesTextSupported([]byte(`{"input":"hello"}`)))
	assert.True(t, openAIManagedResponsesTextSupported([]byte(`{"tools":[{"type":"function"}]}`)))
	assert.True(t, openAIManagedResponsesTextSupported([]byte(`{"include":["reasoning.encrypted_content"]}`)))
	assert.False(t, openAIManagedResponsesTextSupported([]byte(`{"text":{"format":{"type":"json_schema"}}}`)))
	assert.False(t, openAIManagedResponsesTextSupported([]byte(`{"response_format":{"type":"json_object"}}`)))
	assert.True(t, openAIManagedResponsesTextSupported([]byte(`{"input":[{"type":"reasoning","encrypted_content":"opaque"}]}`)))
	assert.True(t, openAIManagedResponsesOpaqueInput([]byte(`{"input":[{"type":"reasoning","encrypted_content":"opaque"}]}`)))
	assert.False(t, openAIManagedResponsesOpaqueInput([]byte(`{"include":["reasoning.encrypted_content"]}`)))
}

func TestOpenAIManagedContinuationRequiresWinnerRoute(t *testing.T) {
	winner := &service.OpenAIRecoveryAliasWinner{AccountID: 1, EgressID: "egress-1"}
	for _, tc := range []struct {
		name    string
		account *service.Account
		want    bool
	}{
		{"missing account", nil, false},
		{"same account and egress", &service.Account{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, SelectedEgress: &service.ResolvedAccountEgress{BindingID: "egress-1"}}, true},
		{"changed account", &service.Account{ID: 2, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, SelectedEgress: &service.ResolvedAccountEgress{BindingID: "egress-1"}}, false},
		{"changed egress", &service.Account{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, SelectedEgress: &service.ResolvedAccountEgress{BindingID: "egress-2"}}, false},
		{"missing egress", &service.Account{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey}, false},
		{"changed credential type", &service.Account{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, SelectedEgress: &service.ResolvedAccountEgress{BindingID: "egress-1"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, openAIManagedContinuationRouteMatches(tc.account, winner))
		})
	}
}
