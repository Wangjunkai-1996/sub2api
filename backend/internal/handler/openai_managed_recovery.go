package handler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const openAIRecoveryProtocolHeader = "X-KKAI-Recovery-Protocol"

const (
	openAIManagedDeliveryReserve = 15 * time.Second
)

type openAIManagedDispatchDeadlineKey struct{}

// Only signed NewAPI attribution can activate the internal recovery protocol.
func openAIManagedRecoveryRequested(c *gin.Context) bool {
	if c == nil || c.Request == nil {
		return false
	}
	version := strings.TrimSpace(c.Request.Header.Get(openAIRecoveryProtocolHeader))
	c.Request.Header.Del(openAIRecoveryProtocolHeader)
	if version != "1" {
		return false
	}
	ctx := c.Request.Context()
	userID, _ := ctx.Value(ctxkey.NewAPIUserID).(int64)
	tokenID, _ := ctx.Value(ctxkey.NewAPITokenID).(int64)
	origin, _ := ctx.Value(ctxkey.NewAPIOrigin).(string)
	return userID > 0 && tokenID > 0 && strings.TrimSpace(origin) != ""
}

func openAIManagedResponsesTextSupported(body []byte) bool {
	if service.OpenAIRecoveryStructuredFormat(body, service.OpenAIRecoveryResponsesSSE) != "" {
		return false
	}
	for _, item := range gjson.GetBytes(body, "input").Array() {
		if item.Get("encrypted_content").Exists() {
			return false
		}
	}
	return true
}

func openAIManagedContinuationRouteMatches(account *service.Account, winner *service.OpenAIRecoveryAliasWinner) bool {
	if account == nil || winner == nil || account.ID != winner.AccountID || !account.IsOpenAIApiKey() {
		return false
	}
	egressID := ""
	if account.SelectedEgress != nil {
		egressID = account.SelectedEgress.BindingID
	}
	return egressID == winner.EgressID
}

func openAIManagedSameAccountRetryLimit(ctx context.Context, failoverErr *service.UpstreamFailoverError, account *service.Account) int {
	if !service.IsOpenAIManagedRecovery(ctx) {
		return effectiveSameAccountRetryLimit(failoverErr, account)
	}
	if account == nil || !account.IsActive() {
		return 0
	}
	limit := 2
	if failoverErr != nil && failoverErr.SameAccountRetryMax > 0 && failoverErr.SameAccountRetryMax < limit {
		limit = failoverErr.SameAccountRetryMax
	}
	return limit
}

func openAIManagedRetryableFailure(ctx context.Context, failure *service.UpstreamFailoverError) *service.UpstreamFailoverError {
	if !service.IsOpenAIManagedRecovery(ctx) || failure == nil || !failure.ShouldRetryNextAccount() || failure.IsCredentialFailure() || failure.IsOpenAISessionBlocked() {
		return failure
	}
	if failure.StatusCode != http.StatusTooManyRequests && failure.StatusCode < 500 {
		return failure
	}
	copy := *failure
	copy.RetryableOnSameAccount = true
	return &copy
}

func openAIManagedCanDispatch(ctx context.Context) bool {
	if !service.IsOpenAIManagedRecovery(ctx) || ctx == nil {
		return true
	}
	deadline, ok := openAIManagedDispatchDeadline(ctx)
	return !ok || time.Until(deadline) > openAIManagedDeliveryReserve
}

func openAIManagedDispatchDeadline(ctx context.Context) (time.Time, bool) {
	if ctx == nil {
		return time.Time{}, false
	}
	if deadline, ok := ctx.Deadline(); ok {
		return deadline, true
	}
	deadline, ok := ctx.Value(openAIManagedDispatchDeadlineKey{}).(time.Time)
	return deadline, ok
}

func openAIManagedAccountRetryAt(account *service.Account, failure *service.UpstreamFailoverError, now time.Time) time.Time {
	if failure == nil || (!failure.RetryableOnSameAccount && !failure.RequestScopedTransient && failure.StatusCode != http.StatusTooManyRequests && failure.StatusCode < 500) {
		return time.Time{}
	}
	eligibleAt := now.Add(sameAccountRetryDelayFor(failure, 1))
	if account != nil {
		for _, deadline := range []*time.Time{account.RateLimitResetAt, account.OverloadUntil, account.TempUnschedulableUntil} {
			if deadline != nil && deadline.After(eligibleAt) {
				eligibleAt = *deadline
			}
		}
	}
	if retryAfter := strings.TrimSpace(failure.ResponseHeaders.Get("Retry-After")); retryAfter != "" {
		if seconds, err := strconv.ParseInt(retryAfter, 10, 32); err == nil && seconds > 0 {
			if deadline := now.Add(time.Duration(seconds) * time.Second); deadline.After(eligibleAt) {
				eligibleAt = deadline
			}
		} else if deadline, err := http.ParseTime(retryAfter); err == nil && deadline.After(eligibleAt) {
			eligibleAt = deadline
		}
	}
	return eligibleAt
}

func openAIManagedReleaseExpiredExclusions(excluded map[int64]struct{}, retryAt map[int64]time.Time, now time.Time) {
	for id, deadline := range retryAt {
		if !deadline.After(now) {
			delete(excluded, id)
			delete(retryAt, id)
		}
	}
}

func openAIManagedWaitForSelection(ctx context.Context, err error, retryAt map[int64]time.Time) bool {
	if !service.IsOpenAIManagedRecovery(ctx) || ctx.Err() != nil {
		return false
	}
	var cooldown *service.OpenAI429CooldownError
	if !errors.Is(err, service.ErrNoAvailableAccounts) && !errors.Is(err, service.ErrAccountEgressCapacityFull) && !errors.As(err, &cooldown) {
		return false
	}
	var earliest time.Time
	for _, deadline := range retryAt {
		if earliest.IsZero() || deadline.Before(earliest) {
			earliest = deadline
		}
	}
	if cooldown != nil && cooldown.RetryAfter > 0 {
		if deadline := time.Now().Add(cooldown.RetryAfter); earliest.IsZero() || deadline.Before(earliest) {
			earliest = deadline
		}
	}
	deadline, bounded := openAIManagedDispatchDeadline(ctx)
	if earliest.IsZero() || !bounded || !earliest.Before(deadline.Add(-openAIManagedDeliveryReserve)) {
		return false
	}
	return sleepWithContext(ctx, max(time.Until(earliest), time.Millisecond))
}
