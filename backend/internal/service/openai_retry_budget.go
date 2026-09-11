package service

import (
	"context"
	"errors"
	"time"
)

type openAIRetryBudgetDeadlineKey struct{}

// WithOpenAIRetryBudgetDeadline carries the request-scoped deadline that limits
// starting additional upstream retries or account failovers. It does not cancel
// the request; an attempt already in flight remains governed by the hard request
// deadline in its context.
func WithOpenAIRetryBudgetDeadline(ctx context.Context, deadline time.Time) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, openAIRetryBudgetDeadlineKey{}, deadline)
}

// OpenAIRetryBudgetExpired reports whether the request may start another retry.
func OpenAIRetryBudgetExpired(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	deadline, ok := ctx.Value(openAIRetryBudgetDeadlineKey{}).(time.Time)
	return ok && !deadline.IsZero() && !time.Now().Before(deadline)
}

var ErrOpenAIRetryBudgetExhausted = errors.New("openai retry budget exhausted")
