package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
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

const maxOpenAIModelDispatches = 11

var (
	ErrOpenAIModelDispatchBudgetExhausted = errors.New("openai model dispatch budget exhausted")
	errOpenAIModelDispatchCanceled        = errors.New("openai model dispatch canceled")
)

type openAIModelDispatchBudgetKey struct{}

type openAIModelDispatchBudget struct {
	mu       sync.Mutex
	entryCtx context.Context
	attempts int
}

// WithOpenAIModelDispatchBudget bounds model dispatches across account changes
// and compatibility repairs. Keep the entry context: detached upstream contexts
// may finish billing, but must not start another model call after cancellation.
func WithOpenAIModelDispatchBudget(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if HasOpenAIModelDispatchBudget(ctx) {
		return ctx
	}
	return context.WithValue(ctx, openAIModelDispatchBudgetKey{}, &openAIModelDispatchBudget{entryCtx: ctx})
}

func HasOpenAIModelDispatchBudget(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	budget, _ := ctx.Value(openAIModelDispatchBudgetKey{}).(*openAIModelDispatchBudget)
	return budget != nil
}

// IsOpenAIModelDispatchStop identifies local control decisions, which must pass
// through transport handlers without affecting account health or failover.
func IsOpenAIModelDispatchStop(err error) bool {
	return errors.Is(err, ErrOpenAIModelDispatchBudgetExhausted) ||
		errors.Is(err, ErrOpenAIRetryBudgetExhausted) ||
		errors.Is(err, errOpenAIModelDispatchCanceled)
}

func takeOpenAIModelDispatch(ctx context.Context, accountID int64) error {
	if ctx == nil {
		return nil
	}
	budget, _ := ctx.Value(openAIModelDispatchBudgetKey{}).(*openAIModelDispatchBudget)
	if budget == nil {
		return nil
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if err := budget.entryCtx.Err(); err != nil {
		return fmt.Errorf("%w: %w", errOpenAIModelDispatchCanceled, err)
	}
	if budget.attempts > 0 && OpenAIRetryBudgetExpired(budget.entryCtx) {
		return ErrOpenAIRetryBudgetExhausted
	}
	if budget.attempts >= maxOpenAIModelDispatches {
		return ErrOpenAIModelDispatchBudgetExhausted
	}
	budget.attempts++
	fields := []zap.Field{
		zap.Int64("account_id", accountID),
		zap.Int("attempt", budget.attempts),
		zap.Int("remaining_dispatches", maxOpenAIModelDispatches-budget.attempts),
	}
	if deadline, ok := budget.entryCtx.Deadline(); ok {
		fields = append(fields, zap.Int64("request_budget_remaining_ms", time.Until(deadline).Milliseconds()))
	}
	if deadline, ok := budget.entryCtx.Value(openAIRetryBudgetDeadlineKey{}).(time.Time); ok {
		fields = append(fields, zap.Int64("retry_budget_remaining_ms", time.Until(deadline).Milliseconds()))
	}
	logger.FromContext(ctx).Info("openai.model_dispatch", fields...)
	return nil
}
