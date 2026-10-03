package service

import (
	"context"
	"sync"
	"sync/atomic"

	coderws "github.com/coder/websocket"
)

type openAIWS429RecoveryContextKey struct{}

type openAIWS429RecoveryTurn struct {
	number  int
	attempt *OpenAI429Attempt
	stop    func() bool
}

// Passthrough reads output and admits new turns on separate goroutines. Keep
// the published turn immutable, including its account recovery generation.
type openAIWS429Recovery struct {
	ctx         context.Context
	cancel      context.CancelCauseFunc
	concurrency *ConcurrencyService
	accountID   int64
	first       *OpenAI429Attempt
	mu          sync.Mutex
	current     atomic.Pointer[openAIWS429RecoveryTurn]
}

func openAI429AttemptFromContext(ctx context.Context, account *Account) *OpenAI429Attempt {
	if account == nil {
		return nil
	}
	if ctx != nil {
		if recovery, ok := ctx.Value(openAIWS429RecoveryContextKey{}).(*openAIWS429Recovery); ok && recovery.accountID == account.ID {
			if turn := recovery.current.Load(); turn != nil {
				return turn.attempt
			}
			return nil
		}
	}
	return account.OpenAI429Attempt
}

func openAI429ProbeContext(ctx context.Context, account *Account) (context.Context, func()) {
	probe := openAI429AttemptFromContext(ctx, account)
	if !probe.Probe() {
		return ctx, func() {}
	}
	upstreamCtx, cancel := context.WithCancelCause(ctx)
	probeCtx := probe.Context()
	cancelPendingProbe := func() {
		probe.lifecycleMu.Lock()
		defer probe.lifecycleMu.Unlock()
		if !probe.settled {
			cancel(context.Cause(probeCtx))
		}
	}
	stop := context.AfterFunc(probeCtx, cancelPendingProbe)
	if probeCtx.Err() != nil {
		cancelPendingProbe()
	}
	return upstreamCtx, func() {
		stop()
		cancel(context.Canceled)
	}
}

func (s *OpenAIGatewayService) withOpenAIWS429Recovery(ctx context.Context, account *Account, hooks *OpenAIWSIngressHooks) (context.Context, *OpenAIWSIngressHooks, func(), error) {
	if account == nil || !account.IsOpenAIOAuthLike() || account.IsShadow() ||
		(s.concurrencyService == nil && account.OpenAI429Attempt == nil) {
		return ctx, hooks, func() {}, nil
	}
	controlCtx, cancel := context.WithCancelCause(ctx)
	recovery := &openAIWS429Recovery{
		cancel: cancel, concurrency: s.concurrencyService,
		accountID: account.ID, first: account.OpenAI429Attempt,
	}
	ctx = context.WithValue(controlCtx, openAIWS429RecoveryContextKey{}, recovery)
	recovery.ctx = ctx
	cleanup := func() {
		recovery.finish(0)
		cancel(context.Canceled)
	}
	if err := recovery.begin(1); err != nil {
		cleanup()
		return ctx, hooks, func() {}, err
	}
	wrapped := &OpenAIWSIngressHooks{}
	if hooks != nil {
		*wrapped = *hooks
	}
	wrapped.BeforeTurn = func(turn int) error {
		if hooks != nil && hooks.BeforeTurn != nil {
			if err := hooks.BeforeTurn(turn); err != nil {
				return err
			}
		}
		return recovery.begin(turn)
	}
	wrapped.AfterTurn = func(turn int, result *OpenAIForwardResult, err error) {
		// Stop the cancellation callback before the handler releases its first
		// account slot, whose release also owns the first recovery attempt.
		recovery.finish(turn)
		if hooks != nil && hooks.AfterTurn != nil {
			hooks.AfterTurn(turn, result, err)
		}
	}
	return ctx, wrapped, cleanup, nil
}

func (r *openAIWS429Recovery) begin(number int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if current := r.current.Load(); current != nil && current.number == number {
		return nil
	}
	r.releaseLocked()
	attempt := r.first
	r.first = nil
	if attempt == nil {
		next, delay, err := r.concurrency.BeginOpenAI429Attempt(r.ctx, r.accountID, "")
		if err != nil {
			return NewOpenAIWSClientCloseError(coderws.StatusTryAgainLater, "account recovery is temporarily unavailable, please reconnect", err)
		}
		if next == nil {
			return NewOpenAIWSClientCloseError(coderws.StatusTryAgainLater, "account is cooling down, please retry later", &OpenAI429CooldownError{RetryAfter: delay})
		}
		attempt = next
	}
	turn := &openAIWS429RecoveryTurn{number: number, attempt: attempt}
	if attempt.Probe() {
		turn.stop = context.AfterFunc(attempt.Context(), func() {
			r.mu.Lock()
			defer r.mu.Unlock()
			attempt.lifecycleMu.Lock()
			defer attempt.lifecycleMu.Unlock()
			if r.current.Load() == turn && !attempt.settled {
				r.cancel(context.Cause(attempt.Context()))
			}
		})
	}
	r.current.Store(turn)
	if attempt.Probe() && attempt.Context().Err() != nil {
		attempt.lifecycleMu.Lock()
		if !attempt.settled {
			r.cancel(context.Cause(attempt.Context()))
		}
		attempt.lifecycleMu.Unlock()
	}
	return nil
}

func (r *openAIWS429Recovery) finish(number int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if turn := r.current.Load(); turn != nil && (number == 0 || turn.number == number) {
		r.releaseLocked()
	}
}

func (r *openAIWS429Recovery) releaseLocked() {
	if turn := r.current.Swap(nil); turn != nil {
		if turn.stop != nil {
			turn.stop()
		}
		turn.attempt.Release()
	}
}
