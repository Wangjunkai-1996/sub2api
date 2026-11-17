package service

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

const (
	openAI429ProbeTTL     = 30 * time.Second
	openAI429ProbeRefresh = 10 * time.Second
	openAI429CacheTimeout = 2 * time.Second
)

var ErrOpenAI429RecoveryUnavailable = errors.New("openai rate limit recovery state unavailable")
var ErrOpenAI429ProbeLost = errors.New("openai rate limit recovery probe lease lost")

// OpenAI429Admission fences all attempts, including requests admitted before
// the first 429. Only the matching recovery probe may advance a later round.
type OpenAI429Admission struct {
	Allowed    bool
	Generation string
	Probe      bool
	RetryAfter time.Duration
}

// OpenAI429RecoveryCache is implemented by the existing shared concurrency
// cache. A model of "" denotes account-wide recovery; nonempty models isolate
// state to one account and model.
type OpenAI429RecoveryCache interface {
	AcquireOpenAI429Attempt(context.Context, int64, string, string, time.Duration) (OpenAI429Admission, error)
	FailOpenAI429Attempt(context.Context, int64, string, string, string, string, time.Duration) (bool, error)
	AcceptOpenAI429Attempt(context.Context, int64, string, string, string, string) (bool, error)
	RefreshOpenAI429Probe(context.Context, int64, string, string, string, time.Duration) (bool, error)
	ReleaseOpenAI429Probe(context.Context, int64, string, string, string) (bool, error)
}

type OpenAI429Attempt struct {
	cache       OpenAI429RecoveryCache
	accountID   int64
	model       string
	generation  string
	token       string
	probe       bool
	ctx         context.Context
	cancel      context.CancelCauseFunc
	done        chan struct{}
	lifecycleMu sync.Mutex
	settled     bool
	stopOnce    sync.Once
	releaseOnce sync.Once
}

// BeginOpenAI429Attempt does not acquire an upstream concurrency slot. Call it
// before forwarding, then release it together with the selected account slot.
// A nil attempt with no error means admission is deferred for RetryAfter.
func (s *ConcurrencyService) BeginOpenAI429Attempt(ctx context.Context, accountID int64, model string) (*OpenAI429Attempt, time.Duration, error) {
	if s == nil || accountID <= 0 {
		return nil, 0, ErrOpenAI429RecoveryUnavailable
	}
	cache, ok := s.cache.(OpenAI429RecoveryCache)
	if !ok {
		return nil, 0, ErrOpenAI429RecoveryUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	token := generateRequestID()
	operationCtx, cancel := context.WithTimeout(ctx, openAI429CacheTimeout)
	admission, err := cache.AcquireOpenAI429Attempt(operationCtx, accountID, model, token, openAI429ProbeTTL)
	cancel()
	if err != nil {
		return nil, 0, errors.Join(ErrOpenAI429RecoveryUnavailable, err)
	}
	if !admission.Allowed {
		return nil, admission.RetryAfter, nil
	}
	attempt := &OpenAI429Attempt{
		cache: cache, accountID: accountID, model: model,
		generation: admission.Generation, token: token, probe: admission.Probe,
		ctx: ctx, done: make(chan struct{}),
	}
	if attempt.probe {
		attempt.ctx, attempt.cancel = context.WithCancelCause(ctx)
		go attempt.renewProbe()
	}
	return attempt, 0, nil
}

func (a *OpenAI429Attempt) Context() context.Context { return a.ctx }
func (a *OpenAI429Attempt) Probe() bool              { return a != nil && a.probe }

// RateLimited records a short transient 429. Known quota/reset and model-only
// failures are classified by the caller before reaching this method.
func (a *OpenAI429Attempt) RateLimited(ctx context.Context, retryAfter time.Duration) (bool, error) {
	if a == nil {
		return false, nil
	}
	a.lifecycleMu.Lock()
	if a.settled {
		a.lifecycleMu.Unlock()
		return false, nil
	}
	a.settled = true
	a.lifecycleMu.Unlock()
	operationCtx, cancel := openAI429StateContext(ctx)
	defer cancel()
	applied, err := a.cache.FailOpenAI429Attempt(operationCtx, a.accountID, a.model, a.generation, a.token, generateRequestID(), retryAfter)
	if err == nil {
		a.stopRenewing()
	}
	return applied, err
}

// Accepted is called on genuine upstream acceptance or meaningful generation,
// never solely on an HTTP 200 SSE header. It stops renewal without canceling
// the ongoing response, which may continue generating for many minutes.
func (a *OpenAI429Attempt) Accepted(ctx context.Context) (bool, error) {
	if a == nil || !a.probe {
		return false, nil
	}
	// The caller has already observed valid generation. A concurrent failed
	// refresh must no longer cancel it, even if persisting recovery fails.
	a.lifecycleMu.Lock()
	if a.settled {
		a.lifecycleMu.Unlock()
		return false, nil
	}
	a.settled = true
	a.stopRenewing()
	a.lifecycleMu.Unlock()
	operationCtx, cancel := openAI429StateContext(ctx)
	defer cancel()
	applied, err := a.cache.AcceptOpenAI429Attempt(operationCtx, a.accountID, a.model, a.generation, a.token, generateRequestID())
	return applied, err
}

func (a *OpenAI429Attempt) Release() {
	if a == nil || !a.probe {
		return
	}
	a.releaseOnce.Do(func() {
		a.stopRenewing()
		operationCtx, cancel := openAI429StateContext(a.ctx)
		defer cancel()
		if _, err := a.cache.ReleaseOpenAI429Probe(operationCtx, a.accountID, a.model, a.generation, a.token); err != nil {
			slog.Warn("openai_429_probe_release_failed", "account_id", a.accountID, "error", err)
		}
		a.cancel(context.Canceled)
	})
}

func (a *OpenAI429Attempt) stopRenewing() {
	a.stopOnce.Do(func() { close(a.done) })
}

func (a *OpenAI429Attempt) renewProbe() {
	ticker := time.NewTicker(openAI429ProbeRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-a.done:
			return
		case <-a.ctx.Done():
			a.Release()
			return
		case <-ticker.C:
			operationCtx, cancel := context.WithTimeout(a.ctx, openAI429CacheTimeout)
			owned, err := a.cache.RefreshOpenAI429Probe(operationCtx, a.accountID, a.model, a.generation, a.token, openAI429ProbeTTL)
			cancel()
			if err == nil && owned {
				continue
			}
			a.lifecycleMu.Lock()
			select {
			case <-a.done:
				a.lifecycleMu.Unlock()
				return
			default:
			}
			a.cancel(ErrOpenAI429ProbeLost)
			a.lifecycleMu.Unlock()
			a.Release()
			return
		}
	}
}

func openAI429StateContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(context.WithoutCancel(ctx), openAI429CacheTimeout)
}
