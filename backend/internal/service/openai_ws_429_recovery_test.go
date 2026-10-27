//go:build unit

package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWS429RecoveryResamplesEveryTurn(t *testing.T) {
	cache := &selectionRecoveryTestCache{probe: true}
	concurrency := NewConcurrencyService(cache)
	first, _, err := concurrency.BeginOpenAI429Attempt(context.Background(), 7201, "")
	require.NoError(t, err)
	account := &Account{ID: 7201, Platform: PlatformOpenAI, Type: AccountTypeOAuth, OpenAI429Attempt: first}
	s := &OpenAIGatewayService{concurrencyService: concurrency}
	var completed []int
	ctx, hooks, release, err := s.withOpenAIWS429Recovery(context.Background(), account, &OpenAIWSIngressHooks{
		AfterTurn: func(turn int, _ *OpenAIForwardResult, _ error) {
			completed = append(completed, turn)
			if turn == 1 {
				first.Release()
			}
		},
	})
	require.NoError(t, err)
	t.Cleanup(release)
	assert.Same(t, first, openAI429AttemptFromContext(ctx, account))
	require.NoError(t, hooks.BeforeTurn(1))
	assert.Len(t, cache.attempts, 1, "first selection already owns its generation")
	observeOpenAI429RecoveryOutput(ctx, account, []byte(`{"type":"response.created","response":{"id":"first"}}`), "")
	assert.False(t, first.settled)
	observeOpenAI429RecoveryOutput(ctx, account, []byte(`{"type":"response.output_text.delta","delta":"hello"}`), "")
	assert.True(t, first.settled, "meaningful output accepts the probe before the turn completes")
	assert.NoError(t, ctx.Err())
	hooks.AfterTurn(1, nil, nil)
	assert.NoError(t, ctx.Err(), "releasing a completed probe must not close the connection")
	require.NoError(t, hooks.BeforeTurn(2))
	second := openAI429AttemptFromContext(ctx, account)
	require.NotNil(t, second)
	assert.NotSame(t, first, second)
	assert.NotEqual(t, first.generation, second.generation)
	require.NoError(t, hooks.BeforeTurn(2))
	assert.Len(t, cache.attempts, 2, "repeated admission callbacks for one turn are idempotent")
	hooks.AfterTurn(1, nil, nil)
	assert.Same(t, second, openAI429AttemptFromContext(ctx, account), "a late terminal callback cannot release a later turn")
	assert.NoError(t, second.Context().Err())
	hooks.AfterTurn(2, nil, nil)
	assert.Nil(t, openAI429AttemptFromContext(ctx, account), "an idle connection cannot reuse the account's first generation")
	assert.Same(t, first, account.OpenAI429Attempt, "the account snapshot remains immutable")
	assert.Equal(t, []int{1, 1, 2}, completed)
	cache.deferred = map[int64]time.Duration{account.ID: 90 * time.Second}
	var cooldown *OpenAI429CooldownError
	require.ErrorAs(t, hooks.BeforeTurn(3), &cooldown)
	assert.Equal(t, 90*time.Second, cooldown.RetryAfter)
	assert.Nil(t, openAI429AttemptFromContext(ctx, account))
}

func TestOpenAIWS429RecoveryLeaseLossCancelsControlOnly(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache := &selectionRecoveryTestCache{probe: true}
	s := &OpenAIGatewayService{concurrencyService: NewConcurrencyService(cache)}
	account := &Account{ID: 7202, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	ctx, _, release, err := s.withOpenAIWS429Recovery(parent, account, nil)
	require.NoError(t, err)
	t.Cleanup(release)
	attempt := openAI429AttemptFromContext(ctx, account)
	require.NotNil(t, attempt)
	attempt.cancel(ErrOpenAI429ProbeLost)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("lost recovery probe did not cancel websocket upstream")
	}
	assert.ErrorIs(t, context.Cause(ctx), ErrOpenAI429ProbeLost)
	assert.NoError(t, parent.Err(), "the downstream lifetime remains available for a retryable close")
}

func TestOpenAIWS429ProbeContextRetainsCancellationAfterClientDetach(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cache := &selectionRecoveryTestCache{probe: true}
	attempt, _, err := NewConcurrencyService(cache).BeginOpenAI429Attempt(context.Background(), 7203, "")
	require.NoError(t, err)
	t.Cleanup(attempt.Release)
	account := &Account{ID: 7203, OpenAI429Attempt: attempt}
	cancel()
	upstream, release := openAI429ProbeContext(context.WithoutCancel(parent), account)
	t.Cleanup(release)
	assert.NoError(t, upstream.Err())
	attempt.cancel(ErrOpenAI429ProbeLost)
	select {
	case <-upstream.Done():
	case <-time.After(time.Second):
		t.Fatal("detached upstream ignored lost recovery probe")
	}
	assert.ErrorIs(t, context.Cause(upstream), ErrOpenAI429ProbeLost)
}

func TestOpenAIWS429ProbeContextRejectsAlreadyLostProbe(t *testing.T) {
	cache := &selectionRecoveryTestCache{probe: true}
	attempt, _, err := NewConcurrencyService(cache).BeginOpenAI429Attempt(context.Background(), 7204, "")
	require.NoError(t, err)
	t.Cleanup(attempt.Release)
	attempt.cancel(ErrOpenAI429ProbeLost)
	upstream, release := openAI429ProbeContext(context.Background(), &Account{ID: 7204, OpenAI429Attempt: attempt})
	t.Cleanup(release)
	assert.ErrorIs(t, context.Cause(upstream), ErrOpenAI429ProbeLost, "already lost probes are rejected before returning a transport context")
}

func TestOpenAIWS429AcceptedProbePreservesDetachedDrain(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cache := &selectionRecoveryTestCache{probe: true}
	attempt, _, err := NewConcurrencyService(cache).BeginOpenAI429Attempt(parent, 7205, "")
	require.NoError(t, err)
	t.Cleanup(attempt.Release)
	account := &Account{ID: 7205, OpenAI429Attempt: attempt}
	upstream, release := openAI429ProbeContext(context.WithoutCancel(parent), account)
	t.Cleanup(release)
	_, err = attempt.Accepted(parent)
	require.NoError(t, err)
	cancel()
	attempt.Release()
	assert.NoError(t, upstream.Err(), "client disconnection and slot release must preserve accepted usage draining")

	// A detached read may be created after the client and original probe end.
	lateDrain, releaseLateDrain := openAI429ProbeContext(context.WithoutCancel(parent), account)
	t.Cleanup(releaseLateDrain)
	assert.NoError(t, lateDrain.Err())
	release()
	assert.ErrorIs(t, upstream.Err(), context.Canceled, "closing the upstream body still releases its transport context")
}
