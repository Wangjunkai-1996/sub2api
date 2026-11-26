package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIRetryBudgetDeadlineDoesNotCancelHardRequestContext(t *testing.T) {
	hardCtx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	ctx := WithOpenAIRetryBudgetDeadline(hardCtx, time.Now().Add(-time.Second))

	require.True(t, OpenAIRetryBudgetExpired(ctx))
	select {
	case <-ctx.Done():
		t.Fatal("retry budget must not cancel the hard request context")
	default:
	}
}

func TestOpenAIModelDispatchBudgetSharedAcrossDetachedContexts(t *testing.T) {
	ctx := WithOpenAIModelDispatchBudget(context.Background())
	require.Same(t, ctx, WithOpenAIModelDispatchBudget(ctx), "nested callers must not reset the budget")
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			detached, release := detachUpstreamContext(ctx)
			defer release()
			if takeOpenAIModelDispatch(detached, int64(id)) == nil {
				allowed.Add(1)
			}
		}(i)
	}
	wg.Wait()
	require.EqualValues(t, 11, allowed.Load())
	require.ErrorIs(t, takeOpenAIModelDispatch(ctx, 99), ErrOpenAIModelDispatchBudgetExhausted)
}

func TestOpenAIModelDispatchBudgetOriginalCancellationStopsDetachedDispatch(t *testing.T) {
	entryCtx, cancel := context.WithCancel(context.Background())
	ctx := WithOpenAIModelDispatchBudget(entryCtx)
	detached, release := detachUpstreamContext(ctx)
	defer release()
	require.NoError(t, takeOpenAIModelDispatch(detached, 1))
	cancel()
	require.NoError(t, detached.Err(), "the in-flight attempt may finish its accounting")
	err := takeOpenAIModelDispatch(detached, 2)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, IsOpenAIModelDispatchStop(err))
}

func TestOpenAIModelDispatchBudgetHonorsRetryAndHardDeadlines(t *testing.T) {
	ctx := WithOpenAIModelDispatchBudget(WithOpenAIRetryBudgetDeadline(context.Background(), time.Now().Add(-time.Second)))
	require.NoError(t, takeOpenAIModelDispatch(ctx, 1), "retry deadline must not forbid the first attempt")
	require.ErrorIs(t, takeOpenAIModelDispatch(ctx, 2), ErrOpenAIRetryBudgetExhausted)
	require.NoError(t, ctx.Err())
	hardCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	ctx = WithOpenAIModelDispatchBudget(hardCtx)
	require.ErrorIs(t, takeOpenAIModelDispatch(context.WithoutCancel(ctx), 1), context.DeadlineExceeded)
}

func TestOpenAIModelDispatchBudgetAbsentPreservesOtherEntrypoints(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 20; i++ {
		require.NoError(t, takeOpenAIModelDispatch(ctx, 1))
	}
}

type openAIModelDispatchCountingUpstream struct {
	HTTPUpstream
	calls             int
	redirectsDisabled bool
}

func (u *openAIModelDispatchCountingUpstream) Do(request *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	u.calls++
	u.redirectsDisabled = HTTPUpstreamRedirectsDisabled(request.Context())
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok"))}, nil
}

func TestOpenAIModelDispatchBudgetStopsTransportWithoutAccountFailure(t *testing.T) {
	ctx := WithOpenAIModelDispatchBudget(context.Background())
	upstream := &openAIModelDispatchCountingUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	request := httptest.NewRequest(http.MethodPost, "http://example.test/v1/responses", nil).WithContext(ctx)
	for i := 0; i < 11; i++ {
		resp, err := svc.doOpenAIUpstream(request, "", account)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
	}
	_, err := svc.doOpenAIUpstream(request, "", account)
	require.ErrorIs(t, err, ErrOpenAIModelDispatchBudgetExhausted)
	require.Equal(t, 11, upstream.calls)
	require.True(t, upstream.redirectsDisabled, "HTTP redirects must not replay model POSTs outside the budget")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	require.Same(t, err, svc.handleOpenAIUpstreamTransportError(ctx, c, account, err, false))
	require.Empty(t, c.Keys, "local budget exhaustion must not create an upstream error or account-health event")
}
