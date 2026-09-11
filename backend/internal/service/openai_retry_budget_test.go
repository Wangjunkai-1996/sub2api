package service

import (
	"context"
	"testing"
	"time"

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
