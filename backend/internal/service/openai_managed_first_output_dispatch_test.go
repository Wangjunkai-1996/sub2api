package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAIManagedFirstOutputStartsAtEachPhysicalDispatch(t *testing.T) {
	logicalDeadline := time.Now().Add(900 * time.Second)
	logicalCtx, cancel := context.WithDeadline(context.Background(), logicalDeadline)
	defer cancel()
	ctx := WithOpenAIModelDispatchBudget(WithOpenAIManagedRecovery(logicalCtx))
	budget := ctx.Value(openAIModelDispatchBudgetKey{}).(*openAIModelDispatchBudget)
	staleForwardStart := time.Now().Add(-5 * time.Minute)
	for attempt := 1; attempt <= 2; attempt++ {
		// A prior physical dispatch does not start the next guard during request
		// preparation, even though both calls share their Forward start time.
		budget.dispatchStartedAt = staleForwardStart
		guardCtx, guard := newOpenAITextFirstOutputGuard(ctx, staleForwardStart.Add(120*time.Second), 120*time.Second)
		require.True(t, guard.deadline.IsZero())
		require.False(t, guard.timedOut())
		require.NoError(t, guardCtx.Err())
		require.NoError(t, takeOpenAIModelDispatch(guardCtx, 1))
		assert.Equal(t, attempt, budget.attempts)
		assert.Equal(t, budget.dispatchStartedAt.Add(120*time.Second), guard.deadline)
		assert.Equal(t, guard.deadline, openAIFirstOutputDeadline(ctx, staleForwardStart, 120*time.Second))
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		assert.Equal(t, logicalDeadline, deadline, "dispatch must not renew the logical budget")
		assert.True(t, guard.observeOutput())
		guard.close()
	}
}

func TestOpenAIManagedFirstOutputPreservesAttemptAndLogicalCaps(t *testing.T) {
	for _, firstAttempt := range []bool{false, true} {
		t.Run(map[bool]string{false: "logical_cap", true: "first_attempt_cap"}[firstAttempt], func(t *testing.T) {
			logicalDeadline := time.Now().Add(45 * time.Second)
			logicalCtx, cancel := context.WithDeadline(context.Background(), logicalDeadline)
			defer cancel()
			ctx := WithOpenAIModelDispatchBudget(WithOpenAIManagedRecovery(logicalCtx))
			expectedDeadline := logicalDeadline
			if firstAttempt {
				expectedDeadline = time.Now().Add(20 * time.Second)
				var cancelAttempt context.CancelFunc
				ctx, cancelAttempt = context.WithDeadline(ctx, expectedDeadline)
				defer cancelAttempt()
			}
			for attempt := 0; attempt < 2; attempt++ {
				guardCtx, textGuard := newOpenAITextFirstOutputGuard(ctx, time.Time{}, 120*time.Second)
				require.NoError(t, takeOpenAIModelDispatch(guardCtx, 1))
				assert.Equal(t, expectedDeadline, textGuard.deadline)
				textGuard.close()
				guardCtx, headerGuard := newOpenAIFirstOutputHeaderGuard(ctx, func() {}, time.Time{}, 120*time.Second)
				require.Nil(t, headerGuard.timer, "preparation must not consume the first-output interval")
				require.NoError(t, takeOpenAIModelDispatch(guardCtx, 1))
				assert.Equal(t, expectedDeadline, headerGuard.deadline)
				assert.False(t, headerGuard.stopHeaderWait())
				headerGuard.close()
			}
		})
	}
}

func TestOpenAIManagedFirstOutputCancellationBeforeDispatch(t *testing.T) {
	entry, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := WithOpenAIModelDispatchBudget(WithOpenAIManagedRecovery(entry))
	ctx = WithOpenAIModelDispatchObserver(ctx, func(int) error {
		cancel() // Cancellation while publishing an attempt marker forbids HTTP.
		return nil
	})
	ctx, guard := newOpenAITextFirstOutputGuard(ctx, time.Time{}, 120*time.Second)
	defer guard.close()
	upstream := &openAIModelDispatchCountingUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.test/v1/responses", nil)
	require.NoError(t, err)
	_, err = svc.doOpenAIUpstream(req, "", &Account{ID: 1, Platform: PlatformOpenAI})
	require.ErrorIs(t, err, context.Canceled)
	assert.True(t, IsOpenAIModelDispatchStop(err))
	assert.Zero(t, upstream.calls)
	assert.True(t, guard.deadline.IsZero())
	assert.Zero(t, ctx.Value(openAIModelDispatchBudgetKey{}).(*openAIModelDispatchBudget).attempts)
}

func TestOpenAIManagedFirstOutputLegacyDeadlineUnchanged(t *testing.T) {
	deadline := time.Now().Add(time.Minute)
	ctx, guard := newOpenAITextFirstOutputGuard(context.Background(), deadline, 120*time.Second)
	defer guard.close()
	require.NoError(t, takeOpenAIModelDispatch(ctx, 1))
	assert.Equal(t, deadline, guard.deadline)
	assert.Equal(t, deadline, openAIFirstOutputDeadline(ctx, deadline.Add(-120*time.Second), 120*time.Second))
}

func TestOpenAIExplicitRefusalIsUsableFirstOutput(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
	}{
		{"refusal delta", `{"type":"response.refusal.delta","delta":"Cannot help"}`},
		{"refusal done", `{"type":"response.refusal.done","refusal":"Cannot help"}`},
		{"refusal content part", `{"type":"response.content_part.done","part":{"type":"refusal","refusal":"Cannot help"}}`},
		{"refusal output item", `{"type":"response.output_item.done","item":{"type":"message","content":[{"type":"refusal","refusal":"Cannot help"}]}}`},
		{"refusal terminal", `{"type":"response.completed","response":{"output":[{"type":"message","content":[{"type":"refusal","refusal":"Cannot help"}]}]}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.True(t, openAIStreamDataStartsVisibleOutput(tc.payload, ""))
		})
	}
	for _, payload := range []string{
		`{"type":"response.content_part.added","part":{"type":"refusal","refusal":""}}`,
		`{"type":"response.content_part.done","part":{"type":"refusal","refusal":null}}`,
		`{"type":"response.content_part.done","part":{"type":"refusal","refusal":123}}`,
	} {
		assert.False(t, openAIStreamDataStartsVisibleOutput(payload, ""), payload)
	}
	refusal := "Cannot help"
	chunk := apicompat.ChatCompletionsChunk{Choices: []apicompat.ChatChunkChoice{{Delta: apicompat.ChatDelta{Refusal: &refusal}}}}
	assert.True(t, openAIChatChunkStartsUsableOutput(chunk))
	assert.True(t, openAIRawChatChunkStartsUsableOutput(`{"choices":[{"delta":{"refusal":"Cannot help"}}]}`))
}

func TestOpenAIManagedFirstOutputNativeReaderUsesCurrentDispatch(t *testing.T) {
	ctx := WithOpenAIModelDispatchBudget(WithOpenAIManagedRecovery(context.Background()))
	require.NoError(t, takeOpenAIModelDispatch(ctx, 1))
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
	stream := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"fresh\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_fresh\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
	resp := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(stream))}
	svc := &OpenAIGatewayService{cfg: &config.Config{}}
	result, err := svc.handleStreamingResponseWithReasoning(ctx, resp, c, &Account{ID: 1, Platform: PlatformOpenAI}, time.Now().Add(-5*time.Minute), "model", "model", "high", false)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Contains(t, recorder.Body.String(), "fresh")
	assert.Contains(t, recorder.Body.String(), "response.completed")
}

func TestOpenAIManagedFirstOutputCCFallbackGuardsStreamingAndJSON(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, expired := range []bool{false, true} {
			name := map[bool]string{false: "json", true: "stream"}[stream] + map[bool]string{false: "_output", true: "_timeout"}[expired]
			t.Run(name, func(t *testing.T) {
				payload := `{"id":"chat_1","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`
				if stream {
					payload = "data: {\"id\":\"chat_1\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\ndata: [DONE]\n\n"
				}
				upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(payload))}}
				svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
				ctx := WithOpenAIModelDispatchBudget(WithOpenAIManagedRecovery(context.Background()))
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
				account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
				resp, err := svc.sendCCUpstreamRequest(ctx, c, account, "https://example.test/v1/chat/completions", []byte(`{"model":"gpt-6.1-sol"}`), stream, "test-key", "", "")
				require.NoError(t, err)
				defer resp.Body.Close()
				body, ok := resp.Body.(*openAIRequestContextReadCloser)
				require.True(t, ok)
				require.NotNil(t, body.firstOutputGuard)
				require.False(t, body.firstOutputGuard.deadline.IsZero())
				if expired {
					body.firstOutputGuard.expire()
				}
				if stream {
					state := svc.scanCCStream(c, resp, account, "test", "", time.Now(), func(*apicompat.ChatCompletionsChunk) {})
					err = state.Err
				} else {
					_, _, err = svc.readCCUpstreamJSONResponse(c, resp, writeOpenAIResponsesFallbackError)
				}
				if expired {
					var failover *UpstreamFailoverError
					require.ErrorAs(t, err, &failover)
					assert.True(t, failover.FirstOutputTimeout)
					assert.False(t, c.Writer.Written(), "timeout must reach recovery without a public terminal error")
				} else {
					require.NoError(t, err)
					assert.True(t, body.firstOutputGuard.observed)
				}
			})
		}
	}
}
