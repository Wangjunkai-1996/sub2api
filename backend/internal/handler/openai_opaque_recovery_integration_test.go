//go:build unit

package handler

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func newOpenAIManagedOpaqueTestHandler(t *testing.T, upstream service.HTTPUpstream, accounts []service.Account) (*OpenAIGatewayHandler, context.Context, service.OpenAIWSStateStore) {
	t.Helper()
	h := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	cache := repository.NewGatewayCache(client)
	h.gatewayService = service.NewOpenAIGatewayService(openAIImagesFailoverAccountRepo{accounts: accounts}, nil, nil, nil, nil, nil, cache, h.cfg, nil, nil, nil, nil, nil, upstream, nil, nil, nil, nil, nil, nil, nil, nil)
	ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(1))
	ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(2))
	ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "newapi")
	return h, ctx, service.NewOpenAIWSStateStore(cache)
}

func TestOpenAIManagedOpaqueResponses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, failure, stopReason      string
		failureAttempts                int
		previous, changeRoute, success bool
		wantAccounts                   []int64
	}{
		{name: "normal success", success: true, wantAccounts: []int64{1}},
		{name: "429 recovers on original account", failure: "before_429", failureAttempts: 1, success: true, wantAccounts: []int64{1, 1}},
		{name: "502 recovers on original account", failure: "before_502", failureAttempts: 1, success: true, wantAccounts: []int64{1, 1}},
		{name: "visible text recovers on original account", failure: "after_500", failureAttempts: 1, success: true, wantAccounts: []int64{1, 1}},
		{name: "retry budget never switches account", failure: "before_500", stopReason: "retry_budget_exhausted", wantAccounts: []int64{1, 1, 1}},
		{name: "invalid encrypted content preserved", failure: "invalid_encrypted_content", stopReason: "opaque_context_rejected", wantAccounts: []int64{1}},
		{name: "stream encrypted rejection stops recovery", failure: "after_invalid_encrypted_content", stopReason: "opaque_context_rejected", wantAccounts: []int64{1}},
		{name: "public tool output stops recovery", failure: "after_tool", stopReason: "tool_output_started", wantAccounts: []int64{1}},
		{name: "changed route cannot dispatch", failure: "before_500", changeRoute: true, stopReason: "route_unavailable", wantAccounts: []int64{1}},
		{name: "continuation without snapshot succeeds", previous: true, success: true, wantAccounts: []int64{1}},
		{name: "continuation without snapshot cannot rebuild", previous: true, failure: "before_500", stopReason: "opaque_context_rejected", wantAccounts: []int64{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accounts := []service.Account{
				{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 0, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
				{ID: 2, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 1, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
			}
			upstream := &openAIManagedResponsesUpstream{failure: tc.failure, failureAttempts: tc.failureAttempts, succeedImmediately: tc.failure == ""}
			if tc.changeRoute {
				upstream.afterDispatch = func() { accounts[0].Credentials["base_url"] = "https://changed.example.test" }
			}
			h, ctx, store := newOpenAIManagedOpaqueTestHandler(t, upstream, accounts)
			const input = `[{"type":"reasoning","id":"rs_original","encrypted_content":"opaque\u003d\u003d","summary":[]},{"role":"user","content":"hello"}]`
			previous := ""
			if tc.previous {
				require.NoError(t, store.BindHTTPResponseOwner(ctx, 3131, "resp_previous", 100, 99, time.Hour))
				require.NoError(t, store.BindResponseAccount(ctx, 3131, "resp_previous", 1, time.Hour))
				previous = `,"previous_response_id":"resp_previous"`
			}
			c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
			c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
			c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"input":` + input + previous + `}`))
			h.Responses(c)

			require.Equal(t, tc.wantAccounts, upstream.accounts, rec.Body.String())
			for _, body := range upstream.bodies {
				assert.Equal(t, input, gjson.GetBytes(body, "input").Raw, "opaque input must remain byte-for-byte intact")
				assert.Equal(t, tc.previous, gjson.GetBytes(body, "previous_response_id").Exists())
				if tc.previous {
					assert.Equal(t, "resp_previous", gjson.GetBytes(body, "previous_response_id").String())
				}
			}
			for _, body := range upstream.bodies[1:] {
				assert.Equal(t, upstream.bodies[0], body, "retry wire body must be unchanged")
			}
			diagnostics := rec.Header().Get(service.OpenAIRecoveryDiagnosticsHeader)
			assert.Equal(t, 1, strings.Count(rec.Body.String(), `"kind":"terminal"`)+len(rec.Header().Values(service.OpenAIRecoveryDiagnosticsHeader)))
			assert.Contains(t, rec.Body.String()+diagnostics, `"account_switches":0`)
			if tc.success {
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				assert.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"response.completed"`))
				assert.NotContains(t, rec.Body.String(), `"type":"response.failed"`)
				assert.Contains(t, rec.Body.String(), "winning-text")
				scope, scoped := service.OpenAIRecoveryScope(ctx, 99)
				require.True(t, scoped)
				var alias string
				for _, line := range strings.Split(rec.Body.String(), "\n") {
					event := strings.TrimPrefix(line, "data: ")
					if gjson.Get(event, "type").String() == "response.completed" {
						alias = gjson.Get(event, "response.id").String()
					}
				}
				winner, err := h.gatewayService.ResolveOpenAIRecoveryAlias(ctx, scope, alias)
				require.NoError(t, err)
				assert.Empty(t, winner.Snapshot, "opaque context must never enter a portable history snapshot")
				if len(tc.wantAccounts) > 1 {
					assert.Contains(t, rec.Body.String(), `"same_account_retries":1`)
				}
			} else {
				assert.Contains(t, rec.Body.String()+diagnostics, `"stop_reason":"`+tc.stopReason+`"`)
				assert.NotContains(t, rec.Body.String(), `"type":"response.completed"`)
			}
			if tc.failure == "invalid_encrypted_content" {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
				assert.Contains(t, rec.Body.String(), `"code":"invalid_encrypted_content"`)
				assert.Contains(t, rec.Body.String(), "Encrypted content could not be verified")
			}
			if tc.failure == "after_invalid_encrypted_content" {
				assert.Contains(t, rec.Body.String(), `"code":"invalid_encrypted_content"`)
				assert.NotContains(t, rec.Body.String(), "Regenerating")
			}
		})
	}
}

func TestOpenAIManagedOpaqueUnsentSetupCanFailOverWithoutPhysicalRetry(t *testing.T) {
	gin.SetMode(gin.TestMode)
	accounts := []service.Account{
		{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 0, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
		{ID: 2, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 1, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
	}
	upstream := &openAIManagedResponsesUpstream{failure: "not_sent_first_account", succeedImmediately: true}
	h, ctx, _ := newOpenAIManagedOpaqueTestHandler(t, upstream, accounts)
	c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
	c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
	const input = `[{"type":"reasoning","encrypted_content":"opaque"},{"role":"user","content":"hello"}]`
	c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"input":` + input + `}`))
	h.Responses(c)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotEmpty(t, upstream.accounts)
	assert.Equal(t, int64(1), upstream.accounts[0])
	assert.Equal(t, int64(2), upstream.accounts[len(upstream.accounts)-1])
	assert.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"response.completed"`))
	assert.Equal(t, 1, strings.Count(rec.Body.String(), `"kind":"terminal"`))
	assert.Contains(t, rec.Body.String(), `"same_account_retries":0`)
	assert.Contains(t, rec.Body.String(), `"account_switches":0`)
	assert.NotContains(t, rec.Body.String(), "Regenerating")
	for _, body := range upstream.bodies {
		assert.Equal(t, input, gjson.GetBytes(body, "input").Raw)
	}
}

func TestOpenAIManagedOpaqueContinuationPinsWinnerRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, changed := range []bool{false, true} {
		name := "same endpoint"
		if changed {
			name = "changed endpoint"
		}
		t.Run(name, func(t *testing.T) {
			accounts := []service.Account{{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}}}
			upstream := &openAIManagedResponsesUpstream{succeedImmediately: true}
			h, ctx, _ := newOpenAIManagedOpaqueTestHandler(t, upstream, accounts)
			c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
			c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
			c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"input":"hello"}`))
			h.Responses(c)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			var alias string
			for _, line := range strings.Split(rec.Body.String(), "\n") {
				event := strings.TrimPrefix(line, "data: ")
				if gjson.Get(event, "type").String() == "response.completed" {
					alias = gjson.Get(event, "response.id").String()
				}
			}
			require.NotEmpty(t, alias)
			scope, scoped := service.OpenAIRecoveryScope(ctx, 99)
			require.True(t, scoped)
			winner, err := h.gatewayService.ResolveOpenAIRecoveryAlias(ctx, scope, alias)
			require.NoError(t, err)
			require.NotEmpty(t, winner.RouteFingerprint)
			if changed {
				accounts[0].Credentials["base_url"] = "https://changed.example.test"
			}
			c, rec = newOpenAIResponsesFailoverTestContext(t, ctx)
			c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
			c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"previous_response_id":"` + alias + `","input":[{"type":"reasoning","encrypted_content":"opaque"},{"role":"user","content":"continue"}]}`))
			h.Responses(c)
			if changed {
				assert.Equal(t, []int64{1}, upstream.accounts)
				assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
				assert.Contains(t, rec.Header().Get(service.OpenAIRecoveryDiagnosticsHeader), `"stop_reason":"route_unavailable"`)
			} else {
				assert.Equal(t, []int64{1, 1}, upstream.accounts)
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				assert.Equal(t, "resp_actual", gjson.GetBytes(upstream.bodies[1], "previous_response_id").String())
				assert.Contains(t, rec.Body.String(), `"type":"response.completed"`)
			}
		})
	}
}

func TestOpenAIManagedOpaqueStreamsBeforeTerminal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	upstream := &recoveryToggleUpstream{release: release}
	accounts := []service.Account{{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}}}
	h, ctx, _ := newOpenAIManagedOpaqueTestHandler(t, upstream, accounts)
	c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
	c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
	c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"input":[{"type":"reasoning","encrypted_content":"opaque"},{"role":"user","content":"hello"}]}`))
	timing := &recoveryToggleTimingWriter{ResponseWriter: c.Writer, first: make(chan time.Time, 1)}
	c.Writer = timing
	done := make(chan struct{})
	go func() { h.Responses(c); close(done) }()
	select {
	case <-timing.first:
	case <-time.After(3 * time.Second):
		unblock()
		<-done
		require.FailNow(t, "first delta was buffered until terminal", rec.Body.String())
	}
	select {
	case <-done:
		require.FailNow(t, "handler completed before upstream terminal was released")
	default:
	}
	unblock()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		require.FailNow(t, "handler did not finish after terminal release")
	}
	assert.Equal(t, int32(1), upstream.calls.Load())
	assert.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"response.completed"`))
	assert.Equal(t, 1, strings.Count(rec.Body.String(), `"kind":"terminal"`))
}
