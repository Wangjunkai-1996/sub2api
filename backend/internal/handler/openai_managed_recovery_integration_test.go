//go:build unit

package handler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestOpenAIManagedClientCancellationKeepsIncompleteDiagnostics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, route := range []string{"responses", "chat/completions"} {
		t.Run(route, func(t *testing.T) {
			core, observed := observer.New(zap.WarnLevel)
			ctx, cancel := context.WithCancel(logger.IntoContext(context.Background(), zap.New(core)))
			defer cancel()
			ctx = context.WithValue(ctx, ctxkey.NewAPIUserID, int64(1))
			ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(2))
			ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "newapi")
			upstream := &openAIResponsesFailoverCancelUpstream{onFirstDo: cancel}
			h := newOpenAIResponsesFailoverTestHandler(t, upstream)
			c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
			c.Request.URL.Path = "/v1/" + route
			c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
			c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"input":"hello","messages":[{"role":"user","content":"hello"}]}`))
			if route == "responses" {
				h.Responses(c)
			} else {
				h.ChatCompletions(c)
			}
			assert.Equal(t, []int64{1}, upstream.calls(), "client cancellation must not dispatch another account")
			assert.Empty(t, rec.Body.String(), "a canceled response must not manufacture a failed terminal")
			logs := observed.FilterMessage("openai.recovery_incomplete").All()
			require.Len(t, logs, 1)
			fields := logs[0].ContextMap()
			assert.Equal(t, "client_disconnected", fields["stop_reason"])
			assert.Equal(t, int64(0), fields["same_account_retries"])
			assert.Equal(t, int64(0), fields["account_switches"])
			assert.Equal(t, false, fields["public_tool_output_started"])
		})
	}
}

func TestOpenAIManagedChatRecoversAfterVisibleText(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &openAICompatFailoverUpstream{failure: "after_text"}
	h := newOpenAIResponsesFailoverTestHandler(t, upstream)
	ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(1))
	ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(2))
	ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "newapi")
	c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
	c.Request.URL.Path = "/v1/chat/completions"
	c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
	c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"max_tokens":32,"messages":[{"role":"user","content":"hello"}]}`))
	h.ChatCompletions(c)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, []int64{1, 1, 1, 2}, upstream.accountIDs, rec.Body.String())
	assert.Equal(t, "1", rec.Header().Get(openAIRecoveryProtocolHeader))
	assert.Contains(t, rec.Body.String(), "partial-text")
	assert.Contains(t, rec.Body.String(), "Regenerating")
	assert.Contains(t, rec.Body.String(), "winning-text")
	assert.Equal(t, 1, strings.Count(rec.Body.String(), "data: [DONE]"))
	assert.Equal(t, 1, strings.Count(rec.Body.String(), `"kind":"terminal"`))
	assert.NotContains(t, rec.Body.String(), `"outcome":"failed"`)
	assert.Contains(t, rec.Body.String(), `"same_account_retries":2`)
	assert.Contains(t, rec.Body.String(), `"account_switches":1`)
}

func TestOpenAIManagedChatStopsAfterPublicToolOutput(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &openAICompatFailoverUpstream{failure: "after_tool"}
	h := newOpenAIResponsesFailoverTestHandler(t, upstream)
	ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(1))
	ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(2))
	ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "newapi")
	c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
	c.Request.URL.Path = "/v1/chat/completions"
	c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
	c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"messages":[{"role":"user","content":"hello"}],"tools":[{"type":"function","function":{"name":"test_tool","parameters":{"type":"object"}}}]}`))
	h.ChatCompletions(c)

	assert.Equal(t, []int64{1}, upstream.accountIDs, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "tool_calls")
	assert.Equal(t, 1, strings.Count(rec.Body.String(), `"kind":"terminal"`))
	assert.Contains(t, rec.Body.String(), `"outcome":"failed"`)
	assert.Contains(t, rec.Body.String(), `"stop_reason":"tool_output_started"`)
	assert.Contains(t, rec.Body.String(), `"same_account_retries":0`)
	assert.Contains(t, rec.Body.String(), `"account_switches":0`)
	assert.NotContains(t, rec.Body.String(), "Regenerating")
}

func TestOpenAIManagedExplicitJSONRejectsBeforeDispatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, path, body string
	}{
		{"chat", "/v1/chat/completions", `{"model":"gpt-5.1","stream":true,"response_format":{"type":"json_object"},"messages":[{"role":"user","content":"hello"}]}`},
		{"responses", "/v1/responses", `{"model":"gpt-5.1","stream":true,"text":{"format":{"type":"json_schema","name":"result","schema":{"type":"object"}}},"input":"hello"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := &openAICompatFailoverUpstream{}
			h := newOpenAIResponsesFailoverTestHandler(t, upstream)
			ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(1))
			ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(2))
			ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "newapi")
			c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
			c.Request.URL.Path = tc.path
			c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
			c.Request.Body = io.NopCloser(strings.NewReader(tc.body))
			if tc.path == "/v1/responses" {
				h.Responses(c)
			} else {
				h.ChatCompletions(c)
			}
			assert.Equal(t, http.StatusUnprocessableEntity, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "recovery_capability_unavailable")
			assert.Empty(t, upstream.accountIDs)
		})
	}
}

func TestOpenAIManagedChatStopsAtSharedDispatchLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &openAIResponsesDispatchBudgetUpstream{}
	h := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, newOpenAIResponsesDispatchBudgetAccounts(12))
	ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(1))
	ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(2))
	ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "newapi")
	c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
	c.Request.URL.Path = "/v1/chat/completions"
	c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
	c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"messages":[{"role":"user","content":"hello"}]}`))
	h.ChatCompletions(c)
	require.NoError(t, upstream.readErr)
	assert.Len(t, upstream.calls, 11, rec.Body.String())
	assert.Equal(t, http.StatusBadGateway, rec.Code)
	assert.Equal(t, "exhausted", rec.Header().Get("X-Sub2-Retry-Status"))
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.Contains(t, rec.Body.String(), "model attempt limit")
	rawEvents, exists := c.Get(service.OpsUpstreamErrorsKey)
	require.True(t, exists)
	events, ok := rawEvents.([]*service.OpsUpstreamErrorEvent)
	require.True(t, ok)
	require.NotEmpty(t, events)
	assert.Equal(t, "dispatch_limit", events[len(events)-1].FinalOutcome)
}

type openAIManagedResponsesUpstream struct {
	service.HTTPUpstream
	accounts           []int64
	bodies             [][]byte
	sessionBlocked     bool
	failure            string
	phaseFinalAnswer   bool
	reasoningOutput    bool
	physicalFailure    bool
	succeedImmediately bool
}

func (u *openAIManagedResponsesUpstream) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	u.accounts = append(u.accounts, accountID)
	u.bodies = append(u.bodies, body)
	if u.physicalFailure && gjson.GetBytes(body, "previous_response_id").Exists() {
		return &http.Response{StatusCode: http.StatusInternalServerError, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"type":"api_error","code":"server_error","message":"temporarily unavailable"}}`))}, nil
	}
	if len(u.accounts) <= 3 && (u.failure == "before_429" || u.failure == "before_500") {
		status, code := http.StatusTooManyRequests, "rate_limit_exceeded"
		if u.failure == "before_500" {
			status, code = http.StatusInternalServerError, "server_error"
		}
		payload := fmt.Sprintf(`{"error":{"type":"api_error","code":%q,"message":"temporarily unavailable"}}`, code)
		return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(payload))}, nil
	}
	if u.sessionBlocked {
		return &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"cyber_policy","message":"This session is blocked by the cyber-security policy. Please start a new session."}}`))}, nil
	}
	response := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}}
	const prefix = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_actual\",\"model\":\"gpt-5.1\",\"status\":\"in_progress\"}}\n\n" +
		"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_actual\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n" +
		"data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"item_id\":\"msg_actual\",\"part\":{\"type\":\"output_text\",\"text\":\"\",\"annotations\":[]}}\n\n"
	if u.failure == "after_tool" {
		response.Body = io.NopCloser(io.MultiReader(strings.NewReader(prefix+
			"data: {\"type\":\"response.output_item.added\",\"output_index\":1,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"test_tool\",\"arguments\":\"{}\"}}\n\n"), openAICompatFailoverReadError{}))
		return response, nil
	}
	if len(u.accounts) <= 3 && (u.failure == "after_429" || u.failure == "after_500") {
		code := "rate_limit_exceeded"
		if u.failure == "after_500" {
			code = "server_error"
		}
		response.Body = io.NopCloser(strings.NewReader(prefix +
			"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"item_id\":\"msg_actual\",\"delta\":\"partial-text\"}\n\n" +
			`data: {"type":"response.failed","response":{"id":"resp_actual","status":"failed","error":{"code":"` + code + `","message":"temporarily unavailable"}}}` + "\n\n"))
		return response, nil
	}
	if len(u.accounts) <= 3 && !u.succeedImmediately {
		response.Body = io.NopCloser(io.MultiReader(strings.NewReader(prefix+"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"item_id\":\"msg_actual\",\"delta\":\"partial-text\"}\n\n"), openAICompatFailoverReadError{}))
		return response, nil
	}
	phase := ""
	if u.phaseFinalAnswer {
		phase = `,"phase":"final_answer"`
	}
	reasoning := ""
	if u.reasoningOutput {
		reasoning = `,{"id":"rs_actual","type":"reasoning","content":null,"summary":[],"encrypted_content":"private-reasoning"}`
	}
	response.Body = io.NopCloser(strings.NewReader(prefix +
		"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"item_id\":\"msg_actual\",\"delta\":\"winning-text\"}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_actual\",\"model\":\"gpt-5.1\",\"status\":\"completed\",\"output\":[{\"id\":\"msg_actual\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\"" + phase + ",\"content\":[{\"type\":\"output_text\",\"text\":\"winning-text\",\"annotations\":[]}]}" + reasoning + "],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"))
	return response, nil
}

func TestOpenAIManagedResponsesRecoversFromTransientFailures(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, failure := range []string{"before_429", "after_429", "before_500", "after_500"} {
		t.Run(failure, func(t *testing.T) {
			upstream := &openAIManagedResponsesUpstream{failure: failure}
			accounts := []service.Account{
				{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 0, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
				{ID: 2, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 1, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
			}
			h := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
			server := miniredis.RunT(t)
			redisClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { require.NoError(t, redisClient.Close()) })
			cache := repository.NewGatewayCache(redisClient)
			h.gatewayService = service.NewOpenAIGatewayService(openAIImagesFailoverAccountRepo{accounts: accounts}, nil, nil, nil, nil, nil, cache, h.cfg, nil, nil, nil, nil, nil, upstream, nil, nil, nil, nil, nil, nil, nil, nil)
			ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(1))
			ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(2))
			ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "newapi")
			c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
			c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
			c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"input":"hello"}`))
			h.Responses(c)

			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Equal(t, []int64{1, 1, 1, 2}, upstream.accounts, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "winning-text")
			assert.NotContains(t, rec.Body.String(), `"type":"response.failed"`)
			assert.Equal(t, 1, strings.Count(rec.Body.String(), `"type":"response.completed"`))
			assert.Equal(t, 1, strings.Count(rec.Body.String(), `"kind":"terminal"`))
			assert.Contains(t, rec.Body.String(), `"same_account_retries":2`)
			assert.Contains(t, rec.Body.String(), `"account_switches":1`)
			var alias string
			for _, line := range strings.Split(rec.Body.String(), "\n") {
				data := strings.TrimPrefix(line, "data: ")
				if gjson.Get(data, "type").String() == "response.completed" {
					alias = gjson.Get(data, "response.id").String()
				}
			}
			scope, ok := service.OpenAIRecoveryScope(ctx, 99)
			require.True(t, ok)
			winner, err := h.gatewayService.ResolveOpenAIRecoveryAlias(ctx, scope, alias)
			require.NoError(t, err)
			assert.Equal(t, int64(2), winner.AccountID)
			assert.Contains(t, string(winner.Snapshot), "winning-text")
			assert.NotContains(t, string(winner.Snapshot), "partial-text")
			if strings.HasPrefix(failure, "before_") {
				assert.NotContains(t, rec.Body.String(), "Regenerating")
			} else {
				assert.Contains(t, rec.Body.String(), "partial-text")
				assert.Contains(t, rec.Body.String(), "Regenerating")
			}
		})
	}
}

func TestOpenAIManagedResponsesStopsAfterPublicToolOutput(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &openAIManagedResponsesUpstream{failure: "after_tool"}
	accounts := []service.Account{
		{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
		{ID: 2, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
	}
	h := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
	server := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, redisClient.Close()) })
	h.gatewayService = service.NewOpenAIGatewayService(openAIImagesFailoverAccountRepo{accounts: accounts}, nil, nil, nil, nil, nil, repository.NewGatewayCache(redisClient), h.cfg, nil, nil, nil, nil, nil, upstream, nil, nil, nil, nil, nil, nil, nil, nil)
	ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(1))
	ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(2))
	ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "newapi")
	c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
	c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
	c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"input":"hello","tools":[{"type":"function","name":"test_tool","parameters":{"type":"object"}}]}`))
	h.Responses(c)

	assert.Equal(t, []int64{1}, upstream.accounts, rec.Body.String())
	assert.Contains(t, rec.Body.String(), `"type":"function_call"`)
	assert.Equal(t, 1, strings.Count(rec.Body.String(), `"kind":"terminal"`))
	assert.Contains(t, rec.Body.String(), `"outcome":"failed"`)
	assert.Contains(t, rec.Body.String(), `"stop_reason":"tool_output_started"`)
	assert.Contains(t, rec.Body.String(), `"same_account_retries":0`)
	assert.Contains(t, rec.Body.String(), `"account_switches":0`)
	assert.NotContains(t, rec.Body.String(), "Regenerating")
}

func TestOpenAIManagedLegacyContinuationDoesNotDropMissingHistory(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &openAIManagedResponsesUpstream{sessionBlocked: true}
	accounts := []service.Account{
		{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 0, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
		{ID: 2, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 1, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
	}
	h := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
	server := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, redisClient.Close()) })
	cache := repository.NewGatewayCache(redisClient)
	h.gatewayService = service.NewOpenAIGatewayService(openAIImagesFailoverAccountRepo{accounts: accounts}, nil, nil, nil, nil, nil, cache, h.cfg, nil, nil, nil, nil, nil, upstream, nil, nil, nil, nil, nil, nil, nil, nil)
	ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(1))
	ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(2))
	ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "newapi")
	store := service.NewOpenAIWSStateStore(cache)
	require.NoError(t, store.BindHTTPResponseOwner(ctx, 3131, "resp_legacy", 100, 99, time.Hour))
	require.NoError(t, store.BindResponseAccount(ctx, 3131, "resp_legacy", 1, time.Hour))
	c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
	// This fixture exercises continuation routing; cyber audit billing has its own tests.
	c.Set(cyberPolicyRecordedKey, true)
	c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
	c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"previous_response_id":"resp_legacy","input":"followup without the previous context"}`))
	h.Responses(c)
	require.Equal(t, []int64{1}, upstream.accounts, rec.Body.String())
	assert.Equal(t, "resp_legacy", gjson.GetBytes(upstream.bodies[0], "previous_response_id").String())
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "continuation_history_missing")
	assert.NotContains(t, rec.Body.String(), "response.completed")
}

func TestOpenAIManagedResponsesRecoversAndRebuildsContinuation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &openAIManagedResponsesUpstream{}
	accounts := []service.Account{
		{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 0, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
		{ID: 2, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 1, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
	}
	h := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
	server := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { require.NoError(t, redisClient.Close()) })
	cache := repository.NewGatewayCache(redisClient)
	h.gatewayService = service.NewOpenAIGatewayService(openAIImagesFailoverAccountRepo{accounts: accounts}, nil, nil, nil, nil, nil, cache, h.cfg, nil, nil, nil, nil, nil, upstream, nil, nil, nil, nil, nil, nil, nil, nil)
	ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(1))
	ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(2))
	ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "newapi")
	c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)
	c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
	c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"input":"hello"}`))
	h.Responses(c)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, []int64{1, 1, 1, 2}, upstream.accounts, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "partial-text")
	assert.Contains(t, rec.Body.String(), "Regenerating")
	assert.Contains(t, rec.Body.String(), "winning-text")
	assert.Equal(t, 1, strings.Count(rec.Body.String(), `"kind":"terminal"`))
	var alias string
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		data := strings.TrimPrefix(line, "data: ")
		if gjson.Get(data, "type").String() == "response.completed" {
			alias = gjson.Get(data, "response.id").String()
		}
	}
	require.True(t, service.IsOpenAIRecoveryAliasID(alias), rec.Body.String())
	scope, ok := service.OpenAIRecoveryScope(ctx, 99)
	require.True(t, ok)
	winner, err := h.gatewayService.ResolveOpenAIRecoveryAlias(ctx, scope, alias)
	require.NoError(t, err)
	require.NotEmpty(t, winner.Snapshot)
	assert.Equal(t, int64(2), winner.AccountID)
	assert.NotContains(t, string(winner.Snapshot), "partial-text")
	assert.NotContains(t, string(winner.Snapshot), "Regenerating")
	accounts[1].Status = "disabled"
	next, nextRec := newOpenAIResponsesFailoverTestContext(t, ctx)
	next.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
	next.Request.Header.Set("X-Codex-Turn-State", "old-account-state")
	next.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"previous_response_id":"` + alias + `","input":"followup"}`))
	h.Responses(next)
	require.Equal(t, http.StatusOK, nextRec.Code, nextRec.Body.String())
	require.Len(t, upstream.accounts, 5, nextRec.Body.String())
	assert.Equal(t, int64(1), upstream.accounts[4])
	assert.False(t, gjson.GetBytes(upstream.bodies[4], "previous_response_id").Exists())
	assert.Contains(t, string(upstream.bodies[4]), "winning-text")
	assert.Contains(t, string(upstream.bodies[4]), "followup")
	assert.Empty(t, next.Request.Header.Get("X-Codex-Turn-State"))
}

func TestOpenAIManagedResponsesRebuildsFinalAnswerAfterPhysicalContinuationFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name            string
		accounts        int
		fallbackAccount int64
	}{
		{name: "alternate account", accounts: 2, fallbackAccount: 2},
		{name: "same account", accounts: 1, fallbackAccount: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := &openAIManagedResponsesUpstream{phaseFinalAnswer: true, reasoningOutput: true, succeedImmediately: true}
			accounts := []service.Account{
				{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 0, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
				{ID: 2, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: 1, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}},
			}
			accounts = accounts[:tc.accounts]
			h := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
			server := miniredis.RunT(t)
			redisClient := redis.NewClient(&redis.Options{Addr: server.Addr()})
			t.Cleanup(func() { require.NoError(t, redisClient.Close()) })
			cache := repository.NewGatewayCache(redisClient)
			h.gatewayService = service.NewOpenAIGatewayService(openAIImagesFailoverAccountRepo{accounts: accounts}, nil, nil, nil, nil, nil, cache, h.cfg, nil, nil, nil, nil, nil, upstream, nil, nil, nil, nil, nil, nil, nil, nil)
			ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(1))
			ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(2))
			ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "newapi")

			first, firstRec := newOpenAIResponsesFailoverTestContext(t, ctx)
			first.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
			first.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"input":"first"}`))
			h.Responses(first)
			require.Equal(t, http.StatusOK, firstRec.Code, firstRec.Body.String())
			var alias string
			for _, line := range strings.Split(firstRec.Body.String(), "\n") {
				if gjson.Get(strings.TrimPrefix(line, "data: "), "type").String() == "response.completed" {
					alias = gjson.Get(strings.TrimPrefix(line, "data: "), "response.id").String()
				}
			}
			require.True(t, service.IsOpenAIRecoveryAliasID(alias), firstRec.Body.String())
			scope, ok := service.OpenAIRecoveryScope(ctx, 99)
			require.True(t, ok)
			winner, err := h.gatewayService.ResolveOpenAIRecoveryAlias(ctx, scope, alias)
			require.NoError(t, err)
			require.NotEmpty(t, winner.Snapshot)
			assert.Contains(t, string(winner.Snapshot), "winning-text")
			assert.NotContains(t, string(winner.Snapshot), "private-reasoning")
			assert.NotContains(t, string(winner.Snapshot), "rs_actual")

			healthy, healthyRec := newOpenAIResponsesFailoverTestContext(t, ctx)
			healthy.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
			healthy.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"previous_response_id":"` + alias + `","input":"second"}`))
			h.Responses(healthy)
			require.Equal(t, http.StatusOK, healthyRec.Code, healthyRec.Body.String())
			assert.Contains(t, healthyRec.Body.String(), `"type":"response.completed"`)
			assert.Equal(t, "resp_actual", gjson.GetBytes(upstream.bodies[len(upstream.bodies)-1], "previous_response_id").String())
			assert.Equal(t, int64(1), upstream.accounts[len(upstream.accounts)-1])

			upstream.physicalFailure = true
			attemptStart := len(upstream.bodies)
			failed, recoveredRec := newOpenAIResponsesFailoverTestContext(t, ctx)
			failed.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
			failed.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"previous_response_id":"` + alias + `","input":"third"}`))
			h.Responses(failed)
			require.Equal(t, http.StatusOK, recoveredRec.Code, recoveredRec.Body.String())
			assert.Equal(t, 1, strings.Count(recoveredRec.Body.String(), `"type":"response.completed"`))
			assert.Equal(t, 1, strings.Count(recoveredRec.Body.String(), `"kind":"terminal"`))
			require.Len(t, upstream.bodies[attemptStart:], 4, recoveredRec.Body.String())
			for _, body := range upstream.bodies[attemptStart : attemptStart+3] {
				assert.Equal(t, "resp_actual", gjson.GetBytes(body, "previous_response_id").String())
			}
			portable := upstream.bodies[len(upstream.bodies)-1]
			assert.False(t, gjson.GetBytes(portable, "previous_response_id").Exists())
			assert.Contains(t, string(portable), "winning-text")
			assert.Contains(t, string(portable), "third")
			assert.Equal(t, tc.fallbackAccount, upstream.accounts[len(upstream.accounts)-1])
		})
	}
}
