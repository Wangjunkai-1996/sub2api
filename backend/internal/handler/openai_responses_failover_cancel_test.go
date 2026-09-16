//go:build unit

package handler

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// openAIResponsesFailoverCancelUpstream 固定返回 HTTP 520，可在首次上游调用时
// 触发回调（用于模拟“上游在途期间客户端断开”）。
type openAIResponsesFailoverCancelUpstream struct {
	service.HTTPUpstream
	mu         sync.Mutex
	accountIDs []int64
	onFirstDo  func()
}

type openAIResponsesCapacityFailoverUpstream struct {
	service.HTTPUpstream
	mu          sync.Mutex
	accountIDs  []int64
	prefixEvent string
}

func (u *openAIResponsesCapacityFailoverUpstream) Do(_ *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	u.accountIDs = append(u.accountIDs, accountID)
	u.mu.Unlock()

	body := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_capacity","instructions":"` + strings.Repeat("p", 8*1024) + `"}}`,
		"",
		`data: {"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"overloaded"}}`,
		"",
		`data: {"type":"response.failed","response":{"id":"resp_capacity","status":"failed","error":{"code":"server_is_overloaded","message":"overloaded"}}}`,
		"",
	}, "\n")
	if u.prefixEvent != "" {
		body = "data: " + u.prefixEvent + "\n\n" + body
	}
	if accountID == 3 {
		body = strings.Join([]string{
			`data: {"type":"response.output_text.delta","delta":"ok"}`,
			"",
			`data: {"type":"response.completed","response":{"id":"resp_capacity_ok","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}`,
			"",
		}, "\n")
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func (u *openAIResponsesCapacityFailoverUpstream) calls() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int64(nil), u.accountIDs...)
}

func (u *openAIResponsesFailoverCancelUpstream) Do(_ *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	u.accountIDs = append(u.accountIDs, accountID)
	first := len(u.accountIDs) == 1
	u.mu.Unlock()
	if first && u.onFirstDo != nil {
		u.onFirstDo()
	}
	return &http.Response{
		StatusCode: 520,
		Header:     http.Header{"Content-Type": []string{"text/html"}},
		Body:       io.NopCloser(bytes.NewBufferString("<html>520: unknown error</html>")),
	}, nil
}

func (u *openAIResponsesFailoverCancelUpstream) calls() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int64(nil), u.accountIDs...)
}

func newOpenAIResponsesFailoverTestHandler(t *testing.T, upstream service.HTTPUpstream) *OpenAIGatewayHandler {
	t.Helper()
	accounts := []service.Account{
		{
			ID:          1,
			Name:        "responses-account-1",
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeOAuth,
			Status:      service.StatusActive,
			Schedulable: true,
			Concurrency: 0,
			Priority:    0,
			Credentials: map[string]any{"access_token": "token-1"},
		},
		{
			ID:          2,
			Name:        "responses-account-2",
			Platform:    service.PlatformOpenAI,
			Type:        service.AccountTypeOAuth,
			Status:      service.StatusActive,
			Schedulable: true,
			Concurrency: 0,
			Priority:    1,
			Credentials: map[string]any{"access_token": "token-2"},
		},
	}
	return newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
}

func newOpenAIFailoverTestHandlerWithAccounts(
	t *testing.T,
	upstream service.HTTPUpstream,
	accounts []service.Account,
) *OpenAIGatewayHandler {
	t.Helper()
	accountRepo := openAIImagesFailoverAccountRepo{accounts: accounts}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	gatewayService := service.NewOpenAIGatewayService(
		accountRepo,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		cfg,
		nil,
		nil,
		nil,
		nil,
		nil,
		upstream,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	billingService := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billingService.Stop)
	concurrencyService := service.NewConcurrencyService(nil)
	handler := NewOpenAIGatewayHandler(
		gatewayService,
		concurrencyService,
		billingService,
		service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg),
		nil,
		nil,
		nil,
		nil,
		cfg,
	)
	handler.maxAccountSwitches = 10
	return handler
}

func newOpenAIResponsesFailoverTestContext(t *testing.T, ctx context.Context) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	groupID := int64(3131)
	body := []byte(`{"model":"gpt-5.1","stream":false,"input":"hello"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	if ctx != nil {
		req = req.WithContext(ctx)
	}
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = req
	c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
		ID:      99,
		GroupID: &groupID,
		Group: &service.Group{
			ID:       groupID,
			Platform: service.PlatformOpenAI,
		},
		User: &service.User{ID: 100},
	})
	c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100, Concurrency: 0})
	return c, rec
}

// TestOpenAIGatewayHandlerResponses_FailoverAbortsWhenClientDisconnected 复现
// #4257：客户端在上游请求在途期间断开，上游随后返回可 failover 的 520。
// 期望：不再用已取消的 context 重新选号（不触达账号 2）、不把取消误报成
// 502 账号耗尽、请求按 499 归类。
func TestOpenAIGatewayHandlerResponses_FailoverAbortsWhenClientDisconnected(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upstream := &openAIResponsesFailoverCancelUpstream{onFirstDo: cancel}
	handler := newOpenAIResponsesFailoverTestHandler(t, upstream)
	c, rec := newOpenAIResponsesFailoverTestContext(t, ctx)

	handler.Responses(c)

	require.Equal(t, []int64{1}, upstream.calls(), "客户端断开后不应再切换到账号 2")
	require.Equal(t, statusClientClosedRequest, c.Writer.Status(), "应按 499 归类")
	require.Zero(t, rec.Body.Len(), "不应写入 502 错误响应体")

	_, hasFinalUpstreamErr := c.Get(service.OpsUpstreamStatusCodeKey)
	require.False(t, hasFinalUpstreamErr, "不应记录 failover 耗尽的上游错误终态")

	// 真实发生过的 520 应保留 failover 事件（service 层在返回 failover 错误前记录）
	rawEvents, ok := c.Get(service.OpsUpstreamErrorsKey)
	require.True(t, ok)
	events, ok := rawEvents.([]*service.OpsUpstreamErrorEvent)
	require.True(t, ok)
	require.Len(t, events, 1)
	require.Equal(t, "failover", events[0].Kind)
	require.Equal(t, 520, events[0].UpstreamStatusCode)
}

// TestOpenAIGatewayHandlerResponses_FailoverContinuesForConnectedClient 回归
// 守卫：客户端在线时 failover 行为不变——切换到账号 2，两个账号都 520 后按
// 耗尽返回 502。
func TestOpenAIGatewayHandlerResponses_FailoverContinuesForConnectedClient(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upstream := &openAIResponsesFailoverCancelUpstream{}
	handler := newOpenAIResponsesFailoverTestHandler(t, upstream)
	c, rec := newOpenAIResponsesFailoverTestContext(t, nil)

	handler.Responses(c)

	require.Equal(t, []int64{1, 2}, upstream.calls(), "在线客户端应正常切换账号")
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Equal(t, "upstream_error", gjson.GetBytes(rec.Body.Bytes(), "error.type").String())
}

func TestOpenAIGatewayHandlerResponses_CapacityRetryThenUsesNormalAccountSwitchBudget(t *testing.T) {
	gin.SetMode(gin.TestMode)
	newCapacityAccount := func(id int64, priority, retryCount int) service.Account {
		return service.Account{
			ID: id, Name: fmt.Sprintf("capacity-account-%d", id),
			Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
			Status: service.StatusActive, Schedulable: true, Priority: priority,
			Credentials: map[string]any{
				"api_key": "sk-test", "base_url": "https://api.example.test",
				"pool_mode": true, "pool_mode_retry_count": float64(retryCount),
			},
			Extra: map[string]any{"openai_passthrough": false},
		}
	}
	accounts := []service.Account{
		newCapacityAccount(1, 0, 1),
		newCapacityAccount(2, 1, 0),
		newCapacityAccount(3, 2, 0),
	}

	upstream := &openAIResponsesCapacityFailoverUpstream{}
	handler := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
	handler.cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 30
	c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
		`{"model":"gpt-5.1","stream":true,"input":"hello"}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")

	handler.Responses(c)

	// Request-scoped capacity errors move to another eligible account before
	// spending the retry window on the same account.
	require.Equal(t, []int64{1, 2, 3}, upstream.calls(), "status=%d body=%s", rec.Code, rec.Body.String())
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"delta":"ok"`)
}

func TestOpenAIGatewayHandlerResponses_CapacityFailoverKeepsDistinctCredentialCandidates(t *testing.T) {
	gin.SetMode(gin.TestMode)

	account := func(id int64, accountType string, credential string) service.Account {
		credentials := map[string]any{}
		if accountType == service.AccountTypeAPIKey {
			credentials["api_key"] = credential
			credentials["base_url"] = "https://vip.mdkj.lol/v1"
		} else {
			credentials["access_token"] = credential
		}
		return service.Account{
			ID: id, Name: fmt.Sprintf("credential-account-%d", id),
			Platform: service.PlatformOpenAI, Type: accountType,
			Status: service.StatusActive, Schedulable: true, Priority: int(id - 1),
			Credentials: credentials,
			Extra:       map[string]any{"openai_passthrough": false},
		}
	}

	tests := []struct {
		name  string
		types []string
	}{
		{
			name:  "same host different API keys",
			types: []string{service.AccountTypeAPIKey, service.AccountTypeAPIKey, service.AccountTypeAPIKey},
		},
		{
			name:  "API key to OAuth to API key",
			types: []string{service.AccountTypeAPIKey, service.AccountTypeOAuth, service.AccountTypeAPIKey},
		},
		{
			name:  "OAuth to API key to OAuth",
			types: []string{service.AccountTypeOAuth, service.AccountTypeAPIKey, service.AccountTypeOAuth},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accounts := []service.Account{
				account(1, tt.types[0], "credential-1"),
				account(2, tt.types[1], "credential-2"),
				account(3, tt.types[2], "credential-3"),
			}
			upstream := &openAIResponsesCapacityFailoverUpstream{}
			handler := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
			c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
				`{"model":"gpt-5.1","stream":true,"input":"hello"}`,
			))
			c.Request.Header.Set("Content-Type", "application/json")

			handler.Responses(c)

			require.Equal(t, []int64{1, 2, 3}, upstream.calls(), "status=%d body=%s", rec.Code, rec.Body.String())
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Contains(t, rec.Body.String(), `"delta":"ok"`)
		})
	}
}

func TestOpenAIGatewayHandlerResponses_StreamReplayBoundary(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		for _, tc := range []struct {
			name   string
			event  string
			replay bool
		}{
			{"empty text delta", `{"type":"response.output_text.delta","delta":""}`, true},
			{"empty text done", `{"type":"response.output_text.done","text":""}`, true},
			{"empty message done", `{"type":"response.output_item.done","item":{"type":"message","content":[]}}`, true},
			{"text delta", `{"type":"response.output_text.delta","delta":"partial"}`, false},
			{"null delta", `{"type":"response.output_text.delta","delta":null}`, false},
			{"missing delta", `{"type":"response.output_text.delta"}`, false},
			{"unknown delta", `{"type":"response.custom_payload.delta","delta":"","payload":"opaque"}`, false},
			{"unknown done item", `{"type":"response.output_item.done","item":{"type":"computer_call","call_id":"call_1","action":{"type":"click","x":1,"y":2}}}`, false},
			{"empty completed function call", `{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call_1","name":"refresh","arguments":""}}`, false},
			{"empty completed custom call", `{"type":"response.output_item.done","item":{"type":"custom_tool_call","call_id":"call_1","name":"refresh","input":""}}`, false},
			{"encrypted done", `{"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"ciphertext","summary":[]}}`, false},
			{"reasoning content done", `{"type":"response.output_item.done","item":{"type":"reasoning","summary":[],"content":[{"type":"reasoning_text","text":"real"}]}}`, false},
			{"refusal done", `{"type":"response.content_part.done","part":{"type":"refusal","refusal":"blocked"}}`, false},
			{"image output", `{"type":"response.image_generation_call.partial_image","partial_image_b64":"aW1hZ2U="}`, false},
		} {
			t.Run(fmt.Sprintf("passthrough=%t/%s", passthrough, tc.name), func(t *testing.T) {
				accounts := make([]service.Account, 3)
				for i := range accounts {
					accounts[i] = service.Account{
						ID: int64(i + 1), Name: fmt.Sprintf("stream-account-%d", i+1),
						Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
						Status: service.StatusActive, Schedulable: true, Priority: i,
						Credentials: map[string]any{"api_key": "sk-test", "base_url": "https://api.example.test"},
						Extra:       map[string]any{"openai_passthrough": passthrough},
					}
				}
				upstream := &openAIResponsesCapacityFailoverUpstream{prefixEvent: tc.event}
				handler := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
				c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.1","stream":true,"input":"hello"}`))
				c.Request.Header.Set("Content-Type", "application/json")
				handler.Responses(c)

				if tc.replay {
					require.Equal(t, []int64{1, 2, 3}, upstream.calls(), rec.Body.String())
					require.Contains(t, rec.Body.String(), `"delta":"ok"`)
					require.NotContains(t, rec.Body.String(), tc.event, "discarded attempt events must remain private")
				} else {
					require.Equal(t, []int64{1}, upstream.calls(), rec.Body.String())
					require.NotContains(t, rec.Body.String(), `"delta":"ok"`, "a second answer must not be appended")
				}
			})
		}
	}
}
