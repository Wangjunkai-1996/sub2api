//go:build unit

package handler

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type selectedAccountAuditResponsesUpstream struct {
	service.HTTPUpstream
	mu         sync.Mutex
	accountIDs []int64
	failFirst  bool
}

func (u *selectedAccountAuditResponsesUpstream) Do(_ *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	u.accountIDs = append(u.accountIDs, accountID)
	call := len(u.accountIDs)
	u.mu.Unlock()

	if u.failFirst && call == 1 {
		return &http.Response{
			StatusCode: 520,
			Header:     http.Header{"Content-Type": []string{"text/html"}},
			Body:       io.NopCloser(bytes.NewBufferString("<html>520: unknown error</html>")),
		}, nil
	}

	body := strings.Join([]string{
		`data: {"type":"response.output_text.delta","delta":"ok"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_selected_audit_ok","status":"completed","usage":{"input_tokens":1,"output_tokens":1}}}`,
		"",
	}, "\n")
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func (u *selectedAccountAuditResponsesUpstream) calls() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int64(nil), u.accountIDs...)
}

func selectedAccountAuditProAccounts() []service.Account {
	return []service.Account{
		{
			ID: 1, Name: "selected-audit-pro-1", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 0,
			Credentials: map[string]any{"access_token": "token-1", "plan_type": "pro"},
		},
		{
			ID: 2, Name: "selected-audit-pro-2", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
			Concurrency: 1, Priority: 1,
			Credentials: map[string]any{"access_token": "token-2", "plan_type": "pro"},
		},
	}
}

func newSelectedAccountAuditHandler(
	t *testing.T,
	upstream service.HTTPUpstream,
	engine *turnCountingEngine,
) (*OpenAIGatewayHandler, *concurrencyCacheMock) {
	t.Helper()
	cache := &concurrencyCacheMock{
		acquireUserSlotFn: func(context.Context, int64, int, string) (bool, error) {
			return true, nil
		},
		acquireAccountSlotFn: func(context.Context, int64, int, string) (bool, error) {
			return true, nil
		},
	}
	concurrencyService := service.NewConcurrencyService(cache)
	cfg := &config.Config{RunMode: config.RunModeSimple}
	gatewayService := service.NewOpenAIGatewayService(
		openAIImagesFailoverAccountRepo{accounts: selectedAccountAuditProAccounts()},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		cfg,
		nil,
		concurrencyService,
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
	handler.securityAuditCoordinator = securityaudit.NewCoordinator(nil, engine)
	return handler, cache
}

func selectedAccountAuditHTTPContext(t *testing.T, path string, body string) (*gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	c, recorder := newOpenAIResponsesFailoverTestContext(t, nil)
	c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	require.True(t, ok)
	apiKey.Group.AllowMessagesDispatch = true
	c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100, Concurrency: 1})
	return c, recorder
}

func TestOpenAIGatewayHandler_SelectedProAuditBlockReleasesConcurrencyBeforeUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name   string
		path   string
		body   string
		invoke func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{
			name: "Responses",
			path: "/v1/responses",
			body: `{"model":"gpt-5.1","stream":false,"input":"hello"}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.Responses(c)
			},
		},
		{
			name: "Messages",
			path: "/v1/messages",
			body: `{"model":"gpt-5.1","stream":false,"max_tokens":16,"messages":[{"role":"user","content":"hello"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.Messages(c)
			},
		},
		{
			name: "Chat Completions",
			path: "/v1/chat/completions",
			body: `{"model":"gpt-5.1","stream":false,"messages":[{"role":"user","content":"hello"}]}`,
			invoke: func(h *OpenAIGatewayHandler, c *gin.Context) {
				h.ChatCompletions(c)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := &selectedAccountAuditResponsesUpstream{}
			engine := &turnCountingEngine{
				mode: securityaudit.ModeBlocking,
				decisions: []*securityaudit.PromptDecision{{
					Kind: securityaudit.DecisionBlock, AllowNextStage: false,
				}},
			}
			handler, cache := newSelectedAccountAuditHandler(t, upstream, engine)
			c, recorder := selectedAccountAuditHTTPContext(t, tt.path, tt.body)

			tt.invoke(handler, c)

			require.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())
			require.Empty(t, upstream.calls(), "a blocked selected-account audit must stop before upstream forwarding")
			require.Equal(t, int64(1), engine.evaluates.Load())
			require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseAccountCalled))
			require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
		})
	}
}

func TestOpenAIGatewayHandlerResponses_SelectedProAuditAllowIsReusedAcrossFailover(t *testing.T) {
	upstream := &selectedAccountAuditResponsesUpstream{failFirst: true}
	engine := &turnCountingEngine{mode: securityaudit.ModeBlocking}
	handler, cache := newSelectedAccountAuditHandler(t, upstream, engine)
	c, recorder := selectedAccountAuditHTTPContext(t, "/v1/responses", `{"model":"gpt-5.1","stream":true,"input":"hello"}`)

	handler.Responses(c)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, []int64{1, 2}, upstream.calls())
	require.Equal(t, int64(1), engine.evaluates.Load(), "an allowed HTTP audit must be reused after account failover")
	require.Equal(t, int32(2), atomic.LoadInt32(&cache.releaseAccountCalled))
	require.Equal(t, int32(1), atomic.LoadInt32(&cache.releaseUserCalled))
}
