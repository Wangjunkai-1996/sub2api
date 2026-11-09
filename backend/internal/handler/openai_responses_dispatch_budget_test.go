//go:build unit

package handler

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type openAIResponsesDispatchBudgetUpstream struct {
	service.HTTPUpstream
	calls         []int64
	repairFirst   bool
	successID     int64
	onFirst       func()
	failureStatus int
	readErr       error
}

func (u *openAIResponsesDispatchBudgetUpstream) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.calls = append(u.calls, accountID)
	body, err := io.ReadAll(req.Body)
	if err != nil {
		u.readErr = err
		return nil, err
	}
	if len(u.calls) == 1 && u.onFirst != nil {
		u.onFirst()
	}
	status := u.failureStatus
	if status == 0 {
		status = http.StatusBadRequest
	}
	payload := `{"error":{"type":"server_error","message":"An error occurred while processing your request"}}`
	if u.repairFirst && accountID == 1 {
		for _, field := range []string{"max_output_tokens", "truncation"} {
			if gjson.GetBytes(body, field).Exists() {
				return openAIResponsesBudgetHTTPResponse(http.StatusBadRequest, fmt.Sprintf(`{"error":{"code":"unsupported_parameter","param":%q,"message":"Unsupported parameter"}}`, field)), nil
			}
		}
	}
	if accountID == u.successID {
		status = http.StatusOK
		payload = `{"id":"resp_dispatch_ok","object":"response","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1,"output_tokens":1}}`
	}
	return openAIResponsesBudgetHTTPResponse(status, payload), nil
}

func openAIResponsesBudgetHTTPResponse(status int, payload string) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(payload))}
}

func newOpenAIResponsesDispatchBudgetAccounts(count int) []service.Account {
	accounts := make([]service.Account, count)
	for i := range accounts {
		accounts[i] = service.Account{
			ID: int64(i + 1), Name: fmt.Sprintf("dispatch-account-%d", i+1),
			Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
			Status: service.StatusActive, Schedulable: true, Priority: i,
			Credentials: map[string]any{"api_key": "test", "base_url": "https://example.test", "pool_mode": true, "pool_mode_retry_count": 10},
			Extra:       map[string]any{"openai_passthrough": false},
		}
	}
	return accounts
}

func newOpenAIResponsesDispatchBudgetContext(t *testing.T, ctx context.Context) (*gin.Context, *httptest.ResponseRecorder) {
	c, recorder := newOpenAIResponsesFailoverTestContext(t, ctx)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.1","stream":false,"input":"hello","max_output_tokens":32,"truncation":"auto"}`)).WithContext(c.Request.Context())
	c.Request.Header.Set("Content-Type", "application/json")
	return c, recorder
}

func TestOpenAIGatewayHandlerResponses_DispatchBudgetFreshAccountsOnly(t *testing.T) {
	for _, successID := range []int64{0, 5} {
		t.Run(fmt.Sprintf("success_%d", successID), func(t *testing.T) {
			upstream := &openAIResponsesDispatchBudgetUpstream{successID: successID}
			handler := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, newOpenAIResponsesDispatchBudgetAccounts(5))
			c, rec := newOpenAIResponsesDispatchBudgetContext(t, context.Background())
			handler.Responses(c)
			require.NoError(t, upstream.readErr)
			require.Equal(t, []int64{1, 2, 3, 4, 5}, upstream.calls, rec.Body.String())
			if successID > 0 {
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				require.Contains(t, rec.Body.String(), "resp_dispatch_ok")
				require.Empty(t, rec.Header().Get("X-Sub2-Retry-Status"))
			} else {
				require.GreaterOrEqual(t, rec.Code, http.StatusBadRequest)
				require.Equal(t, "exhausted", rec.Header().Get("X-Sub2-Retry-Status"))
			}
		})
	}
}

func TestOpenAIGatewayHandlerResponses_DispatchBudgetRepairsThenFreshAccount(t *testing.T) {
	upstream := &openAIResponsesDispatchBudgetUpstream{repairFirst: true, successID: 2}
	handler := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, newOpenAIResponsesDispatchBudgetAccounts(2))
	c, rec := newOpenAIResponsesDispatchBudgetContext(t, context.Background())
	handler.Responses(c)
	require.NoError(t, upstream.readErr)
	require.Equal(t, []int64{1, 1, 1, 2}, upstream.calls, rec.Body.String())
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestOpenAIGatewayHandlerResponses_DispatchBudgetIncludesRepairs(t *testing.T) {
	upstream := &openAIResponsesDispatchBudgetUpstream{repairFirst: true, failureStatus: http.StatusBadGateway}
	handler := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, newOpenAIResponsesDispatchBudgetAccounts(12))
	c, rec := newOpenAIResponsesDispatchBudgetContext(t, context.Background())
	handler.Responses(c)
	require.NoError(t, upstream.readErr)
	require.Equal(t, []int64{1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9}, upstream.calls, rec.Body.String())
	require.Equal(t, http.StatusBadGateway, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "model attempt limit")
	require.Equal(t, "exhausted", rec.Header().Get("X-Sub2-Retry-Status"))
}

func TestOpenAIGatewayHandlerResponses_DispatchBudgetCancelsCompatibilityRepair(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	upstream := &openAIResponsesDispatchBudgetUpstream{repairFirst: true, onFirst: cancel}
	handler := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, newOpenAIResponsesDispatchBudgetAccounts(2))
	c, rec := newOpenAIResponsesDispatchBudgetContext(t, ctx)
	handler.Responses(c)
	require.NoError(t, upstream.readErr)
	require.Equal(t, []int64{1}, upstream.calls)
	require.Equal(t, statusClientClosedRequest, c.Writer.Status())
	require.Empty(t, rec.Body.String())
}

func TestOpenAIGatewayHandlerResponses_DispatchBudgetPassthroughRepairs(t *testing.T) {
	for _, outcome := range []string{"success", "exhausted", "canceled"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			upstream := &openAIResponsesDispatchBudgetUpstream{repairFirst: true, failureStatus: http.StatusBadGateway}
			wantCalls := []int64{1, 1, 1, 2}
			if outcome == "success" {
				upstream.successID = 2
			} else if outcome == "canceled" {
				upstream.onFirst = cancel
				wantCalls = []int64{1}
			} else {
				wantCalls = []int64{1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9}
			}
			accounts := newOpenAIResponsesDispatchBudgetAccounts(12)
			for i := range accounts {
				accounts[i].Extra["openai_passthrough"] = true
			}
			handler := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
			c, rec := newOpenAIResponsesDispatchBudgetContext(t, ctx)
			handler.Responses(c)
			require.NoError(t, upstream.readErr)
			require.Equal(t, wantCalls, upstream.calls, rec.Body.String())
			switch outcome {
			case "success":
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			case "exhausted":
				require.Equal(t, http.StatusBadGateway, rec.Code, rec.Body.String())
				require.Contains(t, rec.Body.String(), "model attempt limit")
			case "canceled":
				require.Equal(t, statusClientClosedRequest, c.Writer.Status())
				require.Empty(t, rec.Body.String())
			}
		})
	}
}
