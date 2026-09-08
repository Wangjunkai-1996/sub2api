//go:build unit

package handler

import (
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type openAIAtomicStreamFailoverUpstream struct {
	service.HTTPUpstream
	mu         sync.Mutex
	accountIDs []int64
}

func (u *openAIAtomicStreamFailoverUpstream) Do(_ *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	u.accountIDs = append(u.accountIDs, accountID)
	u.mu.Unlock()

	requestID := "rid-losing-attempt"
	body := strings.Join([]string{
		`event: response.created`,
		`data: {"type":"response.created","response":{"id":"resp_losing","status":"in_progress"}}`,
		``,
		`event: response.output_text.delta`,
		`data: {"type":"response.output_text.delta","response_id":"resp_losing","delta":"losing-fragment"}`,
		``,
		`event: error`,
		`data: {"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}`,
		``,
		`event: response.failed`,
		`data: {"type":"response.failed","response":{"id":"resp_losing","status":"failed","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}`,
		``,
	}, "\n")
	if accountID == 2 {
		requestID = "rid-winning-attempt"
		body = strings.Join([]string{
			`event: response.created`,
			`data: {"type":"response.created","response":{"id":"resp_winning","status":"in_progress"}}`,
			``,
			`event: response.output_text.delta`,
			`data: {"type":"response.output_text.delta","response_id":"resp_winning","delta":"winning-fragment"}`,
			``,
			`event: response.completed`,
			`data: {"type":"response.completed","response":{"id":"resp_winning","status":"completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
			``,
		}, "\n")
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": []string{"text/event-stream"},
			"X-Request-Id": []string{requestID},
		},
		Body: io.NopCloser(strings.NewReader(body)),
	}, nil
}

func (u *openAIAtomicStreamFailoverUpstream) calls() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int64(nil), u.accountIDs...)
}

func TestOpenAIGatewayHandlerResponses_AtomicCapacityRetriesThenSwitchesAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	accounts := []service.Account{
		{
			ID: 1, Name: "atomic-account-1", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
			Credentials: map[string]any{"access_token": "token-1"}, Priority: 0,
		},
		{
			ID: 2, Name: "atomic-account-2", Platform: service.PlatformOpenAI,
			Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
			Credentials: map[string]any{"access_token": "token-2"}, Priority: 1,
		},
	}
	upstream := &openAIAtomicStreamFailoverUpstream{}
	handler := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
	handler.cfg.Gateway.OpenAIAtomicStreamFailover = true
	c, recorder := newOpenAIResponsesFailoverTestContext(t, nil)
	c.Request = c.Request.Clone(c.Request.Context())
	c.Request.Body = io.NopCloser(strings.NewReader(`{"model":"gpt-5.1","stream":true,"input":"hello"}`))

	handler.Responses(c)

	require.Equal(t, []int64{1, 1, 1, 1, 2}, upstream.calls())
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "winning-fragment")
	require.Contains(t, recorder.Body.String(), "resp_winning")
	require.NotContains(t, recorder.Body.String(), "losing-fragment")
	require.NotContains(t, recorder.Body.String(), "resp_losing")
	require.NotContains(t, recorder.Body.String(), "server_is_overloaded")
	require.Equal(t, "rid-winning-attempt", recorder.Result().Header.Get("X-Request-Id"))
}
