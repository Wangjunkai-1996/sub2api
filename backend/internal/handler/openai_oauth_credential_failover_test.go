//go:build unit

package handler

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type openAIOAuthFailoverRepo struct {
	openAIImagesFailoverAccountRepo
	mu sync.Mutex
}

func (r *openAIOAuthFailoverRepo) SetError(_ context.Context, id int64, _ string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.accounts {
		if r.accounts[i].ID == id {
			r.accounts[i].Status = service.StatusError
			r.accounts[i].Schedulable = false
		}
	}
	return nil
}

type openAIOAuthFailoverUpstream struct {
	service.HTTPUpstream
	mu         sync.Mutex
	accountIDs []int64
}

func (u *openAIOAuthFailoverUpstream) Do(_ *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	u.accountIDs = append(u.accountIDs, accountID)
	u.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewBufferString(`{"id":"resp_oauth_healthy","object":"response","model":"gpt-5.4","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)),
	}, nil
}

func (u *openAIOAuthFailoverUpstream) calls() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int64(nil), u.accountIDs...)
}

func newOpenAIOAuthFailoverHandler(t *testing.T, upstream service.HTTPUpstream, accounts []service.Account) *OpenAIGatewayHandler {
	t.Helper()
	repo := &openAIOAuthFailoverRepo{openAIImagesFailoverAccountRepo: openAIImagesFailoverAccountRepo{accounts: accounts}}
	cfg := &config.Config{RunMode: config.RunModeSimple}
	provider := service.NewOpenAITokenProvider(repo, nil, nil)
	gatewayService := service.NewOpenAIGatewayService(
		repo,
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
		provider,
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
		service.NewConcurrencyService(nil),
		billingService,
		service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg),
		nil, nil, nil, nil, cfg,
	)
	handler.maxAccountSwitches = 10
	return handler
}

func openAIOAuthFailoverAccounts() []service.Account {
	return []service.Account{
		{
			ID: 1, Name: "expired-oauth", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
			Status: service.StatusActive, Schedulable: true, Priority: 0,
			Credentials: map[string]any{"access_token": "expired", "expires_at": time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)},
		},
		{
			ID: 2, Name: "healthy-oauth", Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
			Status: service.StatusActive, Schedulable: true, Priority: 1,
			Credentials: map[string]any{"access_token": "healthy", "expires_at": time.Now().Add(time.Hour).UTC().Format(time.RFC3339)},
		},
	}
}

func TestOpenAIResponsesOAuthTokenFailureRetriesHealthyAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := &openAIOAuthFailoverUpstream{}
	handler := newOpenAIOAuthFailoverHandler(t, upstream, openAIOAuthFailoverAccounts())
	c, recorder := newOpenAIResponsesFailoverTestContext(t, nil)

	handler.Responses(c)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Contains(t, recorder.Body.String(), "resp_oauth_healthy")
	require.Equal(t, []int64{2}, upstream.calls(), "失效账号应在 token 阶段被跳过")
}

func TestOpenAIResponsesOAuthTokenFailureExhaustsWithoutUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	accounts := openAIOAuthFailoverAccounts()
	accounts[1].Credentials["expires_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	upstream := &openAIOAuthFailoverUpstream{}
	handler := newOpenAIOAuthFailoverHandler(t, upstream, accounts)
	c, recorder := newOpenAIResponsesFailoverTestContext(t, nil)

	handler.Responses(c)

	require.Equal(t, http.StatusBadGateway, recorder.Code)
	require.Empty(t, upstream.calls(), "凭据耗尽时不应打开上游请求")
}

func TestOpenAIResponsesOAuthCredentialCancellationDoesNotSwitch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	upstream := &openAIOAuthFailoverUpstream{}
	handler := newOpenAIOAuthFailoverHandler(t, upstream, openAIOAuthFailoverAccounts())
	c, recorder := newOpenAIResponsesFailoverTestContext(t, ctx)

	handler.Responses(c)

	require.Empty(t, upstream.calls(), "已取消请求不应触达上游或切换账号")
	require.Empty(t, recorder.Body.Len(), "已取消请求不应写入错误响应")
}
