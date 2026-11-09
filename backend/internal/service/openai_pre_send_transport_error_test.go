package service

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"syscall"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIPreSendState_AllowsOnlyProvenSetupFailures(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request, _ = http.NewRequest(http.MethodPost, "http://gateway.test", nil)

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "explicit adapter proof", err: MarkHTTPUpstreamRequestNotSent(errors.New("proxy pool exhausted")), want: true},
		{name: "plugin did not send", err: &PluginTransportError{Code: "CONNECT_FAILED", Message: "before upstream", RequestSent: false}, want: true},
		{name: "dial refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, want: true},
		{name: "typed dial timeout", err: &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("i/o timeout")}, want: false},
		{name: "typed DNS timeout", err: &net.DNSError{Err: "i/o timeout", IsTimeout: true}, want: false},
		{name: "read error wrapping refused", err: &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNREFUSED}, want: false},
		{name: "untyped refusal", err: errors.New("connection refused"), want: false},
		{name: "untyped timeout", err: context.DeadlineExceeded, want: false},
		{name: "response body eof", err: errors.New("unexpected EOF"), want: false},
		{name: "plugin sent", err: &PluginTransportError{Code: "UPSTREAM_RESET", Message: "after write", RequestSent: true}, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := &openAIPreSendState{}
			require.Equal(t, tt.want, state.mayFailover(context.Background(), c, tt.err))
		})
	}
}

func TestOpenAIPreSendState_TraceEvidenceWinsOverErrorText(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request, _ = http.NewRequest(http.MethodPost, "http://gateway.test", nil)
	state := &openAIPreSendState{}
	state.requestStarted.Store(true)
	require.False(t, state.mayFailover(context.Background(), c, MarkHTTPUpstreamRequestNotSent(errors.New("proxy pool exhausted"))))
}

func TestOpenAIPreSendState_RejectsCanceledRootContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	root, cancel := context.WithCancel(context.Background())
	c.Request, _ = http.NewRequestWithContext(root, http.MethodPost, "http://gateway.test", nil)
	cancel()
	state := &openAIPreSendState{}
	require.False(t, state.mayFailover(root, c, MarkHTTPUpstreamRequestNotSent(errors.New("proxy pool exhausted"))))
}

func TestOpenAIPreSendForwarding_ReturnsFailoverWithoutCommittingResponse(t *testing.T) {
	type forward func(*OpenAIGatewayService, context.Context, *gin.Context, *Account, []byte) error
	entries := []struct {
		name, endpoint, body string
		oauth                bool
		forward              forward
	}{
		{
			name: "embeddings", endpoint: "/v1/embeddings", body: `{"model":"text-embedding-3-small","input":"hello"}`,
			forward: func(s *OpenAIGatewayService, ctx context.Context, c *gin.Context, a *Account, body []byte) error {
				_, err := s.ForwardEmbeddings(ctx, c, a, body, "")
				return err
			},
		},
		{
			name: "native input tokens", endpoint: "/v1/responses/input_tokens", body: `{"model":"gpt-5.1","input":"hello"}`,
			forward: (*OpenAIGatewayService).ForwardResponsesInputTokens,
		},
		{
			name: "anthropic count tokens", endpoint: "/v1/messages/count_tokens", body: `{"model":"gpt-5.1","messages":[{"role":"user","content":"hello"}]}`,
			forward: func(s *OpenAIGatewayService, ctx context.Context, c *gin.Context, a *Account, body []byte) error {
				return s.ForwardCountTokensAsAnthropic(ctx, c, a, body, "")
			},
		},
	}
	for _, image := range []struct {
		name, body string
		oauth      bool
	}{
		{name: "apikey images", body: `{"model":"gpt-image-2","prompt":"test"}`},
		{name: "oauth image", body: `{"model":"gpt-image-2","prompt":"test"}`, oauth: true},
		{name: "oauth batch", body: `{"model":"gpt-image-2","prompt":"test","n":2}`, oauth: true},
	} {
		entries = append(entries, struct {
			name, endpoint, body string
			oauth                bool
			forward              forward
		}{
			name: image.name, endpoint: "/v1/images/generations", body: image.body, oauth: image.oauth,
			forward: func(s *OpenAIGatewayService, ctx context.Context, c *gin.Context, a *Account, body []byte) error {
				parsed, err := s.ParseOpenAIImagesRequest(c, body)
				if err != nil {
					return err
				}
				_, err = s.ForwardImages(ctx, c, a, body, parsed, "")
				return err
			},
		})
	}
	for _, entry := range entries {
		for _, tc := range []struct {
			name      string
			err       error
			write     bool
			cancel    bool
			wantRetry bool
		}{
			{name: "DNS", err: &net.DNSError{Err: "no such host", Name: "upstream.test", IsNotFound: true}, wantRetry: true},
			{name: "connect refused", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, wantRetry: true},
			{name: "proxy setup", err: &net.OpError{Op: "proxyconnect", Net: "tcp", Err: errors.New("proxy authentication required")}, wantRetry: true},
			{name: "plugin unsent", err: &PluginTransportError{Code: "CONNECT_TIMEOUT"}, wantRetry: true},
			{name: "plugin sent", err: &PluginTransportError{Code: "CONNECT_FAILED", RequestSent: true}},
			{name: "unknown timeout", err: context.DeadlineExceeded},
			{name: "unknown EOF", err: errors.New("unexpected EOF")},
			{name: "partial request sent", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, write: true},
			{name: "client canceled", err: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, cancel: true},
		} {
			t.Run(entry.name+"/"+tc.name, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, entry.endpoint, nil).WithContext(ctx)
				c.Request.Header.Set("Content-Type", "application/json")
				upstream := &httpUpstreamRecorder{err: tc.err}
				upstream.onDo = func() {
					if tc.write {
						httptrace.ContextClientTrace(upstream.lastReq.Context()).WroteHeaders()
					}
					if tc.cancel {
						cancel()
					}
				}
				svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
				account := &Account{ID: 10, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true,
					Credentials: map[string]any{"api_key": "sk-test", "base_url": "https://api.openai.com"}}
				if entry.oauth {
					account.Type = AccountTypeOAuth
					account.Credentials = map[string]any{"access_token": "test-token"}
				}
				err := entry.forward(svc, ctx, c, account, []byte(entry.body))
				require.Error(t, err)
				var failoverErr *UpstreamFailoverError
				require.Equal(t, tc.wantRetry, errors.As(err, &failoverErr), "error: %v", err)
				require.Len(t, upstream.requests, 1, "service must leave retries to the handler")
				if tc.wantRetry {
					require.False(t, c.Writer.Written())
					require.Empty(t, rec.Body.String(), "a 502 must not commit before the handler can switch accounts")
					require.False(t, failoverErr.RetryableOnSameAccount)
				}
			})
		}
	}
}
