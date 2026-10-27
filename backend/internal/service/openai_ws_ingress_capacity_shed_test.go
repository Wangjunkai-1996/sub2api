package service

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// openAIWSIngressCapacityShedRepo 补齐错误状态写入，避免非容量类错误
// 走到账号状态副作用时打空指针。
type openAIWSIngressCapacityShedRepo struct {
	stubOpenAIAccountRepo
}

func (r *openAIWSIngressCapacityShedRepo) SetError(context.Context, int64, string) error { return nil }

func (r *openAIWSIngressCapacityShedRepo) SetRateLimited(context.Context, int64, time.Time) error {
	return nil
}

func (r *openAIWSIngressCapacityShedRepo) UpdateExtra(context.Context, int64, map[string]any) error {
	return nil
}

// ctx_pool 的 ingress 直写路径把 error / response.failed 交给 WS 客户端前，必须和
// HTTP/SSE（openai_gateway_response_handling.go）与 http_bridge
// （openai_ws_http_bridge.go）两条路径一样，把容量降载码改写为可重试的
// server_error：Codex 按闭集判定，server_is_overloaded / slow_down 属致命集，
// 客户端会打印 "Selected model is at capacity" 并直接终止会话而不是退避重试。
//
// 其余用例锁住改写范围和时机：非容量类错误码必须原样下发；已有语义输出后
// 的容量错误不能再 failover，但仍要把致命容量码改写成客户端可重试的 server_error。
func TestProxyResponsesWebSocketFromClient_RewritesCapacityShedCodeForClient(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name           string
		upstreamEvents [][]byte
		wantContains   []string
		wantAbsent     []string
		wantFailover   bool
		responseID     string
		serverErrCount int
	}{
		{
			name: "capacity_shed_after_preamble_fails_over_before_client_output",
			upstreamEvents: [][]byte{
				[]byte(`{"type":"response.created","response":{"id":"resp_shed","status":"in_progress"}}`),
				[]byte(`{"type":"response.failed","response":{"id":"resp_shed","status":"failed","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}`),
			},
			wantAbsent:   []string{"response.created", "server_is_overloaded"},
			wantFailover: true,
			responseID:   "resp_shed",
		},
		{
			name: "capacity_shed_after_json_heartbeats_fails_over_before_client_output",
			upstreamEvents: [][]byte{
				[]byte(`{"type":"response.created","response":{"id":"resp_heartbeat","status":"in_progress"}}`),
				[]byte(`{"type":"keepalive"}`),
				[]byte(`{"type":"ping"}`),
				[]byte(`{"type":"keepalive","timestamp":1789608992,"sequence_number":3}`),
				[]byte(`{"type":"response.failed","response":{"id":"resp_heartbeat","status":"failed","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}`),
			},
			wantAbsent:   []string{"response.created", "keepalive", "ping", "server_is_overloaded"},
			wantFailover: true,
			responseID:   "resp_heartbeat",
		},
		{
			name: "keepalive_with_output_does_not_allow_replay",
			upstreamEvents: [][]byte{
				[]byte(`{"type":"response.created","response":{"id":"resp_heartbeat_output","status":"in_progress"}}`),
				[]byte(`{"type":"keepalive","delta":"hello"}`),
				[]byte(`{"type":"response.failed","response":{"id":"resp_heartbeat_output","status":"failed","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}`),
			},
			wantContains:   []string{`"delta":"hello"`, `"code":"server_error"`},
			wantAbsent:     []string{"server_is_overloaded"},
			responseID:     "resp_heartbeat_output",
			serverErrCount: 1,
		},
		{
			name: "non_retryable_policy_error_is_passed_through",
			upstreamEvents: [][]byte{
				[]byte(`{"type":"error","error":{"type":"invalid_request_error","code":"content_policy_violation","message":"request blocked by content policy"}}`),
				[]byte(`{"type":"response.failed","response":{"id":"resp_policy","status":"failed","error":{"type":"invalid_request_error","code":"content_policy_violation","message":"request blocked by content policy"}}}`),
			},
			wantContains: []string{
				`"code":"content_policy_violation"`,
				"request blocked by content policy",
			},
			wantAbsent: []string{"server_error"},
			responseID: "resp_policy",
		},
		{
			name: "capacity_shed_after_semantic_output_is_rewritten_without_failover",
			upstreamEvents: [][]byte{
				[]byte(`{"type":"response.created","response":{"id":"resp_shed_after_output","status":"in_progress"}}`),
				[]byte(`{"type":"response.output_text.delta","response_id":"resp_shed_after_output","delta":"hello"}`),
				[]byte(`{"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}`),
				[]byte(`{"type":"response.failed","response":{"id":"resp_shed_after_output","status":"failed","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}`),
			},
			wantContains: []string{
				`"type":"response.output_text.delta"`,
				`"code":"server_error"`,
			},
			wantAbsent:     []string{"server_is_overloaded"},
			responseID:     "resp_shed_after_output",
			serverErrCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newOpenAIWSV2TestConfig()
			cfg.Security.URLAllowlist.Enabled = false
			cfg.Security.URLAllowlist.AllowInsecureHTTP = true
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
			cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
			cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
			cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
			cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
			cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
			cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3

			events := make([][]byte, 0, len(tt.upstreamEvents))
			for _, event := range tt.upstreamEvents {
				events = append(events, append([]byte(nil), event...))
			}
			captureConn := &openAIWSCaptureConn{events: events}
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(&openAIWSCaptureDialer{conn: captureConn})

			account := Account{
				ID:          5401,
				Name:        "openai-ingress-capacity-shed",
				Platform:    PlatformOpenAI,
				Type:        AccountTypeAPIKey,
				Status:      StatusActive,
				Schedulable: true,
				Concurrency: 1,
				Credentials: map[string]any{"api_key": "sk-test"},
				Extra:       map[string]any{"responses_websockets_v2_enabled": true},
			}
			account.SelectedEgress = &ResolvedAccountEgress{
				BindingID: StableAccountEgressBindingID(account.ID, 93),
				RouteID:   93,
			}
			store := newStreamingResponseBindingOrderStore()
			repo := &openAIWSIngressCapacityShedRepo{stubOpenAIAccountRepo: stubOpenAIAccountRepo{accounts: []Account{account}}}
			svc := &OpenAIGatewayService{
				accountRepo:        repo,
				rateLimitService:   &RateLimitService{accountRepo: repo},
				httpUpstream:       &httpUpstreamRecorder{},
				cache:              &stubGatewayCache{},
				cfg:                cfg,
				openaiWSResolver:   NewOpenAIWSProtocolResolver(cfg),
				toolCorrector:      NewCodexToolCorrector(),
				openaiWSPool:       pool,
				openaiWSStateStore: store,
			}

			serverResult := make(chan error, 1)
			wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
				if err != nil {
					serverResult <- err
					return
				}
				defer func() { _ = conn.CloseNow() }()

				rec := httptest.NewRecorder()
				ginCtx, _ := gin.CreateTestContext(rec)
				req := r.Clone(r.Context())
				req.Header = req.Header.Clone()
				req.Header.Set("User-Agent", "unit-test-agent/1.0")
				ginCtx.Request = req

				readCtx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
				msgType, firstMessage, readErr := conn.Read(readCtx)
				cancel()
				if readErr != nil || (msgType != coderws.MessageText && msgType != coderws.MessageBinary) {
					serverResult <- readErr
					return
				}
				serverResult <- svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, &account, "sk-test", firstMessage, nil)
			}))
			defer wsServer.Close()

			dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
			clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(wsServer.URL, "http"), nil)
			cancelDial()
			require.NoError(t, err)
			defer func() { _ = clientConn.CloseNow() }()

			writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
			err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","stream":false}`))
			cancelWrite()
			require.NoError(t, err)

			var frames []string
			for len(frames) < len(tt.upstreamEvents) {
				readCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				_, message, readErr := clientConn.Read(readCtx)
				cancel()
				if readErr != nil {
					break
				}
				frames = append(frames, string(message))
			}
			// 本轮已终止，主动断开客户端让 ingress 退出 turn 循环。
			_ = clientConn.CloseNow()

			joined := strings.Join(frames, "\n")
			if tt.wantFailover {
				require.Empty(t, frames, "首个语义输出前的前导事件不得泄露给客户端")
			} else {
				require.NotEmpty(t, frames, "客户端应至少收到一个下发事件")
			}
			for _, want := range tt.wantContains {
				require.Contains(t, joined, want, "客户端收到的事件:\n%s", joined)
			}
			for _, absent := range tt.wantAbsent {
				require.NotContains(t, joined, absent, "客户端收到的事件:\n%s", joined)
			}
			if tt.serverErrCount > 0 {
				require.Equal(t, tt.serverErrCount, strings.Count(joined, `"code":"server_error"`), "客户端收到的事件:\n%s", joined)
			}

			select {
			case proxyErr := <-serverResult:
				if tt.wantFailover {
					var failoverErr *UpstreamFailoverError
					require.ErrorAs(t, proxyErr, &failoverErr)
					require.True(t, failoverErr.RetryableOnSameAccount)
					require.True(t, failoverErr.RequestScopedTransient)
				} else {
					require.NoError(t, proxyErr)
					boundAccountID, bindErr := store.GetResponseAccount(context.Background(), 0, tt.responseID)
					require.NoError(t, bindErr)
					require.Equal(t, account.ID, boundAccountID)
					boundEgress, ok := getOpenAIWSResponseEgress(store, context.Background(), 0, tt.responseID)
					require.True(t, ok)
					require.Equal(t, account.SelectedEgress.BindingID, boundEgress)
					_, connBound := store.GetResponseConn(tt.responseID)
					require.True(t, connBound)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("等待 ingress websocket 结束超时")
			}
		})
	}
}

func TestProxyResponsesWebSocketFromClient_MarksCyberPolicyBeforeEarlyReturn(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name          string
		upstreamEvent []byte
		wantFailover  bool
		wantClientMsg bool
		wantInput     int
		wantOutput    int
	}{
		{
			name:          "error_before_rate_limit_failover",
			upstreamEvent: []byte(`{"type":"error","error":{"type":"rate_limit_error","code":"cyber_policy","message":"rate limit exceeded by cyber policy"},"usage":{"input_tokens":5,"output_tokens":1}}`),
			wantFailover:  true,
			wantInput:     5,
			wantOutput:    1,
		},
		{
			name:          "response_failed_terminal",
			upstreamEvent: []byte(`{"type":"response.failed","response":{"id":"resp_cyber","status":"failed","error":{"code":"cyber_policy","message":"blocked by cyber policy"},"usage":{"input_tokens":9,"output_tokens":2}}}`),
			wantClientMsg: true,
			wantInput:     9,
			wantOutput:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newOpenAIWSV2TestConfig()
			cfg.Security.URLAllowlist.Enabled = false
			cfg.Security.URLAllowlist.AllowInsecureHTTP = true
			cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
			cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0

			captureConn := &openAIWSCaptureConn{events: [][]byte{append([]byte(nil), tt.upstreamEvent...)}}
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(&openAIWSCaptureDialer{conn: captureConn})
			svc := &OpenAIGatewayService{
				cfg:              cfg,
				httpUpstream:     &httpUpstreamRecorder{},
				cache:            &stubGatewayCache{},
				openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
				toolCorrector:    NewCodexToolCorrector(),
				openaiWSPool:     pool,
			}
			account := &Account{
				ID: 5402, Name: "openai-ingress-cyber", Platform: PlatformOpenAI,
				Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1,
				Credentials: map[string]any{"api_key": "sk-test"},
				Extra: map[string]any{
					"openai_apikey_responses_websockets_v2_mode": OpenAIWSIngressModeCtxPool,
				},
			}

			markCh := make(chan *CyberPolicyMark, 1)
			serverErrCh := make(chan error, 1)
			wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, &coderws.AcceptOptions{CompressionMode: coderws.CompressionContextTakeover})
				if err != nil {
					serverErrCh <- err
					return
				}
				defer func() { _ = conn.CloseNow() }()

				readCtx, cancelRead := context.WithTimeout(r.Context(), 3*time.Second)
				_, firstMessage, err := conn.Read(readCtx)
				cancelRead()
				if err != nil {
					serverErrCh <- err
					return
				}

				recorder := httptest.NewRecorder()
				ginCtx, _ := gin.CreateTestContext(recorder)
				ginCtx.Request = r.Clone(r.Context())
				hooks := &OpenAIWSIngressHooks{AfterTurn: func(_ int, _ *OpenAIForwardResult, _ error) {
					markCh <- GetOpsCyberPolicy(ginCtx)
				}}
				serverErrCh <- svc.ProxyResponsesWebSocketFromClient(r.Context(), ginCtx, conn, account, "sk-test", firstMessage, hooks)
			}))
			defer wsServer.Close()

			dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
			clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(wsServer.URL, "http"), nil)
			cancelDial()
			require.NoError(t, err)
			defer func() { _ = clientConn.CloseNow() }()

			writeCtx, cancelWrite := context.WithTimeout(context.Background(), 3*time.Second)
			err = clientConn.Write(writeCtx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.1","stream":false}`))
			cancelWrite()
			require.NoError(t, err)

			if tt.wantClientMsg {
				readCtx, cancelRead := context.WithTimeout(context.Background(), 3*time.Second)
				_, message, readErr := clientConn.Read(readCtx)
				cancelRead()
				require.NoError(t, readErr)
				require.Equal(t, "response.failed", gjson.GetBytes(message, "type").String())
			}

			select {
			case mark := <-markCh:
				require.NotNil(t, mark)
				require.Equal(t, "cyber_policy", mark.Code)
				require.Equal(t, tt.wantInput, mark.UpstreamInTok)
				require.Equal(t, tt.wantOutput, mark.UpstreamOutTok)
			case <-time.After(3 * time.Second):
				t.Fatal("AfterTurn did not observe the cyber mark")
			}

			_ = clientConn.CloseNow()
			select {
			case serverErr := <-serverErrCh:
				var failoverErr *UpstreamFailoverError
				require.Equal(t, tt.wantFailover, errors.As(serverErr, &failoverErr))
			case <-time.After(5 * time.Second):
				t.Fatal("waiting for ingress websocket shutdown timed out")
			}
		})
	}
}
