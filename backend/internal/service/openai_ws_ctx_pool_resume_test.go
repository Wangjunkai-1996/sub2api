package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIWSCtxPoolLaterTurnFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name       string
		second     string
		wantReplay bool
		oauth      bool
		transport  string
		noFailover bool
	}{
		{
			name:       "replacement_receives_only_current_turn",
			second:     `{"type":"response.create","model":"gpt-5.1","store":true,"input":[{"role":"user","content":"second"}]}`,
			wantReplay: true,
		},
		{
			name:       "read_eof_retries_same_account_once_then_current_turn_on_replacement",
			second:     `{"type":"response.create","model":"gpt-5.1","store":true,"input":[{"role":"user","content":"second"}]}`,
			wantReplay: true,
			transport:  "eof",
		},
		{
			name:       "read_reset_retries_same_account_once_then_current_turn_on_replacement",
			second:     `{"type":"response.create","model":"gpt-5.1","store":true,"input":[{"role":"user","content":"second"}]}`,
			wantReplay: true,
			transport:  "reset",
		},
		{
			name:       "read_timeout_retries_current_turn_on_replacement",
			second:     `{"type":"response.create","model":"gpt-5.1","store":true,"input":[{"role":"user","content":"second"}]}`,
			wantReplay: true,
			transport:  "timeout",
		},
		{
			name:       "canceled_root_stops_without_replacement",
			second:     `{"type":"response.create","model":"gpt-5.1","store":true,"input":[{"role":"user","content":"second"}]}`,
			transport:  "canceled",
			noFailover: true,
		},
		{
			name:       "read_eof_after_output_stops_without_replacement",
			second:     `{"type":"response.create","model":"gpt-5.1","store":true,"input":[{"role":"user","content":"second"}]}`,
			transport:  "after_output",
			noFailover: true,
		},
		{
			name:       "replacement_reapplies_own_identity",
			second:     `{"type":"response.create","model":"gpt-5.1","store":true,"prompt_cache_key":"client-session","client_metadata":{"session_id":"client-session","thread_id":"client-thread"},"input":[{"role":"user","content":"second"}]}`,
			wantReplay: true,
			oauth:      true,
		},
		{
			name:   "opaque_previous_response_stops_without_replacement",
			second: `{"type":"response.create","model":"gpt-5.1","store":true,"previous_response_id":"opaque","input":[{"role":"user","content":"second"}]}`,
		},
		{
			name:   "orphan_tool_output_stops_without_replacement",
			second: `{"type":"response.create","model":"gpt-5.1","store":true,"input":[{"type":"function_call_output","call_id":"missing_call","output":"second"}]}`,
		},
		{
			name:   "item_reference_stops_without_replacement",
			second: `{"type":"response.create","model":"gpt-5.1","store":true,"input":[{"type":"item_reference","id":"remote_item"}]}`,
		},
		{
			name:   "encrypted_content_stops_without_replacement",
			second: `{"type":"response.create","model":"gpt-5.1","store":true,"input":[{"type":"reasoning","encrypted_content":"opaque"}]}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.Enabled = true
			cfg.Gateway.OpenAIWS.APIKeyEnabled = true
			cfg.Gateway.OpenAIWS.OAuthEnabled = true
			cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
			cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
			cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
			cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
			cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
			cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
			cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3

			accountAConn := &openAIWSCaptureConn{events: [][]byte{
				[]byte(`{"type":"response.completed","response":{"id":"resp_first","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
				[]byte(`{"type":"response.failed","response":{"id":"resp_failed_second","status":"failed","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}`),
			}}
			accountBConn := &openAIWSCaptureConn{events: [][]byte{
				[]byte(`{"type":"response.completed","response":{"id":"resp_second","model":"gpt-5.1","usage":{"input_tokens":1,"output_tokens":1}}}`),
			}}
			dialer := &openAIWSQueueDialer{conns: []openAIWSClientConn{accountAConn, accountBConn}}
			var retryAConn *openAIWSCaptureConn
			switch tc.transport {
			case "eof", "reset":
				accountAConn.events = accountAConn.events[:1]
				retryAConn = &openAIWSCaptureConn{}
				endErr := io.EOF
				if tc.transport == "reset" {
					endErr = errors.New("connection reset by peer")
				}
				dialer.conns = []openAIWSClientConn{
					&openAIWSResumeErrorConn{openAIWSCaptureConn: accountAConn, endErr: endErr},
					&openAIWSResumeErrorConn{openAIWSCaptureConn: retryAConn, endErr: endErr},
					accountBConn,
				}
			case "timeout", "canceled":
				cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 1
				accountAConn.readDelays = []time.Duration{0, 4 * time.Second}
			case "after_output":
				accountAConn.events[1] = []byte(`{"type":"response.output_text.delta","delta":"partial-second"}`)
			}
			pool := newOpenAIWSConnPool(cfg)
			pool.setClientDialerForTest(dialer)
			defer pool.Close()
			svc := &OpenAIGatewayService{
				cfg:              cfg,
				httpUpstream:     &httpUpstreamRecorder{},
				cache:            &stubGatewayCache{},
				openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
				toolCorrector:    NewCodexToolCorrector(),
				openaiWSPool:     pool,
			}
			accountA := &Account{
				ID: 114, Name: "ctx-pool-account-a", Platform: PlatformOpenAI,
				Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1,
				Credentials: map[string]any{"api_key": "sk-account-a"},
				Extra:       map[string]any{"responses_websockets_v2_enabled": true},
			}
			accountB := *accountA
			accountB.ID = 115
			accountB.Name = "ctx-pool-account-b"
			accountB.Credentials = map[string]any{"api_key": "sk-account-b"}
			if tc.oauth {
				accountA.Type = AccountTypeOAuth
				accountA.Credentials = map[string]any{"chatgpt_account_id": "account-a", "chatgpt_user_id": "user-a"}
				accountB.Type = AccountTypeOAuth
				accountB.Credentials = map[string]any{"chatgpt_account_id": "account-b", "chatgpt_user_id": "user-b"}
			}

			failoverCh := make(chan error, 1)
			serverErrCh := make(chan error, 1)
			wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					serverErrCh <- err
					return
				}
				defer func() { _ = conn.CloseNow() }()
				ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
				defer cancel()
				_, firstMessage, err := conn.Read(ctx)
				if err != nil {
					serverErrCh <- err
					return
				}
				ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
				ginCtx.Request = r.Clone(ctx)
				var hooks *OpenAIWSIngressHooks
				if tc.transport == "canceled" {
					hooks = &OpenAIWSIngressHooks{BeforeTurn: func(turn int) error {
						if turn == 2 {
							cancel()
						}
						return nil
					}}
				}
				proxyErr := svc.ProxyResponsesWebSocketFromClient(ctx, ginCtx, conn, accountA, "sk-account-a", firstMessage, hooks)
				failoverCh <- proxyErr
				retryPayload, retryCurrentTurn := OpenAIWSCurrentTurnRetryPayload(proxyErr)
				if !retryCurrentTurn || len(retryPayload) == 0 {
					serverErrCh <- nil
					return
				}
				serverErrCh <- svc.ProxyResponsesWebSocketFromClient(ctx, ginCtx, conn, &accountB, "sk-account-b", retryPayload, nil)
			}))
			defer wsServer.Close()

			dialCtx, cancelDial := context.WithTimeout(context.Background(), 3*time.Second)
			clientConn, _, err := coderws.Dial(dialCtx, "ws"+strings.TrimPrefix(wsServer.URL, "http"), nil)
			cancelDial()
			require.NoError(t, err)
			defer func() { _ = clientConn.CloseNow() }()
			writeMessage := func(payload string) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				require.NoError(t, clientConn.Write(ctx, coderws.MessageText, []byte(payload)))
			}
			readMessage := func() []byte {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				messageType, message, err := clientConn.Read(ctx)
				require.NoError(t, err)
				require.Equal(t, coderws.MessageText, messageType)
				return message
			}

			writeMessage(`{"type":"response.create","model":"gpt-5.1","store":true,"input":[{"role":"user","content":"first"}]}`)
			firstCompleted := readMessage()
			require.Equal(t, "response.completed", gjson.GetBytes(firstCompleted, "type").String())
			require.Equal(t, "resp_first", gjson.GetBytes(firstCompleted, "response.id").String())
			writeMessage(tc.second)

			select {
			case proxyErr := <-failoverCh:
				var failoverErr *UpstreamFailoverError
				retryPayload, retryCurrentTurn := OpenAIWSCurrentTurnRetryPayload(proxyErr)
				if tc.noFailover {
					require.Error(t, proxyErr)
					require.False(t, errors.As(proxyErr, &failoverErr))
					require.False(t, retryCurrentTurn)
					if tc.transport == "canceled" {
						require.ErrorIs(t, proxyErr, context.Canceled)
					}
				} else {
					require.ErrorAs(t, proxyErr, &failoverErr)
					require.True(t, failoverErr.ShouldRetryNextAccount())
					require.True(t, retryCurrentTurn, "later-turn errors must never fall back to the first message")
				}
				if tc.wantReplay {
					require.JSONEq(t, tc.second, string(retryPayload))
				} else {
					require.Nil(t, retryPayload, "a turn without full replay context cannot move to another account")
				}
			case <-time.After(6 * time.Second):
				t.Fatal("timed out waiting for second-turn failover")
			}

			if tc.wantReplay {
				secondCompleted := readMessage()
				require.Equal(t, "response.completed", gjson.GetBytes(secondCompleted, "type").String())
				require.Equal(t, "resp_second", gjson.GetBytes(secondCompleted, "response.id").String())
				_ = clientConn.Close(coderws.StatusNormalClosure, "done")
			}
			select {
			case serverErr := <-serverErrCh:
				require.NoError(t, serverErr)
			case <-time.After(6 * time.Second):
				t.Fatal("timed out waiting for websocket proxy completion")
			}

			accountAConn.mu.Lock()
			writesA := append([]map[string]any(nil), accountAConn.writes...)
			accountAConn.mu.Unlock()
			accountBConn.mu.Lock()
			writesB := append([]map[string]any(nil), accountBConn.writes...)
			accountBConn.mu.Unlock()
			require.Len(t, writesA, 2, "account A must receive the first and second turns")
			require.Equal(t, []any{map[string]any{"role": "user", "content": "first"}}, writesA[0]["input"])
			require.Equal(t, gjson.Get(tc.second, "input").Value(), writesA[1]["input"])
			if tc.wantReplay {
				require.Len(t, writesB, 1, "account B must receive exactly one response.create")
				require.Equal(t, "response.create", writesB[0]["type"])
				require.Equal(t, []any{map[string]any{"role": "user", "content": "second"}}, writesB[0]["input"])
				require.NotContains(t, writesB[0], "previous_response_id")
				wantDials := 2
				if retryAConn != nil {
					wantDials++
					retryAConn.mu.Lock()
					retryWrites := append([]map[string]any(nil), retryAConn.writes...)
					retryAConn.mu.Unlock()
					require.Len(t, retryWrites, 1)
					require.Equal(t, writesA[1]["input"], retryWrites[0]["input"])
				}
				require.Equal(t, wantDials, dialer.DialCount())
				if tc.oauth {
					for key, kind := range map[string]string{"session_id": "session", "thread_id": "thread"} {
						raw := gjson.Get(tc.second, "client_metadata."+key).String()
						require.Equal(t, scopeCodexAccountIdentityValue(accountA, 0, kind, raw), writesA[1]["client_metadata"].(map[string]any)[key])
						require.Equal(t, scopeCodexAccountIdentityValue(&accountB, 0, kind, raw), writesB[0]["client_metadata"].(map[string]any)[key])
					}
				}
			} else {
				if previousID := gjson.Get(tc.second, "previous_response_id").String(); previousID != "" {
					require.Equal(t, previousID, writesA[1]["previous_response_id"])
				}
				require.Empty(t, writesB, "unsafe continuation must not be dispatched to account B")
				require.Equal(t, 1, dialer.DialCount())
			}
		})
	}
}

type openAIWSResumeErrorConn struct {
	*openAIWSCaptureConn
	endErr error
}

func (c *openAIWSResumeErrorConn) ReadMessage(ctx context.Context) ([]byte, error) {
	message, err := c.openAIWSCaptureConn.ReadMessage(ctx)
	if errors.Is(err, io.EOF) {
		return nil, c.endErr
	}
	return message, err
}
