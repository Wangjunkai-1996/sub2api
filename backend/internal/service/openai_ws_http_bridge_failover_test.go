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
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestProxyOpenAIWSHTTPBridgeTurnLaterTurnFailoverBoundaries(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const created = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_second\"}}\n\n"
	const privateReasoning = "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"rs_second\",\"type\":\"reasoning\",\"encrypted_content\":\"private-second-turn\",\"summary\":[]}}\n\n"
	const output = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
	const overloaded = "data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_second\",\"status\":\"failed\",\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"Please try again later\"}}}\n\n"

	for _, tc := range []struct {
		name          string
		status        int
		body          string
		transportErr  error
		readErr       error
		cancelContext bool
		wantFailover  bool
		wantErr       bool
		wantWrites    []string
	}{
		{
			name:         "http_503",
			status:       http.StatusServiceUnavailable,
			body:         `{"error":{"type":"server_error","message":"temporarily unavailable"}}`,
			wantFailover: true,
			wantErr:      true,
		},
		{
			name:         "transport_error",
			transportErr: io.ErrUnexpectedEOF,
			wantFailover: true,
			wantErr:      true,
		},
		{
			name:         "staged_created_then_eof",
			body:         created,
			wantFailover: true,
			wantErr:      true,
		},
		{
			name:         "staged_created_then_overloaded",
			body:         created + overloaded,
			wantFailover: true,
			wantErr:      true,
		},
		{
			name:       "encrypted_reasoning_then_overloaded",
			body:       created + privateReasoning + overloaded,
			wantWrites: []string{"response.created", "response.output_item.added", "response.failed"},
		},
		{
			name:          "canceled_transport",
			transportErr:  context.Canceled,
			cancelContext: true,
			wantErr:       true,
		},
		{
			name:    "canceled_stream_read",
			body:    created,
			readErr: context.Canceled,
			wantErr: true,
		},
		{
			name:       "semantic_output_then_eof",
			body:       created + output,
			wantErr:    true,
			wantWrites: []string{"response.created", "response.output_text.delta"},
		},
		{
			name:       "semantic_output_then_overloaded",
			body:       created + output + overloaded,
			wantWrites: []string{"response.created", "response.output_text.delta", "response.failed"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelContext {
				cancel()
			}
			status := tc.status
			if status == 0 {
				status = http.StatusOK
			}
			var reader io.Reader = strings.NewReader(tc.body)
			if tc.readErr != nil {
				reader = io.MultiReader(reader, passthroughErrReadCloser{err: tc.readErr})
			}
			upstream := &httpUpstreamRecorder{
				err: tc.transportErr,
				resp: &http.Response{
					StatusCode: status,
					Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
					Body:       io.NopCloser(reader),
				},
			}
			svc := &OpenAIGatewayService{cfg: &config.Config{}, httpUpstream: upstream}
			account := &Account{ID: 151, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Concurrency: 1}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil).WithContext(ctx)
			payload := []byte(`{"type":"response.create","model":"gpt-5.6-sol","input":[{"role":"user","content":"second turn"}]}`)
			var writes []string

			result, err := svc.proxyOpenAIWSHTTPBridgeTurn(
				ctx, c, account, "access-token", payload, len(payload),
				"gpt-5.6-sol", "", "", "", "", 2,
				func(message []byte) error {
					writes = append(writes, gjson.GetBytes(message, "type").String())
					return nil
				},
			)

			require.Equal(t, tc.wantWrites, writes)
			var failoverErr *UpstreamFailoverError
			require.Equal(t, tc.wantFailover, errors.As(err, &failoverErr), "error: %v", err)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if len(tc.wantWrites) == 0 {
				require.Nil(t, result)
			} else {
				require.NotNil(t, result)
			}
			if tc.transportErr == context.Canceled || tc.readErr == context.Canceled {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.Len(t, upstream.requests, 1)
		})
	}
}

func TestOpenAIWSHTTPBridgeLaterTurnUnknownPreviousResponseDoesNotReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.Enabled = true
	cfg.Gateway.OpenAIWS.OAuthEnabled = true
	cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
	cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader("data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_first\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"first-ok\"}]}]}}\n\n")),
		},
		{
			StatusCode: http.StatusServiceUnavailable,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"server_error","message":"temporarily unavailable"}}`)),
		},
	}}
	svc := &OpenAIGatewayService{
		cfg:              cfg,
		httpUpstream:     upstream,
		cache:            &stubGatewayCache{},
		openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
	}
	account := &Account{
		ID: 151, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		Extra: map[string]any{"openai_oauth_responses_websockets_v2_mode": OpenAIWSIngressModeHTTPBridge},
	}
	proxyErrCh := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			proxyErrCh <- err
			return
		}
		defer conn.CloseNow()
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = r
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		_, first, err := conn.Read(ctx)
		if err != nil {
			proxyErrCh <- err
			return
		}
		proxyErrCh <- svc.ProxyResponsesWebSocketFromClient(ctx, c, conn, account, "access-token", first, nil)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	require.NoError(t, err)
	defer client.CloseNow()
	require.NoError(t, client.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"gpt-5.6-sol","input":[{"role":"user","content":"first"}]}`)))
	_, first, err := client.Read(ctx)
	require.NoError(t, err)
	require.Equal(t, "resp_first", gjson.GetBytes(first, "response.id").String())
	require.NoError(t, client.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"gpt-5.6-sol","previous_response_id":"resp_unknown","input":[{"role":"user","content":"second"}]}`)))

	select {
	case proxyErr := <-proxyErrCh:
		var failoverErr *UpstreamFailoverError
		require.ErrorAs(t, proxyErr, &failoverErr)
		retryPayload, retryCurrentTurn := OpenAIWSCurrentTurnRetryPayload(proxyErr)
		require.True(t, retryCurrentTurn)
		require.Nil(t, retryPayload, "unobserved continuation history cannot be moved to another account")
	case <-ctx.Done():
		t.Fatal("timed out waiting for unsafe continuation failover")
	}
	require.Len(t, upstream.requests, 2)
	require.Contains(t, string(upstream.bodies[1]), "second")
}
