//go:build unit

package handler

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type nonstreamTransportFailoverUpstream struct {
	service.HTTPUpstream
	client *http.Client
	target *url.URL
	mode   string
	cancel context.CancelFunc
	mu     sync.Mutex
	ids    []int64
}

func (u *nonstreamTransportFailoverUpstream) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	u.mu.Lock()
	u.ids = append(u.ids, accountID)
	u.mu.Unlock()
	if accountID == 1 {
		switch u.mode {
		case "unsent":
			return nil, service.MarkHTTPUpstreamRequestNotSent(errors.New("proxy connection setup failed"))
		case "sent":
			httptrace.ContextClientTrace(req.Context()).WroteRequest(httptrace.WroteRequestInfo{Err: io.ErrUnexpectedEOF})
			return nil, service.MarkHTTPUpstreamRequestNotSent(errors.New("inconsistent setup acknowledgement"))
		case "timeout":
			return nil, context.DeadlineExceeded
		case "canceled":
			u.cancel()
			return nil, service.MarkHTTPUpstreamRequestNotSent(errors.New("proxy connection setup failed"))
		}
	}
	forward := req.Clone(req.Context())
	forward.URL.Scheme = u.target.Scheme
	forward.URL.Host = u.target.Host
	forward.Header.Set("X-Test-Account", strconv.FormatInt(accountID, 10))
	return u.client.Do(forward)
}

func TestOpenAINonstreamHandlerTransportFailover_RequiresUnsentProof(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, entry := range []struct {
		name, endpoint, body, response, contentType, want string
		oauth                                             bool
		winnerCalls                                       int
		handle                                            func(*OpenAIGatewayHandler, *gin.Context)
	}{
		{name: "embeddings", endpoint: "/v1/embeddings", body: `{"model":"text-embedding-3-small","input":"hello"}`,
			response: `{"data":[{"embedding":[0.125]}],"usage":{"prompt_tokens":1}}`, want: "0.125", winnerCalls: 1, handle: (*OpenAIGatewayHandler).Embeddings},
		{name: "input tokens", endpoint: "/v1/responses/input_tokens", body: `{"model":"gpt-5.1","input":"hello"}`,
			response: `{"input_tokens":7}`, want: `"input_tokens":7`, winnerCalls: 1, handle: (*OpenAIGatewayHandler).ResponsesInputTokens},
		{name: "count tokens", endpoint: "/v1/messages/count_tokens", body: `{"model":"gpt-5.1","messages":[{"role":"user","content":"hello"}]}`,
			response: `{"input_tokens":7}`, want: `"input_tokens":7`, winnerCalls: 1, handle: (*OpenAIGatewayHandler).CountTokens},
		{name: "apikey images", endpoint: "/v1/images/generations", body: `{"model":"gpt-image-2","prompt":"test"}`,
			response: `{"created":1,"data":[{"b64_json":"d2lubmVy"}]}`, want: "d2lubmVy", winnerCalls: 1, handle: (*OpenAIGatewayHandler).Images},
		{name: "oauth image", endpoint: "/v1/images/generations", body: `{"model":"gpt-image-2","prompt":"test"}`, oauth: true,
			response: "data: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"image_generation_call\",\"result\":\"d2lubmVy\"}]}}\n\n", contentType: "text/event-stream", want: "d2lubmVy", winnerCalls: 1, handle: (*OpenAIGatewayHandler).Images},
		{name: "oauth image batch", endpoint: "/v1/images/generations", body: `{"model":"gpt-image-2","prompt":"test","n":2}`, oauth: true,
			response: "data: {\"type\":\"response.completed\",\"response\":{\"output\":[{\"type\":\"image_generation_call\",\"result\":\"d2lubmVy\"}]}}\n\n", contentType: "text/event-stream", want: "d2lubmVy", winnerCalls: 2, handle: (*OpenAIGatewayHandler).Images},
	} {
		for _, mode := range []string{"unsent", "sent", "timeout", "body_read", "canceled"} {
			t.Run(entry.name+"/"+mode, func(t *testing.T) {
				var receivedMu sync.Mutex
				var received []string
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					receivedMu.Lock()
					received = append(received, r.Header.Get("X-Test-Account"))
					receivedMu.Unlock()
					if r.Header.Get("X-Test-Account") == "1" {
						w.Header().Set("Content-Length", "100")
						_, _ = io.WriteString(w, "{")
						return
					}
					w.Header().Set("Content-Type", entry.contentType)
					_, _ = io.WriteString(w, entry.response)
				}))
				defer server.Close()
				target, err := url.Parse(server.URL)
				require.NoError(t, err)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				upstream := &nonstreamTransportFailoverUpstream{mode: mode, client: server.Client(), target: target, cancel: cancel}
				accounts := make([]service.Account, 2)
				for i := range accounts {
					accounts[i] = service.Account{ID: int64(i + 1), Name: fmt.Sprintf("nonstream-%d", i+1),
						Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
						Status: service.StatusActive, Schedulable: true, Priority: i,
						Credentials: map[string]any{"api_key": "sk-test", "base_url": "https://api.openai.com"}}
					if entry.oauth {
						accounts[i].Type = service.AccountTypeOAuth
						accounts[i].Credentials = map[string]any{"access_token": "token-test"}
					}
				}
				h := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
				c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
				c.Request = httptest.NewRequest(http.MethodPost, entry.endpoint, strings.NewReader(entry.body)).WithContext(ctx)
				c.Request.Header.Set("Content-Type", "application/json")
				apiKey, _ := middleware2.GetAPIKeyFromContext(c)
				apiKey.Group.AllowImageGeneration = true
				apiKey.Group.AllowMessagesDispatch = true
				entry.handle(h, c)

				upstream.mu.Lock()
				calls := append([]int64(nil), upstream.ids...)
				upstream.mu.Unlock()
				receivedMu.Lock()
				onWire := append([]string(nil), received...)
				receivedMu.Unlock()
				if mode == "unsent" {
					wantCalls := []int64{1}
					var wantWire []string
					for i := 0; i < entry.winnerCalls; i++ {
						wantCalls = append(wantCalls, 2)
						wantWire = append(wantWire, "2")
					}
					require.Equal(t, wantCalls, calls, rec.Body.String())
					require.Equal(t, wantWire, onWire, "the winning account must actually receive the request")
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
					require.Contains(t, rec.Body.String(), entry.want)
					require.NotContains(t, rec.Body.String(), `"error"`)
				} else {
					require.Equal(t, []int64{1}, calls, rec.Body.String())
					require.NotContains(t, onWire, "2", "an ambiguous or already-sent request must not reach another account")
					if mode == "canceled" {
						require.ErrorIs(t, c.Request.Context().Err(), context.Canceled)
					} else {
						require.Equal(t, http.StatusBadGateway, rec.Code, rec.Body.String())
					}
					require.NotContains(t, rec.Body.String(), entry.want)
				}
			})
		}
	}
}
