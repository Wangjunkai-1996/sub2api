//go:build unit

package handler

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// Keep the production Forward/handler flow and use actual HTTP connections for
// reads, framing errors and cancellation. Only the test destination is replaced.
type responsesLocalHTTPUpstream struct {
	service.HTTPUpstream
	client *http.Client
	url    *url.URL
}

func (u *responsesLocalHTTPUpstream) Do(req *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	forward := req.Clone(req.Context())
	destination := *u.url
	destination.Path = "/" + strconv.FormatInt(accountID, 10)
	forward.URL = &destination
	forward.Host = destination.Host
	return u.client.Do(forward)
}

func TestOpenAIResponsesRealHTTPFailover(t *testing.T) {
	const encrypted = `{"type":"response.output_item.done","item":{"type":"reasoning","encrypted_content":"losing-ciphertext","summary":[]}}`
	const failure = "data: {\"type\":\"error\",\"error\":{\"type\":\"upstream_error\",\"code\":\"stream_read_error\",\"message\":\"stream_read_error\"}}\n\n"
	for _, passthrough := range []bool{false, true} {
		for _, tc := range []struct {
			name, prefix string
			truncated    bool
			replay       bool
		}{
			{"encrypted_error", "data: " + encrypted + "\n\n", false, true},
			{"encrypted_transport_disconnect", "data: " + encrypted + "\n\n", true, true},
			{"text_committed", "data: {\"type\":\"response.output_text.delta\",\"delta\":\"delivered\"}\n\n", false, false},
			{"unknown_reasoning", "data: " + strings.Replace(encrypted, `"summary":[]`, `"summary":[],"future":"opaque"`, 1) + "\n\n", false, false},
		} {
			t.Run(fmt.Sprintf("passthrough_%t/%s", passthrough, tc.name), func(t *testing.T) {
				var mu sync.Mutex
				var calls []string
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					mu.Lock()
					calls = append(calls, r.URL.Path)
					mu.Unlock()
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					w.Header().Set("X-Request-Id", "account"+r.URL.Path)
					if r.URL.Path == "/3" {
						_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"winner\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"winner-id\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n")
						return
					}
					body := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"losing-id\"}}\n\n" + tc.prefix
					if tc.truncated {
						w.Header().Set("Content-Length", strconv.Itoa(len(body)+100))
						_, _ = io.WriteString(w, body)
						w.(http.Flusher).Flush()
						return
					}
					_, _ = io.WriteString(w, body+failure)
				}))
				defer upstream.Close()
				destination, err := url.Parse(upstream.URL)
				require.NoError(t, err)
				accounts := make([]service.Account, 3)
				for i := range accounts {
					accounts[i] = service.Account{
						ID: int64(i + 1), Name: fmt.Sprint(i + 1), Platform: service.PlatformOpenAI,
						Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Priority: i,
						Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"},
						Extra:       map[string]any{"openai_passthrough": passthrough},
					}
				}
				h := newOpenAIFailoverTestHandlerWithAccounts(t, &responsesLocalHTTPUpstream{client: upstream.Client(), url: destination}, accounts)
				h.cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 30
				c, rec := newOpenAIResponsesFailoverTestContext(t, nil)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.1","stream":true,"input":"hello"}`))
				c.Request.Header.Set("Content-Type", "application/json")
				h.Responses(c)
				mu.Lock()
				actual := append([]string(nil), calls...)
				mu.Unlock()
				if tc.replay {
					require.Equal(t, []string{"/1", "/2", "/3"}, actual, rec.Body.String())
					require.Contains(t, rec.Body.String(), `"delta":"winner"`)
					require.NotContains(t, rec.Body.String(), "losing")
					require.NotContains(t, rec.Body.String(), "stream_read_error")
					require.Equal(t, "account/3", rec.Result().Header.Get("X-Request-Id"))
				} else {
					require.Equal(t, []string{"/1"}, actual)
					require.NotContains(t, rec.Body.String(), `"delta":"winner"`)
				}
			})
		}
	}
}
