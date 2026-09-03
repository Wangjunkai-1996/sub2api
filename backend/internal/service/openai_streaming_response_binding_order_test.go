package service

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type streamingResponseBindingOrderStore struct {
	OpenAIWSStateStore
	egress openAIWSEgressStateStore

	mu           sync.Mutex
	accountBinds int
	egressBinds  int
}

func newStreamingResponseBindingOrderStore() *streamingResponseBindingOrderStore {
	base := NewOpenAIWSStateStore(nil)
	return &streamingResponseBindingOrderStore{
		OpenAIWSStateStore: base,
		egress:             base.(openAIWSEgressStateStore),
	}
}

func (s *streamingResponseBindingOrderStore) BindResponseAccount(
	ctx context.Context,
	groupID int64,
	responseID string,
	accountID int64,
	ttl time.Duration,
) error {
	err := s.OpenAIWSStateStore.BindResponseAccount(ctx, groupID, responseID, accountID, ttl)
	if err == nil {
		s.mu.Lock()
		s.accountBinds++
		s.mu.Unlock()
	}
	return err
}

func (s *streamingResponseBindingOrderStore) BindResponseEgress(
	ctx context.Context,
	groupID int64,
	responseID string,
	bindingID string,
	ttl time.Duration,
) error {
	err := s.egress.BindResponseEgress(ctx, groupID, responseID, bindingID, ttl)
	if err == nil {
		s.mu.Lock()
		s.egressBinds++
		s.mu.Unlock()
	}
	return err
}

func (s *streamingResponseBindingOrderStore) GetResponseEgress(ctx context.Context, groupID int64, responseID string) (string, bool) {
	return s.egress.GetResponseEgress(ctx, groupID, responseID)
}

func (s *streamingResponseBindingOrderStore) BindSessionEgress(ctx context.Context, groupID int64, sessionHash, bindingID string, ttl time.Duration) error {
	return s.egress.BindSessionEgress(ctx, groupID, sessionHash, bindingID, ttl)
}

func (s *streamingResponseBindingOrderStore) GetSessionEgress(ctx context.Context, groupID int64, sessionHash string) (string, bool) {
	return s.egress.GetSessionEgress(ctx, groupID, sessionHash)
}

func (s *streamingResponseBindingOrderStore) DeleteSessionEgress(ctx context.Context, groupID int64, sessionHash string) error {
	return s.egress.DeleteSessionEgress(ctx, groupID, sessionHash)
}

func (s *streamingResponseBindingOrderStore) routingPairBound() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accountBinds > 0 && s.egressBinds > 0
}

func (s *streamingResponseBindingOrderStore) bindCounts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.accountBinds, s.egressBinds
}

type streamingResponseBindingOrderWriter struct {
	gin.ResponseWriter
	store                 *streamingResponseBindingOrderStore
	terminalBeforeBinding bool
}

func (w *streamingResponseBindingOrderWriter) Write(data []byte) (int, error) {
	if bytes.Contains(data, []byte(`"type":"response.completed"`)) && !w.store.routingPairBound() {
		w.terminalBeforeBinding = true
	}
	return w.ResponseWriter.Write(data)
}

func (w *streamingResponseBindingOrderWriter) WriteString(data string) (int, error) {
	return w.Write([]byte(data))
}

func TestOpenAIHTTPStreamingBindsRoutingPairBeforeCompletedEvent(t *testing.T) {
	tests := []struct {
		name string
		run  func(*OpenAIGatewayService, context.Context, *http.Response, *gin.Context, *Account) error
	}{
		{
			name: "standard",
			run: func(s *OpenAIGatewayService, ctx context.Context, resp *http.Response, c *gin.Context, account *Account) error {
				_, err := s.handleStreamingResponse(ctx, resp, c, account, time.Now(), "gpt-5", "gpt-5")
				return err
			},
		},
		{
			name: "passthrough",
			run: func(s *OpenAIGatewayService, ctx context.Context, resp *http.Response, c *gin.Context, account *Account) error {
				_, err := s.handleStreamingResponsePassthrough(ctx, resp, c, account, time.Now(), "gpt-5", "gpt-5")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/openai/v1/responses", nil)

			store := newStreamingResponseBindingOrderStore()
			writer := &streamingResponseBindingOrderWriter{ResponseWriter: c.Writer, store: store}
			c.Writer = writer
			svc := &OpenAIGatewayService{openaiWSStateStore: store}
			account := &Account{
				ID:       44001,
				Platform: PlatformOpenAI,
				Type:     AccountTypeOAuth,
				Name:     "binding-order",
				SelectedEgress: &ResolvedAccountEgress{
					BindingID: StableAccountEgressBindingID(44001, 71),
					RouteID:   71,
				},
			}
			stream := strings.Join([]string{
				`event: response.created`,
				`data: {"type":"response.created","response":{"id":"resp_binding_order","status":"in_progress"}}`,
				``,
				`event: response.output_text.delta`,
				`data: {"type":"response.output_text.delta","response_id":"resp_binding_order","delta":"ok"}`,
				``,
				`event: response.completed`,
				`data: {"type":"response.completed","response":{"id":"resp_binding_order","status":"completed","output":[{"type":"message"}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
				``,
			}, "\n")
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(stream)),
			}

			err := tt.run(svc, c.Request.Context(), resp, c, account)

			require.NoError(t, err)
			require.False(t, writer.terminalBeforeBinding)
			accountBinds, egressBinds := store.bindCounts()
			require.Equal(t, 1, accountBinds)
			require.Equal(t, 1, egressBinds)
			require.Contains(t, recorder.Body.String(), `"type":"response.completed"`)
		})
	}
}
