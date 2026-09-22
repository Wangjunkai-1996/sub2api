package service

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func runOpenAICompatStreamForTest(s *OpenAIGatewayService, endpoint string, buffered bool, c *gin.Context, resp *http.Response) (*OpenAIForwardResult, error) {
	account := &Account{ID: 41, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	if endpoint == "messages" {
		if buffered {
			return s.handleAnthropicBufferedStreamingResponse(resp, c, account, "gpt-5.4", "gpt-5.4", "gpt-5.4", time.Now())
		}
		return s.handleAnthropicStreamingResponse(resp, c, account, "gpt-5.4", "gpt-5.4", "gpt-5.4", time.Now())
	}
	if buffered {
		return s.handleChatBufferedStreamingResponse(resp, c, account, "gpt-5.4", "gpt-5.4", "gpt-5.4", time.Now())
	}
	return s.handleChatStreamingResponse(resp, c, account, "gpt-5.4", "gpt-5.4", "gpt-5.4", time.Now(), 0)
}

func TestOpenAICompatStreamReadFailoverBoundaries(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat", "messages"} {
		for _, tc := range []struct {
			name       string
			prefix     string
			readErr    error
			cancel     bool
			deadline   bool
			wantOutput bool
			wantCode   string
		}{
			{name: "eof", readErr: io.EOF, wantCode: OpenAIUpstreamStreamTruncatedCode},
			{name: "unexpected_eof", readErr: io.ErrUnexpectedEOF, wantCode: OpenAIUpstreamStreamReadErrorCode},
			{name: "http2_reset", readErr: errors.New("stream error: stream ID 7; INTERNAL_ERROR; received from peer"), wantCode: OpenAIUpstreamHTTP2StreamErrorCode},
			{name: "connection_reset", readErr: errors.New("read tcp: connection reset by peer"), wantCode: OpenAIUpstreamStreamReadErrorCode},
			{name: "canceled_request", readErr: io.ErrUnexpectedEOF, cancel: true},
			{name: "canceled_read", readErr: context.Canceled},
			{name: "deadline", readErr: context.DeadlineExceeded, wantCode: OpenAIUpstreamStreamReadErrorCode},
			{name: "line_limit", readErr: bufio.ErrTooLong, wantCode: OpenAIUpstreamStreamReadErrorCode},
			{name: "root_deadline", readErr: context.DeadlineExceeded, deadline: true},
			{name: "line_limit_root_deadline", readErr: bufio.ErrTooLong, deadline: true},
			{name: "response_limit", readErr: ErrUpstreamResponseBodyTooLarge},
			{name: "after_text", prefix: "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"partial\"}\n\n", readErr: io.ErrUnexpectedEOF, wantOutput: true},
			{name: "deadline_after_text", prefix: "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"partial\"}\n\n", readErr: context.DeadlineExceeded, wantOutput: true},
			{name: "line_limit_after_text", prefix: "data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"partial\"}\n\n", readErr: bufio.ErrTooLong, wantOutput: true},
			{name: "after_tool", prefix: "data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"test_tool\",\"arguments\":\"{}\"}}\n\n", readErr: io.ErrUnexpectedEOF, wantOutput: true},
		} {
			t.Run(endpoint+"/"+tc.name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tc.deadline {
					var deadlineCancel context.CancelFunc
					ctx, deadlineCancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
					defer deadlineCancel()
				}
				if tc.cancel {
					cancel()
				}
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/"+endpoint, nil).WithContext(ctx)
				resp := &http.Response{
					Header: http.Header{"X-Request-Id": []string{"first-attempt"}},
					Body:   io.NopCloser(io.MultiReader(strings.NewReader(tc.prefix), &openAICompatBufferedReadErrorCloser{err: tc.readErr})),
				}
				result, err := runOpenAICompatStreamForTest(&OpenAIGatewayService{}, endpoint, false, c, resp)
				require.Error(t, err)
				var failoverErr *UpstreamFailoverError
				if tc.wantCode != "" {
					require.ErrorAs(t, err, &failoverErr)
					require.Nil(t, result)
					require.Equal(t, tc.wantCode, gjson.GetBytes(failoverErr.ResponseBody, "error.code").String())
					require.Equal(t, "first-attempt", failoverErr.ResponseHeaders.Get("x-request-id"))
				} else {
					require.NotErrorAs(t, err, &failoverErr)
					require.NotNil(t, result)
				}
				require.Equal(t, tc.wantOutput, c.Writer.Written())
				if !tc.wantOutput {
					require.Empty(t, rec.Body.String())
				}
			})
		}
	}
}

type openAICompatHeartbeatTestWriter struct {
	gin.ResponseWriter
	written chan struct{}
	once    sync.Once
}

func (w *openAICompatHeartbeatTestWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	w.once.Do(func() { close(w.written) })
	return n, err
}

func TestOpenAICompatStreamHeartbeatPreservesFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat", "messages"} {
		for _, terminal := range []string{"read_error", "retryable_event", "request_error"} {
			t.Run(endpoint+"/"+terminal, func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/"+endpoint, nil)
				writer := &openAICompatHeartbeatTestWriter{ResponseWriter: c.Writer, written: make(chan struct{})}
				c.Writer = writer
				reader, upstream := io.Pipe()
				defer reader.Close()
				defer upstream.Close()
				go func() {
					select {
					case <-writer.written:
					case <-time.After(3 * time.Second):
						_ = upstream.CloseWithError(errors.New("heartbeat missing"))
						return
					}
					if terminal == "read_error" {
						_ = upstream.CloseWithError(io.ErrUnexpectedEOF)
						return
					}
					payload := `{"type":"response.failed","response":{"status":"failed","error":{"code":"server_is_overloaded","message":"overloaded"}}}`
					if terminal == "request_error" {
						payload = `{"type":"response.failed","response":{"status":"failed","error":{"type":"invalid_request_error","message":"invalid parameter"}}}`
					}
					_, _ = io.WriteString(upstream, "data: "+payload+"\n\n")
					_ = upstream.Close()
				}()
				svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{StreamKeepaliveInterval: 1}}}
				result, err := runOpenAICompatStreamForTest(svc, endpoint, false, c, &http.Response{Header: make(http.Header), Body: reader})
				require.Error(t, err)
				require.True(t, c.Writer.Written())
				var failoverErr *UpstreamFailoverError
				if terminal == "request_error" {
					require.NotErrorAs(t, err, &failoverErr)
					require.Contains(t, rec.Body.String(), "data: ")
					require.Contains(t, rec.Body.String(), "invalid parameter")
					require.NotContains(t, rec.Body.String(), "\n\n{", "a committed SSE stream must not receive bare JSON")
					return
				}
				require.ErrorAs(t, err, &failoverErr)
				require.Nil(t, result)
				require.Equal(t, -1, OpenAICompactKeepaliveAdjustedWrittenSize(c))
				require.NotContains(t, rec.Body.String(), "overloaded")
			})
		}
	}
}

func TestOpenAICompatReadIdleTimeoutFailsOver(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat", "messages"} {
		for _, buffered := range []bool{false, true} {
			name := endpoint + "/streaming"
			if buffered {
				name = endpoint + "/buffered"
			}
			t.Run(name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/"+endpoint, nil)
				reader, upstream := io.Pipe()
				defer reader.Close()
				defer upstream.Close()
				svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{StreamDataIntervalTimeout: 1}}}
				result, err := runOpenAICompatStreamForTest(svc, endpoint, buffered, c, &http.Response{Header: make(http.Header), Body: reader})
				var failoverErr *UpstreamFailoverError
				require.ErrorAs(t, err, &failoverErr)
				require.Nil(t, result)
				require.Equal(t, OpenAIUpstreamStreamReadErrorCode, gjson.GetBytes(failoverErr.ResponseBody, "error.code").String())
				require.Empty(t, rec.Body.String())
			})
		}
	}
}
