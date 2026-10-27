package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type openAITTFTCommentFailingWriter struct {
	gin.ResponseWriter
	failOnMarker bool
}

func (w *openAITTFTCommentFailingWriter) Write(data []byte) (int, error) {
	if strings.HasPrefix(string(data), ": sub2-ttft-ms=") {
		if w.failOnMarker {
			n, _ := w.ResponseWriter.Write(data[:len(data)/2])
			return n, io.ErrClosedPipe
		}
		return w.ResponseWriter.Write(data)
	}
	return 0, io.ErrClosedPipe
}

func (w *openAITTFTCommentFailingWriter) WriteString(data string) (int, error) {
	return w.Write([]byte(data))
}

func TestOpenAITTFTCommentWriteFailureDoesNotRetryAndDrainsUsage(t *testing.T) {
	const output = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n"
	for _, passthrough := range []bool{false, true} {
		for _, failed := range []bool{false, true} {
			for _, failOnMarker := range []bool{false, true} {
				t.Run(fmt.Sprintf("passthrough=%t/failed=%t/marker=%t", passthrough, failed, failOnMarker), func(t *testing.T) {
					terminal := "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n"
					if failed {
						terminal = "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"overloaded\"},\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n"
					}
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
					c.Request.Header.Set("X-Sub2-TTFT", "1")
					c.Writer = &openAITTFTCommentFailingWriter{ResponseWriter: c.Writer, failOnMarker: failOnMarker}
					svc := &OpenAIGatewayService{cfg: &config.Config{}}
					resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(output + terminal))}
					account := newOpenAIStreamTestAccount(73021)
					var usage *OpenAIUsage
					var err error
					if passthrough {
						var result *openaiStreamingResultPassthrough
						result, err = svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "test", "test")
						require.NotNil(t, result)
						usage = result.usage
					} else {
						var result *openaiStreamingResult
						result, err = svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "test", "test")
						require.NotNil(t, result)
						usage = result.usage
					}
					var failover *UpstreamFailoverError
					require.False(t, errors.As(err, &failover), "a committed measurement must never be replayed: %v", err)
					if failed {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
					require.NotEmpty(t, rec.Body.String())
					require.Equal(t, 2, usage.InputTokens)
					require.Equal(t, 1, usage.OutputTokens)
				})
			}
		}
	}
}

func TestOpenAITTFTCommentResponsesCommit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	createdAtField := regexp.MustCompile(`,"created_at":[0-9]+`)
	const metadata = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_ttft\"}}\n\n" +
		"data: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"reasoning\",\"summary\":[]}}\n\n"
	const output = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n"
	const terminal = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ttft\",\"usage\":{\"input_tokens\":2,\"output_tokens\":1},\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\"}]}]}}\n\n"
	const failed = "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"overloaded\"}}}\n\n"
	for _, path := range []string{"native", "passthrough"} {
		for _, failure := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/failure=%t", path, failure), func(t *testing.T) {
				recorder := newOpenAIResponseFlushRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				c.Request.Header.Set("X-Sub2-TTFT", "1")
				account := newOpenAIStreamTestAccount(73020)
				svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{}}}
				waiting, release := make(chan struct{}), make(chan struct{})
				defer close(release)
				initial, remaining := metadata, output+terminal

				if failure {
					remaining = failed
				}
				reader := &stagedOpenAISSEReadCloser{
					segments: [][]byte{[]byte(initial), []byte(remaining)},
					gates:    []<-chan struct{}{nil, release}, waiting: []chan struct{}{nil, waiting},
				}
				type outcome struct {
					ms  *int
					err error
				}
				done := make(chan outcome, 1)
				forward := func(body io.ReadCloser) (*int, error) {
					resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: body}
					started := time.Now().Add(-1180 * time.Millisecond)
					if path == "passthrough" {
						result, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, started, "test", "test")
						if result == nil {
							return nil, err
						}
						return result.firstTokenMs, err
					}
					result, err := svc.handleStreamingResponse(context.Background(), resp, c, account, started, "test", "test")
					if result == nil {
						return nil, err
					}
					return result.firstTokenMs, err
				}
				go func() { ms, err := forward(reader); done <- outcome{ms, err} }()
				select {
				case <-waiting:
				case <-time.After(2 * time.Second):
					t.Fatal("upstream did not reach the output gate")
				}
				body, flushes := recorder.snapshot()
				require.Empty(t, body, "recording TTFT must not commit a private attempt")
				require.Empty(t, flushes)
				release <- struct{}{}
				var result outcome
				select {
				case result = <-done:
				case <-time.After(2 * time.Second):
					t.Fatal("stream did not finish")
				}
				if failure {
					var failover *UpstreamFailoverError
					require.ErrorAs(t, result.err, &failover)
					body, _ = recorder.snapshot()
					require.Empty(t, body, "a losing attempt must not export its timing")
					require.False(t, c.Writer.Written())
					result.ms, result.err = forward(io.NopCloser(strings.NewReader(metadata + output + terminal)))
				}
				require.NoError(t, result.err)
				require.NotNil(t, result.ms)
				body, _ = recorder.snapshot()
				marker := fmt.Sprintf(": sub2-ttft-ms=%d\n\n", *result.ms)
				require.Equal(t, 1, strings.Count(body, ": sub2-ttft-ms="))
				require.Contains(t, body, marker)
				bodyWithoutMarker := strings.Replace(body, marker, "", 1)
				if path == "native" {
					require.Regexp(t, `"created_at":[0-9]+`, bodyWithoutMarker)
				}
				require.Equal(t, createdAtField.ReplaceAllString(metadata+output+terminal, ""), createdAtField.ReplaceAllString(bodyWithoutMarker, ""))
				normalizedBody := createdAtField.ReplaceAllString(body, "")
				normalizedTerminal := createdAtField.ReplaceAllString(terminal, "")
				require.Less(t, strings.Index(normalizedBody, marker), strings.Index(normalizedBody, normalizedTerminal))
			})
		}
	}
}

func TestOpenAITTFTCommentNegotiation(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	ms := 1180
	require.Empty(t, openAITTFTComment(c, &ms))
	c.Request.Header.Set("X-Sub2-TTFT", "1")
	require.Equal(t, ": sub2-ttft-ms=1180\n\n", openAITTFTComment(c, &ms))
	require.Empty(t, openAITTFTComment(c, nil))
	ms = -1
	require.Empty(t, openAITTFTComment(c, &ms))
	ms = 0
	require.Equal(t, ": sub2-ttft-ms=0\n\n", openAITTFTComment(c, &ms))
}

func TestOpenAITTFTCommentVisibleAfterStructuralOutput(t *testing.T) {
	previous := gatewayForwardingCache.Load()
	gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: OpenAITTFTModeVisible, expiresAt: time.Now().Add(time.Minute).UnixNano()})
	t.Cleanup(func() {
		if previous != nil {
			gatewayForwardingCache.Store(previous)
		} else {
			gatewayForwardingCache.Store(&cachedGatewayForwardingSettings{openAITTFTMode: OpenAITTFTModeSemantic})
		}
	})
	const initial = "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"search_1\",\"type\":\"web_search_call\",\"status\":\"in_progress\"}}\n\n"
	const terminalData = "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_late\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\"}]}],\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}"
	for _, passthrough := range []bool{false, true} {
		for _, terminalOnly := range []bool{false, true} {
			for _, eof := range []bool{false, true} {
				t.Run(fmt.Sprintf("passthrough=%t/terminalOnly=%t/eof=%t", passthrough, terminalOnly, eof), func(t *testing.T) {
					upstream := initial
					if !terminalOnly {
						upstream += "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n"
					}
					upstream += "event: response.completed\n" + terminalData
					if !eof {
						upstream += "\n\n"
					}
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
					c.Request.Header.Set("X-Sub2-TTFT", "1")
					svc := &OpenAIGatewayService{cfg: &config.Config{}}
					resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(upstream))}
					account := &Account{ID: 1, Platform: PlatformOpenAI}
					var ms *int
					if passthrough {
						result, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "test", "test")
						require.NoError(t, err)
						ms = result.firstTokenMs
					} else {
						result, err := svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "test", "test")
						require.NoError(t, err)
						ms = result.firstTokenMs
					}
					require.NotNil(t, ms)
					marker := fmt.Sprintf(": sub2-ttft-ms=%d\n", *ms)
					body := rec.Body.String()
					require.Equal(t, 1, strings.Count(body, marker))
					require.Less(t, strings.Index(body, marker), strings.Index(body, terminalData))
					require.Equal(t, strings.TrimRight(upstream, "\n"), strings.TrimRight(strings.Replace(body, marker, "", 1), "\n"), "the comment must preserve event framing and data")
				})
			}
		}
	}
}

func newOpenAIStreamTestAccount(id int64) *Account {
	return &Account{
		ID:          id,
		Name:        "stream-test",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
	}
}

func TestOpenAIStreamsFirstSemanticOutputBeforeTerminal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		t.Run(fmt.Sprintf("passthrough=%t", passthrough), func(t *testing.T) {
			recorder := newOpenAIResponseFlushRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			waiting, release := make(chan struct{}), make(chan struct{})
			defer close(release)
			body := &stagedOpenAISSEReadCloser{
				segments: [][]byte{
					[]byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"visible-now\"}\n\n"),
					[]byte("data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}}\n\n"),
				},
				gates: []<-chan struct{}{nil, release}, waiting: []chan struct{}{nil, waiting},
			}
			resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: body}
			svc := &OpenAIGatewayService{cfg: &config.Config{}}
			done := make(chan error, 1)
			go func() {
				account := newOpenAIStreamTestAccount(73022)
				if passthrough {
					_, err := svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "test", "test")
					done <- err
				} else {
					_, err := svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "test", "test")
					done <- err
				}
			}()
			require.Eventually(t, func() bool {
				output, flushes := recorder.snapshot()
				return strings.Contains(output, "visible-now") && len(flushes) > 0
			}, 2*time.Second, time.Millisecond)
			select {
			case <-done:
				t.Fatal("stream finished before upstream terminal was released")
			default:
			}
			release <- struct{}{}
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("stream did not finish after upstream terminal")
			}
		})
	}
}
