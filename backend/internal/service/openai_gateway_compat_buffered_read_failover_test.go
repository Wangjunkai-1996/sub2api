package service

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type openAICompatBufferedReadErrorCloser struct {
	err error
}

func (r *openAICompatBufferedReadErrorCloser) Read([]byte) (int, error) { return 0, r.err }
func (r *openAICompatBufferedReadErrorCloser) Close() error             { return nil }

func TestChatCompletionsBufferedResponsesReadErrorReturnsFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)

	readErrors := []struct {
		name         string
		err          error
		expectedCode string
	}{
		{name: "unexpected_eof", err: io.ErrUnexpectedEOF, expectedCode: OpenAIUpstreamStreamReadErrorCode},
		{name: "http2_reset", err: errors.New("stream error: stream ID 7; INTERNAL_ERROR; received from peer"), expectedCode: OpenAIUpstreamHTTP2StreamErrorCode},
		{name: "attempt_deadline", err: context.DeadlineExceeded, expectedCode: OpenAIUpstreamStreamReadErrorCode},
	}

	for _, readError := range readErrors {
		t.Run(readError.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "X-Request-Id": []string{"upstream-rid"}},
				Body:       &openAICompatBufferedReadErrorCloser{err: readError.err},
			}
			account := &Account{ID: 40, Name: "openai-oauth", Platform: PlatformOpenAI}

			result, err := (&OpenAIGatewayService{}).handleChatBufferedStreamingResponse(
				resp, c, account, "gpt-5.6-sol", "gpt-5.6-sol", "gpt-5.6-sol", time.Now(),
			)

			require.Error(t, err)
			require.Nil(t, result)
			var failoverErr *UpstreamFailoverError
			require.ErrorAs(t, err, &failoverErr)
			require.Equal(t, http.StatusBadGateway, failoverErr.StatusCode)
			require.Equal(t, "upstream-rid", failoverErr.ResponseHeaders.Get("x-request-id"))
			require.Equal(t, readError.expectedCode, gjson.GetBytes(failoverErr.ResponseBody, "error.code").String())
			require.Empty(t, rec.Body.String())
			require.False(t, c.Writer.Written())
		})
	}
}

func TestChatCompletionsBufferedResponsesReadErrorDoesNotFailoverAfterClientCancel(t *testing.T) {
	gin.SetMode(gin.TestMode)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	requestContext, cancel := context.WithCancel(context.Background())
	cancel()
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(requestContext)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       &openAICompatBufferedReadErrorCloser{err: io.ErrUnexpectedEOF},
	}

	result, err := (&OpenAIGatewayService{}).handleChatBufferedStreamingResponse(
		resp,
		c,
		&Account{ID: 40, Name: "openai-oauth", Platform: PlatformOpenAI},
		"gpt-5.6-sol",
		"gpt-5.6-sol",
		"gpt-5.6-sol",
		time.Now(),
	)

	require.Error(t, err)
	require.Nil(t, result)
	var failoverErr *UpstreamFailoverError
	require.NotErrorAs(t, err, &failoverErr)
	require.Empty(t, rec.Body.String())
}

func TestChatCompletionsBufferedResponsesOversizedLineReturnsFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       &openAICompatBufferedReadErrorCloser{err: bufio.ErrTooLong},
	}

	result, err := (&OpenAIGatewayService{}).handleChatBufferedStreamingResponse(
		resp,
		c,
		&Account{ID: 40, Name: "openai-oauth", Platform: PlatformOpenAI},
		"gpt-5.6-sol",
		"gpt-5.6-sol",
		"gpt-5.6-sol",
		time.Now(),
	)

	require.Nil(t, result)
	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.Equal(t, OpenAIUpstreamStreamReadErrorCode, gjson.GetBytes(failoverErr.ResponseBody, "error.code").String())
	require.Empty(t, rec.Body.String())
	require.False(t, c.Writer.Written())
}

func TestOpenAICompatBufferedAttemptFailureRequiresLiveRoot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, endpoint := range []string{"chat", "messages"} {
		for _, tc := range []struct {
			name     string
			err      error
			deadline bool
			failover bool
		}{
			{name: "attempt_deadline", err: context.DeadlineExceeded, failover: true},
			{name: "line_limit", err: bufio.ErrTooLong, failover: true},
			{name: "root_deadline", err: context.DeadlineExceeded, deadline: true},
			{name: "line_limit_root_deadline", err: bufio.ErrTooLong, deadline: true},
			{name: "canceled_read", err: context.Canceled},
			{name: "response_limit", err: ErrUpstreamResponseBodyTooLarge},
		} {
			t.Run(endpoint+"/"+tc.name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				ctx := context.Background()
				if tc.deadline {
					var cancel context.CancelFunc
					ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
					defer cancel()
				}
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/"+endpoint, nil).WithContext(ctx)
				resp := &http.Response{Header: make(http.Header), Body: &openAICompatBufferedReadErrorCloser{err: tc.err}}
				result, err := runOpenAICompatStreamForTest(&OpenAIGatewayService{}, endpoint, true, c, resp)
				require.Nil(t, result)
				var failoverErr *UpstreamFailoverError
				if tc.failover {
					require.ErrorAs(t, err, &failoverErr)
				} else {
					require.NotErrorAs(t, err, &failoverErr)
					require.ErrorIs(t, err, tc.err)
				}
				require.Empty(t, rec.Body.String())
				require.False(t, c.Writer.Written())
			})
		}
	}
}

func TestOpenAICompatAttemptFailureWithoutRootDoesNotFailover(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, bufio.ErrTooLong} {
		err := (&OpenAIGatewayService{}).newOpenAICompatReadError(nil, nil, nil, "", cause, true)
		var failoverErr *UpstreamFailoverError
		require.NotErrorAs(t, err, &failoverErr)
		require.ErrorIs(t, err, cause)
	}
}

func TestAnthropicBufferedResponsesReadErrorReturnsFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       &openAICompatBufferedReadErrorCloser{err: io.ErrUnexpectedEOF},
	}

	result, err := (&OpenAIGatewayService{}).handleAnthropicBufferedStreamingResponse(
		resp,
		c,
		&Account{ID: 40, Name: "openai-oauth", Platform: PlatformOpenAI},
		"gpt-5.6-sol",
		"gpt-5.6-sol",
		"gpt-5.6-sol",
		time.Now(),
	)

	require.Nil(t, result)
	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.Equal(t, OpenAIUpstreamStreamReadErrorCode, gjson.GetBytes(failoverErr.ResponseBody, "error.code").String())
	require.Empty(t, rec.Body.String())
	require.False(t, c.Writer.Written())
}
