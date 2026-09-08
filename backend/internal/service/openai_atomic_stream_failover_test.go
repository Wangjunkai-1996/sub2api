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
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func newOpenAIAtomicStreamTestContext() (*gin.Context, *httptest.ResponseRecorder) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	return c, recorder
}

func newOpenAIAtomicStreamTestService(store OpenAIWSStateStore) *OpenAIGatewayService {
	return &OpenAIGatewayService{
		cfg: &config.Config{Gateway: config.GatewayConfig{
			OpenAIAtomicStreamFailover: true,
			MaxLineSize:                defaultMaxLineSize,
		}},
		openaiWSStateStore: store,
	}
}

func newOpenAIAtomicStreamTestAccount(id int64) *Account {
	return &Account{
		ID:          id,
		Name:        "atomic-stream-test",
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 1,
	}
}

func TestOpenAIAtomicStreamLateCapacityFailureIsSafeToReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := NewOpenAIWSStateStore(nil)
	svc := newOpenAIAtomicStreamTestService(store)
	c, recorder := newOpenAIAtomicStreamTestContext()
	account := newOpenAIAtomicStreamTestAccount(73001)
	const responseID = "resp_atomic_capacity"
	stream := strings.Join([]string{
		`event: response.created`,
		`data: {"type":"response.created","response":{"id":"` + responseID + `","status":"in_progress"}}`,
		``,
		`event: response.output_text.delta`,
		`data: {"type":"response.output_text.delta","response_id":"` + responseID + `","delta":"first-attempt-fragment"}`,
		``,
		`event: error`,
		`data: {"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}`,
		``,
		`event: response.failed`,
		`data: {"type":"response.failed","response":{"id":"` + responseID + `","status":"failed","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}`,
		``,
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": []string{"text/event-stream"},
			"X-Request-Id": []string{"rid-atomic-capacity"},
		},
		Body: io.NopCloser(strings.NewReader(stream)),
	}

	result, err := svc.handleStreamingResponse(c.Request.Context(), resp, c, account, time.Now(), "gpt-5", "gpt-5")

	require.Nil(t, result, "a replayable failed attempt must not return billable usage")
	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.True(t, failoverErr.RetryableOnSameAccount)
	require.True(t, failoverErr.RequestScopedTransient)
	require.False(t, failoverErr.ShouldReportAccountScheduleFailure())
	require.Empty(t, recorder.Body.String())
	require.False(t, c.Writer.Written())
	require.Empty(t, recorder.Header().Get("X-Request-Id"))
	boundAccountID, bindErr := store.GetResponseAccount(c.Request.Context(), 0, responseID)
	require.NoError(t, bindErr)
	require.Zero(t, boundAccountID, "a discarded attempt must not bind its response ID")
}

type openAIAtomicShortWriter struct {
	gin.ResponseWriter
	writes int
}

func (w *openAIAtomicShortWriter) Write(data []byte) (int, error) {
	w.writes++
	n, _ := w.ResponseWriter.Write(data[:len(data)/2])
	return n, io.ErrShortWrite
}

func TestOpenAIAtomicStreamPartialCommitCannotReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, recorder := newOpenAIAtomicStreamTestContext()
	writer := &openAIAtomicShortWriter{ResponseWriter: c.Writer}
	c.Writer = writer
	svc := newOpenAIAtomicStreamTestService(NewOpenAIWSStateStore(nil))
	stream := "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_partial\",\"status\":\"completed\",\"output\":[{\"type\":\"message\"}]}}\n\n" +
		"data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"overloaded\"}}}\n\n"
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(stream))}

	result, err := svc.handleStreamingResponse(c.Request.Context(), resp, c, newOpenAIAtomicStreamTestAccount(73004), time.Now(), "gpt-5", "gpt-5")

	require.Error(t, err)
	var failoverErr *UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr), "partial delivery must never be replayed")
	require.NotNil(t, result)
	require.NotEmpty(t, recorder.Body.String())
	require.Equal(t, 1, writer.writes, "a short write must not commit the stage again")
}

func TestOpenAIAtomicStreamIncompleteAttemptStaysPrivate(t *testing.T) {
	const preamble = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_incomplete\"}}\n\n"
	const delta = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"private-fragment\"}\n\n"
	for _, tc := range []struct {
		name     string
		readErr  error
		suffix   string
		canceled bool
	}{
		{name: "eof"},
		{name: "read_error", readErr: io.ErrUnexpectedEOF},
		{name: "canceled", readErr: context.Canceled, canceled: true},
		{name: "deadline", readErr: context.DeadlineExceeded, canceled: true},
		{name: "stage_limit", suffix: strings.Repeat(delta, openAIFirstOutputStageMaxBytes/len(delta)+1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, recorder := newOpenAIAtomicStreamTestContext()
			store := NewOpenAIWSStateStore(nil)
			svc := newOpenAIAtomicStreamTestService(store)
			body := io.NopCloser(strings.NewReader(preamble + delta + tc.suffix))
			if tc.readErr != nil {
				body = &openAIResponseFlushReadError{payload: []byte(preamble + delta), err: tc.readErr}
			}
			resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"X-Request-Id": []string{"private-request"}}, Body: body}

			result, err := svc.handleStreamingResponse(c.Request.Context(), resp, c, newOpenAIAtomicStreamTestAccount(73005), time.Now(), "gpt-5", "gpt-5")

			require.Nil(t, result)
			require.Error(t, err)
			var failoverErr *UpstreamFailoverError
			if tc.canceled {
				require.ErrorIs(t, err, tc.readErr)
				require.False(t, errors.As(err, &failoverErr))
			} else {
				require.ErrorAs(t, err, &failoverErr)
			}
			require.False(t, c.Writer.Written())
			require.Empty(t, recorder.Body.String())
			require.Empty(t, recorder.Header().Get("X-Request-Id"))
			boundAccount, bindErr := store.GetResponseAccount(c.Request.Context(), 0, "resp_incomplete")
			require.NoError(t, bindErr)
			require.Zero(t, boundAccount)
		})
	}
}

func TestOpenAIAtomicStreamIdleTimeoutStaysPrivate(t *testing.T) {
	c, recorder := newOpenAIAtomicStreamTestContext()
	svc := newOpenAIAtomicStreamTestService(NewOpenAIWSStateStore(nil))
	svc.cfg.Gateway.StreamDataIntervalTimeout = 1
	svc.cfg.Gateway.StreamKeepaliveInterval = 1
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	go func() {
		_, _ = io.WriteString(writer, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"private-fragment\"}\n\n")
	}()
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: reader}

	result, err := svc.handleStreamingResponse(c.Request.Context(), resp, c, newOpenAIAtomicStreamTestAccount(73006), time.Now(), "gpt-5", "gpt-5")

	require.Nil(t, result)
	var failoverErr *UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.False(t, c.Writer.Written())
	require.Empty(t, recorder.Body.String())
}

func TestOpenAIAtomicStreamCommitsOnlyAfterSuccessfulTerminal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := NewOpenAIWSStateStore(nil)
	svc := newOpenAIAtomicStreamTestService(store)
	c, recorder := newOpenAIAtomicStreamTestContext()
	account := newOpenAIAtomicStreamTestAccount(73002)
	const responseID = "resp_atomic_success"
	preTerminal := "event: response.created\n" +
		`data: {"type":"response.created","response":{"id":"` + responseID + `","status":"in_progress"}}` + "\n\n" +
		"event: response.output_text.delta\n" +
		`data: {"type":"response.output_text.delta","response_id":"` + responseID + `","delta":"complete-me"}` + "\n\n"
	terminal := "event: response.completed\n" +
		`data: {"type":"response.completed","response":{"id":"` + responseID + `","status":"completed","output":[{"type":"message"}],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}}` + "\n\n"

	reader, writer := io.Pipe()
	preTerminalWritten := make(chan struct{})
	releaseTerminal := make(chan struct{})
	go func() {
		_, _ = io.WriteString(writer, preTerminal)
		close(preTerminalWritten)
		<-releaseTerminal
		_, _ = io.WriteString(writer, terminal)
		_ = writer.Close()
	}()
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": []string{"text/event-stream"},
			"X-Request-Id": []string{"rid-atomic-success"},
		},
		Body: reader,
	}

	type outcome struct {
		result *openaiStreamingResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := svc.handleStreamingResponse(c.Request.Context(), resp, c, account, time.Now(), "gpt-5", "gpt-5")
		done <- outcome{result: result, err: err}
	}()

	<-preTerminalWritten
	require.Never(t, func() bool {
		return c.Writer.Written() || recorder.Body.Len() != 0
	}, 100*time.Millisecond, 10*time.Millisecond)
	close(releaseTerminal)

	var got outcome
	select {
	case got = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("atomic stream did not finish after the terminal event")
	}
	require.NoError(t, got.err)
	require.NotNil(t, got.result)
	require.Equal(t, preTerminal+terminal, recorder.Body.String())
	require.Equal(t, "rid-atomic-success", recorder.Result().Header.Get("X-Request-Id"))
	boundAccountID, bindErr := store.GetResponseAccount(c.Request.Context(), 0, responseID)
	require.NoError(t, bindErr)
	require.Equal(t, account.ID, boundAccountID)
}

func TestOpenAIAtomicStreamStillCommitsNonCapacityFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := newOpenAIAtomicStreamTestService(NewOpenAIWSStateStore(nil))
	c, recorder := newOpenAIAtomicStreamTestContext()
	account := newOpenAIAtomicStreamTestAccount(73003)
	stream := strings.Join([]string{
		`event: response.created`,
		`data: {"type":"response.created","response":{"id":"resp_atomic_invalid","status":"in_progress"}}`,
		``,
		`event: response.output_text.delta`,
		`data: {"type":"response.output_text.delta","delta":"useful-fragment"}`,
		``,
		`event: response.failed`,
		`data: {"type":"response.failed","response":{"id":"resp_atomic_invalid","status":"failed","error":{"type":"invalid_request_error","code":"invalid_request_error","message":"invalid input"}}}`,
		``,
	}, "\n")
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(stream)),
	}

	result, err := svc.handleStreamingResponse(c.Request.Context(), resp, c, account, time.Now(), "gpt-5", "gpt-5")

	require.NotNil(t, result)
	require.Error(t, err)
	var failoverErr *UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr))
	require.True(t, c.Writer.Written())
	require.Contains(t, recorder.Body.String(), "useful-fragment")
	require.Contains(t, recorder.Body.String(), `"type":"response.failed"`)
	require.Contains(t, recorder.Body.String(), "invalid input")
}
