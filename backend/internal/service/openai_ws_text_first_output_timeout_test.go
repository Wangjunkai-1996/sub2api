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
)

func newWSFirstOutputTestService(conn *openAIWSCaptureConn) (*OpenAIGatewayService, *Account) {
	cfg := newOpenAIWSV2TestConfig()
	cfg.Security.URLAllowlist.Enabled = false
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 1
	cfg.Gateway.OpenAIHighEffortFirstOutputTimeoutSeconds = 3
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 5
	cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
	pool := newOpenAIWSConnPool(cfg)
	pool.setClientDialerForTest(&openAIWSCaptureDialer{conn: conn})
	return &OpenAIGatewayService{
			cfg: cfg, httpUpstream: &httpUpstreamRecorder{}, cache: &stubGatewayCache{},
			openaiWSResolver: NewOpenAIWSProtocolResolver(cfg), toolCorrector: NewCodexToolCorrector(), openaiWSPool: pool,
		}, &Account{
			ID: 8851, Name: "ws-first-output", Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
			Status: StatusActive, Schedulable: true, Concurrency: 1,
			Credentials: map[string]any{"api_key": "sk-test"},
			Extra:       map[string]any{"responses_websockets_v2_enabled": true},
		}
}

func TestWSV2TextFirstOutputTimeoutIgnoresMetadataAndEmptyDelta(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "nonstream", true: "stream"}[stream], func(t *testing.T) {
			conn := &openAIWSCaptureConn{
				events: [][]byte{
					[]byte(`{"type":"response.created","response":{"id":"resp_wait"}}`),
					[]byte(`{"type":"response.output_text.delta","delta":""}`),
					[]byte(`{"type":"response.completed","response":{"id":"resp_late","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"too late"}]}]}}`),
				},
				readDelays: []time.Duration{0, 0, 2 * time.Second},
			}
			svc, account := newWSFirstOutputTestService(conn)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			body := []byte(`{"model":"gpt-5.5","reasoning":{"effort":"high"},"stream":` + map[bool]string{false: "false", true: "true"}[stream] + `,"input":"hello"}`)

			_, err := svc.Forward(context.Background(), c, account, body)
			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.True(t, failover.FirstOutputTimeout)
			require.False(t, c.Writer.Written(), "replayable preamble must remain private")
			require.NotContains(t, recorder.Body.String(), "too late")
			require.NotContains(t, recorder.Body.String(), "response.completed")
		})
	}
}

func TestWSV2ExplicitImageKeepsExistingFirstOutputTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	conn := &openAIWSCaptureConn{
		events:     [][]byte{[]byte(`{"type":"response.completed","response":{"id":"resp_image","status":"completed","output":[{"type":"image_generation_call","result":"aW1hZ2U="}],"usage":{"input_tokens":1,"output_tokens":1}}}`)},
		readDelays: []time.Duration{1500 * time.Millisecond},
	}
	svc, account := newWSFirstOutputTestService(conn)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	result, err := svc.Forward(context.Background(), c, account, []byte(`{"model":"gpt-5.5","reasoning":{"effort":"high"},"stream":false,"input":"draw","tools":[{"type":"image_generation"}]}`))
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.OpenAIWSMode)
	require.Equal(t, "resp_image", result.ResponseID)
}

func TestWSV2CommittedStructuralOutputTimeoutCannotFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	conn := &openAIWSCaptureConn{
		events: [][]byte{
			[]byte(`{"type":"response.created","response":{"id":"resp_partial","usage":{"input_tokens":13,"output_tokens":2}}}`),
			[]byte(`{"type":"response.output_item.added","item":{"type":"computer_call","id":"call_1","action":{"type":"click"}}}`),
			[]byte(`{"type":"response.completed","response":{"id":"resp_late","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"too late"}]}]}}`),
		},
		readDelays: []time.Duration{0, 0, 2 * time.Second},
	}
	svc, account := newWSFirstOutputTestService(conn)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	result, err := svc.Forward(context.Background(), c, account, []byte(`{"model":"gpt-5.5","reasoning":{"effort":"high"},"stream":true,"input":"hello"}`))
	require.Error(t, err)
	var failover *UpstreamFailoverError
	require.False(t, errors.As(err, &failover), "committed output cannot be replayed on another account")
	require.NotNil(t, result, "err=%v body=%s", err, recorder.Body.String())
	require.Equal(t, 13, result.Usage.InputTokens)
	require.Equal(t, 2, result.Usage.OutputTokens)
	require.True(t, IsResponseCommitted(c))
	require.Equal(t, 1, strings.Count(recorder.Body.String(), `"type":"response.failed"`))
	require.Contains(t, recorder.Body.String(), `"code":"first_output_timeout"`)
	require.NotContains(t, recorder.Body.String(), "response.completed")
}

func TestWSV2PassthroughEmptyDeltaKeepsFirstOutputDeadline(t *testing.T) {
	upstream := newStagedPassthroughConn()
	defer upstream.Close()
	conn := &openAIWSPassthroughFirstOutputFrameConn{
		inner: upstream, activeReadTimeout: time.Second, deadlineChanged: make(chan struct{}, 1),
		resolveDeadline: func([]byte) openAIWSPassthroughFirstOutputDeadline {
			return openAIWSPassthroughFirstOutputDeadline{timeout: 100 * time.Millisecond}
		},
	}
	require.NoError(t, conn.WriteFrame(context.Background(), coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.5"}`)))
	upstream.Send(`{"type":"response.created"}`)
	upstream.Send(`{"type":"response.output_text.delta","delta":""}`)
	for range 2 {
		_, _, err := conn.ReadFrame(context.Background())
		require.NoError(t, err)
	}
	_, _, err := conn.ReadFrame(context.Background())
	var timeoutErr *openAIWSPassthroughFirstOutputTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
}

func TestWSV2PassthroughExpiredDeadlineWinsOverReadError(t *testing.T) {
	upstream := newStagedPassthroughConn()
	defer upstream.Close()
	conn := &openAIWSPassthroughFirstOutputFrameConn{
		inner: upstream,
		resolveDeadline: func([]byte) openAIWSPassthroughFirstOutputDeadline {
			return openAIWSPassthroughFirstOutputDeadline{
				timeout: time.Second, startedAt: time.Now().Add(-2 * time.Second),
			}
		},
	}
	require.NoError(t, conn.WriteFrame(context.Background(), coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-5.5"}`)))
	upstream.Fail(errors.New("upstream read failed at deadline"))

	_, _, err := conn.ReadFrame(context.Background())
	var timeoutErr *openAIWSPassthroughFirstOutputTimeoutError
	require.ErrorAs(t, err, &timeoutErr)
}

func TestWSV2PassthroughExplicitImageKeepsExistingDeadline(t *testing.T) {
	gin.SetMode(gin.TestMode)
	controlCtx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	upstream := newStagedPassthroughConn()
	cfg := passthroughLifecycleConfig()
	cfg.Gateway.OpenAIHighEffortFirstOutputTimeoutSeconds = 3
	server, serverErr := startPassthroughLifecycleServer(t, controlCtx, newPassthroughLifecycleService(cfg, upstream), passthroughLifecycleAccount())
	defer server.Close()
	client := dialPassthroughLifecycleClientWithPayload(t, server, `{"type":"response.create","model":"gpt-5.5","reasoning":{"effort":"high"},"tools":[{"type":"image_generation"}],"input":"draw"}`)
	defer client.CloseNow()
	_ = requirePassthroughUpstreamWrite(t, upstream, time.Second)
	deadline := time.NewTimer(1300 * time.Millisecond)
	defer deadline.Stop()
	<-deadline.C
	upstream.Send(`{"type":"response.completed","response":{"id":"resp_image","status":"completed","output":[{"type":"image_generation_call","result":"aW1hZ2U="}]}}`)
	frame, err := readPassthroughLifecycleFrame(t, client, 3*time.Second)
	require.NoError(t, err)
	require.Contains(t, string(frame), `"response.completed"`)
	_ = client.Close(coderws.StatusNormalClosure, "done")
	select {
	case err := <-serverErr:
		var timeoutErr *openAIWSPassthroughFirstOutputTimeoutError
		require.False(t, errors.As(err, &timeoutErr))
	case <-time.After(3 * time.Second):
		t.Fatal("passthrough image turn did not finish")
	}
}
