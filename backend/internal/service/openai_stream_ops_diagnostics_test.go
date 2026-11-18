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

func TestOpenAIResponsesPostOutputCapacityFailureRecordsOneOpsEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "passthrough"}[passthrough], func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			stream := strings.Join([]string{
				`data: {"type":"response.output_text.delta","delta":"partial","output_index":0}`,
				"",
				`event: error`,
				`data: {"type":"error","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}`,
				"",
				`event: response.failed`,
				`data: {"type":"response.failed","response":{"error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}`,
				"",
			}, "\n")
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "X-Request-Id": []string{"rid-capacity-post-output"}},
				Body:       io.NopCloser(strings.NewReader(stream)),
			}
			svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{MaxLineSize: defaultMaxLineSize}}}
			account := &Account{ID: 731, Name: "sub2-capacity", Platform: PlatformOpenAI, Type: AccountTypeOAuth}

			var err error
			if passthrough {
				_, err = svc.handleStreamingResponsePassthrough(context.Background(), resp, c, account, time.Now(), "gpt-5.6", "gpt-5.6")
			} else {
				_, err = svc.handleStreamingResponse(context.Background(), resp, c, account, time.Now(), "gpt-5.6", "gpt-5.6")
			}

			require.Error(t, err)
			var failoverErr *UpstreamFailoverError
			require.False(t, errors.As(err, &failoverErr), "post-output failures must not replay the request")
			raw, ok := c.Get(OpsUpstreamErrorsKey)
			require.True(t, ok)
			events, ok := raw.([]*OpsUpstreamErrorEvent)
			require.True(t, ok)
			require.Len(t, events, 1, "error + response.failed must be one diagnostic event")
			require.Equal(t, http.StatusServiceUnavailable, events[0].UpstreamStatusCode)
			require.Equal(t, int64(731), events[0].AccountID)
			require.Equal(t, "rid-capacity-post-output", events[0].UpstreamRequestID)
			require.Contains(t, events[0].Message, "currently overloaded")
			require.Equal(t, passthrough, events[0].Passthrough)
		})
	}
}
