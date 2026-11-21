//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestRelayStallsIdlePenaltyAndReplayClassification(t *testing.T) {
	for _, scenario := range []string{"idle", "canceled_idle", "canceled_first_output", "read_error"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				repo := &errorPolicyRepoStub{}
				settings := NewSettingService(&settingRepoStub{values: map[string]string{
					SettingKeyStreamTimeoutSettings: `{"enabled":true,"action":"temp_unsched","temp_unsched_minutes":1,"threshold_count":1,"threshold_window_minutes":1}`,
				}}, nil)
				svc := &OpenAIGatewayService{cfg: &config.Config{Gateway: config.GatewayConfig{StreamDataIntervalTimeout: 2}},
					rateLimitService: &RateLimitService{accountRepo: repo, settingService: settings}}
				if scenario == "canceled_first_output" {
					svc.cfg.Gateway.OpenAIFirstOutputTimeoutSeconds = 1
				}
				clientCtx, cancel := context.WithCancel(context.Background())
				defer cancel()
				reader, writer := io.Pipe()
				defer reader.Close()
				defer writer.Close()
				go func() {
					_, _ = io.WriteString(writer, "data: {\"type\":\"response.created\",\"response\":{\"usage\":{\"input_tokens\":13}}}\n\n")
					switch scenario {
					case "canceled_idle", "canceled_first_output":
						cancel()
					case "read_error":
						_ = writer.CloseWithError(io.ErrUnexpectedEOF)
					}
				}()
				rec := httptest.NewRecorder()
				ctx, _ := gin.CreateTestContext(rec)
				ctx.Request = httptest.NewRequestWithContext(clientCtx, http.MethodPost, "/v1/responses", nil)
				resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"X-Request-Id": {"private-attempt"}}, Body: reader}
				result, err := svc.handleStreamingResponse(context.Background(), resp, ctx, &Account{ID: 1, Platform: PlatformOpenAI}, time.Now(), "public", "mapped")
				require.NotNil(t, result)
				require.Equal(t, 13, result.usage.InputTokens)
				require.Empty(t, rec.Body.String())
				var failover *UpstreamFailoverError
				switch scenario {
				case "idle":
					require.ErrorAs(t, err, &failover)
					require.True(t, failover.FirstOutputTimeout)
					require.True(t, failover.SafeToFailoverAfterWrite)
					require.Equal(t, "private-attempt", failover.ResponseHeaders.Get("X-Request-Id"))
					require.Equal(t, 1, repo.tempCalls)
				case "read_error":
					require.ErrorAs(t, err, &failover)
					require.False(t, failover.FirstOutputTimeout)
					require.Zero(t, repo.tempCalls)
				default:
					require.ErrorIs(t, err, context.Canceled)
					require.False(t, errors.As(err, &failover))
					require.Zero(t, repo.tempCalls)
				}
			})
		})
	}
}
