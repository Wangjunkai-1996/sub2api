//go:build unit

package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// The native WS adapter must preserve reported TTLs all the way to both the
// customer debit and account statistics, with the same invalid-detail fallback as HTTP.
func TestOpenAIWSPassthroughCacheWriteTTLRecordUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name, details string
		short, long   int
	}{
		{"mixed", `{"ttl_30m_tokens":4,"ttl_12h_tokens":6}`, 4, 6},
		{"12h", `{"ttl_30m_tokens":0,"ttl_12h_tokens":10}`, 0, 10},
		{"invalid", `{"ttl_30m_tokens":4,"ttl_12h_tokens":9}`, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			usageRepo := &openAIRecordUsageLogRepoStub{inserted: true}
			userRepo := &openAIRecordUsageUserRepoStub{}
			svc := newOpenAIRecordUsageServiceForTest(usageRepo, userRepo, &openAIRecordUsageSubRepoStub{}, nil)
			cfg := svc.cfg
			cfg.Security.URLAllowlist.Enabled = false
			cfg.Security.URLAllowlist.AllowInsecureHTTP = true
			cfg.Gateway.OpenAIWS.Enabled = true
			cfg.Gateway.OpenAIWS.APIKeyEnabled = true
			cfg.Gateway.OpenAIWS.ResponsesWebsocketsV2 = true
			cfg.Gateway.OpenAIWS.ModeRouterV2Enabled = true
			cfg.Gateway.OpenAIWS.IngressModeDefault = OpenAIWSIngressModeCtxPool
			cfg.Gateway.OpenAIWS.DialTimeoutSeconds = 3
			cfg.Gateway.OpenAIWS.ReadTimeoutSeconds = 3
			cfg.Gateway.OpenAIWS.WriteTimeoutSeconds = 3
			svc.httpUpstream = &httpUpstreamRecorder{}
			svc.cache = &stubGatewayCache{}
			svc.openaiWSResolver = NewOpenAIWSProtocolResolver(cfg)
			svc.toolCorrector = NewCodexToolCorrector()
			usageJSON := fmt.Sprintf(`{"input_tokens":13,"output_tokens":2,"input_tokens_details":{"cache_write_tokens":10,"cache_write_tokens_details":%s}}`, tc.details)
			svc.openaiWSPassthroughDialer = &openAIWSCaptureDialer{conn: &openAIWSCaptureConn{events: [][]byte{
				[]byte(`{"type":"response.created","response":{"id":"resp_ttl","model":"gpt-6-luna"}}`),
				[]byte(`{"type":"response.output_text.delta","delta":"ok"}`),
				[]byte(`{"type":"response.completed","response":{"id":"resp_ttl","model":"gpt-6-luna","usage":` + usageJSON + `}}`),
			}}}
			account := &Account{
				ID: 454, Platform: PlatformOpenAI, Type: AccountTypeAPIKey,
				Status: StatusActive, Schedulable: true, Concurrency: 1,
				Credentials: map[string]any{"api_key": "sk-test"},
				Extra:       map[string]any{"openai_apikey_responses_websockets_v2_mode": OpenAIWSIngressModePassthrough},
			}
			const groupID int64 = 777
			channel := Channel{ID: 1, Status: StatusActive, GroupIDs: []int64{groupID}, ApplyPricingToAccountStats: true, ModelPricing: []ChannelModelPricing{{
				Platform: PlatformOpenAI, Models: []string{"gpt-6-luna"}, BillingMode: BillingModeToken,
				InputPrice: float64Ptr(1e-6), OutputPrice: float64Ptr(3e-6),
				CacheWritePrice: float64Ptr(2e-6), CacheWrite12hPrice: float64Ptr(5e-6),
			}}}
			svc.channelService = &ChannelService{}
			svc.channelService.cache.Store(populateChannelCache([]Channel{channel}, map[int64]string{groupID: PlatformOpenAI}))
			svc.resolver = NewModelPricingResolver(svc.channelService, svc.billingService)
			results := make(chan *OpenAIForwardResult, 1)
			hooks := &OpenAIWSIngressHooks{AfterTurn: func(_ int, result *OpenAIForwardResult, err error) {
				if err == nil && result != nil {
					results <- result
				}
			}}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = r
				ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
				defer cancel()
				_, first, err := conn.Read(ctx)
				if err == nil {
					_ = svc.ProxyResponsesWebSocketFromClient(ctx, c, conn, account, "sk-test", first, hooks)
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, _, err := coderws.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			require.NoError(t, err)
			defer client.CloseNow()
			require.NoError(t, client.Write(ctx, coderws.MessageText, []byte(`{"type":"response.create","model":"gpt-6-luna","input":"hi"}`)))
			for {
				_, payload, err := client.Read(ctx)
				require.NoError(t, err)
				if gjson.GetBytes(payload, "type").String() == "response.completed" {
					break
				}
			}
			var result *OpenAIForwardResult
			select {
			case result = <-results:
			case <-ctx.Done():
				t.Fatal("WS terminal usage was not reported")
			}
			httpUsage, ok := openAIUsageFromGJSON(gjson.Parse(usageJSON))
			require.True(t, ok)
			require.Equal(t, httpUsage.CacheCreation30mInputTokens, result.Usage.CacheCreation30mInputTokens)
			require.Equal(t, httpUsage.CacheCreation12hInputTokens, result.Usage.CacheCreation12hInputTokens)
			require.NoError(t, svc.RecordUsage(ctx, &OpenAIRecordUsageInput{
				Result: result, Account: account, User: &User{ID: 1},
				APIKey:             &APIKey{ID: 1, GroupID: i64p(groupID), Group: &Group{ID: groupID, Platform: PlatformOpenAI, RateMultiplier: 0.5}},
				ChannelUsageFields: ChannelUsageFields{OriginalModel: "gpt-6-luna", BillingModelSource: BillingModelSourceRequested},
			}))
			log := usageRepo.lastLog
			require.NotNil(t, log)
			require.Equal(t, tc.short, log.CacheCreation30mTokens)
			require.Equal(t, tc.long, log.CacheCreation12hTokens)
			cacheCost := float64(tc.short)*2e-6 + float64(tc.long)*5e-6
			if tc.short+tc.long == 0 {
				cacheCost = 10 * 2e-6
			}
			totalCost := 3*1e-6 + 2*3e-6 + cacheCost
			require.InDelta(t, cacheCost, log.CacheCreationCost, 1e-12)
			require.InDelta(t, totalCost*0.5, userRepo.lastAmount, 1e-12)
			require.NotNil(t, log.AccountStatsCost)
			require.InDelta(t, totalCost, *log.AccountStatsCost, 1e-12)
		})
	}
}
