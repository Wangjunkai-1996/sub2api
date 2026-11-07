//go:build unit && (darwin || linux)

package handler

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/repository"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/alicebob/miniredis/v2"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type openAIOpaqueBenchmarkUpstream struct {
	service.HTTPUpstream
	calls atomic.Int32
}

func (u *openAIOpaqueBenchmarkUpstream) Do(_ *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	// Mirror the streaming integration fixture, with the unique physical
	// response IDs expected from a real upstream across repeated requests.
	id := u.calls.Add(1)
	body := fmt.Sprintf("data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_bench_%[1]d\",\"status\":\"in_progress\"}}\n\n"+
		"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_bench_%[1]d\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n"+
		"data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"item_id\":\"msg_bench_%[1]d\",\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n"+
		"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"item_id\":\"msg_bench_%[1]d\",\"delta\":\"first-text\"}\n\n"+
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_bench_%[1]d\",\"status\":\"completed\",\"output\":[{\"id\":\"msg_bench_%[1]d\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"first-text\"}]}],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n", id)
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}, nil
}

// BenchmarkOpenAIOpaqueRecovery compares the actual Responses handler with the
// same opaque payload and controlled upstream, including the in-process Redis
// server used by the functional tests. It has no latency pass/fail threshold.
// First-delta/completion samples cover h.Responses only; ns/op, CPU and allocs
// also include request fixture construction and result validation. Process CPU
// includes the Redis stub and Go background work, not production Redis/network.
// Run with -benchtime=200x to stay below the real 256-alias retention limit for
// one scope; this is a bounded normal-path sample, not an alias-capacity test.
func BenchmarkOpenAIOpaqueRecovery(b *testing.B) {
	gin.SetMode(gin.TestMode)
	previousLogWriter := log.Writer()
	log.SetOutput(io.Discard)
	b.Cleanup(func() { log.SetOutput(previousLogWriter) })
	payload := `{"model":"gpt-5.1","stream":true,"input":[{"type":"reasoning","id":"rs_original","encrypted_content":"` + strings.Repeat("opaque-test-", 192) + `","summary":[]},{"role":"user","content":"hello"}]}`
	for _, managed := range []bool{false, true} {
		name := "ordinary"
		if managed {
			name = "managed"
		}
		b.Run(name, func(b *testing.B) {
			if b.N > 200 {
				b.Skip("use -benchtime=200x for a bounded sample below the alias retention limit")
			}
			upstream := &openAIOpaqueBenchmarkUpstream{}
			accounts := []service.Account{{ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true, Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"}, Extra: map[string]any{"openai_passthrough": true}}}
			server := miniredis.RunT(b)
			client := redis.NewClient(&redis.Options{Addr: server.Addr()})
			b.Cleanup(func() { require.NoError(b, client.Close()) })
			cache := repository.NewGatewayCache(client)
			cfg := &config.Config{RunMode: config.RunModeSimple}
			gateway := service.NewOpenAIGatewayService(openAIImagesFailoverAccountRepo{accounts: accounts}, nil, nil, nil, nil, nil, cache, cfg, nil, nil, nil, nil, nil, upstream, nil, nil, nil, nil, nil, nil, nil, nil)
			billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
			b.Cleanup(billing.Stop)
			h := NewOpenAIGatewayHandler(gateway, service.NewConcurrencyService(nil), billing, service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg)
			ctx := context.WithValue(context.Background(), ctxkey.NewAPIUserID, int64(1))
			ctx = context.WithValue(ctx, ctxkey.NewAPITokenID, int64(2))
			ctx = context.WithValue(ctx, ctxkey.NewAPIOrigin, "newapi")
			groupID := int64(3131)
			key := &service.APIKey{ID: 99, GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI}, User: &service.User{ID: 100}}
			firstDeltas, completions := make([]time.Duration, b.N), make([]time.Duration, b.N)
			var cpuStart, cpuEnd syscall.Rusage
			b.ReportAllocs()
			b.StopTimer()
			for i := -1; i < b.N; i++ {
				// Warm one request/Redis connection before recording the bounded sample.
				if i == 0 {
					require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &cpuStart))
					b.ResetTimer()
					b.StartTimer()
				}
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(payload)).WithContext(ctx)
				c.Request.Header.Set("Content-Type", "application/json")
				if managed {
					c.Request.Header.Set(openAIRecoveryProtocolHeader, "1")
				}
				c.Set(string(middleware2.ContextKeyAPIKey), key)
				c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100, Concurrency: 0})
				timing := &recoveryToggleTimingWriter{ResponseWriter: c.Writer, first: make(chan time.Time, 1)}
				c.Writer = timing
				start := time.Now()
				h.Responses(c)
				completed := time.Since(start)
				var first time.Duration
				select {
				case firstAt := <-timing.first:
					first = firstAt.Sub(start)
				default:
					b.Fatalf("missing first text: status=%d response=%s", rec.Code, rec.Body.String())
				}
				if rec.Code != http.StatusOK || strings.Count(rec.Body.String(), `"type":"response.completed"`) != 1 {
					b.Fatalf("request/alias publication did not complete: sample=%d status=%d response=%s", i, rec.Code, rec.Body.String())
				}
				if managed && !strings.Contains(rec.Body.String(), `"kind":"terminal"`) {
					b.Fatal("managed terminal missing")
				}
				if i >= 0 {
					firstDeltas[i], completions[i] = first, completed
				}
			}
			b.StopTimer()
			require.NoError(b, syscall.Getrusage(syscall.RUSAGE_SELF, &cpuEnd))
			require.Equal(b, int32(b.N+1), upstream.calls.Load(), "normal completion must dispatch once per request")
			sort.Slice(firstDeltas, func(i, j int) bool { return firstDeltas[i] < firstDeltas[j] })
			sort.Slice(completions, func(i, j int) bool { return completions[i] < completions[j] })
			b.ReportMetric(float64(firstDeltas[(b.N-1)*50/100].Nanoseconds()), "first_p50_ns")
			b.ReportMetric(float64(firstDeltas[(b.N-1)*95/100].Nanoseconds()), "first_p95_ns")
			b.ReportMetric(float64(completions[(b.N-1)*50/100].Nanoseconds()), "complete_p50_ns")
			b.ReportMetric(float64(completions[(b.N-1)*95/100].Nanoseconds()), "complete_p95_ns")
			cpuNanos := cpuEnd.Utime.Nano() + cpuEnd.Stime.Nano() - cpuStart.Utime.Nano() - cpuStart.Stime.Nano()
			b.ReportMetric(float64(cpuNanos)/float64(b.N), "process_cpu_ns/op")
			b.ReportMetric(1, "dispatches/op")
			if managed {
				b.ReportMetric(0, "alias_publish_errors/op")
			}
		})
	}
}
