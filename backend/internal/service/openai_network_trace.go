package service

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

const (
	openAINetworkDNS = iota
	openAINetworkTCP
	openAINetworkTLS
	openAINetworkConnection
)

// Overlapping callbacks (including Happy Eyeballs) measure the union of active
// intervals, not a sum of parallel dials. Repeated exchanges add intervals.
// An unfinished interval makes the entire stage unavailable at log time.
type openAINetworkPhase struct {
	active    int
	completed bool
	started   time.Time
	elapsed   time.Duration
}

type openAINetworkTrace struct {
	mu                sync.Mutex
	finished          bool
	started           time.Time
	fields            []zap.Field
	phases            [4]openAINetworkPhase
	connections       int
	reused            int
	writes            int
	firstBytes        int
	requestWritten    time.Time
	firstResponseByte time.Time
}

// This is one native HTTPUpstream.Do invocation, including any adapter pre-read,
// not the caller's body lifetime or a WS trace. Existing callbacks are preserved.
func startOpenAINetworkTrace(request *http.Request, accountID int64) (*http.Request, *openAINetworkTrace) {
	t := &openAINetworkTrace{started: time.Now(), fields: []zap.Field{
		zap.String("component", "service.openai_gateway"),
		zap.Int64("account_id", accountID),
		zap.Bool(logger.OpsSystemLogSkipField, true),
	}}
	ctx := request.Context()
	for _, key := range []ctxkey.Key{ctxkey.RequestID, ctxkey.ClientRequestID} {
		if id, ok := ctx.Value(key).(string); ok && id != "" {
			name := "request_id"
			if key == ctxkey.ClientRequestID {
				name = "client_request_id"
			}
			t.fields = append(t.fields, zap.String(name, id))
		}
	}
	if budget, _ := ctx.Value(openAIModelDispatchBudgetKey{}).(*openAIModelDispatchBudget); budget != nil {
		budget.mu.Lock()
		t.fields = append(t.fields, zap.Int("dispatch_attempt", budget.attempts))
		budget.mu.Unlock()
	}
	return request.WithContext(httptrace.WithClientTrace(ctx, t.callbacks())), t
}

func (t *openAINetworkTrace) update(fn func(time.Time)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.finished {
		fn(time.Now())
	}
}

func (t *openAINetworkTrace) begin(stage int, now time.Time) {
	p := &t.phases[stage]
	if p.active == 0 {
		p.started = now
	}
	p.active++
}

func (t *openAINetworkTrace) end(stage int, now time.Time) {
	p := &t.phases[stage]
	if p.active == 0 {
		return
	}
	p.active--
	if p.active == 0 {
		p.elapsed += now.Sub(p.started)
		p.completed = true
	}
}

func (t *openAINetworkTrace) callbacks() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) { t.update(func(now time.Time) { t.begin(openAINetworkDNS, now) }) },
		DNSDone:  func(httptrace.DNSDoneInfo) { t.update(func(now time.Time) { t.end(openAINetworkDNS, now) }) },
		ConnectStart: func(string, string) {
			t.update(func(now time.Time) { t.begin(openAINetworkTCP, now) })
		},
		ConnectDone: func(string, string, error) {
			t.update(func(now time.Time) { t.end(openAINetworkTCP, now) })
		},
		TLSHandshakeStart: func() { t.update(func(now time.Time) { t.begin(openAINetworkTLS, now) }) },
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			t.update(func(now time.Time) { t.end(openAINetworkTLS, now) })
		},
		GetConn: func(string) {
			t.update(func(now time.Time) {
				t.connections++
				t.begin(openAINetworkConnection, now)
			})
		},
		GotConn: func(info httptrace.GotConnInfo) {
			t.update(func(now time.Time) {
				t.end(openAINetworkConnection, now)
				if info.Reused {
					t.reused++
				}
			})
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			t.update(func(now time.Time) {
				t.writes++
				if info.Err == nil && t.writes == 1 {
					t.requestWritten = now
				}
			})
		},
		GotFirstResponseByte: func() {
			t.update(func(now time.Time) {
				t.firstBytes++
				if t.firstBytes == 1 {
					t.firstResponseByte = now
				}
			})
		},
	}
}

func (t *openAINetworkTrace) log(log *zap.Logger, response *http.Response, err error) {
	t.mu.Lock()
	if t.finished {
		t.mu.Unlock()
		return
	}
	t.finished = true
	elapsed := time.Since(t.started)
	fields := append(t.fields,
		zap.String("result", openAINetworkTraceResult(response, err)),
		zap.Float64("attempt_ms", float64(elapsed)/float64(time.Millisecond)),
	)
	if response != nil {
		fields = append(fields, zap.Int("status_code", response.StatusCode))
	}
	for stage, name := range []string{"dns_ms", "tcp_ms", "tls_ms", "connection_acquire_ms"} {
		p := t.phases[stage]
		if p.completed && p.active == 0 {
			fields = append(fields, zap.Float64(name, float64(p.elapsed)/float64(time.Millisecond)))
		}
	}
	if t.connections > 0 {
		fields = append(fields, zap.Int("connection_requests", t.connections), zap.Int("connections_reused", t.reused))
	}
	// Redirects/retries may interleave callbacks without exchange IDs. Report
	// write-to-first-byte only for one unambiguous, fully written exchange.
	if t.connections == 1 && t.writes == 1 && t.firstBytes == 1 &&
		!t.requestWritten.IsZero() && !t.firstResponseByte.Before(t.requestWritten) {
		fields = append(fields, zap.Float64("write_to_first_byte_ms", float64(t.firstResponseByte.Sub(t.requestWritten))/float64(time.Millisecond)))
	}
	t.mu.Unlock()
	log.Info("openai.network_trace", fields...)
}

func openAINetworkTraceResult(response *http.Response, err error) string {
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var networkErr net.Error
	if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &networkErr) && networkErr.Timeout() {
		return "timeout"
	}
	if err != nil {
		return "transport_error"
	}
	if response == nil {
		return "no_response"
	}
	if response.StatusCode >= http.StatusBadRequest {
		return "http_error"
	}
	return "response"
}
