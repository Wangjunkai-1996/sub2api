package service

import (
	"context"
	"io"
	"sync"
	"time"
)

const openAITextFirstOutputLimit = 120 * time.Second

// Text attempts cannot disable or extend their first usable output deadline.
// Keep shorter operator limits; high reasoning effort does not extend it.
func (s *OpenAIGatewayService) openAITextFirstOutputTimeout() time.Duration {
	if s != nil && s.cfg != nil {
		seconds := s.cfg.Gateway.OpenAIFirstOutputTimeoutSeconds
		if seconds > 0 && seconds < 120 {
			return time.Duration(seconds) * time.Second
		}
	}
	return openAITextFirstOutputLimit
}

// This guard cancels a blocked upstream read without writing to the client.
// The stream reader owns output classification and decides whether replay is safe.
type openAITextFirstOutputGuard struct {
	mu       sync.Mutex
	deadline time.Time
	cancel   context.CancelFunc
	timer    *time.Timer
	body     io.ReadCloser
	observed bool
	expired  bool
	closed   bool
}

func newOpenAITextFirstOutputGuard(ctx context.Context, deadline time.Time) (context.Context, *openAITextFirstOutputGuard) {
	ctx, cancel := context.WithCancel(ctx)
	g := &openAITextFirstOutputGuard{deadline: deadline, cancel: cancel}
	g.timer = time.AfterFunc(time.Until(deadline), g.expire)
	return ctx, g
}

func (g *openAITextFirstOutputGuard) expire() {
	g.mu.Lock()
	if g.observed || g.expired || g.closed {
		g.mu.Unlock()
		return
	}
	g.expired = true
	body := g.body
	g.mu.Unlock()
	// Cancel first: Close can wait for an in-flight network read.
	g.cancel()
	if body != nil {
		_ = body.Close()
	}
}

func (g *openAITextFirstOutputGuard) setBody(body io.ReadCloser) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.body = body
	closeBody := g.expired || g.closed
	g.mu.Unlock()
	if closeBody && body != nil {
		_ = body.Close()
	}
}

// observeOutput is idempotent and rejects output that arrives after the deadline,
// even when the timer callback has not yet been scheduled.
func (g *openAITextFirstOutputGuard) observeOutput() bool {
	if g == nil {
		return true
	}
	g.mu.Lock()
	if g.expired || g.closed {
		g.mu.Unlock()
		return false
	}
	if !g.observed && !time.Now().Before(g.deadline) {
		g.mu.Unlock()
		g.expire()
		return false
	}
	g.observed = true
	g.timer.Stop()
	g.mu.Unlock()
	return true
}

func (g *openAITextFirstOutputGuard) timedOut() bool {
	if g == nil {
		return false
	}
	g.mu.Lock()
	due := !g.observed && !g.closed && !time.Now().Before(g.deadline)
	g.mu.Unlock()
	if due {
		g.expire()
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.expired
}

func (g *openAITextFirstOutputGuard) close() {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.closed = true
	g.timer.Stop()
	g.mu.Unlock()
	g.cancel()
}
