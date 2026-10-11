package service

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptrace"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/gin-gonic/gin"
)

type openAIPreSendState struct {
	requestStarted atomic.Bool
}

func trackOpenAIUpstreamSend(request *http.Request) (*http.Request, *openAIPreSendState) {
	state := &openAIPreSendState{}
	trace := &httptrace.ClientTrace{
		WroteHeaders:         func() { state.requestStarted.Store(true) },
		WroteRequest:         func(httptrace.WroteRequestInfo) { state.requestStarted.Store(true) },
		GotFirstResponseByte: func() { state.requestStarted.Store(true) },
	}
	return request.WithContext(httptrace.WithClientTrace(request.Context(), trace)), state
}

func (s *openAIPreSendState) mayFailover(ctx context.Context, c *gin.Context, err error) bool {
	if err == nil || s == nil || s.requestStarted.Load() || IsOpenAIModelDispatchStop(err) ||
		(ctx != nil && ctx.Err() != nil) ||
		(c != nil && c.Request != nil && c.Request.Context().Err() != nil) ||
		errors.Is(err, context.Canceled) {
		return false
	}
	var pluginErr *PluginTransportError
	if errors.As(err, &pluginErr) {
		return pluginErr != nil && !pluginErr.RequestSent
	}
	if IsHTTPUpstreamRequestNotSent(err) {
		return true
	}
	// An absent trace callback alone is not proof of a safe replay. Require an
	// explicit setup operation or a typed error that precedes the model request.
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if opErr == nil {
			return false
		}
		switch opErr.Op {
		case "dial", "proxyconnect", "socks connect":
			// Continue with the concrete DNS/connect failure below.
		default:
			return false
		}
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && dnsErr != nil && dnsErr.IsNotFound {
		return true
	}
	if errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) {
		return true
	}
	if opErr != nil {
		message := strings.ToLower(err.Error())
		for _, marker := range []string{
			"connection refused",
			"no route to host",
			"network is unreachable",
			"no such host",
			"proxy authentication required",
			"authentication failed",
		} {
			if strings.Contains(message, marker) {
				return true
			}
		}
	}
	return false
}
