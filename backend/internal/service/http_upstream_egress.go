package service

import (
	"context"
	"strings"
)

// HTTPUpstreamEgress identifies the already-admitted route for one upstream
// request. It contains only stable, non-secret identifiers; proxy credentials
// remain on the request-local Account and are never copied into context values.
type HTTPUpstreamEgress struct {
	BindingID    string
	RouteID      int64
	IdentityID   string
	PoolRevision int64
}

type httpUpstreamEgressContextKey struct{}

func WithHTTPUpstreamEgress(ctx context.Context, egress HTTPUpstreamEgress) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	egress.BindingID = strings.TrimSpace(egress.BindingID)
	egress.IdentityID = strings.TrimSpace(egress.IdentityID)
	if egress.BindingID == "" {
		return ctx
	}
	return context.WithValue(ctx, httpUpstreamEgressContextKey{}, egress)
}

func HTTPUpstreamEgressFromContext(ctx context.Context) (HTTPUpstreamEgress, bool) {
	if ctx == nil {
		return HTTPUpstreamEgress{}, false
	}
	egress, ok := ctx.Value(httpUpstreamEgressContextKey{}).(HTTPUpstreamEgress)
	if !ok || strings.TrimSpace(egress.BindingID) == "" {
		return HTTPUpstreamEgress{}, false
	}
	return egress, true
}
