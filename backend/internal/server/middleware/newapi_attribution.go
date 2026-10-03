package middleware

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/kkaiattribution"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

type attributionNonces struct{ client *redis.Client }

func (s attributionNonces) Reserve(ctx context.Context, nonce string, expiresAt time.Time) (bool, error) {
	ttl := time.Until(expiresAt)
	if ttl <= 0 {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.client.SetNX(ctx, "newapi:attribution:nonce:"+nonce, "1", ttl).Result()
}

// NewAPIAttribution verifies relay identities only on Messages requests. Internal
// attribution headers are removed from every request before any upstream relay.
func NewAPIAttribution(cfg config.NewAPIAttributionConfig, client *redis.Client) (gin.HandlerFunc, error) {
	if len(cfg.Origins) != 0 || cfg.Secret != "" {
		if client == nil {
			return nil, kkaiattribution.ErrInvalidConfiguration
		}
	}
	return newAPIAttribution(cfg, attributionNonces{client: client})
}

func newAPIAttribution(cfg config.NewAPIAttributionConfig, nonces kkaiattribution.NonceStore) (gin.HandlerFunc, error) {
	var verifier *kkaiattribution.Verifier
	var origins []*url.URL
	if len(cfg.Origins) != 0 || cfg.Secret != "" {
		var err error
		verifier, err = kkaiattribution.NewVerifier(cfg.Origins, cfg.Secret, nonces)
		if err != nil {
			return nil, kkaiattribution.ErrInvalidConfiguration
		}
		for _, origin := range cfg.Origins {
			parsed, err := url.Parse(strings.TrimSpace(origin))
			if err != nil {
				return nil, kkaiattribution.ErrInvalidConfiguration
			}
			origins = append(origins, parsed)
		}
	}
	return func(c *gin.Context) {
		present := len(c.Request.Header.Values(kkaiattribution.LegacyTokenNameHeader)) != 0
		for _, name := range kkaiattribution.HeaderNames() {
			present = present || len(c.Request.Header.Values(name)) != 0
		}
		if verifier == nil || !present || !strings.HasSuffix(strings.TrimRight(c.Request.URL.Path, "/"), "/v1/messages") {
			stripNewAPIAttributionHeaders(c.Request.Header)
			c.Next()
			return
		}
		request := c.Request.Clone(c.Request.Context())
		stripNewAPIAttributionHeaders(c.Request.Header)
		// The router rewrites Host and may remove the port. Reconstruct only from
		// explicitly configured origins for this receiver; never trust forwarded
		// headers. The original origin, path/query and method remain HMAC-bound.
		for _, origin := range origins {
			target := *request.URL
			target.Scheme, target.Host = origin.Scheme, origin.Host
			request.URL, request.Host = &target, origin.Host
			claims, err := verifier.VerifyRequest(request.Context(), request)
			if err != nil || claims.UserID <= 0 {
				continue
			}
			ctx := context.WithValue(c.Request.Context(), ctxkey.NewAPIUserID, int64(claims.UserID))
			c.Request = c.Request.WithContext(ctx)
			c.Next()
			return
		}
		AbortWithError(c, http.StatusBadRequest, "INVALID_NEWAPI_ATTRIBUTION", "Invalid NewAPI user identity signature")
	}, nil
}

func stripNewAPIAttributionHeaders(headers http.Header) {
	for name := range headers {
		if strings.HasPrefix(strings.ToLower(name), "x-newapi-") {
			delete(headers, name)
		}
	}
}
