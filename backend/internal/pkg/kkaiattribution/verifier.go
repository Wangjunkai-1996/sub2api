package kkaiattribution

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type NonceStore interface {
	Reserve(context.Context, string, time.Time) (bool, error)
}

type Verifier struct {
	origins originSet
	secret  []byte
	nonces  NonceStore
	now     func() time.Time
	maxSkew time.Duration
}

func NewVerifier(allowedOrigins []string, secret string, nonces NonceStore) (*Verifier, error) {
	origins, err := newOriginSet(allowedOrigins)
	if err != nil || len(secret) < 32 || nonces == nil {
		return nil, ErrInvalidConfiguration
	}
	return &Verifier{origins: origins, secret: []byte(secret), nonces: nonces, now: time.Now, maxSkew: defaultMaxSkew}, nil
}

func (v *Verifier) VerifyRequest(ctx context.Context, req *http.Request) (Claims, error) {
	if v == nil || req == nil || req.URL == nil {
		return Claims{}, ErrInvalidEnvelope
	}
	target, allowed := v.origins.match(req.URL.String())
	if !allowed || req.Host != "" && !sameAuthority(target.origin, req.Host) {
		return Claims{}, ErrOriginNotAllowed
	}
	return v.verify(ctx, req.Header, req.Method, target)
}

func (v *Verifier) verify(ctx context.Context, headers http.Header, method string, target parsedTarget) (Claims, error) {
	if headers == nil || headers.Get(LegacyTokenNameHeader) != "" {
		return Claims{}, ErrInvalidEnvelope
	}
	values := make(map[string]string, len(attributionHeaderNames))
	for _, name := range attributionHeaderNames {
		value, err := singleHeader(headers, name)
		if err != nil {
			return Claims{}, err
		}
		values[name] = value
	}
	if values[VersionHeader] != attributionVersion || !validNonce(values[NonceHeader]) {
		return Claims{}, ErrInvalidEnvelope
	}
	claims, err := claimsFromHeaders(values)
	if err != nil {
		return Claims{}, err
	}
	timestamp, err := strconv.ParseInt(values[TimestampHeader], 10, 64)
	if err != nil {
		return Claims{}, ErrInvalidEnvelope
	}
	signedAt := time.Unix(timestamp, 0)
	if signedAt.Before(v.now().Add(-v.maxSkew)) || signedAt.After(v.now().Add(v.maxSkew)) {
		return Claims{}, ErrStaleEnvelope
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	if !validHeaderText(method, 32) {
		return Claims{}, ErrInvalidEnvelope
	}
	if len(values[SignatureHeader]) != sha256.Size*2 {
		return Claims{}, ErrInvalidSignature
	}
	provided, err := hex.DecodeString(values[SignatureHeader])
	if err != nil || !hmac.Equal(envelopeMAC(v.secret, method, target, values[TimestampHeader], values[NonceHeader], claims), provided) {
		return Claims{}, ErrInvalidSignature
	}
	reserved, err := v.nonces.Reserve(ctx, values[NonceHeader], signedAt.Add(v.maxSkew))
	if err != nil {
		return Claims{}, fmt.Errorf("%w: %v", ErrNonceStore, err)
	}
	if !reserved {
		return Claims{}, ErrReplay
	}
	return claims, nil
}
