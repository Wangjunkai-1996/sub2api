package kkaiattribution

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Signer struct {
	origins originSet
	secret  []byte
	now     func() time.Time
	nonce   func() (string, error)
}

func NewSigner(allowedOrigins []string, secret string) (*Signer, error) {
	origins, err := newOriginSet(allowedOrigins)
	if err != nil || len(secret) < 32 {
		return nil, ErrInvalidConfiguration
	}
	return &Signer{origins: origins, secret: []byte(secret), now: time.Now, nonce: randomNonce}, nil
}

func (s *Signer) ApplyRequest(req *http.Request, claims Claims) (bool, error) {
	if req == nil || req.URL == nil {
		return false, ErrInvalidEnvelope
	}
	return s.Apply(req.Header, req.Method, req.URL.String(), req.Host, claims)
}

func (s *Signer) Apply(headers http.Header, method string, rawURL string, authority string, claims Claims) (bool, error) {
	Strip(headers)
	if s == nil {
		return false, nil
	}
	target, allowed := s.origins.match(rawURL)
	if !allowed {
		return false, nil
	}
	if authority != "" && !sameAuthority(target.origin, authority) {
		return false, ErrOriginNotAllowed
	}
	if headers == nil || validateClaims(claims) != nil {
		return false, ErrInvalidEnvelope
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	if !validHeaderText(method, 32) {
		return false, ErrInvalidEnvelope
	}
	nonce, err := s.nonce()
	if err != nil || !validNonce(nonce) {
		return false, ErrInvalidEnvelope
	}
	timestamp := strconv.FormatInt(s.now().Unix(), 10)
	signature := hex.EncodeToString(envelopeMAC(s.secret, method, target, timestamp, nonce, claims))

	headers.Set(RequestIDHeader, claims.RequestID)
	headers.Set(UserIDHeader, strconv.Itoa(claims.UserID))
	headers.Set(TokenIDHeader, strconv.Itoa(claims.TokenID))
	headers.Set(ChannelIDHeader, strconv.Itoa(claims.ChannelID))
	headers.Set(MultiKeyIndexHeader, strconv.Itoa(claims.MultiKeyIndex))
	headers.Set(ModelHeader, claims.Model)
	headers.Set(SourceHeader, claims.Source)
	headers.Set(VersionHeader, attributionVersion)
	headers.Set(TimestampHeader, timestamp)
	headers.Set(NonceHeader, nonce)
	headers.Set(SignatureHeader, signature)
	return true, nil
}

func randomNonce() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}
