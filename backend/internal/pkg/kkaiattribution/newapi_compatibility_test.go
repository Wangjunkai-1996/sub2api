package kkaiattribution

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This fixture was signed by the original NewAPI public Signer at
// eb1e97b425ea85dd0392d7013d0edbb92a723cea, independently of this package.
func newAPICompatibilityRequest(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "https://sub2api.internal.example:8443/v1/messages?beta=true", nil)
	require.NoError(t, err)
	for name, value := range map[string]string{
		RequestIDHeader: "newapi-compatibility-8871",
		UserIDHeader:    "8871", TokenIDHeader: "8872", ChannelIDHeader: "17", MultiKeyIndexHeader: "0",
		ModelHeader: "claude-sonnet-4-6", SourceHeader: "new-api", VersionHeader: "v1",
		TimestampHeader: "1790226113", NonceHeader: "873d14943f8c26f4342730a2f36059dd",
		SignatureHeader: "613770636f128a5987de47b32cc4eecaad822cb407e90396ddc720bb78ce3b03",
	} {
		req.Header.Set(name, value)
	}
	return req
}

func TestVerifierNewAPISignerCompatibility(t *testing.T) {
	signedAt := time.Unix(1790226113, 0)
	tests := []struct {
		name     string
		mutate   func(*http.Request)
		now      time.Time
		storeErr error
		wantErr  error
	}{
		{name: "signed NewAPI request accepted"},
		{name: "user tampering rejected", mutate: func(r *http.Request) { r.Header.Set(UserIDHeader, "42") }, wantErr: ErrInvalidSignature},
		{name: "method binding", mutate: func(r *http.Request) { r.Method = http.MethodGet }, wantErr: ErrInvalidSignature},
		{name: "path binding", mutate: func(r *http.Request) { r.URL.Path = "/v1/chat/completions" }, wantErr: ErrInvalidSignature},
		{name: "query binding", mutate: func(r *http.Request) { r.URL.RawQuery = "beta=false" }, wantErr: ErrInvalidSignature},
		{name: "origin binding", mutate: func(r *http.Request) { r.URL.Host = "other.internal.example:8443"; r.Host = r.URL.Host }, wantErr: ErrOriginNotAllowed},
		{name: "authority binding", mutate: func(r *http.Request) { r.Host = "other.internal.example:8443" }, wantErr: ErrOriginNotAllowed},
		{name: "relative URL rejected", mutate: func(r *http.Request) { r.URL.Scheme = ""; r.URL.Host = "" }, wantErr: ErrOriginNotAllowed},
		{name: "duplicate user rejected", mutate: func(r *http.Request) { r.Header.Add(UserIDHeader, "8871") }, wantErr: ErrInvalidEnvelope},
		{name: "unsigned user rejected", mutate: func(r *http.Request) { r.Header.Del(SignatureHeader) }, wantErr: ErrInvalidEnvelope},
		{name: "legacy identity rejected", mutate: func(r *http.Request) { r.Header.Set(LegacyTokenNameHeader, "legacy") }, wantErr: ErrInvalidEnvelope},
		{name: "expired timestamp", now: signedAt.Add(2*time.Minute + time.Second), wantErr: ErrStaleEnvelope},
		{name: "future timestamp", now: signedAt.Add(-2*time.Minute - time.Second), wantErr: ErrStaleEnvelope},
		{name: "nonce store outage fails closed", storeErr: errors.New("unavailable"), wantErr: ErrNonceStore},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req := newAPICompatibilityRequest(t)
			if test.mutate != nil {
				test.mutate(req)
			}
			nonces := &memoryNonceStore{reserved: make(map[string]struct{}), err: test.storeErr}
			verifier, err := NewVerifier([]string{"https://sub2api.internal.example:8443"}, attributionTestSecret, nonces)
			require.NoError(t, err)
			verifier.now = func() time.Time {
				if !test.now.IsZero() {
					return test.now
				}
				return signedAt
			}
			claims, err := verifier.VerifyRequest(context.Background(), req)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				require.Equal(t, Claims{}, claims)
				require.Empty(t, nonces.reserved)
				return
			}
			require.NoError(t, err)
			require.Equal(t, Claims{RequestID: "newapi-compatibility-8871", UserID: 8871, TokenID: 8872, ChannelID: 17, Model: "claude-sonnet-4-6", Source: "new-api"}, claims)
		})
	}
}

func TestNewVerifierRejectsIncompleteTrustConfiguration(t *testing.T) {
	nonces := &memoryNonceStore{reserved: make(map[string]struct{})}
	for _, test := range []struct {
		name    string
		origins []string
		secret  string
		nonces  NonceStore
	}{
		{name: "missing origins", secret: attributionTestSecret, nonces: nonces},
		{name: "missing secret", origins: []string{"https://sub2api.internal.example"}, nonces: nonces},
		{name: "weak secret", origins: []string{"https://sub2api.internal.example"}, secret: "short", nonces: nonces},
		{name: "missing replay guard", origins: []string{"https://sub2api.internal.example"}, secret: attributionTestSecret},
	} {
		t.Run(test.name, func(t *testing.T) {
			verifier, err := NewVerifier(test.origins, test.secret, test.nonces)
			require.ErrorIs(t, err, ErrInvalidConfiguration)
			require.Nil(t, verifier)
		})
	}
}
