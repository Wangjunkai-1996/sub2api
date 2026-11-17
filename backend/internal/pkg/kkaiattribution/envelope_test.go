package kkaiattribution

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const attributionTestSecret = "0123456789abcdef0123456789abcdef"

type memoryNonceStore struct {
	reserved map[string]struct{}
	err      error
}

func (s *memoryNonceStore) Reserve(_ context.Context, nonce string, _ time.Time) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	if _, exists := s.reserved[nonce]; exists {
		return false, nil
	}
	s.reserved[nonce] = struct{}{}
	return true, nil
}

func validAttributionClaims() Claims {
	return Claims{
		RequestID:     "request-123",
		UserID:        10,
		TokenID:       11,
		ChannelID:     12,
		MultiKeyIndex: 2,
		Model:         "gpt-test",
		Source:        "new-api",
	}
}

func TestSignerAndVerifierAuthenticateClaimsAndRejectReplay(t *testing.T) {
	now := time.Unix(1_720_000_000, 0)
	signer, err := NewSigner([]string{"https://guard.internal.example:8443"}, attributionTestSecret)
	require.NoError(t, err)
	signer.now = func() time.Time { return now }
	signer.nonce = func() (string, error) { return "00112233445566778899aabbccddeeff", nil }
	req, err := http.NewRequest(http.MethodPost, "https://guard.internal.example:8443/v1/policy?mode=strict", nil)
	require.NoError(t, err)

	applied, err := signer.ApplyRequest(req, validAttributionClaims())
	require.NoError(t, err)
	require.True(t, applied)
	require.Empty(t, req.Header.Get(LegacyTokenNameHeader))
	require.Equal(t, "request-123", req.Header.Get(RequestIDHeader))
	require.NotEmpty(t, req.Header.Get(SignatureHeader))

	nonces := &memoryNonceStore{reserved: make(map[string]struct{})}
	verifier, err := NewVerifier([]string{"https://guard.internal.example:8443"}, attributionTestSecret, nonces)
	require.NoError(t, err)
	verifier.now = func() time.Time { return now }
	claims, err := verifier.VerifyRequest(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, validAttributionClaims(), claims)

	_, err = verifier.VerifyRequest(context.Background(), req)
	require.ErrorIs(t, err, ErrReplay)
}

func TestVerifierRejectsTamperedClaims(t *testing.T) {
	now := time.Unix(1_720_000_000, 0)
	signer, err := NewSigner([]string{"https://guard.internal.example:8443"}, attributionTestSecret)
	require.NoError(t, err)
	signer.now = func() time.Time { return now }
	signer.nonce = func() (string, error) { return "00112233445566778899aabbccddeeff", nil }
	req, err := http.NewRequest(http.MethodPost, "https://guard.internal.example:8443/v1/policy", nil)
	require.NoError(t, err)
	_, err = signer.ApplyRequest(req, validAttributionClaims())
	require.NoError(t, err)
	req.Header.Set(ModelHeader, "forged-model")

	verifier, err := NewVerifier(
		[]string{"https://guard.internal.example:8443"},
		attributionTestSecret,
		&memoryNonceStore{reserved: make(map[string]struct{})},
	)
	require.NoError(t, err)
	verifier.now = func() time.Time { return now }
	_, err = verifier.VerifyRequest(context.Background(), req)
	require.ErrorIs(t, err, ErrInvalidSignature)
}

func TestSignerStripsForgedHeadersForUnlistedOrigin(t *testing.T) {
	signer, err := NewSigner([]string{"https://guard.internal.example:8443"}, attributionTestSecret)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, "https://api.external.example/v1/chat", nil)
	require.NoError(t, err)
	for _, name := range HeaderNames() {
		req.Header.Set(name, "forged")
	}
	req.Header.Set(LegacyTokenNameHeader, "sensitive-token-name")

	applied, err := signer.ApplyRequest(req, validAttributionClaims())
	require.NoError(t, err)
	require.False(t, applied)
	for _, name := range HeaderNames() {
		require.Empty(t, req.Header.Get(name))
	}
	require.Empty(t, req.Header.Get(LegacyTokenNameHeader))
}

func TestSignerRejectsHostOverrideForAllowedURL(t *testing.T) {
	signer, err := NewSigner([]string{"https://guard.internal.example:8443"}, attributionTestSecret)
	require.NoError(t, err)
	req, err := http.NewRequest(http.MethodPost, "https://guard.internal.example:8443/v1/policy", nil)
	require.NoError(t, err)
	req.Host = "other-service.internal.example:8443"

	applied, err := signer.ApplyRequest(req, validAttributionClaims())
	require.ErrorIs(t, err, ErrOriginNotAllowed)
	require.False(t, applied)
	require.Empty(t, req.Header.Get(SignatureHeader))
}

func TestVerifierRejectsStaleTimestamp(t *testing.T) {
	signedAt := time.Unix(1_720_000_000, 0)
	signer, err := NewSigner([]string{"https://guard.internal.example:8443"}, attributionTestSecret)
	require.NoError(t, err)
	signer.now = func() time.Time { return signedAt }
	signer.nonce = func() (string, error) { return "00112233445566778899aabbccddeeff", nil }
	req, err := http.NewRequest(http.MethodPost, "https://guard.internal.example:8443/v1/policy", nil)
	require.NoError(t, err)
	_, err = signer.ApplyRequest(req, validAttributionClaims())
	require.NoError(t, err)

	verifier, err := NewVerifier(
		[]string{"https://guard.internal.example:8443"},
		attributionTestSecret,
		&memoryNonceStore{reserved: make(map[string]struct{})},
	)
	require.NoError(t, err)
	verifier.now = func() time.Time { return signedAt.Add(10 * time.Minute) }
	_, err = verifier.VerifyRequest(context.Background(), req)
	require.ErrorIs(t, err, ErrStaleEnvelope)
}
