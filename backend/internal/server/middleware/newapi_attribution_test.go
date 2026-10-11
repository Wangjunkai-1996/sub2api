//go:build unit

package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/kkaiattribution"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

const (
	newAPIAttributionTestOrigin = "https://sub2api.internal.example:8443"
	newAPIAttributionTestSecret = "0123456789abcdef0123456789abcdef"
)

type newAPIAttributionTestNonces struct {
	reserved map[string]time.Time
	err      error
}

func (s *newAPIAttributionTestNonces) Reserve(_ context.Context, nonce string, expiresAt time.Time) (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	if _, exists := s.reserved[nonce]; exists {
		return false, nil
	}
	s.reserved[nonce] = expiresAt
	return true, nil
}

func newAPIAttributionSignedRequest(t *testing.T, origin, path string, userID int) *http.Request {
	t.Helper()
	signer, err := kkaiattribution.NewSigner([]string{origin}, newAPIAttributionTestSecret)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, origin+path, nil)
	applied, err := signer.ApplyRequest(req, kkaiattribution.Claims{
		RequestID: "newapi-request-8871", UserID: userID, TokenID: 12,
		ChannelID: 23, Model: "claude-sonnet-4-6", Source: "new-api",
	})
	require.NoError(t, err)
	require.True(t, applied)
	// A reverse proxy can strip the destination port from Host and always
	// delivers a relative request URL to the backend.
	req.URL.Scheme, req.URL.Host = "", ""
	req.Host = "sub2api.internal.example"
	return req
}

type newAPIAttributionObserved struct {
	billingID any
	trustedID any
	headers   http.Header
	called    bool
}

func newAPIAttributionTestRouter(handler gin.HandlerFunc) (*gin.Engine, *newAPIAttributionObserved) {
	observed := &newAPIAttributionObserved{}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		// Both NewAPI end users share the same authenticated Sub2API billing user.
		ctx := context.WithValue(c.Request.Context(), ctxkey.UserID, int64(1))
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		observed.billingID = c.Request.Context().Value(ctxkey.UserID)
		observed.trustedID = c.Request.Context().Value(ctxkey.NewAPIUserID)
		observed.headers = c.Request.Header.Clone()
	})
	router.Use(handler)
	router.NoRoute(func(c *gin.Context) {
		observed.called = true
		c.Status(http.StatusOK)
	})
	return router, observed
}

func TestNewAPIAttributionSignedUsersRemainDistinct(t *testing.T) {
	gin.SetMode(gin.TestMode)
	nonces := &newAPIAttributionTestNonces{reserved: make(map[string]time.Time)}
	handler, err := newAPIAttribution(config.NewAPIAttributionConfig{
		Origins: []string{newAPIAttributionTestOrigin}, Secret: newAPIAttributionTestSecret,
	}, nonces)
	require.NoError(t, err)
	for _, userID := range []int{8871, 42, 8871} {
		req := newAPIAttributionSignedRequest(t, newAPIAttributionTestOrigin, "/v1/messages?beta=true", userID)
		// Forwarded headers cannot override the configured signed origin.
		req.Header.Set("X-Forwarded-Host", "attacker.example")
		req.Header.Set("X-Forwarded-Proto", "http")
		req.Header.Set("X-NewAPI-Debug", "internal-details")
		router, observed := newAPIAttributionTestRouter(handler)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, req)
		require.Equal(t, http.StatusOK, recorder.Code)
		require.True(t, observed.called)
		require.Equal(t, int64(userID), observed.trustedID)
		require.Equal(t, int64(1), observed.billingID)
		for name := range observed.headers {
			require.False(t, strings.HasPrefix(strings.ToLower(name), "x-newapi-"), name)
		}
	}
	require.Len(t, nonces.reserved, 3)
}

func TestNewAPIAttributionRejectsInvalidEnvelopes(t *testing.T) {
	for _, test := range []struct {
		name     string
		origin   string
		userID   int
		mutate   func(*http.Request)
		storeErr error
	}{
		{name: "forged user", userID: 8871, mutate: func(r *http.Request) { r.Header.Set(kkaiattribution.UserIDHeader, "42") }},
		{name: "unsigned user", userID: 8871, mutate: func(r *http.Request) { r.Header = http.Header{}; r.Header.Set(kkaiattribution.UserIDHeader, "8871") }},
		{name: "missing signature", userID: 8871, mutate: func(r *http.Request) { r.Header.Del(kkaiattribution.SignatureHeader) }},
		{name: "invalid signature", userID: 8871, mutate: func(r *http.Request) { r.Header.Set(kkaiattribution.SignatureHeader, strings.Repeat("0", 64)) }},
		{name: "duplicate identity", userID: 8871, mutate: func(r *http.Request) { r.Header.Add(kkaiattribution.UserIDHeader, "8871") }},
		{name: "duplicate signature", userID: 8871, mutate: func(r *http.Request) {
			r.Header.Add(kkaiattribution.SignatureHeader, r.Header.Get(kkaiattribution.SignatureHeader))
		}},
		{name: "missing model", userID: 8871, mutate: func(r *http.Request) { r.Header.Del(kkaiattribution.ModelHeader) }},
		{name: "expired timestamp", userID: 8871, mutate: func(r *http.Request) {
			r.Header.Set(kkaiattribution.TimestampHeader, strconv.FormatInt(time.Now().Add(-5*time.Minute).Unix(), 10))
		}},
		{name: "future timestamp", userID: 8871, mutate: func(r *http.Request) {
			r.Header.Set(kkaiattribution.TimestampHeader, strconv.FormatInt(time.Now().Add(5*time.Minute).Unix(), 10))
		}},
		{name: "invalid nonce", userID: 8871, mutate: func(r *http.Request) { r.Header.Set(kkaiattribution.NonceHeader, "not-a-nonce") }},
		{name: "legacy identity", userID: 8871, mutate: func(r *http.Request) { r.Header.Set(kkaiattribution.LegacyTokenNameHeader, "internal-token-name") }},
		{name: "zero user", userID: 0},
		{name: "path mismatch", userID: 8871, mutate: func(r *http.Request) { r.URL.Path = "/another/v1/messages" }},
		{name: "query mismatch", userID: 8871, mutate: func(r *http.Request) { r.URL.RawQuery = "beta=false" }},
		{name: "method mismatch", userID: 8871, mutate: func(r *http.Request) { r.Method = http.MethodGet }},
		{name: "untrusted signed origin", userID: 8871, origin: "https://another.internal.example:8443"},
		{name: "nonce store unavailable", userID: 8871, storeErr: errors.New("unavailable")},
	} {
		t.Run(test.name, func(t *testing.T) {
			nonces := &newAPIAttributionTestNonces{reserved: make(map[string]time.Time), err: test.storeErr}
			handler, err := newAPIAttribution(config.NewAPIAttributionConfig{
				Origins: []string{newAPIAttributionTestOrigin}, Secret: newAPIAttributionTestSecret,
			}, nonces)
			require.NoError(t, err)
			origin := test.origin
			if origin == "" {
				origin = newAPIAttributionTestOrigin
			}
			req := newAPIAttributionSignedRequest(t, origin, "/v1/messages?beta=true", test.userID)
			if test.mutate != nil {
				test.mutate(req)
			}
			router, observed := newAPIAttributionTestRouter(handler)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			require.Equal(t, http.StatusBadRequest, recorder.Code)
			require.Contains(t, recorder.Body.String(), "INVALID_NEWAPI_ATTRIBUTION")
			require.False(t, observed.called)
			require.Nil(t, observed.trustedID)
			require.Equal(t, int64(1), observed.billingID)
			for name := range observed.headers {
				require.False(t, strings.HasPrefix(strings.ToLower(name), "x-newapi-"), name)
			}
		})
	}
}

func TestNewAPIAttributionRejectsReplayAcrossInstances(t *testing.T) {
	nonces := &newAPIAttributionTestNonces{reserved: make(map[string]time.Time)}
	cfg := config.NewAPIAttributionConfig{Origins: []string{newAPIAttributionTestOrigin}, Secret: newAPIAttributionTestSecret}
	original := newAPIAttributionSignedRequest(t, newAPIAttributionTestOrigin, "/v1/messages", 8871)
	for _, wantStatus := range []int{http.StatusOK, http.StatusBadRequest} {
		handler, err := newAPIAttribution(cfg, nonces)
		require.NoError(t, err)
		router, observed := newAPIAttributionTestRouter(handler)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, original.Clone(context.Background()))
		require.Equal(t, wantStatus, recorder.Code)
		if wantStatus == http.StatusOK {
			require.Equal(t, int64(8871), observed.trustedID)
		} else {
			require.False(t, observed.called)
			require.Nil(t, observed.trustedID)
		}
		require.Equal(t, int64(1), observed.billingID)
	}
	require.Len(t, nonces.reserved, 1)
}

func TestNewAPIAttributionDirectAndNonMessagesCompatibility(t *testing.T) {
	for _, test := range []struct {
		name     string
		disabled bool
		path     string
		signed   bool
	}{
		{name: "direct messages", path: "/v1/messages"},
		{name: "direct messages while disabled", disabled: true, path: "/v1/messages"},
		{name: "signed messages while disabled", disabled: true, path: "/v1/messages", signed: true},
		{name: "signed non Messages", path: "/v1/chat/completions", signed: true},
		{name: "signed non Messages while disabled", disabled: true, path: "/v1/responses", signed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := config.NewAPIAttributionConfig{}
			if !test.disabled {
				cfg = config.NewAPIAttributionConfig{Origins: []string{newAPIAttributionTestOrigin}, Secret: newAPIAttributionTestSecret}
			}
			nonces := &newAPIAttributionTestNonces{reserved: make(map[string]time.Time)}
			handler, err := newAPIAttribution(cfg, nonces)
			require.NoError(t, err)
			req := httptest.NewRequest(http.MethodPost, test.path, nil)
			if test.signed {
				req = newAPIAttributionSignedRequest(t, newAPIAttributionTestOrigin, test.path, 8871)
			}
			req.Header.Set("X-NewAPI-Private-Debug", "must-not-be-forwarded")
			req.Header.Set("Anthropic-Version", "2023-06-01")
			router, observed := newAPIAttributionTestRouter(handler)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, req)
			require.Equal(t, http.StatusOK, recorder.Code)
			require.True(t, observed.called)
			require.Nil(t, observed.trustedID)
			require.Equal(t, int64(1), observed.billingID)
			require.Equal(t, "2023-06-01", observed.headers.Get("Anthropic-Version"))
			for name := range observed.headers {
				require.False(t, strings.HasPrefix(strings.ToLower(name), "x-newapi-"), name)
			}
			require.Empty(t, nonces.reserved)
		})
	}
}

func TestNewAPIAttributionConfigurationFailsClosed(t *testing.T) {
	handler, err := NewAPIAttribution(config.NewAPIAttributionConfig{}, nil)
	require.NoError(t, err)
	require.NotNil(t, handler)
	for _, cfg := range []config.NewAPIAttributionConfig{
		{Origins: []string{newAPIAttributionTestOrigin}},
		{Secret: newAPIAttributionTestSecret},
		{Origins: []string{newAPIAttributionTestOrigin}, Secret: "short"},
	} {
		handler, err = newAPIAttribution(cfg, &newAPIAttributionTestNonces{reserved: make(map[string]time.Time)})
		require.ErrorIs(t, err, kkaiattribution.ErrInvalidConfiguration)
		require.Nil(t, handler)
	}
	handler, err = NewAPIAttribution(config.NewAPIAttributionConfig{
		Origins: []string{newAPIAttributionTestOrigin}, Secret: newAPIAttributionTestSecret,
	}, nil)
	require.ErrorIs(t, err, kkaiattribution.ErrInvalidConfiguration)
	require.Nil(t, handler)
}
