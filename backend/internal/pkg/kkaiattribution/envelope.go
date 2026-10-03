package kkaiattribution

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	RequestIDHeader       = "X-NewAPI-Request-Id"
	UserIDHeader          = "X-NewAPI-User-Id"
	TokenIDHeader         = "X-NewAPI-Token-Id"
	ChannelIDHeader       = "X-NewAPI-Channel-Id"
	MultiKeyIndexHeader   = "X-NewAPI-Multi-Key-Index"
	ModelHeader           = "X-NewAPI-Model"
	SourceHeader          = "X-NewAPI-Source"
	VersionHeader         = "X-NewAPI-Attribution-Version"
	TimestampHeader       = "X-NewAPI-Attribution-Timestamp"
	NonceHeader           = "X-NewAPI-Attribution-Nonce"
	SignatureHeader       = "X-NewAPI-Attribution-Signature"
	LegacyTokenNameHeader = "X-NewAPI-Token-Name"

	attributionVersion = "v1"
	defaultMaxSkew     = 2 * time.Minute
)

var (
	ErrInvalidConfiguration = errors.New("invalid attribution configuration")
	ErrInvalidOrigin        = errors.New("invalid attribution origin")
	ErrOriginNotAllowed     = errors.New("attribution origin is not allowed")
	ErrInvalidEnvelope      = errors.New("invalid attribution envelope")
	ErrInvalidSignature     = errors.New("invalid attribution signature")
	ErrStaleEnvelope        = errors.New("attribution envelope outside accepted time window")
	ErrReplay               = errors.New("attribution nonce replayed")
	ErrNonceStore           = errors.New("attribution nonce store failed")
)

var attributionHeaderNames = []string{
	RequestIDHeader,
	UserIDHeader,
	TokenIDHeader,
	ChannelIDHeader,
	MultiKeyIndexHeader,
	ModelHeader,
	SourceHeader,
	VersionHeader,
	TimestampHeader,
	NonceHeader,
	SignatureHeader,
}

type Claims struct {
	RequestID     string
	UserID        int
	TokenID       int
	ChannelID     int
	MultiKeyIndex int
	Model         string
	Source        string
}

func HeaderNames() []string {
	return append([]string(nil), attributionHeaderNames...)
}

func Strip(headers http.Header) {
	if headers == nil {
		return
	}
	for _, name := range attributionHeaderNames {
		headers.Del(name)
	}
	headers.Del(LegacyTokenNameHeader)
}

func claimsFromHeaders(values map[string]string) (Claims, error) {
	userID, err := parseNonNegativeInt(values[UserIDHeader])
	if err != nil {
		return Claims{}, err
	}
	tokenID, err := parseNonNegativeInt(values[TokenIDHeader])
	if err != nil {
		return Claims{}, err
	}
	channelID, err := parseNonNegativeInt(values[ChannelIDHeader])
	if err != nil {
		return Claims{}, err
	}
	multiKeyIndex, err := parseNonNegativeInt(values[MultiKeyIndexHeader])
	if err != nil {
		return Claims{}, err
	}
	claims := Claims{
		RequestID:     values[RequestIDHeader],
		UserID:        userID,
		TokenID:       tokenID,
		ChannelID:     channelID,
		MultiKeyIndex: multiKeyIndex,
		Model:         values[ModelHeader],
		Source:        values[SourceHeader],
	}
	if err := validateClaims(claims); err != nil {
		return Claims{}, err
	}
	return claims, nil
}

func validateClaims(claims Claims) error {
	if claims.UserID < 0 || claims.TokenID < 0 || claims.ChannelID < 0 || claims.MultiKeyIndex < 0 {
		return ErrInvalidEnvelope
	}
	if !validHeaderText(claims.RequestID, 128) || !validHeaderText(claims.Model, 256) || claims.Source != "new-api" {
		return ErrInvalidEnvelope
	}
	return nil
}

func validHeaderText(value string, maxLength int) bool {
	if value == "" || value != strings.TrimSpace(value) || len(value) > maxLength {
		return false
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}

func singleHeader(headers http.Header, name string) (string, error) {
	values := headers.Values(name)
	if len(values) != 1 || values[0] == "" {
		return "", ErrInvalidEnvelope
	}
	return values[0], nil
}

func parseNonNegativeInt(value string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, ErrInvalidEnvelope
	}
	return parsed, nil
}

func validNonce(value string) bool {
	if len(value) != 32 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 16
}

func envelopeMAC(secret []byte, method string, target parsedTarget, timestamp string, nonce string, claims Claims) []byte {
	mac := hmac.New(sha256.New, secret)
	for _, field := range []string{
		attributionVersion,
		method,
		target.origin.String(),
		target.requestTarget,
		timestamp,
		nonce,
		claims.RequestID,
		strconv.Itoa(claims.UserID),
		strconv.Itoa(claims.TokenID),
		strconv.Itoa(claims.ChannelID),
		strconv.Itoa(claims.MultiKeyIndex),
		claims.Model,
		claims.Source,
	} {
		_, _ = fmt.Fprintf(mac, "%d:%s\n", len(field), field)
	}
	return mac.Sum(nil)
}
