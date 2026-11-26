package service

import (
	"bytes"
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const openAIImagesFailureMessageMaxBytes = 2048

var (
	imageFailureAuthorization = regexp.MustCompile(`(?i)\bauthorization\s*["']?\s*[:=]\s*(?:"[^"\r\n]*"|'[^'\r\n]*'|[^,;}\r\n]+)`)
	imageFailureAuthScheme    = regexp.MustCompile(`(?i)\b(?:bearer|basic)\s+[^\s"',;}]+`)
	imageFailureCredential    = regexp.MustCompile(`(?i)\b(?:api[_-]?key|access[_-]?token|refresh[_-]?token|client[_-]?secret|token|secret|password|passwd|credential)\s*["']?\s*[:=]\s*(?:"[^"\r\n]*"|'[^'\r\n]*'|[^,;}\r\n]+)`)
	imageFailureAPIKey        = regexp.MustCompile(`\bsk-[A-Za-z0-9_-]+`)
	imageFailureURL           = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s<>"']+`)
	imageFailureIP            = regexp.MustCompile(`\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?::[0-9]+)?\b|\[[0-9a-fA-F:]+\](?::[0-9]+)?`)
	imageFailureIdentifier    = regexp.MustCompile(`^[A-Za-z0-9_.:-]{1,128}$`)
	imageFailureMarkup        = regexp.MustCompile(`(?is)^\s*<(?:!doctype|\?xml|/?[a-z][a-z0-9]*(?:\s|>))`)
)

// openAIImagesDefinitiveFailure only recognizes a complete, generic provider
// failure. The caller must separately prove the first body read completed and
// that no earlier dispatched attempt has an unproven outcome.
func openAIImagesDefinitiveFailure(body []byte) bool {
	if !utf8.Valid(body) || !gjson.ValidBytes(body) || !gjson.ParseBytes(body).IsObject() ||
		!readUniqueImageFailureJSON(json.NewDecoder(bytes.NewReader(body))) || extractOpenAIImageCountFromJSONBytes(body) != 0 {
		return false
	}
	errorObject := gjson.GetBytes(body, "error")
	errorType, message := errorObject.Get("type"), errorObject.Get("message")
	if !errorObject.IsObject() || errorType.Type != gjson.String || message.Type != gjson.String ||
		strings.TrimSpace(errorType.Str) == "" || strings.TrimSpace(message.Str) == "" {
		return false
	}
	// Dedicated policy, authentication, capacity and balance semantics win.
	return openAIImagesSSEErrorStatus(errorType.Str, errorObject.Get("code").String()) == 502 &&
		!isOpenAIImagesContentPolicyRefusal(errorType.Str+" "+errorObject.Get("code").String()) &&
		!isOpenAIImagesInsufficientBalance(body) &&
		!isOpenAIRequestScopedCapacityShed(message.Str, body) &&
		!isOpenAIUpstreamAccessStateError(message.Str, body)
}

// Duplicate JSON fields have different first/last-value behavior across
// parsers. None may be used to certify a financially terminal image failure.
func readUniqueImageFailureJSON(decoder *json.Decoder) bool {
	token, err := decoder.Token()
	if err != nil {
		return false
	}
	switch token {
	case json.Delim('{'):
		seen := make(map[string]bool)
		for decoder.More() {
			key, err := decoder.Token()
			name, ok := key.(string)
			if err != nil || !ok || seen[name] {
				return false
			}
			seen[name] = true
			if !readUniqueImageFailureJSON(decoder) {
				return false
			}
		}
		_, err = decoder.Token()
	case json.Delim('['):
		for decoder.More() {
			if !readUniqueImageFailureJSON(decoder) {
				return false
			}
		}
		_, err = decoder.Token()
	}
	return err == nil
}

// SanitizeOpenAIImagesFailureMessage bounds messages selected by the final
// error rule. Exact active credentials are removed earlier, in the service
// that owns them; this function does not claim to recognize arbitrary secrets.
func SanitizeOpenAIImagesFailureMessage(message string) string {
	return sanitizeOpenAIImagesFailureMessage(message)
}

func safeOpenAIImagesFailureIdentifier(value string, sensitiveValues ...string) string {
	if imageFailureIdentifier.MatchString(value) && sanitizeOpenAIImagesFailureMessage(value, sensitiveValues...) == value {
		return value
	}
	return ""
}

func stripUnprovenImageFailureCode(body []byte) ([]byte, bool) {
	if !gjson.ValidBytes(body) {
		return body, false
	}
	changed := false
	if !readUniqueImageFailureJSON(json.NewDecoder(bytes.NewReader(body))) {
		// Resolve ambiguous duplicate fields once before removing the reserved
		// code. UseNumber preserves numeric output/usage values while doing so.
		decoder := json.NewDecoder(bytes.NewReader(body))
		decoder.UseNumber()
		var payload any
		if decoder.Decode(&payload) != nil {
			return []byte(`{"type":"error","error":{"type":"upstream_error","message":"Invalid image provider response"}}`), true
		}
		canonical, err := json.Marshal(payload)
		if err != nil {
			return []byte(`{"type":"error","error":{"type":"upstream_error","message":"Invalid image provider response"}}`), true
		}
		body = canonical
		changed = true
	}
	for _, path := range []string{"error.code", "response.error.code", "code"} {
		if strings.EqualFold(strings.TrimSpace(gjson.GetBytes(body, path).String()), "image_provider_error") {
			body, _ = sjson.DeleteBytes(body, path)
			changed = true
		}
	}
	return body, changed
}

func sanitizeOpenAIImagesFailureMessage(message string, sensitiveValues ...string) string {
	if len(message) > 64<<10 || !utf8.ValidString(message) {
		return "Image provider returned an error"
	}
	// Normalize before matching: dropping controls afterwards could reassemble
	// a split credential label or URL that escaped the checks.
	message = strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return ' '
		}
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return -1
		}
		return r
	}, message)
	message = strings.ReplaceAll(message, `\/`, "/")
	trimmed := strings.TrimSpace(message)
	if (json.Valid([]byte(trimmed)) && (strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "["))) || imageFailureMarkup.MatchString(trimmed) {
		return "Image provider returned an error"
	}
	for _, value := range sensitiveValues {
		if value == "" {
			continue
		}
		message = strings.ReplaceAll(message, value, "[redacted]")
		if address, err := url.Parse(value); err == nil && address.Hostname() != "" {
			message = strings.ReplaceAll(message, address.Hostname(), "[redacted address]")
		}
		encoded, err := json.Marshal(value)
		if err == nil && len(encoded) > 2 {
			message = strings.ReplaceAll(message, string(encoded[1:len(encoded)-1]), "[redacted]")
		}
	}
	message = sanitizeUpstreamErrorMessage(message)
	message = imageFailureAuthorization.ReplaceAllString(message, "[redacted authorization]")
	message = imageFailureAuthScheme.ReplaceAllString(message, "[redacted authorization]")
	message = imageFailureCredential.ReplaceAllString(message, "[redacted credential]")
	message = imageFailureAPIKey.ReplaceAllString(message, "[redacted credential]")
	message = imageFailureURL.ReplaceAllString(message, "[redacted URL]")
	message = imageFailureIP.ReplaceAllString(message, "[redacted address]")
	message = strings.TrimSpace(message)
	if len(message) > openAIImagesFailureMessageMaxBytes {
		message = message[:openAIImagesFailureMessageMaxBytes]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
	}
	if message == "" {
		return "Image provider returned an error"
	}
	return message
}
