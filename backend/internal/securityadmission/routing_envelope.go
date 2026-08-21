package securityadmission

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const RoutingEnvelopeWindowBytes = 4 << 10

var (
	ErrRoutingEnvelopeUnavailable = errors.New("security admission routing envelope unavailable")
	ErrRoutingEnvelopeInvalid     = errors.New("security admission routing envelope invalid")
)

// RoutingEnvelope contains only request metadata decoded from a fixed-size
// prefix. Opaque is always true: callers must not treat this as proof that the
// complete body is valid or that trailing fields and duplicates are absent.
type RoutingEnvelope struct {
	Protocol                   Protocol
	Type                       string
	Model                      string
	Stream                     bool
	PreviousResponseID         string
	PreviousResponseIDPresent  bool
	PreviousResponseIDExplicit bool
	Opaque                     bool
	WindowBytes                int
}

type routingEnvelopeField uint8

const (
	routingEnvelopeType routingEnvelopeField = 1 << iota
	routingEnvelopeModel
	routingEnvelopeStream
	routingEnvelopePreviousResponseID
)

// ExtractRoutingEnvelope decodes only body[:min(len(body), 4KiB)] and returns
// as soon as the protocol's routing fields are complete. It exists solely for
// requests that Classify has already marked uninspectable because of body size.
func ExtractRoutingEnvelope(protocol string, body []byte) (RoutingEnvelope, error) {
	normalizedProtocol := NormalizeProtocol(protocol)
	windowBytes := len(body)
	if windowBytes > RoutingEnvelopeWindowBytes {
		windowBytes = RoutingEnvelopeWindowBytes
	}
	envelope := RoutingEnvelope{
		Protocol:    normalizedProtocol,
		Opaque:      true,
		WindowBytes: windowBytes,
	}
	if windowBytes == 0 {
		return envelope, ErrRoutingEnvelopeUnavailable
	}

	decoder := json.NewDecoder(bytes.NewReader(body[:windowBytes]))
	root, err := decoder.Token()
	if err != nil {
		return envelope, fmt.Errorf("%w: root token: %v", ErrRoutingEnvelopeInvalid, err)
	}
	if delimiter, ok := root.(json.Delim); !ok || delimiter != '{' {
		return envelope, fmt.Errorf("%w: root must be an object", ErrRoutingEnvelopeInvalid)
	}

	var seen routingEnvelopeField
	for decoder.More() {
		keyToken, tokenErr := decoder.Token()
		if tokenErr != nil {
			return envelope, fmt.Errorf("%w: object key: %v", ErrRoutingEnvelopeUnavailable, tokenErr)
		}
		key, ok := keyToken.(string)
		if !ok {
			return envelope, fmt.Errorf("%w: object key is not a string", ErrRoutingEnvelopeInvalid)
		}

		field := routingEnvelopeFieldForKey(key)
		if field != 0 && seen&field != 0 {
			return envelope, fmt.Errorf("%w: duplicate routing field %q", ErrRoutingEnvelopeInvalid, key)
		}

		var raw json.RawMessage
		if decodeErr := decoder.Decode(&raw); decodeErr != nil {
			return envelope, fmt.Errorf("%w: field %q: %v", ErrRoutingEnvelopeUnavailable, key, decodeErr)
		}
		if field == 0 {
			continue
		}
		seen |= field

		switch field {
		case routingEnvelopeType:
			if err := decodeRoutingEnvelopeString(raw, &envelope.Type); err != nil {
				return envelope, fmt.Errorf("%w: field %q: %v", ErrRoutingEnvelopeInvalid, key, err)
			}
			envelope.Type = strings.TrimSpace(envelope.Type)
		case routingEnvelopeModel:
			if err := decodeRoutingEnvelopeString(raw, &envelope.Model); err != nil {
				return envelope, fmt.Errorf("%w: field %q: %v", ErrRoutingEnvelopeInvalid, key, err)
			}
			envelope.Model = strings.TrimSpace(envelope.Model)
		case routingEnvelopeStream:
			if err := json.Unmarshal(raw, &envelope.Stream); err != nil {
				return envelope, fmt.Errorf("%w: field %q must be boolean", ErrRoutingEnvelopeInvalid, key)
			}
		case routingEnvelopePreviousResponseID:
			envelope.PreviousResponseIDPresent = true
			if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				envelope.PreviousResponseIDExplicit = true
				break
			}
			if err := decodeRoutingEnvelopeString(raw, &envelope.PreviousResponseID); err != nil {
				return envelope, fmt.Errorf("%w: field %q must be string or null", ErrRoutingEnvelopeInvalid, key)
			}
			envelope.PreviousResponseID = strings.TrimSpace(envelope.PreviousResponseID)
			envelope.PreviousResponseIDExplicit = true
		}

		if routingEnvelopeComplete(normalizedProtocol, envelope, seen) {
			return envelope, nil
		}
	}

	return envelope, ErrRoutingEnvelopeUnavailable
}

// ExtractBoundedWebSocketFrameType inspects at most the routing-envelope
// window and returns as soon as it finds a top-level string type field. A
// false result is deliberately ambiguous: the field may be absent, malformed,
// truncated, escaped, or beyond the bounded prefix.
func ExtractBoundedWebSocketFrameType(body []byte) (string, bool) {
	windowBytes := len(body)
	if windowBytes > RoutingEnvelopeWindowBytes {
		windowBytes = RoutingEnvelopeWindowBytes
	}
	if windowBytes == 0 {
		return "", false
	}

	p := jsonParser{body: body[:windowBytes], limits: CurrentLimits()}
	if err := p.expect('{'); err != nil {
		return "", false
	}
	p.skipWhitespace()
	if p.pos < len(p.body) && p.body[p.pos] == '}' {
		return "", false
	}
	for {
		key, err := p.scanString(true)
		if err != nil || key.escaped {
			return "", false
		}
		if err := p.expect(':'); err != nil {
			return "", false
		}
		if bytes.Equal(p.body[key.start:key.end], []byte("type")) {
			if p.peek() != '"' {
				return "", false
			}
			value, parseErr := p.parseString()
			if parseErr != nil {
				return "", false
			}
			decoded, decodeErr := value.decode(p.body)
			if decodeErr != nil {
				return "", false
			}
			return strings.TrimSpace(decoded), true
		}
		if err := p.skipValue(); err != nil {
			return "", false
		}
		p.skipWhitespace()
		if p.pos >= len(p.body) || p.body[p.pos] != ',' {
			return "", false
		}
		p.pos++
	}
}

func routingEnvelopeFieldForKey(key string) routingEnvelopeField {
	switch key {
	case "type":
		return routingEnvelopeType
	case "model":
		return routingEnvelopeModel
	case "stream":
		return routingEnvelopeStream
	case "previous_response_id":
		return routingEnvelopePreviousResponseID
	default:
		return 0
	}
}

func routingEnvelopeComplete(protocol Protocol, envelope RoutingEnvelope, seen routingEnvelopeField) bool {
	if seen&routingEnvelopeModel == 0 || seen&routingEnvelopeStream == 0 || envelope.Model == "" {
		return false
	}
	if protocol != ProtocolResponsesWebSocket {
		return true
	}
	return seen&routingEnvelopeType != 0 && envelope.Type == "response.create" &&
		seen&routingEnvelopePreviousResponseID != 0 && envelope.PreviousResponseIDExplicit
}

func decodeRoutingEnvelopeString(raw json.RawMessage, dst *string) error {
	if dst == nil {
		return errors.New("nil string destination")
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return errors.New("must be a string")
	}
	return nil
}
