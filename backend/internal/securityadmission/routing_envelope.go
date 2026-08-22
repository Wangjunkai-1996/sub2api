package securityadmission

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const RoutingEnvelopeWindowBytes = 4 << 10

// Chat and Messages are decoded downstream with encoding/json, which accepts
// at most 10,000 nested containers. Complete routing validation uses the same
// boundary instead of the lower admission-inspection limit: exceeding that
// lower limit makes content uninspectable, but does not make otherwise valid
// request JSON malformed.
const completeRoutingEnvelopeMaxDepth = 10_000

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

// ExtractCompleteRoutingEnvelope validates the complete JSON root before using
// the bounded envelope extractor. It enforces canonical, unique top-level
// model and stream fields; large payload values are skipped directly in body
// without copying them.
//
// This is intended for oversized HTTP requests whose body is already resident
// in memory. Bounded callers such as WebSocket preflight must continue to use
// ExtractRoutingEnvelope.
func ExtractCompleteRoutingEnvelope(protocol string, body []byte) (RoutingEnvelope, error) {
	if err := ValidateCompleteRoutingEnvelope(body); err != nil {
		return RoutingEnvelope{}, err
	}
	return ExtractRoutingEnvelope(protocol, body)
}

// ValidateCompleteRoutingEnvelope validates the complete JSON root and rejects
// duplicate or non-canonical top-level model and stream fields. Non-canonical
// case variants are unsafe because encoding/json matches struct fields without
// case sensitivity while the routing layer uses exact JSON paths. Other
// duplicate keys retain their existing protocol semantics.
func ValidateCompleteRoutingEnvelope(body []byte) error {
	if err := validateCompleteRoutingEnvelope(body); err != nil {
		if errors.Is(err, ErrRoutingEnvelopeUnavailable) {
			return err
		}
		return fmt.Errorf("%w: complete root: %v", ErrRoutingEnvelopeInvalid, err)
	}
	return nil
}

func validateCompleteRoutingEnvelope(body []byte) error {
	limits := CurrentLimits()
	// Primitive parsing reuses jsonParser's strict literal and number scanners.
	// A request body cannot contain enough tokens to reach maxInt.
	limits.MaxTokens = int(^uint(0) >> 1)
	p := jsonParser{body: body, limits: limits}
	if err := p.expect('{'); err != nil {
		return err
	}

	p.skipWhitespace()
	if p.pos < len(p.body) && p.body[p.pos] == '}' {
		p.pos++
		return p.finish()
	}

	var seen routingEnvelopeField
	for {
		key, err := p.scanString(true)
		if err != nil {
			return err
		}
		field, canonical, err := completeRoutingEnvelopeFieldForKey(p.body, key)
		if err != nil {
			return fmt.Errorf("%w: decode routing key: %v", ErrRoutingEnvelopeUnavailable, err)
		}
		if field != 0 && !canonical {
			return p.invalid(fmt.Sprintf("routing field %q must use canonical lowercase spelling", completeRoutingEnvelopeFieldName(field)))
		}
		if field != 0 && seen&field != 0 {
			return fmt.Errorf("duplicate routing field %q", completeRoutingEnvelopeFieldName(field))
		}
		if field != 0 {
			seen |= field
		}

		if err := p.expect(':'); err != nil {
			return err
		}
		if err := skipCompleteJSONValue(&p, 1, completeRoutingEnvelopeMaxDepth); err != nil {
			return err
		}

		p.skipWhitespace()
		if p.pos >= len(p.body) {
			return p.invalid("unterminated object")
		}
		switch p.body[p.pos] {
		case ',':
			p.pos++
			p.skipWhitespace()
			if p.pos < len(p.body) && p.body[p.pos] == '}' {
				return p.invalid("trailing comma")
			}
		case '}':
			p.pos++
			return p.finish()
		default:
			return p.invalid("expected object separator")
		}
	}
}

func completeRoutingEnvelopeFieldForKey(body []byte, key stringRef) (routingEnvelopeField, bool, error) {
	if !key.escaped {
		raw := body[key.start:key.end]
		switch {
		case bytes.Equal(raw, []byte("model")):
			return routingEnvelopeModel, true, nil
		case bytes.Equal(raw, []byte("stream")):
			return routingEnvelopeStream, true, nil
		case bytes.EqualFold(raw, []byte("model")):
			return routingEnvelopeModel, false, nil
		case bytes.EqualFold(raw, []byte("stream")):
			return routingEnvelopeStream, false, nil
		default:
			return 0, false, nil
		}
	}
	// Only strings with the decoded length of a routing key can match. This
	// avoids allocating for arbitrarily large escaped non-routing keys.
	if key.runes != len("model") && key.runes != len("stream") {
		return 0, false, nil
	}
	decoded, err := key.decode(body)
	if err != nil {
		return 0, false, err
	}
	switch {
	case decoded == "model":
		return routingEnvelopeModel, true, nil
	case decoded == "stream":
		return routingEnvelopeStream, true, nil
	case strings.EqualFold(decoded, "model"):
		return routingEnvelopeModel, false, nil
	case strings.EqualFold(decoded, "stream"):
		return routingEnvelopeStream, false, nil
	default:
		return 0, false, nil
	}
}

func completeRoutingEnvelopeFieldName(field routingEnvelopeField) string {
	switch field {
	case routingEnvelopeModel:
		return "model"
	case routingEnvelopeStream:
		return "stream"
	default:
		return "unknown"
	}
}

type completeJSONContainerState uint8

const (
	completeJSONObjectFirstKey completeJSONContainerState = iota
	completeJSONObjectNextKey
	completeJSONObjectAfterValue
	completeJSONArrayFirstValue
	completeJSONArrayNextValue
	completeJSONArrayAfterValue
)

func skipCompleteJSONValue(p *jsonParser, depth, maxDepth int) error {
	// Keep ordinary payloads allocation-free. Exceptionally deep input grows a
	// one-byte-per-container state stack instead of growing the goroutine stack.
	var inline [32]completeJSONContainerState
	stack := inline[:0]
	state, container, err := startCompleteJSONValue(p, depth, maxDepth)
	if err != nil {
		return err
	}
	if container {
		stack = append(stack, state)
	}

	for len(stack) > 0 {
		index := len(stack) - 1
		switch stack[index] {
		case completeJSONObjectFirstKey, completeJSONObjectNextKey:
			first := stack[index] == completeJSONObjectFirstKey
			p.skipWhitespace()
			if first && p.pos < len(p.body) && p.body[p.pos] == '}' {
				p.pos++
				stack = stack[:index]
				continue
			}
			if _, err := p.scanString(false); err != nil {
				return err
			}
			if err := p.expect(':'); err != nil {
				return err
			}
			stack[index] = completeJSONObjectAfterValue
			state, container, err := startCompleteJSONValue(p, depth+len(stack), maxDepth)
			if err != nil {
				return err
			}
			if container {
				stack = append(stack, state)
			}

		case completeJSONArrayFirstValue, completeJSONArrayNextValue:
			first := stack[index] == completeJSONArrayFirstValue
			p.skipWhitespace()
			if first && p.pos < len(p.body) && p.body[p.pos] == ']' {
				p.pos++
				stack = stack[:index]
				continue
			}
			stack[index] = completeJSONArrayAfterValue
			state, container, err := startCompleteJSONValue(p, depth+len(stack), maxDepth)
			if err != nil {
				return err
			}
			if container {
				stack = append(stack, state)
			}

		case completeJSONObjectAfterValue:
			p.skipWhitespace()
			if p.pos >= len(p.body) {
				return p.invalid("unterminated object")
			}
			switch p.body[p.pos] {
			case ',':
				p.pos++
				p.skipWhitespace()
				if p.pos < len(p.body) && p.body[p.pos] == '}' {
					return p.invalid("trailing comma")
				}
				stack[index] = completeJSONObjectNextKey
			case '}':
				p.pos++
				stack = stack[:index]
			default:
				return p.invalid("expected object separator")
			}

		case completeJSONArrayAfterValue:
			p.skipWhitespace()
			if p.pos >= len(p.body) {
				return p.invalid("unterminated array")
			}
			switch p.body[p.pos] {
			case ',':
				p.pos++
				p.skipWhitespace()
				if p.pos < len(p.body) && p.body[p.pos] == ']' {
					return p.invalid("trailing comma")
				}
				stack[index] = completeJSONArrayNextValue
			case ']':
				p.pos++
				stack = stack[:index]
			default:
				return p.invalid("expected array separator")
			}
		}
	}
	return nil
}

func startCompleteJSONValue(
	p *jsonParser,
	depth, maxDepth int,
) (completeJSONContainerState, bool, error) {
	switch p.peek() {
	case '{':
		if depth >= maxDepth {
			return 0, false, p.invalid(fmt.Sprintf("complete root nesting exceeds %d", maxDepth))
		}
		p.pos++
		return completeJSONObjectFirstKey, true, nil
	case '[':
		if depth >= maxDepth {
			return 0, false, p.invalid(fmt.Sprintf("complete root nesting exceeds %d", maxDepth))
		}
		p.pos++
		return completeJSONArrayFirstValue, true, nil
	case '"':
		return 0, false, p.skipString()
	case 't':
		return 0, false, p.parseLiteral("true")
	case 'f':
		return 0, false, p.parseLiteral("false")
	case 'n':
		return 0, false, p.parseLiteral("null")
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return 0, false, p.parseNumber()
	default:
		return 0, false, p.invalid("expected value")
	}
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
