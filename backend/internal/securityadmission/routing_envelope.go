package securityadmission

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
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
	ErrRoutingEnvelopeUnavailable   = errors.New("security admission routing envelope unavailable")
	ErrRoutingEnvelopeInvalid       = errors.New("security admission routing envelope invalid")
	ErrRoutingEnvelopeResourceLimit = errors.New("security admission routing envelope resource limit")
)

// RoutingEnvelope contains request metadata decoded from either a fixed-size
// prefix or a complete structural scan. Opaque is always true because routing
// extraction does not make the request content auditable.
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
		// A bounded prefix cannot distinguish an actually truncated root token
		// from a valid token that starts after the inspection window.
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return envelope, fmt.Errorf("%w: root token: %v", ErrRoutingEnvelopeUnavailable, err)
		}
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

// ExtractCompleteRoutingEnvelope validates the complete JSON root and extracts
// routing fields during that same scan. Large payload values are skipped
// directly in body without copying them, so routing fields may appear after
// the bounded 4 KiB inspection window.
//
// This is intended for oversized HTTP requests whose body is already resident
// in memory. Bounded callers such as WebSocket preflight must continue to use
// ExtractRoutingEnvelope.
func ExtractCompleteRoutingEnvelope(protocol string, body []byte) (RoutingEnvelope, error) {
	normalizedProtocol := NormalizeProtocol(protocol)
	envelope, seen, err := scanCompleteRoutingEnvelope(normalizedProtocol, body, true)
	if err != nil {
		if errors.Is(err, ErrRoutingEnvelopeUnavailable) {
			return envelope, err
		}
		return envelope, fmt.Errorf("%w: complete root: %v", ErrRoutingEnvelopeInvalid, err)
	}
	if !completeRoutingEnvelopeComplete(normalizedProtocol, envelope, seen) {
		return envelope, ErrRoutingEnvelopeUnavailable
	}
	return envelope, nil
}

// ValidateCompleteRoutingEnvelope validates the complete JSON root and rejects
// duplicate or non-canonical top-level model and stream fields. Non-canonical
// case variants are unsafe because encoding/json matches struct fields without
// case sensitivity while the routing layer uses exact JSON paths. Other
// duplicate keys retain their existing protocol semantics.
func ValidateCompleteRoutingEnvelope(body []byte) error {
	if _, _, err := scanCompleteRoutingEnvelope("", body, false); err != nil {
		if errors.Is(err, ErrRoutingEnvelopeUnavailable) {
			return err
		}
		return fmt.Errorf("%w: complete root: %v", ErrRoutingEnvelopeInvalid, err)
	}
	return nil
}

// ValidateCompleteResponsesJSON validates a complete Responses request object.
// Exact duplicate names are rejected in every object. Non-canonical Unicode
// case-fold aliases are rejected only along structured request paths that the
// gateway reads exactly but downstream structs decode case-insensitively;
// opaque metadata and tool-schema objects retain case-sensitive key semantics.
// Compact key storage is bounded independently from JSON syntax validity.
func ValidateCompleteResponsesJSON(body []byte) error {
	limits := CurrentLimits()
	limits.MaxTokens = int(^uint(0) >> 1)
	p := jsonParser{body: body, limits: limits}
	if p.peek() != '{' {
		return fmt.Errorf("%w: complete Responses root: %v", ErrRoutingEnvelopeInvalid, p.invalid("root must be an object"))
	}
	options := completeJSONScanOptions{
		rejectDuplicateKeys: true,
		policyScope:         completeJSONPolicyScopeRoot,
		maxTrackedKeySlots:  completeJSONMaxTrackedKeySlots,
	}
	if err := skipCompleteJSONValueWithOptions(&p, 0, completeRoutingEnvelopeMaxDepth, options); err != nil {
		if errors.Is(err, ErrRoutingEnvelopeResourceLimit) {
			return fmt.Errorf("%w: complete Responses root: %v", ErrRoutingEnvelopeResourceLimit, err)
		}
		return fmt.Errorf("%w: complete Responses root: %v", ErrRoutingEnvelopeInvalid, err)
	}
	if err := p.finish(); err != nil {
		return fmt.Errorf("%w: complete Responses root: %v", ErrRoutingEnvelopeInvalid, err)
	}
	return nil
}

func scanCompleteRoutingEnvelope(
	protocol Protocol,
	body []byte,
	extractValues bool,
) (RoutingEnvelope, routingEnvelopeField, error) {
	limits := CurrentLimits()
	// Primitive parsing reuses jsonParser's strict literal and number scanners.
	// A request body cannot contain enough tokens to reach maxInt.
	limits.MaxTokens = int(^uint(0) >> 1)
	p := jsonParser{body: body, limits: limits}
	windowBytes := len(body)
	if windowBytes > RoutingEnvelopeWindowBytes {
		windowBytes = RoutingEnvelopeWindowBytes
	}
	envelope := RoutingEnvelope{
		Protocol:    protocol,
		Opaque:      true,
		WindowBytes: windowBytes,
	}
	if err := p.expect('{'); err != nil {
		return envelope, 0, err
	}

	p.skipWhitespace()
	if p.pos < len(p.body) && p.body[p.pos] == '}' {
		p.pos++
		return envelope, 0, p.finish()
	}

	var seen routingEnvelopeField
	relevantFields := routingEnvelopeModel | routingEnvelopeStream
	if protocol == ProtocolResponsesWebSocket {
		relevantFields |= routingEnvelopeType | routingEnvelopePreviousResponseID
	}
	for {
		key, err := p.scanString(true)
		if err != nil {
			return envelope, seen, err
		}
		field, canonical, err := completeRoutingEnvelopeFieldForKey(p.body, key)
		if err != nil {
			return envelope, seen, fmt.Errorf("%w: decode routing key: %v", ErrRoutingEnvelopeUnavailable, err)
		}
		if field&relevantFields == 0 {
			field = 0
		}
		if field != 0 && !canonical {
			return envelope, seen, p.invalid(fmt.Sprintf("routing field %q must use canonical lowercase spelling", completeRoutingEnvelopeFieldName(field)))
		}
		if field != 0 && seen&field != 0 {
			return envelope, seen, fmt.Errorf("duplicate routing field %q", completeRoutingEnvelopeFieldName(field))
		}
		if field != 0 {
			seen |= field
		}

		if err := p.expect(':'); err != nil {
			return envelope, seen, err
		}
		if extractValues && field != 0 {
			if err := parseCompleteRoutingEnvelopeValue(&p, field, &envelope); err != nil {
				return envelope, seen, err
			}
		} else if err := skipCompleteJSONValue(&p, 1, completeRoutingEnvelopeMaxDepth); err != nil {
			return envelope, seen, err
		}

		p.skipWhitespace()
		if p.pos >= len(p.body) {
			return envelope, seen, p.invalid("unterminated object")
		}
		switch p.body[p.pos] {
		case ',':
			p.pos++
			p.skipWhitespace()
			if p.pos < len(p.body) && p.body[p.pos] == '}' {
				return envelope, seen, p.invalid("trailing comma")
			}
		case '}':
			p.pos++
			return envelope, seen, p.finish()
		default:
			return envelope, seen, p.invalid("expected object separator")
		}
	}
}

func parseCompleteRoutingEnvelopeValue(
	p *jsonParser,
	field routingEnvelopeField,
	envelope *RoutingEnvelope,
) error {
	if envelope == nil {
		return ErrRoutingEnvelopeUnavailable
	}
	switch field {
	case routingEnvelopeType, routingEnvelopeModel:
		if p.peek() != '"' {
			return p.invalid(fmt.Sprintf("routing field %q must be a string", completeRoutingEnvelopeFieldName(field)))
		}
		value, err := p.parseString()
		if err != nil {
			return err
		}
		decoded, err := value.decode(p.body)
		if err != nil {
			return fmt.Errorf("%w: decode routing field %q", ErrRoutingEnvelopeUnavailable, completeRoutingEnvelopeFieldName(field))
		}
		decoded = strings.TrimSpace(decoded)
		if field == routingEnvelopeType {
			envelope.Type = decoded
		} else {
			envelope.Model = decoded
		}
		return nil

	case routingEnvelopeStream:
		switch p.peek() {
		case 't':
			if err := p.parseLiteral("true"); err != nil {
				return err
			}
			envelope.Stream = true
			return nil
		case 'f':
			if err := p.parseLiteral("false"); err != nil {
				return err
			}
			envelope.Stream = false
			return nil
		default:
			return p.invalid("routing field \"stream\" must be boolean")
		}

	case routingEnvelopePreviousResponseID:
		envelope.PreviousResponseIDPresent = true
		switch p.peek() {
		case 'n':
			if err := p.parseLiteral("null"); err != nil {
				return err
			}
			envelope.PreviousResponseIDExplicit = true
			return nil
		case '"':
			value, err := p.parseString()
			if err != nil {
				return err
			}
			decoded, err := value.decode(p.body)
			if err != nil {
				return fmt.Errorf("%w: decode routing field \"previous_response_id\"", ErrRoutingEnvelopeUnavailable)
			}
			envelope.PreviousResponseID = strings.TrimSpace(decoded)
			envelope.PreviousResponseIDExplicit = true
			return nil
		default:
			return p.invalid("routing field \"previous_response_id\" must be string or null")
		}
	default:
		return skipCompleteJSONValue(p, 1, completeRoutingEnvelopeMaxDepth)
	}
}

func completeRoutingEnvelopeFieldForKey(body []byte, key stringRef) (routingEnvelopeField, bool, error) {
	if !key.escaped {
		raw := body[key.start:key.end]
		switch {
		case bytes.Equal(raw, []byte("type")):
			return routingEnvelopeType, true, nil
		case bytes.Equal(raw, []byte("model")):
			return routingEnvelopeModel, true, nil
		case bytes.Equal(raw, []byte("stream")):
			return routingEnvelopeStream, true, nil
		case bytes.Equal(raw, []byte("previous_response_id")):
			return routingEnvelopePreviousResponseID, true, nil
		case bytes.EqualFold(raw, []byte("type")):
			return routingEnvelopeType, false, nil
		case bytes.EqualFold(raw, []byte("model")):
			return routingEnvelopeModel, false, nil
		case bytes.EqualFold(raw, []byte("stream")):
			return routingEnvelopeStream, false, nil
		case bytes.EqualFold(raw, []byte("previous_response_id")):
			return routingEnvelopePreviousResponseID, false, nil
		default:
			return 0, false, nil
		}
	}
	// Only strings with the decoded length of a routing key can match. This
	// avoids allocating for arbitrarily large escaped non-routing keys.
	if key.runes != len("type") && key.runes != len("model") && key.runes != len("stream") &&
		key.runes != len("previous_response_id") {
		return 0, false, nil
	}
	decoded, err := key.decode(body)
	if err != nil {
		return 0, false, err
	}
	field := routingEnvelopeFieldForKey(decoded)
	if field != 0 {
		return field, true, nil
	}
	for _, candidate := range []struct {
		name  string
		field routingEnvelopeField
	}{
		{name: "type", field: routingEnvelopeType},
		{name: "model", field: routingEnvelopeModel},
		{name: "stream", field: routingEnvelopeStream},
		{name: "previous_response_id", field: routingEnvelopePreviousResponseID},
	} {
		if strings.EqualFold(decoded, candidate.name) {
			return candidate.field, false, nil
		}
	}
	return 0, false, nil
}

func completeRoutingEnvelopeFieldName(field routingEnvelopeField) string {
	switch field {
	case routingEnvelopeType:
		return "type"
	case routingEnvelopeModel:
		return "model"
	case routingEnvelopeStream:
		return "stream"
	case routingEnvelopePreviousResponseID:
		return "previous_response_id"
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

type completeJSONScanOptions struct {
	rejectDuplicateKeys bool
	policyScope         completeJSONPolicyScope
	maxTrackedKeySlots  int
}

type completeJSONPolicyScope uint8

const (
	completeJSONPolicyScopeNone completeJSONPolicyScope = iota
	completeJSONPolicyScopeRoot
	completeJSONPolicyScopeReasoning
	completeJSONPolicyScopeText
	completeJSONPolicyScopeInputItem
	completeJSONPolicyScopeContentPart
	completeJSONPolicyScopeTool
	completeJSONPolicyScopeToolChoice
)

func (s completeJSONPolicyScope) String() string {
	switch s {
	case completeJSONPolicyScopeRoot:
		return "Responses root"
	case completeJSONPolicyScopeReasoning:
		return "Responses reasoning"
	case completeJSONPolicyScopeText:
		return "Responses text"
	case completeJSONPolicyScopeInputItem:
		return "Responses input item"
	case completeJSONPolicyScopeContentPart:
		return "Responses content part"
	case completeJSONPolicyScopeTool:
		return "Responses tool"
	case completeJSONPolicyScopeToolChoice:
		return "Responses tool choice"
	default:
		return "generic object"
	}
}

const (
	completeJSONInlineObjectKeys   = 4
	completeJSONMinKeySlotCapacity = 8
	// Packed key references use 16 bytes each. The per-scan pool is capped at
	// roughly 32 MiB and is reused across sibling objects.
	completeJSONMaxTrackedKeySlots = 2 << 20
	completeJSONEscapedKeyEndFlag  = uint32(1 << 31)
)

var (
	completeJSONRootPolicyKeys = []string{
		"type", "model", "instructions", "input", "max_output_tokens", "max_tokens",
		"max_completion_tokens", "temperature", "top_p", "stream", "tools", "include",
		"store", "parallel_tool_calls", "reasoning", "reasoning_effort", "text",
		"tool_choice", "service_tier", "prompt_cache_key", "previous_response_id", "metadata",
	}
	completeJSONReasoningPolicyKeys = []string{"effort", "summary", "context"}
	completeJSONTextPolicyKeys      = []string{"format", "verbosity"}
	completeJSONInputItemPolicyKeys = []string{
		"type", "role", "content", "encrypted_content", "call_id", "name", "arguments",
		"id", "output", "namespace", "tools",
	}
	completeJSONContentPartPolicyKeys = []string{"type", "text", "image_url"}
	completeJSONToolPolicyKeys        = []string{
		"type", "name", "description", "parameters", "strict", "tools", "children",
		"namespace", "allowed_x_handles", "excluded_x_handles", "from_date", "to_date",
		"enable_image_understanding", "enable_video_understanding", "function",
	}
	completeJSONToolChoicePolicyKeys = []string{"type", "name", "function"}
)

type completeJSONKeyRef struct {
	hash  uint64
	start uint32
	end   uint32
}

type completeJSONContainerFrame struct {
	state    completeJSONContainerState
	scope    completeJSONPolicyScope
	keyCount int
	inline   [completeJSONInlineObjectKeys]completeJSONKeyRef
	keys     []completeJSONKeyRef
}

type completeJSONKeyPool struct {
	free           [32][][]completeJSONKeyRef
	allocatedSlots int
	maxSlots       int
}

func skipCompleteJSONValue(p *jsonParser, depth, maxDepth int) error {
	return skipCompleteJSONValueWithOptions(p, depth, maxDepth, completeJSONScanOptions{})
}

func skipCompleteJSONValueWithOptions(
	p *jsonParser,
	depth, maxDepth int,
	options completeJSONScanOptions,
) error {
	// Keep ordinary payloads allocation-free. Exceptionally deep input grows a
	// compact per-container state stack instead of growing the goroutine stack.
	// Four exact-key references live inline in each object frame. Wider objects
	// borrow compact slices from a request-local pool so sibling objects reuse
	// storage instead of allocating one map per object.
	var inline [32]completeJSONContainerFrame
	stack := inline[:0]
	keyPool := completeJSONKeyPool{maxSlots: options.maxTrackedKeySlots}
	state, container, err := startCompleteJSONValue(p, depth, maxDepth)
	if err != nil {
		return err
	}
	if container {
		stack = append(stack, completeJSONContainerFrame{state: state, scope: options.policyScope})
	}

	for len(stack) > 0 {
		index := len(stack) - 1
		switch stack[index].state {
		case completeJSONObjectFirstKey, completeJSONObjectNextKey:
			first := stack[index].state == completeJSONObjectFirstKey
			p.skipWhitespace()
			if first && p.pos < len(p.body) && p.body[p.pos] == '}' {
				p.pos++
				if err := finishCompleteJSONObjectFrame(p, &stack[index], &keyPool); err != nil {
					return err
				}
				stack[index] = completeJSONContainerFrame{}
				stack = stack[:index]
				continue
			}
			key, err := p.scanString(options.rejectDuplicateKeys)
			if err != nil {
				return err
			}
			canonicalKey, matchedPolicyKey, canonical, matchErr := completeJSONCanonicalPolicyKey(p.body, key, stack[index].scope)
			if matchErr != nil {
				return fmt.Errorf("%w: decode policy key", ErrRoutingEnvelopeUnavailable)
			}
			if matchedPolicyKey && !canonical {
				return p.invalid(fmt.Sprintf("policy field %q in %s must use canonical spelling", canonicalKey, stack[index].scope))
			}
			if options.rejectDuplicateKeys {
				if err := recordCompleteJSONObjectKey(p, &stack[index], key, &keyPool); err != nil {
					return err
				}
			}
			if err := p.expect(':'); err != nil {
				return err
			}
			stack[index].state = completeJSONObjectAfterValue
			state, container, err := startCompleteJSONValue(p, depth+len(stack), maxDepth)
			if err != nil {
				return err
			}
			if container {
				childScope := completeJSONChildPolicyScope(stack[index].scope, canonicalKey, matchedPolicyKey)
				stack = append(stack, completeJSONContainerFrame{state: state, scope: childScope})
			}

		case completeJSONArrayFirstValue, completeJSONArrayNextValue:
			first := stack[index].state == completeJSONArrayFirstValue
			p.skipWhitespace()
			if first && p.pos < len(p.body) && p.body[p.pos] == ']' {
				p.pos++
				stack[index] = completeJSONContainerFrame{}
				stack = stack[:index]
				continue
			}
			stack[index].state = completeJSONArrayAfterValue
			state, container, err := startCompleteJSONValue(p, depth+len(stack), maxDepth)
			if err != nil {
				return err
			}
			if container {
				stack = append(stack, completeJSONContainerFrame{state: state, scope: stack[index].scope})
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
				stack[index].state = completeJSONObjectNextKey
			case '}':
				p.pos++
				if err := finishCompleteJSONObjectFrame(p, &stack[index], &keyPool); err != nil {
					return err
				}
				stack[index] = completeJSONContainerFrame{}
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
				stack[index].state = completeJSONArrayNextValue
			case ']':
				p.pos++
				stack[index] = completeJSONContainerFrame{}
				stack = stack[:index]
			default:
				return p.invalid("expected array separator")
			}
		}
	}
	return nil
}

func recordCompleteJSONObjectKey(
	p *jsonParser,
	frame *completeJSONContainerFrame,
	key stringRef,
	pool *completeJSONKeyPool,
) error {
	if frame == nil {
		return ErrRoutingEnvelopeUnavailable
	}
	packed, err := newCompleteJSONKeyRef(key)
	if err != nil {
		return err
	}
	if frame.keyCount < completeJSONInlineObjectKeys {
		for index := 0; index < frame.keyCount; index++ {
			if completeJSONKeyRefsEqual(p.body, frame.inline[index], packed) {
				return p.invalid("duplicate JSON object key")
			}
		}
		frame.inline[frame.keyCount] = packed
		frame.keyCount++
		return nil
	}
	if frame.keys == nil {
		frame.keys, err = pool.acquire(completeJSONMinKeySlotCapacity)
		if err != nil {
			return err
		}
		frame.keys = append(frame.keys, frame.inline[:]...)
	}
	if len(frame.keys) == cap(frame.keys) {
		grown, growErr := pool.acquire(len(frame.keys) + 1)
		if growErr != nil {
			return growErr
		}
		grown = append(grown, frame.keys...)
		pool.release(frame.keys)
		frame.keys = grown
	}
	frame.keys = append(frame.keys, packed)
	frame.keyCount++
	return nil
}

func finishCompleteJSONObjectFrame(
	p *jsonParser,
	frame *completeJSONContainerFrame,
	pool *completeJSONKeyPool,
) error {
	if frame == nil || frame.keys == nil {
		return nil
	}
	keys := frame.keys
	frame.keys = nil
	defer pool.release(keys)
	slices.SortFunc(keys, func(left, right completeJSONKeyRef) int {
		switch {
		case left.hash < right.hash:
			return -1
		case left.hash > right.hash:
			return 1
		default:
			return 0
		}
	})
	for start := 0; start < len(keys); {
		end := start + 1
		for end < len(keys) && keys[end].hash == keys[start].hash {
			end++
		}
		for left := start; left < end; left++ {
			for right := left + 1; right < end; right++ {
				if completeJSONKeyRefsEqual(p.body, keys[left], keys[right]) {
					return p.invalid("duplicate JSON object key")
				}
			}
		}
		start = end
	}
	return nil
}

func newCompleteJSONKeyRef(key stringRef) (completeJSONKeyRef, error) {
	if key.start < 0 || key.end < key.start || uint64(key.end) >= uint64(completeJSONEscapedKeyEndFlag) {
		return completeJSONKeyRef{}, fmt.Errorf("%w: object key offset exceeds compact tracking capacity", ErrRoutingEnvelopeResourceLimit)
	}
	end := uint32(key.end)
	if key.escaped {
		end |= completeJSONEscapedKeyEndFlag
	}
	return completeJSONKeyRef{hash: key.hash, start: uint32(key.start), end: end}, nil
}

func (r completeJSONKeyRef) stringRef() stringRef {
	return stringRef{
		start:   int(r.start),
		end:     int(r.end &^ completeJSONEscapedKeyEndFlag),
		escaped: r.end&completeJSONEscapedKeyEndFlag != 0,
	}
}

func completeJSONKeyRefsEqual(body []byte, left, right completeJSONKeyRef) bool {
	if left.hash != right.hash {
		return false
	}
	return equalStringRefs(body, left.stringRef(), right.stringRef())
}

func (p *completeJSONKeyPool) acquire(minimum int) ([]completeJSONKeyRef, error) {
	if p == nil {
		return nil, ErrRoutingEnvelopeUnavailable
	}
	capacity := completeJSONMinKeySlotCapacity
	bucket := 3
	for capacity < minimum {
		capacity <<= 1
		bucket++
		if capacity <= 0 || bucket >= len(p.free) {
			return nil, fmt.Errorf("%w: object key tracking capacity overflow", ErrRoutingEnvelopeResourceLimit)
		}
	}
	available := p.free[bucket]
	if len(available) > 0 {
		storage := available[len(available)-1]
		p.free[bucket] = available[:len(available)-1]
		return storage[:0], nil
	}
	if p.maxSlots <= 0 || capacity > p.maxSlots-p.allocatedSlots {
		return nil, fmt.Errorf("%w: object key tracking exceeds %d slots", ErrRoutingEnvelopeResourceLimit, p.maxSlots)
	}
	p.allocatedSlots += capacity
	return make([]completeJSONKeyRef, 0, capacity), nil
}

func (p *completeJSONKeyPool) release(keys []completeJSONKeyRef) {
	if p == nil || cap(keys) < completeJSONMinKeySlotCapacity {
		return
	}
	capacity := cap(keys)
	bucket := 0
	for size := 1; size < capacity; size <<= 1 {
		bucket++
	}
	if bucket >= len(p.free) {
		return
	}
	p.free[bucket] = append(p.free[bucket], keys[:0])
}

func completeJSONCanonicalPolicyKey(
	body []byte,
	key stringRef,
	scope completeJSONPolicyScope,
) (name string, matched bool, canonical bool, err error) {
	candidates := completeJSONPolicyKeys(scope)
	if len(candidates) == 0 {
		return "", false, false, nil
	}
	if key.start < 0 || key.end < key.start || key.end > len(body) {
		return "", false, false, ErrBodyMismatch
	}
	if !key.escaped {
		raw := body[key.start:key.end]
		for _, candidate := range candidates {
			if bytes.Equal(raw, []byte(candidate)) {
				return candidate, true, true, nil
			}
		}
		for _, candidate := range candidates {
			if bytes.EqualFold(raw, []byte(candidate)) {
				return candidate, true, false, nil
			}
		}
		return "", false, false, nil
	}
	decoded, err := key.decode(body)
	if err != nil {
		return "", false, false, err
	}
	for _, candidate := range candidates {
		if decoded == candidate {
			return candidate, true, true, nil
		}
	}
	for _, candidate := range candidates {
		if strings.EqualFold(decoded, candidate) {
			return candidate, true, false, nil
		}
	}
	return "", false, false, nil
}

func completeJSONPolicyKeys(scope completeJSONPolicyScope) []string {
	switch scope {
	case completeJSONPolicyScopeRoot:
		return completeJSONRootPolicyKeys
	case completeJSONPolicyScopeReasoning:
		return completeJSONReasoningPolicyKeys
	case completeJSONPolicyScopeText:
		return completeJSONTextPolicyKeys
	case completeJSONPolicyScopeInputItem:
		return completeJSONInputItemPolicyKeys
	case completeJSONPolicyScopeContentPart:
		return completeJSONContentPartPolicyKeys
	case completeJSONPolicyScopeTool:
		return completeJSONToolPolicyKeys
	case completeJSONPolicyScopeToolChoice:
		return completeJSONToolChoicePolicyKeys
	default:
		return nil
	}
}

func completeJSONChildPolicyScope(
	parent completeJSONPolicyScope,
	key string,
	matched bool,
) completeJSONPolicyScope {
	if !matched {
		return completeJSONPolicyScopeNone
	}
	switch parent {
	case completeJSONPolicyScopeRoot:
		switch key {
		case "reasoning":
			return completeJSONPolicyScopeReasoning
		case "text":
			return completeJSONPolicyScopeText
		case "input":
			return completeJSONPolicyScopeInputItem
		case "tools":
			return completeJSONPolicyScopeTool
		case "tool_choice":
			return completeJSONPolicyScopeToolChoice
		}
	case completeJSONPolicyScopeInputItem:
		switch key {
		case "content":
			return completeJSONPolicyScopeContentPart
		case "tools":
			return completeJSONPolicyScopeTool
		}
	case completeJSONPolicyScopeTool:
		switch key {
		case "tools", "children", "function":
			return completeJSONPolicyScopeTool
		}
	case completeJSONPolicyScopeToolChoice:
		if key == "function" {
			return completeJSONPolicyScopeToolChoice
		}
	}
	return completeJSONPolicyScopeNone
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

func completeRoutingEnvelopeComplete(protocol Protocol, envelope RoutingEnvelope, seen routingEnvelopeField) bool {
	if seen&routingEnvelopeModel == 0 || envelope.Model == "" {
		return false
	}
	if protocol != ProtocolResponsesWebSocket {
		// HTTP Chat, Messages, and Responses default an omitted stream field to
		// false. The bounded extractor remains stricter because it cannot prove
		// that a late stream field is absent rather than merely out of view.
		return true
	}
	return routingEnvelopeComplete(protocol, envelope, seen)
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
