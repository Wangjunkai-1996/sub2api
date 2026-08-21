package securityadmission

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

var errClassificationComplete = errors.New("security admission classification complete")

type ParseError struct {
	Offset int
	Reason string
}

func (e *ParseError) Error() string {
	if e == nil {
		return "invalid JSON"
	}
	return fmt.Sprintf("invalid JSON at byte %d: %s", e.Offset, e.Reason)
}

type stringRef struct {
	start       int
	end         int
	escaped     bool
	runes       int
	hasNonSpace bool
	hash        uint64
}

func (r stringRef) decode(body []byte) (string, error) {
	if r.start < 0 || r.end < r.start || r.end > len(body) {
		return "", ErrBodyMismatch
	}
	if !r.escaped {
		return string(body[r.start:r.end]), nil
	}
	return decodeJSONString(body, r.start, r.end)
}

// decodeJSONString uses the JSON decoder for the uncommon escaped-string
// path. strconv.Unquote implements Go string syntax and does not accept every
// legal JSON escape (notably \/), so it cannot be used for canonical keys or
// scanner text.
func decodeJSONString(body []byte, start, end int) (string, error) {
	if start <= 0 || end < start || end >= len(body) || body[start-1] != '"' || body[end] != '"' {
		return "", ErrBodyMismatch
	}
	var value string
	if err := json.Unmarshal(body[start-1:end+1], &value); err != nil {
		return "", ErrBodyMismatch
	}
	return value, nil
}

func equalStringRefs(body []byte, left, right stringRef) bool {
	if !left.escaped && !right.escaped {
		return bytes.Equal(body[left.start:left.end], body[right.start:right.end])
	}
	lv, leftErr := left.decode(body)
	rv, rightErr := right.decode(body)
	return leftErr == nil && rightErr == nil && lv == rv
}

type jsonParser struct {
	body           []byte
	pos            int
	depth          int
	tokens         int
	limits         Limits
	lineage        LineageTrust
	resolveLineage func(previousResponseID string) LineageTrust
	reason         ReasonCode
	noTextReason   ReasonCode
	segments       []textSegment
	textRunes      int
	segmentTotal   int
	groups         []parsedGroup
	segmentSource  segmentSource
	// previousResponseID is a remote lineage reference. It is not itself
	// prompt text and is never admissible without independently trusted
	// lineage. The root parser defers that decision until all fields have been
	// visited so JSON member order cannot change the classification reason.
	previousResponseID  bool
	promptContainerSeen bool
}

type parsedGroup struct {
	start         int
	end           int
	role          string
	toolish       bool
	assistantTool bool
}

type segmentSource uint8

const (
	segmentSourceOther segmentSource = iota
	segmentSourceInstruction
	segmentSourceInput
)

func (p *jsonParser) withSegmentSource(source segmentSource, parse func() error) error {
	previous := p.segmentSource
	p.segmentSource = source
	err := parse()
	p.segmentSource = previous
	return err
}

func (p *jsonParser) invalid(reason string) error {
	return &ParseError{Offset: p.pos, Reason: reason}
}

func (p *jsonParser) uninspectable(reason ReasonCode) error {
	if p.reason == "" {
		p.reason = reason
	}
	return errClassificationComplete
}

func (p *jsonParser) bumpToken() error {
	p.tokens++
	if p.tokens > p.limits.MaxTokens {
		return p.uninspectable(ReasonParserLimit)
	}
	return nil
}

func (p *jsonParser) enterContainer() error {
	p.depth++
	if p.depth > p.limits.MaxDepth {
		return p.uninspectable(ReasonParserLimit)
	}
	return nil
}

func (p *jsonParser) leaveContainer() {
	p.depth--
}

func (p *jsonParser) skipWhitespace() {
	for p.pos < len(p.body) {
		switch p.body[p.pos] {
		case ' ', '\t', '\r', '\n':
			p.pos++
		default:
			return
		}
	}
}

func (p *jsonParser) peek() byte {
	p.skipWhitespace()
	if p.pos >= len(p.body) {
		return 0
	}
	return p.body[p.pos]
}

func (p *jsonParser) unknownValue(reason ReasonCode) error {
	next := p.peek()
	if p.pos >= len(p.body) {
		return p.invalid("missing value")
	}
	// A syntactically valid JSON value with an unsupported shape is a
	// semantic admission failure. Anything else cannot be JSON at this
	// position and should retain the parser error for callers that need to
	// distinguish malformed input from an intentionally unsupported shape.
	switch next {
	case '{', '[', '"', 't', 'f', 'n', '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return p.uninspectable(reason)
	default:
		return p.invalid("expected value")
	}
}

func (p *jsonParser) expect(value byte) error {
	p.skipWhitespace()
	if p.pos >= len(p.body) || p.body[p.pos] != value {
		return p.invalid(fmt.Sprintf("expected %q", value))
	}
	p.pos++
	return nil
}

func (p *jsonParser) parseObject(visitor func(key stringRef) error) error {
	if err := p.bumpToken(); err != nil {
		return err
	}
	if err := p.expect('{'); err != nil {
		return err
	}
	if err := p.enterContainer(); err != nil {
		return err
	}
	defer p.leaveContainer()

	p.skipWhitespace()
	if p.pos < len(p.body) && p.body[p.pos] == '}' {
		p.pos++
		return nil
	}

	memberCount := 0
	var seen map[uint64]stringRef
	for {
		memberCount++
		if memberCount > p.limits.MaxObjectMembers {
			return p.uninspectable(ReasonParserLimit)
		}
		if err := p.bumpToken(); err != nil {
			return err
		}
		key, err := p.scanString(true)
		if err != nil {
			return err
		}
		if key.end-key.start > p.limits.MaxKeyBytes {
			return p.uninspectable(ReasonParserLimit)
		}
		if seen == nil {
			seen = make(map[uint64]stringRef, min(16, p.limits.MaxObjectMembers))
		}
		if previous, exists := seen[key.hash]; exists {
			if equalStringRefs(p.body, previous, key) {
				return p.uninspectable(ReasonDuplicateJSONKey)
			}
			return p.uninspectable(ReasonJSONKeyCollision)
		}
		seen[key.hash] = key

		if err := p.expect(':'); err != nil {
			return err
		}
		if visitor == nil {
			if err := p.skipValue(); err != nil {
				return err
			}
		} else if err := visitor(key); err != nil {
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
			return nil
		default:
			return p.invalid("expected object separator")
		}
	}
}

func (p *jsonParser) parseArray(visitor func(index int) error) error {
	if err := p.bumpToken(); err != nil {
		return err
	}
	if err := p.expect('['); err != nil {
		return err
	}
	if err := p.enterContainer(); err != nil {
		return err
	}
	defer p.leaveContainer()

	p.skipWhitespace()
	if p.pos < len(p.body) && p.body[p.pos] == ']' {
		p.pos++
		return nil
	}

	for index := 0; ; index++ {
		if visitor == nil {
			if err := p.skipValue(); err != nil {
				return err
			}
		} else if err := visitor(index); err != nil {
			return err
		}
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
		case ']':
			p.pos++
			return nil
		default:
			return p.invalid("expected array separator")
		}
	}
}

func (p *jsonParser) skipValue() error {
	switch p.peek() {
	case '{':
		return p.parseObject(nil)
	case '[':
		return p.parseArray(nil)
	case '"':
		if err := p.bumpToken(); err != nil {
			return err
		}
		return p.skipString()
	case 't':
		return p.parseLiteral("true")
	case 'f':
		return p.parseLiteral("false")
	case 'n':
		return p.parseLiteral("null")
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return p.parseNumber()
	default:
		return p.invalid("expected value")
	}
}

func (p *jsonParser) skipString() error {
	p.skipWhitespace()
	if p.pos >= len(p.body) || p.body[p.pos] != '"' {
		return p.invalid("expected string")
	}
	p.pos++
	for p.pos < len(p.body) {
		for p.pos+8 <= len(p.body) {
			word := binary.LittleEndian.Uint64(p.body[p.pos : p.pos+8])
			if !safeJSONStringWord(word) {
				break
			}
			p.pos += 8
		}
		if p.pos >= len(p.body) {
			break
		}
		value := p.body[p.pos]
		switch {
		case value == '"':
			p.pos++
			return nil
		case value < 0x20:
			return p.invalid("control byte in string")
		case value == '\\':
			p.pos++
			if p.pos >= len(p.body) {
				return p.invalid("unterminated escape")
			}
			escaped := p.body[p.pos]
			p.pos++
			switch escaped {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			case 'u':
				first, err := p.readHexRune()
				if err != nil {
					return err
				}
				if first >= 0xD800 && first <= 0xDBFF {
					if p.pos+2 > len(p.body) || p.body[p.pos] != '\\' || p.body[p.pos+1] != 'u' {
						return p.invalid("unpaired high surrogate")
					}
					p.pos += 2
					second, err := p.readHexRune()
					if err != nil {
						return err
					}
					if second < 0xDC00 || second > 0xDFFF {
						return p.invalid("invalid surrogate pair")
					}
				} else if first >= 0xDC00 && first <= 0xDFFF {
					return p.invalid("unpaired low surrogate")
				}
			default:
				return p.invalid("invalid string escape")
			}
		case value < utf8.RuneSelf:
			p.pos++
		default:
			decoded, size := utf8.DecodeRune(p.body[p.pos:])
			if decoded == utf8.RuneError && size == 1 {
				return p.invalid("invalid UTF-8 in string")
			}
			p.pos += size
		}
	}
	return p.invalid("unterminated string")
}

const (
	jsonByteOnes  = uint64(0x0101010101010101)
	jsonByteHigh  = uint64(0x8080808080808080)
	jsonByteSpace = uint64(0x2020202020202020)
)

func safeJSONStringWord(word uint64) bool {
	if word&jsonByteHigh != 0 || hasJSONByte(word, '"') || hasJSONByte(word, '\\') {
		return false
	}
	// has-less-than is allowed to report a false positive across byte borrows;
	// that only falls back to the scalar validator. It never skips a control byte.
	return ((word - jsonByteSpace) &^ word & jsonByteHigh) == 0
}

func hasJSONByte(word uint64, value byte) bool {
	x := word ^ (jsonByteOnes * uint64(value))
	return ((x - jsonByteOnes) &^ x & jsonByteHigh) != 0
}

func (p *jsonParser) parseLiteral(value string) error {
	if err := p.bumpToken(); err != nil {
		return err
	}
	p.skipWhitespace()
	if p.pos+len(value) > len(p.body) || string(p.body[p.pos:p.pos+len(value)]) != value {
		return p.invalid("invalid literal")
	}
	p.pos += len(value)
	return nil
}

func (p *jsonParser) parseNumber() error {
	if err := p.bumpToken(); err != nil {
		return err
	}
	p.skipWhitespace()
	start := p.pos
	if p.pos < len(p.body) && p.body[p.pos] == '-' {
		p.pos++
	}
	if p.pos >= len(p.body) {
		return p.invalid("invalid number")
	}
	if p.body[p.pos] == '0' {
		p.pos++
		if p.pos < len(p.body) && p.body[p.pos] >= '0' && p.body[p.pos] <= '9' {
			return p.invalid("leading zero in number")
		}
	} else if p.body[p.pos] >= '1' && p.body[p.pos] <= '9' {
		for p.pos < len(p.body) && p.body[p.pos] >= '0' && p.body[p.pos] <= '9' {
			p.pos++
		}
	} else {
		return p.invalid("invalid number")
	}
	if p.pos < len(p.body) && p.body[p.pos] == '.' {
		p.pos++
		fractionStart := p.pos
		for p.pos < len(p.body) && p.body[p.pos] >= '0' && p.body[p.pos] <= '9' {
			p.pos++
		}
		if p.pos == fractionStart {
			return p.invalid("invalid number fraction")
		}
	}
	if p.pos < len(p.body) && (p.body[p.pos] == 'e' || p.body[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.body) && (p.body[p.pos] == '+' || p.body[p.pos] == '-') {
			p.pos++
		}
		exponentStart := p.pos
		for p.pos < len(p.body) && p.body[p.pos] >= '0' && p.body[p.pos] <= '9' {
			p.pos++
		}
		if p.pos == exponentStart {
			return p.invalid("invalid number exponent")
		}
	}
	if p.pos == start {
		return p.invalid("invalid number")
	}
	return nil
}

func (p *jsonParser) scanString(hashDecoded bool) (stringRef, error) {
	p.skipWhitespace()
	if p.pos >= len(p.body) || p.body[p.pos] != '"' {
		return stringRef{}, p.invalid("expected string")
	}
	p.pos++
	ref := stringRef{start: p.pos}
	hash := uint64(14695981039346656037)
	for p.pos < len(p.body) {
		value := p.body[p.pos]
		if value == '"' {
			ref.end = p.pos
			ref.hash = hash
			p.pos++
			return ref, nil
		}
		if value < 0x20 {
			return stringRef{}, p.invalid("control byte in string")
		}
		if value == '\\' {
			ref.escaped = true
			p.pos++
			if p.pos >= len(p.body) {
				return stringRef{}, p.invalid("unterminated escape")
			}
			escaped := p.body[p.pos]
			p.pos++
			var decoded rune
			switch escaped {
			case '"', '\\', '/':
				decoded = rune(escaped)
			case 'b':
				decoded = '\b'
			case 'f':
				decoded = '\f'
			case 'n':
				decoded = '\n'
			case 'r':
				decoded = '\r'
			case 't':
				decoded = '\t'
			case 'u':
				first, err := p.readHexRune()
				if err != nil {
					return stringRef{}, err
				}
				decoded = first
				if first >= 0xD800 && first <= 0xDBFF {
					if p.pos+2 > len(p.body) || p.body[p.pos] != '\\' || p.body[p.pos+1] != 'u' {
						return stringRef{}, p.invalid("unpaired high surrogate")
					}
					p.pos += 2
					second, err := p.readHexRune()
					if err != nil {
						return stringRef{}, err
					}
					if second < 0xDC00 || second > 0xDFFF {
						return stringRef{}, p.invalid("invalid surrogate pair")
					}
					decoded = utf16.DecodeRune(first, second)
				} else if first >= 0xDC00 && first <= 0xDFFF {
					return stringRef{}, p.invalid("unpaired low surrogate")
				}
			default:
				return stringRef{}, p.invalid("invalid string escape")
			}
			ref.runes++
			if !unicode.IsSpace(decoded) {
				ref.hasNonSpace = true
			}
			if hashDecoded {
				hash = hashRune(hash, decoded)
			}
			continue
		}
		if value < utf8.RuneSelf {
			ref.runes++
			if !unicode.IsSpace(rune(value)) {
				ref.hasNonSpace = true
			}
			if hashDecoded {
				hash = hashByte(hash, value)
			}
			p.pos++
			continue
		}
		decoded, size := utf8.DecodeRune(p.body[p.pos:])
		if decoded == utf8.RuneError && size == 1 {
			return stringRef{}, p.invalid("invalid UTF-8 in string")
		}
		ref.runes++
		if !unicode.IsSpace(decoded) {
			ref.hasNonSpace = true
		}
		if hashDecoded {
			for _, b := range p.body[p.pos : p.pos+size] {
				hash = hashByte(hash, b)
			}
		}
		p.pos += size
	}
	return stringRef{}, p.invalid("unterminated string")
}

func (p *jsonParser) readHexRune() (rune, error) {
	if p.pos+4 > len(p.body) {
		return 0, p.invalid("short unicode escape")
	}
	value := rune(0)
	for index := 0; index < 4; index++ {
		value <<= 4
		switch b := p.body[p.pos+index]; {
		case b >= '0' && b <= '9':
			value += rune(b - '0')
		case b >= 'a' && b <= 'f':
			value += rune(b-'a') + 10
		case b >= 'A' && b <= 'F':
			value += rune(b-'A') + 10
		default:
			return 0, p.invalid("invalid unicode escape")
		}
	}
	p.pos += 4
	return value, nil
}

func hashByte(hash uint64, value byte) uint64 {
	hash ^= uint64(value)
	return hash * 1099511628211
}

func hashRune(hash uint64, value rune) uint64 {
	var encoded [utf8.UTFMax]byte
	size := utf8.EncodeRune(encoded[:], value)
	for _, b := range encoded[:size] {
		hash = hashByte(hash, b)
	}
	return hash
}

func (p *jsonParser) parseString() (stringRef, error) {
	if err := p.bumpToken(); err != nil {
		return stringRef{}, err
	}
	return p.scanString(false)
}

func (p *jsonParser) parseStringValue() (string, error) {
	ref, err := p.parseString()
	if err != nil {
		return "", err
	}
	return ref.decode(p.body)
}

// parseStringShape keeps a valid JSON value with an unsupported type in the
// semantic fail-closed path. parseStringValue is intentionally strict for the
// actual string decoder, while protocol fields need to distinguish a number,
// object, or null from malformed JSON at the same position.
func (p *jsonParser) parseStringShape(reason ReasonCode) (string, error) {
	if p.peek() != '"' {
		return "", p.unknownValue(reason)
	}
	return p.parseStringValue()
}

func (p *jsonParser) addText(ref stringRef, kind TextKind, role string) error {
	if !ref.hasNonSpace {
		return nil
	}
	if p.segmentTotal >= p.limits.MaxSegments {
		return p.uninspectable(ReasonParserLimit)
	}
	if p.textRunes+ref.runes > p.limits.MaxTextRunes {
		return p.uninspectable(ReasonTextLimit)
	}
	p.textRunes += ref.runes
	p.segmentTotal++
	p.segments = append(p.segments, textSegment{
		start: ref.start, end: ref.end, escaped: ref.escaped, runes: ref.runes, kind: kind, role: role,
		source: p.segmentSource,
	})
	return nil
}

func (p *jsonParser) recordGroup(start int, role string, toolish, assistantTool bool) {
	if start < 0 || start > len(p.segments) {
		return
	}
	p.groups = append(p.groups, parsedGroup{
		start: start, end: len(p.segments), role: strings.ToLower(strings.TrimSpace(role)),
		toolish: toolish, assistantTool: assistantTool,
	})
}

func (p *jsonParser) applyRole(start int, role string) {
	role = strings.ToLower(strings.TrimSpace(role))
	if role == "" {
		return
	}
	for index := start; index < len(p.segments); index++ {
		if strings.TrimSpace(p.segments[index].role) == "" {
			p.segments[index].role = role
		}
	}
}

func (p *jsonParser) parseAndAddString(kind TextKind, role string) error {
	ref, err := p.parseString()
	if err != nil {
		return err
	}
	return p.addText(ref, kind, role)
}

func (p *jsonParser) isNull() bool {
	p.skipWhitespace()
	return p.pos+4 <= len(p.body) && string(p.body[p.pos:p.pos+4]) == "null"
}

func (p *jsonParser) finish() error {
	p.skipWhitespace()
	if p.pos != len(p.body) {
		return p.invalid("trailing data")
	}
	return nil
}
