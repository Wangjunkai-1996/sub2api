package securityadmission

import (
	"errors"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const ParserVersion = "canonical-v1"

const (
	DefaultBodyCapBytes     = 1 << 20
	DefaultMaxTextRunes     = 12_000
	DefaultMaxDepth         = 64
	DefaultMaxSegments      = 1_024
	DefaultMaxObjectMembers = 4_096
	DefaultMaxTokens        = 131_072
	DefaultMaxKeyBytes      = 1_024
)

type Protocol string

const (
	ProtocolOpenAIResponses    Protocol = "openai_responses"
	ProtocolOpenAIChat         Protocol = "openai_chat_completions"
	ProtocolAnthropicMessages  Protocol = "anthropic_messages"
	ProtocolResponsesWebSocket Protocol = "responses_websocket"
)

type RequestClass string

const (
	RequestKnownViolation RequestClass = "known_violation"
	RequestAuditableText  RequestClass = "auditable_text"
	RequestKnownNoText    RequestClass = "known_no_text"
	RequestUninspectable  RequestClass = "uninspectable"
)

type AccountClass string

const (
	AccountAuditRequired       AccountClass = "audit_required"
	AccountAuditExemptVerified AccountClass = "audit_exempt_verified"
	AccountUnknown             AccountClass = "account_unknown"
)

type AccountRequirement string

const (
	AccountRequirementAny         AccountRequirement = "any_account"
	AccountRequirementAuditExempt AccountRequirement = "require_audit_exempt_account"
)

type LineageTrust string

const (
	LineageUntrusted LineageTrust = "untrusted"
	LineageTrusted   LineageTrust = "trusted"
)

type ReasonCode string

const (
	ReasonAuditableText       ReasonCode = "auditable_text"
	ReasonKnownNoText         ReasonCode = "known_no_text"
	ReasonKnownControlFrame   ReasonCode = "known_control_frame"
	ReasonKnownViolation      ReasonCode = "known_violation"
	ReasonLargeBody           ReasonCode = "uninspectable_large_body"
	ReasonTextLimit           ReasonCode = "uninspectable_text_limit"
	ReasonParserLimit         ReasonCode = "uninspectable_parser_limit"
	ReasonDuplicateJSONKey    ReasonCode = "uninspectable_duplicate_json_key"
	ReasonJSONKeyCollision    ReasonCode = "uninspectable_json_key_collision"
	ReasonUnknownField        ReasonCode = "uninspectable_unknown_field"
	ReasonUnknownType         ReasonCode = "uninspectable_unknown_type"
	ReasonUnknownRole         ReasonCode = "uninspectable_unknown_role"
	ReasonUnknownContentShape ReasonCode = "uninspectable_unknown_content_shape"
	ReasonRemoteContent       ReasonCode = "uninspectable_remote_content"
	ReasonEncryptedContent    ReasonCode = "uninspectable_encrypted_content"
	ReasonMediaContent        ReasonCode = "uninspectable_media_content"
	ReasonUntrustedLineage    ReasonCode = "uninspectable_untrusted_lineage"
	ReasonInvalidJSON         ReasonCode = "invalid_json"
	ReasonUnsupportedProtocol ReasonCode = "uninspectable_unsupported_protocol"
)

type TextKind string

const (
	TextInstruction TextKind = "instruction"
	TextMessage     TextKind = "message"
	TextToolInput   TextKind = "tool_input"
	TextToolOutput  TextKind = "tool_output"
)

type Limits struct {
	BodyCapBytes     int
	MaxTextRunes     int
	MaxDepth         int
	MaxSegments      int
	MaxObjectMembers int
	MaxTokens        int
	MaxKeyBytes      int
}

func DefaultLimits() Limits {
	return Limits{
		BodyCapBytes:     DefaultBodyCapBytes,
		MaxTextRunes:     DefaultMaxTextRunes,
		MaxDepth:         DefaultMaxDepth,
		MaxSegments:      DefaultMaxSegments,
		MaxObjectMembers: DefaultMaxObjectMembers,
		MaxTokens:        DefaultMaxTokens,
		MaxKeyBytes:      DefaultMaxKeyBytes,
	}
}

func normalizeLimits(value Limits) Limits {
	defaults := DefaultLimits()
	if value.BodyCapBytes <= 0 {
		value.BodyCapBytes = defaults.BodyCapBytes
	}
	if value.MaxTextRunes <= 0 {
		value.MaxTextRunes = defaults.MaxTextRunes
	}
	if value.MaxDepth <= 0 {
		value.MaxDepth = defaults.MaxDepth
	}
	if value.MaxSegments <= 0 {
		value.MaxSegments = defaults.MaxSegments
	}
	if value.MaxObjectMembers <= 0 {
		value.MaxObjectMembers = defaults.MaxObjectMembers
	}
	if value.MaxTokens <= 0 {
		value.MaxTokens = defaults.MaxTokens
	}
	if value.MaxKeyBytes <= 0 {
		value.MaxKeyBytes = defaults.MaxKeyBytes
	}
	return value
}

var activeLimits atomic.Pointer[Limits]

func init() {
	limits := DefaultLimits()
	activeLimits.Store(&limits)
}

func CurrentLimits() Limits {
	value := activeLimits.Load()
	if value == nil {
		return DefaultLimits()
	}
	return *value
}

func ConfigureLimits(value Limits) {
	value = normalizeLimits(value)
	activeLimits.Store(&value)
}

type Options struct {
	Limits  Limits
	Lineage LineageTrust
	// ResolveLineage validates a parsed previous_response_id against a
	// request-local proof without requiring a second pass over the body.
	// The canonical parser remains responsible for duplicate keys and shape
	// validation before the resulting admission can be used.
	ResolveLineage func(previousResponseID string) LineageTrust
}

// TextScope selects which already-classified spans are materialized for a
// scanner. FullTranscript is the fail-safe replay scope; CurrentTurn is only
// populated as a narrower view when the caller has established trusted
// lineage. Neither scope causes a JSON tree or a copy of the request body to
// be retained by Admission.
type TextScope uint8

const (
	TextScopeFullTranscript TextScope = iota
	TextScopeCurrentTurn
)

type MaterializedDocument struct {
	Text         string
	SegmentCount int
	TextRunes    int
}

type textSegment struct {
	start   int
	end     int
	escaped bool
	runes   int
	kind    TextKind
	role    string
	source  segmentSource
	current bool
}

// Admission is immutable after Classify returns. It stores offsets into the
// caller-owned request body, never the body itself.
type Admission struct {
	protocol    Protocol
	class       RequestClass
	reason      ReasonCode
	lineage     LineageTrust
	bodyBytes   int
	textRunes   int
	segments    []textSegment
	requirement AccountRequirement
	parseNanos  int64
}

func (a Admission) Protocol() Protocol              { return a.protocol }
func (a Admission) Class() RequestClass             { return a.class }
func (a Admission) Reason() ReasonCode              { return a.reason }
func (a Admission) Lineage() LineageTrust           { return a.lineage }
func (a Admission) BodyBytes() int                  { return a.bodyBytes }
func (a Admission) TextRunes() int                  { return a.textRunes }
func (a Admission) SegmentCount() int               { return len(a.segments) }
func (a Admission) Requirement() AccountRequirement { return a.requirement }
func (a Admission) ParseDuration() time.Duration    { return time.Duration(a.parseNanos) }
func (a Admission) ParserVersion() string           { return ParserVersion }

func (a Admission) RequiresAuditExemptAccount() bool {
	return a.requirement == AccountRequirementAuditExempt
}

// WithKnownViolation returns a derived policy outcome after an authoritative
// synchronous policy engine has blocked the request. Classify deliberately
// never calls this method: a bounded structural parse cannot prove a content
// policy violation. Callers must keep the original Admission immutable for
// routing, auditing, and failover reuse.
func (a Admission) WithKnownViolation(reason ReasonCode) Admission {
	if reason == "" {
		reason = ReasonKnownViolation
	}
	a.class = RequestKnownViolation
	a.reason = reason
	a.requirement = AccountRequirementAny
	return a
}

var ErrBodyMismatch = errors.New("security admission body does not match classified spans")

// MaterializeText decodes only the selected JSON string spans. It is intended
// to run after the final effective account is known to require blocking audit.
func (a Admission) MaterializeText(body []byte) (string, error) {
	document, err := a.MaterializeDocument(body, TextScopeFullTranscript)
	if err != nil {
		return "", err
	}
	return document.Text, nil
}

// MaterializeDocument decodes only the selected JSON string spans. It is
// intended to run after the final effective account is known to require
// blocking audit. The body remains caller-owned and is never stored in the
// admission object.
func (a Admission) MaterializeDocument(body []byte, scope TextScope) (MaterializedDocument, error) {
	if a.class != RequestAuditableText || len(a.segments) == 0 {
		return MaterializedDocument{}, nil
	}
	if len(body) != a.bodyBytes {
		return MaterializedDocument{}, ErrBodyMismatch
	}
	if scope != TextScopeCurrentTurn {
		scope = TextScopeFullTranscript
	}
	// A narrowed document is only sound when the caller proved the request's
	// previous-response lineage. Untrusted bodies must retain the complete
	// classified transcript even if a caller asks for the latest-turn view.
	if scope == TextScopeCurrentTurn && a.lineage != LineageTrusted {
		scope = TextScopeFullTranscript
	}
	var out strings.Builder
	selectedRunes := a.textRunes
	selectedSegments := len(a.segments)
	if scope == TextScopeCurrentTurn {
		selectedRunes = 0
		selectedSegments = 0
		for _, segment := range a.segments {
			if segment.current {
				selectedRunes += segment.runes
				selectedSegments++
			}
		}
	}
	if selectedRunes > 0 {
		out.Grow(min(len(body), selectedRunes*2))
	}
	written := 0
	for _, segment := range a.segments {
		if scope == TextScopeCurrentTurn && !segment.current {
			continue
		}
		if segment.start < 0 || segment.end < segment.start || segment.end > len(body) {
			return MaterializedDocument{}, ErrBodyMismatch
		}
		if written > 0 {
			out.WriteString("\n\n")
		}
		if !segment.escaped {
			out.Write(body[segment.start:segment.end])
		} else {
			decoded, decodeErr := decodeJSONString(body, segment.start, segment.end)
			if decodeErr != nil {
				return MaterializedDocument{}, ErrBodyMismatch
			}
			out.WriteString(decoded)
		}
		written++
	}
	if written != selectedSegments {
		return MaterializedDocument{}, ErrBodyMismatch
	}
	return MaterializedDocument{Text: out.String(), SegmentCount: written, TextRunes: selectedRunes}, nil
}

func (a Admission) TextKinds() []TextKind {
	result := make([]TextKind, len(a.segments))
	for index, segment := range a.segments {
		result[index] = segment.kind
	}
	return result
}

func (a Admission) TextRoles() []string {
	result := make([]string, len(a.segments))
	for index, segment := range a.segments {
		result[index] = segment.role
	}
	return result
}

func countRunes(value []byte) int {
	return utf8.RuneCount(value)
}
