package securityadmission

import (
	"errors"
	"strings"
	"time"
)

func NormalizeProtocol(value string) Protocol {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "openai_responses", "responses":
		return ProtocolOpenAIResponses
	case "openai_chat_completions", "openai_chat", "chat_completions":
		return ProtocolOpenAIChat
	case "anthropic_messages", "claude_messages", "messages":
		return ProtocolAnthropicMessages
	case "responses_websocket", "openai_responses_websocket":
		return ProtocolResponsesWebSocket
	default:
		return Protocol(strings.ToLower(strings.TrimSpace(value)))
	}
}

func Classify(protocol string, body []byte, options Options) (Admission, error) {
	started := time.Now()
	limits := normalizeLimits(options.Limits)
	lineage := options.Lineage
	if lineage != LineageTrusted {
		lineage = LineageUntrusted
	}
	normalizedProtocol := NormalizeProtocol(protocol)
	base := Admission{
		protocol:    normalizedProtocol,
		lineage:     lineage,
		bodyBytes:   len(body),
		requirement: AccountRequirementAny,
	}
	if len(body) > limits.BodyCapBytes {
		base.class = RequestUninspectable
		base.reason = ReasonLargeBody
		base.requirement = AccountRequirementAuditExempt
		base.parseNanos = time.Since(started).Nanoseconds()
		observeClassification(base)
		return base, nil
	}

	parser := jsonParser{body: body, limits: limits, lineage: lineage, resolveLineage: options.ResolveLineage}
	var err error
	switch normalizedProtocol {
	case ProtocolOpenAIResponses:
		err = parser.parseResponsesRoot(false)
	case ProtocolOpenAIChat:
		err = parser.parseChatRoot()
	case ProtocolAnthropicMessages:
		err = parser.parseAnthropicRoot()
	case ProtocolResponsesWebSocket:
		err = parser.parseResponsesRoot(true)
	default:
		base.class = RequestUninspectable
		base.reason = ReasonUnsupportedProtocol
		base.requirement = AccountRequirementAuditExempt
		base.parseNanos = time.Since(started).Nanoseconds()
		observeClassification(base)
		return base, nil
	}

	if errors.Is(err, errClassificationComplete) {
		base.lineage = parser.lineage
		base.class = RequestUninspectable
		base.reason = parser.reason
		if base.reason == "" {
			base.reason = ReasonUnknownContentShape
		}
		base.requirement = AccountRequirementAuditExempt
		base.parseNanos = time.Since(started).Nanoseconds()
		observeClassification(base)
		return base, nil
	}
	if err == nil {
		err = parser.finish()
	}
	if err != nil {
		base.lineage = parser.lineage
		base.class = RequestUninspectable
		base.reason = ReasonInvalidJSON
		base.requirement = AccountRequirementAuditExempt
		base.parseNanos = time.Since(started).Nanoseconds()
		observeClassification(base)
		return base, err
	}

	base.textRunes = parser.textRunes
	base.lineage = parser.lineage
	base.segments = parser.segments
	markCurrentSegments(&parser, normalizedProtocol)
	if normalizedProtocol == ProtocolOpenAIResponses || normalizedProtocol == ProtocolResponsesWebSocket {
		reorderResponsesSegments(&parser)
	}
	base.segments = parser.segments
	if len(parser.segments) == 0 {
		base.class = RequestKnownNoText
		base.reason = parser.noTextReason
		if base.reason == "" {
			base.reason = ReasonKnownNoText
		}
	} else {
		base.class = RequestAuditableText
		base.reason = ReasonAuditableText
	}
	base.parseNanos = time.Since(started).Nanoseconds()
	observeClassification(base)
	return base, nil
}

func reorderResponsesSegments(parser *jsonParser) {
	if parser == nil || len(parser.segments) < 2 {
		return
	}
	// JSON object member order is not semantic. Responses instructions are
	// always the leading scanner document section, followed by input items,
	// regardless of whether the client serialized input first.
	ordered := make([]textSegment, 0, len(parser.segments))
	for _, source := range []segmentSource{segmentSourceInstruction, segmentSourceInput, segmentSourceOther} {
		for _, segment := range parser.segments {
			if segment.source == source {
				ordered = append(ordered, segment)
			}
		}
	}
	parser.segments = ordered
}

func markCurrentSegments(parser *jsonParser, protocol Protocol) {
	if parser == nil || len(parser.segments) == 0 {
		return
	}
	for index := range parser.segments {
		parser.segments[index].current = parser.segments[index].kind == TextInstruction
	}
	// Segments outside a conversation group are root-level, client-controlled
	// model-visible values (most notably tool definitions). Keep them in a
	// trusted current-turn audit even when the latest message group is narrow.
	grouped := make([]bool, len(parser.segments))
	for _, group := range parser.groups {
		start, end := group.start, group.end
		if start < 0 {
			start = 0
		}
		if end > len(grouped) {
			end = len(grouped)
		}
		for index := start; index < end; index++ {
			grouped[index] = true
		}
	}
	for index := range parser.segments {
		if !grouped[index] && parser.segments[index].source == segmentSourceOther {
			parser.segments[index].current = true
		}
	}
	// The most recent parsed group is authoritative. Never backfill an older
	// non-empty group into a trusted latest-turn audit scope.
	lastGroup := len(parser.groups) - 1
	if lastGroup < 0 {
		return
	}
	markGroup := func(index int) {
		if index < 0 || index >= len(parser.groups) {
			return
		}
		group := parser.groups[index]
		start, end := group.start, group.end
		if start < 0 {
			start = 0
		}
		if end > len(parser.segments) {
			end = len(parser.segments)
		}
		for segmentIndex := start; segmentIndex < end; segmentIndex++ {
			parser.segments[segmentIndex].current = true
		}
	}
	markGroupRange := func(start, end int) {
		if start < 0 {
			start = 0
		}
		if end > len(parser.groups) {
			end = len(parser.groups)
		}
		for index := start; index < end; index++ {
			markGroup(index)
		}
	}

	switch protocol {
	case ProtocolOpenAIResponses, ProtocolResponsesWebSocket:
		// A trusted previous_response_id proves only the server-side prefix.
		// Every input item in this request is part of the new delta, regardless
		// of role or item ordering, and must reach the blocking scanner.
		markGroupRange(0, len(parser.groups))
	case ProtocolOpenAIChat:
		for index, group := range parser.groups {
			if group.role == "system" || group.role == "developer" {
				markGroup(index)
			}
		}
		last := lastGroup
		role := parser.groups[last].role
		if role == "tool" || role == "function" {
			start := last
			for start > 0 {
				previous := parser.groups[start-1]
				if previous.role != "tool" && previous.role != "function" {
					break
				}
				start--
			}
			if start > 0 && parser.groups[start-1].role == "assistant" && parser.groups[start-1].assistantTool {
				start--
			}
			markGroupRange(start, last+1)
		} else {
			markGroup(last)
		}
	case ProtocolAnthropicMessages:
		for index, group := range parser.groups {
			if group.role == "system" {
				markGroup(index)
			}
		}
		last := lastGroup
		if parser.groups[last].role == "user" && parser.groups[last].toolish && last > 0 &&
			parser.groups[last-1].role == "assistant" && parser.groups[last-1].assistantTool {
			markGroup(last - 1)
		}
		markGroup(last)
	default:
		for index := range parser.segments {
			parser.segments[index].current = true
		}
	}
}

func (p *jsonParser) parseResponsesRoot(websocket bool) error {
	frameType := ""
	seenType := false
	seenResponsePayload := false
	seenNestedResponse := false
	seenFlatPayload := false
	seenResponseID := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "type":
			seenType = true
			frameType, err = p.parseStringShape(ReasonUnknownType)
			return err
		case "response":
			if !websocket {
				return p.uninspectable(ReasonUnknownField)
			}
			if seenFlatPayload {
				return p.uninspectable(ReasonUnknownContentShape)
			}
			seenNestedResponse = true
			seenResponsePayload = true
			return p.parseResponsesPayloadObject()
		case "instructions":
			p.promptContainerSeen = true
			if websocket && seenNestedResponse {
				return p.uninspectable(ReasonUnknownContentShape)
			}
			seenFlatPayload = true
			return p.withSegmentSource(segmentSourceInstruction, p.parseInstructionValue)
		case "input":
			p.promptContainerSeen = true
			if websocket && seenNestedResponse {
				return p.uninspectable(ReasonUnknownContentShape)
			}
			seenFlatPayload = true
			seenResponsePayload = true
			return p.withSegmentSource(segmentSourceInput, p.parseResponsesInput)
		case "previous_response_id":
			if websocket && seenNestedResponse {
				return p.uninspectable(ReasonUnknownContentShape)
			}
			seenFlatPayload = true
			return p.parsePreviousResponseID()
		case "response_id":
			if !websocket {
				return p.uninspectable(ReasonUnknownField)
			}
			seenResponseID = true
			_, err = p.parseStringShape(ReasonUnknownContentShape)
			return err
		case "conversation", "prompt":
			return p.rejectNonNull(ReasonRemoteContent)
		case "tools":
			p.promptContainerSeen = true
			return p.parseToolDefinitions(toolDefinitionsRequireType)
		case "functions":
			p.promptContainerSeen = true
			return p.parseToolDefinitions(toolDefinitionsImplicitFunction)
		case "text":
			start := len(p.segments)
			if err := p.parseResponsesTextConfig(); err != nil {
				return err
			}
			if len(p.segments) > start {
				p.promptContainerSeen = true
			}
			return nil
		case "tool_choice":
			return p.parseToolChoice()
		default:
			if isKnownResponsesRootField(name) {
				return p.skipValue()
			}
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	if !p.previousResponseID && p.resolveLineage != nil {
		p.lineage = p.resolveLineage("")
		if p.lineage != LineageTrusted {
			p.lineage = LineageUntrusted
		}
	}
	if !websocket {
		if seenType {
			return p.uninspectable(ReasonUnknownType)
		}
		if !p.promptContainerSeen {
			return p.uninspectable(ReasonUnknownContentShape)
		}
		// A non-empty previous_response_id is only admissible after the caller
		// has independently established trusted lineage.
		if p.previousResponseID && p.lineage != LineageTrusted {
			return p.uninspectable(ReasonUntrustedLineage)
		}
		return nil
	}
	frameType = strings.ToLower(strings.TrimSpace(frameType))
	switch frameType {
	case "response.create":
		if seenResponseID {
			return p.uninspectable(ReasonUnknownField)
		}
		if !seenResponsePayload {
			p.noTextReason = ReasonKnownNoText
		}
		if p.previousResponseID && p.lineage != LineageTrusted {
			return p.uninspectable(ReasonUntrustedLineage)
		}
		return nil
	case "response.cancel", "input_audio_buffer.clear", "input_audio_buffer.commit":
		if seenResponseID && frameType != "response.cancel" {
			return p.uninspectable(ReasonUnknownField)
		}
		if len(p.segments) != 0 {
			return p.uninspectable(ReasonUnknownContentShape)
		}
		p.noTextReason = ReasonKnownControlFrame
		return nil
	case "":
		return p.uninspectable(ReasonUnknownType)
	default:
		return p.uninspectable(ReasonUnknownType)
	}
}

func isKnownResponsesRootField(name string) bool {
	switch name {
	case "model", "background", "include", "max_output_tokens", "max_tool_calls", "metadata",
		"parallel_tool_calls", "reasoning", "safety_identifier", "service_tier", "store", "stream",
		"stream_options", "temperature", "tools", "top_logprobs", "top_p",
		"truncation", "user", "prompt_cache_key", "prompt_cache_retention":
		return true
	default:
		return false
	}
}

func (p *jsonParser) parseResponsesPayloadObject() error {
	if p.peek() != '{' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	return p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "instructions":
			p.promptContainerSeen = true
			return p.withSegmentSource(segmentSourceInstruction, p.parseInstructionValue)
		case "input":
			p.promptContainerSeen = true
			return p.withSegmentSource(segmentSourceInput, p.parseResponsesInput)
		case "previous_response_id":
			return p.parsePreviousResponseID()
		case "conversation", "prompt":
			return p.rejectNonNull(ReasonRemoteContent)
		case "tools":
			p.promptContainerSeen = true
			return p.parseToolDefinitions(toolDefinitionsRequireType)
		case "functions":
			p.promptContainerSeen = true
			return p.parseToolDefinitions(toolDefinitionsImplicitFunction)
		case "text":
			start := len(p.segments)
			if err := p.parseResponsesTextConfig(); err != nil {
				return err
			}
			if len(p.segments) > start {
				p.promptContainerSeen = true
			}
			return nil
		case "tool_choice":
			return p.parseToolChoice()
		default:
			if isKnownResponsesRootField(name) {
				return p.skipValue()
			}
			return p.uninspectable(ReasonUnknownField)
		}
	})
}

func (p *jsonParser) parsePreviousResponseID() error {
	if p.isNull() {
		return p.parseLiteral("null")
	}
	if p.peek() != '"' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	value, err := p.parseStringShape(ReasonUnknownContentShape)
	if err != nil {
		return err
	}
	if value = strings.TrimSpace(value); value != "" {
		p.previousResponseID = true
		if p.resolveLineage != nil {
			p.lineage = p.resolveLineage(value)
			if p.lineage != LineageTrusted {
				p.lineage = LineageUntrusted
			}
		}
	}
	return nil
}

func (p *jsonParser) parseInstructionValue() error {
	switch p.peek() {
	case '"':
		return p.parseAndAddString(TextInstruction, "system")
	case '[':
		return p.parseArray(func(_ int) error {
			return p.parseTextContentBlock(TextInstruction, "system", false)
		})
	case 'n':
		return p.parseLiteral("null")
	default:
		return p.unknownValue(ReasonUnknownContentShape)
	}
}

func (p *jsonParser) parseResponsesInput() error {
	switch p.peek() {
	case '"':
		start := len(p.segments)
		err := p.parseAndAddString(TextMessage, "user")
		if err == nil {
			p.recordGroup(start, "user", false, false)
		}
		return err
	case '[':
		return p.parseArray(func(_ int) error { return p.parseResponsesItem() })
	case '{':
		return p.parseResponsesItem()
	case 'n':
		return p.parseLiteral("null")
	default:
		return p.unknownValue(ReasonUnknownContentShape)
	}
}

func (p *jsonParser) parseResponsesItem() error {
	start := len(p.segments)
	if p.peek() == '"' {
		err := p.parseAndAddString(TextMessage, "user")
		if err == nil {
			p.recordGroup(start, "user", false, false)
		}
		return err
	}
	if p.peek() != '{' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	typeName := ""
	role := ""
	hasType := false
	hasContent := false
	hasText := false
	hasArguments := false
	hasOutput := false
	hasInput := false
	hasAction := false
	hasTools := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "type":
			hasType = true
			typeName, err = p.parseStringShape(ReasonUnknownType)
			return err
		case "role":
			role, err = p.parseStringShape(ReasonUnknownRole)
			return err
		case "content":
			hasContent = true
			return p.parseTextContentValue(TextMessage, role, true)
		case "text":
			hasContent = true
			hasText = true
			return p.parseRequiredText(TextMessage, role)
		case "arguments":
			hasContent = true
			hasArguments = true
			return p.parseToolTextValue(TextToolInput, "tool")
		case "output":
			hasContent = true
			hasOutput = true
			return p.parseToolTextValue(TextToolOutput, "tool")
		case "input":
			hasContent = true
			hasInput = true
			return p.parseToolTextValue(TextToolInput, "tool")
		case "command":
			hasContent = true
			hasInput = true
			return p.parseToolTextValue(TextToolInput, "tool")
		case "action":
			hasContent = true
			hasAction = true
			return p.parseToolTextValue(TextToolInput, "tool")
		case "summary":
			hasContent = true
			return p.parseTextContentValue(TextMessage, "assistant", false)
		case "tools":
			hasTools = true
			return p.parseToolDefinitions(toolDefinitionsRequireType)
		case "encrypted_content":
			return p.rejectNonNull(ReasonEncryptedContent)
		case "image_url", "input_audio", "audio", "file", "file_id", "source", "data", "url":
			return p.rejectNonNull(ReasonMediaContent)
		case "item_reference", "conversation", "prompt_id":
			return p.rejectNonNull(ReasonRemoteContent)
		case "name", "namespace", "server_label":
			return p.parseRequiredVisibleText(TextToolInput, "tool")
		case "id", "call_id", "status", "approval_request_id", "execution", "format":
			if name == "format" {
				return p.parseToolSchemaValue()
			}
			return p.skipValue()
		default:
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	typeName = strings.ToLower(strings.TrimSpace(typeName))
	role = strings.ToLower(strings.TrimSpace(role))
	if role != "" && !isKnownClientRole(role) {
		return p.uninspectable(ReasonUnknownRole)
	}
	if role == "" {
		role = responseItemDefaultRole(typeName)
	}
	p.applyRole(start, role)
	if hasTools && typeName != "additional_tools" {
		return p.uninspectable(ReasonUnknownField)
	}
	switch typeName {
	case "item_reference", "computer_call", "computer_call_output":
		return p.uninspectable(ReasonRemoteContent)
	case "input_image", "image", "input_audio", "audio", "file", "input_file":
		return p.uninspectable(ReasonMediaContent)
	}
	switch typeName {
	case "message":
		if !hasContent || len(p.segments) == start {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "input_text", "output_text", "text":
		if !hasText || len(p.segments) == start {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "function_call", "tool_call", "mcp_call", "custom_tool_call", "local_shell_call", "shell_call":
		if !hasArguments && !hasInput && !hasAction {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "function_call_output", "tool_call_output", "mcp_call_output", "mcp_tool_call_output", "custom_tool_call_output", "tool_search_output", "local_shell_call_output", "shell_call_output":
		if !hasOutput {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "mcp_tool_call", "tool_search_call":
		if !hasArguments && !hasInput {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "reasoning":
		if !hasContent || len(p.segments) == start {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "additional_tools":
		if !hasTools || hasContent {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "":
		if hasType || !hasContent {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	default:
		return p.uninspectable(ReasonUnknownType)
	}
	if typeName == "" && !hasContent {
		return p.uninspectable(ReasonUnknownContentShape)
	}
	toolish := responseItemIsTool(typeName)
	assistantTool := toolish && (role == "assistant" || responseItemIsAssistantTool(typeName))
	p.recordGroup(start, role, toolish, assistantTool)
	return nil
}

func responseItemDefaultRole(typeName string) string {
	switch typeName {
	case "additional_tools":
		return "developer"
	case "function_call", "tool_call", "custom_tool_call", "mcp_call", "mcp_tool_call", "tool_search_call", "local_shell_call", "shell_call", "reasoning", "output_text":
		return "assistant"
	case "function_call_output", "tool_call_output", "custom_tool_call_output", "mcp_call_output", "mcp_tool_call_output", "tool_search_output", "local_shell_call_output", "shell_call_output":
		return "tool"
	default:
		return "user"
	}
}

func responseItemIsTool(typeName string) bool {
	switch typeName {
	case "function_call", "function_call_output", "tool_call", "tool_call_output", "custom_tool_call", "custom_tool_call_output",
		"mcp_call", "mcp_call_output", "mcp_tool_call", "mcp_tool_call_output", "tool_search_call", "tool_search_output",
		"local_shell_call", "local_shell_call_output", "shell_call", "shell_call_output":
		return true
	default:
		return false
	}
}

func responseItemIsAssistantTool(typeName string) bool {
	switch typeName {
	case "function_call", "tool_call", "custom_tool_call", "mcp_call", "mcp_tool_call", "tool_search_call", "local_shell_call", "shell_call":
		return true
	default:
		return false
	}
}

func isKnownClientRole(role string) bool {
	switch role {
	case "user", "system", "developer", "assistant", "tool", "function", "model":
		return true
	default:
		return false
	}
}

func (p *jsonParser) parseRequiredText(kind TextKind, role string) error {
	if p.peek() != '"' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	return p.parseAndAddString(kind, role)
}

func (p *jsonParser) parseRequiredVisibleText(kind TextKind, role string) error {
	start := len(p.segments)
	if err := p.parseRequiredText(kind, role); err != nil {
		return err
	}
	if len(p.segments) == start {
		return p.uninspectable(ReasonUnknownContentShape)
	}
	return nil
}

func (p *jsonParser) parseToolTextValue(kind TextKind, role string) error {
	start := len(p.segments)
	if err := p.parseToolJSONValue(kind, role); err != nil {
		return err
	}
	// Tool arguments/results are typed prompt-bearing values. A null, empty
	// container, empty string, or scalar-only value must not collapse the
	// enclosing tool item into known_no_text.
	if len(p.segments) == start {
		return p.uninspectable(ReasonUnknownContentShape)
	}
	return nil
}

// parseToolJSONValue handles tool arguments and results without constructing a
// generic JSON tree. Object keys and string values are retained as canonical
// spans; sensitive protocol markers are rejected before their values can be
// silently treated as ordinary text. Plain URL fields remain auditable text,
// while explicit media/file shapes are uninspectable because the scanner
// cannot inspect the referenced bytes.
func (p *jsonParser) parseToolJSONValue(kind TextKind, role string) error {
	switch p.peek() {
	case '"':
		ref, err := p.parseString()
		if err != nil {
			return err
		}
		value, err := ref.decode(p.body)
		if err != nil {
			return err
		}
		if looksLikeMediaData(value) {
			return p.uninspectable(ReasonMediaContent)
		}
		return p.addText(ref, kind, role)
	case '[', '{':
		if p.peek() == '[' {
			return p.parseArray(func(_ int) error { return p.parseToolJSONValue(kind, role) })
		}
		return p.parseObject(func(key stringRef) error {
			name, err := key.decode(p.body)
			if err != nil {
				return p.invalid("invalid object key")
			}
			name = strings.ToLower(strings.TrimSpace(name))
			switch name {
			case "encrypted_content", "signature":
				if err := p.rejectNonNull(ReasonEncryptedContent); err != nil {
					return err
				}
				return p.addText(key, kind, role)
			case "item_reference", "conversation", "prompt", "prompt_id", "remote_id":
				if err := p.rejectNonNull(ReasonRemoteContent); err != nil {
					return err
				}
				return p.addText(key, kind, role)
			case "image_url", "input_image", "input_audio", "audio", "file", "file_id", "source", "data":
				if err := p.rejectNonNull(ReasonMediaContent); err != nil {
					return err
				}
				return p.addText(key, kind, role)
			case "type":
				if p.peek() != '"' {
					return p.unknownValue(ReasonUnknownType)
				}
				valueRef, err := p.parseString()
				if err != nil {
					return err
				}
				value, err := valueRef.decode(p.body)
				if err != nil {
					return p.invalid("invalid tool type")
				}
				switch strings.ToLower(strings.TrimSpace(value)) {
				case "image", "image_url", "input_image", "input_audio", "audio", "file", "input_file":
					return p.uninspectable(ReasonMediaContent)
				case "encrypted", "encrypted_content", "redacted_thinking":
					return p.uninspectable(ReasonEncryptedContent)
				case "item_reference", "remote", "conversation":
					return p.uninspectable(ReasonRemoteContent)
				}
				if err := p.addText(key, kind, role); err != nil {
					return err
				}
				return p.addText(valueRef, kind, role)
			}
			if err := p.addText(key, kind, role); err != nil {
				return err
			}
			return p.parseToolJSONValue(kind, role)
		})
	case 'n':
		return p.parseLiteral("null")
	default:
		return p.unknownValue(ReasonUnknownContentShape)
	}
}

func looksLikeMediaData(value string) bool {
	value = strings.ToLower(strings.TrimSpace(value))
	return strings.HasPrefix(value, "data:image/") || strings.HasPrefix(value, "data:audio/") ||
		strings.HasPrefix(value, "data:video/") || strings.HasPrefix(value, "blob:")
}

type toolDefinitionMode uint8

const (
	toolDefinitionsRequireType toolDefinitionMode = iota
	toolDefinitionsImplicitFunction
	toolDefinitionsAnthropicFunction
)

// parseToolDefinitions scans inline client tools while rejecting hosted tools
// whose retrieved content cannot be included in the blocking audit corpus.
func (p *jsonParser) parseToolDefinitions(mode toolDefinitionMode) error {
	switch p.peek() {
	case 'n':
		return p.parseLiteral("null")
	case '{':
		return p.parseToolDefinition(mode)
	case '[':
		return p.parseArray(func(_ int) error {
			if p.peek() != '{' {
				return p.unknownValue(ReasonUnknownContentShape)
			}
			return p.parseToolDefinition(mode)
		})
	default:
		return p.unknownValue(ReasonUnknownContentShape)
	}
}

func (p *jsonParser) parseToolDefinition(mode toolDefinitionMode) error {
	toolType := ""
	toolName := ""
	hasType := false
	hasNestedTools := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "type":
			hasType = true
			toolType, err = p.parseStringShape(ReasonUnknownType)
			return err
		case "tools":
			hasNestedTools = true
			return p.parseToolDefinitions(toolDefinitionsRequireType)
		case "name":
			if p.peek() != '"' {
				return p.unknownValue(ReasonUnknownContentShape)
			}
			if err := p.addText(key, TextToolInput, "tool"); err != nil {
				return err
			}
			ref, err := p.parseString()
			if err != nil {
				return err
			}
			toolName, err = ref.decode(p.body)
			if err != nil {
				return p.invalid("invalid tool name")
			}
			return p.addText(ref, TextToolInput, "tool")
		default:
			if err := p.addText(key, TextToolInput, "tool"); err != nil {
				return err
			}
			return p.parseToolSchemaValue()
		}
	})
	if err != nil {
		return err
	}
	if mode == toolDefinitionsAnthropicFunction && isAnthropicEmulatedHostedToolName(toolName) {
		return p.uninspectable(ReasonRemoteContent)
	}
	if !hasType {
		if mode == toolDefinitionsImplicitFunction || mode == toolDefinitionsAnthropicFunction {
			return nil
		}
		return p.uninspectable(ReasonUnknownContentShape)
	}

	toolType = strings.ToLower(strings.TrimSpace(toolType))
	switch toolType {
	case "function", "custom":
		return nil
	case "namespace":
		if !hasNestedTools {
			return p.uninspectable(ReasonUnknownContentShape)
		}
		return nil
	}
	if reason := hostedToolDefinitionReason(toolType); reason != "" {
		return p.uninspectable(reason)
	}
	return p.uninspectable(ReasonUnknownType)
}

func isAnthropicEmulatedHostedToolName(toolName string) bool {
	switch strings.ToLower(strings.TrimSpace(toolName)) {
	case "web_search", "google_search", "web_search_20250305":
		return true
	default:
		return false
	}
}

func hostedToolDefinitionReason(toolType string) ReasonCode {
	switch {
	case hostedToolTypeMatches(toolType, "image_generation"),
		hostedToolTypeMatches(toolType, "audio_generation"),
		hostedToolTypeMatches(toolType, "video_generation"),
		hostedToolTypeMatches(toolType, "computer_use"),
		hostedToolTypeMatches(toolType, "computer"):
		return ReasonMediaContent
	case hostedToolTypeMatches(toolType, "file_search"),
		hostedToolTypeMatches(toolType, "web_search"),
		hostedToolTypeMatches(toolType, "x_search"),
		hostedToolTypeMatches(toolType, "google_search"),
		hostedToolTypeMatches(toolType, "web_fetch"),
		hostedToolTypeMatches(toolType, "url_context"),
		hostedToolTypeMatches(toolType, "mcp"),
		hostedToolTypeMatches(toolType, "hosted_mcp"),
		hostedToolTypeMatches(toolType, "tool_search"),
		hostedToolTypeMatches(toolType, "code_interpreter"),
		hostedToolTypeMatches(toolType, "code_execution"),
		hostedToolTypeMatches(toolType, "local_shell"),
		hostedToolTypeMatches(toolType, "shell"),
		hostedToolTypeMatches(toolType, "bash"),
		hostedToolTypeMatches(toolType, "text_editor"),
		hostedToolTypeMatches(toolType, "apply_patch"),
		hostedToolTypeMatches(toolType, "container"),
		hostedToolTypeMatches(toolType, "memory"):
		return ReasonRemoteContent
	default:
		return ""
	}
}

func hostedToolTypeMatches(toolType, base string) bool {
	return toolType == base || strings.HasPrefix(toolType, base+"_")
}

func (p *jsonParser) parseToolSchemaValue() error {
	switch p.peek() {
	case '"':
		return p.parseAndAddString(TextToolInput, "tool")
	case '{':
		return p.parseObject(func(key stringRef) error {
			if err := p.addText(key, TextToolInput, "tool"); err != nil {
				return err
			}
			return p.parseToolSchemaValue()
		})
	case '[':
		return p.parseArray(func(_ int) error { return p.parseToolSchemaValue() })
	case 't':
		return p.parseLiteral("true")
	case 'f':
		return p.parseLiteral("false")
	case 'n':
		return p.parseLiteral("null")
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		return p.parseNumber()
	default:
		return p.unknownValue(ReasonUnknownContentShape)
	}
}

func (p *jsonParser) parseResponsesTextConfig() error {
	if p.isNull() {
		return p.parseLiteral("null")
	}
	if p.peek() != '{' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	return p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "format":
			return p.parseResponsesTextFormat()
		case "verbosity":
			value, err := p.parseStringShape(ReasonUnknownType)
			if err != nil {
				return err
			}
			switch strings.ToLower(strings.TrimSpace(value)) {
			case "low", "medium", "high":
				return nil
			default:
				return p.uninspectable(ReasonUnknownType)
			}
		default:
			return p.uninspectable(ReasonUnknownField)
		}
	})
}

func (p *jsonParser) parseResponsesTextFormat() error {
	if p.isNull() {
		return p.parseLiteral("null")
	}
	if p.peek() != '{' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	typeName := ""
	hasName := false
	hasDescription := false
	hasSchema := false
	hasStrict := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "type":
			typeName, err = p.parseStringShape(ReasonUnknownType)
			return err
		case "name":
			hasName = true
			return p.parseRequiredVisibleText(TextInstruction, "system")
		case "description":
			hasDescription = true
			return p.parseRequiredText(TextInstruction, "system")
		case "schema":
			hasSchema = true
			if p.peek() != '{' {
				return p.unknownValue(ReasonUnknownContentShape)
			}
			return p.parseToolSchemaValue()
		case "strict":
			hasStrict = true
			return p.parseBoolean()
		default:
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(typeName)) {
	case "text", "json_object":
		if hasName || hasDescription || hasSchema || hasStrict {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "json_schema":
		if !hasName || !hasSchema {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "":
		return p.uninspectable(ReasonUnknownContentShape)
	default:
		return p.uninspectable(ReasonUnknownType)
	}
	return nil
}

func (p *jsonParser) parseChatResponseFormat() error {
	if p.isNull() {
		return p.parseLiteral("null")
	}
	if p.peek() != '{' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	typeName := ""
	hasJSONSchema := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "type":
			typeName, err = p.parseStringShape(ReasonUnknownType)
			return err
		case "json_schema":
			hasJSONSchema = true
			return p.parseNamedJSONSchema()
		default:
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(typeName)) {
	case "text", "json_object":
		if hasJSONSchema {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "json_schema":
		if !hasJSONSchema {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "":
		return p.uninspectable(ReasonUnknownContentShape)
	default:
		return p.uninspectable(ReasonUnknownType)
	}
	return nil
}

func (p *jsonParser) parseNamedJSONSchema() error {
	if p.peek() != '{' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	hasName := false
	hasSchema := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "name":
			hasName = true
			return p.parseRequiredVisibleText(TextInstruction, "system")
		case "description":
			return p.parseRequiredText(TextInstruction, "system")
		case "schema":
			hasSchema = true
			if p.peek() != '{' {
				return p.unknownValue(ReasonUnknownContentShape)
			}
			return p.parseToolSchemaValue()
		case "strict":
			return p.parseBoolean()
		default:
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	if !hasName || !hasSchema {
		return p.uninspectable(ReasonUnknownContentShape)
	}
	return nil
}

func (p *jsonParser) parseChatPrediction() error {
	if p.isNull() {
		return p.parseLiteral("null")
	}
	if p.peek() != '{' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	start := len(p.segments)
	typeName := ""
	hasContent := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "type":
			typeName, err = p.parseStringShape(ReasonUnknownType)
			return err
		case "content":
			hasContent = true
			return p.parseTextContentValue(TextMessage, "assistant", true)
		default:
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(typeName)) {
	case "content":
		if !hasContent || len(p.segments) == start {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "":
		return p.uninspectable(ReasonUnknownContentShape)
	default:
		return p.uninspectable(ReasonUnknownType)
	}
	return nil
}

func (p *jsonParser) parseToolChoice() error {
	switch p.peek() {
	case 'n':
		return p.parseLiteral("null")
	case '"':
		value, err := p.parseStringShape(ReasonUnknownType)
		if err != nil {
			return err
		}
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "auto", "none", "required", "any":
			return nil
		default:
			return p.uninspectable(ReasonUnknownType)
		}
	case '{':
		_, err := p.parseToolChoiceObject(true)
		return err
	default:
		return p.unknownValue(ReasonUnknownContentShape)
	}
}

func (p *jsonParser) parseLegacyFunctionChoice() error {
	switch p.peek() {
	case 'n':
		return p.parseLiteral("null")
	case '"':
		value, err := p.parseStringShape(ReasonUnknownType)
		if err != nil {
			return err
		}
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "auto", "none":
			return nil
		default:
			return p.uninspectable(ReasonUnknownType)
		}
	case '{':
		_, err := p.parseToolChoiceObject(false)
		return err
	default:
		return p.unknownValue(ReasonUnknownContentShape)
	}
}

func (p *jsonParser) parseToolChoiceObject(requireType bool) (bool, error) {
	typeName := ""
	hasSelector := false
	hasNestedSelector := false
	nestedSelectorCount := 0
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "type":
			typeName, err = p.parseStringShape(ReasonUnknownType)
			return err
		case "name", "namespace", "server_label":
			hasSelector = true
			return p.parseRequiredVisibleText(TextToolInput, "tool")
		case "function", "tool":
			if p.peek() != '{' {
				return p.unknownValue(ReasonUnknownContentShape)
			}
			nested, err := p.parseToolChoiceObject(false)
			if err != nil {
				return err
			}
			hasNestedSelector = hasNestedSelector || nested
			nestedSelectorCount++
			return nil
		case "disable_parallel_tool_use":
			return p.parseBoolean()
		default:
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return false, err
	}
	if nestedSelectorCount > 1 || (hasSelector && hasNestedSelector) {
		return false, p.uninspectable(ReasonUnknownContentShape)
	}
	typeName = strings.ToLower(strings.TrimSpace(typeName))
	switch typeName {
	case "auto", "none", "required", "any":
		if hasSelector || hasNestedSelector {
			return false, p.uninspectable(ReasonUnknownContentShape)
		}
	case "function", "custom", "tool", "namespace", "mcp", "mcp_tool":
		if !hasSelector && !hasNestedSelector {
			return false, p.uninspectable(ReasonUnknownContentShape)
		}
	case "image_generation", "tool_search", "local_shell":
		if hasNestedSelector {
			return false, p.uninspectable(ReasonUnknownContentShape)
		}
	case "":
		if requireType && !hasNestedSelector {
			return false, p.uninspectable(ReasonUnknownContentShape)
		}
		if !hasSelector && !hasNestedSelector {
			return false, p.uninspectable(ReasonUnknownContentShape)
		}
	default:
		return false, p.uninspectable(ReasonUnknownType)
	}
	return hasSelector || hasNestedSelector, nil
}

func (p *jsonParser) parseBoolean() error {
	switch p.peek() {
	case 't':
		return p.parseLiteral("true")
	case 'f':
		return p.parseLiteral("false")
	default:
		return p.unknownValue(ReasonUnknownContentShape)
	}
}

func (p *jsonParser) parseTextContentValue(kind TextKind, role string, allowStrings bool) error {
	switch p.peek() {
	case '"':
		if !allowStrings {
			return p.uninspectable(ReasonUnknownContentShape)
		}
		return p.parseAndAddString(kind, role)
	case '[':
		return p.parseArray(func(_ int) error {
			return p.parseTextContentBlock(kind, role, allowStrings)
		})
	case '{':
		return p.parseTextContentBlock(kind, role, allowStrings)
	case 'n':
		return p.parseLiteral("null")
	default:
		return p.unknownValue(ReasonUnknownContentShape)
	}
}

func (p *jsonParser) parseTextContentBlock(kind TextKind, role string, allowString bool) error {
	start := len(p.segments)
	if p.peek() == '"' {
		if !allowString {
			return p.uninspectable(ReasonUnknownContentShape)
		}
		return p.parseAndAddString(kind, role)
	}
	if p.peek() != '{' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	typeName := ""
	hasText := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "type":
			typeName, err = p.parseStringShape(ReasonUnknownType)
			return err
		case "text":
			hasText = true
			return p.parseRequiredText(kind, role)
		case "content":
			hasText = true
			return p.parseTextContentValue(kind, role, allowString)
		case "image_url", "input_audio", "audio", "file", "file_id", "source", "data", "url":
			return p.rejectNonNull(ReasonMediaContent)
		case "encrypted_content":
			return p.rejectNonNull(ReasonEncryptedContent)
		case "id", "name", "annotations":
			return p.skipValue()
		default:
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(typeName)) {
	case "", "text", "input_text", "output_text", "message":
		if !hasText || len(p.segments) == start {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "summary_text":
		if !hasText || len(p.segments) == start {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "image_url", "input_image", "image", "input_audio", "audio", "file", "input_file":
		return p.uninspectable(ReasonMediaContent)
	default:
		return p.uninspectable(ReasonUnknownType)
	}
	return nil
}

func (p *jsonParser) rejectNonNull(reason ReasonCode) error {
	if p.isNull() {
		return p.parseLiteral("null")
	}
	return p.unknownValue(reason)
}

func (p *jsonParser) parseChatRoot() error {
	seenMessages := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "model":
			return p.parseChatModel()
		case "messages":
			seenMessages = true
			if p.peek() != '[' {
				return p.unknownValue(ReasonUnknownContentShape)
			}
			return p.parseArray(func(_ int) error { return p.parseChatMessage() })
		case "conversation", "prompt", "previous_response_id":
			return p.rejectNonNull(ReasonRemoteContent)
		case "tools":
			return p.parseToolDefinitions(toolDefinitionsRequireType)
		case "functions":
			return p.parseToolDefinitions(toolDefinitionsImplicitFunction)
		case "function_call":
			return p.parseLegacyFunctionChoice()
		case "tool_choice":
			return p.parseToolChoice()
		case "response_format":
			return p.parseChatResponseFormat()
		case "prediction":
			return p.parseChatPrediction()
		case "web_search_options":
			return p.rejectNonNull(ReasonRemoteContent)
		default:
			if isKnownChatRootField(name) {
				return p.skipValue()
			}
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	if !seenMessages {
		return p.uninspectable(ReasonUnknownContentShape)
	}
	return nil
}

func (p *jsonParser) parseChatModel() error {
	model, err := p.parseStringShape(ReasonUnknownContentShape)
	if err != nil {
		return err
	}
	if ModelImpliesRemoteSearch(model) {
		return p.uninspectable(ReasonRemoteContent)
	}
	return nil
}

// ModelImpliesRemoteSearch reports whether selecting model necessarily adds
// hosted search results that are absent from the blocking scanner corpus. It is
// shared with post-classification channel/account model mapping admission.
func ModelImpliesRemoteSearch(model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	return strings.Contains(model, "search-preview") ||
		strings.Contains(model, "search-api") ||
		strings.Contains(model, "deep-research")
}

func isKnownChatRootField(name string) bool {
	switch name {
	case "audio", "frequency_penalty", "functions", "logit_bias", "logprobs",
		"max_completion_tokens", "max_tokens", "metadata", "modalities", "n", "parallel_tool_calls",
		"presence_penalty", "reasoning_effort", "safety_identifier", "seed",
		"service_tier", "stop", "store", "stream", "stream_options", "temperature", "tools",
		"top_logprobs", "top_p", "user":
		return true
	default:
		return false
	}
}

func (p *jsonParser) parseChatMessage() error {
	start := len(p.segments)
	if p.peek() != '{' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	role := ""
	hasPromptField := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "role":
			role, err = p.parseStringShape(ReasonUnknownRole)
			return err
		case "content", "refusal":
			hasPromptField = true
			return p.parseTextContentValue(TextMessage, role, true)
		case "tool_calls":
			hasPromptField = true
			if p.peek() != '[' {
				return p.unknownValue(ReasonUnknownContentShape)
			}
			return p.parseArray(func(_ int) error { return p.parseChatToolCall() })
		case "function_call":
			hasPromptField = true
			return p.parseChatFunctionCall()
		case "audio":
			return p.rejectNonNull(ReasonMediaContent)
		case "name":
			return p.parseRequiredVisibleText(TextMessage, role)
		case "tool_call_id":
			return p.skipValue()
		default:
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	role = strings.ToLower(strings.TrimSpace(role))
	if role == "" || !isKnownClientRole(role) {
		return p.uninspectable(ReasonUnknownRole)
	}
	if !hasPromptField {
		return p.uninspectable(ReasonUnknownContentShape)
	}
	if (role == "tool" || role == "function") && len(p.segments) == start {
		return p.uninspectable(ReasonUnknownContentShape)
	}
	p.applyRole(start, role)
	toolish := role == "tool" || role == "function"
	assistantTool := false
	for index := start; index < len(p.segments); index++ {
		if p.segments[index].kind == TextToolInput {
			assistantTool = role == "assistant"
			toolish = toolish || assistantTool
			break
		}
	}
	p.recordGroup(start, role, toolish, assistantTool)
	return nil
}

func (p *jsonParser) parseChatToolCall() error {
	if p.peek() != '{' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	typeName := ""
	hasCallObject := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "type":
			typeName, err = p.parseStringShape(ReasonUnknownType)
			return err
		case "function", "custom":
			hasCallObject = true
			return p.parseChatFunctionCall()
		case "name", "namespace", "server_label":
			return p.parseRequiredVisibleText(TextToolInput, "assistant")
		case "id", "index":
			return p.skipValue()
		default:
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	if typeName != "" && strings.ToLower(strings.TrimSpace(typeName)) != "function" && strings.ToLower(strings.TrimSpace(typeName)) != "custom" {
		return p.uninspectable(ReasonUnknownType)
	}
	if !hasCallObject {
		return p.uninspectable(ReasonUnknownContentShape)
	}
	return nil
}

func (p *jsonParser) parseChatFunctionCall() error {
	if p.isNull() {
		return p.parseLiteral("null")
	}
	if p.peek() != '{' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	hasArguments := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "arguments", "input":
			hasArguments = true
			return p.parseToolTextValue(TextToolInput, "assistant")
		case "name", "namespace", "server_label":
			return p.parseRequiredVisibleText(TextToolInput, "assistant")
		default:
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	if !hasArguments {
		return p.uninspectable(ReasonUnknownContentShape)
	}
	return nil
}

func (p *jsonParser) parseAnthropicRoot() error {
	seenMessages := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "system":
			return p.parseAnthropicSystem()
		case "messages":
			seenMessages = true
			if p.peek() != '[' {
				return p.unknownValue(ReasonUnknownContentShape)
			}
			return p.parseArray(func(_ int) error { return p.parseAnthropicMessage() })
		case "mcp_servers", "container", "conversation":
			return p.rejectNonNull(ReasonRemoteContent)
		case "tools":
			return p.parseToolDefinitions(toolDefinitionsAnthropicFunction)
		case "tool_choice":
			return p.parseToolChoice()
		default:
			if isKnownAnthropicRootField(name) {
				return p.skipValue()
			}
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	if !seenMessages {
		return p.uninspectable(ReasonUnknownContentShape)
	}
	return nil
}

func isKnownAnthropicRootField(name string) bool {
	switch name {
	case "model", "max_tokens", "metadata", "stop_sequences", "stream", "temperature", "thinking",
		"tools", "top_k", "top_p", "service_tier", "context_management":
		return true
	default:
		return false
	}
}

func (p *jsonParser) parseAnthropicSystem() error {
	switch p.peek() {
	case '"':
		return p.parseAndAddString(TextInstruction, "system")
	case '[':
		return p.parseArray(func(_ int) error { return p.parseAnthropicBlock("system") })
	case 'n':
		return p.parseLiteral("null")
	default:
		return p.unknownValue(ReasonUnknownContentShape)
	}
}

func (p *jsonParser) parseAnthropicMessage() error {
	start := len(p.segments)
	if p.peek() != '{' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	role := ""
	hasContent := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "role":
			role, err = p.parseStringShape(ReasonUnknownRole)
			return err
		case "content":
			hasContent = true
			switch p.peek() {
			case '"':
				return p.parseAndAddString(TextMessage, role)
			case '[':
				return p.parseArray(func(_ int) error { return p.parseAnthropicBlock(role) })
			default:
				return p.unknownValue(ReasonUnknownContentShape)
			}
		default:
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	role = strings.ToLower(strings.TrimSpace(role))
	if role != "user" && role != "assistant" {
		return p.uninspectable(ReasonUnknownRole)
	}
	if !hasContent {
		return p.uninspectable(ReasonUnknownContentShape)
	}
	p.applyRole(start, role)
	toolish := false
	assistantTool := false
	for index := start; index < len(p.segments); index++ {
		switch p.segments[index].kind {
		case TextToolOutput:
			toolish = role == "user"
		case TextToolInput:
			assistantTool = role == "assistant"
		}
	}
	p.recordGroup(start, role, toolish, assistantTool)
	return nil
}

func (p *jsonParser) parseAnthropicBlock(role string) error {
	if p.peek() == '"' {
		kind := TextMessage
		if strings.EqualFold(strings.TrimSpace(role), "tool") {
			kind = TextToolOutput
		}
		return p.parseAndAddString(kind, role)
	}
	if p.peek() != '{' {
		return p.unknownValue(ReasonUnknownContentShape)
	}
	typeName := ""
	start := len(p.segments)
	hasText := false
	hasData := false
	err := p.parseObject(func(key stringRef) error {
		name, err := key.decode(p.body)
		if err != nil {
			return p.invalid("invalid object key")
		}
		switch name {
		case "type":
			typeName, err = p.parseStringShape(ReasonUnknownType)
			return err
		case "text", "thinking":
			hasText = true
			kind := TextMessage
			if strings.EqualFold(strings.TrimSpace(role), "tool") {
				kind = TextToolOutput
			}
			return p.parseRequiredText(kind, role)
		case "content":
			hasText = true
			switch p.peek() {
			case '"':
				return p.parseAndAddString(TextToolOutput, "tool")
			case '[':
				return p.parseArray(func(_ int) error { return p.parseAnthropicBlock("tool") })
			default:
				return p.unknownValue(ReasonUnknownContentShape)
			}
		case "input":
			hasText = true
			return p.parseToolTextValue(TextToolInput, "tool")
		case "signature", "encrypted_content":
			return p.rejectNonNull(ReasonEncryptedContent)
		case "source", "image", "document", "audio", "file_id":
			return p.rejectNonNull(ReasonMediaContent)
		case "data":
			// `redacted_thinking` commonly carries an opaque data payload. Defer
			// the semantic reason until the type field is known; JSON member
			// order is not significant and data may precede type.
			if p.isNull() {
				return p.parseLiteral("null")
			}
			hasData = true
			return p.skipValue()
		case "name", "namespace", "server_label":
			return p.parseRequiredVisibleText(TextToolInput, "tool")
		case "tool_use_id", "id", "cache_control", "is_error", "citations":
			return p.skipValue()
		default:
			return p.uninspectable(ReasonUnknownField)
		}
	})
	if err != nil {
		return err
	}
	switch strings.ToLower(strings.TrimSpace(typeName)) {
	case "text", "thinking":
		if !hasText || len(p.segments) == start {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "tool_result":
		if !hasText || len(p.segments) == start {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "tool_use", "server_tool_use":
		if !hasText || len(p.segments) == start {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	case "redacted_thinking":
		return p.uninspectable(ReasonEncryptedContent)
	case "image", "document", "audio":
		return p.uninspectable(ReasonMediaContent)
	case "web_search_tool_result", "code_execution_tool_result", "mcp_tool_result":
		return p.uninspectable(ReasonRemoteContent)
	case "":
		if !hasText {
			return p.uninspectable(ReasonUnknownContentShape)
		}
	default:
		if hasData {
			return p.uninspectable(ReasonMediaContent)
		}
		return p.uninspectable(ReasonUnknownType)
	}
	return nil
}
