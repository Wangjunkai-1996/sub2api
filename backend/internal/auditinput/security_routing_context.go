package auditinput

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
)

const SecurityRoutingReasonIncomplete = "incomplete"

// SecurityRoutingContext is the bounded security text used for both
// account routing and any subsequent OAuth account audit. Document deliberately
// excludes request size contributors that are not useful audit text, such as
// tool definitions, images, assistant history, and protocol metadata.
type SecurityRoutingContext struct {
	Document         *Document
	AuditTextRunes   int
	Reliable         bool
	UnreliableReason string
}

// ParseSecurityRoutingContext extracts stable instructions plus the same
// current user/tool increment selected by ParseForTextAudit. Its Document can
// be passed directly to the strict auditor so routing and auditing use exactly
// the same text and Unicode rune count.
func ParseSecurityRoutingContext(protocol string, body []byte) *SecurityRoutingContext {
	protocol = canonicalProtocol(protocol)
	b := &builder{
		doc:          Document{ParserVersion: ParserVersion, Protocol: protocol},
		ignoreImages: true,
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		b.issue(IssueEmptyContent, "$")
		return finishSecurityRoutingContext(b)
	}
	decoder := json.NewDecoder(bytes.NewReader(trimmed))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		b.issue(IssueInvalidJSON, "$")
		return finishSecurityRoutingContext(b)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		b.issue(IssueInvalidJSON, "$")
		return finishSecurityRoutingContext(b)
	}
	root, ok := value.(map[string]any)
	if !ok {
		b.issue(IssueInvalidRoot, "$")
		return finishSecurityRoutingContext(b)
	}
	if duplicatePath, duplicate := securityRoutingDuplicateJSONFieldPath(trimmed, protocol, root); duplicate {
		b.issue(IssueDuplicateField, duplicatePath)
	}

	switch protocol {
	case ProtocolOpenAIResponses:
		b.parseInstruction(root["instructions"], "system", "$.instructions")
		b.parseResponses(root, "$")
	case ProtocolOpenAIChat:
		b.parseChatSecurityInstructions(root, "$")
		b.parseChat(root, "$")
	default:
		b.issue(IssueUnsupportedProtocol, "$")
	}
	return finishSecurityRoutingContext(b)
}

func finishSecurityRoutingContext(b *builder) *SecurityRoutingContext {
	document := b.finish()
	result := &SecurityRoutingContext{
		Document:       document,
		AuditTextRunes: document.AuditTextRunes,
		Reliable: document.Complete &&
			(document.TextAuditClass == TextAuditAuditableText || document.TextAuditClass == TextAuditKnownNoText),
	}
	if !result.Reliable {
		result.UnreliableReason = SecurityRoutingReasonIncomplete
		if len(document.Issues) > 0 {
			result.UnreliableReason = document.Issues[0].Code
		}
	}
	return result
}

func (b *builder) parseChatSecurityInstructions(root map[string]any, path string) {
	rawMessages, exists := root["messages"]
	if !exists || rawMessages == nil {
		return
	}
	messages, ok := rawMessages.([]any)
	if !ok {
		b.issue(IssueInvalidShape, childPath(path, "messages"))
		return
	}
	for index, rawMessage := range messages {
		messagePath := indexPath(childPath(path, "messages"), index)
		message, ok := rawMessage.(map[string]any)
		if !ok {
			b.issue(IssueInvalidShape, messagePath)
			continue
		}
		role, valid := b.requiredStringField(message, "role", messagePath)
		if !valid {
			continue
		}
		role = strings.ToLower(strings.TrimSpace(role))
		if !knownOpenAIChatRole(role) {
			b.issue(IssueUnknownRole, childPath(messagePath, "role"))
			continue
		}
		if role != "system" && role != "developer" {
			continue
		}

		b.rejectUnknownFields(message, messagePath,
			"role", "content", "name", "audio", "refusal", "tool_calls", "function_call", "tool_call_id")
		if _, valid := b.optionalStringField(message, "name", messagePath); !valid {
			continue
		}
		if content, exists := message["content"]; exists && content != nil {
			b.parseContentParts(content, role, childPath(messagePath, "content"), contentFlavorChat)
		}
		if refusal, exists := message["refusal"]; exists && refusal != nil {
			text, valid := b.optionalStringField(message, "refusal", messagePath)
			if valid {
				b.addText(text, role, "refusal", childPath(messagePath, "refusal"))
			}
		}
		if message["audio"] != nil {
			b.issue(IssueUnsupportedMedia, childPath(messagePath, "audio"))
		}
		// Tool calls on system/developer messages are not a supported instruction
		// surface. Treat them as uninspectable rather than silently routing on a
		// partial view of model-visible text.
		for _, field := range []string{"function_call", "tool_calls"} {
			if hasNonEmptyValue(message[field]) {
				b.issue(IssueInvalidShape, childPath(messagePath, field))
			}
		}
		if _, valid := b.optionalStringField(message, "tool_call_id", messagePath); !valid {
			continue
		}
	}
}

func securityRoutingDuplicateJSONFieldPath(raw []byte, protocol string, root map[string]any) (string, bool) {
	checkedPaths := map[string]struct{}{"$": {}}
	selectionFields := make(map[string]map[string]struct{})
	switch protocol {
	case ProtocolOpenAIResponses:
		addResponsesTextAuditDuplicatePaths(checkedPaths, selectionFields, root["input"], "$.input")
		addOpenAITextContentDuplicatePaths(checkedPaths, selectionFields, root["instructions"], "$.instructions")
	case ProtocolOpenAIChat:
		addChatTextAuditDuplicatePaths(checkedPaths, selectionFields, root["messages"], "$.messages")
		addChatSecurityInstructionDuplicatePaths(checkedPaths, selectionFields, root["messages"], "$.messages")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	path, duplicate, err := scanJSONValueForDuplicateFieldsAtPaths(decoder, "$", checkedPaths, selectionFields)
	return path, duplicate && err == nil
}

func addChatSecurityInstructionDuplicatePaths(
	checkedPaths map[string]struct{},
	selectionFields map[string]map[string]struct{},
	value any,
	path string,
) {
	messages, ok := value.([]any)
	if !ok {
		return
	}
	for index, rawMessage := range messages {
		message, ok := rawMessage.(map[string]any)
		if !ok {
			continue
		}
		messagePath := indexPath(path, index)
		addTextAuditSelectionFields(selectionFields, messagePath, "role")
		role, ok := message["role"].(string)
		if !ok {
			continue
		}
		role = strings.ToLower(strings.TrimSpace(role))
		if role != "system" && role != "developer" {
			continue
		}
		checkedPaths[messagePath] = struct{}{}
		addOpenAITextContentDuplicatePaths(
			checkedPaths,
			selectionFields,
			message["content"],
			childPath(messagePath, "content"),
		)
	}
}
