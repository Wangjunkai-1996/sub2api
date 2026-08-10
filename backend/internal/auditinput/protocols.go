package auditinput

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
)

func (b *builder) parseResponses(root map[string]any, path string) {
	b.rejectUnknownFields(root, path, responseRootFields...)
	frameType, frameTypeValid := b.optionalStringField(root, "type", path)
	if !frameTypeValid {
		return
	}
	if frameType = strings.ToLower(frameType); frameType != "" {
		if frameType != "response.create" {
			b.issue(IssueUnknownType, childPath(path, "type"))
			return
		}
	}
	// Client response.create events use the same flat request fields that are
	// forwarded upstream. Accepting a nested response object here would let the
	// audit parser inspect one payload while the transport forwards another.
	if _, exists := root["response"]; exists {
		b.issue(IssueInvalidShape, childPath(path, "response"))
		return
	}
	if hasNonEmptyValue(root["conversation"]) {
		b.issue(IssueRemoteFile, childPath(path, "conversation"))
	}
	if hasNonEmptyValue(root["prompt"]) {
		b.issue(IssueRemoteFile, childPath(path, "prompt"))
	}
	previousResponseID, previousResponseIDValid := b.optionalStringField(root, "previous_response_id", path)
	if !previousResponseIDValid {
		return
	}
	b.doc.PreviousResponseID = previousResponseID
	b.parseInstruction(root["instructions"], "system", childPath(path, "instructions"))
	b.parseToolDefinitions(root["tools"], childPath(path, "tools"))
	b.addJSON(root["text"], "system", "text_config", childPath(path, "text"))
	input, exists := root["input"]
	if !exists || input == nil {
		return
	}
	b.parseResponsesInput(input, childPath(path, "input"))
	if b.requiresLineage && strings.TrimSpace(b.doc.PreviousResponseID) == "" {
		b.issue(IssueLineageRequired, childPath(path, "previous_response_id"))
	}
}

func (b *builder) parseResponsesInput(value any, path string) {
	switch typed := value.(type) {
	case string:
		b.addText(typed, "user", "input_text", path)
	case []any:
		for index, item := range typed {
			itemPath := indexPath(path, index)
			switch entry := item.(type) {
			case string:
				b.addText(entry, "user", "input_text", itemPath)
			case map[string]any:
				b.parseResponsesItem(entry, itemPath)
			default:
				b.issue(IssueInvalidShape, itemPath)
			}
		}
	case map[string]any:
		b.parseResponsesItem(typed, path)
	default:
		b.issue(IssueInvalidShape, path)
	}
}

func (b *builder) parseResponsesItem(item map[string]any, path string) {
	typeName, typeValid := b.optionalStringField(item, "type", path)
	role, roleValid := b.optionalStringField(item, "role", path)
	name, nameValid := b.optionalStringField(item, "name", path)
	if !typeValid || !roleValid || !nameValid {
		return
	}
	for _, field := range []string{"id", "status"} {
		if _, valid := b.optionalStringField(item, field, path); !valid {
			return
		}
	}
	typeName = strings.ToLower(typeName)
	role = strings.ToLower(role)
	if role != "" && !knownResponsesRole(role) {
		b.issue(IssueUnknownRole, childPath(path, "role"))
		return
	}
	if b.containsOpaqueEncryptedContent(item, path) {
		b.issue(IssueEncryptedContent, path)
		return
	}
	switch typeName {
	case "message", "":
		b.rejectUnknownResponsesItemFields(item, path, "content", "text", "name", "refusal")
		if role == "" {
			b.issue(IssueUnknownRole, childPath(path, "role"))
			return
		}
		b.addText(name, role, "name", childPath(path, "name"))
		payloadCount := 0
		if content, exists := item["content"]; exists {
			payloadCount++
			if content == nil {
				b.issue(IssueInvalidShape, childPath(path, "content"))
			} else {
				b.parseContentParts(content, role, childPath(path, "content"), contentFlavorResponses)
			}
		}
		if _, exists := item["text"]; exists {
			payloadCount++
			text, valid := b.requiredStringField(item, "text", path)
			if valid {
				b.addText(text, role, "text", childPath(path, "text"))
			}
		}
		if _, exists := item["refusal"]; exists {
			payloadCount++
			refusal, valid := b.requiredStringField(item, "refusal", path)
			if valid {
				b.addText(refusal, role, "refusal", childPath(path, "refusal"))
			}
		}
		if payloadCount == 0 || payloadCount > 1 {
			b.issue(IssueInvalidShape, path)
		}
	case "input_text", "output_text", "text", "refusal":
		b.rejectUnknownResponsesItemFields(item, path, "text", "annotations", "logprobs")
		text, valid := b.requiredStringField(item, "text", path)
		if !valid {
			return
		}
		b.addText(text, role, typeName, childPath(path, "text"))
		b.addJSON(item["annotations"], role, "annotations", childPath(path, "annotations"))
		b.addJSON(item["logprobs"], role, "logprobs", childPath(path, "logprobs"))
	case "input_image", "image_url", "image":
		b.rejectUnknownResponsesItemFields(item, path, "image_url", "url", "source", "data", "base64", "mime_type", "media_type", "mimeType", "detail", "file_id", "fileId")
		b.parseImageObject(item, childPath(path, "image_url"))
	case "input_file", "file":
		b.rejectUnknownResponsesItemFields(item, path, "file_id", "fileId", "file_url", "fileUrl", "file_data", "filename", "name", "source", "data", "base64", "mime_type", "media_type", "mimeType")
		b.parseFileObject(item, path)
	case "function_call":
		b.rejectUnknownResponsesItemFields(item, path, "name", "arguments", "call_id")
		name, valid := b.requiredStringField(item, "name", path)
		if !valid {
			return
		}
		b.addText(name, role, typeName, childPath(path, "name"))
		arguments, exists := item["arguments"]
		if !exists || arguments == nil {
			b.issue(IssueInvalidShape, childPath(path, "arguments"))
		} else {
			b.addTextOrJSON(arguments, role, typeName, childPath(path, "arguments"))
		}
	case "function_call_output":
		b.rejectUnknownResponsesItemFields(item, path, "call_id", "output")
		b.requiresLineage = true
		b.parseRequiredResponsesToolPayloads(item, path, "output")
	case "custom_tool_call":
		b.rejectUnknownResponsesItemFields(item, path, "name", "input", "arguments", "call_id")
		b.addText(name, role, typeName, childPath(path, "name"))
		b.parseRequiredResponsesToolPayloads(item, path, "input", "arguments")
	case "custom_tool_call_output", "local_shell_call_output", "apply_patch_call_output", "tool_search_output":
		b.rejectUnknownResponsesItemFields(item, path, "call_id", "output", "content")
		b.requiresLineage = true
		b.parseRequiredResponsesToolPayloads(item, path, "output", "content")
	case "tool_call", "mcp_tool_call":
		b.rejectUnknownResponsesItemFields(item, path, "function", "name", "arguments", "args", "call_id", "server_label")
		b.parseFunctionCall(item, role, path)
	case "tool_call_output", "mcp_tool_call_output":
		b.rejectUnknownResponsesItemFields(item, path, "call_id", "output", "content", "server_label")
		b.requiresLineage = true
		b.parseRequiredResponsesToolPayloads(item, path, "output", "content")
	case "computer_call_output":
		b.rejectUnknownResponsesItemFields(item, path, "call_id", "output", "acknowledged_safety_checks")
		b.requiresLineage = true
		rawOutput, exists := item["output"]
		if !exists || rawOutput == nil {
			b.issue(IssueInvalidShape, childPath(path, "output"))
			return
		}
		output, ok := rawOutput.(map[string]any)
		if !ok {
			b.issue(IssueInvalidShape, childPath(path, "output"))
			return
		}
		outputType, valid := b.requiredStringField(output, "type", childPath(path, "output"))
		if !valid {
			return
		}
		if strings.ToLower(outputType) != "computer_screenshot" {
			b.issue(IssueUnknownType, childPath(path, "output.type"))
			return
		}
		b.rejectUnknownFields(output, childPath(path, "output"), "type", "image_url", "url", "source", "data", "base64", "mime_type", "media_type", "mimeType", "detail", "file_id", "fileId")
		b.parseImageObject(output, childPath(path, "output"))
		b.addJSON(item["acknowledged_safety_checks"], "tool", "safety_checks", childPath(path, "acknowledged_safety_checks"))
	case "reasoning":
		b.rejectUnknownResponsesItemFields(item, path, "summary", "content", "encrypted_content")
		if summary, exists := item["summary"]; exists {
			b.parseContentParts(summary, "assistant", childPath(path, "summary"), contentFlavorResponses)
		}
		if content, exists := item["content"]; exists && content != nil {
			b.parseContentParts(content, "assistant", childPath(path, "content"), contentFlavorResponses)
		}
	case "item_reference":
		b.rejectUnknownResponsesItemFields(item, path)
		b.issue(IssueRemoteFile, path)
	case "computer_call", "web_search_call", "file_search_call", "image_generation_call", "code_interpreter_call",
		"local_shell_call", "apply_patch_call", "mcp_call", "mcp_list_tools", "mcp_approval_request",
		"mcp_approval_response", "tool_search_call":
		b.rejectUnknownResponsesItemFields(item, path,
			"name", "arguments", "input", "call_id", "output", "content", "summary", "action", "pending_safety_checks",
			"acknowledged_safety_checks", "results", "queries", "server_label", "error", "tools", "approval_request_id",
			"approve", "reason", "command", "timeout_ms", "max_output_chars", "metadata", "function", "args")
		b.addJSON(stripOpaqueIdentifiers(item), role, typeName, path)
	default:
		b.issue(IssueUnknownType, childPath(path, "type"))
	}
}

func (b *builder) rejectUnknownResponsesItemFields(item map[string]any, path string, fields ...string) {
	allowed := make([]string, 0, len(fields)+4)
	allowed = append(allowed, "type", "id", "status", "role")
	allowed = append(allowed, fields...)
	b.rejectUnknownFields(item, path, allowed...)
}

func (b *builder) parseChat(root map[string]any, path string) {
	b.rejectUnknownFields(root, path, chatRootFields...)
	b.parseToolDefinitions(root["tools"], childPath(path, "tools"))
	b.parseToolDefinitions(root["functions"], childPath(path, "functions"))
	b.addJSON(root["prediction"], "system", "prediction", childPath(path, "prediction"))
	if responseFormat, exists := root["response_format"]; exists {
		b.addJSON(responseFormat, "system", "response_format", childPath(path, "response_format"))
	}
	messages, ok := root["messages"].([]any)
	if !ok {
		b.issue(IssueInvalidShape, childPath(path, "messages"))
		return
	}
	for index, raw := range messages {
		messagePath := indexPath(childPath(path, "messages"), index)
		message, ok := raw.(map[string]any)
		if !ok {
			b.issue(IssueInvalidShape, messagePath)
			continue
		}
		b.rejectUnknownFields(message, messagePath, "role", "content", "name", "audio", "refusal", "tool_calls", "function_call", "tool_call_id")
		role, roleValid := b.optionalStringField(message, "role", messagePath)
		name, nameValid := b.optionalStringField(message, "name", messagePath)
		if !roleValid || !nameValid {
			continue
		}
		role = strings.ToLower(role)
		if !knownOpenAIChatRole(role) {
			b.issue(IssueUnknownRole, childPath(messagePath, "role"))
			continue
		}
		b.addText(name, role, "name", childPath(messagePath, "name"))
		if content, exists := message["content"]; exists && content != nil {
			b.parseContentParts(content, role, childPath(messagePath, "content"), contentFlavorChat)
		}
		if refusalValue, exists := message["refusal"]; exists && refusalValue != nil {
			refusal, valid := b.optionalStringField(message, "refusal", messagePath)
			if valid {
				b.addText(refusal, role, "refusal", childPath(messagePath, "refusal"))
			}
		}
		if message["audio"] != nil {
			b.issue(IssueUnsupportedMedia, childPath(messagePath, "audio"))
		}
		if call, exists := message["function_call"]; exists {
			b.parseFunctionCall(call, role, childPath(messagePath, "function_call"))
		}
		if calls, exists := message["tool_calls"]; exists {
			array, ok := calls.([]any)
			if !ok {
				b.issue(IssueInvalidShape, childPath(messagePath, "tool_calls"))
			} else {
				for callIndex, call := range array {
					b.parseFunctionCall(call, role, indexPath(childPath(messagePath, "tool_calls"), callIndex))
				}
			}
		}
	}
}

func (b *builder) parseAnthropic(root map[string]any, path string) {
	b.rejectUnknownFields(root, path, anthropicRootFields...)
	if hasNonEmptyValue(root["container"]) {
		b.issue(IssueRemoteFile, childPath(path, "container"))
	}
	if hasNonEmptyValue(root["mcp_servers"]) {
		b.issue(IssueRemoteFile, childPath(path, "mcp_servers"))
	}
	b.parseInstruction(root["system"], "system", childPath(path, "system"))
	b.parseToolDefinitions(root["tools"], childPath(path, "tools"))
	b.addJSON(root["output_config"], "system", "output_config", childPath(path, "output_config"))
	messages, ok := root["messages"].([]any)
	if !ok {
		b.issue(IssueInvalidShape, childPath(path, "messages"))
		return
	}
	for index, raw := range messages {
		messagePath := indexPath(childPath(path, "messages"), index)
		message, ok := raw.(map[string]any)
		if !ok {
			b.issue(IssueInvalidShape, messagePath)
			continue
		}
		b.rejectUnknownFields(message, messagePath, "role", "content")
		role, roleValid := b.optionalStringField(message, "role", messagePath)
		if !roleValid {
			continue
		}
		role = strings.ToLower(role)
		if role != "user" && role != "assistant" {
			b.issue(IssueUnknownRole, childPath(messagePath, "role"))
			continue
		}
		b.parseContentParts(message["content"], role, childPath(messagePath, "content"), contentFlavorAnthropic)
	}
}

func (b *builder) parseGemini(root map[string]any, path string) {
	b.rejectUnknownFields(root, path, geminiRootFields...)
	if cached, field, exists, valid := b.singleAliasValue(root, path, "cachedContent", "cached_content"); valid && exists && hasNonEmptyValue(cached) {
		b.issue(IssueRemoteFile, childPath(path, field))
	}
	if instruction, field, exists, valid := b.singleAliasValue(root, path, "systemInstruction", "system_instruction"); valid && exists {
		b.parseGeminiContent(instruction, "system", childPath(path, field))
	}
	b.parseToolDefinitions(root["tools"], childPath(path, "tools"))
	if generation, field, exists, valid := b.singleAliasValue(root, path, "generationConfig", "generation_config"); valid && exists {
		b.addJSON(generation, "system", "generation_config", childPath(path, field))
	}
	if value, field, exists, valid := b.singleAliasValue(root, path, "contents", "content"); valid && exists {
		switch typed := value.(type) {
		case []any:
			for index, content := range typed {
				b.parseGeminiContent(content, "", indexPath(childPath(path, field), index))
			}
		case map[string]any:
			b.parseGeminiContent(typed, "", childPath(path, field))
		default:
			b.issue(IssueInvalidShape, childPath(path, field))
		}
	}
	if instances, exists := root["instances"]; exists {
		b.parseGenericPromptContainer(instances, childPath(path, "instances"))
	}
	if requests, exists := root["requests"]; exists {
		array, ok := requests.([]any)
		if !ok {
			b.issue(IssueInvalidShape, childPath(path, "requests"))
			return
		}
		for index, request := range array {
			requestRoot, ok := request.(map[string]any)
			if !ok {
				b.issue(IssueInvalidShape, indexPath(childPath(path, "requests"), index))
				continue
			}
			b.parseGemini(requestRoot, indexPath(childPath(path, "requests"), index))
		}
	}
}

var responseRootFields = []string{
	"type", "response", "event_id", "background", "conversation", "include", "input", "instructions",
	"max_output_tokens", "max_tool_calls", "metadata", "model", "parallel_tool_calls", "previous_response_id",
	"prompt", "prompt_cache_key", "reasoning", "safety_identifier", "service_tier", "store", "stream",
	"stream_options", "temperature", "text", "tool_choice", "tools", "top_logprobs", "top_p", "truncation",
	"user", "generate", "client_metadata",
}

var chatRootFields = []string{
	"audio", "frequency_penalty", "function_call", "functions", "logit_bias", "logprobs", "max_completion_tokens",
	"max_tokens", "messages", "modalities", "model", "n", "parallel_tool_calls", "prediction", "presence_penalty",
	"prompt_cache_key", "reasoning", "reasoning_effort", "response_format", "safety_identifier", "seed", "service_tier",
	"stop", "store", "stream", "stream_options", "temperature", "thinking", "tool_choice", "tools", "top_logprobs",
	"top_p", "user", "verbosity", "web_search_options",
}

var anthropicRootFields = []string{
	"container", "context_management", "max_tokens", "mcp_servers", "messages", "metadata", "model", "output_config",
	"service_tier", "stop_sequences", "stream", "system", "temperature", "thinking", "tool_choice", "tools", "top_k", "top_p",
}

var geminiRootFields = []string{
	"cachedContent", "cached_content", "contents", "content", "generationConfig", "generation_config", "instances", "labels",
	"model", "requests", "safetySettings", "safety_settings", "systemInstruction", "system_instruction", "toolConfig", "tool_config", "tools",
}

func (b *builder) parseGeminiContent(value any, forcedRole, path string) {
	content, ok := value.(map[string]any)
	if !ok {
		if text, ok := value.(string); ok {
			b.addText(text, forcedRole, "text", path)
			return
		}
		b.issue(IssueInvalidShape, path)
		return
	}
	b.rejectUnknownFields(content, path, "role", "parts", "text")
	role, roleValid := b.optionalStringField(content, "role", path)
	if !roleValid {
		return
	}
	role = strings.ToLower(role)
	if forcedRole != "" {
		role = forcedRole
	}
	if role != "" && role != "user" && role != "model" && role != "system" {
		b.issue(IssueUnknownRole, childPath(path, "role"))
		return
	}
	contentText, contentTextValid := "", true
	_, contentTextExists := content["text"]
	if contentTextExists {
		contentText, contentTextValid = b.requiredStringField(content, "text", path)
	}
	rawParts, partsExist := content["parts"]
	if contentTextExists && partsExist {
		b.issue(IssueInvalidShape, path)
	}
	if contentTextExists && contentTextValid {
		b.addText(contentText, role, "text", childPath(path, "text"))
	}
	if !partsExist {
		if !contentTextExists {
			b.issue(IssueInvalidShape, childPath(path, "parts"))
		}
		return
	}
	parts, ok := rawParts.([]any)
	if !ok {
		b.issue(IssueInvalidShape, childPath(path, "parts"))
		return
	}
	for index, raw := range parts {
		partPath := indexPath(childPath(path, "parts"), index)
		part, ok := raw.(map[string]any)
		if !ok {
			b.issue(IssueInvalidShape, partPath)
			continue
		}
		b.rejectUnknownFields(part, partPath, "text", "thought", "functionCall", "function_call", "functionResponse", "function_response",
			"inlineData", "inline_data", "fileData", "file_data", "executableCode", "executable_code", "codeExecutionResult", "code_execution_result",
			"thoughtSignature", "thought_signature", "videoMetadata", "video_metadata")
		if thought, exists := part["thought"]; exists && thought != nil {
			if _, valid := thought.(bool); !valid {
				b.issue(IssueInvalidShape, childPath(partPath, "thought"))
			}
		}

		signatureFields := make([]string, 0, 2)
		for _, field := range []string{"thoughtSignature", "thought_signature"} {
			if _, exists := part[field]; !exists {
				continue
			}
			signatureFields = append(signatureFields, field)
			signature, valid := b.requiredStringField(part, field, partPath)
			if valid && signature != "" {
				b.issue(IssueEncryptedContent, childPath(partPath, field))
			}
		}
		if len(signatureFields) > 1 {
			b.issue(IssueInvalidShape, partPath)
		}

		payloadFields := make([]string, 0, 2)
		for _, field := range []string{
			"text", "functionCall", "function_call", "functionResponse", "function_response",
			"inlineData", "inline_data", "fileData", "file_data", "executableCode", "executable_code",
			"codeExecutionResult", "code_execution_result",
		} {
			if _, exists := part[field]; exists {
				payloadFields = append(payloadFields, field)
			}
		}
		if len(payloadFields) == 0 {
			b.issue(IssueUnknownType, partPath)
			continue
		}
		if len(payloadFields) > 1 {
			b.issue(IssueInvalidShape, partPath)
		}
		for _, field := range payloadFields {
			value := part[field]
			fieldPath := childPath(partPath, field)
			switch field {
			case "text":
				text, valid := b.requiredStringField(part, field, partPath)
				if valid {
					b.addText(text, role, "text", fieldPath)
				}
			case "functionCall", "function_call":
				b.parseFunctionCall(value, role, fieldPath)
			case "functionResponse", "function_response":
				if _, valid := value.(map[string]any); !valid {
					b.issue(IssueInvalidShape, fieldPath)
					continue
				}
				b.addJSON(value, "tool", "function_response", fieldPath)
			case "inlineData", "inline_data":
				b.parseGeminiInlineData(value, fieldPath)
			case "fileData", "file_data":
				if _, valid := value.(map[string]any); !valid {
					b.issue(IssueInvalidShape, fieldPath)
					continue
				}
				b.issue(IssueRemoteFile, fieldPath)
			case "executableCode", "executable_code", "codeExecutionResult", "code_execution_result":
				if _, valid := value.(map[string]any); !valid {
					b.issue(IssueInvalidShape, fieldPath)
					continue
				}
				b.addJSON(value, role, "code", fieldPath)
			}
		}
	}
}

func (b *builder) parseMediaRequest(root map[string]any, path string) {
	if b.doc.Protocol != ProtocolOpenAIImages {
		b.walkMediaRequest(root, path, "")
		return
	}
	b.rejectUnknownFields(root, path, "prompt", "image", "images")
	if prompt, exists := root["prompt"]; exists {
		text, ok := prompt.(string)
		if !ok {
			b.issue(IssueInvalidShape, childPath(path, "prompt"))
		} else {
			b.addText(text, "user", "prompt", childPath(path, "prompt"))
		}
	}
	if image, exists := root["image"]; exists {
		b.parseImageValue(image, childPath(path, "image"))
	}
	if images, exists := root["images"]; exists {
		array, ok := images.([]any)
		if !ok {
			b.issue(IssueInvalidShape, childPath(path, "images"))
			return
		}
		for index, image := range array {
			imagePath := indexPath(childPath(path, "images"), index)
			object, ok := image.(map[string]any)
			if !ok {
				b.issue(IssueInvalidShape, imagePath)
				continue
			}
			b.rejectUnknownFields(object, imagePath, "image_url")
			b.parseImageObject(object, imagePath)
		}
	}
}

func (b *builder) walkMediaRequest(value any, path, key string) {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for child := range typed {
			keys = append(keys, child)
		}
		sort.Strings(keys)
		for _, child := range keys {
			childValue, childValuePath := typed[child], childPath(path, child)
			if isImageField(child) {
				b.parseImageValue(childValue, childValuePath)
				// Media request image containers can also carry model-visible
				// descriptions. Extract those prompts without parsing the same image
				// payload a second time.
				b.walkMediaPromptFields(childValue, childValuePath, child)
				continue
			}
			b.walkMediaRequest(childValue, childValuePath, child)
		}
	case []any:
		for index, item := range typed {
			b.walkMediaRequest(item, indexPath(path, index), key)
		}
	case string:
		if isPromptField(key) && !looksLikeMediaPayload(typed) {
			b.addText(typed, "user", "prompt", path)
		}
	}
}

func (b *builder) walkMediaPromptFields(value any, path, key string) {
	switch typed := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for child := range typed {
			keys = append(keys, child)
		}
		sort.Strings(keys)
		for _, child := range keys {
			if isImageField(child) {
				continue
			}
			b.walkMediaPromptFields(typed[child], childPath(path, child), child)
		}
	case []any:
		for index, item := range typed {
			b.walkMediaPromptFields(item, indexPath(path, index), key)
		}
	case string:
		if isPromptField(key) && !looksLikeMediaPayload(typed) {
			b.addText(typed, "user", "prompt", path)
		}
	}
}

func (b *builder) parseEmbedding(root map[string]any, path string) {
	b.rejectUnknownFields(root, path, "input", "model", "encoding_format", "dimensions", "user")
	value, exists := root["input"]
	if !exists {
		return
	}
	switch typed := value.(type) {
	case string:
		b.addText(typed, "user", "input", childPath(path, "input"))
	case []any:
		for index, item := range typed {
			if text, ok := item.(string); ok {
				b.addText(text, "user", "input", indexPath(childPath(path, "input"), index))
			} else {
				b.issue(IssueInvalidShape, indexPath(childPath(path, "input"), index))
			}
		}
	default:
		b.issue(IssueInvalidShape, childPath(path, "input"))
	}
}

func (b *builder) parseSearch(root map[string]any, path string) {
	b.rejectUnknownFields(root, path, "id", "model", "reasoning", "input", "commands", "settings", "max_output_tokens")
	for _, field := range []string{"id", "model"} {
		if _, valid := b.optionalStringField(root, field, path); !valid {
			return
		}
	}
	b.addJSON(root["reasoning"], "system", "reasoning", childPath(path, "reasoning"))
	for _, field := range []string{"commands", "settings"} {
		value, exists := root[field]
		if !exists {
			continue
		}
		if _, ok := value.(map[string]any); !ok {
			b.issue(IssueInvalidShape, childPath(path, field))
			continue
		}
		b.addJSON(value, "user", field, childPath(path, field))
	}
	if input, exists := root["input"]; exists {
		b.parseResponsesInput(input, childPath(path, "input"))
	}
}

func (b *builder) parseGenericPromptContainer(value any, path string) {
	switch typed := value.(type) {
	case string:
		b.addText(typed, "user", "prompt", path)
	case []any:
		for index, item := range typed {
			b.parseGenericPromptContainer(item, indexPath(path, index))
		}
	case map[string]any:
		fields := []string{"prompt", "text", "input", "content", "query"}
		b.rejectUnknownFields(typed, path, fields...)
		child, field, exists, valid := b.singleAliasValue(typed, path, fields...)
		if !valid {
			return
		}
		if !exists {
			b.issue(IssueInvalidShape, path)
			return
		}
		b.parseGenericPromptContainer(child, childPath(path, field))
	default:
		b.issue(IssueInvalidShape, path)
	}
}

type contentFlavor int

const (
	contentFlavorResponses contentFlavor = iota
	contentFlavorChat
	contentFlavorAnthropic
)

func (b *builder) parseContentParts(value any, role, path string, flavor contentFlavor) {
	switch typed := value.(type) {
	case string:
		b.addText(typed, role, "text", path)
	case []any:
		for index, raw := range typed {
			partPath := indexPath(path, index)
			part, ok := raw.(map[string]any)
			if !ok {
				b.issue(IssueInvalidShape, partPath)
				continue
			}
			b.parseContentPart(part, role, partPath, flavor)
		}
	case map[string]any:
		b.parseContentPart(typed, role, path, flavor)
	case nil:
		return
	default:
		b.issue(IssueInvalidShape, path)
	}
}

func (b *builder) parseContentPart(part map[string]any, role, path string, flavor contentFlavor) {
	typeName, typeValid := b.optionalStringField(part, "type", path)
	if !typeValid {
		return
	}
	typeName = strings.ToLower(typeName)
	if b.containsOpaqueEncryptedContent(part, path) {
		b.issue(IssueEncryptedContent, path)
		return
	}
	switch typeName {
	case "":
		b.rejectUnknownContentPartFields(part, path, "text", "content")
		payloadCount := 0
		if rawText, exists := part["text"]; exists && rawText != nil {
			payloadCount++
			text, valid := b.optionalStringField(part, "text", path)
			if valid && text != "" {
				b.addText(text, role, typeName, childPath(path, "text"))
			}
		}
		if content, exists := part["content"]; exists && content != nil {
			payloadCount++
			b.parseContentParts(content, role, childPath(path, "content"), flavor)
		}
		if payloadCount == 0 || payloadCount > 1 {
			b.issue(IssueInvalidShape, path)
		}
	case "text", "input_text", "output_text", "refusal", "summary_text", "reasoning_text":
		b.rejectUnknownContentPartFields(part, path, "text", "annotations", "logprobs")
		if typeName != "" {
			text, valid := b.requiredStringField(part, "text", path)
			if !valid {
				return
			}
			b.addText(text, role, typeName, childPath(path, "text"))
		}
		b.addJSON(part["annotations"], role, "annotations", childPath(path, "annotations"))
		b.addJSON(part["logprobs"], role, "logprobs", childPath(path, "logprobs"))
	case "image", "input_image", "image_url", "computer_screenshot":
		b.rejectUnknownContentPartFields(part, path, "image_url", "url", "source", "data", "base64", "mime_type", "media_type", "mimeType", "detail", "file_id", "fileId")
		b.parseImageObject(part, path)
	case "input_file", "file", "document":
		b.rejectUnknownContentPartFields(part, path, "file_id", "fileId", "file_url", "fileUrl", "file_data", "filename", "name", "source", "data", "base64", "mime_type", "media_type", "mimeType")
		b.parseFileObject(part, path)
	case "input_audio", "audio", "video", "input_video":
		b.rejectUnknownContentPartFields(part, path, "input_audio", "audio", "data", "base64", "mime_type", "media_type", "mimeType", "format", "url", "source")
		b.issue(IssueUnsupportedMedia, path)
	case "tool_use", "server_tool_use":
		b.rejectUnknownContentPartFields(part, path, "name", "input")
		name, valid := b.requiredStringField(part, "name", path)
		if !valid {
			return
		}
		b.addText(name, role, typeName, childPath(path, "name"))
		input, exists := part["input"]
		if !exists || input == nil {
			b.issue(IssueInvalidShape, childPath(path, "input"))
			return
		}
		if _, valid := input.(map[string]any); !valid {
			b.issue(IssueInvalidShape, childPath(path, "input"))
			return
		}
		b.addJSON(input, role, typeName, childPath(path, "input"))
	case "tool_result":
		b.rejectUnknownContentPartFields(part, path, "tool_use_id", "content", "is_error")
		if isError, exists := part["is_error"]; exists && isError != nil {
			if _, valid := isError.(bool); !valid {
				b.issue(IssueInvalidShape, childPath(path, "is_error"))
			}
		}
		content, exists := part["content"]
		if !exists || content == nil {
			b.issue(IssueInvalidShape, childPath(path, "content"))
			return
		}
		b.parseContentParts(content, "tool", childPath(path, "content"), flavor)
	case "thinking":
		b.rejectUnknownContentPartFields(part, path, "thinking", "signature")
		thinking, valid := b.requiredStringField(part, "thinking", path)
		if !valid {
			return
		}
		b.addText(thinking, role, typeName, childPath(path, "thinking"))
	case "redacted_thinking":
		b.rejectUnknownContentPartFields(part, path, "data", "encrypted_content")
		b.issue(IssueEncryptedContent, path)
	case "web_search_tool_result", "code_execution_tool_result", "bash_code_execution_tool_result", "text_editor_code_execution_tool_result":
		b.rejectUnknownContentPartFields(part, path, "content", "tool_use_id", "is_error")
		if isError, exists := part["is_error"]; exists && isError != nil {
			if _, valid := isError.(bool); !valid {
				b.issue(IssueInvalidShape, childPath(path, "is_error"))
			}
		}
		content, exists := part["content"]
		if !exists || content == nil {
			b.issue(IssueInvalidShape, childPath(path, "content"))
			return
		}
		switch content.(type) {
		case string, []any, map[string]any:
			b.parseServerToolResult(content, childPath(path, "content"))
		default:
			b.issue(IssueInvalidShape, childPath(path, "content"))
		}
	default:
		b.issue(IssueUnknownType, childPath(path, "type"))
	}
}

func (b *builder) parseServerToolResult(value any, path string) {
	switch typed := value.(type) {
	case nil:
		b.issue(IssueInvalidShape, path)
	case string:
		b.addText(typed, "tool", "server_tool_result", path)
	case json.Number, float64, bool:
		b.addJSON(typed, "tool", "server_tool_result", path)
	case []any:
		for index, item := range typed {
			b.parseServerToolResult(item, indexPath(path, index))
		}
	case map[string]any:
		if b.containsOpaqueEncryptedContent(typed, path) {
			b.issue(IssueEncryptedContent, path)
		}
		for _, field := range []string{"file_id", "fileId", "file_uri", "fileUri"} {
			if raw, exists := typed[field]; exists && raw != nil {
				if _, ok := raw.(string); !ok {
					b.issue(IssueInvalidShape, childPath(path, field))
				} else if hasNonEmptyValue(raw) {
					b.issue(IssueRemoteFile, childPath(path, field))
				}
			}
		}

		typeName, typeValid := b.optionalStringField(typed, "type", path)
		if !typeValid {
			return
		}
		typeName = strings.ToLower(typeName)
		if _, typeExists := typed["type"]; typeExists && typeName == "" {
			b.issue(IssueInvalidShape, childPath(path, "type"))
		}
		switch typeName {
		case "text", "input_text", "output_text", "refusal", "image", "input_image", "image_url", "computer_screenshot", "input_file", "file", "document":
			b.parseContentPart(typed, "tool", path, contentFlavorAnthropic)
			return
		case "web_search_result", "web_search_tool_result_error", "code_execution_result", "bash_code_execution_result", "text_editor_code_execution_result", "":
			// Known structured results and untyped JSON are traversed below so
			// every nested value remains part of the audited document.
		case "url", "file_id":
			b.issue(IssueRemoteFile, path)
		default:
			b.issue(IssueUnknownType, childPath(path, "type"))
		}

		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			switch key {
			case "encrypted_content", "encryptedContent", "signature", "file_id", "fileId", "file_uri", "fileUri":
				continue
			}
			fieldPath := childPath(path, key)
			b.addText(key, "tool", "server_tool_result_field", fieldPath)
			b.parseServerToolResult(typed[key], fieldPath)
		}
	default:
		b.issue(IssueInvalidShape, path)
	}
}

func (b *builder) rejectUnknownContentPartFields(part map[string]any, path string, fields ...string) {
	allowed := make([]string, 0, len(fields)+3)
	allowed = append(allowed, "type", "id", "cache_control")
	allowed = append(allowed, fields...)
	b.rejectUnknownFields(part, path, allowed...)
}

func (b *builder) parseInstruction(value any, role, path string) {
	switch typed := value.(type) {
	case nil:
		return
	case string:
		b.addText(typed, role, "instructions", path)
	case []any, map[string]any:
		b.parseContentParts(typed, role, path, contentFlavorResponses)
	default:
		b.issue(IssueInvalidShape, path)
	}
}

func (b *builder) parseToolDefinitions(value any, path string) {
	if value == nil {
		return
	}
	array, ok := value.([]any)
	if !ok {
		b.issue(IssueInvalidShape, path)
		return
	}
	for index, tool := range array {
		toolPath := indexPath(path, index)
		if b.doc.Protocol == ProtocolOpenAIResponses {
			b.rejectRemoteResponsesToolDefinition(tool, toolPath)
		}
		b.addJSON(tool, "system", "tool_definition", toolPath)
	}
}

func (b *builder) rejectRemoteResponsesToolDefinition(value any, path string) {
	tool, ok := value.(map[string]any)
	if !ok {
		b.issue(IssueInvalidShape, path)
		return
	}
	typeName, valid := b.optionalStringField(tool, "type", path)
	if !valid {
		return
	}
	switch strings.ToLower(typeName) {
	case "mcp", "file_search":
		b.issue(IssueRemoteFile, path)
	}
	for _, field := range []string{"server_url", "serverUrl", "connector_id", "connectorId", "vector_store_ids", "vectorStoreIds", "file_ids", "fileIds"} {
		if hasNonEmptyValue(tool[field]) {
			b.issue(IssueRemoteFile, childPath(path, field))
		}
	}
}

func (b *builder) parseFunctionCall(value any, role, path string) {
	call, ok := value.(map[string]any)
	if !ok {
		b.issue(IssueInvalidShape, path)
		return
	}
	if function, ok := call["function"].(map[string]any); ok {
		b.rejectUnknownFields(call, path, "id", "type", "function", "index")
		call, path = function, childPath(path, "function")
		b.rejectUnknownFields(call, path, "name", "arguments", "args")
	} else {
		b.rejectUnknownFields(call, path, "id", "type", "name", "arguments", "args", "index", "call_id", "server_label")
	}
	name, nameValid := b.requiredStringField(call, "name", path)
	if !nameValid {
		return
	}
	b.addText(name, role, "function_call", childPath(path, "name"))
	arguments, argumentField, exists, valid := b.singleAliasValue(call, path, "arguments", "args")
	if !valid || !exists {
		return
	}
	if arguments == nil {
		b.issue(IssueInvalidShape, childPath(path, argumentField))
		return
	}
	b.addTextOrJSON(arguments, role, "function_call", childPath(path, argumentField))
}

func (b *builder) addTextOrJSON(value any, role, kind, path string) {
	b.addTextOrJSONDepth(value, role, kind, path, 0)
}

const maxStringifiedJSONDepth = 8

func (b *builder) addTextOrJSONDepth(value any, role, kind, path string, depth int) {
	if text, ok := value.(string); ok {
		if structured, duplicate, valid := decodeStringifiedJSON(text); valid {
			if duplicate {
				b.issue(IssueDuplicateField, path)
				b.addText(text, role, kind, path)
				return
			}
			if decodedText, ok := structured.(string); ok {
				if depth >= maxStringifiedJSONDepth {
					b.issue(IssueInvalidShape, path)
					b.addText(decodedText, role, kind, path)
					return
				}
				b.addTextOrJSONDepth(decodedText, role, kind, path, depth+1)
				return
			}
			b.addJSON(structured, role, kind, path)
			return
		}
		b.addText(text, role, kind, path)
		return
	}
	b.addJSON(value, role, kind, path)
}

func decodeStringifiedJSON(value string) (any, bool, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil, false, false
	}
	decoder := json.NewDecoder(strings.NewReader(trimmed))
	decoder.UseNumber()
	var structured any
	if err := decoder.Decode(&structured); err != nil {
		return nil, false, false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return nil, false, false
	}
	return structured, HasDuplicateJSONFields([]byte(trimmed)), true
}

func (b *builder) parseRequiredResponsesToolPayloads(item map[string]any, path string, fields ...string) {
	found := 0
	for _, field := range fields {
		payload, exists := item[field]
		if !exists {
			continue
		}
		found++
		fieldPath := childPath(path, field)
		if payload == nil {
			b.issue(IssueInvalidShape, fieldPath)
			continue
		}
		b.parseResponsesToolPayload(payload, fieldPath)
	}
	if found == 0 {
		b.issue(IssueInvalidShape, childPath(path, fields[0]))
	}
	if found > 1 {
		b.issue(IssueInvalidShape, path)
	}
}

func (b *builder) parseResponsesToolPayload(value any, path string) {
	b.parseResponsesToolPayloadDepth(value, path, 0)
}

func (b *builder) parseResponsesToolPayloadDepth(value any, path string, depth int) {
	switch typed := value.(type) {
	case string:
		trimmed := strings.TrimSpace(typed)
		if strings.HasPrefix(strings.ToLower(trimmed), "data:image/") {
			b.addImage(trimmed, "", path)
			return
		}
		if structured, duplicate, valid := decodeStringifiedJSON(trimmed); valid {
			if duplicate {
				b.issue(IssueDuplicateField, path)
				b.addText(typed, "tool", "tool_output", path)
				return
			}
			if decodedText, ok := structured.(string); ok {
				if depth >= maxStringifiedJSONDepth {
					b.issue(IssueInvalidShape, path)
					b.addText(decodedText, "tool", "tool_output", path)
					return
				}
				b.parseResponsesToolPayloadDepth(decodedText, path, depth+1)
				return
			}
			switch structured.(type) {
			case []any, map[string]any:
				b.parseResponsesToolPayloadDepth(structured, path, depth+1)
				return
			}
		}
		b.addText(typed, "tool", "tool_output", path)
	case []any:
		for index, item := range typed {
			b.parseResponsesToolPayloadDepth(item, indexPath(path, index), depth)
		}
	case map[string]any:
		if typeName, ok := typed["type"].(string); ok && knownToolOutputContentType(typeName) {
			b.parseContentPart(typed, "tool", path, contentFlavorResponses)
			return
		}
		if content, exists := typed["content"]; exists {
			if content == nil {
				b.issue(IssueInvalidShape, childPath(path, "content"))
			} else {
				b.parseResponsesToolPayloadDepth(content, childPath(path, "content"), depth)
			}
			remainder := make(map[string]any, len(typed)-1)
			for key, child := range typed {
				if key != "content" {
					remainder[key] = child
				}
			}
			b.addJSON(remainder, "tool", "tool_output", path)
			return
		}
		b.addJSON(typed, "tool", "tool_output", path)
	default:
		b.issue(IssueInvalidShape, path)
	}
}

func knownToolOutputContentType(typeName string) bool {
	switch strings.ToLower(strings.TrimSpace(typeName)) {
	case "text", "input_text", "output_text", "refusal", "image", "input_image", "image_url", "computer_screenshot",
		"input_file", "file", "document", "input_audio", "audio", "video", "input_video":
		return true
	default:
		return false
	}
}

func (b *builder) parseImageObject(value map[string]any, path string) {
	mimeType, _, mimeExists, mimeValid := b.optionalStringAlias(value, path, "mime_type", "media_type", "mimeType")
	if !mimeValid {
		return
	}
	rawSource, sourceField, sourceExists, sourceValid := b.singleAliasValue(
		value, path, "image_url", "url", "source", "data", "base64", "file_id", "fileId",
	)
	if !sourceValid {
		return
	}
	if !sourceExists || rawSource == nil {
		b.issue(IssueInvalidMedia, path)
		return
	}
	sourcePath := childPath(path, sourceField)

	switch sourceField {
	case "image_url":
		if imageURL, ok := rawSource.(map[string]any); ok {
			b.rejectUnknownFields(imageURL, sourcePath, "url", "detail", "mime_type", "media_type")
			nestedMIME, _, nestedMIMEExists, nestedMIMEValid := b.optionalStringAlias(imageURL, sourcePath, "mime_type", "media_type")
			if !nestedMIMEValid {
				return
			}
			if mimeExists && nestedMIMEExists {
				b.issue(IssueInvalidShape, sourcePath)
				return
			}
			if nestedMIMEExists {
				mimeType = nestedMIME
			}
			image, valid := b.requiredStringField(imageURL, "url", sourcePath)
			if !valid {
				return
			}
			b.addImage(image, mimeType, childPath(sourcePath, "url"))
			return
		}
		image, valid := b.requiredStringField(value, sourceField, path)
		if !valid {
			return
		}
		b.addImage(image, mimeType, sourcePath)
	case "url", "data", "base64":
		image, valid := b.requiredStringField(value, sourceField, path)
		if !valid {
			return
		}
		b.addImage(image, mimeType, sourcePath)
	case "file_id", "fileId":
		if _, valid := b.requiredStringField(value, sourceField, path); valid {
			b.issue(IssueRemoteFile, sourcePath)
		}
	case "source":
		source, ok := rawSource.(map[string]any)
		if !ok {
			b.issue(IssueInvalidShape, sourcePath)
			return
		}
		b.rejectUnknownFields(source, sourcePath, "type", "data", "media_type", "mime_type", "url", "file_id", "fileId")
		sourceType, valid := b.requiredStringField(source, "type", sourcePath)
		if !valid {
			return
		}
		sourceMIME, _, sourceMIMEExists, sourceMIMEValid := b.optionalStringAlias(source, sourcePath, "media_type", "mime_type")
		if !sourceMIMEValid {
			return
		}
		if mimeExists && sourceMIMEExists {
			b.issue(IssueInvalidShape, sourcePath)
			return
		}
		if sourceMIMEExists {
			mimeType = sourceMIME
		}
		payload, payloadField, payloadExists, payloadValid := b.singleAliasValue(source, sourcePath, "data", "url", "file_id", "fileId")
		if !payloadValid {
			return
		}
		if !payloadExists || payload == nil {
			b.issue(IssueInvalidMedia, sourcePath)
			return
		}
		sourceType = strings.ToLower(sourceType)
		switch sourceType {
		case "base64":
			if payloadField != "data" {
				b.issue(IssueInvalidShape, sourcePath)
				return
			}
			image, valid := b.requiredStringField(source, payloadField, sourcePath)
			if valid {
				b.addImage(image, mimeType, childPath(sourcePath, payloadField))
			}
		case "url":
			if payloadField != "url" {
				b.issue(IssueInvalidShape, sourcePath)
				return
			}
			image, valid := b.requiredStringField(source, payloadField, sourcePath)
			if valid {
				b.addImage(image, mimeType, childPath(sourcePath, payloadField))
			}
		case "file", "file_id":
			b.issue(IssueRemoteFile, sourcePath)
		case "":
			b.issue(IssueInvalidShape, childPath(sourcePath, "type"))
		default:
			b.issue(IssueUnknownType, childPath(sourcePath, "type"))
		}
	}
}

func (b *builder) parseImageValue(value any, path string) {
	switch typed := value.(type) {
	case string:
		b.addImage(typed, "", path)
	case map[string]any:
		b.parseImageObject(typed, path)
	case []any:
		for index, item := range typed {
			b.parseImageValue(item, indexPath(path, index))
		}
	default:
		b.issue(IssueInvalidShape, path)
	}
}

func (b *builder) parseFileObject(value map[string]any, path string) {
	filename, _, _, filenameValid := b.optionalStringAlias(value, path, "filename", "name")
	if !filenameValid {
		return
	}
	filename = strings.ToLower(filename)
	mimeType, _, mimeExists, mimeValid := b.optionalStringAlias(value, path, "mime_type", "media_type", "mimeType")
	if !mimeValid {
		return
	}
	rawSource, sourceField, sourceExists, sourceValid := b.singleAliasValue(
		value, path, "file_id", "fileId", "file_url", "fileUrl", "file_data", "data", "base64", "source",
	)
	if !sourceValid {
		return
	}
	if !sourceExists || rawSource == nil {
		b.issue(IssueInvalidMedia, path)
		return
	}
	sourcePath := childPath(path, sourceField)

	switch sourceField {
	case "file_id", "fileId", "file_url", "fileUrl":
		if _, valid := b.requiredStringField(value, sourceField, path); valid {
			b.issue(IssueRemoteFile, sourcePath)
		}
		return
	case "file_data", "data", "base64":
		data, valid := b.requiredStringField(value, sourceField, path)
		if !valid {
			return
		}
		if mimeType == "" {
			mimeType = mimeFromFilename(filename)
		}
		b.addTextFile(data, mimeType, sourcePath)
		return
	case "source":
		source, ok := rawSource.(map[string]any)
		if !ok {
			b.issue(IssueInvalidShape, sourcePath)
			return
		}
		b.rejectUnknownFields(source, sourcePath, "type", "data", "media_type", "mime_type", "url", "file_id", "fileId")
		sourceType, typeValid := b.requiredStringField(source, "type", sourcePath)
		if !typeValid {
			return
		}
		sourceMIME, _, sourceMIMEExists, sourceMIMEValid := b.optionalStringAlias(source, sourcePath, "media_type", "mime_type")
		if !sourceMIMEValid {
			return
		}
		if mimeExists && sourceMIMEExists {
			b.issue(IssueInvalidShape, sourcePath)
			return
		}
		if sourceMIMEExists {
			mimeType = sourceMIME
		}
		payload, payloadField, payloadExists, payloadValid := b.singleAliasValue(source, sourcePath, "data", "url", "file_id", "fileId")
		if !payloadValid {
			return
		}
		sourceType = strings.ToLower(sourceType)
		switch sourceType {
		case "base64", "text":
			if !payloadExists || payload == nil || payloadField != "data" {
				b.issue(IssueInvalidShape, sourcePath)
				return
			}
			data, valid := b.requiredStringField(source, payloadField, sourcePath)
			if !valid {
				return
			}
			if mimeType == "" {
				if sourceType == "text" {
					mimeType = "text/plain"
				} else {
					mimeType = mimeFromFilename(filename)
				}
			}
			if sourceType == "text" {
				if !isTextMIME(mimeType) || data == "" {
					b.issue(IssueUnsupportedMedia, sourcePath)
					return
				}
				b.addText(data, "user", "text_file", childPath(sourcePath, payloadField))
				return
			}
			b.addTextFile(data, mimeType, childPath(sourcePath, payloadField))
		case "encrypted", "encrypted_content":
			b.issue(IssueEncryptedContent, sourcePath)
		case "url", "file", "file_id":
			b.issue(IssueRemoteFile, sourcePath)
		case "":
			b.issue(IssueInvalidShape, childPath(sourcePath, "type"))
		default:
			b.issue(IssueUnknownType, childPath(sourcePath, "type"))
		}
	}
}

func (b *builder) optionalStringField(value map[string]any, field, path string) (string, bool) {
	raw, exists := value[field]
	if !exists {
		return "", true
	}
	text, ok := raw.(string)
	if !ok {
		b.issue(IssueInvalidShape, childPath(path, field))
		return "", false
	}
	return strings.TrimSpace(text), true
}

func (b *builder) requiredStringField(value map[string]any, field, path string) (string, bool) {
	raw, exists := value[field]
	if !exists {
		b.issue(IssueInvalidShape, childPath(path, field))
		return "", false
	}
	text, ok := raw.(string)
	if !ok {
		b.issue(IssueInvalidShape, childPath(path, field))
		return "", false
	}
	return strings.TrimSpace(text), true
}

func (b *builder) parseGeminiInlineData(value any, path string) {
	data, ok := value.(map[string]any)
	if !ok {
		b.issue(IssueInvalidShape, path)
		return
	}
	b.rejectUnknownFields(data, path, "mimeType", "mime_type", "data")
	mimeType, _, _, mimeValid := b.optionalStringAlias(data, path, "mimeType", "mime_type")
	if !mimeValid {
		return
	}
	payload, payloadValid := b.requiredStringField(data, "data", path)
	if !payloadValid {
		return
	}
	if strings.HasPrefix(strings.ToLower(mimeType), "image/") {
		b.addImage(payload, mimeType, path)
		return
	}
	if isTextMIME(mimeType) {
		b.addTextFile(payload, mimeType, path)
		return
	}
	b.issue(IssueUnsupportedMedia, path)
}

func (b *builder) containsOpaqueEncryptedContent(value map[string]any, path string) bool {
	for _, key := range []string{"encrypted_content", "encryptedContent", "signature"} {
		raw, exists := value[key]
		if !exists || raw == nil {
			continue
		}
		text, ok := raw.(string)
		if !ok {
			b.issue(IssueInvalidShape, childPath(path, key))
			continue
		}
		if strings.TrimSpace(text) != "" {
			return true
		}
	}
	return false
}

func knownResponsesRole(role string) bool {
	switch role {
	case "user", "system", "developer", "assistant":
		return true
	default:
		return false
	}
}

func knownOpenAIChatRole(role string) bool {
	switch role {
	case "user", "system", "developer", "assistant", "tool", "function":
		return true
	default:
		return false
	}
}

// singleAliasValue resolves fields that are accepted only as spelling or
// compatibility aliases for one semantic value. Multiple aliases are
// ambiguous across upstream parsers and therefore cannot be audited safely.
func (b *builder) singleAliasValue(value map[string]any, path string, fields ...string) (any, string, bool, bool) {
	var selected any
	selectedField := ""
	found := 0
	for _, field := range fields {
		raw, exists := value[field]
		if !exists {
			continue
		}
		found++
		selected = raw
		selectedField = field
	}
	if found > 1 {
		b.issue(IssueInvalidShape, path)
		return nil, "", true, false
	}
	return selected, selectedField, found == 1, true
}

func (b *builder) optionalStringAlias(value map[string]any, path string, fields ...string) (string, string, bool, bool) {
	raw, field, exists, valid := b.singleAliasValue(value, path, fields...)
	if !valid {
		return "", "", exists, false
	}
	if !exists {
		return "", "", false, true
	}
	text, ok := raw.(string)
	if !ok {
		b.issue(IssueInvalidShape, childPath(path, field))
		return "", field, true, false
	}
	return strings.TrimSpace(text), field, true, true
}

func firstNonNil(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func hasNonEmptyValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(typed) != ""
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	default:
		return true
	}
}

func stripOpaqueIdentifiers(value map[string]any) map[string]any {
	result := make(map[string]any, len(value))
	for key, item := range value {
		switch key {
		case "id", "call_id", "status":
			continue
		default:
			result[key] = item
		}
	}
	return result
}

func mimeFromFilename(filename string) string {
	switch {
	case strings.HasSuffix(filename, ".txt"), strings.HasSuffix(filename, ".md"), strings.HasSuffix(filename, ".csv"):
		return "text/plain"
	case strings.HasSuffix(filename, ".json"):
		return "application/json"
	case strings.HasSuffix(filename, ".xml"):
		return "application/xml"
	case strings.HasSuffix(filename, ".yaml"), strings.HasSuffix(filename, ".yml"):
		return "application/yaml"
	default:
		return ""
	}
}

func isPromptField(key string) bool {
	normalized := strings.NewReplacer("_", "", "-", "").Replace(strings.ToLower(strings.TrimSpace(key)))
	switch normalized {
	case "prompt", "inputprompt", "textprompt", "description", "query", "lyrics", "negativeprompt", "positiveprompt", "imageprompt",
		"gptdescriptionprompt", "prompten", "finalprompt", "finalzhprompt", "origprompt", "actualprompt", "input":
		return true
	default:
		return false
	}
}

func isImageField(key string) bool {
	normalized := strings.NewReplacer("_", "", "-", "").Replace(strings.ToLower(strings.TrimSpace(key)))
	switch normalized {
	case "image", "images", "imageurl", "referenceimage", "referenceimages", "sourceimage", "sourceimages":
		return true
	default:
		return false
	}
}

func looksLikeMediaPayload(value string) bool {
	trimmed := strings.TrimSpace(value)
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "data:") || strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") {
		return true
	}
	if len(trimmed) < 256 {
		return false
	}
	for _, r := range trimmed {
		if !((r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '+' || r == '/' || r == '=') {
			return false
		}
	}
	return true
}

func (b *builder) rejectUnknownFields(value map[string]any, path string, fields ...string) {
	allowed := make(map[string]struct{}, len(fields))
	for _, field := range fields {
		allowed[field] = struct{}{}
	}
	for field := range value {
		if _, ok := allowed[field]; !ok {
			b.issue(IssueUnknownField, childPath(path, field))
		}
	}
}

func canonicalJSON(value any) string {
	raw, _ := json.Marshal(value)
	return fmt.Sprint(string(raw))
}
