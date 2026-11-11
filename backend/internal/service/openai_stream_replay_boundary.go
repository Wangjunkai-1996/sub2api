package service

import (
	"strings"

	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

// This records the existing output classifier's decision; it never decides
// whether replay is safe. No payload, delta, tool arguments or ciphertext is kept.
type openAIStreamReplayBoundary struct {
	eventType, itemType, classification, commitReason string
	stagedBytes                                       int64
}

func (b *openAIStreamReplayBoundary) observe(data, eventType string) {
	if b.eventType != "" {
		return
	}
	b.eventType = openAIStreamDiagnosticType(eventType)
	b.classification = "conservative_event"
	if strings.TrimSpace(data) == "[DONE]" {
		b.eventType, b.classification = "[DONE]", "terminal_event"
		return
	}
	if !gjson.Valid(data) {
		b.classification = "malformed_event"
		return
	}
	if item := gjson.Get(data, "item"); item.Exists() {
		b.itemType = openAIStreamDiagnosticType(item.Get("type").String())
		if b.itemType == "reasoning" && item.Get("encrypted_content").String() != "" {
			b.classification = "encrypted_reasoning"
			return
		}
		if b.itemType == "function_call" || b.itemType == "custom_tool_call" {
			b.classification = "tool_call"
			return
		}
	}
	switch {
	case eventType == "error" || openAIStreamEventTypeIsTerminal(eventType):
		b.classification = "terminal_event"
	case strings.HasPrefix(eventType, "response.function_call_arguments.") || strings.HasPrefix(eventType, "response.custom_tool_call_input."):
		b.classification = "tool_arguments"
	case strings.HasSuffix(eventType, ".delta"):
		b.classification = "nonempty_or_malformed_delta"
	case eventType == "response.output_item.added" || eventType == "response.output_item.done" ||
		eventType == "response.content_part.added" || eventType == "response.content_part.done" ||
		eventType == "response.reasoning_summary_part.added" || eventType == "response.reasoning_summary_part.done":
		b.classification = "nonempty_or_unknown_structure"
	}
}

// Event names are protocol metadata, but unknown upstream values are untrusted.
func openAIStreamDiagnosticType(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 96 {
		return "unknown"
	}
	for _, char := range value {
		if char != '.' && char != '_' && (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return "unknown"
		}
	}
	return value
}

func (b *openAIStreamReplayBoundary) fields() []zap.Field {
	return []zap.Field{
		zap.String("first_replay_blocking_event", b.eventType),
		zap.String("first_replay_blocking_item_type", b.itemType),
		zap.String("replay_blocking_classification", b.classification),
		zap.String("replay_commit_reason", b.commitReason),
		zap.Int64("replay_staged_bytes", b.stagedBytes),
	}
}

func (b *openAIStreamReplayBoundary) commit(log *zap.Logger, reason string, stagedBytes int64) {
	if b.commitReason != "" {
		return
	}
	if b.eventType == "" {
		b.eventType, b.classification = "unknown", "non_event_write"
	}
	b.commitReason, b.stagedBytes = reason, stagedBytes
	// Freeze before writing: even a failed write can expose part of the event.
	log.Info("gateway.stream_replay_boundary_committed", b.fields()...)
}
