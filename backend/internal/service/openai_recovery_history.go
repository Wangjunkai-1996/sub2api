package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
)

var ErrOpenAIRecoveryHistoryUnavailable = errors.New("complete portable OpenAI recovery history unavailable")

type openAIRecoveryHistory struct {
	Version int               `json:"version"`
	Turns   int               `json:"turns"`
	Items   []json.RawMessage `json:"items"`
}

// BuildOpenAIRecoverySnapshot records only the successful attempt's visible
// text context. A reconstructed continuation cannot preserve hidden reasoning.
// A missing snapshot must disable cross-account reconstruction, not same-account
// continuation. Output must be captured before public ID and display rewriting.
func BuildOpenAIRecoverySnapshot(request, winnerOutput, previousSnapshot []byte) ([]byte, error) {
	var body map[string]json.RawMessage
	if json.Unmarshal(request, &body) != nil || !openAIRecoveryPortableRequest(body) {
		return nil, ErrOpenAIRecoveryHistoryUnavailable
	}
	current, err := openAIRecoveryTextItems(body["input"], false)
	if err != nil {
		return nil, err
	}
	history := openAIRecoveryHistory{Version: 1, Turns: 1}
	previousID := bytes.TrimSpace(body["previous_response_id"])
	hasPrevious := len(previousID) > 0 && !bytes.Equal(previousID, []byte("null")) && !bytes.Equal(previousID, []byte(`""`))
	if hasPrevious {
		if json.Unmarshal(previousSnapshot, &history) != nil || history.Version != 1 || history.Turns < 1 || history.Turns >= 16 || len(previousSnapshot) > openAIRecoveryAliasMaxSnapshot {
			return nil, ErrOpenAIRecoveryHistoryUnavailable
		}
		history.Turns++
	}
	output, err := openAIRecoveryTextItems(winnerOutput, true)
	if err != nil {
		return nil, err
	}
	history.Items = append(history.Items, current...)
	history.Items = append(history.Items, output...)
	snapshot, err := json.Marshal(history)
	if err != nil || len(snapshot) > openAIRecoveryAliasMaxSnapshot {
		return nil, ErrOpenAIRecoveryHistoryUnavailable
	}
	return snapshot, nil
}

// RebuildOpenAIRecoveryContinuation uses a verified alias snapshot to form a
// self-contained request. The caller still validates the target model, account,
// and egress and removes the previous account's turn-state header.
func RebuildOpenAIRecoveryContinuation(request, snapshot []byte) ([]byte, error) {
	var history openAIRecoveryHistory
	if len(snapshot) == 0 || len(snapshot) > openAIRecoveryAliasMaxSnapshot || json.Unmarshal(snapshot, &history) != nil || history.Version != 1 || history.Turns < 1 || history.Turns >= 16 {
		return nil, ErrOpenAIRecoveryHistoryUnavailable
	}
	var body map[string]json.RawMessage
	if json.Unmarshal(request, &body) != nil || !openAIRecoveryPortableRequest(body) {
		return nil, ErrOpenAIRecoveryHistoryUnavailable
	}
	current, err := openAIRecoveryTextItems(body["input"], false)
	if err != nil {
		return nil, err
	}
	// Revalidate persisted content instead of trusting version alone.
	stored, err := json.Marshal(history.Items)
	if err != nil {
		return nil, ErrOpenAIRecoveryHistoryUnavailable
	}
	items, err := openAIRecoveryTextItems(stored, false)
	if err != nil {
		return nil, err
	}
	items = append(items, current...)
	body["input"], err = json.Marshal(items)
	if err != nil || len(body["input"]) > 16<<20 {
		return nil, ErrOpenAIRecoveryHistoryUnavailable
	}
	delete(body, "previous_response_id")
	rebuilt, err := json.Marshal(body)
	if err != nil {
		return nil, ErrOpenAIRecoveryHistoryUnavailable
	}
	return rebuilt, nil
}

func openAIRecoveryPortableRequest(body map[string]json.RawMessage) bool {
	if body == nil {
		return false
	}
	for _, key := range []string{"tools", "conversation", "context_management"} {
		value := bytes.TrimSpace(body[key])
		if len(value) > 0 && !bytes.Equal(value, []byte("null")) && !bytes.Equal(value, []byte("[]")) {
			return false
		}
	}
	return true
}

// Text-only canonical items deliberately omit account-owned item IDs and status.
// Known output reasoning is omitted only alongside visible assistant text;
// unknown content, tools, and input reasoning are never silently dropped.
func openAIRecoveryTextItems(raw []byte, output bool) ([]json.RawMessage, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, ErrOpenAIRecoveryHistoryUnavailable
	}
	var text string
	if !output && json.Unmarshal(raw, &text) == nil {
		item, _ := json.Marshal(map[string]any{"type": "message", "role": "user", "content": text})
		return []json.RawMessage{item}, nil
	}
	var items []map[string]json.RawMessage
	if len(raw) == 0 || json.Unmarshal(raw, &items) != nil || items == nil {
		return nil, ErrOpenAIRecoveryHistoryUnavailable
	}
	result := make([]json.RawMessage, 0, len(items))
	sawReasoning, sawText := false, false
	for _, item := range items {
		var itemType, role string
		_ = json.Unmarshal(item["type"], &itemType)
		_ = json.Unmarshal(item["role"], &role)
		if output && itemType == "reasoning" {
			if role != "" {
				return nil, ErrOpenAIRecoveryHistoryUnavailable
			}
			var reasoningID string
			for key, raw := range item {
				switch key {
				case "type":
				case "id":
					if json.Unmarshal(raw, &reasoningID) != nil || reasoningID == "" {
						return nil, ErrOpenAIRecoveryHistoryUnavailable
					}
				case "encrypted_content":
					if !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
						var value string
						if json.Unmarshal(raw, &value) != nil {
							return nil, ErrOpenAIRecoveryHistoryUnavailable
						}
					}
				case "status":
					var status string
					if json.Unmarshal(raw, &status) != nil || status != "completed" {
						return nil, ErrOpenAIRecoveryHistoryUnavailable
					}
				case "content", "summary":
					if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
						continue
					}
					var parts []map[string]json.RawMessage
					if json.Unmarshal(raw, &parts) != nil || parts == nil {
						return nil, ErrOpenAIRecoveryHistoryUnavailable
					}
					for _, part := range parts {
						if len(part) != 2 {
							return nil, ErrOpenAIRecoveryHistoryUnavailable
						}
						var partType, partText string
						if json.Unmarshal(part["type"], &partType) != nil || json.Unmarshal(part["text"], &partText) != nil ||
							(key == "content" && partType != "reasoning_text") || (key == "summary" && partType != "summary_text") {
							return nil, ErrOpenAIRecoveryHistoryUnavailable
						}
					}
				default:
					return nil, ErrOpenAIRecoveryHistoryUnavailable
				}
			}
			if reasoningID == "" {
				return nil, ErrOpenAIRecoveryHistoryUnavailable
			}
			sawReasoning = true
			continue
		}
		if (itemType != "" && itemType != "message") || (role != "user" && role != "assistant" && role != "system" && role != "developer") || (output && role != "assistant") {
			return nil, ErrOpenAIRecoveryHistoryUnavailable
		}
		for key := range item {
			if key != "type" && key != "role" && key != "content" && key != "id" && key != "status" && key != "phase" {
				return nil, ErrOpenAIRecoveryHistoryUnavailable
			}
		}
		if phase, ok := item["phase"]; ok {
			var value string
			if !output || json.Unmarshal(phase, &value) != nil || value != "final_answer" {
				return nil, ErrOpenAIRecoveryHistoryUnavailable
			}
		}
		var content string
		if json.Unmarshal(item["content"], &content) != nil {
			var parts []map[string]json.RawMessage
			if json.Unmarshal(item["content"], &parts) != nil || parts == nil {
				return nil, ErrOpenAIRecoveryHistoryUnavailable
			}
			var builder strings.Builder
			for _, part := range parts {
				for key := range part {
					if key != "type" && key != "text" && key != "annotations" && key != "logprobs" {
						return nil, ErrOpenAIRecoveryHistoryUnavailable
					}
				}
				var partType, partText string
				if json.Unmarshal(part["type"], &partType) != nil || json.Unmarshal(part["text"], &partText) != nil || (partType != "input_text" && partType != "output_text") {
					return nil, ErrOpenAIRecoveryHistoryUnavailable
				}
				builder.WriteString(partText)
			}
			content = builder.String()
		}
		if output && content != "" {
			sawText = true
		}
		canonical, err := json.Marshal(map[string]any{"type": "message", "role": role, "content": content})
		if err != nil {
			return nil, ErrOpenAIRecoveryHistoryUnavailable
		}
		result = append(result, canonical)
	}
	if sawReasoning && !sawText {
		return nil, ErrOpenAIRecoveryHistoryUnavailable
	}
	return result, nil
}
