package auditinput

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseForTextAuditIgnoresRedactedCodexFullHistory(t *testing.T) {
	items := []any{
		map[string]any{
			"type":              "reasoning",
			"id":                "rs_redacted",
			"summary":           []any{},
			"encrypted_content": "redacted-reasoning-ciphertext",
			"internal_chat_message_metadata_passthrough": map[string]any{
				"turn_id": "00000000-0000-4000-8000-000000000001",
			},
		},
		map[string]any{
			"type":    "custom_tool_call",
			"id":      "ctc_redacted",
			"status":  "completed",
			"call_id": "call_redacted",
			"name":    "view_page",
			"input":   `{"page":"redacted"}`,
			"internal_chat_message_metadata_passthrough": map[string]any{
				"turn_id": "00000000-0000-4000-8000-000000000002",
			},
		},
	}

	toolOutput := make([]any, 0, 60)
	toolOutput = append(toolOutput, map[string]any{"type": "input_text", "text": "redacted tool output"})
	for index := 0; index < 59; index++ {
		imagePayload := fmt.Sprintf("opaque-screenshot-payload-%02d", index)
		if index == 0 {
			imagePayload = strings.Repeat("x", MaxImageBytes+1)
		}
		toolOutput = append(toolOutput, map[string]any{
			"type":      "input_image",
			"image_url": imagePayload,
		})
	}
	items = append(items,
		map[string]any{
			"type":    "custom_tool_call_output",
			"id":      "ctco_redacted",
			"call_id": "call_redacted",
			"output":  toolOutput,
			"internal_chat_message_metadata_passthrough": map[string]any{
				"turn_id": "00000000-0000-4000-8000-000000000003",
			},
		},
		map[string]any{
			"type":      "function_call",
			"id":        "fc_redacted",
			"call_id":   "call_function_redacted",
			"name":      "exec_command",
			"arguments": `{"cmd":"redacted"}`,
			"internal_chat_message_metadata_passthrough": map[string]any{
				"turn_id": "00000000-0000-4000-8000-000000000004",
			},
		},
		map[string]any{
			"type":    "function_call_output",
			"id":      "fco_redacted",
			"call_id": "call_function_redacted",
			"output":  "redacted command output",
			"internal_chat_message_metadata_passthrough": map[string]any{
				"turn_id": "00000000-0000-4000-8000-000000000005",
			},
		},
		map[string]any{
			"type": "message",
			"id":   "msg_user_redacted",
			"role": "user",
			"content": []any{
				map[string]any{"type": "input_text", "text": "redacted user request"},
			},
			"internal_chat_message_metadata_passthrough": map[string]any{
				"turn_id": "00000000-0000-4000-8000-000000000006",
			},
		},
		map[string]any{
			"type":      "agent_message",
			"id":        "msg_agent_redacted",
			"author":    "/root/redacted_worker",
			"recipient": "/root",
			"content": []any{
				map[string]any{"type": "input_text", "text": "redacted agent reply"},
				map[string]any{"type": "encrypted_content", "encrypted_content": "redacted-agent-ciphertext"},
			},
			"internal_chat_message_metadata_passthrough": map[string]any{
				"turn_id": "00000000-0000-4000-8000-000000000007",
			},
		},
	)

	body, err := json.Marshal(map[string]any{
		"model": "gpt-5.6-sol",
		"store": false,
		"input": items,
	})
	require.NoError(t, err)

	document := ParseForTextAudit(ProtocolOpenAIResponses, body)

	require.True(t, document.Complete, "%+v", document.Issues)
	require.False(t, document.HasImages)
	require.Empty(t, document.Media)
	require.Len(t, document.ControlItems, 1)
	require.Empty(t, document.OpaqueStates)
	require.Empty(t, document.NormalizedText)
	serialized, err := json.Marshal(document)
	require.NoError(t, err)
	for _, secret := range []string{
		"redacted-reasoning-ciphertext", "redacted-agent-ciphertext",
		"/root/redacted_worker", "00000000-0000-4000-8000-000000000001", "opaque-screenshot-payload-01",
		strings.Repeat("x", 256), "view_page", "redacted tool output", "exec_command",
		"redacted command output", "redacted user request", "redacted agent reply",
	} {
		require.NotContains(t, string(serialized), secret)
	}
}
