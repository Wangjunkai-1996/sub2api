package securityadmission

import (
	"strings"
	"testing"
)

func requireMaterializedCanaries(t *testing.T, protocol Protocol, body string, canaries ...string) {
	t.Helper()
	admission, err := Classify(string(protocol), []byte(body), Options{})
	if err != nil || admission.Class() != RequestAuditableText {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	text, err := admission.MaterializeText([]byte(body))
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	for _, canary := range canaries {
		if !strings.Contains(text, canary) {
			t.Fatalf("materialized text missing %q: %q", canary, text)
		}
	}
}

func TestResponsesOuterToolMetadataIsMaterialized(t *testing.T) {
	body := `{"input":[
		{"type":"function_call","name":"responses-function-name-canary","namespace":"responses-namespace-canary","arguments":"{}"},
		{"type":"mcp_tool_call","name":"responses-mcp-name-canary","server_label":"responses-server-label-canary","arguments":"{}"}
	]}`
	requireMaterializedCanaries(t, ProtocolOpenAIResponses, body,
		"responses-function-name-canary",
		"responses-namespace-canary",
		"responses-mcp-name-canary",
		"responses-server-label-canary",
	)
}

func TestChatOuterToolMetadataAndChoicesAreMaterialized(t *testing.T) {
	body := `{
		"function_call":{"name":"legacy-function-choice-canary"},
		"tool_choice":{"type":"function","function":{"name":"chat-choice-name-canary","namespace":"chat-choice-namespace-canary"}},
		"messages":[
			{"role":"assistant","tool_calls":[{"type":"function","namespace":"chat-call-namespace-canary","server_label":"chat-call-server-canary","function":{"name":"chat-function-name-canary","arguments":"{}"}}]},
			{"role":"function","name":"chat-function-result-name-canary","content":"result"}
		]
	}`
	requireMaterializedCanaries(t, ProtocolOpenAIChat, body,
		"legacy-function-choice-canary",
		"chat-choice-name-canary",
		"chat-choice-namespace-canary",
		"chat-call-namespace-canary",
		"chat-call-server-canary",
		"chat-function-name-canary",
		"chat-function-result-name-canary",
	)
}

func TestAnthropicOuterToolMetadataAndChoiceAreMaterialized(t *testing.T) {
	body := `{
		"tool_choice":{"type":"tool","name":"messages-choice-name-canary"},
		"messages":[{"role":"assistant","content":[{
			"type":"tool_use",
			"id":"tool-1",
			"name":"messages-tool-name-canary",
			"namespace":"messages-namespace-canary",
			"server_label":"messages-server-label-canary",
			"input":{"query":"messages-tool-input-canary"}
		}]}]
	}`
	requireMaterializedCanaries(t, ProtocolAnthropicMessages, body,
		"messages-choice-name-canary",
		"messages-tool-name-canary",
		"messages-namespace-canary",
		"messages-server-label-canary",
		"messages-tool-input-canary",
	)
}

func TestResponsesTextFormatSchemaIsMaterialized(t *testing.T) {
	body := `{
		"input":"return structured output",
		"text":{
			"verbosity":"low",
			"format":{
				"type":"json_schema",
				"name":"responses-format-name-canary",
				"description":"responses-format-description-canary",
				"schema":{
					"type":"object",
					"properties":{
						"responses-schema-key-canary":{"description":"responses-schema-description-canary","type":"string"}
					}
				},
				"strict":true
			}
		}
	}`
	requireMaterializedCanaries(t, ProtocolOpenAIResponses, body,
		"responses-format-name-canary",
		"responses-format-description-canary",
		"responses-schema-key-canary",
		"responses-schema-description-canary",
	)
}

func TestChatResponseFormatAndPredictionAreMaterialized(t *testing.T) {
	body := `{
		"messages":[{"role":"user","content":"return structured output"}],
		"response_format":{
			"type":"json_schema",
			"json_schema":{
				"name":"chat-format-name-canary",
				"description":"chat-format-description-canary",
				"schema":{
					"type":"object",
					"properties":{"chat-schema-key-canary":{"description":"chat-schema-description-canary","type":"string"}}
				},
				"strict":true
			}
		},
		"prediction":{
			"type":"content",
			"content":[{"type":"text","text":"chat-prediction-canary"}]
		}
	}`
	requireMaterializedCanaries(t, ProtocolOpenAIChat, body,
		"chat-format-name-canary",
		"chat-format-description-canary",
		"chat-schema-key-canary",
		"chat-schema-description-canary",
		"chat-prediction-canary",
	)
}

func TestOuterModelVisibleFieldsFailClosedOnUnknownShapes(t *testing.T) {
	tests := []struct {
		name     string
		protocol Protocol
		body     string
		reason   ReasonCode
	}{
		{
			name:     "responses tool name object",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"function_call","name":{"value":"ambiguous"},"arguments":"{}"}]}`,
			reason:   ReasonUnknownContentShape,
		},
		{
			name:     "responses text unknown field",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":"x","text":{"future":{"description":"hidden"}}}`,
			reason:   ReasonUnknownField,
		},
		{
			name:     "responses text format unknown type",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":"x","text":{"format":{"type":"future","description":"hidden"}}}`,
			reason:   ReasonUnknownType,
		},
		{
			name:     "chat function name array",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"assistant","function_call":{"name":["ambiguous"],"arguments":"{}"}}]}`,
			reason:   ReasonUnknownContentShape,
		},
		{
			name:     "chat response format unknown field",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"user","content":"x"}],"response_format":{"type":"json_schema","future":{"description":"hidden"}}}`,
			reason:   ReasonUnknownField,
		},
		{
			name:     "chat response format missing schema",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"user","content":"x"}],"response_format":{"type":"json_schema","json_schema":{"name":"answer"}}}`,
			reason:   ReasonUnknownContentShape,
		},
		{
			name:     "chat prediction unknown type",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"user","content":"x"}],"prediction":{"type":"future","content":"hidden"}}`,
			reason:   ReasonUnknownType,
		},
		{
			name:     "chat prediction empty content",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"user","content":"x"}],"prediction":{"type":"content","content":[]}}`,
			reason:   ReasonUnknownContentShape,
		},
		{
			name:     "messages tool name object",
			protocol: ProtocolAnthropicMessages,
			body:     `{"messages":[{"role":"assistant","content":[{"type":"tool_use","name":{"value":"ambiguous"},"input":{"query":"x"}}]}]}`,
			reason:   ReasonUnknownContentShape,
		},
		{
			name:     "tool choice unknown selector field",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":"x","tool_choice":{"type":"function","future_name":"hidden"}}`,
			reason:   ReasonUnknownField,
		},
		{
			name:     "tool choice unknown type",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":"x","tool_choice":{"type":"future","name":"hidden"}}`,
			reason:   ReasonUnknownType,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			admission, err := Classify(string(test.protocol), []byte(test.body), Options{})
			if err != nil {
				t.Fatalf("classify: %v", err)
			}
			if admission.Class() != RequestUninspectable || admission.Reason() != test.reason {
				t.Fatalf("admission=%+v want reason=%s", admission, test.reason)
			}
		})
	}
}
