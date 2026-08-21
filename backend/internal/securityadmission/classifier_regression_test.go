package securityadmission

import (
	"strings"
	"testing"
)

func classifyRegression(t *testing.T, protocol Protocol, body string) Admission {
	t.Helper()
	admission, err := Classify(string(protocol), []byte(body), Options{})
	if err != nil {
		t.Fatalf("classify %s: %v", protocol, err)
	}
	return admission
}

func TestResponsesToolValuesAreMaterializedRecursively(t *testing.T) {
	body := `{"instructions":"response-instructions","input":[{"type":"function_call","arguments":{"function-key-canary":"function-argument-canary","nested":{"value":"nested-argument-canary"},"type":"safe-type-canary","url":"https://example.invalid/business-value"}},{"type":"function_call_output","output":{"output-key-canary":"function-output-canary"}}]}`
	admission := classifyRegression(t, ProtocolOpenAIResponses, body)
	if admission.Class() != RequestAuditableText {
		t.Fatalf("class=%s reason=%s", admission.Class(), admission.Reason())
	}
	text, err := admission.MaterializeText([]byte(body))
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	for _, want := range []string{"response-instructions", "function-key-canary", "function-argument-canary", "nested-argument-canary", "safe-type-canary", "url", "https://example.invalid/business-value", "output-key-canary", "function-output-canary"} {
		if !strings.Contains(text, want) {
			t.Fatalf("materialized text missing %q: %q", want, text)
		}
	}
}

func TestResponsesToolVariantsRemainInspectablyBounded(t *testing.T) {
	body := `{"input":[{"type":"mcp_tool_call","arguments":{"command":"mcp-call-canary"}},{"type":"mcp_tool_call_output","output":{"result":"mcp-output-canary"}},{"type":"local_shell_call","action":{"command":"shell-command-canary"}}]}`
	admission := classifyRegression(t, ProtocolOpenAIResponses, body)
	if admission.Class() != RequestAuditableText {
		t.Fatalf("class=%s reason=%s", admission.Class(), admission.Reason())
	}
	text, err := admission.MaterializeText([]byte(body))
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	for _, want := range []string{"mcp-call-canary", "mcp-output-canary", "shell-command-canary"} {
		if !strings.Contains(text, want) {
			t.Fatalf("materialized text missing %q: %q", want, text)
		}
	}
}

func TestChatCurrentToolSuffixIncludesConsecutiveToolAndFunctionMessages(t *testing.T) {
	body := `{"model":"x","messages":[{"role":"system","content":"system-canary"},{"role":"user","content":"old-user-canary"},{"role":"assistant","tool_calls":[{"type":"function","function":{"name":"lookup","arguments":"assistant-argument-canary"}}]},{"role":"tool","tool_call_id":"call-1","content":"tool-output-canary"},{"role":"function","name":"lookup","content":"function-output-canary"}]}`
	admission, err := Classify(string(ProtocolOpenAIChat), []byte(body), Options{Lineage: LineageTrusted})
	if err != nil || admission.Class() != RequestAuditableText {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	document, err := admission.MaterializeDocument([]byte(body), TextScopeCurrentTurn)
	if err != nil {
		t.Fatalf("materialize current turn: %v", err)
	}
	for _, want := range []string{"system-canary", "assistant-argument-canary", "tool-output-canary", "function-output-canary"} {
		if !strings.Contains(document.Text, want) {
			t.Fatalf("current suffix missing %q: %q", want, document.Text)
		}
	}
	if strings.Contains(document.Text, "old-user-canary") {
		t.Fatalf("current suffix included old user text: %q", document.Text)
	}
}

func TestAnthropicToolResultContentIsMaterialized(t *testing.T) {
	body := `{"model":"claude","system":"system-canary","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"call-1","name":"lookup","input":{"query":"tool-input-canary"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1","content":[{"type":"text","text":"tool-result-canary"}]}]}]}`
	admission := classifyRegression(t, ProtocolAnthropicMessages, body)
	if admission.Class() != RequestAuditableText {
		t.Fatalf("class=%s reason=%s", admission.Class(), admission.Reason())
	}
	text, err := admission.MaterializeText([]byte(body))
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	for _, want := range []string{"system-canary", "tool-input-canary", "tool-result-canary"} {
		if !strings.Contains(text, want) {
			t.Fatalf("materialized text missing %q: %q", want, text)
		}
	}
}

func TestTypedEmptyAndMissingRootContainersFailClosed(t *testing.T) {
	tests := []struct {
		name     string
		protocol Protocol
		body     string
		reason   ReasonCode
	}{
		{name: "responses typed empty message", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user"}]}`, reason: ReasonUnknownContentShape},
		{name: "responses typed empty text block", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","content":[{"type":"input_text"}]}]}`, reason: ReasonUnknownContentShape},
		{name: "responses typed empty content array", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","content":[]}]}`, reason: ReasonUnknownContentShape},
		{name: "responses null function arguments", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"function_call","arguments":null}]}`, reason: ReasonUnknownContentShape},
		{name: "responses empty function arguments", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"function_call","arguments":{}}]}`, reason: ReasonUnknownContentShape},
		{name: "responses null function output", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"function_call_output","output":null}]}`, reason: ReasonUnknownContentShape},
		{name: "responses empty function output", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"function_call_output","output":{}}]}`, reason: ReasonUnknownContentShape},
		{name: "responses missing input and instructions", protocol: ProtocolOpenAIResponses, body: `{"model":"x"}`, reason: ReasonUnknownContentShape},
		{name: "chat missing messages", protocol: ProtocolOpenAIChat, body: `{"model":"x"}`, reason: ReasonUnknownContentShape},
		{name: "chat typed empty tool call", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"assistant","tool_calls":[{"type":"function"}]}]}`, reason: ReasonUnknownContentShape},
		{name: "chat null function arguments", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"assistant","function_call":{"arguments":null}}]}`, reason: ReasonUnknownContentShape},
		{name: "chat empty tool output", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"tool","tool_call_id":"call-1","content":[]}]}`, reason: ReasonUnknownContentShape},
		{name: "messages missing messages", protocol: ProtocolAnthropicMessages, body: `{"model":"claude"}`, reason: ReasonUnknownContentShape},
		{name: "messages typed empty tool result", protocol: ProtocolAnthropicMessages, body: `{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1"}]}]}`, reason: ReasonUnknownContentShape},
		{name: "messages empty tool result", protocol: ProtocolAnthropicMessages, body: `{"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1","content":[]}]}]}`, reason: ReasonUnknownContentShape},
		{name: "messages empty tool input", protocol: ProtocolAnthropicMessages, body: `{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"call-1","name":"lookup","input":{}}]}]}`, reason: ReasonUnknownContentShape},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			admission := classifyRegression(t, test.protocol, test.body)
			if admission.Class() != RequestUninspectable || admission.Reason() != test.reason {
				t.Fatalf("class=%s reason=%s want uninspectable/%s", admission.Class(), admission.Reason(), test.reason)
			}
		})
	}
}

func TestFailClosedReasonsAcrossProtocols(t *testing.T) {
	tests := []struct {
		name     string
		protocol Protocol
		body     string
		reason   ReasonCode
	}{
		{name: "responses unknown root type", protocol: ProtocolOpenAIResponses, body: `{"type":"future_request","input":"x"}`, reason: ReasonUnknownType},
		{name: "chat unknown role", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"future","content":"x"}]}`, reason: ReasonUnknownRole},
		{name: "messages unknown block type", protocol: ProtocolAnthropicMessages, body: `{"messages":[{"role":"user","content":[{"type":"future","text":"x"}]}]}`, reason: ReasonUnknownType},
		{name: "responses duplicate", protocol: ProtocolOpenAIResponses, body: `{"input":"a","input":"b"}`, reason: ReasonDuplicateJSONKey},
		{name: "chat duplicate", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":"a","content":"b"}]}`, reason: ReasonDuplicateJSONKey},
		{name: "messages duplicate", protocol: ProtocolAnthropicMessages, body: `{"messages":[{"role":"user","content":"a","content":"b"}]}`, reason: ReasonDuplicateJSONKey},
		{name: "responses remote", protocol: ProtocolOpenAIResponses, body: `{"conversation":{"id":"remote"},"input":"x"}`, reason: ReasonRemoteContent},
		{name: "responses item reference", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"item_reference","id":"item-1"}]}`, reason: ReasonRemoteContent},
		{name: "chat remote", protocol: ProtocolOpenAIChat, body: `{"conversation":{"id":"remote"},"messages":[{"role":"user","content":"x"}]}`, reason: ReasonRemoteContent},
		{name: "messages remote", protocol: ProtocolAnthropicMessages, body: `{"conversation":{"id":"remote"},"messages":[{"role":"user","content":"x"}]}`, reason: ReasonRemoteContent},
		{name: "responses encrypted", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"reasoning","encrypted_content":"cipher"}]}`, reason: ReasonEncryptedContent},
		{name: "responses typed media", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"input_image","image_url":null}]}`, reason: ReasonMediaContent},
		{name: "responses tool input media", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"function_call","arguments":{"file_id":"file-1"}}]}`, reason: ReasonMediaContent},
		{name: "responses tool input data media", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"function_call","arguments":"data:image/png;base64,AAAA"}]}`, reason: ReasonMediaContent},
		{name: "chat media", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.invalid/a"}}]}]}`, reason: ReasonMediaContent},
		{name: "chat tool input media", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"assistant","function_call":{"arguments":{"input_audio":"payload"}}}]}`, reason: ReasonMediaContent},
		{name: "messages encrypted", protocol: ProtocolAnthropicMessages, body: `{"messages":[{"role":"assistant","content":[{"type":"redacted_thinking","data":"cipher"}]}]}`, reason: ReasonEncryptedContent},
		{name: "messages tool input encrypted", protocol: ProtocolAnthropicMessages, body: `{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"call-1","name":"lookup","input":{"encrypted_content":"cipher"}}]}]}`, reason: ReasonEncryptedContent},
		{name: "messages tool input remote", protocol: ProtocolAnthropicMessages, body: `{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"call-1","name":"lookup","input":{"item_reference":"remote"}}]}]}`, reason: ReasonRemoteContent},
		{name: "messages tool input media", protocol: ProtocolAnthropicMessages, body: `{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"call-1","name":"lookup","input":{"file":"payload"}}]}]}`, reason: ReasonMediaContent},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			admission := classifyRegression(t, test.protocol, test.body)
			if admission.Class() != RequestUninspectable || admission.Reason() != test.reason {
				t.Fatalf("class=%s reason=%s want uninspectable/%s", admission.Class(), admission.Reason(), test.reason)
			}
		})
	}
}

func TestResponsesWebSocketPreviousResponseRequiresTrustedLineage(t *testing.T) {
	for _, body := range []string{
		`{"type":"response.create","previous_response_id":"resp_1","input":"local-delta-canary"}`,
		`{"type":"response.create","previous_response_id":"resp_1"}`,
	} {
		admission := classifyRegression(t, ProtocolResponsesWebSocket, body)
		if admission.Class() != RequestUninspectable || admission.Reason() != ReasonUntrustedLineage {
			t.Fatalf("body=%s class=%s reason=%s", body, admission.Class(), admission.Reason())
		}
	}

	body := `{"type":"response.create","previous_response_id":"resp_1","input":"trusted-delta-canary"}`
	trusted, err := Classify(string(ProtocolResponsesWebSocket), []byte(body), Options{Lineage: LineageTrusted})
	if err != nil || trusted.Class() != RequestAuditableText {
		t.Fatalf("trusted admission=%+v err=%v", trusted, err)
	}
	text, err := trusted.MaterializeText([]byte(body))
	if err != nil || !strings.Contains(text, "trusted-delta-canary") {
		t.Fatalf("trusted text=%q err=%v", text, err)
	}
}

func TestToolSchemaStringsAndKeysAreNotSilentlySkipped(t *testing.T) {
	body := `{"tools":[{"type":"function","function":{"name":"lookup","description":"schema-description-canary","parameters":{"type":"object","properties":{"schema-key-canary":{"description":"nested-schema-canary","enum":["enum-canary"]}}}}}]}`
	admission := classifyRegression(t, ProtocolOpenAIResponses, body)
	if admission.Class() != RequestAuditableText {
		t.Fatalf("class=%s reason=%s", admission.Class(), admission.Reason())
	}
	text, err := admission.MaterializeText([]byte(body))
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	for _, want := range []string{"schema-description-canary", "schema-key-canary", "nested-schema-canary", "enum-canary"} {
		if !strings.Contains(text, want) {
			t.Fatalf("schema text missing %q: %q", want, text)
		}
	}
}

func TestTrustedResponsesCurrentTurnKeepsToolSchemaAndAllInputItems(t *testing.T) {
	body := `{"tools":[{"type":"function","function":{"name":"lookup","description":"current-schema-canary","parameters":{"type":"object"}}}],"input":[{"type":"message","role":"user","content":"old-user-canary"},{"type":"message","role":"user","content":"current-user-canary"}]}`
	admission, err := Classify(string(ProtocolOpenAIResponses), []byte(body), Options{Lineage: LineageTrusted})
	if err != nil || admission.Class() != RequestAuditableText {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	document, err := admission.MaterializeDocument([]byte(body), TextScopeCurrentTurn)
	if err != nil {
		t.Fatalf("materialize current turn: %v", err)
	}
	for _, want := range []string{"current-schema-canary", "old-user-canary", "current-user-canary"} {
		if !strings.Contains(document.Text, want) {
			t.Fatalf("current document missing %q: %q", want, document.Text)
		}
	}

	wsBody := []byte(`{"type":"response.create","previous_response_id":"resp_prev","input":[{"role":"user","content":"ws-block-canary"},{"role":"user","content":"ws-allow-canary"}]}`)
	wsAdmission, err := Classify(string(ProtocolResponsesWebSocket), wsBody, Options{Lineage: LineageTrusted})
	if err != nil || wsAdmission.Class() != RequestAuditableText {
		t.Fatalf("ws admission=%+v err=%v", wsAdmission, err)
	}
	wsDocument, err := wsAdmission.MaterializeDocument(wsBody, TextScopeCurrentTurn)
	if err != nil {
		t.Fatalf("materialize ws current turn: %v", err)
	}
	for _, want := range []string{"ws-block-canary", "ws-allow-canary"} {
		if !strings.Contains(wsDocument.Text, want) {
			t.Fatalf("ws current document missing %q: %q", want, wsDocument.Text)
		}
	}
}

func TestHostedAndUnknownToolDefinitionsFailClosed(t *testing.T) {
	tests := []struct {
		name     string
		protocol Protocol
		body     string
		reason   ReasonCode
	}{
		{name: "responses file search", protocol: ProtocolOpenAIResponses, body: `{"input":"summarize","tools":[{"type":"file_search","vector_store_ids":["vs_123"]}]}`, reason: ReasonRemoteContent},
		{name: "responses web search", protocol: ProtocolOpenAIResponses, body: `{"input":"research","tools":[{"type":"web_search_preview"}]}`, reason: ReasonRemoteContent},
		{name: "responses image generation", protocol: ProtocolOpenAIResponses, body: `{"input":"draw","tools":[{"type":"image_generation"}]}`, reason: ReasonMediaContent},
		{name: "responses Lite hosted search", protocol: ProtocolOpenAIResponses, body: `{"input":[{"tools":[{"name":"search","type":"web_search"}],"role":"developer","type":"additional_tools"},{"type":"message","role":"user","content":"x"}]}`, reason: ReasonRemoteContent},
		{name: "websocket mcp", protocol: ProtocolResponsesWebSocket, body: `{"type":"response.create","response":{"input":"inspect","tools":[{"type":"mcp","server_label":"remote"}]}}`, reason: ReasonRemoteContent},
		{name: "websocket Lite hosted search", protocol: ProtocolResponsesWebSocket, body: `{"type":"response.create","response":{"input":[{"type":"additional_tools","tools":[{"type":"file_search","name":"files"}]},{"type":"message","role":"user","content":"x"}]}}`, reason: ReasonRemoteContent},
		{name: "chat unknown", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":"x"}],"tools":[{"type":"future_tool","description":"hidden"}]}`, reason: ReasonUnknownType},
		{name: "chat web search options", protocol: ProtocolOpenAIChat, body: `{"model":"gpt-4o","messages":[{"role":"user","content":"x"}],"web_search_options":{"search_context_size":"low"}}`, reason: ReasonRemoteContent},
		{name: "chat search preview model", protocol: ProtocolOpenAIChat, body: `{"model":"gpt-4o-search-preview-2025-03-11","messages":[{"role":"user","content":"x"}]}`, reason: ReasonRemoteContent},
		{name: "chat search API model", protocol: ProtocolOpenAIChat, body: `{"model":"gpt-5-search-api","messages":[{"role":"user","content":"x"}]}`, reason: ReasonRemoteContent},
		{name: "anthropic hosted search", protocol: ProtocolAnthropicMessages, body: `{"messages":[{"role":"user","content":"x"}],"tools":[{"type":"web_search_20250305","name":"search"}]}`, reason: ReasonRemoteContent},
		{name: "anthropic emulated name-only web search", protocol: ProtocolAnthropicMessages, body: `{"messages":[{"role":"user","content":"x"}],"tools":[{"name":"web_search","input_schema":{"type":"object"}}]}`, reason: ReasonRemoteContent},
		{name: "anthropic emulated explicit function search", protocol: ProtocolAnthropicMessages, body: `{"messages":[{"role":"user","content":"x"}],"tools":[{"type":"function","name":"google_search","input_schema":{"type":"object"}}]}`, reason: ReasonRemoteContent},
		{name: "responses Lite unknown tool", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"additional_tools","tools":[{"type":"future_tool","name":"future"}]},{"type":"message","role":"user","content":"x"}]}`, reason: ReasonUnknownType},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			admission := classifyRegression(t, test.protocol, test.body)
			if admission.Class() != RequestUninspectable || admission.Reason() != test.reason || admission.Requirement() != AccountRequirementAuditExempt {
				t.Fatalf("admission=%+v want uninspectable/%s/audit-exempt", admission, test.reason)
			}
		})
	}
}

func TestInlineFunctionAndCustomToolDefinitionsRemainAuditable(t *testing.T) {
	tests := []struct {
		name     string
		protocol Protocol
		body     string
		canary   string
	}{
		{name: "responses custom", protocol: ProtocolOpenAIResponses, body: `{"input":"x","tools":[{"type":"custom","name":"grammar","description":"custom-schema-canary"}]}`, canary: "custom-schema-canary"},
		{name: "responses Lite custom with late escaped type", protocol: ProtocolOpenAIResponses, body: `{"input":[{"tools":[{"t\u0079pe":"custom","name":"exec","description":"lite-schema-canary"}],"role":"developer","t\u0079pe":"additional_tools"},{"type":"message","role":"user","content":"x"}]}`, canary: "lite-schema-canary"},
		{name: "websocket Lite namespace", protocol: ProtocolResponsesWebSocket, body: `{"type":"response.create","response":{"input":[{"type":"additional_tools","role":"developer","tools":[{"type":"namespace","name":"workspace","tools":[{"type":"function","name":"lookup","description":"ws-lite-schema-canary"}]}]},{"type":"message","role":"user","content":"x"}]}}`, canary: "ws-lite-schema-canary"},
		{name: "chat function", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":"x"}],"tools":[{"type":"function","function":{"name":"lookup","description":"chat-schema-canary"}}]}`, canary: "chat-schema-canary"},
		{name: "chat legacy functions", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":"x"}],"functions":[{"name":"lookup","description":"legacy-schema-canary"}]}`, canary: "legacy-schema-canary"},
		{name: "anthropic inline tool", protocol: ProtocolAnthropicMessages, body: `{"messages":[{"role":"user","content":"x"}],"tools":[{"name":"lookup","description":"messages-schema-canary","input_schema":{"type":"object"}}]}`, canary: "messages-schema-canary"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			admission := classifyRegression(t, test.protocol, test.body)
			if admission.Class() != RequestAuditableText {
				t.Fatalf("admission=%+v", admission)
			}
			text, err := admission.MaterializeText([]byte(test.body))
			if err != nil || !strings.Contains(text, test.canary) {
				t.Fatalf("text=%q err=%v want %q", text, err, test.canary)
			}
		})
	}
}

func TestAdmissionDefaultsToAnyAccountForInspectableRequests(t *testing.T) {
	for _, test := range []struct {
		protocol Protocol
		body     string
	}{
		{ProtocolOpenAIResponses, `{"input":"x"}`},
		{ProtocolOpenAIChat, `{"messages":[{"role":"user","content":"x"}]}`},
		{ProtocolAnthropicMessages, `{"messages":[{"role":"user","content":"x"}]}`},
		{ProtocolResponsesWebSocket, `{"type":"response.create","input":"x"}`},
	} {
		admission := classifyRegression(t, test.protocol, test.body)
		if admission.Requirement() != AccountRequirementAny {
			t.Fatalf("protocol=%s requirement=%q class=%s", test.protocol, admission.Requirement(), admission.Class())
		}
	}
}
