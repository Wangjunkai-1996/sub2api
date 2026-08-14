package auditinput

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestParseSecurityRoutingContextResponsesUsesInstructionsAndCurrentIncrement(t *testing.T) {
	largeToolSchema := strings.Repeat("tool schema must not affect routing; ", 500)
	largeAssistantHistory := strings.Repeat("assistant history must not affect routing; ", 500)
	body := []byte(`{
		"model":"gpt-5.6-sol",
		"store":false,
		"metadata":{"trace":"protocol metadata excluded"},
		"instructions":[{"type":"input_text","text":"系统规则😀"}],
		"tools":[{"type":"function","name":"ignored_tool","description":` + quoteJSON(largeToolSchema) + `,"parameters":{"type":"object"}}],
		"input":[
			{"type":"message","role":"user","content":"历史用户文本"},
			{"type":"message","role":"assistant","content":` + quoteJSON(largeAssistantHistory) + `},
			{"type":"reasoning","encrypted_content":"opaque-reasoning","summary":[{"type":"summary_text","text":"assistant reasoning summary"}]},
			{"type":"message","role":"user","content":[
				{"type":"input_image","image_url":"data:image/png;base64,aGVsbG8="},
				{"type":"input_text","text":"当前请求🌍"}
			]}
		]
	}`)
	require.Greater(t, len(body), 12_000)

	context := ParseSecurityRoutingContext("responses_websocket", body)
	require.True(t, context.Reliable, "%+v", context.Document.Issues)
	require.Empty(t, context.UnreliableReason)
	require.Equal(t, ProtocolOpenAIResponses, context.Document.Protocol)
	require.Equal(t, "系统规则😀\n当前请求🌍", context.Document.NormalizedText)
	require.Equal(t, utf8.RuneCountInString(context.Document.NormalizedText), context.AuditTextRunes)
	require.True(t, context.Document.HasImages)
	for _, excluded := range []string{
		"tool schema", "ignored_tool", "assistant history", "历史用户文本",
		"opaque-reasoning", "reasoning summary", "protocol metadata", "aGVsbG8",
	} {
		require.NotContains(t, context.Document.NormalizedText, excluded)
	}

	// The existing strict audit parser remains increment-only.
	increment := ParseForTextAudit(ProtocolOpenAIResponses, body)
	require.True(t, increment.Complete, "%+v", increment.Issues)
	require.Equal(t, "当前请求🌍", increment.NormalizedText)
	require.NotContains(t, increment.NormalizedText, "系统规则")
}

func TestParseSecurityRoutingContextResponsesUsesTrailingToolIncrement(t *testing.T) {
	body := []byte(`{
		"instructions":"tenant instruction",
		"tools":[{"type":"function","name":"ignored_schema","description":"schema text","parameters":{"type":"object"}}],
		"input":[
			{"type":"message","role":"user","content":"already audited user turn"},
			{"type":"function_call","name":"current_tool_call","call_id":"call_1","arguments":"{\"target\":\"current\"}"},
			{"type":"function_call_output","call_id":"call_1","output":"first current tool output"},
			{"type":"custom_tool_call_output","call_id":"call_2","output":"second current tool output"}
		]
	}`)

	context := ParseSecurityRoutingContext(ProtocolOpenAIResponses, body)
	require.True(t, context.Reliable, "%+v", context.Document.Issues)
	require.Equal(t,
		"tenant instruction\ncurrent_tool_call\n{\"target\":\"current\"}\nfirst current tool output\nsecond current tool output",
		context.Document.NormalizedText,
	)
	for _, excluded := range []string{"already audited", "ignored_schema", "schema text"} {
		require.NotContains(t, context.Document.NormalizedText, excluded)
	}
}

func TestParseSecurityRoutingContextChatUsesSystemDeveloperAndCurrentUser(t *testing.T) {
	largeToolSchema := strings.Repeat("chat tool schema excluded; ", 600)
	body := []byte(`{
		"model":"gpt-5.6-sol",
		"user":"protocol-user-id",
		"tools":[{"type":"function","function":{"name":"ignored_tool","description":` + quoteJSON(largeToolSchema) + `,"parameters":{"type":"object"}}}],
		"messages":[
			{"role":"system","content":"系统规则"},
			{"role":"user","content":"历史用户文本"},
			{"role":"assistant","content":[{"type":"text","text":"assistant history"},{"type":"image_url","image_url":{"url":"data:image/png;base64,YXNzaXN0YW50"}}]},
			{"role":"developer","content":[{"type":"text","text":"开发约束😀"}]},
			{"role":"user","content":[
				{"type":"image_url","image_url":{"url":"data:image/png;base64,aGVsbG8="}},
				{"type":"text","text":"当前问题🌍"}
			]}
		]
	}`)
	require.Greater(t, len(body), 12_000)

	context := ParseSecurityRoutingContext(ProtocolOpenAIChat, body)
	require.True(t, context.Reliable, "%+v", context.Document.Issues)
	require.Equal(t, "系统规则\n开发约束😀\n当前问题🌍", context.Document.NormalizedText)
	require.Equal(t, utf8.RuneCountInString(context.Document.NormalizedText), context.AuditTextRunes)
	for _, excluded := range []string{
		"chat tool schema", "ignored_tool", "protocol-user-id", "历史用户文本",
		"assistant history", "YXNzaXN0YW50", "aGVsbG8",
	} {
		require.NotContains(t, context.Document.NormalizedText, excluded)
	}
}

func TestParseSecurityRoutingContextChatUsesContiguousToolSuffix(t *testing.T) {
	body := []byte(`{
		"messages":[
			{"role":"system","content":"system policy"},
			{"role":"developer","content":"developer policy"},
			{"role":"user","content":"already audited user"},
			{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"hidden\":true}"}}]},
			{"role":"tool","tool_call_id":"call_1","content":"first tool result"},
			{"role":"function","name":"legacy","content":"second tool result"}
		]
	}`)

	context := ParseSecurityRoutingContext(ProtocolOpenAIChat, body)
	require.True(t, context.Reliable, "%+v", context.Document.Issues)
	require.Equal(t,
		"system policy\ndeveloper policy\nfirst tool result\nsecond tool result",
		context.Document.NormalizedText,
	)
	for _, excluded := range []string{"already audited", "lookup", "hidden"} {
		require.NotContains(t, context.Document.NormalizedText, excluded)
	}
}

func TestParseSecurityRoutingContextPreservesKnownNoTextClassification(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		body     string
	}{
		{
			name:     "responses image only",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"input_image","image_url":"data:image/png;base64,aGVsbG8="}]}`,
		},
		{
			name:     "chat image only",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,aGVsbG8="}}]}]}`,
		},
		{
			name:     "responses empty increment",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			context := ParseSecurityRoutingContext(test.protocol, []byte(test.body))
			require.True(t, context.Reliable, "%+v", context.Document.Issues)
			require.True(t, context.Document.Complete)
			require.Equal(t, TextAuditKnownNoText, context.Document.TextAuditClass)
			require.Zero(t, context.AuditTextRunes)
			require.Empty(t, context.Document.NormalizedText)
		})
	}
}

func TestParseSecurityRoutingContextReportsUnreliableInputs(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		body     string
		reason   string
	}{
		{
			name:     "malformed responses instructions",
			protocol: ProtocolOpenAIResponses,
			body:     `{"instructions":42,"input":"current text"}`,
			reason:   IssueInvalidShape,
		},
		{
			name:     "duplicate responses instruction text",
			protocol: ProtocolOpenAIResponses,
			body:     `{"instructions":[{"type":"input_text","text":"first","text":"second"}],"input":"current text"}`,
			reason:   IssueDuplicateField,
		},
		{
			name:     "unknown chat instruction content",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"system","content":[{"type":"future_text","text":"hidden"}]},{"role":"user","content":"current text"}]}`,
			reason:   IssueUnknownType,
		},
		{
			name:     "unknown historical chat role",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"future_role","content":"hidden"},{"role":"user","content":"current text"}]}`,
			reason:   IssueUnknownRole,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			context := ParseSecurityRoutingContext(test.protocol, []byte(test.body))
			require.False(t, context.Reliable)
			require.False(t, context.Document.Complete)
			require.Equal(t, test.reason, context.UnreliableReason)
			require.True(t, context.Document.HasIssue(test.reason), "%+v", context.Document.Issues)
			require.Contains(t, context.Document.NormalizedText, "current text")
		})
	}
}
