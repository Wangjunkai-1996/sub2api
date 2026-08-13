package auditinput

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTextAuditClassificationIsExplicitAndFailClosed(t *testing.T) {
	require.Equal(t, TextAuditIndeterminate, TextAuditClassification(""))

	tests := []struct {
		name     string
		protocol string
		body     string
		want     TextAuditClassification
		wantText string
	}{
		{
			name:     "responses text",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":"inspect this"}`,
			want:     TextAuditAuditableText,
			wantText: "inspect this",
		},
		{
			name:     "responses image only",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"input_image","image_url":"https://example.test/image.png"}]}`,
			want:     TextAuditKnownNoText,
		},
		{
			name:     "responses empty tool output",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"function_call_output","call_id":"call_1","output":""}]}`,
			want:     TextAuditKnownNoText,
		},
		{
			name:     "chat empty tool output",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"tool","tool_call_id":"call_1","content":""}]}`,
			want:     TextAuditKnownNoText,
		},
		{
			name:     "unknown responses item",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"future_item","payload":null}]}`,
			want:     TextAuditIndeterminate,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := ParseForTextAudit(test.protocol, []byte(test.body))
			require.Equal(t, test.want, document.TextAuditClass, "%+v", document.Issues)
			require.Equal(t, test.wantText, document.NormalizedText)
			require.Equal(t, test.want != TextAuditIndeterminate, document.Complete, "%+v", document.Issues)
		})
	}
}

func TestParseForTextAuditValidatesOpenAIImageEnvelopesWithoutRetainingPayloads(t *testing.T) {
	tests := []struct {
		name      string
		protocol  string
		body      string
		wantText  string
		hasImages bool
	}{
		{
			name:      "responses direct image item",
			protocol:  ProtocolOpenAIResponses,
			wantText:  "auditable text",
			hasImages: true,
			body: `{"input":[
				{"type":"input_text","text":"auditable text"},
				{"type":"input_image","image_url":{"url":"ignored-image-payload"}}
			]}`,
		},
		{
			name:      "responses image content part",
			protocol:  ProtocolOpenAIResponses,
			wantText:  "auditable text",
			hasImages: true,
			body: `{"input":[{"type":"message","role":"user","content":[
				{"type":"input_text","text":"auditable text"},
				{"type":"input_image","image_url":"ignored-image-payload"}
			]}]}`,
		},
		{
			name:      "chat image content part",
			protocol:  ProtocolOpenAIChat,
			wantText:  "auditable text",
			hasImages: true,
			body: `{"messages":[{"role":"user","content":[
				{"type":"text","text":"auditable text"},
				{"type":"image_url","image_url":{"url":"ignored-image-payload"}}
			]}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			document := ParseForTextAudit(tt.protocol, []byte(tt.body))

			require.True(t, document.Complete, "%+v", document.Issues)
			require.Equal(t, tt.hasImages, document.HasImages)
			require.Empty(t, document.Media)
			require.Equal(t, tt.wantText, document.NormalizedText)
			serialized, err := json.Marshal(document)
			require.NoError(t, err)
			require.NotContains(t, string(serialized), "ignored-image-payload")
		})
	}

	malformed := []string{
		`{"input":[{"type":"input_image","image_url":{"unexpected":"hidden"}}]}`,
		`{"input":[{"type":"message","role":"user","content":[{"type":"input_image","image_url":42}]}]}`,
	}
	for _, body := range malformed {
		document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(body))
		require.False(t, document.Complete)
		require.Equal(t, TextAuditIndeterminate, document.TextAuditClass)
	}
}

func TestParseForTextAuditResponsesAuditsOnlyLatestUserText(t *testing.T) {
	body := []byte(`{
		"instructions":"ignored system instructions",
		"tools":[{"type":"function","name":"ignored_root_tool","future_definition_field":"ignored"}],
		"input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"historical user text","future_text_field":"ignored"}]},
			{"type":"custom_tool_call","name":"ignored_tool","input":"ignored tool input","future_tool_field_1":true,"future_tool_field_2":true},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ignored assistant text"}],"future_assistant_field":true},
			{"type":"future_non_text_item","payload":{"secret":"ignored unknown payload"}},
			{"type":"message","role":"assistant","content":"ignored trailing assistant","future_assistant_field":true},
			{"type":"future_trailing_control","payload":"ignored trailing control"},
			{"type":"message","role":"user","content":[
				{"type":"input_text","text":"current user text"},
				{"type":"input_image","image_url":{"url":"ignored image payload"}}
			]}
		]
	}`)

	document := ParseForTextAudit(ProtocolOpenAIResponses, body)

	require.True(t, document.Complete, "%+v", document.Issues)
	require.Empty(t, document.Issues)
	require.Equal(t, "current user text", document.NormalizedText)
	require.True(t, document.HasImages)
	require.Empty(t, document.Media)
	serialized, err := json.Marshal(document)
	require.NoError(t, err)
	for _, ignored := range []string{
		"historical user text", "ignored system instructions", "ignored_root_tool", "ignored_tool",
		"ignored assistant text", "ignored unknown payload", "ignored image payload",
	} {
		require.NotContains(t, string(serialized), ignored)
	}

	fullDocument := Parse(ProtocolOpenAIResponses, body)
	require.False(t, fullDocument.Complete)
	require.True(t, fullDocument.HasIssue(IssueUnknownField), "%+v", fullDocument.Issues)
}

func TestParseForTextAuditChatAuditsOnlyLatestUserText(t *testing.T) {
	body := []byte(`{
		"tools":[{"type":"function","function":{"name":"ignored_root_tool","future_definition_field":"ignored"}}],
		"messages":[
			{"role":"system","content":"ignored system text","future_system_field":true},
			{"role":"user","content":[{"type":"text","text":"historical user text","future_text_field":"ignored"}]},
			{"role":"assistant","content":"ignored assistant text","future_assistant_field":true},
			{"role":"tool","content":"ignored tool output","future_tool_field_1":true,"future_tool_field_2":true},
			{"role":"assistant","content":"ignored trailing assistant","future_trailing_field":true},
			{"role":"user","content":[
				{"type":"text","text":"current user text"},
				{"type":"image_url","image_url":{"url":"ignored image payload"}}
			]}
		]
	}`)

	document := ParseForTextAudit(ProtocolOpenAIChat, body)

	require.True(t, document.Complete, "%+v", document.Issues)
	require.Empty(t, document.Issues)
	require.Equal(t, "current user text", document.NormalizedText)
	require.True(t, document.HasImages)
	require.Empty(t, document.Media)
	serialized, err := json.Marshal(document)
	require.NoError(t, err)
	for _, ignored := range []string{
		"ignored_root_tool", "ignored system text", "historical user text", "ignored assistant text",
		"ignored tool output", "ignored image payload", "ignored trailing assistant",
	} {
		require.NotContains(t, string(serialized), ignored)
	}

	fullDocument := Parse(ProtocolOpenAIChat, body)
	require.False(t, fullDocument.Complete)
	require.True(t, fullDocument.HasIssue(IssueUnknownField), "%+v", fullDocument.Issues)
}

func TestParseForTextAuditDoesNotRewindPastFinalBusinessItem(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		body     string
	}{
		{
			name:     "responses assistant",
			protocol: ProtocolOpenAIResponses,
			body: `{"input":[
				{"type":"message","role":"user","content":"historical user text"},
				{"type":"message","role":"assistant","content":"ignored assistant output"}
			]}`,
		},
		{
			name:     "chat assistant",
			protocol: ProtocolOpenAIChat,
			body: `{"messages":[
				{"role":"user","content":"historical user text"},
				{"role":"assistant","content":"ignored assistant output"}
			]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := ParseForTextAudit(test.protocol, []byte(test.body))

			require.True(t, document.Complete, "%+v", document.Issues)
			require.Empty(t, document.NormalizedText)
			require.False(t, document.HasImages)
			require.Empty(t, document.Media)
			require.NotEmpty(t, document.ControlItems)
			serialized, err := json.Marshal(document)
			require.NoError(t, err)
			require.NotContains(t, string(serialized), "historical user text")
			require.NotContains(t, string(serialized), "ignored")
		})
	}

	invalid := []string{
		`{"input":[{"type":"message","role":"user","content":"history"},{"type":"future_business_item","payload":"opaque"}]}`,
		`{"input":[{"type":"message","role":"user","content":"history"},null]}`,
		`{"input":[{"type":"message","role":"user","content":"history"},42]}`,
		`{"input":[{"type":"message","role":"user","content":"history"},{"type":"agent_message","role":null,"content":[{"type":"output_text","text":"assistant output"}]}]}`,
	}
	for _, body := range invalid {
		document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(body))
		require.False(t, document.Complete)
		require.Equal(t, TextAuditIndeterminate, document.TextAuditClass)
	}

	tool := ParseForTextAudit(ProtocolOpenAIChat, []byte(`{"messages":[{"role":"user","content":"history"},{"role":"tool","content":"tool output"}]}`))
	require.True(t, tool.Complete, "%+v", tool.Issues)
	require.Equal(t, "tool output", tool.NormalizedText)
	require.Equal(t, TextAuditAuditableText, tool.TextAuditClass)
}

func TestParseForTextAuditAuditsTrailingResponsesToolSuffixOnly(t *testing.T) {
	body := []byte(`{"previous_response_id":"resp_parent","input":[
		{"type":"message","role":"user","content":"historical user text"},
		{"type":"message","role":"assistant","content":"historical assistant text"},
		{"type":"function_call","name":"exec","arguments":"{\"cmd\":\"dangerous command\"}","call_id":"call_1"},
		{"type":"function_call_output","call_id":"call_1","output":"tool output"},
		{"type":"compaction_trigger"}
	]}`)

	document := ParseForTextAudit(ProtocolOpenAIResponses, body)

	require.True(t, document.Complete, "%+v", document.Issues)
	require.Contains(t, document.NormalizedText, "exec")
	require.Contains(t, document.NormalizedText, "dangerous command")
	require.Contains(t, document.NormalizedText, "tool output")
	require.NotContains(t, document.NormalizedText, "historical user text")
	require.NotContains(t, document.NormalizedText, "historical assistant text")
}

func TestParseForTextAuditRejectsDuplicateFieldsInSelectedResponsesTool(t *testing.T) {
	document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(
		`{"input":[{"type":"function_call_output","call_id":"call_1","output":"first","output":"second"}]}`,
	))

	require.False(t, document.Complete)
	require.True(t, document.HasIssue(IssueDuplicateField), "%+v", document.Issues)
}

func TestParseForTextAuditTracksFullTextBeyondFormerBoundary(t *testing.T) {
	text := strings.Repeat("界", 12_001)
	document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(`{"input":"`+text+`"}`))

	require.True(t, document.Complete, "%+v", document.Issues)
	require.Equal(t, TextAuditAuditableText, document.TextAuditClass)
	require.Equal(t, text, document.NormalizedText)
	require.Equal(t, 12_001, document.AuditTextRunes)
}

func TestParseForTextAuditRejectsMalformedCurrentTextBoundary(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		body     string
		issue    string
	}{
		{
			name:     "responses input text null role",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"input_text","role":null,"text":"blocked current text"}]}`,
			issue:    IssueInvalidShape,
		},
		{
			name:     "responses message numeric role",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"message","role":42,"content":"blocked current text"}]}`,
			issue:    IssueInvalidShape,
		},
		{
			name:     "responses unknown user text type",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"future_text","role":"user","text":"blocked current text"}]}`,
			issue:    IssueUnknownType,
		},
		{
			name:     "responses unknown implicit text type",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"future_text","content":"blocked current text"}]}`,
			issue:    IssueUnknownType,
		},
		{
			name:     "chat missing role",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"content":"blocked current text"}]}`,
			issue:    IssueInvalidShape,
		},
		{
			name:     "chat null role",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":null,"content":"blocked current text"}]}`,
			issue:    IssueInvalidShape,
		},
		{
			name:     "chat numeric role",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":42,"content":"blocked current text"}]}`,
			issue:    IssueInvalidShape,
		},
		{
			name:     "chat unknown role",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"future_role","content":"blocked current text"}]}`,
			issue:    IssueUnknownRole,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := ParseForTextAudit(test.protocol, []byte(test.body))

			require.False(t, document.Complete)
			require.True(t, document.HasIssue(test.issue), "%+v", document.Issues)
		})
	}
}

func TestParseForTextAuditTrailingCompactionOnlySkipsTransparentControl(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "current user then compaction",
			body: `{"input":[
				{"type":"message","role":"user","content":"current user text"},
				{"type":"compaction_trigger"}
			]}`,
			want: "current user text",
		},
		{
			name: "tool output then compaction",
			body: `{"input":[
				{"type":"message","role":"user","content":"historical user text"},
				{"type":"function_call_output","call_id":"call_1","output":"ignored tool output"},
				{"type":"compaction_trigger"}
			]}`,
			want: "ignored tool output",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(test.body))

			require.True(t, document.Complete, "%+v", document.Issues)
			require.Equal(t, test.want, document.NormalizedText)
			require.NotEmpty(t, document.ControlItems)
		})
	}

	malformed := ParseForTextAudit(ProtocolOpenAIResponses, []byte(
		`{"input":[{"type":"compaction_trigger","future_control_field":"hidden"}]}`,
	))
	require.False(t, malformed.Complete)
	require.Equal(t, TextAuditIndeterminate, malformed.TextAuditClass)
	require.True(t, malformed.HasIssue(IssueUnknownField), "%+v", malformed.Issues)

	validMetadata := ParseForTextAudit(ProtocolOpenAIResponses, []byte(
		`{"input":[{"type":"compaction_trigger","internal_chat_message_metadata_passthrough":{"turn_id":"turn_1"}}]}`,
	))
	require.True(t, validMetadata.Complete, "%+v", validMetadata.Issues)
	require.Equal(t, TextAuditKnownNoText, validMetadata.TextAuditClass)

	for _, body := range []string{
		`{"input":[{"type":"compaction_trigger","internal_chat_message_metadata_passthrough":null}]}`,
		`{"input":[{"type":"compaction_trigger","internal_chat_message_metadata_passthrough":{"turn_id":""}}]}`,
		`{"input":[{"type":"compaction_trigger","internal_chat_message_metadata_passthrough":{"turn_id":"turn_1","future":"hidden"}}]}`,
		`{"input":[{"type":"compaction_trigger","internal_chat_message_metadata_passthrough":{"turn_id":"first","turn_id":"second"}}]}`,
	} {
		document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(body))
		require.False(t, document.Complete, "%+v", document.Issues)
		require.Equal(t, TextAuditIndeterminate, document.TextAuditClass)
	}
}

func TestParseForTextAuditMatchesForwardSanitizerAtResponsesBoundary(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantText  string
		hasImages bool
	}{
		{
			name: "top-level empty image is removed before forwarding",
			body: `{"input":[
				{"type":"message","role":"user","content":"current user text"},
				{"type":"input_image","image_url":"data:image/png;base64,   "}
			]}`,
			wantText: "current user text",
		},
		{
			name: "empty image-only message is removed before forwarding",
			body: `{"input":[
				{"type":"message","role":"user","content":"current user text"},
				{"type":"message","role":"user","content":[
					{"type":"input_image","image_url":"data:image/png;base64,"}
				]}
			]}`,
			wantText: "current user text",
		},
		{
			name: "removed empty image does not cross tool output",
			body: `{"input":[
				{"type":"message","role":"user","content":"historical user text"},
				{"type":"function_call_output","call_id":"call_1","output":"done"},
				{"type":"input_image","image_url":"data:image/png;base64,"}
			]}`,
			wantText: "done",
		},
		{
			name: "real image remains the final current input",
			body: `{"input":[
				{"type":"message","role":"user","content":"historical user text"},
				{"type":"input_image","image_url":"data:image/png;base64,AA=="}
			]}`,
			hasImages: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(test.body))

			require.True(t, document.Complete, "%+v", document.Issues)
			require.Equal(t, test.wantText, document.NormalizedText)
			require.Equal(t, test.hasImages, document.HasImages)
			require.Empty(t, document.Media)
		})
	}

	for _, body := range []string{
		`{"input":[{"type":"input_image","image_url":"data:image/png;base64,","future":"hidden"}]}`,
		`{"input":[{"type":"input_image","image_url":"data:image/png;base64,","image_url":"data:image/png;base64,"}]}`,
	} {
		document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(body))
		require.False(t, document.Complete, "%+v", document.Issues)
		require.Equal(t, TextAuditIndeterminate, document.TextAuditClass)
	}
}

func TestParseForTextAuditResponsesAllowsEmptyTurnsWithoutOwningUpstreamSchema(t *testing.T) {
	allowed := []string{
		`{"model":"gpt-5.6-sol"}`,
		`{"model":"gpt-5.6-sol","input":null}`,
		`{"model":"gpt-5.6-sol","input":[]}`,
		`{"type":"response.create","model":"gpt-5.6-sol"}`,
		`{"type":"response.create","model":"gpt-5.6-sol","input":null}`,
		`{"type":"response.create","model":"gpt-5.6-sol","input":[]}`,
		`{"model":"gpt-5.6-sol","previous_response_id":"resp_parent"}`,
		`{"model":"gpt-5.6-sol","previous_response_id":"resp_parent","input":null}`,
		`{"model":"gpt-5.6-sol","previous_response_id":"resp_parent","input":[]}`,
	}
	for index, body := range allowed {
		t.Run("allowed "+string(rune('a'+index)), func(t *testing.T) {
			document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(body))

			require.True(t, document.Complete, "%+v", document.Issues)
			require.Empty(t, document.NormalizedText)
			require.NotEmpty(t, document.ControlItems)
			fullDocument := Parse(ProtocolOpenAIResponses, []byte(body))
			require.False(t, fullDocument.Complete, "full parsing remains responsible for request schema")
			require.True(t, fullDocument.HasIssue(IssueEmptyContent), "%+v", fullDocument.Issues)
		})
	}
}

func TestParseForTextAuditChatAllowsEmptyTurnsWithoutOwningUpstreamSchema(t *testing.T) {
	bodies := []string{
		`{"model":"gpt-5.6-sol"}`,
		`{"model":"gpt-5.6-sol","messages":null}`,
		`{"model":"gpt-5.6-sol","messages":[]}`,
	}
	for index, body := range bodies {
		t.Run("empty "+string(rune('a'+index)), func(t *testing.T) {
			document := ParseForTextAudit(ProtocolOpenAIChat, []byte(body))

			require.True(t, document.Complete, "%+v", document.Issues)
			require.Empty(t, document.NormalizedText)
			require.NotEmpty(t, document.ControlItems)
			fullDocument := Parse(ProtocolOpenAIChat, []byte(body))
			require.False(t, fullDocument.Complete, "full parsing remains responsible for request schema")
		})
	}
}

func TestParseForTextAuditScopesDuplicateFieldsToSelectedText(t *testing.T) {
	ignoredDuplicates := []string{
		`{"input":[
			{"type":"message","role":"user","content":[{"type":"input_image","image_url":"first","image_url":"second"}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"current user text"}]}
		]}`,
		`{"input":[
			{"type":"function_call_output","call_id":"call_1","output":"first","output":"second"},
			{"type":"message","role":"user","content":"current user text"}
		]}`,
	}
	for index, body := range ignoredDuplicates {
		t.Run("ignored duplicate "+string(rune('a'+index)), func(t *testing.T) {
			document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(body))

			require.True(t, document.Complete, "%+v", document.Issues)
			require.Equal(t, "current user text", document.NormalizedText)
			require.False(t, document.HasIssue(IssueDuplicateField))
			require.False(t, Parse(ProtocolOpenAIResponses, []byte(body)).Complete, "full parsing remains globally strict")
		})
	}

	selectedDuplicates := []string{
		`{"input":[{"type":"message","type":"message","role":"user","content":"current user text"}]}`,
		`{"input":[{"type":"message","role":"user","role":"user","content":"current user text"}]}`,
		`{"input":[{"role":"user","role":"assistant","content":"blocked current text"}]}`,
		`{"input":[{"type":"input_text","type":"compaction_trigger","text":"blocked current text"}]}`,
		`{"input":[{"type":"message","role":"user","content":"first","content":"second"}]}`,
		`{"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"first","text":"second"}]}]}`,
		`{"input":[{"type":"message","role":"user","content":[{"type":"input_text","type":"input_image","text":"blocked current text","image_url":"ignored"}]}]}`,
		`{"input":[{"type":"input_text","type":"input_text","text":"current user text"}]}`,
		`{"input":[{"type":"input_text","type":"function_call_output","text":"blocked current text","output":"ok"}]}`,
		`{"input":[{"type":"input_text","text":"first","text":"second"}]}`,
		`{"input":[
			{"type":"message","role":"assistant","content":"historical assistant text"},
			{"type":"input_text","text":"current user text"},
			{"type":"input_image","image_url":"first","image_url":"second"}
		]}`,
		`{"input":"first","input":"second"}`,
		`{"messages":[{"role":"user","role":"user","content":"current user text"}]}`,
		`{"messages":[{"role":"user","content":[{"type":"text","text":"first","text":"second"}]}]}`,
		`{"messages":[{"role":"user","content":[{"type":"text","type":"image_url","text":"blocked current text","image_url":{"url":"ignored"}}]}]}`,
	}
	for index, body := range selectedDuplicates {
		protocol := ProtocolOpenAIResponses
		if index >= len(selectedDuplicates)-3 {
			protocol = ProtocolOpenAIChat
		}
		t.Run("selected duplicate "+string(rune('a'+index)), func(t *testing.T) {
			document := ParseForTextAudit(protocol, []byte(body))

			require.False(t, document.Complete)
			require.True(t, document.HasIssue(IssueDuplicateField), "%+v", document.Issues)
		})
	}
}

func TestParseForTextAuditRejectsDuplicatesInsideCurrentImageNodes(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		body     string
	}{
		{
			name:     "responses direct image",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"input_image","type":"input_image","image_url":"first","image_url":"second"}]}`,
		},
		{
			name:     "responses image content part",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"message","role":"user","content":[{"type":"input_image","type":"input_image","image_url":"first","image_url":"second"}]}]}`,
		},
		{
			name:     "chat image content part",
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"user","content":[{"type":"image_url","type":"image_url","image_url":{"url":"first","url":"second"}}]}]}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := ParseForTextAudit(test.protocol, []byte(test.body))

			require.False(t, document.Complete, "%+v", document.Issues)
			require.True(t, document.HasIssue(IssueDuplicateField), "%+v", document.Issues)
			require.Empty(t, document.NormalizedText)
			require.Equal(t, TextAuditIndeterminate, document.TextAuditClass)
		})
	}
}

func TestParseForTextAuditRejectsAmbiguousFinalOpaqueItem(t *testing.T) {
	bodies := []string{
		`{"input":[{"type":"message","role":"assistant","role":"assistant","content":"first","content":"second"}]}`,
		`{"input":[{"type":"compaction_trigger","type":"compaction_trigger","future_control_field":"first","future_control_field":"second"}]}`,
	}

	for index, body := range bodies {
		t.Run("opaque "+string(rune('a'+index)), func(t *testing.T) {
			document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(body))

			require.False(t, document.Complete, "%+v", document.Issues)
			require.Equal(t, TextAuditIndeterminate, document.TextAuditClass)
			require.Empty(t, document.NormalizedText)
		})
	}
}

func TestParseForTextAuditResponsesRejectsUnknownNonTextOnlyInput(t *testing.T) {
	body := []byte(`{"input":[
		{"type":"function_call_output","call_id":"call_1","output":"ignored tool output","future_tool_field_1":true,"future_tool_field_2":true},
		{"type":"message","role":"assistant","content":"ignored assistant output","future_assistant_field":true},
		{"type":"future_non_text_item","payload":"ignored unknown payload"},
		{"type":"compaction_trigger","future_control_field":"ignored control payload"}
	]}`)

	document := ParseForTextAudit(ProtocolOpenAIResponses, body)

	require.False(t, document.Complete, "%+v", document.Issues)
	require.Equal(t, TextAuditIndeterminate, document.TextAuditClass)
	require.Empty(t, document.NormalizedText)
	require.NotEmpty(t, document.ControlItems)
	serialized, err := json.Marshal(document)
	require.NoError(t, err)
	for _, ignored := range []string{
		"ignored tool output", "ignored assistant output", "ignored unknown payload", "ignored control payload",
	} {
		require.NotContains(t, string(serialized), ignored)
	}
}

func TestParseForTextAuditChatAuditsToolOutputAndRejectsUnknownFields(t *testing.T) {
	body := []byte(`{"messages":[
		{"role":"assistant","content":"ignored assistant output","future_assistant_field":true},
		{"role":"tool","content":"ignored tool output","future_tool_field_1":true,"future_tool_field_2":true}
	]}`)

	document := ParseForTextAudit(ProtocolOpenAIChat, body)

	require.False(t, document.Complete, "%+v", document.Issues)
	require.Equal(t, TextAuditIndeterminate, document.TextAuditClass)
	require.Equal(t, "ignored tool output", document.NormalizedText)
	serialized, err := json.Marshal(document)
	require.NoError(t, err)
	require.NotContains(t, string(serialized), "ignored assistant output")
	require.Contains(t, string(serialized), "ignored tool output")
}

func TestParseForTextAuditAuditsContiguousChatToolSuffixInOrder(t *testing.T) {
	document := ParseForTextAudit(ProtocolOpenAIChat, []byte(`{"messages":[
		{"role":"user","content":"historical user text"},
		{"role":"assistant","content":"historical assistant text"},
		{"role":"tool","tool_call_id":"call_1","content":"first tool output"},
		{"role":"function","name":"legacy_tool","content":"second tool output"},
		{"role":"tool","tool_call_id":"call_2","content":"third tool output"}
	]}`))

	require.True(t, document.Complete, "%+v", document.Issues)
	require.Equal(t, "first tool output\nsecond tool output\nthird tool output", document.NormalizedText)
	require.Equal(t, TextAuditAuditableText, document.TextAuditClass)
	require.NotContains(t, document.NormalizedText, "historical")
}

func TestParseForTextAuditStrictlyValidatesKnownNoTextChatTail(t *testing.T) {
	valid := ParseForTextAudit(ProtocolOpenAIChat, []byte(
		`{"messages":[{"role":"user","content":"history"},{"role":"assistant","content":"assistant output"}]}`,
	))
	require.True(t, valid.Complete, "%+v", valid.Issues)
	require.Equal(t, TextAuditKnownNoText, valid.TextAuditClass)
	require.Empty(t, valid.NormalizedText)

	for _, body := range []string{
		`{"messages":[{"role":"assistant","content":42}]}`,
		`{"messages":[{"role":"assistant","content":"output","future":"hidden"}]}`,
		`{"messages":[{"role":"assistant","role":"assistant","content":"output"}]}`,
	} {
		document := ParseForTextAudit(ProtocolOpenAIChat, []byte(body))
		require.False(t, document.Complete, "%+v", document.Issues)
		require.Equal(t, TextAuditIndeterminate, document.TextAuditClass)
	}
}

func TestParseForTextAuditStillRejectsOpenAINonImageUnknownFields(t *testing.T) {
	tests := []struct {
		protocol string
		body     string
	}{
		{
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"input_text","text":"auditable text","future_text_field":"hidden text"}]}`,
		},
		{
			protocol: ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"user","content":[{"type":"text","text":"auditable text","future_text_field":"hidden text"}]}]}`,
		},
	}
	for _, test := range tests {
		t.Run(test.protocol, func(t *testing.T) {
			document := ParseForTextAudit(test.protocol, []byte(test.body))

			require.False(t, document.Complete)
			require.False(t, document.HasImages)
			require.True(t, document.HasIssue(IssueUnknownField), "%+v", document.Issues)
		})
	}
}

func TestParseForTextAuditRejectsUnknownTextBearingContentParts(t *testing.T) {
	textBearing := []string{
		`{"input":[{"type":"message","role":"user","content":[{"type":"future_part","text":"hidden text"}]}]}`,
		`{"input":[{"type":"message","role":"user","content":[{"type":"future_part","content":"hidden text"}]}]}`,
		`{"input":[{"type":"message","role":"user","content":[{"type":"future_part","refusal":"hidden text"}]}]}`,
	}
	for _, body := range textBearing {
		document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(body))

		require.False(t, document.Complete)
		require.True(t, document.HasIssue(IssueUnknownType), "%+v", document.Issues)
	}

	opaque := ParseForTextAudit(ProtocolOpenAIResponses, []byte(
		`{"input":[{"type":"message","role":"user","content":[{"type":"future_part","payload":"opaque"}]}]}`,
	))
	require.False(t, opaque.Complete, "%+v", opaque.Issues)
	require.Equal(t, TextAuditIndeterminate, opaque.TextAuditClass)
	require.Empty(t, opaque.NormalizedText)
}
