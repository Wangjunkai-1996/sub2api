package auditinput

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseForTextAuditSkipsEntireOpenAIImageNodes(t *testing.T) {
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
				{"type":"input_image","id":42,"image_url":{"unexpected":"ignored-image-payload"},"future_image_field":true}
			]}`,
		},
		{
			name:      "responses image content part",
			protocol:  ProtocolOpenAIResponses,
			wantText:  "auditable text",
			hasImages: true,
			body: `{"input":[{"type":"message","role":"user","content":[
				{"type":"input_text","text":"auditable text"},
				{"type":"input_image","image_url":42,"future_image_field":{"payload":"ignored-image-payload"}}
			]}]}`,
		},
		{
			name:      "chat image content part",
			protocol:  ProtocolOpenAIChat,
			wantText:  "auditable text",
			hasImages: true,
			body: `{"messages":[{"role":"user","content":[
				{"type":"text","text":"auditable text"},
				{"type":"image_url","image_url":false,"future_image_field":"ignored-image-payload"}
			]}]}`,
		},
		{
			name:     "responses computer output is outside user text audit",
			protocol: ProtocolOpenAIResponses,
			body: `{"input":[
				{"type":"input_text","text":"auditable text"},
				{"type":"computer_call_output","id":false,"output":{"future_image_field":"ignored-image-payload"}}
			]}`,
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
				{"type":"future_non_text_part","payload":"ignored current-turn control"},
				{"type":"input_image","image_url":{"malformed":"ignored image payload"},"future_image_field":true}
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
		"ignored assistant text", "ignored unknown payload", "ignored current-turn control", "ignored image payload",
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
				{"type":"future_non_text_part","payload":"ignored current-turn control"},
				{"type":"image_url","image_url":{"malformed":"ignored image payload"},"future_image_field":true}
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
		"ignored tool output", "ignored current-turn control", "ignored image payload", "ignored trailing assistant",
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
			name:     "responses unknown business item",
			protocol: ProtocolOpenAIResponses,
			body: `{"input":[
				{"type":"message","role":"user","content":"historical user text"},
				{"type":"future_business_item","payload":"ignored unknown output"}
			]}`,
		},
		{
			name:     "responses null tail",
			protocol: ProtocolOpenAIResponses,
			body: `{"input":[
				{"type":"message","role":"user","content":"historical user text"},
				null
			]}`,
		},
		{
			name:     "responses numeric tail",
			protocol: ProtocolOpenAIResponses,
			body: `{"input":[
				{"type":"message","role":"user","content":"historical user text"},
				42
			]}`,
		},
		{
			name:     "responses known non-user item with malformed role",
			protocol: ProtocolOpenAIResponses,
			body: `{"input":[
				{"type":"message","role":"user","content":"historical user text"},
				{"type":"agent_message","role":null,"content":[{"type":"output_text","text":"ignored assistant output"}]}
			]}`,
		},
		{
			name:     "chat tool",
			protocol: ProtocolOpenAIChat,
			body: `{"messages":[
				{"role":"user","content":"historical user text"},
				{"role":"tool","content":"ignored tool output"}
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

func TestParseForTextAuditTracksTwelveThousandRuneBoundary(t *testing.T) {
	atLimit := ParseForTextAudit(ProtocolOpenAIResponses, []byte(`{"input":"`+strings.Repeat("界", MaxAuditTextRunes)+`"}`))
	require.True(t, atLimit.Complete, "%+v", atLimit.Issues)
	require.Equal(t, MaxAuditTextRunes, atLimit.AuditTextRunes)
	require.False(t, atLimit.AuditLimitExceeded)

	overLimit := ParseForTextAudit(ProtocolOpenAIResponses, []byte(`{"input":"`+strings.Repeat("界", MaxAuditTextRunes+1)+`"}`))
	require.True(t, overLimit.Complete, "%+v", overLimit.Issues)
	require.Equal(t, MaxAuditTextRunes+1, overLimit.AuditTextRunes)
	require.True(t, overLimit.AuditLimitExceeded)
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
				{"type":"compaction_trigger","future_control_field":"ignored"}
			]}`,
			want: "current user text",
		},
		{
			name: "tool output then compaction",
			body: `{"input":[
				{"type":"message","role":"user","content":"historical user text"},
				{"type":"function_call_output","call_id":"call_1","output":"ignored tool output"},
				{"type":"compaction_trigger","future_control_field":"ignored"}
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
		`{"input":[
			{"type":"message","role":"assistant","content":"first","content":"second"},
			{"type":"input_text","text":"current user text"},
			{"type":"input_image","image_url":"first","image_url":"second"}
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

func TestParseForTextAuditAllowsDuplicatesInsideCurrentImageNodes(t *testing.T) {
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

			require.True(t, document.Complete, "%+v", document.Issues)
			require.False(t, document.HasIssue(IssueDuplicateField), "%+v", document.Issues)
			require.Empty(t, document.NormalizedText)
			require.True(t, document.HasImages)
		})
	}
}

func TestParseForTextAuditIgnoresOrdinaryDuplicatesInFinalOpaqueItem(t *testing.T) {
	bodies := []string{
		`{"input":[{"type":"message","role":"assistant","role":"assistant","content":"first","content":"second"}]}`,
		`{"input":[{"type":"compaction_trigger","type":"compaction_trigger","future_control_field":"first","future_control_field":"second"}]}`,
	}

	for index, body := range bodies {
		t.Run("opaque "+string(rune('a'+index)), func(t *testing.T) {
			document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(body))

			require.True(t, document.Complete, "%+v", document.Issues)
			require.False(t, document.HasIssue(IssueDuplicateField), "%+v", document.Issues)
			require.Empty(t, document.NormalizedText)
			require.NotEmpty(t, document.ControlItems)
		})
	}
}

func TestParseForTextAuditResponsesKeepsNonTextOnlyInputOpaqueAndComplete(t *testing.T) {
	body := []byte(`{"input":[
		{"type":"function_call_output","call_id":"call_1","output":"ignored tool output","future_tool_field_1":true,"future_tool_field_2":true},
		{"type":"message","role":"assistant","content":"ignored assistant output","future_assistant_field":true},
		{"type":"future_non_text_item","payload":"ignored unknown payload"},
		{"type":"compaction_trigger","future_control_field":"ignored control payload"}
	]}`)

	document := ParseForTextAudit(ProtocolOpenAIResponses, body)

	require.True(t, document.Complete, "%+v", document.Issues)
	require.Empty(t, document.Issues)
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

func TestParseForTextAuditChatKeepsNonTextOnlyInputOpaqueAndComplete(t *testing.T) {
	body := []byte(`{"messages":[
		{"role":"assistant","content":"ignored assistant output","future_assistant_field":true},
		{"role":"tool","content":"ignored tool output","future_tool_field_1":true,"future_tool_field_2":true}
	]}`)

	document := ParseForTextAudit(ProtocolOpenAIChat, body)

	require.True(t, document.Complete, "%+v", document.Issues)
	require.Empty(t, document.Issues)
	require.Empty(t, document.NormalizedText)
	require.NotEmpty(t, document.ControlItems)
	serialized, err := json.Marshal(document)
	require.NoError(t, err)
	require.NotContains(t, string(serialized), "ignored assistant output")
	require.NotContains(t, string(serialized), "ignored tool output")
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
	require.True(t, opaque.Complete, "%+v", opaque.Issues)
	require.Empty(t, opaque.NormalizedText)
	require.NotEmpty(t, opaque.ControlItems)
}
