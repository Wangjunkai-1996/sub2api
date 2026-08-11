package auditinput

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseForTextAuditSkipsEntireOpenAIImageNodes(t *testing.T) {
	tests := []struct {
		name     string
		protocol string
		body     string
	}{
		{
			name:     "responses direct image item",
			protocol: ProtocolOpenAIResponses,
			body: `{"input":[
				{"type":"input_text","text":"auditable text"},
				{"type":"input_image","id":42,"image_url":{"unexpected":"ignored-image-payload"},"future_image_field":true}
			]}`,
		},
		{
			name:     "responses image content part",
			protocol: ProtocolOpenAIResponses,
			body: `{"input":[{"type":"message","role":"user","content":[
				{"type":"input_text","text":"auditable text"},
				{"type":"input_image","image_url":42,"future_image_field":{"payload":"ignored-image-payload"}}
			]}]}`,
		},
		{
			name:     "chat image content part",
			protocol: ProtocolOpenAIChat,
			body: `{"messages":[{"role":"user","content":[
				{"type":"text","text":"auditable text"},
				{"type":"image_url","image_url":false,"future_image_field":"ignored-image-payload"}
			]}]}`,
		},
		{
			name:     "responses computer screenshot output",
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
			require.True(t, document.HasImages)
			require.Empty(t, document.Media)
			require.Equal(t, "auditable text", document.NormalizedText)
			serialized, err := json.Marshal(document)
			require.NoError(t, err)
			require.NotContains(t, string(serialized), "ignored-image-payload")
		})
	}
}

func TestParseForTextAuditStillRejectsOpenAINonImageUnknownFields(t *testing.T) {
	document := ParseForTextAudit(ProtocolOpenAIResponses, []byte(`{
		"input":[{"type":"input_text","text":"auditable text","future_text_field":"hidden text"}]
	}`))

	require.False(t, document.Complete)
	require.False(t, document.HasImages)
	require.True(t, document.HasIssue(IssueUnknownField), "%+v", document.Issues)
}
