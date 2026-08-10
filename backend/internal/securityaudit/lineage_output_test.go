package securityaudit

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAppendResponsesOutputExtendsCumulativeContextAndHashes(t *testing.T) {
	groupID := int64(12)
	summary := AuditSummary{
		ParserVersion: "auditinput/v1", ConfigVersion: 9, APIKeyID: 7, GroupID: &groupID,
		PromptHash: strings.Repeat("a", 64), DocumentHash: strings.Repeat("b", 64),
		NormalizedContext: "prior request", RedactedContext: "prior request",
		ContextComplete: true, Verdict: AuditVerdictAllow,
	}
	augmented, err := AppendResponsesOutput(summary, []byte(`[
		{"type":"message","role":"assistant","content":[{"type":"output_text","text":"assistant answer"}]},
		{"type":"function_call","id":"fc_1","call_id":"call_1","name":"shell","arguments":"{\"cmd\":\"sensitive command\"}"}
	]`))
	require.NoError(t, err)
	require.Contains(t, augmented.RedactedContext, "prior request")
	require.Contains(t, augmented.RedactedContext, "assistant answer")
	require.Contains(t, augmented.RedactedContext, "sensitive command")
	require.Equal(t, summary.PromptHash, augmented.ParentPromptHash)
	require.NotEqual(t, summary.PromptHash, augmented.PromptHash)
	require.NotEqual(t, summary.DocumentHash, augmented.DocumentHash)
	require.Equal(t, summary.RedactedContext, "prior request", "input summary must remain immutable")
}

func TestAppendResponsesOutputFailsClosedForUnknownMediaAndLimits(t *testing.T) {
	summary := AuditSummary{
		ParserVersion: "auditinput/v1", ConfigVersion: 9, APIKeyID: 7,
		PromptHash: strings.Repeat("a", 64), DocumentHash: strings.Repeat("b", 64),
		RedactedContext: "prior", ContextComplete: true, Verdict: AuditVerdictAllow,
	}
	tests := []struct {
		name   string
		output []byte
	}{
		{name: "unknown item", output: []byte(`[{"type":"future_item","payload":"hidden"}]`)},
		{name: "response media", output: []byte(`[{"type":"message","role":"assistant","content":[{"type":"input_image","image_url":"data:image/png;base64,aGVsbG8="}]}]`)},
		{name: "text limit", output: []byte(`[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"` + strings.Repeat("x", 65_537) + `"}]}]`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := AppendResponsesOutput(summary, tt.output)
			require.ErrorIs(t, err, ErrLineageInvalid)
		})
	}
}
