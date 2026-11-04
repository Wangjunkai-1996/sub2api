package service

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpenAIRecoveryStructuredOutput(t *testing.T) {
	for _, test := range []struct {
		name    string
		request string
		mode    OpenAIRecoveryWireFormat
		output  string
		valid   bool
	}{
		{"plain text", `{"input":"hello"}`, OpenAIRecoveryResponsesSSE, "normal streaming text", true},
		{"JSON object", `{"text":{"format":{"type":"json_object"}}}`, OpenAIRecoveryResponsesSSE, `{"answer":1}`, true},
		{"JSON array is not object", `{"text":{"format":{"type":"json_object"}}}`, OpenAIRecoveryResponsesSSE, `[]`, false},
		{"truncated object", `{"text":{"format":{"type":"json_object"}}}`, OpenAIRecoveryResponsesSSE, `{"answer":`, false},
		{"multiple documents", `{"text":{"format":{"type":"json_object"}}}`, OpenAIRecoveryResponsesSSE, `{} {}`, false},
		{"schema valid", `{"response_format":{"type":"json_schema","json_schema":{"schema":{"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}}}}`, OpenAIRecoveryChatSSE, `{"answer":42}`, true},
		{"schema mismatch", `{"text":{"format":{"type":"json_schema","schema":{"type":"object","properties":{"answer":{"type":"integer"}},"required":["answer"],"additionalProperties":false}}}}`, OpenAIRecoveryResponsesSSE, `{"answer":"wrong"}`, false},
		{"no remote references", `{"text":{"format":{"type":"json_schema","schema":{"$ref":"http://127.0.0.1:1/private"}}}}`, OpenAIRecoveryResponsesSSE, `{}`, false},
		{"no file references", `{"text":{"format":{"type":"json_schema","schema":{"$ref":"file:///etc/passwd"}}}}`, OpenAIRecoveryResponsesSSE, `{}`, false},
		{"local definitions", `{"text":{"format":{"type":"json_schema","schema":{"$defs":{"answer":{"type":"integer"}},"$ref":"#/$defs/answer"}}}}`, OpenAIRecoveryResponsesJSON, `42`, true},
		{"legacy Responses response_format", `{"response_format":{"type":"json_schema","json_schema":{"schema":{"type":"integer"}}}}`, OpenAIRecoveryResponsesSSE, `"wrong"`, false},
		{"legacy nested schema", `{"text":{"format":{"type":"json_schema","json_schema":{"schema":{"type":"integer"}}}}}`, OpenAIRecoveryResponsesSSE, `42`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateOpenAIRecoveryStructuredOutput([]byte(test.request), test.mode, test.output)
			if test.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrOpenAIRecoveryStructuredOutput)
			}
		})
	}
}

func TestOpenAIRecoveryToolArguments(t *testing.T) {
	for _, request := range []string{
		`{"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"],"additionalProperties":false}}]}`,
		`{"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"],"additionalProperties":false}}}]}`,
	} {
		require.NoError(t, ValidateOpenAIRecoveryToolArguments([]byte(request), "lookup", `{"id":1}`))
		require.ErrorIs(t, ValidateOpenAIRecoveryToolArguments([]byte(request), "lookup", `{"id":"wrong"}`), ErrOpenAIRecoveryStructuredOutput)
		require.ErrorIs(t, ValidateOpenAIRecoveryToolArguments([]byte(request), "lookup", `{"id":`), ErrOpenAIRecoveryStructuredOutput)
		require.ErrorIs(t, ValidateOpenAIRecoveryToolArguments([]byte(request), "undeclared", `{"id":1}`), ErrOpenAIRecoveryStructuredOutput)
	}
}
