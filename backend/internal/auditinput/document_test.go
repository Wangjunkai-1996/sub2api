package auditinput

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseResponsesIncludesCompleteToolLoopAndLineage(t *testing.T) {
	doc := Parse(ProtocolOpenAIResponses, []byte(`{
		"instructions":"follow the tenant policy",
		"previous_response_id":"resp_parent",
		"tools":[{"type":"function","name":"run_tests","description":"run a selected suite","parameters":{"type":"object","properties":{"suite":{"type":"string"}}}}],
		"input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"运行测试"}]},
			{"type":"function_call","call_id":"call_1","name":"run_tests","arguments":"{\"suite\":\"security\"}"},
			{"type":"function_call_output","call_id":"call_1","output":"all passed"}
		]
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	require.Equal(t, "resp_parent", doc.PreviousResponseID)
	for _, value := range []string{"follow the tenant policy", "run_tests", "selected suite", "运行测试", "security", "all passed"} {
		require.Contains(t, doc.NormalizedText, value)
	}
	require.NotEmpty(t, doc.Hash)
	require.Equal(t, ParserVersion, doc.ParserVersion)
}

func TestParseResponsesInspectableToolOutputSupportsStoreFalseFullHistory(t *testing.T) {
	doc := Parse(ProtocolOpenAIResponses, []byte(`{
		"store":false,
		"input":[
			{"type":"function_call","call_id":"call_1","name":"shell","arguments":"{\"cmd\":\"pwd\"}"},
			{"type":"function_call_output","call_id":"call_1","output":"/workspace"},
			{"type":"message","role":"user","content":"continue"}
		]
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	require.Empty(t, doc.PreviousResponseID)
	for _, value := range []string{"shell", "pwd", "/workspace", "continue"} {
		require.Contains(t, doc.NormalizedText, value)
	}
}

func TestParseResponsesCapturesExplicitStore(t *testing.T) {
	stored := Parse(ProtocolOpenAIResponses, []byte(`{"store":true,"input":"hello"}`))
	require.True(t, stored.Complete, "%+v", stored.Issues)
	require.NotNil(t, stored.Store)
	require.True(t, *stored.Store)

	notStored := Parse(ProtocolOpenAIResponses, []byte(`{"store":false,"input":"hello"}`))
	require.True(t, notStored.Complete, "%+v", notStored.Issues)
	require.NotNil(t, notStored.Store)
	require.False(t, *notStored.Store)

	implicit := Parse(ProtocolOpenAIResponses, []byte(`{"input":"hello"}`))
	require.True(t, implicit.Complete, "%+v", implicit.Issues)
	require.Nil(t, implicit.Store)

	invalid := Parse(ProtocolOpenAIResponses, []byte(`{"store":"false","input":"hello"}`))
	require.False(t, invalid.Complete)
	require.True(t, invalid.HasIssue(IssueInvalidShape), "%+v", invalid.Issues)
	require.Contains(t, invalid.NormalizedText, "hello")
}

func TestParseResponsesIncludesMCPAndGenericToolLoops(t *testing.T) {
	doc := Parse(ProtocolOpenAIResponses, []byte(`{
		"previous_response_id":"resp_parent",
		"input":[
			{"type":"mcp_tool_call","server_label":"security","name":"inspect","arguments":"{\"target\":\"repo\"}"},
			{"type":"mcp_tool_call_output","output":"mcp clean"},
			{"type":"tool_call","function":{"name":"verify","arguments":"{\"scope\":\"all\"}"}},
			{"type":"tool_call_output","output":"tool clean"}
		]
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	for _, value := range []string{"inspect", "target", "repo", "mcp clean", "verify", "scope", "all", "tool clean"} {
		require.Contains(t, doc.NormalizedText, value)
	}
}

func TestParseChatIncludesEveryRoleToolArgumentsAndDefinition(t *testing.T) {
	doc := Parse(ProtocolOpenAIChat, []byte(`{
		"messages":[
			{"role":"system","content":"system policy"},
			{"role":"user","content":"list orders"},
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"orders","arguments":"{\"owner\":\"alice\"}"}}]},
			{"role":"tool","tool_call_id":"call_1","content":"[{\"id\":7}]"}
		],
		"tools":[{"type":"function","function":{"name":"orders","description":"lookup private orders","parameters":{"type":"object"}}}]
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	for _, value := range []string{"system policy", "list orders", "orders", "alice", "id", "lookup private orders"} {
		require.Contains(t, doc.NormalizedText, value)
	}
}

func TestParseIncludesModelVisibleNamesAndOutputConfiguration(t *testing.T) {
	tests := []struct {
		name, protocol, body string
		want                 []string
	}{
		{
			name:     "responses text config and message name",
			protocol: ProtocolOpenAIResponses,
			body:     `{"text":{"format":{"type":"json_schema","name":"security_report","schema":{"description":"include exploit details"}}},"input":[{"type":"message","role":"user","name":"trusted_operator","content":"safe"}]}`,
			want:     []string{"security_report", "include exploit details", "trusted_operator"},
		},
		{
			name:     "chat prediction and message name",
			protocol: ProtocolOpenAIChat,
			body:     `{"prediction":{"type":"content","content":"predicted unsafe payload"},"messages":[{"role":"user","name":"security_admin","content":"safe"}]}`,
			want:     []string{"predicted unsafe payload", "security_admin"},
		},
		{
			name:     "anthropic output config",
			protocol: ProtocolAnthropicMessages,
			body:     `{"max_tokens":10,"output_config":{"format":{"type":"json_schema","schema":{"description":"return restricted instructions"}}},"messages":[{"role":"user","content":"safe"}]}`,
			want:     []string{"return restricted instructions"},
		},
		{
			name:     "gemini generation config",
			protocol: ProtocolGemini,
			body:     `{"generationConfig":{"responseMimeType":"application/json","responseSchema":{"description":"expose protected material"}},"contents":[{"role":"user","parts":[{"text":"safe"}]}]}`,
			want:     []string{"expose protected material"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(tt.protocol, []byte(tt.body))
			require.True(t, doc.Complete, "%+v", doc.Issues)
			for _, value := range tt.want {
				require.Contains(t, doc.NormalizedText, value)
			}
		})
	}
}

func TestParseResponsesRejectsRemoteToolDefinitions(t *testing.T) {
	tests := []struct {
		name, tool string
	}{
		{
			name: "remote mcp server",
			tool: `{"type":"mcp","server_label":"remote","server_url":"https://example.test/mcp"}`,
		},
		{
			name: "mcp connector",
			tool: `{"type":"mcp","server_label":"connector","connector_id":"connector_123"}`,
		},
		{
			name: "file search vector store",
			tool: `{"type":"file_search","vector_store_ids":["vs_123"]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(ProtocolOpenAIResponses, []byte(`{
				"input":"safe sibling",
				"tools":[
					{"type":"function","name":"local","parameters":{"type":"object"}},
					`+tt.tool+`
				]
			}`))
			require.False(t, doc.Complete)
			require.True(t, doc.HasIssue(IssueRemoteFile), "%+v", doc.Issues)
			require.Contains(t, doc.NormalizedText, "safe sibling")
			require.Contains(t, doc.NormalizedText, "local")
		})
	}

	local := Parse(ProtocolOpenAIResponses, []byte(`{
		"input":"safe sibling",
		"tools":[{"type":"function","name":"local","parameters":{"type":"object"}}]
	}`))
	require.True(t, local.Complete, "%+v", local.Issues)
}

func TestParseAlphaSearchAuditsCompleteModelInput(t *testing.T) {
	doc := Parse("openai_alpha_search", []byte(`{
		"id":"search-session",
		"model":"gpt-5.6-sol",
		"reasoning":{"context":"all_turns"},
		"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"recent context"}]}],
		"commands":{"search_query":[{"q":"restricted query","recency":1}]},
		"settings":{"allowed_callers":["direct"],"future_nested_instruction":"do not omit this"},
		"max_output_tokens":2000
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	for _, value := range []string{"recent context", "restricted query", "future_nested_instruction", "do not omit this", "all_turns"} {
		require.Contains(t, doc.NormalizedText, value)
	}

	unknownRoot := Parse("openai_alpha_search", []byte(`{"id":"search-session","model":"gpt-5.6-sol","commands":{"search_query":[{"q":"safe"}]},"future_payload":"hidden"}`))
	require.False(t, unknownRoot.Complete)
	require.True(t, unknownRoot.HasIssue(IssueUnknownField), "%+v", unknownRoot.Issues)
}

func TestParseAnthropicIncludesToolUseToolResultAndImage(t *testing.T) {
	doc := Parse(ProtocolAnthropicMessages, []byte(`{
		"system":"system rule",
		"tools":[{"name":"weather","description":"weather lookup","input_schema":{"type":"object"}}],
		"messages":[
			{"role":"user","content":"weather please"},
			{"role":"assistant","content":[{"type":"tool_use","id":"tool_1","name":"weather","input":{"city":"Shanghai"}}]},
			{"role":"user","content":[
				{"type":"tool_result","tool_use_id":"tool_1","content":"晴 25 度"},
				{"type":"image","source":{"type":"base64","media_type":"image/png","data":"aGVsbG8="}}
			]}
		]
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	for _, value := range []string{"system rule", "weather lookup", "weather please", "Shanghai", "晴 25 度"} {
		require.Contains(t, doc.NormalizedText, value)
	}
	require.Len(t, doc.Media, 1)
	require.Equal(t, "image/png", doc.Media[0].MIMEType)
	require.NotEmpty(t, doc.Media[0].Digest)
}

func TestParseGeminiIncludesFunctionCallResponseAndInlineImage(t *testing.T) {
	doc := Parse(ProtocolGemini, []byte(`{
		"systemInstruction":{"parts":[{"text":"gemini rule"}]},
		"tools":[{"functionDeclarations":[{"name":"weather","description":"weather lookup","parameters":{"type":"object"}}]}],
		"contents":[
			{"role":"user","parts":[{"text":"query weather"}]},
			{"role":"model","parts":[{"functionCall":{"name":"weather","args":{"city":"Shanghai"}}}]},
			{"role":"user","parts":[
				{"functionResponse":{"name":"weather","response":{"temp":25}}},
				{"inlineData":{"mimeType":"image/png","data":"aGVsbG8="}}
			]}
		]
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	for _, value := range []string{"gemini rule", "weather lookup", "query weather", "Shanghai", "temp", "25"} {
		require.Contains(t, doc.NormalizedText, value)
	}
	require.Len(t, doc.Media, 1)
}

func TestNormalizeTextNFKCRemovesFormatControlsAndBuildsFoldedText(t *testing.T) {
	doc := Parse(ProtocolOpenAIResponses, []byte(`{"input":"Ｃｙ\u200bｂｅｒ － p.o_l/i\\c\\y"}`))

	require.True(t, doc.Complete)
	require.Equal(t, "Cyber - p.o_l/i\\c\\y", doc.NormalizedText)
	require.Equal(t, "Cyberpolicy", doc.FoldedText)
}

func TestParseTextAttachmentDecodesAndAuditsContents(t *testing.T) {
	payload := base64.StdEncoding.EncodeToString([]byte("attachment policy text"))
	doc := Parse(ProtocolOpenAIResponses, []byte(`{"input":[{"type":"message","role":"user","content":[{"type":"input_file","filename":"rules.txt","file_data":"data:text/plain;base64,`+payload+`"}]}]}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	require.Contains(t, doc.NormalizedText, "attachment policy text")
}

func TestParseTextAttachmentValidatesInlineSourceType(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("base64 source policy"))
	tests := []struct {
		name, source, want string
	}{
		{name: "text", source: `{"type":"text","media_type":"text/plain","data":"plain source policy"}`, want: "plain source policy"},
		{name: "base64", source: `{"type":"base64","media_type":"text/plain","data":"` + encoded + `"}`, want: "base64 source policy"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(ProtocolAnthropicMessages, []byte(`{"max_tokens":10,"messages":[{"role":"user","content":[{"type":"document","source":`+tt.source+`}]}]}`))
			require.True(t, doc.Complete, "%+v", doc.Issues)
			require.Contains(t, doc.NormalizedText, tt.want)
		})
	}
}

func TestParseStrictCompletenessFailures(t *testing.T) {
	tests := []struct {
		name, protocol, body, code string
	}{
		{name: "invalid json", protocol: ProtocolOpenAIResponses, body: `{`, code: IssueInvalidJSON},
		{name: "trailing json garbage", protocol: ProtocolOpenAIResponses, body: `{"input":"safe"} trailing`, code: IssueInvalidJSON},
		{name: "duplicate root field", protocol: ProtocolOpenAIResponses, body: `{"input":"safe","input":"hidden"}`, code: IssueDuplicateField},
		{name: "duplicate nested field", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","content":{"type":"input_text","text":"safe","text":"hidden"}}]}`, code: IssueDuplicateField},
		{name: "nonempty empty context", protocol: ProtocolOpenAIResponses, body: `{"model":"gpt-test"}`, code: IssueEmptyContent},
		{name: "unknown responses item", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"future_item","payload":"hidden"}]}`, code: IssueUnknownType},
		{name: "non-object websocket response", protocol: ProtocolOpenAIResponses, body: `{"type":"response.create","response":"hidden"}`, code: IssueInvalidShape},
		{name: "remote responses conversation", protocol: ProtocolOpenAIResponses, body: `{"conversation":"conv_123","input":"safe"}`, code: IssueRemoteFile},
		{name: "remote responses prompt template", protocol: ProtocolOpenAIResponses, body: `{"prompt":{"id":"pmpt_123","variables":{"payload":"hidden"}},"input":"safe"}`, code: IssueRemoteFile},
		{name: "unknown content field", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"safe","future_payload":"hidden"}]}]}`, code: IssueUnknownField},
		{name: "message field smuggling", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","content":"safe","arguments":"hidden"}]}`, code: IssueUnknownField},
		{name: "typed content field smuggling", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"safe","content":"hidden"}]}]}`, code: IssueUnknownField},
		{name: "unknown nested image field", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,aGVsbG8=","future":"hidden"}}]}]}`, code: IssueUnknownField},
		{name: "unknown chat role", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"future_role","content":"hidden"}]}`, code: IssueUnknownRole},
		{name: "gemini role spoofed into chat", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"model","content":"hidden"}]}`, code: IssueUnknownRole},
		{name: "chat role spoofed into responses", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"tool","content":[{"type":"input_text","text":"hidden"}]}]}`, code: IssueUnknownRole},
		{name: "numeric responses frame type", protocol: ProtocolOpenAIResponses, body: `{"type":7,"input":"safe"}`, code: IssueInvalidShape},
		{name: "numeric responses item type", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":7,"role":"user","content":"safe"}]}`, code: IssueInvalidShape},
		{name: "numeric responses role", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":7,"content":"safe"}]}`, code: IssueInvalidShape},
		{name: "numeric previous response", protocol: ProtocolOpenAIResponses, body: `{"previous_response_id":7,"input":"safe"}`, code: IssueInvalidShape},
		{name: "numeric chat name", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","name":7,"content":"safe"}]}`, code: IssueInvalidShape},
		{name: "unknown anthropic message field", protocol: ProtocolAnthropicMessages, body: `{"max_tokens":10,"messages":[{"role":"user","content":"safe","future_payload":"hidden"}]}`, code: IssueUnknownField},
		{name: "remote anthropic mcp server", protocol: ProtocolAnthropicMessages, body: `{"max_tokens":10,"mcp_servers":[{"name":"remote","url":"https://example.test"}],"messages":[{"role":"user","content":"safe"}]}`, code: IssueRemoteFile},
		{name: "remote anthropic container", protocol: ProtocolAnthropicMessages, body: `{"max_tokens":10,"container":"container_123","messages":[{"role":"user","content":"safe"}]}`, code: IssueRemoteFile},
		{name: "unknown gemini content field", protocol: ProtocolGemini, body: `{"contents":[{"role":"user","parts":[{"text":"safe"}],"futurePayload":"hidden"}]}`, code: IssueUnknownField},
		{name: "remote gemini cached content", protocol: ProtocolGemini, body: `{"cachedContent":"cachedContents/123","contents":[{"role":"user","parts":[{"text":"safe"}]}]}`, code: IssueRemoteFile},
		{name: "embedding token ids are uninspectable", protocol: "openai_embeddings", body: `{"input":[1,2,3],"model":"text-embedding"}`, code: IssueInvalidShape},
		{name: "unknown media root field", protocol: ProtocolOpenAIImages, body: `{"prompt":"safe","future_payload":"hidden"}`, code: IssueUnknownField},
		{name: "unknown media image shape", protocol: ProtocolOpenAIImages, body: `{"prompt":"safe","images":[42]}`, code: IssueInvalidShape},
		{name: "remote file", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","content":[{"type":"input_file","file_id":"file_123"}]}]}`, code: IssueRemoteFile},
		{name: "file source missing type", protocol: ProtocolAnthropicMessages, body: `{"max_tokens":10,"messages":[{"role":"user","content":[{"type":"document","source":{"media_type":"text/plain","data":"c2FmZQ=="}}]}]}`, code: IssueInvalidShape},
		{name: "file source unknown type", protocol: ProtocolAnthropicMessages, body: `{"max_tokens":10,"messages":[{"role":"user","content":[{"type":"document","source":{"type":"future","media_type":"text/plain","data":"c2FmZQ=="}}]}]}`, code: IssueUnknownType},
		{name: "file source invalid shape", protocol: ProtocolAnthropicMessages, body: `{"max_tokens":10,"messages":[{"role":"user","content":[{"type":"document","source":"opaque"}]}]}`, code: IssueInvalidShape},
		{name: "file source encrypted", protocol: ProtocolAnthropicMessages, body: `{"max_tokens":10,"messages":[{"role":"user","content":[{"type":"document","source":{"type":"encrypted","data":"opaque"}}]}]}`, code: IssueEncryptedContent},
		{name: "encrypted message", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","content":"safe","encrypted_content":"opaque"}]}`, code: IssueEncryptedContent},
		{name: "unsupported audio", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"aGVsbG8=","format":"wav"}}]}]}`, code: IssueUnsupportedMedia},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(tt.protocol, []byte(tt.body))
			require.False(t, doc.Complete)
			require.True(t, doc.HasIssue(tt.code), "%+v", doc.Issues)
		})
	}
}

func TestParseRejectsAmbiguousSemanticAliases(t *testing.T) {
	tests := []struct {
		name, protocol, body string
	}{
		{
			name:     "tool call arguments and args",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"tool_call","name":"inspect","arguments":"{\"target\":\"safe\"}","args":{"target":"hidden"}}]}`,
		},
		{
			name:     "file data aliases",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"input_file","filename":"policy.txt","file_data":"c2FmZQ==","data":"aGlkZGVu"}]}`,
		},
		{
			name:     "image source aliases",
			protocol: ProtocolOpenAIResponses,
			body:     `{"input":[{"type":"input_image","image_url":"data:image/png;base64,aGVsbG8=","data":"aGlkZGVu"}]}`,
		},
		{
			name:     "nested and outer image source",
			protocol: ProtocolAnthropicMessages,
			body:     `{"max_tokens":10,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"aGVsbG8="},"data":"aGlkZGVu"}]}]}`,
		},
		{
			name:     "gemini generation config aliases",
			protocol: ProtocolGemini,
			body:     `{"generationConfig":{"responseMimeType":"text/plain"},"generation_config":{"responseMimeType":"application/json"},"contents":[{"role":"user","parts":[{"text":"safe"}]}]}`,
		},
		{
			name:     "gemini inline mime aliases",
			protocol: ProtocolGemini,
			body:     `{"contents":[{"role":"user","parts":[{"inlineData":{"mimeType":"image/png","mime_type":"text/plain","data":"aGVsbG8="}}]}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(tt.protocol, []byte(tt.body))
			require.False(t, doc.Complete)
			require.True(t, doc.HasIssue(IssueInvalidShape), "%+v", doc.Issues)
		})
	}
}

func TestParseResponsesRejectsNestedResponseCreatePayload(t *testing.T) {
	tests := []string{
		`{"type":"response.create","model":"gpt-5.1","response":{"input":"safe"}}`,
		`{"type":"response.create","model":"gpt-5.1","input":"danger","response":{"input":"safe"}}`,
	}

	for _, body := range tests {
		doc := Parse(ProtocolOpenAIResponses, []byte(body))
		require.False(t, doc.Complete)
		require.True(t, doc.HasIssue(IssueInvalidShape), "%+v", doc.Issues)
	}
}

func TestParseRejectsDuplicateFieldsInsideStringToolOutput(t *testing.T) {
	doc := Parse(ProtocolOpenAIResponses, []byte(`{
		"previous_response_id":"resp_parent",
		"input":[{
			"type":"function_call_output",
			"call_id":"call_1",
			"output":"{\"result\":\"safe\",\"result\":\"hidden\"}"
		}]
	}`))

	require.False(t, doc.Complete)
	require.True(t, doc.HasIssue(IssueDuplicateField), "%+v", doc.Issues)
}

func TestParseToolOutputDecodesStringifiedJSONScalarBeforeAuditing(t *testing.T) {
	doc := Parse(ProtocolOpenAIResponses, []byte(`{
		"previous_response_id":"resp_parent",
		"input":[{
			"type":"function_call_output",
			"call_id":"call_1",
			"output":"\"c\\u0079ber\""
		}]
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	require.Contains(t, strings.ToLower(doc.NormalizedText), "cyber")
	require.Contains(t, strings.ToLower(doc.FoldedText), "cyber")
}

func TestParseFunctionArgumentsDecodesStringifiedJSONBeforeAuditing(t *testing.T) {
	doc := Parse(ProtocolOpenAIResponses, []byte(`{
		"input":[{
			"type":"function_call",
			"name":"shell",
			"arguments":"{\"cmd\":\"c\\u0079ber\"}"
		}]
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	require.Contains(t, strings.ToLower(doc.NormalizedText), "cyber")
	require.Contains(t, strings.ToLower(doc.FoldedText), "cyber")

	duplicate := Parse(ProtocolOpenAIResponses, []byte(`{
		"input":[{
			"type":"function_call",
			"name":"shell",
			"arguments":"{\"cmd\":\"safe\",\"cmd\":\"hidden\"}"
		}]
	}`))
	require.False(t, duplicate.Complete)
	require.True(t, duplicate.HasIssue(IssueDuplicateField), "%+v", duplicate.Issues)
}

func TestParseAnthropicServerToolResultsFailClosedOnOpaqueOrUnknownContent(t *testing.T) {
	tests := []struct {
		name, content, code string
	}{
		{
			name:    "encrypted web search result",
			content: `[{"type":"web_search_result","url":"https://example.test","title":"result","encrypted_content":"opaque"}]`,
			code:    IssueEncryptedContent,
		},
		{
			name:    "unknown result type",
			content: `[{"type":"future_tool_result","payload":"hidden"}]`,
			code:    IssueUnknownType,
		},
		{
			name:    "remote nested document",
			content: `[{"type":"document","file_id":"file_123"}]`,
			code:    IssueRemoteFile,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(ProtocolAnthropicMessages, []byte(`{
				"max_tokens":10,
				"messages":[{"role":"user","content":[{
					"type":"web_search_tool_result",
					"tool_use_id":"srvtoolu_1",
					"content":`+tt.content+`
				}]}]
			}`))
			require.False(t, doc.Complete)
			require.True(t, doc.HasIssue(tt.code), "%+v", doc.Issues)
		})
	}
}

func TestParseAnthropicServerToolResultAuditsNestedInlineImage(t *testing.T) {
	doc := Parse(ProtocolAnthropicMessages, []byte(`{
		"max_tokens":10,
		"messages":[{"role":"user","content":[{
			"type":"code_execution_tool_result",
			"tool_use_id":"srvtoolu_1",
			"content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"aGVsbG8="}}]
		}]}]
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	require.Len(t, doc.Media, 1)
}

func TestParseVertexInstancesRejectsUnknownPromptContainerFields(t *testing.T) {
	doc := Parse(ProtocolGemini, []byte(`{
		"instances":[
			{"messages":[{"role":"user","content":"danger"}]},
			{"prompt":"safe"}
		]
	}`))

	require.False(t, doc.Complete)
	require.True(t, doc.HasIssue(IssueUnknownField), "%+v", doc.Issues)
	require.Contains(t, doc.NormalizedText, "safe")
}

func TestParseResponsesOutputRejectsAmbiguousToolArguments(t *testing.T) {
	doc := ParseResponsesOutput([]byte(`[
		{"type":"tool_call","name":"inspect","arguments":"{\"target\":\"safe\"}","args":{"target":"hidden"}}
	]`))

	require.False(t, doc.Complete)
	require.True(t, doc.HasIssue(IssueInvalidShape), "%+v", doc.Issues)
}

func TestParseResponsesReasoningAuditsSummaryAndContent(t *testing.T) {
	doc := Parse(ProtocolOpenAIResponses, []byte(`{
		"input":[{
			"type":"reasoning",
			"summary":[{"type":"summary_text","text":"visible summary"}],
			"content":[{"type":"reasoning_text","text":"visible reasoning content"}]
		}]
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	require.Contains(t, doc.NormalizedText, "visible summary")
	require.Contains(t, doc.NormalizedText, "visible reasoning content")
}

func TestParseResponsesAcceptsCanonicalOpaqueAssistantState(t *testing.T) {
	reasoningCipher := "reasoning-ciphertext"
	compactionCipher := "compaction-ciphertext"
	summaryCipher := "compaction-summary-ciphertext"
	doc := Parse(ProtocolOpenAIResponses, []byte(`{
		"input":[
			{"id":"rs_1","type":"reasoning","status":"completed","encrypted_content":"`+reasoningCipher+`","summary":[{"type":"summary_text","text":"visible reasoning summary"}],"content":[{"type":"reasoning_text","text":"visible reasoning content"}]},
			{"id":"cmp_1","type":"compaction","status":"completed","encrypted_content":"`+compactionCipher+`","summary":[{"type":"summary_text","text":"visible compaction summary"}]},
			{"id":"cmp_2","type":"compaction_summary","encrypted_content":"`+summaryCipher+`","content":[{"type":"reasoning_text","text":"visible compacted content"}]},
			{"type":"message","role":"user","content":"continue"}
		]
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	require.Equal(t, "auditinput/v2", doc.ParserVersion)
	require.Len(t, doc.OpaqueStates, 3)
	require.Equal(t, OpaqueState{Kind: "reasoning", Path: "$.input[0].encrypted_content", Digest: sha256Hex(reasoningCipher)}, doc.OpaqueStates[0])
	require.Equal(t, OpaqueState{Kind: "compaction", Path: "$.input[1].encrypted_content", Digest: sha256Hex(compactionCipher)}, doc.OpaqueStates[1])
	require.Equal(t, OpaqueState{Kind: "compaction_summary", Path: "$.input[2].encrypted_content", Digest: sha256Hex(summaryCipher)}, doc.OpaqueStates[2])
	for _, visible := range []string{"visible reasoning summary", "visible reasoning content", "visible compaction summary", "visible compacted content", "continue"} {
		require.Contains(t, doc.NormalizedText, visible)
	}
	for _, ciphertext := range []string{reasoningCipher, compactionCipher, summaryCipher} {
		require.NotContains(t, doc.NormalizedText, ciphertext)
		for _, segment := range doc.Segments {
			require.NotContains(t, segment.Text, ciphertext)
		}
	}
	encoded, err := json.Marshal(doc)
	require.NoError(t, err)
	for _, ciphertext := range []string{reasoningCipher, compactionCipher, summaryCipher} {
		require.NotContains(t, string(encoded), ciphertext)
	}

	changed := Parse(ProtocolOpenAIResponses, []byte(`{"input":[{"type":"reasoning","encrypted_content":"different-ciphertext","summary":[{"type":"summary_text","text":"visible reasoning summary"}],"content":[{"type":"reasoning_text","text":"visible reasoning content"}]},{"type":"message","role":"user","content":"continue"}]}`))
	baseline := Parse(ProtocolOpenAIResponses, []byte(`{"input":[{"type":"reasoning","encrypted_content":"`+reasoningCipher+`","summary":[{"type":"summary_text","text":"visible reasoning summary"}],"content":[{"type":"reasoning_text","text":"visible reasoning content"}]},{"type":"message","role":"user","content":"continue"}]}`))
	require.Equal(t, baseline.NormalizedText, changed.NormalizedText)
	require.NotEqual(t, baseline.Hash, changed.Hash)
}

func TestParseResponsesOutputAcceptsCanonicalOpaqueAssistantState(t *testing.T) {
	doc := ParseResponsesOutput([]byte(`[
		{"id":"rs_1","type":"reasoning","status":"completed","encrypted_content":"reasoning-output","summary":[{"type":"summary_text","text":"output summary"}]},
		{"id":"cmp_1","type":"compaction","status":"completed","encrypted_content":"compaction-output"},
		{"id":"cmp_2","type":"compaction_summary","encrypted_content":"compaction-summary-output","content":[{"type":"reasoning_text","text":"output compacted content"}]}
	]`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	require.Len(t, doc.OpaqueStates, 3)
	require.Contains(t, doc.NormalizedText, "output summary")
	require.Contains(t, doc.NormalizedText, "output compacted content")
	require.NotContains(t, doc.NormalizedText, "reasoning-output")
	require.NotContains(t, doc.NormalizedText, "compaction-output")
	require.NotContains(t, doc.NormalizedText, "compaction-summary-output")

	opaqueOnly := ParseResponsesOutput([]byte(`[{"id":"cmp_only","type":"compaction","status":"completed","encrypted_content":"opaque-only-output"}]`))
	require.True(t, opaqueOnly.Complete, "%+v", opaqueOnly.Issues)
	require.Empty(t, opaqueOnly.NormalizedText)
	require.Equal(t, []OpaqueState{{Kind: "compaction", Path: "$.output[0].encrypted_content", Digest: sha256Hex("opaque-only-output")}}, opaqueOnly.OpaqueStates)
}

func TestParseResponsesCompactionTriggerIsExactControlItem(t *testing.T) {
	doc := Parse(ProtocolOpenAIResponses, []byte(`{"input":[{"type":"compaction_trigger"}]}`))
	require.True(t, doc.Complete, "%+v", doc.Issues)
	require.Empty(t, doc.NormalizedText)
	require.Equal(t, []ControlItem{{Kind: "compaction_trigger", Path: "$.input[0]"}}, doc.ControlItems)

	withPayload := Parse(ProtocolOpenAIResponses, []byte(`{"input":[{"type":"compaction_trigger","payload":"hidden"}]}`))
	require.False(t, withPayload.Complete)
	require.True(t, withPayload.HasIssue(IssueUnknownField), "%+v", withPayload.Issues)
}

func TestParseResponsesRejectsOpaqueContentOutsideCanonicalAssistantState(t *testing.T) {
	tests := []struct {
		name string
		body string
		code string
	}{
		{name: "message", body: `{"input":[{"type":"message","role":"user","content":"safe","encrypted_content":"opaque"}]}`, code: IssueEncryptedContent},
		{name: "tool output", body: `{"input":[{"type":"tool_call_output","call_id":"call_1","output":"safe","encrypted_content":"opaque"}]}`, code: IssueEncryptedContent},
		{name: "content part", body: `{"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"safe","encrypted_content":"opaque"}]}]}`, code: IssueEncryptedContent},
		{name: "unknown item", body: `{"input":[{"type":"future_item","encrypted_content":"opaque"}]}`, code: IssueEncryptedContent},
		{name: "reasoning alias", body: `{"input":[{"type":"reasoning","encryptedContent":"opaque","summary":[{"type":"summary_text","text":"visible"}]}]}`, code: IssueEncryptedContent},
		{name: "reasoning signature", body: `{"input":[{"type":"reasoning","signature":"opaque","summary":[{"type":"summary_text","text":"visible"}]}]}`, code: IssueEncryptedContent},
		{name: "reasoning empty canonical", body: `{"input":[{"type":"reasoning","encrypted_content":"","summary":[{"type":"summary_text","text":"visible"}]}]}`, code: IssueInvalidShape},
		{name: "reasoning malformed canonical", body: `{"input":[{"type":"reasoning","encrypted_content":{"opaque":true},"summary":[{"type":"summary_text","text":"visible"}]}]}`, code: IssueInvalidShape},
		{name: "compaction missing canonical", body: `{"input":[{"type":"compaction","summary":[{"type":"summary_text","text":"visible"}]}]}`, code: IssueInvalidShape},
		{name: "compaction trigger encrypted", body: `{"input":[{"type":"compaction_trigger","encrypted_content":"opaque"}]}`, code: IssueEncryptedContent},
		{name: "assistant state user role", body: `{"input":[{"type":"reasoning","role":"user","encrypted_content":"opaque","summary":[{"type":"summary_text","text":"visible"}]}]}`, code: IssueUnknownRole},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(ProtocolOpenAIResponses, []byte(tt.body))
			require.False(t, doc.Complete)
			require.True(t, doc.HasIssue(tt.code), "%+v", doc.Issues)
			if strings.Contains(tt.body, "visible") {
				require.Contains(t, doc.NormalizedText, "visible")
			}
		})
	}
}

func TestParseResponsesSupportsAdditionalToolsAndFunctionNamespace(t *testing.T) {
	doc := Parse(ProtocolOpenAIResponses, []byte(`{
		"input":[
			{"type":"additional_tools","role":"developer","tools":[{"type":"namespace","name":"collaboration","description":"multi-agent tools","tools":[{"type":"function","name":"spawn_agent","description":"spawn a bounded agent","parameters":{"type":"object","properties":{"task":{"type":"string"}}}}]}]},
			{"type":"function_call","namespace":"collaboration","name":"spawn_agent","call_id":"call_1","arguments":"{\"task\":\"inspect parser\"}"},
			{"type":"message","role":"user","content":"continue"}
		]
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	for _, visible := range []string{"collaboration", "multi-agent tools", "spawn_agent", "spawn a bounded agent", "inspect parser", "continue"} {
		require.Contains(t, doc.NormalizedText, visible)
	}
	require.Equal(t, "function_namespace", doc.Segments[1].Kind)
	require.Equal(t, "$.input[1].namespace", doc.Segments[1].Path)
}

func TestParseProductionLikeLargeCodexFullHistory(t *testing.T) {
	largeToolDescription := strings.Repeat("inspect repository state and return structured evidence; ", 24_000)
	responsesBody := []byte(`{
		"model":"gpt-5.6-sol",
		"store":false,
		"tools":[{"type":"function","name":"exec_command","description":` + quoteJSON(largeToolDescription) + `,"parameters":{"type":"object","properties":{"cmd":{"type":"string"}}}}],
		"input":[
			{"id":"rs_1","type":"reasoning","encrypted_content":"reasoning-state","summary":[{"type":"summary_text","text":"inspect the current failure"}]},
			{"id":"cmp_1","type":"compaction","encrypted_content":"compaction-state"},
			{"type":"additional_tools","role":"developer","tools":[{"type":"namespace","name":"collaboration","tools":[{"type":"function","name":"spawn_agent","parameters":{"type":"object"}}]}]},
			{"type":"function_call","namespace":"collaboration","name":"spawn_agent","call_id":"call_1","arguments":"{\"task\":\"inspect 422\"}"},
			{"type":"function_call_output","call_id":"call_1","output":"parser evidence collected"},
			{"type":"message","role":"user","content":"continue without previous_response_id"}
		]
	}`)
	require.Greater(t, len(responsesBody), 1<<20)

	responses := Parse(ProtocolOpenAIResponses, responsesBody)
	require.True(t, responses.Complete, "%+v", responses.Issues)
	require.Empty(t, responses.PreviousResponseID)
	require.NotNil(t, responses.Store)
	require.False(t, *responses.Store)
	require.Len(t, responses.OpaqueStates, 2)
	for _, visible := range []string{"exec_command", "inspect the current failure", "spawn_agent", "inspect 422", "parser evidence collected", "continue without previous_response_id"} {
		require.Contains(t, responses.NormalizedText, visible)
	}
	require.NotContains(t, responses.NormalizedText, "reasoning-state")
	require.NotContains(t, responses.NormalizedText, "compaction-state")

	chatBody := []byte(`{"model":"gpt-5.6-sol","store":false,"tools":[{"type":"function","function":{"name":"exec_command","description":` + quoteJSON(largeToolDescription) + `,"parameters":{"type":"object"}}}],"messages":[{"role":"user","content":"continue chat"}]}`)
	require.Greater(t, len(chatBody), 1<<20)
	chat := Parse(ProtocolOpenAIChat, chatBody)
	require.True(t, chat.Complete, "%+v", chat.Issues)
	require.Contains(t, chat.NormalizedText, "continue chat")
	require.Contains(t, chat.NormalizedText, "exec_command")
}

func sha256Hex(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func TestParseResponsesOutputRejectsDuplicateFields(t *testing.T) {
	doc := ParseResponsesOutput([]byte(`[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"safe","text":"hidden"}]}]`))

	require.False(t, doc.Complete)
	require.True(t, doc.HasIssue(IssueDuplicateField), "%+v", doc.Issues)
}

func TestParseRejectsMalformedKnownTextFieldsWithSafeSibling(t *testing.T) {
	tests := []struct {
		name, protocol, body string
	}{
		{name: "responses input_text missing text", protocol: ProtocolOpenAIResponses, body: `{"input":["safe",{"type":"input_text"}]}`},
		{name: "responses output_text non-string text", protocol: ProtocolOpenAIResponses, body: `{"input":["safe",{"type":"output_text","text":7}]}`},
		{name: "responses text missing text", protocol: ProtocolOpenAIResponses, body: `{"input":["safe",{"type":"text"}]}`},
		{name: "responses refusal non-string text", protocol: ProtocolOpenAIResponses, body: `{"input":["safe",{"type":"refusal","text":{"hidden":"payload"}}]}`},
		{name: "responses nested typed part missing text", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"safe"},{"type":"output_text"}]}]}`},
		{name: "responses message null content", protocol: ProtocolOpenAIResponses, body: `{"input":["safe",{"type":"message","role":"user","content":null}]}`},
		{name: "chat non-string refusal", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":"safe","refusal":{"hidden":"payload"}}]}`},
		{name: "gemini non-string content text", protocol: ProtocolGemini, body: `{"contents":[{"role":"user","text":7,"parts":[{"text":"safe"}]}]}`},
		{name: "gemini non-string part text", protocol: ProtocolGemini, body: `{"contents":[{"role":"user","parts":[{"text":7},{"text":"safe"}]}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(tt.protocol, []byte(tt.body))
			require.False(t, doc.Complete)
			require.True(t, doc.HasIssue(IssueInvalidShape), "%+v", doc.Issues)
			require.Contains(t, doc.NormalizedText, "safe")
		})
	}
}

func TestParseRejectsMissingFunctionNamesWithSafeSibling(t *testing.T) {
	tests := []struct {
		name, protocol, body string
	}{
		{name: "responses", protocol: ProtocolOpenAIResponses, body: `{"input":["safe",{"type":"function_call","arguments":"{\"payload\":\"hidden\"}"}]}`},
		{name: "chat", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":"safe"},{"role":"assistant","tool_calls":[{"type":"function","function":{"arguments":"{\"payload\":\"hidden\"}"}}]}]}`},
		{name: "gemini", protocol: ProtocolGemini, body: `{"contents":[{"role":"user","parts":[{"text":"safe"},{"functionCall":{"args":{"payload":"hidden"}}}]}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(tt.protocol, []byte(tt.body))
			require.False(t, doc.Complete)
			require.True(t, doc.HasIssue(IssueInvalidShape), "%+v", doc.Issues)
			require.Contains(t, doc.NormalizedText, "safe")
		})
	}
}

func TestParseRejectsMalformedOpaqueFieldsWithSafeSibling(t *testing.T) {
	tests := []struct {
		name, protocol, body string
	}{
		{name: "responses encrypted content", protocol: ProtocolOpenAIResponses, body: `{"input":["safe",{"type":"reasoning","encrypted_content":{"opaque":true}}]}`},
		{name: "anthropic signature", protocol: ProtocolAnthropicMessages, body: `{"max_tokens":10,"messages":[{"role":"user","content":[{"type":"text","text":"safe"},{"type":"thinking","thinking":"visible","signature":7}]}]}`},
		{name: "gemini thought signature", protocol: ProtocolGemini, body: `{"contents":[{"role":"user","parts":[{"text":"safe","thoughtSignature":{"opaque":true}}]}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(tt.protocol, []byte(tt.body))
			require.False(t, doc.Complete)
			require.True(t, doc.HasIssue(IssueInvalidShape), "%+v", doc.Issues)
			require.Contains(t, doc.NormalizedText, "safe")
		})
	}
}

func TestParseGeminiRejectsMultiplePayloadsWithoutDroppingLaterContent(t *testing.T) {
	doc := Parse(ProtocolGemini, []byte(`{
		"contents":[{"role":"user","parts":[{
			"text":"safe",
			"functionCall":{"name":"inspect","args":{"payload":"later model-visible content"}}
		}]}]
	}`))

	require.False(t, doc.Complete)
	require.True(t, doc.HasIssue(IssueInvalidShape), "%+v", doc.Issues)
	require.Contains(t, doc.NormalizedText, "safe")
	require.Contains(t, doc.NormalizedText, "later model-visible content")
}

func TestParseAcceptsOptionalEmptyStringsAndEmptyTypedText(t *testing.T) {
	tests := []struct {
		name, protocol, body string
	}{
		{name: "responses", protocol: ProtocolOpenAIResponses, body: `{"previous_response_id":"","input":[{"type":"input_text","text":"safe"},{"type":"output_text","text":""}]}`},
		{name: "chat", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","name":"","content":"safe","refusal":""}]}`},
		{name: "anthropic", protocol: ProtocolAnthropicMessages, body: `{"max_tokens":10,"messages":[{"role":"user","content":[{"type":"text","text":"safe"},{"type":"text","text":""}]}]}`},
		{name: "gemini", protocol: ProtocolGemini, body: `{"contents":[{"role":"user","parts":[{"text":"safe","thoughtSignature":""},{"text":""}]}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(tt.protocol, []byte(tt.body))
			require.True(t, doc.Complete, "%+v", doc.Issues)
			require.Equal(t, "safe", doc.NormalizedText)
		})
	}
}

func TestParseImageSourceRequiresNonEmptyStringTypeWithSafeSibling(t *testing.T) {
	tests := []struct {
		name, source string
	}{
		{name: "missing", source: `{"media_type":"image/png","data":"aGVsbG8="}`},
		{name: "non-string", source: `{"type":7,"media_type":"image/png","data":"aGVsbG8="}`},
		{name: "empty", source: `{"type":"","media_type":"image/png","data":"aGVsbG8="}`},
		{name: "source non-object with outer fallback", source: `7,"data":"aGVsbG8=","media_type":"image/png"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(ProtocolAnthropicMessages, []byte(`{
				"max_tokens":10,
				"messages":[{"role":"user","content":[
					{"type":"text","text":"safe"},
					{"type":"image","source":`+tt.source+`}
				]}]
			}`))
			require.False(t, doc.Complete)
			require.True(t, doc.HasIssue(IssueInvalidShape), "%+v", doc.Issues)
			require.Contains(t, doc.NormalizedText, "safe")
			require.Empty(t, doc.Media)
		})
	}
}

func TestParseResponsesToolOutputsRequireInspectablePayload(t *testing.T) {
	tests := []struct {
		name, item string
	}{
		{name: "function output missing", item: `{"type":"function_call_output","call_id":"call_1"}`},
		{name: "function output null", item: `{"type":"function_call_output","call_id":"call_1","output":null}`},
		{name: "function output scalar", item: `{"type":"function_call_output","call_id":"call_1","output":7}`},
		{name: "generic tool output missing", item: `{"type":"tool_call_output","call_id":"call_1"}`},
		{name: "custom tool output scalar", item: `{"type":"custom_tool_call_output","call_id":"call_1","output":true}`},
		{name: "ambiguous output aliases", item: `{"type":"mcp_tool_call_output","output":"first","content":"second"}`},
		{name: "computer output missing", item: `{"type":"computer_call_output","call_id":"call_1"}`},
		{name: "computer output wrong shape", item: `{"type":"computer_call_output","call_id":"call_1","output":"screenshot"}`},
		{name: "computer output missing type", item: `{"type":"computer_call_output","call_id":"call_1","output":{"image_url":"data:image/png;base64,aGVsbG8="}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(ProtocolOpenAIResponses, []byte(`{
				"previous_response_id":"resp_parent",
				"input":[{"type":"input_text","text":"safe"},`+tt.item+`]
			}`))
			require.False(t, doc.Complete)
			require.True(t, doc.HasIssue(IssueInvalidShape), "%+v", doc.Issues)
			require.Contains(t, doc.NormalizedText, "safe")
		})
	}
}

func TestParseResponsesToolOutputsAuditStructuredMedia(t *testing.T) {
	doc := Parse(ProtocolOpenAIResponses, []byte(`{
		"previous_response_id":"resp_parent",
		"input":[
			{"type":"input_text","text":"safe"},
			{"type":"function_call_output","call_id":"call_1","output":{
				"status":"ok",
				"content":[
					{"type":"input_text","text":"render complete"},
					{"type":"input_image","image_url":"data:image/png;base64,aGVsbG8="}
				]
			}},
			{"type":"tool_search_output","call_id":"call_2","output":{"groups":["git"]}}
		]
	}`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	require.Contains(t, doc.NormalizedText, "render complete")
	require.Contains(t, doc.NormalizedText, "groups")
	require.Len(t, doc.Media, 1)

	remote := Parse(ProtocolOpenAIResponses, []byte(`{
		"previous_response_id":"resp_parent",
		"input":[
			{"type":"input_text","text":"safe"},
			{"type":"function_call_output","call_id":"call_1","output":[
				{"type":"input_image","image_url":"https://example.test/output.png"}
			]}
		]
	}`))
	require.False(t, remote.Complete)
	require.True(t, remote.HasIssue(IssueRemoteFile), "%+v", remote.Issues)
}

func TestParseAnthropicToolBlocksRequirePayloads(t *testing.T) {
	tests := []struct {
		name, block string
	}{
		{name: "tool result missing content", block: `{"type":"tool_result","tool_use_id":"tool_1"}`},
		{name: "tool result null content", block: `{"type":"tool_result","tool_use_id":"tool_1","content":null}`},
		{name: "tool result scalar content", block: `{"type":"tool_result","tool_use_id":"tool_1","content":7}`},
		{name: "tool result malformed is_error", block: `{"type":"tool_result","tool_use_id":"tool_1","content":"done","is_error":"false"}`},
		{name: "tool use missing input", block: `{"type":"tool_use","id":"tool_1","name":"inspect"}`},
		{name: "tool use non-object input", block: `{"type":"tool_use","id":"tool_1","name":"inspect","input":"hidden"}`},
		{name: "server result missing content", block: `{"type":"web_search_tool_result","tool_use_id":"tool_1"}`},
		{name: "server result scalar content", block: `{"type":"web_search_tool_result","tool_use_id":"tool_1","content":false}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(ProtocolAnthropicMessages, []byte(`{
				"max_tokens":10,
				"messages":[{"role":"user","content":[
					{"type":"text","text":"safe"},`+tt.block+`
				]}]
			}`))
			require.False(t, doc.Complete)
			require.True(t, doc.HasIssue(IssueInvalidShape), "%+v", doc.Issues)
			require.Contains(t, doc.NormalizedText, "safe")
		})
	}
}

func TestParseAcceptsPresentEmptyToolOutput(t *testing.T) {
	responses := Parse(ProtocolOpenAIResponses, []byte(`{
		"previous_response_id":"resp_parent",
		"input":[
			{"type":"input_text","text":"safe"},
			{"type":"function_call_output","call_id":"call_1","output":""}
		]
	}`))
	require.True(t, responses.Complete, "%+v", responses.Issues)
	require.Equal(t, "safe", responses.NormalizedText)

	anthropic := Parse(ProtocolAnthropicMessages, []byte(`{
		"max_tokens":10,
		"messages":[{"role":"user","content":[
			{"type":"text","text":"safe"},
			{"type":"tool_result","tool_use_id":"tool_1","content":""}
		]}]
	}`))
	require.True(t, anthropic.Complete, "%+v", anthropic.Issues)
	require.Equal(t, "safe", anthropic.NormalizedText)
}

func TestParseRejectsRemoteImagesAndFilesAcrossProtocols(t *testing.T) {
	tests := []struct {
		name, protocol, body string
	}{
		{name: "responses image url", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"inspect this"},{"type":"input_image","image_url":"https://example.test/image.png"}]}]}`},
		{name: "chat image url", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":[{"type":"text","text":"inspect this"},{"type":"image_url","image_url":{"url":"https://example.test/image.png"}}]}]}`},
		{name: "anthropic source url", protocol: ProtocolAnthropicMessages, body: `{"max_tokens":10,"messages":[{"role":"user","content":[{"type":"text","text":"inspect this"},{"type":"image","source":{"type":"url","url":"https://example.test/image.png"}}]}]}`},
		{name: "gemini file data", protocol: ProtocolGemini, body: `{"contents":[{"role":"user","parts":[{"text":"inspect this"},{"fileData":{"mimeType":"image/png","fileUri":"https://example.test/image.png"}}]}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := Parse(tt.protocol, []byte(tt.body))
			require.False(t, doc.Complete)
			require.True(t, doc.HasIssue(IssueRemoteFile), "%+v", doc.Issues)
		})
	}
}

func TestParseRejectsUnknownProtocolRootFields(t *testing.T) {
	tests := []struct {
		protocol, body string
	}{
		{ProtocolOpenAIResponses, `{"input":"safe","future_payload":"hidden"}`},
		{ProtocolOpenAIChat, `{"messages":[{"role":"user","content":"safe"}],"future_payload":"hidden"}`},
		{ProtocolAnthropicMessages, `{"max_tokens":10,"messages":[{"role":"user","content":"safe"}],"future_payload":"hidden"}`},
		{ProtocolGemini, `{"contents":[{"role":"user","parts":[{"text":"safe"}]}],"futurePayload":"hidden"}`},
	}
	for _, tt := range tests {
		doc := Parse(tt.protocol, []byte(tt.body))
		require.False(t, doc.Complete)
		require.True(t, doc.HasIssue(IssueUnknownField), "%s: %+v", tt.protocol, doc.Issues)
	}
}

func TestParseEnforcesTextAndImageLimitsDeterministically(t *testing.T) {
	textDoc := Parse(ProtocolOpenAIResponses, []byte(`{"input":`+quoteJSON(strings.Repeat("x", MaxTextRunes+1))+`}`))
	require.False(t, textDoc.Complete)
	require.True(t, textDoc.Truncated)
	require.True(t, textDoc.HasIssue(IssueTextLimit))
	require.Len(t, []rune(textDoc.NormalizedText), MaxTextRunes)

	image := `{"type":"input_image","image_url":"data:image/png;base64,aGVsbG8="}`
	imagesDoc := Parse(ProtocolOpenAIResponses, []byte(`{"input":[{"type":"message","role":"user","content":[`+strings.Join([]string{image, image, image, image, image}, ",")+`]}]}`))
	require.False(t, imagesDoc.Complete)
	require.True(t, imagesDoc.HasIssue(IssueImageLimit))
	require.Len(t, imagesDoc.Media, MaxImages)
}

func TestParseRetainsRepeatedSegmentsPathsAndCountsThemTowardTextLimit(t *testing.T) {
	repeated := Parse(ProtocolOpenAIResponses, []byte(`{"input":[{"type":"input_text","text":"repeat"},{"type":"input_text","text":"repeat"}]}`))
	require.True(t, repeated.Complete, "%+v", repeated.Issues)
	require.Len(t, repeated.Segments, 2)
	require.Equal(t, "$.input[0].text", repeated.Segments[0].Path)
	require.Equal(t, "$.input[1].text", repeated.Segments[1].Path)
	require.Equal(t, "repeat\nrepeat", repeated.NormalizedText)

	half := strings.Repeat("x", MaxTextRunes/2)
	overLimit := Parse(ProtocolOpenAIResponses, []byte(`{"input":[{"type":"input_text","text":`+quoteJSON(half)+`},{"type":"input_text","text":`+quoteJSON(half)+`}]}`))
	require.False(t, overLimit.Complete)
	require.True(t, overLimit.Truncated)
	require.True(t, overLimit.HasIssue(IssueTextLimit))
	require.Len(t, overLimit.Segments, 2)
	require.Len(t, []rune(overLimit.NormalizedText), MaxTextRunes)
}

func TestDocumentCloneDoesNotAliasSlices(t *testing.T) {
	doc := Parse(ProtocolOpenAIResponses, []byte(`{"store":false,"input":[{"type":"reasoning","encrypted_content":"opaque"},{"type":"compaction_trigger"},{"type":"message","role":"user","content":"hello"}]}`))
	clone := doc.Clone()
	clone.Segments[0].Normalized = "changed"
	clone.OpaqueStates[0].Digest = "changed"
	clone.ControlItems[0].Kind = "changed"
	*clone.Store = true
	require.Equal(t, "hello", doc.Segments[0].Normalized)
	require.NotEqual(t, "changed", doc.OpaqueStates[0].Digest)
	require.Equal(t, "compaction_trigger", doc.ControlItems[0].Kind)
	require.False(t, *doc.Store)
}

func TestParseResponsesOutputUsesStrictResponsesItemRules(t *testing.T) {
	doc := ParseResponsesOutput([]byte(`[
		{"type":"message","role":"assistant","content":[{"type":"output_text","text":"safe answer"}]},
		{"type":"function_call","id":"fc_1","call_id":"call_1","name":"shell","arguments":"{\"cmd\":\"echo safe\"}"}
	]`))
	require.True(t, doc.Complete, "%+v", doc.Issues)
	require.Contains(t, doc.NormalizedText, "safe answer")
	require.Contains(t, doc.NormalizedText, "shell")
	require.Contains(t, doc.NormalizedText, "echo safe")

	unknown := ParseResponsesOutput([]byte(`[{"type":"future_tool_output","payload":"hidden"}]`))
	require.False(t, unknown.Complete)
	require.True(t, unknown.HasIssue(IssueUnknownType), "%+v", unknown.Issues)

	nonArray := ParseResponsesOutput([]byte(`{"type":"message"}`))
	require.False(t, nonArray.Complete)
	require.True(t, nonArray.HasIssue(IssueInvalidShape), "%+v", nonArray.Issues)
}

func TestParseResponsesOutputAcceptsCompletedMessagePhase(t *testing.T) {
	doc := ParseResponsesOutput([]byte(`[
		{"id":"msg_1","type":"message","status":"completed","phase":"final_answer","role":"assistant","content":[{"type":"output_text","annotations":[],"logprobs":[],"text":"OK"}]}
	]`))

	require.True(t, doc.Complete, "%+v", doc.Issues)
	require.Equal(t, "OK", doc.NormalizedText)
}

func FuzzParseNeverPanics(f *testing.F) {
	f.Add(ProtocolOpenAIResponses, []byte(`{"input":"hello"}`))
	f.Add(ProtocolOpenAIChat, []byte(`{"messages":[]}`))
	f.Add(ProtocolOpenAIResponses, []byte(`{"input":[{"type":"tool_call","name":"inspect","arguments":"{}","args":{}}]}`))
	f.Add(ProtocolOpenAIResponses, []byte(`{"type":"response.create","model":"gpt-5.1","input":"danger","response":{"input":"safe"}}`))
	f.Add(ProtocolGemini, []byte(`{"instances":[{"messages":[{"role":"user","content":"hidden"}]},{"prompt":"safe"}]}`))
	f.Fuzz(func(t *testing.T, protocol string, body []byte) {
		doc := Parse(protocol, body)
		require.NotNil(t, doc)
	})
}

func quoteJSON(value string) string {
	return `"` + value + `"`
}
