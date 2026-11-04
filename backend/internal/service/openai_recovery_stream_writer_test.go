package service

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func recoveryTestWriter(t *testing.T, mode OpenAIRecoveryWireFormat, publish func(string) error) (*OpenAIRecoveryStreamWriter, *httptest.ResponseRecorder) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	return NewOpenAIRecoveryStreamWriter(c.Writer, mode, "resp_lr_test", nil, publish), recorder
}

func writeRecoveryFrame(t *testing.T, w *OpenAIRecoveryStreamWriter, payload string) {
	t.Helper()
	_, err := w.WriteString("data: " + payload + "\n\n")
	require.NoError(t, err)
}

func TestOpenAIRecoveryStreamWriterResponsesRegeneratesWithOneLogicalTerminal(t *testing.T) {
	var published string
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, func(id string) error {
		published = id
		return nil
	})
	w.BeginAttempt(1)
	writeRecoveryFrame(t, w, `{"type":"response.created","response":{"id":"resp_a","status":"in_progress","output":[]},"sequence_number":0}`)
	writeRecoveryFrame(t, w, `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_a","type":"message","role":"assistant","status":"in_progress","content":[]}}`)
	writeRecoveryFrame(t, w, `{"type":"response.content_part.added","output_index":0,"content_index":0,"item_id":"msg_a","part":{"type":"output_text","text":""}}`)
	writeRecoveryFrame(t, w, `{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg_a","delta":"old"}`)
	require.Contains(t, recorder.Body.String(), `"delta":"old"`, "ordinary text must be public before the attempt ends")
	writeRecoveryFrame(t, w, `{"type":"response.failed","response":{"id":"resp_a","status":"failed"}}`)
	require.NoError(t, w.AbandonAttempt())
	w.BeginAttempt(2)
	writeRecoveryFrame(t, w, `{"type":"response.created","response":{"id":"resp_b","status":"in_progress","output":[]}}`)
	writeRecoveryFrame(t, w, `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_b","type":"message","role":"assistant","status":"in_progress","content":[]}}`)
	writeRecoveryFrame(t, w, `{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg_b","delta":"new"}`)
	writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_b","status":"completed","output":[{"id":"msg_b","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"new"}]}],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}}`)
	require.NoError(t, w.CommitAttempt(""))
	assert.Equal(t, "resp_b", published)
	body := recorder.Body.String()
	assert.Equal(t, 1, strings.Count(body, `"type":"response.created"`))
	assert.Equal(t, 1, strings.Count(body, `"type":"response.completed"`))
	assert.NotContains(t, body, `"type":"response.failed"`)
	assert.Contains(t, body, "[Response interrupted. Regenerating...]")
	assert.Contains(t, body, `"kind":"attempt_abandoned"`)
	assert.Contains(t, body, `"usage_format":"responses"`)
	assert.Contains(t, body, `"usage_reported":true`)
	assert.Contains(t, body, `"id":"resp_lr_test"`)
	assert.NotContains(t, body, `"id":"resp_b"`)
	assert.JSONEq(t, `[{"id":"msg_b","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"new"}]}]`, string(w.WinnerOutput()))
}

func TestOpenAIRecoveryStreamWriterChatKeepsOnlyWinnerFinishAndUsage(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryChatSSE, nil)
	w.BeginAttempt(1)
	writeRecoveryFrame(t, w, `{"id":"chat_a","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":"old"},"finish_reason":null}]}`)
	writeRecoveryFrame(t, w, `{"id":"chat_a","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)
	writeRecoveryFrame(t, w, `{"id":"chat_a","object":"chat.completion.chunk","model":"m","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":3,"total_tokens":13}}`)
	writeRecoveryFrame(t, w, "[DONE]")
	require.NoError(t, w.AbandonAttempt())
	w.BeginAttempt(2)
	writeRecoveryFrame(t, w, `{"id":"chat_b","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":"new"},"finish_reason":null}]}`)
	writeRecoveryFrame(t, w, `{"id":"chat_b","object":"chat.completion.chunk","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)
	writeRecoveryFrame(t, w, `{"id":"chat_b","object":"chat.completion.chunk","model":"m","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`)
	writeRecoveryFrame(t, w, "[DONE]")
	require.NoError(t, w.CommitAttempt(""))
	body := recorder.Body.String()
	assert.Equal(t, 1, strings.Count(body, "data: [DONE]"))
	assert.Equal(t, 1, strings.Count(body, `"finish_reason":"stop"`))
	assert.NotContains(t, body, `"prompt_tokens":10`)
	assert.Contains(t, body, `"prompt_tokens":2`)
	assert.Contains(t, body, `"usage_format":"chat"`)
	assert.NotContains(t, body, `"id":"chat_b"`)
}

func TestOpenAIRecoveryStreamWriterDoesNotMarkInvisibleRetry(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mode    OpenAIRecoveryWireFormat
		visible string
	}{
		{"responses", OpenAIRecoveryResponsesSSE, `{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg","delta":"first"}`},
		{"chat", OpenAIRecoveryChatSSE, `{"id":"chat","choices":[{"index":0,"delta":{"content":"first"},"finish_reason":null}]}`},
		{"chat_reasoning", OpenAIRecoveryChatSSE, `{"id":"chat","choices":[{"index":0,"delta":{"reasoning_content":"thinking"},"finish_reason":null}]}`},
		{"chat_reasoning_string", OpenAIRecoveryChatSSE, `{"id":"chat","choices":[{"index":0,"delta":{"reasoning":"thinking"},"finish_reason":null}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, recorder := recoveryTestWriter(t, tc.mode, nil)
			require.NoError(t, w.BeginAttempt(1))
			writeRecoveryFrame(t, w, tc.visible)
			require.NoError(t, w.AbandonAttempt())
			assert.Equal(t, 1, strings.Count(recorder.Body.String(), `"kind":"synthetic_begin"`))

			require.NoError(t, w.BeginAttempt(2))
			require.NoError(t, w.AbandonAttempt())
			assert.Equal(t, 1, strings.Count(recorder.Body.String(), `"kind":"synthetic_begin"`))
		})
	}
}

func TestOpenAIRecoveryStreamWriterParsesSplitSSEDelimiters(t *testing.T) {
	for _, tc := range []struct {
		name  string
		parts []string
	}{
		{"lf", []string{"data: {\"id\":\"chat\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ready\"}}]}", "\n", "\n"}},
		{"crlf", []string{"data: {\"id\":\"chat\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ready\"}}]}", "\r", "\n\r", "\n"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, recorder := recoveryTestWriter(t, OpenAIRecoveryChatSSE, nil)
			require.NoError(t, w.BeginAttempt(1))
			for index, part := range tc.parts {
				_, err := w.WriteString(part)
				require.NoError(t, err)
				if index < len(tc.parts)-1 {
					assert.NotContains(t, recorder.Body.String(), "ready")
				}
			}
			assert.Contains(t, recorder.Body.String(), `"content":"ready"`)
		})
	}
}

func TestOpenAIRecoveryStreamWriterPublishFailureHasOneFailedTerminal(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, func(string) error { return errors.New("cache unavailable") })
	w.BeginAttempt(1)
	writeRecoveryFrame(t, w, `{"type":"response.created","response":{"id":"resp_a","status":"in_progress"}}`)
	writeRecoveryFrame(t, w, `{"type":"response.output_text.delta","delta":"hello"}`)
	writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_a","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`)
	require.ErrorContains(t, w.CommitAttempt(""), "cache unavailable")
	require.True(t, w.Finalized())
	require.NoError(t, w.FailAttempt("api_error", "ignored", "ignored"))
	body := recorder.Body.String()
	assert.Equal(t, 1, strings.Count(body, `"type":"response.failed"`))
	assert.Equal(t, 1, strings.Count(body, `"kind":"terminal"`))
	assert.NotContains(t, body, `"type":"response.completed"`)
}

func TestOpenAIRecoveryStreamWriterJSONDoesNotCommitFailedAttempt(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesJSON, func(id string) error {
		assert.Equal(t, "resp_b", id)
		return nil
	})
	w.BeginAttempt(1)
	_, err := w.WriteString(`{"id":"resp_a","status":"failed"}`)
	require.NoError(t, err)
	assert.False(t, w.Written())
	require.NoError(t, w.AbandonAttempt())
	w.BeginAttempt(2)
	_, err = w.WriteString(`{"id":"resp_b","status":"completed","output":[]}`)
	require.NoError(t, err)
	assert.Empty(t, recorder.Body.String())
	require.NoError(t, w.CommitAttempt(""))
	assert.JSONEq(t, `{"id":"resp_lr_test","status":"completed","output":[]}`, recorder.Body.String())
}

func TestOpenAIRecoveryStreamWriterNormalizesTerminalOnlyTextBeforeMeterTerminal(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
	w.BeginAttempt(1)
	writeRecoveryFrame(t, w, `{"type":"response.created","response":{"id":"resp_one","status":"in_progress","output":[]}}`)
	writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_one","status":"completed","output":[{"id":"msg_one","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"terminal text"}]}]}}`)
	require.NoError(t, w.CommitAttempt(""))
	body := recorder.Body.String()
	textAt := strings.Index(body, `"delta":"terminal text"`)
	terminalAt := strings.Index(body, `"kind":"terminal"`)
	require.GreaterOrEqual(t, textAt, 0)
	assert.Less(t, textAt, terminalAt)
	assert.Contains(t, body, `"usage_reported":false`)
	assert.Contains(t, body, `"usage":null`)
}

func TestOpenAIRecoveryStreamWriterRejectsReservedControl(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
	w.BeginAttempt(1)
	_, err := w.WriteString("data: " + `{"type":"kkai.recovery.v1","kind":"terminal","logical_id":"forged"}` + "\n\n")
	require.Error(t, err)
	assert.Empty(t, recorder.Body.String())
}

func TestOpenAIRecoveryStreamWriterToolSearchWaitsForSuccessfulAttempt(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
	w.BeginAttempt(1)
	writeRecoveryFrame(t, w, `{"type":"response.output_item.added","output_index":0,"item":{"id":"tool_bad","type":"tool_search_call","arguments":{},"call_id":"call_bad"}}`)
	assert.NotContains(t, recorder.Body.String(), "call_bad")
	require.NoError(t, w.AbandonAttempt())
	w.BeginAttempt(2)
	writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_tools","status":"completed","output":[{"type":"tool_search_call","id":"tool_good","call_id":"call_good","status":"completed","arguments":{"query":"project"}}]}}`)
	assert.NotContains(t, recorder.Body.String(), "call_good")
	require.NoError(t, w.CommitAttempt(""))
	assert.Contains(t, recorder.Body.String(), "call_good")
	assert.NotContains(t, recorder.Body.String(), "call_bad")
	assert.Less(t, strings.Index(recorder.Body.String(), "call_good"), strings.Index(recorder.Body.String(), `"kind":"terminal"`))
}

func TestOpenAIRecoveryStreamWriterChatRequiresEveryChoice(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryChatSSE, nil)
	w.expectedChoices = 2
	w.BeginAttempt(1)
	writeRecoveryFrame(t, w, `{"id":"chat_a","choices":[{"index":0,"delta":{"content":"first"},"finish_reason":"stop"},{"index":1,"delta":{"content":"second"},"finish_reason":null}]}`)
	assert.Contains(t, recorder.Body.String(), "first")
	assert.Contains(t, recorder.Body.String(), "second")
	writeRecoveryFrame(t, w, "[DONE]")
	var failover *UpstreamFailoverError
	require.ErrorAs(t, w.CommitAttempt(""), &failover)
	assert.NotContains(t, recorder.Body.String(), "[DONE]")
}

func TestOpenAIRecoveryStreamWriterStructuredUnitStaysPrivateUntilValidated(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryChatSSE, nil)
	w.request = []byte(`{"response_format":{"type":"json_object"}}`)
	w.structured = true
	w.BeginAttempt(1)
	writeRecoveryFrame(t, w, `{"id":"chat_a","choices":[{"index":0,"delta":{"content":"broken"},"finish_reason":null}]}`)
	writeRecoveryFrame(t, w, `{"id":"chat_a","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)
	writeRecoveryFrame(t, w, "[DONE]")
	var failover *UpstreamFailoverError
	require.ErrorAs(t, w.CommitAttempt(""), &failover)
	assert.Empty(t, recorder.Body.String())
	require.NoError(t, w.AbandonAttempt())
	w.BeginAttempt(2)
	writeRecoveryFrame(t, w, `{"id":"chat_b","choices":[{"index":0,"delta":{"content":"{\"ok\":true}"},"finish_reason":null}]}`)
	writeRecoveryFrame(t, w, `{"id":"chat_b","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)
	writeRecoveryFrame(t, w, "[DONE]")
	assert.Empty(t, recorder.Body.String())
	require.NoError(t, w.CommitAttempt(""))
	assert.NotContains(t, recorder.Body.String(), "broken")
	assert.NotContains(t, recorder.Body.String(), "Regenerating")
	assert.Contains(t, recorder.Body.String(), `"content":"{\"ok\":true}"`)
}

func TestOpenAIRecoveryStreamWriterStrictJSONCommitsOrFailsWithBody(t *testing.T) {
	tests := []struct {
		name    string
		mode    OpenAIRecoveryWireFormat
		request string
		body    string
	}{
		{"responses", OpenAIRecoveryResponsesJSON, `{"text":{"format":{"type":"json_object"}}}`, `{"id":"resp_json","status":"completed","output":[{"id":"msg_json","type":"message","content":[{"type":"output_text","text":"{\"ok\":true}"}]}]}`},
		{"chat", OpenAIRecoveryChatJSON, `{"response_format":{"type":"json_object"}}`, `{"id":"chat_json","choices":[{"index":0,"message":{"role":"assistant","content":"{\"ok\":true}"},"finish_reason":"stop"}]}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w, recorder := recoveryTestWriter(t, tc.mode, nil)
			w.request = []byte(tc.request)
			w.structured = true
			require.NoError(t, w.BeginAttempt(1))
			_, err := w.WriteString(tc.body)
			require.NoError(t, err)
			assert.Empty(t, recorder.Body.String())
			require.NoError(t, w.CommitAttempt(""))
			assert.Contains(t, recorder.Body.String(), `"id":"resp_lr_test"`)
			assert.Contains(t, recorder.Body.String(), `\"ok\":true`)
		})
	}
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryChatSSE, nil)
	w.structured = true
	require.NoError(t, w.BeginAttempt(1))
	writeRecoveryFrame(t, w, `{"id":"chat_a","choices":[{"index":0,"delta":{"content":"{"},"finish_reason":null}]}`)
	w.WriteHeader(http.StatusServiceUnavailable)
	require.NoError(t, w.FailAttempt("upstream_error", "unavailable", "Try later"))
	assert.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	assert.Contains(t, recorder.Body.String(), `"message":"Try later"`)
	assert.NotContains(t, recorder.Body.String(), "kkai.recovery.v1")
}

func TestOpenAIRecoveryStreamWriterStructuredDoneCompletesMissingDelta(t *testing.T) {
	for _, prefix := range []string{"", "{"} {
		t.Run(map[bool]string{true: "done_only", false: "delta_prefix"}[prefix == ""], func(t *testing.T) {
			w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
			w.request = []byte(`{"text":{"format":{"type":"json_object"}}}`)
			w.structured = true
			require.NoError(t, w.BeginAttempt(1))
			if prefix != "" {
				writeRecoveryFrame(t, w, `{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg_json","delta":"`+prefix+`"}`)
			}
			writeRecoveryFrame(t, w, `{"type":"response.output_text.done","output_index":0,"content_index":0,"item_id":"msg_json","text":"{}"}`)
			writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_json","status":"completed","output":[]}}`)
			assert.Empty(t, recorder.Body.String(), "strict output remains private until commit")
			require.NoError(t, w.CommitAttempt(""))
			var streamed strings.Builder
			for _, line := range strings.Split(recorder.Body.String(), "\n") {
				data := strings.TrimPrefix(line, "data: ")
				if gjson.Get(data, "type").String() == "response.output_text.delta" {
					streamed.WriteString(gjson.Get(data, "delta").String())
				}
			}
			assert.Equal(t, "{}", streamed.String())
			assert.Equal(t, "{}", gjson.GetBytes(w.WinnerOutput(), "0.content.0.text").String())
		})
	}
}

func TestOpenAIRecoveryStreamWriterDoneOnlyOutputGetsRegenerationMarker(t *testing.T) {
	for _, field := range []string{"text", "refusal"} {
		t.Run(field, func(t *testing.T) {
			w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
			require.NoError(t, w.BeginAttempt(1))
			eventName := "response.output_text"
			if field == "refusal" {
				eventName = "response.refusal"
			}
			writeRecoveryFrame(t, w, `{"type":"`+eventName+`.done","output_index":0,"content_index":0,"item_id":"msg_first","`+field+`":"first"}`)
			assert.Contains(t, recorder.Body.String(), `"delta":"first"`)
			require.NoError(t, w.AbandonAttempt())
			assert.Contains(t, recorder.Body.String(), "Regenerating")
			doneCount := 0
			for _, line := range strings.Split(recorder.Body.String(), "\n") {
				data := strings.TrimPrefix(line, "data: ")
				if gjson.Get(data, "type").String() == eventName+".done" && gjson.Get(data, "item_id").String() == "resp_lr_test_item_1_0" {
					doneCount++
				}
			}
			assert.Equal(t, 1, doneCount)
		})
	}
}

func TestOpenAIRecoveryStreamWriterPartSnapshotsNormalizeBeforeCompletion(t *testing.T) {
	for _, structured := range []bool{false, true} {
		for _, field := range []string{"text", "refusal"} {
			for _, source := range []string{"added", "done"} {
				t.Run(strconv.FormatBool(structured)+"/"+field+"/"+source, func(t *testing.T) {
					w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
					w.request = []byte(`{"text":{"format":{"type":"json_object"}}}`)
					w.structured = structured
					require.NoError(t, w.BeginAttempt(1))
					partType, eventName, full, prefix := "output_text", "response.output_text", "{}", "{"
					if field == "refusal" {
						partType, eventName, full, prefix = "refusal", "response.refusal", "Cannot help", "Cannot"
					}
					writeRecoveryFrame(t, w, `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_snapshot","type":"message","role":"assistant","status":"in_progress","content":[]}}`)
					initial := ""
					if source == "added" {
						initial = full
					}
					writeRecoveryFrame(t, w, `{"type":"response.content_part.added","output_index":0,"content_index":0,"item_id":"msg_snapshot","part":{"type":"`+partType+`","`+field+`":"`+initial+`"}}`)
					if source == "done" {
						writeRecoveryFrame(t, w, `{"type":"`+eventName+`.delta","output_index":0,"content_index":0,"item_id":"msg_snapshot","delta":"`+prefix+`"}`)
					}
					writeRecoveryFrame(t, w, `{"type":"response.content_part.done","output_index":0,"content_index":0,"item_id":"msg_snapshot","part":{"type":"`+partType+`","`+field+`":"`+full+`"}}`)
					if structured {
						assert.Empty(t, recorder.Body.String())
						writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_snapshot","status":"completed","output":[]}}`)
						require.NoError(t, w.CommitAttempt(""))
						assert.Equal(t, full, gjson.GetBytes(w.WinnerOutput(), "0.content.0."+field).String())
					} else {
						require.NoError(t, w.AbandonAttempt())
						assert.Contains(t, recorder.Body.String(), "Regenerating")
					}
					var streamed strings.Builder
					textDone, partDone := 0, 0
					addedSeq, firstDeltaSeq := -1, -1
					for _, line := range strings.Split(recorder.Body.String(), "\n") {
						data := strings.TrimPrefix(line, "data: ")
						if gjson.Get(data, "item_id").String() != "resp_lr_test_item_1_0" {
							continue
						}
						switch gjson.Get(data, "type").String() {
						case "response.content_part.added":
							addedSeq = int(gjson.Get(data, "sequence_number").Int())
							assert.Empty(t, gjson.Get(data, "part."+field).String())
						case eventName + ".delta":
							if firstDeltaSeq < 0 {
								firstDeltaSeq = int(gjson.Get(data, "sequence_number").Int())
							}
							streamed.WriteString(gjson.Get(data, "delta").String())
						case eventName + ".done":
							textDone++
						case "response.content_part.done":
							partDone++
						}
					}
					assert.Equal(t, full, streamed.String())
					assert.Equal(t, 1, textDone)
					assert.Equal(t, 1, partDone)
					require.GreaterOrEqual(t, addedSeq, 0)
					assert.Less(t, addedSeq, firstDeltaSeq)
				})
			}
		}
	}
}

func TestOpenAIRecoveryStreamWriterItemSnapshotsCloseAfterContent(t *testing.T) {
	for _, field := range []string{"text", "refusal"} {
		for _, source := range []string{"added", "done"} {
			t.Run(field+"/"+source, func(t *testing.T) {
				w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
				require.NoError(t, w.BeginAttempt(1))
				partType, eventName, full := "output_text", "response.output_text", "{}"
				if field == "refusal" {
					partType, eventName, full = "refusal", "response.refusal", "Cannot help"
				}
				content := `[{"type":"` + partType + `","` + field + `":"` + full + `"}]`
				initial := "[]"
				if source == "added" {
					initial = content
				}
				writeRecoveryFrame(t, w, `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_snapshot","type":"message","role":"assistant","status":"in_progress","content":`+initial+`}}`)
				if source == "added" {
					assert.Contains(t, recorder.Body.String(), `"delta":"`+full+`"`, "non-empty item snapshots stream immediately")
				}
				item := `{"id":"msg_snapshot","type":"message","role":"assistant","status":"completed","content":` + content + `}`
				writeRecoveryFrame(t, w, `{"type":"response.output_item.done","output_index":0,"item":`+item+`}`)
				assert.Contains(t, recorder.Body.String(), `"delta":"`+full+`"`)
				writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_snapshot","status":"completed","output":[`+item+`]}}`)
				require.NoError(t, w.CommitAttempt(""))
				var streamed strings.Builder
				textDone, partDone, itemDone := 0, 0, 0
				lastPartSeq, itemDoneSeq := -1, -1
				for _, line := range strings.Split(recorder.Body.String(), "\n") {
					data := strings.TrimPrefix(line, "data: ")
					typeName := gjson.Get(data, "type").String()
					if typeName == "response.output_item.done" {
						itemDone++
						itemDoneSeq = int(gjson.Get(data, "sequence_number").Int())
					}
					if gjson.Get(data, "item_id").String() != "resp_lr_test_item_1_0" {
						continue
					}
					lastPartSeq = int(gjson.Get(data, "sequence_number").Int())
					switch typeName {
					case eventName + ".delta":
						streamed.WriteString(gjson.Get(data, "delta").String())
					case eventName + ".done":
						textDone++
					case "response.content_part.done":
						partDone++
					}
				}
				assert.Equal(t, full, streamed.String())
				assert.Equal(t, 1, textDone)
				assert.Equal(t, 1, partDone)
				assert.Equal(t, 1, itemDone)
				require.GreaterOrEqual(t, lastPartSeq, 0)
				assert.Less(t, lastPartSeq, itemDoneSeq, "no content event may follow item completion")
				assert.Equal(t, full, gjson.GetBytes(w.WinnerOutput(), "0.content.0."+field).String())
			})
		}
	}
}

func TestOpenAIRecoveryStreamWriterRejectsEmptyStructuredCompletion(t *testing.T) {
	for _, format := range []string{"json_object", "json_schema"} {
		for _, tc := range []struct {
			name string
			mode OpenAIRecoveryWireFormat
			body string
		}{
			{"responses SSE no output", OpenAIRecoveryResponsesSSE, `{"type":"response.completed","response":{"id":"resp_empty","status":"completed","output":[]}}`},
			{"responses SSE empty text", OpenAIRecoveryResponsesSSE, `{"type":"response.completed","response":{"id":"resp_empty","status":"completed","output":[{"id":"msg_empty","type":"message","content":[{"type":"output_text","text":""}]}]}}`},
			{"chat SSE", OpenAIRecoveryChatSSE, `{"id":"chat_empty","choices":[{"index":0,"delta":{"content":""},"finish_reason":"stop"}]}`},
			{"responses JSON no output", OpenAIRecoveryResponsesJSON, `{"id":"resp_empty","status":"completed","output":[]}`},
			{"responses JSON empty text", OpenAIRecoveryResponsesJSON, `{"id":"resp_empty","status":"completed","output":[{"id":"msg_empty","type":"message","content":[{"type":"output_text","text":""}]}]}`},
			{"chat JSON", OpenAIRecoveryChatJSON, `{"id":"chat_empty","choices":[{"index":0,"message":{"role":"assistant","content":""},"finish_reason":"stop"}]}`},
			{"chat JSON missing incomplete message", OpenAIRecoveryChatJSON, `{"id":"chat_empty","choices":[{"index":0,"finish_reason":"length"}]}`},
		} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				published := false
				w, recorder := recoveryTestWriter(t, tc.mode, func(string) error { published = true; return nil })
				w.request = []byte(`{"text":{"format":{"type":"` + format + `","schema":{"type":"object"}}},"response_format":{"type":"` + format + `","json_schema":{"schema":{"type":"object"}}}}`)
				w.structured = true
				require.NoError(t, w.BeginAttempt(1))
				if tc.mode == OpenAIRecoveryResponsesSSE || tc.mode == OpenAIRecoveryChatSSE {
					writeRecoveryFrame(t, w, tc.body)
					if tc.mode == OpenAIRecoveryChatSSE {
						writeRecoveryFrame(t, w, "[DONE]")
					}
				} else {
					_, err := w.WriteString(tc.body)
					require.NoError(t, err)
				}
				var failover *UpstreamFailoverError
				require.ErrorAs(t, w.CommitAttempt(""), &failover)
				assert.False(t, published, "an invalid structured winner must never publish its alias")
				assert.False(t, w.Finalized())
				assert.Empty(t, recorder.Body.String(), "invalid structured output remains private for recovery")
			})
		}
	}
}

func TestOpenAIRecoveryStreamWriterStructuredNoTextTerminalAlternatives(t *testing.T) {
	for _, tc := range []struct {
		name string
		mode OpenAIRecoveryWireFormat
		body string
		want string
	}{
		{"responses SSE refusal", OpenAIRecoveryResponsesSSE, `{"type":"response.completed","response":{"id":"resp_refusal","status":"completed","output":[{"id":"msg_refusal","type":"message","content":[{"type":"refusal","refusal":"Cannot help with that request"}]}]}}`, "Cannot help"},
		{"chat SSE refusal", OpenAIRecoveryChatSSE, `{"id":"chat_refusal","choices":[{"index":0,"delta":{"refusal":"Cannot help with that request"},"finish_reason":"stop"}]}`, "Cannot help"},
		{"responses JSON refusal", OpenAIRecoveryResponsesJSON, `{"id":"resp_refusal","status":"completed","output":[{"id":"msg_refusal","type":"message","content":[{"type":"refusal","refusal":"Cannot help with that request"}]}]}`, "Cannot help"},
		{"chat JSON refusal", OpenAIRecoveryChatJSON, `{"id":"chat_refusal","choices":[{"index":0,"message":{"role":"assistant","content":null,"refusal":"Cannot help with that request"},"finish_reason":"stop"}]}`, "Cannot help"},
		{"responses SSE tool", OpenAIRecoveryResponsesSSE, `{"type":"response.completed","response":{"id":"resp_tool","status":"completed","output":[{"id":"tool_one","type":"function_call","name":"lookup","call_id":"call_one","arguments":"{}","status":"completed"}]}}`, "call_one"},
		{"chat SSE tool", OpenAIRecoveryChatSSE, `{"id":"chat_tool","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_one","type":"function","function":{"name":"lookup","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`, "call_one"},
		{"responses JSON tool", OpenAIRecoveryResponsesJSON, `{"id":"resp_tool","status":"completed","output":[{"id":"tool_one","type":"function_call","name":"lookup","call_id":"call_one","arguments":"{}","status":"completed"}]}`, "call_one"},
		{"chat JSON tool", OpenAIRecoveryChatJSON, `{"id":"chat_tool","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_one","type":"function","function":{"name":"lookup","arguments":"{}"}}]},"finish_reason":"tool_calls"}]}`, "call_one"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			published := false
			w, recorder := recoveryTestWriter(t, tc.mode, func(string) error { published = true; return nil })
			w.request = []byte(`{"text":{"format":{"type":"json_object"}},"response_format":{"type":"json_object"},"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}`)
			w.structured = true
			require.NoError(t, w.BeginAttempt(1))
			if tc.mode == OpenAIRecoveryResponsesSSE || tc.mode == OpenAIRecoveryChatSSE {
				writeRecoveryFrame(t, w, tc.body)
				if tc.mode == OpenAIRecoveryChatSSE {
					writeRecoveryFrame(t, w, "[DONE]")
				}
			} else {
				_, err := w.WriteString(tc.body)
				require.NoError(t, err)
			}
			assert.Empty(t, recorder.Body.String())
			require.NoError(t, w.CommitAttempt(""))
			assert.True(t, published)
			assert.Contains(t, recorder.Body.String(), tc.want)
		})
	}
}

func TestOpenAIRecoveryStreamWriterStructuredChoiceAndAttemptIsolation(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryChatSSE, nil)
	w.request = []byte(`{"response_format":{"type":"json_object"}}`)
	w.structured = true
	w.expectedChoices = 2
	require.NoError(t, w.BeginAttempt(1))
	writeRecoveryFrame(t, w, `{"choices":[{"index":1,"delta":{"refusal":"Cannot help"},"finish_reason":null}]}`)
	require.NoError(t, w.AbandonAttempt())
	require.NoError(t, w.BeginAttempt(2))
	writeRecoveryFrame(t, w, `{"choices":[{"index":0,"delta":{"content":"{}"},"finish_reason":"stop"},{"index":1,"delta":{"content":""},"finish_reason":"stop"}]}`)
	writeRecoveryFrame(t, w, "[DONE]")
	var failover *UpstreamFailoverError
	require.ErrorAs(t, w.CommitAttempt(""), &failover)
	assert.Empty(t, recorder.Body.String(), "another choice or abandoned refusal cannot authorize an empty winner")
}

func TestOpenAIRecoveryStreamWriterStructuredRefusalPreservesStreamedPrefix(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
	w.request = []byte(`{"text":{"format":{"type":"json_object"}}}`)
	w.structured = true
	require.NoError(t, w.BeginAttempt(1))
	writeRecoveryFrame(t, w, `{"type":"response.created","response":{"id":"resp_refusal","status":"in_progress","output":[]}}`)
	writeRecoveryFrame(t, w, `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_refusal","type":"message","role":"assistant","status":"in_progress","content":[]}}`)
	writeRecoveryFrame(t, w, `{"type":"response.content_part.added","output_index":0,"content_index":0,"item_id":"msg_refusal","part":{"type":"refusal","refusal":""}}`)
	writeRecoveryFrame(t, w, `{"type":"response.refusal.delta","output_index":0,"content_index":0,"item_id":"msg_refusal","delta":"Cannot help"}`)
	writeRecoveryFrame(t, w, `{"type":"response.refusal.done","output_index":0,"content_index":0,"item_id":"msg_refusal","refusal":"Cannot help"}`)
	writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_refusal","status":"completed","output":[]}}`)
	require.NoError(t, w.CommitAttempt(""))
	assert.Equal(t, 1, strings.Count(recorder.Body.String(), `"type":"response.refusal.delta"`))
	assert.Equal(t, 1, strings.Count(recorder.Body.String(), `"type":"response.refusal.done"`))
	assert.NotContains(t, recorder.Body.String(), `"type":"response.output_text`)
	assert.JSONEq(t, `[{"id":"msg_refusal","type":"message","role":"assistant","status":"completed","content":[{"type":"refusal","refusal":"Cannot help"}]}]`, string(w.WinnerOutput()))
}

func TestOpenAIRecoveryStreamWriterAbandonsRefusalBeforeSeparateMarker(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
	require.NoError(t, w.BeginAttempt(1))
	writeRecoveryFrame(t, w, `{"type":"response.created","response":{"id":"resp_refusal","status":"in_progress","output":[]}}`)
	writeRecoveryFrame(t, w, `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_refusal","type":"message","role":"assistant","status":"in_progress","content":[]}}`)
	writeRecoveryFrame(t, w, `{"type":"response.content_part.added","output_index":0,"content_index":0,"item_id":"msg_refusal","part":{"type":"refusal","refusal":""}}`)
	writeRecoveryFrame(t, w, `{"type":"response.refusal.delta","output_index":0,"content_index":0,"item_id":"msg_refusal","delta":"Cannot help"}`)
	require.NoError(t, w.AbandonAttempt())
	require.NoError(t, w.BeginAttempt(2))
	writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_winner","status":"completed","output":[{"id":"msg_winner","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"winner"}]}]}}`)
	require.NoError(t, w.CommitAttempt(""))
	body := recorder.Body.String()
	assert.Equal(t, 1, strings.Count(body, `"type":"response.refusal.done"`))
	assert.Less(t, strings.Index(body, `"kind":"attempt_abandoned"`), strings.Index(body, `"type":"response.refusal.done"`))
	assert.Less(t, strings.Index(body, `"type":"response.refusal.done"`), strings.Index(body, "Regenerating"))
	assert.Contains(t, body, `"refusal":"Cannot help"`)
	assert.NotContains(t, body, `"refusal":"Cannot help\n`)
	assert.Equal(t, 1, strings.Count(body, `"type":"response.completed"`))
	assert.NotContains(t, string(w.WinnerOutput()), "Cannot help")
	assert.NotContains(t, string(w.WinnerOutput()), "Regenerating")
}

func TestOpenAIRecoveryStreamWriterClosesEveryPartBeforeRegeneration(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
	require.NoError(t, w.BeginAttempt(1))
	writeRecoveryFrame(t, w, `{"type":"response.created","response":{"id":"resp_first","status":"in_progress","output":[]}}`)
	writeRecoveryFrame(t, w, `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_first","type":"message","role":"assistant","status":"in_progress","content":[]}}`)
	writeRecoveryFrame(t, w, `{"type":"response.content_part.added","output_index":0,"content_index":0,"item_id":"msg_first","part":{"type":"output_text","text":""}}`)
	writeRecoveryFrame(t, w, `{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg_first","delta":"A"}`)
	writeRecoveryFrame(t, w, `{"type":"response.output_text.done","output_index":0,"content_index":0,"item_id":"msg_first","text":"A"}`)
	writeRecoveryFrame(t, w, `{"type":"response.content_part.done","output_index":0,"content_index":0,"item_id":"msg_first","part":{"type":"output_text","text":"A","annotations":[{"type":"url_citation","url":"https://example.test/source","title":"Source","start_index":0,"end_index":1}]}}`)
	writeRecoveryFrame(t, w, `{"type":"response.content_part.added","output_index":0,"content_index":1,"item_id":"msg_first","part":{"type":"output_text","text":""}}`)
	writeRecoveryFrame(t, w, `{"type":"response.output_text.delta","output_index":0,"content_index":1,"item_id":"msg_first","delta":"B"}`)
	require.NoError(t, w.AbandonAttempt())
	require.NoError(t, w.BeginAttempt(2))
	writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_winner","status":"completed","output":[{"id":"msg_winner","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"winner"}]}]}}`)
	require.NoError(t, w.CommitAttempt(""))
	body := recorder.Body.String()
	oldPartDone := make(map[int]int)
	oldContentPartDone := make(map[int]int)
	oldPartCloseSeq := make(map[int]int)
	oldItemDone := 0
	markerDeltaSeq := 0
	for _, line := range strings.Split(body, "\n") {
		data := strings.TrimPrefix(line, "data: ")
		eventType := gjson.Get(data, "type").String()
		itemID := gjson.Get(data, "item_id").String()
		contentIndex := int(gjson.Get(data, "content_index").Int())
		if eventType == "response.output_item.done" && gjson.Get(data, "item.id").String() == "resp_lr_test_item_1_0" {
			oldItemDone++
		}
		if itemID == "resp_lr_test_item_1_0" {
			switch eventType {
			case "response.output_text.done":
				oldPartDone[contentIndex]++
				oldPartCloseSeq[contentIndex] = int(gjson.Get(data, "sequence_number").Int())
			case "response.content_part.done":
				oldContentPartDone[contentIndex]++
				oldPartCloseSeq[contentIndex] = int(gjson.Get(data, "sequence_number").Int())
			}
		}
		if eventType == "response.output_text.delta" && strings.Contains(gjson.Get(data, "delta").String(), "Regenerating") {
			markerDeltaSeq = int(gjson.Get(data, "sequence_number").Int())
		}
	}
	assert.Equal(t, map[int]int{0: 1, 1: 1}, oldPartDone, "each old text part must close once")
	assert.Equal(t, map[int]int{0: 1, 1: 1}, oldContentPartDone, "each old content part must close once")
	assert.Equal(t, 1, oldItemDone, "the old message item must close once")
	require.Positive(t, markerDeltaSeq)
	assert.Less(t, oldPartCloseSeq[0], oldPartCloseSeq[1])
	assert.Less(t, oldPartCloseSeq[1], markerDeltaSeq, "all old parts must close before the regeneration marker")
	assert.Contains(t, body, `"id":"resp_lr_test_marker_1"`)
	assert.NotContains(t, string(w.WinnerOutput()), `"text":"A"`)
	assert.NotContains(t, string(w.WinnerOutput()), `"text":"B"`)
	var completed string
	for _, line := range strings.Split(body, "\n") {
		data := strings.TrimPrefix(line, "data: ")
		if gjson.Get(data, "type").String() == "response.completed" {
			completed = data
		}
	}
	require.NotEmpty(t, completed)
	assert.Equal(t, "A", gjson.Get(completed, "response.output.0.content.0.text").String())
	assert.Equal(t, "https://example.test/source", gjson.Get(completed, "response.output.0.content.0.annotations.0.url").String())
	assert.Equal(t, "B", gjson.Get(completed, "response.output.0.content.1.text").String())
	assert.Equal(t, "winner", gjson.Get(completed, "response.output.2.content.0.text").String())
}

func TestOpenAIRecoveryStreamWriterKeepsTextAndRefusalPartsSeparate(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
	require.NoError(t, w.BeginAttempt(1))
	writeRecoveryFrame(t, w, `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_first","type":"message","role":"assistant","status":"in_progress","content":[]}}`)
	writeRecoveryFrame(t, w, `{"type":"response.content_part.added","output_index":0,"content_index":0,"item_id":"msg_first","part":{"type":"output_text","text":""}}`)
	writeRecoveryFrame(t, w, `{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg_first","delta":"A"}`)
	writeRecoveryFrame(t, w, `{"type":"response.output_text.done","output_index":0,"content_index":0,"item_id":"msg_first","text":"A"}`)
	writeRecoveryFrame(t, w, `{"type":"response.content_part.done","output_index":0,"content_index":0,"item_id":"msg_first","part":{"type":"output_text","text":"A"}}`)
	writeRecoveryFrame(t, w, `{"type":"response.content_part.added","output_index":0,"content_index":1,"item_id":"msg_first","part":{"type":"refusal","refusal":""}}`)
	writeRecoveryFrame(t, w, `{"type":"response.refusal.delta","output_index":0,"content_index":1,"item_id":"msg_first","delta":"B"}`)
	require.NoError(t, w.AbandonAttempt())
	require.NoError(t, w.BeginAttempt(2))
	writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_winner","status":"completed","output":[{"id":"msg_winner","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"winner"}]}]}}`)
	require.NoError(t, w.CommitAttempt(""))
	body := recorder.Body.String()
	refusalDone := 0
	refusalDoneContentIndex := -1
	for _, line := range strings.Split(body, "\n") {
		data := strings.TrimPrefix(line, "data: ")
		if gjson.Get(data, "type").String() == "response.refusal.done" && gjson.Get(data, "item_id").String() == "resp_lr_test_item_1_0" {
			refusalDone++
			refusalDoneContentIndex = int(gjson.Get(data, "content_index").Int())
			assert.Equal(t, "B", gjson.Get(data, "refusal").String())
		}
	}
	assert.Equal(t, 1, refusalDone)
	assert.Equal(t, 1, refusalDoneContentIndex)
	assert.Contains(t, body, `"content":[{"text":"A","type":"output_text"},{"refusal":"B","type":"refusal"}]`)
	assert.NotContains(t, body, `"refusal":"AB"`)
	assert.NotContains(t, string(w.WinnerOutput()), `"refusal":"B"`)
}

func TestOpenAIRecoveryStreamWriterSuccessfulMultipleParts(t *testing.T) {
	for _, terminalOnly := range []bool{false, true} {
		t.Run(map[bool]string{false: "streamed", true: "terminal_only"}[terminalOnly], func(t *testing.T) {
			w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
			require.NoError(t, w.BeginAttempt(1))
			if !terminalOnly {
				writeRecoveryFrame(t, w, `{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_multi","type":"message","role":"assistant","status":"in_progress","content":[]}}`)
				for index, value := range []string{"A", "B"} {
					writeRecoveryFrame(t, w, `{"type":"response.content_part.added","output_index":0,"content_index":`+strconv.Itoa(index)+`,"item_id":"msg_multi","part":{"type":"output_text","text":""}}`)
					writeRecoveryFrame(t, w, `{"type":"response.output_text.delta","output_index":0,"content_index":`+strconv.Itoa(index)+`,"item_id":"msg_multi","delta":"`+value+`"}`)
					writeRecoveryFrame(t, w, `{"type":"response.output_text.done","output_index":0,"content_index":`+strconv.Itoa(index)+`,"item_id":"msg_multi","text":"`+value+`"}`)
					writeRecoveryFrame(t, w, `{"type":"response.content_part.done","output_index":0,"content_index":`+strconv.Itoa(index)+`,"item_id":"msg_multi","part":{"type":"output_text","text":"`+value+`"}}`)
				}
			}
			writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_multi","status":"completed","output":[{"id":"msg_multi","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"A"},{"type":"output_text","text":"B"}]}]}}`)
			require.NoError(t, w.CommitAttempt(""))
			assert.Contains(t, recorder.Body.String(), `"content_index":1`)
			assert.JSONEq(t, `[{"id":"msg_multi","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"A"},{"type":"output_text","text":"B"}]}]`, string(w.WinnerOutput()))
			var completed string
			for _, line := range strings.Split(recorder.Body.String(), "\n") {
				data := strings.TrimPrefix(line, "data: ")
				if gjson.Get(data, "type").String() == "response.completed" {
					completed = data
				}
			}
			require.NotEmpty(t, completed)
			assert.Equal(t, "A", gjson.Get(completed, "response.output.0.content.0.text").String())
			assert.Equal(t, "B", gjson.Get(completed, "response.output.0.content.1.text").String())
		})
	}
}

func TestOpenAIRecoveryStreamWriterOpaqueFailureDoesNotLeakCiphertext(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
	require.NoError(t, w.BeginAttempt(1))
	writeRecoveryFrame(t, w, `{"type":"response.output_item.done","output_index":0,"item":{"id":"reason_old","type":"reasoning","summary":[],"encrypted_content":"oldcipher"}}`)
	require.NoError(t, w.AbandonAttempt())
	require.NoError(t, w.BeginAttempt(2))
	writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_final","status":"completed","output":[{"type":"reasoning","id":"reason_final","summary":[],"encrypted_content":"winnercipher"}]}}`)
	assert.NotContains(t, recorder.Body.String(), "winnercipher")
	require.NoError(t, w.CommitAttempt(""))
	assert.NotContains(t, recorder.Body.String(), "oldcipher")
	assert.Contains(t, recorder.Body.String(), `"id":"reason_final"`)
	assert.Contains(t, recorder.Body.String(), "winnercipher")
}

func TestOpenAIRecoveryStreamWriterReasoningSummaryStreamsWithoutLateCipherLeak(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
	require.NoError(t, w.BeginAttempt(1))
	writeRecoveryFrame(t, w, `{"type":"response.output_item.added","output_index":0,"item":{"id":"reason_a","type":"reasoning","summary":[]}}`)
	writeRecoveryFrame(t, w, `{"type":"response.reasoning_summary_part.added","output_index":0,"summary_index":0,"item_id":"reason_a","part":{"type":"summary_text","text":""}}`)
	writeRecoveryFrame(t, w, `{"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":0,"item_id":"reason_a","delta":"thinking"}`)
	assert.Contains(t, recorder.Body.String(), `"delta":"thinking"`)
	writeRecoveryFrame(t, w, `{"type":"response.output_item.done","output_index":0,"item":{"id":"reason_a","type":"reasoning","summary":[{"type":"summary_text","text":"thinking"}],"encrypted_content":"late_failed_cipher"}}`)
	assert.NotContains(t, recorder.Body.String(), "late_failed_cipher")
	require.NoError(t, w.AbandonAttempt())
	assert.NotContains(t, recorder.Body.String(), "late_failed_cipher")
	assert.Contains(t, recorder.Body.String(), `"type":"response.reasoning_summary_text.done"`)
	require.NoError(t, w.BeginAttempt(2))
	writeRecoveryFrame(t, w, `{"type":"response.completed","response":{"id":"resp_winner","status":"completed","output":[{"type":"reasoning","id":"reason_b","summary":[],"encrypted_content":"winner_cipher"}]}}`)
	require.NoError(t, w.CommitAttempt(""))
	body := recorder.Body.String()
	assert.NotContains(t, body, "late_failed_cipher")
	assert.Contains(t, body, "winner_cipher")
	assert.Equal(t, 1, strings.Count(body, `"type":"response.completed"`))
}

func TestOpenAIRecoveryStreamWriterCompatibilityDispatchAbandonsAutomatically(t *testing.T) {
	w, recorder := recoveryTestWriter(t, OpenAIRecoveryChatSSE, nil)
	require.NoError(t, w.BeginAttempt(1))
	require.NoError(t, w.BeginAttempt(2))
	writeRecoveryFrame(t, w, `{"id":"chat_b","choices":[{"index":0,"delta":{"content":"ok"},"finish_reason":null}]}`)
	writeRecoveryFrame(t, w, `{"id":"chat_b","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)
	writeRecoveryFrame(t, w, "[DONE]")
	require.NoError(t, w.CommitAttempt(""))
	assert.Contains(t, recorder.Body.String(), `"kind":"attempt_abandoned"`)
	assert.NotContains(t, recorder.Body.String(), "Regenerating")
}
