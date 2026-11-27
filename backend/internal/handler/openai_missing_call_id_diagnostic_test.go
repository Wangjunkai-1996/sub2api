//go:build unit

package handler

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestOpenAIResponsesMissingCallIDDiagnostic(t *testing.T) {
	testLog := func(body string) (bool, map[string]any) {
		t.Helper()
		log, entries := newObservedLogger(t)
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		valid := (&OpenAIGatewayHandler{}).validateFunctionCallOutputRequest(c, []byte(body), log)
		if valid {
			require.Zero(t, entries.Len())
			return true, nil
		}
		require.Equal(t, 400, recorder.Code)
		require.Equal(t, "openai.request_validation_failed", entries.All()[0].Message)
		return false, entries.All()[0].ContextMap()
	}

	t.Run("mixed rejected items and private fields", func(t *testing.T) {
		valid, fields := testLog(`{"input":[
			{"type":"function_call_output","call_id":"PRIVATE_ID","output":"PRIVATE_HISTORY"},
			{"type":"tool_search_output","name":"PRIVATE_NAME","namespace":"PRIVATE_NS","output":{"PRIVATE_PROPERTY":"PRIVATE_OUTPUT"}},
			{"type":"function_call_output","name":"terminal_info","namespace":"agent","call_id":null,"output":"PRIVATE_TERMINAL"},
			{"type":"mcp_tool_call_output","call_id":" \t","output":["PRIVATE_ARRAY"]},
			{"type":"custom_tool_call_output","call_id":"","output":null}]}`)
		require.False(t, valid)
		raw, err := json.Marshal(fields)
		require.NoError(t, err)
		require.JSONEq(t, `{"reason":"function_call_output_missing_call_id","missing_call_id_items_truncated":false,"missing_call_id_items":[
			{"index":1,"type":"tool_search_output","name":"other","namespace":"other","call_id_shape":"missing","output_shape":"object"},
			{"index":2,"type":"function_call_output","name":"terminal_info","namespace":"agent","call_id_shape":"null","output_shape":"string"},
			{"index":3,"type":"mcp_tool_call_output","name":"missing","namespace":"missing","call_id_shape":"blank_string","output_shape":"array"},
			{"index":4,"type":"custom_tool_call_output","name":"missing","namespace":"missing","call_id_shape":"empty_string","output_shape":"null"}]}`, string(raw))
	})

	t.Run("bounded samples", func(t *testing.T) {
		item := `{"type":"function_call_output"}`
		_, fields := testLog(`{"input":[` + strings.Repeat(item+",", 8) + item + `]}`)
		require.True(t, fields["missing_call_id_items_truncated"].(bool))
		require.Len(t, fields["missing_call_id_items"], 8)
	})

	t.Run("other validation reasons", func(t *testing.T) {
		_, fields := testLog(`{"input":[{"type":"function_call_output","call_id":"valid"}]}`)
		require.NotContains(t, fields, "missing_call_id_items")
		valid, _ := testLog(`{"previous_response_id":"resp_test","input":[{"type":"function_call_output"}]}`)
		require.True(t, valid)
	})
}
