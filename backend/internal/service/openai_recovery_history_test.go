package service

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenAIRecoveryHistoryBuildAndReplay(t *testing.T) {
	first, err := BuildOpenAIRecoverySnapshot(
		[]byte(`{"model":"m","input":"first question","instructions":"old instructions"}`),
		[]byte(`[{"id":"msg_actual","type":"message","role":"assistant","status":"completed","phase":"final_answer","content":[{"type":"output_text","text":"first answer","annotations":[]}]}]`), nil)
	require.NoError(t, err)
	assert.NotContains(t, string(first), `"phase"`)
	request := []byte(`{"model":"m","previous_response_id":"resp_actual","input":[{"role":"user","content":[{"type":"input_text","text":"next question"}]}],"instructions":"new instructions","stream":true}`)
	rebuilt, err := RebuildOpenAIRecoveryContinuation(request, first)
	require.NoError(t, err)
	assert.JSONEq(t, `{"model":"m","instructions":"new instructions","stream":true,"input":[{"type":"message","role":"user","content":"first question"},{"type":"message","role":"assistant","content":"first answer"},{"type":"message","role":"user","content":"next question"}]}`, string(rebuilt))

	second, err := BuildOpenAIRecoverySnapshot(request, []byte(`[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"second answer"}]}]`), first)
	require.NoError(t, err)
	var history openAIRecoveryHistory
	require.NoError(t, json.Unmarshal(second, &history))
	assert.Equal(t, 2, history.Turns)
	assert.Len(t, history.Items, 4)
	assert.NotContains(t, string(second), "msg_actual")
	assert.NotContains(t, string(second), "old instructions")
}

func TestOpenAIRecoveryHistoryRejectsIncompleteOrNonportableContext(t *testing.T) {
	for _, test := range []struct {
		name    string
		request string
		output  string
	}{
		{"missing parent", `{"previous_response_id":"resp_previous","input":"next"}`, `[]`},
		{"tool input", `{"input":[{"type":"function_call_output","call_id":"call_a","output":"ok"}]}`, `[]`},
		{"attachment", `{"input":[{"role":"user","content":[{"type":"input_image","image_url":"https://example.test/x"}]}]}`, `[]`},
		{"opaque output", `{"input":"text"}`, `[{"type":"reasoning","id":"rs_a","encrypted_content":"opaque"}]`},
		{"tool output", `{"input":"text"}`, `[{"type":"function_call","name":"charge","arguments":"{}","call_id":"call_a"}]`},
		{"unknown message field", `{"input":[{"role":"user","content":"text","unknown_semantics":true}]}`, `[]`},
		{"input message phase", `{"input":[{"role":"user","phase":"final_answer","content":"text"}]}`, `[]`},
		{"unknown message phase", `{"input":"text"}`, `[{"type":"message","role":"assistant","phase":"commentary","content":"answer"}]`},
		{"unknown part field", `{"input":[{"role":"user","content":[{"type":"input_text","text":"text","unknown_semantics":true}]}]}`, `[]`},
		{"null input", `{"input":null}`, `[]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := BuildOpenAIRecoverySnapshot([]byte(test.request), []byte(test.output), nil)
			require.ErrorIs(t, err, ErrOpenAIRecoveryHistoryUnavailable)
		})
	}
	_, err := RebuildOpenAIRecoveryContinuation([]byte(`{"input":"next"}`), nil)
	require.ErrorIs(t, err, ErrOpenAIRecoveryHistoryUnavailable)
	_, err = RebuildOpenAIRecoveryContinuation([]byte(`{"input":"next"}`), []byte(`{"version":1,"turns":16,"items":[]}`))
	require.ErrorIs(t, err, ErrOpenAIRecoveryHistoryUnavailable)
}

func TestOpenAIRecoveryHistoryPreservesExplicitRepeatedInput(t *testing.T) {
	parent := []byte(`{"version":1,"turns":1,"items":[{"type":"message","role":"user","content":"A"},{"type":"message","role":"assistant","content":"B"}]}`)
	request := []byte(`{"model":"m","previous_response_id":"resp_previous","input":[{"role":"user","content":"A"},{"role":"assistant","content":"B"},{"role":"user","content":"C"}]}`)
	rebuilt, err := RebuildOpenAIRecoveryContinuation(request, parent)
	require.NoError(t, err)
	var body struct {
		Input []json.RawMessage `json:"input"`
	}
	require.NoError(t, json.Unmarshal(rebuilt, &body))
	assert.Len(t, body.Input, 5)
	next, err := BuildOpenAIRecoverySnapshot(request, []byte(`[{"role":"assistant","content":"D"}]`), parent)
	require.NoError(t, err)
	var history openAIRecoveryHistory
	require.NoError(t, json.Unmarshal(next, &history))
	assert.Len(t, history.Items, 6)
}
