package securityadmission

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"unicode"
)

func TestMaterializeCurrentTurnKeepsInstructionsAndToolSuffix(t *testing.T) {
	body := []byte(`{"model":"x","messages":[{"role":"system","content":"root-system"},{"role":"user","content":"old-user"},{"role":"assistant","tool_calls":[{"type":"function","function":{"name":"lookup","arguments":"assistant-args"}}]},{"role":"tool","tool_call_id":"call-1","content":"current-tool-output"}]}`)
	admission, err := Classify(string(ProtocolOpenAIChat), body, Options{Lineage: LineageTrusted})
	if err != nil || admission.Class() != RequestAuditableText {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	full, err := admission.MaterializeDocument(body, TextScopeFullTranscript)
	if err != nil {
		t.Fatalf("full materialize: %v", err)
	}
	if !strings.Contains(full.Text, "old-user") {
		t.Fatalf("full transcript lost old user: %q", full.Text)
	}
	current, err := admission.MaterializeDocument(body, TextScopeCurrentTurn)
	if err != nil {
		t.Fatalf("current materialize: %v", err)
	}
	for _, want := range []string{"root-system", "assistant-args", "current-tool-output"} {
		if !strings.Contains(current.Text, want) {
			t.Fatalf("current transcript missing %q: %q", want, current.Text)
		}
	}
	if strings.Contains(current.Text, "old-user") {
		t.Fatalf("current transcript included old user: %q", current.Text)
	}

	// An untrusted caller cannot opt into a narrower view by accident.
	untrusted, err := Classify(string(ProtocolOpenAIChat), body, Options{Lineage: LineageUntrusted})
	if err != nil {
		t.Fatalf("untrusted classify: %v", err)
	}
	untrustedCurrent, err := untrusted.MaterializeDocument(body, TextScopeCurrentTurn)
	if err != nil || !strings.Contains(untrustedCurrent.Text, "old-user") {
		t.Fatalf("untrusted scope was narrowed: text=%q err=%v", untrustedCurrent.Text, err)
	}
}

func TestMaterializeCurrentTurnDoesNotFallBackFromEmptyToolGroup(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"old-user"},{"role":"tool","tool_call_id":"call-1","content":null}]}`)
	admission, err := Classify(string(ProtocolOpenAIChat), body, Options{Lineage: LineageTrusted})
	if err != nil || admission.Class() != RequestUninspectable || admission.Reason() != ReasonUnknownContentShape {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
}

func TestMaterializeCurrentTurnResponsesAndAnthropicLineage(t *testing.T) {
	responses := []byte(`{"instructions":"root-instructions","input":[{"type":"message","role":"user","content":"old-user"},{"type":"function_call","arguments":"call-args"},{"type":"function_call_output","output":"current-output"}]}`)
	admission, err := Classify(string(ProtocolOpenAIResponses), responses, Options{Lineage: LineageTrusted})
	if err != nil || admission.Class() != RequestAuditableText {
		t.Fatalf("responses admission=%+v err=%v", admission, err)
	}
	document, err := admission.MaterializeDocument(responses, TextScopeCurrentTurn)
	if err != nil {
		t.Fatalf("responses materialize: %v", err)
	}
	for _, want := range []string{"root-instructions", "old-user", "call-args", "current-output"} {
		if !strings.Contains(document.Text, want) {
			t.Fatalf("responses current missing %q: %q", want, document.Text)
		}
	}

	anthropic := []byte(`{"system":"root-system","messages":[{"role":"user","content":"old-user"},{"role":"assistant","content":[{"type":"tool_use","id":"call-1","input":{"q":"tool-input"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1","content":"current-result"}]}]}`)
	admission, err = Classify(string(ProtocolAnthropicMessages), anthropic, Options{Lineage: LineageTrusted})
	if err != nil || admission.Class() != RequestAuditableText {
		t.Fatalf("anthropic admission=%+v err=%v", admission, err)
	}
	document, err = admission.MaterializeDocument(anthropic, TextScopeCurrentTurn)
	if err != nil {
		t.Fatalf("anthropic materialize: %v", err)
	}
	for _, want := range []string{"root-system", "tool-input", "current-result"} {
		if !strings.Contains(document.Text, want) {
			t.Fatalf("anthropic current missing %q: %q", want, document.Text)
		}
	}
	if strings.Contains(document.Text, "old-user") {
		t.Fatalf("anthropic current included old user: %q", document.Text)
	}
}

func TestSafeJSONStringWordRejectsUnsafeBytesAtEveryOffset(t *testing.T) {
	for offset := 0; offset < 8; offset++ {
		for value := 0; value <= 0xff; value++ {
			var bytes [8]byte
			for index := range bytes {
				bytes[index] = 'a'
			}
			bytes[offset] = byte(value)
			word := binary.LittleEndian.Uint64(bytes[:])
			unsafe := value < 0x20 || value == '"' || value == '\\' || value >= 0x80
			if unsafe && safeJSONStringWord(word) {
				t.Fatalf("unsafe byte 0x%02x at offset %d accepted by fast path", value, offset)
			}
		}
	}
}

func TestJSONStringsCoverASCIIControlsEscapesAndUnicode(t *testing.T) {
	for value := 0; value <= 0x7f; value++ {
		want := string([]byte{byte(value)})
		encoded, err := json.Marshal(want)
		if err != nil {
			t.Fatalf("marshal byte 0x%02x: %v", value, err)
		}
		body := append([]byte(`{"messages":[{"role":"user","content":`), encoded...)
		body = append(body, []byte(`}]}`)...)
		admission, classifyErr := Classify(string(ProtocolOpenAIChat), body, Options{})
		wantClass := RequestAuditableText
		if unicode.IsSpace(rune(value)) {
			wantClass = RequestKnownNoText
		}
		if classifyErr != nil || admission.Class() != wantClass {
			t.Fatalf("byte 0x%02x admission=%+v err=%v body=%q", value, admission, classifyErr, body)
		}
		if wantClass == RequestKnownNoText {
			continue
		}
		text, materializeErr := admission.MaterializeText(body)
		if materializeErr != nil || text != want {
			t.Fatalf("byte 0x%02x text=%q err=%v want=%q", value, text, materializeErr, want)
		}
	}

	for _, want := range []string{
		"slash / quote \" backslash \\",
		"U+0080: \u0080",
		"U+07FF: \u07ff",
		"U+0800: \u0800",
		"U+FFFF: \uffff",
		"U+10000: \U00010000",
		"U+10FFFF: \U0010ffff",
		"surrogate pair: 😀",
	} {
		encoded, err := json.Marshal(want)
		if err != nil {
			t.Fatalf("marshal %q: %v", want, err)
		}
		body := append([]byte(`{"messages":[{"role":"user","content":`), encoded...)
		body = append(body, []byte(`}]}`)...)
		admission, classifyErr := Classify(string(ProtocolOpenAIChat), body, Options{})
		if classifyErr != nil || admission.Class() != RequestAuditableText {
			t.Fatalf("unicode %q admission=%+v err=%v", want, admission, classifyErr)
		}
		text, materializeErr := admission.MaterializeText(body)
		if materializeErr != nil || text != want {
			t.Fatalf("unicode %q text=%q err=%v", want, text, materializeErr)
		}
	}

	// A raw isolated high byte is not valid UTF-8 JSON and must not be treated
	// as scanner text or as known empty input.
	for value := 0x80; value <= 0xff; value++ {
		body := append([]byte(`{"messages":[{"role":"user","content":"`), byte(value))
		body = append(body, []byte(`"}]}`)...)
		_, classifyErr := Classify(string(ProtocolOpenAIChat), body, Options{})
		var parseErr *ParseError
		if !errors.As(classifyErr, &parseErr) {
			t.Fatalf("raw byte 0x%02x was not rejected as invalid JSON: %v", value, classifyErr)
		}
	}
}

func TestDuplicateJSONKeysUseDecodedNames(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "root", body: `{"input":"a","input":"b"}`},
		{name: "nested", body: `{"input":[{"type":"message","content":"a","content":"b"}]}`},
		{name: "unicode escape", body: `{"input":"a","\u0069nput":"b"}`},
		{name: "escaped suffix", body: `{"input":"a","inpu\u0074":"b"}`},
		{name: "skipped tool schema", body: `{"tools":[{"type":"function","function":{"name":"f","parameters":{"type":"object","type":"string"}}}],"input":"x"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			admission, err := Classify(string(ProtocolOpenAIResponses), []byte(test.body), Options{})
			if err != nil {
				t.Fatalf("duplicate returned parse error: %v", err)
			}
			if admission.Class() != RequestUninspectable || admission.Reason() != ReasonDuplicateJSONKey {
				t.Fatalf("admission=%+v", admission)
			}
		})
	}
}

func TestResponsesWebSocketControlAndPayloadShapes(t *testing.T) {
	control, err := Classify(string(ProtocolResponsesWebSocket), []byte(`{"type":"response.cancel","response_id":"resp_123"}`), Options{})
	if err != nil || control.Class() != RequestKnownNoText || control.Reason() != ReasonKnownControlFrame {
		t.Fatalf("response.cancel admission=%+v err=%v", control, err)
	}

	nested, err := Classify(string(ProtocolResponsesWebSocket), []byte(`{"type":"response.create","response":{"instructions":"root","input":"current"}}`), Options{})
	if err != nil || nested.Class() != RequestAuditableText {
		t.Fatalf("nested response admission=%+v err=%v", nested, err)
	}

	for _, body := range []string{
		`{"type":"response.create","input":"flat","response":{"input":"nested"}}`,
		`{"type":"response.create","response":{"input":"nested"},"instructions":"flat"}`,
	} {
		admission, classifyErr := Classify(string(ProtocolResponsesWebSocket), []byte(body), Options{})
		if classifyErr != nil {
			t.Fatalf("mixed payload returned parse error: %v", classifyErr)
		}
		if admission.Class() != RequestUninspectable || admission.Reason() != ReasonUnknownContentShape {
			t.Fatalf("mixed payload admission=%+v body=%s", admission, body)
		}
	}
}
