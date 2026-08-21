package securityadmission

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestClassifyResponsesToolTextAndSystemReminder(t *testing.T) {
	body := []byte(`{"instructions":"<system-reminder>keep this text</system-reminder>","input":[{"type":"function_call","arguments":"function-args-canary"},{"type":"function_call_output","output":"tool-output-canary"},{"type":"message","role":"user","content":[{"type":"input_text","text":"current-user-canary"}]}]}`)
	admission, err := Classify(string(ProtocolOpenAIResponses), body, Options{})
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if admission.Class() != RequestAuditableText {
		t.Fatalf("class=%s reason=%s", admission.Class(), admission.Reason())
	}
	text, err := admission.MaterializeText(body)
	if err != nil {
		t.Fatalf("materialize: %v", err)
	}
	for _, want := range []string{"<system-reminder>keep this text</system-reminder>", "function-args-canary", "tool-output-canary", "current-user-canary"} {
		if !strings.Contains(text, want) {
			t.Fatalf("materialized text missing %q: %q", want, text)
		}
	}
	if got := strings.Index(text, "function-args-canary"); got < strings.Index(text, "tool-output-canary") {
		// The ordered source spans must retain the call/output order.
	} else {
		t.Fatalf("tool order changed: %q", text)
	}
}

func TestClassifyResponsesFormerAuditSeparatorRemainsAuditableText(t *testing.T) {
	const separator = "\x00SUB2API_PROMPT_AUDIT_PRIORITY_END\x00"
	for _, input := range []string{separator, "before" + separator + "after"} {
		body, err := json.Marshal(map[string]any{"input": input})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		admission, err := Classify(string(ProtocolOpenAIResponses), body, Options{})
		if err != nil {
			t.Fatalf("classify: %v", err)
		}
		if admission.Class() != RequestAuditableText {
			t.Fatalf("class=%s reason=%s", admission.Class(), admission.Reason())
		}
		text, err := admission.MaterializeText(body)
		if err != nil {
			t.Fatalf("materialize: %v", err)
		}
		if text != input {
			t.Fatalf("materialized text=%q want=%q", text, input)
		}
	}
}

func TestClassifyChatToolSuffixAndMessagesToolResult(t *testing.T) {
	chat := []byte(`{"model":"x","messages":[{"role":"user","content":"old-user"},{"role":"assistant","tool_calls":[{"type":"function","function":{"name":"f","arguments":"assistant-args"}}]},{"role":"tool","tool_call_id":"1","content":"current-tool-output"}]}`)
	admission, err := Classify(string(ProtocolOpenAIChat), chat, Options{})
	if err != nil || admission.Class() != RequestAuditableText {
		t.Fatalf("chat admission=%+v err=%v", admission, err)
	}
	text, err := admission.MaterializeText(chat)
	if err != nil || !strings.Contains(text, "current-tool-output") || !strings.Contains(text, "assistant-args") {
		t.Fatalf("chat text=%q err=%v", text, err)
	}

	messages := []byte(`{"model":"claude","system":"root-system","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"x","input":{"query":"tool-input"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"x","content":[{"type":"text","text":"tool-result-canary"}]}]}]}`)
	admission, err = Classify(string(ProtocolAnthropicMessages), messages, Options{})
	if err != nil || admission.Class() != RequestAuditableText {
		t.Fatalf("messages admission=%+v err=%v", admission, err)
	}
	text, err = admission.MaterializeText(messages)
	if err != nil || !strings.Contains(text, "tool-result-canary") || !strings.Contains(text, "tool-input") {
		t.Fatalf("messages text=%q err=%v", text, err)
	}
}

func TestClassifyFailClosedShapesAndLimits(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		want   RequestClass
		reason ReasonCode
	}{
		{name: "unknown type", body: `{"input":[{"type":"future_item","text":"x"}]}`, want: RequestUninspectable, reason: ReasonUnknownType},
		{name: "duplicate key", body: `{"input":"first","input":"second"}`, want: RequestUninspectable, reason: ReasonDuplicateJSONKey},
		{name: "remote conversation", body: `{"conversation":{"id":"remote"},"input":"x"}`, want: RequestUninspectable, reason: ReasonRemoteContent},
		{name: "encrypted", body: `{"input":[{"type":"reasoning","encrypted_content":"cipher"}]}`, want: RequestUninspectable, reason: ReasonEncryptedContent},
		{name: "media", body: `{"input":[{"type":"input_image","image_url":"https://example.invalid/a"}]}`, want: RequestUninspectable, reason: ReasonMediaContent},
		{name: "untrusted previous", body: `{"previous_response_id":"resp_1","input":"delta"}`, want: RequestUninspectable, reason: ReasonUntrustedLineage},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			admission, err := Classify(string(ProtocolOpenAIResponses), []byte(test.body), Options{})
			if test.name == "duplicate key" && err != nil {
				t.Fatalf("duplicate should classify, not parse error: %v", err)
			}
			if admission.Class() != test.want || admission.Reason() != test.reason {
				t.Fatalf("class=%s reason=%s want=%s/%s", admission.Class(), admission.Reason(), test.want, test.reason)
			}
		})
	}

	large := []byte(`{"input":"x"}`)
	large = append(large, make([]byte, DefaultBodyCapBytes)...)
	admission, err := Classify(string(ProtocolOpenAIResponses), large, Options{})
	if err != nil || admission.Class() != RequestUninspectable || admission.Reason() != ReasonLargeBody {
		t.Fatalf("oversize admission=%+v err=%v", admission, err)
	}
}

func TestClassifyKnownNoTextAndTrustedLineage(t *testing.T) {
	control, err := Classify(string(ProtocolResponsesWebSocket), []byte(`{"type":"response.cancel"}`), Options{})
	if err != nil || control.Class() != RequestKnownNoText || control.Reason() != ReasonKnownControlFrame {
		t.Fatalf("control admission=%+v err=%v", control, err)
	}
	trusted, err := Classify(string(ProtocolOpenAIResponses), []byte(`{"previous_response_id":"resp_1","input":"delta"}`), Options{Lineage: LineageTrusted})
	if err != nil || trusted.Class() != RequestAuditableText {
		t.Fatalf("trusted admission=%+v err=%v", trusted, err)
	}
	violation := trusted.WithKnownViolation(ReasonKnownViolation)
	if violation.Class() != RequestKnownViolation || violation.RequiresAuditExemptAccount() {
		t.Fatalf("violation admission=%+v", violation)
	}
}

func TestClassifyResolvesLineageDuringCanonicalPass(t *testing.T) {
	t.Run("exact previous response", func(t *testing.T) {
		calls := 0
		admission, err := Classify(
			string(ProtocolResponsesWebSocket),
			[]byte(`{"type":"response.create","previous_response_id":"resp_local","input":"delta"}`),
			Options{ResolveLineage: func(previousResponseID string) LineageTrust {
				calls++
				if previousResponseID == "resp_local" {
					return LineageTrusted
				}
				return LineageUntrusted
			}},
		)
		if err != nil || admission.Class() != RequestAuditableText || admission.Lineage() != LineageTrusted {
			t.Fatalf("admission=%+v err=%v", admission, err)
		}
		if calls != 1 {
			t.Fatalf("resolver calls=%d want=1", calls)
		}
	})

	t.Run("missing previous response invalidates proof", func(t *testing.T) {
		calls := 0
		admission, err := Classify(
			string(ProtocolResponsesWebSocket),
			[]byte(`{"type":"response.create","input":"new root"}`),
			Options{ResolveLineage: func(previousResponseID string) LineageTrust {
				calls++
				if previousResponseID != "" {
					t.Fatalf("previous_response_id=%q want empty", previousResponseID)
				}
				return LineageUntrusted
			}},
		)
		if err != nil || admission.Lineage() != LineageUntrusted {
			t.Fatalf("admission=%+v err=%v", admission, err)
		}
		if calls != 1 {
			t.Fatalf("resolver calls=%d want=1", calls)
		}
	})

	t.Run("oversize gate never scans for lineage", func(t *testing.T) {
		body := make([]byte, DefaultBodyCapBytes+1)
		calls := 0
		admission, err := Classify(string(ProtocolResponsesWebSocket), body, Options{
			ResolveLineage: func(string) LineageTrust {
				calls++
				return LineageTrusted
			},
		})
		if err != nil || admission.Reason() != ReasonLargeBody || calls != 0 {
			t.Fatalf("admission=%+v calls=%d err=%v", admission, calls, err)
		}
	})
}

func TestClassifyUnicodeAndInvalidJSON(t *testing.T) {
	admission, err := Classify(string(ProtocolOpenAIChat), []byte(`{"messages":[{"role":"user","content":"hello \u4e16\u754c"}]}`), Options{})
	if err != nil || admission.Class() != RequestAuditableText {
		t.Fatalf("unicode admission=%+v err=%v", admission, err)
	}
	text, err := admission.MaterializeText([]byte(`{"messages":[{"role":"user","content":"hello \u4e16\u754c"}]}`))
	if err != nil || !strings.Contains(text, "hello 世界") {
		t.Fatalf("unicode text=%q err=%v", text, err)
	}
	_, err = Classify(string(ProtocolOpenAIResponses), []byte(`{"input":`), Options{})
	var parseErr *ParseError
	if !errors.As(err, &parseErr) {
		t.Fatalf("invalid JSON error=%v", err)
	}
}
