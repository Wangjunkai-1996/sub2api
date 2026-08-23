package securityadmission

import (
	"strings"
	"testing"
)

func TestClassifyWithResourceExpansionAuditsLargeTextBody(t *testing.T) {
	body := []byte(`{"model":"gpt-test","messages":[{"role":"user","content":"` +
		"prefix-canary-" + strings.Repeat("x", 128) +
		"-suffix-canary" + `"}]}`)
	options := Options{Limits: Limits{BodyCapBytes: 64}}

	bounded, err := Classify(string(ProtocolOpenAIChat), body, options)
	if err != nil {
		t.Fatalf("bounded classify: %v", err)
	}
	if bounded.Class() != RequestUninspectable || bounded.Reason() != ReasonLargeBody {
		t.Fatalf("bounded admission=%+v", bounded)
	}

	expanded, err := ClassifyWithResourceExpansion(string(ProtocolOpenAIChat), body, options)
	if err != nil {
		t.Fatalf("expanded classify: %v", err)
	}
	if expanded.Class() != RequestAuditableText || expanded.Requirement() != AccountRequirementAny {
		t.Fatalf("expanded admission=%+v", expanded)
	}
	text, err := expanded.MaterializeText(body)
	if err != nil {
		t.Fatalf("materialize expanded text: %v", err)
	}
	if !strings.Contains(text, "prefix-canary-") || !strings.Contains(text, "-suffix-canary") {
		t.Fatalf("expanded text lost content: prefix=%t suffix=%t", strings.Contains(text, "prefix-canary-"), strings.Contains(text, "-suffix-canary"))
	}
}

func TestClassifyWithResourceExpansionKeepsOverBudgetTextEligibleForPro(t *testing.T) {
	body := []byte(`{"model":"gpt-test","messages":[{"role":"user","content":"` +
		"over-budget-prefix-" + strings.Repeat("x", MaxAuditableTextRunes+100) +
		"-over-budget-suffix" + `"}]}`)

	bounded, err := Classify(string(ProtocolOpenAIChat), body, Options{})
	if err != nil || bounded.Class() != RequestUninspectable || bounded.Reason() != ReasonTextLimit {
		t.Fatalf("bounded admission=%+v err=%v", bounded, err)
	}
	expanded, err := ClassifyWithResourceExpansion(string(ProtocolOpenAIChat), body, Options{})
	if err != nil {
		t.Fatalf("expanded classify: %v", err)
	}
	if expanded.Class() != RequestAuditableText || expanded.Requirement() != AccountRequirementAny {
		t.Fatalf("expanded admission=%+v", expanded)
	}
	if expanded.TextRunes() <= MaxAuditableTextRunes {
		t.Fatalf("expanded text runes=%d want > %d", expanded.TextRunes(), MaxAuditableTextRunes)
	}
	text, err := expanded.MaterializeText(body)
	if err != nil || !strings.Contains(text, "-over-budget-suffix") {
		t.Fatalf("expanded text=%q err=%v", text, err)
	}
}

func TestClassifyWithResourceExpansionAuditsTextLimitBody(t *testing.T) {
	body := []byte(`{"model":"gpt-test","messages":[{"role":"user","content":"` +
		"text-limit-prefix-" + strings.Repeat("x", DefaultMaxTextRunes+100) +
		"-text-limit-suffix" + `"}]}`)

	bounded, err := Classify(string(ProtocolOpenAIChat), body, Options{})
	if err != nil || bounded.Class() != RequestUninspectable || bounded.Reason() != ReasonTextLimit {
		t.Fatalf("bounded admission=%+v err=%v", bounded, err)
	}
	expanded, err := ClassifyWithResourceExpansion(string(ProtocolOpenAIChat), body, Options{})
	if err != nil || expanded.Class() != RequestAuditableText || expanded.Requirement() != AccountRequirementAny {
		t.Fatalf("expanded admission=%+v err=%v", expanded, err)
	}
	text, err := expanded.MaterializeText(body)
	if err != nil || !strings.Contains(text, "-text-limit-suffix") {
		t.Fatalf("expanded text=%q err=%v", text, err)
	}
}

func TestClassifyWithResourceExpansionKeepsOpaqueReasonsFailClosed(t *testing.T) {
	largeMedia := []byte(`{"input":[{"type":"input_image","image_url":"https://example.invalid/a"}],"instructions":"` +
		strings.Repeat("x", DefaultBodyCapBytes) + `"}`)
	admission, err := ClassifyWithResourceExpansion(string(ProtocolOpenAIResponses), largeMedia, Options{})
	if err != nil {
		t.Fatalf("large media classify: %v", err)
	}
	if admission.Class() != RequestUninspectable || admission.Reason() != ReasonMediaContent ||
		admission.Requirement() != AccountRequirementAuditExempt {
		t.Fatalf("large media admission=%+v", admission)
	}

	textLimitWithUnknown := []byte(`{"model":"gpt-test","messages":[{"role":"user","content":"` +
		strings.Repeat("x", DefaultMaxTextRunes+100) + `"}],"future_field":"x"}`)
	admission, err = ClassifyWithResourceExpansion(string(ProtocolOpenAIChat), textLimitWithUnknown, Options{})
	if err != nil {
		t.Fatalf("unknown-field classify: %v", err)
	}
	if admission.Class() != RequestUninspectable || admission.Reason() != ReasonUnknownField ||
		admission.Requirement() != AccountRequirementAuditExempt {
		t.Fatalf("unknown-field admission=%+v", admission)
	}
}

func TestClassifyWithResourceExpansionMalformedLargeBodyRemainsFailClosed(t *testing.T) {
	body := append([]byte(`{"model":"gpt-test","messages":[{"role":"user","content":"`),
		[]byte(strings.Repeat("x", DefaultBodyCapBytes))...)
	body = append(body, []byte(`"}]}`+" trailing garbage")...)
	admission, err := ClassifyWithResourceExpansion(string(ProtocolOpenAIChat), body, Options{})
	if err != nil {
		t.Fatalf("malformed large classify: %v", err)
	}
	if admission.Class() != RequestUninspectable || admission.Reason() != ReasonLargeBody ||
		admission.Requirement() != AccountRequirementAuditExempt {
		t.Fatalf("malformed large admission=%+v", admission)
	}
}
