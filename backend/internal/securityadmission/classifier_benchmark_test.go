package securityadmission

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func benchmarkBody(size int) []byte {
	if size < 64 {
		size = 64
	}
	prefix := []byte(`{"model":"bench","input":"canary","metadata":"`)
	suffix := []byte(`"}`)
	body := make([]byte, 0, size)
	body = append(body, prefix...)
	for len(body)+len(suffix) < size {
		body = append(body, 'a')
	}
	body = append(body, suffix...)
	return body
}

func BenchmarkClassifyCorpus(b *testing.B) {
	for _, size := range []int{
		1 << 10, 64 << 10, 256 << 10,
		DefaultBodyCapBytes - 1, DefaultBodyCapBytes, DefaultBodyCapBytes + 1,
		4 << 20, 8 << 20, 16 << 20, 32 << 20,
	} {
		body := benchmarkBody(size)
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			b.ReportMetric(float64(len(body)), "bytes/request")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				admission, err := Classify(string(ProtocolOpenAIResponses), body, Options{})
				if err != nil || admission.BodyBytes() != len(body) {
					b.Fatalf("classify size=%d admission=%+v err=%v", size, admission, err)
				}
			}
		})
	}
}

func BenchmarkClassifyOversizeGate(b *testing.B) {
	body := bytes.Repeat([]byte{'x'}, DefaultBodyCapBytes+1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		admission, err := Classify(string(ProtocolOpenAIResponses), body, Options{})
		if err != nil || admission.Reason() != ReasonLargeBody {
			b.Fatalf("admission=%+v err=%v", admission, err)
		}
	}
}

func BenchmarkClassifyShapes(b *testing.B) {
	largeSchema := `{"tools":[{"type":"function","function":{"name":"lookup","description":"` +
		strings.Repeat("schema ", 16<<10) +
		`","parameters":{"type":"object"}}}],"input":"schema-canary"}`
	tests := []struct {
		name     string
		protocol Protocol
		body     string
	}{
		{name: "normal", protocol: ProtocolOpenAIResponses, body: `{"instructions":"system","input":"user"}`},
		{name: "tool", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"function_call","arguments":{"query":"canary"}},{"type":"function_call_output","output":{"result":"ok"}}]}`},
		{name: "unknown", protocol: ProtocolOpenAIResponses, body: `{"input":[{"type":"future_item","text":"canary"}]}`},
		{name: "duplicate", protocol: ProtocolOpenAIResponses, body: `{"input":"first","input":"second"}`},
		{name: "unicode", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":"你好，世界 😀"}]}`},
		{name: "outer_model_visible", protocol: ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":"return json"}],"response_format":{"type":"json_schema","json_schema":{"name":"answer","description":"structured output","schema":{"type":"object","properties":{"value":{"type":"string"}}},"strict":true}},"prediction":{"type":"content","content":"{\"value\":\"known\"}"}}`},
		{name: "large_tool_schema", protocol: ProtocolOpenAIResponses, body: largeSchema},
	}
	for _, test := range tests {
		test := test
		b.Run(test.name, func(b *testing.B) {
			body := []byte(test.body)
			b.ReportMetric(float64(len(body)), "bytes/request")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				admission, err := Classify(string(test.protocol), body, Options{})
				if err != nil || admission.BodyBytes() != len(body) {
					b.Fatalf("admission=%+v err=%v", admission, err)
				}
			}
		})
	}
}
