package securityadmission

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractRoutingEnvelopeReturnsAfterRequiredPrefix(t *testing.T) {
	body := []byte(`{"model":"gpt-5.1","stream":true,"input":"` + strings.Repeat("x", 8<<20))

	envelope, err := ExtractRoutingEnvelope(string(ProtocolOpenAIResponses), body)

	require.NoError(t, err)
	require.Equal(t, "gpt-5.1", envelope.Model)
	require.True(t, envelope.Stream)
	require.True(t, envelope.Opaque)
	require.Equal(t, RoutingEnvelopeWindowBytes, envelope.WindowBytes)
}

func TestExtractRoutingEnvelopeRequiresFieldsInsideWindow(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "model after window",
			body: `{"padding":"` + strings.Repeat("x", RoutingEnvelopeWindowBytes) + `","model":"gpt-5.1","stream":true}`,
		},
		{
			name: "truncated model string",
			body: `{"model":"` + strings.Repeat("x", RoutingEnvelopeWindowBytes) + `","stream":true}`,
		},
		{
			name: "stream omitted before payload",
			body: `{"model":"gpt-5.1","input":"` + strings.Repeat("x", RoutingEnvelopeWindowBytes) + `"}`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ExtractRoutingEnvelope(string(ProtocolOpenAIResponses), []byte(test.body))
			require.ErrorIs(t, err, ErrRoutingEnvelopeUnavailable)
		})
	}
}

func TestExtractRoutingEnvelopeTreatsTruncatedRootTokenAsUnavailable(t *testing.T) {
	tests := []struct {
		name string
		body []byte
	}{
		{
			name: "object starts after whitespace window",
			body: []byte(strings.Repeat(" ", RoutingEnvelopeWindowBytes+1) +
				`{"model":"gpt-5.1","stream":false}`),
		},
		{
			name: "root string truncated by window",
			body: []byte(`"` + strings.Repeat("x", RoutingEnvelopeWindowBytes+1) + `"`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ExtractRoutingEnvelope(string(ProtocolOpenAIChat), test.body)

			require.ErrorIs(t, err, ErrRoutingEnvelopeUnavailable)
			require.NotErrorIs(t, err, ErrRoutingEnvelopeInvalid)
		})
	}
}

func TestExtractRoutingEnvelopeRejectsInvalidTypesAndPrefixDuplicates(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "model type", body: `{"model":123,"stream":true,"input":"large"}`},
		{name: "stream type", body: `{"model":"gpt-5.1","stream":"true","input":"large"}`},
		{name: "duplicate before completion", body: `{"model":"first","model":"second","stream":true,"input":"large"}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ExtractRoutingEnvelope(string(ProtocolOpenAIResponses), []byte(test.body))
			require.ErrorIs(t, err, ErrRoutingEnvelopeInvalid)
		})
	}
}

func TestExtractRoutingEnvelopeSupportsUnicodeAndStaysOpaqueToTrailingFields(t *testing.T) {
	body := []byte(`{"model":"模型-一","stream":false,"model":"trailing-duplicate","input":`)

	envelope, err := ExtractRoutingEnvelope(string(ProtocolOpenAIChat), body)

	require.NoError(t, err)
	require.Equal(t, "模型-一", envelope.Model)
	require.False(t, envelope.Stream)
	require.True(t, envelope.Opaque, "a bounded envelope cannot prove trailing duplicates are absent")
}

func TestExtractCompleteRoutingEnvelopeScansLargeValuesAndAcceptsUniqueRoutingFields(t *testing.T) {
	payload := strings.Repeat("x", DefaultBodyCapBytes+1)
	body := []byte(`{"model":"gpt-5.1","stream":false,"payload":{"items":["` + payload +
		`",{"model":"nested-model","stream":true}]}}`)

	envelope, err := ExtractCompleteRoutingEnvelope(string(ProtocolOpenAIChat), body)

	require.NoError(t, err)
	require.Equal(t, "gpt-5.1", envelope.Model)
	require.False(t, envelope.Stream)
	require.True(t, envelope.Opaque)
}

func TestExtractCompleteRoutingEnvelopeExtractsFieldsAfterBoundedWindow(t *testing.T) {
	tests := []struct {
		name     string
		protocol Protocol
		body     string
		assert   func(*testing.T, RoutingEnvelope)
	}{
		{
			name:     "messages before routing fields",
			protocol: ProtocolAnthropicMessages,
			body: `{"messages":[{"role":"user","content":"` +
				strings.Repeat("x", RoutingEnvelopeWindowBytes+1) +
				`"}],"model":"claude-sonnet-4-5","stream":true}`,
			assert: func(t *testing.T, envelope RoutingEnvelope) {
				require.Equal(t, "claude-sonnet-4-5", envelope.Model)
				require.True(t, envelope.Stream)
			},
		},
		{
			name:     "leading whitespace before root",
			protocol: ProtocolOpenAIChat,
			body: strings.Repeat(" ", RoutingEnvelopeWindowBytes+1) +
				`{"model":"gpt-5.1","stream":false}`,
			assert: func(t *testing.T, envelope RoutingEnvelope) {
				require.Equal(t, "gpt-5.1", envelope.Model)
				require.False(t, envelope.Stream)
			},
		},
		{
			name:     "websocket continuation fields after payload",
			protocol: ProtocolResponsesWebSocket,
			body: `{"input":"` + strings.Repeat("x", RoutingEnvelopeWindowBytes+1) +
				`","type":"response.create","model":"gpt-5.1","stream":true,"previous_response_id":null}`,
			assert: func(t *testing.T, envelope RoutingEnvelope) {
				require.Equal(t, "response.create", envelope.Type)
				require.Equal(t, "gpt-5.1", envelope.Model)
				require.True(t, envelope.Stream)
				require.True(t, envelope.PreviousResponseIDPresent)
				require.True(t, envelope.PreviousResponseIDExplicit)
				require.Empty(t, envelope.PreviousResponseID)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, boundedErr := ExtractRoutingEnvelope(string(test.protocol), []byte(test.body))
			require.ErrorIs(t, boundedErr, ErrRoutingEnvelopeUnavailable)

			envelope, err := ExtractCompleteRoutingEnvelope(string(test.protocol), []byte(test.body))
			require.NoError(t, err)
			test.assert(t, envelope)
			require.True(t, envelope.Opaque)
			require.Equal(t, RoutingEnvelopeWindowBytes, envelope.WindowBytes)
		})
	}
}

func TestExtractCompleteRoutingEnvelopeDefaultsOmittedHTTPStreamToFalse(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":"` +
		strings.Repeat("x", RoutingEnvelopeWindowBytes+1) +
		`"}],"model":"claude-sonnet-4-5"}`)

	_, boundedErr := ExtractRoutingEnvelope(string(ProtocolAnthropicMessages), body)
	require.ErrorIs(t, boundedErr, ErrRoutingEnvelopeUnavailable)

	envelope, err := ExtractCompleteRoutingEnvelope(string(ProtocolAnthropicMessages), body)
	require.NoError(t, err)
	require.Equal(t, "claude-sonnet-4-5", envelope.Model)
	require.False(t, envelope.Stream)
}

func TestExtractCompleteRoutingEnvelopeReplaysProductionFailureMetadata(t *testing.T) {
	// The 2026-08-22 03:20-03:26 UTC failed candidate retained no request
	// bodies, by design. These buckets replay every observed endpoint/body-size
	// signature from its structured logs without retaining user content.
	tests := []struct {
		protocol      Protocol
		field         string
		bodyBytes     int
		observedCount int
	}{
		{ProtocolOpenAIChat, "messages", 1_080_553, 3},
		{ProtocolOpenAIChat, "messages", 1_080_588, 2},
		{ProtocolOpenAIResponses, "input", 1_054_736, 13},
		{ProtocolOpenAIResponses, "input", 1_067_809, 25},
		{ProtocolOpenAIResponses, "input", 1_080_318, 25},
		{ProtocolOpenAIResponses, "input", 1_083_823, 15},
		{ProtocolOpenAIResponses, "input", 1_087_873, 7},
		{ProtocolOpenAIResponses, "input", 1_109_158, 14},
		{ProtocolOpenAIResponses, "input", 1_113_353, 25},
		{ProtocolOpenAIResponses, "input", 1_122_511, 16},
		{ProtocolOpenAIResponses, "input", 1_195_035, 16},
		{ProtocolOpenAIResponses, "input", 1_219_549, 25},
		{ProtocolOpenAIResponses, "input", 1_282_968, 21},
		{ProtocolOpenAIResponses, "input", 1_380_452, 30},
		{ProtocolOpenAIResponses, "input", 1_398_267, 1},
		{ProtocolOpenAIResponses, "input", 1_418_890, 1},
		{ProtocolOpenAIResponses, "input", 1_421_472, 1},
		{ProtocolOpenAIResponses, "input", 1_431_098, 1},
		{ProtocolOpenAIResponses, "input", 1_438_595, 1},
		{ProtocolOpenAIResponses, "input", 1_583_870, 20},
		{ProtocolOpenAIResponses, "input", 1_843_661, 10},
		{ProtocolOpenAIResponses, "input", 2_180_976, 15},
		{ProtocolOpenAIResponses, "input", 2_487_111, 6},
		{ProtocolOpenAIResponses, "input", 2_887_113, 7},
		{ProtocolOpenAIResponses, "input", 3_092_453, 3},
		{ProtocolOpenAIResponses, "input", 3_137_110, 1},
		{ProtocolOpenAIResponses, "input", 3_515_313, 1},
		{ProtocolOpenAIResponses, "input", 4_390_371, 18},
		{ProtocolOpenAIResponses, "input", 4_668_875, 5},
		{ProtocolOpenAIResponses, "input", 5_220_367, 5},
		{ProtocolOpenAIResponses, "input", 7_429_271, 6},
		{ProtocolOpenAIResponses, "input", 7_706_076, 2},
		{ProtocolOpenAIResponses, "input", 12_976_019, 1},
		{ProtocolOpenAIResponses, "input", 16_176_951, 1},
		{ProtocolOpenAIResponses, "input", 30_080_700, 1},
		{ProtocolOpenAIResponses, "input", 41_205_801, 1},
		{ProtocolOpenAIResponses, "input", 54_628_726, 1},
	}

	totalObserved := 0
	for _, test := range tests {
		totalObserved += test.observedCount
		name := fmt.Sprintf("%s/%d-bytes/%d-observed", test.protocol, test.bodyBytes, test.observedCount)
		t.Run(name, func(t *testing.T) {
			body := productionOversizeRoutingFixture(t, test.protocol, test.bodyBytes)

			_, boundedErr := ExtractRoutingEnvelope(string(test.protocol), body)
			require.ErrorIs(t, boundedErr, ErrRoutingEnvelopeUnavailable)
			require.Contains(t, boundedErr.Error(), fmt.Sprintf("field %q", test.field))

			envelope, err := ExtractCompleteRoutingEnvelope(string(test.protocol), body)
			require.NoError(t, err)
			require.Equal(t, "gpt-5.1", envelope.Model)
			require.True(t, envelope.Stream)
			if test.protocol == ProtocolOpenAIResponses {
				require.NoError(t, ValidateCompleteResponsesJSON(body))
			}
		})
	}
	require.Equal(t, 346, totalObserved)
}

func productionOversizeRoutingFixture(t *testing.T, protocol Protocol, bodyBytes int) []byte {
	t.Helper()
	prefix := `{"input":"`
	suffix := `","model":"gpt-5.1","stream":true}`
	if protocol == ProtocolOpenAIChat {
		prefix = `{"messages":[{"role":"user","content":"`
		suffix = `"}],"model":"gpt-5.1","stream":true}`
	}
	require.GreaterOrEqual(t, bodyBytes, len(prefix)+len(suffix))
	body := make([]byte, bodyBytes)
	start := copy(body, prefix)
	end := bodyBytes - len(suffix)
	for index := start; index < end; index++ {
		body[index] = 'x'
	}
	copy(body[end:], suffix)
	return body
}

func TestExtractCompleteRoutingEnvelopeRejectsTrailingRoutingDuplicates(t *testing.T) {
	payload := strings.Repeat("x", RoutingEnvelopeWindowBytes+1)
	tests := []struct {
		name string
		tail string
	}{
		{name: "model", tail: `,"model":"gpt-5.2"}`},
		{name: "stream", tail: `,"stream":true}`},
		{name: "escaped model", tail: `,"mo\u0064el":"gpt-5.2"}`},
		{name: "escaped stream", tail: `,"str\u0065am":true}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := []byte(`{"model":"gpt-5.1","stream":false,"payload":"` + payload + `"` + test.tail)

			_, err := ExtractCompleteRoutingEnvelope(string(ProtocolOpenAIChat), body)

			require.ErrorIs(t, err, ErrRoutingEnvelopeInvalid)
		})
	}
}

func TestValidateCompleteRoutingEnvelopeRejectsCaseFoldedRoutingAliases(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "model alias after canonical", body: `{"model":"gpt-5.1","Model":"gpt-5.2","stream":false}`},
		{name: "stream alias without canonical", body: `{"model":"gpt-5.1","Stream":true}`},
		{name: "escaped stream alias", body: `{"model":"gpt-5.1","\u0053tream":true}`},
		{name: "unicode-folded stream alias", body: `{"model":"gpt-5.1","\u017ftream":true}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateCompleteRoutingEnvelope([]byte(test.body))

			require.ErrorIs(t, err, ErrRoutingEnvelopeInvalid)
		})
	}
}

func TestValidateCompleteRoutingEnvelopeAllowsNonRoutingAndNestedDuplicates(t *testing.T) {
	body := []byte(`{
		"model":"gpt-5.1",
		"stream":false,
		"type":"first",
		"type":"second",
		"previous_response_id":"first",
		"previous_response_id":"second",
		"metadata":1,
		"metadata":2,
		"payload":{"model":"nested-1","model":"nested-2","stream":false,"stream":true}
	}`)

	require.NoError(t, ValidateCompleteRoutingEnvelope(body))
}

func TestValidateCompleteResponsesJSONRejectsExactDuplicateKeysAtEveryDepth(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "root duplicate", body: `{"input":"first","input":"second"}`},
		{name: "nested duplicate", body: `{"input":[{"type":"message","content":"a","content":"b"}]}`},
		{name: "escaped duplicate", body: `{"input":[{"type":"message","ty\u0070e":"input_text"}]}`},
		{name: "skipped schema duplicate", body: `{"tools":[{"type":"function","parameters":{"type":"object","type":"string"}}]}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateCompleteResponsesJSON([]byte(test.body))

			require.ErrorIs(t, err, ErrRoutingEnvelopeInvalid)
		})
	}
}

func TestValidateCompleteResponsesJSONRejectsNonCanonicalPolicyKeys(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "root model", body: `{"Model":"gpt-5.1","stream":false}`},
		{name: "root stream", body: `{"model":"gpt-5.1","Stream":true}`},
		{name: "root previous response", body: `{"model":"gpt-5.1","Previous_Response_Id":"resp_1"}`},
		{name: "root reasoning", body: `{"model":"gpt-5.1","input":"x","Reasoning":{"effort":"max"}}`},
		{name: "reasoning effort", body: `{"model":"gpt-5.1","input":"x","reasoning":{"Effort":"max"}}`},
		{name: "root input", body: `{"model":"gpt-5.1","Input":"x"}`},
		{name: "input item type", body: `{"model":"gpt-5.1","input":[{"Type":"function_call_output","call_id":"call_1","output":"x"}]}`},
		{name: "input item content", body: `{"model":"gpt-5.1","input":[{"type":"message","Content":"x"}]}`},
		{name: "input item call id", body: `{"model":"gpt-5.1","input":[{"type":"function_call_output","Call_Id":"call_1","output":"x"}]}`},
		{name: "root tools", body: `{"model":"gpt-5.1","input":"x","Tools":[{"type":"image_generation"}]}`},
		{name: "tool type", body: `{"model":"gpt-5.1","input":"x","tools":[{"Type":"image_generation"}]}`},
		{name: "nested tool type", body: `{"model":"gpt-5.1","input":"x","tools":[{"type":"namespace","tools":[{"Type":"function","name":"lookup"}]}]}`},
		{name: "root tool choice", body: `{"model":"gpt-5.1","input":"x","Tool_Choice":{"type":"image_generation"}}`},
		{name: "tool choice type", body: `{"model":"gpt-5.1","input":"x","tool_choice":{"Type":"image_generation"}}`},
		{name: "escaped policy key", body: `{"model":"gpt-5.1","input":"x","reasoning":{"\u0045ffort":"max"}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateCompleteResponsesJSON([]byte(test.body))

			require.ErrorIs(t, err, ErrRoutingEnvelopeInvalid)
		})
	}
}

func TestValidateCompleteResponsesJSONAllowsCaseSensitiveOpaqueKeys(t *testing.T) {
	body := []byte(`{
		"model":"gpt-5.1",
		"input":"x",
		"metadata":{"stream":false,"\u017ftream":true,"key":1,"\u212Aey":2,"\u03A3":3,"\u03C2":4},
		"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"id":{"type":"string"},"ID":{"type":"string"}}}}]
	}`)

	require.NoError(t, ValidateCompleteResponsesJSON(body))
}

func TestValidateCompleteResponsesJSONAllowsLargeToolSchemaObjectsAndKeys(t *testing.T) {
	var body strings.Builder
	body.WriteString(`{"model":"gpt-5.1","input":"x","tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{`)
	for index := 0; index <= DefaultMaxObjectMembers; index++ {
		if index > 0 {
			body.WriteByte(',')
		}
		fmt.Fprintf(&body, `"field_%d":{"type":"string"}`, index)
	}
	body.WriteString(`,"`)
	body.WriteString(strings.Repeat("k", DefaultMaxKeyBytes+1))
	body.WriteString(`":{"type":"string"}}}}]}`)

	require.NoError(t, ValidateCompleteResponsesJSON([]byte(body.String())))
}

func TestValidateCompleteResponsesJSONSiblingObjectFloodHasBoundedAllocations(t *testing.T) {
	const objectCount = 10_000
	keyCounts := [...]int{2, 3, completeJSONInlineObjectKeys + 1, completeJSONMinKeySlotCapacity + 1}
	var body strings.Builder
	body.Grow(objectCount*64 + 64)
	body.WriteString(`{"model":"gpt-5.1","metadata":[`)
	for index := 0; index < objectCount; index++ {
		if index > 0 {
			body.WriteByte(',')
		}
		body.WriteByte('{')
		for keyIndex := 0; keyIndex < keyCounts[index%len(keyCounts)]; keyIndex++ {
			if keyIndex > 0 {
				body.WriteByte(',')
			}
			fmt.Fprintf(&body, `"k%d":0`, keyIndex)
		}
		body.WriteByte('}')
	}
	body.WriteString(`]}`)
	payload := []byte(body.String())
	require.NoError(t, ValidateCompleteResponsesJSON(payload))

	allocations := testing.AllocsPerRun(5, func() {
		if err := ValidateCompleteResponsesJSON(payload); err != nil {
			panic(err)
		}
	})
	require.Less(t, allocations, float64(50), "2/3/5/9-key sibling objects must reuse exact-key storage")
}

func TestCompleteResponsesJSONKeyTrackingLimitIsNotMalformedJSON(t *testing.T) {
	body := []byte(`{"model":"gpt-5.1","a":0,"b":0,"c":0,"d":0,"e":0,"f":0,"g":0,"h":0}`)
	limits := CurrentLimits()
	limits.MaxTokens = int(^uint(0) >> 1)
	p := jsonParser{body: body, limits: limits}
	err := skipCompleteJSONValueWithOptions(&p, 0, completeRoutingEnvelopeMaxDepth, completeJSONScanOptions{
		rejectDuplicateKeys: true,
		policyScope:         completeJSONPolicyScopeRoot,
		maxTrackedKeySlots:  completeJSONMinKeySlotCapacity,
	})

	require.ErrorIs(t, err, ErrRoutingEnvelopeResourceLimit)
	require.NotErrorIs(t, err, ErrRoutingEnvelopeInvalid)
}

func TestValidateCompleteResponsesJSONRejectsNonCanonicalRootRoutingKeys(t *testing.T) {
	for _, body := range []string{
		`{"model":"gpt-5.1","\u0054ype":"response.create"}`,
	} {
		t.Run(body, func(t *testing.T) {
			err := ValidateCompleteResponsesJSON([]byte(body))

			require.ErrorIs(t, err, ErrRoutingEnvelopeInvalid)
		})
	}
}

func TestValidateCompleteResponsesJSONKeepsSiblingKeyNamespacesIndependent(t *testing.T) {
	body := []byte(`{
		"model":"gpt-5.1",
		"stream":false,
		"input":[
			{"type":"message","content":[{"type":"input_text","text":"first"}]},
			{"type":"message","content":[{"type":"input_text","text":"second"}]}
		],
		"metadata":{"CamelCaseProperty":true}
	}`)

	require.NoError(t, ValidateCompleteResponsesJSON(body))
}

func TestValidateCompleteResponsesJSONRejectsMalformedCompleteRoot(t *testing.T) {
	for _, body := range []string{
		`[]`,
		`{"input":[1,2}`,
		`{"input":"x"} trailing`,
	} {
		t.Run(body, func(t *testing.T) {
			err := ValidateCompleteResponsesJSON([]byte(body))

			require.ErrorIs(t, err, ErrRoutingEnvelopeInvalid)
		})
	}
}

func TestValidateCompleteRoutingEnvelopeMatchesJSONSyntaxForNestedValues(t *testing.T) {
	values := []string{
		`null`, `true`, `-12.3e+4`, `"escaped\nvalue"`, `{}`, `[]`,
		`{"a":[1,{"b":[]}],"c":2}`, `[{},[],[1,2],false]`,
		``, `{`, `[`, `{"a" 1}`, `{"a":}`, `{"a":1,}`, `[1,]`, `[1 2]`,
		`{"a":1 "b":2}`, `{"a":[1,2}`, `"unterminated`, `01`, `truex`,
	}

	for _, value := range values {
		body := []byte(`{"model":"gpt-5.1","stream":false,"payload":` + value + `}`)
		err := ValidateCompleteRoutingEnvelope(body)

		require.Equal(t, json.Valid(body), err == nil, "value: %s; error: %v", value, err)
	}
}

func TestValidateCompleteRoutingEnvelopeDoesNotUseAdmissionInspectionDepth(t *testing.T) {
	depth := DefaultMaxDepth + 1
	nested := strings.Repeat("[", depth) + "0" + strings.Repeat("]", depth)
	body := []byte(`{"model":"gpt-5.1","stream":false,"payload":` + nested + `}`)

	require.NoError(t, ValidateCompleteRoutingEnvelope(body))
}

func TestValidateCompleteRoutingEnvelopeMatchesDownstreamJSONDepthBoundary(t *testing.T) {
	bodyAtLimit := []byte(`{"model":"gpt-5.1","stream":false,"payload":` +
		strings.Repeat("[", completeRoutingEnvelopeMaxDepth-1) + "0" +
		strings.Repeat("]", completeRoutingEnvelopeMaxDepth-1) + `}`)
	require.NoError(t, ValidateCompleteRoutingEnvelope(bodyAtLimit))

	bodyPastLimit := []byte(`{"model":"gpt-5.1","stream":false,"payload":` +
		strings.Repeat("[", completeRoutingEnvelopeMaxDepth) + "0" +
		strings.Repeat("]", completeRoutingEnvelopeMaxDepth) + `}`)
	err := ValidateCompleteRoutingEnvelope(bodyPastLimit)
	require.ErrorIs(t, err, ErrRoutingEnvelopeInvalid)
	require.NotErrorIs(t, err, ErrRoutingEnvelopeUnavailable)
}

func TestExtractCompleteRoutingEnvelopeRejectsMalformedCompleteRoot(t *testing.T) {
	payload := strings.Repeat("x", RoutingEnvelopeWindowBytes+1)
	tests := []struct {
		name string
		body string
	}{
		{
			name: "truncated nested value",
			body: `{"model":"gpt-5.1","stream":false,"payload":["` + payload + `"]`,
		},
		{
			name: "trailing data",
			body: `{"model":"gpt-5.1","stream":false,"payload":"` + payload + `"} trailing`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ExtractCompleteRoutingEnvelope(string(ProtocolOpenAIChat), []byte(test.body))

			require.ErrorIs(t, err, ErrRoutingEnvelopeInvalid)
		})
	}
}

func TestExtractCompleteRoutingEnvelopeNoLongerRequiresFieldsInsideBoundedWindow(t *testing.T) {
	body := []byte(`{"padding":"` + strings.Repeat("x", RoutingEnvelopeWindowBytes) +
		`","model":"gpt-5.1","stream":false}`)

	envelope, err := ExtractCompleteRoutingEnvelope(string(ProtocolOpenAIChat), body)

	require.NoError(t, err)
	require.Equal(t, "gpt-5.1", envelope.Model)
	require.False(t, envelope.Stream)
}

func TestExtractRoutingEnvelopeWebSocketRequiresExplicitContinuationSemantics(t *testing.T) {
	valid := []byte(`{"type":"response.create","model":"gpt-5.1","stream":true,"previous_response_id":null,"input":"` + strings.Repeat("x", 1<<20))
	envelope, err := ExtractRoutingEnvelope(string(ProtocolResponsesWebSocket), valid)
	require.NoError(t, err)
	require.True(t, envelope.PreviousResponseIDPresent)
	require.True(t, envelope.PreviousResponseIDExplicit)
	require.Empty(t, envelope.PreviousResponseID)

	missingContinuation := []byte(`{"type":"response.create","model":"gpt-5.1","stream":true,"input":"` + strings.Repeat("x", RoutingEnvelopeWindowBytes))
	_, err = ExtractRoutingEnvelope(string(ProtocolResponsesWebSocket), missingContinuation)
	require.True(t, errors.Is(err, ErrRoutingEnvelopeUnavailable))
}

func TestExtractBoundedWebSocketFrameType(t *testing.T) {
	eventType, ok := ExtractBoundedWebSocketFrameType([]byte(`{"metadata":{},"type":"response.create","input":"large"}`))
	require.True(t, ok)
	require.Equal(t, "response.create", eventType)

	lateType := []byte(`{"padding":"` + strings.Repeat("x", RoutingEnvelopeWindowBytes) + `","type":"response.create"}`)
	eventType, ok = ExtractBoundedWebSocketFrameType(lateType)
	require.False(t, ok)
	require.Empty(t, eventType)

	_, ok = ExtractBoundedWebSocketFrameType([]byte(`{"type":123}`))
	require.False(t, ok)
	_, ok = ExtractBoundedWebSocketFrameType([]byte(`{"t\u0079pe":"response.create"}`))
	require.False(t, ok, "escaped routing keys remain ambiguous in a bounded prefix")
}
