package securityadmission

import (
	"encoding/json"
	"errors"
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
		"metadata":1,
		"metadata":2,
		"payload":{"model":"nested-1","model":"nested-2","stream":false,"stream":true}
	}`)

	require.NoError(t, ValidateCompleteRoutingEnvelope(body))
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

func TestExtractCompleteRoutingEnvelopeKeepsBoundedAvailabilityContract(t *testing.T) {
	body := []byte(`{"padding":"` + strings.Repeat("x", RoutingEnvelopeWindowBytes) +
		`","model":"gpt-5.1","stream":false}`)

	_, err := ExtractCompleteRoutingEnvelope(string(ProtocolOpenAIChat), body)

	require.ErrorIs(t, err, ErrRoutingEnvelopeUnavailable)
	require.NotErrorIs(t, err, ErrRoutingEnvelopeInvalid)
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
