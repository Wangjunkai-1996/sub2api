package securityadmission

import (
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
