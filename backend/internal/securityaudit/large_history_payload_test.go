//go:build largepayload

package securityaudit

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
	"github.com/stretchr/testify/require"
)

func TestCoordinatorStrictIgnoresLargeHistoricalOpaqueImagePayload(t *testing.T) {
	const largeImageBytes = 34 << 20
	const currentUserText = "audit only this current user turn"

	prefix := []byte(`{"store":false,"input":[{"type":"future_history_item","future_unknown_field":{"opaque":"history-only-secret"},"future_image":{"type":"input_image","image_url":"data:image/png;base64,`)
	suffix := []byte(`","future_image_field":"history-image-secret"}},{"type":"message","role":"user","content":[{"type":"input_text","text":"` + currentUserText + `"}]}]}`)
	body := make([]byte, 0, len(prefix)+largeImageBytes+len(suffix))
	body = append(body, prefix...)
	payloadStart := len(body)
	body = append(body, make([]byte, largeImageBytes)...)
	for index := payloadStart; index < len(body); index++ {
		body[index] = 'A'
	}
	body = append(body, suffix...)

	baselineBody := []byte(`{"store":false,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"` + currentUserText + `"}]}]}`)
	baseline := auditinput.ParseForTextAudit(auditinput.ProtocolOpenAIResponses, baselineBody)
	document := auditinput.ParseForTextAudit(auditinput.ProtocolOpenAIResponses, body)

	require.Greater(t, len(body), largeImageBytes)
	require.True(t, baseline.Complete, "%+v", baseline.Issues)
	require.True(t, document.Complete, "%+v", document.Issues)
	require.Empty(t, document.Issues)
	require.Equal(t, currentUserText, document.NormalizedText)
	require.Equal(t, baseline.Hash, document.Hash, "historical opaque/image values must not affect the current-turn audit hash")
	require.False(t, document.HasImages, "historical images are outside the current user turn")
	require.Empty(t, document.Media)
	require.Empty(t, document.OpaqueStates)

	serialized, err := json.Marshal(document)
	require.NoError(t, err)
	require.NotContains(t, string(serialized), "history-only-secret")
	require.NotContains(t, string(serialized), "history-image-secret")
	require.NotContains(t, string(serialized), "data:image/png;base64")

	groupID := int64(12)
	legacy := &fakeLegacyEngine{strict: true, check: func(_ context.Context, req Request) (*LegacyDecision, error) {
		require.True(t, req.Strict)
		require.NotNil(t, req.Document)
		require.True(t, req.Document.Complete, "%+v", req.Document.Issues)
		require.Equal(t, currentUserText, req.Document.NormalizedText)
		require.Equal(t, baseline.Hash, req.Document.Hash)
		require.Empty(t, req.Document.Media)
		return &LegacyDecision{Allowed: true}, nil
	}}
	prompt := &fakePromptEngine{mode: ModeOff}

	decision := NewCoordinator(legacy, prompt).Check(context.Background(), Request{
		APIKeyID: 7,
		GroupID:  &groupID,
		Protocol: auditinput.ProtocolOpenAIResponses,
		Body:     body,
	})

	require.True(t, decision.AllowNextStage)
	require.Equal(t, DecisionAllow, decision.Kind)
	require.Equal(t, http.StatusOK, decision.HTTPStatus)
	require.NotEqual(t, http.StatusUnprocessableEntity, decision.HTTPStatus)
	require.Equal(t, int64(1), legacy.calls.Load())
	require.Zero(t, prompt.evaluates.Load())
}
