package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIManagedReasoningEffortUsesSelectedModelEvidence(t *testing.T) {
	account := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"model_mapping": map[string]any{"public-alias": "actual-model"}}}
	account.SetUpstreamModelMetadataSnapshot(UpstreamModelMetadataSnapshot{Models: map[string]UpstreamModelMetadata{
		"actual-model": {ID: "actual-model", SupportedReasoningLevels: []string{"low", "high"}},
	}})
	mappings := []ReasoningEffortMapping{{From: "none", To: "low", MatchType: domain.ReasoningEffortMatchExact, Model: "actual-model"}}
	for _, body := range []string{`{"model":"public-alias","reasoning":{"effort":"none"}}`, `{"model":"public-alias","reasoning_effort":"none"}`} {
		got, err := ApplyOpenAIManagedReasoningEffortPolicy(context.Background(), account, []byte(body), "", mappings, "")
		require.NoError(t, err)
		assert.Equal(t, "public-alias", gjson.GetBytes(got, "model").String())
		effort := gjson.GetBytes(got, "reasoning.effort").String()
		if effort == "" {
			effort = gjson.GetBytes(got, "reasoning_effort").String()
		}
		assert.Equal(t, "low", effort)
		assert.Contains(t, body, "none", "original request remains immutable")
	}
}

func TestOpenAIManagedReasoningEffortNeverGuessesMappingTarget(t *testing.T) {
	for _, test := range []struct {
		name     string
		level    string
		metadata []string
		mappings []ReasoningEffortMapping
	}{
		{"none without mapping", "none", []string{"low", "high"}, nil},
		{"minimal without evidence", "minimal", nil, nil},
		{"unverified target", "none", nil, []ReasoningEffortMapping{{From: "none", To: "low"}}},
		{"unsupported target", "none", []string{"high"}, []ReasoningEffortMapping{{From: "none", To: "low"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			account := &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
			account.SetUpstreamModelMetadataSnapshot(UpstreamModelMetadataSnapshot{Models: map[string]UpstreamModelMetadata{
				"actual-model": {ID: "actual-model", SupportedReasoningLevels: test.metadata},
			}})
			body := []byte(`{"model":"actual-model","reasoning_effort":"` + test.level + `"}`)
			got, err := ApplyOpenAIManagedReasoningEffortPolicy(context.Background(), account, body, "", test.mappings, "")
			require.ErrorIs(t, err, ErrOpenAIReasoningEffortUnsupported)
			assert.Equal(t, body, got)
		})
	}
}
