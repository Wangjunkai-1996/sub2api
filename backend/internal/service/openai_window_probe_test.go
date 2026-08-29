package service

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	openaiPkg "github.com/Wei-Shaw/sub2api/internal/pkg/openai"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type openAIWindowProbeExecutorStub struct {
	request OpenAIOutboundRequest
	result  *OpenAIOutboundResult
	err     error
}

func (s *openAIWindowProbeExecutorStub) Execute(_ context.Context, request OpenAIOutboundRequest) (*OpenAIOutboundResult, error) {
	s.request = request
	return s.result, s.err
}

func openAIWarmupProbeAccount() *Account {
	return &Account{
		ID:          42,
		Platform:    PlatformOpenAI,
		Type:        AccountTypeOAuth,
		Status:      StatusActive,
		Schedulable: true,
		Concurrency: 2,
		Credentials: map[string]any{
			"access_token":       "secret-access-token",
			"refresh_token":      "secret-refresh-token",
			"chatgpt_account_id": "chatgpt-account",
		},
	}
}

func TestBuildOpenAIWindowWarmupPayloadIsFixedAndMinimal(t *testing.T) {
	payload := BuildOpenAIWindowWarmupPayload("gpt-5.4")
	require.True(t, gjson.ValidBytes(payload))
	require.Equal(t, "gpt-5.4", gjson.GetBytes(payload, "model").String())
	require.Equal(t, openAIWindowWarmupInstructions, gjson.GetBytes(payload, "instructions").String())
	require.Equal(t, "ping", gjson.GetBytes(payload, "input.0.content.0.text").String())
	require.True(t, gjson.GetBytes(payload, "stream").Bool())
	require.True(t, gjson.GetBytes(payload, "store").Exists())
	require.False(t, gjson.GetBytes(payload, "store").Bool())

	for _, forbidden := range []string{
		"tools", "tool_choice", "previous_response_id",
		"metadata", "prompt_cache_key", "max_output_tokens",
	} {
		require.Falsef(t, gjson.GetBytes(payload, forbidden).Exists(), "unexpected payload field %s", forbidden)
	}
	require.NotContains(t, string(payload), "secret")
}

func TestOpenAICodexWindowProbeBuildsMetadataOnlyRequest(t *testing.T) {
	reset := time.Now().UTC().Add(4 * time.Hour).Truncate(time.Second)
	executor := &openAIWindowProbeExecutorStub{result: &OpenAIOutboundResult{
		StatusCode:   http.StatusOK,
		Headers:      http.Header{"X-Codex-Secondary-Window-Minutes": {"300"}},
		Terminal:     true,
		TerminalType: "response.completed",
		ResetAt:      &reset,
		Started:      true,
	}}
	probe := NewOpenAICodexWindowProbe(executor, "gpt-5.4")
	account := openAIWarmupProbeAccount()

	result, err := probe.Probe(context.Background(), account, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, openAIWindowWarmupOutcomeCompleted, result.Outcome)
	require.Equal(t, reset, *result.ResetAt)

	request := executor.request
	require.Same(t, account, request.Account)
	require.Equal(t, "gpt-5.4", request.Model)
	require.Equal(t, chatgptCodexURL, request.Endpoint)
	require.Equal(t, openAIWindowWarmupDefaultTimeout, request.Timeout)
	require.Equal(t, "chatgpt-account", request.Headers.Get("chatgpt-account-id"))
	require.Equal(t, "text/event-stream", request.Headers.Get("Accept"))
	require.Equal(t, "responses=experimental", request.Headers.Get("OpenAI-Beta"))
	require.NotEmpty(t, request.Headers.Get("X-Codex-Window-ID"))
	require.NotEmpty(t, request.Headers.Get("Originator"))
	require.NotEmpty(t, request.Headers.Get("Version"))
	require.Empty(t, request.Headers.Get("Authorization"), "the probe must not copy credentials into the request port")
	require.NotContains(t, string(request.Payload), account.GetOpenAIAccessToken())
	require.NotContains(t, string(request.Payload), account.GetOpenAIRefreshToken())
}

func TestOpenAICodexWindowProbeDefaultsToControlledNonSparkModel(t *testing.T) {
	probe := NewOpenAICodexWindowProbe(&openAIWindowProbeExecutorStub{})
	require.Equal(t, openaiPkg.CodexUsageProbeModel, probe.Model())
	require.NotEqual(t, "gpt-5.3-codex-spark", probe.Model())
}

func TestNormalizeOpenAIWindowWarmupProbeModelFailsClosed(t *testing.T) {
	for _, model := range []string{"codex-auto-review", "gpt-5.4", "gpt-5.6-sol"} {
		normalized, err := NormalizeOpenAIWindowWarmupProbeModel("  " + model + "  ")
		require.NoError(t, err)
		require.Equal(t, model, normalized)
	}
	for _, model := range []string{"", "gpt-5.3-codex-spark", "gpt-image-2", "claude-opus-4-6", strings.Repeat("x", openAIWindowWarmupMaxModelBytes+1)} {
		_, err := NormalizeOpenAIWindowWarmupProbeModel(model)
		require.ErrorIs(t, err, ErrOpenAIWindowWarmupBlockedConfig)
	}
}

func TestOpenAICodexWindowProbeInvalidModelNeverCallsExecutor(t *testing.T) {
	executor := &openAIWindowProbeExecutorStub{}
	probe := NewOpenAICodexWindowProbe(executor, "gpt-5.3-codex-spark")

	result, err := probe.Probe(context.Background(), openAIWarmupProbeAccount(), nil)

	require.ErrorIs(t, err, ErrOpenAIWindowWarmupBlockedConfig)
	require.Nil(t, result)
	require.Nil(t, executor.request.Account)
}

func TestOpenAICodexWindowProbeUsesServiceConfiguredTimeout(t *testing.T) {
	reset := time.Now().UTC().Add(4 * time.Hour)
	executor := &openAIWindowProbeExecutorStub{result: &OpenAIOutboundResult{
		StatusCode: http.StatusOK, Terminal: true, TerminalType: "response.completed", ResetAt: &reset,
	}}
	probe := NewOpenAICodexWindowProbe(executor)
	NewOpenAIWindowWarmupService(nil, nil, executor, probe, nil, OpenAIWindowWarmupOptions{RequestTimeout: 75 * time.Second})

	_, err := probe.Probe(context.Background(), openAIWarmupProbeAccount(), nil)

	require.NoError(t, err)
	require.Equal(t, 75*time.Second, executor.request.Timeout)
}

func TestOpenAICodexWindowProbeRejectsIneligibleAccountsWithoutCallingExecutor(t *testing.T) {
	executor := &openAIWindowProbeExecutorStub{}
	probe := NewOpenAICodexWindowProbe(executor)
	account := openAIWarmupProbeAccount()
	parent := int64(9)
	account.ParentAccountID = &parent

	result, err := probe.Probe(context.Background(), account, nil)
	require.Error(t, err)
	require.Nil(t, result)
	require.Nil(t, executor.request.Account)
}

func TestParseOpenAIWindowWarmupResultRequiresCompletedTerminalAndAdvancedReset(t *testing.T) {
	observed := time.Now().UTC().Add(30 * time.Minute).Truncate(time.Second)
	advanced := observed.Add(4 * time.Hour)
	body := []byte("event: response.completed\n" +
		`data: {"type":"response.completed","response":{"id":"resp_probe","status":"completed"}}` + "\n\n")
	parsed := ParseOpenAIWindowWarmupResult(&OpenAIOutboundResult{
		StatusCode: http.StatusOK,
		Body:       body,
		ResetAt:    &advanced,
	}, &observed)
	require.True(t, parsed.Terminal)
	require.Equal(t, "response.completed", parsed.TerminalType)
	require.Equal(t, advanced, *parsed.ResetAt)
	require.Equal(t, openAIWindowWarmupOutcomeCompleted, parsed.Outcome)
	require.NoError(t, validateOpenAIWindowWarmupOutcome(parsed, &observed))

	stale := *parsed
	stale.ResetAt = &observed
	require.ErrorContains(t, validateOpenAIWindowWarmupOutcome(&stale, &observed), "did not advance")

	missingTerminal := *parsed
	missingTerminal.Terminal = false
	missingTerminal.TerminalType = ""
	require.ErrorContains(t, validateOpenAIWindowWarmupOutcome(&missingTerminal, &observed), "possibly_sent")
}

func TestParseOpenAIWindowWarmupResultRecognizesDoneAliasAndExplicitResetJSON(t *testing.T) {
	reset := time.Now().UTC().Add(3 * time.Hour).Truncate(time.Second)
	body := []byte(`data: {"type":"response.done","metadata":{"codex_5h_reset_at":"` + reset.Format(time.RFC3339) + `"}}` + "\n\n")
	parsed := ParseOpenAIWindowWarmupResult(&OpenAIOutboundResult{
		StatusCode: http.StatusOK,
		Body:       body,
	}, nil)
	require.True(t, parsed.Terminal)
	require.Equal(t, "response.done", parsed.TerminalType)
	require.NotNil(t, parsed.ResetAt)
	require.Equal(t, reset.Unix(), parsed.ResetAt.Unix())
}

func TestParseOpenAIWindowWarmupResultRejectsDoneWithFailedOrIncompleteStatus(t *testing.T) {
	reset := time.Now().UTC().Add(time.Hour)
	for _, test := range []struct {
		name   string
		status string
		want   string
	}{
		{name: "failed", status: "failed", want: openAIWindowWarmupOutcomeFailed},
		{name: "incomplete", status: "incomplete", want: openAIWindowWarmupOutcomeIncomplete},
		{name: "in-progress", status: "in_progress", want: openAIWindowWarmupOutcomeIncomplete},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := []byte(`data: {"type":"response.done","response":{"status":"` + test.status + `","codex_5h_reset_at":"` + reset.Format(time.RFC3339) + `"}}` + "\n\n")
			parsed := ParseOpenAIWindowWarmupResult(&OpenAIOutboundResult{
				StatusCode: http.StatusOK,
				Body:       body,
				ResetAt:    &reset,
			}, nil)
			require.NotNil(t, parsed)
			require.Equal(t, test.want, parsed.Outcome)
			wantType := "response." + test.status
			if test.status == "in_progress" {
				wantType = "response.incomplete"
			}
			require.Equal(t, wantType, parsed.TerminalType)
			require.Error(t, validateOpenAIWindowWarmupOutcome(parsed, nil))
		})
	}
}

func TestParseOpenAIWindowWarmupResultRejectsRawDoneWithNestedFailure(t *testing.T) {
	reset := time.Now().UTC().Add(time.Hour)
	for _, test := range []struct {
		name   string
		status string
		want   string
	}{
		{name: "failed", status: "failed", want: openAIWindowWarmupOutcomeFailed},
		{name: "incomplete", status: "incomplete", want: openAIWindowWarmupOutcomeIncomplete},
		{name: "in-progress", status: "in_progress", want: openAIWindowWarmupOutcomeIncomplete},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := []byte(`{"type":"response.done","response":{"status":"` + test.status + `","codex_5h_reset_at":"` + reset.Format(time.RFC3339) + `"}}`)
			parsed := ParseOpenAIWindowWarmupResult(&OpenAIOutboundResult{
				StatusCode: http.StatusOK,
				Body:       body,
			}, nil)
			require.NotNil(t, parsed)
			require.Equal(t, test.want, parsed.Outcome)
			wantType := "response." + test.status
			if test.status == "in_progress" {
				wantType = "response.incomplete"
			}
			require.Equal(t, wantType, parsed.TerminalType)
			require.Error(t, validateOpenAIWindowWarmupOutcome(parsed, nil))
		})
	}
}

func TestParseOpenAIWindowWarmupResultBodyStatusFencesPrefilledTerminalType(t *testing.T) {
	reset := time.Now().UTC().Add(time.Hour)
	for _, test := range []struct {
		name       string
		bodyStatus string
		prefilled  string
		wantType   string
		wantResult string
	}{
		{name: "body failed beats prefilled done", bodyStatus: "failed", prefilled: "response.done", wantType: "response.failed", wantResult: openAIWindowWarmupOutcomeFailed},
		{name: "body incomplete beats prefilled completed", bodyStatus: "incomplete", prefilled: "response.completed", wantType: "response.incomplete", wantResult: openAIWindowWarmupOutcomeIncomplete},
		{name: "prefilled failure beats contradictory body completed", bodyStatus: "completed", prefilled: "response.failed", wantType: "response.failed", wantResult: openAIWindowWarmupOutcomeFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := []byte(`data: {"type":"response.done","response":{"status":"` + test.bodyStatus + `","codex_5h_reset_at":"` + reset.Format(time.RFC3339) + `"}}` + "\n\n")
			parsed := ParseOpenAIWindowWarmupResult(&OpenAIOutboundResult{
				StatusCode:   http.StatusOK,
				Body:         body,
				Terminal:     true,
				TerminalType: test.prefilled,
				ResetAt:      &reset,
			}, nil)
			require.NotNil(t, parsed)
			require.Equal(t, test.wantType, parsed.TerminalType)
			require.Equal(t, test.wantResult, parsed.Outcome)
			require.Error(t, validateOpenAIWindowWarmupOutcome(parsed, nil))
		})
	}
}

func TestWarmupTerminalTypePreservesExplicitFailureOverContradictoryStatus(t *testing.T) {
	payload := []byte(`{"type":"response.failed","response":{"status":"completed"}}`)
	require.Equal(t, "response.failed", warmupTerminalTypeWithStatus(payload, "response.failed"))
}

func TestParseOpenAIWindowWarmupResultRejectsGenericAndWeeklyResetJSON(t *testing.T) {
	reset := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
	for _, body := range []string{
		`{"type":"response.done","reset_at":"` + reset.Format(time.RFC3339) + `"}`,
		`{"type":"response.done","response":{"rate_limit":{"reset_at":"` + reset.Format(time.RFC3339) + `"}}}`,
		`{"type":"response.done","metadata":{"codex_7d_reset_at":"` + reset.Format(time.RFC3339) + `"}}`,
	} {
		parsed := ParseOpenAIWindowWarmupResult(&OpenAIOutboundResult{
			StatusCode: http.StatusOK,
			Body:       []byte("data: " + body + "\n\n"),
		}, nil)
		require.True(t, parsed.Terminal)
		require.Equal(t, "response.done", parsed.TerminalType)
		require.Nil(t, parsed.ResetAt)
		require.ErrorContains(t, validateOpenAIWindowWarmupOutcome(parsed, nil), "no future reset evidence")
	}
}

func TestWarmupResetFromHeadersIgnoresZeroResetAfter(t *testing.T) {
	headers := make(http.Header)
	headers.Set("x-codex-primary-window-minutes", "10080")
	headers.Set("x-codex-primary-reset-after-seconds", "604800")
	headers.Set("x-codex-secondary-window-minutes", "300")
	headers.Set("x-codex-secondary-reset-after-seconds", "0")

	require.Nil(t, warmupResetFromHeaders(headers))
}

func TestOpenAICodexWindowProbeMarksPostSendEOFUncertain(t *testing.T) {
	executor := &openAIWindowProbeExecutorStub{
		result: &OpenAIOutboundResult{StatusCode: http.StatusOK, Started: true, EOF: true},
		err:    errors.New("unexpected EOF"),
	}
	probe := NewOpenAICodexWindowProbe(executor)
	result, err := probe.Probe(context.Background(), openAIWarmupProbeAccount(), nil)
	require.ErrorContains(t, err, "possibly_sent")
	require.NotNil(t, result)
	require.True(t, result.EOF)
	require.Equal(t, openAIWindowWarmupOutcomeUncertain, result.Outcome)
}

func TestOpenAICodexWindowProbeHonorsPluginExplicitNotSent(t *testing.T) {
	executor := &openAIWindowProbeExecutorStub{
		err: &PluginTransportError{
			Code:        "PLUGIN_CONNECT_TIMEOUT",
			Message:     "upstream timeout before connect",
			RequestSent: false,
		},
	}
	probe := NewOpenAICodexWindowProbe(executor)

	result, err := probe.Probe(context.Background(), openAIWarmupProbeAccount(), nil)

	require.Error(t, err)
	require.NotContains(t, err.Error(), "possibly_sent")
	require.NotNil(t, result)
	require.Equal(t, openAIWindowWarmupOutcomeUncertain, result.Outcome)
}

func TestOpenAICodexWindowProbeHandlesTypedNilPluginError(t *testing.T) {
	var pluginErr *PluginTransportError
	executor := &openAIWindowProbeExecutorStub{err: pluginErr}
	probe := NewOpenAICodexWindowProbe(executor)

	result, err := probe.Probe(context.Background(), openAIWarmupProbeAccount(), nil)

	require.Error(t, err)
	require.NotNil(t, result)
	require.Equal(t, openAIWindowWarmupOutcomeUncertain, result.Outcome)
	require.Contains(t, err.Error(), "possibly_sent")
}

func TestOpenAICodexWindowProbeRejectsPluginNotSentFlagAfterResponseEvidence(t *testing.T) {
	executor := &openAIWindowProbeExecutorStub{
		result: &OpenAIOutboundResult{Started: true, StatusCode: http.StatusOK},
		err: &PluginTransportError{
			Code:        "PLUGIN_BODY_ERROR",
			Message:     "stream ended",
			RequestSent: false,
		},
	}
	probe := NewOpenAICodexWindowProbe(executor)

	result, err := probe.Probe(context.Background(), openAIWarmupProbeAccount(), nil)

	require.Error(t, err)
	require.NotNil(t, result)
	require.Contains(t, err.Error(), "possibly_sent")
}

func TestOpenAICodexWindowProbeClassifiesHTTPFailures(t *testing.T) {
	tests := []struct {
		status int
		want   string
	}{
		{status: http.StatusUnauthorized, want: "needs_reauth"},
		{status: http.StatusForbidden, want: "blocked"},
		{status: http.StatusBadRequest, want: "blocked_config"},
		{status: http.StatusNotFound, want: "blocked_config"},
		{status: http.StatusTooManyRequests, want: "rate_limited"},
		{status: http.StatusServiceUnavailable, want: "upstream_status_503"},
	}
	for _, test := range tests {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			executor := &openAIWindowProbeExecutorStub{result: &OpenAIOutboundResult{StatusCode: test.status, Started: true}}
			result, err := NewOpenAICodexWindowProbe(executor).Probe(context.Background(), openAIWarmupProbeAccount(), nil)
			require.ErrorContains(t, err, test.want)
			require.Equal(t, test.status, result.StatusCode)
		})
	}
}

func TestOpenAIWindowWarmupBodyIsBounded(t *testing.T) {
	body := make([]byte, openAIWindowWarmupMaxBodyBytes+1024)
	parsed := ParseOpenAIWindowWarmupResult(&OpenAIOutboundResult{StatusCode: http.StatusOK, Body: body}, nil)
	require.Len(t, parsed.Body, openAIWindowWarmupMaxBodyBytes)
}
