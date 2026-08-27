package service

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/stretchr/testify/require"
)

type openAITrafficDirectorCheckerOnlyStub struct {
	releaseCalls int
	lastRelease  TrafficDirectorHealthProbeReleaseInput
}

type openAITrafficDirectorReporterErrorStub struct {
	releaseCalls int
	releases     []TrafficDirectorHealthProbeReleaseInput
}

func (s *openAITrafficDirectorReporterErrorStub) AccountHealthy(context.Context, int64, string) (bool, error) {
	return true, nil
}

func (s *openAITrafficDirectorReporterErrorStub) RecordSuccess(
	context.Context,
	TrafficDirectorHealthSuccessInput,
) (bool, error) {
	return false, errors.New("record success unavailable")
}

func (s *openAITrafficDirectorReporterErrorStub) RecordFailure(
	context.Context,
	TrafficDirectorHealthFailureInput,
) (TrafficDirectorHealthFailureResult, error) {
	return TrafficDirectorHealthFailureResult{}, errors.New("record failure unavailable")
}

func (s *openAITrafficDirectorReporterErrorStub) ReleaseProbe(
	_ context.Context,
	input TrafficDirectorHealthProbeReleaseInput,
) (bool, error) {
	s.releaseCalls++
	s.releases = append(s.releases, input)
	return true, nil
}

func (s *openAITrafficDirectorCheckerOnlyStub) AccountHealthy(context.Context, int64, string) (bool, error) {
	return true, nil
}

func (s *openAITrafficDirectorCheckerOnlyStub) Check(
	_ context.Context,
	input TrafficDirectorHealthCheckInput,
) (TrafficDirectorHealthDecision, error) {
	return TrafficDirectorHealthDecision{
		AccountID:     input.AccountID,
		Model:         input.Model,
		HealthMode:    input.HealthMode,
		State:         TrafficDirectorHealthStateHalfOpen,
		Allowed:       true,
		HalfOpenProbe: true,
		ProbeToken:    "checker-only-probe",
	}, nil
}

func (s *openAITrafficDirectorCheckerOnlyStub) ReleaseProbe(
	_ context.Context,
	input TrafficDirectorHealthProbeReleaseInput,
) (bool, error) {
	s.releaseCalls++
	s.lastRelease = input
	return true, nil
}

func TestOpenAITrafficDirectorHealthReportingKeepsHalfOpenProbeToken(t *testing.T) {
	store := &trafficDirectorHealthStoreStub{
		checkSnapshot: TrafficDirectorHealthSnapshot{
			State:         TrafficDirectorHealthStateHalfOpen,
			FailureStreak: 2,
			LastFailureAt: time.Unix(1_800_000_000, 0),
			ProbeUntil:    time.Unix(1_800_000_020, 0),
			ProbeAcquired: true,
		},
		successRestored: true,
	}
	health := NewTrafficDirectorHealthServiceWithOptions(store, TrafficDirectorHealthOptions{
		NewProbeToken: func() (string, error) { return "probe-1", nil },
	})
	svc := &OpenAIGatewayService{}
	svc.SetOpenAITrafficDirectorHealthResolver(health)
	ctx := svc.WithOpenAITrafficDirectorHealthRequestContext(context.Background())
	account := &Account{ID: 17, Platform: PlatformOpenAI}

	decision, err := svc.CheckOpenAITrafficDirectorHealth(ctx, account, "gpt-5", domain.TrafficDirectorHealthModeEnforce)
	require.NoError(t, err)
	require.True(t, decision.Allowed)
	require.True(t, decision.HalfOpenProbe)
	require.Equal(t, "probe-1", decision.ProbeToken)

	outcome, err := svc.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
		Account: account,
		Model:   "gpt-5",
		Result:  &OpenAIForwardResult{Model: "gpt-5"},
	})
	require.NoError(t, err)
	require.True(t, outcome.Success)
	require.True(t, outcome.Recorded)
	require.Equal(t, "probe-1", outcome.ProbeToken)
	require.Equal(t, 1, store.successCalls)
	require.Equal(t, "probe-1", store.lastSuccess.ProbeToken)

	// The token is consumed after one outcome and cannot be accidentally reused
	// by a later retry on the same request.
	require.NotPanics(t, func() {
		_, _ = svc.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
			Account: account,
			Model:   "gpt-5",
			Result:  &OpenAIForwardResult{Model: "gpt-5"},
		})
	})
	require.Equal(t, 1, store.successCalls)
	require.Equal(t, "probe-1", store.lastSuccess.ProbeToken)
}

func TestOpenAITrafficDirectorHealthReportingReleasesCheckerOnlyProbe(t *testing.T) {
	resolver := &openAITrafficDirectorCheckerOnlyStub{}
	svc := &OpenAIGatewayService{}
	svc.SetOpenAITrafficDirectorHealthResolver(resolver)
	ctx := svc.WithOpenAITrafficDirectorHealthRequestContext(context.Background())
	account := &Account{ID: 24, Platform: PlatformOpenAI}

	decision, err := svc.CheckOpenAITrafficDirectorHealth(ctx, account, "gpt-5", domain.TrafficDirectorHealthModeEnforce)
	require.NoError(t, err)
	require.True(t, decision.HalfOpenProbe)

	outcome, err := svc.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
		Account: account,
		Model:   "gpt-5",
		Result:  &OpenAIForwardResult{Model: "gpt-5"},
	})
	require.NoError(t, err)
	require.False(t, outcome.Recorded)
	require.Equal(t, "health_reporting_unavailable_or_off", outcome.IgnoredReason)
	require.Equal(t, "checker-only-probe", outcome.ProbeToken)
	require.Equal(t, 1, resolver.releaseCalls)
	require.Equal(t, int64(24), resolver.lastRelease.AccountID)
	require.Equal(t, "gpt-5", resolver.lastRelease.Model)
	require.Equal(t, "checker-only-probe", resolver.lastRelease.ProbeToken)

	_, ok := takeOpenAITrafficDirectorHealthAttempt(ctx, account.ID, "gpt-5")
	require.False(t, ok)
}

func TestOpenAITrafficDirectorHealthReportingReleasesProbeOnReporterError(t *testing.T) {
	resolver := &openAITrafficDirectorReporterErrorStub{}
	svc := &OpenAIGatewayService{}
	svc.SetOpenAITrafficDirectorHealthResolver(resolver)
	ctx := svc.WithOpenAITrafficDirectorHealthRequestContext(context.Background())
	account := &Account{ID: 25, Platform: PlatformOpenAI}

	storeOpenAITrafficDirectorHealthProbe(ctx, account.ID, "gpt-5", domain.TrafficDirectorHealthModeEnforce, "success-probe")
	_, err := svc.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
		Account:    account,
		Model:      "gpt-5",
		HealthMode: domain.TrafficDirectorHealthModeEnforce,
		Result:     &OpenAIForwardResult{UpstreamModel: "gpt-5"},
	})
	require.EqualError(t, err, "record success unavailable")

	storeOpenAITrafficDirectorHealthProbe(ctx, account.ID, "gpt-5", domain.TrafficDirectorHealthModeEnforce, "failure-probe")
	_, err = svc.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
		Account:          account,
		Model:            "gpt-5",
		HealthMode:       domain.TrafficDirectorHealthModeEnforce,
		Err:              errors.New("connection reset by peer"),
		AccountScoped:    true,
		AccountScopedSet: true,
	})
	require.EqualError(t, err, "record failure unavailable")
	require.Equal(t, 2, resolver.releaseCalls)
	require.Equal(t, []string{"success-probe", "failure-probe"}, []string{
		resolver.releases[0].ProbeToken,
		resolver.releases[1].ProbeToken,
	})
}

func TestOpenAITrafficDirectorCommittedAttemptKeepsProbeUntilOutcome(t *testing.T) {
	store := &trafficDirectorHealthStoreStub{
		checkSnapshot: TrafficDirectorHealthSnapshot{
			State:         TrafficDirectorHealthStateHalfOpen,
			FailureStreak: 2,
			LastFailureAt: time.Unix(1_800_000_000, 0),
			ProbeUntil:    time.Unix(1_800_000_020, 0),
			ProbeAcquired: true,
		},
		successRestored: true,
	}
	health := NewTrafficDirectorHealthServiceWithOptions(store, TrafficDirectorHealthOptions{
		NewProbeToken: func() (string, error) { return "probe-committed", nil },
	})
	svc := &OpenAIGatewayService{}
	svc.SetOpenAITrafficDirectorHealthResolver(health)
	ctx := svc.WithOpenAITrafficDirectorHealthRequestContext(context.Background())
	account := &Account{ID: 23, Platform: PlatformOpenAI}
	releaseCalls := 0

	selection := &AccountSelectionResult{
		Account:     account,
		Acquired:    true,
		ReleaseFunc: func() { releaseCalls++ },
	}
	selection.setTrafficDirectorAdmission(func(admitCtx context.Context, selected *Account) (bool, func()) {
		decision, err := svc.CheckOpenAITrafficDirectorHealth(admitCtx, selected, "gpt-5", domain.TrafficDirectorHealthModeEnforce)
		require.NoError(t, err)
		require.True(t, decision.HalfOpenProbe)
		return decision.Allowed, func() {
			_, _ = takeOpenAITrafficDirectorHealthAttempt(admitCtx, selected.ID, decision.Model)
		}
	})
	require.True(t, selection.AdmitTrafficDirector(ctx, selection.ReleaseFunc))
	selection.CommitTrafficDirectorAttempt()

	// Handlers release the concurrency slot before best-effort health reporting.
	// A committed attempt must leave the probe token for that outcome.
	selection.ReleaseFunc()
	require.Equal(t, 1, releaseCalls)
	outcome, err := svc.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
		Account: account,
		Model:   "gpt-5",
		Result:  &OpenAIForwardResult{Model: "gpt-5"},
	})
	require.NoError(t, err)
	require.Equal(t, "probe-committed", outcome.ProbeToken)
	require.Equal(t, "probe-committed", store.lastSuccess.ProbeToken)

	abandonedCleanupCalls := 0
	abandoned := &AccountSelectionResult{Account: account, ReleaseFunc: func() {}}
	abandoned.setTrafficDirectorAdmission(func(context.Context, *Account) (bool, func()) {
		return true, func() { abandonedCleanupCalls++ }
	})
	require.True(t, abandoned.AdmitTrafficDirector(ctx, abandoned.ReleaseFunc))
	abandoned.ReleaseFunc()
	require.Equal(t, 1, abandonedCleanupCalls, "an admission released before upstream start must be abandoned")

	withoutSlotCleanupCalls := 0
	withoutSlot := &AccountSelectionResult{Account: account}
	withoutSlot.setTrafficDirectorAdmission(func(context.Context, *Account) (bool, func()) {
		return true, func() { withoutSlotCleanupCalls++ }
	})
	require.True(t, withoutSlot.AdmitTrafficDirector(ctx, nil))
	withoutSlot.CommitTrafficDirectorAttempt()
	withoutSlot.ReleaseFunc()
	require.Zero(t, withoutSlotCleanupCalls, "a committed attempt without a slot still belongs to outcome reporting")
}

func TestOpenAITrafficDirectorHealthProbeTokensDoNotOverwrite(t *testing.T) {
	svc := &OpenAIGatewayService{}
	ctx := svc.WithOpenAITrafficDirectorHealthRequestContext(context.Background())

	storeOpenAITrafficDirectorHealthProbe(ctx, 21, "gpt-5", domain.TrafficDirectorHealthModeEnforce, "probe-1")
	storeOpenAITrafficDirectorHealthProbe(ctx, 21, "gpt-5", domain.TrafficDirectorHealthModeEnforce, "probe-2")
	storeOpenAITrafficDirectorHealthProbe(ctx, 21, "gpt-5-mini", domain.TrafficDirectorHealthModeEnforce, "probe-mini")

	_, ok := takeOpenAITrafficDirectorHealthAttempt(ctx, 21, "gpt-5.1")
	require.False(t, ok, "a model miss must not consume another circuit's probe")
	require.Empty(t, peekOpenAITrafficDirectorHealthAttemptMode(ctx, 21, "gpt-5.1"),
		"a model miss must not inherit another circuit's mode")

	first, ok := takeOpenAITrafficDirectorHealthAttempt(ctx, 21, "gpt-5")
	require.True(t, ok)
	require.Equal(t, "probe-1", first.probeToken)
	second, ok := takeOpenAITrafficDirectorHealthAttempt(ctx, 21, "gpt-5")
	require.True(t, ok)
	require.Equal(t, "probe-2", second.probeToken)
	_, ok = takeOpenAITrafficDirectorHealthAttempt(ctx, 21, "gpt-5")
	require.False(t, ok)
	mini, ok := takeOpenAITrafficDirectorHealthAttempt(ctx, 21, "gpt-5-mini")
	require.True(t, ok)
	require.Equal(t, "probe-mini", mini.probeToken)
}

func TestOpenAITrafficDirectorHealthReportingFailureClassificationAndScope(t *testing.T) {
	store := &trafficDirectorHealthStoreStub{
		failureSnapshot: TrafficDirectorHealthSnapshot{
			State:           TrafficDirectorHealthStateSuspect,
			FailureStreak:   1,
			LastFailureAt:   time.Unix(1_800_000_000, 0),
			MutationApplied: true,
		},
	}
	health := NewTrafficDirectorHealthService(store)
	svc := &OpenAIGatewayService{}
	svc.SetOpenAITrafficDirectorHealthResolver(health)
	ctx := svc.WithOpenAITrafficDirectorHealthRequestContext(context.Background())
	account := &Account{ID: 18, Platform: PlatformOpenAI}

	// 429 is deliberately ignored even when the selected account is known.
	ignored, err := svc.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
		Account:    account,
		Model:      "gpt-5",
		HealthMode: domain.TrafficDirectorHealthModeObserve,
		Err:        &UpstreamFailoverError{StatusCode: http.StatusTooManyRequests, ResponseBody: []byte("rate limit")},
	})
	require.NoError(t, err)
	require.False(t, ignored.Recorded)
	require.Equal(t, "rate_limit_or_overload", ignored.IgnoredReason)
	require.Zero(t, store.failureCalls)

	// A provider-scoped 5xx must not be attributed to the account.
	providerScoped, err := svc.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
		Account:    account,
		Model:      "gpt-5",
		HealthMode: domain.TrafficDirectorHealthModeObserve,
		Err: &UpstreamFailoverError{
			StatusCode: http.StatusBadGateway,
			Scope:      GatewayFailureScopeProvider,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "unscoped_http_5xx", providerScoped.IgnoredReason)
	require.Zero(t, store.failureCalls)

	accountScoped, err := svc.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
		Account:    account,
		Model:      "gpt-5",
		HealthMode: domain.TrafficDirectorHealthModeObserve,
		Err: &UpstreamFailoverError{
			StatusCode:   http.StatusBadGateway,
			Scope:        GatewayFailureScopeAccount,
			ResponseBody: []byte("upstream failed"),
		},
	})
	require.NoError(t, err)
	require.True(t, accountScoped.Recorded)
	require.Equal(t, TrafficDirectorHealthFailureKindHTTP5xx, accountScoped.Classification.Kind)
	require.Equal(t, 1, store.failureCalls)
	require.Equal(t, int64(18), store.lastFailure.AccountID)
	require.Equal(t, "gpt-5", store.lastFailure.NormalizedModel)
}

func TestOpenAITrafficDirectorHealthReportingUsesActualUpstreamModel(t *testing.T) {
	t.Run("channel and account mapping", func(t *testing.T) {
		store := &trafficDirectorHealthStoreStub{
			failureSnapshot: TrafficDirectorHealthSnapshot{
				State:           TrafficDirectorHealthStateSuspect,
				FailureStreak:   1,
				LastFailureAt:   time.Unix(1_800_000_000, 0),
				MutationApplied: true,
			},
		}
		svc := &OpenAIGatewayService{}
		svc.SetOpenAITrafficDirectorHealthResolver(NewTrafficDirectorHealthService(store))
		account := &Account{
			ID:   17,
			Type: AccountTypeOAuth,
			Credentials: map[string]any{
				"model_mapping": map[string]any{"channel-model": "account-upstream-model"},
			},
		}
		ctx := WithOpenAITrafficDirectorHealthModel(context.Background(), "channel-model")

		outcome, err := svc.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
			Account:          account,
			Model:            "channel-model",
			Err:              errors.New("connection reset by peer"),
			AccountScoped:    true,
			AccountScopedSet: true,
			HealthMode:       domain.TrafficDirectorHealthModeObserve,
		})
		require.NoError(t, err)
		require.Equal(t, "account-upstream-model", outcome.Model)
		require.Equal(t, "account-upstream-model", store.lastFailure.NormalizedModel)
	})

	t.Run("forward result is already canonical", func(t *testing.T) {
		store := &trafficDirectorHealthStoreStub{}
		svc := &OpenAIGatewayService{}
		svc.SetOpenAITrafficDirectorHealthResolver(NewTrafficDirectorHealthService(store))
		account := &Account{
			ID:   18,
			Type: AccountTypeOAuth,
			Credentials: map[string]any{
				"model_mapping": map[string]any{"actual-upstream-model": "must-not-remap"},
			},
		}

		outcome, err := svc.ReportOpenAITrafficDirectorOutcome(context.Background(), OpenAITrafficDirectorHealthOutcomeInput{
			Account:    account,
			Model:      "public-alias",
			Result:     &OpenAIForwardResult{UpstreamModel: "actual-upstream-model"},
			HealthMode: domain.TrafficDirectorHealthModeObserve,
		})
		require.NoError(t, err)
		require.Equal(t, "actual-upstream-model", outcome.Model)
		require.Equal(t, "actual-upstream-model", store.lastSuccess.NormalizedModel)
	})
}

func TestOpenAITrafficDirectorHealthReportingClassifiesWebSocketTerminals(t *testing.T) {
	tests := []struct {
		name         string
		event        string
		terminalJSON string
		wantRecorded bool
		wantKind     string
		wantIgnored  string
	}{
		{name: "unclassified failed is stream anomaly", event: "response.failed", wantRecorded: true, wantKind: TrafficDirectorHealthFailureKindStream},
		{name: "server failure", event: "response.failed", terminalJSON: `{"response":{"error":{"code":"server_error"}}}`, wantRecorded: true, wantKind: TrafficDirectorHealthFailureKindHTTP5xx},
		{name: "rate limit", event: "response.failed", terminalJSON: `{"response":{"error":{"code":"rate_limit_exceeded"}}}`, wantIgnored: "rate_limit_or_overload"},
		{name: "invalid request", event: "response.failed", terminalJSON: `{"response":{"error":{"type":"invalid_request_error"}}}`, wantIgnored: "client_http_status"},
		{name: "max output tokens incomplete", event: "response.incomplete", terminalJSON: `{"response":{"incomplete_details":{"reason":"max_output_tokens"}}}`, wantIgnored: "client_http_status"},
		{name: "cancelled", event: "response.cancelled", wantIgnored: "client_canceled"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &trafficDirectorHealthStoreStub{
				successRestored: true,
				failureSnapshot: TrafficDirectorHealthSnapshot{
					MutationApplied: true,
					State:           TrafficDirectorHealthStateSuspect,
					FailureStreak:   1,
					LastFailureAt:   time.Unix(1_800_000_000, 0),
				},
			}
			health := NewTrafficDirectorHealthService(store)
			svc := &OpenAIGatewayService{}
			svc.SetOpenAITrafficDirectorHealthResolver(health)
			ctx := svc.WithOpenAITrafficDirectorHealthRequestContext(context.Background())
			outcome, err := svc.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
				Account:    &Account{ID: 20, Platform: PlatformOpenAI},
				Model:      "gpt-5",
				HealthMode: domain.TrafficDirectorHealthModeEnforce,
				Result: &OpenAIForwardResult{
					OpenAIWSMode:               true,
					UpstreamTerminalEvent:      tt.event,
					UpstreamTerminalStatusCode: openAIWSTerminalHealthStatus([]byte(tt.terminalJSON)),
				},
			})
			require.NoError(t, err)
			require.False(t, outcome.Success)
			require.Equal(t, tt.wantRecorded, outcome.Recorded)
			if tt.wantKind != "" {
				require.Equal(t, tt.wantKind, outcome.Classification.Kind)
			}
			require.Equal(t, tt.wantIgnored, outcome.IgnoredReason)
			if tt.wantRecorded {
				require.Equal(t, 1, store.failureCalls)
			} else {
				require.Zero(t, store.failureCalls)
			}
		})
	}
}

func TestOpenAITrafficDirectorHealthReportingClassifiesImagesErrors(t *testing.T) {
	tests := []struct {
		name         string
		statusCode   int
		wantRecorded bool
		wantIgnored  string
	}{
		{name: "server failure", statusCode: http.StatusBadGateway, wantRecorded: true},
		{name: "bad request", statusCode: http.StatusBadRequest, wantIgnored: "client_http_status"},
		{name: "rate limit", statusCode: http.StatusTooManyRequests, wantIgnored: "rate_limit_or_overload"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &trafficDirectorHealthStoreStub{
				failureSnapshot: TrafficDirectorHealthSnapshot{
					MutationApplied: true,
					State:           TrafficDirectorHealthStateSuspect,
					FailureStreak:   1,
					LastFailureAt:   time.Unix(1_800_000_000, 0),
				},
			}
			svc := &OpenAIGatewayService{}
			svc.SetOpenAITrafficDirectorHealthResolver(NewTrafficDirectorHealthService(store))
			outcome, err := svc.ReportOpenAITrafficDirectorOutcome(context.Background(), OpenAITrafficDirectorHealthOutcomeInput{
				Account:    &Account{ID: 22, Platform: PlatformOpenAI},
				Model:      "gpt-image-1",
				HealthMode: domain.TrafficDirectorHealthModeEnforce,
				Err: &OpenAIImagesUpstreamError{
					StatusCode: tt.statusCode,
					ErrorType:  "upstream_error",
					Message:    "image request failed",
				},
			})
			require.NoError(t, err)
			require.Equal(t, tt.wantRecorded, outcome.Recorded)
			require.Equal(t, tt.wantIgnored, outcome.IgnoredReason)
			if tt.wantRecorded {
				require.Equal(t, TrafficDirectorHealthFailureKindHTTP5xx, outcome.Classification.Kind)
				require.Equal(t, 1, store.failureCalls)
			} else {
				require.Zero(t, store.failureCalls)
			}
		})
	}
}

func TestOpenAITrafficDirectorHealthReportingObserveAndOff(t *testing.T) {
	store := &trafficDirectorHealthStoreStub{
		failureSnapshot: TrafficDirectorHealthSnapshot{
			State:           TrafficDirectorHealthStateSuspect,
			FailureStreak:   1,
			LastFailureAt:   time.Unix(1_800_000_000, 0),
			MutationApplied: true,
		},
	}
	health := NewTrafficDirectorHealthService(store)
	svc := &OpenAIGatewayService{}
	svc.SetOpenAITrafficDirectorHealthResolver(health)
	ctx := svc.WithOpenAITrafficDirectorHealthRequestContext(context.Background())
	account := &Account{ID: 19, Platform: PlatformOpenAI}

	observed, err := svc.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
		Account:    account,
		Model:      "gpt-5",
		HealthMode: domain.TrafficDirectorHealthModeObserve,
		StatusCode: http.StatusInternalServerError,
	})
	require.NoError(t, err)
	require.True(t, observed.Recorded)
	require.Equal(t, 1, store.failureCalls)

	off, err := svc.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
		Account:    account,
		Model:      "gpt-5",
		HealthMode: domain.TrafficDirectorHealthModeOff,
		Err:        errors.New("timeout"),
	})
	require.NoError(t, err)
	require.False(t, off.Recorded)
	require.Equal(t, 1, store.failureCalls)
}
