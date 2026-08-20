package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
)

// OpenAITrafficDirectorHealthChecker is the optional richer health port. The
// legacy AccountHealthy method remains the compatibility fallback, while this
// port also returns a half-open probe token that must be associated with the
// request which was admitted by the probe.
type OpenAITrafficDirectorHealthChecker interface {
	Check(context.Context, TrafficDirectorHealthCheckInput) (TrafficDirectorHealthDecision, error)
}

// OpenAITrafficDirectorHealthReporter is the optional outcome port. It is
// deliberately expressed in terms of the existing health service DTOs so a
// custom implementation can be installed without depending on Redis details.
type OpenAITrafficDirectorHealthReporter interface {
	RecordSuccess(context.Context, TrafficDirectorHealthSuccessInput) (bool, error)
	RecordFailure(context.Context, TrafficDirectorHealthFailureInput) (TrafficDirectorHealthFailureResult, error)
}

type openAITrafficDirectorHealthProbeRenewer interface {
	RenewProbe(context.Context, TrafficDirectorHealthProbeRenewInput) (bool, error)
	ProbeRenewInterval() time.Duration
}

// OpenAITrafficDirectorHealthPort documents the full optional implementation
// used by TrafficDirectorHealthService. Checker and reporter are separate
// interfaces so deployments may provide only one side during a migration.
type OpenAITrafficDirectorHealthPort interface {
	OpenAITrafficDirectorHealthChecker
	OpenAITrafficDirectorHealthReporter
}

// OpenAITrafficDirectorHealthOutcomeInput is the narrow input accepted by the
// gateway service after an upstream attempt. StatusCode and ResponseBody are
// optional when Err is an UpstreamFailoverError; they are then derived from
// that error. AccountID is optional when Account is provided.
//
// AccountScopedSet distinguishes an explicit false from the zero value. This
// matters for HTTP 5xx classification: provider/request-scoped failures must
// not open an individual account circuit.
type OpenAITrafficDirectorHealthOutcomeInput struct {
	Account          *Account
	AccountID        int64
	Model            string
	Result           *OpenAIForwardResult
	Err              error
	StatusCode       int
	ResponseBody     []byte
	AccountScoped    bool
	AccountScopedSet bool
	HealthMode       string
}

// OpenAITrafficDirectorHealthOutcome is returned for telemetry and tests. A
// non-recorded outcome is not an error: ignored statuses (429/529, quota,
// client errors), health_mode=off, and a missing optional reporter are all
// intentional no-op cases.
type OpenAITrafficDirectorHealthOutcome struct {
	AccountID      int64
	Model          string
	HealthMode     string
	Success        bool
	Recorded       bool
	ProbeToken     string
	Classification TrafficDirectorHealthFailureClassification
	FailureResult  *TrafficDirectorHealthFailureResult
	IgnoredReason  string
}

type openAITrafficDirectorHealthAttemptKey struct {
	accountID int64
	model     string
}

type openAITrafficDirectorHealthAttempt struct {
	mode       string
	model      string
	probeToken string
	stopRenew  context.CancelFunc
}

// WithOpenAITrafficDirectorHealthRequestContext is an explicit convenience
// wrapper for callers that do not otherwise use the Traffic Director request
// context. Existing OpenAI handlers already install this state through
// WithOpenAITrafficDirectorRetryLoopContext.
func (s *OpenAIGatewayService) WithOpenAITrafficDirectorHealthRequestContext(ctx context.Context) context.Context {
	return s.WithOpenAITrafficDirectorRequestContext(ctx)
}

func openAITrafficDirectorHealthState(ctx context.Context) *openAITrafficDirectorRequestState {
	return openAITrafficDirectorRequestStateFromContext(ctx)
}

func storeOpenAITrafficDirectorHealthProbe(
	ctx context.Context,
	accountID int64,
	model string,
	mode string,
	token string,
	stopRenew ...context.CancelFunc,
) {
	state := openAITrafficDirectorHealthState(ctx)
	if state == nil || accountID <= 0 || strings.TrimSpace(token) == "" {
		return
	}
	model = NormalizeTrafficDirectorHealthModel(model)
	if model == "" {
		return
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.healthAttempts == nil {
		state.healthAttempts = make(map[openAITrafficDirectorHealthAttemptKey][]openAITrafficDirectorHealthAttempt)
	}
	key := openAITrafficDirectorHealthAttemptKey{accountID: accountID, model: model}
	var stop context.CancelFunc
	if len(stopRenew) > 0 {
		stop = stopRenew[0]
	}
	state.healthAttempts[key] = append(state.healthAttempts[key], openAITrafficDirectorHealthAttempt{
		mode:       strings.ToLower(strings.TrimSpace(mode)),
		model:      model,
		probeToken: strings.TrimSpace(token),
		stopRenew:  stop,
	})
}

func takeOpenAITrafficDirectorHealthAttempt(
	ctx context.Context,
	accountID int64,
	model string,
) (openAITrafficDirectorHealthAttempt, bool) {
	state := openAITrafficDirectorHealthState(ctx)
	if state == nil || accountID <= 0 {
		return openAITrafficDirectorHealthAttempt{}, false
	}
	model = NormalizeTrafficDirectorHealthModel(model)
	state.mu.Lock()
	defer state.mu.Unlock()
	if len(state.healthAttempts) == 0 {
		return openAITrafficDirectorHealthAttempt{}, false
	}
	key := openAITrafficDirectorHealthAttemptKey{accountID: accountID, model: model}
	if attempts, ok := state.healthAttempts[key]; ok && len(attempts) > 0 {
		attempt := attempts[0]
		if attempt.model == "" {
			attempt.model = key.model
		}
		if len(attempts) == 1 {
			delete(state.healthAttempts, key)
		} else {
			state.healthAttempts[key] = attempts[1:]
		}
		if attempt.stopRenew != nil {
			attempt.stopRenew()
		}
		return attempt, true
	}
	// The scheduler records the mapped/canonical model, while a handler may
	// report the public alias. There should normally be one attempt per account
	// in a request; use it as a compatibility fallback and consume it once.
	for candidate, attempts := range state.healthAttempts {
		if candidate.accountID == accountID && len(attempts) > 0 {
			attempt := attempts[0]
			if attempt.model == "" {
				attempt.model = candidate.model
			}
			if len(attempts) == 1 {
				delete(state.healthAttempts, candidate)
			} else {
				state.healthAttempts[candidate] = attempts[1:]
			}
			if attempt.stopRenew != nil {
				attempt.stopRenew()
			}
			return attempt, true
		}
	}
	return openAITrafficDirectorHealthAttempt{}, false
}

type openAITrafficDirectorHealthProbeReleaser interface {
	ReleaseProbe(context.Context, TrafficDirectorHealthProbeReleaseInput) (bool, error)
}

// abandonOpenAITrafficDirectorHealthAttempt stops local renewal and atomically
// releases Redis ownership when it still belongs to this request. Token matching
// prevents an expired request from releasing a newer process's probe.
func (s *OpenAIGatewayService) abandonOpenAITrafficDirectorHealthAttempt(
	ctx context.Context,
	accountID int64,
	model string,
) (openAITrafficDirectorHealthAttempt, bool) {
	attempt, ok := takeOpenAITrafficDirectorHealthAttempt(ctx, accountID, model)
	if !ok || attempt.probeToken == "" {
		return attempt, ok
	}
	releaser, supportsRelease := s.trafficDirectorHealthResolver().(openAITrafficDirectorHealthProbeReleaser)
	if !supportsRelease {
		return attempt, ok
	}
	if ctx == nil {
		ctx = context.Background()
	}
	releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	_, err := releaser.ReleaseProbe(releaseCtx, TrafficDirectorHealthProbeReleaseInput{
		AccountID:  accountID,
		Model:      attempt.model,
		ProbeToken: attempt.probeToken,
	})
	if err != nil {
		slog.Warn("openai.traffic_director.health_probe_release_failed",
			"account_id", accountID,
			"model", attempt.model,
			"error", err,
		)
	}
	return attempt, ok
}

func trafficDirectorHealthAttemptCoordinates(
	attempt openAITrafficDirectorHealthAttempt,
	model string,
	mode string,
) (string, string) {
	if attempt.model != "" {
		model = attempt.model
	}
	if attempt.mode != "" {
		mode = attempt.mode
	}
	return model, mode
}

func peekOpenAITrafficDirectorHealthAttemptMode(ctx context.Context, accountID int64, model string) string {
	state := openAITrafficDirectorHealthState(ctx)
	if state == nil || accountID <= 0 {
		return ""
	}
	model = NormalizeTrafficDirectorHealthModel(model)
	state.mu.Lock()
	defer state.mu.Unlock()
	if attempts, ok := state.healthAttempts[openAITrafficDirectorHealthAttemptKey{accountID: accountID, model: model}]; ok && len(attempts) > 0 {
		return attempts[0].mode
	}
	for key, attempts := range state.healthAttempts {
		if key.accountID == accountID && len(attempts) > 0 {
			return attempts[0].mode
		}
	}
	return ""
}

func peekOpenAITrafficDirectorHealthMode(
	ctx context.Context,
	accountID int64,
) string {
	state := openAITrafficDirectorHealthState(ctx)
	if state == nil {
		return ""
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	var fallback string
	for _, entry := range state.plans {
		plan := entry.plan
		if plan == nil || plan.policy.Spec == nil {
			continue
		}
		mode := strings.ToLower(strings.TrimSpace(plan.policy.Spec.HealthMode))
		if mode == "" {
			continue
		}
		if fallback == "" {
			fallback = mode
		}
		if accountID > 0 {
			if _, configured := plan.poolByAccount[accountID]; !configured {
				continue
			}
		}
		return mode
	}
	return fallback
}

func healthReporterFromResolver(resolver OpenAITrafficDirectorHealthResolver) OpenAITrafficDirectorHealthReporter {
	if resolver == nil {
		return nil
	}
	reporter, _ := resolver.(OpenAITrafficDirectorHealthReporter)
	return reporter
}

// CheckOpenAITrafficDirectorHealth exposes the richer checker to handlers and
// records a half-open probe token in the request state. When only the legacy
// AccountHealthy port is configured, it returns an equivalent decision without
// probe metadata. Missing health dependencies fail open for compatibility.
func (s *OpenAIGatewayService) CheckOpenAITrafficDirectorHealth(
	ctx context.Context,
	account *Account,
	model string,
	healthMode string,
) (TrafficDirectorHealthDecision, error) {
	return s.checkOpenAITrafficDirectorHealth(ctx, account, model, healthMode, nil)
}

func (s *OpenAIGatewayService) checkOpenAITrafficDirectorHealth(
	ctx context.Context,
	account *Account,
	model string,
	healthMode string,
	acquireProbe *bool,
) (TrafficDirectorHealthDecision, error) {
	accountID := int64(0)
	if account != nil {
		accountID = account.ID
		if mapped := strings.TrimSpace(account.GetMappedModel(model)); mapped != "" {
			model = mapped
		}
	}
	model = NormalizeTrafficDirectorHealthModel(model)
	if healthMode == "" {
		healthMode = peekOpenAITrafficDirectorHealthMode(ctx, accountID)
	}
	healthMode = strings.ToLower(strings.TrimSpace(healthMode))
	decision := TrafficDirectorHealthDecision{
		AccountID:  accountID,
		Model:      model,
		HealthMode: healthMode,
		State:      TrafficDirectorHealthStateHealthy,
		Allowed:    true,
	}
	if accountID <= 0 || model == "" || healthMode == "" {
		return decision, nil
	}
	resolver := s.trafficDirectorHealthResolver()
	if healthMode == domain.TrafficDirectorHealthModeOff {
		return decision, nil
	}
	if resolver == nil {
		decision.FailOpen = true
		recordTrafficDirectorHealthFailOpen()
		return decision, nil
	}
	if checker, ok := resolver.(OpenAITrafficDirectorHealthChecker); ok {
		checked, err := checker.Check(ctx, TrafficDirectorHealthCheckInput{
			AccountID:    accountID,
			Model:        model,
			HealthMode:   healthMode,
			AcquireProbe: acquireProbe,
		})
		if checked.AccountID == 0 {
			checked.AccountID = accountID
		}
		if checked.Model == "" {
			checked.Model = model
		}
		if checked.HealthMode == "" {
			checked.HealthMode = healthMode
		}
		if checked.HalfOpenProbe && checked.ProbeToken != "" {
			stopRenew := s.startOpenAITrafficDirectorHealthProbeRenewal(
				ctx,
				accountID,
				checked.Model,
				checked.ProbeToken,
			)
			storeOpenAITrafficDirectorHealthProbe(
				ctx,
				accountID,
				checked.Model,
				healthMode,
				checked.ProbeToken,
				stopRenew,
			)
		}
		if err != nil || checked.FailOpen {
			recordTrafficDirectorHealthFailOpen()
		}
		return checked, err
	}
	healthy, err := resolver.AccountHealthy(ctx, accountID, model)
	decision.Allowed = healthy
	decision.ShouldFilter = !healthy
	if err != nil {
		decision.Allowed = true
		decision.ShouldFilter = false
		decision.FailOpen = true
		recordTrafficDirectorHealthFailOpen()
	}
	return decision, err
}

func (s *OpenAIGatewayService) startOpenAITrafficDirectorHealthProbeRenewal(
	ctx context.Context,
	accountID int64,
	model string,
	probeToken string,
) context.CancelFunc {
	resolver := s.trafficDirectorHealthResolver()
	renewer, ok := resolver.(openAITrafficDirectorHealthProbeRenewer)
	if !ok || accountID <= 0 || model == "" || probeToken == "" {
		return nil
	}
	interval := renewer.ProbeRenewInterval()
	if interval <= 0 {
		return nil
	}
	renewCtx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				return
			case <-ticker.C:
				callCtx, callCancel := context.WithTimeout(context.WithoutCancel(renewCtx), 2*time.Second)
				renewed, err := renewer.RenewProbe(callCtx, TrafficDirectorHealthProbeRenewInput{
					AccountID:  accountID,
					Model:      model,
					ProbeToken: probeToken,
				})
				callCancel()
				if err != nil || !renewed {
					return
				}
			}
		}
	}()
	return cancel
}

// ReportOpenAITrafficDirectorOutcome records one completed upstream attempt.
// It is safe to call for every attempt, including failover attempts: the
// method is idempotent with respect to a consumed half-open token and ignores
// classifications that are not account-health evidence.
func (s *OpenAIGatewayService) ReportOpenAITrafficDirectorOutcome(
	ctx context.Context,
	input OpenAITrafficDirectorHealthOutcomeInput,
) (OpenAITrafficDirectorHealthOutcome, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Outcome reporting is best-effort and must survive a client disconnect or
	// an upstream timeout; otherwise the very failures we need for quarantine
	// disappear with the request context.
	reportCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	ctx = reportCtx
	accountID := input.AccountID
	if accountID <= 0 && input.Account != nil {
		accountID = input.Account.ID
	}
	model := strings.TrimSpace(input.Model)
	if model == "" && input.Result != nil {
		model = strings.TrimSpace(input.Result.UpstreamModel)
		if model == "" {
			model = strings.TrimSpace(input.Result.Model)
		}
	}
	if input.Account != nil {
		if mapped := strings.TrimSpace(input.Account.GetMappedModel(model)); mapped != "" {
			model = mapped
		}
	}
	model = NormalizeTrafficDirectorHealthModel(model)
	mode := strings.ToLower(strings.TrimSpace(input.HealthMode))
	if mode == "" {
		mode = peekOpenAITrafficDirectorHealthMode(ctx, accountID)
	}
	if mode == "" {
		mode = peekOpenAITrafficDirectorHealthAttemptMode(ctx, accountID, model)
	}
	outcome := OpenAITrafficDirectorHealthOutcome{
		AccountID:  accountID,
		Model:      model,
		HealthMode: mode,
		Success:    input.Err == nil && (input.StatusCode == 0 || input.StatusCode < http.StatusBadRequest),
		Classification: TrafficDirectorHealthFailureClassification{
			Kind: TrafficDirectorHealthFailureKindNotApplicable,
		},
	}
	if accountID <= 0 || model == "" || mode == "" {
		return outcome, nil
	}
	resolver := s.trafficDirectorHealthResolver()
	reporter := healthReporterFromResolver(resolver)
	if reporter == nil || mode == domain.TrafficDirectorHealthModeOff {
		outcome.IgnoredReason = "health_reporting_unavailable_or_off"
		return outcome, nil
	}

	statusCode := input.StatusCode
	responseBody := append([]byte(nil), input.ResponseBody...)
	accountScoped := input.AccountScoped
	accountScopedSet := input.AccountScopedSet
	// A WebSocket turn can terminate with response.failed/incomplete while the
	// transport itself returns nil error and HTTP 200. Prefer the structured
	// terminal status when available so request/auth/quota failures stay out of
	// the account circuit. Only an unclassified failed terminal is a stream
	// anomaly. Client cancellation says nothing about the upstream account.
	if input.Result != nil && input.Result.OpenAIWSMode && !input.Result.SucceededForScheduling() {
		event := strings.ToLower(strings.TrimSpace(input.Result.UpstreamTerminalEvent))
		if input.Result.ClientDisconnect || event == "response.cancelled" || event == "response.canceled" || errors.Is(input.Err, context.Canceled) {
			attempt, _ := s.abandonOpenAITrafficDirectorHealthAttempt(ctx, accountID, model)
			model, mode = trafficDirectorHealthAttemptCoordinates(attempt, model, mode)
			outcome.Model = model
			outcome.HealthMode = mode
			outcome.ProbeToken = attempt.probeToken
			outcome.Success = false
			outcome.IgnoredReason = "client_canceled"
			return outcome, nil
		}
		if statusCode == 0 {
			statusCode = input.Result.UpstreamTerminalStatusCode
		}
		if (event == "response.failed" || event == "response.incomplete") && input.Err == nil && statusCode == 0 {
			input.Err = fmt.Errorf("stream error: %s", event)
		}
	}
	var failoverErr *UpstreamFailoverError
	if errors.As(input.Err, &failoverErr) && failoverErr != nil {
		if statusCode == 0 {
			statusCode = failoverErr.StatusCode
		}
		if len(responseBody) == 0 {
			responseBody = append([]byte(nil), failoverErr.ResponseBody...)
		}
		if !accountScopedSet {
			accountScoped = failoverErr.Scope == "" || failoverErr.Scope == GatewayFailureScopeAccount
			accountScopedSet = true
		}
	}
	var imagesErr *OpenAIImagesUpstreamError
	if errors.As(input.Err, &imagesErr) && imagesErr != nil {
		if statusCode == 0 {
			statusCode = imagesErr.StatusCode
		}
		if len(responseBody) == 0 {
			responseBody = openAIImagesUpstreamErrorResponseBody(imagesErr)
		}
	}
	if !accountScopedSet {
		// A selected account is the default scope for ordinary transport/HTTP
		// failures. Callers can explicitly set AccountScoped=false for provider
		// or request-scoped errors.
		accountScoped = true
	}
	outcome.Success = input.Err == nil && (statusCode == 0 || statusCode < http.StatusBadRequest)
	if outcome.Success {
		attempt, _ := takeOpenAITrafficDirectorHealthAttempt(ctx, accountID, model)
		model, mode = trafficDirectorHealthAttemptCoordinates(attempt, model, mode)
		outcome.Model = model
		outcome.HealthMode = mode
		outcome.ProbeToken = attempt.probeToken
		restored, err := reporter.RecordSuccess(ctx, TrafficDirectorHealthSuccessInput{
			AccountID:  accountID,
			Model:      model,
			HealthMode: mode,
			ProbeToken: attempt.probeToken,
		})
		outcome.Recorded = err == nil
		if !restored && err == nil {
			outcome.IgnoredReason = "success_not_probe_owner"
		}
		return outcome, err
	}

	classification := ClassifyTrafficDirectorHealthFailure(TrafficDirectorHealthFailure{
		StatusCode:    statusCode,
		Err:           input.Err,
		ResponseBody:  responseBody,
		AccountScoped: accountScoped,
	})
	outcome.Classification = classification
	if !classification.Eligible {
		outcome.IgnoredReason = classification.IgnoredReason
		// A half-open probe must be consumed even when its result is ignored;
		// leaving the token around could incorrectly attach it to a later retry.
		attempt, _ := s.abandonOpenAITrafficDirectorHealthAttempt(ctx, accountID, model)
		model, mode = trafficDirectorHealthAttemptCoordinates(attempt, model, mode)
		outcome.Model = model
		outcome.HealthMode = mode
		outcome.ProbeToken = attempt.probeToken
		return outcome, nil
	}
	attempt, _ := takeOpenAITrafficDirectorHealthAttempt(ctx, accountID, model)
	model, mode = trafficDirectorHealthAttemptCoordinates(attempt, model, mode)
	outcome.Model = model
	outcome.HealthMode = mode
	outcome.ProbeToken = attempt.probeToken
	recorded, err := reporter.RecordFailure(ctx, TrafficDirectorHealthFailureInput{
		AccountID:  accountID,
		Model:      model,
		HealthMode: mode,
		ProbeToken: attempt.probeToken,
		Failure: TrafficDirectorHealthFailure{
			StatusCode:    statusCode,
			Err:           input.Err,
			ResponseBody:  responseBody,
			AccountScoped: accountScoped,
		},
	})
	outcome.Recorded = err == nil && recorded.Recorded
	outcome.FailureResult = &recorded
	return outcome, err
}

// ReportOpenAITrafficDirectorHealthOutcome is a positional convenience for
// handlers that already have the account/model/result/error tuple. It derives
// HTTP status and body from UpstreamFailoverError when present.
func (s *OpenAIGatewayService) ReportOpenAITrafficDirectorHealthOutcome(
	ctx context.Context,
	account *Account,
	model string,
	result *OpenAIForwardResult,
	err error,
	statusCode int,
	responseBody []byte,
) (OpenAITrafficDirectorHealthOutcome, error) {
	return s.ReportOpenAITrafficDirectorOutcome(ctx, OpenAITrafficDirectorHealthOutcomeInput{
		Account:      account,
		Model:        model,
		Result:       result,
		Err:          err,
		StatusCode:   statusCode,
		ResponseBody: responseBody,
	})
}

// RecordOpenAITrafficDirectorOutcome is an alias with a shorter name for
// callers that use the service as an outcome reporter.
func (s *OpenAIGatewayService) RecordOpenAITrafficDirectorOutcome(
	ctx context.Context,
	input OpenAITrafficDirectorHealthOutcomeInput,
) (OpenAITrafficDirectorHealthOutcome, error) {
	return s.ReportOpenAITrafficDirectorOutcome(ctx, input)
}
