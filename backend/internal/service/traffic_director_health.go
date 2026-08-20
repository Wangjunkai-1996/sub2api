package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
)

const (
	TrafficDirectorHealthStateHealthy  = "healthy"
	TrafficDirectorHealthStateSuspect  = "suspect"
	TrafficDirectorHealthStateOpen     = "open"
	TrafficDirectorHealthStateHalfOpen = "half_open"
	TrafficDirectorHealthStateUnknown  = "unknown"

	TrafficDirectorHealthFailureKindHTTP5xx       = "account_http_5xx"
	TrafficDirectorHealthFailureKindTimeout       = "timeout"
	TrafficDirectorHealthFailureKindEOF           = "eof"
	TrafficDirectorHealthFailureKindReset         = "connection_reset"
	TrafficDirectorHealthFailureKindStream        = "stream_error"
	TrafficDirectorHealthFailureKindNotApplicable = "not_applicable"

	defaultTrafficDirectorHealthFailureStreakTTL = 30 * time.Minute
	defaultTrafficDirectorHealthShortOpen        = 10 * time.Second
	defaultTrafficDirectorHealthLongOpen         = 45 * time.Second
	defaultTrafficDirectorHealthProbeLease       = 2 * time.Minute
	trafficDirectorHealthMaxModelBytes           = 512
	trafficDirectorHealthMaxProbeTokenBytes      = 256
)

var (
	ErrTrafficDirectorHealthInvalidInput = errors.New("invalid traffic director health input")
	ErrTrafficDirectorHealthUnavailable  = errors.New("traffic director health store unavailable")
)

// TrafficDirectorHealthStore owns the distributed account+model state machine.
// Every method must be atomic for one normalized account+model key.
type TrafficDirectorHealthStore interface {
	CheckTrafficDirectorHealth(
		ctx context.Context,
		request TrafficDirectorHealthStoreCheckRequest,
	) (TrafficDirectorHealthSnapshot, error)
	RecordTrafficDirectorHealthFailure(
		ctx context.Context,
		request TrafficDirectorHealthStoreFailureRequest,
	) (TrafficDirectorHealthSnapshot, error)
	RecordTrafficDirectorHealthSuccess(
		ctx context.Context,
		request TrafficDirectorHealthStoreSuccessRequest,
	) (bool, error)
	RenewTrafficDirectorHealthProbe(
		ctx context.Context,
		request TrafficDirectorHealthStoreProbeRequest,
	) (bool, error)
	ReleaseTrafficDirectorHealthProbe(
		ctx context.Context,
		request TrafficDirectorHealthStoreProbeReleaseRequest,
	) (bool, error)
}

type TrafficDirectorHealthStoreCheckRequest struct {
	AccountID        int64
	NormalizedModel  string
	Now              time.Time
	FailureStreakTTL time.Duration
	AcquireProbe     bool
	ProbeToken       string
	ProbeLease       time.Duration
}

type TrafficDirectorHealthStoreFailureRequest struct {
	AccountID        int64
	NormalizedModel  string
	Now              time.Time
	ProbeToken       string
	FailureStreakTTL time.Duration
	ShortOpen        time.Duration
	LongOpen         time.Duration
}

type TrafficDirectorHealthStoreSuccessRequest struct {
	AccountID            int64
	NormalizedModel      string
	ProbeToken           string
	AllowObserveRecovery bool
}

type TrafficDirectorHealthStoreProbeRequest struct {
	AccountID       int64
	NormalizedModel string
	Now             time.Time
	ProbeToken      string
	ProbeLease      time.Duration
}

type TrafficDirectorHealthStoreProbeReleaseRequest struct {
	AccountID       int64
	NormalizedModel string
	ProbeToken      string
}

// TrafficDirectorHealthSnapshot is the authoritative Redis state after one
// atomic operation. MutationApplied is used by record operations to reject a
// stale or non-owner probe result without overwriting the current probe.
type TrafficDirectorHealthSnapshot struct {
	State           string
	FailureStreak   int
	LastFailureAt   time.Time
	OpenUntil       time.Time
	ProbeUntil      time.Time
	ProbeAcquired   bool
	MutationApplied bool
}

type TrafficDirectorHealthOptions struct {
	Clock                   func() time.Time
	NewProbeToken           func() (string, error)
	FailureStreakTTL        time.Duration
	ShortOpenDuration       time.Duration
	HalfOpenFailureDuration time.Duration
	ProbeLeaseDuration      time.Duration
}

type TrafficDirectorHealthService struct {
	store         TrafficDirectorHealthStore
	clock         func() time.Time
	newProbeToken func() (string, error)
	streakTTL     time.Duration
	shortOpen     time.Duration
	longOpen      time.Duration
	probeLease    time.Duration
}

type TrafficDirectorHealthCheckInput struct {
	AccountID    int64
	Model        string
	HealthMode   string
	AcquireProbe *bool
}

// TrafficDirectorHealthDecision never selects another pool or account. On a
// Redis error Allowed remains true and FailOpen is set so the caller can stay
// within the already evaluated pool boundary.
type TrafficDirectorHealthDecision struct {
	AccountID     int64
	Model         string
	HealthMode    string
	State         string
	FailureStreak int
	OpenUntil     time.Time
	ProbeUntil    time.Time
	Allowed       bool
	ShouldFilter  bool
	HalfOpenProbe bool
	ProbeToken    string
	FailOpen      bool
}

type TrafficDirectorHealthFailure struct {
	StatusCode    int
	Err           error
	ResponseBody  []byte
	AccountScoped bool
}

type TrafficDirectorHealthFailureClassification struct {
	Eligible      bool
	Kind          string
	IgnoredReason string
}

type TrafficDirectorHealthFailureInput struct {
	AccountID  int64
	Model      string
	HealthMode string
	ProbeToken string
	Failure    TrafficDirectorHealthFailure
}

type TrafficDirectorHealthFailureResult struct {
	Eligible      bool
	Recorded      bool
	Kind          string
	IgnoredReason string
	State         string
	FailureStreak int
	OpenUntil     time.Time
}

type TrafficDirectorHealthSuccessInput struct {
	AccountID  int64
	Model      string
	HealthMode string
	ProbeToken string
}

type TrafficDirectorHealthProbeRenewInput struct {
	AccountID  int64
	Model      string
	ProbeToken string
}

type TrafficDirectorHealthProbeReleaseInput struct {
	AccountID  int64
	Model      string
	ProbeToken string
}

// AccountHealthy adapts the health state machine to the narrow OpenAI routing
// resolver. It is intentionally enforce-only: callers invoke it only after a
// published policy selects health_mode=enforce. Backend failures are returned
// so the caller can record fail-open telemetry while retaining pool bounds.
func (s *TrafficDirectorHealthService) AccountHealthy(
	ctx context.Context,
	accountID int64,
	normalizedModel string,
) (bool, error) {
	decision, err := s.Check(ctx, TrafficDirectorHealthCheckInput{
		AccountID:  accountID,
		Model:      normalizedModel,
		HealthMode: domain.TrafficDirectorHealthModeEnforce,
	})
	if err != nil {
		return true, err
	}
	return decision.Allowed, nil
}

func NewTrafficDirectorHealthService(store TrafficDirectorHealthStore) *TrafficDirectorHealthService {
	return NewTrafficDirectorHealthServiceWithOptions(store, TrafficDirectorHealthOptions{})
}

func NewTrafficDirectorHealthServiceWithOptions(
	store TrafficDirectorHealthStore,
	options TrafficDirectorHealthOptions,
) *TrafficDirectorHealthService {
	if options.Clock == nil {
		options.Clock = time.Now
	}
	if options.NewProbeToken == nil {
		options.NewProbeToken = newTrafficDirectorHealthProbeToken
	}
	if options.FailureStreakTTL <= 0 {
		options.FailureStreakTTL = defaultTrafficDirectorHealthFailureStreakTTL
	}
	if options.ShortOpenDuration <= 0 {
		options.ShortOpenDuration = defaultTrafficDirectorHealthShortOpen
	}
	if options.HalfOpenFailureDuration <= 0 {
		options.HalfOpenFailureDuration = defaultTrafficDirectorHealthLongOpen
	}
	if options.ProbeLeaseDuration <= 0 {
		options.ProbeLeaseDuration = defaultTrafficDirectorHealthProbeLease
	}
	return &TrafficDirectorHealthService{
		store:         store,
		clock:         options.Clock,
		newProbeToken: options.NewProbeToken,
		streakTTL:     options.FailureStreakTTL,
		shortOpen:     options.ShortOpenDuration,
		longOpen:      options.HalfOpenFailureDuration,
		probeLease:    options.ProbeLeaseDuration,
	}
}

func (s *TrafficDirectorHealthService) Check(
	ctx context.Context,
	input TrafficDirectorHealthCheckInput,
) (TrafficDirectorHealthDecision, error) {
	model, mode, err := validateTrafficDirectorHealthCoordinates(input.AccountID, input.Model, input.HealthMode)
	if err != nil {
		return TrafficDirectorHealthDecision{}, err
	}
	decision := TrafficDirectorHealthDecision{
		AccountID:  input.AccountID,
		Model:      model,
		HealthMode: mode,
		State:      TrafficDirectorHealthStateHealthy,
		Allowed:    true,
	}
	// Off and observe never consult distributed state for scheduling. Observe
	// still records outcomes through RecordFailure and RecordSuccess.
	if mode != domain.TrafficDirectorHealthModeEnforce {
		return decision, nil
	}
	if s == nil || s.store == nil {
		decision.State = TrafficDirectorHealthStateUnknown
		decision.FailOpen = true
		return decision, ErrTrafficDirectorHealthUnavailable
	}

	acquireProbe := true
	if input.AcquireProbe != nil {
		acquireProbe = *input.AcquireProbe
	}
	probeToken := ""
	if acquireProbe {
		var tokenErr error
		probeToken, tokenErr = s.newProbeToken()
		if tokenErr != nil || strings.TrimSpace(probeToken) == "" {
			decision.State = TrafficDirectorHealthStateUnknown
			decision.FailOpen = true
			return decision, fmt.Errorf("%w: create half-open probe token: %v", ErrTrafficDirectorHealthUnavailable, tokenErr)
		}
	}
	snapshot, storeErr := s.store.CheckTrafficDirectorHealth(ctx, TrafficDirectorHealthStoreCheckRequest{
		AccountID:        input.AccountID,
		NormalizedModel:  model,
		Now:              s.now(),
		FailureStreakTTL: s.streakTTL,
		AcquireProbe:     acquireProbe,
		ProbeToken:       probeToken,
		ProbeLease:       s.probeLease,
	})
	if storeErr != nil {
		decision.State = TrafficDirectorHealthStateUnknown
		decision.FailOpen = true
		return decision, fmt.Errorf("%w: check account health: %w", ErrTrafficDirectorHealthUnavailable, storeErr)
	}
	if err := validateTrafficDirectorHealthSnapshot(snapshot); err != nil {
		decision.State = TrafficDirectorHealthStateUnknown
		decision.FailOpen = true
		return decision, fmt.Errorf("%w: %w", ErrTrafficDirectorHealthUnavailable, err)
	}

	decision.State = snapshot.State
	decision.FailureStreak = snapshot.FailureStreak
	decision.OpenUntil = snapshot.OpenUntil
	decision.ProbeUntil = snapshot.ProbeUntil
	switch snapshot.State {
	case TrafficDirectorHealthStateHealthy, TrafficDirectorHealthStateSuspect:
		decision.Allowed = true
	case TrafficDirectorHealthStateOpen:
		decision.Allowed = false
	case TrafficDirectorHealthStateHalfOpen:
		decision.Allowed = snapshot.ProbeAcquired
		decision.HalfOpenProbe = snapshot.ProbeAcquired
		if snapshot.ProbeAcquired {
			decision.ProbeToken = probeToken
		}
	}
	decision.ShouldFilter = !decision.Allowed
	return decision, nil
}

func (s *TrafficDirectorHealthService) RecordFailure(
	ctx context.Context,
	input TrafficDirectorHealthFailureInput,
) (TrafficDirectorHealthFailureResult, error) {
	model, mode, err := validateTrafficDirectorHealthCoordinates(input.AccountID, input.Model, input.HealthMode)
	if err != nil {
		return TrafficDirectorHealthFailureResult{}, err
	}
	classification := ClassifyTrafficDirectorHealthFailure(input.Failure)
	result := TrafficDirectorHealthFailureResult{
		Eligible:      classification.Eligible,
		Kind:          classification.Kind,
		IgnoredReason: classification.IgnoredReason,
		State:         TrafficDirectorHealthStateHealthy,
	}
	if !classification.Eligible {
		return result, nil
	}
	if mode == domain.TrafficDirectorHealthModeOff {
		result.IgnoredReason = "health_mode_off"
		return result, nil
	}
	if s == nil || s.store == nil {
		return result, ErrTrafficDirectorHealthUnavailable
	}
	probeToken := strings.TrimSpace(input.ProbeToken)
	if len(probeToken) > trafficDirectorHealthMaxProbeTokenBytes {
		return result, fmt.Errorf("%w: probe token exceeds %d bytes", ErrTrafficDirectorHealthInvalidInput, trafficDirectorHealthMaxProbeTokenBytes)
	}

	snapshot, storeErr := s.store.RecordTrafficDirectorHealthFailure(ctx, TrafficDirectorHealthStoreFailureRequest{
		AccountID:        input.AccountID,
		NormalizedModel:  model,
		Now:              s.now(),
		ProbeToken:       probeToken,
		FailureStreakTTL: s.streakTTL,
		ShortOpen:        s.shortOpen,
		LongOpen:         s.longOpen,
	})
	if storeErr != nil {
		return result, fmt.Errorf("%w: record account health failure: %w", ErrTrafficDirectorHealthUnavailable, storeErr)
	}
	if err := validateTrafficDirectorHealthSnapshot(snapshot); err != nil {
		return result, fmt.Errorf("%w: %w", ErrTrafficDirectorHealthUnavailable, err)
	}
	result.Recorded = snapshot.MutationApplied
	result.State = snapshot.State
	result.FailureStreak = snapshot.FailureStreak
	result.OpenUntil = snapshot.OpenUntil
	if !snapshot.MutationApplied {
		result.IgnoredReason = "stale_or_non_owner_probe"
	}
	return result, nil
}

func (s *TrafficDirectorHealthService) RecordSuccess(
	ctx context.Context,
	input TrafficDirectorHealthSuccessInput,
) (bool, error) {
	model, mode, err := validateTrafficDirectorHealthCoordinates(input.AccountID, input.Model, input.HealthMode)
	if err != nil {
		return false, err
	}
	if mode == domain.TrafficDirectorHealthModeOff {
		return false, nil
	}
	if s == nil || s.store == nil {
		return false, ErrTrafficDirectorHealthUnavailable
	}
	probeToken := strings.TrimSpace(input.ProbeToken)
	if len(probeToken) > trafficDirectorHealthMaxProbeTokenBytes {
		return false, fmt.Errorf("%w: probe token exceeds %d bytes", ErrTrafficDirectorHealthInvalidInput, trafficDirectorHealthMaxProbeTokenBytes)
	}
	restored, storeErr := s.store.RecordTrafficDirectorHealthSuccess(ctx, TrafficDirectorHealthStoreSuccessRequest{
		AccountID:            input.AccountID,
		NormalizedModel:      model,
		ProbeToken:           probeToken,
		AllowObserveRecovery: mode == domain.TrafficDirectorHealthModeObserve,
	})
	if storeErr != nil {
		return false, fmt.Errorf("%w: record account health success: %w", ErrTrafficDirectorHealthUnavailable, storeErr)
	}
	return restored, nil
}

func (s *TrafficDirectorHealthService) RenewProbe(
	ctx context.Context,
	input TrafficDirectorHealthProbeRenewInput,
) (bool, error) {
	model := NormalizeTrafficDirectorHealthModel(input.Model)
	probeToken := strings.TrimSpace(input.ProbeToken)
	if input.AccountID <= 0 || model == "" || probeToken == "" || len(probeToken) > trafficDirectorHealthMaxProbeTokenBytes {
		return false, ErrTrafficDirectorHealthInvalidInput
	}
	if s == nil || s.store == nil {
		return false, ErrTrafficDirectorHealthUnavailable
	}
	renewed, err := s.store.RenewTrafficDirectorHealthProbe(ctx, TrafficDirectorHealthStoreProbeRequest{
		AccountID:       input.AccountID,
		NormalizedModel: model,
		Now:             s.now(),
		ProbeToken:      probeToken,
		ProbeLease:      s.probeLease,
	})
	if err != nil {
		return false, fmt.Errorf("%w: renew half-open probe: %w", ErrTrafficDirectorHealthUnavailable, err)
	}
	return renewed, nil
}

// ReleaseProbe abandons a half-open probe only when the caller still owns its
// token. The store returns the key to an immediately probeable open state; a
// stale owner can never release a newer process's lease.
func (s *TrafficDirectorHealthService) ReleaseProbe(
	ctx context.Context,
	input TrafficDirectorHealthProbeReleaseInput,
) (bool, error) {
	model := NormalizeTrafficDirectorHealthModel(input.Model)
	probeToken := strings.TrimSpace(input.ProbeToken)
	if input.AccountID <= 0 || model == "" || probeToken == "" || len(probeToken) > trafficDirectorHealthMaxProbeTokenBytes {
		return false, ErrTrafficDirectorHealthInvalidInput
	}
	if s == nil || s.store == nil {
		return false, ErrTrafficDirectorHealthUnavailable
	}
	released, err := s.store.ReleaseTrafficDirectorHealthProbe(ctx, TrafficDirectorHealthStoreProbeReleaseRequest{
		AccountID:       input.AccountID,
		NormalizedModel: model,
		ProbeToken:      probeToken,
	})
	if err != nil {
		return false, fmt.Errorf("%w: release half-open probe: %w", ErrTrafficDirectorHealthUnavailable, err)
	}
	return released, nil
}

// ProbeRenewInterval lets the gateway renew only a real half-open request. A
// third of the lease leaves room for one transient Redis delay without allowing
// a second process to acquire the same account+model probe.
func (s *TrafficDirectorHealthService) ProbeRenewInterval() time.Duration {
	if s == nil || s.probeLease <= 0 {
		return 30 * time.Second
	}
	interval := s.probeLease / 3
	if interval < time.Second {
		return time.Second
	}
	return interval
}

// NormalizeTrafficDirectorHealthModel expects the canonical upstream model
// after account mapping and makes its account+model cache identity stable.
func NormalizeTrafficDirectorHealthModel(model string) string {
	model = strings.ToLower(strings.TrimSpace(model))
	if model == "" || len(model) > trafficDirectorHealthMaxModelBytes {
		return ""
	}
	return model
}

func ClassifyTrafficDirectorHealthFailure(
	failure TrafficDirectorHealthFailure,
) TrafficDirectorHealthFailureClassification {
	ignored := func(reason string) TrafficDirectorHealthFailureClassification {
		return TrafficDirectorHealthFailureClassification{
			Kind:          TrafficDirectorHealthFailureKindNotApplicable,
			IgnoredReason: reason,
		}
	}
	if errors.Is(failure.Err, context.Canceled) {
		return ignored("client_canceled")
	}

	switch failure.StatusCode {
	case http.StatusUnauthorized, http.StatusForbidden:
		return ignored("authentication_or_authorization")
	case http.StatusTooManyRequests, 529:
		return ignored("rate_limit_or_overload")
	}
	message := trafficDirectorHealthFailureMessage(failure)
	if containsTrafficDirectorHealthMarker(message, trafficDirectorHealthQuotaMarkers) {
		return ignored("quota_or_rate_limit")
	}
	if containsTrafficDirectorHealthMarker(message, trafficDirectorHealthClientErrorMarkers) {
		return ignored("unsupported_or_client_error")
	}
	if failure.StatusCode >= 400 && failure.StatusCode < 500 {
		return ignored("client_http_status")
	}
	if failure.StatusCode >= 500 && failure.StatusCode <= 599 {
		if !failure.AccountScoped {
			return ignored("unscoped_http_5xx")
		}
		return TrafficDirectorHealthFailureClassification{
			Eligible: true,
			Kind:     TrafficDirectorHealthFailureKindHTTP5xx,
		}
	}
	// A successful HTTP handshake does not make a later EOF, reset, timeout, or
	// stream/protocol error healthy. Continue into transport classification when
	// Err is present; only a completed non-error status is non-failure evidence.
	if failure.StatusCode != 0 && failure.Err == nil {
		return ignored("non_failure_http_status")
	}
	if failure.Err == nil {
		return ignored("missing_failure_signal")
	}
	if errors.Is(failure.Err, context.DeadlineExceeded) || errors.Is(failure.Err, os.ErrDeadlineExceeded) {
		return TrafficDirectorHealthFailureClassification{Eligible: true, Kind: TrafficDirectorHealthFailureKindTimeout}
	}
	var netErr net.Error
	if errors.As(failure.Err, &netErr) && netErr.Timeout() {
		return TrafficDirectorHealthFailureClassification{Eligible: true, Kind: TrafficDirectorHealthFailureKindTimeout}
	}
	if errors.Is(failure.Err, io.EOF) || errors.Is(failure.Err, io.ErrUnexpectedEOF) {
		return TrafficDirectorHealthFailureClassification{Eligible: true, Kind: TrafficDirectorHealthFailureKindEOF}
	}
	if errors.Is(failure.Err, syscall.ECONNRESET) {
		return TrafficDirectorHealthFailureClassification{Eligible: true, Kind: TrafficDirectorHealthFailureKindReset}
	}
	switch {
	case strings.Contains(message, "timeout"),
		strings.Contains(message, "timed out"),
		strings.Contains(message, "deadline exceeded"):
		return TrafficDirectorHealthFailureClassification{Eligible: true, Kind: TrafficDirectorHealthFailureKindTimeout}
	case message == "eof", strings.Contains(message, "unexpected eof"):
		return TrafficDirectorHealthFailureClassification{Eligible: true, Kind: TrafficDirectorHealthFailureKindEOF}
	case strings.Contains(message, "connection reset"):
		return TrafficDirectorHealthFailureClassification{Eligible: true, Kind: TrafficDirectorHealthFailureKindReset}
	case strings.Contains(message, "stream error"),
		strings.Contains(message, "error in stream"),
		strings.Contains(message, "http2:") && strings.Contains(message, "stream"):
		return TrafficDirectorHealthFailureClassification{Eligible: true, Kind: TrafficDirectorHealthFailureKindStream}
	default:
		return ignored("unqualified_transport_error")
	}
}

var trafficDirectorHealthQuotaMarkers = []string{
	"insufficient_quota",
	"quota exceeded",
	"quota exhausted",
	"usage limit",
	"rate limit",
	"rate_limit",
}

var trafficDirectorHealthClientErrorMarkers = []string{
	"unsupported",
	"not supported",
	"invalid_request",
	"invalid request",
	"client_error",
	"client error",
	"bad request",
}

func trafficDirectorHealthFailureMessage(failure TrafficDirectorHealthFailure) string {
	var message strings.Builder
	if failure.Err != nil {
		message.WriteString(failure.Err.Error())
	}
	if len(failure.ResponseBody) > 0 {
		if message.Len() > 0 {
			message.WriteByte(' ')
		}
		message.Write(failure.ResponseBody)
	}
	return strings.ToLower(message.String())
}

func containsTrafficDirectorHealthMarker(message string, markers []string) bool {
	for _, marker := range markers {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func validateTrafficDirectorHealthCoordinates(accountID int64, model, healthMode string) (string, string, error) {
	model = NormalizeTrafficDirectorHealthModel(model)
	healthMode = strings.ToLower(strings.TrimSpace(healthMode))
	if accountID <= 0 || model == "" {
		return "", "", ErrTrafficDirectorHealthInvalidInput
	}
	switch healthMode {
	case domain.TrafficDirectorHealthModeOff,
		domain.TrafficDirectorHealthModeObserve,
		domain.TrafficDirectorHealthModeEnforce:
		return model, healthMode, nil
	default:
		return "", "", fmt.Errorf("%w: unsupported health mode %q", ErrTrafficDirectorHealthInvalidInput, healthMode)
	}
}

func validateTrafficDirectorHealthSnapshot(snapshot TrafficDirectorHealthSnapshot) error {
	switch snapshot.State {
	case TrafficDirectorHealthStateHealthy:
		if snapshot.FailureStreak != 0 {
			return fmt.Errorf("invalid healthy traffic director health snapshot")
		}
	case TrafficDirectorHealthStateSuspect:
		if snapshot.FailureStreak != 1 || snapshot.LastFailureAt.IsZero() {
			return fmt.Errorf("invalid suspect traffic director health snapshot")
		}
	case TrafficDirectorHealthStateOpen:
		if snapshot.FailureStreak < 2 || snapshot.LastFailureAt.IsZero() || snapshot.OpenUntil.IsZero() {
			return fmt.Errorf("invalid open traffic director health snapshot")
		}
	case TrafficDirectorHealthStateHalfOpen:
		if snapshot.FailureStreak < 2 || snapshot.LastFailureAt.IsZero() {
			return fmt.Errorf("invalid half-open traffic director health snapshot")
		}
		if snapshot.ProbeAcquired && snapshot.ProbeUntil.IsZero() {
			return fmt.Errorf("invalid half-open traffic director probe snapshot")
		}
	default:
		return fmt.Errorf("invalid traffic director health state %q", snapshot.State)
	}
	return nil
}

func (s *TrafficDirectorHealthService) now() time.Time {
	if s == nil || s.clock == nil {
		return time.Now().UTC()
	}
	now := s.clock()
	if now.IsZero() {
		return time.Now().UTC()
	}
	return now.UTC()
}

func newTrafficDirectorHealthProbeToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(token[:]), nil
}
