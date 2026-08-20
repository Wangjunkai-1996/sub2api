package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
)

const (
	TrafficDirectorLegacyVersion   int64 = 0
	trafficDirectorMaxOperationKey       = 200
	trafficDirectorMaxNoteBytes          = 2000
)

var (
	ErrTrafficDirectorGroupNotFound   = infraerrors.NotFound("TRAFFIC_DIRECTOR_GROUP_NOT_FOUND", "traffic director group not found")
	ErrTrafficDirectorVersionNotFound = infraerrors.NotFound(
		"TRAFFIC_DIRECTOR_VERSION_NOT_FOUND",
		"traffic director version not found",
	)
	ErrTrafficDirectorValidation = infraerrors.New(
		http.StatusUnprocessableEntity,
		"TRAFFIC_DIRECTOR_VALIDATION_FAILED",
		"traffic director policy validation failed",
	)
	ErrTrafficDirectorVersionConflict = infraerrors.Conflict(
		"TRAFFIC_DIRECTOR_VERSION_CONFLICT",
		"traffic director policy version changed",
	)
	ErrTrafficDirectorIdempotencyConflict = infraerrors.Conflict(
		"TRAFFIC_DIRECTOR_IDEMPOTENCY_CONFLICT",
		"idempotency key was already used for a different request",
	)
	ErrTrafficDirectorPolicyUnavailable = infraerrors.ServiceUnavailable(
		"TRAFFIC_DIRECTOR_POLICY_UNAVAILABLE",
		"the current traffic director policy is temporarily unavailable",
	)
	ErrTrafficDirectorNoAvailablePool = infraerrors.ServiceUnavailable(
		"TRAFFIC_DIRECTOR_NO_AVAILABLE_POOL",
		"no account is available in the configured traffic director pool chain",
	)
)

// TrafficDirectorHead is the group-level pointer to the currently published policy.
// Version zero is the synthetic legacy policy and has no spec.
type TrafficDirectorHead struct {
	GroupID int64                       `json:"group_id"`
	Version int64                       `json:"version"`
	Mode    string                      `json:"mode"`
	Spec    *domain.TrafficDirectorSpec `json:"spec,omitempty"`
}

// TrafficDirectorVersion is one immutable policy publication or rollback.
type TrafficDirectorVersion struct {
	GroupID             int64                       `json:"group_id"`
	Version             int64                       `json:"version"`
	Mode                string                      `json:"mode"`
	Spec                *domain.TrafficDirectorSpec `json:"spec,omitempty"`
	Checksum            string                      `json:"checksum"`
	OperatorID          *int64                      `json:"operator_id,omitempty"`
	Note                string                      `json:"note"`
	RollbackFromVersion *int64                      `json:"rollback_from_version,omitempty"`
	CreatedAt           time.Time                   `json:"created_at"`

	OperationKey       string `json:"-"`
	RequestFingerprint string `json:"-"`
}

// TrafficDirectorVersionSummary is the compact representation returned by
// history listings. The immutable spec remains available through GetVersion,
// but is deliberately excluded here so one page cannot materialize many large
// policy bodies.
type TrafficDirectorVersionSummary struct {
	GroupID             int64     `json:"group_id"`
	Version             int64     `json:"version"`
	Mode                string    `json:"mode"`
	Checksum            string    `json:"checksum"`
	OperatorID          *int64    `json:"operator_id,omitempty"`
	Note                string    `json:"note"`
	RollbackFromVersion *int64    `json:"rollback_from_version,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
}

// TrafficDirectorAccount is the account inventory used by preview and the editor.
// It intentionally excludes credentials and other account configuration.
type TrafficDirectorAccount struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	Status      string `json:"status"`
	Schedulable bool   `json:"schedulable"`
}

// TrafficDirectorGroupState is the policy head plus the authoritative account inventory.
type TrafficDirectorGroupState struct {
	GroupID   int64                    `json:"group_id"`
	GroupName string                   `json:"group_name"`
	Platform  string                   `json:"platform"`
	Head      TrafficDirectorHead      `json:"head"`
	Accounts  []TrafficDirectorAccount `json:"accounts"`
}

type TrafficDirectorPreview struct {
	GroupID              int64                       `json:"group_id"`
	ExpectedVersion      int64                       `json:"expected_version"`
	Mode                 string                      `json:"mode"`
	NormalizedSpec       *domain.TrafficDirectorSpec `json:"normalized_spec,omitempty"`
	Checksum             string                      `json:"checksum"`
	UnassignedAccountIDs []int64                     `json:"unassigned_account_ids"`
	Accounts             []TrafficDirectorAccount    `json:"accounts"`
}

// TrafficDirectorPublishCommand contains all inputs needed for the repository's
// single publication transaction. The repository must revalidate membership
// while holding the group row lock; service-layer preview is not authoritative.
type TrafficDirectorPublishCommand struct {
	GroupID                   int64
	ExpectedVersion           int64
	Mode                      string
	Spec                      *domain.TrafficDirectorSpec
	ConfirmUnassignedAccounts bool
	IdempotencyKey            string
	RequestFingerprint        string
	OperatorID                *int64
	Note                      string
	RollbackFromVersion       *int64
}

type TrafficDirectorPublishResult struct {
	Version              TrafficDirectorVersion `json:"version"`
	Replayed             bool                   `json:"replayed"`
	UnassignedAccountIDs []int64                `json:"unassigned_account_ids"`
}

type TrafficDirectorPreviewInput struct {
	GroupID         int64
	ExpectedVersion int64
	Mode            string
	Spec            *domain.TrafficDirectorSpec
}

type TrafficDirectorPublishInput struct {
	TrafficDirectorPreviewInput
	ConfirmUnassignedAccounts bool
	IdempotencyKey            string
	OperatorID                *int64
	Note                      string
}

type TrafficDirectorRollbackInput struct {
	GroupID                   int64
	TargetVersion             int64
	ExpectedVersion           int64
	ConfirmUnassignedAccounts bool
	IdempotencyKey            string
	OperatorID                *int64
	Note                      string
}

// TrafficDirectorRepository owns the transactional publication boundary. It is
// deliberately separate from GroupRepository so ordinary group updates cannot
// mutate traffic-director head fields.
type TrafficDirectorRepository interface {
	GetTrafficDirectorGroupState(ctx context.Context, groupID int64) (*TrafficDirectorGroupState, error)
	GetTrafficDirectorHead(ctx context.Context, groupID int64) (*TrafficDirectorHead, error)
	ListTrafficDirectorVersions(ctx context.Context, groupID int64, limit, offset int) ([]TrafficDirectorVersionSummary, int64, error)
	GetTrafficDirectorVersion(ctx context.Context, groupID, version int64) (*TrafficDirectorVersion, error)
	PublishTrafficDirector(ctx context.Context, command TrafficDirectorPublishCommand) (*TrafficDirectorPublishResult, error)
}

// TrafficDirectorPolicyStore is the immutable version lookup used by the
// process-LRU -> Redis -> PostgreSQL read path.
type TrafficDirectorPolicyStore interface {
	GetTrafficDirectorVersion(ctx context.Context, groupID, version int64) (*TrafficDirectorVersion, error)
}

type TrafficDirectorService struct {
	repository           TrafficDirectorRepository
	authCacheInvalidator APIKeyAuthCacheInvalidator
}

func NewTrafficDirectorService(
	repository TrafficDirectorRepository,
	invalidators ...APIKeyAuthCacheInvalidator,
) *TrafficDirectorService {
	var invalidator APIKeyAuthCacheInvalidator
	if len(invalidators) > 0 {
		invalidator = invalidators[0]
	}
	return &TrafficDirectorService{repository: repository, authCacheInvalidator: invalidator}
}

func (s *TrafficDirectorService) Get(ctx context.Context, groupID int64) (*TrafficDirectorGroupState, error) {
	if s == nil || s.repository == nil {
		return nil, ErrTrafficDirectorPolicyUnavailable
	}
	return s.repository.GetTrafficDirectorGroupState(ctx, groupID)
}

func (s *TrafficDirectorService) ListVersions(
	ctx context.Context,
	groupID int64,
	limit, offset int,
) ([]TrafficDirectorVersionSummary, int64, error) {
	if s == nil || s.repository == nil {
		return nil, 0, ErrTrafficDirectorPolicyUnavailable
	}
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if offset < 0 {
		offset = 0
	}
	return s.repository.ListTrafficDirectorVersions(ctx, groupID, limit, offset)
}

func (s *TrafficDirectorService) GetVersion(
	ctx context.Context,
	groupID, version int64,
) (*TrafficDirectorVersion, error) {
	if s == nil || s.repository == nil {
		return nil, ErrTrafficDirectorPolicyUnavailable
	}
	if version < TrafficDirectorLegacyVersion {
		return nil, ErrTrafficDirectorVersionNotFound
	}
	return s.repository.GetTrafficDirectorVersion(ctx, groupID, version)
}

func (s *TrafficDirectorService) Preview(
	ctx context.Context,
	input TrafficDirectorPreviewInput,
) (*TrafficDirectorPreview, error) {
	if s == nil || s.repository == nil {
		return nil, ErrTrafficDirectorPolicyUnavailable
	}
	state, err := s.repository.GetTrafficDirectorGroupState(ctx, input.GroupID)
	if err != nil {
		return nil, err
	}
	if err := validateTrafficDirectorGroupState(state); err != nil {
		return nil, err
	}
	if state.Head.Version != input.ExpectedVersion {
		return nil, trafficDirectorVersionConflict(input.ExpectedVersion, state.Head.Version)
	}

	mode, spec, checksum, unassigned, err := validateTrafficDirectorPolicyInput(input.Mode, input.Spec, state.Accounts, true)
	if err != nil {
		return nil, err
	}
	return &TrafficDirectorPreview{
		GroupID:              input.GroupID,
		ExpectedVersion:      input.ExpectedVersion,
		Mode:                 mode,
		NormalizedSpec:       spec,
		Checksum:             checksum,
		UnassignedAccountIDs: unassigned,
		Accounts:             append([]TrafficDirectorAccount(nil), state.Accounts...),
	}, nil
}

func (s *TrafficDirectorService) Publish(
	ctx context.Context,
	input TrafficDirectorPublishInput,
) (*TrafficDirectorPublishResult, error) {
	if s == nil || s.repository == nil {
		return nil, ErrTrafficDirectorPolicyUnavailable
	}
	// Only perform policy-local normalization here. Group platform, account
	// membership and unassigned-account confirmation are authoritative only while
	// the repository holds the group row lock. This also lets a completed
	// operation replay before mutable group membership is checked again.
	mode, spec, _, _, err := validateTrafficDirectorPolicyInput(input.Mode, input.Spec, nil, false)
	if err != nil {
		return nil, err
	}
	operationKey, note, err := normalizeTrafficDirectorOperation(input.IdempotencyKey, input.Note)
	if err != nil {
		return nil, err
	}

	command := TrafficDirectorPublishCommand{
		GroupID:                   input.GroupID,
		ExpectedVersion:           input.ExpectedVersion,
		Mode:                      mode,
		Spec:                      spec,
		ConfirmUnassignedAccounts: input.ConfirmUnassignedAccounts,
		IdempotencyKey:            operationKey,
		OperatorID:                input.OperatorID,
		Note:                      note,
	}
	command.RequestFingerprint, err = TrafficDirectorPublishFingerprint(command)
	if err != nil {
		return nil, err
	}
	result, err := s.repository.PublishTrafficDirector(ctx, command)
	if err == nil {
		s.invalidateAuthCacheAfterPublication(ctx, input.GroupID)
	}
	return result, err
}

func (s *TrafficDirectorService) Rollback(
	ctx context.Context,
	input TrafficDirectorRollbackInput,
) (*TrafficDirectorPublishResult, error) {
	if s == nil || s.repository == nil {
		return nil, ErrTrafficDirectorPolicyUnavailable
	}
	if input.TargetVersion < TrafficDirectorLegacyVersion {
		return nil, ErrTrafficDirectorVersionNotFound
	}
	target, err := s.repository.GetTrafficDirectorVersion(ctx, input.GroupID, input.TargetVersion)
	if err != nil {
		return nil, err
	}
	operationKey, note, err := normalizeTrafficDirectorOperation(input.IdempotencyKey, input.Note)
	if err != nil {
		return nil, err
	}
	targetVersion := input.TargetVersion
	command := TrafficDirectorPublishCommand{
		GroupID:                   input.GroupID,
		ExpectedVersion:           input.ExpectedVersion,
		Mode:                      target.Mode,
		Spec:                      cloneTrafficDirectorSpec(target.Spec),
		ConfirmUnassignedAccounts: input.ConfirmUnassignedAccounts,
		IdempotencyKey:            operationKey,
		OperatorID:                input.OperatorID,
		Note:                      note,
		RollbackFromVersion:       &targetVersion,
	}
	command.RequestFingerprint, err = TrafficDirectorPublishFingerprint(command)
	if err != nil {
		return nil, err
	}
	result, err := s.repository.PublishTrafficDirector(ctx, command)
	if err == nil {
		s.invalidateAuthCacheAfterPublication(ctx, input.GroupID)
	}
	return result, err
}

func (s *TrafficDirectorService) invalidateAuthCacheAfterPublication(ctx context.Context, groupID int64) {
	if s == nil || s.authCacheInvalidator == nil || groupID <= 0 {
		return
	}
	// Cache invalidation is a post-commit best-effort accelerator. The durable
	// trigger/outbox remains the crash-recovery backstop; never turn a committed
	// policy into an HTTP failure because one cache node is unavailable.
	base := context.WithoutCancel(ctx)
	if deadline, ok := base.Deadline(); !ok || time.Until(deadline) > 2*time.Second {
		var cancel context.CancelFunc
		base, cancel = context.WithTimeout(base, 2*time.Second)
		defer cancel()
	}
	s.authCacheInvalidator.InvalidateAuthCacheByGroupID(base, groupID)
}

func validateTrafficDirectorGroupState(state *TrafficDirectorGroupState) error {
	if state == nil || state.GroupID <= 0 {
		return ErrTrafficDirectorGroupNotFound
	}
	if state.Platform != PlatformOpenAI {
		return trafficDirectorValidationError("traffic director V1 supports OpenAI groups only")
	}
	return nil
}

func validateTrafficDirectorPolicyInput(
	mode string,
	spec *domain.TrafficDirectorSpec,
	accounts []TrafficDirectorAccount,
	validateMembership bool,
) (string, *domain.TrafficDirectorSpec, string, []int64, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch mode {
	case domain.TrafficDirectorModeLegacy:
		if spec != nil {
			return "", nil, "", nil, trafficDirectorValidationError("legacy mode must not include a policy spec")
		}
		// Keep the API contract JSON-stable: the database column and every
		// publication result represent an empty legacy assignment as [] rather
		// than null.
		return mode, nil, TrafficDirectorLegacyChecksum(), []int64{}, nil
	case domain.TrafficDirectorModeShadow, domain.TrafficDirectorModeEnforced:
	default:
		return "", nil, "", nil, trafficDirectorValidationError("mode must be one of legacy, shadow, or enforced")
	}
	if spec == nil {
		return "", nil, "", nil, trafficDirectorValidationError("shadow and enforced modes require a policy spec")
	}

	var groupAccountIDs map[int64]struct{}
	if validateMembership {
		groupAccountIDs = make(map[int64]struct{}, len(accounts))
		for _, account := range accounts {
			if account.ID > 0 {
				groupAccountIDs[account.ID] = struct{}{}
			}
		}
	}
	validation, err := ValidateTrafficDirectorSpec(*spec, groupAccountIDs)
	if err != nil {
		return "", nil, "", nil, trafficDirectorValidationCause(err)
	}
	checksum, err := TrafficDirectorSpecChecksum(validation.NormalizedSpec)
	if err != nil {
		return "", nil, "", nil, trafficDirectorValidationCause(err)
	}
	normalized := validation.NormalizedSpec
	return mode, &normalized, checksum, validation.UnassignedAccountIDs, nil
}

func normalizeTrafficDirectorOperation(operationKey, note string) (string, string, error) {
	operationKey = strings.TrimSpace(operationKey)
	if operationKey == "" {
		return "", "", trafficDirectorValidationError("Idempotency-Key is required")
	}
	if len(operationKey) > trafficDirectorMaxOperationKey {
		return "", "", trafficDirectorValidationError("Idempotency-Key exceeds %d bytes", trafficDirectorMaxOperationKey)
	}
	note = strings.TrimSpace(note)
	if len(note) > trafficDirectorMaxNoteBytes {
		return "", "", trafficDirectorValidationError("note exceeds %d bytes", trafficDirectorMaxNoteBytes)
	}
	return operationKey, note, nil
}

// TrafficDirectorPublishFingerprint is the canonical idempotency fingerprint
// shared by the service and transactional repository.
func TrafficDirectorPublishFingerprint(command TrafficDirectorPublishCommand) (string, error) {
	payload := struct {
		GroupID                   int64                       `json:"group_id"`
		ExpectedVersion           int64                       `json:"expected_version"`
		Mode                      string                      `json:"mode"`
		Spec                      *domain.TrafficDirectorSpec `json:"spec"`
		ConfirmUnassignedAccounts bool                        `json:"confirm_unassigned_accounts"`
		Note                      string                      `json:"note"`
		RollbackFromVersion       *int64                      `json:"rollback_from_version"`
	}{
		GroupID:                   command.GroupID,
		ExpectedVersion:           command.ExpectedVersion,
		Mode:                      command.Mode,
		Spec:                      command.Spec,
		ConfirmUnassignedAccounts: command.ConfirmUnassignedAccounts,
		Note:                      command.Note,
		RollbackFromVersion:       command.RollbackFromVersion,
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal traffic director request fingerprint: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

// TrafficDirectorLegacyChecksum identifies the synthetic version-zero policy.
func TrafficDirectorLegacyChecksum() string {
	sum := sha256.Sum256([]byte(`{"mode":"legacy","spec":null}`))
	return hex.EncodeToString(sum[:])
}

func trafficDirectorVersionConflict(expected, actual int64) error {
	return ErrTrafficDirectorVersionConflict.WithMetadata(map[string]string{
		"expected_version": fmt.Sprintf("%d", expected),
		"actual_version":   fmt.Sprintf("%d", actual),
	})
}

func trafficDirectorValidationError(format string, args ...any) error {
	return trafficDirectorValidationCause(errors.New(fmt.Sprintf(format, args...)))
}

func trafficDirectorValidationCause(cause error) error {
	if cause == nil {
		return ErrTrafficDirectorValidation
	}
	return infraerrors.New(
		http.StatusUnprocessableEntity,
		ErrTrafficDirectorValidation.Reason,
		cause.Error(),
	).WithCause(cause)
}

func cloneTrafficDirectorSpec(spec *domain.TrafficDirectorSpec) *domain.TrafficDirectorSpec {
	if spec == nil {
		return nil
	}
	cloned := *spec
	cloned.Pools = make([]domain.TrafficDirectorPool, len(spec.Pools))
	for index, pool := range spec.Pools {
		cloned.Pools[index] = pool
		cloned.Pools[index].AccountIDs = append([]int64(nil), pool.AccountIDs...)
	}
	return &cloned
}
