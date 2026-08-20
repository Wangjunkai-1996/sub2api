package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

const (
	trafficDirectorOperationKeyMaxBytes = 200
	trafficDirectorNoteMaxBytes         = 2000
)

type trafficDirectorRepository struct {
	db *sql.DB
}

type trafficDirectorVersionRecord struct {
	version              service.TrafficDirectorVersion
	unassignedAccountIDs []int64
}

type trafficDirectorRowScanner interface {
	Scan(dest ...any) error
}

func NewTrafficDirectorRepository(db *sql.DB) service.TrafficDirectorRepository {
	return &trafficDirectorRepository{db: db}
}

func (r *trafficDirectorRepository) GetTrafficDirectorGroupState(
	ctx context.Context,
	groupID int64,
) (*service.TrafficDirectorGroupState, error) {
	if r == nil || r.db == nil {
		return nil, trafficDirectorRepositoryUnavailable()
	}

	var (
		state   service.TrafficDirectorGroupState
		specRaw []byte
	)
	err := r.db.QueryRowContext(ctx, `
		SELECT id, name, platform,
		       traffic_director_version, traffic_director_mode, traffic_director_spec
		FROM groups
		WHERE id = $1 AND deleted_at IS NULL
	`, groupID).Scan(
		&state.GroupID,
		&state.GroupName,
		&state.Platform,
		&state.Head.Version,
		&state.Head.Mode,
		&specRaw,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrTrafficDirectorGroupNotFound.WithCause(err)
	}
	if err != nil {
		return nil, fmt.Errorf("load traffic director group: %w", err)
	}
	state.Head.GroupID = state.GroupID
	if state.Head.Version == service.TrafficDirectorLegacyVersion {
		state.Head.Mode = domain.TrafficDirectorModeLegacy
		state.Head.Spec = nil
	} else {
		state.Head.Spec, err = decodeTrafficDirectorSpec(specRaw)
		if err != nil {
			return nil, fmt.Errorf("decode traffic director group head: %w", err)
		}
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT a.id, a.name, a.status, a.schedulable
		FROM account_groups AS ag
		JOIN accounts AS a ON a.id = ag.account_id
		WHERE ag.group_id = $1
		  AND a.deleted_at IS NULL
		ORDER BY a.id ASC
	`, groupID)
	if err != nil {
		return nil, fmt.Errorf("load traffic director group accounts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	state.Accounts = make([]service.TrafficDirectorAccount, 0)
	for rows.Next() {
		var account service.TrafficDirectorAccount
		if err := rows.Scan(&account.ID, &account.Name, &account.Status, &account.Schedulable); err != nil {
			return nil, fmt.Errorf("scan traffic director group account: %w", err)
		}
		state.Accounts = append(state.Accounts, account)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate traffic director group accounts: %w", err)
	}
	return &state, nil
}

func (r *trafficDirectorRepository) GetTrafficDirectorHead(
	ctx context.Context,
	groupID int64,
) (*service.TrafficDirectorHead, error) {
	if r == nil || r.db == nil {
		return nil, trafficDirectorRepositoryUnavailable()
	}

	// The scheduler only needs the immutable head coordinates here. The policy
	// body is resolved through the versioned policy cache, so selecting and
	// decoding traffic_director_spec on this fallback path would add an
	// unnecessary PostgreSQL read/allocation and could make an enforced request
	// depend on a large JSON payload that it will not use.
	var head service.TrafficDirectorHead
	err := r.db.QueryRowContext(ctx, `
		SELECT id, traffic_director_version, traffic_director_mode
		FROM groups
		WHERE id = $1 AND deleted_at IS NULL
	`, groupID).Scan(&head.GroupID, &head.Version, &head.Mode)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrTrafficDirectorGroupNotFound.WithCause(err)
	}
	if err != nil {
		return nil, fmt.Errorf("load traffic director head: %w", err)
	}
	if head.Version == service.TrafficDirectorLegacyVersion {
		head.Mode = domain.TrafficDirectorModeLegacy
	}
	return &head, nil
}

func (r *trafficDirectorRepository) ListTrafficDirectorVersions(
	ctx context.Context,
	groupID int64,
	limit, offset int,
) ([]service.TrafficDirectorVersionSummary, int64, error) {
	if r == nil || r.db == nil {
		return nil, 0, trafficDirectorRepositoryUnavailable()
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	tx, err := r.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelRepeatableRead,
		ReadOnly:  true,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("begin traffic director version list: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var persistedCount int64
	err = tx.QueryRowContext(ctx, `
		SELECT COUNT(v.version)
		FROM groups AS g
		LEFT JOIN traffic_director_versions AS v ON v.group_id = g.id
		WHERE g.id = $1 AND g.deleted_at IS NULL
		GROUP BY g.id
	`, groupID).Scan(&persistedCount)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, 0, service.ErrTrafficDirectorGroupNotFound.WithCause(err)
	}
	if err != nil {
		return nil, 0, fmt.Errorf("count traffic director versions: %w", err)
	}

	total := persistedCount + 1
	versions := make([]service.TrafficDirectorVersionSummary, 0, limit)
	if int64(offset) < persistedCount {
		persistedLimit := int64(limit)
		if remaining := persistedCount - int64(offset); persistedLimit > remaining {
			persistedLimit = remaining
		}
		rows, queryErr := tx.QueryContext(ctx, `
			SELECT group_id, version, mode, checksum, operator_id, note,
			       rollback_from_version, created_at
			FROM traffic_director_versions
			WHERE group_id = $1
			ORDER BY version DESC
			LIMIT $2 OFFSET $3
		`, groupID, persistedLimit, offset)
		if queryErr != nil {
			return nil, 0, fmt.Errorf("list traffic director versions: %w", queryErr)
		}
		for rows.Next() {
			version, scanErr := scanTrafficDirectorVersionSummary(rows)
			if scanErr != nil {
				_ = rows.Close()
				return nil, 0, fmt.Errorf("scan traffic director version: %w", scanErr)
			}
			versions = append(versions, version)
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			return nil, 0, fmt.Errorf("iterate traffic director versions: %w", rowsErr)
		}
		if closeErr := rows.Close(); closeErr != nil {
			return nil, 0, fmt.Errorf("close traffic director versions: %w", closeErr)
		}
	}

	nextIndex := int64(offset) + int64(len(versions))
	if len(versions) < limit && nextIndex == persistedCount {
		versions = append(versions, syntheticTrafficDirectorLegacyVersionSummary(groupID))
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, fmt.Errorf("commit traffic director version list: %w", err)
	}
	return versions, total, nil
}

func (r *trafficDirectorRepository) GetTrafficDirectorVersion(
	ctx context.Context,
	groupID, version int64,
) (*service.TrafficDirectorVersion, error) {
	if r == nil || r.db == nil {
		return nil, trafficDirectorRepositoryUnavailable()
	}
	if version < service.TrafficDirectorLegacyVersion {
		return nil, service.ErrTrafficDirectorVersionNotFound
	}
	if version == service.TrafficDirectorLegacyVersion {
		exists, err := trafficDirectorGroupExists(ctx, r.db, groupID)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, service.ErrTrafficDirectorGroupNotFound
		}
		legacy := syntheticTrafficDirectorLegacyVersion(groupID)
		return &legacy, nil
	}

	record, err := scanTrafficDirectorVersion(r.db.QueryRowContext(ctx, `
		SELECT v.group_id, v.version, v.mode, v.spec, v.checksum, v.operator_id, v.note,
		       v.rollback_from_version, v.created_at, v.operation_key,
		       v.request_fingerprint, v.unassigned_account_ids
		FROM traffic_director_versions AS v
		JOIN groups AS g ON g.id = v.group_id AND g.deleted_at IS NULL
		WHERE v.group_id = $1 AND v.version = $2
	`, groupID, version))
	if errors.Is(err, sql.ErrNoRows) {
		exists, existsErr := trafficDirectorGroupExists(ctx, r.db, groupID)
		if existsErr != nil {
			return nil, existsErr
		}
		if !exists {
			return nil, service.ErrTrafficDirectorGroupNotFound
		}
		return nil, service.ErrTrafficDirectorVersionNotFound.WithCause(err)
	}
	if err != nil {
		return nil, fmt.Errorf("load traffic director version: %w", err)
	}
	return &record.version, nil
}

func (r *trafficDirectorRepository) PublishTrafficDirector(
	ctx context.Context,
	command service.TrafficDirectorPublishCommand,
) (*service.TrafficDirectorPublishResult, error) {
	if r == nil || r.db == nil {
		return nil, trafficDirectorRepositoryUnavailable()
	}
	if err := validateTrafficDirectorPublishCommand(command); err != nil {
		return nil, err
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin traffic director publication: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var (
		groupPlatform string
		actualVersion int64
	)
	err = tx.QueryRowContext(ctx, `
		SELECT platform, traffic_director_version
		FROM groups
		WHERE id = $1 AND deleted_at IS NULL
		FOR UPDATE
	`, command.GroupID).Scan(&groupPlatform, &actualVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrTrafficDirectorGroupNotFound.WithCause(err)
	}
	if err != nil {
		return nil, fmt.Errorf("lock traffic director group: %w", err)
	}

	replayed, err := loadTrafficDirectorVersionByOperation(ctx, tx, command.GroupID, command.IdempotencyKey)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("load traffic director idempotency record: %w", err)
	}
	if err == nil {
		if replayed.version.RequestFingerprint != command.RequestFingerprint {
			return nil, service.ErrTrafficDirectorIdempotencyConflict
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit traffic director replay: %w", err)
		}
		return &service.TrafficDirectorPublishResult{
			Version:              replayed.version,
			Replayed:             true,
			UnassignedAccountIDs: cloneTrafficDirectorAccountIDs(replayed.unassignedAccountIDs),
		}, nil
	}

	if actualVersion != command.ExpectedVersion {
		return nil, trafficDirectorVersionConflict(command.ExpectedVersion, actualVersion)
	}
	if groupPlatform != service.PlatformOpenAI {
		return nil, trafficDirectorRepositoryValidationError(
			"traffic director V1 supports OpenAI groups only",
		)
	}

	accounts, err := lockTrafficDirectorGroupAccounts(ctx, tx, command.GroupID)
	if err != nil {
		return nil, err
	}
	mode, spec, checksum, unassignedAccountIDs, err := validateTrafficDirectorPublicationPolicy(
		command.Mode,
		command.Spec,
		accounts,
	)
	if err != nil {
		return nil, err
	}
	if mode == domain.TrafficDirectorModeEnforced &&
		len(unassignedAccountIDs) > 0 &&
		!command.ConfirmUnassignedAccounts {
		return nil, trafficDirectorRepositoryValidationError(
			"enforced publication requires explicit confirmation for unassigned accounts",
		)
	}
	if err := validateTrafficDirectorRollbackSource(
		ctx,
		tx,
		command.GroupID,
		command.RollbackFromVersion,
		mode,
		checksum,
	); err != nil {
		return nil, err
	}

	specJSON, err := encodeTrafficDirectorSpec(spec)
	if err != nil {
		return nil, err
	}
	// pq.Int64Array encodes a nil slice as SQL NULL. The history column is
	// intentionally NOT NULL, so keep the empty legacy/unassigned set as a
	// non-nil PostgreSQL empty array.
	if unassignedAccountIDs == nil {
		unassignedAccountIDs = []int64{}
	}
	nextVersion := actualVersion + 1
	record, err := scanTrafficDirectorVersion(tx.QueryRowContext(ctx, `
		INSERT INTO traffic_director_versions (
			group_id, version, mode, spec, checksum, operation_key,
			request_fingerprint, operator_id, note, rollback_from_version,
			unassigned_account_ids
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING group_id, version, mode, spec, checksum, operator_id, note,
		          rollback_from_version, created_at, operation_key,
		          request_fingerprint, unassigned_account_ids
	`,
		command.GroupID,
		nextVersion,
		mode,
		specJSON,
		checksum,
		command.IdempotencyKey,
		command.RequestFingerprint,
		command.OperatorID,
		command.Note,
		command.RollbackFromVersion,
		pq.Array(unassignedAccountIDs),
	))
	if err != nil {
		if isUniqueConstraintViolation(err) {
			return nil, service.ErrTrafficDirectorIdempotencyConflict.WithCause(err)
		}
		return nil, fmt.Errorf("insert traffic director version: %w", err)
	}

	result, err := tx.ExecContext(ctx, `
		UPDATE groups
		SET traffic_director_mode = $1,
		    traffic_director_version = $2,
		    traffic_director_spec = $3,
		    updated_at = NOW()
		WHERE id = $4
		  AND deleted_at IS NULL
		  AND traffic_director_version = $5
	`, mode, nextVersion, specJSON, command.GroupID, command.ExpectedVersion)
	if err != nil {
		return nil, fmt.Errorf("update traffic director head: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("read traffic director head update result: %w", err)
	}
	if affected != 1 {
		return nil, trafficDirectorVersionConflict(command.ExpectedVersion, actualVersion)
	}

	if err := enqueueSchedulerOutbox(
		ctx,
		tx,
		service.SchedulerOutboxEventGroupChanged,
		nil,
		&command.GroupID,
		nil,
	); err != nil {
		return nil, fmt.Errorf("enqueue traffic director scheduler event: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit traffic director publication: %w", err)
	}
	return &service.TrafficDirectorPublishResult{
		Version:              record.version,
		UnassignedAccountIDs: cloneTrafficDirectorAccountIDs(record.unassignedAccountIDs),
	}, nil
}

func loadTrafficDirectorVersionByOperation(
	ctx context.Context,
	tx *sql.Tx,
	groupID int64,
	operationKey string,
) (trafficDirectorVersionRecord, error) {
	return scanTrafficDirectorVersion(tx.QueryRowContext(ctx, `
		SELECT group_id, version, mode, spec, checksum, operator_id, note,
		       rollback_from_version, created_at, operation_key,
		       request_fingerprint, unassigned_account_ids
		FROM traffic_director_versions
		WHERE group_id = $1 AND operation_key = $2
	`, groupID, operationKey))
}

func lockTrafficDirectorGroupAccounts(
	ctx context.Context,
	tx *sql.Tx,
	groupID int64,
) ([]service.TrafficDirectorAccount, error) {
	// PublishTrafficDirector holds FOR UPDATE on the parent Group before calling
	// this helper. The account_groups foreign key takes a KEY SHARE lock on that
	// parent for inserts, so new memberships for this Group cannot commit ahead
	// of the publication. Locking the existing join/account rows below covers
	// deletes, moves, account soft-deletes, and eligibility changes. Together
	// these locks protect the validated membership snapshot without blocking
	// unrelated Groups through a table-wide lock.
	rows, err := tx.QueryContext(ctx, `
		SELECT a.id, a.name, a.status, a.schedulable
		FROM account_groups AS ag
		JOIN accounts AS a ON a.id = ag.account_id
		WHERE ag.group_id = $1
		  AND a.deleted_at IS NULL
		ORDER BY a.id ASC
		FOR UPDATE OF ag, a
	`, groupID)
	if err != nil {
		return nil, fmt.Errorf("lock traffic director group accounts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	accounts := make([]service.TrafficDirectorAccount, 0)
	for rows.Next() {
		var account service.TrafficDirectorAccount
		if err := rows.Scan(&account.ID, &account.Name, &account.Status, &account.Schedulable); err != nil {
			return nil, fmt.Errorf("scan locked traffic director group account: %w", err)
		}
		accounts = append(accounts, account)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate locked traffic director group accounts: %w", err)
	}
	return accounts, nil
}

func validateTrafficDirectorPublicationPolicy(
	mode string,
	spec *domain.TrafficDirectorSpec,
	accounts []service.TrafficDirectorAccount,
) (string, *domain.TrafficDirectorSpec, string, []int64, error) {
	switch mode {
	case domain.TrafficDirectorModeLegacy:
		if spec != nil {
			return "", nil, "", nil, trafficDirectorRepositoryValidationError(
				"legacy mode must not include a policy spec",
			)
		}
		return mode, nil, service.TrafficDirectorLegacyChecksum(), []int64{}, nil
	case domain.TrafficDirectorModeShadow, domain.TrafficDirectorModeEnforced:
	default:
		return "", nil, "", nil, trafficDirectorRepositoryValidationError(
			"mode must be one of legacy, shadow, or enforced",
		)
	}
	if spec == nil {
		return "", nil, "", nil, trafficDirectorRepositoryValidationError(
			"shadow and enforced modes require a policy spec",
		)
	}

	accountIDs := make(map[int64]struct{}, len(accounts))
	for _, account := range accounts {
		accountIDs[account.ID] = struct{}{}
	}
	validation, err := service.ValidateTrafficDirectorSpec(*spec, accountIDs)
	if err != nil {
		return "", nil, "", nil, trafficDirectorRepositoryValidationCause(err)
	}
	checksum, err := service.TrafficDirectorSpecChecksum(validation.NormalizedSpec)
	if err != nil {
		return "", nil, "", nil, trafficDirectorRepositoryValidationCause(err)
	}
	normalized := validation.NormalizedSpec
	return mode, &normalized, checksum, validation.UnassignedAccountIDs, nil
}

func validateTrafficDirectorRollbackSource(
	ctx context.Context,
	tx *sql.Tx,
	groupID int64,
	rollbackFromVersion *int64,
	mode, checksum string,
) error {
	if rollbackFromVersion == nil {
		return nil
	}
	if *rollbackFromVersion < service.TrafficDirectorLegacyVersion {
		return service.ErrTrafficDirectorVersionNotFound
	}
	if *rollbackFromVersion == service.TrafficDirectorLegacyVersion {
		if mode != domain.TrafficDirectorModeLegacy || checksum != service.TrafficDirectorLegacyChecksum() {
			return trafficDirectorRepositoryValidationError("rollback policy does not match source version 0")
		}
		return nil
	}

	var sourceMode, sourceChecksum string
	err := tx.QueryRowContext(ctx, `
		SELECT mode, checksum
		FROM traffic_director_versions
		WHERE group_id = $1 AND version = $2
	`, groupID, *rollbackFromVersion).Scan(&sourceMode, &sourceChecksum)
	if errors.Is(err, sql.ErrNoRows) {
		return service.ErrTrafficDirectorVersionNotFound.WithCause(err)
	}
	if err != nil {
		return fmt.Errorf("load traffic director rollback source: %w", err)
	}
	if sourceMode != mode || sourceChecksum != checksum {
		return trafficDirectorRepositoryValidationError(
			"rollback policy does not match source version %d",
			*rollbackFromVersion,
		)
	}
	return nil
}

func validateTrafficDirectorPublishCommand(command service.TrafficDirectorPublishCommand) error {
	if command.GroupID <= 0 {
		return trafficDirectorRepositoryValidationError("group ID must be positive")
	}
	if command.ExpectedVersion < service.TrafficDirectorLegacyVersion {
		return trafficDirectorRepositoryValidationError("expected version must not be negative")
	}
	if command.IdempotencyKey == "" || command.IdempotencyKey != strings.TrimSpace(command.IdempotencyKey) {
		return trafficDirectorRepositoryValidationError("Idempotency-Key is required and must be normalized")
	}
	if len(command.IdempotencyKey) > trafficDirectorOperationKeyMaxBytes {
		return trafficDirectorRepositoryValidationError(
			"Idempotency-Key exceeds %d bytes",
			trafficDirectorOperationKeyMaxBytes,
		)
	}
	if command.Note != strings.TrimSpace(command.Note) {
		return trafficDirectorRepositoryValidationError("note must be normalized")
	}
	if len(command.Note) > trafficDirectorNoteMaxBytes {
		return trafficDirectorRepositoryValidationError("note exceeds %d bytes", trafficDirectorNoteMaxBytes)
	}
	if len(command.RequestFingerprint) != 64 {
		return trafficDirectorRepositoryValidationError("request fingerprint must be a SHA-256 hex digest")
	}
	expectedFingerprint, err := service.TrafficDirectorPublishFingerprint(command)
	if err != nil {
		return fmt.Errorf("calculate traffic director request fingerprint: %w", err)
	}
	if expectedFingerprint != command.RequestFingerprint {
		return trafficDirectorRepositoryValidationError("request fingerprint does not match publication command")
	}
	return nil
}

func scanTrafficDirectorVersion(row trafficDirectorRowScanner) (trafficDirectorVersionRecord, error) {
	var (
		record          trafficDirectorVersionRecord
		specRaw         []byte
		operatorID      sql.NullInt64
		rollbackVersion sql.NullInt64
		unassigned      pq.Int64Array
	)
	err := row.Scan(
		&record.version.GroupID,
		&record.version.Version,
		&record.version.Mode,
		&specRaw,
		&record.version.Checksum,
		&operatorID,
		&record.version.Note,
		&rollbackVersion,
		&record.version.CreatedAt,
		&record.version.OperationKey,
		&record.version.RequestFingerprint,
		&unassigned,
	)
	if err != nil {
		return trafficDirectorVersionRecord{}, err
	}
	record.version.Spec, err = decodeTrafficDirectorSpec(specRaw)
	if err != nil {
		return trafficDirectorVersionRecord{}, err
	}
	if operatorID.Valid {
		value := operatorID.Int64
		record.version.OperatorID = &value
	}
	if rollbackVersion.Valid {
		value := rollbackVersion.Int64
		record.version.RollbackFromVersion = &value
	}
	record.unassignedAccountIDs = cloneTrafficDirectorAccountIDs(unassigned)
	return record, nil
}

func scanTrafficDirectorVersionSummary(row trafficDirectorRowScanner) (service.TrafficDirectorVersionSummary, error) {
	var (
		version         service.TrafficDirectorVersionSummary
		operatorID      sql.NullInt64
		rollbackVersion sql.NullInt64
	)
	if err := row.Scan(
		&version.GroupID,
		&version.Version,
		&version.Mode,
		&version.Checksum,
		&operatorID,
		&version.Note,
		&rollbackVersion,
		&version.CreatedAt,
	); err != nil {
		return service.TrafficDirectorVersionSummary{}, err
	}
	if operatorID.Valid {
		value := operatorID.Int64
		version.OperatorID = &value
	}
	if rollbackVersion.Valid {
		value := rollbackVersion.Int64
		version.RollbackFromVersion = &value
	}
	return version, nil
}

func cloneTrafficDirectorAccountIDs(ids []int64) []int64 {
	if len(ids) == 0 {
		return []int64{}
	}
	return append([]int64(nil), ids...)
}

func encodeTrafficDirectorSpec(spec *domain.TrafficDirectorSpec) (any, error) {
	if spec == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("encode traffic director spec: %w", err)
	}
	return encoded, nil
}

func decodeTrafficDirectorSpec(raw []byte) (*domain.TrafficDirectorSpec, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var spec domain.TrafficDirectorSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

func trafficDirectorGroupExists(ctx context.Context, db *sql.DB, groupID int64) (bool, error) {
	var exists bool
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM groups WHERE id = $1 AND deleted_at IS NULL
		)
	`, groupID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check traffic director group: %w", err)
	}
	return exists, nil
}

func syntheticTrafficDirectorLegacyVersion(groupID int64) service.TrafficDirectorVersion {
	return service.TrafficDirectorVersion{
		GroupID:  groupID,
		Version:  service.TrafficDirectorLegacyVersion,
		Mode:     domain.TrafficDirectorModeLegacy,
		Checksum: service.TrafficDirectorLegacyChecksum(),
	}
}

func syntheticTrafficDirectorLegacyVersionSummary(groupID int64) service.TrafficDirectorVersionSummary {
	return service.TrafficDirectorVersionSummary{
		GroupID:  groupID,
		Version:  service.TrafficDirectorLegacyVersion,
		Mode:     domain.TrafficDirectorModeLegacy,
		Checksum: service.TrafficDirectorLegacyChecksum(),
	}
}

func trafficDirectorVersionConflict(expected, actual int64) error {
	return service.ErrTrafficDirectorVersionConflict.WithMetadata(map[string]string{
		"expected_version": fmt.Sprintf("%d", expected),
		"actual_version":   fmt.Sprintf("%d", actual),
	})
}

func trafficDirectorRepositoryValidationError(format string, args ...any) error {
	return trafficDirectorRepositoryValidationCause(fmt.Errorf(format, args...))
}

func trafficDirectorRepositoryValidationCause(cause error) error {
	return infraerrors.New(
		http.StatusUnprocessableEntity,
		service.ErrTrafficDirectorValidation.Reason,
		cause.Error(),
	).WithCause(cause)
}

func trafficDirectorRepositoryUnavailable() error {
	return service.ErrTrafficDirectorPolicyUnavailable.WithCause(errors.New("traffic director repository is unavailable"))
}
