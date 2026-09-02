package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"sort"
	"strings"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	dbaccount "github.com/Wei-Shaw/sub2api/ent/account"
	dbaccountegressbinding "github.com/Wei-Shaw/sub2api/ent/accountegressbinding"
	dbegressroute "github.com/Wei-Shaw/sub2api/ent/egressroute"
	dbproxy "github.com/Wei-Shaw/sub2api/ent/proxy"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

type egressRepository struct {
	client *dbent.Client
	db     *sql.DB
}

var _ service.EgressRepository = (*egressRepository)(nil)

func NewEgressRepository(client *dbent.Client, db *sql.DB) service.EgressRepository {
	return &egressRepository{client: client, db: db}
}

func (r *egressRepository) LoadAccountEgressAuthorities(
	ctx context.Context,
	accountIDs []int64,
) (map[int64]service.AccountEgressAuthority, error) {
	accountIDs = uniqueSortedPositiveInt64s(accountIDs)
	result := make(map[int64]service.AccountEgressAuthority, len(accountIDs))
	if len(accountIDs) == 0 {
		return result, nil
	}
	var exec sqlExecutor
	if r != nil && r.db != nil {
		exec = r.db
	} else if r != nil && r.client != nil {
		exec = r.client
	} else {
		return nil, service.ErrEgressRouteInvalid
	}
	rows, err := exec.QueryContext(ctx, `
		SELECT id, egress_mode, egress_revision
		FROM accounts
		WHERE id=ANY($1) AND deleted_at IS NULL
		ORDER BY id`, pq.Array(accountIDs))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var authority service.AccountEgressAuthority
		if err := rows.Scan(&authority.AccountID, &authority.Mode, &authority.Revision); err != nil {
			return nil, err
		}
		result[authority.AccountID] = authority
	}
	return result, rows.Err()
}

func (r *egressRepository) ResolveAccountPool(ctx context.Context, accountID int64) (*service.AccountEgressPoolConfigDomain, error) {
	if r == nil || r.client == nil || accountID <= 0 {
		return nil, service.ErrEgressPoolInvalid
	}
	return resolveAccountPoolWithClient(ctx, r.client, accountID)
}

func resolveAccountPoolWithClient(ctx context.Context, client *dbent.Client, accountID int64) (*service.AccountEgressPoolConfigDomain, error) {
	account, err := client.Account.Query().Where(dbaccount.IDEQ(accountID)).Only(ctx)
	if err != nil {
		if dbent.IsNotFound(err) {
			return nil, service.ErrAccountNotFound
		}
		return nil, err
	}

	source := account
	if account.ParentAccountID != nil {
		source, err = client.Account.Query().Where(dbaccount.IDEQ(*account.ParentAccountID)).Only(ctx)
		if err != nil {
			if dbent.IsNotFound(err) {
				return nil, service.ErrAccountNotFound
			}
			return nil, err
		}
	}

	bindings, err := client.AccountEgressBinding.Query().
		Where(dbaccountegressbinding.AccountIDEQ(source.ID)).
		Order(dbent.Asc(dbaccountegressbinding.FieldPosition), dbent.Asc(dbaccountegressbinding.FieldRouteID)).
		WithRoute(func(q *dbent.EgressRouteQuery) {
			q.WithExpectedIdentity().WithProxy()
		}).
		All(ctx)
	if err != nil {
		return nil, err
	}

	out := &service.AccountEgressPoolConfigDomain{
		AccountID:            account.ID,
		SourceAccountID:      source.ID,
		EgressMode:           string(source.EgressMode),
		EgressRevision:       account.EgressRevision,
		ConcurrencyPerEgress: account.Concurrency,
		PrimaryProxyID:       source.ProxyID,
		Bindings:             make([]service.AccountEgressBinding, 0, len(bindings)),
	}
	for _, binding := range bindings {
		out.Bindings = append(out.Bindings, accountEgressBindingEntityToService(binding, account.ID))
	}
	return out, nil
}

func (r *egressRepository) ListAssignableRoutes(ctx context.Context) ([]service.EgressRoute, error) {
	if r == nil || r.client == nil {
		return nil, service.ErrEgressRouteInvalid
	}
	routes, err := r.client.EgressRoute.Query().
		Where(
			dbegressroute.StateNEQ(dbegressroute.State(service.EgressRouteStateRetired)),
			dbegressroute.Or(
				dbegressroute.KindEQ(dbegressroute.Kind(service.EgressRouteKindDirect)),
				dbegressroute.And(
					dbegressroute.KindEQ(dbegressroute.Kind(service.EgressRouteKindProxy)),
					dbegressroute.HasProxyWith(dbproxy.DeletedAtIsNil()),
				),
			),
		).
		Order(dbent.Asc(dbegressroute.FieldKind), dbent.Asc(dbegressroute.FieldID)).
		WithExpectedIdentity().
		WithProxy().
		All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]service.EgressRoute, 0, len(routes))
	for _, route := range routes {
		mapped := egressRouteEntityToService(route)
		if mapped != nil {
			out = append(out, *mapped)
		}
	}
	return out, nil
}

func (r *egressRepository) GetRoute(ctx context.Context, routeID int64) (*service.EgressRoute, error) {
	if r == nil || r.client == nil || routeID <= 0 {
		return nil, service.ErrEgressRouteInvalid
	}
	return getRouteWithClient(ctx, r.client, routeID)
}

func getRouteWithClient(ctx context.Context, client *dbent.Client, routeID int64) (*service.EgressRoute, error) {
	route, err := client.EgressRoute.Query().
		Where(dbegressroute.IDEQ(routeID)).
		WithExpectedIdentity().
		WithProxy().
		Only(ctx)
	if err != nil {
		if dbent.IsNotFound(err) {
			return nil, service.ErrEgressRouteNotFound
		}
		return nil, err
	}
	return egressRouteEntityToService(route), nil
}

func (r *egressRepository) EnsureProxyRoute(ctx context.Context, proxyID int64) (*service.EgressRoute, error) {
	if r == nil || r.client == nil || proxyID <= 0 {
		return nil, service.ErrEgressRouteInvalid
	}
	lookup := func() (*dbent.EgressRoute, error) {
		return r.client.EgressRoute.Query().Where(dbegressroute.ProxyIDEQ(proxyID)).Only(ctx)
	}
	if existing, err := lookup(); err == nil {
		return r.GetRoute(ctx, existing.ID)
	} else if !dbent.IsNotFound(err) {
		return nil, err
	}

	proxyEntity, err := r.client.Proxy.Query().
		Where(dbproxy.IDEQ(proxyID), dbproxy.DeletedAtIsNil()).
		Only(ctx)
	if err != nil {
		return nil, service.ErrEgressRouteInvalid
	}
	state := service.EgressRouteStatePendingVerification
	if proxyEntity.Status != service.StatusActive {
		state = service.EgressRouteStateInactive
	} else if proxyEntity.ExpiresAt != nil && !proxyEntity.ExpiresAt.After(time.Now()) {
		state = service.EgressRouteStateExpired
	}
	created, err := r.client.EgressRoute.Create().
		SetKind(dbegressroute.Kind(service.EgressRouteKindProxy)).
		SetProxyID(proxyID).
		SetState(dbegressroute.State(state)).
		Save(ctx)
	if err != nil {
		if !dbent.IsConstraintError(err) {
			return nil, err
		}
		created, err = lookup()
		if err != nil {
			return nil, err
		}
	}
	return r.GetRoute(ctx, created.ID)
}

func (r *egressRepository) EnsureDirectRoute(ctx context.Context, runtimeScope string) (*service.EgressRoute, error) {
	if r == nil || r.client == nil {
		return nil, service.ErrEgressRouteInvalid
	}
	runtimeScope = strings.TrimSpace(runtimeScope)
	if runtimeScope == "" || len(runtimeScope) > 128 {
		return nil, service.ErrEgressRouteInvalid
	}
	lookup := func() (*dbent.EgressRoute, error) {
		return r.client.EgressRoute.Query().Where(dbegressroute.RuntimeScopeEQ(runtimeScope)).Only(ctx)
	}
	if existing, err := lookup(); err == nil {
		return r.GetRoute(ctx, existing.ID)
	} else if !dbent.IsNotFound(err) {
		return nil, err
	}
	created, err := r.client.EgressRoute.Create().
		SetKind(dbegressroute.Kind(service.EgressRouteKindDirect)).
		SetRuntimeScope(runtimeScope).
		SetState(dbegressroute.State(service.EgressRouteStatePendingVerification)).
		Save(ctx)
	if err != nil {
		if !dbent.IsConstraintError(err) {
			return nil, err
		}
		created, err = lookup()
		if err != nil {
			return nil, err
		}
	}
	return r.GetRoute(ctx, created.ID)
}

func (r *egressRepository) RecordProbeObservation(ctx context.Context, observation service.EgressProbeObservation) (*service.EgressRoute, error) {
	if r == nil || r.client == nil || observation.RouteID <= 0 || observation.ExpectedRevision <= 0 {
		return nil, service.ErrEgressRouteInvalid
	}
	observedAt := observation.ObservedAt
	if observedAt.IsZero() {
		observedAt = time.Now()
	}
	probeError := strings.TrimSpace(observation.ProbeError)
	if probeError == "" {
		addr, parseErr := netip.ParseAddr(strings.TrimSpace(observation.ObservedIP))
		if parseErr != nil {
			return nil, service.ErrEgressRouteInvalid
		}
		observation.ObservedIP = addr.Unmap().String()
	}

	tx, err := r.client.Tx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := recordProbeObservationTx(ctx, tx, observation, observedAt, probeError); err != nil {
		return nil, err
	}
	updated, err := getRouteWithClient(ctx, tx.Client(), observation.RouteID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return updated, nil
}

func recordProbeObservationTx(
	ctx context.Context,
	tx sqlExecutor,
	observation service.EgressProbeObservation,
	observedAt time.Time,
	probeError string,
) error {
	var (
		currentRevision  int64
		currentState     string
		expectedIdentity sql.NullInt64
		expectedIP       sql.NullString
	)
	err := queryOneRow(ctx, tx, `
			SELECT er.revision, er.state, er.expected_identity_id, host(ei.public_ip)
		FROM egress_routes er
		LEFT JOIN egress_identities ei ON ei.id=er.expected_identity_id
		WHERE er.id=$1
			FOR UPDATE OF er`, []any{observation.RouteID},
		&currentRevision, &currentState, &expectedIdentity, &expectedIP)
	if errors.Is(err, sql.ErrNoRows) {
		return service.ErrEgressRouteNotFound
	}
	if err != nil {
		return err
	}
	if currentRevision != observation.ExpectedRevision {
		return service.ErrEgressRouteConflict
	}

	newState := currentState
	identityChanged := false
	var verifiedIdentityID sql.NullInt64
	switch {
	case currentState == service.EgressRouteStateExpired || currentState == service.EgressRouteStateRetired:
		// Lifecycle state wins over a concurrent or delayed probe result.
	case probeError != "":
		newState = service.EgressRouteStateInactive
	case !expectedIdentity.Valid:
		err = queryOneRow(ctx, tx, `
				INSERT INTO egress_identities (public_ip, status, created_at, updated_at)
			VALUES ($1::inet, $2, NOW(), NOW())
			ON CONFLICT (public_ip) DO UPDATE
				SET status=EXCLUDED.status, updated_at=NOW()
				RETURNING id`, []any{observation.ObservedIP, service.EgressIdentityStatusActive}, &verifiedIdentityID.Int64)
		if err != nil {
			return err
		}
		verifiedIdentityID.Valid = true
		identityChanged = true
		newState = service.EgressRouteStateActive
	case canonicalInetHost(expectedIP.String) == observation.ObservedIP:
		verifiedIdentityID = expectedIdentity
		newState = service.EgressRouteStateActive
	default:
		verifiedIdentityID = expectedIdentity
		newState = service.EgressRouteStateIdentityMismatch
	}
	routeChanged := identityChanged || newState != currentState

	var result sql.Result
	if probeError != "" {
		result, err = tx.ExecContext(ctx, `
			UPDATE egress_routes
			SET state=$1, last_probed_at=$2, last_error=$3,
				revision=revision+CASE WHEN $4 THEN 1 ELSE 0 END, updated_at=NOW()
			WHERE id=$5 AND revision=$6`, newState, observedAt, probeError, routeChanged,
			observation.RouteID, observation.ExpectedRevision)
	} else if identityChanged {
		result, err = tx.ExecContext(ctx, `
			UPDATE egress_routes
			SET expected_identity_id=$1, state=$2, last_observed_ip=$3::inet,
				last_probed_at=$4, verified_at=$4, last_error=NULL,
				revision=revision+1, updated_at=NOW()
			WHERE id=$5 AND revision=$6`, verifiedIdentityID.Int64, newState,
			observation.ObservedIP, observedAt, observation.RouteID, observation.ExpectedRevision)
	} else {
		result, err = tx.ExecContext(ctx, `
			UPDATE egress_routes
			SET state=$1, last_observed_ip=$2::inet, last_probed_at=$3,
				verified_at=CASE WHEN $1=$4 THEN $3 ELSE verified_at END,
				last_error=NULL,
				revision=revision+CASE WHEN $5 THEN 1 ELSE 0 END, updated_at=NOW()
			WHERE id=$6 AND revision=$7`, newState, observation.ObservedIP, observedAt,
			service.EgressRouteStateActive, routeChanged, observation.RouteID, observation.ExpectedRevision)
	}
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated != 1 {
		return service.ErrEgressRouteConflict
	}
	if routeChanged {
		if err := invalidateAccountsForRouteTx(ctx, tx, observation.RouteID); err != nil {
			return err
		}
	}
	return nil
}

func (r *egressRepository) ConfirmIdentity(ctx context.Context, input service.ConfirmEgressIdentityInput) (*service.EgressRoute, error) {
	if r == nil || r.client == nil || input.RouteID <= 0 || input.ExpectedRevision <= 0 {
		return nil, service.ErrEgressRouteInvalid
	}
	addr, err := netip.ParseAddr(strings.TrimSpace(input.ObservedIP))
	if err != nil {
		return nil, service.ErrEgressRouteInvalid
	}
	observedIP := addr.Unmap().String()

	tx, err := r.client.Tx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	var revision int64
	var routeState string
	var lastObservedIP, lastError sql.NullString
	err = queryOneRow(ctx, tx, `
		SELECT revision, state, host(last_observed_ip), last_error
		FROM egress_routes
		WHERE id=$1
		FOR UPDATE`, []any{input.RouteID}, &revision, &routeState, &lastObservedIP, &lastError)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, service.ErrEgressRouteNotFound
	}
	if err != nil {
		return nil, err
	}
	if revision != input.ExpectedRevision {
		return nil, service.ErrEgressRouteConflict
	}
	if routeState == service.EgressRouteStateExpired || routeState == service.EgressRouteStateRetired || routeState == service.EgressRouteStateInactive {
		return nil, service.ErrEgressRouteInvalid
	}
	if !lastObservedIP.Valid || lastObservedIP.String != observedIP || (lastError.Valid && strings.TrimSpace(lastError.String) != "") {
		return nil, service.ErrEgressRouteConflict
	}

	var identityID int64
	err = queryOneRow(ctx, tx, `
		INSERT INTO egress_identities (public_ip, status, created_at, updated_at)
		VALUES ($1::inet, $2, NOW(), NOW())
		ON CONFLICT (public_ip) DO UPDATE
		SET status=EXCLUDED.status, updated_at=NOW()
		RETURNING id`, []any{observedIP, service.EgressIdentityStatusActive}, &identityID)
	if err != nil {
		return nil, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE egress_routes
		SET expected_identity_id=$1, state=$2, verified_at=NOW(), last_error=NULL,
			revision=revision+1, updated_at=NOW()
		WHERE id=$3 AND revision=$4
			AND state NOT IN ($5, $6, $7)
			AND (
				kind=$8
				OR EXISTS (
					SELECT 1 FROM proxies p
					WHERE p.id=egress_routes.proxy_id AND p.deleted_at IS NULL
						AND p.status=$9 AND (p.expires_at IS NULL OR p.expires_at>NOW())
				)
			)`, identityID, service.EgressRouteStateActive, input.RouteID, input.ExpectedRevision,
		service.EgressRouteStateExpired, service.EgressRouteStateRetired, service.EgressRouteStateInactive,
		service.EgressRouteKindDirect, service.StatusActive)
	if err != nil {
		return nil, err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if updated != 1 {
		return nil, service.ErrEgressRouteConflict
	}
	if err := invalidateAccountsForRouteTx(ctx, tx, input.RouteID); err != nil {
		return nil, err
	}
	updatedRoute, err := getRouteWithClient(ctx, tx.Client(), input.RouteID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return updatedRoute, nil
}

func (r *egressRepository) ReplaceAccountPool(ctx context.Context, accountID int64, input service.ReplaceAccountPoolInput) (*service.AccountEgressPoolConfigDomain, error) {
	if r == nil || r.client == nil || accountID <= 0 {
		return nil, service.ErrEgressPoolInvalid
	}
	for attempt := 0; attempt < accountConfigurationLockRetryLimit; attempt++ {
		updated, err := r.replaceAccountPoolOnce(ctx, accountID, input)
		if errors.Is(err, errAccountConfigurationLockSetChanged) {
			continue
		}
		return updated, err
	}
	return nil, service.ErrEgressPoolConflict
}

func (r *egressRepository) replaceAccountPoolOnce(ctx context.Context, accountID int64, input service.ReplaceAccountPoolInput) (*service.AccountEgressPoolConfigDomain, error) {
	tx, err := r.client.Tx(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	if err := replaceAccountPoolTx(ctx, tx, accountID, input); err != nil {
		return nil, err
	}
	updated, err := resolveAccountPoolWithClient(ctx, tx.Client(), accountID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return updated, nil
}

func (r *egressRepository) ReplaceAccountPools(ctx context.Context, accountIDs []int64, input service.ReplaceAccountPoolInput) error {
	if r == nil || r.db == nil {
		return service.ErrEgressPoolInvalid
	}
	accountIDs = uniqueSortedPositiveInt64s(accountIDs)
	if len(accountIDs) == 0 || len(accountIDs) > service.MaxBulkAccountEgressAccounts {
		return service.ErrEgressPoolInvalid
	}
	for attempt := 0; attempt < accountConfigurationLockRetryLimit; attempt++ {
		err := r.replaceAccountPoolsOnce(ctx, accountIDs, input)
		if errors.Is(err, errAccountConfigurationLockSetChanged) {
			continue
		}
		return err
	}
	return service.ErrEgressPoolConflict
}

func (r *egressRepository) ApplyAccountPools(ctx context.Context, accountIDs []int64, input service.ApplyAccountPoolsInput) error {
	if r == nil || r.db == nil {
		return service.ErrEgressPoolInvalid
	}
	accountIDs = uniqueSortedPositiveInt64s(accountIDs)
	if len(accountIDs) == 0 || len(accountIDs) > service.MaxBulkAccountEgressAccounts {
		return service.ErrEgressPoolInvalid
	}

	for attempt := 0; attempt < accountConfigurationLockRetryLimit; attempt++ {
		err := r.applyAccountPoolsOnce(ctx, accountIDs, input)
		if errors.Is(err, errAccountConfigurationLockSetChanged) {
			continue
		}
		return err
	}
	return service.ErrEgressPoolConflict
}

func (r *egressRepository) replaceAccountPoolsOnce(
	ctx context.Context,
	accountIDs []int64,
	input service.ReplaceAccountPoolInput,
) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	plan, err := readBulkAccountPoolLockPlanTx(ctx, tx, accountIDs, input.RouteIDs, service.AccountPoolOperationReplace, input.Mode)
	if err != nil {
		return err
	}
	lockedProxies, err := lockProxiesForShareInOrder(ctx, tx, plan.proxyIDs)
	if err != nil {
		return err
	}
	if err := validateWritableProxyTargets(plan.targetProxyIDs, lockedProxies, time.Now()); err != nil {
		return err
	}
	if err := lockEgressRoutesInOrder(ctx, tx, plan.routeIDs); err != nil {
		return err
	}
	if err := lockAccountsInOrder(ctx, tx, plan.accountIDs); err != nil {
		return err
	}
	lockedPlan, err := readBulkAccountPoolLockPlanTx(ctx, tx, accountIDs, input.RouteIDs, service.AccountPoolOperationReplace, input.Mode)
	if err != nil {
		return err
	}
	if !slices.Equal(plan.proxyIDs, lockedPlan.proxyIDs) ||
		!slices.Equal(plan.targetProxyIDs, lockedPlan.targetProxyIDs) ||
		!slices.Equal(plan.routeIDs, lockedPlan.routeIDs) ||
		!slices.Equal(plan.accountIDs, lockedPlan.accountIDs) {
		return errAccountConfigurationLockSetChanged
	}
	for _, accountID := range accountIDs {
		if err := replaceAccountPoolLockedTx(ctx, tx, accountID, input); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (r *egressRepository) applyAccountPoolsOnce(
	ctx context.Context,
	accountIDs []int64,
	input service.ApplyAccountPoolsInput,
) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	plan, err := readBulkAccountPoolLockPlanTx(ctx, tx, accountIDs, input.RouteIDs, input.Operation, service.EgressModePool)
	if err != nil {
		return err
	}
	lockedProxies, err := lockProxiesForShareInOrder(ctx, tx, plan.proxyIDs)
	if err != nil {
		return err
	}
	if err := validateWritableProxyTargets(plan.targetProxyIDs, lockedProxies, time.Now()); err != nil {
		return err
	}
	if err := lockEgressRoutesInOrder(ctx, tx, plan.routeIDs); err != nil {
		return err
	}
	if err := lockAccountsInOrder(ctx, tx, plan.accountIDs); err != nil {
		return err
	}
	lockedPlan, err := readBulkAccountPoolLockPlanTx(ctx, tx, accountIDs, input.RouteIDs, input.Operation, service.EgressModePool)
	if err != nil {
		return err
	}
	if !slices.Equal(plan.proxyIDs, lockedPlan.proxyIDs) ||
		!slices.Equal(plan.targetProxyIDs, lockedPlan.targetProxyIDs) ||
		!slices.Equal(plan.routeIDs, lockedPlan.routeIDs) ||
		!slices.Equal(plan.accountIDs, lockedPlan.accountIDs) {
		return errAccountConfigurationLockSetChanged
	}
	for _, accountID := range accountIDs {
		replaceInput, err := accountPoolMutationToReplaceInputTx(ctx, tx, accountID, input)
		if err != nil {
			return err
		}
		if err := replaceAccountPoolLockedTx(ctx, tx, accountID, replaceInput); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func readBulkAccountPoolLockPlanTx(
	ctx context.Context,
	exec sqlExecutor,
	rootIDs []int64,
	requestedRouteIDs []int64,
	operation string,
	mode string,
) (accountConfigurationLockPlan, error) {
	plan := accountConfigurationLockPlan{routeIDs: append([]int64(nil), requestedRouteIDs...)}
	rows, err := exec.QueryContext(ctx, `
			SELECT id, proxy_id, proxy_fallback_origin_id, egress_mode
			FROM accounts
			WHERE (id=ANY($1) OR parent_account_id=ANY($1)) AND deleted_at IS NULL
			ORDER BY id`, pq.Array(rootIDs))
	if err != nil {
		return accountConfigurationLockPlan{}, err
	}
	foundRoots := make(map[int64]struct{}, len(rootIDs))
	rootSet := make(map[int64]struct{}, len(rootIDs))
	rootModes := make(map[int64]string, len(rootIDs))
	rootProxyIDs := make(map[int64]int64, len(rootIDs))
	for _, rootID := range rootIDs {
		rootSet[rootID] = struct{}{}
	}
	for rows.Next() {
		var accountID int64
		var egressMode string
		var proxyID, fallbackOriginID sql.NullInt64
		if err := rows.Scan(&accountID, &proxyID, &fallbackOriginID, &egressMode); err != nil {
			_ = rows.Close()
			return accountConfigurationLockPlan{}, err
		}
		plan.accountIDs = append(plan.accountIDs, accountID)
		if proxyID.Valid {
			plan.proxyIDs = append(plan.proxyIDs, proxyID.Int64)
		}
		if fallbackOriginID.Valid {
			plan.proxyIDs = append(plan.proxyIDs, fallbackOriginID.Int64)
		}
		if _, requested := rootSet[accountID]; requested {
			foundRoots[accountID] = struct{}{}
			rootModes[accountID] = egressMode
			if proxyID.Valid {
				rootProxyIDs[accountID] = proxyID.Int64
			}
		}
	}
	if err := rows.Close(); err != nil {
		return accountConfigurationLockPlan{}, err
	}
	if len(foundRoots) != len(rootIDs) {
		return accountConfigurationLockPlan{}, service.ErrAccountNotFound
	}

	rows, err = exec.QueryContext(ctx, `
			SELECT account_id, route_id
			FROM account_egress_bindings
			WHERE account_id=ANY($1)
			ORDER BY account_id, route_id`, pq.Array(rootIDs))
	if err != nil {
		return accountConfigurationLockPlan{}, err
	}
	currentRouteIDs := make(map[int64][]int64, len(rootIDs))
	for rows.Next() {
		var accountID, routeID int64
		if err := rows.Scan(&accountID, &routeID); err != nil {
			_ = rows.Close()
			return accountConfigurationLockPlan{}, err
		}
		plan.routeIDs = append(plan.routeIDs, routeID)
		currentRouteIDs[accountID] = append(currentRouteIDs[accountID], routeID)
	}
	if err := rows.Close(); err != nil {
		return accountConfigurationLockPlan{}, err
	}
	plan.accountIDs = uniqueSortedPositiveInt64s(plan.accountIDs)
	plan.routeIDs = uniqueSortedPositiveInt64s(plan.routeIDs)

	mode = strings.TrimSpace(mode)
	if mode == "" {
		mode = service.EgressModePool
	}
	operation = strings.TrimSpace(operation)
	targetRouteIDs := make([]int64, 0, len(plan.routeIDs))
	if mode == service.EgressModeLegacy {
		for _, rootID := range rootIDs {
			if proxyID, ok := rootProxyIDs[rootID]; ok {
				plan.targetProxyIDs = append(plan.targetProxyIDs, proxyID)
			}
		}
	} else {
		removed := make(map[int64]struct{}, len(requestedRouteIDs))
		for _, routeID := range requestedRouteIDs {
			removed[routeID] = struct{}{}
		}
		for _, rootID := range rootIDs {
			switch operation {
			case service.AccountPoolOperationAppend:
				if rootModes[rootID] == service.EgressModePool {
					targetRouteIDs = append(targetRouteIDs, currentRouteIDs[rootID]...)
				}
				targetRouteIDs = append(targetRouteIDs, requestedRouteIDs...)
			case service.AccountPoolOperationRemove:
				if rootModes[rootID] == service.EgressModePool {
					for _, routeID := range currentRouteIDs[rootID] {
						if _, remove := removed[routeID]; !remove {
							targetRouteIDs = append(targetRouteIDs, routeID)
						}
					}
				}
			default:
				targetRouteIDs = append(targetRouteIDs, requestedRouteIDs...)
			}
		}
	}
	targetRouteIDs = uniqueSortedPositiveInt64s(targetRouteIDs)
	targetRouteProxyIDs, err := proxyIDsForRouteIDsTx(ctx, exec, targetRouteIDs)
	if err != nil {
		return accountConfigurationLockPlan{}, err
	}
	plan.targetProxyIDs = uniqueSortedPositiveInt64s(append(plan.targetProxyIDs, targetRouteProxyIDs...))
	routeProxyIDs, err := proxyIDsForRouteIDsTx(ctx, exec, plan.routeIDs)
	if err != nil {
		return accountConfigurationLockPlan{}, err
	}
	plan.proxyIDs = uniqueSortedPositiveInt64s(append(plan.proxyIDs, routeProxyIDs...))
	plan.proxyIDs = uniqueSortedPositiveInt64s(append(plan.proxyIDs, plan.targetProxyIDs...))
	proxyRouteIDs, err := egressRouteIDsForProxyIDs(ctx, exec, plan.proxyIDs)
	if err != nil {
		return accountConfigurationLockPlan{}, err
	}
	plan.routeIDs = uniqueSortedPositiveInt64s(append(plan.routeIDs, proxyRouteIDs...))
	return plan, nil
}

func (r *egressRepository) SyncProxyRouteLifecycle(ctx context.Context, proxyID int64, proxyStatus string) error {
	if r == nil || r.db == nil || proxyID <= 0 {
		return service.ErrEgressRouteInvalid
	}
	state := service.EgressRouteStateInactive
	switch strings.ToLower(strings.TrimSpace(proxyStatus)) {
	case service.StatusActive:
		state = service.EgressRouteStatePendingVerification
	case "expired":
		state = service.EgressRouteStateExpired
	case "deleted", "retired":
		state = service.EgressRouteStateRetired
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	locked, err := lockProxiesForNoKeyUpdateInOrder(ctx, tx, []int64{proxyID})
	if err != nil {
		return err
	}
	if locked != 1 {
		return service.ErrProxyNotFound
	}
	if err := syncProxyEgressRouteTx(ctx, tx, proxyID, state, true); err != nil {
		return err
	}
	return tx.Commit()
}

// syncProxyEgressRouteTx keeps the proxy transport and its routable egress
// projection in the same transaction. Transport/status edits reset identity
// verification; expiry sweeps may retain the last confirmed identity while
// making the route ineligible immediately.
func syncProxyEgressRouteTx(
	ctx context.Context,
	exec sqlExecutor,
	proxyID int64,
	state string,
	resetVerification bool,
) error {
	routeID, err := syncProxyEgressRouteStateTx(ctx, exec, proxyID, state, resetVerification)
	if err != nil {
		return err
	}
	return invalidateAccountsForRouteTx(ctx, exec, routeID)
}

func syncProxyEgressRouteStateTx(
	ctx context.Context,
	exec sqlExecutor,
	proxyID int64,
	state string,
	resetVerification bool,
) (int64, error) {
	if exec == nil || proxyID <= 0 || !validProxyLifecycleRouteState(state) {
		return 0, service.ErrEgressRouteInvalid
	}

	var (
		rows *sql.Rows
		err  error
	)
	if resetVerification {
		rows, err = exec.QueryContext(ctx, `
			INSERT INTO egress_routes
				(kind, proxy_id, state, revision, created_at, updated_at)
			VALUES ($1, $2, $3, 1, NOW(), NOW())
			ON CONFLICT (proxy_id) DO UPDATE
			SET state=EXCLUDED.state,
				expected_identity_id=NULL,
				last_observed_ip=NULL,
				last_probed_at=NULL,
				verified_at=NULL,
				last_error=NULL,
				revision=egress_routes.revision+1,
				updated_at=NOW()
			RETURNING id`, service.EgressRouteKindProxy, proxyID, state)
	} else {
		rows, err = exec.QueryContext(ctx, `
			INSERT INTO egress_routes
				(kind, proxy_id, state, revision, created_at, updated_at)
			VALUES ($1, $2, $3, 1, NOW(), NOW())
			ON CONFLICT (proxy_id) DO UPDATE
			SET state=EXCLUDED.state,
				last_error=NULL,
				revision=egress_routes.revision+1,
				updated_at=NOW()
			RETURNING id`, service.EgressRouteKindProxy, proxyID, state)
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if rowsErr := rows.Err(); rowsErr != nil {
			return 0, rowsErr
		}
		return 0, service.ErrEgressRouteInvalid
	}
	var routeID int64
	if err := rows.Scan(&routeID); err != nil {
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	return routeID, nil
}

func validProxyLifecycleRouteState(state string) bool {
	switch state {
	case service.EgressRouteStatePendingVerification,
		service.EgressRouteStateInactive,
		service.EgressRouteStateExpired,
		service.EgressRouteStateRetired:
		return true
	default:
		return false
	}
}

// applyAccountEgressWrite is called by the account repository while its
// account/group transaction is still open. Keeping this here (rather than in
// the HTTP handler) guarantees that a failed route validation rolls back the
// account row, bindings, mirror proxy_id, and scheduler outbox together.
func applyAccountEgressWrite(ctx context.Context, exec sqlExecutor, account *service.Account) error {
	if account == nil || account.EgressPoolWrite == nil {
		return nil
	}
	input := *account.EgressPoolWrite
	input.RouteIDs = append([]int64(nil), input.RouteIDs...)
	if strings.TrimSpace(input.Mode) == "" {
		input.Mode = service.EgressModePool
	}
	if !service.ValidateReplaceAccountPoolInput(input) {
		return service.ErrEgressPoolInvalid
	}
	if err := replaceAccountPoolTx(ctx, exec, account.ID, input); err != nil {
		return err
	}

	// Refresh the in-memory object from the same transaction. This object may be
	// returned directly by the admin create path, and must never advertise the
	// stale legacy mirror or revision after the pool write.
	rows, err := exec.QueryContext(ctx, `
		SELECT egress_mode, egress_revision, concurrency, proxy_id
		FROM accounts
		WHERE id=$1 AND deleted_at IS NULL`, account.ID)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if rowsErr := rows.Err(); rowsErr != nil {
			return rowsErr
		}
		return service.ErrAccountNotFound
	}
	var mode string
	var revision int64
	var concurrency int
	var proxyID sql.NullInt64
	if err := rows.Scan(&mode, &revision, &concurrency, &proxyID); err != nil {
		return err
	}
	account.EgressMode = mode
	account.EgressRevision = revision
	account.Concurrency = concurrency
	if proxyID.Valid {
		id := proxyID.Int64
		account.ProxyID = &id
	} else {
		account.ProxyID = nil
	}
	account.EgressPoolWrite = nil
	return nil
}

func replaceAccountPoolTx(ctx context.Context, tx sqlExecutor, accountID int64, input service.ReplaceAccountPoolInput) error {
	plan, err := readAccountPoolLockPlanTx(ctx, tx, accountID, input)
	if err != nil {
		return err
	}
	lockedProxies, err := lockProxiesForShareInOrder(ctx, tx, plan.proxyIDs)
	if err != nil {
		return err
	}
	if err := validateWritableProxyTargets(plan.targetProxyIDs, lockedProxies, time.Now()); err != nil {
		return err
	}
	if err := lockEgressRoutesInOrder(ctx, tx, plan.routeIDs); err != nil {
		return err
	}
	if err := lockAccountsInOrder(ctx, tx, plan.accountIDs); err != nil {
		return err
	}
	lockedPlan, err := readAccountPoolLockPlanTx(ctx, tx, accountID, input)
	if err != nil {
		return err
	}
	if !slices.Equal(plan.proxyIDs, lockedPlan.proxyIDs) ||
		!slices.Equal(plan.targetProxyIDs, lockedPlan.targetProxyIDs) ||
		!slices.Equal(plan.routeIDs, lockedPlan.routeIDs) ||
		!slices.Equal(plan.accountIDs, lockedPlan.accountIDs) {
		return errAccountConfigurationLockSetChanged
	}
	return replaceAccountPoolLockedTx(ctx, tx, accountID, input)
}

func readAccountPoolLockPlanTx(
	ctx context.Context,
	exec sqlExecutor,
	accountID int64,
	input service.ReplaceAccountPoolInput,
) (accountConfigurationLockPlan, error) {
	plan := accountConfigurationLockPlan{routeIDs: append([]int64(nil), input.RouteIDs...)}
	rows, err := exec.QueryContext(ctx, `
		SELECT id, proxy_id, proxy_fallback_origin_id
		FROM accounts
		WHERE (id=$1 OR parent_account_id=$1) AND deleted_at IS NULL
		ORDER BY id`, accountID)
	if err != nil {
		return accountConfigurationLockPlan{}, err
	}
	var rootProxyID sql.NullInt64
	for rows.Next() {
		var id int64
		var proxyID, fallbackOriginID sql.NullInt64
		if err := rows.Scan(&id, &proxyID, &fallbackOriginID); err != nil {
			_ = rows.Close()
			return accountConfigurationLockPlan{}, err
		}
		plan.accountIDs = append(plan.accountIDs, id)
		if proxyID.Valid {
			plan.proxyIDs = append(plan.proxyIDs, proxyID.Int64)
			if id == accountID {
				rootProxyID = proxyID
			}
		}
		if fallbackOriginID.Valid {
			plan.proxyIDs = append(plan.proxyIDs, fallbackOriginID.Int64)
		}
	}
	if err := rows.Close(); err != nil {
		return accountConfigurationLockPlan{}, err
	}
	if len(plan.accountIDs) == 0 || plan.accountIDs[0] != accountID {
		return accountConfigurationLockPlan{}, service.ErrAccountNotFound
	}

	rows, err = exec.QueryContext(ctx, `
		SELECT route_id
		FROM account_egress_bindings
		WHERE account_id=$1
		ORDER BY route_id`, accountID)
	if err != nil {
		return accountConfigurationLockPlan{}, err
	}
	for rows.Next() {
		var routeID int64
		if err := rows.Scan(&routeID); err != nil {
			_ = rows.Close()
			return accountConfigurationLockPlan{}, err
		}
		plan.routeIDs = append(plan.routeIDs, routeID)
	}
	if err := rows.Close(); err != nil {
		return accountConfigurationLockPlan{}, err
	}
	plan.routeIDs = uniqueSortedPositiveInt64s(plan.routeIDs)
	mode := strings.TrimSpace(input.Mode)
	if mode == "" || mode == service.EgressModePool {
		targetProxyIDs, err := proxyIDsForRouteIDsTx(ctx, exec, input.RouteIDs)
		if err != nil {
			return accountConfigurationLockPlan{}, err
		}
		plan.targetProxyIDs = append(plan.targetProxyIDs, targetProxyIDs...)
	} else if mode == service.EgressModeLegacy && rootProxyID.Valid {
		// The root mirror is written back to every shadow when switching out of
		// pool mode; fallback origins and shadow mirrors are source-only locks.
		plan.targetProxyIDs = append(plan.targetProxyIDs, rootProxyID.Int64)
	}
	plan.targetProxyIDs = uniqueSortedPositiveInt64s(plan.targetProxyIDs)
	routeProxyIDs, err := proxyIDsForRouteIDsTx(ctx, exec, plan.routeIDs)
	if err != nil {
		return accountConfigurationLockPlan{}, err
	}
	plan.proxyIDs = uniqueSortedPositiveInt64s(append(plan.proxyIDs, routeProxyIDs...))
	plan.proxyIDs = uniqueSortedPositiveInt64s(append(plan.proxyIDs, plan.targetProxyIDs...))
	proxyRouteIDs, err := egressRouteIDsForProxyIDs(ctx, exec, plan.proxyIDs)
	if err != nil {
		return accountConfigurationLockPlan{}, err
	}
	plan.routeIDs = uniqueSortedPositiveInt64s(append(plan.routeIDs, proxyRouteIDs...))
	return plan, nil
}

func replaceAccountPoolLockedTx(ctx context.Context, tx sqlExecutor, accountID int64, input service.ReplaceAccountPoolInput) error {
	mode := strings.TrimSpace(input.Mode)
	if mode == "" {
		mode = service.EgressModePool
	}
	if mode != service.EgressModeLegacy && mode != service.EgressModePool {
		return service.ErrEgressPoolInvalid
	}
	routeIDs, ok := uniquePositiveInt64sPreserveOrder(input.RouteIDs)
	if !ok || len(routeIDs) > service.MaxAccountEgressRoutes {
		return service.ErrEgressPoolInvalid
	}
	if mode == service.EgressModePool && len(routeIDs) == 0 {
		return service.ErrEgressPoolInvalid
	}
	if len(routeIDs) > 0 && !containsInt64(routeIDs, input.PrimaryRouteID) {
		return service.ErrEgressPoolInvalid
	}

	var currentConcurrency int
	var currentRevision int64
	var parentAccountID sql.NullInt64
	var currentMode string
	var currentProxyID sql.NullInt64
	accountRows, err := tx.QueryContext(ctx, `
			SELECT concurrency, egress_revision, parent_account_id, egress_mode, proxy_id
			FROM accounts
			WHERE id=$1 AND deleted_at IS NULL
			FOR UPDATE`, accountID)
	if err != nil {
		return err
	}
	if !accountRows.Next() {
		if rowsErr := accountRows.Err(); rowsErr != nil {
			_ = accountRows.Close()
			return rowsErr
		}
		_ = accountRows.Close()
		return service.ErrAccountNotFound
	}
	if err := accountRows.Scan(&currentConcurrency, &currentRevision, &parentAccountID, &currentMode, &currentProxyID); err != nil {
		_ = accountRows.Close()
		return err
	}
	if err := accountRows.Close(); err != nil {
		return err
	}
	if parentAccountID.Valid {
		return service.ErrEgressPoolInvalid
	}
	if input.ExpectedRevision != nil && currentRevision != *input.ExpectedRevision {
		return service.ErrEgressPoolConflict
	}
	concurrency := currentConcurrency
	if input.ConcurrencyPerEgress != nil {
		concurrency = *input.ConcurrencyPerEgress
	}
	if mode == service.EgressModePool && (concurrency < 1 || concurrency > 10000) {
		return service.ErrEgressPoolInvalid
	}

	currentBindingRows, err := tx.QueryContext(ctx, `
		SELECT route_id, is_primary
		FROM account_egress_bindings
		WHERE account_id=$1
		ORDER BY position, route_id`, accountID)
	if err != nil {
		return err
	}
	currentRouteIDs := make([]int64, 0, service.MaxAccountEgressRoutes)
	currentPrimaryRouteID := int64(0)
	for currentBindingRows.Next() {
		var routeID int64
		var primary bool
		if err := currentBindingRows.Scan(&routeID, &primary); err != nil {
			_ = currentBindingRows.Close()
			return err
		}
		currentRouteIDs = append(currentRouteIDs, routeID)
		if primary {
			currentPrimaryRouteID = routeID
		}
	}
	if err := currentBindingRows.Close(); err != nil {
		return err
	}

	routingChanged := currentMode != mode
	bindingsChanged := false
	var primaryProxyID *int64
	if mode == service.EgressModePool {
		routeProxyIDs, err := validateSelectedRoutesTx(ctx, tx, routeIDs, true)
		if err != nil {
			return err
		}
		primaryProxyID = routeProxyIDs[input.PrimaryRouteID]
		bindingsChanged = currentMode != service.EgressModePool ||
			!slices.Equal(currentRouteIDs, routeIDs) || currentPrimaryRouteID != input.PrimaryRouteID
		routingChanged = routingChanged || bindingsChanged
	} else if currentProxyID.Valid {
		// Switching back to legacy keeps the mirrored primary transport. Bindings
		// remain durable so rollback/re-entry does not require reconstructing them.
		proxyID := currentProxyID.Int64
		primaryProxyID = &proxyID
	}
	capacityChanged := concurrency != currentConcurrency
	if !routingChanged && !capacityChanged {
		return nil
	}

	if bindingsChanged {
		if _, err := tx.ExecContext(ctx, `DELETE FROM account_egress_bindings WHERE account_id=$1`, accountID); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `
			INSERT INTO account_egress_bindings
				(account_id, route_id, position, is_primary, status, created_at, updated_at)
			SELECT $1, route_id, ordinality-1, route_id=$3, $4, NOW(), NOW()
			FROM unnest($2::bigint[]) WITH ORDINALITY AS selected(route_id, ordinality)`,
			accountID, pq.Array(routeIDs), input.PrimaryRouteID, service.AccountEgressBindingStatusActive)
		if err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `
		UPDATE accounts
		SET egress_mode=$2, concurrency=$3, proxy_id=$4, proxy_fallback_origin_id=NULL,
			egress_revision=egress_revision+1, updated_at=NOW()
		WHERE id=$1 AND deleted_at IS NULL`, accountID, mode, concurrency, primaryProxyID)
	if err != nil {
		return err
	}

	shadowIDs := make([]int64, 0, 1)
	if routingChanged {
		rows, err := tx.QueryContext(ctx, `
			UPDATE accounts
			SET proxy_id=$2, proxy_fallback_origin_id=NULL,
				egress_revision=egress_revision+1, updated_at=NOW()
			WHERE parent_account_id=$1 AND deleted_at IS NULL
			RETURNING id`, accountID, primaryProxyID)
		if err != nil {
			return err
		}
		for rows.Next() {
			var shadowID int64
			if err := rows.Scan(&shadowID); err != nil {
				_ = rows.Close()
				return err
			}
			shadowIDs = append(shadowIDs, shadowID)
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	if err := enqueueSchedulerOutbox(ctx, tx, service.SchedulerOutboxEventAccountChanged, &accountID, nil, nil); err != nil {
		return err
	}
	for _, shadowID := range shadowIDs {
		if err := enqueueSchedulerOutbox(ctx, tx, service.SchedulerOutboxEventAccountChanged, &shadowID, nil, nil); err != nil {
			return err
		}
	}
	return nil
}

func accountPoolMutationToReplaceInputTx(
	ctx context.Context,
	tx *sql.Tx,
	accountID int64,
	input service.ApplyAccountPoolsInput,
) (service.ReplaceAccountPoolInput, error) {
	operation := strings.TrimSpace(input.Operation)
	switch operation {
	case service.AccountPoolOperationAppend, service.AccountPoolOperationRemove, service.AccountPoolOperationReplace:
	default:
		return service.ReplaceAccountPoolInput{}, service.ErrEgressPoolInvalid
	}
	mutationRouteIDs, ok := uniquePositiveInt64sPreserveOrder(input.RouteIDs)
	if !ok || len(mutationRouteIDs) > service.MaxAccountEgressRoutes {
		return service.ReplaceAccountPoolInput{}, service.ErrEgressPoolInvalid
	}
	// append/remove with an empty route list is the explicit concurrency-only
	// bulk form. The current bindings are read below and carried forward. A
	// replace operation must always name the complete replacement set.
	if len(mutationRouteIDs) == 0 && (input.ConcurrencyPerEgress == nil || operation == service.AccountPoolOperationReplace) {
		return service.ReplaceAccountPoolInput{}, service.ErrEgressPoolInvalid
	}

	var currentRevision int64
	var parentAccountID sql.NullInt64
	var currentMode string
	err := tx.QueryRowContext(ctx, `
			SELECT egress_revision, parent_account_id, egress_mode
			FROM accounts
			WHERE id=$1 AND deleted_at IS NULL
			FOR UPDATE`, accountID).Scan(&currentRevision, &parentAccountID, &currentMode)
	if errors.Is(err, sql.ErrNoRows) {
		return service.ReplaceAccountPoolInput{}, service.ErrAccountNotFound
	}
	if err != nil {
		return service.ReplaceAccountPoolInput{}, err
	}
	if parentAccountID.Valid {
		return service.ReplaceAccountPoolInput{}, service.ErrEgressPoolInvalid
	}
	if currentMode != service.EgressModePool && (operation == service.AccountPoolOperationRemove || len(mutationRouteIDs) == 0) {
		// Migration 241 left dormant bindings on legacy accounts. A
		// concurrency-only append/remove is not an explicit opt-in and must not
		// silently convert those accounts to pool mode.
		return service.ReplaceAccountPoolInput{}, service.ErrEgressPoolInvalid
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT route_id, is_primary
		FROM account_egress_bindings
		WHERE account_id=$1
		ORDER BY position, route_id`, accountID)
	if err != nil {
		return service.ReplaceAccountPoolInput{}, err
	}
	currentRouteIDs := make([]int64, 0, service.MaxAccountEgressRoutes)
	var currentPrimaryRouteID int64
	for rows.Next() {
		var routeID int64
		var primary bool
		if err := rows.Scan(&routeID, &primary); err != nil {
			_ = rows.Close()
			return service.ReplaceAccountPoolInput{}, err
		}
		currentRouteIDs = append(currentRouteIDs, routeID)
		if primary {
			currentPrimaryRouteID = routeID
		}
	}
	if err := rows.Close(); err != nil {
		return service.ReplaceAccountPoolInput{}, err
	}

	var routeIDs []int64
	switch operation {
	case service.AccountPoolOperationReplace:
		routeIDs = append([]int64(nil), mutationRouteIDs...)
	case service.AccountPoolOperationAppend:
		if currentMode == service.EgressModePool {
			routeIDs = append([]int64(nil), currentRouteIDs...)
		}
		seen := make(map[int64]struct{}, len(routeIDs)+len(mutationRouteIDs))
		for _, routeID := range routeIDs {
			seen[routeID] = struct{}{}
		}
		for _, routeID := range mutationRouteIDs {
			if _, exists := seen[routeID]; exists {
				continue
			}
			seen[routeID] = struct{}{}
			routeIDs = append(routeIDs, routeID)
		}
	case service.AccountPoolOperationRemove:
		removed := make(map[int64]struct{}, len(mutationRouteIDs))
		for _, routeID := range mutationRouteIDs {
			removed[routeID] = struct{}{}
		}
		routeIDs = make([]int64, 0, len(currentRouteIDs))
		for _, routeID := range currentRouteIDs {
			if _, remove := removed[routeID]; !remove {
				routeIDs = append(routeIDs, routeID)
			}
		}
	}
	if len(routeIDs) == 0 || len(routeIDs) > service.MaxAccountEgressRoutes {
		return service.ReplaceAccountPoolInput{}, service.ErrEgressPoolInvalid
	}

	primaryRouteID := currentPrimaryRouteID
	if input.PrimaryRouteID != nil {
		primaryRouteID = *input.PrimaryRouteID
	}
	if !containsInt64(routeIDs, primaryRouteID) {
		primaryRouteID = routeIDs[0]
	}
	return service.ReplaceAccountPoolInput{
		Mode:                 service.EgressModePool,
		RouteIDs:             routeIDs,
		PrimaryRouteID:       primaryRouteID,
		ConcurrencyPerEgress: input.ConcurrencyPerEgress,
		ExpectedRevision:     &currentRevision,
	}, nil
}

func validateSelectedRoutesTx(ctx context.Context, tx sqlExecutor, routeIDs []int64, requireEligible bool) (map[int64]*int64, error) {
	result := make(map[int64]*int64, len(routeIDs))
	if len(routeIDs) == 0 {
		return result, nil
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT er.id, er.kind, er.proxy_id, er.state, er.expected_identity_id,
			er.verified_at, COALESCE(ei.status, ''), COALESCE(p.status, ''), p.expires_at, p.deleted_at
		FROM egress_routes er
		LEFT JOIN egress_identities ei ON ei.id=er.expected_identity_id
		LEFT JOIN proxies p ON p.id=er.proxy_id
		WHERE er.id=ANY($1)
		FOR SHARE OF er`, pq.Array(routeIDs))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	now := time.Now()
	for rows.Next() {
		var routeID int64
		var kind, state, identityStatus, proxyStatus string
		var proxyID, identityID sql.NullInt64
		var verifiedAt, expiresAt, deletedAt sql.NullTime
		if err := rows.Scan(&routeID, &kind, &proxyID, &state, &identityID, &verifiedAt, &identityStatus, &proxyStatus, &expiresAt, &deletedAt); err != nil {
			return nil, err
		}
		if state == service.EgressRouteStateRetired {
			return nil, service.ErrEgressPoolInvalid
		}
		if requireEligible {
			// Direct routes are retained only as dormant legacy/backfill data. New
			// pool configurations must be backed by explicit proxy transports.
			if kind != service.EgressRouteKindProxy {
				return nil, service.ErrEgressPoolInvalid
			}
			if state != service.EgressRouteStateActive || !identityID.Valid || identityStatus != service.EgressIdentityStatusActive {
				return nil, service.ErrEgressPoolInvalid
			}
			if !verifiedAt.Valid || !service.IsEgressIdentityVerificationFresh(&verifiedAt.Time, now) {
				return nil, service.ErrEgressPoolInvalid
			}
			if kind == service.EgressRouteKindProxy && (!proxyID.Valid || proxyStatus != service.StatusActive || deletedAt.Valid || (expiresAt.Valid && !expiresAt.Time.After(now))) {
				return nil, service.ErrEgressPoolInvalid
			}
		}
		if proxyID.Valid {
			id := proxyID.Int64
			result[routeID] = &id
		} else {
			result[routeID] = nil
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result) != len(routeIDs) {
		return nil, service.ErrEgressPoolInvalid
	}
	return result, nil
}

func invalidateAccountsForRouteTx(ctx context.Context, tx sqlExecutor, routeID int64) error {
	accountIDs, err := accountIDsForRouteTx(ctx, tx, routeID)
	if err != nil {
		return err
	}
	if len(accountIDs) == 0 {
		return nil
	}
	if err := lockAccountsInOrder(ctx, tx, accountIDs); err != nil {
		return err
	}
	return invalidateLockedAccountsTx(ctx, tx, accountIDs)
}

func accountIDsForRouteTx(ctx context.Context, exec sqlExecutor, routeID int64) ([]int64, error) {
	rows, err := exec.QueryContext(ctx, `
		WITH roots AS (
			SELECT DISTINCT b.account_id
			FROM account_egress_bindings b
			JOIN accounts root ON root.id=b.account_id
			WHERE b.route_id=$1
				AND root.deleted_at IS NULL
				AND root.platform=$2
				AND root.egress_mode=$3
				AND root.parent_account_id IS NULL
		)
		SELECT account_id AS id FROM roots
		UNION
		SELECT a.id
		FROM accounts a
		JOIN roots r ON r.account_id=a.parent_account_id
		WHERE a.deleted_at IS NULL
		ORDER BY id`, routeID, service.PlatformOpenAI, service.EgressModePool)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	accountIDs := make([]int64, 0)
	for rows.Next() {
		var accountID int64
		if err := rows.Scan(&accountID); err != nil {
			return nil, err
		}
		accountIDs = append(accountIDs, accountID)
	}
	return accountIDs, rows.Err()
}

func invalidateLockedAccountsTx(ctx context.Context, exec sqlExecutor, accountIDs []int64) error {
	accountIDs = uniqueSortedPositiveInt64s(accountIDs)
	if len(accountIDs) == 0 {
		return nil
	}
	result, err := exec.ExecContext(ctx, `
		UPDATE accounts
		SET egress_revision=egress_revision+1, updated_at=NOW()
		WHERE id=ANY($1) AND deleted_at IS NULL`, pq.Array(accountIDs))
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated != int64(len(accountIDs)) {
		return errAccountConfigurationLockSetChanged
	}
	payload := map[string]any{"account_ids": accountIDs}
	return enqueueSchedulerOutbox(ctx, exec, service.SchedulerOutboxEventAccountBulkChanged, nil, nil, payload)
}

func egressIdentityEntityToService(entity *dbent.EgressIdentity) *service.EgressIdentity {
	if entity == nil {
		return nil
	}
	return &service.EgressIdentity{
		ID:        entity.ID,
		PublicIP:  canonicalInetHost(entity.PublicIP),
		Status:    string(entity.Status),
		CreatedAt: entity.CreatedAt,
		UpdatedAt: entity.UpdatedAt,
	}
}

func egressRouteEntityToService(entity *dbent.EgressRoute) *service.EgressRoute {
	if entity == nil {
		return nil
	}
	out := &service.EgressRoute{
		ID:                 entity.ID,
		Kind:               string(entity.Kind),
		ProxyID:            entity.ProxyID,
		RuntimeScope:       entity.RuntimeScope,
		ExpectedIdentityID: entity.ExpectedIdentityID,
		State:              string(entity.State),
		LastObservedIP:     entity.LastObservedIP,
		LastProbedAt:       entity.LastProbedAt,
		VerifiedAt:         entity.VerifiedAt,
		Revision:           entity.Revision,
		LastError:          entity.LastError,
		CreatedAt:          entity.CreatedAt,
		UpdatedAt:          entity.UpdatedAt,
	}
	if entity.LastObservedIP != nil {
		ip := canonicalInetHost(*entity.LastObservedIP)
		out.LastObservedIP = &ip
	}
	if entity.Edges.ExpectedIdentity != nil {
		out.ExpectedIdentity = egressIdentityEntityToService(entity.Edges.ExpectedIdentity)
	}
	if entity.Edges.Proxy != nil {
		out.Proxy = proxyEntityToService(entity.Edges.Proxy)
	}
	return out
}

func accountEgressBindingEntityToService(entity *dbent.AccountEgressBinding, runtimeAccountID int64) service.AccountEgressBinding {
	if runtimeAccountID <= 0 {
		runtimeAccountID = entity.AccountID
	}
	out := service.AccountEgressBinding{
		BindingID: service.StableAccountEgressBindingID(runtimeAccountID, entity.RouteID),
		AccountID: runtimeAccountID,
		RouteID:   entity.RouteID,
		Position:  entity.Position,
		IsPrimary: entity.IsPrimary,
		Status:    string(entity.Status),
		CreatedAt: entity.CreatedAt,
		UpdatedAt: entity.UpdatedAt,
	}
	if entity.Edges.Route != nil {
		out.Route = egressRouteEntityToService(entity.Edges.Route)
	}
	return out
}

func uniquePositiveInt64sPreserveOrder(values []int64) ([]int64, bool) {
	out := make([]int64, 0, len(values))
	seen := make(map[int64]struct{}, len(values))
	for _, value := range values {
		if value <= 0 {
			return nil, false
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, false
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, true
}

func uniqueSortedPositiveInt64s(values []int64) []int64 {
	seen := make(map[int64]struct{}, len(values))
	for _, value := range values {
		if value > 0 {
			seen[value] = struct{}{}
		}
	}
	out := make([]int64, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func queryOneRow(ctx context.Context, exec sqlExecutor, query string, args []any, destinations ...any) error {
	rows, err := exec.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return err
		}
		return sql.ErrNoRows
	}
	if err := rows.Scan(destinations...); err != nil {
		return err
	}
	return rows.Err()
}

func containsInt64(values []int64, target int64) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func canonicalInetHost(value string) string {
	value = strings.TrimSpace(value)
	if addr, err := netip.ParseAddr(value); err == nil {
		return addr.Unmap().String()
	}
	if prefix, err := netip.ParsePrefix(value); err == nil {
		return prefix.Addr().Unmap().String()
	}
	return value
}

func egressPoolError(reason string) error {
	return fmt.Errorf("%w: %s", service.ErrEgressPoolInvalid, reason)
}
