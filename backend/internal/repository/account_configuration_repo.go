package repository

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	dbaccount "github.com/Wei-Shaw/sub2api/ent/account"
	dbaccountgroup "github.com/Wei-Shaw/sub2api/ent/accountgroup"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

const accountConfigurationLockRetryLimit = 3

var errAccountConfigurationLockSetChanged = errors.New("account configuration lock set changed")

type accountConfigurationLockPlan struct {
	accountIDs     []int64
	routeIDs       []int64
	proxyIDs       []int64
	targetProxyIDs []int64
}

type proxyShareLockState struct {
	status    string
	expiresAt sql.NullTime
	deletedAt sql.NullTime
}

// UpdateAccountConfiguration is the single durable boundary for an admin PUT.
// Routes are locked before root/shadow accounts; bindings, exact groups, mirror
// columns, revisions and outbox rows are then committed together.
func (r *accountRepository) UpdateAccountConfiguration(
	ctx context.Context,
	mutation service.AccountConfigurationMutation,
) (*service.Account, error) {
	if r == nil || r.client == nil || mutation.Desired == nil || mutation.Desired.ID <= 0 {
		return nil, service.ErrAccountNilInput
	}

	for attempt := 0; attempt < accountConfigurationLockRetryLimit; attempt++ {
		updated, affectedIDs, err := r.updateAccountConfigurationOnce(ctx, mutation)
		if errors.Is(err, errAccountConfigurationLockSetChanged) {
			continue
		}
		if err != nil {
			return nil, translatePersistenceError(err, service.ErrAccountNotFound, nil)
		}
		for _, accountID := range affectedIDs {
			r.syncSchedulerAccountSnapshot(ctx, accountID)
		}
		return updated, nil
	}
	return nil, service.ErrEgressPoolConflict
}

func (r *accountRepository) updateAccountConfigurationOnce(
	ctx context.Context,
	mutation service.AccountConfigurationMutation,
) (*service.Account, []int64, error) {
	tx, err := r.client.Tx(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = tx.Rollback() }()

	plan, err := readAccountConfigurationLockPlan(ctx, tx, mutation)
	if err != nil {
		return nil, nil, err
	}
	lockedProxies, err := lockProxiesForShareInOrder(ctx, tx, plan.proxyIDs)
	if err != nil {
		return nil, nil, err
	}
	if err := validateWritableProxyTargets(plan.targetProxyIDs, lockedProxies, time.Now()); err != nil {
		return nil, nil, err
	}
	if err := lockEgressRoutesInOrder(ctx, tx, plan.routeIDs); err != nil {
		return nil, nil, err
	}
	if err := lockAccountsInOrder(ctx, tx, plan.accountIDs); err != nil {
		return nil, nil, err
	}
	lockedPlan, err := readAccountConfigurationLockPlan(ctx, tx, mutation)
	if err != nil {
		return nil, nil, err
	}
	if !slices.Equal(plan.proxyIDs, lockedPlan.proxyIDs) ||
		!slices.Equal(plan.targetProxyIDs, lockedPlan.targetProxyIDs) ||
		!slices.Equal(plan.routeIDs, lockedPlan.routeIDs) ||
		!slices.Equal(plan.accountIDs, lockedPlan.accountIDs) {
		return nil, nil, errAccountConfigurationLockSetChanged
	}

	txClient := tx.Client()
	currentEntity, err := txClient.Account.Query().Where(dbaccount.IDEQ(mutation.Desired.ID)).Only(ctx)
	if err != nil {
		return nil, nil, err
	}
	current := accountEntityToService(currentEntity)
	effective, err := effectiveAccountConfiguration(current, mutation)
	if err != nil {
		return nil, nil, err
	}
	if err := mutateLockedAccountConfiguration(ctx, txClient, current, effective, mutation); err != nil {
		return nil, nil, err
	}

	if mutation.EgressPool != nil {
		input := *mutation.EgressPool
		input.RouteIDs = append([]int64(nil), mutation.EgressPool.RouteIDs...)
		if err := replaceAccountPoolLockedTx(ctx, tx, mutation.Desired.ID, input); err != nil {
			return nil, nil, err
		}
	} else if mutation.Fields.ProxyID && current.ParentAccountID == nil && !sameOptionalInt64(current.ProxyID, effective.ProxyID) {
		if err := updateRootAndShadowLegacyProxyTx(ctx, tx, mutation.Desired.ID, effective.ProxyID); err != nil {
			return nil, nil, err
		}
	}

	oldGroupIDsByAccount, err := loadAccountGroupIDsForAccountsTx(ctx, tx, plan.accountIDs)
	if err != nil {
		return nil, nil, err
	}
	finalGroupIDs := append([]int64(nil), oldGroupIDsByAccount[mutation.Desired.ID]...)
	if mutation.GroupIDs != nil {
		finalGroupIDs = append([]int64(nil), (*mutation.GroupIDs)...)
		if err := replaceAccountGroupsExactTx(ctx, txClient, mutation.Desired.ID, finalGroupIDs); err != nil {
			return nil, nil, err
		}
	}

	impactedGroupIDs := make([]int64, 0)
	for _, accountID := range plan.accountIDs {
		impactedGroupIDs = append(impactedGroupIDs, oldGroupIDsByAccount[accountID]...)
	}
	impactedGroupIDs = append(impactedGroupIDs, finalGroupIDs...)
	impactedGroupIDs = uniqueSortedPositiveInt64s(impactedGroupIDs)
	if len(plan.accountIDs) == 1 {
		accountID := mutation.Desired.ID
		if err := enqueueSchedulerOutbox(ctx, tx, service.SchedulerOutboxEventAccountChanged, &accountID, nil, buildSchedulerGroupPayload(impactedGroupIDs)); err != nil {
			return nil, nil, err
		}
	} else {
		payload := map[string]any{"account_ids": plan.accountIDs}
		if len(impactedGroupIDs) > 0 {
			payload["group_ids"] = impactedGroupIDs
		}
		if err := enqueueSchedulerOutbox(ctx, tx, service.SchedulerOutboxEventAccountBulkChanged, nil, nil, payload); err != nil {
			return nil, nil, err
		}
	}

	txRepo := newAccountRepositoryWithSQL(txClient, tx, nil)
	updated, err := txRepo.GetByID(ctx, mutation.Desired.ID)
	if err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	return updated, plan.accountIDs, nil
}

func readAccountConfigurationLockPlan(
	ctx context.Context,
	tx *dbent.Tx,
	mutation service.AccountConfigurationMutation,
) (accountConfigurationLockPlan, error) {
	accountID := mutation.Desired.ID
	var parentID, proxyID, fallbackOriginID sql.NullInt64
	rows, err := tx.QueryContext(ctx, `
		SELECT parent_account_id, proxy_id, proxy_fallback_origin_id
		FROM accounts
		WHERE id=$1 AND deleted_at IS NULL`, accountID)
	if err != nil {
		return accountConfigurationLockPlan{}, err
	}
	if !rows.Next() {
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			return accountConfigurationLockPlan{}, rowsErr
		}
		_ = rows.Close()
		return accountConfigurationLockPlan{}, service.ErrAccountNotFound
	}
	if err := rows.Scan(&parentID, &proxyID, &fallbackOriginID); err != nil {
		_ = rows.Close()
		return accountConfigurationLockPlan{}, err
	}
	if err := rows.Close(); err != nil {
		return accountConfigurationLockPlan{}, err
	}

	plan := accountConfigurationLockPlan{accountIDs: []int64{accountID}}
	if proxyID.Valid {
		plan.proxyIDs = append(plan.proxyIDs, proxyID.Int64)
	}
	if fallbackOriginID.Valid {
		plan.proxyIDs = append(plan.proxyIDs, fallbackOriginID.Int64)
	}
	if !parentID.Valid {
		rows, err := tx.QueryContext(ctx, `
			SELECT id, proxy_id, proxy_fallback_origin_id
			FROM accounts
			WHERE (id=$1 OR parent_account_id=$1) AND deleted_at IS NULL
			ORDER BY id`, accountID)
		if err != nil {
			return accountConfigurationLockPlan{}, err
		}
		plan.accountIDs = plan.accountIDs[:0]
		for rows.Next() {
			var id int64
			var accountProxyID, accountFallbackOriginID sql.NullInt64
			if err := rows.Scan(&id, &accountProxyID, &accountFallbackOriginID); err != nil {
				_ = rows.Close()
				return accountConfigurationLockPlan{}, err
			}
			plan.accountIDs = append(plan.accountIDs, id)
			if accountProxyID.Valid {
				plan.proxyIDs = append(plan.proxyIDs, accountProxyID.Int64)
			}
			if accountFallbackOriginID.Valid {
				plan.proxyIDs = append(plan.proxyIDs, accountFallbackOriginID.Int64)
			}
		}
		if err := rows.Close(); err != nil {
			return accountConfigurationLockPlan{}, err
		}
	}

	if mutation.EgressPool != nil {
		plan.routeIDs = append(plan.routeIDs, mutation.EgressPool.RouteIDs...)
		rows, err := tx.QueryContext(ctx, `
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
		mode := strings.TrimSpace(mutation.EgressPool.Mode)
		if mode == "" || mode == service.EgressModePool {
			targetProxyIDs, err := proxyIDsForRouteIDsTx(ctx, tx, mutation.EgressPool.RouteIDs)
			if err != nil {
				return accountConfigurationLockPlan{}, err
			}
			plan.targetProxyIDs = append(plan.targetProxyIDs, targetProxyIDs...)
		} else if mode == service.EgressModeLegacy && proxyID.Valid {
			plan.targetProxyIDs = append(plan.targetProxyIDs, proxyID.Int64)
		}
	}
	if mutation.Fields.ProxyID && mutation.EgressPool == nil && !parentID.Valid {
		if mutation.Desired.ProxyID != nil {
			plan.proxyIDs = append(plan.proxyIDs, *mutation.Desired.ProxyID)
			plan.targetProxyIDs = append(plan.targetProxyIDs, *mutation.Desired.ProxyID)
		}
	}
	plan.targetProxyIDs = uniqueSortedPositiveInt64s(plan.targetProxyIDs)
	plan.proxyIDs = uniqueSortedPositiveInt64s(plan.proxyIDs)
	proxyRouteIDs, err := egressRouteIDsForProxyIDs(ctx, tx, plan.proxyIDs)
	if err != nil {
		return accountConfigurationLockPlan{}, err
	}
	plan.routeIDs = uniqueSortedPositiveInt64s(append(plan.routeIDs, proxyRouteIDs...))
	proxyIDs, err := proxyIDsForRouteIDsTx(ctx, tx, plan.routeIDs)
	if err != nil {
		return accountConfigurationLockPlan{}, err
	}
	plan.proxyIDs = uniqueSortedPositiveInt64s(append(plan.proxyIDs, proxyIDs...))
	plan.proxyIDs = uniqueSortedPositiveInt64s(append(plan.proxyIDs, plan.targetProxyIDs...))
	return plan, nil
}

func lockProxiesForShareInOrder(ctx context.Context, exec sqlExecutor, proxyIDs []int64) (map[int64]proxyShareLockState, error) {
	proxyIDs = uniqueSortedPositiveInt64s(proxyIDs)
	locked := make(map[int64]proxyShareLockState, len(proxyIDs))
	if len(proxyIDs) == 0 {
		return locked, nil
	}
	rows, err := exec.QueryContext(ctx, `
			SELECT id, status, expires_at, deleted_at
			FROM proxies
			WHERE id=ANY($1)
			ORDER BY id
			FOR SHARE`, pq.Array(proxyIDs))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id int64
		var state proxyShareLockState
		if err := rows.Scan(&id, &state.status, &state.expiresAt, &state.deletedAt); err != nil {
			_ = rows.Close()
			return nil, err
		}
		locked[id] = state
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return locked, nil
}

func validateWritableProxyTargets(targetProxyIDs []int64, locked map[int64]proxyShareLockState, now time.Time) error {
	for _, proxyID := range uniqueSortedPositiveInt64s(targetProxyIDs) {
		state, ok := locked[proxyID]
		if !ok || state.deletedAt.Valid {
			return service.ErrProxyNotFound
		}
		if state.status != service.StatusActive || (state.expiresAt.Valid && !state.expiresAt.Time.After(now)) {
			return service.ErrEgressPoolInvalid
		}
	}
	return nil
}

func lockProxiesForNoKeyUpdateInOrder(ctx context.Context, exec sqlExecutor, proxyIDs []int64) (int, error) {
	proxyIDs = uniqueSortedPositiveInt64s(proxyIDs)
	if len(proxyIDs) == 0 {
		return 0, nil
	}
	rows, err := exec.QueryContext(ctx, `
		SELECT id
		FROM proxies
		WHERE id=ANY($1) AND deleted_at IS NULL
		ORDER BY id
		FOR NO KEY UPDATE`, pq.Array(proxyIDs))
	if err != nil {
		return 0, err
	}
	locked := 0
	for rows.Next() {
		locked++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, err
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	return locked, nil
}

func proxyIDsForRouteIDsTx(ctx context.Context, exec sqlExecutor, routeIDs []int64) ([]int64, error) {
	routeIDs = uniqueSortedPositiveInt64s(routeIDs)
	if len(routeIDs) == 0 {
		return nil, nil
	}
	rows, err := exec.QueryContext(ctx, `
		SELECT DISTINCT proxy_id
		FROM egress_routes
		WHERE id=ANY($1) AND proxy_id IS NOT NULL
		ORDER BY proxy_id`, pq.Array(routeIDs))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	proxyIDs := make([]int64, 0, len(routeIDs))
	for rows.Next() {
		var proxyID int64
		if err := rows.Scan(&proxyID); err != nil {
			return nil, err
		}
		proxyIDs = append(proxyIDs, proxyID)
	}
	return proxyIDs, rows.Err()
}

func lockEgressRoutesInOrder(ctx context.Context, exec sqlExecutor, routeIDs []int64) error {
	if len(routeIDs) == 0 {
		return nil
	}
	rows, err := exec.QueryContext(ctx, `
		SELECT id
		FROM egress_routes
		WHERE id=ANY($1)
		ORDER BY id
		FOR SHARE`, pq.Array(routeIDs))
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	return rows.Close()
}

func lockEgressRoutesForUpdateInOrder(ctx context.Context, exec sqlExecutor, routeIDs []int64) error {
	if len(routeIDs) == 0 {
		return nil
	}
	rows, err := exec.QueryContext(ctx, `
		SELECT id
		FROM egress_routes
		WHERE id=ANY($1)
		ORDER BY id
		FOR UPDATE`, pq.Array(routeIDs))
	if err != nil {
		return err
	}
	for rows.Next() {
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return err
	}
	return rows.Close()
}

func lockAccountsInOrder(ctx context.Context, exec sqlExecutor, accountIDs []int64) error {
	rows, err := exec.QueryContext(ctx, `
		SELECT id
		FROM accounts
		WHERE id=ANY($1) AND deleted_at IS NULL
		ORDER BY id
		FOR UPDATE`, pq.Array(accountIDs))
	if err != nil {
		return err
	}
	locked := 0
	for rows.Next() {
		locked++
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if locked != len(accountIDs) {
		return errAccountConfigurationLockSetChanged
	}
	return nil
}

func effectiveAccountConfiguration(
	current *service.Account,
	mutation service.AccountConfigurationMutation,
) (*service.Account, error) {
	if current == nil || mutation.Desired == nil {
		return nil, service.ErrAccountNotFound
	}
	desired := mutation.Desired
	effective := *current
	fields := mutation.Fields
	if fields.Name {
		effective.Name = desired.Name
	}
	if fields.Notes {
		effective.Notes = desired.Notes
	}
	if fields.Type {
		effective.Type = desired.Type
	}
	if fields.Credentials {
		if current.IsCredentialShadow() {
			effective.Credentials = copyJSONMap(desired.Credentials)
		} else {
			effective.Credentials = service.MergePreservingSensitiveCreds(current.Credentials, mutation.CredentialsPatch)
			if err := service.NormalizeHeaderOverrideCredentials(effective.Credentials); err != nil {
				return nil, err
			}
			effective.Credentials = service.SanitizeStoredCredentials(effective.Platform, effective.Credentials)
		}
	}
	if fields.Extra {
		effective.Extra = copyJSONMap(desired.Extra)
	}
	if fields.ProxyID {
		effective.ProxyID = copyOptionalInt64(desired.ProxyID)
	}
	if fields.Concurrency {
		effective.Concurrency = desired.Concurrency
	}
	if fields.Priority {
		effective.Priority = desired.Priority
	}
	if fields.RateMultiplier {
		effective.RateMultiplier = desired.RateMultiplier
	}
	if fields.LoadFactor {
		effective.LoadFactor = desired.LoadFactor
	}
	if fields.Status {
		effective.Status = desired.Status
	}
	if fields.ExpiresAt {
		effective.ExpiresAt = desired.ExpiresAt
	}
	if fields.AutoPauseOnExpired {
		effective.AutoPauseOnExpired = desired.AutoPauseOnExpired
	}
	return &effective, nil
}

func mutateLockedAccountConfiguration(
	ctx context.Context,
	client *dbent.Client,
	current *service.Account,
	effective *service.Account,
	mutation service.AccountConfigurationMutation,
) error {
	fields := mutation.Fields
	needsExtra := fields.Extra || fields.Credentials || fields.Type || fields.ProxyID ||
		mutation.ProbeEnabled != nil || mutation.RateSyncEnabled != nil
	if needsExtra {
		extra, err := lockAndMergeAccountProbeExtra(ctx, client, effective, mutation.ProbeEnabled, mutation.RateSyncEnabled)
		if err != nil {
			return err
		}
		effective.Extra = extra
	}

	builder := client.Account.UpdateOneID(effective.ID)
	changed := false
	if fields.Name {
		builder.SetName(effective.Name)
		changed = true
	}
	if fields.Notes {
		builder.SetNillableNotes(effective.Notes)
		if effective.Notes == nil {
			builder.ClearNotes()
		}
		changed = true
	}
	if fields.Type {
		builder.SetType(effective.Type)
		changed = true
	}
	if fields.Credentials {
		builder.SetCredentials(normalizeJSONMap(effective.Credentials))
		changed = true
	}
	if needsExtra {
		builder.SetExtra(normalizeJSONMap(effective.Extra))
		changed = true
	}
	if fields.Concurrency && mutation.EgressPool == nil {
		builder.SetConcurrency(effective.Concurrency)
		changed = true
	}
	if fields.Priority {
		builder.SetPriority(effective.Priority)
		changed = true
	}
	if fields.RateMultiplier && effective.RateMultiplier != nil {
		builder.SetRateMultiplier(*effective.RateMultiplier)
		changed = true
	}
	if fields.LoadFactor {
		if effective.LoadFactor == nil {
			builder.ClearLoadFactor()
		} else {
			builder.SetLoadFactor(*effective.LoadFactor)
		}
		changed = true
	}
	if fields.Status {
		builder.SetStatus(effective.Status)
		if effective.Status == service.StatusError {
			builder.SetSchedulable(false)
		}
		changed = true
	}
	if fields.ExpiresAt {
		if effective.ExpiresAt == nil {
			builder.ClearExpiresAt()
		} else {
			builder.SetExpiresAt(*effective.ExpiresAt)
		}
		changed = true
	}
	if fields.AutoPauseOnExpired {
		builder.SetAutoPauseOnExpired(effective.AutoPauseOnExpired)
		changed = true
	}
	// Proxy writes are handled by updateRootAndShadowLegacyProxyTx so root and
	// shadows receive the same revision fence while already locked.
	_ = current
	if !changed {
		return nil
	}
	_, err := builder.Save(ctx)
	return err
}

func updateRootAndShadowLegacyProxyTx(ctx context.Context, exec sqlExecutor, rootID int64, proxyID *int64) error {
	var value any
	if proxyID != nil {
		value = *proxyID
	}
	_, err := exec.ExecContext(ctx, `
		UPDATE accounts
		SET proxy_id=$2, proxy_fallback_origin_id=NULL,
			egress_revision=egress_revision+1, updated_at=NOW()
		WHERE (id=$1 OR parent_account_id=$1) AND deleted_at IS NULL`, rootID, value)
	return err
}

func loadAccountGroupIDsForAccountsTx(ctx context.Context, exec sqlExecutor, accountIDs []int64) (map[int64][]int64, error) {
	result := make(map[int64][]int64, len(accountIDs))
	rows, err := exec.QueryContext(ctx, `
		SELECT account_id, group_id
		FROM account_groups
		WHERE account_id=ANY($1)
		ORDER BY account_id, priority, group_id`, pq.Array(accountIDs))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var accountID, groupID int64
		if err := rows.Scan(&accountID, &groupID); err != nil {
			return nil, err
		}
		result[accountID] = append(result[accountID], groupID)
	}
	return result, rows.Err()
}

func replaceAccountGroupsExactTx(ctx context.Context, client *dbent.Client, accountID int64, groupIDs []int64) error {
	if _, err := client.AccountGroup.Delete().Where(dbaccountgroup.AccountIDEQ(accountID)).Exec(ctx); err != nil {
		return err
	}
	if len(groupIDs) == 0 {
		return nil
	}
	builders := make([]*dbent.AccountGroupCreate, 0, len(groupIDs))
	for index, groupID := range groupIDs {
		builders = append(builders, client.AccountGroup.Create().
			SetAccountID(accountID).
			SetGroupID(groupID).
			SetPriority(index+1))
	}
	_, err := client.AccountGroup.CreateBulk(builders...).Save(ctx)
	return err
}

func sameOptionalInt64(left, right *int64) bool {
	return (left == nil && right == nil) || (left != nil && right != nil && *left == *right)
}

func copyOptionalInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
