package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/ent/proxy"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"

	entsql "entgo.io/ent/dialect/sql"
)

// sqlQuerier 已替换为 sqlExecutor（定义在 group_repo.go），
// proxyRepository 使用同一接口以支持 ExecContext。
type proxyRepository struct {
	client *dbent.Client
	sql    sqlExecutor
}

const proxyProbeOutboxAccountChunkSize = 500

var errProxyUpdateLockSetChanged = fmt.Errorf(
	"proxy update lock set changed; rollback and retry: %w",
	service.ErrEgressPoolConflict,
)

func NewProxyRepository(client *dbent.Client, sqlDB *sql.DB) service.ProxyRepository {
	return newProxyRepositoryWithSQL(client, sqlDB)
}

func newProxyRepositoryWithSQL(client *dbent.Client, sqlq sqlExecutor) *proxyRepository {
	return &proxyRepository{client: client, sql: sqlq}
}

func (r *proxyRepository) beginProxyLifecycleTransaction(ctx context.Context) (context.Context, *dbent.Client, sqlExecutor, *dbent.Tx, bool, error) {
	client := clientFromContext(ctx, r.client)
	if client == nil {
		return nil, nil, nil, nil, false, errors.New("proxy lifecycle client is unavailable")
	}
	if dbent.TxFromContext(ctx) != nil {
		return ctx, client, client, nil, false, nil
	}
	tx, err := client.Tx(ctx)
	if errors.Is(err, dbent.ErrTxStarted) {
		return ctx, client, client, nil, false, nil
	}
	if err != nil {
		return nil, nil, nil, nil, false, err
	}
	txCtx := dbent.NewTxContext(ctx, tx)
	txClient := tx.Client()
	return txCtx, txClient, txClient, tx, true, nil
}

func (r *proxyRepository) Create(ctx context.Context, proxyIn *service.Proxy) error {
	if proxyIn == nil {
		return service.ErrEgressRouteInvalid
	}
	txCtx, client, exec, tx, ownsTx, err := r.beginProxyLifecycleTransaction(ctx)
	if err != nil {
		return err
	}
	if ownsTx {
		defer func() { _ = tx.Rollback() }()
	}
	builder := client.Proxy.Create().
		SetName(proxyIn.Name).
		SetProtocol(proxyIn.Protocol).
		SetHost(proxyIn.Host).
		SetPort(proxyIn.Port).
		SetStatus(proxyIn.Status).
		SetFallbackMode(proxyIn.FallbackMode).
		SetExpiryWarnDays(proxyIn.ExpiryWarnDays)
	if proxyIn.Username != "" {
		builder.SetUsername(proxyIn.Username)
	}
	if proxyIn.Password != "" {
		builder.SetPassword(proxyIn.Password)
	}
	if proxyIn.ExpiresAt != nil {
		builder.SetExpiresAt(*proxyIn.ExpiresAt)
	}
	if proxyIn.BackupProxyID != nil {
		builder.SetBackupProxyID(*proxyIn.BackupProxyID)
	}

	created, err := builder.Save(txCtx)
	if err != nil {
		return err
	}
	if err := syncProxyEgressRouteTx(txCtx, exec, created.ID, proxyLifecycleRouteState(proxyIn, time.Now()), true); err != nil {
		return err
	}
	if ownsTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	applyProxyEntityToService(proxyIn, created)
	return nil
}

func (r *proxyRepository) GetByID(ctx context.Context, id int64) (*service.Proxy, error) {
	m, err := r.client.Proxy.Get(ctx, id)
	if err != nil {
		if dbent.IsNotFound(err) {
			return nil, service.ErrProxyNotFound
		}
		return nil, err
	}
	return proxyEntityToService(m), nil
}

func (r *proxyRepository) ListByIDs(ctx context.Context, ids []int64) ([]service.Proxy, error) {
	if len(ids) == 0 {
		return []service.Proxy{}, nil
	}

	proxies, err := r.client.Proxy.Query().
		Where(proxy.IDIn(ids...)).
		All(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]service.Proxy, 0, len(proxies))
	for i := range proxies {
		out = append(out, *proxyEntityToService(proxies[i]))
	}
	return out, nil
}

func (r *proxyRepository) Update(ctx context.Context, proxyIn *service.Proxy) error {
	if proxyIn == nil {
		return service.ErrEgressRouteInvalid
	}
	for attempt := 0; attempt < accountConfigurationLockRetryLimit; attempt++ {
		updated, ownsTx, err := r.updateProxyOnce(ctx, proxyIn)
		if errors.Is(err, errProxyUpdateLockSetChanged) && ownsTx {
			continue
		}
		if err != nil {
			return err
		}
		applyProxyEntityToService(proxyIn, updated)
		return nil
	}
	return service.ErrEgressPoolConflict
}

func (r *proxyRepository) updateProxyOnce(ctx context.Context, proxyIn *service.Proxy) (*dbent.Proxy, bool, error) {
	txCtx, client, _, tx, ownsTx, err := r.beginProxyLifecycleTransaction(ctx)
	if err != nil {
		return nil, false, err
	}
	if ownsTx {
		defer func() { _ = tx.Rollback() }()
	}

	updated, err := updateProxyAndInvalidateProbeSnapshots(txCtx, client, proxyIn)
	if err != nil {
		return nil, ownsTx, err
	}
	if ownsTx {
		if err := tx.Commit(); err != nil {
			return nil, true, err
		}
	}
	return updated, ownsTx, nil
}

type proxyProbeIdentity struct {
	protocol        string
	host            string
	port            int
	username        string
	password        string
	status          string
	hasExpiresAt    bool
	expiresAtUnixNs int64
}

type proxyUpdateLockState struct {
	identity      proxyProbeIdentity
	backupProxyID sql.NullInt64
	deletedAt     sql.NullTime
}

type proxyUpdateLockPlan struct {
	proxyIDs []int64
	states   map[int64]proxyUpdateLockState
}

func proxyProbeIdentityFromService(proxyIn *service.Proxy) proxyProbeIdentity {
	identity := proxyProbeIdentity{
		protocol: proxyIn.Protocol,
		host:     proxyIn.Host,
		port:     proxyIn.Port,
		username: proxyIn.Username,
		password: proxyIn.Password,
		status:   proxyIn.Status,
	}
	if proxyIn.ExpiresAt != nil {
		identity.hasExpiresAt = true
		identity.expiresAtUnixNs = proxyIn.ExpiresAt.UnixNano()
	}
	return identity
}

func updateProxyAndInvalidateProbeSnapshots(ctx context.Context, client *dbent.Client, proxyIn *service.Proxy) (*dbent.Proxy, error) {
	preLockPlan, err := readProxyUpdateLockPlan(ctx, client, proxyIn.ID, proxyIn.BackupProxyID)
	if err != nil {
		return nil, err
	}
	locked, err := lockProxyUpdateRowsInOrder(ctx, client, preLockPlan.proxyIDs)
	if err != nil {
		return nil, err
	}
	if locked != len(preLockPlan.proxyIDs) {
		return nil, errProxyUpdateLockSetChanged
	}
	lockedPlan, err := readProxyUpdateLockPlan(ctx, client, proxyIn.ID, proxyIn.BackupProxyID)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(preLockPlan.proxyIDs, lockedPlan.proxyIDs) {
		return nil, errProxyUpdateLockSetChanged
	}
	current, err := validateProxyUpdateLockPlan(lockedPlan, proxyIn.ID, proxyIn.BackupProxyID, time.Now())
	if err != nil {
		return nil, err
	}
	currentIdentity := current.identity
	builder := client.Proxy.UpdateOneID(proxyIn.ID).
		SetName(proxyIn.Name).
		SetProtocol(proxyIn.Protocol).
		SetHost(proxyIn.Host).
		SetPort(proxyIn.Port).
		SetStatus(proxyIn.Status).
		SetFallbackMode(proxyIn.FallbackMode).
		SetExpiryWarnDays(proxyIn.ExpiryWarnDays)
	if proxyIn.Username != "" {
		builder.SetUsername(proxyIn.Username)
	} else {
		builder.ClearUsername()
	}
	if proxyIn.Password != "" {
		builder.SetPassword(proxyIn.Password)
	} else {
		builder.ClearPassword()
	}
	if proxyIn.ExpiresAt != nil {
		builder.SetExpiresAt(*proxyIn.ExpiresAt)
	} else {
		builder.ClearExpiresAt()
	}
	if proxyIn.BackupProxyID != nil {
		builder.SetBackupProxyID(*proxyIn.BackupProxyID)
	} else {
		builder.ClearBackupProxyID()
	}

	updated, err := builder.Save(ctx)
	if dbent.IsNotFound(err) {
		return nil, service.ErrProxyNotFound
	}
	if err != nil {
		return nil, err
	}
	if currentIdentity == proxyProbeIdentityFromService(proxyIn) {
		return updated, nil
	}
	routeID, err := syncProxyEgressRouteStateTx(ctx, client, proxyIn.ID, proxyLifecycleRouteState(proxyIn, time.Now()), true)
	if err != nil {
		return nil, err
	}
	poolAccountIDs, err := accountIDsForRouteTx(ctx, client, routeID)
	if err != nil {
		return nil, err
	}
	probeAccountIDs, err := proxyProbeSnapshotAccountIDs(ctx, client, proxyIn.ID)
	if err != nil {
		return nil, err
	}
	if err := lockAccountsInOrder(ctx, client, sortedUniqueAccountIDs(append(append([]int64(nil), poolAccountIDs...), probeAccountIDs...))); err != nil {
		return nil, err
	}
	if err := invalidateLockedAccountsTx(ctx, client, poolAccountIDs); err != nil {
		return nil, err
	}
	accountIDs, err := invalidateProxyProbeSnapshots(ctx, client, proxyIn.ID)
	if err != nil {
		return nil, err
	}
	if err := enqueueProxyProbeAccountChanges(ctx, client, accountIDs); err != nil {
		return nil, err
	}
	return updated, nil
}

func readProxyUpdateLockPlan(ctx context.Context, exec sqlExecutor, proxyID int64, newBackupProxyID *int64) (proxyUpdateLockPlan, error) {
	var target any
	if newBackupProxyID != nil {
		target = *newBackupProxyID
	}
	rows, err := exec.QueryContext(ctx, `
		WITH current AS (
			SELECT backup_proxy_id
			FROM proxies
			WHERE id=$1
		), affected AS (
			SELECT candidate.id
			FROM current
			JOIN proxies candidate ON candidate.id=$1
				OR candidate.id=current.backup_proxy_id
				OR candidate.id=$2
				OR candidate.backup_proxy_id=$1
		)
		SELECT candidate.id, candidate.protocol, candidate.host, candidate.port,
			COALESCE(candidate.username, ''), COALESCE(candidate.password, ''),
			candidate.status, candidate.expires_at, candidate.backup_proxy_id, candidate.deleted_at
		FROM proxies candidate
		JOIN affected ON affected.id=candidate.id
		ORDER BY candidate.id`, proxyID, target)
	if err != nil {
		return proxyUpdateLockPlan{}, err
	}
	defer func() { _ = rows.Close() }()
	plan := proxyUpdateLockPlan{states: make(map[int64]proxyUpdateLockState)}
	for rows.Next() {
		var id int64
		var expiresAt sql.NullTime
		var state proxyUpdateLockState
		if err := rows.Scan(
			&id, &state.identity.protocol, &state.identity.host, &state.identity.port,
			&state.identity.username, &state.identity.password, &state.identity.status,
			&expiresAt, &state.backupProxyID, &state.deletedAt,
		); err != nil {
			return proxyUpdateLockPlan{}, err
		}
		if expiresAt.Valid {
			state.identity.hasExpiresAt = true
			state.identity.expiresAtUnixNs = expiresAt.Time.UnixNano()
		}
		plan.proxyIDs = append(plan.proxyIDs, id)
		plan.states[id] = state
	}
	if err := rows.Err(); err != nil {
		return proxyUpdateLockPlan{}, err
	}
	return plan, nil
}

func lockProxyUpdateRowsInOrder(ctx context.Context, exec sqlExecutor, proxyIDs []int64) (int, error) {
	proxyIDs = uniqueSortedPositiveInt64s(proxyIDs)
	if len(proxyIDs) == 0 {
		return 0, nil
	}
	rows, err := exec.QueryContext(ctx, `
		SELECT id
		FROM proxies
		WHERE id=ANY($1)
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

func validateProxyUpdateLockPlan(
	plan proxyUpdateLockPlan,
	proxyID int64,
	newBackupProxyID *int64,
	now time.Time,
) (proxyUpdateLockState, error) {
	current, ok := plan.states[proxyID]
	if !ok || current.deletedAt.Valid {
		return proxyUpdateLockState{}, service.ErrProxyNotFound
	}
	if newBackupProxyID == nil {
		return current, nil
	}
	if *newBackupProxyID == proxyID {
		return proxyUpdateLockState{}, service.ErrEgressRouteInvalid
	}
	target, ok := plan.states[*newBackupProxyID]
	if !ok || target.deletedAt.Valid {
		return proxyUpdateLockState{}, service.ErrProxyNotFound
	}
	if target.identity.status != service.StatusActive ||
		(target.identity.hasExpiresAt && target.identity.expiresAtUnixNs <= now.UnixNano()) {
		return proxyUpdateLockState{}, service.ErrEgressPoolInvalid
	}
	return current, nil
}

func proxyLifecycleRouteState(proxyIn *service.Proxy, now time.Time) string {
	if proxyIn == nil || proxyIn.Status != service.StatusActive {
		return service.EgressRouteStateInactive
	}
	if proxyIn.IsExpired(now) {
		return service.EgressRouteStateExpired
	}
	return service.EgressRouteStatePendingVerification
}

func invalidateProxyProbeSnapshots(ctx context.Context, exec sqlExecutor, proxyID int64) ([]int64, error) {
	rows, err := exec.QueryContext(ctx, `
		UPDATE accounts
		SET extra = COALESCE(extra, '{}'::jsonb)
				- 'upstream_billing_probe'
				- 'ollama_cloud_usage_snapshot',
			updated_at = NOW()
		WHERE proxy_id = $1
			AND type = 'apikey'
			AND (
				(extra ? 'upstream_billing_probe'
					AND extra -> 'upstream_billing_probe' <> 'null'::jsonb)
				OR (platform IN ('openai', 'anthropic')
					AND extra ? 'ollama_cloud_usage_snapshot'
					AND extra -> 'ollama_cloud_usage_snapshot' <> 'null'::jsonb)
			)
			AND deleted_at IS NULL
		RETURNING id
	`, proxyID)
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return accountIDs, nil
}

func proxyProbeSnapshotAccountIDs(ctx context.Context, exec sqlExecutor, proxyID int64) ([]int64, error) {
	rows, err := exec.QueryContext(ctx, `
		SELECT id
		FROM accounts
		WHERE proxy_id = $1
			AND type = 'apikey'
			AND (
				(extra ? 'upstream_billing_probe'
					AND extra -> 'upstream_billing_probe' <> 'null'::jsonb)
				OR (platform IN ('openai', 'anthropic')
					AND extra ? 'ollama_cloud_usage_snapshot'
					AND extra -> 'ollama_cloud_usage_snapshot' <> 'null'::jsonb)
			)
			AND deleted_at IS NULL
		ORDER BY id`, proxyID)
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

func enqueueProxyProbeAccountChanges(ctx context.Context, exec sqlExecutor, accountIDs []int64) error {
	accountIDs = sortedUniqueAccountIDs(accountIDs)
	for start := 0; start < len(accountIDs); start += proxyProbeOutboxAccountChunkSize {
		end := start + proxyProbeOutboxAccountChunkSize
		if end > len(accountIDs) {
			end = len(accountIDs)
		}
		payload := map[string]any{"account_ids": accountIDs[start:end]}
		if err := enqueueSchedulerOutbox(ctx, exec, service.SchedulerOutboxEventAccountBulkChanged, nil, nil, payload); err != nil {
			return err
		}
	}
	return nil
}

func (r *proxyRepository) Delete(ctx context.Context, id int64) error {
	if id <= 0 {
		return service.ErrProxyNotFound
	}
	txCtx, client, exec, tx, ownsTx, err := r.beginProxyLifecycleTransaction(ctx)
	if err != nil {
		return err
	}
	if ownsTx {
		defer func() { _ = tx.Rollback() }()
	}
	locked, err := lockProxiesForNoKeyUpdateInOrder(txCtx, exec, []int64{id})
	if err != nil {
		return err
	}
	if locked != 1 {
		return service.ErrProxyNotFound
	}
	fallbackReferences, err := countLiveProxyFallbackReferences(txCtx, exec, id)
	if err != nil {
		return err
	}
	if fallbackReferences > 0 {
		return service.ErrProxyInUse
	}
	routeIDs, err := egressRouteIDsForProxyIDs(txCtx, exec, []int64{id})
	if err != nil {
		return err
	}
	if err := lockEgressRoutesForUpdateInOrder(txCtx, exec, routeIDs); err != nil {
		return err
	}
	accountIDs, err := proxyReferencedAccountIDs(txCtx, exec, id)
	if err != nil {
		return err
	}
	if err := lockAccountsInOrder(txCtx, exec, accountIDs); err != nil {
		return err
	}
	count, err := countProxyAccountReferences(txCtx, exec, id)
	if err != nil {
		return err
	}
	if count > 0 {
		return service.ErrProxyInUse
	}
	deleted, err := client.Proxy.Delete().Where(proxy.IDEQ(id)).Exec(txCtx)
	if err != nil {
		return err
	}
	if deleted == 0 {
		return service.ErrProxyNotFound
	}
	if err := syncProxyEgressRouteTx(txCtx, exec, id, service.EgressRouteStateRetired, false); err != nil {
		return err
	}
	if ownsTx {
		return tx.Commit()
	}
	return nil
}

func (r *proxyRepository) List(ctx context.Context, params pagination.PaginationParams) ([]service.Proxy, *pagination.PaginationResult, error) {
	return r.ListWithFilters(ctx, params, "", "", "")
}

// ListWithFilters lists proxies with optional filtering by protocol, status, and search query
func (r *proxyRepository) ListWithFilters(ctx context.Context, params pagination.PaginationParams, protocol, status, search string) ([]service.Proxy, *pagination.PaginationResult, error) {
	q := r.client.Proxy.Query()
	if protocol != "" {
		q = q.Where(proxy.ProtocolEQ(protocol))
	}
	if status != "" {
		q = q.Where(proxy.StatusEQ(status))
	}
	if search != "" {
		q = q.Where(proxy.NameContainsFold(search))
	}

	total, err := q.Count(ctx)
	if err != nil {
		return nil, nil, err
	}

	proxiesQuery := q.
		Offset(params.Offset()).
		Limit(params.Limit())
	for _, order := range proxyListOrder(params) {
		proxiesQuery = proxiesQuery.Order(order)
	}

	proxies, err := proxiesQuery.All(ctx)
	if err != nil {
		return nil, nil, err
	}

	outProxies := make([]service.Proxy, 0, len(proxies))
	for i := range proxies {
		outProxies = append(outProxies, *proxyEntityToService(proxies[i]))
	}

	return outProxies, paginationResultFromTotal(int64(total), params), nil
}

// ListWithFiltersAndAccountCount lists proxies with filters and includes account count per proxy
func (r *proxyRepository) ListWithFiltersAndAccountCount(ctx context.Context, params pagination.PaginationParams, protocol, status, search string) ([]service.ProxyWithAccountCount, *pagination.PaginationResult, error) {
	q := r.client.Proxy.Query()
	if protocol != "" {
		q = q.Where(proxy.ProtocolEQ(protocol))
	}
	if status != "" {
		q = q.Where(proxy.StatusEQ(status))
	}
	if search != "" {
		q = q.Where(proxy.NameContainsFold(search))
	}

	total, err := q.Count(ctx)
	if err != nil {
		return nil, nil, err
	}

	if strings.EqualFold(strings.TrimSpace(params.SortBy), "account_count") {
		return r.listWithAccountCountSort(ctx, q, params, total)
	}

	proxiesQuery := q.
		Offset(params.Offset()).
		Limit(params.Limit())
	for _, order := range proxyListOrder(params) {
		proxiesQuery = proxiesQuery.Order(order)
	}

	proxies, err := proxiesQuery.All(ctx)
	if err != nil {
		return nil, nil, err
	}

	return r.buildProxyWithAccountCountResult(ctx, proxies, params, int64(total))
}

func (r *proxyRepository) listWithAccountCountSort(ctx context.Context, q *dbent.ProxyQuery, params pagination.PaginationParams, total int) ([]service.ProxyWithAccountCount, *pagination.PaginationResult, error) {
	proxies, err := q.
		Order(dbent.Desc(proxy.FieldID)).
		All(ctx)
	if err != nil {
		return nil, nil, err
	}

	result, _, err := r.buildProxyWithAccountCountResult(ctx, proxies, params, int64(total))
	if err != nil {
		return nil, nil, err
	}

	sortOrder := params.NormalizedSortOrder(pagination.SortOrderDesc)
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].AccountCount == result[j].AccountCount {
			return result[i].ID > result[j].ID
		}
		if sortOrder == pagination.SortOrderAsc {
			return result[i].AccountCount < result[j].AccountCount
		}
		return result[i].AccountCount > result[j].AccountCount
	})

	return paginateSlice(result, params), paginationResultFromTotal(int64(total), params), nil
}

func (r *proxyRepository) buildProxyWithAccountCountResult(ctx context.Context, proxies []*dbent.Proxy, params pagination.PaginationParams, total int64) ([]service.ProxyWithAccountCount, *pagination.PaginationResult, error) {
	counts, err := r.GetAccountCountsForProxies(ctx)
	if err != nil {
		return nil, nil, err
	}

	result := make([]service.ProxyWithAccountCount, 0, len(proxies))
	for i := range proxies {
		proxyOut := proxyEntityToService(proxies[i])
		if proxyOut == nil {
			continue
		}
		result = append(result, service.ProxyWithAccountCount{
			Proxy:        *proxyOut,
			AccountCount: counts[proxyOut.ID],
		})
	}

	return result, paginationResultFromTotal(total, params), nil
}

func proxyListOrder(params pagination.PaginationParams) []func(*entsql.Selector) {
	sortBy := strings.ToLower(strings.TrimSpace(params.SortBy))
	sortOrder := params.NormalizedSortOrder(pagination.SortOrderDesc)

	var field string
	switch sortBy {
	case "name":
		field = proxy.FieldName
	case "protocol":
		field = proxy.FieldProtocol
	case "status":
		field = proxy.FieldStatus
	case "created_at":
		field = proxy.FieldCreatedAt
	case "expiry":
		// expires_at 可空(NULL=永不过期)。不写显式 NULLS:
		// dbent.Asc/Desc 不带 NULLS 子句,继承 PG 默认
		// (ASC→NULLS LAST、DESC→NULLS FIRST),即 NULL 视为最晚——
		// 升序垫底、降序置顶。
		field = proxy.FieldExpiresAt
	default:
		field = proxy.FieldID
	}

	if sortOrder == pagination.SortOrderAsc {
		return []func(*entsql.Selector){dbent.Asc(field), dbent.Asc(proxy.FieldID)}
	}
	return []func(*entsql.Selector){dbent.Desc(field), dbent.Desc(proxy.FieldID)}
}

func (r *proxyRepository) ListActive(ctx context.Context) ([]service.Proxy, error) {
	proxies, err := r.client.Proxy.Query().
		Where(proxy.StatusEQ(service.StatusActive)).
		All(ctx)
	if err != nil {
		return nil, err
	}
	outProxies := make([]service.Proxy, 0, len(proxies))
	for i := range proxies {
		outProxies = append(outProxies, *proxyEntityToService(proxies[i]))
	}
	return outProxies, nil
}

// ExistsByHostPortAuth checks if a proxy with the same host, port, username, and password exists
func (r *proxyRepository) ExistsByHostPortAuth(ctx context.Context, host string, port int, username, password string) (bool, error) {
	q := r.client.Proxy.Query().
		Where(proxy.HostEQ(host), proxy.PortEQ(port))

	if username == "" {
		q = q.Where(proxy.Or(proxy.UsernameIsNil(), proxy.UsernameEQ("")))
	} else {
		q = q.Where(proxy.UsernameEQ(username))
	}
	if password == "" {
		q = q.Where(proxy.Or(proxy.PasswordIsNil(), proxy.PasswordEQ("")))
	} else {
		q = q.Where(proxy.PasswordEQ(password))
	}

	count, err := q.Count(ctx)
	return count > 0, err
}

// CountAccountsByProxyID returns the number of accounts using a specific proxy
func (r *proxyRepository) CountAccountsByProxyID(ctx context.Context, proxyID int64) (int64, error) {
	return countProxyAccountReferences(ctx, r.sql, proxyID)
}

func countProxyAccountReferences(ctx context.Context, exec sqlExecutor, proxyID int64) (int64, error) {
	if exec == nil || proxyID <= 0 {
		return 0, nil
	}
	var count int64
	err := scanSingleRow(ctx, exec, `
		SELECT COUNT(*)
		FROM (
			SELECT a.id
			FROM accounts a
			WHERE a.proxy_id=$1 AND a.deleted_at IS NULL
			UNION
			SELECT a.id
			FROM account_egress_bindings b
			JOIN egress_routes er ON er.id=b.route_id
			JOIN accounts a ON a.id=b.account_id
			WHERE er.proxy_id=$1
				AND a.deleted_at IS NULL
				AND a.platform=$2
				AND a.egress_mode=$3
		) referenced_accounts`, []any{proxyID, service.PlatformOpenAI, service.EgressModePool}, &count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func countLiveProxyFallbackReferences(ctx context.Context, exec sqlExecutor, proxyID int64) (int64, error) {
	if exec == nil || proxyID <= 0 {
		return 0, nil
	}
	var count int64
	err := scanSingleRow(ctx, exec, `
		SELECT COUNT(*)
		FROM proxies
		WHERE deleted_at IS NULL
			AND ((id=$1 AND backup_proxy_id IS NOT NULL) OR backup_proxy_id=$1)`,
		[]any{proxyID}, &count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func proxyReferencedAccountIDs(ctx context.Context, exec sqlExecutor, proxyID int64) ([]int64, error) {
	if exec == nil || proxyID <= 0 {
		return nil, nil
	}
	rows, err := exec.QueryContext(ctx, `
		SELECT id
		FROM (
			SELECT account.id
			FROM accounts account
			WHERE account.proxy_id=$1 AND account.deleted_at IS NULL
			UNION
			SELECT account.id
			FROM account_egress_bindings binding
			JOIN egress_routes route ON route.id=binding.route_id
			JOIN accounts account ON account.id=binding.account_id
			WHERE route.proxy_id=$1
				AND account.deleted_at IS NULL
				AND account.platform=$2
				AND account.egress_mode=$3
		) referenced_accounts
		ORDER BY id`, proxyID, service.PlatformOpenAI, service.EgressModePool)
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

func (r *proxyRepository) ListAccountSummariesByProxyID(ctx context.Context, proxyID int64) ([]service.ProxyAccountSummary, error) {
	rows, err := r.sql.QueryContext(ctx, `
		SELECT a.id, a.name, a.platform, a.type, a.notes
		FROM accounts a
		JOIN (
			SELECT id AS account_id
			FROM accounts
			WHERE proxy_id=$1 AND deleted_at IS NULL
			UNION
			SELECT a.id AS account_id
			FROM account_egress_bindings b
			JOIN egress_routes er ON er.id=b.route_id
			JOIN accounts a ON a.id=b.account_id
			WHERE er.proxy_id=$1
				AND a.deleted_at IS NULL
				AND a.platform=$2
				AND a.egress_mode=$3
		) refs ON refs.account_id=a.id
		ORDER BY a.id DESC
	`, proxyID, service.PlatformOpenAI, service.EgressModePool)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	out := make([]service.ProxyAccountSummary, 0)
	for rows.Next() {
		var (
			id       int64
			name     string
			platform string
			accType  string
			notes    sql.NullString
		)
		if err := rows.Scan(&id, &name, &platform, &accType, &notes); err != nil {
			return nil, err
		}
		var notesPtr *string
		if notes.Valid {
			notesPtr = &notes.String
		}
		out = append(out, service.ProxyAccountSummary{
			ID:       id,
			Name:     name,
			Platform: platform,
			Type:     accType,
			Notes:    notesPtr,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// GetAccountCountsForProxies returns a map of proxy ID to account count for all proxies
func (r *proxyRepository) GetAccountCountsForProxies(ctx context.Context) (counts map[int64]int64, err error) {
	rows, err := r.sql.QueryContext(ctx, `
		SELECT proxy_id, COUNT(*) AS count
		FROM (
			SELECT a.id AS account_id, a.proxy_id
			FROM accounts a
			WHERE a.proxy_id IS NOT NULL AND a.deleted_at IS NULL
			UNION
			SELECT a.id AS account_id, er.proxy_id
			FROM account_egress_bindings b
			JOIN egress_routes er ON er.id=b.route_id
			JOIN accounts a ON a.id=b.account_id
			WHERE er.proxy_id IS NOT NULL
				AND a.deleted_at IS NULL
				AND a.platform=$1
				AND a.egress_mode=$2
		) referenced_accounts
		GROUP BY proxy_id`, service.PlatformOpenAI, service.EgressModePool)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && err == nil {
			err = closeErr
			counts = nil
		}
	}()

	counts = make(map[int64]int64)
	for rows.Next() {
		var proxyID, count int64
		if err = rows.Scan(&proxyID, &count); err != nil {
			return nil, err
		}
		counts[proxyID] = count
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return counts, nil
}

// ListActiveWithAccountCount returns all active proxies with account count, sorted by creation time descending
func (r *proxyRepository) ListActiveWithAccountCount(ctx context.Context) ([]service.ProxyWithAccountCount, error) {
	proxies, err := r.client.Proxy.Query().
		Where(proxy.StatusEQ(service.StatusActive)).
		Order(dbent.Desc(proxy.FieldCreatedAt)).
		All(ctx)
	if err != nil {
		return nil, err
	}

	// Get account counts
	counts, err := r.GetAccountCountsForProxies(ctx)
	if err != nil {
		return nil, err
	}

	// Build result with account counts
	result := make([]service.ProxyWithAccountCount, 0, len(proxies))
	for i := range proxies {
		proxyOut := proxyEntityToService(proxies[i])
		if proxyOut == nil {
			continue
		}
		result = append(result, service.ProxyWithAccountCount{
			Proxy:        *proxyOut,
			AccountCount: counts[proxyOut.ID],
		})
	}

	return result, nil
}

func proxyEntityToService(m *dbent.Proxy) *service.Proxy {
	if m == nil {
		return nil
	}
	out := &service.Proxy{
		ID:             m.ID,
		Name:           m.Name,
		Protocol:       m.Protocol,
		Host:           m.Host,
		Port:           m.Port,
		Status:         m.Status,
		CreatedAt:      m.CreatedAt,
		UpdatedAt:      m.UpdatedAt,
		ExpiresAt:      m.ExpiresAt,
		FallbackMode:   m.FallbackMode,
		BackupProxyID:  m.BackupProxyID,
		ExpiryWarnDays: m.ExpiryWarnDays,
	}
	if m.Username != nil {
		out.Username = *m.Username
	}
	if m.Password != nil {
		out.Password = *m.Password
	}
	return out
}

func applyProxyEntityToService(dst *service.Proxy, src *dbent.Proxy) {
	if dst == nil || src == nil {
		return
	}
	dst.ID = src.ID
	dst.CreatedAt = src.CreatedAt
	dst.UpdatedAt = src.UpdatedAt
}

// ListAllForFallback 返回所有代理（含过期/非活跃），供改投逻辑使用。
func (r *proxyRepository) ListAllForFallback(ctx context.Context) ([]service.Proxy, error) {
	proxies, err := r.client.Proxy.Query().All(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]service.Proxy, 0, len(proxies))
	for i := range proxies {
		out = append(out, *proxyEntityToService(proxies[i]))
	}
	return out, nil
}

// SweepExpiredProxies 扫描到期 active 代理，标记 expired 并按 fallback 策略改写绑定账号的 proxy_id，
// 最终触发 scheduler outbox 使 Redis 快照缓存失效。返回受影响的账号行数。
// 每个过期代理的「标记 expired + 改投账号 + outbox」在各自子事务内原子执行。
func (r *proxyRepository) SweepExpiredProxies(ctx context.Context, now time.Time) (int64, error) {
	// 快照读（事务前）：允许脏读不影响正确性，事务内已加锁写。
	all, err := r.ListAllForFallback(ctx)
	if err != nil {
		return 0, err
	}
	var totalChanged int64
	for _, p := range all {
		if p.Status != service.StatusActive || !p.IsExpired(now) {
			continue
		}

		changedAccountIDs, sweepErr := r.sweepOneExpiredProxy(ctx, p.ID, now)
		if sweepErr != nil {
			return totalChanged, sweepErr
		}
		totalChanged += int64(len(changedAccountIDs))
	}
	return totalChanged, nil
}

func sortedUniqueAccountIDs(accountIDs []int64) []int64 {
	if len(accountIDs) < 2 {
		return accountIDs
	}
	sort.Slice(accountIDs, func(i, j int) bool { return accountIDs[i] < accountIDs[j] })
	write := 1
	for _, accountID := range accountIDs[1:] {
		if accountID == accountIDs[write-1] {
			continue
		}
		accountIDs[write] = accountID
		write++
	}
	return accountIDs[:write]
}

// sweepOneExpiredProxy 在单事务内原子执行：标记代理 expired + 改投绑定账号。
// 若 r.client 已绑定事务（测试注入场景），直接在 r.sql 上执行，由外层事务保证原子性。
func (r *proxyRepository) sweepOneExpiredProxy(ctx context.Context, proxyID int64, now time.Time) ([]int64, error) {
	for attempt := 0; attempt < accountConfigurationLockRetryLimit; attempt++ {
		// 若 client 已绑定外层事务，无法释放漂移后的锁并重试，由调用者处理。
		tx, txErr := r.client.Tx(ctx)
		if txErr != nil {
			if txErr != dbent.ErrTxStarted {
				return nil, txErr
			}
			return r.sweepOneExpiredProxyOnExec(ctx, r.sql, proxyID, now)
		}

		accountIDs, err := r.sweepOneExpiredProxyOnExec(ctx, tx, proxyID, now)
		if err != nil {
			_ = tx.Rollback()
			if errors.Is(err, errAccountConfigurationLockSetChanged) {
				continue
			}
			return nil, err
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return nil, commitErr
		}
		return accountIDs, nil
	}
	return nil, service.ErrEgressPoolConflict
}

// sweepOneExpiredProxyOnExec 在给定的 sqlExecutor 上执行：标记 expired + 改投账号。
func (r *proxyRepository) sweepOneExpiredProxyOnExec(ctx context.Context, exec sqlExecutor, proxyID int64, now time.Time) ([]int64, error) {
	preLock, err := loadProxyFallbackSnapshot(ctx, exec)
	if err != nil {
		return nil, err
	}
	preLockIDs := proxySnapshotIDs(preLock)
	locked, err := lockProxiesForNoKeyUpdateInOrder(ctx, exec, preLockIDs)
	if err != nil {
		return nil, err
	}
	if locked != len(preLockIDs) {
		return nil, errAccountConfigurationLockSetChanged
	}
	all, err := loadProxyFallbackSnapshot(ctx, exec)
	if err != nil {
		return nil, err
	}
	if !slices.Equal(preLockIDs, proxySnapshotIDs(all)) {
		return nil, errAccountConfigurationLockSetChanged
	}
	byID := make(map[int64]service.Proxy, len(all))
	for _, proxy := range all {
		byID[proxy.ID] = proxy
	}
	current, ok := byID[proxyID]
	if !ok || current.Status != service.StatusActive || !current.IsExpired(now) {
		return nil, nil
	}
	target, change := service.ResolveProxyFallbackTarget(current, byID, now)
	if !change && current.FallbackMode == service.FallbackModeProxy {
		logger.LegacyPrintf("repository.proxy", "[ProxyExpiry] proxy %d expired but fallback chain unresolved (cycle/all-expired); accounts kept", current.ID)
	}
	var backupProxyID any
	if current.BackupProxyID != nil {
		backupProxyID = *current.BackupProxyID
	}
	result, err := exec.ExecContext(ctx, `
		UPDATE proxies
		SET status=$1, updated_at=NOW()
		WHERE id=$2 AND deleted_at IS NULL
			AND status=$3
			AND expires_at IS NOT NULL AND expires_at <= $4
			AND fallback_mode=$5
			AND backup_proxy_id IS NOT DISTINCT FROM $6`,
		service.StatusExpired, proxyID, service.StatusActive, now, current.FallbackMode, backupProxyID)
	if err != nil {
		return nil, err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if updated == 0 {
		return nil, nil
	}
	if updated != 1 {
		return nil, errAccountConfigurationLockSetChanged
	}
	proxyIDs := []int64{proxyID}
	if target != nil {
		proxyIDs = append(proxyIDs, *target)
	}
	routeIDs, err := egressRouteIDsForProxyIDs(ctx, exec, proxyIDs)
	if err != nil {
		return nil, err
	}
	if err := lockEgressRoutesForUpdateInOrder(ctx, exec, routeIDs); err != nil {
		return nil, err
	}
	routeID, err := syncProxyEgressRouteStateTx(ctx, exec, proxyID, service.EgressRouteStateExpired, false)
	if err != nil {
		return nil, err
	}
	poolAccountIDs, err := accountIDsForRouteTx(ctx, exec, routeID)
	if err != nil {
		return nil, err
	}
	var legacyAccountIDs []int64
	if change {
		legacyAccountIDs, err = proxyFallbackAccountIDs(ctx, exec, proxyID)
	} else {
		legacyAccountIDs, err = proxyProbeSnapshotAccountIDs(ctx, exec, proxyID)
	}
	if err != nil {
		return nil, err
	}
	allAccountIDs := sortedUniqueAccountIDs(append(append([]int64(nil), poolAccountIDs...), legacyAccountIDs...))
	if err := lockAccountsInOrder(ctx, exec, allAccountIDs); err != nil {
		return nil, err
	}
	if err := invalidateLockedAccountsTx(ctx, exec, poolAccountIDs); err != nil {
		return nil, err
	}
	if !change {
		accountIDs, err := invalidateProxyProbeSnapshots(ctx, exec, proxyID)
		if err != nil {
			return nil, err
		}
		if err := enqueueProxyProbeAccountChanges(ctx, exec, accountIDs); err != nil {
			return nil, err
		}
		return nil, nil
	}
	var rows *sql.Rows
	if target == nil {
		rows, err = exec.QueryContext(ctx, `
			UPDATE accounts SET proxy_id=NULL, proxy_fallback_origin_id=$1,
				egress_revision=egress_revision+1,
				extra=CASE
					WHEN type='apikey' AND extra ? 'upstream_billing_probe'
					THEN extra - 'upstream_billing_probe'
					ELSE extra
				END,
				updated_at=NOW()
			WHERE proxy_id=$1 AND proxy_fallback_origin_id IS NULL AND deleted_at IS NULL
				AND NOT (platform=$2 AND egress_mode=$3)
			RETURNING id`, proxyID, service.PlatformOpenAI, service.EgressModePool)
	} else {
		rows, err = exec.QueryContext(ctx, `
			UPDATE accounts SET proxy_id=$2, proxy_fallback_origin_id=$1,
				egress_revision=egress_revision+1,
				extra=CASE
					WHEN type='apikey' AND extra ? 'upstream_billing_probe'
					THEN extra - 'upstream_billing_probe'
					ELSE extra
				END,
				updated_at=NOW()
			WHERE proxy_id=$1 AND proxy_fallback_origin_id IS NULL AND deleted_at IS NULL
				AND NOT (platform=$3 AND egress_mode=$4)
			RETURNING id`, proxyID, *target, service.PlatformOpenAI, service.EgressModePool)
	}
	if err != nil {
		return nil, err
	}

	// 必须在提交子事务前读完并关闭 RETURNING 结果集，否则连接仍可能处于 busy 状态。
	accountIDs := make([]int64, 0)
	for rows.Next() {
		var accountID int64
		if err := rows.Scan(&accountID); err != nil {
			_ = rows.Close()
			return nil, err
		}
		accountIDs = append(accountIDs, accountID)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	accountIDs = sortedUniqueAccountIDs(accountIDs)
	if len(accountIDs) > 0 {
		payload := map[string]any{"account_ids": accountIDs}
		if err := enqueueSchedulerOutbox(ctx, exec, service.SchedulerOutboxEventAccountBulkChanged, nil, nil, payload); err != nil {
			return nil, err
		}
	}
	return accountIDs, nil
}

func loadProxyFallbackSnapshot(ctx context.Context, exec sqlExecutor) ([]service.Proxy, error) {
	rows, err := exec.QueryContext(ctx, `
		SELECT id, name, protocol, host, port, username, password, status,
			created_at, updated_at, expires_at, fallback_mode, backup_proxy_id, expiry_warn_days
		FROM proxies
		WHERE deleted_at IS NULL
		ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	proxies := make([]service.Proxy, 0)
	for rows.Next() {
		var proxy service.Proxy
		var username, password sql.NullString
		var expiresAt sql.NullTime
		var backupProxyID sql.NullInt64
		if err := rows.Scan(
			&proxy.ID, &proxy.Name, &proxy.Protocol, &proxy.Host, &proxy.Port,
			&username, &password, &proxy.Status, &proxy.CreatedAt, &proxy.UpdatedAt,
			&expiresAt, &proxy.FallbackMode, &backupProxyID, &proxy.ExpiryWarnDays,
		); err != nil {
			return nil, err
		}
		if username.Valid {
			proxy.Username = username.String
		}
		if password.Valid {
			proxy.Password = password.String
		}
		if expiresAt.Valid {
			value := expiresAt.Time
			proxy.ExpiresAt = &value
		}
		if backupProxyID.Valid {
			value := backupProxyID.Int64
			proxy.BackupProxyID = &value
		}
		proxies = append(proxies, proxy)
	}
	return proxies, rows.Err()
}

func proxySnapshotIDs(proxies []service.Proxy) []int64 {
	ids := make([]int64, 0, len(proxies))
	for _, proxy := range proxies {
		ids = append(ids, proxy.ID)
	}
	return ids
}

func egressRouteIDsForProxyIDs(ctx context.Context, exec sqlExecutor, proxyIDs []int64) ([]int64, error) {
	proxyIDs = sortedUniqueAccountIDs(proxyIDs)
	if len(proxyIDs) == 0 {
		return nil, nil
	}
	rows, err := exec.QueryContext(ctx, `
		SELECT id
		FROM egress_routes
		WHERE proxy_id=ANY($1)
		ORDER BY id`, pq.Array(proxyIDs))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	routeIDs := make([]int64, 0, len(proxyIDs))
	for rows.Next() {
		var routeID int64
		if err := rows.Scan(&routeID); err != nil {
			return nil, err
		}
		routeIDs = append(routeIDs, routeID)
	}
	return routeIDs, rows.Err()
}

func proxyFallbackAccountIDs(ctx context.Context, exec sqlExecutor, proxyID int64) ([]int64, error) {
	rows, err := exec.QueryContext(ctx, `
		SELECT id
		FROM accounts
		WHERE proxy_id=$1 AND proxy_fallback_origin_id IS NULL AND deleted_at IS NULL
			AND NOT (platform=$2 AND egress_mode=$3)
		ORDER BY id`, proxyID, service.PlatformOpenAI, service.EgressModePool)
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

// CountExpired 返回已过期（status=expired）的代理数量。
func (r *proxyRepository) CountExpired(ctx context.Context) (int64, error) {
	var c int64
	err := scanSingleRow(ctx, r.sql, `SELECT COUNT(*) FROM proxies WHERE status=$1 AND deleted_at IS NULL`, []any{service.StatusExpired}, &c)
	return c, err
}

// CountExpiringSoon 返回即将到期（在 expiry_warn_days 天内）的活跃代理数量。
func (r *proxyRepository) CountExpiringSoon(ctx context.Context, now time.Time) (int64, error) {
	var c int64
	err := scanSingleRow(ctx, r.sql, `
		SELECT COUNT(*) FROM proxies
		WHERE deleted_at IS NULL AND status=$1 AND expires_at IS NOT NULL
		  AND expires_at > $2 AND expires_at <= $2 + (expiry_warn_days || ' days')::interval`,
		[]any{service.StatusActive, now}, &c)
	return c, err
}
