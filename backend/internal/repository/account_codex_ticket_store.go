package repository

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/lib/pq"
)

const codexStorePrefix = "codex_turn_ticket:v2:"
const codexStoreConfigKey = codexStorePrefix + "config"
const codexStoreLeaseKey = codexStorePrefix + "lease"

var _ service.CodexTicketStore = (*accountRepository)(nil)

// All mutations bypass scheduler/outbox: runtime tickets are loaded from the
// writer database and are never authoritative in scheduler snapshots.
type codexTicketSQL struct{ args []any }

func (q *codexTicketSQL) bind(value any) string {
	q.args = append(q.args, value)
	return "$" + strconv.Itoa(len(q.args))
}

func codexTicketValueJSON(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func codexTicketValuesValid(values map[string]service.CodexTicketExtraValue) error {
	for key, value := range values {
		if !strings.HasPrefix(key, codexStorePrefix) || key == codexStorePrefix || key == codexStoreLeaseKey || strings.HasPrefix(key, codexStoreLeaseKey+":") {
			return errors.New("invalid codex ticket private key")
		}
		if value.Exists {
			if _, err := codexTicketValueJSON(value.Value); err != nil {
				return err
			}
		}
	}
	return nil
}

func codexTicketSortedKeys(values map[string]service.CodexTicketExtraValue) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (q *codexTicketSQL) expected(values map[string]service.CodexTicketExtraValue) string {
	var guards []string
	for _, key := range codexTicketSortedKeys(values) {
		value := values[key]
		arg := q.bind(key)
		if !value.Exists {
			guards = append(guards, "NOT (COALESCE(a.extra, '{}'::jsonb) ? "+arg+")")
		} else {
			raw, _ := codexTicketValueJSON(value.Value)
			guards = append(guards, "a.extra -> "+arg+" = "+q.bind(raw)+"::jsonb")
		}
	}
	return strings.Join(guards, " AND ")
}

func (q *codexTicketSQL) account(account *service.Account) (string, error) {
	credentials, err := json.Marshal(normalizeJSONMap(account.Credentials))
	if err != nil {
		return "", err
	}
	mode := account.EgressMode
	if mode == "" {
		mode = service.EgressModeLegacy
	}
	revision := account.EgressRevision
	if revision == 0 {
		revision = 1
	}
	guards := []string{
		"a.id = " + q.bind(account.ID), "a.deleted_at IS NULL",
		"a.status = " + q.bind(account.Status), "a.platform = " + q.bind(account.Platform),
		"a.type = " + q.bind(account.Type), "a.credentials = " + q.bind(string(credentials)) + "::jsonb",
		"a.egress_mode = " + q.bind(mode), "a.egress_revision = " + q.bind(revision),
		"a.parent_account_id IS NULL",
	}
	// WithResolvedAccountEgress replaces ProxyID with the selected route's proxy.
	// In pool mode it is the binding/route, not accounts.proxy_id, that is fenced.
	if account.SelectedEgress == nil {
		guards = append(guards, "a.proxy_id IS NOT DISTINCT FROM "+q.bind(account.ProxyID))
	} else {
		selected := account.SelectedEgress
		guards = append(guards, "a.egress_revision = "+q.bind(selected.AuthorityRevision),
			"EXISTS (SELECT 1 FROM account_egress_bindings b WHERE b.account_id = a.id AND b.route_id = "+q.bind(selected.RouteID)+" AND b.status = 'active')")
	}
	return strings.Join(guards, " AND "), nil
}

func (q *codexTicketSQL) liveLease(lease *service.CodexTicketLease) string {
	return "a.status = 'active' AND a.id = " + q.bind(lease.AccountID) +
		" AND a.extra -> '" + codexStoreLeaseKey + "' ->> 'owner' = " + q.bind(lease.Owner) +
		" AND a.extra -> '" + codexStoreLeaseKey + "' ->> 'model' = " + q.bind(lease.Model) +
		" AND (a.extra -> '" + codexStoreLeaseKey + "' ->> 'fence')::bigint = " + q.bind(lease.Fence) +
		" AND (a.extra -> '" + codexStoreLeaseKey + "' ->> 'expires_at')::timestamptz > clock_timestamp()" +
		" AND (a.extra -> '" + codexStoreLeaseKey + "' -> 'config') IS NOT DISTINCT FROM (a.extra -> '" + codexStoreConfigKey + "')" +
		" AND a.extra -> '" + codexStoreLeaseKey + "' -> 'identity' = " + codexTicketIdentitySQL
}

// Pin the acquisition identity as well as the caller's snapshot. Re-reading a
// changed account must not let old work claim the new account configuration.
const codexTicketIdentitySQL = `jsonb_build_object(
 'credentials_sha256', encode(sha256(convert_to(a.credentials::text, 'UTF8')), 'hex'),
 'proxy_id', a.proxy_id, 'egress_mode', a.egress_mode, 'egress_revision', a.egress_revision,
 'proxy_sha256', (SELECT encode(sha256(convert_to(jsonb_build_object(
  'protocol', p.protocol, 'host', p.host, 'port', p.port, 'username', p.username,
  'password', p.password, 'status', p.status, 'expires_at', p.expires_at)::text, 'UTF8')), 'hex')
  FROM proxies p WHERE p.id = a.proxy_id AND p.deleted_at IS NULL))`

func validCodexTicketLease(lease *service.CodexTicketLease) bool {
	return lease != nil && lease.AccountID > 0 && lease.Model != "" && lease.Owner != "" && lease.Fence > 0
}

// Respect the same proxy -> route -> account lock order as account edits, so a
// proxy or route change cannot race between checking a fingerprint and saving.
func (r *accountRepository) withCodexTicketLocks(ctx context.Context, account *service.Account, requireActive bool, apply func(sqlExecutor) (bool, error)) (bool, error) {
	if r == nil || r.client == nil {
		return false, errors.New("codex ticket SQL client unavailable")
	}
	if account == nil || account.ID <= 0 || account.ParentAccountID != nil {
		return false, service.ErrAccountNilInput
	}
	client := clientFromContext(ctx, r.client)
	var owned *dbent.Tx
	if dbent.TxFromContext(ctx) == nil {
		tx, err := client.Tx(ctx)
		if err != nil {
			return false, err
		}
		owned, client = tx, tx.Client()
		defer func() { _ = tx.Rollback() }()
	}
	if account.ProxyID != nil {
		if account.Proxy == nil || account.Proxy.ID != *account.ProxyID {
			return false, nil
		}
		p := account.Proxy
		rows, err := client.QueryContext(ctx, `SELECT id FROM proxies WHERE id = $1
   AND protocol = $2 AND host = $3 AND port = $4 AND COALESCE(username, '') = $5
   AND COALESCE(password, '') = $6 AND status = $7 AND deleted_at IS NULL
   AND (NOT $8::boolean OR (status = 'active' AND (expires_at IS NULL OR expires_at > clock_timestamp()))) FOR SHARE`,
			p.ID, p.Protocol, p.Host, p.Port, p.Username, p.Password, p.Status, requireActive)
		if err != nil {
			return false, err
		}
		match := rows.Next()
		readErr := rows.Err()
		_ = rows.Close()
		if readErr != nil || !match {
			return false, readErr
		}
	}
	if selected := account.SelectedEgress; selected != nil {
		var route *service.EgressRoute
		for _, binding := range account.EgressBindings {
			if binding.RouteID == selected.RouteID && binding.BindingID == selected.BindingID && binding.Status == service.AccountEgressBindingStatusActive {
				route = binding.Route
				break
			}
		}
		if route == nil || route.ID != selected.RouteID || route.ExpectedIdentityID == nil || route.ExpectedIdentity == nil || strconv.FormatInt(*route.ExpectedIdentityID, 10) != selected.IdentityID {
			return false, nil
		}
		rows, err := client.QueryContext(ctx, `SELECT er.id FROM egress_routes er
   JOIN egress_identities ei ON ei.id = er.expected_identity_id
   WHERE er.id = $1 AND er.revision = $2 AND er.kind = $3
    AND er.proxy_id IS NOT DISTINCT FROM $4 AND er.runtime_scope IS NOT DISTINCT FROM $5
    AND er.expected_identity_id = $6 AND er.state = 'active' AND ei.status = 'active'
    AND host(ei.public_ip) = $7 FOR SHARE OF er, ei`,
			route.ID, route.Revision, route.Kind, route.ProxyID, route.RuntimeScope, *route.ExpectedIdentityID, route.ExpectedIdentity.PublicIP)
		if err != nil {
			return false, err
		}
		match := rows.Next()
		readErr := rows.Err()
		_ = rows.Close()
		if readErr != nil || !match {
			return false, readErr
		}
	}
	changed, err := apply(client)
	if err != nil || !changed {
		return changed, err
	}
	if owned != nil {
		err = owned.Commit()
	}
	return err == nil, err
}

func (r *accountRepository) CompareAndSwapCodexTicket(ctx context.Context, account *service.Account, expected, updates map[string]service.CodexTicketExtraValue, lease *service.CodexTicketLease) (bool, error) {
	if err := codexTicketValuesValid(expected); err != nil {
		return false, err
	}
	if err := codexTicketValuesValid(updates); err != nil {
		return false, err
	}
	if _, ok := expected[codexStoreConfigKey]; !ok {
		return false, errors.New("codex ticket CAS requires configuration snapshot")
	}
	if len(updates) == 0 {
		return false, nil
	}
	present := make(map[string]any)
	removed := make([]string, 0)
	for key, value := range updates {
		if _, ok := expected[key]; !ok {
			return false, errors.New("codex ticket CAS requires every changed key snapshot")
		}
		if value.Exists {
			// Only a live collector can publish a ticket. Configuration and watchdog
			// mutations still use CAS but do not need to impersonate a collector.
			if key != codexStoreConfigKey && key != codexStorePrefix+"watchdog" && (!validCodexTicketLease(lease) || key != codexStorePrefix+lease.Model) {
				return false, errors.New("codex ticket publication requires matching lease")
			}
			present[key] = value.Value
		} else {
			removed = append(removed, key)
		}
	}
	payload, err := json.Marshal(present)
	if err != nil {
		return false, err
	}
	return r.withCodexTicketLocks(ctx, account, lease != nil, func(exec sqlExecutor) (bool, error) {
		q := &codexTicketSQL{}
		guard, err := q.account(account)
		if err != nil {
			return false, err
		}
		guard += " AND " + q.expected(expected)
		if lease != nil {
			if !validCodexTicketLease(lease) {
				return false, errors.New("invalid codex ticket lease")
			}
			guard += " AND " + q.liveLease(lease)
		}
		extra := "(COALESCE(a.extra, '{}'::jsonb) || " + q.bind(string(payload)) + "::jsonb) - " + q.bind(pq.Array(removed)) + "::text[]"
		result, err := exec.ExecContext(ctx, "UPDATE accounts AS a SET extra = "+extra+", updated_at = clock_timestamp() WHERE "+guard, q.args...)
		if err != nil {
			return false, err
		}
		count, err := result.RowsAffected()
		return count == 1, err
	})
}

func (r *accountRepository) AcquireCodexTicketLease(ctx context.Context, account *service.Account, model string, expected map[string]service.CodexTicketExtraValue, ttl time.Duration) (*service.CodexTicketLease, error) {
	if ttl < time.Millisecond || ttl > time.Hour || strings.TrimSpace(model) == "" {
		return nil, errors.New("invalid codex ticket lease")
	}
	if err := codexTicketValuesValid(expected); err != nil {
		return nil, err
	}
	if _, ok := expected[codexStoreConfigKey]; !ok {
		return nil, errors.New("codex ticket lease requires configuration snapshot")
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, err
	}
	lease := &service.CodexTicketLease{Model: strings.TrimSpace(model), Owner: hex.EncodeToString(random[:])}
	changed, err := r.withCodexTicketLocks(ctx, account, true, func(exec sqlExecutor) (bool, error) {
		q := &codexTicketSQL{}
		guard, err := q.account(account)
		if err != nil {
			return false, err
		}
		guard += " AND a.status = 'active' AND " + q.expected(expected)
		guard += " AND (NOT (COALESCE(a.extra, '{}'::jsonb) ? '" + codexStoreLeaseKey + "') OR (a.extra -> '" + codexStoreLeaseKey + "' ->> 'expires_at')::timestamptz <= clock_timestamp())"
		nextLease := "jsonb_build_object('owner', " + q.bind(lease.Owner) + "::text, 'model', " + q.bind(lease.Model) + "::text, 'config', a.extra -> '" + codexStoreConfigKey + "', 'identity', " + codexTicketIdentitySQL + ", 'fence', COALESCE((a.extra -> '" + codexStoreLeaseKey + "' ->> 'fence')::bigint, 0) + 1, 'expires_at', clock_timestamp() + " + q.bind(ttl.Milliseconds()) + " * interval '1 millisecond')"
		rows, err := exec.QueryContext(ctx, "UPDATE accounts AS a SET extra = jsonb_set(COALESCE(a.extra, '{}'::jsonb), '{"+codexStoreLeaseKey+"}', "+nextLease+", true), updated_at = clock_timestamp() WHERE "+guard+" RETURNING (extra -> '"+codexStoreLeaseKey+"' ->> 'fence')::bigint, (extra -> '"+codexStoreLeaseKey+"' ->> 'expires_at')::timestamptz", q.args...)
		if err != nil {
			return false, err
		}
		defer rows.Close()
		if !rows.Next() {
			return false, rows.Err()
		}
		if err := rows.Scan(&lease.Fence, &lease.ExpiresAt); err != nil {
			return false, err
		}
		lease.AccountID = account.ID
		return true, rows.Err()
	})
	if err != nil || !changed {
		return nil, err
	}
	return lease, nil
}

func (r *accountRepository) RenewCodexTicketLease(ctx context.Context, account *service.Account, lease *service.CodexTicketLease, ttl time.Duration) (bool, error) {
	if !validCodexTicketLease(lease) || ttl < time.Millisecond || ttl > time.Hour {
		return false, errors.New("invalid codex ticket lease")
	}
	return r.withCodexTicketLocks(ctx, account, true, func(exec sqlExecutor) (bool, error) {
		q := &codexTicketSQL{}
		guard, err := q.account(account)
		if err != nil {
			return false, err
		}
		guard += " AND " + q.liveLease(lease)
		rows, err := exec.QueryContext(ctx, "UPDATE accounts AS a SET extra = jsonb_set(a.extra, '{"+codexStoreLeaseKey+",expires_at}', to_jsonb(clock_timestamp() + "+q.bind(ttl.Milliseconds())+" * interval '1 millisecond')), updated_at = clock_timestamp() WHERE "+guard+" RETURNING (extra -> '"+codexStoreLeaseKey+"' ->> 'expires_at')::timestamptz", q.args...)
		if err != nil {
			return false, err
		}
		defer rows.Close()
		if !rows.Next() {
			return false, rows.Err()
		}
		if err := rows.Scan(&lease.ExpiresAt); err != nil {
			return false, err
		}
		return true, rows.Err()
	})
}

func (r *accountRepository) ReleaseCodexTicketLease(ctx context.Context, lease *service.CodexTicketLease) (bool, error) {
	if r == nil || r.sql == nil || !validCodexTicketLease(lease) {
		return false, errors.New("invalid codex ticket lease")
	}
	// Keep the monotonic fence after release. A stale release can never clear a
	// later collector's lease, even if the old owner resumes after expiry.
	exec := r.sql
	if tx := dbent.TxFromContext(ctx); tx != nil {
		exec = tx.Client()
	}
	result, err := exec.ExecContext(ctx, `UPDATE accounts AS a
  SET extra = jsonb_set(a.extra, '{`+codexStoreLeaseKey+`}',
   (a.extra -> '`+codexStoreLeaseKey+`') || jsonb_build_object('owner', '', 'expires_at', clock_timestamp())),
   updated_at = clock_timestamp()
  WHERE a.id = $1 AND a.extra -> '`+codexStoreLeaseKey+`' ->> 'owner' = $2
   AND (a.extra -> '`+codexStoreLeaseKey+`' ->> 'fence')::bigint = $3
   AND a.extra -> '`+codexStoreLeaseKey+`' ->> 'model' = $4`, lease.AccountID, lease.Owner, lease.Fence, lease.Model)
	if err != nil {
		return false, err
	}
	count, err := result.RowsAffected()
	return count == 1, err
}
