package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

// Opt-in against a disposable local PostgreSQL database. This test creates an
// isolated schema and never runs application migrations or contacts production.
func TestCodexTicketStorePostgresConcurrentCollectors(t *testing.T) {
	dsn := os.Getenv("CODEX_TICKET_TEST_DSN")
	if dsn == "" {
		t.Skip("CODEX_TICKET_TEST_DSN is unset")
	}
	ctx := context.Background()
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer db.Close()
	schema := fmt.Sprintf("codex_ticket_test_%d", time.Now().UnixNano())
	_, err = db.ExecContext(ctx, "CREATE SCHEMA "+schema)
	require.NoError(t, err)
	defer func() { _, _ = db.ExecContext(ctx, "DROP SCHEMA "+schema+" CASCADE") }()
	parsed, err := url.Parse(dsn)
	require.NoError(t, err)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	scoped, err := sql.Open("postgres", parsed.String())
	require.NoError(t, err)
	defer scoped.Close()
	_, err = scoped.ExecContext(ctx, `CREATE TABLE accounts (
  id bigint PRIMARY KEY, extra jsonb, status text, platform text, type text,
  credentials jsonb, proxy_id bigint, parent_account_id bigint,
  egress_mode text, egress_revision bigint, deleted_at timestamptz, updated_at timestamptz
 ); CREATE TABLE proxies (id bigint PRIMARY KEY, protocol text, host text, port integer, username text, password text, status text, deleted_at timestamptz, expires_at timestamptz);`)
	require.NoError(t, err)
	config := map[string]any{"enabled": true, "revision": "r1", "model": "gpt-6-astra", "ticket_plan": "pro"}
	account := &service.Account{ID: 1, Status: service.StatusActive, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth,
		Credentials: map[string]any{"refresh_token": "r"}, EgressMode: service.EgressModeLegacy, EgressRevision: 1,
		Extra: map[string]any{service.CodexTicketV2ConfigKey: config}}
	extra, err := json.Marshal(account.Extra)
	require.NoError(t, err)
	_, err = scoped.ExecContext(ctx, `INSERT INTO accounts VALUES (1,$1,'active','openai','oauth','{"refresh_token":"r"}',NULL,NULL,'legacy',1,NULL,clock_timestamp())`, string(extra))
	require.NoError(t, err)
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, scoped)))
	repoA := newAccountRepositoryWithSQL(client, scoped, nil)
	repoB := newAccountRepositoryWithSQL(client, scoped, nil)
	configSnapshot := service.CodexTicketExtraSnapshot(account, service.CodexTicketV2ConfigKey)
	var acquired [2]*service.CodexTicketLease
	var acquireErrors [2]error
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i, repo := range []*accountRepository{repoA, repoB} {
		workers.Add(1)
		go func(i int, repo *accountRepository) {
			defer workers.Done()
			<-start
			acquired[i], acquireErrors[i] = repo.AcquireCodexTicketLease(ctx, account, "gpt-6-astra", configSnapshot, time.Minute)
		}(i, repo)
	}
	close(start)
	workers.Wait()
	require.NoError(t, acquireErrors[0])
	require.NoError(t, acquireErrors[1])
	require.NotEqual(t, acquired[0] == nil, acquired[1] == nil, "exactly one collector owns the account")
	old := acquired[0]
	if old == nil {
		old = acquired[1]
	}
	require.Len(t, old.Owner, 32)
	renewed, err := repoA.RenewCodexTicketLease(ctx, account, old, time.Minute)
	require.NoError(t, err)
	require.True(t, renewed)
	otherModel, err := repoB.AcquireCodexTicketLease(ctx, account, "gpt-5.6-sol", configSnapshot, time.Minute)
	require.NoError(t, err)
	require.Nil(t, otherModel, "different model is still the same account lease")
	_, err = scoped.ExecContext(ctx, `UPDATE accounts SET extra = jsonb_set(extra, '{codex_turn_ticket:v2:lease,expires_at}', to_jsonb(clock_timestamp() - interval '1 second')) WHERE id=1`)
	require.NoError(t, err)
	newer, err := repoB.AcquireCodexTicketLease(ctx, account, "gpt-6-astra", configSnapshot, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, newer)
	require.Greater(t, newer.Fence, old.Fence)
	renewed, err = repoA.RenewCodexTicketLease(ctx, account, old, time.Minute)
	require.NoError(t, err)
	require.False(t, renewed)
	released, err := repoA.ReleaseCodexTicketLease(ctx, old)
	require.NoError(t, err)
	require.False(t, released)
	ticketKey := service.CodexTicketV2Key("gpt-6-astra")
	expected := service.CodexTicketExtraSnapshot(account, service.CodexTicketV2ConfigKey, ticketKey)
	firstTicket := map[string]any{"state": "first", "captured_at": "2026-09-22T00:00:00Z"}
	update := map[string]service.CodexTicketExtraValue{ticketKey: {Exists: true, Value: firstTicket}}
	saved, err := repoA.CompareAndSwapCodexTicket(ctx, account, expected, update, old)
	require.NoError(t, err)
	require.False(t, saved)
	saved, err = repoB.CompareAndSwapCodexTicket(ctx, account, expected, update, newer)
	require.NoError(t, err)
	require.True(t, saved)
	account.Extra[ticketKey] = firstTicket
	staleWatchdog := service.CodexTicketExtraSnapshot(account, service.CodexTicketV2ConfigKey, ticketKey)
	replacement := map[string]service.CodexTicketExtraValue{ticketKey: {Exists: true, Value: map[string]any{"state": "second"}}}
	saved, err = repoB.CompareAndSwapCodexTicket(ctx, account, staleWatchdog, replacement, newer)
	require.NoError(t, err)
	require.True(t, saved)
	revoked, err := repoA.CompareAndSwapCodexTicket(ctx, account, staleWatchdog, map[string]service.CodexTicketExtraValue{ticketKey: {}}, nil)
	require.NoError(t, err)
	require.False(t, revoked, "an old response must not revoke the new ticket")
	// Explicit null is distinct from an absent key, including when deleting.
	watchdogKey := service.CodexTicketV2WatchdogKey
	account.Extra[watchdogKey] = nil
	nullExpected := service.CodexTicketExtraSnapshot(account, service.CodexTicketV2ConfigKey, watchdogKey)
	saved, err = repoA.CompareAndSwapCodexTicket(ctx, account, nullExpected, map[string]service.CodexTicketExtraValue{watchdogKey: {}}, nil)
	require.NoError(t, err)
	require.False(t, saved)
	delete(account.Extra, watchdogKey)
	absentExpected := service.CodexTicketExtraSnapshot(account, service.CodexTicketV2ConfigKey, watchdogKey)
	saved, err = repoA.CompareAndSwapCodexTicket(ctx, account, absentExpected, map[string]service.CodexTicketExtraValue{watchdogKey: {Exists: true, Value: nil}}, nil)
	require.NoError(t, err)
	require.True(t, saved)
	saved, err = repoA.CompareAndSwapCodexTicket(ctx, account, nullExpected, map[string]service.CodexTicketExtraValue{watchdogKey: {}}, nil)
	require.NoError(t, err)
	require.True(t, saved)
	// All late work loses authority when configuration or account identity changes.
	_, err = scoped.ExecContext(ctx, `UPDATE accounts SET extra = jsonb_set(extra,'{codex_turn_ticket:v2:config,revision}','"r2"') WHERE id=1`)
	require.NoError(t, err)
	renewed, err = repoB.RenewCodexTicketLease(ctx, account, newer, time.Minute)
	require.NoError(t, err)
	require.False(t, renewed)
	released, err = repoB.ReleaseCodexTicketLease(ctx, newer)
	require.NoError(t, err)
	require.True(t, released)
	config["revision"] = "r2"
	currentConfig := service.CodexTicketExtraSnapshot(account, service.CodexTicketV2ConfigKey)
	third, err := repoA.AcquireCodexTicketLease(ctx, account, "gpt-6-astra", currentConfig, time.Minute)
	require.NoError(t, err)
	require.NotNil(t, third)
	require.Greater(t, third.Fence, newer.Fence)
	_, err = scoped.ExecContext(ctx, `UPDATE accounts SET credentials='{"refresh_token":"replacement"}' WHERE id=1`)
	require.NoError(t, err)
	fresh := *account
	fresh.Credentials = map[string]any{"refresh_token": "replacement"}
	renewed, err = repoA.RenewCodexTicketLease(ctx, &fresh, third, time.Minute)
	require.NoError(t, err)
	require.False(t, renewed, "a fresh snapshot cannot lend its authority to old work")
	_, err = scoped.ExecContext(ctx, `UPDATE accounts SET credentials='{"refresh_token":"r"}' WHERE id=1`)
	require.NoError(t, err)
	for _, mutation := range []string{"status='disabled'", "status='active', credentials='{}'", "credentials='{\"refresh_token\":\"r\"}', proxy_id=99", "proxy_id=NULL, egress_revision=2", "egress_revision=1, deleted_at=clock_timestamp()"} {
		_, err = scoped.ExecContext(ctx, "UPDATE accounts SET "+mutation+" WHERE id=1")
		require.NoError(t, err)
		renewed, err = repoA.RenewCodexTicketLease(ctx, account, third, time.Minute)
		require.NoError(t, err)
		require.False(t, renewed, mutation)
	}
}
