package repository

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestCodexTicketExtraSnapshotDistinguishesMissingAndNull(t *testing.T) {
	account := &service.Account{Extra: map[string]any{"codex_turn_ticket:v2:config": nil}}
	snapshot := service.CodexTicketExtraSnapshot(account, "codex_turn_ticket:v2:config", "codex_turn_ticket:v2:gpt-6-astra")
	require.True(t, snapshot["codex_turn_ticket:v2:config"].Exists)
	require.JSONEq(t, "null", string(snapshot["codex_turn_ticket:v2:config"].Value.(json.RawMessage)))
	require.False(t, snapshot["codex_turn_ticket:v2:gpt-6-astra"].Exists)
}

func TestCodexTicketCASRejectsUnfencedPublication(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })
	repo := newAccountRepositoryWithSQL(client, db, nil)
	account := &service.Account{ID: 7, Status: service.StatusActive, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Credentials: map[string]any{"refresh_token": "r"}, EgressMode: service.EgressModeLegacy}
	expected := service.CodexTicketExtraSnapshot(account, service.CodexTicketV2ConfigKey, service.CodexTicketV2Key("gpt-6-astra"))
	_, err = repo.CompareAndSwapCodexTicket(context.Background(), account, expected, map[string]service.CodexTicketExtraValue{
		service.CodexTicketV2Key("gpt-6-astra"): {Exists: true, Value: map[string]any{"state": "new"}},
	}, nil)
	require.Error(t, err)
	require.ErrorContains(t, err, "matching lease")
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestCodexTicketCASUsesExpectedJSONBAndNoOutbox(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
	t.Cleanup(func() { _ = client.Close() })
	repo := newAccountRepositoryWithSQL(client, db, nil)
	account := &service.Account{ID: 7, Status: service.StatusActive, Platform: service.PlatformOpenAI, Type: service.AccountTypeOAuth, Credentials: map[string]any{"refresh_token": "r"}, EgressMode: service.EgressModeLegacy}
	expected := map[string]service.CodexTicketExtraValue{
		service.CodexTicketV2ConfigKey:          {Exists: true, Value: map[string]any{"revision": "1"}},
		service.CodexTicketV2Key("gpt-6-astra"): {Exists: false},
	}
	updates := map[string]service.CodexTicketExtraValue{
		service.CodexTicketV2Key("gpt-6-astra"): {Exists: true, Value: map[string]any{"state": "new"}},
	}
	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta("UPDATE accounts AS a SET extra =")).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()
	ok, err := repo.CompareAndSwapCodexTicket(context.Background(), account, expected, updates, &service.CodexTicketLease{AccountID: 7, Model: "gpt-6-astra", Owner: "owner", Fence: 2, ExpiresAt: time.Now().Add(time.Minute)})
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, mock.ExpectationsWereMet())
}
