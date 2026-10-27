package repository

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	dbent "github.com/Wei-Shaw/sub2api/ent"
	dbaccount "github.com/Wei-Shaw/sub2api/ent/account"
	"github.com/stretchr/testify/require"

	"entgo.io/ent/dialect"
	entsql "entgo.io/ent/dialect/sql"
)

func TestLoadAccountEgressHydrationLegacySourcesSkipBindings(t *testing.T) {
	parentID := int64(41)
	tests := []struct {
		name           string
		account        *dbent.Account
		runtimeID      int64
		expectedSource int64
		loadsSource    bool
	}{
		{
			name:           "direct account",
			account:        &dbent.Account{ID: parentID, EgressMode: dbaccount.EgressModeLegacy},
			runtimeID:      parentID,
			expectedSource: parentID,
		},
		{
			name: "shadow inherits legacy parent",
			account: &dbent.Account{
				ID:              42,
				ParentAccountID: &parentID,
				EgressMode:      dbaccount.EgressModeLegacy,
			},
			runtimeID:      42,
			expectedSource: parentID,
			loadsSource:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })

			client := dbent.NewClient(dbent.Driver(entsql.OpenDB(dialect.Postgres, db)))
			t.Cleanup(func() { _ = client.Close() })
			repo := newAccountRepositoryWithSQL(client, db, nil)

			if tt.loadsSource {
				mock.ExpectQuery("SELECT").
					WithArgs(tt.expectedSource).
					WillReturnRows(updatedAccountRows(tt.expectedSource, `{}`))
			}
			// No AccountEgressBinding expectation is registered. Any query against
			// the new table therefore fails this test.

			bindings, sources, err := repo.loadAccountEgressHydration(
				context.Background(),
				[]*dbent.Account{tt.account},
			)

			require.NoError(t, err)
			require.Empty(t, bindings[tt.runtimeID])
			require.NotNil(t, sources[tt.runtimeID])
			require.Equal(t, tt.expectedSource, sources[tt.runtimeID].ID)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
