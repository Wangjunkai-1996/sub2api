package repository

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestAccountRepositorySetSchedulableAtomicMutation(t *testing.T) {
	outboxErr := errors.New("outbox insert failed")
	rowsErr := errors.New("rows affected unavailable")
	for _, tt := range []struct {
		name        string
		schedulable bool
		execErr     error
		wantErr     error
	}{
		{name: "disable"},
		{name: "enable", schedulable: true},
		{name: "missing or deleted", wantErr: service.ErrAccountNotFound},
		{name: "outbox failure", execErr: outboxErr, wantErr: outboxErr},
		{name: "query failure", execErr: rowsErr, wantErr: rowsErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			if tt.execErr != nil {
				mock.ExpectQuery("WITH updated AS").WillReturnError(tt.execErr)
			} else if tt.wantErr != nil {
				mock.ExpectQuery("WITH updated AS").WillReturnRows(sqlmock.NewRows([]string{"id"}))
			} else {
				mock.ExpectQuery("WITH updated AS").WithArgs(tt.schedulable, int64(42), service.SchedulerOutboxEventAccountChanged, sqlmock.AnyArg()).
					WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(42)))
			}
			repo := newAccountRepositoryWithSQL(nil, db, nil)

			err = repo.SetSchedulable(context.Background(), 42, tt.schedulable)

			require.ErrorIs(t, err, tt.wantErr)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
