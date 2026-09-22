package repository

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestPauseIneligibleJobsUsesBoundedFencedUpdate(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\) FROM updated`).WithArgs(500).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(2)))

	repo := &openAIWindowWarmupRepository{db: db}
	changed, err := repo.PauseIneligibleJobs(context.Background(), 0)
	require.NoError(t, err)
	require.Equal(t, int64(2), changed)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPauseIneligibleJobsCapsLimit(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	mock.ExpectQuery(`(?s)SELECT COUNT\(\*\) FROM updated`).WithArgs(500).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(0)))

	repo := &openAIWindowWarmupRepository{db: db}
	changed, err := repo.PauseIneligibleJobs(context.Background(), 5001)
	require.NoError(t, err)
	require.Zero(t, changed)
	require.NoError(t, mock.ExpectationsWereMet())
}
