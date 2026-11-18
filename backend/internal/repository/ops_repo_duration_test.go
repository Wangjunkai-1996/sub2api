package repository

import (
	"context"
	"database/sql/driver"
	"math"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

func TestOpsErrorDurationPersistsInSingleAndBatchInserts(t *testing.T) {
	zero, elapsed, negative := int64(0), int64(24477), int64(-1)
	maximum, overflow := int64(math.MaxInt32), int64(math.MaxInt32)+1
	for _, tc := range []struct {
		name     string
		value    *int64
		expected driver.Value
	}{
		{name: "missing"},
		{name: "zero", value: &zero, expected: int64(0)},
		{name: "measured", value: &elapsed, expected: int64(24477)},
		{name: "negative", value: &negative},
		{name: "maximum", value: &maximum, expected: int64(math.MaxInt32)},
		{name: "overflow", value: &overflow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock := newSQLMock(t)
			repo := NewOpsRepository(db)
			input := &service.OpsInsertErrorLogInput{
				ErrorPhase: "upstream", ErrorType: "api_error", StatusCode: 502,
				DurationMs: tc.value, CreatedAt: time.Now(),
			}
			args := make([]driver.Value, 39)
			for i := range 38 {
				args[i] = sqlmock.AnyArg()
			}
			args[38] = tc.expected
			const insertPattern = `(?s)INSERT INTO ops_error_logs.*api_key_prefix,\s+duration_ms\s*\) VALUES.*\$39`
			mock.ExpectQuery(insertPattern + `\s*\) RETURNING id`).
				WithArgs(args...).
				WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
			id, err := repo.InsertErrorLog(context.Background(), input)
			require.NoError(t, err)
			require.EqualValues(t, 1, id)

			mock.ExpectBegin()
			prepared := mock.ExpectPrepare(insertPattern)
			prepared.ExpectExec().WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, 1))
			prepared.ExpectExec().WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()
			n, err := repo.BatchInsertErrorLogs(context.Background(), []*service.OpsInsertErrorLogInput{input, input})
			require.NoError(t, err)
			require.EqualValues(t, 2, n)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
