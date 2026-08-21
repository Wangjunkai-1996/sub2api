package securityadmission

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestObserveDispatchCountsAccountClasses(t *testing.T) {
	before := SnapshotMetrics()

	ObserveDispatch(AccountAuditRequired)
	ObserveDispatch(AccountAuditExemptVerified)
	ObserveDispatch(AccountUnknown)

	after := SnapshotMetrics()
	require.Equal(t, int64(3), after.DispatchTotal-before.DispatchTotal)
	require.Equal(t, int64(1), after.DispatchAuditRequiredTotal-before.DispatchAuditRequiredTotal)
	require.Equal(t, int64(1), after.DispatchAuditExemptTotal-before.DispatchAuditExemptTotal)
	require.Equal(t, int64(1), after.DispatchUnknownTotal-before.DispatchUnknownTotal)
}
