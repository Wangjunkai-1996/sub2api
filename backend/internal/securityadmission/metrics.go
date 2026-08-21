package securityadmission

import "sync/atomic"

type MetricsSnapshot struct {
	ClassifyTotal              int64
	AuditableTotal             int64
	KnownNoTextTotal           int64
	KnownViolationTotal        int64
	UninspectableTotal         int64
	LargeBodyTotal             int64
	ParseNanosTotal            int64
	ParseNanosMax              int64
	DispatchTotal              int64
	DispatchAuditRequiredTotal int64
	DispatchAuditExemptTotal   int64
	DispatchUnknownTotal       int64
}

var admissionMetrics struct {
	classifyTotal              atomic.Int64
	auditableTotal             atomic.Int64
	knownNoTextTotal           atomic.Int64
	knownViolationTotal        atomic.Int64
	uninspectableTotal         atomic.Int64
	largeBodyTotal             atomic.Int64
	parseNanosTotal            atomic.Int64
	parseNanosMax              atomic.Int64
	dispatchTotal              atomic.Int64
	dispatchAuditRequiredTotal atomic.Int64
	dispatchAuditExemptTotal   atomic.Int64
	dispatchUnknownTotal       atomic.Int64
}

func observeClassification(admission Admission) {
	admissionMetrics.classifyTotal.Add(1)
	admissionMetrics.parseNanosTotal.Add(admission.parseNanos)
	for {
		current := admissionMetrics.parseNanosMax.Load()
		if admission.parseNanos <= current || admissionMetrics.parseNanosMax.CompareAndSwap(current, admission.parseNanos) {
			break
		}
	}
	switch admission.class {
	case RequestAuditableText:
		admissionMetrics.auditableTotal.Add(1)
	case RequestKnownNoText:
		admissionMetrics.knownNoTextTotal.Add(1)
	case RequestKnownViolation:
		admissionMetrics.knownViolationTotal.Add(1)
	case RequestUninspectable:
		admissionMetrics.uninspectableTotal.Add(1)
		if admission.reason == ReasonLargeBody {
			admissionMetrics.largeBodyTotal.Add(1)
		}
	}
}

// ObserveDispatch records a request that crossed terminal admission and is
// entering an upstream forwarding path.
func ObserveDispatch(accountClass AccountClass) {
	admissionMetrics.dispatchTotal.Add(1)
	switch accountClass {
	case AccountAuditRequired:
		admissionMetrics.dispatchAuditRequiredTotal.Add(1)
	case AccountAuditExemptVerified:
		admissionMetrics.dispatchAuditExemptTotal.Add(1)
	default:
		admissionMetrics.dispatchUnknownTotal.Add(1)
	}
}

func SnapshotMetrics() MetricsSnapshot {
	return MetricsSnapshot{
		ClassifyTotal:              admissionMetrics.classifyTotal.Load(),
		AuditableTotal:             admissionMetrics.auditableTotal.Load(),
		KnownNoTextTotal:           admissionMetrics.knownNoTextTotal.Load(),
		KnownViolationTotal:        admissionMetrics.knownViolationTotal.Load(),
		UninspectableTotal:         admissionMetrics.uninspectableTotal.Load(),
		LargeBodyTotal:             admissionMetrics.largeBodyTotal.Load(),
		ParseNanosTotal:            admissionMetrics.parseNanosTotal.Load(),
		ParseNanosMax:              admissionMetrics.parseNanosMax.Load(),
		DispatchTotal:              admissionMetrics.dispatchTotal.Load(),
		DispatchAuditRequiredTotal: admissionMetrics.dispatchAuditRequiredTotal.Load(),
		DispatchAuditExemptTotal:   admissionMetrics.dispatchAuditExemptTotal.Load(),
		DispatchUnknownTotal:       admissionMetrics.dispatchUnknownTotal.Load(),
	}
}
