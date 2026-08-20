package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestClassifyTrafficDirectorHealthFailure(t *testing.T) {
	tests := []struct {
		name       string
		failure    TrafficDirectorHealthFailure
		eligible   bool
		wantKind   string
		wantReason string
	}{
		{
			name:     "account scoped five hundred",
			failure:  TrafficDirectorHealthFailure{StatusCode: http.StatusInternalServerError, AccountScoped: true},
			eligible: true,
			wantKind: TrafficDirectorHealthFailureKindHTTP5xx,
		},
		{
			name:       "unscoped five hundred",
			failure:    TrafficDirectorHealthFailure{StatusCode: http.StatusInternalServerError},
			wantReason: "unscoped_http_5xx",
		},
		{
			name:       "rate limit",
			failure:    TrafficDirectorHealthFailure{StatusCode: http.StatusTooManyRequests},
			wantReason: "rate_limit_or_overload",
		},
		{
			name:       "overload 529",
			failure:    TrafficDirectorHealthFailure{StatusCode: 529},
			wantReason: "rate_limit_or_overload",
		},
		{
			name:       "authentication",
			failure:    TrafficDirectorHealthFailure{StatusCode: http.StatusUnauthorized},
			wantReason: "authentication_or_authorization",
		},
		{
			name:       "quota body",
			failure:    TrafficDirectorHealthFailure{StatusCode: http.StatusBadGateway, AccountScoped: true, ResponseBody: []byte(`{"error":{"code":"insufficient_quota"}}`)},
			wantReason: "quota_or_rate_limit",
		},
		{
			name:       "unsupported body",
			failure:    TrafficDirectorHealthFailure{StatusCode: http.StatusBadGateway, AccountScoped: true, ResponseBody: []byte(`{"error":{"message":"Unsupported parameter"}}`)},
			wantReason: "unsupported_or_client_error",
		},
		{
			name:       "generic four hundred",
			failure:    TrafficDirectorHealthFailure{StatusCode: http.StatusBadRequest, AccountScoped: true},
			wantReason: "client_http_status",
		},
		{
			name:     "timeout",
			failure:  TrafficDirectorHealthFailure{Err: context.DeadlineExceeded},
			eligible: true,
			wantKind: TrafficDirectorHealthFailureKindTimeout,
		},
		{
			name:     "eof",
			failure:  TrafficDirectorHealthFailure{Err: io.ErrUnexpectedEOF},
			eligible: true,
			wantKind: TrafficDirectorHealthFailureKindEOF,
		},
		{
			name:     "eof after successful headers",
			failure:  TrafficDirectorHealthFailure{StatusCode: http.StatusOK, Err: io.ErrUnexpectedEOF},
			eligible: true,
			wantKind: TrafficDirectorHealthFailureKindEOF,
		},
		{
			name:     "connection reset text",
			failure:  TrafficDirectorHealthFailure{Err: errors.New("read: connection reset by peer")},
			eligible: true,
			wantKind: TrafficDirectorHealthFailureKindReset,
		},
		{
			name:     "stream error text",
			failure:  TrafficDirectorHealthFailure{Err: errors.New("stream error: stream ID 9; INTERNAL_ERROR")},
			eligible: true,
			wantKind: TrafficDirectorHealthFailureKindStream,
		},
		{
			name:     "sse error event text",
			failure:  TrafficDirectorHealthFailure{Err: errors.New("have error in stream")},
			eligible: true,
			wantKind: TrafficDirectorHealthFailureKindStream,
		},
		{
			name:       "client cancellation",
			failure:    TrafficDirectorHealthFailure{Err: context.Canceled},
			wantReason: "client_canceled",
		},
		{
			name:       "unqualified transport",
			failure:    TrafficDirectorHealthFailure{Err: errors.New("connection refused")},
			wantReason: "unqualified_transport_error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ClassifyTrafficDirectorHealthFailure(tt.failure)
			require.Equal(t, tt.eligible, got.Eligible)
			if tt.eligible {
				require.Equal(t, tt.wantKind, got.Kind)
			} else {
				require.Equal(t, tt.wantReason, got.IgnoredReason)
				require.Equal(t, TrafficDirectorHealthFailureKindNotApplicable, got.Kind)
			}
		})
	}
}

func TestTrafficDirectorHealthService_ModeSemanticsAndFailOpen(t *testing.T) {
	store := &trafficDirectorHealthStoreStub{
		checkSnapshot: TrafficDirectorHealthSnapshot{
			State:         TrafficDirectorHealthStateOpen,
			FailureStreak: 2,
			LastFailureAt: time.Unix(1_800_000_000, 0),
			OpenUntil:     time.Unix(1_800_000_010, 0),
		},
	}
	now := time.Unix(1_800_000_005, 0)
	service := NewTrafficDirectorHealthServiceWithOptions(store, TrafficDirectorHealthOptions{
		Clock: func() time.Time { return now },
		NewProbeToken: func() (string, error) {
			return "probe-token", nil
		},
	})

	off, err := service.Check(context.Background(), TrafficDirectorHealthCheckInput{
		AccountID: 1, Model: " GPT-4 ", HealthMode: domain.TrafficDirectorHealthModeOff,
	})
	require.NoError(t, err)
	require.True(t, off.Allowed)
	require.Equal(t, "gpt-4", off.Model)

	observe, err := service.Check(context.Background(), TrafficDirectorHealthCheckInput{
		AccountID: 1, Model: "gpt-4", HealthMode: domain.TrafficDirectorHealthModeObserve,
	})
	require.NoError(t, err)
	require.True(t, observe.Allowed)
	require.Zero(t, store.checkCalls)

	enforce, err := service.Check(context.Background(), TrafficDirectorHealthCheckInput{
		AccountID: 1, Model: "gpt-4", HealthMode: domain.TrafficDirectorHealthModeEnforce,
	})
	require.NoError(t, err)
	require.False(t, enforce.Allowed)
	require.True(t, enforce.ShouldFilter)
	require.Equal(t, TrafficDirectorHealthStateOpen, enforce.State)
	require.Equal(t, 1, store.checkCalls)

	store.checkErr = errors.New("redis down")
	failOpen, err := service.Check(context.Background(), TrafficDirectorHealthCheckInput{
		AccountID: 2, Model: "gpt-4", HealthMode: domain.TrafficDirectorHealthModeEnforce,
	})
	require.ErrorIs(t, err, ErrTrafficDirectorHealthUnavailable)
	require.True(t, failOpen.Allowed)
	require.False(t, failOpen.ShouldFilter)
	require.True(t, failOpen.FailOpen)
	require.Equal(t, int64(2), failOpen.AccountID, "fail-open must preserve the pool candidate")
}

func TestTrafficDirectorHealthServiceCanInspectHalfOpenWithoutTakingProbe(t *testing.T) {
	store := &trafficDirectorHealthStoreStub{
		checkSnapshot: TrafficDirectorHealthSnapshot{
			State:         TrafficDirectorHealthStateHalfOpen,
			FailureStreak: 2,
			LastFailureAt: time.Unix(1_800_000_000, 0),
			ProbeUntil:    time.Unix(1_800_000_020, 0),
		},
	}
	tokenCalls := 0
	health := NewTrafficDirectorHealthServiceWithOptions(store, TrafficDirectorHealthOptions{
		NewProbeToken: func() (string, error) {
			tokenCalls++
			return "probe", nil
		},
	})
	noProbe := false
	decision, err := health.Check(context.Background(), TrafficDirectorHealthCheckInput{
		AccountID: 1, Model: "gpt-5", HealthMode: domain.TrafficDirectorHealthModeEnforce, AcquireProbe: &noProbe,
	})
	require.NoError(t, err)
	require.Equal(t, TrafficDirectorHealthStateHalfOpen, decision.State)
	require.False(t, decision.Allowed)
	require.False(t, store.lastCheck.AcquireProbe)
	require.Zero(t, tokenCalls)

	store.checkSnapshot.ProbeAcquired = true
	decision, err = health.Check(context.Background(), TrafficDirectorHealthCheckInput{
		AccountID: 1, Model: "gpt-5", HealthMode: domain.TrafficDirectorHealthModeEnforce,
	})
	require.NoError(t, err)
	require.Equal(t, 1, tokenCalls)
	require.True(t, decision.HalfOpenProbe)
}

func TestTrafficDirectorHealthService_RecordModesAndIgnoredFailures(t *testing.T) {
	store := &trafficDirectorHealthStoreStub{
		failureSnapshot: TrafficDirectorHealthSnapshot{
			State:           TrafficDirectorHealthStateSuspect,
			FailureStreak:   1,
			LastFailureAt:   time.Unix(1_800_000_000, 0),
			MutationApplied: true,
		},
	}
	service := NewTrafficDirectorHealthService(store)

	ignored, err := service.RecordFailure(context.Background(), TrafficDirectorHealthFailureInput{
		AccountID: 1, Model: "gpt-4", HealthMode: domain.TrafficDirectorHealthModeObserve,
		Failure: TrafficDirectorHealthFailure{StatusCode: http.StatusTooManyRequests},
	})
	require.NoError(t, err)
	require.False(t, ignored.Eligible)
	require.Zero(t, store.failureCalls)

	off, err := service.RecordFailure(context.Background(), TrafficDirectorHealthFailureInput{
		AccountID: 1, Model: "gpt-4", HealthMode: domain.TrafficDirectorHealthModeOff,
		Failure: TrafficDirectorHealthFailure{StatusCode: http.StatusInternalServerError, AccountScoped: true},
	})
	require.NoError(t, err)
	require.True(t, off.Eligible)
	require.False(t, off.Recorded)
	require.Equal(t, "health_mode_off", off.IgnoredReason)
	require.Zero(t, store.failureCalls)

	observed, err := service.RecordFailure(context.Background(), TrafficDirectorHealthFailureInput{
		AccountID: 1, Model: "gpt-4", HealthMode: domain.TrafficDirectorHealthModeObserve,
		Failure: TrafficDirectorHealthFailure{StatusCode: http.StatusInternalServerError, AccountScoped: true},
	})
	require.NoError(t, err)
	require.True(t, observed.Eligible)
	require.True(t, observed.Recorded)
	require.Equal(t, 1, store.failureCalls)

	store.successRestored = true
	restored, err := service.RecordSuccess(context.Background(), TrafficDirectorHealthSuccessInput{
		AccountID: 1, Model: "gpt-4", HealthMode: domain.TrafficDirectorHealthModeObserve,
	})
	require.NoError(t, err)
	require.True(t, restored)
	require.Equal(t, 1, store.successCalls)
	require.True(t, store.lastSuccess.AllowObserveRecovery)

	_, err = service.RecordSuccess(context.Background(), TrafficDirectorHealthSuccessInput{
		AccountID: 1, Model: "gpt-4", HealthMode: domain.TrafficDirectorHealthModeEnforce,
	})
	require.NoError(t, err)
	require.False(t, store.lastSuccess.AllowObserveRecovery)
}

func TestNormalizeTrafficDirectorHealthModel(t *testing.T) {
	require.Equal(t, "gpt-5.1", NormalizeTrafficDirectorHealthModel("  GPT-5.1 "))
	require.Empty(t, NormalizeTrafficDirectorHealthModel(""))
	require.Empty(t, NormalizeTrafficDirectorHealthModel(string(make([]byte, trafficDirectorHealthMaxModelBytes+1))))
}

type trafficDirectorHealthStoreStub struct {
	checkSnapshot   TrafficDirectorHealthSnapshot
	checkErr        error
	checkCalls      int
	lastCheck       TrafficDirectorHealthStoreCheckRequest
	failureSnapshot TrafficDirectorHealthSnapshot
	failureErr      error
	failureCalls    int
	lastFailure     TrafficDirectorHealthStoreFailureRequest
	successRestored bool
	successErr      error
	successCalls    int
	lastSuccess     TrafficDirectorHealthStoreSuccessRequest
}

func (s *trafficDirectorHealthStoreStub) CheckTrafficDirectorHealth(
	_ context.Context,
	request TrafficDirectorHealthStoreCheckRequest,
) (TrafficDirectorHealthSnapshot, error) {
	s.checkCalls++
	s.lastCheck = request
	if s.checkErr != nil {
		return TrafficDirectorHealthSnapshot{}, s.checkErr
	}
	return s.checkSnapshot, nil
}

func (s *trafficDirectorHealthStoreStub) RecordTrafficDirectorHealthFailure(
	_ context.Context,
	request TrafficDirectorHealthStoreFailureRequest,
) (TrafficDirectorHealthSnapshot, error) {
	s.failureCalls++
	s.lastFailure = request
	if s.failureErr != nil {
		return TrafficDirectorHealthSnapshot{}, s.failureErr
	}
	return s.failureSnapshot, nil
}

func (s *trafficDirectorHealthStoreStub) RecordTrafficDirectorHealthSuccess(
	_ context.Context,
	request TrafficDirectorHealthStoreSuccessRequest,
) (bool, error) {
	s.successCalls++
	s.lastSuccess = request
	return s.successRestored, s.successErr
}

func (s *trafficDirectorHealthStoreStub) RenewTrafficDirectorHealthProbe(
	context.Context,
	TrafficDirectorHealthStoreProbeRequest,
) (bool, error) {
	return true, nil
}

func (s *trafficDirectorHealthStoreStub) ReleaseTrafficDirectorHealthProbe(
	context.Context,
	TrafficDirectorHealthStoreProbeReleaseRequest,
) (bool, error) {
	return true, nil
}

var _ TrafficDirectorHealthStore = (*trafficDirectorHealthStoreStub)(nil)
