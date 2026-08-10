package securityaudit

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type scriptedScanner struct {
	mu      sync.Mutex
	calls   []string
	block   <-chan struct{}
	entered chan<- struct{}
}

func (s *scriptedScanner) Scan(ctx context.Context, endpoint ActiveEndpoint, _ string, _ []string) (*NormalizedResult, error) {
	s.mu.Lock()
	s.calls = append(s.calls, endpoint.ID)
	s.mu.Unlock()
	if s.entered != nil {
		select {
		case s.entered <- struct{}{}:
		default:
		}
	}
	if s.block != nil {
		select {
		case <-s.block:
		case <-ctx.Done():
			return nil, &GuardError{Code: ErrorCodeUnavailable, Retryable: true, Timeout: true, Cause: ctx.Err()}
		}
	}
	if endpoint.ID == "bad" {
		return nil, &GuardError{Code: ErrorCodeUnavailable, Retryable: true}
	}
	if endpoint.ID == "invalid" {
		return nil, &GuardError{Code: ErrorCodeInvalidResponse}
	}
	return &NormalizedResult{Decision: EventPass, RiskLevel: RiskLow, Action: ActionAllow, Safety: "Safe", ScannerScores: map[string]float64{}, ScannerEvidence: map[string]string{}, GuardEndpointID: endpoint.ID}, nil
}

func guardConfig(endpoints ...ActiveEndpoint) ActiveConfig {
	return ActiveConfig{RiskControlEnabled: true, Enabled: true, BlockingEnabled: true, ConfigVersion: 2, Scanners: AllScannerIDs, Endpoints: endpoints}
}

func TestGuardEvaluatorOrderedFailoverIncludesInvalidResponse(t *testing.T) {
	scanner := &scriptedScanner{}
	metrics := NewAtomicMetrics()
	evaluator := newGuardEvaluator(scanner, nil, metrics, 4, 2)
	snapshot := PromptSnapshot{RequestID: "r", ScanText: "hello", PromptLength: 5}
	decision, err := evaluator.Evaluate(context.Background(), guardConfig(
		ActiveEndpoint{ID: "bad", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
		ActiveEndpoint{ID: "good", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
	), snapshot)
	require.NoError(t, err)
	require.Equal(t, DecisionAllow, decision.Kind)
	require.Equal(t, int64(1), metrics.Snapshot().Failovers)
	decision, err = evaluator.Evaluate(context.Background(), guardConfig(
		ActiveEndpoint{ID: "invalid", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
		ActiveEndpoint{ID: "good", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
	), snapshot)
	require.NoError(t, err)
	require.Equal(t, DecisionAllow, decision.Kind)
	require.Equal(t, []string{"bad", "good", "invalid", "good"}, scanner.calls)
	snapshotMetrics := metrics.Snapshot()
	require.Equal(t, int64(2), snapshotMetrics.Total)
	require.Equal(t, int64(2), snapshotMetrics.Allowed)
	require.Zero(t, snapshotMetrics.Invalid)
	require.Equal(t, int64(2), snapshotMetrics.Failovers)
}

func TestGuardEvaluatorFailoverUsesAtMostOneIndependentEndpoint(t *testing.T) {
	scanner := &scriptedScanner{}
	metrics := NewAtomicMetrics()
	evaluator := newGuardEvaluator(scanner, nil, metrics, 4, 2)
	decision, err := evaluator.Evaluate(context.Background(), guardConfig(
		ActiveEndpoint{ID: "bad", BaseURL: "https://guard-a.test/v1/", Token: "shared", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
		ActiveEndpoint{ID: "duplicate", BaseURL: "https://GUARD-A.test/v1", Token: "shared", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
		ActiveEndpoint{ID: "good", BaseURL: "https://guard-b.test/v1", Token: "independent", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
		ActiveEndpoint{ID: "never", BaseURL: "https://guard-c.test/v1", Token: "third", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
	), PromptSnapshot{RequestID: "r", ScanText: "hello", PromptLength: 5})
	require.NoError(t, err)
	require.Equal(t, DecisionAllow, decision.Kind)
	require.Equal(t, []string{"bad", "good"}, scanner.calls)
	require.Equal(t, int64(1), metrics.Snapshot().Failovers)
}

func TestGuardEvaluatorPrimaryAuthenticationFailureUsesIndependentBackup(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			calls := make([]string, 0, 2)
			metrics := NewAtomicMetrics()
			evaluator := newGuardEvaluator(PromptScannerFunc(func(_ context.Context, endpoint ActiveEndpoint, _ string, _ []string) (*NormalizedResult, error) {
				calls = append(calls, endpoint.ID)
				if endpoint.ID == "primary" {
					return nil, &GuardError{Code: ErrorCodeUnavailable, HTTPStatus: status, Retryable: false}
				}
				return &NormalizedResult{
					Decision: EventPass, RiskLevel: RiskLow, Action: ActionAllow,
					ScannerScores: map[string]float64{}, ScannerEvidence: map[string]string{}, GuardEndpointID: endpoint.ID,
				}, nil
			}), nil, metrics, 2, 2)

			decision, err := evaluator.Evaluate(context.Background(), guardConfig(
				ActiveEndpoint{ID: "primary", BaseURL: "https://guard-a.test/v1", Token: "primary-token", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
				ActiveEndpoint{ID: "backup", BaseURL: "https://guard-b.test/v1", Token: "backup-token", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
			), PromptSnapshot{RequestID: "r", ScanText: "hello", PromptLength: 5})
			require.NoError(t, err)
			require.Equal(t, DecisionAllow, decision.Kind)
			require.Equal(t, []string{"primary", "backup"}, calls)
			require.Equal(t, int64(1), metrics.Snapshot().Failovers)
		})
	}
}

func TestGuardEvaluatorGlobalBulkheadIsNonBlocking(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	scanner := &scriptedScanner{block: release, entered: entered}
	metrics := NewAtomicMetrics()
	evaluator := newGuardEvaluator(scanner, nil, metrics, 1, 1)
	cfg := guardConfig(ActiveEndpoint{ID: "good", Enabled: true, TimeoutMS: 2000, InputLimit: 100})
	done := make(chan error, 1)
	go func() {
		_, err := evaluator.Evaluate(context.Background(), cfg, PromptSnapshot{ScanText: "one", PromptLength: 3})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first evaluation did not enter scanner")
	}
	start := time.Now()
	_, err := evaluator.Evaluate(context.Background(), cfg, PromptSnapshot{ScanText: "two", PromptLength: 3})
	require.Error(t, err)
	require.Less(t, time.Since(start), 200*time.Millisecond)
	require.Equal(t, int64(1), metrics.Snapshot().BulkheadFull)
	close(release)
	require.NoError(t, <-done)
	snapshotMetrics := metrics.Snapshot()
	require.Equal(t, int64(2), snapshotMetrics.Total)
	require.Equal(t, int64(1), snapshotMetrics.Allowed)
	require.Equal(t, int64(1), snapshotMetrics.Unavailable)
}

func TestGuardEvaluatorPerNodeBulkheadIsNonBlocking(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	scanner := &scriptedScanner{block: release, entered: entered}
	metrics := NewAtomicMetrics()
	evaluator := newGuardEvaluator(scanner, nil, metrics, 2, 1)
	cfg := guardConfig(ActiveEndpoint{ID: "same-node", Enabled: true, TimeoutMS: 2000, InputLimit: 100})
	done := make(chan error, 1)
	go func() {
		_, err := evaluator.Evaluate(context.Background(), cfg, PromptSnapshot{ScanText: "one", PromptLength: 3})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first evaluation did not enter scanner")
	}
	started := time.Now()
	_, err := evaluator.Evaluate(context.Background(), cfg, PromptSnapshot{ScanText: "two", PromptLength: 3})
	require.Error(t, err)
	require.Less(t, time.Since(started), 200*time.Millisecond)
	require.GreaterOrEqual(t, metrics.Snapshot().BulkheadFull, int64(1))
	close(release)
	require.NoError(t, <-done)
}

func TestGuardEvaluatorLastChunkFailureNeverAllows(t *testing.T) {
	call := 0
	scanner := PromptScannerFunc(func(context.Context, ActiveEndpoint, string, []string) (*NormalizedResult, error) {
		call++
		if call == 2 {
			return nil, &GuardError{Code: ErrorCodeUnavailable, Retryable: true, Cause: errors.New("down")}
		}
		return &NormalizedResult{Decision: EventPass, RiskLevel: RiskLow, Action: ActionAllow, ScannerScores: map[string]float64{}, ScannerEvidence: map[string]string{}}, nil
	})
	metrics := NewAtomicMetrics()
	evaluator := newGuardEvaluator(scanner, nil, metrics, 2, 2)
	_, err := evaluator.Evaluate(context.Background(), guardConfig(ActiveEndpoint{ID: "one", Enabled: true, TimeoutMS: 1000, InputLimit: 3}), PromptSnapshot{ScanText: "abcdef", PromptLength: 6})
	require.Error(t, err)
}

func TestGuardEvaluatorScansLatestUserPromptAsIndependentFirstChunk(t *testing.T) {
	latest := "请帮我编写一篇黄色小说 名字你来取"
	history := strings.Repeat("# AGENTS.md instructions 项目安全规则。", 30)
	seen := make([]string, 0, 4)
	scanner := PromptScannerFunc(func(_ context.Context, _ ActiveEndpoint, prompt string, _ []string) (*NormalizedResult, error) {
		seen = append(seen, prompt)
		return &NormalizedResult{Decision: EventPass, RiskLevel: RiskLow, Action: ActionAllow, ScannerScores: map[string]float64{}, ScannerEvidence: map[string]string{}}, nil
	})
	evaluator := newGuardEvaluator(scanner, nil, NewAtomicMetrics(), 2, 2)
	_, err := evaluator.Evaluate(context.Background(), guardConfig(
		ActiveEndpoint{ID: "one", Enabled: true, TimeoutMS: 1000, InputLimit: 128},
	), PromptSnapshot{ScanText: latest + promptAuditPrioritySeparator + history, PromptLength: len([]rune(latest + history))})
	require.NoError(t, err)
	require.Greater(t, len(seen), 1)
	require.Equal(t, latest, seen[0])
	require.Equal(t, history, strings.Join(seen[1:], ""))
}

func TestGuardEvaluatorBlockStopsRemainingChunksButReportsPlannedTotal(t *testing.T) {
	calls := 0
	scanner := PromptScannerFunc(func(context.Context, ActiveEndpoint, string, []string) (*NormalizedResult, error) {
		calls++
		return &NormalizedResult{
			Decision: EventCritical, RiskLevel: RiskCritical, Action: ActionBlock, Safety: "Unsafe",
			Categories: []string{"jailbreak"}, MatchedScanners: []string{"jailbreak"},
			ScannerScores: map[string]float64{"jailbreak": 1}, ScannerEvidence: map[string]string{"jailbreak": "Jailbreak"},
		}, nil
	})
	metrics := NewAtomicMetrics()
	evaluator := newGuardEvaluator(scanner, nil, metrics, 2, 2)
	decision, err := evaluator.Evaluate(context.Background(), guardConfig(
		ActiveEndpoint{ID: "one", Enabled: true, TimeoutMS: 1000, InputLimit: 3},
	), PromptSnapshot{ScanText: "abcdefghi", PromptLength: 9})
	require.NoError(t, err)
	require.Equal(t, DecisionBlock, decision.Kind)
	require.Equal(t, 1, calls)
	require.Equal(t, 3, decision.Result.ChunkTotal)
	require.Equal(t, int64(1), metrics.Snapshot().Blocked)
}

func TestGuardEvaluatorFlagPerAttemptDeadlineFailClosedAndContextCancel(t *testing.T) {
	t.Run("flag allows next stage", func(t *testing.T) {
		metrics := NewAtomicMetrics()
		evaluator := newGuardEvaluator(PromptScannerFunc(func(context.Context, ActiveEndpoint, string, []string) (*NormalizedResult, error) {
			return &NormalizedResult{Decision: EventFlag, RiskLevel: RiskMedium, Action: ActionWarn, Safety: "Controversial", Categories: []string{"violent"}, MatchedScanners: []string{"violent"}, ScannerScores: map[string]float64{"violent": .5}, ScannerEvidence: map[string]string{"violent": "Violent"}}, nil
		}), nil, metrics, 2, 2)
		decision, err := evaluator.Evaluate(context.Background(), guardConfig(ActiveEndpoint{ID: "one", Enabled: true, TimeoutMS: 1000, InputLimit: 100}), PromptSnapshot{ScanText: "review", PromptLength: 6})
		require.NoError(t, err)
		require.Equal(t, DecisionFlag, decision.Kind)
		require.True(t, decision.AllowNextStage)
		require.Equal(t, int64(1), metrics.Snapshot().Flagged)
	})

	t.Run("primary timeout gives independent backup its own deadline", func(t *testing.T) {
		calls := make([]string, 0, 2)
		scanner := PromptScannerFunc(func(ctx context.Context, endpoint ActiveEndpoint, _ string, _ []string) (*NormalizedResult, error) {
			calls = append(calls, endpoint.ID)
			if endpoint.ID == "first" {
				<-ctx.Done()
				return nil, &GuardError{Code: ErrorCodeUnavailable, Retryable: true, Timeout: true, Cause: ctx.Err()}
			}
			select {
			case <-ctx.Done():
				return nil, &GuardError{Code: ErrorCodeUnavailable, Retryable: true, Timeout: true, Cause: ctx.Err()}
			default:
				return &NormalizedResult{Decision: EventPass, RiskLevel: RiskLow, Action: ActionAllow, ScannerScores: map[string]float64{}, ScannerEvidence: map[string]string{}}, nil
			}
		})
		metrics := NewAtomicMetrics()
		evaluator := newGuardEvaluator(scanner, nil, metrics, 2, 2)
		decision, err := evaluator.Evaluate(context.Background(), guardConfig(
			ActiveEndpoint{ID: "first", Enabled: true, TimeoutMS: 40, InputLimit: 100},
			ActiveEndpoint{ID: "second", Enabled: true, TimeoutMS: 500, InputLimit: 100},
		), PromptSnapshot{ScanText: "deadline", PromptLength: 8})
		require.NoError(t, err)
		require.Equal(t, DecisionAllow, decision.Kind)
		require.Equal(t, []string{"first", "second"}, calls)
		require.Equal(t, int64(1), metrics.Snapshot().Failovers)
	})

	t.Run("each chunk receives a fresh endpoint deadline", func(t *testing.T) {
		calls := 0
		evaluator := newGuardEvaluator(PromptScannerFunc(func(ctx context.Context, _ ActiveEndpoint, _ string, _ []string) (*NormalizedResult, error) {
			calls++
			select {
			case <-time.After(60 * time.Millisecond):
				return &NormalizedResult{Decision: EventPass, RiskLevel: RiskLow, Action: ActionAllow, ScannerScores: map[string]float64{}, ScannerEvidence: map[string]string{}}, nil
			case <-ctx.Done():
				return nil, &GuardError{Code: ErrorCodeUnavailable, Retryable: true, Timeout: true, Cause: ctx.Err()}
			}
		}), nil, NewAtomicMetrics(), 2, 2)
		decision, err := evaluator.Evaluate(context.Background(), guardConfig(
			ActiveEndpoint{ID: "one", Enabled: true, TimeoutMS: 100, InputLimit: 3},
		), PromptSnapshot{ScanText: "abcdef", PromptLength: 6})
		require.NoError(t, err)
		require.Equal(t, DecisionAllow, decision.Kind)
		require.Equal(t, 2, calls)
	})

	t.Run("block is terminal and never fails over", func(t *testing.T) {
		calls := make([]string, 0, 1)
		evaluator := newGuardEvaluator(PromptScannerFunc(func(_ context.Context, endpoint ActiveEndpoint, _ string, _ []string) (*NormalizedResult, error) {
			calls = append(calls, endpoint.ID)
			return &NormalizedResult{Decision: EventCritical, RiskLevel: RiskCritical, Action: ActionBlock, ScannerScores: map[string]float64{}, ScannerEvidence: map[string]string{}}, nil
		}), nil, NewAtomicMetrics(), 2, 2)
		decision, err := evaluator.Evaluate(context.Background(), guardConfig(
			ActiveEndpoint{ID: "first", Enabled: true, TimeoutMS: 100, InputLimit: 100},
			ActiveEndpoint{ID: "second", Enabled: true, TimeoutMS: 100, InputLimit: 100},
		), PromptSnapshot{ScanText: "blocked", PromptLength: 7})
		require.NoError(t, err)
		require.Equal(t, DecisionBlock, decision.Kind)
		require.Equal(t, []string{"first"}, calls)
	})

	t.Run("invalid scanner response at deadline fails over", func(t *testing.T) {
		calls := make([]string, 0, 2)
		evaluator := newGuardEvaluator(PromptScannerFunc(func(ctx context.Context, endpoint ActiveEndpoint, _ string, _ []string) (*NormalizedResult, error) {
			calls = append(calls, endpoint.ID)
			if endpoint.ID == "first" {
				<-ctx.Done()
				return nil, &GuardError{Code: ErrorCodeInvalidResponse, Retryable: false}
			}
			return &NormalizedResult{Decision: EventPass, RiskLevel: RiskLow, Action: ActionAllow, ScannerScores: map[string]float64{}, ScannerEvidence: map[string]string{}}, nil
		}), nil, NewAtomicMetrics(), 2, 2)
		decision, err := evaluator.Evaluate(context.Background(), guardConfig(
			ActiveEndpoint{ID: "first", Enabled: true, TimeoutMS: 20, InputLimit: 100},
			ActiveEndpoint{ID: "second", Enabled: true, TimeoutMS: 100, InputLimit: 100},
		), PromptSnapshot{ScanText: "invalid", PromptLength: 7})
		require.NoError(t, err)
		require.Equal(t, DecisionAllow, decision.Kind)
		require.Equal(t, []string{"first", "second"}, calls)
	})

	t.Run("canceled parent never allows", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		evaluator := newGuardEvaluator(PromptScannerFunc(func(ctx context.Context, _ ActiveEndpoint, _ string, _ []string) (*NormalizedResult, error) {
			<-ctx.Done()
			return nil, &GuardError{Code: ErrorCodeUnavailable, Retryable: true, Cause: ctx.Err()}
		}), nil, NewAtomicMetrics(), 2, 2)
		decision, err := evaluator.Evaluate(ctx, guardConfig(ActiveEndpoint{ID: "one", Enabled: true, TimeoutMS: 1000, InputLimit: 100}), PromptSnapshot{ScanText: "cancel", PromptLength: 6})
		require.Error(t, err)
		require.Nil(t, decision)
	})
}

func TestGuardEvaluatorRecordsExistingResultOnceAndRecordFailureDoesNotChangeDecision(t *testing.T) {
	for _, recordErr := range []error{nil, errors.New("database unavailable")} {
		repo := &fakeJobRepository{recordBlockingErr: recordErr}
		metrics := NewAtomicMetrics()
		scannerCalls := 0
		evaluator := newGuardEvaluator(PromptScannerFunc(func(context.Context, ActiveEndpoint, string, []string) (*NormalizedResult, error) {
			scannerCalls++
			return &NormalizedResult{Decision: EventCritical, RiskLevel: RiskCritical, Action: ActionBlock, Safety: "Unsafe", Categories: []string{"pii"}, MatchedScanners: []string{"pii"}, ScannerScores: map[string]float64{"pii": 1}, ScannerEvidence: map[string]string{"pii": "PII"}}, nil
		}), repo, metrics, 2, 2)
		decision, err := evaluator.Evaluate(context.Background(), guardConfig(ActiveEndpoint{ID: "one", Enabled: true, TimeoutMS: 1000, InputLimit: 100}), PromptSnapshot{ScanText: "raw prompt", RedactedPreview: "raw***", PromptLength: 10})
		require.NoError(t, err)
		require.Equal(t, DecisionBlock, decision.Kind)
		require.Equal(t, 1, scannerCalls)
		require.Equal(t, 1, repo.recordBlockingCalls)
		require.Empty(t, repo.recordBlockingSnapshot.ScanText)
		require.Same(t, decision.Result, repo.recordBlockingResult)
		if recordErr != nil {
			require.Equal(t, int64(1), metrics.Snapshot().RecordFailed)
		} else {
			require.Zero(t, metrics.Snapshot().RecordFailed)
		}
	}
}

func TestGuardEvaluatorNilResultAndScannerPanicBecomeStableFailures(t *testing.T) {
	tests := []struct {
		name string
		scan PromptScannerFunc
		code string
	}{
		{name: "nil result", scan: func(context.Context, ActiveEndpoint, string, []string) (*NormalizedResult, error) { return nil, nil }, code: ErrorCodeInvalidResponse},
		{name: "panic", scan: func(context.Context, ActiveEndpoint, string, []string) (*NormalizedResult, error) {
			panic("raw prompt canary")
		}, code: ErrorCodeUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evaluator := newGuardEvaluator(tt.scan, nil, NewAtomicMetrics(), 2, 2)
			_, err := evaluator.Evaluate(context.Background(), guardConfig(ActiveEndpoint{ID: "one", Enabled: true, TimeoutMS: 1000, InputLimit: 100}), PromptSnapshot{ScanText: "input", PromptLength: 5})
			var guardErr *GuardError
			require.ErrorAs(t, err, &guardErr)
			require.Equal(t, tt.code, guardErr.Code)
			require.NotContains(t, err.Error(), "canary")
		})
	}
}

func TestGuardEvaluatorScannerPanicFailsOverOnce(t *testing.T) {
	t.Run("backup allow", func(t *testing.T) {
		calls := make([]string, 0, 2)
		metrics := NewAtomicMetrics()
		evaluator := newGuardEvaluator(PromptScannerFunc(func(_ context.Context, endpoint ActiveEndpoint, _ string, _ []string) (*NormalizedResult, error) {
			calls = append(calls, endpoint.ID)
			if endpoint.ID == "primary" {
				panic("primary scanner panic")
			}
			return &NormalizedResult{
				Decision: EventPass, RiskLevel: RiskLow, Action: ActionAllow,
				ScannerScores: map[string]float64{}, ScannerEvidence: map[string]string{}, GuardEndpointID: endpoint.ID,
			}, nil
		}), nil, metrics, 2, 2)

		decision, err := evaluator.Evaluate(context.Background(), guardConfig(
			ActiveEndpoint{ID: "primary", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
			ActiveEndpoint{ID: "backup", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
		), PromptSnapshot{ScanText: "input", PromptLength: 5})
		require.NoError(t, err)
		require.Equal(t, DecisionAllow, decision.Kind)
		require.Equal(t, []string{"primary", "backup"}, calls)
		require.Equal(t, int64(1), metrics.Snapshot().Failovers)
	})

	t.Run("backup unavailable", func(t *testing.T) {
		calls := make([]string, 0, 2)
		metrics := NewAtomicMetrics()
		evaluator := newGuardEvaluator(PromptScannerFunc(func(_ context.Context, endpoint ActiveEndpoint, _ string, _ []string) (*NormalizedResult, error) {
			calls = append(calls, endpoint.ID)
			panic("scanner panic")
		}), nil, metrics, 2, 2)

		decision, err := evaluator.Evaluate(context.Background(), guardConfig(
			ActiveEndpoint{ID: "primary", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
			ActiveEndpoint{ID: "backup", Enabled: true, TimeoutMS: 1000, InputLimit: 100},
		), PromptSnapshot{ScanText: "input", PromptLength: 5})
		var guardErr *GuardError
		require.ErrorAs(t, err, &guardErr)
		require.Equal(t, ErrorCodeUnavailable, guardErr.Code)
		require.Nil(t, decision)
		require.Equal(t, []string{"primary", "backup"}, calls)
		require.Equal(t, int64(1), metrics.Snapshot().Failovers)
		require.NotContains(t, err.Error(), "scanner panic")
	})
}

type PromptScannerFunc func(context.Context, ActiveEndpoint, string, []string) (*NormalizedResult, error)

func (f PromptScannerFunc) Scan(ctx context.Context, endpoint ActiveEndpoint, chunk string, scanners []string) (*NormalizedResult, error) {
	return f(ctx, endpoint, chunk, scanners)
}
