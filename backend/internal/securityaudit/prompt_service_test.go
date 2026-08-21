package securityaudit

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/stretchr/testify/require"
)

type staticSettingRepository struct {
	values map[string]string
}

func (r staticSettingRepository) Get(context.Context, string) (*service.Setting, error) {
	return nil, service.ErrSettingNotFound
}
func (r staticSettingRepository) GetValue(context.Context, string) (string, error) {
	return "", service.ErrSettingNotFound
}
func (r staticSettingRepository) Set(context.Context, string, string) error { return nil }
func (r staticSettingRepository) GetMultiple(_ context.Context, keys []string) (map[string]string, error) {
	result := make(map[string]string, len(keys))
	for _, key := range keys {
		result[key] = r.values[key]
	}
	return result, nil
}
func (r staticSettingRepository) SetMultiple(context.Context, map[string]string) error { return nil }
func (r staticSettingRepository) GetAll(context.Context) (map[string]string, error) {
	return r.values, nil
}
func (r staticSettingRepository) Delete(context.Context, string) error { return nil }

func TestPromptServiceHasExplicitIdempotentLifecycle(t *testing.T) {
	config := NewConfigManager(nil, staticSettingRepository{values: map[string]string{
		SettingKeyPromptAuditConfig: "",
		SettingKeyRiskControl:       "false",
	}}, nil, prefixEncryptor{}, testTotpKeyConfig())
	service := NewPromptService(
		config,
		NewPostgreSQLRepository(nil),
		NewRedisPayloadStore(nil),
		NewOpenAICompatibleScanner(),
		NewAtomicMetrics(),
	)

	require.Nil(t, service.cancel, "construction must not start background work")
	require.NoError(t, service.Start(context.Background()))
	require.NotNil(t, service.cancel)
	require.NoError(t, service.Start(context.Background()), "Start must be idempotent")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, service.Shutdown(ctx))
	require.Nil(t, service.cancel)
	require.NoError(t, service.Shutdown(ctx), "Shutdown must be idempotent")
}

func TestPromptServiceStartReportsDependencyFailureWithoutPanic(t *testing.T) {
	service := &PromptService{}
	require.Error(t, service.Start(context.Background()))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.NoError(t, service.Shutdown(ctx))
}

func TestPromptServiceBlockingLatestTurnOnlyUsesNarrowSnapshot(t *testing.T) {
	seen := make([]string, 0, 2)
	evaluator := newGuardEvaluator(PromptScannerFunc(func(_ context.Context, _ ActiveEndpoint, chunk string, _ []string) (*NormalizedResult, error) {
		seen = append(seen, chunk)
		return &NormalizedResult{Decision: EventPass, RiskLevel: RiskLow, Action: ActionAllow, ScannerScores: map[string]float64{}, ScannerEvidence: map[string]string{}}, nil
	}), nil, NewAtomicMetrics(), 2, 2)
	service := &PromptService{
		config: &fakeConfigStore{active: true, cfg: ActiveConfig{
			RiskControlEnabled: true, Enabled: true, BlockingEnabled: true, BlockingLatestTurnOnly: true, AllGroups: true,
			Scanners: AllScannerIDs, Endpoints: []ActiveEndpoint{{ID: "guard-1", Enabled: true, TimeoutMS: 1000, InputLimit: 4096}},
		}},
		evaluator: evaluator,
	}
	decision, err := service.Evaluate(context.Background(), Request{Protocol: "openai_chat_completions", Body: []byte(`{"messages":[{"role":"system","content":"system instruction"},{"role":"user","content":"older user input"},{"role":"assistant","content":"previous output"},{"role":"user","content":"latest user input"}]}`)})
	require.NoError(t, err)
	require.Equal(t, DecisionAllow, decision.Kind)
	require.Equal(t, []string{"latest user input", "previous output"}, seen)
}

func TestPromptServiceGroupScopeCannotExemptRequiredBlockingProof(t *testing.T) {
	groupID := int64(42)
	otherGroupID := int64(43)
	scannerCalls := 0
	evaluator := newGuardEvaluator(PromptScannerFunc(func(_ context.Context, _ ActiveEndpoint, _ string, _ []string) (*NormalizedResult, error) {
		scannerCalls++
		return &NormalizedResult{Decision: EventPass, RiskLevel: RiskLow, Action: ActionAllow}, nil
	}), nil, NewAtomicMetrics(), 2, 2)
	service := &PromptService{
		config: &fakeConfigStore{active: true, cfg: ActiveConfig{
			RiskControlEnabled: true, Enabled: true, BlockingEnabled: true,
			GroupIDs: []int64{groupID}, Scanners: AllScannerIDs,
			Endpoints: []ActiveEndpoint{{ID: "guard-1", Enabled: true, TimeoutMS: 1000, InputLimit: 4096}},
		}},
		evaluator: evaluator,
	}
	body := []byte(`{"messages":[{"role":"user","content":"scope canary"}]}`)

	decision, err := service.Evaluate(context.Background(), Request{
		Protocol: "openai_chat_completions", Body: body, GroupID: &otherGroupID,
	})
	require.NoError(t, err)
	require.Equal(t, DecisionAllow, decision.Kind)
	require.Zero(t, scannerCalls, "ordinary out-of-scope requests retain the configured group filter")

	decision, err = service.Evaluate(context.Background(), Request{
		Protocol: "openai_chat_completions", Body: body, GroupID: &otherGroupID, RequireBlocking: true,
	})
	require.Nil(t, decision)
	var guardErr *GuardError
	require.ErrorAs(t, err, &guardErr)
	require.Equal(t, ErrorCodeUnavailable, guardErr.Code)
	require.Zero(t, scannerCalls, "out-of-scope configuration is unavailable, not a Pro scan proof")
}

func TestPromptServiceCanonicalTextReachesBlockingScanner(t *testing.T) {
	tests := []struct {
		name       string
		protocol   securityadmission.Protocol
		body       string
		want       []string
		doNotWant  []string
		lineage    securityadmission.LineageTrust
		latestOnly bool
	}{
		{
			name:     "responses all current input items",
			protocol: securityadmission.ProtocolOpenAIResponses,
			body:     `{"previous_response_id":"resp_previous","instructions":"root-canary","input":[{"type":"message","role":"user","content":"earlier-current-canary"},{"type":"function_call","arguments":{"query":"arguments-canary"}},{"type":"function_call_output","output":{"result":"output-canary"}}]}`,
			want:     []string{"root-canary", "earlier-current-canary", "arguments-canary", "output-canary"},
			lineage:  securityadmission.LineageTrusted, latestOnly: true,
		},
		{
			name:     "responses websocket all current input items",
			protocol: securityadmission.ProtocolResponsesWebSocket,
			body:     `{"type":"response.create","previous_response_id":"resp_previous","input":[{"role":"user","content":"ws-earlier-current-canary"},{"role":"user","content":"ws-later-current-canary"}]}`,
			want:     []string{"ws-earlier-current-canary", "ws-later-current-canary"},
			lineage:  securityadmission.LineageTrusted, latestOnly: true,
		},
		{
			name:     "chat consecutive tool suffix",
			protocol: securityadmission.ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"system","content":"system-canary"},{"role":"user","content":"old-user"},{"role":"assistant","tool_calls":[{"type":"function","function":{"name":"lookup","arguments":"arguments-canary"}}]},{"role":"tool","tool_call_id":"call-1","content":"tool-output-canary"},{"role":"function","name":"lookup","content":"function-output-canary"}]}`,
			want:     []string{"system-canary", "arguments-canary", "tool-output-canary", "function-output-canary"}, doNotWant: []string{"old-user"},
			lineage: securityadmission.LineageTrusted, latestOnly: true,
		},
		{
			name:     "anthropic tool result",
			protocol: securityadmission.ProtocolAnthropicMessages,
			body:     `{"system":"system-canary","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"call-1","name":"lookup","input":{"query":"tool-input-canary"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1","content":[{"type":"text","text":"tool-result-canary"}]}]}]}`,
			want:     []string{"system-canary", "tool-input-canary", "tool-result-canary"},
			lineage:  securityadmission.LineageTrusted, latestOnly: true,
		},
		{
			name:     "untrusted replay stays full",
			protocol: securityadmission.ProtocolOpenAIChat,
			body:     `{"messages":[{"role":"user","content":"old-user-canary"},{"role":"assistant","content":"assistant-canary"},{"role":"user","content":"latest-user-canary"}]}`,
			want:     []string{"old-user-canary", "assistant-canary", "latest-user-canary"},
			lineage:  securityadmission.LineageUntrusted, latestOnly: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := []byte(test.body)
			admission, err := securityadmission.Classify(string(test.protocol), body, securityadmission.Options{Lineage: test.lineage})
			require.NoError(t, err)
			require.Equal(t, securityadmission.RequestAuditableText, admission.Class())

			var scannerText []string
			evaluator := newGuardEvaluator(PromptScannerFunc(func(_ context.Context, _ ActiveEndpoint, chunk string, _ []string) (*NormalizedResult, error) {
				scannerText = append(scannerText, chunk)
				return &NormalizedResult{Decision: EventPass, RiskLevel: RiskLow, Action: ActionAllow}, nil
			}), nil, NewAtomicMetrics(), 2, 2)
			promptService := &PromptService{
				config: &fakeConfigStore{active: true, cfg: ActiveConfig{
					RiskControlEnabled: true, Enabled: true, BlockingEnabled: true,
					BlockingLatestTurnOnly: test.latestOnly, AllGroups: true, Scanners: AllScannerIDs,
					Endpoints: []ActiveEndpoint{{ID: "guard-1", Enabled: true, TimeoutMS: 1000, InputLimit: 16_384}},
				}},
				evaluator: evaluator,
			}

			decision, err := promptService.Evaluate(context.Background(), Request{
				Protocol: string(test.protocol), Body: body, Admission: &admission, RequireBlocking: true,
			})
			require.NoError(t, err)
			require.Equal(t, DecisionAllow, decision.Kind)
			require.Len(t, scannerText, 1, "the bounded corpus fits one real scanner call")
			for _, want := range test.want {
				require.Contains(t, scannerText[0], want)
			}
			for _, omitted := range test.doNotWant {
				require.NotContains(t, scannerText[0], omitted)
			}
		})
	}
}

func TestPromptServiceRejectsInvalidDeleteConfirmationClaims(t *testing.T) {
	now := time.Date(2026, 7, 16, 10, 0, 0, 0, time.UTC)
	start, end := now.Add(-time.Hour), now.Add(time.Hour)
	filter := EventFilter{Decision: string(EventCritical), StartAt: &start, EndAt: &end}
	const snapshotMaxID int64 = 10
	filterHash := FilterHash(filter, snapshotMaxID)
	validClaims := deleteClaims{
		FilterHash: filterHash, SnapshotMaxID: snapshotMaxID, AdminID: 7,
		IssuedAt: now, ExpiresAt: now.Add(5 * time.Minute),
	}
	claimsToken := func(claims deleteClaims) string {
		raw, err := json.Marshal(claims)
		require.NoError(t, err)
		return string(raw)
	}
	validRequest := DeleteByFilterRequest{
		Filter: filter, SnapshotMaxID: snapshotMaxID, FilterHash: filterHash,
		ConfirmationToken: claimsToken(validClaims), Confirm: true,
	}

	tests := []struct {
		name    string
		request DeleteByFilterRequest
		adminID int64
	}{
		{name: "confirm false", request: func() DeleteByFilterRequest { value := validRequest; value.Confirm = false; return value }(), adminID: 7},
		{name: "malformed token", request: func() DeleteByFilterRequest {
			value := validRequest
			value.ConfirmationToken = "not-json"
			return value
		}(), adminID: 7},
		{name: "different administrator", request: validRequest, adminID: 8},
		{name: "filter hash mismatch", request: func() DeleteByFilterRequest {
			value := validRequest
			value.FilterHash = strings.Repeat("b", 64)
			return value
		}(), adminID: 7},
		{name: "snapshot mismatch", request: func() DeleteByFilterRequest { value := validRequest; value.SnapshotMaxID++; return value }(), adminID: 7},
		{name: "expired", request: func() DeleteByFilterRequest {
			value := validRequest
			claims := validClaims
			claims.ExpiresAt = now
			value.ConfirmationToken = claimsToken(claims)
			return value
		}(), adminID: 7},
	}

	service := &PromptService{config: &fakeConfigStore{}, clock: fixedClock{now: now}}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := service.DeleteByFilter(context.Background(), test.request, test.adminID)
			require.Error(t, err)
			require.Nil(t, result)
		})
	}
}
