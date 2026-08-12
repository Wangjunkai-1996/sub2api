package securityaudit

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
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

func TestPromptServiceBlockingAlwaysUsesCompleteSnapshot(t *testing.T) {
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
	decision, err := service.Evaluate(context.Background(), Request{Protocol: "openai_chat_completions", Model: "gpt-5.6-terra", Body: []byte(`{"messages":[{"role":"system","content":"system instruction"},{"role":"user","content":"older user input"},{"role":"assistant","content":"previous output"},{"role":"user","content":"latest user input"}]}`)})
	require.NoError(t, err)
	require.Equal(t, DecisionAllow, decision.Kind)
	require.Equal(t, []string{"latest user input", "system instruction\n\nolder user input\n\nprevious output"}, seen)
}

func TestPromptServiceBlockingAllowsStrictImageOnlyWithoutScanning(t *testing.T) {
	scannerCalled := false
	evaluator := newGuardEvaluator(PromptScannerFunc(func(_ context.Context, _ ActiveEndpoint, _ string, _ []string) (*NormalizedResult, error) {
		scannerCalled = true
		return nil, nil
	}), nil, NewAtomicMetrics(), 2, 2)
	service := &PromptService{
		config: &fakeConfigStore{active: true, cfg: ActiveConfig{
			RiskControlEnabled: true, Enabled: true, BlockingEnabled: true, AllGroups: true,
			Scanners: AllScannerIDs, Endpoints: []ActiveEndpoint{{ID: "guard-1", Enabled: true, TimeoutMS: 1000, InputLimit: 4096}},
		}},
		evaluator: evaluator,
	}
	body := []byte(`{"input":[{"type":"input_image","image_url":"not-validated-by-text-audit"}]}`)
	document := auditinput.ParseForTextAudit(auditinput.ProtocolOpenAIResponses, body)
	require.True(t, document.Complete, "%+v", document.Issues)
	require.True(t, document.HasImages)
	require.Empty(t, document.Media)

	decision, err := service.Evaluate(context.Background(), Request{
		Strict: true, Protocol: auditinput.ProtocolOpenAIResponses, Model: "gpt-5.6-terra", Body: body, Document: document,
	})

	require.NoError(t, err)
	require.NotNil(t, decision)
	require.Equal(t, DecisionAllow, decision.Kind)
	require.True(t, decision.AllowNextStage)
	require.False(t, scannerCalled)
}

func TestPromptServiceStrictImageTurnDoesNotFallBackToHistoricalUserText(t *testing.T) {
	tests := []struct {
		protocol string
		body     string
	}{
		{
			protocol: auditinput.ProtocolOpenAIChat,
			body: `{"messages":[
				{"role":"user","content":"historical user text"},
				{"role":"assistant","content":"assistant output"},
				{"role":"user","content":[{"type":"image_url","image_url":{"opaque":true}}]}
			]}`,
		},
		{
			protocol: auditinput.ProtocolOpenAIResponses,
			body: `{"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"historical user text"}]},
				{"type":"message","role":"assistant","content":[{"type":"output_text","text":"assistant output"}]},
				{"type":"input_image","image_url":{"opaque":true}}
			]}`,
		},
	}
	for _, test := range tests {
		t.Run(test.protocol, func(t *testing.T) {
			scannerCalled := false
			evaluator := newGuardEvaluator(PromptScannerFunc(func(context.Context, ActiveEndpoint, string, []string) (*NormalizedResult, error) {
				scannerCalled = true
				return nil, nil
			}), nil, NewAtomicMetrics(), 2, 2)
			promptService := &PromptService{
				config: &fakeConfigStore{active: true, cfg: ActiveConfig{
					RiskControlEnabled: true, Enabled: true, BlockingEnabled: true, AllGroups: true,
					Scanners: AllScannerIDs, Endpoints: []ActiveEndpoint{{ID: "guard-1", Enabled: true, TimeoutMS: 1000, InputLimit: 4096}},
				}},
				evaluator: evaluator,
			}
			body := []byte(test.body)
			document := auditinput.ParseForTextAudit(test.protocol, body)
			require.True(t, document.Complete, "%+v", document.Issues)
			require.True(t, document.HasImages)
			require.Empty(t, document.NormalizedText)

			decision, err := promptService.Evaluate(context.Background(), Request{
				Strict: true, Protocol: test.protocol, Model: "gpt-5.6-terra", Body: body, Document: document,
			})

			require.NoError(t, err)
			require.Equal(t, DecisionAllow, decision.Kind)
			require.True(t, decision.AllowNextStage)
			require.False(t, scannerCalled)
		})
	}
}

func TestPromptServiceStrictImageOnlyAllowsBeforeDependencies(t *testing.T) {
	body := []byte(`{"input":[{"type":"input_image","image_url":"opaque"},{"type":"compaction_trigger"}]}`)
	document := auditinput.ParseForTextAudit(auditinput.ProtocolOpenAIResponses, body)
	require.True(t, document.Complete, "%+v", document.Issues)
	require.True(t, document.HasImages)
	req := Request{
		Strict: true, Protocol: auditinput.ProtocolOpenAIResponses, Model: "gpt-5.6-terra", Body: body, Document: document,
	}

	var promptService *PromptService
	decision, err := promptService.Evaluate(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, DecisionAllow, decision.Kind)
	require.True(t, decision.AllowNextStage)
	require.NoError(t, promptService.Enqueue(context.Background(), req))
}

func TestPromptServiceStrictAsyncUsesDurableQueue(t *testing.T) {
	cfg := asyncConfig()
	config := &fakeConfigStore{active: true, cfg: cfg}
	repo := &fakeJobRepository{createJob: &Job{ID: 46}}
	payload := &fakePayloadStore{values: map[int64]string{}}
	promptService := &PromptService{
		config: config, enqueuer: NewEnqueuer(config, repo, payload), metrics: NewAtomicMetrics(),
		background: context.Background(), enqueueSlots: make(chan struct{}, 1),
	}
	body := []byte(`{"messages":[
		{"role":"system","content":"system instruction"},
		{"role":"user","content":"historical user"},
		{"role":"assistant","content":"assistant output"},
		{"role":"tool","content":"tool output"},
		{"role":"user","content":"latest user"}
	]}`)
	document := auditinput.ParseForTextAudit(auditinput.ProtocolOpenAIChat, body)
	require.True(t, document.Complete, "%+v", document.Issues)

	require.NoError(t, promptService.Enqueue(context.Background(), Request{
		Strict: true, Protocol: auditinput.ProtocolOpenAIChat, Model: "gpt-5.6-terra", Body: body, Document: document,
		AuditContext: strings.Repeat("full historical context", 1000),
	}))
	require.Eventually(t, func() bool {
		repo.mu.Lock()
		attempts := repo.createdAttempts
		repo.mu.Unlock()
		payload.mu.Lock()
		text := payload.values[46]
		payload.mu.Unlock()
		return attempts == strictAsyncMaxAttempts && text == "latest user"
	}, time.Second, 10*time.Millisecond)
}

func TestPromptServiceStrictBlockingUsesOneBoundedPrimaryScan(t *testing.T) {
	type scanCall struct {
		endpoint string
		text     string
	}
	calls := make([]scanCall, 0, 1)
	evaluator := newGuardEvaluator(PromptScannerFunc(func(_ context.Context, endpoint ActiveEndpoint, chunk string, _ []string) (*NormalizedResult, error) {
		calls = append(calls, scanCall{endpoint: endpoint.ID, text: chunk})
		return &NormalizedResult{
			Decision: EventPass, RiskLevel: RiskLow, Action: ActionAllow,
			ScannerScores: map[string]float64{}, ScannerEvidence: map[string]string{}, GuardEndpointID: endpoint.ID,
		}, nil
	}), nil, NewAtomicMetrics(), 2, 2)
	promptService := &PromptService{
		config: &fakeConfigStore{active: true, cfg: ActiveConfig{
			RiskControlEnabled: true, Enabled: true, BlockingEnabled: true, AllGroups: true,
			Scanners: AllScannerIDs, Endpoints: []ActiveEndpoint{
				{ID: "primary", Enabled: true, TimeoutMS: 1000, InputLimit: MaxInputLimit},
				{ID: "backup", Enabled: true, TimeoutMS: 1000, InputLimit: MaxInputLimit},
			},
		}},
		evaluator: evaluator,
	}
	latest := "current:" + strings.Repeat("界", StrictPromptGuardMaxRunes+100)
	body := []byte(`{"messages":[
		{"role":"system","content":"system instruction"},
		{"role":"user","content":"older user"},
		{"role":"assistant","content":"assistant output"},
		{"role":"tool","content":"tool output"},
		{"role":"user","content":` + string(mustJSON(t, latest)) + `}
	]}`)
	document := auditinput.ParseForTextAudit(auditinput.ProtocolOpenAIChat, body)
	require.True(t, document.Complete, "%+v", document.Issues)

	decision, err := promptService.Evaluate(context.Background(), Request{
		Strict: true, Protocol: auditinput.ProtocolOpenAIChat, Model: "gpt-5.6-terra", Body: body, Document: document,
		AuditContext: strings.Repeat("full lineage context", 1000),
	})

	require.NoError(t, err)
	require.Equal(t, DecisionAllow, decision.Kind)
	require.Equal(t, []scanCall{{endpoint: "primary", text: hardLimitRunes(latest, StrictPromptGuardMaxRunes)}}, calls)
	require.Equal(t, StrictPromptGuardMaxRunes, len([]rune(calls[0].text)))
}

func TestPromptServiceStrictBlockingDoesNotFailOverGuardScan(t *testing.T) {
	calls := make([]string, 0, 1)
	evaluator := newGuardEvaluator(PromptScannerFunc(func(_ context.Context, endpoint ActiveEndpoint, _ string, _ []string) (*NormalizedResult, error) {
		calls = append(calls, endpoint.ID)
		return nil, &GuardError{Code: ErrorCodeUnavailable, Retryable: true}
	}), nil, NewAtomicMetrics(), 2, 2)
	promptService := &PromptService{
		config: &fakeConfigStore{active: true, cfg: ActiveConfig{
			RiskControlEnabled: true, Enabled: true, BlockingEnabled: true, AllGroups: true,
			Scanners: AllScannerIDs, Endpoints: []ActiveEndpoint{
				{ID: "primary", Enabled: true, TimeoutMS: 1000, InputLimit: 4096},
				{ID: "backup", Enabled: true, TimeoutMS: 1000, InputLimit: 4096},
			},
		}},
		evaluator: evaluator,
	}

	decision, err := promptService.Evaluate(context.Background(), Request{
		Strict: true, Protocol: auditinput.ProtocolOpenAIResponses, Model: "gpt-5.6-terra", Body: []byte(`{"input":"current user text"}`),
	})

	require.Error(t, err)
	require.Nil(t, decision)
	require.Equal(t, []string{"primary"}, calls)
}

func TestPromptServiceStrictControlAndImageIsAllowedWithoutScanning(t *testing.T) {
	scannerCalled := false
	evaluator := newGuardEvaluator(PromptScannerFunc(func(context.Context, ActiveEndpoint, string, []string) (*NormalizedResult, error) {
		scannerCalled = true
		return nil, nil
	}), nil, NewAtomicMetrics(), 2, 2)
	promptService := &PromptService{
		config: &fakeConfigStore{active: true, cfg: ActiveConfig{
			RiskControlEnabled: true, Enabled: true, BlockingEnabled: true, AllGroups: true,
			Scanners: AllScannerIDs, Endpoints: []ActiveEndpoint{{ID: "guard-1", Enabled: true, TimeoutMS: 1000, InputLimit: 4096}},
		}},
		evaluator: evaluator,
	}
	body := []byte(`{"input":[{"type":"input_image","image_url":"opaque"},{"type":"compaction_trigger"}]}`)
	document := auditinput.ParseForTextAudit(auditinput.ProtocolOpenAIResponses, body)
	require.True(t, document.Complete, "%+v", document.Issues)
	require.True(t, document.HasImages)
	require.NotEmpty(t, document.ControlItems)

	decision, err := promptService.Evaluate(context.Background(), Request{
		Strict: true, Protocol: auditinput.ProtocolOpenAIResponses, Model: "gpt-5.6-terra", Body: body, Document: document,
	})

	require.NoError(t, err)
	require.NotNil(t, decision)
	require.Equal(t, DecisionAllow, decision.Kind)
	require.True(t, decision.AllowNextStage)
	require.False(t, scannerCalled)
}

func TestPromptServiceStrictCompleteNonUserTurnsAllowWithoutScanning(t *testing.T) {
	scannerCalls := 0
	evaluator := newGuardEvaluator(PromptScannerFunc(func(context.Context, ActiveEndpoint, string, []string) (*NormalizedResult, error) {
		scannerCalls++
		return nil, nil
	}), nil, NewAtomicMetrics(), 2, 2)
	promptService := &PromptService{
		config: &fakeConfigStore{active: true, cfg: ActiveConfig{
			RiskControlEnabled: true, Enabled: true, BlockingEnabled: true, AllGroups: true,
			Scanners: AllScannerIDs, Endpoints: []ActiveEndpoint{{ID: "guard-1", Enabled: true, TimeoutMS: 1000, InputLimit: 4096}},
		}},
		evaluator: evaluator,
	}
	tests := []struct {
		name     string
		protocol string
		body     string
	}{
		{name: "control only", protocol: auditinput.ProtocolOpenAIResponses, body: `{"input":[{"type":"compaction_trigger"}]}`},
		{name: "responses tool output", protocol: auditinput.ProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","content":"historical user"},{"type":"function_call_output","call_id":"call_1","output":"ok"}]}`},
		{name: "responses missing input", protocol: auditinput.ProtocolOpenAIResponses, body: `{"model":"gpt-5.6-terra"}`},
		{name: "chat tool output", protocol: auditinput.ProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":"historical user"},{"role":"tool","content":"ok"}]}`},
		{name: "chat missing messages", protocol: auditinput.ProtocolOpenAIChat, body: `{"model":"gpt-5.6-terra"}`},
		{name: "chat empty messages", protocol: auditinput.ProtocolOpenAIChat, body: `{"model":"gpt-5.6-terra","messages":[]}`},
		{name: "empty websocket turn", protocol: auditinput.ProtocolOpenAIResponses, body: `{"type":"response.create","model":"gpt-5.6-terra","input":[]}`},
		{name: "empty continuation", protocol: auditinput.ProtocolOpenAIResponses, body: `{"model":"gpt-5.6-terra","previous_response_id":"resp_parent","input":null}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := []byte(test.body)
			document := auditinput.ParseForTextAudit(test.protocol, body)
			require.True(t, document.Complete, "%+v", document.Issues)
			require.Empty(t, document.NormalizedText)
			require.NotEmpty(t, document.ControlItems)

			decision, err := promptService.Evaluate(context.Background(), Request{
				Strict: true, Protocol: test.protocol, Model: "gpt-5.6-terra", Body: body, Document: document,
			})

			require.NoError(t, err)
			require.NotNil(t, decision)
			require.Equal(t, DecisionAllow, decision.Kind)
			require.True(t, decision.AllowNextStage)
			require.Equal(t, 0, scannerCalls)
		})
	}
}

func TestPromptServiceBlockingScopeNeverExpandsWithoutTrustedConfig(t *testing.T) {
	blocking := ModeBlocking
	service := &PromptService{config: &fakeConfigStore{active: false, effectiveMode: &blocking}}
	group12, group13 := int64(12), int64(13)
	require.False(t, service.BlockingApplies(Request{GroupID: &group12, Protocol: auditinput.ProtocolOpenAIResponses, Model: "gpt-5.6-terra"}))
	require.False(t, service.BlockingApplies(Request{GroupID: &group13, Protocol: auditinput.ProtocolOpenAIResponses, Model: "gpt-5.6-terra"}))

	service.config = &fakeConfigStore{active: true, cfg: ActiveConfig{
		RiskControlEnabled: true, Enabled: true, BlockingEnabled: true, GroupIDs: []int64{12},
	}}
	require.True(t, service.BlockingApplies(Request{GroupID: &group12, Protocol: auditinput.ProtocolOpenAIResponses, Model: "gpt-5.6-terra"}))
	require.False(t, service.BlockingApplies(Request{GroupID: &group13, Protocol: auditinput.ProtocolOpenAIResponses, Model: "gpt-5.6-terra"}))
}

func TestPromptServiceNoLongerUsesProtocolOrModelAsScope(t *testing.T) {
	tests := []Request{
		{Protocol: "anthropic_messages", Model: "gpt-5.6-terra"},
		{Protocol: "gemini", Model: "gpt-5.6-terra"},
		{Protocol: "openai_images", Model: "gpt-5.6-terra"},
		{Protocol: auditinput.ProtocolOpenAIResponses, Model: "claude-sonnet-4"},
		{Protocol: auditinput.ProtocolOpenAIResponses, Model: "gemini-3-pro"},
		{Protocol: auditinput.ProtocolOpenAIResponses, Model: "gpt-image-2"},
		{Protocol: auditinput.ProtocolOpenAIChat, Model: "gpt-image-1.5"},
	}
	var promptService *PromptService
	for _, req := range tests {
		require.False(t, promptService.BlockingApplies(req), "%s/%s", req.Protocol, req.Model)
		require.NoError(t, promptService.Enqueue(context.Background(), req), "%s/%s", req.Protocol, req.Model)
		_, err := promptService.Evaluate(context.Background(), req)
		require.Error(t, err, "%s/%s", req.Protocol, req.Model)
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
