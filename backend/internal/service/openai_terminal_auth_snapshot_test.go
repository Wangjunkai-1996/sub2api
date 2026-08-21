package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func terminalAuthTestContext(admission *OpenAIAccountRequirementAdmission) context.Context {
	ctx := WithOpenAIAccountRequirement(context.Background(), admission.Requirement)
	return WithOpenAIAccountTerminalAdmission(ctx, admission)
}

func terminalAuthTestGinContext(t *testing.T, ctx context.Context, body []byte) *gin.Context {
	t.Helper()
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body)).WithContext(ctx)
	return c
}

func decodeAgentAssertionEnvelope(t *testing.T, header string) struct {
	AgentRuntimeID string `json:"agent_runtime_id"`
	TaskID         string `json:"task_id"`
} {
	t.Helper()
	require.True(t, strings.HasPrefix(header, "AgentAssertion "))
	encoded := strings.TrimPrefix(header, "AgentAssertion ")
	decoded, err := base64.RawURLEncoding.DecodeString(encoded)
	require.NoError(t, err)
	var envelope struct {
		AgentRuntimeID string `json:"agent_runtime_id"`
		TaskID         string `json:"task_id"`
	}
	require.NoError(t, json.Unmarshal(decoded, &envelope))
	return envelope
}

func TestOpenAITerminalAuthenticationUsesFinalizedShadowOwnerSnapshot(t *testing.T) {
	parentID := int64(8201)
	parent := &Account{
		ID: parentID, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{
			"access_token":       "final-plus-token",
			"plan_type":          "plus",
			"chatgpt_account_id": "final-owner",
		},
	}
	shadow := &Account{
		ID: 8202, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, ParentAccountID: &parentID,
	}
	repo := &admissionTestAccountRepo{accounts: map[int64]*Account{shadow.ID: shadow, parent.ID: parent}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	ctx := terminalAuthTestContext(&OpenAIAccountRequirementAdmission{
		Selected: shadow, EffectiveCredentialOwner: parent,
		Requirement:  securityadmission.AccountRequirementAuditExempt,
		AccountClass: securityadmission.AccountAuditExemptVerified,
	})

	token, authMode, err := svc.GetAccessToken(ctx, shadow)
	require.NoError(t, err)
	require.Equal(t, "oauth", authMode)
	require.Equal(t, "final-plus-token", token)
	require.Equal(t, 1, repo.calls(shadow.ID))
	require.Equal(t, 1, repo.calls(parent.ID))

	driftedParent := *parent
	driftedParent.Credentials = shallowCopyMap(parent.Credentials)
	driftedParent.Credentials["access_token"] = "post-final-pro-token"
	driftedParent.Credentials["plan_type"] = "pro"
	driftedParent.Credentials["chatgpt_account_id"] = "post-final-owner"
	driftedParent.Credentials["chatgpt_account_is_fedramp"] = true
	repo.setAccount(&driftedParent)

	body := []byte(`{"model":"gpt-5.4","input":"hello"}`)
	c := terminalAuthTestGinContext(t, ctx, body)
	req, err := svc.buildUpstreamRequest(ctx, c, shadow, body, token, false, "", false)
	require.NoError(t, err)
	require.Equal(t, "Bearer final-plus-token", req.Header.Get("Authorization"))
	require.Equal(t, "final-owner", req.Header.Get("chatgpt-account-id"))
	require.Empty(t, req.Header.Get("x-openai-fedramp"))
	require.Equal(t, 1, repo.calls(shadow.ID), "request construction must not reload the selected shadow after final admission")
	require.Equal(t, 1, repo.calls(parent.ID), "request construction must not reload or switch the effective owner after final admission")

	_, err = svc.buildOpenAIAuthenticationHeaders(ctx, shadow, "post-final-pro-token")
	require.Error(t, err, "a caller cannot substitute a post-final token")
}

func TestOpenAITerminalAgentIdentitySignsFinalizedSnapshotWithoutRepositoryRead(t *testing.T) {
	key, privateKey := newTestAgentIdentityKey(t)
	parentID := int64(8211)
	parent := &Account{
		ID: parentID, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{
			"auth_mode":          OpenAIAuthModeAgentIdentity,
			"agent_runtime_id":   key.runtimeID,
			"agent_private_key":  privateKey,
			"task_id":            "task-final",
			"plan_type":          "plus",
			"_token_version":     int64(31),
			"chatgpt_account_id": "agent-final-owner",
		},
	}
	shadow := &Account{
		ID: 8212, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, ParentAccountID: &parentID,
	}
	repo := &admissionTestAccountRepo{accounts: map[int64]*Account{shadow.ID: shadow, parent.ID: parent}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	ctx := terminalAuthTestContext(&OpenAIAccountRequirementAdmission{
		Selected: shadow, EffectiveCredentialOwner: parent,
		Requirement:  securityadmission.AccountRequirementAuditExempt,
		AccountClass: securityadmission.AccountAuditExemptVerified,
	})

	token, authMode, err := svc.GetAccessToken(ctx, shadow)
	require.NoError(t, err)
	require.Empty(t, token)
	require.Equal(t, OpenAIAuthModeAgentIdentity, authMode)
	require.Equal(t, 1, repo.calls(shadow.ID))
	require.Equal(t, 1, repo.calls(parent.ID))

	_, replacementPrivateKey := newTestAgentIdentityKey(t)
	driftedParent := *parent
	driftedParent.Credentials = shallowCopyMap(parent.Credentials)
	driftedParent.Credentials["agent_runtime_id"] = "runtime-post-final"
	driftedParent.Credentials["agent_private_key"] = replacementPrivateKey
	driftedParent.Credentials["task_id"] = "task-post-final"
	driftedParent.Credentials["_token_version"] = int64(32)
	driftedParent.Credentials["chatgpt_account_id"] = "agent-post-final-owner"
	repo.setAccount(&driftedParent)

	body := []byte(`{"model":"gpt-5.4","input":"hello"}`)
	c := terminalAuthTestGinContext(t, ctx, body)
	req, err := svc.buildUpstreamRequest(ctx, c, shadow, body, token, false, "", false)
	require.NoError(t, err)
	envelope := decodeAgentAssertionEnvelope(t, req.Header.Get("Authorization"))
	require.Equal(t, key.runtimeID, envelope.AgentRuntimeID)
	require.Equal(t, "task-final", envelope.TaskID)
	require.Equal(t, "agent-final-owner", req.Header.Get("chatgpt-account-id"))
	require.Equal(t, 1, repo.calls(shadow.ID))
	require.Equal(t, 1, repo.calls(parent.ID), "signing must consume the immutable finalized owner without a DB read")
}

func TestOpenAITerminalAgentIdentityRejectsModeDriftAtFinalAdmission(t *testing.T) {
	key, privateKey := newTestAgentIdentityKey(t)
	selected := &Account{
		ID: 8221, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{
			"auth_mode":         OpenAIAuthModeAgentIdentity,
			"agent_runtime_id":  key.runtimeID,
			"agent_private_key": privateKey,
			"task_id":           "task-terminal",
			"plan_type":         "plus",
		},
	}
	drifted := *selected
	drifted.Credentials = map[string]any{
		"access_token": "bearer-after-drift",
		"plan_type":    "plus",
	}
	repo := &admissionTestAccountRepo{accounts: map[int64]*Account{selected.ID: &drifted}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	ctx := terminalAuthTestContext(&OpenAIAccountRequirementAdmission{
		Selected: selected, EffectiveCredentialOwner: selected,
		Requirement:  securityadmission.AccountRequirementAuditExempt,
		AccountClass: securityadmission.AccountAuditExemptVerified,
	})

	token, _, err := svc.GetAccessToken(ctx, selected)
	require.Error(t, err)
	require.Empty(t, token)
	_, buildErr := svc.buildOpenAIAuthenticationHeaders(ctx, selected, "")
	require.Error(t, buildErr, "failed final admission must leave no reusable dispatch credential")
	require.Equal(t, 1, repo.calls(selected.ID))
}

func TestOpenAITerminalAgentIdentityRecoveryRequiresFreshReadmission(t *testing.T) {
	key, privateKey := newTestAgentIdentityKey(t)
	selected := &Account{
		ID: 8231, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{
			"auth_mode":         OpenAIAuthModeAgentIdentity,
			"agent_runtime_id":  key.runtimeID,
			"agent_private_key": privateKey,
			"task_id":           "task-old",
			"plan_type":         "plus",
		},
	}
	repo := &admissionTestAccountRepo{accounts: map[int64]*Account{selected.ID: selected}}
	svc := &OpenAIGatewayService{accountRepo: repo}
	ctx := terminalAuthTestContext(&OpenAIAccountRequirementAdmission{
		Selected: selected, EffectiveCredentialOwner: selected,
		Requirement:  securityadmission.AccountRequirementAuditExempt,
		AccountClass: securityadmission.AccountAuditExemptVerified,
	})

	_, _, err := svc.GetAccessToken(ctx, selected)
	require.NoError(t, err)
	require.Equal(t, 1, repo.calls(selected.ID))

	drifted := *selected
	drifted.Credentials = shallowCopyMap(selected.Credentials)
	drifted.Credentials["task_id"] = "task-new"
	drifted.Credentials["plan_type"] = "pro"
	repo.setAccount(&drifted)

	err = svc.recoverAgentIdentityTask(ctx, selected, "task-old")
	require.Error(t, err)
	_, buildErr := svc.buildOpenAIAuthenticationHeaders(ctx, selected, "")
	require.Error(t, buildErr, "retry dispatch must remain disabled after Plus-to-Pro recovery drift")
	require.Equal(t, 2, repo.calls(selected.ID), "the recovery fresh read rejects Pro immediately and header rebuild must not read afterward")
}

func TestOpenAITerminalHTTPRecoveryDispatchesWithFreshTaskSnapshot(t *testing.T) {
	key, privateKey := newTestAgentIdentityKey(t)
	account := &Account{
		ID: 8241, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{
			"auth_mode":          OpenAIAuthModeAgentIdentity,
			"agent_runtime_id":   key.runtimeID,
			"agent_private_key":  privateKey,
			"task_id":            "task-http-old",
			"plan_type":          "plus",
			"chatgpt_account_id": "http-owner",
		},
	}
	repo := &agentIdentityForwardRepo{account: account}
	registerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"task_id":"task-http-new"}`)
	}))
	defer registerServer.Close()
	oldBase := openAIAgentIdentityAuthAPIBaseURL
	openAIAgentIdentityAuthAPIBaseURL = registerServer.URL
	t.Cleanup(func() { openAIAgentIdentityAuthAPIBaseURL = oldBase })

	upstream := &httpUpstreamRecorder{responses: []*http.Response{
		{StatusCode: http.StatusUnauthorized, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"invalid_task_id"}}`))},
		{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":"resp-terminal-recovery","object":"response","model":"gpt-5.4","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))},
	}}
	svc := &OpenAIGatewayService{cfg: &config.Config{}, accountRepo: repo, httpUpstream: upstream}
	ctx := terminalAuthTestContext(&OpenAIAccountRequirementAdmission{
		Selected: account, EffectiveCredentialOwner: account,
		Requirement:  securityadmission.AccountRequirementAuditExempt,
		AccountClass: securityadmission.AccountAuditExemptVerified,
	})
	body := []byte(`{"model":"gpt-5.4","instructions":"Reply OK","input":[],"stream":false}`)
	c := terminalAuthTestGinContext(t, ctx, body)
	SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)

	result, err := svc.Forward(ctx, c, account, body)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, upstream.requests, 2)
	require.Equal(t, "task-http-old", decodeAgentAssertionTask(t, upstream.requests[0].Header.Get("Authorization")))
	require.Equal(t, "task-http-new", decodeAgentAssertionTask(t, upstream.requests[1].Header.Get("Authorization")))
	require.Equal(t, "http-owner", upstream.requests[1].Header.Get("chatgpt-account-id"))
}

type terminalAgentIdentityDriftDialer struct {
	mu      sync.Mutex
	repo    *agentIdentityForwardRepo
	headers []http.Header
	conn    *openAIWSCaptureConn
}

func (d *terminalAgentIdentityDriftDialer) Dial(
	_ context.Context,
	_ string,
	headers http.Header,
	_ string,
) (openAIWSClientConn, int, http.Header, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.headers = append(d.headers, cloneHeader(headers))
	if len(d.headers) == 1 {
		drifted := *d.repo.account
		drifted.Credentials = shallowCopyMap(d.repo.account.Credentials)
		drifted.Credentials["task_id"] = "task-ws-new"
		drifted.Credentials["plan_type"] = "pro"
		d.repo.account = &drifted
		return nil, http.StatusUnauthorized, nil, &openAIWSHandshakeError{
			Body: []byte(`{"error":{"code":"invalid_task_id"}}`),
			Err:  errors.New("invalid task id"),
		}
	}
	return d.conn, 0, nil, nil
}

func (d *terminalAgentIdentityDriftDialer) snapshotHeaders() []http.Header {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make([]http.Header, len(d.headers))
	for i := range d.headers {
		result[i] = cloneHeader(d.headers[i])
	}
	return result
}

func TestOpenAITerminalWSRecoveryPlanDriftPreventsSecondDialAndDispatch(t *testing.T) {
	key, privateKey := newTestAgentIdentityKey(t)
	account := &Account{
		ID: 8251, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true, Concurrency: 1,
		Credentials: map[string]any{
			"auth_mode":          OpenAIAuthModeAgentIdentity,
			"agent_runtime_id":   key.runtimeID,
			"agent_private_key":  privateKey,
			"task_id":            "task-ws-old",
			"plan_type":          "plus",
			"_token_version":     int64(41),
			"chatgpt_account_id": "ws-owner",
		},
		Extra: map[string]any{"responses_websockets_v2_enabled": true},
	}
	repo := &agentIdentityForwardRepo{account: account}
	captureConn := &openAIWSCaptureConn{events: [][]byte{
		[]byte(`{"type":"response.completed","response":{"id":"must-not-dispatch","model":"gpt-5.4","usage":{"input_tokens":1,"output_tokens":1}}}`),
	}}
	dialer := &terminalAgentIdentityDriftDialer{repo: repo, conn: captureConn}
	cfg := newOpenAIWSV2TestConfig()
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 0
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	cfg.Gateway.OpenAIWS.QueueLimitPerConn = 8
	pool := newOpenAIWSConnPool(cfg)
	defer pool.Close()
	pool.setClientDialerForTest(dialer)
	httpFallback := &httpUpstreamRecorder{}
	svc := &OpenAIGatewayService{
		cfg: cfg, accountRepo: repo, httpUpstream: httpFallback,
		cache: &stubGatewayCache{}, openaiWSResolver: NewOpenAIWSProtocolResolver(cfg),
		toolCorrector: NewCodexToolCorrector(), openaiWSPool: pool,
	}
	ctx := terminalAuthTestContext(&OpenAIAccountRequirementAdmission{
		Selected: account, EffectiveCredentialOwner: account,
		Requirement:  securityadmission.AccountRequirementAuditExempt,
		AccountClass: securityadmission.AccountAuditExemptVerified,
	})
	body := []byte(`{"model":"gpt-5.4","stream":false,"input":"hello"}`)
	c := terminalAuthTestGinContext(t, ctx, body)

	result, err := svc.Forward(ctx, c, account, body)
	require.Error(t, err)
	require.Nil(t, result)
	headers := dialer.snapshotHeaders()
	require.Len(t, headers, 1, "Plus-to-Pro drift must prevent the recovery dial")
	require.Equal(t, "task-ws-old", decodeAgentAssertionTask(t, headers[0].Get("Authorization")))
	require.Empty(t, captureConn.writes, "no response.create may be written after recovery admission fails")
	require.Empty(t, httpFallback.requests, "WS recovery failure must not fall back to HTTP")
	_, buildErr := svc.buildOpenAIAuthenticationHeaders(ctx, account, "")
	require.Error(t, buildErr, "the old finalized permit must remain revoked")
}

var _ openAIWSClientDialer = (*terminalAgentIdentityDriftDialer)(nil)
