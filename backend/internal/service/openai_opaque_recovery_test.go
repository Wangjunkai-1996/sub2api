package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

type openAIOpaqueNotSentUpstream struct {
	HTTPUpstream
	err          error
	sentAccounts []int64
}

func (u *openAIOpaqueNotSentUpstream) Do(_ *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	if u.err != nil {
		return nil, u.err
	}
	u.sentAccounts = append(u.sentAccounts, accountID)
	return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header)}, nil
}

func TestOpenAIOpaqueRecoveryUnsentDispatchCanFailOverWithinPhysicalBudget(t *testing.T) {
	for _, scenario := range []struct {
		name         string
		err          error
		provenUnsent bool
	}{
		{"http setup", MarkHTTPUpstreamRequestNotSent(errors.New("local proxy configuration invalid")), true},
		{"plugin setup", &PluginTransportError{Code: "setup_failed", RequestSent: false}, true},
		{"plugin sent", &PluginTransportError{Code: "read_failed", RequestSent: true}, false},
		{"unknown transport", errors.New("ambiguous transport failure"), false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			writer, _ := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
			ctx := WithOpenAIOpaqueRecovery(WithOpenAIManagedRecovery(context.Background()))
			ctx = WithOpenAIModelDispatchObserver(ctx, writer.BeginAttempt)
			ctx = WithOpenAIModelDispatchStartedObserver(ctx, writer.RecordDispatch)
			body := `{"input":[{"type":"reasoning","encrypted_content":"opaque"}]}`
			rememberOpenAIOpaqueRequest(ctx, []byte(body))
			upstream := &openAIOpaqueNotSentUpstream{err: scenario.err}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.test/v1/responses", strings.NewReader(body))
			require.NoError(t, err)
			_, err = svc.doOpenAIUpstream(request, "", account)
			require.ErrorIs(t, err, scenario.err)
			assert.Equal(t, !scenario.provenUnsent, OpenAIOpaqueRecoveryRoutePinned(ctx))
			upstream.err = nil
			account.ID = 2
			if !scenario.provenUnsent {
				_, err = svc.doOpenAIUpstream(request, "", account)
				require.ErrorIs(t, err, ErrOpenAIOpaqueRouteUnavailable)
				assert.Empty(t, upstream.sentAccounts)
				return
			}
			assert.Empty(t, OpenAIRecoveryRouteFingerprint(ctx))
			for i := 0; i < 3; i++ {
				_, err = svc.doOpenAIUpstream(request, "", account)
				require.NoError(t, err)
			}
			_, err = svc.doOpenAIUpstream(request, "", account)
			require.ErrorIs(t, err, ErrOpenAIModelDispatchBudgetExhausted)
			assert.Equal(t, []int64{2, 2, 2}, upstream.sentAccounts)
			assert.Equal(t, 2, writer.SameAccountRetryCount())
			assert.Zero(t, writer.AccountSwitchCount(), "an unsent first account is not a physical switch")
		})
	}
}

func TestOpenAIOpaqueRecoveryUnsentRetryPreservesAlreadyDispatchedRoute(t *testing.T) {
	writer, _ := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
	ctx := WithOpenAIOpaqueRecovery(WithOpenAIManagedRecovery(context.Background()))
	ctx = WithOpenAIModelDispatchObserver(ctx, writer.BeginAttempt)
	ctx = WithOpenAIModelDispatchStartedObserver(ctx, writer.RecordDispatch)
	upstream := &openAIOpaqueNotSentUpstream{}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.test/v1/responses", strings.NewReader(`{"input":[]}`))
	require.NoError(t, err)
	_, err = svc.doOpenAIUpstream(request, "", account)
	require.NoError(t, err)
	fingerprint := OpenAIRecoveryRouteFingerprint(ctx)
	upstream.err = MarkHTTPUpstreamRequestNotSent(errors.New("local setup failed"))
	_, err = svc.doOpenAIUpstream(request, "", account)
	require.ErrorIs(t, err, ErrHTTPUpstreamRequestNotSent)
	require.True(t, OpenAIOpaqueRecoveryRoutePinned(ctx))
	assert.Equal(t, fingerprint, OpenAIRecoveryRouteFingerprint(ctx))
	account.ID = 2
	_, err = svc.doOpenAIUpstream(request, "", account)
	require.ErrorIs(t, err, ErrOpenAIOpaqueRouteUnavailable)
	account.ID = 1
	upstream.err = nil
	for i := 0; i < 2; i++ {
		_, err = svc.doOpenAIUpstream(request, "", account)
		require.NoError(t, err)
	}
	_, err = svc.doOpenAIUpstream(request, "", account)
	require.ErrorIs(t, err, ErrOpenAIModelDispatchBudgetExhausted)
	assert.Equal(t, []int64{1, 1, 1}, upstream.sentAccounts)
	assert.Equal(t, 2, writer.SameAccountRetryCount())
	assert.Zero(t, writer.AccountSwitchCount())
}

func TestOpenAIOpaqueRecoveryUnsentAttemptsRespectProtocolSequenceLimit(t *testing.T) {
	writer, _ := recoveryTestWriter(t, OpenAIRecoveryResponsesSSE, nil)
	ctx := WithOpenAIOpaqueRecovery(WithOpenAIManagedRecovery(context.Background()))
	ctx = WithOpenAIModelDispatchObserver(ctx, writer.BeginAttempt)
	ctx = WithOpenAIModelDispatchStartedObserver(ctx, writer.RecordDispatch)
	upstream := &openAIOpaqueNotSentUpstream{err: MarkHTTPUpstreamRequestNotSent(errors.New("local setup failed"))}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.test/v1/responses", strings.NewReader(`{"input":[]}`))
	require.NoError(t, err)
	for i := 0; i < 11; i++ {
		_, err = svc.doOpenAIUpstream(request, "", account)
		require.ErrorIs(t, err, ErrHTTPUpstreamRequestNotSent)
	}
	_, err = svc.doOpenAIUpstream(request, "", account)
	require.ErrorIs(t, err, ErrOpenAIModelDispatchBudgetExhausted)
	assert.Empty(t, upstream.sentAccounts)
	assert.False(t, OpenAIOpaqueRecoveryRoutePinned(ctx))
	assert.Zero(t, writer.SameAccountRetryCount())
	assert.Zero(t, writer.AccountSwitchCount())
}

func TestOpenAIOpaqueRecoveryDispatchPreservesContextAndBudget(t *testing.T) {
	ctx := WithOpenAIOpaqueRecovery(WithOpenAIManagedRecovery(WithOpenAIModelDispatchBudget(context.Background())))
	account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
	source := []byte(`{"model":"gpt-5","input":[{ "type": "reasoning", "encrypted_content": "opaque-data", "nonce":9007199254740993}],"previous_response_id":"resp_original"}`)
	rememberOpenAIOpaqueRequest(ctx, source)
	upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: make(http.Header)}}
	svc := &OpenAIGatewayService{httpUpstream: upstream}
	var firstBody []byte
	for i := 0; i < 3; i++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.test/v1/responses", strings.NewReader(`{"model":"gpt-5","input":[],"previous_response_id":"wrong"}`))
		require.NoError(t, err)
		_, err = svc.doOpenAIUpstream(request, "", account)
		require.NoError(t, err)
		assert.Equal(t, gjson.GetBytes(source, "input").Raw, gjson.GetBytes(upstream.lastBody, "input").Raw)
		assert.Equal(t, "resp_original", gjson.GetBytes(upstream.lastBody, "previous_response_id").String())
		if i == 0 {
			firstBody = append([]byte(nil), upstream.lastBody...)
		} else {
			assert.Equal(t, firstBody, upstream.lastBody)
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.test/v1/responses", strings.NewReader(string(source)))
	require.NoError(t, err)
	_, err = svc.doOpenAIUpstream(request, "", account)
	require.ErrorIs(t, err, ErrOpenAIModelDispatchBudgetExhausted)
	assert.Len(t, upstream.bodies, 3)
}

func TestOpenAIOpaqueRecoveryRejectsRouteDriftBeforeDispatch(t *testing.T) {
	for _, scenario := range []string{"account", "binding", "legacy_identity", "proxy", "endpoint", "base_url"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := WithOpenAIOpaqueRecovery(WithOpenAIManagedRecovery(WithOpenAIModelDispatchBudget(context.Background())))
			account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, SelectedEgress: &ResolvedAccountEgress{BindingID: "binding1", RouteID: 1, IdentityID: "identity1"}}
			if scenario == "legacy_identity" {
				account.SelectedEgress = nil
				account.LegacyEgressAdmission = &LegacyAccountEgressAdmission{BindingID: "legacy1", RouteID: 1, IdentityID: "identity1"}
			}
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: make(http.Header)}}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.test/v1/responses", strings.NewReader(`{"input":[]}`))
			require.NoError(t, err)
			_, err = svc.doOpenAIUpstream(request, "", account)
			require.NoError(t, err)
			proxyURL := ""
			switch scenario {
			case "account":
				account.ID = 2
			case "binding":
				account.SelectedEgress.BindingID = "binding2"
			case "legacy_identity":
				account.LegacyEgressAdmission.IdentityID = "identity2"
			case "proxy":
				proxyURL = "http://other-proxy.test"
			case "endpoint":
				request.URL.Path = "/v1/chat/completions"
			case "base_url":
				account.Credentials = map[string]any{"base_url": "https://different.test"}
			}
			_, err = svc.doOpenAIUpstream(request, proxyURL, account)
			require.ErrorIs(t, err, ErrOpenAIOpaqueRouteUnavailable)
			assert.Len(t, upstream.bodies, 1)
			assert.True(t, IsOpenAIModelDispatchStop(err))
		})
	}
}

func TestOpenAIOpaqueRecoveryContinuationValidatesPreviousRoute(t *testing.T) {
	for _, scenario := range []string{"same_route", "changed_endpoint", "changed_proxy"} {
		t.Run(scenario, func(t *testing.T) {
			firstCtx := WithOpenAIManagedRecovery(WithOpenAIModelDispatchBudget(context.Background()))
			account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey}
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: make(http.Header)}}
			svc := &OpenAIGatewayService{httpUpstream: upstream}
			request, err := http.NewRequestWithContext(firstCtx, http.MethodPost, "https://example.test/v1/responses", strings.NewReader(`{"input":"hello"}`))
			require.NoError(t, err)
			_, err = svc.doOpenAIUpstream(request, "", account)
			require.NoError(t, err)
			fingerprint := OpenAIRecoveryRouteFingerprint(firstCtx)
			require.Len(t, fingerprint, 64)

			ctx := WithOpenAIOpaqueRecovery(WithOpenAIManagedRecovery(context.Background()))
			ctx = WithOpenAIOpaqueRecoveryRouteFingerprint(ctx, fingerprint)
			body := `{"input":[{"type":"reasoning","encrypted_content":"opaque"}],"previous_response_id":"resp_first"}`
			rememberOpenAIOpaqueRequest(ctx, []byte(body))
			request, err = http.NewRequestWithContext(ctx, http.MethodPost, "https://example.test/v1/responses", strings.NewReader(body))
			require.NoError(t, err)
			proxyURL := ""
			if scenario == "changed_endpoint" {
				request.URL.Host = "different.test"
			} else if scenario == "changed_proxy" {
				proxyURL = "http://different-proxy.test"
			}
			_, err = svc.doOpenAIUpstream(request, proxyURL, account)
			if scenario == "same_route" {
				require.NoError(t, err)
				assert.Len(t, upstream.bodies, 2)
				assert.Equal(t, fingerprint, OpenAIRecoveryRouteFingerprint(ctx))
			} else {
				require.ErrorIs(t, err, ErrOpenAIOpaqueRouteUnavailable)
				assert.Len(t, upstream.bodies, 1)
				assert.False(t, OpenAIOpaqueRecoveryRoutePinned(ctx))
			}
		})
	}
}

func TestOpenAIOpaqueRecoveryInvalidEncryptedContentDoesNotRepair(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "passthrough"}[passthrough], func(t *testing.T) {
			ctx := WithOpenAIOpaqueRecovery(WithOpenAIManagedRecovery(WithOpenAIModelDispatchBudget(context.Background())))
			errorBody := `{"error":{"code":"invalid_encrypted_content","type":"invalid_request_error","message":"bad encrypted content"}}`
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusBadRequest, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(errorBody))}}
			cfg := &config.Config{}
			cfg.Security.URLAllowlist.Enabled = false
			svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
			account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-key", "base_url": "https://example.test"}, Extra: map[string]any{"use_responses_api": true, "openai_passthrough": passthrough}}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
			SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
			body := []byte(`{"model":"gpt-5","stream":false,"input":[{"type":"reasoning","encrypted_content":"opaque-data","summary":[]}],"previous_response_id":"resp_original"}`)
			_, err := svc.Forward(ctx, c, account, body)
			var failure *UpstreamFailoverError
			require.ErrorAs(t, err, &failure)
			assert.Equal(t, http.StatusBadRequest, failure.StatusCode)
			assert.Equal(t, errorBody, string(failure.ResponseBody))
			assert.False(t, failure.ShouldRetryNextAccount())
			assert.Len(t, upstream.bodies, 1)
			assert.Equal(t, gjson.GetBytes(body, "input").Raw, gjson.GetBytes(upstream.lastBody, "input").Raw)
			assert.Equal(t, "resp_original", gjson.GetBytes(upstream.lastBody, "previous_response_id").String())
		})
	}
}

func TestOpenAIOpaqueRecoverySchedulerNeverFallsBack(t *testing.T) {
	for _, scenario := range []string{"original_available", "original_missing", "original_excluded"} {
		t.Run(scenario, func(t *testing.T) {
			ctx := WithOpenAIOpaqueRecovery(WithOpenAIManagedRecovery(context.Background()))
			original := Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Status: StatusActive, Schedulable: true, Concurrency: 1}
			require.NoError(t, ValidateOpenAIOpaqueRecoveryRoute(ctx, &original))
			require.False(t, OpenAIOpaqueRecoveryRoutePinned(ctx))
			first := &OpenAIGatewayService{httpUpstream: &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: make(http.Header)}}}
			request, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, "https://example.test/v1/responses", strings.NewReader(`{"input":[]}`))
			require.NoError(t, requestErr)
			_, dispatchErr := first.doOpenAIUpstream(request, "", &original)
			require.NoError(t, dispatchErr)
			require.True(t, OpenAIOpaqueRecoveryRoutePinned(ctx))
			other := original
			other.ID = 2
			accounts := []Account{other}
			var excluded map[int64]struct{}
			if scenario != "original_missing" {
				accounts = append(accounts, original)
			}
			if scenario == "original_excluded" {
				excluded = map[int64]struct{}{1: {}}
			}
			svc := &OpenAIGatewayService{cfg: &config.Config{}, accountRepo: schedulerTestOpenAIAccountRepo{accounts: accounts}}
			selection, _, err := svc.SelectAccountWithScheduler(ctx, nil, "", "", "gpt-5", excluded, OpenAIUpstreamTransportHTTPSSE, false)
			if scenario == "original_available" {
				require.NoError(t, err)
				require.NotNil(t, selection)
				assert.Equal(t, int64(1), selection.Account.ID)
				if selection.ReleaseFunc != nil {
					selection.ReleaseFunc()
				}
			} else {
				require.ErrorIs(t, err, ErrOpenAIOpaqueRouteUnavailable)
				assert.Nil(t, selection)
			}
		})
	}
}

func TestOpenAIOpaqueRecoverySSEContextRejectionStopsNativeAndPassthrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, passthrough := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			for _, event := range []string{"response.failed", "error"} {
				name := map[bool]string{false: "native", true: "passthrough"}[passthrough] + "/" + map[bool]string{false: "nonstream", true: "stream"}[stream] + "/" + event
				t.Run(name, func(t *testing.T) {
					ctx := WithOpenAIOpaqueRecovery(WithOpenAIManagedRecovery(context.Background()))
					payload := `{"type":"response.failed","response":{"id":"resp_fail","status":"failed","error":{"code":"invalid_encrypted_content","message":"opaque context rejected"}}}`
					if event == "error" {
						payload = `{"type":"error","error":{"code":"invalid_encrypted_content","message":"opaque context rejected"}}`
					}
					upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader("data: " + payload + "\n\n"))}}
					cfg := &config.Config{}
					cfg.Security.URLAllowlist.Enabled = false
					svc := &OpenAIGatewayService{cfg: cfg, httpUpstream: upstream}
					account := &Account{ID: 1, Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Credentials: map[string]any{"api_key": "test-key", "base_url": "https://example.test"}, Extra: map[string]any{"use_responses_api": true, "openai_passthrough": passthrough}}
					c, _ := gin.CreateTestContext(httptest.NewRecorder())
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
					SetOpenAIClientTransport(c, OpenAIClientTransportHTTP)
					body := `{"model":"gpt-5","stream":true,"input":[{"type":"reasoning","encrypted_content":"opaque-data","summary":[]}]}`
					if !stream {
						body = strings.Replace(body, `"stream":true`, `"stream":false`, 1)
					}
					_, err := svc.Forward(ctx, c, account, []byte(body))
					var failure *UpstreamFailoverError
					require.ErrorAs(t, err, &failure)
					assert.Equal(t, http.StatusBadRequest, failure.StatusCode)
					assert.Equal(t, payload, string(failure.ResponseBody))
					assert.EqualValues(t, "invalid_encrypted_content", failure.Reason)
					assert.False(t, failure.ShouldRetryNextAccount())
					assert.Len(t, upstream.bodies, 1)
				})
			}
		}
	}
}
