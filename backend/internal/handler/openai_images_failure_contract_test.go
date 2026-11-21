//go:build unit

package handler

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/model"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const imageFailureContractMessage = "抱歉，刚才图片生成服务出现错误，这次没能成功生成 BLG 夺冠海报。目前无法提供成品图片。"

type imageFailureHTTPReply struct {
	status int
	body   string
	err    error
}

type imageFailureSequenceUpstream struct {
	service.HTTPUpstream
	replies    []imageFailureHTTPReply
	accountIDs []int64
}

func (u *imageFailureSequenceUpstream) Do(_ *http.Request, _ string, accountID int64, _ int) (*http.Response, error) {
	index := len(u.accountIDs)
	u.accountIDs = append(u.accountIDs, accountID)
	if index >= len(u.replies) {
		return nil, errors.New("unexpected extra image request")
	}
	reply := u.replies[index]
	if reply.err != nil {
		return nil, reply.err
	}
	return &http.Response{
		StatusCode: reply.status,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(reply.body)),
	}, nil
}

func newOpenAIImagesFailureHandler(t *testing.T, accounts []service.Account, upstream service.HTTPUpstream) *OpenAIGatewayHandler {
	t.Helper()
	cfg := &config.Config{RunMode: config.RunModeSimple}
	gateway := service.NewOpenAIGatewayService(
		openAIImagesFailoverAccountRepo{accounts: accounts}, nil, nil, nil, nil, nil, nil,
		cfg, nil, nil, nil, nil, nil, upstream, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	billing := service.NewBillingCacheService(nil, nil, nil, nil, nil, nil, cfg, nil)
	t.Cleanup(billing.Stop)
	h := NewOpenAIGatewayHandler(
		gateway, service.NewConcurrencyService(nil), billing,
		service.NewAPIKeyService(nil, nil, nil, nil, nil, nil, cfg), nil, nil, nil, nil, cfg,
	)
	h.maxAccountSwitches = 10
	return h
}

func TestOpenAIGatewayHandlerImages_NativeFailureProofSurvivesAccountExhaustion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	complete := `{"error":{"type":"upstream_error","message":"` + imageFailureContractMessage + `"}}`
	unknown := `<html>upstream unavailable</html>`
	for _, tc := range []struct {
		name        string
		replies     []imageFailureHTTPReply
		maxSwitches int
		wantCalls   int
		wantCode    string
	}{
		{name: "single_account_missing_code", replies: []imageFailureHTTPReply{{status: 502, body: complete}}, maxSwitches: 10, wantCalls: 1, wantCode: "image_provider_error"},
		{name: "single_account_null_code", replies: []imageFailureHTTPReply{{status: 502, body: strings.Replace(complete, `"type":`, `"code":null,"type":`, 1)}}, maxSwitches: 10, wantCalls: 1, wantCode: "image_provider_error"},
		{name: "maximum_switches", replies: []imageFailureHTTPReply{{status: 502, body: complete}, {status: 502, body: complete}}, maxSwitches: 0, wantCalls: 1, wantCode: "image_provider_error"},
		{name: "all_confirmed_failures", replies: []imageFailureHTTPReply{{status: 502, body: complete}, {status: 502, body: complete}}, maxSwitches: 10, wantCalls: 2, wantCode: "image_provider_error"},
		{name: "unknown_then_confirmed", replies: []imageFailureHTTPReply{{status: 502, body: unknown}, {status: 502, body: complete}}, maxSwitches: 10, wantCalls: 2},
		{name: "confirmed_then_unknown", replies: []imageFailureHTTPReply{{status: 502, body: complete}, {status: 502, body: unknown}}, maxSwitches: 10, wantCalls: 2},
		{name: "unknown_then_http200_error", replies: []imageFailureHTTPReply{{status: 502, body: unknown}, {status: 200, body: complete}}, maxSwitches: 10, wantCalls: 2},
		{name: "not_sent_then_confirmed", replies: []imageFailureHTTPReply{{err: &service.PluginTransportError{RequestSent: false, Message: "plugin rejected before dispatch"}}, {status: 502, body: complete}}, maxSwitches: 10, wantCalls: 2, wantCode: "image_provider_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			accounts := make([]service.Account, len(tc.replies))
			for i := range accounts {
				accounts[i] = service.Account{
					ID: int64(i + 1), Name: fmt.Sprintf("image-api-%d", i+1),
					Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey,
					Status: service.StatusActive, Schedulable: true, Priority: i,
					Credentials: map[string]any{"api_key": "image-test-key", "base_url": "https://image.example/v1"},
				}
			}
			upstream := &imageFailureSequenceUpstream{replies: tc.replies}
			h := newOpenAIImagesFailureHandler(t, accounts, upstream)
			h.maxAccountSwitches = tc.maxSwitches
			body := []byte(`{"model":"gpt-image-2.6-sunburst","prompt":"draw a poster"}`)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", bytes.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")
			groupID := int64(3130)
			c.Set(string(middleware2.ContextKeyAPIKey), &service.APIKey{
				ID: 99, GroupID: &groupID, Group: &service.Group{ID: groupID, AllowImageGeneration: true}, User: &service.User{ID: 100},
			})
			c.Set(string(middleware2.ContextKeyUser), middleware2.AuthSubject{UserID: 100})

			h.Images(c)

			require.True(t, gjson.ValidBytes(rec.Body.Bytes()), rec.Body.String())
			assert.Len(t, upstream.accountIDs, tc.wantCalls, "only existing safe failover paths may issue another request")
			assert.Equal(t, http.StatusBadGateway, rec.Code)
			assert.Equal(t, "upstream_error", gjson.GetBytes(rec.Body.Bytes(), "error.type").String())
			assert.Equal(t, tc.wantCode, gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
			if tc.wantCode != "" {
				assert.Equal(t, imageFailureContractMessage, gjson.GetBytes(rec.Body.Bytes(), "error.message").String())
			}
		})
	}
}

func TestOpenAIGatewayHandlerImages_FailureMarkerRespectsPassthrough(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name          string
		status        int
		customMessage string
		definitive    bool
		wantCode      string
		wantMessage   string
	}{
		{name: "original_502", status: 502, definitive: true, wantCode: "image_provider_error", wantMessage: imageFailureContractMessage},
		{name: "custom_502", status: 502, customMessage: "Please try another image", definitive: true, wantCode: "image_provider_error", wantMessage: "Please try another image"},
		{name: "custom_message_is_sanitized", status: 502, customMessage: "Image failed; Authorization: Bearer secret-example", definitive: true, wantCode: "image_provider_error", wantMessage: "Image failed; [redacted authorization]"},
		{name: "mapped_503", status: 503, customMessage: "Image service unavailable", definitive: true, wantMessage: "Image service unavailable"},
		{name: "explicit_200", status: 200, customMessage: "Image service unavailable", definitive: true, wantMessage: "Image service unavailable"},
		{name: "no_source_proof", status: 502, customMessage: "Please try another image", wantMessage: "Please try another image"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rule := &model.ErrorPassthroughRule{
				ID: 1, Name: "image failure rule", Enabled: true, Priority: 1,
				ErrorCodes: []int{502}, MatchMode: model.MatchModeAny, Platforms: []string{model.PlatformOpenAI},
				PassthroughCode: tc.status == 502, ResponseCode: &tc.status,
				PassthroughBody: tc.customMessage == "",
			}
			if tc.customMessage != "" {
				rule.CustomMessage = &tc.customMessage
			}
			h := newBodyLimitPassthroughHandler(t, rule)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", nil)
			c.Set(service.OpsUpstreamErrorsKey, []*service.OpsUpstreamErrorEvent{{UpstreamStatusCode: 502}})
			h.handleFailoverExhausted(c, &service.UpstreamFailoverError{
				StatusCode: 502, ResponseBody: []byte(`{"error":{"type":"upstream_error","message":"raw reason must not override sanitized source"}}`),
				ImageOutcomeDefinitive: tc.definitive, ImageFailureMessage: imageFailureContractMessage,
			}, false)

			require.True(t, gjson.ValidBytes(rec.Body.Bytes()), rec.Body.String())
			assert.Equal(t, tc.status, rec.Code)
			assert.Equal(t, tc.wantCode, gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
			assert.Equal(t, tc.wantMessage, gjson.GetBytes(rec.Body.Bytes(), "error.message").String())
			events := c.MustGet(service.OpsUpstreamErrorsKey).([]*service.OpsUpstreamErrorEvent)
			require.Len(t, events, 1)
			assert.Equal(t, "error_passthrough", events[0].FinalOutcome)
			assert.Equal(t, tc.status, events[0].FinalClientStatusCode)
		})
	}
}

func TestOpenAIGatewayHandlerImages_FailureMarkerAfterCommittedJSONWhitespace(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", nil)
	c.Header("Content-Type", "application/json")
	c.Writer.WriteHeader(http.StatusOK)
	_, err := c.Writer.WriteString(" \n")
	require.NoError(t, err)

	(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, &service.UpstreamFailoverError{
		StatusCode:             http.StatusBadGateway,
		ResponseBody:           []byte(`{"error":{"type":"upstream_error","message":"` + imageFailureContractMessage + `"}}`),
		ImageOutcomeDefinitive: true, ImageFailureMessage: imageFailureContractMessage,
	}, false)

	assert.Equal(t, http.StatusOK, rec.Code, "JSON whitespace already committed the wire status")
	require.True(t, gjson.ValidBytes(rec.Body.Bytes()), "there must be exactly one final JSON document")
	assert.Equal(t, "image_provider_error", gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
	assert.Equal(t, imageFailureContractMessage, gjson.GetBytes(rec.Body.Bytes(), "error.message").String())
}

func TestOpenAIGatewayHandlerImages_UnknownOutcomeRetainsSafeDiagnosticWithoutMarker(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", nil)
	(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, &service.UpstreamFailoverError{
		StatusCode:          http.StatusBadGateway,
		ImageFailureMessage: imageFailureContractMessage,
	}, false)
	assert.Equal(t, http.StatusBadGateway, rec.Code)
	assert.Equal(t, imageFailureContractMessage, gjson.GetBytes(rec.Body.Bytes(), "error.message").String())
	assert.Empty(t, gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
}

func TestOpenAIGatewayHandlerImages_FailureMarkerDoesNotOverrideSpecializedErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name        string
		failure     service.UpstreamFailoverError
		wantStatus  int
		wantCode    string
		wantMessage string
	}{
		{name: "balance", failure: service.UpstreamFailoverError{Reason: service.OpenAIImagesInsufficientBalanceReason}, wantStatus: http.StatusPaymentRequired, wantCode: service.OpenAIImagesInsufficientBalanceCode, wantMessage: service.OpenAIImagesInsufficientBalanceMessage},
		{name: "credential", failure: service.UpstreamFailoverError{Stage: service.GatewayFailureStageAccountAuth, Reason: service.GrokCredentialReasonRevoked}, wantStatus: http.StatusServiceUnavailable, wantMessage: service.GrokCredentialUnavailableClientMessage},
		{name: "capacity", failure: service.UpstreamFailoverError{RequestScopedTransient: true, ResponseBody: []byte(`{"error":{"code":"server_is_overloaded"}}`), ClientMessage: "Image provider is overloaded"}, wantStatus: http.StatusServiceUnavailable, wantMessage: "Image provider is overloaded"},
		{name: "body_limit", failure: *bodyLimitFailoverTestError(), wantStatus: http.StatusRequestEntityTooLarge, wantMessage: service.OpenAIRequestBodyTooLargeClientMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := tc.failure
			failure.ImageOutcomeDefinitive = true
			failure.ImageFailureMessage = imageFailureContractMessage
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", nil)
			(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, &failure, false)
			assert.Equal(t, tc.wantStatus, rec.Code)
			assert.Equal(t, tc.wantCode, gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
			assert.Equal(t, tc.wantMessage, gjson.GetBytes(rec.Body.Bytes(), "error.message").String())
		})
	}

	t.Run("text_source_zero_value", func(t *testing.T) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		(&OpenAIGatewayHandler{}).handleFailoverExhausted(c, &service.UpstreamFailoverError{
			StatusCode:   http.StatusBadGateway,
			ResponseBody: []byte(`{"error":{"type":"upstream_error","message":"` + imageFailureContractMessage + `"}}`),
		}, false)
		assert.Equal(t, http.StatusBadGateway, rec.Code)
		assert.Empty(t, gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
		assert.Equal(t, "Upstream service temporarily unavailable", gjson.GetBytes(rec.Body.Bytes(), "error.message").String())
	})
}
