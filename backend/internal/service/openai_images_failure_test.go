package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

const imageFailureContractReason = "抱歉，刚才图片生成服务出现错误，这次没能成功生成 BLG 夺冠海报。目前无法提供成品图片。"

func TestForwardOpenAIImagesAPIKey_FailureContract(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
		definitive    bool
		images        int
	}{
		{"missing code", `{"error":{"type":"upstream_error","message":"` + imageFailureContractReason + `"}}`, true, 0},
		{"null code", `{"error":{"type":"upstream_error","code":null,"message":"provider failed"}}`, true, 0},
		{"unknown code", `{"error":{"type":"provider_error","code":"new_provider_code","message":"provider failed"}}`, true, 0},
		{"html", `<html>bad gateway</html>`, false, 0},
		{"empty", ``, false, 0},
		{"trailing garbage", `{"error":{"type":"upstream_error","message":"failed"}} trailing`, false, 0},
		{"type number", `{"error":{"type":42,"message":"failed"}}`, false, 0},
		{"message object", `{"error":{"type":"upstream_error","message":{"text":"failed"}}}`, false, 0},
		{"invalid UTF8", "{\"error\":{\"type\":\"upstream_error\",\"message\":\"bad" + string([]byte{0xff}) + "\"}}", false, 0},
		{"policy type object", `{"error":{"type":{"text":"content_policy"},"message":"failed"}}`, false, 0},
		{"policy code object", `{"error":{"type":{},"code":{"text":"content_policy"},"message":"failed"}}`, false, 0},
		{"blank type", `{"error":{"type":" ","message":"failed"}}`, false, 0},
		{"empty error", `{"error":{}}`, false, 0},
		{"unproven uppercase marker", `{"error":{"type":"invalid_request_error","code":" IMAGE_PROVIDER_ERROR ","message":"failed"}}`, false, 0},
		{"duplicate data", `{"error":{"type":"upstream_error","message":"failed"},"data":[],"data":[{"b64_json":"AA=="}]}`, false, 0},
		{"duplicate error", `{"error":{"type":"upstream_error","message":"failed"},"error":{"message":{}}}`, false, 0},
		{"duplicate message", `{"error":{"type":"upstream_error","message":"failed","message":{}}}`, false, 0},
		{"duplicate type", `{"error":{"type":"upstream_error","type":{},"message":"failed"}}`, false, 0},
		{"duplicate code", `{"error":{"type":"upstream_error","code":null,"code":"x","message":"failed"}}`, false, 0},
		{"duplicate nested output", `{"error":{"type":"upstream_error","message":"failed"},"response":{"output":[],"output":[{"type":"image_generation_call","result":"AA=="}]}}`, false, 0},
		{"policy content filter", `{"error":{"type":"upstream_error","code":"content_filter","message":"blocked"}}`, false, 0},
		{"image b64", `{"error":{"type":"upstream_error","message":"failed"},"data":[{"b64_json":"AA=="}]}`, false, 1},
		{"image url", `{"error":{"type":"upstream_error","message":"failed"},"data":[{"url":"https://images.example/a.png"}]}`, false, 1},
		{"responses image", `{"error":{"type":"upstream_error","message":"failed"},"output":[{"type":"image_generation_call","result":"AA=="}]}`, false, 1},
	} {
		for _, status := range []int{http.StatusOK, http.StatusBadGateway} {
			t.Run(tc.name+"/"+http.StatusText(status), func(t *testing.T) {
				request := []byte(`{"model":"gpt-image-2.6-sunburst","prompt":"draw"}`)
				c, rec := newOpenAIImagesTestContext(t, request)
				response := openAIImagesJSONResponse()
				response.StatusCode = status
				response.Body = io.NopCloser(strings.NewReader(tc.payload))
				upstream := &httpUpstreamRecorder{resp: response}
				svc := newOpenAIImagesTestService(upstream)
				parsed, err := svc.ParseOpenAIImagesRequest(c, request)
				require.NoError(t, err)
				result, err := svc.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), request, parsed, "")
				require.Len(t, upstream.requests, 1)
				if status == http.StatusBadGateway {
					var failure *UpstreamFailoverError
					require.ErrorAs(t, err, &failure)
					assert.Equal(t, tc.definitive, failure.ImageOutcomeDefinitive)
					if tc.name == "missing code" {
						assert.Equal(t, imageFailureContractReason, failure.ImageFailureMessage)
					}
					return
				}
				assert.Equal(t, tc.definitive, gjson.GetBytes(rec.Body.Bytes(), "error.code").String() == "image_provider_error")
				if tc.name == "message object" {
					assert.Equal(t, "Image provider returned an error", gjson.GetBytes(rec.Body.Bytes(), "error.message").String())
				}
				if strings.HasPrefix(tc.name, "policy ") && strings.HasSuffix(tc.name, " object") {
					assert.Empty(t, gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
				}
				if tc.name == "policy content filter" {
					assert.Equal(t, http.StatusBadRequest, rec.Code)
					assert.Equal(t, "content_policy_violation", gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
				}
				if tc.images > 0 {
					require.NoError(t, err)
					require.NotNil(t, result)
					assert.Equal(t, tc.images, result.ImageCount)
					assert.False(t, gjson.GetBytes(rec.Body.Bytes(), "error").Exists(), "successful output cannot carry a competing terminal error")
				}
			})
		}
	}
}

func TestOpenAIImagesProviderStreamCannotForwardUnprovenMarker(t *testing.T) {
	for _, accountType := range []string{AccountTypeAPIKey, AccountTypeOAuth} {
		t.Run(accountType, func(t *testing.T) {
			request := []byte(`{"model":"gpt-image-2.6-flare","prompt":"draw","stream":true}`)
			c, rec := newOpenAIImagesTestContext(t, request)
			response := openAIImagesJSONResponse()
			response.Header.Set("Content-Type", "text/event-stream")
			response.Body = io.NopCloser(strings.NewReader("data: {\"type\":\"image_generation.completed\",\"b64_json\":\"AA==\"}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"code\":\"old-code\",\"code\":\" IMAGE_PROVIDER_ERROR \",\"message\":\"failed\"}}\n\n"))
			upstream := &httpUpstreamRecorder{resp: response}
			svc := newOpenAIImagesTestService(upstream)
			parsed, err := svc.ParseOpenAIImagesRequest(c, request)
			require.NoError(t, err)
			account := newOpenAIImagesAPIKeyAccount()
			if accountType == AccountTypeOAuth {
				account = directImagesTestAccount()
			}
			result, _ := svc.ForwardImages(context.Background(), c, account, request, parsed, "")
			require.NotNil(t, result)
			assert.Equal(t, 1, result.ImageCount)
			assert.Contains(t, rec.Body.String(), "AA==")
			assert.NotContains(t, strings.ToLower(rec.Body.String()), "image_provider_error")
			assert.Len(t, upstream.requests, 1)
		})
	}
}

func TestOpenAIImagesProviderMarkerRequiresSourceProofInEverySerializer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		proof  bool
		status int
		want   bool
	}{
		{"upstream claimed marker", false, 502, false},
		{"known failure", true, 502, true},
		{"mapped other status", true, 503, false},
		{"explicit 200", true, 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, code := range []string{"image_provider_error", " IMAGE_PROVIDER_ERROR ", "Image_Provider_Error"} {
				failure := &OpenAIImagesUpstreamError{StatusCode: tc.status, ErrorType: "upstream_error", Code: code, Message: "failed", ImageOutcomeDefinitive: tc.proof}
				body := openAIImagesUpstreamErrorResponseBody(failure)
				assert.Equal(t, tc.want, gjson.GetBytes(body, "error.code").String() == "image_provider_error")
				streamBody := buildOpenAIImagesStreamErrorBodyFromUpstream(failure)
				assert.Equal(t, tc.want, gjson.GetBytes(streamBody, "error.code").String() == "image_provider_error")
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				require.True(t, writeOpenAIImagesUpstreamErrorResponse(c, failure))
				assert.Equal(t, tc.want, gjson.GetBytes(rec.Body.Bytes(), "error.code").String() == "image_provider_error")
			}
		})
	}
}

func TestForwardOpenAIImagesAPIKey_IncompleteFailureBodyCannotReplay(t *testing.T) {
	complete := `{"error":{"type":"upstream_error","message":"provider failed"}}`
	for _, tc := range []struct {
		name string
		body io.Reader
	}{
		{"complete JSON then read error", io.MultiReader(strings.NewReader(complete), passthroughErrReadCloser{err: io.ErrUnexpectedEOF})},
		{"limit plus one", strings.NewReader(complete + strings.Repeat(" ", int(openAIUpstreamErrorBodyReadLimit)+1-len(complete)))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := []byte(`{"model":"gpt-image-2","prompt":"draw"}`)
			c, rec := newOpenAIImagesTestContext(t, request)
			response := openAIImagesJSONResponse()
			response.StatusCode = http.StatusBadGateway
			response.Body = io.NopCloser(tc.body)
			upstream := &httpUpstreamRecorder{resp: response}
			svc := newOpenAIImagesTestService(upstream)
			parsed, err := svc.ParseOpenAIImagesRequest(c, request)
			require.NoError(t, err)
			result, err := svc.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), request, parsed, "")
			require.Error(t, err)
			require.Nil(t, result)
			var failover *UpstreamFailoverError
			assert.NotErrorAs(t, err, &failover)
			assert.NotContains(t, rec.Body.String(), "image_provider_error")
			assert.Len(t, upstream.requests, 1)
		})
	}
}

func TestOpenAIImagesProviderMessageSanitization(t *testing.T) {
	for _, tc := range []struct {
		name, input, secret string
	}{
		{"exact token", "failed with naked-active-secret", "naked-active-secret"},
		{"bearer", "failed Bearer bearer-secret", "bearer-secret"},
		{"bearer newline", "failed Bearer\nnewline-secret", "newline-secret"},
		{"basic auth", "Authorization: Basic encoded-secret", "encoded-secret"},
		{"json key", `{"API_Key": "json-secret"}`, "json-secret"},
		{"equals key", "api-key = equals-secret", "equals-secret"},
		{"token", "token=token-secret", "token-secret"},
		{"secret", "secret: secret-value", "secret-value"},
		{"password", "password=password-value", "password-value"},
		{"credential", "credential=credential-value", "credential-value"},
		{"foreign key", "failed sk-other-provider-secret", "sk-other-provider-secret"},
		{"unterminated quote", `api_key="unterminated secret`, "unterminated secret"},
		{"URL", "failed https://user:pass@private.example/path?key=url-secret#fragment", "private.example"},
		{"other URL scheme", "failed ftp://private.example/path", "private.example"},
		{"split URL", "failed ht\x00tps://private.example", "private.example"},
		{"split key", "api\x00_key=joined-secret", "joined-secret"},
		{"address", "failed 10.1.2.3:8080", "10.1.2.3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sensitive []string
			if tc.name == "exact token" {
				sensitive = []string{tc.secret}
			}
			out := sanitizeOpenAIImagesFailureMessage(tc.input, sensitive...)
			assert.NotContains(t, out, tc.secret)
			assert.True(t, utf8.ValidString(out))
		})
	}
	assert.Equal(t, imageFailureContractReason, sanitizeOpenAIImagesFailureMessage(imageFailureContractReason))
	assert.Equal(t, "Image provider returned an error", sanitizeOpenAIImagesFailureMessage("bad\xffvalue"))
	assert.Equal(t, "Image provider returned an error", sanitizeOpenAIImagesFailureMessage(`{"text":"private words"}`))
	assert.Equal(t, "Image provider returned an error", sanitizeOpenAIImagesFailureMessage(`<html>private words</html>`))
	assert.Equal(t, "Use {width} < 2048", sanitizeOpenAIImagesFailureMessage("Use {width} < 2048"))
	assert.NotContains(t, sanitizeOpenAIImagesFailureMessage("lookup private.example failed", "https://private.example/v1"), "private.example")
	for _, identifier := range []string{"api_key:other-secret", "password:other-secret", "sk-other-secret", "private.example"} {
		assert.Empty(t, safeOpenAIImagesFailureIdentifier(identifier, "https://private.example/v1"))
	}
	assert.Equal(t, "Image provider returned an error", sanitizeOpenAIImagesFailureMessage(strings.Repeat("x", 64<<10+1)))
	secret := "exact-secret-crossing-boundary"
	out := sanitizeOpenAIImagesFailureMessage(strings.Repeat("中", 680)+secret, secret)
	assert.LessOrEqual(t, len(out), 2048)
	assert.True(t, utf8.ValidString(out))
	assert.NotContains(t, out, "exact-secret")
}

func TestForwardOpenAIImagesAPIKey_FailureDiagnosticDoesNotLeakCredential(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusBadRequest, http.StatusBadGateway} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			request := []byte(`{"model":"gpt-image-2","prompt":"draw"}`)
			c, rec := newOpenAIImagesTestContext(t, request)
			response := openAIImagesJSONResponse()
			response.StatusCode = status
			payload, err := json.Marshal(map[string]any{"error": map[string]string{"type": "upstream_error", "message": imageFailureContractReason + " naked-active-secret https://private.example/key"}})
			require.NoError(t, err)
			response.Body = io.NopCloser(strings.NewReader(string(payload)))
			svc := newOpenAIImagesTestService(&httpUpstreamRecorder{resp: response})
			parsed, err := svc.ParseOpenAIImagesRequest(c, request)
			require.NoError(t, err)
			account := newOpenAIImagesAPIKeyAccount()
			account.Credentials["api_key"] = "naked-active-secret"
			_, err = svc.ForwardImages(context.Background(), c, account, request, parsed, "")
			require.Error(t, err)
			message := gjson.GetBytes(rec.Body.Bytes(), "error.message").String()
			if status == http.StatusBadGateway {
				var failure *UpstreamFailoverError
				require.ErrorAs(t, err, &failure)
				message = failure.ImageFailureMessage
			}
			assert.Contains(t, message, imageFailureContractReason)
			assert.NotContains(t, message, "naked-active-secret")
			assert.NotContains(t, message, "private.example")
			value, ok := c.Get(OpsUpstreamErrorsKey)
			require.True(t, ok)
			events, ok := value.([]*OpsUpstreamErrorEvent)
			require.True(t, ok)
			require.NotEmpty(t, events)
			for _, event := range events {
				assert.NotContains(t, event.Message, "naked-active-secret")
				assert.NotContains(t, event.Message, "private.example")
			}
		})
	}
}
