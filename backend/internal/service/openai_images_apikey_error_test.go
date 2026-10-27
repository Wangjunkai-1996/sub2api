package service

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestForwardOpenAIImagesAPIKey_HTTP200StructuredErrorIsFailure(t *testing.T) {
	body := []byte(`{"model":"gpt-image-2","prompt":"draw","n":3}`)
	require.True(t, gjson.ValidBytes(body), "request fixture must be valid JSON")
	c, rec := newOpenAIImagesTestContext(t, body)
	response := openAIImagesJSONResponse()
	response.Header.Set("X-Request-Id", "provider-request-123")
	response.Body = io.NopCloser(strings.NewReader(`{"error":{"type":"server_error","code":"upstream_error","message":"provider temporary failure"}}`))
	service := newOpenAIImagesTestService(&httpUpstreamRecorder{resp: response})
	parsed, err := service.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)

	result, err := service.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), body, parsed, "")

	var upstreamError *OpenAIImagesUpstreamError
	require.ErrorAs(t, err, &upstreamError)
	require.Nil(t, result)
	require.Equal(t, http.StatusBadGateway, upstreamError.StatusCode)
	require.Equal(t, "image_provider_error", upstreamError.Code)
	require.Equal(t, "provider-request-123", upstreamError.UpstreamRequestID)
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Equal(t, "upstream_error", gjson.GetBytes(rec.Body.Bytes(), "error.type").String())
	require.Equal(t, "image_provider_error", gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
	require.Equal(t, "Image provider returned an error", gjson.GetBytes(rec.Body.Bytes(), "error.message").String())
	require.True(t, gjson.ValidBytes(rec.Body.Bytes()), rec.Body.String())
	require.NotContains(t, rec.Body.String(), "provider temporary failure")

	value, ok := c.Get(OpsUpstreamErrorsKey)
	require.True(t, ok)
	events, ok := value.([]*OpsUpstreamErrorEvent)
	require.True(t, ok)
	require.Len(t, events, 1)
	require.Equal(t, http.StatusOK, events[0].UpstreamStatusCode)
	require.Equal(t, "provider-request-123", events[0].UpstreamRequestID)
	require.Contains(t, events[0].Detail, "provider_error_type=server_error")
	require.Contains(t, events[0].Detail, "provider_error_code=upstream_error")
}

func TestForwardOpenAIImagesAPIKey_HTTP200StructuredErrorAfterKeepaliveIsRecorded(t *testing.T) {
	gin.SetMode(gin.TestMode)
	body := []byte(`{"model":"gpt-image-2","prompt":"draw"}`)
	c, rec := newOpenAIImagesTestContext(t, body)
	stopKeepalive := StartOpenAIImagesJSONKeepalive(c, 5*time.Millisecond)
	defer stopKeepalive()
	waitForOpenAIImagesJSONKeepalive(t, c)

	response := openAIImagesJSONResponse()
	response.Body = io.NopCloser(strings.NewReader(`{"error":{"type":"server_error","message":"provider temporary failure"}}`))
	service := newOpenAIImagesTestService(&httpUpstreamRecorder{resp: response})
	parsed, err := service.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)

	result, err := service.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), body, parsed, "")
	stopKeepalive()

	var upstreamError *OpenAIImagesUpstreamError
	require.ErrorAs(t, err, &upstreamError)
	require.Nil(t, result)
	require.Equal(t, http.StatusOK, rec.Code, "keepalive has already committed the wire status")
	require.True(t, gjson.ValidBytes(rec.Body.Bytes()), rec.Body.String())
	require.Equal(t, "image_provider_error", gjson.GetBytes(rec.Body.Bytes(), "error.code").String())
	streamErrors := GetOpsStreamErrors(c)
	require.Len(t, streamErrors, 1)
	require.Equal(t, http.StatusBadGateway, streamErrors[0].IntendedStatus)
	require.True(t, streamErrors[0].CountTowardsSLA)
	require.True(t, streamErrors[0].NonStream)
}

func TestForwardOpenAIImagesAPIKey_EmptyOutputDoesNotAssumeRequestedCount(t *testing.T) {
	body := []byte(`{"model":"gpt-image-2","prompt":"draw","n":3}`)
	c, _ := newOpenAIImagesTestContext(t, body)
	response := openAIImagesJSONResponse()
	response.Body = io.NopCloser(strings.NewReader(`{"data":[]}`))
	service := newOpenAIImagesTestService(&httpUpstreamRecorder{resp: response})
	parsed, err := service.ParseOpenAIImagesRequest(c, body)
	require.NoError(t, err)

	result, err := service.ForwardImages(context.Background(), c, newOpenAIImagesAPIKeyAccount(), body, parsed, "")

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Zero(t, result.ImageCount, "only returned image outputs may be billed")
}
