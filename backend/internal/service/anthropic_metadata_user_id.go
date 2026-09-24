package service

import (
	"context"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
)

// AnthropicMetadataUserIDEnabledExtraKey enables forwarding the authenticated
// Sub2API user ID as metadata.user_id for this account's /v1/messages calls.
// It is deliberately opt-in so each upstream account can be controlled without
// relying on mutable account or group names.
const AnthropicMetadataUserIDEnabledExtraKey = "anthropic_metadata_user_id_enabled"

func isAnthropicMessagesEndpoint(c *gin.Context) bool {
	if c == nil || c.Request == nil || c.Request.URL == nil {
		return false
	}
	path := c.Request.URL.Path
	for len(path) > 1 && path[len(path)-1] == '/' {
		path = path[:len(path)-1]
	}
	return len(path) >= len("/messages") && path[len(path)-len("/messages"):] == "/messages"
}

// injectAnthropicMetadataUserID adds the authenticated Sub2API user ID only when the
// account switch is enabled. The inbound body remains untouched so existing
// Claude session detection and sticky routing keep using the client metadata.
func injectAnthropicMetadataUserID(ctx context.Context, account *Account, body []byte) []byte {
	if ctx == nil || account == nil || account.Platform != PlatformAnthropic || account.Type != AccountTypeAPIKey || account.Extra == nil {
		return body
	}
	enabled, _ := account.Extra[AnthropicMetadataUserIDEnabledExtraKey].(bool)
	if !enabled {
		return body
	}
	userID, ok := ctx.Value(ctxkey.UserID).(int64)
	if !ok || userID <= 0 {
		return body
	}

	value := strconv.FormatInt(userID, 10)
	if next, changed := setJSONValueBytes(body, "metadata.user_id", value); changed {
		return next
	}
	// If a malformed/non-object metadata value was supplied, replace only that
	// envelope so the upstream still receives the required user_id field.
	if next, changed := setJSONRawBytes(body, "metadata", []byte(`{"user_id":`+strconv.Quote(value)+`}`)); changed {
		return next
	}
	return body
}
