package service

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func TestIsAnthropicMessagesEndpoint(t *testing.T) {
	for _, path := range []string{"/v1/messages", "/openai/v1/messages/"} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", path, nil)
		if !isAnthropicMessagesEndpoint(c) {
			t.Fatalf("%q should be recognized as a messages endpoint", path)
		}
	}
	for _, path := range []string{"/v1/responses", "/v1/messages/count_tokens"} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", path, nil)
		if isAnthropicMessagesEndpoint(c) {
			t.Fatalf("%q should not be recognized as a messages endpoint", path)
		}
	}
}

func TestInjectAnthropicMetadataUserID(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxkey.UserID, int64(8871))
	account := &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Extra: map[string]any{AnthropicMetadataUserIDEnabledExtraKey: true}}

	got := injectAnthropicMetadataUserID(ctx, account, []byte(`{"metadata":{"trace":"keep","user_id":"client-value"},"stream":true}`))
	if userID := gjson.GetBytes(got, "metadata.user_id").String(); userID != "8871" {
		t.Fatalf("metadata.user_id = %q, want 8871", userID)
	}
	if trace := gjson.GetBytes(got, "metadata.trace").String(); trace != "keep" {
		t.Fatalf("metadata.trace = %q, want keep", trace)
	}
}

func TestInjectAnthropicMetadataUserIDReplacesInvalidMetadata(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxkey.UserID, int64(42))
	account := &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Extra: map[string]any{AnthropicMetadataUserIDEnabledExtraKey: true}}

	got := injectAnthropicMetadataUserID(ctx, account, []byte(`{"metadata":"client-value"}`))
	if userID := gjson.GetBytes(got, "metadata.user_id").String(); userID != "42" {
		t.Fatalf("metadata.user_id = %q, want 42", userID)
	}
}

func TestInjectAnthropicMetadataUserIDCreatesMetadata(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxkey.UserID, int64(7))
	account := &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Extra: map[string]any{AnthropicMetadataUserIDEnabledExtraKey: true}}

	got := injectAnthropicMetadataUserID(ctx, account, []byte(`{"model":"claude-sonnet-4-5"}`))
	if userID := gjson.GetBytes(got, "metadata.user_id").String(); userID != "7" {
		t.Fatalf("metadata.user_id = %q, want 7", userID)
	}
}

func TestInjectAnthropicMetadataUserIDRequiresAccountSwitch(t *testing.T) {
	ctx := context.WithValue(context.Background(), ctxkey.UserID, int64(8871))
	body := []byte(`{"metadata":{"user_id":"client-value"}}`)

	if got := injectAnthropicMetadataUserID(ctx, &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey}, body); string(got) != string(body) {
		t.Fatalf("disabled account body changed: %s", got)
	}
	if got := injectAnthropicMetadataUserID(ctx, &Account{Platform: PlatformAnthropic, Type: AccountTypeAPIKey, Extra: map[string]any{AnthropicMetadataUserIDEnabledExtraKey: false}}, body); string(got) != string(body) {
		t.Fatalf("explicitly disabled account body changed: %s", got)
	}
	if got := injectAnthropicMetadataUserID(ctx, &Account{Platform: PlatformOpenAI, Type: AccountTypeAPIKey, Extra: map[string]any{AnthropicMetadataUserIDEnabledExtraKey: true}}, body); string(got) != string(body) {
		t.Fatalf("non-Anthropic account body changed: %s", got)
	}
	if got := injectAnthropicMetadataUserID(ctx, &Account{Platform: PlatformAnthropic, Type: AccountTypeOAuth, Extra: map[string]any{AnthropicMetadataUserIDEnabledExtraKey: true}}, body); string(got) != string(body) {
		t.Fatalf("non-API-key account body changed: %s", got)
	}
}
