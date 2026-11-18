package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/gin-gonic/gin"
)

// CyberSessionBlockStore is implemented by the gateway cache when Redis-backed
// session blocks are available.
type CyberSessionBlockStore interface {
	SetCyberSessionBlocked(ctx context.Context, key string, ttl time.Duration) error
	FindCyberSessionBlocked(ctx context.Context, key string) (bool, error)
}

// CyberSessionIdentityStatus describes only whether a block key can be derived.
// Ready does not imply that blocking is enabled or that a store write succeeded.
type CyberSessionIdentityStatus string

const (
	CyberSessionIdentityReady           CyberSessionIdentityStatus = "ready"
	CyberSessionIdentityMissingAPIKey   CyberSessionIdentityStatus = "missing_api_key"
	CyberSessionIdentityInvalidRequest  CyberSessionIdentityStatus = "invalid_request"
	CyberSessionIdentityUnverifiedRelay CyberSessionIdentityStatus = "unverified_relay"
	CyberSessionIdentityMissingSession  CyberSessionIdentityStatus = "missing_session"
	CyberSessionIdentityInvalidSession  CyberSessionIdentityStatus = "invalid_session"
)

// CyberSessionExplicitBlockKey requires a verified relay identity and a
// client-provided X-Session-Id. Other session hints and prompt-cache keys do not
// establish a unique conversation and must not authorize persistent blocking.
func CyberSessionExplicitBlockKey(apiKeyID int64, c *gin.Context, _ []byte) string {
	key, _ := ResolveCyberSessionBlockIdentity(apiKeyID, c)
	return key
}

// ResolveCyberSessionBlockIdentity returns the existing key and a fixed diagnostic
// status. It does not use body hints, log session IDs, or change admission policy.
func ResolveCyberSessionBlockIdentity(apiKeyID int64, c *gin.Context) (string, CyberSessionIdentityStatus) {
	if apiKeyID <= 0 {
		return "", CyberSessionIdentityMissingAPIKey
	}
	if c == nil || c.Request == nil {
		return "", CyberSessionIdentityInvalidRequest
	}
	ctx := c.Request.Context()
	userID, _ := ctx.Value(ctxkey.NewAPIUserID).(int64)
	tokenID, _ := ctx.Value(ctxkey.NewAPITokenID).(int64)
	origin, _ := ctx.Value(ctxkey.NewAPIOrigin).(string)
	if userID <= 0 || tokenID <= 0 || origin == "" {
		return "", CyberSessionIdentityUnverifiedRelay
	}
	rawSessionID := c.GetHeader(openCodeSessionIDHeader)
	if strings.TrimSpace(rawSessionID) == "" {
		return "", CyberSessionIdentityMissingSession
	}
	sessionID := sanitizeSessionID(rawSessionID)
	if sessionID == "" {
		return "", CyberSessionIdentityInvalidSession
	}
	raw := fmt.Sprintf("cyber-session:v2|api=%d|origin=%d:%s|user=%d|token=%d|session=%s", apiKeyID, len(origin), origin, userID, tokenID, sessionID)
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:]), CyberSessionIdentityReady
}

func (s *OpenAIGatewayService) cyberSessionBlockStore() CyberSessionBlockStore {
	if s == nil || s.cache == nil {
		return nil
	}
	store, ok := s.cache.(CyberSessionBlockStore)
	if !ok {
		return nil
	}
	return store
}

func (s *OpenAIGatewayService) CyberSessionBlockRuntime(ctx context.Context) (bool, time.Duration) {
	if s == nil || s.settingService == nil {
		return false, time.Hour
	}
	return s.settingService.GetCyberSessionBlockRuntime(ctx)
}

func (s *OpenAIGatewayService) MarkCyberSessionBlocked(ctx context.Context, key string) {
	if key == "" {
		return
	}
	enabled, ttl := s.CyberSessionBlockRuntime(ctx)
	if !enabled {
		return
	}
	store := s.cyberSessionBlockStore()
	if store == nil {
		return
	}
	if err := store.SetCyberSessionBlocked(ctx, key, ttl); err != nil {
		logger.LegacyPrintf("service.openai_gateway", "cyber session block write failed: err=%v", err)
	}
}

func (s *OpenAIGatewayService) FindCyberSessionBlockedForRequest(ctx context.Context, apiKeyID int64, c *gin.Context, body []byte) string {
	key := CyberSessionExplicitBlockKey(apiKeyID, c, body)
	if key == "" {
		return ""
	}
	enabled, _ := s.CyberSessionBlockRuntime(ctx)
	if !enabled {
		return ""
	}
	store := s.cyberSessionBlockStore()
	if store == nil {
		return ""
	}
	blocked, err := store.FindCyberSessionBlocked(ctx, key)
	if err != nil {
		logger.LegacyPrintf("service.openai_gateway", "cyber session block read failed: err=%v", err)
		return ""
	}
	if blocked {
		return key
	}
	return ""
}
