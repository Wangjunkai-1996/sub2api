package service

import (
	"context"
	"strings"

	"github.com/gin-gonic/gin"
)

// bindOpenAIResponseRouting persists the account and, when available, the
// selected egress route for a Responses/WebSocket continuation. The helper is
// intentionally best effort: response delivery must not fail because a
// durable affinity marker could not be written.
func (s *OpenAIGatewayService) bindOpenAIResponseRouting(
	ctx context.Context,
	c *gin.Context,
	account *Account,
	responseID string,
	bindingID string,
	connID string,
) {
	if s == nil || account == nil || account.ID <= 0 {
		return
	}
	responseID = strings.TrimSpace(responseID)
	if responseID == "" {
		return
	}
	store := s.getOpenAIWSStateStore()
	if store == nil {
		return
	}
	ttl := s.openAIWSResponseStickyTTL()
	_ = bindOpenAIWSResponseRoutingPair(
		store,
		ctx,
		getOpenAIGroupIDFromContext(c),
		responseID,
		account.ID,
		strings.TrimSpace(bindingID),
		ttl,
	)
	if connID = strings.TrimSpace(connID); connID != "" {
		store.BindResponseConn(responseID, connID, ttl)
	}
}
