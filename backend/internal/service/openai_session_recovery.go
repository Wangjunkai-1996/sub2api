package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
)

var ErrOpenAIContinuationStateUnavailable = errors.New("OpenAI continuation state is unavailable")

// GetOpenAIHTTPContinuationInvalidReason checks the fail-closed invalidation
// fence before an HTTP previous_response_id is sent upstream again.
func (s *OpenAIGatewayService) GetOpenAIHTTPContinuationInvalidReason(
	ctx context.Context,
	groupID int64,
	responseID string,
) (GatewayFailureReason, error) {
	if s == nil || strings.TrimSpace(responseID) == "" {
		return "", nil
	}
	store := s.getOpenAIWSStateStore()
	invalidStore, ok := store.(openAIHTTPResponseInvalidStateStore)
	if !ok {
		return "", fmt.Errorf("%w: invalidation store is unsupported", ErrOpenAIContinuationStateUnavailable)
	}
	reason, invalid, err := invalidStore.GetHTTPResponseInvalidReason(ctx, groupID, responseID)
	if err != nil {
		return "", fmt.Errorf("%w: read invalid response marker: %v", ErrOpenAIContinuationStateUnavailable, err)
	}
	if !invalid {
		return "", nil
	}
	return reason, nil
}

// InvalidateOpenAIHTTPContinuation fences a blocked response ID before removing
// its routing state. Persist the provider decision even if the client disconnects.
func (s *OpenAIGatewayService) InvalidateOpenAIHTTPContinuation(
	ctx context.Context,
	c *gin.Context,
	groupID int64,
	responseID string,
	sessionHash string,
	expectedAccountID int64,
) error {
	if s == nil || expectedAccountID <= 0 {
		return fmt.Errorf("%w: invalid account", ErrOpenAIContinuationStateUnavailable)
	}
	ctx, cancel := openAIAccountStateContext(ctx)
	defer cancel()
	store := s.getOpenAIWSStateStore()
	if store == nil {
		return fmt.Errorf("%w: state store is nil", ErrOpenAIContinuationStateUnavailable)
	}

	responseID = strings.TrimSpace(responseID)
	if responseID != "" {
		invalidStore, ok := store.(openAIHTTPResponseInvalidStateStore)
		if !ok {
			return fmt.Errorf("%w: invalidation store is unsupported", ErrOpenAIContinuationStateUnavailable)
		}
		if err := invalidStore.MarkHTTPResponseInvalid(
			ctx, groupID, responseID, OpenAISessionBlockedReason, s.openAIWSResponseStickyTTL(),
		); err != nil {
			return fmt.Errorf("%w: persist invalid response marker: %v", ErrOpenAIContinuationStateUnavailable, err)
		}
		if err := store.DeleteResponseAccount(ctx, groupID, responseID); err != nil {
			return fmt.Errorf("%w: delete response account binding: %v", ErrOpenAIContinuationStateUnavailable, err)
		}
		store.DeleteResponseConn(responseID)
	}

	sessionHash = strings.TrimSpace(sessionHash)
	if sessionHash != "" {
		if owned, ok := store.(*defaultOpenAIWSStateStore); ok {
			if err := owned.clearSessionRoutingForAccount(ctx, groupID, sessionHash, expectedAccountID); err != nil {
				return fmt.Errorf("%w: conditionally clear session routing: %v", ErrOpenAIContinuationStateUnavailable, err)
			}
		}
	}
	if sessionHash != "" && s.cache != nil {
		compareDeleter, ok := s.cache.(GatewaySessionBindingCompareDeleter)
		if !ok {
			return fmt.Errorf("%w: compare-and-delete cache capability is unsupported", ErrOpenAIContinuationStateUnavailable)
		}
		primaryKey := s.openAISessionCacheKey(sessionHash)
		if primaryKey != "" {
			if _, err := compareDeleter.CompareAndDeleteSessionAccountID(ctx, groupID, primaryKey, expectedAccountID); err != nil {
				return fmt.Errorf("%w: conditionally delete sticky account binding: %v", ErrOpenAIContinuationStateUnavailable, err)
			}
		}
		legacyKey := s.openAILegacySessionCacheKey(ctx, sessionHash)
		if legacyKey != "" {
			if _, err := compareDeleter.CompareAndDeleteSessionAccountID(ctx, groupID, legacyKey, expectedAccountID); err != nil {
				return fmt.Errorf("%w: conditionally delete legacy sticky account binding: %v", ErrOpenAIContinuationStateUnavailable, err)
			}
		}
	}

	s.clearOpenAICodexTurnStateForRecovery(c, expectedAccountID)
	return nil
}
