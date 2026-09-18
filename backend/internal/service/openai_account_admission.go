package service

import (
	"context"
	"errors"
	"fmt"
)

// RecheckOpenAIAccountSchedulable fences dispatch against an account disabled
// while its concurrency slot was being acquired. Read the authority, not the
// scheduler cache; keep the request's already admitted transport and leases.
func (s *OpenAIGatewayService) RecheckOpenAIAccountSchedulable(ctx context.Context, selected *Account) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if selected == nil {
		return ErrNoAvailableAccounts
	}
	// Minimal standalone services have no authoritative repository.
	if s == nil || s.accountRepo == nil {
		return nil
	}
	latest, err := s.accountRepo.GetByID(ctx, selected.ID)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, ErrAccountNotFound) {
		return ErrNoAvailableAccounts
	}
	if err != nil {
		return fmt.Errorf("recheck account scheduling state: %w", err)
	}
	if latest == nil || !latest.IsSchedulable() {
		return ErrNoAvailableAccounts
	}
	return nil
}
