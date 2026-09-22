package service

import (
	"context"
	"fmt"
)

// Pool admission must use the same business route that verified this ticket.
// Existing continuation fences and same-account session affinity remain binding.
func (s *OpenAIGatewayService) codexTicketSelectionContext(ctx context.Context, account *Account, requestedModel string, requireCompact bool) (context.Context, error) {
	if account == nil || account.EgressMode != EgressModePool {
		return ctx, nil
	}
	model := s.openAICodexTicketOutboundModel(account, requestedModel, requireCompact)
	binding, err := s.openAICodexTicketRequiredBinding(ctx, account, model)
	if err != nil {
		return ctx, fmt.Errorf("%w: %w", ErrAccountEgressUnavailable, err)
	}
	if binding == "" {
		return ctx, nil
	}
	if !accountUsesEnforcedEgressPool(ctx, s.settingService, account) {
		return ctx, ErrAccountEgressUnavailable
	}
	if required := RequiredAccountEgressBindingFromContext(ctx); required != "" && required != binding {
		return ctx, ErrAccountEgressConfigStale
	}
	if preferred := PreferredAccountEgressBindingFromContext(ctx); preferred != "" && preferred != binding {
		if accountID, _, valid := parseStableAccountEgressBindingID(preferred); valid && accountID == account.ID {
			return ctx, ErrAccountEgressConfigStale
		}
	}
	return WithRequiredAccountEgressBinding(ctx, binding), nil
}
