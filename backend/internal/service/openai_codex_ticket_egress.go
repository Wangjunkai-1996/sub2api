package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// A single opted-in account/model owns one verified business route. Keep that
// binding on refresh; new tickets initially use the configured primary route.
func codexTicketBusinessBinding(account *Account) string {
	if account == nil || account.EgressMode != EgressModePool {
		return ""
	}
	if account.SelectedEgress != nil {
		return account.SelectedEgress.BindingID
	}
	ac := codexAccountTicketConfigOf(account)
	if ticket := parseOpenAICodexTicketFromAny(account.ID, ac.Model, account.Extra[openAICodexTicketExtraKey(ac.Model)]); ticket != nil && ticket.EgressBindingID != "" {
		if codexTicketFixedProxyFingerprintForBinding(account, ticket.EgressBindingID) != "" {
			return ticket.EgressBindingID
		}
	}
	for _, binding := range account.EgressBindings {
		if binding.IsPrimary {
			return binding.BindingID
		}
	}
	return ""
}

func codexTicketFixedProxyFingerprint(account *Account) string {
	return codexTicketFixedProxyFingerprintForBinding(account, codexTicketBusinessBinding(account))
}

func codexTicketFixedProxyFingerprintForBinding(account *Account, bindingID string) string {
	if account == nil {
		return ""
	}
	var raw string
	if account.EgressMode == EgressModePool {
		if bindingID == "" || (account.SelectedEgress != nil && account.SelectedEgress.BindingID != bindingID) {
			return ""
		}
		if AccountEgressLeaseLost(account) {
			return ""
		}
		var selected *AccountEgressBinding
		for i := range account.EgressBindings {
			if account.EgressBindings[i].BindingID == bindingID {
				selected = &account.EgressBindings[i]
				break
			}
		}
		if selected == nil || selected.Status != AccountEgressBindingStatusActive || selected.Route == nil {
			return ""
		}
		route := selected.Route
		if route.State != EgressRouteStateActive || route.ExpectedIdentity == nil || route.ExpectedIdentity.Status != EgressIdentityStatusActive || !IsEgressIdentityVerificationFresh(route.VerifiedAt, time.Now()) {
			return ""
		}
		endpoint := ""
		switch route.Kind {
		case EgressRouteKindProxy:
			if route.Proxy == nil || route.ProxyID == nil || !route.Proxy.IsActive() || route.Proxy.IsExpired(time.Now()) {
				return ""
			}
			endpoint = route.Proxy.URL()
		case EgressRouteKindDirect:
			if route.RuntimeScope == nil || strings.TrimSpace(*route.RuntimeScope) != DefaultDirectEgressRuntimeScope {
				return ""
			}
			endpoint = *route.RuntimeScope
		default:
			return ""
		}
		raw = fmt.Sprintf("pool\x00%d\x00%s\x00%d\x00%d\x00%d\x00%d\x00%d\x00%s\x00%s\x00%v", account.ID, bindingID, selected.RouteID, route.ExpectedIdentity.ID, route.Revision, accountEgressRuntimeVersion(account), accountEgressAuthorityRevision(account), route.ExpectedIdentity.PublicIP, endpoint, account.Credentials["chatgpt_account_id"])
	} else {
		if bindingID != "" || account.Proxy == nil || account.ProxyID == nil {
			return ""
		}
		raw = fmt.Sprintf("%d\x00%d\x00%s\x00%v", account.ID, *account.ProxyID, account.Proxy.URL(), account.Credentials["chatgpt_account_id"])
	}
	digest := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(digest[:])
}

// Called before pool admission. Required affinity prevents selecting a different
// exit and subsequently sending an opted-in model without its verified ticket.
func (s *OpenAIGatewayService) openAICodexTicketRequiredBinding(ctx context.Context, account *Account, outboundModel string) (string, error) {
	if s == nil || !isOpenAICodexTicketAccount(account) || !s.openAICodexTicketEnabledContext(ctx) {
		return "", nil
	}
	live, err := s.codexTicketLiveAccount(ctx, account)
	if err != nil {
		return "", ErrOpenAICodexTicketUnavailable
	}
	ac := codexAccountTicketConfigOf(live)
	if !ac.Enabled || ac.Model != normalizeOpenAICodexTicketModel(outboundModel) {
		return "", nil
	}
	ticket := s.lookupOpenAICodexTicket(live, ac.Model)
	if !ticket.validFor(live, ac, time.Now()) {
		return "", ErrOpenAICodexTicketUnavailable
	}
	return ticket.EgressBindingID, nil
}

func (s *OpenAIGatewayService) acquireCodexTicketBusinessAccount(ctx context.Context, account *Account) (*Account, func(), error) {
	if account.EgressMode == EgressModePool && !accountUsesEnforcedEgressPool(ctx, s.settingService, account) {
		return nil, nil, ErrAccountEgressUnavailable
	}
	acquired, err := acquireAccountSlotForSelectionWithBinding(ctx, s.concurrencyService, s.settingService, account, codexTicketBusinessBinding(account))
	if err != nil {
		return nil, nil, err
	}
	if acquired == nil || !acquired.Acquired {
		return nil, nil, ErrAccountEgressCapacityFull
	}
	release := acquired.ReleaseFunc
	if release == nil {
		release = func() {}
	}
	selected, err := PreserveSelectedAccountEgress(account, acquired.Account)
	if err != nil {
		release()
		return nil, nil, err
	}
	return selected, release, nil
}

func (s *OpenAIGatewayService) updateCodexTicketExtra(ctx context.Context, account *Account, updates map[string]any) error {
	store, ok := s.accountRepo.(CodexTicketStore)
	if !ok {
		return ErrOpenAICodexTicketUnavailable
	}
	keys := []string{codexAccountTicketConfigKey, codexTicketWatchdogExtraKey}
	changes := make(map[string]CodexTicketExtraValue, len(updates))
	for key, value := range updates {
		keys = append(keys, key)
		changes[key] = CodexTicketExtraValue{Exists: value != nil, Value: value}
	}
	changed, err := store.CompareAndSwapCodexTicket(ctx, account, CodexTicketExtraSnapshot(account, keys...), changes, nil)
	if err != nil {
		return err
	}
	if !changed {
		return ErrOpenAICodexTicketUnavailable
	}
	return nil
}

func codexTicketBusinessProxyURL(account *Account) string {
	if account != nil && account.Proxy != nil {
		return account.Proxy.URL()
	}
	return ""
}

func (s *OpenAIGatewayService) codexTicketBusinessReady(ctx context.Context, account *Account) bool {
	if !codexAccountTicketEligible(account) {
		return false
	}
	return account.EgressMode != EgressModePool || accountUsesEnforcedEgressPool(ctx, s.settingService, account)
}
