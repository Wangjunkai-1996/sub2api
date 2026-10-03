package service

import (
	"context"
	"errors"
	"strings"
	"time"
)

const SettingKeyAccountEgressPoolRolloutMode = "account_egress_pool_rollout_mode"

type AccountEgressPoolRolloutMode string

const (
	AccountEgressPoolRolloutOff     AccountEgressPoolRolloutMode = "off"
	AccountEgressPoolRolloutShadow  AccountEgressPoolRolloutMode = "shadow"
	AccountEgressPoolRolloutEnforce AccountEgressPoolRolloutMode = "enforce"

	accountEgressRolloutCacheTTL = 2 * time.Second
)

type cachedAccountEgressRolloutMode struct {
	mode              AccountEgressPoolRolloutMode
	expiresAt         time.Time
	hasSuccessfulRead bool
}

func NormalizeAccountEgressPoolRolloutMode(value string) AccountEgressPoolRolloutMode {
	switch AccountEgressPoolRolloutMode(strings.ToLower(strings.TrimSpace(value))) {
	case AccountEgressPoolRolloutShadow:
		return AccountEgressPoolRolloutShadow
	case AccountEgressPoolRolloutEnforce:
		return AccountEgressPoolRolloutEnforce
	default:
		return AccountEgressPoolRolloutOff
	}
}

// GetAccountEgressPoolRolloutMode is fail closed on cold start and for an
// explicitly missing or invalid value. After a successful read, transient read
// failures reuse that last value so a database blip cannot flip live traffic
// between the legacy and pool admission paths.
func (s *SettingService) GetAccountEgressPoolRolloutMode(ctx context.Context) AccountEgressPoolRolloutMode {
	if s == nil || s.settingRepo == nil {
		return AccountEgressPoolRolloutOff
	}
	now := time.Now()
	var cached *cachedAccountEgressRolloutMode
	if raw := s.accountEgressRolloutCache.Load(); raw != nil {
		cached, _ = raw.(*cachedAccountEgressRolloutMode)
		if cached != nil && now.Before(cached.expiresAt) {
			return cached.mode
		}
	}

	value, err, _ := s.accountEgressRolloutSF.Do("load", func() (any, error) {
		baseCtx := context.Background()
		if ctx != nil {
			baseCtx = context.WithoutCancel(ctx)
		}
		dbCtx, cancel := context.WithTimeout(baseCtx, gatewayForwardingDBTimeout)
		defer cancel()
		raw, readErr := s.settingRepo.GetValue(dbCtx, SettingKeyAccountEgressPoolRolloutMode)
		if readErr != nil {
			if errors.Is(readErr, ErrSettingNotFound) {
				return AccountEgressPoolRolloutOff, nil
			}
			return AccountEgressPoolRolloutOff, readErr
		}
		return NormalizeAccountEgressPoolRolloutMode(raw), nil
	})
	mode, _ := value.(AccountEgressPoolRolloutMode)
	hasSuccessfulRead := err == nil
	if err != nil {
		mode = AccountEgressPoolRolloutOff
		if cached != nil && cached.hasSuccessfulRead {
			mode = cached.mode
			hasSuccessfulRead = true
		}
	}
	s.accountEgressRolloutCache.Store(&cachedAccountEgressRolloutMode{
		mode:              mode,
		expiresAt:         now.Add(accountEgressRolloutCacheTTL),
		hasSuccessfulRead: hasSuccessfulRead,
	})
	return mode
}

func (s *SettingService) IsAccountEgressPoolEnforced(ctx context.Context, account *Account) bool {
	return account != nil && account.EgressMode == EgressModePool &&
		s.GetAccountEgressPoolRolloutMode(ctx) == AccountEgressPoolRolloutEnforce
}
