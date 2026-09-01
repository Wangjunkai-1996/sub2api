package service

import (
	"context"
	"errors"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"go.uber.org/zap"
)

func acquireAccountSlotForSelection(
	ctx context.Context,
	concurrency *ConcurrencyService,
	settings *SettingService,
	account *Account,
) (*AcquireResult, error) {
	return acquireAccountSlotForSelectionWithBinding(
		ctx,
		concurrency,
		settings,
		account,
		RequiredAccountEgressBindingFromContext(ctx),
	)
}

func acquireAccountSlotForSelectionWithBinding(
	ctx context.Context,
	concurrency *ConcurrencyService,
	settings *SettingService,
	account *Account,
	requiredBindingID string,
) (*AcquireResult, error) {
	if account == nil {
		return nil, ErrAccountEgressConfigStale
	}
	rollout := AccountEgressPoolRolloutOff
	if settings != nil {
		rollout = settings.GetAccountEgressPoolRolloutMode(ctx)
	}
	if accountSupportsEgressPoolRuntime(account) && account.EgressMode == EgressModePool && rollout == AccountEgressPoolRolloutShadow {
		if config, err := AccountEgressPoolConfigForRuntime(account, 0); err != nil {
			logger.L().Warn("account_egress_shadow_config_invalid",
				zap.Int64("account_id", account.ID),
				zap.Error(err),
			)
		} else {
			logger.L().Debug("account_egress_shadow_evaluated",
				zap.Int("effective_capacity", config.EffectiveCapacity()),
				zap.Int("candidate_count", len(config.Candidates)),
			)
		}
	}
	if !accountSupportsEgressPoolRuntime(account) || account.EgressMode != EgressModePool || rollout != AccountEgressPoolRolloutEnforce {
		if concurrency == nil {
			return &AcquireResult{Acquired: true, ReleaseFunc: func() {}, Account: account.CloneForRequest()}, nil
		}
		admissionCtx := ctx
		if accountUsesLegacyEgressMirror(concurrency, account, rollout) {
			if admission, resolveErr := resolveLegacyAccountEgressAdmission(account); resolveErr == nil {
				admissionCtx = contextWithLegacyAccountEgressAdmission(ctx, admission)
			}
		}
		result, err := concurrency.AcquireAccountSlot(admissionCtx, account.ID, account.Concurrency)
		if result != nil && result.Acquired {
			requestAccount := account.CloneForRequest()
			if result.LegacyEgressAdmission != nil {
				requestAccount.LegacyEgressAdmission = result.LegacyEgressAdmission
			}
			result.Account = requestAccount
		}
		return result, err
	}
	if concurrency == nil {
		return nil, ErrAccountEgressUnavailable
	}
	config, err := AccountEgressPoolConfigForRuntime(account, 0)
	if err != nil {
		return nil, err
	}
	resolved, err := concurrency.AcquireAccountEgress(ctx, AccountEgressAcquireRequest{
		Config:            config,
		RequiredBindingID: strings.TrimSpace(requiredBindingID),
	})
	if err != nil {
		return nil, err
	}
	requestAccount, err := withResolvedAccountEgressSelection(account, resolved)
	if err != nil {
		resolved.Lease.Release()
		return nil, err
	}
	return &AcquireResult{
		Acquired:    true,
		ReleaseFunc: resolved.Lease.Release,
		Account:     requestAccount,
		Egress:      resolved,
	}, nil
}

func accountUsesLegacyEgressMirror(
	concurrency *ConcurrencyService,
	account *Account,
	rollout AccountEgressPoolRolloutMode,
) bool {
	if concurrency == nil || concurrency.cache == nil || !accountSupportsEgressPoolRuntime(account) ||
		account.EgressMode != EgressModePool ||
		(rollout != AccountEgressPoolRolloutOff && rollout != AccountEgressPoolRolloutShadow) {
		return false
	}
	_, supported := concurrency.cache.(AccountEgressLegacySlotCache)
	return supported
}

func isAccountEgressAdmissionError(err error) bool {
	return errors.Is(err, ErrAccountEgressCapacityFull) ||
		errors.Is(err, ErrAccountEgressUnavailable) ||
		errors.Is(err, ErrAccountEgressNoRoute) ||
		errors.Is(err, ErrAccountEgressConfigStale)
}

func accountUsesEnforcedEgressPool(ctx context.Context, settings *SettingService, account *Account) bool {
	return settings != nil && accountSupportsEgressPoolRuntime(account) && account.EgressMode == EgressModePool &&
		settings.GetAccountEgressPoolRolloutMode(ctx) == AccountEgressPoolRolloutEnforce
}

// getAccountLoadsForScheduling merges legacy counters with pool lease loads.
// Invalid or unreadable pool state is represented as saturated so a scheduler
// can still use healthy legacy accounts without ever failing a pool open.
func getAccountLoadsForScheduling(
	ctx context.Context,
	concurrency *ConcurrencyService,
	settings *SettingService,
	accounts []*Account,
	fresh bool,
) (map[int64]*AccountLoadInfo, error) {
	loads := make(map[int64]*AccountLoadInfo, len(accounts))
	if concurrency == nil || len(accounts) == 0 {
		return loads, nil
	}

	legacy := make([]AccountWithConcurrency, 0, len(accounts))
	poolConfigs := make([]AccountEgressPoolConfig, 0, len(accounts))
	for _, account := range accounts {
		if account == nil {
			continue
		}
		if !accountUsesEnforcedEgressPool(ctx, settings, account) {
			legacy = append(legacy, AccountWithConcurrency{
				ID:             account.ID,
				MaxConcurrency: account.EffectiveLoadFactor(),
			})
			continue
		}
		config, err := AccountEgressPoolConfigForRuntime(account, 0)
		if err != nil {
			loads[account.ID] = &AccountLoadInfo{AccountID: account.ID, LoadRate: 100}
			continue
		}
		poolConfigs = append(poolConfigs, config)
	}

	var legacyErr error
	if len(legacy) > 0 {
		var legacyLoads map[int64]*AccountLoadInfo
		if fresh {
			legacyLoads, legacyErr = concurrency.GetAccountsLoadBatchFresh(ctx, legacy)
		} else {
			legacyLoads, legacyErr = concurrency.GetAccountsLoadBatch(ctx, legacy)
		}
		for accountID, load := range legacyLoads {
			loads[accountID] = load
		}
	}

	if len(poolConfigs) > 0 {
		poolLoads, err := concurrency.GetAccountEgressLoads(ctx, poolConfigs)
		if err != nil {
			for _, config := range poolConfigs {
				loads[config.AccountID] = &AccountLoadInfo{AccountID: config.AccountID, LoadRate: 100}
			}
		} else {
			for _, config := range poolConfigs {
				load := poolLoads[config.AccountID]
				loadRate := 100
				if load.EffectiveCapacity > 0 && load.Status != AccountEgressStatusConfigStale && load.Status != AccountEgressStatusConfigUnavailable {
					loadRate = load.ActiveTotal * 100 / load.EffectiveCapacity
				}
				loads[config.AccountID] = &AccountLoadInfo{
					AccountID:          config.AccountID,
					CurrentConcurrency: load.ActiveTotal,
					WaitingCount:       load.WaitingCount,
					LoadRate:           loadRate,
				}
			}
		}
	}

	return loads, legacyErr
}

func selectionAccount(acquired *AcquireResult, fallback *Account) *Account {
	// A pool admission attaches the resolved transport to its request-local
	// account and must win. Legacy admission only mirrors the candidate that was
	// used to reserve a slot; prefer the later DB-rechecked account in fallback.
	if acquired != nil && acquired.Account != nil &&
		(acquired.Egress != nil || acquired.LegacyEgressAdmission != nil) {
		return acquired.Account
	}
	if fallback != nil {
		return fallback
	}
	if acquired != nil {
		return acquired.Account
	}
	return fallback
}
