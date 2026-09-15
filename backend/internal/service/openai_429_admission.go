package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// OpenAI429CooldownError means an eligible account is temporarily waiting for
// its shared recovery round. It is not a missing account or an unhealthy IP.
type OpenAI429CooldownError struct {
	RetryAfter time.Duration
}

func (e *OpenAI429CooldownError) Error() string {
	return fmt.Sprintf("openai account recovery pending; retry after %s", e.RetryAfter)
}

// Pool cooldowns report the earliest recovery opportunity. Other admission
// errors retain the scheduler's existing last-error precedence.
func preferEarlierOpenAI429Cooldown(previous, next error) error {
	var previousCooldown, nextCooldown *OpenAI429CooldownError
	if errors.As(previous, &previousCooldown) && errors.As(next, &nextCooldown) &&
		previousCooldown.RetryAfter < nextCooldown.RetryAfter {
		return previous
	}
	return next
}

// Confirmed recovery rejections must not become concurrency wait plans.
type openAI429SelectionRejections map[int64]error

func (c *openAI429SelectionRejections) record(accountID int64, err error) {
	var cooldown *OpenAI429CooldownError
	if !errors.As(err, &cooldown) && !errors.Is(err, ErrOpenAI429RecoveryUnavailable) {
		return
	}
	if *c == nil {
		*c = make(openAI429SelectionRejections)
	}
	(*c)[accountID] = preferEarlierOpenAI429Cooldown((*c)[accountID], err)
}

// AdmitOpenAI429Selection runs after authoritative selection and again after a
// WaitPlan acquires capacity. The caller owns releasing the slot on error.
func (s *OpenAIGatewayService) AdmitOpenAI429Selection(ctx context.Context, selection *AccountSelectionResult) error {
	if selection == nil || selection.Account == nil || !selection.Acquired {
		return nil
	}
	account := selection.Account
	if !account.IsOpenAIOAuthLike() || account.IsShadow() || account.OpenAI429Attempt != nil {
		return nil
	}
	// Standalone services without concurrency admission have no shared cache.
	// A configured concurrency service must support recovery and fails closed.
	if s == nil || s.concurrencyService == nil {
		return nil
	}
	attempt, delay, err := s.concurrencyService.BeginOpenAI429Attempt(ctx, account.ID, "")
	if err != nil {
		return err
	}
	if attempt == nil {
		return &OpenAI429CooldownError{RetryAfter: delay}
	}
	requestAccount := *account
	requestAccount.OpenAI429Attempt = attempt
	selection.Account = &requestAccount
	release := selection.ReleaseFunc
	var once sync.Once
	selection.ReleaseFunc = func() {
		once.Do(func() {
			if release != nil {
				release()
			}
			attempt.Release()
		})
	}
	return nil
}
