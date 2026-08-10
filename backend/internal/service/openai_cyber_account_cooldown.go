package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"log/slog"
	"strconv"
	"strings"
	"time"
)

const (
	openAICyberAccountCooldownReason       = "upstream_cyber_policy"
	openAICyberAccountCooldownRedisTimeout = 500 * time.Millisecond
)

// OpenAICyberAccountCooldownStrike is the atomic Redis result for one real
// upstream Cyber event. Duplicate events preserve the current strike count.
type OpenAICyberAccountCooldownStrike struct {
	Strikes   int
	Duplicate bool
}

// OpenAICyberAccountCooldownStore is implemented by the Redis gateway cache.
// It is deliberately separate from GatewayCache so existing test doubles and
// decorators do not need unrelated methods.
type OpenAICyberAccountCooldownStore interface {
	RecordOpenAICyberAccountCooldownStrike(
		ctx context.Context,
		accountID int64,
		eventDigest string,
		window time.Duration,
		now time.Time,
	) (OpenAICyberAccountCooldownStrike, error)
}

// OpenAICyberAccountCooldownEvent contains only request/upstream evidence from
// a CyberPolicyMark. Local policy blocks and audit failures never create it.
type OpenAICyberAccountCooldownEvent struct {
	RequestID          string
	ClientRequestID    string
	RequestPayloadHash string
	Code               string
	UpstreamStatus     int
	UpstreamBody       string
	ObservedAt         time.Time
}

type OpenAICyberAccountCooldownResult struct {
	Applied          bool
	Duplicate        bool
	RedisFallback    bool
	Strikes          int
	CooldownUntil    time.Time
	PersistenceError error
}

func writeOpenAICyberEventDigestPart(h hash.Hash, value string) {
	value = strings.TrimSpace(value)
	_, _ = h.Write([]byte(strconv.Itoa(len(value))))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(value))
	_, _ = h.Write([]byte{'|'})
}

func openAICyberAccountEventDigest(accountID int64, event OpenAICyberAccountCooldownEvent) string {
	h := sha256.New()
	writeOpenAICyberEventDigestPart(h, strconv.FormatInt(accountID, 10))
	writeOpenAICyberEventDigestPart(h, event.RequestID)
	writeOpenAICyberEventDigestPart(h, event.ClientRequestID)
	writeOpenAICyberEventDigestPart(h, event.RequestPayloadHash)
	writeOpenAICyberEventDigestPart(h, event.Code)
	writeOpenAICyberEventDigestPart(h, strconv.Itoa(event.UpstreamStatus))
	writeOpenAICyberEventDigestPart(h, event.UpstreamBody)
	// ObservedAt is assigned once when the real upstream event is first marked.
	// It keeps repeated handling of that mark idempotent while distinguishing
	// separate WebSocket turns that can share connection IDs and payloads.
	if !event.ObservedAt.IsZero() {
		writeOpenAICyberEventDigestPart(h, strconv.FormatInt(event.ObservedAt.UTC().UnixNano(), 10))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (s *OpenAIGatewayService) openAICyberAccountCooldownStore() OpenAICyberAccountCooldownStore {
	if s == nil || s.cache == nil {
		return nil
	}
	store, _ := s.cache.(OpenAICyberAccountCooldownStore)
	return store
}

func openAICyberAccountCooldownRedisContext(ctx context.Context) (context.Context, context.CancelFunc) {
	base := context.Background()
	if ctx != nil {
		base = context.WithoutCancel(ctx)
	}
	return context.WithTimeout(base, openAICyberAccountCooldownRedisTimeout)
}

// ApplyCyberPolicyAccountCooldown applies the post-upstream account circuit
// breaker. Redis failures select the escalated tier; the runtime block is
// installed before the durable, extend-only account update.
func (s *OpenAIGatewayService) ApplyCyberPolicyAccountCooldown(
	ctx context.Context,
	account *Account,
	event OpenAICyberAccountCooldownEvent,
) OpenAICyberAccountCooldownResult {
	result := OpenAICyberAccountCooldownResult{}
	if s == nil || account == nil || account.ID <= 0 || account.Platform != PlatformOpenAI || s.settingService == nil {
		return result
	}

	clearPending := s.beginOpenAICyberCooldownClassification(account.ID)
	pending := true
	defer func() {
		if pending {
			clearPending()
		}
	}()
	policy := s.settingService.GetOpenAICyberAccountCooldownRuntime(ctx)
	if !policy.Enabled() {
		return result
	}

	now := time.Now().UTC()
	duration := policy.EscalatedDuration()
	strike := OpenAICyberAccountCooldownStrike{}
	store := s.openAICyberAccountCooldownStore()
	if store == nil {
		result.RedisFallback = true
		slog.Warn("openai cyber account cooldown store unavailable; using escalated tier", "account_id", account.ID)
	} else {
		stateCtx, cancel := openAICyberAccountCooldownRedisContext(ctx)
		var err error
		strike, err = store.RecordOpenAICyberAccountCooldownStrike(
			stateCtx,
			account.ID,
			openAICyberAccountEventDigest(account.ID, event),
			policy.Window(),
			now,
		)
		cancel()
		if err != nil || strike.Strikes <= 0 {
			result.RedisFallback = true
			if err == nil {
				err = errors.New("invalid zero strike count")
			}
			slog.Warn("openai cyber account cooldown strike failed; using escalated tier", "account_id", account.ID, "error", err)
		} else {
			result.Strikes = strike.Strikes
			result.Duplicate = strike.Duplicate
			if strike.Strikes == 1 {
				duration = policy.FirstDuration()
			}
		}
	}

	until := now.Add(duration)
	reason := fmt.Sprintf("%s:strikes=%d", openAICyberAccountCooldownReason, result.Strikes)
	if result.RedisFallback {
		reason = openAICyberAccountCooldownReason + ":redis_fallback"
	}

	// Install the in-process guard first so a persistence failure cannot return
	// this account to the current process scheduler. Keep the classification gate
	// active until the formal runtime block is visible, then remove only this
	// event's pending reference before touching the database.
	s.BlockAccountScheduling(account, until, reason)
	clearPending()
	pending = false
	result.Applied = true
	result.CooldownUntil = until

	if s.accountRepo == nil {
		result.PersistenceError = errors.New("account repository unavailable")
		slog.Error("openai cyber account cooldown persistence unavailable", "account_id", account.ID)
		return result
	}
	persistCtx, cancel := openAIAccountStateContext(ctx)
	result.PersistenceError = s.accountRepo.SetTempUnschedulable(persistCtx, account.ID, until, reason)
	cancel()
	if result.PersistenceError != nil {
		slog.Error("openai cyber account cooldown persistence failed", "account_id", account.ID, "error", result.PersistenceError)
	}
	return result
}
