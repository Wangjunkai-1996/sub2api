package service

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type openAICyberCooldownSettingRepo struct {
	SettingRepository
	values        map[string]string
	onGetMultiple func(context.Context)
}

func (r *openAICyberCooldownSettingRepo) GetMultiple(ctx context.Context, keys []string) (map[string]string, error) {
	if r.onGetMultiple != nil {
		r.onGetMultiple(ctx)
	}
	result := make(map[string]string, len(keys))
	for _, key := range keys {
		if value, ok := r.values[key]; ok {
			result[key] = value
		}
	}
	return result, nil
}

type openAICyberCooldownCache struct {
	GatewayCache
	mu            sync.Mutex
	strike        OpenAICyberAccountCooldownStrike
	err           error
	deadline      time.Time
	deadlineErr   error
	calls         int
	deadlineCalls int
	onRecord      func(context.Context)
}

func (c *openAICyberCooldownCache) RecordOpenAICyberAccountCooldownStrike(
	ctx context.Context,
	_ int64,
	_ string,
	_ time.Duration,
	firstDuration time.Duration,
	escalatedDuration time.Duration,
	now time.Time,
) (OpenAICyberAccountCooldownStrike, error) {
	c.mu.Lock()
	c.calls++
	strike := c.strike
	err := c.err
	c.mu.Unlock()
	if c.onRecord != nil {
		c.onRecord(ctx)
	}
	if err != nil {
		return OpenAICyberAccountCooldownStrike{}, err
	}
	if strike.EventRecordedAt.IsZero() {
		strike.EventRecordedAt = now.UTC()
	}
	duration := escalatedDuration
	if strike.Strikes == 1 {
		duration = firstDuration
	}
	if strike.EventCooldownUntil.IsZero() {
		strike.EventCooldownUntil = strike.EventRecordedAt.Add(duration)
	}
	if strike.AccountCooldownUntil.IsZero() {
		strike.AccountCooldownUntil = strike.EventCooldownUntil
	}
	c.mu.Lock()
	if strike.AccountCooldownUntil.After(c.deadline) {
		c.deadline = strike.AccountCooldownUntil
	}
	c.mu.Unlock()
	return strike, nil
}

func (c *openAICyberCooldownCache) GetOpenAICyberAccountCooldownDeadline(context.Context, int64) (time.Time, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deadlineCalls++
	return c.deadline, c.deadlineErr
}

type openAICyberCooldownAccountRepo struct {
	AccountRepository
	until  time.Time
	reason string
	calls  int
	err    error
	onSet  func()
}

func (r *openAICyberCooldownAccountRepo) SetTempUnschedulable(_ context.Context, _ int64, until time.Time, reason string) error {
	r.calls++
	r.until = until
	r.reason = reason
	if r.onSet != nil {
		r.onSet()
	}
	return r.err
}

func newOpenAICyberCooldownService(enabled bool, cache *openAICyberCooldownCache, repo *openAICyberCooldownAccountRepo) *OpenAIGatewayService {
	settingService := NewSettingService(&openAICyberCooldownSettingRepo{values: map[string]string{
		SettingKeyOpenAICyberAccountCooldownEnabled:          strconv.FormatBool(enabled),
		SettingKeyOpenAICyberAccountCooldownWindowSeconds:    "86400",
		SettingKeyOpenAICyberAccountCooldownFirstSeconds:     "3600",
		SettingKeyOpenAICyberAccountCooldownEscalatedSeconds: "86400",
	}}, nil)
	return &OpenAIGatewayService{settingService: settingService, cache: cache, accountRepo: repo}
}

func TestOpenAICyberAccountCooldownFirstAndEscalatedTiers(t *testing.T) {
	for _, tc := range []struct {
		name         string
		strike       OpenAICyberAccountCooldownStrike
		wantDuration time.Duration
	}{
		{name: "first", strike: OpenAICyberAccountCooldownStrike{Strikes: 1}, wantDuration: time.Hour},
		{name: "escalated", strike: OpenAICyberAccountCooldownStrike{Strikes: 2}, wantDuration: 24 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := &openAICyberCooldownCache{strike: tc.strike}
			repo := &openAICyberCooldownAccountRepo{}
			svc := newOpenAICyberCooldownService(true, cache, repo)
			account := &Account{ID: 10616, Platform: PlatformOpenAI}
			repo.onSet = func() {
				require.True(t, svc.isOpenAIAccountRuntimeBlocked(account), "runtime block must precede durable persistence")
				require.False(t, svc.isOpenAICyberCooldownClassificationPending(account.ID), "pending gate must be cleared after the formal runtime block is installed")
			}
			started := time.Now()

			result := svc.ApplyCyberPolicyAccountCooldown(context.Background(), account, OpenAICyberAccountCooldownEvent{RequestID: "req-1", Code: "cyber_policy", ObservedAt: started})

			require.True(t, result.Applied)
			require.Equal(t, tc.strike.Duplicate, result.Duplicate)
			require.Equal(t, tc.strike.Strikes, result.Strikes)
			require.False(t, result.RedisFallback)
			require.Equal(t, 1, cache.calls)
			require.Equal(t, 1, repo.calls)
			require.WithinDuration(t, started.Add(tc.wantDuration), repo.until, 2*time.Second)
			require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
		})
	}
}

func TestOpenAICyberAccountEventDigestIsStableAndAccountScoped(t *testing.T) {
	event := OpenAICyberAccountCooldownEvent{
		RequestID:          "req-stable",
		ClientRequestID:    "connection-stable",
		RequestPayloadHash: "payload-hash",
		Code:               "cyber_policy",
		UpstreamStatus:     400,
		UpstreamBody:       "same upstream body",
		ObservedAt:         time.Unix(100, 0),
	}
	first := openAICyberAccountEventDigest(10616, event)
	require.Equal(t, first, openAICyberAccountEventDigest(10616, event), "the same marked event must remain stable")
	event.ObservedAt = time.Unix(200, 0)
	require.NotEqual(t, first, openAICyberAccountEventDigest(10616, event), "separate turns with identical connection evidence must not be deduplicated")
	require.NotEqual(t, first, openAICyberAccountEventDigest(10617, event), "account ID must isolate event markers")

	fallbackA := openAICyberAccountEventDigest(10616, OpenAICyberAccountCooldownEvent{Code: "cyber_policy", ObservedAt: time.Unix(100, 0)})
	fallbackB := openAICyberAccountEventDigest(10616, OpenAICyberAccountCooldownEvent{Code: "cyber_policy", ObservedAt: time.Unix(200, 0)})
	require.NotEqual(t, fallbackA, fallbackB, "observation time separates otherwise unidentifiable events")
}

func TestOpenAICyberAccountCooldownDuplicateReusesOriginalDeadline(t *testing.T) {
	eventRecordedAt := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	cache := &openAICyberCooldownCache{strike: OpenAICyberAccountCooldownStrike{
		Strikes: 1, Duplicate: true, EventRecordedAt: eventRecordedAt,
	}}
	repo := &openAICyberCooldownAccountRepo{}
	svc := newOpenAICyberCooldownService(true, cache, repo)
	account := &Account{ID: 10616, Platform: PlatformOpenAI}

	first := svc.ApplyCyberPolicyAccountCooldown(context.Background(), account, OpenAICyberAccountCooldownEvent{RequestID: "req-duplicate"})
	firstUntil := repo.until
	second := svc.ApplyCyberPolicyAccountCooldown(context.Background(), account, OpenAICyberAccountCooldownEvent{RequestID: "req-duplicate"})

	require.True(t, first.Applied)
	require.True(t, first.Duplicate)
	require.Equal(t, 1, first.Strikes)
	require.True(t, second.Applied)
	require.True(t, second.Duplicate)
	require.Equal(t, eventRecordedAt.Add(time.Hour), firstUntil)
	require.Equal(t, firstUntil, repo.until, "reprocessing one event must not move its deadline")
	require.Equal(t, 2, cache.calls)
	require.Equal(t, 2, repo.calls)
	require.Contains(t, repo.reason, "strikes=1")
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAICyberAccountCooldownExpiredDuplicateDoesNotRestartWindow(t *testing.T) {
	cache := &openAICyberCooldownCache{strike: OpenAICyberAccountCooldownStrike{
		Strikes: 1, Duplicate: true, EventRecordedAt: time.Now().UTC().Add(-2 * time.Hour),
	}}
	repo := &openAICyberCooldownAccountRepo{}
	svc := newOpenAICyberCooldownService(true, cache, repo)
	account := &Account{ID: 10616, Platform: PlatformOpenAI}

	result := svc.ApplyCyberPolicyAccountCooldown(context.Background(), account, OpenAICyberAccountCooldownEvent{RequestID: "req-expired-duplicate"})

	require.False(t, result.Applied)
	require.True(t, result.Duplicate)
	require.Equal(t, 1, result.Strikes)
	require.Zero(t, repo.calls)
	require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAICyberAccountCooldownPendingGateBlocksSchedulingDuringDedup(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	cache := &openAICyberCooldownCache{
		strike: OpenAICyberAccountCooldownStrike{Strikes: 1, Duplicate: true, EventRecordedAt: time.Now().UTC()},
		onRecord: func(ctx context.Context) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		},
	}
	repo := &openAICyberCooldownAccountRepo{}
	svc := newOpenAICyberCooldownService(true, cache, repo)
	account := &Account{ID: 10616, Platform: PlatformOpenAI}
	resultCh := make(chan OpenAICyberAccountCooldownResult, 1)

	go func() {
		resultCh <- svc.ApplyCyberPolicyAccountCooldown(context.Background(), account, OpenAICyberAccountCooldownEvent{RequestID: "req-pending"})
	}()

	<-entered
	require.True(t, svc.isOpenAICyberCooldownClassificationPending(account.ID))
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account), "scheduler must reject the account while Redis classifies the event")
	close(release)
	result := <-resultCh

	require.True(t, result.Duplicate)
	require.True(t, result.Applied)
	require.False(t, svc.isOpenAICyberCooldownClassificationPending(account.ID))
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account), "duplicate classification must restore the formal block")
	require.Equal(t, 1, repo.calls)
}

func TestOpenAICyberAccountCooldownPendingGateCoversPolicyLookup(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	settingRepo := &openAICyberCooldownSettingRepo{
		values: map[string]string{
			SettingKeyOpenAICyberAccountCooldownEnabled: "false",
		},
		onGetMultiple: func(ctx context.Context) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		},
	}
	cache := &openAICyberCooldownCache{strike: OpenAICyberAccountCooldownStrike{Strikes: 1}}
	repo := &openAICyberCooldownAccountRepo{}
	svc := &OpenAIGatewayService{
		settingService: NewSettingService(settingRepo, nil),
		cache:          cache,
		accountRepo:    repo,
	}
	account := &Account{ID: 10616, Platform: PlatformOpenAI}
	resultCh := make(chan OpenAICyberAccountCooldownResult, 1)

	go func() {
		resultCh <- svc.ApplyCyberPolicyAccountCooldown(context.Background(), account, OpenAICyberAccountCooldownEvent{RequestID: "req-policy-load"})
	}()

	<-entered
	require.True(t, svc.isOpenAICyberCooldownClassificationPending(account.ID))
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account), "scheduler must reject the account while the runtime policy loads")
	close(release)
	result := <-resultCh

	require.False(t, result.Applied)
	require.False(t, svc.isOpenAICyberCooldownClassificationPending(account.ID))
	require.False(t, svc.isOpenAIAccountRuntimeBlocked(account), "disabled policy must leave no formal block")
	require.Zero(t, cache.calls)
	require.Zero(t, repo.calls)
}

func TestOpenAICyberAccountCooldownPendingGateIsReferenceCounted(t *testing.T) {
	svc := &OpenAIGatewayService{}
	account := &Account{ID: 10616, Platform: PlatformOpenAI}
	clearFirst := svc.beginOpenAICyberCooldownClassification(account.ID)
	clearSecond := svc.beginOpenAICyberCooldownClassification(account.ID)

	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
	clearFirst()
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account), "one in-flight classification must keep the gate closed")
	clearFirst()
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account), "cleanup must be idempotent")
	clearSecond()
	require.False(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAICyberAccountCooldownUsesShortRedisDeadline(t *testing.T) {
	deadlineRemaining := make(chan time.Duration, 1)
	cache := &openAICyberCooldownCache{
		strike: OpenAICyberAccountCooldownStrike{Strikes: 1},
		onRecord: func(ctx context.Context) {
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			deadlineRemaining <- time.Until(deadline)
		},
	}
	svc := newOpenAICyberCooldownService(true, cache, &openAICyberCooldownAccountRepo{})

	result := svc.ApplyCyberPolicyAccountCooldown(context.Background(), &Account{ID: 10616, Platform: PlatformOpenAI}, OpenAICyberAccountCooldownEvent{RequestID: "req-deadline"})

	require.True(t, result.Applied)
	remaining := <-deadlineRemaining
	require.Positive(t, remaining)
	require.LessOrEqual(t, remaining, openAICyberAccountCooldownRedisTimeout)
}

func TestOpenAICyberAccountCooldownRedisFailureUsesEscalatedTier(t *testing.T) {
	cache := &openAICyberCooldownCache{err: errors.New("redis unavailable")}
	repo := &openAICyberCooldownAccountRepo{}
	svc := newOpenAICyberCooldownService(true, cache, repo)
	account := &Account{ID: 10616, Platform: PlatformOpenAI}
	started := time.Now()

	result := svc.ApplyCyberPolicyAccountCooldown(context.Background(), account, OpenAICyberAccountCooldownEvent{RequestID: "req-fallback"})

	require.True(t, result.Applied)
	require.True(t, result.RedisFallback)
	require.WithinDuration(t, started.Add(24*time.Hour), repo.until, 2*time.Second)
	require.Contains(t, repo.reason, "redis_fallback")
}

func TestOpenAICyberAccountCooldownDisabledOrNonOpenAISkips(t *testing.T) {
	cache := &openAICyberCooldownCache{strike: OpenAICyberAccountCooldownStrike{Strikes: 1}}
	repo := &openAICyberCooldownAccountRepo{}
	disabled := newOpenAICyberCooldownService(false, cache, repo)
	require.False(t, disabled.ApplyCyberPolicyAccountCooldown(context.Background(), &Account{ID: 1, Platform: PlatformOpenAI}, OpenAICyberAccountCooldownEvent{}).Applied)

	enabled := newOpenAICyberCooldownService(true, cache, repo)
	require.False(t, enabled.ApplyCyberPolicyAccountCooldown(context.Background(), &Account{ID: 2, Platform: PlatformGrok}, OpenAICyberAccountCooldownEvent{}).Applied)
	require.Zero(t, cache.calls)
	require.Zero(t, repo.calls)
}

func TestOpenAICyberAccountCooldownPersistenceFailureKeepsRuntimeBlock(t *testing.T) {
	cache := &openAICyberCooldownCache{strike: OpenAICyberAccountCooldownStrike{Strikes: 1}}
	repo := &openAICyberCooldownAccountRepo{err: errors.New("db unavailable")}
	svc := newOpenAICyberCooldownService(true, cache, repo)
	account := &Account{ID: 10616, Platform: PlatformOpenAI}

	result := svc.ApplyCyberPolicyAccountCooldown(context.Background(), account, OpenAICyberAccountCooldownEvent{RequestID: "req-db-error"})

	require.Error(t, result.PersistenceError)
	require.True(t, svc.isOpenAIAccountRuntimeBlocked(account))
}

func TestOpenAICyberAccountCooldownPersistenceFailureBlocksAnotherServiceInstance(t *testing.T) {
	cache := &openAICyberCooldownCache{strike: OpenAICyberAccountCooldownStrike{Strikes: 1}}
	writer := newOpenAICyberCooldownService(true, cache, &openAICyberCooldownAccountRepo{err: errors.New("db unavailable")})
	reader := newOpenAICyberCooldownService(true, cache, &openAICyberCooldownAccountRepo{})
	account := &Account{ID: 10616, Platform: PlatformOpenAI}

	result := writer.ApplyCyberPolicyAccountCooldown(context.Background(), account, OpenAICyberAccountCooldownEvent{RequestID: "req-cross-instance"})

	require.Error(t, result.PersistenceError)
	require.ErrorIs(t, reader.RecheckOpenAICyberAccountCooldown(context.Background(), account), ErrOpenAICyberAccountCooldownBlocked)
	require.True(t, reader.isOpenAIAccountRuntimeBlocked(account), "Redis deadline must install a runtime block in the other instance")
	require.Equal(t, 1, cache.deadlineCalls)
}

func TestOpenAICyberAccountCooldownRecheckFailsClosedWhenRedisReadFails(t *testing.T) {
	cache := &openAICyberCooldownCache{deadlineErr: errors.New("redis unavailable")}
	svc := newOpenAICyberCooldownService(true, cache, &openAICyberCooldownAccountRepo{})
	account := &Account{ID: 10616, Platform: PlatformOpenAI}

	err := svc.RecheckOpenAICyberAccountCooldown(context.Background(), account)

	require.ErrorIs(t, err, ErrOpenAICyberAccountCooldownStateUnavailable)
	require.Equal(t, 1, cache.deadlineCalls)
}
