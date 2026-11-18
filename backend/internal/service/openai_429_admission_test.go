//go:build unit

package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type selectionRecoveryTestCache struct {
	schedulerTestConcurrencyCache
	deferred map[int64]time.Duration
	stateErr error
	probe    bool
	attempts []int64
}

func (c *selectionRecoveryTestCache) AcquireOpenAI429Attempt(ctx context.Context, id int64, model, token string, ttl time.Duration) (OpenAI429Admission, error) {
	c.attempts = append(c.attempts, id)
	if c.stateErr != nil {
		return OpenAI429Admission{}, c.stateErr
	}
	if delay := c.deferred[id]; delay > 0 {
		return OpenAI429Admission{RetryAfter: delay}, nil
	}
	return OpenAI429Admission{Allowed: true, Generation: token, Probe: c.probe}, nil
}

func TestOpenAI429SelectionSkipsCoolingAccountAndReleasesSlot(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	accounts := []Account{
		{ID: 7001, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0},
		{ID: 7002, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 1},
	}
	var released []int64
	cache := &selectionRecoveryTestCache{
		schedulerTestConcurrencyCache: schedulerTestConcurrencyCache{releasedIDs: &released},
		deferred:                      map[int64]time.Duration{7001: 2 * time.Second},
	}
	s := &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: accounts},
		concurrencyService: NewConcurrencyService(cache),
		cfg:                &config.Config{},
	}
	selection, _, err := s.SelectAccountWithScheduler(context.Background(), nil, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
	require.NoError(t, err)
	require.NotNil(t, selection)
	assert.Equal(t, int64(7002), selection.Account.ID)
	assert.Equal(t, []int64{7001}, released)
	assert.Equal(t, []int64{7001, 7002}, cache.attempts)
	assert.NotNil(t, selection.Account.OpenAI429Attempt)
	assert.Nil(t, accounts[1].OpenAI429Attempt, "cached account must not acquire request state")
	selection.ReleaseFunc()
	selection.ReleaseFunc()
	assert.Equal(t, []int64{7001, 7002}, released)
}

func TestOpenAI429SelectionPreservesCooldownAndFailsClosed(t *testing.T) {
	resetOpenAIAdvancedSchedulerSettingCacheForTest()
	account := Account{ID: 7003, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1}
	cache := &selectionRecoveryTestCache{deferred: map[int64]time.Duration{account.ID: 90 * time.Second}}
	s := &OpenAIGatewayService{
		accountRepo:        schedulerTestOpenAIAccountRepo{accounts: []Account{account}},
		concurrencyService: NewConcurrencyService(cache), cfg: &config.Config{},
	}
	selection, _, err := s.SelectAccountWithScheduler(context.Background(), nil, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
	require.Error(t, err)
	assert.Nil(t, selection)
	var cooldown *OpenAI429CooldownError
	require.ErrorAs(t, err, &cooldown)
	assert.Equal(t, 90*time.Second, cooldown.RetryAfter)
	cache.stateErr = errors.New("cache offline")
	selection, _, err = s.SelectAccountWithScheduler(context.Background(), nil, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
	assert.Nil(t, selection)
	assert.ErrorIs(t, err, ErrOpenAI429RecoveryUnavailable)
}

func TestOpenAI429SelectionReturnsEarliestPoolRecovery(t *testing.T) {
	for _, mode := range []string{"default", "default load-aware", "advanced"} {
		t.Run(mode, func(t *testing.T) {
			resetOpenAIAdvancedSchedulerSettingCacheForTest()
			t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)
			accounts := []Account{
				{ID: 7006, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0},
				{ID: 7007, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 1},
			}
			cache := &selectionRecoveryTestCache{deferred: map[int64]time.Duration{
				7006: 2 * time.Second, 7007: 90 * time.Second,
			}}
			s := &OpenAIGatewayService{
				accountRepo:        schedulerTestOpenAIAccountRepo{accounts: accounts},
				concurrencyService: NewConcurrencyService(cache), cfg: &config.Config{},
			}
			if mode == "advanced" {
				s.rateLimitService = newOpenAIAdvancedSchedulerRateLimitService("true")
			} else if mode == "default load-aware" {
				s.cfg.Gateway.Scheduling.LoadBatchEnabled = true
			}

			selection, _, err := s.SelectAccountWithScheduler(context.Background(), nil, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)

			var cooldown *OpenAI429CooldownError
			require.ErrorAs(t, err, &cooldown)
			assert.Nil(t, selection)
			assert.Equal(t, 2*time.Second, cooldown.RetryAfter)
			assert.Contains(t, cache.attempts, int64(7006))
			assert.Contains(t, cache.attempts, int64(7007))

			cache.stateErr = errors.New("cache offline")
			selection, _, err = s.SelectAccountWithScheduler(context.Background(), nil, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)
			assert.ErrorIs(t, err, ErrOpenAI429RecoveryUnavailable)
			assert.Nil(t, selection, "a rejected recovery check must not become a concurrency wait plan")
		})
	}
}

func TestOpenAI429SelectionKeepsHealthyCapacityWaitPlan(t *testing.T) {
	for _, mode := range []string{"default load-aware", "advanced"} {
		t.Run(mode, func(t *testing.T) {
			resetOpenAIAdvancedSchedulerSettingCacheForTest()
			t.Cleanup(resetOpenAIAdvancedSchedulerSettingCacheForTest)
			accounts := []Account{
				{ID: 7008, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 0},
				{ID: 7009, Platform: PlatformOpenAI, Type: AccountTypeOAuth, Status: StatusActive, Schedulable: true, Concurrency: 1, Priority: 1},
			}
			cache := &selectionRecoveryTestCache{
				schedulerTestConcurrencyCache: schedulerTestConcurrencyCache{acquireResults: map[int64]bool{7009: false}},
				deferred:                      map[int64]time.Duration{7008: 90 * time.Second},
			}
			s := &OpenAIGatewayService{
				accountRepo:        schedulerTestOpenAIAccountRepo{accounts: accounts},
				concurrencyService: NewConcurrencyService(cache), cfg: &config.Config{},
			}
			s.cfg.Gateway.Scheduling.LoadBatchEnabled = true
			if mode == "advanced" {
				s.rateLimitService = newOpenAIAdvancedSchedulerRateLimitService("true")
			}

			selection, _, err := s.SelectAccountWithScheduler(context.Background(), nil, "", "", "gpt-5.1", nil, OpenAIUpstreamTransportAny, false)

			require.NoError(t, err)
			require.NotNil(t, selection)
			require.NotNil(t, selection.WaitPlan)
			assert.Equal(t, int64(7009), selection.Account.ID)
			assert.False(t, selection.Acquired)
		})
	}
}

func TestOpenAI429AdmissionErrorAggregationPreservesOtherFailures(t *testing.T) {
	short := &OpenAI429CooldownError{RetryAfter: 2 * time.Second}
	long := &OpenAI429CooldownError{RetryAfter: 90 * time.Second}
	assert.Same(t, short, preferEarlierOpenAI429Cooldown(short, long))
	assert.Same(t, short, preferEarlierOpenAI429Cooldown(long, short))
	assert.ErrorIs(t, preferEarlierOpenAI429Cooldown(short, ErrAccountEgressUnavailable), ErrAccountEgressUnavailable)
	assert.Same(t, long, preferEarlierOpenAI429Cooldown(ErrAccountEgressUnavailable, long))
	assert.ErrorIs(t, preferEarlierOpenAI429Cooldown(short, ErrOpenAI429RecoveryUnavailable), ErrOpenAI429RecoveryUnavailable)
}

func TestOpenAI429WaitPlanAdmissionIsDeferredUntilSlotAcquired(t *testing.T) {
	cache := &selectionRecoveryTestCache{deferred: map[int64]time.Duration{7004: time.Second}}
	s := &OpenAIGatewayService{concurrencyService: NewConcurrencyService(cache)}
	selection := &AccountSelectionResult{Account: &Account{ID: 7004, Platform: PlatformOpenAI, Type: AccountTypeOAuth}}
	require.NoError(t, s.AdmitOpenAI429Selection(context.Background(), selection))
	assert.Empty(t, cache.attempts)
	selection.Acquired = true
	var cooldown *OpenAI429CooldownError
	require.ErrorAs(t, s.AdmitOpenAI429Selection(context.Background(), selection), &cooldown)
	assert.Nil(t, selection.Account.OpenAI429Attempt)
	assert.Equal(t, []int64{7004}, cache.attempts)
}

func TestOpenAI429HTTPProbeCancellationPreservesRequestLifetime(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		name := "lost probe cancels only upstream"
		if accepted {
			name = "accepted probe keeps generating until body closes"
		}
		t.Run(name, func(t *testing.T) {
			parent, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
			defer cancel()
			cache := &selectionRecoveryTestCache{probe: true}
			attempt, _, err := NewConcurrencyService(cache).BeginOpenAI429Attempt(parent, 7005, "")
			require.NoError(t, err)
			t.Cleanup(attempt.Release)
			upstream := &httpUpstreamRecorder{resp: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("data"))}}
			s := &OpenAIGatewayService{httpUpstream: upstream}
			request, err := http.NewRequestWithContext(parent, http.MethodPost, "https://example.test/v1/responses", nil)
			require.NoError(t, err)
			response, err := s.doOpenAIUpstream(request, "", &Account{ID: 7005, OpenAI429Attempt: attempt})
			require.NoError(t, err)
			upstreamCtx := upstream.lastReq.Context()
			if accepted {
				_, err = attempt.Accepted(parent)
				require.NoError(t, err)
				assert.NoError(t, upstreamCtx.Err())
				assert.NoError(t, attempt.Context().Err())
				deadline, ok := upstreamCtx.Deadline()
				require.True(t, ok)
				parentDeadline, _ := parent.Deadline()
				assert.Equal(t, parentDeadline, deadline)
			} else {
				attempt.cancel(ErrOpenAI429ProbeLost)
				select {
				case <-upstreamCtx.Done():
				case <-time.After(time.Second):
					t.Fatal("lost recovery lease did not cancel upstream transport")
				}
				assert.ErrorIs(t, context.Cause(upstreamCtx), ErrOpenAI429ProbeLost)
			}
			require.NoError(t, response.Body.Close())
			assert.ErrorIs(t, upstreamCtx.Err(), context.Canceled)
			assert.NoError(t, parent.Err())
		})
	}
}
