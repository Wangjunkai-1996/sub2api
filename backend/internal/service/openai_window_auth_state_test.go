package service

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

type openAIWindowAuthStateRepo struct {
	AccountRepository
	account      *Account
	casApplied   bool
	casErr       error
	casCalls     int
	tempCalls    int
	expected     map[string]any
	errorMessage string
	tempReason   string
	tempUntil    time.Time
}

func (r *openAIWindowAuthStateRepo) GetByID(context.Context, int64) (*Account, error) {
	return r.account, nil
}

func (r *openAIWindowAuthStateRepo) SetOpenAIAuthErrorIfCredentialsUnchanged(_ context.Context, _ int64, expected map[string]any, message string) (bool, error) {
	r.casCalls++
	r.expected = shallowCopyMap(expected)
	r.errorMessage = message
	return r.casApplied, r.casErr
}

func (r *openAIWindowAuthStateRepo) SetTempUnschedulable(_ context.Context, _ int64, until time.Time, reason string) error {
	r.tempCalls++
	r.tempUntil = until
	r.tempReason = reason
	return nil
}

type openAIWindowAuthRuntimeBlocker struct {
	calls  int
	reason string
}

func (b *openAIWindowAuthRuntimeBlocker) BlockAccountScheduling(_ *Account, _ time.Time, reason string) {
	b.calls++
	b.reason = reason
}

func (*openAIWindowAuthRuntimeBlocker) ClearAccountSchedulingBlock(int64) {}

type openAIWindowAuth403Counter struct {
	count int64
	calls int
}

func (c *openAIWindowAuth403Counter) IncrementOpenAI403Count(context.Context, int64, int) (int64, error) {
	c.calls++
	return c.count, nil
}

func (*openAIWindowAuth403Counter) ResetOpenAI403Count(context.Context, int64) error { return nil }

func openAIWindowAuthStateAccount() *Account {
	return &Account{
		ID: 77, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Status: StatusActive, Schedulable: true,
		Credentials: map[string]any{
			"access_token": "rejected-access", "refresh_token": "refresh-token", "_token_version": int64(7),
		},
	}
}

func TestRateLimitServiceOpenAIWindowPermanent401UsesCredentialsCAS(t *testing.T) {
	account := openAIWindowAuthStateAccount()
	repo := &openAIWindowAuthStateRepo{account: account, casApplied: true}
	blocker := &openAIWindowAuthRuntimeBlocker{}
	service := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
	service.SetAccountRuntimeBlocker(blocker)
	failure := OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusUnauthorized,
		Disposition:         OpenAIWindowWarmupAuthReplayRejected,
		ExpectedCredentials: shallowCopyMap(account.Credentials),
	}

	err := service.HandleOpenAIWindowWarmupAuthFailure(context.Background(), failure)

	require.NoError(t, err)
	require.Equal(t, 1, repo.casCalls)
	require.Equal(t, failure.ExpectedCredentials, repo.expected)
	require.NotContains(t, repo.errorMessage, "rejected-access")
	require.Equal(t, 1, blocker.calls)
	require.Equal(t, "auth_error", blocker.reason)
}

func TestRateLimitServiceOpenAIWindow401SkipsConcurrentCredentials(t *testing.T) {
	account := openAIWindowAuthStateAccount()
	expected := shallowCopyMap(account.Credentials)
	account.Credentials["access_token"] = "newly-authorized-access"
	account.Credentials["_token_version"] = int64(8)
	repo := &openAIWindowAuthStateRepo{account: account, casApplied: true}
	service := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)

	err := service.HandleOpenAIWindowWarmupAuthFailure(context.Background(), OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusUnauthorized,
		Disposition: OpenAIWindowWarmupAuthRefreshTerminal, ExpectedCredentials: expected,
	})

	require.ErrorIs(t, err, ErrOpenAIWindowWarmupCredentialsChanged)
	require.Zero(t, repo.casCalls)
}

func TestRateLimitServiceOpenAIWindow401ReportsCredentialsCASMiss(t *testing.T) {
	account := openAIWindowAuthStateAccount()
	repo := &openAIWindowAuthStateRepo{account: account, casApplied: false}
	service := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)

	err := service.HandleOpenAIWindowWarmupAuthFailure(context.Background(), OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusUnauthorized,
		Disposition:         OpenAIWindowWarmupAuthReplayRejected,
		ExpectedCredentials: shallowCopyMap(account.Credentials),
	})

	require.ErrorIs(t, err, ErrOpenAIWindowWarmupCredentialsChanged)
	require.Equal(t, 1, repo.casCalls)
}

func TestRateLimitServiceOpenAIWindowPermanent401PropagatesRepositoryFailure(t *testing.T) {
	account := openAIWindowAuthStateAccount()
	repo := &openAIWindowAuthStateRepo{account: account, casErr: errors.New("database unavailable")}
	service := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)

	err := service.HandleOpenAIWindowWarmupAuthFailure(context.Background(), OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusUnauthorized,
		Disposition:         OpenAIWindowWarmupAuthNotRefreshable,
		ExpectedCredentials: shallowCopyMap(account.Credentials),
	})

	require.ErrorContains(t, err, "database unavailable")
}

func TestRateLimitServiceOpenAIWindowTransient401ReusesTemporaryCooldown(t *testing.T) {
	account := openAIWindowAuthStateAccount()
	repo := &openAIWindowAuthStateRepo{account: account}
	service := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)

	err := service.HandleOpenAIWindowWarmupAuthFailure(context.Background(), OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusUnauthorized,
		Disposition:         OpenAIWindowWarmupAuthRefreshTransient,
		ExpectedCredentials: shallowCopyMap(account.Credentials),
	})

	require.NoError(t, err)
	require.Equal(t, 1, repo.tempCalls)
	require.Zero(t, repo.casCalls)
	require.Contains(t, repo.tempReason, "OAuth 401")
	require.True(t, repo.tempUntil.After(time.Now()))
}

func TestRateLimitServiceOpenAIWindowAgentIdentityTransientRecoveryDoesNotPermanentlyDisable(t *testing.T) {
	account := openAIWindowAuthStateAccount()
	delete(account.Credentials, "refresh_token")
	account.Credentials[openAIAuthModeCredentialKey] = OpenAIAuthModeAgentIdentity
	repo := &openAIWindowAuthStateRepo{account: account, casApplied: true}
	service := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)

	err := service.HandleOpenAIWindowWarmupAuthFailure(context.Background(), OpenAIWindowWarmupAuthFailure{
		AccountID: account.ID, StatusCode: http.StatusUnauthorized,
		Disposition:         OpenAIWindowWarmupAuthRefreshTransient,
		ExpectedCredentials: shallowCopyMap(account.Credentials),
	})

	require.NoError(t, err)
	require.Equal(t, 1, repo.tempCalls)
	require.Zero(t, repo.casCalls)
	require.Contains(t, repo.tempReason, "Agent Identity")
}

func TestRateLimitServiceOpenAIWindow403ReusesExistingPolicyWithoutRawBody(t *testing.T) {
	for _, test := range []struct {
		name        string
		disposition OpenAIWindowWarmupAuthDisposition
		count       int64
		wantCounter int
		wantTemp    int
		wantCAS     int
	}{
		{name: "html bypass", disposition: OpenAIWindowWarmupAuthForbiddenHTML},
		{name: "structured cooldown", disposition: OpenAIWindowWarmupAuthForbidden, count: 1, wantCounter: 1, wantTemp: 1},
		{name: "structured threshold", disposition: OpenAIWindowWarmupAuthForbidden, count: openAI403DisableThreshold, wantCounter: 1, wantCAS: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			account := openAIWindowAuthStateAccount()
			repo := &openAIWindowAuthStateRepo{account: account}
			repo.casApplied = true
			counter := &openAIWindowAuth403Counter{count: test.count}
			service := NewRateLimitService(repo, nil, &config.Config{}, nil, nil)
			service.SetOpenAI403CounterCache(counter)

			err := service.HandleOpenAIWindowWarmupAuthFailure(context.Background(), OpenAIWindowWarmupAuthFailure{
				AccountID: account.ID, StatusCode: http.StatusForbidden,
				Disposition:         test.disposition,
				ExpectedCredentials: shallowCopyMap(account.Credentials),
			})

			require.NoError(t, err)
			require.Equal(t, test.wantCounter, counter.calls)
			require.Equal(t, test.wantTemp, repo.tempCalls)
			require.Equal(t, test.wantCAS, repo.casCalls)
			require.NotContains(t, repo.tempReason, "access_token")
		})
	}
}
