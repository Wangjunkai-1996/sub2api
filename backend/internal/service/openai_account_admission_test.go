package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type accountAdmissionRepo struct {
	AccountRepository
	latest *Account
	err    error
	cancel context.CancelFunc
	reads  int
}

func (r *accountAdmissionRepo) GetByID(context.Context, int64) (*Account, error) {
	r.reads++
	if r.cancel != nil {
		r.cancel()
	}
	return r.latest, r.err
}

func TestRecheckOpenAIAccountSchedulable(t *testing.T) {
	expired := time.Now().Add(-time.Minute)
	for _, tc := range []struct {
		name   string
		latest *Account
		err    error
		cancel bool
		want   error
	}{
		{name: "enabled", latest: &Account{Status: StatusActive, Schedulable: true}},
		{name: "disabled", latest: &Account{Status: StatusActive}, want: ErrNoAvailableAccounts},
		{name: "inactive", latest: &Account{Status: StatusDisabled, Schedulable: true}, want: ErrNoAvailableAccounts},
		{name: "expired", latest: &Account{Status: StatusActive, Schedulable: true, AutoPauseOnExpired: true, ExpiresAt: &expired}, want: ErrNoAvailableAccounts},
		{name: "deleted", err: ErrAccountNotFound, want: ErrNoAvailableAccounts},
		{name: "missing", want: ErrNoAvailableAccounts},
		{name: "canceled_after_read", latest: &Account{Status: StatusActive, Schedulable: true}, cancel: true, want: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			repo := &accountAdmissionRepo{latest: tc.latest, err: tc.err}
			if tc.cancel {
				repo.cancel = cancel
			}
			svc := &OpenAIGatewayService{accountRepo: repo}
			selected := &Account{ID: 1, Status: StatusActive, Schedulable: true}
			err := svc.RecheckOpenAIAccountSchedulable(ctx, selected)
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
			require.True(t, selected.Schedulable, "do not mutate request or cached objects")
			require.Equal(t, 1, repo.reads)
		})
	}
	t.Run("repository_failure_is_not_pool_exhaustion", func(t *testing.T) {
		unavailable := errors.New("database unavailable")
		svc := &OpenAIGatewayService{accountRepo: &accountAdmissionRepo{err: unavailable}}
		err := svc.RecheckOpenAIAccountSchedulable(context.Background(), &Account{ID: 1})
		require.ErrorIs(t, err, unavailable)
		require.NotErrorIs(t, err, ErrNoAvailableAccounts)
	})
	t.Run("canceled_before_read", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		repo := &accountAdmissionRepo{}
		svc := &OpenAIGatewayService{accountRepo: repo}
		require.ErrorIs(t, svc.RecheckOpenAIAccountSchedulable(ctx, &Account{ID: 1}), context.Canceled)
		require.Zero(t, repo.reads)
	})
}
