//go:build unit

package handler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type schedulableSlotRepo struct {
	service.AccountRepository
	current service.Account
}

func (r *schedulableSlotRepo) GetByID(context.Context, int64) (*service.Account, error) {
	a := r.current
	return &a, nil
}

type schedulableSlotSnapshot struct {
	service.SchedulerCache
	repo *schedulableSlotRepo
}

func (s *schedulableSlotSnapshot) GetAccount(ctx context.Context, id int64) (*service.Account, error) {
	return s.repo.GetByID(ctx, id)
}

type schedulableSlotCache struct {
	*helperConcurrencyCacheStub
	repo             *schedulableSlotRepo
	disableOnAcquire bool
}

func (s *schedulableSlotCache) AcquireAccountSlot(ctx context.Context, id int64, max int, requestID string) (bool, error) {
	acquired, err := s.helperConcurrencyCacheStub.AcquireAccountSlot(ctx, id, max, requestID)
	if acquired && s.disableOnAcquire {
		s.repo.current.Schedulable = false
	}
	return acquired, err
}

// The selection snapshot is valid when selected; the authoritative repository
// and cache become disabled before admission finishes. No upstream is called.
func TestOpenAIAccountAdmissionDisabledAfterSelection(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name     string
		acquired bool
		seq      []bool
		disable  bool
	}{
		{"already_acquired_then_disabled", true, nil, true},
		{"disabled_during_fast_acquire", false, []bool{true}, true},
		{"disabled_during_queued_acquire", false, []bool{false, true}, true},
		{"enabled_queued_control", false, []bool{false, true}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			selected := profitSlotTestAccount(1, 0.3)
			repo := &schedulableSlotRepo{current: *selected}
			cache := &schedulableSlotCache{helperConcurrencyCacheStub: &helperConcurrencyCacheStub{accountSeq: tc.seq, waitAllowed: true}, repo: repo, disableOnAcquire: tc.disable}
			concurrency := service.NewConcurrencyService(cache)
			cfg := &config.Config{}
			snapshot := service.NewSchedulerSnapshotService(&schedulableSlotSnapshot{repo: repo}, nil, repo, nil, cfg)
			gw := service.NewOpenAIGatewayService(repo, nil, nil, nil, nil, nil, nil, cfg, snapshot, concurrency, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
			h := &OpenAIGatewayHandler{gatewayService: gw, concurrencyHelper: NewConcurrencyHelper(concurrency, SSEPingFormatClaude, 0)}
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			c.Request = httptest.NewRequest("POST", "/v1/responses", nil)
			selection := &service.AccountSelectionResult{Account: selected, Acquired: tc.acquired, WaitPlan: &service.AccountWaitPlan{AccountID: 1, MaxConcurrency: 2, Timeout: time.Second, MaxWaiting: 2}}
			if tc.acquired {
				repo.current.Schedulable = false
				selection.ReleaseFunc = func() { cache.accountReleaseCalls++ }
			}
			streamStarted := false
			release, result := h.acquireResponsesAccountSlot(c, nil, "", selection, false, &streamStarted, zap.NewNop())
			if release != nil {
				defer release()
			}
			t.Logf("authoritative_schedulable=%v admitted=%v acquire_calls=%d response_bytes=%d", repo.current.Schedulable, result == openAISlotAcquireOK, cache.accountAcquireCalls, w.Body.Len())
			if tc.disable {
				if result != openAISlotAcquireReselect || release != nil || selection.Acquired || selection.ReleaseFunc != nil {
					t.Fatalf("disabled account must release and reselect: result=%v selection=%+v", result, selection)
				}
				if cache.accountReleaseCalls != 1 || w.Body.Len() != 0 {
					t.Fatalf("rejection must release once without output: releases=%d body=%s", cache.accountReleaseCalls, w.Body.String())
				}
			}
			if !tc.disable && result != openAISlotAcquireOK {
				t.Errorf("healthy control rejected: %v", result)
			}
		})
	}
}

type schedulableFailoverCache struct {
	*helperConcurrencyCacheStub
	accounts   []service.Account
	disableAll bool
}

func (s *schedulableFailoverCache) AcquireAccountSlot(ctx context.Context, id int64, max int, requestID string) (bool, error) {
	acquired, err := s.helperConcurrencyCacheStub.AcquireAccountSlot(ctx, id, max, requestID)
	if acquired && (s.disableAll || id == 1) {
		for i := range s.accounts {
			if s.accounts[i].ID == id {
				s.accounts[i].Schedulable = false
			}
		}
	}
	return acquired, err
}

func TestOpenAIAccountAdmissionReselectsWithoutDispatch(t *testing.T) {
	for _, allDisabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("all_disabled_%t", allDisabled), func(t *testing.T) {
			accounts := newOpenAIResponsesDispatchBudgetAccounts(2)
			for i := range accounts {
				accounts[i].Concurrency = 1
			}
			cache := &schedulableFailoverCache{helperConcurrencyCacheStub: &helperConcurrencyCacheStub{accountSeq: []bool{true, true}}, accounts: accounts, disableAll: allDisabled}
			upstream := &openAIResponsesDispatchBudgetUpstream{successID: 2}
			h := newRecoveryAdmissionTestHandler(t, cache, accounts, upstream)
			c, rec := newOpenAIResponsesDispatchBudgetContext(t, context.Background())
			h.Responses(c)
			if allDisabled {
				require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
				require.Contains(t, rec.Body.String(), "account_pool_exhausted")
				require.Empty(t, upstream.calls, "pre-dispatch rejection must not reach the upstream")
			} else {
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				require.Equal(t, []int64{2}, upstream.calls, "only the healthy alternate may be dispatched")
			}
			require.Empty(t, rec.Header().Get("X-Sub2-Retry-Status"), "a rejected selection does not spend the dispatch budget")
			require.Equal(t, 2, cache.accountReleaseCalls, "each acquired slot must be released once")
		})
	}
}
