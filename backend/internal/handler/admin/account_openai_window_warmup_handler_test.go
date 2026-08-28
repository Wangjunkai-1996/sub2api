//go:build unit

package admin

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	servermiddleware "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type warmupHandlerAdminService struct {
	service.AdminService
	account  *service.Account
	getCalls int
}

func (s *warmupHandlerAdminService) GetAccount(context.Context, int64) (*service.Account, error) {
	s.getCalls++
	copyAccount := *s.account
	return &copyAccount, nil
}

type warmupHandlerAccountRepository struct {
	service.AccountRepository
	account  *service.Account
	getCalls int
}

func (r *warmupHandlerAccountRepository) GetByID(context.Context, int64) (*service.Account, error) {
	r.getCalls++
	copyAccount := *r.account
	return &copyAccount, nil
}

type warmupHandlerRepository struct {
	service.OpenAIWindowWarmupRepository
	mu              sync.Mutex
	current         *service.OpenAIWindowWarmupJob
	getCurrentErr   error
	enqueueCalls    int
	unblockCalls    int
	getCurrentCalls int
}

func (r *warmupHandlerRepository) GetCurrent(context.Context, int64, string) (*service.OpenAIWindowWarmupJob, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.getCurrentCalls++
	if r.getCurrentErr != nil {
		return nil, r.getCurrentErr
	}
	if r.current == nil {
		return nil, sql.ErrNoRows
	}
	copyJob := *r.current
	return &copyJob, nil
}

func (r *warmupHandlerRepository) Enqueue(_ context.Context, in service.OpenAIWindowWarmupEnqueue) (*service.OpenAIWindowWarmupJob, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.enqueueCalls++
	if r.current != nil {
		copyJob := *r.current
		return &copyJob, false, nil
	}
	r.current = &service.OpenAIWindowWarmupJob{
		ID:              91,
		AccountID:       in.AccountID,
		QuotaScope:      in.QuotaScope,
		State:           service.OpenAIWindowWarmupStatePending,
		Trigger:         in.Trigger,
		CycleKey:        in.CycleKey,
		ObservedResetAt: in.ObservedResetAt,
		NextAttemptAt:   in.NextAttemptAt,
	}
	copyJob := *r.current
	return &copyJob, true, nil
}

func (r *warmupHandlerRepository) UnblockAccount(_ context.Context, accountID int64, next time.Time, reset *time.Time) (*service.OpenAIWindowWarmupJob, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.unblockCalls++
	if r.current == nil {
		r.current = &service.OpenAIWindowWarmupJob{ID: 92, AccountID: accountID, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal}
	}
	r.current.State = service.OpenAIWindowWarmupStatePending
	r.current.NextAttemptAt = next
	r.current.ObservedResetAt = reset
	copyJob := *r.current
	return &copyJob, true, nil
}

func warmupHandlerTestAccount() *service.Account {
	return &service.Account{
		ID: 42, Name: "warmup", Platform: service.PlatformOpenAI,
		Type: service.AccountTypeOAuth, Status: service.StatusActive, Schedulable: true,
		CreatedAt: time.Date(2026, 8, 28, 1, 2, 3, 0, time.UTC),
		Extra: map[string]any{
			service.OpenAICodexWarmupPolicyExtraKey: service.OpenAIWindowWarmupPolicyContinuous,
		},
	}
}

func setupWarmupHandlerRouter(t *testing.T, repo *warmupHandlerRepository) (*gin.Engine, *warmupHandlerAdminService, *warmupHandlerAccountRepository, *service.OpenAIWindowWarmupService) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	account := warmupHandlerTestAccount()
	adminService := &warmupHandlerAdminService{account: account}
	accountRepo := &warmupHandlerAccountRepository{account: account}
	warmup := service.NewOpenAIWindowWarmupService(repo, accountRepo, nil, nil, nil, service.OpenAIWindowWarmupOptions{})
	handler := NewAccountHandler(adminService, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	handler.SetOpenAIWindowWarmupService(warmup, nil)

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(servermiddleware.ContextKeyUser), servermiddleware.AuthSubject{UserID: 77})
		c.Next()
	})
	router.GET("/api/v1/admin/codex-window-warmup/metrics", handler.GetOpenAIWindowWarmupMetrics)
	router.POST("/api/v1/admin/accounts/:id/codex-warmup/requeue", handler.RequeueOpenAIWindowWarmup)
	router.POST("/api/v1/admin/accounts/:id/codex-warmup/unblock", handler.UnblockOpenAIWindowWarmup)
	router.POST("/api/v1/admin/codex-window-warmup/requeue-batch", handler.RequeueOpenAIWindowWarmupBatch)
	router.POST("/api/v1/admin/codex-window-warmup/policy-batch", handler.UpdateOpenAIWindowWarmupPolicyBatch)
	return router, adminService, accountRepo, warmup
}

func TestOpenAIWindowWarmupMetricsHandlerReturnsBoundedSnapshot(t *testing.T) {
	repo := &warmupHandlerRepository{}
	router, _, _, warmup := setupWarmupHandlerRouter(t, repo)
	_, inserted, err := warmup.ScheduleAccountWarmup(context.Background(), warmupHandlerTestAccount(), service.OpenAIWindowWarmupTriggerImport)
	require.NoError(t, err)
	require.True(t, inserted)

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/admin/codex-window-warmup/metrics", nil))

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
	var payload struct {
		Data map[string]any `json:"data"`
	}
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &payload))
	require.Equal(t, float64(1), payload.Data["enqueued"])
	for _, key := range []string{
		"started", "success", "failed", "retry", "uncertain", "real_traffic_suppressed",
		"duplicate_suppressed", "due", "oldest_due_age_seconds", "inflight", "reset_lag_seconds",
	} {
		require.Contains(t, payload.Data, key)
	}
	require.NotContains(t, recorder.Body.String(), "account_id")
}

func TestScheduleOpenAIWindowWarmupReturnsTriggerProjectionWhenCurrentReadFails(t *testing.T) {
	repo := &warmupHandlerRepository{getCurrentErr: context.DeadlineExceeded}
	_, _, _, warmup := setupWarmupHandlerRouter(t, repo)

	status, err := scheduleOpenAIWindowWarmup(
		context.Background(),
		warmup,
		warmupHandlerTestAccount(),
		service.OpenAIWindowWarmupTriggerImport,
	)

	require.NoError(t, err)
	require.NotNil(t, status)
	require.True(t, status.Queued)
	require.Equal(t, "queued_by_trigger", status.State)
	require.Equal(t, "projection_unavailable", status.LastErrorCode)
	require.Empty(t, status.LastError)
	require.Nil(t, status.Job)
	require.Equal(t, 1, repo.getCurrentCalls)
	require.Zero(t, repo.enqueueCalls)
}

func TestOpenAIWindowWarmupRequeueHandlerReplaysIdempotencyKey(t *testing.T) {
	previousCoordinator := service.DefaultIdempotencyCoordinator()
	service.SetDefaultIdempotencyCoordinator(service.NewIdempotencyCoordinator(newMemoryIdempotencyRepoStub(), service.DefaultIdempotencyConfig()))
	t.Cleanup(func() { service.SetDefaultIdempotencyCoordinator(previousCoordinator) })

	repo := &warmupHandlerRepository{}
	router, adminService, accountRepo, _ := setupWarmupHandlerRouter(t, repo)
	call := func() *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/accounts/42/codex-warmup/requeue", bytes.NewBufferString(`{}`))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Idempotency-Key", "warmup-requeue-42")
		router.ServeHTTP(recorder, request)
		return recorder
	}

	first := call()
	second := call()
	require.Equal(t, http.StatusOK, first.Code)
	require.Equal(t, http.StatusOK, second.Code)
	require.Equal(t, "true", second.Header().Get("X-Idempotency-Replayed"))
	require.Equal(t, 1, repo.enqueueCalls)
	require.Equal(t, 1, adminService.getCalls)
	require.Equal(t, 1, accountRepo.getCalls)
}

func TestOpenAIWindowWarmupUnblockHandler(t *testing.T) {
	previousCoordinator := service.DefaultIdempotencyCoordinator()
	service.SetDefaultIdempotencyCoordinator(nil)
	t.Cleanup(func() { service.SetDefaultIdempotencyCoordinator(previousCoordinator) })

	repo := &warmupHandlerRepository{current: &service.OpenAIWindowWarmupJob{
		ID: 92, AccountID: 42, QuotaScope: service.OpenAIWindowWarmupQuotaScopeGlobal,
		State: service.OpenAIWindowWarmupStateBlocked,
	}}
	router, _, _, _ := setupWarmupHandlerRouter(t, repo)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/accounts/42/codex-warmup/unblock", nil)
	router.ServeHTTP(recorder, request)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, 1, repo.unblockCalls)
	require.Contains(t, recorder.Body.String(), `"changed":true`)
	require.Contains(t, recorder.Body.String(), `"state":"pending"`)
}

func TestOpenAIWindowWarmupBatchHandlersValidateInput(t *testing.T) {
	previousCoordinator := service.DefaultIdempotencyCoordinator()
	service.SetDefaultIdempotencyCoordinator(nil)
	t.Cleanup(func() { service.SetDefaultIdempotencyCoordinator(previousCoordinator) })

	router, adminService, _, _ := setupWarmupHandlerRouter(t, &warmupHandlerRepository{})
	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "requeue requires IDs", path: "/api/v1/admin/codex-window-warmup/requeue-batch", body: `{}`},
		{name: "policy rejects nonpositive ID", path: "/api/v1/admin/codex-window-warmup/policy-batch", body: `{"account_ids":[0],"policy":"continuous"}`},
		{name: "policy rejects invalid value", path: "/api/v1/admin/codex-window-warmup/policy-batch", body: `{"account_ids":[42],"policy":"forever"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, test.path, bytes.NewBufferString(test.body))
			request.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(recorder, request)
			require.Equal(t, http.StatusBadRequest, recorder.Code)
		})
	}
	require.Zero(t, adminService.getCalls)
}
