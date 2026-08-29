package admin

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"time"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/Wei-Shaw/sub2api/internal/util/logredact"
	"github.com/gin-gonic/gin"
)

const maxOpenAIWindowWarmupBatchAccounts = 1000

// OpenAIWindowWarmupStatusResponse is the redacted account-level projection
// exposed to administrators. It deliberately excludes lease ownership and any
// upstream response body.
type OpenAIWindowWarmupStatusResponse struct {
	Policy          service.OpenAIWindowWarmupPolicy `json:"policy"`
	State           string                           `json:"state,omitempty"`
	NextRunAt       *time.Time                       `json:"next_run_at,omitempty"`
	NextAttemptAt   *time.Time                       `json:"next_attempt_at,omitempty"`
	LastSuccessAt   *time.Time                       `json:"last_success_at,omitempty"`
	ObservedResetAt *time.Time                       `json:"observed_reset_at,omitempty"`
	AttemptCount    int                              `json:"attempt_count,omitempty"`
	LastErrorCode   string                           `json:"last_error_code,omitempty"`
	LastError       string                           `json:"last_error,omitempty"`
	Queued          bool                             `json:"queued"`
	Changed         bool                             `json:"changed,omitempty"`
	Job             *service.OpenAIWindowWarmupJob   `json:"job,omitempty"`
}

type openAIWindowWarmupBatchRequest struct {
	AccountIDs []int64 `json:"account_ids" binding:"required"`
}

type openAIWindowWarmupBatchPolicyRequest struct {
	AccountIDs []int64 `json:"account_ids"`
	GroupIDs   []int64 `json:"group_ids"`
	Policy     string  `json:"policy" binding:"required"`
}

type openAIWindowWarmupBatchItem struct {
	AccountID int64  `json:"account_id"`
	Queued    bool   `json:"queued"`
	Changed   bool   `json:"changed,omitempty"`
	State     string `json:"state,omitempty"`
	Policy    string `json:"policy,omitempty"`
	Error     string `json:"error,omitempty"`
}

type openAIWindowWarmupBatchResult struct {
	Total   int                           `json:"total"`
	Queued  int                           `json:"queued"`
	Updated int                           `json:"updated,omitempty"`
	Skipped int                           `json:"skipped"`
	Failed  int                           `json:"failed"`
	Results []openAIWindowWarmupBatchItem `json:"results"`
}

type openAIWindowWarmupMetricsResponse struct {
	Enqueued              int64 `json:"enqueued"`
	Started               int64 `json:"started"`
	Success               int64 `json:"success"`
	Failed                int64 `json:"failed"`
	Retry                 int64 `json:"retry"`
	Uncertain             int64 `json:"uncertain"`
	RealTrafficSuppressed int64 `json:"real_traffic_suppressed"`
	DuplicateSuppressed   int64 `json:"duplicate_suppressed"`
	Due                   int64 `json:"due"`
	OldestDueAgeSeconds   int64 `json:"oldest_due_age_seconds"`
	Inflight              int64 `json:"inflight"`
	ResetLagSeconds       int64 `json:"reset_lag_seconds"`
}

func isOpenAIWindowWarmupAccount(account *service.Account) bool {
	return account != nil && account.Platform == service.PlatformOpenAI &&
		account.Type == service.AccountTypeOAuth && account.ParentAccountID == nil &&
		account.QuotaDimensionOrDefault() == service.QuotaDimensionGlobal
}

func validateOpenAIWindowWarmupPolicy(raw string) (service.OpenAIWindowWarmupPolicy, error) {
	normalized := strings.ToLower(strings.TrimSpace(raw))
	switch normalized {
	case service.OpenAIWindowWarmupPolicyOff,
		service.OpenAIWindowWarmupPolicyInitialOnce,
		service.OpenAIWindowWarmupPolicyContinuous:
		return service.OpenAIWindowWarmupPolicy(normalized), nil
	default:
		return service.OpenAIWindowWarmupPolicyOff,
			infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_POLICY_INVALID", "OpenAI window warmup policy must be off, initial_once, or continuous")
	}
}

// resolveOpenAIWindowWarmupImportPolicy uses the global default only when the
// request omitted the field. An explicit off always wins.
func resolveOpenAIWindowWarmupImportPolicy(ctx context.Context, requested *string, settings *service.SettingService) (service.OpenAIWindowWarmupPolicy, error) {
	if requested != nil {
		return validateOpenAIWindowWarmupPolicy(*requested)
	}
	if settings == nil {
		return service.OpenAIWindowWarmupPolicyOff, nil
	}
	current, err := settings.GetAllSettings(ctx)
	if err != nil {
		return service.OpenAIWindowWarmupPolicyOff, err
	}
	return validateOpenAIWindowWarmupPolicy(current.OpenAIWindowWarmupDefaultPolicy)
}

func withOpenAIWindowWarmupPolicy(extra map[string]any, policy service.OpenAIWindowWarmupPolicy) map[string]any {
	out := make(map[string]any, len(extra)+1)
	for key, value := range extra {
		out[key] = value
	}
	delete(out, service.OpenAICodexWarmupPolicyExtraKey)
	delete(out, service.CodexWarmupPolicyExtraKey)
	delete(out, service.OpenAIWindowWarmupPolicyExtraKey)
	// Persist off explicitly so exports and later imports can distinguish an
	// intentional account override from a missing policy that inherits defaults.
	out[service.OpenAICodexWarmupPolicyExtraKey] = string(policy)
	return out
}

func redactedWarmupError(message string) string {
	message = logredact.RedactText(message, "agent_private_key", "private_key", "prompt", "response")
	if len(message) > 512 {
		message = message[:512]
	}
	return message
}

func openAIWindowWarmupStatus(account *service.Account, job *service.OpenAIWindowWarmupJob) *OpenAIWindowWarmupStatusResponse {
	if !isOpenAIWindowWarmupAccount(account) {
		return nil
	}
	status := &OpenAIWindowWarmupStatusResponse{
		Policy: service.OpenAIWindowWarmupPolicyForAccount(account),
		Queued: job != nil,
	}
	if job == nil {
		return status
	}
	copyJob := *job
	copyJob.LastError = redactedWarmupError(copyJob.LastError)
	status.State = copyJob.State
	status.NextRunAt = &copyJob.NextAttemptAt
	status.NextAttemptAt = &copyJob.NextAttemptAt
	status.LastSuccessAt = copyJob.LastSuccessAt
	status.ObservedResetAt = copyJob.ObservedResetAt
	status.AttemptCount = copyJob.AttemptCount
	status.LastErrorCode = copyJob.LastErrorCode
	status.LastError = copyJob.LastError
	status.Job = &copyJob
	return status
}

func (h *AccountHandler) enrichOpenAIWindowWarmup(ctx context.Context, accounts []service.Account, items []AccountWithConcurrency) {
	if len(accounts) == 0 || len(items) == 0 {
		return
	}
	ids := make([]int64, 0, len(accounts))
	accountsByID := make(map[int64]*service.Account, len(accounts))
	for i := range accounts {
		account := &accounts[i]
		if !isOpenAIWindowWarmupAccount(account) {
			continue
		}
		ids = append(ids, account.ID)
		accountsByID[account.ID] = account
	}
	jobs := map[int64]*service.OpenAIWindowWarmupJob{}
	if h != nil && h.openAIWindowWarmup != nil && len(ids) > 0 {
		var err error
		jobs, err = h.openAIWindowWarmup.CurrentJobsForAccounts(ctx, ids)
		if err != nil {
			slog.Warn("openai_window_warmup_account_status_batch_failed", "error", redactedWarmupError(err.Error()))
			jobs = map[int64]*service.OpenAIWindowWarmupJob{}
		}
	}
	for i := range items {
		if items[i].Account == nil {
			continue
		}
		account := accountsByID[items[i].ID]
		if account == nil {
			continue
		}
		items[i].OpenAIWindowWarmup = openAIWindowWarmupStatus(account, jobs[account.ID])
	}
}

func (h *AccountHandler) scheduleOpenAIWindowWarmup(ctx context.Context, account *service.Account, trigger string) (*OpenAIWindowWarmupStatusResponse, error) {
	var warmup *service.OpenAIWindowWarmupService
	if h != nil {
		warmup = h.openAIWindowWarmup
	}
	return scheduleOpenAIWindowWarmup(ctx, warmup, account, trigger)
}

func scheduleOpenAIWindowWarmup(ctx context.Context, warmup *service.OpenAIWindowWarmupService, account *service.Account, trigger string) (*OpenAIWindowWarmupStatusResponse, error) {
	if !isOpenAIWindowWarmupAccount(account) {
		return nil, nil
	}
	if !service.OpenAIWindowWarmupPolicyForAccount(account).Enabled() {
		return openAIWindowWarmupStatus(account, nil), nil
	}
	if warmup == nil {
		return nil, infraerrors.ServiceUnavailable("OPENAI_WINDOW_WARMUP_UNAVAILABLE", "OpenAI window warmup service is unavailable")
	}
	job, _, err := warmup.ScheduleAccountWarmup(ctx, account, trigger)
	if err != nil {
		// The migration trigger already enqueues the initial cycle in the same
		// transaction as the account write. A transient projection read failure
		// must not turn that committed write into a failed HTTP response.
		slog.Warn("openai_window_warmup_projection_unavailable",
			"account_id", account.ID,
			"error", redactedWarmupError(err.Error()),
		)
		return &OpenAIWindowWarmupStatusResponse{
			Policy:        service.OpenAIWindowWarmupPolicyForAccount(account),
			State:         "queued_by_trigger",
			Queued:        true,
			LastErrorCode: "projection_unavailable",
		}, nil
	}
	return openAIWindowWarmupStatus(account, job), nil
}

func (h *AccountHandler) requireOpenAIWindowWarmup(c *gin.Context) bool {
	if h == nil || h.openAIWindowWarmup == nil {
		response.ErrorFrom(c, infraerrors.ServiceUnavailable("OPENAI_WINDOW_WARMUP_UNAVAILABLE", "OpenAI window warmup service is unavailable"))
		return false
	}
	return true
}

func openAIWindowWarmupAccountID(c *gin.Context) (int64, bool) {
	accountID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || accountID <= 0 {
		response.BadRequest(c, "Invalid account ID")
		return 0, false
	}
	return accountID, true
}

func (h *AccountHandler) GetOpenAIWindowWarmup(c *gin.Context) {
	if !h.requireOpenAIWindowWarmup(c) {
		return
	}
	accountID, ok := openAIWindowWarmupAccountID(c)
	if !ok {
		return
	}
	account, err := h.adminService.GetAccount(c.Request.Context(), accountID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if !isOpenAIWindowWarmupAccount(account) {
		response.ErrorFrom(c, infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_ACCOUNT_INELIGIBLE", "Account is not an OpenAI OAuth parent account"))
		return
	}
	jobs, err := h.openAIWindowWarmup.CurrentJobsForAccounts(c.Request.Context(), []int64{accountID})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, openAIWindowWarmupStatus(account, jobs[accountID]))
}

// GetOpenAIWindowWarmupMetrics returns one process-local counter snapshot plus
// database-derived queue gauges. It intentionally has no account labels.
func (h *AccountHandler) GetOpenAIWindowWarmupMetrics(c *gin.Context) {
	if !h.requireOpenAIWindowWarmup(c) {
		return
	}
	c.Header("Cache-Control", "no-store")
	metrics := h.openAIWindowWarmup.Metrics()
	response.Success(c, openAIWindowWarmupMetricsResponse{
		Enqueued: metrics.Enqueued, Started: metrics.Started, Success: metrics.Success,
		Failed: metrics.Failed, Retry: metrics.Retry, Uncertain: metrics.Uncertain,
		RealTrafficSuppressed: metrics.RealTrafficSuppressed,
		DuplicateSuppressed:   metrics.DuplicateSuppressed,
		Due:                   metrics.Due,
		OldestDueAgeSeconds:   metrics.OldestDueAgeSeconds,
		Inflight:              metrics.Inflight,
		ResetLagSeconds:       metrics.ResetLagSeconds,
	})
}

func (h *AccountHandler) RequeueOpenAIWindowWarmup(c *gin.Context) {
	if !h.requireOpenAIWindowWarmup(c) {
		return
	}
	accountID, ok := openAIWindowWarmupAccountID(c)
	if !ok {
		return
	}
	var body map[string]any
	_ = c.ShouldBindJSON(&body)
	payload := struct {
		AccountID int64          `json:"account_id"`
		Body      map[string]any `json:"body,omitempty"`
	}{AccountID: accountID, Body: body}
	executeAdminIdempotentJSON(c, "system.openai.window_warmup.requeue", payload, service.DefaultWriteIdempotencyTTL(), func(ctx context.Context) (any, error) {
		account, err := h.adminService.GetAccount(ctx, accountID)
		if err != nil {
			return nil, err
		}
		if !isOpenAIWindowWarmupAccount(account) || !service.OpenAIWindowWarmupPolicyForAccount(account).Enabled() {
			return nil, infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_ACCOUNT_INELIGIBLE", "Account must be an enabled OpenAI OAuth parent account")
		}
		job, inserted, err := h.openAIWindowWarmup.RequeueAccount(ctx, accountID)
		if err != nil {
			return nil, err
		}
		status := openAIWindowWarmupStatus(account, job)
		status.Changed = inserted
		return status, nil
	})
}

func (h *AccountHandler) UnblockOpenAIWindowWarmup(c *gin.Context) {
	if !h.requireOpenAIWindowWarmup(c) {
		return
	}
	accountID, ok := openAIWindowWarmupAccountID(c)
	if !ok {
		return
	}
	payload := struct {
		AccountID int64 `json:"account_id"`
	}{AccountID: accountID}
	executeAdminIdempotentJSON(c, "system.openai.window_warmup.unblock", payload, service.DefaultWriteIdempotencyTTL(), func(ctx context.Context) (any, error) {
		account, err := h.adminService.GetAccount(ctx, accountID)
		if err != nil {
			return nil, err
		}
		if !isOpenAIWindowWarmupAccount(account) || !service.OpenAIWindowWarmupPolicyForAccount(account).Enabled() {
			return nil, infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_ACCOUNT_INELIGIBLE", "Account must be an enabled OpenAI OAuth parent account")
		}
		job, changed, err := h.openAIWindowWarmup.UnblockAccount(ctx, accountID)
		if err != nil {
			return nil, err
		}
		status := openAIWindowWarmupStatus(account, job)
		status.Changed = changed
		return status, nil
	})
}

func (h *AccountHandler) ListOpenAIWindowWarmupJobs(c *gin.Context) {
	if !h.requireOpenAIWindowWarmup(c) {
		return
	}
	page, pageSize := response.ParsePagination(c)
	accountID, _ := strconv.ParseInt(strings.TrimSpace(c.Query("account_id")), 10, 64)
	states := make([]string, 0)
	for _, state := range strings.Split(c.Query("state"), ",") {
		if state = strings.TrimSpace(state); state != "" {
			states = append(states, state)
		}
	}
	jobs, total, err := h.openAIWindowWarmup.ListJobsPage(c.Request.Context(), service.OpenAIWindowWarmupListOptions{
		AccountID: accountID,
		States:    states,
		Limit:     pageSize,
		Offset:    (page - 1) * pageSize,
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	for _, job := range jobs {
		if job != nil {
			job.LastError = redactedWarmupError(job.LastError)
		}
	}
	response.Paginated(c, jobs, total, page, pageSize)
}

func normalizeOpenAIWindowWarmupBatchIDs(ids []int64) ([]int64, error) {
	if len(ids) == 0 || len(ids) > maxOpenAIWindowWarmupBatchAccounts {
		return nil, infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_BATCH_INVALID", "account_ids must contain between 1 and 1000 accounts")
	}
	seen := make(map[int64]struct{}, len(ids))
	out := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			return nil, infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_BATCH_INVALID", "account_ids must contain positive IDs")
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out, nil
}

func (h *AccountHandler) resolveOpenAIWindowWarmupPolicyBatchIDs(ctx context.Context, accountIDs, groupIDs []int64) ([]int64, error) {
	resolved := append([]int64(nil), accountIDs...)
	for _, groupID := range groupIDs {
		if groupID <= 0 {
			return nil, infraerrors.BadRequest("OPENAI_WINDOW_WARMUP_GROUP_INVALID", "group_ids must contain positive IDs")
		}
		accounts, err := h.listAccountsFiltered(ctx, service.PlatformOpenAI, service.AccountTypeOAuth, "", "", groupID, "", "created_at", "asc")
		if err != nil {
			return nil, err
		}
		for i := range accounts {
			if isOpenAIWindowWarmupAccount(&accounts[i]) {
				resolved = append(resolved, accounts[i].ID)
			}
		}
	}
	return normalizeOpenAIWindowWarmupBatchIDs(resolved)
}

func (h *AccountHandler) RequeueOpenAIWindowWarmupBatch(c *gin.Context) {
	if !h.requireOpenAIWindowWarmup(c) {
		return
	}
	var req openAIWindowWarmupBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	ids, err := normalizeOpenAIWindowWarmupBatchIDs(req.AccountIDs)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	req.AccountIDs = ids
	executeAdminIdempotentJSON(c, "system.openai.window_warmup.requeue_batch", req, service.DefaultWriteIdempotencyTTL(), func(ctx context.Context) (any, error) {
		result := openAIWindowWarmupBatchResult{Total: len(ids), Results: make([]openAIWindowWarmupBatchItem, 0, len(ids))}
		for _, accountID := range ids {
			item := openAIWindowWarmupBatchItem{AccountID: accountID}
			account, accountErr := h.adminService.GetAccount(ctx, accountID)
			if accountErr != nil || !isOpenAIWindowWarmupAccount(account) || !service.OpenAIWindowWarmupPolicyForAccount(account).Enabled() {
				item.Error = "account is not an enabled OpenAI OAuth parent account"
				result.Failed++
				result.Results = append(result.Results, item)
				continue
			}
			job, inserted, requeueErr := h.openAIWindowWarmup.RequeueAccount(ctx, accountID)
			if requeueErr != nil {
				item.Error = redactedWarmupError(requeueErr.Error())
				result.Failed++
			} else {
				item.Queued = job != nil
				item.Changed = inserted
				if job != nil {
					item.State = job.State
				}
				if item.Queued {
					result.Queued++
				} else {
					result.Skipped++
				}
			}
			result.Results = append(result.Results, item)
		}
		return result, nil
	})
}

func (h *AccountHandler) UpdateOpenAIWindowWarmupPolicyBatch(c *gin.Context) {
	if !h.requireOpenAIWindowWarmup(c) {
		return
	}
	var req openAIWindowWarmupBatchPolicyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	ids, err := h.resolveOpenAIWindowWarmupPolicyBatchIDs(c.Request.Context(), req.AccountIDs, req.GroupIDs)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	policy, err := validateOpenAIWindowWarmupPolicy(req.Policy)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	req.AccountIDs = ids
	req.Policy = string(policy)
	executeAdminIdempotentJSON(c, "system.openai.window_warmup.policy_batch", req, service.DefaultWriteIdempotencyTTL(), func(ctx context.Context) (any, error) {
		result := openAIWindowWarmupBatchResult{Total: len(ids), Results: make([]openAIWindowWarmupBatchItem, 0, len(ids))}
		for _, accountID := range ids {
			item := openAIWindowWarmupBatchItem{AccountID: accountID, Policy: string(policy)}
			account, accountErr := h.adminService.GetAccount(ctx, accountID)
			if accountErr != nil || !isOpenAIWindowWarmupAccount(account) {
				item.Error = "account is not an OpenAI OAuth parent account"
				result.Failed++
				result.Results = append(result.Results, item)
				continue
			}
			updated, updateErr := h.adminService.UpdateAccount(ctx, accountID, &service.UpdateAccountInput{
				Extra: withOpenAIWindowWarmupPolicy(account.Extra, policy),
			})
			if updateErr != nil {
				item.Error = redactedWarmupError(updateErr.Error())
				result.Failed++
				result.Results = append(result.Results, item)
				continue
			}
			item.Changed = service.OpenAIWindowWarmupPolicyForAccount(account) != policy
			result.Updated++
			if policy.Enabled() {
				status, scheduleErr := h.scheduleOpenAIWindowWarmup(ctx, updated, service.OpenAIWindowWarmupTriggerManual)
				if scheduleErr != nil {
					item.Error = redactedWarmupError(scheduleErr.Error())
					result.Failed++
					result.Updated--
				} else if status != nil {
					item.Queued = status.Queued
					item.State = status.State
					if status.Queued {
						result.Queued++
					}
				}
			}
			result.Results = append(result.Results, item)
		}
		return result, nil
	})
}
