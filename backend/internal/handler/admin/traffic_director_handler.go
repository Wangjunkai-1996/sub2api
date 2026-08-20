package admin

import (
	"context"
	"errors"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
)

// TrafficDirectorServiceAPI is the narrow service contract used by the admin
// handler. Keeping the HTTP layer on an interface makes it possible to test
// request validation and error mapping without a database or Redis instance.
type TrafficDirectorServiceAPI interface {
	Get(ctx context.Context, groupID int64) (*service.TrafficDirectorGroupState, error)
	ListVersions(ctx context.Context, groupID int64, limit, offset int) ([]service.TrafficDirectorVersion, int64, error)
	GetVersion(ctx context.Context, groupID, version int64) (*service.TrafficDirectorVersion, error)
	Preview(ctx context.Context, input service.TrafficDirectorPreviewInput) (*service.TrafficDirectorPreview, error)
	Publish(ctx context.Context, input service.TrafficDirectorPublishInput) (*service.TrafficDirectorPublishResult, error)
	Rollback(ctx context.Context, input service.TrafficDirectorRollbackInput) (*service.TrafficDirectorPublishResult, error)
}

// TrafficDirectorHandler exposes the versioned Group traffic policy API.
// It is deliberately independent from GroupHandler so ordinary group updates
// cannot accidentally mutate or publish a traffic policy.
type TrafficDirectorHandler struct {
	service TrafficDirectorServiceAPI
}

// NewTrafficDirectorHandler constructs the admin Traffic Director handler.
// A nil dependency is allowed during partial wiring; requests then receive a
// stable 503 instead of panicking during server startup.
func NewTrafficDirectorHandler(s TrafficDirectorServiceAPI) *TrafficDirectorHandler {
	return &TrafficDirectorHandler{service: s}
}

// TrafficDirectorPreviewRequest is shared by preview and publish payloads.
// expected_version is a pointer so omission cannot be confused with the valid
// synthetic legacy version (0).
type TrafficDirectorPreviewRequest struct {
	ExpectedVersion *int64                      `json:"expected_version"`
	Mode            string                      `json:"mode"`
	Spec            *domain.TrafficDirectorSpec `json:"spec"`
}

// TrafficDirectorPublishRequest is the publish payload. Idempotency-Key is
// intentionally read from the HTTP header rather than duplicated in JSON.
type TrafficDirectorPublishRequest struct {
	TrafficDirectorPreviewRequest
	Note                      string `json:"note"`
	ConfirmUnassignedAccounts bool   `json:"confirm_unassigned_accounts"`
}

type trafficDirectorRollbackRequest struct {
	ExpectedVersion *int64 `json:"expected_version"`
	Note            string `json:"note"`
}

// Get returns the current policy head and the account inventory used by the
// editor.
// GET /api/v1/admin/groups/:id/traffic-director
func (h *TrafficDirectorHandler) Get(c *gin.Context) {
	groupID, ok := parsePositiveIDParam(c, "id")
	if !ok {
		return
	}
	if h == nil || h.service == nil {
		h.writeUnavailable(c)
		return
	}
	state, err := h.service.Get(c.Request.Context(), groupID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if err := ensureOpenAIState(state); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, state)
}

// ListVersions lists immutable publications in descending version order.
// GET /api/v1/admin/groups/:id/traffic-director/versions
func (h *TrafficDirectorHandler) ListVersions(c *gin.Context) {
	groupID, ok := parsePositiveIDParam(c, "id")
	if !ok {
		return
	}
	if h == nil || h.service == nil {
		h.writeUnavailable(c)
		return
	}
	if err := h.requireOpenAIGroup(c, groupID); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	limit, offset := parseTrafficDirectorWindow(c)
	versions, total, err := h.service.ListVersions(c.Request.Context(), groupID, limit, offset)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, gin.H{
		"items":  versions,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// GetVersion returns one immutable publication. Version 0 is the synthetic
// legacy record and is therefore accepted by this endpoint.
// GET /api/v1/admin/groups/:id/traffic-director/versions/:version
func (h *TrafficDirectorHandler) GetVersion(c *gin.Context) {
	groupID, ok := parsePositiveIDParam(c, "id")
	if !ok {
		return
	}
	version, ok := parseNonNegativeIDParam(c, "version")
	if !ok {
		return
	}
	if h == nil || h.service == nil {
		h.writeUnavailable(c)
		return
	}
	if err := h.requireOpenAIGroup(c, groupID); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	item, err := h.service.GetVersion(c.Request.Context(), groupID, version)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, item)
}

// Preview validates and normalizes a policy without changing the current
// head. It intentionally reports unassigned accounts for the UI to confirm.
// POST /api/v1/admin/groups/:id/traffic-director/preview
func (h *TrafficDirectorHandler) Preview(c *gin.Context) {
	groupID, ok := parsePositiveIDParam(c, "id")
	if !ok {
		return
	}
	var req TrafficDirectorPreviewRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request body: "+err.Error())
		return
	}
	expected, ok := requiredExpectedVersion(c, req.ExpectedVersion)
	if !ok {
		return
	}
	if h == nil || h.service == nil {
		h.writeUnavailable(c)
		return
	}
	result, err := h.service.Preview(c.Request.Context(), service.TrafficDirectorPreviewInput{
		GroupID:         groupID,
		ExpectedVersion: expected,
		Mode:            req.Mode,
		Spec:            req.Spec,
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

// Publish atomically creates the next immutable version. The idempotency key
// is mandatory and is passed unchanged to the service for transactional replay
// and fingerprint conflict handling.
// POST /api/v1/admin/groups/:id/traffic-director/publish
func (h *TrafficDirectorHandler) Publish(c *gin.Context) {
	groupID, ok := parsePositiveIDParam(c, "id")
	if !ok {
		return
	}
	var req TrafficDirectorPublishRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request body: "+err.Error())
		return
	}
	expected, ok := requiredExpectedVersion(c, req.ExpectedVersion)
	if !ok {
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" {
		response.ErrorFrom(c, service.ErrTrafficDirectorValidation.WithCause(errors.New("Idempotency-Key is required")))
		return
	}
	if h == nil || h.service == nil {
		h.writeUnavailable(c)
		return
	}
	operatorID := trafficDirectorOperatorID(c)
	result, err := h.service.Publish(c.Request.Context(), service.TrafficDirectorPublishInput{
		TrafficDirectorPreviewInput: service.TrafficDirectorPreviewInput{
			GroupID:         groupID,
			ExpectedVersion: expected,
			Mode:            req.Mode,
			Spec:            req.Spec,
		},
		ConfirmUnassignedAccounts: req.ConfirmUnassignedAccounts,
		IdempotencyKey:            idempotencyKey,
		OperatorID:                operatorID,
		Note:                      req.Note,
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

// Rollback publishes a new version containing a prior immutable version. The
// database history is never edited in place.
// POST /api/v1/admin/groups/:id/traffic-director/rollback/:version
func (h *TrafficDirectorHandler) Rollback(c *gin.Context) {
	groupID, ok := parsePositiveIDParam(c, "id")
	if !ok {
		return
	}
	targetVersion, ok := parseNonNegativeIDParam(c, "version")
	if !ok {
		return
	}
	var req trafficDirectorRollbackRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request body: "+err.Error())
		return
	}
	expected, ok := requiredExpectedVersion(c, req.ExpectedVersion)
	if !ok {
		return
	}
	idempotencyKey := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
	if idempotencyKey == "" {
		response.ErrorFrom(c, service.ErrTrafficDirectorValidation.WithCause(errors.New("Idempotency-Key is required")))
		return
	}
	if h == nil || h.service == nil {
		h.writeUnavailable(c)
		return
	}
	result, err := h.service.Rollback(c.Request.Context(), service.TrafficDirectorRollbackInput{
		GroupID:         groupID,
		TargetVersion:   targetVersion,
		ExpectedVersion: expected,
		IdempotencyKey:  idempotencyKey,
		OperatorID:      trafficDirectorOperatorID(c),
		Note:            req.Note,
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, result)
}

// Status returns a compact operator-facing summary. It intentionally does not
// add a second health persistence model; account health remains owned by the
// Redis health service and is observed by the request path.
// GET /api/v1/admin/groups/:id/traffic-director/status
func (h *TrafficDirectorHandler) Status(c *gin.Context) {
	groupID, ok := parsePositiveIDParam(c, "id")
	if !ok {
		return
	}
	if h == nil || h.service == nil {
		h.writeUnavailable(c)
		return
	}
	state, err := h.service.Get(c.Request.Context(), groupID)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	if err := ensureOpenAIState(state); err != nil {
		response.ErrorFrom(c, err)
		return
	}
	response.Success(c, summarizeTrafficDirectorState(state))
}

func (h *TrafficDirectorHandler) requireOpenAIGroup(c *gin.Context, groupID int64) error {
	state, err := h.service.Get(c.Request.Context(), groupID)
	if err != nil {
		return err
	}
	return ensureOpenAIState(state)
}

func (h *TrafficDirectorHandler) writeUnavailable(c *gin.Context) {
	response.ErrorFrom(c, service.ErrTrafficDirectorPolicyUnavailable)
}

func ensureOpenAIState(state *service.TrafficDirectorGroupState) error {
	if state == nil || state.GroupID <= 0 {
		return service.ErrTrafficDirectorGroupNotFound
	}
	if strings.ToLower(strings.TrimSpace(state.Platform)) != service.PlatformOpenAI {
		return service.ErrTrafficDirectorValidation.WithCause(errors.New("traffic director V1 supports OpenAI groups only"))
	}
	return nil
}

func summarizeTrafficDirectorState(state *service.TrafficDirectorGroupState) gin.H {
	assigned := make(map[int64]struct{})
	poolSummaries := make([]gin.H, 0)
	healthMode := domain.TrafficDirectorHealthModeOff
	checksum := service.TrafficDirectorLegacyChecksum()
	if state.Head.Spec != nil {
		healthMode = state.Head.Spec.HealthMode
		if computed, err := service.TrafficDirectorSpecChecksum(*state.Head.Spec); err == nil {
			checksum = computed
		}
		for _, pool := range state.Head.Spec.Pools {
			poolAssigned := 0
			poolAvailable := 0
			for _, accountID := range pool.AccountIDs {
				if accountID > 0 {
					assigned[accountID] = struct{}{}
					poolAssigned++
				}
			}
			for _, account := range state.Accounts {
				for _, accountID := range pool.AccountIDs {
					if account.ID == accountID && account.Schedulable {
						poolAvailable++
						break
					}
				}
			}
			poolSummaries = append(poolSummaries, gin.H{
				"key":               pool.Key,
				"weight_bps":        pool.WeightBPS,
				"account_count":     poolAssigned,
				"available_count":   poolAvailable,
				"min_available":     pool.MinAvailable,
				"fallback_pool_key": pool.FallbackPoolKey,
			})
		}
	}
	unassigned := make([]int64, 0)
	available := 0
	for _, account := range state.Accounts {
		if account.Schedulable {
			available++
		}
		if _, ok := assigned[account.ID]; !ok {
			unassigned = append(unassigned, account.ID)
		}
	}
	return gin.H{
		"group_id":                state.GroupID,
		"group_name":              state.GroupName,
		"platform":                state.Platform,
		"head":                    state.Head,
		"mode":                    state.Head.Mode,
		"version":                 state.Head.Version,
		"checksum":                checksum,
		"health_mode":             healthMode,
		"pools":                   poolSummaries,
		"account_count":           len(state.Accounts),
		"available_account_count": available,
		"assigned_account_count":  len(assigned),
		"unassigned_account_ids":  unassigned,
	}
}

func requiredExpectedVersion(c *gin.Context, value *int64) (int64, bool) {
	if value == nil || *value < service.TrafficDirectorLegacyVersion {
		response.ErrorFrom(c, service.ErrTrafficDirectorValidation.WithCause(errors.New("expected_version is required and must be non-negative")))
		return 0, false
	}
	return *value, true
}

func parseNonNegativeIDParam(c *gin.Context, name string) (int64, bool) {
	raw := c.Param(name)
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id < 0 {
		response.BadRequest(c, "Invalid "+name)
		return 0, false
	}
	return id, true
}

func parseTrafficDirectorWindow(c *gin.Context) (limit, offset int) {
	limit = 20
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}
	if raw := strings.TrimSpace(c.Query("offset")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			offset = parsed
		}
	}
	return limit, offset
}

func trafficDirectorOperatorID(c *gin.Context) *int64 {
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok || subject.UserID <= 0 {
		return nil
	}
	operatorID := subject.UserID
	return &operatorID
}

var _ TrafficDirectorServiceAPI = (*service.TrafficDirectorService)(nil)
