package admin

import (
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/handler/dto"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// EgressRouteHandler exposes only the administrative, redacted view of
// account egress routes. Credentials remain inside EgressService/repository.
type EgressRouteHandler struct {
	egressService *service.EgressService
}

func NewEgressRouteHandler(egressService *service.EgressService) *EgressRouteHandler {
	return &EgressRouteHandler{egressService: egressService}
}

// ListAssignable returns routes that may be selected by an account. Existing
// bindings are included by the repository even when their current state is
// degraded, so an administrator can remove a stale binding safely.
// GET /api/v1/admin/egress-routes/assignable
func (h *EgressRouteHandler) ListAssignable(c *gin.Context) {
	if h == nil || h.egressService == nil {
		response.ErrorFrom(c, service.ErrEgressRouteInvalid)
		return
	}
	routes, err := h.egressService.ListAssignableRoutes(c.Request.Context())
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	out := make([]dto.AssignableEgressRoute, 0, len(routes))
	for i := range routes {
		if item := dto.AssignableEgressRouteFromService(&routes[i]); item != nil {
			out = append(out, *item)
		}
	}
	response.Success(c, out)
}

type verifyEgressRoutesRequest struct {
	RouteIDs []int64 `json:"route_ids" binding:"required"`
}

// Verify probes each requested route server-side. EgressService bounds probe
// workers to four; one failed route does not fail the whole batch.
// POST /api/v1/admin/egress-routes/verify
func (h *EgressRouteHandler) Verify(c *gin.Context) {
	var req verifyEgressRoutesRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	if len(req.RouteIDs) == 0 || len(req.RouteIDs) > service.MaxEgressVerifyBatchSize {
		response.BadRequest(c, "route_ids must contain between 1 and 32 items")
		return
	}
	seen := make(map[int64]struct{}, len(req.RouteIDs))
	for _, routeID := range req.RouteIDs {
		if routeID <= 0 {
			response.BadRequest(c, "route_ids must contain positive integers")
			return
		}
		if _, exists := seen[routeID]; exists {
			response.BadRequest(c, "route_ids must not contain duplicates")
			return
		}
		seen[routeID] = struct{}{}
	}
	if h == nil || h.egressService == nil {
		response.ErrorFrom(c, service.ErrEgressRouteInvalid)
		return
	}

	results, err := h.egressService.ProbeRoutes(c.Request.Context(), req.RouteIDs)
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	out := make([]dto.AssignableEgressRoute, 0, len(results))
	successCount := 0
	for i := range results {
		result := &results[i]
		item := dto.AssignableEgressRouteFromService(result.Route)
		if item == nil {
			// Keep one safe result per requested route even when lookup failed.
			item = &dto.AssignableEgressRoute{ID: result.RouteID, State: service.EgressRouteStateInactive, Eligible: false}
		}
		success := result.Success
		item.ProbeSuccess = &success
		if result.LatencyMs >= 0 {
			latency := result.LatencyMs
			item.ProbeLatencyMs = &latency
		}
		item.ProbeReasonCode = strings.TrimSpace(result.ReasonCode)
		if !result.Success && item.ProbeReasonCode == "" {
			item.ProbeReasonCode = "probe_failed"
		}
		item.ProbeObservedAt = cloneTime(result.ObservedAt)
		if result.Success {
			successCount++
		}
		out = append(out, *item)
	}
	middleware.SetAuditExtra(c, map[string]any{
		"requested_count": len(req.RouteIDs),
		"result":          probeBatchResult(successCount, len(out)),
	})
	response.Success(c, out)
}

type confirmEgressIdentityRequest struct {
	RouteRevision int64  `json:"route_revision" binding:"required"`
	ObservedIP    string `json:"observed_ip" binding:"required"`
}

// ConfirmIdentity performs a route-revision CAS after the administrator has
// confirmed the observed public identity.
// POST /api/v1/admin/egress-routes/:id/confirm-identity
func (h *EgressRouteHandler) ConfirmIdentity(c *gin.Context) {
	routeID, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || routeID <= 0 {
		response.BadRequest(c, "Invalid route ID")
		return
	}
	var req confirmEgressIdentityRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, "Invalid request: "+err.Error())
		return
	}
	if strings.TrimSpace(req.ObservedIP) == "" {
		response.BadRequest(c, "observed_ip is required")
		return
	}
	if h == nil || h.egressService == nil {
		response.ErrorFrom(c, service.ErrEgressRouteInvalid)
		return
	}
	route, err := h.egressService.ConfirmIdentity(c.Request.Context(), service.ConfirmEgressIdentityInput{
		RouteID:          routeID,
		ExpectedRevision: req.RouteRevision,
		ObservedIP:       req.ObservedIP,
	})
	if err != nil {
		response.ErrorFrom(c, err)
		return
	}
	middleware.SetAuditExtra(c, map[string]any{"result": "success"})
	response.Success(c, dto.AssignableEgressRouteFromService(route))
}

func probeBatchResult(success, total int) string {
	switch {
	case total == 0:
		return "empty"
	case success == total:
		return "success"
	case success == 0:
		return "failed"
	default:
		return "partial"
	}
}

func cloneTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copy := value
	return &copy
}
