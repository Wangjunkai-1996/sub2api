package admin

import (
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/handler/dto"
	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// EgressRouteHandler exposes only the administrative, redacted view of
// account egress routes. Credentials remain inside EgressService/repository.
type EgressRouteHandler struct {
	egressService  *service.EgressService
	settingService *service.SettingService
}

func NewEgressRouteHandler(egressService *service.EgressService, settingService *service.SettingService) *EgressRouteHandler {
	return &EgressRouteHandler{egressService: egressService, settingService: settingService}
}

// ListAssignable returns routes that may be selected by an account. Existing
// bindings are included by the repository even when their current state is
// degraded, so an administrator can remove a stale binding safely.
// GET /api/v1/admin/egress-routes/assignable
func (h *EgressRouteHandler) ListAssignable(c *gin.Context) {
	if h == nil || h.egressService == nil {
		response.Success(c, dto.AssignableEgressRouteCatalog{
			Items:              []dto.AssignableEgressRoute{},
			DefaultConcurrency: service.DefaultOpenAIOAuthEgressConcurrency,
			Capabilities: dto.AccountEgressCatalogCapabilities{
				MutationEnabled: false,
				ReasonCode:      accountEgressMutationFrozenReason,
			},
		})
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
	var defaultRouteID *int64
	defaultReasonCode := ""
	if h.settingService != nil {
		proxy, resolveErr := h.settingService.ResolveOpenAIOAuthDefaultProxy(c.Request.Context())
		if resolveErr != nil {
			defaultReasonCode = serviceErrorReason(resolveErr)
		} else if proxy != nil {
			pool, poolErr := service.DefaultOpenAIOAuthEgressPool(routes, proxy.ID, time.Now())
			if poolErr != nil {
				defaultReasonCode = serviceErrorReason(poolErr)
			} else {
				id := pool.PrimaryRouteID
				defaultRouteID = &id
			}
		}
	}
	response.Success(c, dto.AssignableEgressRouteCatalog{
		Items:              out,
		DefaultRouteID:     defaultRouteID,
		DefaultConcurrency: service.DefaultOpenAIOAuthEgressConcurrency,
		DefaultReasonCode:  defaultReasonCode,
		Capabilities: dto.AccountEgressCatalogCapabilities{
			MutationEnabled: true,
		},
	})
}

func serviceErrorReason(err error) string {
	if err == nil {
		return ""
	}
	if reason := infraerrors.Reason(err); reason != "" {
		return reason
	}
	return "default_egress_unavailable"
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
		response.ErrorFrom(c, errAccountEgressMutationFrozen)
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
		item := dto.AssignableEgressProbeResultFromService(result)
		if item == nil {
			continue
		}
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
		response.ErrorFrom(c, errAccountEgressMutationFrozen)
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
