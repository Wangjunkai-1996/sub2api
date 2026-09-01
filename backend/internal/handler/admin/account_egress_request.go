package admin

import (
	"strings"

	infraerrors "github.com/Wei-Shaw/sub2api/internal/pkg/errors"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

// accountEgressPoolInput converts the public JSON form into the request-local
// service intent. The legacy fields remain untouched when no pool is sent.
func accountEgressPoolInput(mode *string, pool *AccountEgressPoolRequest, create bool) (*service.ReplaceAccountPoolInput, error) {
	normalizedMode, err := normalizeEgressMode(mode)
	if err != nil {
		return nil, err
	}
	if pool == nil {
		if mode != nil && normalizedMode == service.EgressModePool {
			return nil, infraerrors.BadRequest("ACCOUNT_EGRESS_POOL_REQUIRED", "egress_pool is required when egress_mode is pool")
		}
		if !create && mode != nil && normalizedMode == service.EgressModeLegacy {
			return &service.ReplaceAccountPoolInput{Mode: service.EgressModeLegacy}, nil
		}
		// Omitted mode/pool and legacy account creation preserve the old
		// proxy_id/concurrency path.
		return nil, nil
	}
	if normalizedMode == service.EgressModeLegacy {
		return nil, infraerrors.BadRequest("ACCOUNT_EGRESS_MODE_CONFLICT", "egress_mode legacy cannot include egress_pool")
	}
	if create && pool.Revision != nil {
		return nil, infraerrors.BadRequest("ACCOUNT_EGRESS_REVISION_ON_CREATE", "egress_pool.revision is only valid when updating an account")
	}
	if !create && pool.Revision == nil {
		return nil, infraerrors.BadRequest("ACCOUNT_EGRESS_REVISION_REQUIRED", "egress_pool.revision is required when updating an account")
	}

	routeIDs := append([]int64(nil), pool.RouteIDs...)
	primaryRouteID := int64(0)
	if pool.PrimaryRouteID != nil {
		primaryRouteID = *pool.PrimaryRouteID
	} else if len(routeIDs) > 0 {
		// The first selected route is the deterministic primary when clients
		// omit the optional field.
		primaryRouteID = routeIDs[0]
	}
	input := &service.ReplaceAccountPoolInput{
		Mode:                 service.EgressModePool,
		RouteIDs:             routeIDs,
		PrimaryRouteID:       primaryRouteID,
		ConcurrencyPerEgress: pool.ConcurrencyPerEgress,
		ExpectedRevision:     pool.Revision,
	}
	return input, nil
}

func normalizeEgressMode(mode *string) (string, error) {
	if mode == nil || strings.TrimSpace(*mode) == "" {
		return service.EgressModePool, nil
	}
	normalized := strings.ToLower(strings.TrimSpace(*mode))
	switch normalized {
	case service.EgressModePool, service.EgressModeLegacy:
		return normalized, nil
	default:
		return "", infraerrors.BadRequest("ACCOUNT_EGRESS_MODE_INVALID", "egress_mode must be pool or legacy")
	}
}

func bulkAccountEgressInput(mode *string, pool *BulkAccountEgressPoolRequest) (*service.ApplyAccountPoolsInput, error) {
	if pool == nil {
		if mode != nil {
			if _, err := normalizeEgressMode(mode); err != nil {
				return nil, err
			}
			if strings.TrimSpace(*mode) != "" && strings.ToLower(strings.TrimSpace(*mode)) != service.EgressModePool {
				return nil, infraerrors.BadRequest("ACCOUNT_EGRESS_MODE_INVALID", "bulk egress updates require egress_mode pool")
			}
		}
		return nil, infraerrors.BadRequest("ACCOUNT_EGRESS_POOL_REQUIRED", "egress_pool is required for bulk egress updates")
	}
	if mode != nil && strings.TrimSpace(*mode) != "" && strings.ToLower(strings.TrimSpace(*mode)) != service.EgressModePool {
		return nil, infraerrors.BadRequest("ACCOUNT_EGRESS_MODE_INVALID", "bulk egress updates require egress_mode pool")
	}
	if pool.Revision != nil {
		return nil, infraerrors.BadRequest("ACCOUNT_EGRESS_REVISION_BULK_UNSUPPORTED", "egress_pool.revision is not supported for bulk updates")
	}
	return &service.ApplyAccountPoolsInput{
		Operation:            strings.ToLower(strings.TrimSpace(pool.Operation)),
		RouteIDs:             append([]int64(nil), pool.RouteIDs...),
		PrimaryRouteID:       pool.PrimaryRouteID,
		ConcurrencyPerEgress: pool.ConcurrencyPerEgress,
	}, nil
}

func hasAccountEgressFields(mode *string, pool *BulkAccountEgressPoolRequest) bool {
	return mode != nil || pool != nil
}

// validateOpenAIEgressWrite keeps the first runtime rollout deliberately
// scoped to OpenAI accounts.  Other platforms continue using their existing
// proxy/concurrency path until they have a corresponding runtime allocator.
func validateOpenAIEgressWrite(platform, accountType string, shadow bool) error {
	if shadow {
		return infraerrors.BadRequest(
			"SPARK_SHADOW_EGRESS_INHERITED",
			"spark shadow account egress pool is inherited from its parent and cannot be edited",
		)
	}
	if strings.TrimSpace(platform) != service.PlatformOpenAI || strings.TrimSpace(accountType) != service.AccountTypeOAuth {
		return service.ErrEgressAccountUnsupported
	}
	return nil
}
