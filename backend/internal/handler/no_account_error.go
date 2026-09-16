package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/Wei-Shaw/sub2api/internal/service"
)

// noAccountErrorClassification describes the HTTP response to emit when
// account selection failed with ErrNoAvailableAccounts. Handlers obtain it
// via classifyNoAccountError and choose between:
//
//   - 404 model_not_found — the group has accounts, but none of them are
//     configured to serve the requested model (config / typo / unsupported
//     model). Returning 503 here misleads operators and trips reverse-proxy
//     health checks; 404 lets the client surface the real problem.
//
//   - 503 api_error — accounts that could serve the model exist but are
//     temporarily exhausted (rate limit, quota auto-pause, runtime block) OR
//     the group has no accounts at all. Both stay on 503 because retrying
//     after a backoff can plausibly succeed (or, in the empty-pool case, the
//     operator may be in the middle of adding accounts).
type noAccountErrorClassification struct {
	Status            int
	ErrType           string
	ErrCode           string
	Message           string
	RetryAfterSeconds int
	ModelNotFound     bool // true when this is a 404 model_not_found classification
}

var selectionModelRateLimitedPattern = regexp.MustCompile(`(?:model_rate_limited|rate_limited)=(\d+)`)

// classifySelectionFailureError preserves typed admission failures before the
// generic no-account diagnosis can collapse them into a model/pool response.
func classifySelectionFailureError(err error, fallback noAccountErrorClassification) noAccountErrorClassification {
	if err == nil {
		return fallback
	}
	var cooldown *service.OpenAI429CooldownError
	if errors.As(err, &cooldown) {
		retryAfter := int(cooldown.RetryAfter / time.Second)
		if cooldown.RetryAfter%time.Second > 0 {
			retryAfter++
		}
		if retryAfter < 1 {
			retryAfter = 1
		}
		return noAccountErrorClassification{
			Status:            http.StatusTooManyRequests,
			ErrType:           "rate_limit_error",
			ErrCode:           "account_pool_rate_limited",
			Message:           "Account capacity is recovering from an upstream rate limit. Please retry later.",
			RetryAfterSeconds: retryAfter,
		}
	}
	if errors.Is(err, service.ErrOpenAI429RecoveryUnavailable) {
		return noAccountErrorClassification{
			Status:            http.StatusServiceUnavailable,
			ErrType:           "api_error",
			ErrCode:           "account_recovery_unavailable",
			Message:           "Account recovery state is temporarily unavailable. Please retry later.",
			RetryAfterSeconds: 2,
		}
	}
	if errors.Is(err, service.ErrAccountEgressCapacityFull) {
		return noAccountErrorClassification{
			Status:            http.StatusTooManyRequests,
			ErrType:           "rate_limit_error",
			ErrCode:           "egress_capacity_exhausted",
			Message:           "Account egress capacity is full. Please retry later.",
			RetryAfterSeconds: 2,
		}
	}
	if errors.Is(err, service.ErrAccountEgressUnavailable) ||
		errors.Is(err, service.ErrAccountEgressNoRoute) ||
		errors.Is(err, service.ErrAccountEgressConfigStale) {
		return noAccountErrorClassification{
			Status:            http.StatusServiceUnavailable,
			ErrType:           "api_error",
			ErrCode:           "egress_unavailable",
			Message:           "Account egress is temporarily unavailable. Please retry later.",
			RetryAfterSeconds: 2,
		}
	}
	// A 404 model_not_found fallback is authoritative and must not be downgraded
	// to a rate-limit verdict. classifyNoAccountError only reaches it through
	// DiagnoseModelAvailabilityForPlatform, a dedicated database query over
	// persistent eligibility (active + schedulable + model_mapping) that already
	// established no account in the group can serve this model at all. A transient
	// per-model cooldown on one of the remaining candidates does not make "all
	// available accounts are rate-limited" true.
	//
	// Reporting 429 here is actively harmful: retrying can never succeed, and
	// clients that treat 429 as a rate limit retry hard and swallow the body
	// (Codex surfaces only "exceeded retry limit, last status: 429"), losing the
	// one message that names the real problem. It also flips the ops attribution
	// from a local model-configuration issue to routing capacity, because call
	// sites gate markOpsRoutingCapacityLimitedIfNoAvailable on ModelNotFound.
	if fallback.ModelNotFound {
		return fallback
	}
	if !errors.Is(err, service.ErrNoAvailableAccounts) {
		// Do not turn a repository/transport failure into a pool health signal.
		fallback.ErrCode = ""
		fallback.RetryAfterSeconds = 0
	}
	match := selectionModelRateLimitedPattern.FindStringSubmatch(strings.ToLower(err.Error()))
	if len(match) != 2 {
		return fallback
	}
	count, parseErr := strconv.Atoi(match[1])
	if parseErr != nil || count <= 0 {
		return fallback
	}
	return noAccountErrorClassification{
		Status:            http.StatusTooManyRequests,
		ErrType:           "rate_limit_error",
		ErrCode:           fallback.ErrCode,
		Message:           "All available accounts are currently rate-limited. Please retry later.",
		RetryAfterSeconds: fallback.RetryAfterSeconds,
	}
}

// classifyNoAccountError decides between 404 model_not_found and 503
// api_error for "no available accounts" failures.
//
// The classifier intentionally does not consume the original error: the
// selection layer never tells us *why* the pool came up empty (rate-limited
// vs. unsupported model are both wrapped as ErrNoAvailableAccounts). Instead
// we re-check pool composition through DiagnoseModelAvailabilityForPlatform.
// Its dedicated database query considers only persistent eligibility
// (active status + schedulable setting) and model_mapping, bypassing scheduler
// snapshots and transient filters. That guarantees a 404 is only returned
// when persistent account/group/model configuration must change before the
// request can succeed.
//
// routingModel is the model name that account selection actually compared
// against (i.e. after group-level dispatch mapping). displayModel is the
// raw model the caller asked for; it is used only in the user-facing error
// message so that internal mapping details don't leak. Most callers pass
// the same value for both.
//
// platform is the platform the request was routed to (use
// service.PlatformOpenAI / PlatformAnthropic / PlatformGemini). It is
// required because Anthropic/Gemini routes additionally surface
// mixed-scheduled Antigravity accounts; passing the wrong platform would
// flip a legitimate 503 to a misleading 404 (or vice versa).
func classifyNoAccountError(
	ctx context.Context,
	diag service.ModelAvailabilityDiagnoser,
	apiKey *service.APIKey,
	routingModel string,
	displayModel string,
	platform string,
) noAccountErrorClassification {
	fallback := noAccountErrorClassification{
		Status:            http.StatusServiceUnavailable,
		ErrType:           "api_error",
		ErrCode:           "account_pool_exhausted",
		Message:           "Service temporarily unavailable",
		RetryAfterSeconds: 2,
	}

	routingModel = strings.TrimSpace(routingModel)
	displayModel = strings.TrimSpace(displayModel)
	if displayModel == "" {
		displayModel = routingModel
	}
	if diag == nil || apiKey == nil || apiKey.GroupID == nil || routingModel == "" {
		return fallback
	}

	result := diag.DiagnoseModelAvailabilityForPlatform(ctx, apiKey.GroupID, routingModel, platform)
	if result.HasAccountsInPool && !result.HasModelSupport {
		return noAccountErrorClassification{
			Status:        http.StatusNotFound,
			ErrType:       "model_not_found",
			ErrCode:       "model_not_found",
			Message:       fmt.Sprintf("Model %q is not supported by any configured account in this group", displayModel),
			ModelNotFound: true,
		}
	}
	return fallback
}

func (h *OpenAIGatewayHandler) handleSelectionFailure(c *gin.Context, classification noAccountErrorClassification, streamStarted bool) {
	classification = openAIDeferredSelectionFailure(c, classification)
	// Stop the compact heartbeat before inspecting or changing response headers.
	if service.StopOpenAICompactSSEKeepaliveCommitted(c) {
		streamStarted = true
	}
	if !streamStarted && !c.Writer.Written() && classification.RetryAfterSeconds > 0 {
		c.Header("Retry-After", strconv.Itoa(classification.RetryAfterSeconds))
	}
	h.handleStreamingAwareErrorWithCode(c, classification.Status, classification.ErrType,
		classification.ErrCode, classification.Message, streamStarted, false)
}

func (h *OpenAIGatewayHandler) handleAnthropicSelectionFailure(c *gin.Context, classification noAccountErrorClassification, streamStarted bool) {
	classification = openAIDeferredSelectionFailure(c, classification)
	if !streamStarted && !c.Writer.Written() && classification.RetryAfterSeconds > 0 {
		c.Header("Retry-After", strconv.Itoa(classification.RetryAfterSeconds))
	}
	h.anthropicStreamingAwareError(c, classification.Status, classification.ErrType,
		classification.Message, streamStarted, classification.ErrCode)
}

// Only a shared admission decision with a near recovery time permits one
// selection retry. Empty pools, unknown failures and long quota resets do not.
func waitForOpenAI429Selection(c *gin.Context, err error, excludedAccounts int) bool {
	const waitedKey = "openai_429_selection_waited"
	const maxWait = 3 * time.Second
	if c == nil || c.Request == nil || excludedAccounts != 0 ||
		service.OpenAICompactKeepaliveAdjustedWrittenSize(c) > 0 || c.GetBool(waitedKey) {
		return false
	}
	var cooldown *service.OpenAI429CooldownError
	if !errors.As(err, &cooldown) || cooldown.RetryAfter <= 0 || cooldown.RetryAfter > maxWait {
		return false
	}
	ctx := c.Request.Context()
	if ctx.Err() != nil {
		return false
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= cooldown.RetryAfter {
		return false
	}
	if deadline, ok := openAIRequestBudgetDeadline(c); ok && time.Until(deadline) <= cooldown.RetryAfter {
		return false
	}
	c.Set(waitedKey, true)
	return sleepWithContext(ctx, cooldown.RetryAfter)
}

// classifyNoAccountErrorFromGin is a thin wrapper that forwards the gin
// context's underlying request context. Most call sites already have a
// *gin.Context handy, so this keeps the call sites uncluttered.
func classifyNoAccountErrorFromGin(
	c *gin.Context,
	diag service.ModelAvailabilityDiagnoser,
	apiKey *service.APIKey,
	routingModel string,
	displayModel string,
	platform string,
) noAccountErrorClassification {
	ctx := context.Background()
	if c != nil && c.Request != nil {
		ctx = c.Request.Context()
	}
	classification := classifyNoAccountError(ctx, diag, apiKey, routingModel, displayModel, platform)
	if classification.ModelNotFound {
		service.MarkOpsClientBusinessLimited(c, service.OpsClientBusinessLimitedReasonLocalModelConfiguration)
	}
	return classification
}

func classifyOpenAICompatibleNoAccountErrorFromGin(
	c *gin.Context,
	diag service.ModelAvailabilityDiagnoser,
	apiKey *service.APIKey,
	routingModel string,
	displayModel string,
) noAccountErrorClassification {
	ctx := context.Background()
	if c != nil && c.Request != nil {
		ctx = c.Request.Context()
	}
	return classifyNoAccountErrorFromGin(
		c,
		diag,
		apiKey,
		routingModel,
		displayModel,
		openAICompatibleRequestPlatform(ctx, apiKey),
	)
}

func openAICompatibleSelectionErrorForLog(err error, platform string) error {
	if err == nil || platform != service.PlatformGrok {
		return err
	}
	message := strings.ReplaceAll(err.Error(), "OpenAI accounts", "Grok accounts")
	if message == err.Error() {
		return err
	}
	return fmt.Errorf("%s", message)
}
