package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// GrokCountTokens handles Anthropic-compatible count_tokens requests locally.
// The route middleware already authenticates the API key and resolves the
// group; this handler intentionally does not select an account or check billing.
func (h *OpenAIGatewayHandler) GrokCountTokens(c *gin.Context) {
	body, err := readLenientJSONRequestBodyWithPrealloc(c.Request, h.cfg)
	if err != nil {
		if maxErr, ok := extractMaxBytesError(err); ok {
			h.anthropicErrorResponse(c, http.StatusRequestEntityTooLarge, "invalid_request_error", buildBodyTooLargeMessage(maxErr.Limit))
			return
		}
		h.anthropicErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to read request body")
		return
	}
	if len(body) == 0 {
		h.anthropicErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Request body is empty")
		return
	}

	bodyRef := service.NewRequestBodyRef(body)
	parsedReq, err := service.ParseGatewayRequest(bodyRef, domain.PlatformAnthropic)
	if err != nil {
		logRequestBodyParseFailure(requestLogger(c, "handler.openai_gateway.grok_count_tokens"), body, err)
		h.anthropicErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		return
	}
	if parsedReq.Model == "" {
		h.anthropicErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "model is required")
		return
	}

	estimated, err := service.EstimateGrokCountTokens(parsedReq.Body.Bytes())
	if err != nil {
		requestLogger(c, "handler.openai_gateway.grok_count_tokens").Warn("grok_count_tokens.local_estimate_failed", zap.Error(err))
		h.anthropicErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		return
	}

	setOpsRequestContext(c, parsedReq.Model, false)
	setOpsEndpointContext(c, "", int16(service.RequestTypeFromLegacy(false, false)))
	c.JSON(http.StatusOK, gin.H{"input_tokens": estimated})
}

// CountTokens handles Anthropic-compatible POST /v1/messages/count_tokens for OpenAI groups.
// It validates billing and routes to an OpenAI token-count bridge without recording usage.
func (h *OpenAIGatewayHandler) CountTokens(c *gin.Context) {
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok {
		h.anthropicErrorResponse(c, http.StatusUnauthorized, "authentication_error", "Invalid API key")
		return
	}

	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		h.anthropicErrorResponse(c, http.StatusInternalServerError, "api_error", "User context not found")
		return
	}
	reqLog := requestLogger(
		c,
		"handler.openai_gateway.count_tokens",
		zap.Int64("user_id", subject.UserID),
		zap.Int64("api_key_id", apiKey.ID),
		zap.Any("group_id", apiKey.GroupID),
	)

	if !allowOpenAICompatibleMessagesDispatch(apiKey) {
		h.anthropicErrorResponse(c, http.StatusForbidden, "permission_error",
			"This group does not allow /v1/messages dispatch")
		return
	}

	if !h.ensureResponsesDependencies(c, reqLog) {
		return
	}

	body, err := readLenientJSONRequestBodyWithPrealloc(c.Request, h.cfg)
	if err != nil {
		if maxErr, ok := extractMaxBytesError(err); ok {
			h.anthropicErrorResponse(c, http.StatusRequestEntityTooLarge, "invalid_request_error", buildBodyTooLargeMessage(maxErr.Limit))
			return
		}
		h.anthropicErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to read request body")
		return
	}
	if len(body) == 0 {
		h.anthropicErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Request body is empty")
		return
	}

	bodyRef := service.NewRequestBodyRef(body)
	parsedReq, err := service.ParseGatewayRequest(bodyRef, domain.PlatformAnthropic)
	if err != nil {
		logRequestBodyParseFailure(reqLog, body, err)
		h.anthropicErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		return
	}
	body = parsedReq.Body.Bytes()
	if parsedReq.Model == "" {
		h.anthropicErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "model is required")
		return
	}

	reqModel := parsedReq.Model
	ensureCompositeTargetPlatform(c, apiKey, reqModel)
	if !compositeTargetPlatformAllowed(c, apiKey, reqModel, service.PlatformOpenAI) {
		h.anthropicErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Model is not supported by this OpenAI-compatible endpoint for composite groups")
		return
	}
	routingModel := service.NormalizeOpenAICompatRequestedModel(reqModel)
	preferredMappedModel := resolveOpenAIMessagesDispatchMappedModel(apiKey, reqModel)
	reqLog = reqLog.With(zap.String("model", reqModel), zap.Bool("stream", parsedReq.Stream))

	setOpsRequestContext(c, reqModel, false)
	setOpsEndpointContext(c, "", int16(service.RequestTypeFromLegacy(false, false)))

	channelMapping, _ := h.gatewayService.ResolveChannelMappingAndRestrict(c.Request.Context(), apiKey.GroupID, reqModel)
	mappedBodyForMessages := newOpenAIModelMappedBodyCache(body, h.gatewayService.ReplaceModelInBody)

	subscription, _ := middleware2.GetSubscriptionFromContext(c)
	if err := h.billingCacheService.CheckBillingEligibility(c.Request.Context(), apiKey.User, apiKey, apiKey.Group, subscription, service.QuotaPlatform(c.Request.Context(), apiKey)); err != nil {
		reqLog.Info("openai_count_tokens.billing_eligibility_check_failed", zap.Error(err))
		status, code, message, retryAfter := billingErrorDetails(err)
		if retryAfter > 0 {
			c.Header("Retry-After", strconv.Itoa(retryAfter))
		}
		h.anthropicErrorResponse(c, status, code, message)
		return
	}

	requestStart := time.Now()
	// count_tokens 不计费：显式豁免利润门，避免高倍率账号池被门排除后连
	// token 计数都返回 no available accounts。
	requestCtx := service.WithOpenAIProfitControlSuppressed(c.Request.Context())
	requestCtx = h.gatewayService.WithOpenAITrafficDirectorRetryLoopContext(requestCtx)
	c.Request = c.Request.WithContext(requestCtx)
	sessionHash := h.gatewayService.GenerateSessionHash(c, body)
	currentRoutingModel := routingModel
	if preferredMappedModel != "" {
		currentRoutingModel = preferredMappedModel
	}
	failedAccountIDs := make(map[int64]struct{})
	requestPlatform := openAICompatibleRequestPlatform(c.Request.Context(), apiKey)
	for {
		selection, _, selectErr := h.gatewayService.SelectAccountWithSchedulerForCapability(
			c.Request.Context(),
			apiKey.GroupID,
			"",
			sessionHash,
			currentRoutingModel,
			failedAccountIDs,
			service.OpenAIUpstreamTransportAny,
			service.OpenAIEndpointCapabilityChatCompletions,
			false,
			false,
			false,
			requestPlatform,
		)
		service.SetOpsLatencyMs(c, service.OpsAuthLatencyMsKey, time.Since(requestStart).Milliseconds())
		if selectErr != nil {
			reqLog.Warn("openai_count_tokens.account_select_failed", zap.Error(openAICompatibleSelectionErrorForLog(selectErr, requestPlatform)))
			cls := classifyOpenAICompatibleSelectionErrorFromGin(c, h.gatewayService, apiKey, currentRoutingModel, reqModel, selectErr)
			if !cls.ModelNotFound {
				markOpsRoutingCapacityLimitedIfNoAvailable(c, selectErr)
			}
			h.anthropicErrorResponse(c, cls.Status, cls.ErrType, cls.Message)
			return
		}
		if selection == nil || selection.Account == nil {
			cls := classifyOpenAICompatibleNoAccountErrorFromGin(c, h.gatewayService, apiKey, currentRoutingModel, reqModel)
			if !cls.ModelNotFound {
				markOpsRoutingCapacityLimited(c)
			}
			h.anthropicErrorResponse(c, cls.Status, cls.ErrType, cls.Message)
			return
		}

		account := selection.Account
		sessionHash = ensureOpenAIPoolModeSessionHash(sessionHash, account)
		setOpsSelectedAccount(c, account.ID, account.Platform)
		accountRelease, retry, acquireErr := h.acquireCountTokensAccountSlot(c, apiKey.GroupID, sessionHash, selection, reqLog)
		if retry {
			failedAccountIDs[account.ID] = struct{}{}
			reqLog.Info("openai_count_tokens.traffic_director_wait_failed_reselect", zap.Int64("account_id", account.ID))
			continue
		}
		if acquireErr != nil {
			status, errType, message := concurrencyErrorResponse(acquireErr, "account")
			h.anthropicErrorResponse(c, status, errType, message)
			return
		}

		account = selection.Account
		forwardBody := mappedBodyForMessages(channelMapping.Mapped, channelMapping.MappedModel)
		defaultMappedModel := preferredMappedModel
		forwardErr := func() error {
			if accountRelease != nil {
				defer accountRelease()
			}
			selection.CommitTrafficDirectorAttempt()
			return h.gatewayService.ForwardCountTokensAsAnthropic(c.Request.Context(), c, account, forwardBody, defaultMappedModel)
		}()
		h.reportCountTokensTrafficDirectorOutcome(c, account, currentRoutingModel, forwardErr)
		if forwardErr != nil {
			reqLog.Error("openai_count_tokens.forward_failed", zap.Int64("account_id", account.ID), zap.Error(forwardErr))
		}
		return
	}
}

var errCountTokensLocalFailure = errors.New("count_tokens local failure")

// reportCountTokensTrafficDirectorOutcome keeps local request/conversion
// failures out of the account circuit while preserving explicit upstream
// HTTP and transport evidence. The probe is still consumed for ignored local
// failures so a half-open token cannot leak into a later retry.
func (h *OpenAIGatewayHandler) reportCountTokensTrafficDirectorOutcome(
	c *gin.Context,
	account *service.Account,
	model string,
	err error,
) {
	if h == nil || h.gatewayService == nil || account == nil {
		return
	}
	statusValue, _ := getContextInt64(c, service.OpsUpstreamStatusCodeKey)
	statusCode := int(statusValue)
	hasUpstreamEvidence := countTokensHasUpstreamEvidence(c)
	accountScoped := hasUpstreamEvidence
	// Keep local parser/access-token/build errors out of the account circuit.
	// The ignored marker still consumes an owned half-open probe.
	reportErr := countTokensHealthReportError(c, err, accountScoped)
	ctx := context.Background()
	if c != nil && c.Request != nil {
		ctx = c.Request.Context()
	}
	if _, reportErr := h.gatewayService.ReportOpenAITrafficDirectorOutcome(ctx, service.OpenAITrafficDirectorHealthOutcomeInput{
		Account:          account,
		Model:            model,
		Err:              reportErr,
		StatusCode:       statusCode,
		AccountScoped:    accountScoped,
		AccountScopedSet: true,
	}); reportErr != nil && logger.L() != nil {
		logger.L().Debug("openai.count_tokens.traffic_director_health_report_failed",
			zap.Int64("account_id", account.ID), zap.Error(reportErr))
	}
}

func countTokensHealthReportError(c *gin.Context, err error, accountScoped bool) error {
	if !accountScoped {
		return errCountTokensLocalFailure
	}
	if err == nil {
		return nil
	}
	marker := countTokensUpstreamErrorMarker(c)
	if marker == "" || strings.Contains(err.Error(), marker) {
		return err
	}
	return fmt.Errorf("%s: %w", marker, err)
}

func countTokensUpstreamErrorMarker(c *gin.Context) string {
	if c == nil {
		return ""
	}
	for _, key := range []string{
		service.OpsUpstreamErrorMessageKey,
		service.OpsUpstreamErrorDetailKey,
	} {
		if value, ok := c.Get(key); ok {
			if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
				return strings.TrimSpace(text)
			}
		}
	}
	if events, ok := c.Get(service.OpsUpstreamErrorsKey); ok {
		if list, ok := events.([]*service.OpsUpstreamErrorEvent); ok {
			for i := len(list) - 1; i >= 0; i-- {
				if list[i] == nil {
					continue
				}
				if marker := strings.TrimSpace(list[i].Message); marker != "" {
					return marker
				}
				if marker := strings.TrimSpace(list[i].Detail); marker != "" {
					return marker
				}
			}
		}
	}
	return ""
}

func countTokensHasUpstreamEvidence(c *gin.Context) bool {
	if c == nil {
		return false
	}
	if status, ok := getContextInt64(c, service.OpsUpstreamStatusCodeKey); ok && status > 0 {
		return true
	}
	for _, key := range []string{
		service.OpsUpstreamErrorMessageKey,
		service.OpsUpstreamErrorDetailKey,
	} {
		if value, ok := c.Get(key); ok {
			if text, ok := value.(string); ok && strings.TrimSpace(text) != "" {
				return true
			}
		}
	}
	if events, ok := c.Get(service.OpsUpstreamErrorsKey); ok {
		if list, ok := events.([]*service.OpsUpstreamErrorEvent); ok && len(list) > 0 {
			return true
		}
	}
	return false
}

// acquireCountTokensAccountSlot turns a scheduler WaitPlan into a real account
// slot before Traffic Director health admission. A retry result is returned only
// for enforced TD requests, allowing the caller to exclude this account and
// advance through the configured pool chain without changing legacy errors.
func (h *OpenAIGatewayHandler) acquireCountTokensAccountSlot(
	c *gin.Context,
	groupID *int64,
	sessionHash string,
	selection *service.AccountSelectionResult,
	reqLog *zap.Logger,
) (release func(), retry bool, err error) {
	if selection == nil || selection.Account == nil {
		return nil, false, service.ErrNoAvailableAccounts
	}
	ctx := service.ContextWithSelectionProfitGate(c.Request.Context(), selection)
	account := selection.Account
	if selection.Acquired {
		if recheckErr := h.recheckOpenAICyberCooldownAfterAcquire(ctx, account, selection.ReleaseFunc, reqLog); recheckErr != nil {
			return nil, h.gatewayService.OpenAITrafficDirectorRetryEnabledInContext(ctx), recheckErr
		}
		return wrapReleaseOnDone(ctx, selection.ReleaseFunc), false, nil
	}
	if selection.WaitPlan == nil {
		return nil, false, service.ErrNoAvailableAccounts
	}

	accountRelease, acquired, acquireErr := h.concurrencyHelper.TryAcquireAccountSlot(
		ctx,
		account.ID,
		selection.WaitPlan.MaxConcurrency,
	)
	if acquireErr != nil {
		return nil, false, acquireErr
	}
	if !acquired {
		canWait, waitErr := h.concurrencyHelper.IncrementAccountWaitCount(ctx, account.ID, selection.WaitPlan.MaxWaiting)
		if waitErr != nil {
			reqLog.Warn("openai_count_tokens.account_wait_counter_increment_failed", zap.Int64("account_id", account.ID), zap.Error(waitErr))
		} else if !canWait {
			if h.gatewayService.OpenAITrafficDirectorRetryEnabledInContext(ctx) {
				return nil, true, nil
			}
			return nil, false, &WaitQueueFullError{SlotType: "account"}
		}

		waitCounted := waitErr == nil && canWait
		releaseWait := func() {
			if waitCounted {
				h.concurrencyHelper.DecrementAccountWaitCount(ctx, account.ID)
				waitCounted = false
			}
		}
		defer releaseWait()

		streamStarted := false
		accountRelease, acquireErr = h.concurrencyHelper.AcquireAccountSlotWithWaitTimeout(
			c,
			account.ID,
			selection.WaitPlan.MaxConcurrency,
			selection.WaitPlan.Timeout,
			false,
			&streamStarted,
		)
		if acquireErr != nil {
			var concurrencyErr *ConcurrencyError
			if h.gatewayService.OpenAITrafficDirectorRetryEnabledInContext(ctx) &&
				errors.As(acquireErr, &concurrencyErr) && concurrencyErr.IsTimeout {
				return nil, true, nil
			}
			return nil, false, acquireErr
		}
		releaseWait()
	}

	if recheckErr := h.recheckOpenAICyberCooldownAfterAcquire(ctx, account, accountRelease, reqLog); recheckErr != nil {
		return nil, h.gatewayService.OpenAITrafficDirectorRetryEnabledInContext(ctx), recheckErr
	}
	selection.ReleaseFunc = accountRelease
	if !selection.AdmitTrafficDirector(ctx, selection.ReleaseFunc) {
		if selection.ReleaseFunc != nil {
			selection.ReleaseFunc()
		}
		if h.gatewayService.OpenAITrafficDirectorRetryEnabledInContext(ctx) {
			return nil, true, nil
		}
		return nil, false, service.ErrTrafficDirectorNoAvailablePool
	}
	accountRelease = selection.ReleaseFunc
	if bindErr := h.gatewayService.BindStickySessionAfterProfitAdmission(ctx, groupID, sessionHash, account.ID); bindErr != nil {
		reqLog.Warn("openai_count_tokens.bind_sticky_session_failed", zap.Int64("account_id", account.ID), zap.Error(bindErr))
	}
	return wrapReleaseOnDone(ctx, accountRelease), false, nil
}
