package handler

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/domain"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

// ResponsesInputTokens handles native OpenAI POST
// /v1/responses/input_tokens requests without routing them through the normal
// Responses generation and usage-recording pipeline.
func (h *OpenAIGatewayHandler) ResponsesInputTokens(c *gin.Context) {
	apiKey, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok {
		h.errorResponse(c, http.StatusUnauthorized, "authentication_error", "Invalid API key")
		return
	}
	subject, ok := middleware2.GetAuthSubjectFromContext(c)
	if !ok {
		h.errorResponse(c, http.StatusInternalServerError, "api_error", "User context not found")
		return
	}
	reqLog := requestLogger(
		c,
		"handler.openai_gateway.responses_input_tokens",
		zap.Int64("user_id", subject.UserID),
		zap.Int64("api_key_id", apiKey.ID),
		zap.Any("group_id", apiKey.GroupID),
	)
	if !h.ensureResponsesDependencies(c, reqLog) {
		return
	}

	body, err := readLenientJSONRequestBodyWithPrealloc(c.Request, h.cfg)
	if err != nil {
		if maxErr, ok := extractMaxBytesError(err); ok {
			h.errorResponse(c, http.StatusRequestEntityTooLarge, "invalid_request_error", buildBodyTooLargeMessage(maxErr.Limit))
			return
		}
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to read request body")
		return
	}
	if len(body) == 0 || !gjson.ValidBytes(body) {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		return
	}
	modelResult := gjson.GetBytes(body, "model")
	if !modelResult.Exists() || modelResult.Type != gjson.String || strings.TrimSpace(modelResult.String()) == "" {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "model is required")
		return
	}
	reqModel := strings.TrimSpace(modelResult.String())
	ensureCompositeTargetPlatform(c, apiKey, reqModel)
	if !openAICompatibleTextTargetAllowed(c, apiKey, reqModel) {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Model is not supported by this OpenAI-compatible endpoint for composite groups")
		return
	}

	setOpsRequestContext(c, reqModel, false)
	setOpsEndpointContext(c, "", int16(service.RequestTypeFromLegacy(false, false)))
	if decision := h.checkSecurityAudit(c, reqLog, apiKey, subject, service.ContentModerationProtocolOpenAIResponses, reqModel, body); decision != nil && !decision.AllowNextStage {
		h.openAISecurityAuditError(c, decision)
		return
	}

	subscription, _ := middleware2.GetSubscriptionFromContext(c)
	if err := h.billingCacheService.CheckBillingEligibility(c.Request.Context(), apiKey.User, apiKey, apiKey.Group, subscription, service.QuotaPlatform(c.Request.Context(), apiKey)); err != nil {
		reqLog.Info("openai_input_tokens.billing_eligibility_check_failed", zap.Error(err))
		status, code, message, retryAfter := billingErrorDetails(err)
		if retryAfter > 0 {
			c.Header("Retry-After", strconv.Itoa(retryAfter))
		}
		h.errorResponse(c, status, code, message)
		return
	}

	channelMapping, _ := h.gatewayService.ResolveChannelMappingAndRestrict(c.Request.Context(), apiKey.GroupID, reqModel)
	routingModel := reqModel
	forwardBody := body
	if channelMapping.Mapped {
		routingModel = channelMapping.MappedModel
		forwardBody = h.gatewayService.ReplaceModelInBody(body, routingModel)
	}

	// Token counting is not billed, so it must not be excluded by the profit gate.
	c.Request = c.Request.WithContext(service.WithOpenAIProfitControlSuppressed(c.Request.Context()))
	requestPlatform := openAICompatibleRequestPlatform(c.Request.Context(), apiKey)
	sessionHash := h.gatewayService.GenerateSessionHash(c, body)
	requestStart := time.Now()
	account, err := h.gatewayService.SelectAccountForTokenCount(
		c.Request.Context(),
		apiKey.GroupID,
		sessionHash,
		routingModel,
		service.OpenAIEndpointCapabilityChatCompletions,
		requestPlatform,
	)
	service.SetOpsLatencyMs(c, service.OpsAuthLatencyMsKey, time.Since(requestStart).Milliseconds())
	if err != nil {
		reqLog.Warn("openai_input_tokens.account_select_failed", zap.Error(openAICompatibleSelectionErrorForLog(err, requestPlatform)))
		cls := classifyOpenAICompatibleNoAccountErrorFromGin(c, h.gatewayService, apiKey, routingModel, reqModel)
		if !cls.ModelNotFound {
			markOpsRoutingCapacityLimitedIfNoAvailable(c, err)
		}
		h.errorResponse(c, cls.Status, cls.ErrType, cls.Message)
		return
	}
	if account == nil {
		cls := classifyOpenAICompatibleNoAccountErrorFromGin(c, h.gatewayService, apiKey, routingModel, reqModel)
		if !cls.ModelNotFound {
			markOpsRoutingCapacityLimited(c)
		}
		h.errorResponse(c, cls.Status, cls.ErrType, cls.Message)
		return
	}

	setOpsSelectedAccount(c, account.ID, account.Platform)
	if err := h.gatewayService.ForwardResponsesInputTokens(c.Request.Context(), c, account, forwardBody); err != nil {
		reqLog.Error("openai_input_tokens.forward_failed", zap.Int64("account_id", account.ID), zap.Error(err))
	}
}

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

	if !allowOpenAICompatibleMessagesDispatch(c, apiKey) {
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
	// composite+grok 在路由层已分流到 GrokCountTokens，这里可达的目标平台是
	// openai 与 CN 供应商；CN 账号由 ForwardCountTokensAsAnthropic 本地估算。
	if !openAICompatibleTextTargetAllowed(c, apiKey, reqModel) {
		h.anthropicErrorResponse(c, http.StatusBadRequest, "invalid_request_error", "Model is not supported by this OpenAI-compatible endpoint for composite groups")
		return
	}
	routingModel := service.NormalizeOpenAICompatRequestedModel(reqModel)
	preferredMappedModel := resolveOpenAIMessagesDispatchMappedModel(c, apiKey, reqModel)
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
	c.Request = c.Request.WithContext(service.WithOpenAIProfitControlSuppressed(c.Request.Context()))
	sessionHash := h.gatewayService.GenerateSessionHash(c, body)
	currentRoutingModel := routingModel
	if preferredMappedModel != "" {
		currentRoutingModel = preferredMappedModel
	}
	requestPlatform := openAICompatibleRequestPlatform(c.Request.Context(), apiKey)
	selection, _, selectErr := h.gatewayService.SelectAccountWithSchedulerForCapability(
		c.Request.Context(),
		apiKey.GroupID,
		"",
		sessionHash,
		currentRoutingModel,
		nil,
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
		cls := classifyOpenAICompatibleNoAccountErrorFromGin(c, h.gatewayService, apiKey, currentRoutingModel, reqModel)
		cls = classifySelectionFailureError(selectErr, cls)
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
	accountRelease, acquireErr := h.acquireCountTokensAccountSlot(c, apiKey.GroupID, sessionHash, selection, reqLog)
	if acquireErr != nil {
		status, errType, message := concurrencyErrorResponse(acquireErr, "account")
		h.anthropicErrorResponse(c, status, errType, message)
		return
	}

	account = selection.Account
	forwardBody := mappedBodyForMessages(channelMapping.Mapped, channelMapping.MappedModel)
	defaultMappedModel := preferredMappedModel
	if accountRelease != nil {
		defer accountRelease()
	}
	if forwardErr := h.gatewayService.ForwardCountTokensAsAnthropic(c.Request.Context(), c, account, forwardBody, defaultMappedModel); forwardErr != nil {
		reqLog.Error("openai_count_tokens.forward_failed", zap.Int64("account_id", account.ID), zap.Error(forwardErr))
	}
}

// acquireCountTokensAccountSlot turns a scheduler WaitPlan into a real account
// slot while preserving the normal wait, cooldown, and sticky-session semantics.
func (h *OpenAIGatewayHandler) acquireCountTokensAccountSlot(
	c *gin.Context,
	groupID *int64,
	sessionHash string,
	selection *service.AccountSelectionResult,
	reqLog *zap.Logger,
) (release func(), err error) {
	if selection == nil || selection.Account == nil {
		return nil, service.ErrNoAvailableAccounts
	}
	ctx := service.ContextWithSelectionProfitGate(c.Request.Context(), selection)
	account := selection.Account
	if selection.Acquired {
		if recheckErr := h.recheckOpenAICyberCooldownAfterAcquire(ctx, account, selection.ReleaseFunc, reqLog); recheckErr != nil {
			return nil, recheckErr
		}
		return wrapReleaseOnDone(ctx, selection.ReleaseFunc), nil
	}
	if selection.WaitPlan == nil {
		return nil, service.ErrNoAvailableAccounts
	}

	accountRelease, acquired, acquireErr := h.concurrencyHelper.TryAcquireAccountSlot(
		ctx,
		account.ID,
		selection.WaitPlan.MaxConcurrency,
	)
	if acquireErr != nil {
		return nil, acquireErr
	}
	if !acquired {
		canWait, waitErr := h.concurrencyHelper.IncrementAccountWaitCount(ctx, account.ID, selection.WaitPlan.MaxWaiting)
		if waitErr != nil {
			reqLog.Warn("openai_count_tokens.account_wait_counter_increment_failed", zap.Int64("account_id", account.ID), zap.Error(waitErr))
		} else if !canWait {
			return nil, &WaitQueueFullError{SlotType: "account"}
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
			return nil, acquireErr
		}
		releaseWait()
	}

	if recheckErr := h.recheckOpenAICyberCooldownAfterAcquire(ctx, account, accountRelease, reqLog); recheckErr != nil {
		return nil, recheckErr
	}
	selection.ReleaseFunc = accountRelease
	if bindErr := h.gatewayService.BindStickySessionAfterProfitAdmission(ctx, groupID, sessionHash, account.ID); bindErr != nil {
		reqLog.Warn("openai_count_tokens.bind_sticky_session_failed", zap.Int64("account_id", account.ID), zap.Error(bindErr))
	}
	return wrapReleaseOnDone(ctx, accountRelease), nil
}
