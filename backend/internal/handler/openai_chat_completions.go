package handler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ip"
	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/Wei-Shaw/sub2api/internal/pkg/openai_compat"
	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

// ChatCompletions handles OpenAI Chat Completions API requests.
// POST /v1/chat/completions
func (h *OpenAIGatewayHandler) ChatCompletions(c *gin.Context) {
	streamStarted := false
	defer h.recoverResponsesPanic(c, &streamStarted)

	requestStart := time.Now()

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
		"handler.openai_gateway.chat_completions",
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
	if len(body) == 0 {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Request body is empty")
		return
	}
	var admissionState *openAISecurityAdmissionState
	if len(body) > securityadmission.CurrentLimits().BodyCapBytes {
		state, classifyErr := classifyOpenAISecurityAdmission(string(securityadmission.ProtocolOpenAIChat), body, securityadmission.LineageUntrusted)
		if classifyErr != nil {
			warnOpenAISecurityAdmission(c, reqLog, "security_admission.classification_failed", nil, 0,
				securityadmission.AccountUnknown, "classification_failed", "upstream_not_dispatched", zap.Error(classifyErr))
			h.errorResponse(c, openAIAdmissionErrorStatus(classifyErr), "invalid_request_error", "Failed to inspect request body")
			return
		}
		admissionState = state
		installOpenAISecurityAdmission(c, admissionState)
		logOpenAISecurityAdmission(c, reqLog, admissionState, securityadmission.AccountUnknown, "oversize_gate")
	}
	oversizeRequest := isOpenAIOversizeAdmission(admissionState)
	var oversizeEnvelope securityadmission.RoutingEnvelope
	if oversizeRequest {
		oversizeEnvelope, err = extractOpenAIOversizeRoutingEnvelope(
			admissionState, securityadmission.ProtocolOpenAIChat, body,
		)
		if err != nil {
			warnOpenAISecurityAdmission(c, reqLog, "security_admission.oversize_envelope_unavailable", admissionState, 0,
				securityadmission.AccountUnknown, "oversize_envelope_unavailable", "upstream_not_dispatched", zap.Error(err))
			h.errorResponse(c, http.StatusServiceUnavailable, "service_unavailable", "Oversized request routing metadata is unavailable")
			return
		}
		if openAIOversizeReasoningPolicyConfigured(c, apiKey) {
			warnOpenAISecurityAdmission(c, reqLog, "security_admission.oversize_preprocessing_required", admissionState, 0,
				securityadmission.AccountUnknown, "oversize_reasoning_policy_required", "upstream_not_dispatched")
			h.errorResponse(c, http.StatusServiceUnavailable, "service_unavailable", "Oversized request requires unsupported gateway preprocessing")
			return
		}
	}

	if !oversizeRequest && !gjson.ValidBytes(body) {
		logRequestBodyParseFailure(reqLog, body, nil)
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		return
	}

	var reqModel string
	if oversizeRequest {
		reqModel = oversizeEnvelope.Model
	} else {
		modelResult := gjson.GetBytes(body, "model")
		if !modelResult.Exists() || modelResult.Type != gjson.String || modelResult.String() == "" {
			h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "model is required")
			return
		}
		reqModel = modelResult.String()
	}
	ensureCompositeTargetPlatform(c, apiKey, reqModel)
	if !openAICompatibleTextTargetAllowed(c, apiKey, reqModel) {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "Model is not supported by this OpenAI-compatible endpoint for composite groups")
		return
	}
	auditBody := body
	if !oversizeRequest {
		if cappedBody, changed := applyOpenAIReasoningEffortPolicyForRequest(c, apiKey, body); changed {
			body = cappedBody
		}
	}
	reqStream := oversizeEnvelope.Stream
	if !oversizeRequest {
		reqStream, ok = parseOpenAICompatibleStream(body)
		if !ok {
			h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", invalidStreamFieldTypeMessage)
			return
		}
	}
	if service.IsGPTImageGenerationModel(reqModel) {
		h.errorResponse(c, http.StatusBadRequest, "invalid_request_error", "This model is not supported on the Chat Completions endpoint")
		return
	}

	if admissionState == nil {
		state, classifyErr := classifyOpenAISecurityAdmission(
			string(securityadmission.ProtocolOpenAIChat), auditBody, securityadmission.LineageUntrusted,
		)
		if classifyErr != nil {
			warnOpenAISecurityAdmission(c, reqLog, "security_admission.classification_failed", nil, 0,
				securityadmission.AccountUnknown, "classification_failed", "upstream_not_dispatched", zap.Error(classifyErr))
			h.errorResponse(c, openAIAdmissionErrorStatus(classifyErr), "invalid_request_error", "Failed to inspect request body")
			return
		}
		admissionState = state
		installOpenAISecurityAdmission(c, admissionState)
		logOpenAISecurityAdmission(c, reqLog, admissionState, securityadmission.AccountUnknown, "classified")
	}
	if openAIAdmissionShouldRejectBeforeRouting(admissionState) {
		h.errorResponse(c, http.StatusForbidden, "permission_error", "Request blocked by content policy")
		return
	}

	reqLog = reqLog.With(zap.String("model", reqModel), zap.Bool("stream", reqStream))

	setOpsRequestContext(c, reqModel, reqStream)
	setOpsEndpointContext(c, "", int16(service.RequestTypeFromLegacy(reqStream, false)))

	// 解析渠道级模型映射
	channelMapping, _ := h.gatewayService.ResolveChannelMappingAndRestrict(c.Request.Context(), apiKey.GroupID, reqModel)
	if oversizeRequest && channelMapping.Mapped {
		warnOpenAISecurityAdmission(c, reqLog, "security_admission.oversize_preprocessing_required", admissionState, 0,
			securityadmission.AccountUnknown, "oversize_channel_model_mapping_required", "upstream_not_dispatched")
		h.errorResponse(c, http.StatusServiceUnavailable, "service_unavailable", "Oversized request requires unsupported gateway preprocessing")
		return
	}
	forwardModel := reqModel
	if channelMapping.Mapped {
		forwardModel = channelMapping.MappedModel
	}
	forwardCtx := service.WithOpenAIDirectForwardModel(c.Request.Context(), forwardModel)
	forwardCtx = withOpenAIRemoteModelRequirement(forwardCtx, forwardModel)
	c.Request = c.Request.WithContext(forwardCtx)
	healthModel := openAITrafficDirectorHealthModel(reqModel, channelMapping)

	if h.errorPassthroughService != nil {
		service.BindErrorPassthroughService(c, h.errorPassthroughService)
	}

	subscription, _ := middleware2.GetSubscriptionFromContext(c)
	requestPlatform := openAICompatibleRequestPlatform(c.Request.Context(), apiKey)

	service.SetOpsLatencyMs(c, service.OpsAuthLatencyMsKey, time.Since(requestStart).Milliseconds())
	routingStart := time.Now()

	userReleaseFunc, acquired := h.acquireResponsesUserSlot(c, subject.UserID, subject.Concurrency, reqStream, &streamStarted, reqLog)
	if !acquired {
		return
	}
	if userReleaseFunc != nil {
		defer userReleaseFunc()
	}

	if err := h.billingCacheService.CheckBillingEligibility(c.Request.Context(), apiKey.User, apiKey, apiKey.Group, subscription, service.QuotaPlatform(c.Request.Context(), apiKey)); err != nil {
		reqLog.Info("openai_chat_completions.billing_eligibility_check_failed", zap.Error(err))
		status, code, message, retryAfter := billingErrorDetails(err)
		if retryAfter > 0 {
			c.Header("Retry-After", strconv.Itoa(retryAfter))
		}
		h.handleStreamingAwareError(c, status, code, message, streamStarted)
		return
	}

	sessionMetadataBody := body
	if oversizeRequest {
		sessionMetadataBody = nil
	}
	sessionHash := h.gatewayService.GenerateSessionHash(c, sessionMetadataBody)
	promptCacheKey := h.gatewayService.ExtractSessionID(c, sessionMetadataBody)

	maxAccountSwitches := h.maxAccountSwitches
	switchCount := 0
	profitVetoCount := 0
	failedAccountIDs := make(map[int64]struct{})
	sameAccountRetryCount := make(map[int64]int)
	var lastFailoverErr *service.UpstreamFailoverError
	var oauth429FailoverState service.OpenAIOAuth429FailoverState

	// 分组利润控制：chat completions 文本入口请求级装门并固定 pricingAt。
	ccPricingCtx, pricingAt := h.gatewayService.WithOpenAIRequestPricingContext(c.Request.Context(), apiKey.GroupID)
	ccPricingCtx = service.WithOpenAITrafficDirectorHealthModel(ccPricingCtx, healthModel)
	ccPricingCtx = h.gatewayService.WithOpenAITrafficDirectorRetryLoopContext(ccPricingCtx)
	c.Request = c.Request.WithContext(ccPricingCtx)

	for {
		if failoverClientGone(c) {
			return
		}
		reqLog.Debug("openai_chat_completions.account_selecting", zap.Int("excluded_account_count", len(failedAccountIDs)))
		selection, scheduleDecision, err := h.gatewayService.SelectAccountWithSchedulerForCapability(
			c.Request.Context(),
			apiKey.GroupID,
			"",
			sessionHash,
			reqModel,
			failedAccountIDs,
			service.OpenAIUpstreamTransportAny,
			service.OpenAIEndpointCapabilityChatCompletions,
			false,
			false,
			true,
			requestPlatform,
		)
		if err != nil {
			if failoverClientGone(c) {
				reqLog.Info("openai_chat_completions.account_select_aborted_client_disconnected", zap.Error(err))
				return
			}
			reqLog.Warn("openai_chat_completions.account_select_failed",
				zap.Error(openAICompatibleSelectionErrorForLog(err, requestPlatform)),
				zap.Int("excluded_account_count", len(failedAccountIDs)),
			)
			if cls, ok := classifyTrafficDirectorSelectionError(err); ok {
				markOpsRoutingCapacityLimitedIfNoAvailable(c, err)
				h.handleStreamingAwareError(c, cls.Status, cls.ErrType, cls.Message, streamStarted)
				return
			}
			// A request-local audit-exempt constraint is a security capacity
			// boundary, regardless of whether it came from classification, a final
			// model mapping, or a failed Pro audit fallback. Do not run ordinary
			// model diagnosis or replay an earlier upstream error: both can mask the
			// absence of a verified non-Pro account as 404/502.
			if openAIAuditFallbackExhausted(c, admissionState) {
				markOpsRoutingCapacityLimitedIfNoAvailable(c, err)
				h.handleStreamingAwareError(c, http.StatusServiceUnavailable, "service_unavailable", securityAuditMessage(securityAuditUnavailableDecision()), streamStarted)
				return
			}
			if len(failedAccountIDs) == 0 {
				cls := classifyOpenAICompatibleSelectionErrorFromGin(c, h.gatewayService, apiKey, reqModel, reqModel, err)
				if !cls.ModelNotFound {
					markOpsRoutingCapacityLimitedIfNoAvailable(c, err)
				}
				h.handleStreamingAwareError(c, cls.Status, cls.ErrType, cls.Message, streamStarted)
				return
			} else {
				if lastFailoverErr != nil {
					h.handleFailoverExhausted(c, lastFailoverErr, streamStarted)
				} else {
					h.handleStreamingAwareError(c, http.StatusBadGateway, "api_error", "Upstream request failed", streamStarted)
				}
				return
			}
		}
		if selection == nil || selection.Account == nil {
			if openAIAuditFallbackExhausted(c, admissionState) {
				h.handleStreamingAwareError(c, http.StatusServiceUnavailable, "service_unavailable", securityAuditMessage(securityAuditUnavailableDecision()), streamStarted)
				return
			}
			cls := classifyOpenAICompatibleNoAccountErrorFromGin(c, h.gatewayService, apiKey, reqModel, reqModel)
			if !cls.ModelNotFound {
				markOpsRoutingCapacityLimited(c)
			}
			h.handleStreamingAwareError(c, cls.Status, cls.ErrType, cls.Message, streamStarted)
			return
		}
		account := selection.Account
		sessionHash = ensureOpenAIPoolModeSessionHash(sessionHash, account)
		reqLog.Debug("openai_chat_completions.account_selected", zap.Int64("account_id", account.ID), zap.String("account_name", account.Name))
		_ = scheduleDecision
		setOpsSelectedAccount(c, account.ID, account.Platform)

		accountReleaseFunc, slotResult := h.acquireResponsesAccountSlot(c, apiKey.GroupID, sessionHash, selection, reqStream, &streamStarted, reqLog)
		if slotResult == openAISlotAcquireProfitVetoed {
			// 利润终检否决：排除该账号重新选号；否决次数达上限则按无可用账号终止。
			if !recordOpenAIProfitVeto(failedAccountIDs, account.ID, &profitVetoCount) {
				h.handleOpenAIProfitVetoExhausted(c, streamStarted, reqLog, profitVetoCount)
				return
			}
			continue
		}
		if slotResult == openAISlotAcquireRetry {
			failedAccountIDs[account.ID] = struct{}{}
			reqLog.Info("openai.traffic_director_wait_failed_reselect", zap.Int64("account_id", account.ID))
			continue
		}
		if slotResult != openAISlotAcquireOK {
			return
		}
		account = selection.Account
		terminalAdmission, terminalErr := h.gatewayService.AdmitOpenAIAccountRequirement(c.Request.Context(), account)
		if terminalErr != nil {
			if accountReleaseFunc != nil {
				accountReleaseFunc()
				accountReleaseFunc = nil
			}
			if openAITerminalAdmissionCanReselect(terminalErr) {
				failedAccountIDs[account.ID] = struct{}{}
				warnOpenAISecurityAdmission(c, reqLog, "security_admission.terminal_rejected", admissionState, account.ID,
					securityadmission.AccountUnknown, "terminal_admission_reselect", "upstream_not_dispatched", zap.Error(terminalErr))
				continue
			}
			errorOpenAISecurityAdmission(c, reqLog, "security_admission.terminal_unavailable", admissionState, account.ID,
				securityadmission.AccountUnknown, "terminal_admission_unavailable", "upstream_not_dispatched", zap.Error(terminalErr))
			h.handleStreamingAwareError(c, http.StatusServiceUnavailable, "service_unavailable", "Account security admission is temporarily unavailable", streamStarted)
			return
		}
		account = terminalAdmission.Selected
		selection.Account = account
		terminalCtx := service.WithOpenAIAccountTerminalAdmission(c.Request.Context(), terminalAdmission)
		c.Request = c.Request.WithContext(terminalCtx)
		releaseAccount := func() {
			if accountReleaseFunc != nil {
				accountReleaseFunc()
				accountReleaseFunc = nil
			}
		}
		if decision := h.checkSecurityAuditForSelectedOpenAIProAccount(
			c,
			reqLog,
			apiKey,
			subject,
			account,
			service.ContentModerationProtocolOpenAIChat,
			reqModel,
			auditBody,
		); decision != nil && !decision.AllowNextStage {
			if securityAuditCanReselect(decision) && h.selectedOpenAIAccountMayUseAuditFallback(c, account) && admissionState != nil &&
				admissionState.admission.Class() == securityadmission.RequestAuditableText && !admissionState.fallback {
				admissionState.fallback = true
				releaseAccount()
				failedAccountIDs[account.ID] = struct{}{}
				setOpenAIAccountRequirement(c, securityadmission.AccountRequirementAuditExempt)
				warnOpenAISecurityAdmission(c, reqLog, "security_admission.pro_audit_unavailable_reselect", admissionState, account.ID,
					securityadmission.AccountUnknown, string(decision.Kind), "upstream_not_dispatched")
				continue
			}
			releaseAccount()
			h.handleStreamingAwareErrorWithCode(
				c,
				securityAuditStatus(decision),
				securityAuditErrorType(decision),
				securityAuditErrorCode(decision),
				securityAuditMessage(decision),
				streamStarted,
				false,
			)
			return
		}

		service.SetOpsLatencyMs(c, service.OpsRoutingLatencyMsKey, time.Since(routingStart).Milliseconds())
		forwardStart := time.Now()

		forwardBody := body
		if channelMapping.Mapped {
			forwardBody = h.gatewayService.ReplaceModelInBody(body, channelMapping.MappedModel)
		}
		writerSizeBeforeForward := c.Writer.Size()
		result, err := func() (*service.OpenAIForwardResult, error) {
			defer releaseAccount()
			selection.CommitTrafficDirectorAttempt()
			forwardCtx := withOpenAISecurityDispatchObserver(c.Request.Context(), c, reqLog, account)
			return h.gatewayService.ForwardAsChatCompletions(forwardCtx, c, account, forwardBody, promptCacheKey, "")
		}()
		h.reportOpenAITrafficDirectorOutcome(c.Request.Context(), account, healthModel, result, err)
		h.recordCyberPolicyIfMarked(c, apiKey, account, subscription, reqModel, err != nil, clientRequestedUsageFields(c, channelMapping, reqModel, ""), service.HashUsageRequestPayload(body))

		forwardDurationMs := time.Since(forwardStart).Milliseconds()
		upstreamLatencyMs, _ := getContextInt64(c, service.OpsUpstreamLatencyMsKey)
		responseLatencyMs := forwardDurationMs
		if upstreamLatencyMs > 0 && forwardDurationMs > upstreamLatencyMs {
			responseLatencyMs = forwardDurationMs - upstreamLatencyMs
		}
		service.SetOpsLatencyMs(c, service.OpsResponseLatencyMsKey, responseLatencyMs)
		if err == nil && result != nil && result.FirstTokenMs != nil {
			service.SetOpsLatencyMs(c, service.OpsTimeToFirstTokenMsKey, int64(*result.FirstTokenMs))
		}
		// #5148 对齐：错误返回携带的部分 result（流中断前上游已计量的 usage）照常
		// 入账；failover 错误恒定 result=nil，不会重复计费。
		submitChatUsage := func(res *service.OpenAIForwardResult) {
			if res == nil {
				return
			}
			userAgent := c.GetHeader("User-Agent")
			clientIP := ip.GetClientIP(c)
			inboundEndpoint := GetInboundEndpoint(c)
			upstreamEndpoint := resolveOpenAIUpstreamEndpoint(c, account, res)
			quotaPlatform := service.QuotaPlatform(c.Request.Context(), apiKey)
			sessionID := service.ExtractClientSessionID(c)
			cyberBlocked := service.GetOpsCyberPolicy(c) != nil
			h.submitOpenAIUsageRecordTask(c.Request.Context(), res, func(ctx context.Context) {
				if err := h.gatewayService.RecordUsage(ctx, &service.OpenAIRecordUsageInput{
					Result:             res,
					APIKey:             apiKey,
					User:               apiKey.User,
					Account:            account,
					Subscription:       subscription,
					InboundEndpoint:    inboundEndpoint,
					UpstreamEndpoint:   upstreamEndpoint,
					UserAgent:          userAgent,
					IPAddress:          clientIP,
					APIKeyService:      h.apiKeyService,
					QuotaPlatform:      quotaPlatform,
					SessionID:          sessionID,
					ChannelUsageFields: clientRequestedUsageFields(c, channelMapping, reqModel, res.UpstreamModel),
					PricingAt:          pricingAt,
					CyberBlocked:       cyberBlocked,
				}); err != nil {
					logger.L().With(
						zap.String("component", "handler.openai_gateway.chat_completions"),
						zap.Int64("user_id", subject.UserID),
						zap.Int64("api_key_id", apiKey.ID),
						zap.Any("group_id", apiKey.GroupID),
						zap.String("model", reqModel),
						zap.Int64("account_id", account.ID),
					).Error("openai_chat_completions.record_usage_failed", zap.Error(err))
				}
			})
		}
		if err != nil {
			if result != nil && result.ImageCount > 0 {
				reqLog.Warn("openai_chat_completions.forward_partial_error_with_image_result",
					zap.Int64("account_id", account.ID),
					zap.Int("image_count", result.ImageCount),
					zap.Error(err),
				)
			} else {
				var failoverErr *service.UpstreamFailoverError
				if errors.As(err, &failoverErr) {
					if failoverClientGone(c) {
						reqLog.Info("openai_chat_completions.failover_aborted_client_disconnected",
							zap.Int64("account_id", account.ID),
							zap.Int("upstream_status", failoverErr.StatusCode),
						)
						return
					}
					if c.Writer.Size() != writerSizeBeforeForward {
						h.handleFailoverExhausted(c, failoverErr, true)
						return
					}
					if failoverErr.ShouldReportAccountScheduleFailure() {
						h.gatewayService.ReportOpenAIAccountScheduleResult(account.ID, account.GetMappedModel(reqModel), false, nil)
					}
					if !failoverErr.ShouldRetryNextAccount() {
						h.handleFailoverExhausted(c, failoverErr, streamStarted)
						return
					}
					// Pool mode: retry on the same account
					if failoverErr.RetryableOnSameAccount {
						retryLimit := account.GetPoolModeRetryCount()
						if sameAccountRetryCount[account.ID] < retryLimit {
							sameAccountRetryCount[account.ID]++
							retryDelay := sameAccountRetryDelayFor(failoverErr, sameAccountRetryCount[account.ID])
							reqLog.Warn("openai_chat_completions.pool_mode_same_account_retry",
								zap.Int64("account_id", account.ID),
								zap.Int("upstream_status", failoverErr.StatusCode),
								zap.Int("retry_limit", retryLimit),
								zap.Int("retry_count", sameAccountRetryCount[account.ID]),
								zap.Duration("retry_delay", retryDelay),
							)
							select {
							case <-c.Request.Context().Done():
								return
							case <-time.After(retryDelay):
							}
							continue
						}
					}
					h.gatewayService.RecordOpenAIAccountSwitch()
					failedAccountIDs[account.ID] = struct{}{}
					lastFailoverErr = failoverErr
					if switchCount >= maxAccountSwitches {
						h.handleFailoverExhausted(c, failoverErr, streamStarted)
						return
					}
					switchCount++
					if h.gatewayService.ShouldStopOpenAIOAuth429Failover(account, failoverErr.StatusCode, switchCount, &oauth429FailoverState) {
						h.handleFailoverExhausted(c, failoverErr, streamStarted)
						return
					}
					reqLog.Warn("openai_chat_completions.upstream_failover_switching",
						zap.Int64("account_id", account.ID),
						zap.Int("upstream_status", failoverErr.StatusCode),
						zap.Int("switch_count", switchCount),
						zap.Int("max_switches", maxAccountSwitches),
					)
					continue
				}
				h.gatewayService.ReportOpenAIAccountScheduleResult(account.ID, account.GetMappedModel(reqModel), false, nil)
				upstreamErrorAlreadyCommunicated := openAIForwardErrorAlreadyCommunicated(c, writerSizeBeforeForward, err)
				wroteFallback := false
				if !upstreamErrorAlreadyCommunicated {
					wroteFallback = h.ensureOpenAIStreamReadErrorResponse(c, err, streamStarted)
					if !wroteFallback {
						wroteFallback = h.ensureForwardErrorResponse(c, streamStarted)
					}
				}
				reqLog.Warn("openai_chat_completions.forward_failed",
					zap.Int64("account_id", account.ID),
					zap.Bool("fallback_error_response_written", wroteFallback),
					zap.Bool("upstream_error_response_already_written", upstreamErrorAlreadyCommunicated),
					zap.Error(err),
				)
				submitChatUsage(result)
				return
			}
		}
		if result != nil {
			h.gatewayService.ReportOpenAIAccountScheduleResult(account.ID, account.GetMappedModel(reqModel), true, result.FirstTokenMs)
		} else {
			h.gatewayService.ReportOpenAIAccountScheduleResult(account.ID, account.GetMappedModel(reqModel), true, nil)
		}

		submitChatUsage(result)
		reqLog.Debug("openai_chat_completions.request_completed",
			zap.Int64("account_id", account.ID),
			zap.Int("switch_count", switchCount),
		)
		return
	}
}

// resolveOpenAIUpstreamEndpoint returns the actual upstream endpoint for an
// OpenAI-compatible account. A forwarding result is authoritative because a
// single inbound route may choose raw Chat or a Responses bridge at runtime.
// The account-based derivation remains as a fallback for existing callers and
// forwarding paths that do not report their endpoint yet.
func resolveOpenAIUpstreamEndpoint(c *gin.Context, account *service.Account, result *service.OpenAIForwardResult) string {
	if result != nil {
		if endpoint := strings.TrimSpace(result.UpstreamEndpoint); endpoint != "" {
			return endpoint
		}
	}
	if endpoint := service.GetActualOpenAIUpstreamEndpoint(c); endpoint != "" {
		return endpoint
	}
	if account != nil && account.Type == service.AccountTypeAPIKey &&
		!openai_compat.ShouldUseResponsesAPI(account.Extra) {
		return EndpointChatCompletions
	}
	return GetUpstreamEndpoint(c, account.Platform)
}
