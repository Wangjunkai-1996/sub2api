package handler

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/securityaudit"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

const openAISecurityAdmissionContextKey = "sub2api.openai.security_admission"

// openAISecurityAdmissionState is request-local. The Admission itself is
// immutable and stores offsets into the caller-owned body; no body copy is
// retained here. bodyStart makes reuse an O(1) identity check. The body must
// remain immutable while this state is installed, as production handlers do.
// The fallback bit is mutated only by the serial handler loop.
type openAISecurityAdmissionState struct {
	admission securityadmission.Admission
	protocol  string
	lineage   securityadmission.LineageTrust
	bodyStart *byte
	fallback  bool
}

func installOpenAISecurityAdmission(c *gin.Context, state *openAISecurityAdmissionState) {
	if c == nil || c.Request == nil || state == nil {
		return
	}
	c.Set(openAISecurityAdmissionContextKey, state)
	ctx := service.WithOpenAIAccountRequirement(c.Request.Context(), state.admission.Requirement())
	c.Request = c.Request.WithContext(ctx)
}

func openAISecurityAdmissionFromContext(c *gin.Context) *openAISecurityAdmissionState {
	if c == nil {
		return nil
	}
	value, exists := c.Get(openAISecurityAdmissionContextKey)
	if !exists {
		return nil
	}
	state, _ := value.(*openAISecurityAdmissionState)
	return state
}

func setOpenAIAccountRequirement(c *gin.Context, requirement securityadmission.AccountRequirement) {
	if c == nil || c.Request == nil {
		return
	}
	ctx := service.WithOpenAIAccountRequirement(c.Request.Context(), requirement)
	c.Request = c.Request.WithContext(ctx)
}

func withOpenAIRemoteModelRequirement(ctx context.Context, model string) context.Context {
	if securityadmission.ModelImpliesRemoteSearch(model) {
		return service.WithOpenAIAccountRequirement(ctx, securityadmission.AccountRequirementAuditExempt)
	}
	return ctx
}

func classifyOpenAISecurityAdmission(protocol string, body []byte, lineage securityadmission.LineageTrust) (*openAISecurityAdmissionState, error) {
	return classifyOpenAISecurityAdmissionWithOptions(protocol, body, securityadmission.Options{Lineage: lineage})
}

// classifyOpenAISecurityAdmissionWithResourceExpansion is used by the
// OpenAI-compatible gateway handlers (including Responses WebSocket frames).
// It keeps structurally valid large text auditable while leaving opaque
// resources on the existing verified-account path.
func classifyOpenAISecurityAdmissionWithResourceExpansion(
	protocol string,
	body []byte,
	lineage securityadmission.LineageTrust,
) (*openAISecurityAdmissionState, error) {
	return classifyOpenAISecurityAdmissionWithResourceExpansionOptions(
		protocol, body, securityadmission.Options{Lineage: lineage},
	)
}

func classifyOpenAISecurityAdmissionWithOptions(
	protocol string,
	body []byte,
	options securityadmission.Options,
) (*openAISecurityAdmissionState, error) {
	return classifyOpenAISecurityAdmissionWithOptionsInternal(protocol, body, options, false)
}

func classifyOpenAISecurityAdmissionWithResourceExpansionOptions(
	protocol string,
	body []byte,
	options securityadmission.Options,
) (*openAISecurityAdmissionState, error) {
	return classifyOpenAISecurityAdmissionWithOptionsInternal(protocol, body, options, true)
}

func classifyOpenAISecurityAdmissionWithOptionsInternal(
	protocol string,
	body []byte,
	options securityadmission.Options,
	expandResources bool,
) (*openAISecurityAdmissionState, error) {
	var (
		admission securityadmission.Admission
		err       error
	)
	if expandResources {
		admission, err = securityadmission.ClassifyWithResourceExpansion(protocol, body, options)
	} else {
		admission, err = securityadmission.Classify(protocol, body, options)
	}
	if err != nil {
		return nil, err
	}
	return &openAISecurityAdmissionState{
		admission: admission,
		protocol:  protocol,
		lineage:   admission.Lineage(),
		bodyStart: openAISecurityAdmissionBodyStart(body),
	}, nil
}

func openAISecurityAdmissionBodyStart(body []byte) *byte {
	if len(body) == 0 {
		return nil
	}
	return &body[0]
}

// matchesBody proves that Admission offsets belong to this exact immutable
// body allocation. Length alone is insufficient for successive equal-sized WS
// frames, while hashing here would add an avoidable O(body) pre-scan.
func (state *openAISecurityAdmissionState) matchesBody(protocol string, body []byte) bool {
	if state == nil || state.protocol != protocol || state.admission.BodyBytes() != len(body) {
		return false
	}
	if len(body) == 0 {
		return true
	}
	return state.bodyStart == &body[0]
}

func openAIAdmissionShouldRejectBeforeRouting(state *openAISecurityAdmissionState) bool {
	if state == nil {
		return false
	}
	return state.admission.Class() == securityadmission.RequestKnownViolation
}

func logOpenAISecurityAdmission(c *gin.Context, reqLog *zap.Logger, state *openAISecurityAdmissionState, accountClass securityadmission.AccountClass, dispatch string) {
	if reqLog == nil || state == nil {
		return
	}
	fields := openAISecurityAdmissionLogFields(c, state, 0, accountClass, "", dispatch)
	fields = append(fields,
		zap.String("request_reason", string(state.admission.Reason())),
		zap.String("account_requirement", string(state.admission.Requirement())),
		zap.String("lineage", string(state.admission.Lineage())),
		zap.Int("body_bytes", state.admission.BodyBytes()),
		zap.Int("text_runes", state.admission.TextRunes()),
	)
	reqLog.Info("security_admission.classified", fields...)
}

func openAISecurityAdmissionLogFields(
	c *gin.Context,
	state *openAISecurityAdmissionState,
	accountID int64,
	accountClass securityadmission.AccountClass,
	reason string,
	dispatch string,
) []zap.Field {
	requestID := ""
	if c != nil && c.Request != nil {
		requestID = contentModerationRequestID(c.Request.Context())
		if terminal := service.OpenAIAccountTerminalAdmissionFromContext(c.Request.Context()); terminal != nil && terminal.Selected != nil &&
			(accountID == 0 || terminal.Selected.ID == accountID) {
			accountID = terminal.Selected.ID
			if accountClass == "" || accountClass == securityadmission.AccountUnknown {
				accountClass = terminal.AccountClass
			}
		}
	}
	if accountClass == "" {
		accountClass = securityadmission.AccountUnknown
	}
	requestClass := string(securityadmission.RequestUninspectable)
	requestReason := ""
	if state != nil {
		requestClass = string(state.admission.Class())
		requestReason = string(state.admission.Reason())
	}
	if strings.TrimSpace(reason) == "" {
		reason = requestReason
	}
	if strings.TrimSpace(reason) == "" {
		reason = "unspecified"
	}
	if strings.TrimSpace(dispatch) == "" {
		dispatch = "upstream_not_dispatched"
	}
	return []zap.Field{
		zap.String("request_id", requestID),
		zap.Int64("account_id", accountID),
		zap.String("request_class", requestClass),
		zap.String("account_class", string(accountClass)),
		zap.String("reason", reason),
		zap.String("dispatch", dispatch),
	}
}

func warnOpenAISecurityAdmission(
	c *gin.Context,
	reqLog *zap.Logger,
	event string,
	state *openAISecurityAdmissionState,
	accountID int64,
	accountClass securityadmission.AccountClass,
	reason string,
	dispatch string,
	extra ...zap.Field,
) {
	if reqLog == nil {
		return
	}
	fields := openAISecurityAdmissionLogFields(c, state, accountID, accountClass, reason, dispatch)
	reqLog.Warn(event, append(fields, extra...)...)
}

func errorOpenAISecurityAdmission(
	c *gin.Context,
	reqLog *zap.Logger,
	event string,
	state *openAISecurityAdmissionState,
	accountID int64,
	accountClass securityadmission.AccountClass,
	reason string,
	dispatch string,
	extra ...zap.Field,
) {
	if reqLog == nil {
		return
	}
	fields := openAISecurityAdmissionLogFields(c, state, accountID, accountClass, reason, dispatch)
	reqLog.Error(event, append(fields, extra...)...)
}

func observeOpenAISecurityDispatch(c *gin.Context, reqLog *zap.Logger, account *service.Account, dispatch string) {
	state := openAISecurityAdmissionFromContext(c)
	if state == nil || account == nil {
		return
	}
	accountClass := securityadmission.AccountUnknown
	if c != nil && c.Request != nil {
		if terminal := service.OpenAIAccountTerminalAdmissionFromContext(c.Request.Context()); terminal != nil &&
			terminal.Selected != nil && terminal.Selected.ID == account.ID {
			accountClass = terminal.AccountClass
		}
	}
	securityadmission.ObserveDispatch(accountClass)
	if reqLog == nil {
		return
	}
	fields := openAISecurityAdmissionLogFields(c, state, account.ID, accountClass, "", dispatch)
	fields = append(fields,
		zap.String("request_reason", string(state.admission.Reason())),
		zap.String("account_requirement", string(state.admission.Requirement())),
		zap.String("lineage", string(state.admission.Lineage())),
		zap.Int("body_bytes", state.admission.BodyBytes()),
		zap.Int("text_runes", state.admission.TextRunes()),
	)
	reqLog.Info("security_admission.dispatch", fields...)
}

func withOpenAISecurityDispatchObserver(ctx context.Context, c *gin.Context, reqLog *zap.Logger, account *service.Account) context.Context {
	return service.WithOpenAIUpstreamDispatchObserver(ctx, func() {
		observeOpenAISecurityDispatch(c, reqLog, account, "upstream_dispatch")
	})
}

func securityAuditCanReselect(decision *securityaudit.Decision) bool {
	if decision == nil || decision.AllowNextStage {
		return false
	}
	return decision.Kind == securityaudit.DecisionUnavailable || decision.Kind == securityaudit.DecisionInvalid
}

func openAITerminalAdmissionCanReselect(err error) bool {
	return errors.Is(err, service.ErrOpenAIAccountRequirementIncompatible) ||
		errors.Is(err, service.ErrOpenAIAccountAdmissionUnavailable) ||
		errors.Is(err, service.ErrGatewaySessionLimitExceeded)
}

func openAIAuditFallbackExhausted(c *gin.Context, state *openAISecurityAdmissionState) bool {
	// An audit-exempt requirement only describes the account class allowed to
	// serve the request. It does not mean that Prompt Audit was attempted. The
	// latter is true only after a Pro audit failure explicitly switched this
	// request to the verified fallback pool. Keeping these states separate avoids
	// reporting an empty verified pool as a Prompt Audit outage.
	return state != nil && state.fallback
}

func openAISecurityAdmissionRequired(c *gin.Context) bool {
	return c != nil && c.Request != nil &&
		service.OpenAIAccountRequirementFromContext(c.Request.Context()) == securityadmission.AccountRequirementAuditExempt
}

func openAISecurityAdmissionErrorMessage(c *gin.Context, state *openAISecurityAdmissionState) string {
	if openAIAuditFallbackExhausted(c, state) {
		return securityAuditMessage(securityAuditUnavailableDecision())
	}
	return "No available accounts"
}

// openAISecurityAdmissionUnavailable reports a security-capacity failure only
// when the request has not already reached an upstream. Once an upstream
// failover error exists, that error is the authoritative result and must not
// be rewritten as the generic security-admission 503. This preserves provider
// status codes (for example, a shared Cloudflare 502) after the verified
// account pool is exhausted.
func openAISecurityAdmissionUnavailable(
	c *gin.Context,
	state *openAISecurityAdmissionState,
	lastFailoverErr *service.UpstreamFailoverError,
) bool {
	if lastFailoverErr != nil {
		return false
	}
	return openAIAuditFallbackExhausted(c, state) || openAISecurityAdmissionRequired(c)
}

func openAIAdmissionErrorStatus(err error) int {
	if err == nil {
		return http.StatusBadRequest
	}
	var parseErr *securityadmission.ParseError
	if errors.As(err, &parseErr) {
		return http.StatusBadRequest
	}
	return http.StatusServiceUnavailable
}

func openAIAdmissionProtocolForPath(protocol string) string {
	return string(securityadmission.NormalizeProtocol(protocol))
}
