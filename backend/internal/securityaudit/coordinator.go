package securityaudit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
)

type LegacyEngine interface {
	Check(ctx context.Context, req Request) (*LegacyDecision, error)
}

type PromptEngine interface {
	EffectiveMode() Mode
	Enqueue(ctx context.Context, req Request) error
	Evaluate(ctx context.Context, req Request) (*PromptDecision, error)
}

type promptBlockingScope interface {
	BlockingApplies(req Request) bool
}

type legacyBlockingScope interface {
	BlockingApplies(ctx context.Context, req Request) (bool, error)
}

type Coordinator struct {
	legacy  LegacyEngine
	prompt  PromptEngine
	lineage LineageStore
}

func NewCoordinator(legacy LegacyEngine, prompt PromptEngine) *Coordinator {
	return &Coordinator{legacy: legacy, prompt: prompt}
}

func (c *Coordinator) Check(ctx context.Context, req Request) Decision {
	if c == nil {
		return auditUnavailableDecision(nil, nil)
	}
	mode := ModeOff
	if c.prompt != nil {
		mode = c.prompt.EffectiveMode()
	}
	strict, err := c.legacyBlockingApplies(ctx, req)
	if err != nil {
		return auditUnavailableDecision(nil, nil)
	}
	if strict {
		prepared, rejected := c.prepareStrict(ctx, req)
		if rejected != nil {
			return *rejected
		}
		req = prepared
		if req.Document.TextAuditClass == auditinput.TextAuditKnownNoText {
			decision := allowDecision(nil, nil)
			decision.Audit = buildAuditSummary(req, nil)
			if decision.Audit == nil {
				return auditUnavailableDecision(nil, nil)
			}
			return decision
		}
		// A blocking mode may be the fail-closed effective state while the active
		// snapshot is unavailable or stale. Always enter Evaluate in that mode: the
		// prompt service returns allow for a trusted out-of-scope group and
		// unavailable for a degraded blocking configuration.
		promptRequired := mode == ModeBlocking
		decision := c.checkStrict(ctx, req, promptRequired)
		if decision.AllowNextStage && mode == ModeAsync && c.prompt != nil {
			_ = c.prompt.Enqueue(ctx, req.Clone())
		}
		return decision
	}
	switch mode {
	case ModeAsync:
		_ = c.prompt.Enqueue(ctx, req.Clone())
		legacy, _ := c.checkLegacy(ctx, req)
		return prioritize(legacy, nil, false, nil)
	case ModeBlocking:
		if c.promptBlockingApplies(req) {
			return c.checkBlocking(ctx, req)
		}
		legacy, _ := c.checkLegacy(ctx, req)
		return prioritize(legacy, nil, false, nil)
	default:
		legacy, _ := c.checkLegacy(ctx, req)
		return prioritize(legacy, nil, false, nil)
	}
}

func (c *Coordinator) legacyBlockingApplies(ctx context.Context, req Request) (bool, error) {
	if c == nil || c.legacy == nil {
		return false, nil
	}
	if scoped, ok := c.legacy.(legacyBlockingScope); ok {
		return scoped.BlockingApplies(ctx, req)
	}
	return false, nil
}

func (c *Coordinator) promptBlockingApplies(req Request) bool {
	if c == nil || c.prompt == nil {
		return false
	}
	if scoped, ok := c.prompt.(promptBlockingScope); ok {
		return scoped.BlockingApplies(req)
	}
	return false
}

func (c *Coordinator) prepareStrict(ctx context.Context, req Request) (Request, *Decision) {
	var document *auditinput.Document
	if len(req.Body) > 0 {
		document = auditinput.ParseForTextAudit(req.Protocol, req.Body)
	} else {
		document = req.Document.Clone()
	}
	if document == nil || document.TextAuditClass == auditinput.TextAuditIndeterminate {
		logStrictInputIncomplete(req, document)
		decision := contextIncompleteDecision(nil, nil)
		return req, &decision
	}
	if document.TextAuditClass != auditinput.TextAuditAuditableText &&
		document.TextAuditClass != auditinput.TextAuditKnownNoText {
		logStrictInputIncomplete(req, document)
		decision := contextIncompleteDecision(nil, nil)
		return req, &decision
	}
	req.Strict = true
	req.Document = document
	previousResponseID := strings.TrimSpace(document.PreviousResponseID)
	if previousResponseID == "" {
		contextText, ok := cumulativeContext("", document.NormalizedText)
		if !ok {
			decision := contextIncompleteDecision(nil, nil)
			return req, &decision
		}
		req.AuditContext = contextText
		return req, nil
	}
	if c.lineage == nil {
		decision := auditUnavailableDecision(nil, nil)
		return req, &decision
	}
	prior, err := c.lineage.Load(ctx, LineageLookup{
		GroupID: cloneInt64Ptr(req.GroupID), APIKeyID: req.APIKeyID, PreviousResponseID: previousResponseID,
	})
	if err != nil {
		if errors.Is(err, ErrLineageNotFound) || errors.Is(err, ErrLineageInvalid) {
			decision := lineageIncompatibleDecision(nil, nil)
			return req, &decision
		}
		decision := auditUnavailableDecision(nil, nil)
		return req, &decision
	}
	if !validPriorAudit(prior, req, previousResponseID) {
		decision := lineageIncompatibleDecision(nil, nil)
		return req, &decision
	}
	contextText, ok := cumulativeContext(prior.RedactedContext, document.NormalizedText)
	if !ok {
		decision := contextIncompleteDecision(nil, nil)
		return req, &decision
	}
	cloned := prior.Clone()
	req.PriorAudit = &cloned
	req.AuditContext = contextText
	return req, nil
}

type strictInputIssueLog struct {
	Code      string
	PathClass string
	Count     int
}

func logStrictInputIncomplete(req Request, document *auditinput.Document) {
	issues := []auditinput.Issue(nil)
	truncated := false
	classification := auditinput.TextAuditIndeterminate
	textRunes := 0
	if document != nil {
		issues = document.Issues
		truncated = document.Truncated
		classification = document.TextAuditClass
		textRunes = document.AuditTextRunes
	}
	slog.Warn("security_audit.strict_input_incomplete",
		"request_id", req.RequestID,
		"api_key_id", req.APIKeyID,
		"group_id", pointerLogID(req.GroupID),
		"protocol", req.Protocol,
		"body_bytes", len(req.Body),
		"text_runes", textRunes,
		"text_audit_classification", classification,
		"issue_count", len(issues),
		"truncated", truncated,
		"issues", summarizeStrictInputIssues(issues))
}

func summarizeStrictInputIssues(issues []auditinput.Issue) []strictInputIssueLog {
	counts := make(map[string]int)
	for _, issue := range issues {
		code := strings.TrimSpace(issue.Code)
		pathClass := strictInputPathClass(issue.Path)
		counts[code+"\x00"+pathClass]++
	}
	summary := make([]strictInputIssueLog, 0, len(counts))
	for key, count := range counts {
		parts := strings.SplitN(key, "\x00", 2)
		summary = append(summary, strictInputIssueLog{Code: parts[0], PathClass: parts[1], Count: count})
	}
	sort.Slice(summary, func(i, j int) bool {
		if summary[i].Code == summary[j].Code {
			return summary[i].PathClass < summary[j].PathClass
		}
		return summary[i].Code < summary[j].Code
	})
	return summary
}

func strictInputPathClass(path string) string {
	path = strings.TrimSpace(path)
	switch {
	case path == "" || path == "$":
		return "root"
	case strings.HasPrefix(path, "$.input[") || path == "$.input":
		return "responses_input_item"
	case strings.HasPrefix(path, "$.output[") || path == "$.output":
		return "responses_output_item"
	case strings.HasPrefix(path, "$.messages[") || path == "$.messages":
		return "message"
	case strings.HasPrefix(path, "$.tools[") || strings.HasPrefix(path, "$.functions["):
		return "tool_definition"
	case strings.HasPrefix(path, "$.instructions"):
		return "instructions"
	case strings.HasPrefix(path, "$.previous_response_id"):
		return "previous_response_id"
	default:
		return "other"
	}
}

func validPriorAudit(prior *AuditSummary, req Request, previousResponseID string) bool {
	if prior == nil || prior.Verdict != AuditVerdictAllow || !prior.ContextComplete || prior.APIKeyID != req.APIKeyID ||
		!auditSummaryHasContext(*prior) || strings.TrimSpace(previousResponseID) == "" {
		return false
	}
	if prior.ParserVersion != "" && prior.ParserVersion != auditinput.ParserVersion {
		return false
	}
	return sameGroupID(prior.GroupID, req.GroupID)
}

func sameGroupID(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func (c *Coordinator) checkBlocking(ctx context.Context, req Request) Decision {
	var wg sync.WaitGroup
	wg.Add(2)
	var legacy *LegacyDecision
	var legacyErr error
	var prompt *PromptDecision
	go func() {
		defer wg.Done()
		legacy, legacyErr = c.checkLegacy(ctx, req.Clone())
	}()
	go func() {
		defer wg.Done()
		if c.prompt == nil {
			prompt = unavailablePromptDecision(ErrorCodeUnavailable)
			return
		}
		result, err := c.prompt.Evaluate(ctx, req.Clone())
		if err != nil {
			var guardErr *GuardError
			if errors.As(err, &guardErr) && guardErr.Code == ErrorCodeInvalidResponse {
				prompt = unavailablePromptDecision(ErrorCodeInvalidResponse)
				return
			}
			prompt = unavailablePromptDecision(ErrorCodeUnavailable)
			return
		}
		if result == nil {
			prompt = unavailablePromptDecision(ErrorCodeUnavailable)
			return
		}
		prompt = result
	}()
	wg.Wait()
	_ = legacyErr
	return prioritize(legacy, prompt, false, nil)
}

func (c *Coordinator) checkStrict(ctx context.Context, req Request, promptRequired bool) Decision {
	legacy, err := c.checkLegacy(ctx, req.Clone())
	if err != nil || legacy == nil || legacy.Unavailable || (!legacy.Allowed && !legacy.Blocked && !legacy.Flagged) {
		slog.Warn("security_audit.strict_legacy_unavailable",
			"request_id", req.RequestID,
			"api_key_id", req.APIKeyID,
			"group_id", pointerLogID(req.GroupID),
			"protocol", req.Protocol,
			"body_bytes", len(req.Body),
			"text_runes", req.Document.AuditTextRunes,
			"text_audit_classification", req.Document.TextAuditClass,
			"error_kind", "legacy_audit_unavailable")
		return auditUnavailableDecision(legacy, nil)
	}
	if legacy.Blocked || legacy.Flagged {
		return policyBlockedDecision(legacy, nil)
	}
	if !promptRequired {
		decision := allowDecision(legacy, nil)
		decision.Audit = buildAuditSummary(req, nil)
		if decision.Audit == nil {
			return auditUnavailableDecision(legacy, nil)
		}
		return decision
	}
	if c.prompt == nil {
		return auditUnavailableDecision(legacy, nil)
	}
	prompt, err := c.prompt.Evaluate(ctx, req.Clone())
	if err != nil || prompt == nil {
		code := ErrorCodeUnavailable
		var guardErr *GuardError
		if errors.As(err, &guardErr) && guardErr.Code == ErrorCodeInvalidResponse {
			code = ErrorCodeInvalidResponse
		}
		prompt = unavailablePromptDecision(code)
	}
	return prioritize(legacy, prompt, true, func() *AuditSummary {
		return buildAuditSummary(req, prompt)
	})
}

func (c *Coordinator) checkLegacy(ctx context.Context, req Request) (*LegacyDecision, error) {
	if c.legacy == nil {
		return nil, nil
	}
	return c.legacy.Check(ctx, req)
}

func prioritize(legacy *LegacyDecision, prompt *PromptDecision, strict bool, audit func() *AuditSummary) Decision {
	if strict {
		if legacy != nil && (legacy.Blocked || legacy.Flagged) {
			return policyBlockedDecision(legacy, prompt)
		}
		if legacy != nil && (legacy.Unavailable || !legacy.Allowed) {
			return auditUnavailableDecision(legacy, prompt)
		}
		if prompt == nil || prompt.Kind == DecisionUnavailable || prompt.Kind == DecisionInvalid ||
			(prompt.Kind == DecisionAllow && !prompt.AllowNextStage) {
			return auditUnavailableDecision(legacy, prompt)
		}
		if prompt.Kind == DecisionBlock || prompt.Kind == DecisionFlag {
			return policyBlockedDecision(legacy, prompt)
		}
		if prompt.Kind != DecisionAllow {
			return auditUnavailableDecision(legacy, prompt)
		}
		decision := allowDecision(legacy, prompt)
		if audit != nil {
			decision.Audit = audit()
		}
		if decision.Audit == nil {
			return auditUnavailableDecision(legacy, prompt)
		}
		return decision
	}
	if legacy != nil && legacy.Blocked {
		status := legacy.StatusCode
		if status < 400 || status > 599 {
			status = http.StatusForbidden
		}
		code := legacy.ErrorCode
		if code == "" {
			code = "content_policy_violation"
		}
		return Decision{
			Kind: DecisionBlock, HTTPStatus: status, ErrorCode: code, ClientMessage: legacy.Message,
			Legacy: legacy, Prompt: prompt, AllowNextStage: false,
		}
	}
	if prompt == nil {
		return allowDecision(legacy, nil)
	}
	switch prompt.Kind {
	case DecisionBlock:
		return Decision{Kind: DecisionBlock, HTTPStatus: http.StatusForbidden, ErrorCode: ErrorCodeBlocked,
			ClientMessage: "提示词安全审计拒绝了该请求，请调整输入后重试", Legacy: legacy, Prompt: prompt}
	case DecisionInvalid:
		return Decision{Kind: DecisionInvalid, HTTPStatus: http.StatusServiceUnavailable, ErrorCode: ErrorCodeInvalidResponse,
			ClientMessage: "提示词安全审计暂时不可用，请稍后重试", Legacy: legacy, Prompt: prompt}
	case DecisionUnavailable:
		return Decision{Kind: DecisionUnavailable, HTTPStatus: http.StatusServiceUnavailable, ErrorCode: ErrorCodeUnavailable,
			ClientMessage: "提示词安全审计暂时不可用，请稍后重试", Legacy: legacy, Prompt: prompt}
	case DecisionFlag:
		return Decision{Kind: DecisionFlag, HTTPStatus: http.StatusOK, Legacy: legacy, Prompt: prompt, AllowNextStage: true}
	default:
		return allowDecision(legacy, prompt)
	}
}

func policyBlockedDecision(legacy *LegacyDecision, prompt *PromptDecision) Decision {
	status := http.StatusForbidden
	if legacy != nil && legacy.StatusCode >= 400 && legacy.StatusCode <= 599 {
		status = legacy.StatusCode
	}
	return Decision{
		Kind: DecisionBlock, HTTPStatus: status, ErrorCode: ErrorCodePolicyBlocked,
		ClientMessage: "请求未通过安全策略审计，请调整输入后重试", Legacy: legacy, Prompt: prompt,
	}
}

func contextIncompleteDecision(legacy *LegacyDecision, prompt *PromptDecision) Decision {
	return policyInputBlockedDecision(
		legacy, prompt, ErrorCodeContextIncomplete,
		"请求内容无法完整审计，已在发送上游前拦截；请检查请求格式后重试",
	)
}

func lineageIncompatibleDecision(legacy *LegacyDecision, prompt *PromptDecision) Decision {
	return policyInputBlockedDecision(
		legacy, prompt, ErrorCodeLineageIncompatible,
		"当前会话没有有效的安全审计记录，已停止续接；请新建会话后重试",
	)
}

func policyInputBlockedDecision(legacy *LegacyDecision, prompt *PromptDecision, code, message string) Decision {
	status := http.StatusUnprocessableEntity
	if code == ErrorCodeLineageIncompatible {
		status = http.StatusForbidden
	}
	return Decision{
		Kind: DecisionBlock, HTTPStatus: status, ErrorCode: code,
		ClientMessage: message, Legacy: legacy, Prompt: prompt,
	}
}

func auditUnavailableDecision(legacy *LegacyDecision, prompt *PromptDecision) Decision {
	return Decision{
		Kind: DecisionUnavailable, HTTPStatus: http.StatusServiceUnavailable, ErrorCode: ErrorCodeAuditUnavailable,
		ClientMessage: "安全审计暂时不可用，请稍后重试", Legacy: legacy, Prompt: prompt,
	}
}

func allowDecision(legacy *LegacyDecision, prompt *PromptDecision) Decision {
	return Decision{Kind: DecisionAllow, HTTPStatus: http.StatusOK, Legacy: legacy, Prompt: prompt, AllowNextStage: true}
}

func unavailablePromptDecision(code string) *PromptDecision {
	kind := DecisionUnavailable
	if code == ErrorCodeInvalidResponse {
		kind = DecisionInvalid
	}
	return &PromptDecision{Kind: kind, ErrorCode: code, AllowNextStage: false}
}

func buildAuditSummary(req Request, prompt *PromptDecision) *AuditSummary {
	if !req.Strict || req.Document == nil || req.Document.TextAuditClass == auditinput.TextAuditIndeterminate {
		return nil
	}
	configVersion := int64(0)
	if prompt != nil {
		if prompt.Kind != DecisionAllow || !prompt.AllowNextStage {
			return nil
		}
		configVersion = prompt.ConfigVersion
	}
	priorContext := ""
	parentPromptHash := ""
	mediaDigests := make([]string, 0, len(req.Document.Media))
	if req.PriorAudit != nil {
		priorContext = req.PriorAudit.RedactedContext
		parentPromptHash = req.PriorAudit.PromptHash
		mediaDigests = append(mediaDigests, req.PriorAudit.MediaDigests...)
	}
	contextText := strings.TrimSpace(req.AuditContext)
	if contextText == "" {
		var ok bool
		contextText, ok = cumulativeContext(priorContext, req.Document.NormalizedText)
		if !ok {
			return nil
		}
	} else if utf8.RuneCountInString(contextText) > auditinput.MaxTextRunes {
		return nil
	}
	for _, media := range req.Document.Media {
		mediaDigests = append(mediaDigests, media.Digest)
	}
	skipResponseLineage := skipsResponseLineage(req)
	normalizedContext := contextText
	redactedContext := ""
	if skipResponseLineage {
		normalizedContext = ""
	} else {
		redactedContext = RedactContext(contextText)
	}
	return &AuditSummary{
		ParserVersion: auditinput.ParserVersion, ConfigVersion: configVersion,
		APIKeyID: req.APIKeyID, GroupID: cloneInt64Ptr(req.GroupID), PreviousResponseID: req.Document.PreviousResponseID,
		PromptHash: hashAuditContext(contextText, mediaDigests), ParentPromptHash: parentPromptHash, DocumentHash: req.Document.Hash,
		NormalizedContext: normalizedContext, RedactedContext: redactedContext,
		MediaDigests: mediaDigests, ContextComplete: true, Verdict: AuditVerdictAllow,
		SkipResponseLineage: skipResponseLineage,
	}
}

func skipsResponseLineage(req Request) bool {
	if !strings.EqualFold(strings.TrimSpace(req.Protocol), auditinput.ProtocolOpenAIResponses) ||
		req.Document == nil || strings.TrimSpace(req.Document.PreviousResponseID) != "" || req.Document.Store == nil {
		return false
	}
	return !*req.Document.Store
}

func hashAuditContext(contextText string, mediaDigests []string) string {
	h := sha256.New()
	_, _ = h.Write([]byte(auditinput.ParserVersion + "\n" + contextText))
	for _, digest := range mediaDigests {
		_, _ = h.Write([]byte("\nmedia:" + digest))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func cumulativeContext(prior, current string) (string, bool) {
	prior = strings.TrimSpace(prior)
	current = strings.TrimSpace(current)
	combined := current
	if prior != "" && current != "" {
		combined = prior + "\n" + current
	} else if prior != "" {
		combined = prior
	}
	return combined, utf8.RuneCountInString(combined) <= auditinput.MaxTextRunes
}
