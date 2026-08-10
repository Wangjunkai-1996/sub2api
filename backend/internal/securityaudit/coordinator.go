package securityaudit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
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

type blockingScope interface {
	BlockingApplies(req Request) bool
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
	strict := mode == ModeBlocking && c.blockingApplies(req)
	if strict {
		prepared, rejected := c.prepareStrict(ctx, req)
		if rejected != nil {
			return *rejected
		}
		req = prepared
	}
	switch mode {
	case ModeAsync:
		_ = c.prompt.Enqueue(ctx, req.Clone())
		legacy, _ := c.checkLegacy(ctx, req)
		return prioritize(legacy, nil, false, nil)
	case ModeBlocking:
		return c.checkBlocking(ctx, req, strict)
	default:
		legacy, _ := c.checkLegacy(ctx, req)
		return prioritize(legacy, nil, false, nil)
	}
}

func (c *Coordinator) blockingApplies(req Request) bool {
	if c == nil || c.prompt == nil {
		return false
	}
	if scoped, ok := c.prompt.(blockingScope); ok {
		return scoped.BlockingApplies(req)
	}
	return false
}

func (c *Coordinator) prepareStrict(ctx context.Context, req Request) (Request, *Decision) {
	document := req.Document.Clone()
	if document == nil {
		document = auditinput.Parse(req.Protocol, req.Body)
	}
	if document == nil || !document.Complete {
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
			decision := contextIncompleteDecision(nil, nil)
			return req, &decision
		}
		decision := auditUnavailableDecision(nil, nil)
		return req, &decision
	}
	if !validPriorAudit(prior, req, previousResponseID) {
		decision := contextIncompleteDecision(nil, nil)
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

func (c *Coordinator) checkBlocking(ctx context.Context, req Request, strict bool) Decision {
	if strict && c.legacy == nil {
		return auditUnavailableDecision(nil, nil)
	}
	if strict {
		return c.checkStrictBlocking(ctx, req)
	}
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

func (c *Coordinator) checkStrictBlocking(ctx context.Context, req Request) Decision {
	legacy, err := c.checkLegacy(ctx, req.Clone())
	if err != nil || legacy == nil || legacy.Unavailable || (!legacy.Allowed && !legacy.Blocked && !legacy.Flagged) {
		return auditUnavailableDecision(legacy, nil)
	}
	if legacy.Blocked || legacy.Flagged {
		return policyBlockedDecision(legacy, nil)
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
	return Decision{
		Kind: DecisionBlock, HTTPStatus: http.StatusForbidden, ErrorCode: ErrorCodePolicyBlocked,
		ClientMessage: "请求未通过安全策略审计，请调整输入后重试", Legacy: legacy, Prompt: prompt,
	}
}

func contextIncompleteDecision(legacy *LegacyDecision, prompt *PromptDecision) Decision {
	return Decision{
		Kind: DecisionInvalid, HTTPStatus: http.StatusUnprocessableEntity, ErrorCode: ErrorCodeContextIncomplete,
		ClientMessage: "请求上下文无法完整审计，请重新发起完整请求", Legacy: legacy, Prompt: prompt,
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
	if !req.Strict || req.Document == nil || !req.Document.Complete || prompt == nil || prompt.Kind != DecisionAllow || !prompt.AllowNextStage {
		return nil
	}
	priorContext := ""
	parentPromptHash := ""
	mediaDigests := make([]string, 0, len(req.Document.Media))
	if req.PriorAudit != nil {
		priorContext = req.PriorAudit.RedactedContext
		parentPromptHash = req.PriorAudit.PromptHash
		mediaDigests = append(mediaDigests, req.PriorAudit.MediaDigests...)
	}
	contextText, ok := cumulativeContext(priorContext, req.Document.NormalizedText)
	if !ok {
		return nil
	}
	for _, media := range req.Document.Media {
		mediaDigests = append(mediaDigests, media.Digest)
	}
	return &AuditSummary{
		ParserVersion: auditinput.ParserVersion, ConfigVersion: prompt.ConfigVersion,
		APIKeyID: req.APIKeyID, GroupID: cloneInt64Ptr(req.GroupID), PreviousResponseID: req.Document.PreviousResponseID,
		PromptHash: hashAuditContext(contextText, mediaDigests), ParentPromptHash: parentPromptHash, DocumentHash: req.Document.Hash,
		NormalizedContext: contextText, RedactedContext: RedactContext(contextText),
		MediaDigests: mediaDigests, ContextComplete: true, Verdict: AuditVerdictAllow,
	}
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
