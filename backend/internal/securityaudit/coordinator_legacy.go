package securityaudit

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type legacyModerationChecker interface {
	Check(context.Context, service.ContentModerationCheckInput) (*service.ContentModerationDecision, error)
}

type LegacyModerationAdapter struct {
	service legacyModerationChecker
}

// canonicalAuditWorkerCount bounds the synchronous fan-out used when the
// legacy risk-control API has a smaller per-request input limit than the
// canonical transcript. The bound is intentionally independent of the number
// of chunks: every chunk is still audited, but a large request cannot create an
// unbounded number of in-flight calls.
const canonicalAuditWorkerCount = 4

func NewLegacyModerationAdapter(svc *service.ContentModerationService) LegacyEngine {
	return &LegacyModerationAdapter{service: svc}
}

func (a *LegacyModerationAdapter) Check(ctx context.Context, req Request) (*LegacyDecision, error) {
	if a == nil || a.service == nil {
		return nil, nil
	}
	input := service.ContentModerationCheckInput{
		RequestID: req.RequestID, UserID: req.UserID, UserEmail: req.UserEmail,
		APIKeyID: req.APIKeyID, APIKeyName: req.APIKeyName, GroupID: cloneInt64Ptr(req.GroupID),
		GroupName: req.GroupName, Endpoint: req.Endpoint, Provider: req.Provider,
		Model: req.Model, Protocol: req.Protocol, Body: req.Body,
	}
	if req.Admission != nil {
		switch req.Admission.Class() {
		case securityadmission.RequestKnownNoText:
			return &LegacyDecision{Allowed: true, Action: string(service.ContentModerationActionAllow)}, nil
		case securityadmission.RequestUninspectable, securityadmission.RequestKnownViolation:
			return nil, errors.New("canonical request is not legacy-auditable")
		case securityadmission.RequestAuditableText:
			document, materializeErr := req.Admission.MaterializeDocument(req.Body, securityadmission.TextScopeFullTranscript)
			if materializeErr != nil {
				return nil, materializeErr
			}
			input.CanonicalClass = string(req.Admission.Class())
			return a.checkCanonicalTextChunks(ctx, input, document.Text)
		}
	}
	decision, err := a.service.Check(ctx, input)
	if err != nil || decision == nil {
		return nil, err
	}
	return &LegacyDecision{
		Allowed: decision.Allowed, Blocked: decision.Blocked, Flagged: decision.Flagged, Audited: decision.Audited,
		Message: decision.Message, StatusCode: decision.StatusCode,
		ErrorCode: "content_policy_violation", Action: decision.Action,
	}, nil
}

// checkCanonicalTextChunks keeps the legacy moderation API's per-request
// input bound without silently dropping text from a canonical transcript. The
// admission parser has already validated and materialized the complete text;
// each chunk is therefore an independent synchronous proof obligation.
func (a *LegacyModerationAdapter) checkCanonicalTextChunks(
	ctx context.Context,
	input service.ContentModerationCheckInput,
	text string,
) (*LegacyDecision, error) {
	chunks := splitCanonicalText(text, service.ContentModerationMaxInputRunes())
	if len(chunks) == 0 {
		return nil, errors.New("canonical request produced no moderation text")
	}

	// Identical chunks have identical moderation semantics. De-duplicating them
	// preserves complete coverage while preventing a repeated transcript (a
	// common shape for tool/replay payloads) from multiplying API calls.
	uniqueChunks := make([]string, 0, len(chunks))
	seen := make(map[string]struct{}, len(chunks))
	for _, chunk := range chunks {
		if _, ok := seen[chunk]; ok {
			continue
		}
		seen[chunk] = struct{}{}
		uniqueChunks = append(uniqueChunks, chunk)
	}

	type chunkResult struct {
		index    int
		decision *service.ContentModerationDecision
		err      error
	}
	auditCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan int)
	results := make(chan chunkResult, len(uniqueChunks))
	workerCount := canonicalAuditWorkerCount
	if workerCount > len(uniqueChunks) {
		workerCount = len(uniqueChunks)
	}
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for worker := 0; worker < workerCount; worker++ {
		go func() {
			defer workers.Done()
			for index := range jobs {
				select {
				case <-auditCtx.Done():
					results <- chunkResult{index: index, err: auditCtx.Err()}
					continue
				default:
				}
				chunkInput := input
				chunkInput.CanonicalText = uniqueChunks[index]
				decision, err := a.service.Check(auditCtx, chunkInput)
				results <- chunkResult{index: index, decision: decision, err: err}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for index := range uniqueChunks {
			select {
			case jobs <- index:
			case <-auditCtx.Done():
				return
			}
		}
	}()
	go func() {
		workers.Wait()
		close(results)
	}()

	aggregated := &LegacyDecision{
		Allowed:   true,
		Audited:   true,
		Action:    string(service.ContentModerationActionAllow),
		ErrorCode: "content_policy_violation",
	}
	var firstErr error
	var blocked *service.ContentModerationDecision
	completed := 0
	for result := range results {
		completed++
		if result.decision != nil && result.decision.Blocked {
			// A policy block is authoritative and can stop the remaining scans;
			// dispatch is already forbidden, so auditing later chunks is unnecessary.
			if blocked == nil {
				blocked = result.decision
				cancel()
			}
			continue
		}
		if result.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("legacy moderation chunk %d/%d: %w", result.index+1, len(uniqueChunks), result.err)
			}
			cancel()
			continue
		}
		if result.decision == nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("legacy moderation chunk %d/%d returned no decision", result.index+1, len(uniqueChunks))
			}
			cancel()
			continue
		}
		if !result.decision.Allowed || !result.decision.Audited {
			if firstErr == nil {
				firstErr = fmt.Errorf("legacy moderation chunk %d/%d did not produce an audit proof", result.index+1, len(uniqueChunks))
			}
			cancel()
			continue
		}
		aggregated.Flagged = aggregated.Flagged || result.decision.Flagged
		if aggregated.Message == "" {
			aggregated.Message = result.decision.Message
		}
		if result.decision.StatusCode > aggregated.StatusCode {
			aggregated.StatusCode = result.decision.StatusCode
		}
		if result.decision.Action != "" && result.decision.Action != string(service.ContentModerationActionAllow) {
			aggregated.Action = result.decision.Action
		}
	}
	if blocked != nil {
		return &LegacyDecision{
			Allowed:    false,
			Blocked:    true,
			Flagged:    true,
			Audited:    blocked.Audited,
			Message:    blocked.Message,
			StatusCode: blocked.StatusCode,
			ErrorCode:  "content_policy_violation",
			Action:     blocked.Action,
		}, nil
	}
	if firstErr != nil {
		return nil, firstErr
	}
	if completed != len(uniqueChunks) {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("legacy moderation canceled after %d/%d chunks: %w", completed, len(uniqueChunks), err)
		}
		return nil, fmt.Errorf("legacy moderation stopped after %d/%d chunks", completed, len(uniqueChunks))
	}
	return aggregated, nil
}

// splitCanonicalText splits at UTF-8 rune boundaries without converting the
// complete transcript to []rune, which would temporarily double memory for
// production-sized request bodies.
func splitCanonicalText(text string, maxRunes int) []string {
	if text == "" || maxRunes <= 0 {
		return nil
	}
	chunks := make([]string, 0, (len(text)/maxRunes)+1)
	start, runes := 0, 0
	for index := range text {
		if runes == maxRunes {
			chunks = append(chunks, text[start:index])
			start, runes = index, 0
		}
		runes++
	}
	if start < len(text) {
		chunks = append(chunks, text[start:])
	}
	return chunks
}
