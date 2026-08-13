package securityaudit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
)

var (
	ErrLineageNotFound = errors.New("strict audit lineage not found")
	ErrLineageInvalid  = errors.New("strict audit lineage is invalid")
)

type LineageLookup struct {
	GroupID            *int64
	APIKeyID           int64
	PreviousResponseID string
}

type LineageStore interface {
	Load(ctx context.Context, lookup LineageLookup) (*AuditSummary, error)
	BindAllowedResponse(ctx context.Context, summary AuditSummary, responseID string) error
}

func (c *Coordinator) SetLineageStore(store LineageStore) *Coordinator {
	if c != nil {
		c.lineage = store
	}
	return c
}

func (c *Coordinator) BindAllowedResponse(ctx context.Context, summary AuditSummary, responseID string) error {
	if summary.SkipResponseLineage {
		return nil
	}
	if c == nil || c.lineage == nil {
		return errors.New("strict audit lineage store unavailable")
	}
	if strings.TrimSpace(responseID) == "" || summary.Verdict != AuditVerdictAllow || !summary.ContextComplete || summary.PromptHash == "" || !auditSummaryHasContext(summary) {
		return ErrLineageInvalid
	}
	return c.lineage.BindAllowedResponse(ctx, summary.Clone(), strings.TrimSpace(responseID))
}

// AppendResponsesOutput extends an allowed request summary with the complete
// assistant/tool output that became model-visible at the successful terminal
// event. The output is not trusted as an allow decision: it is persisted only
// so the next continuation re-audits the complete cumulative context.
func AppendResponsesOutput(summary AuditSummary, output []byte) (AuditSummary, error) {
	if summary.Verdict != AuditVerdictAllow || !summary.ContextComplete || strings.TrimSpace(summary.PromptHash) == "" {
		return AuditSummary{}, fmt.Errorf("%w: request summary is incomplete", ErrLineageInvalid)
	}
	document := auditinput.ParseResponsesOutput(output)
	if document == nil || !document.Complete {
		return AuditSummary{}, fmt.Errorf("%w: response output is incomplete", ErrLineageInvalid)
	}
	// A digest alone cannot prove that a model-generated image was audited on
	// the next turn. Until lineage can carry an independently verified media
	// artifact, any response media makes the continuation context incomplete.
	if len(document.Media) > 0 {
		return AuditSummary{}, fmt.Errorf("%w: response output contains media", ErrLineageInvalid)
	}
	contextText, ok := cumulativeContext(summary.RedactedContext, document.NormalizedText)
	if !ok {
		return AuditSummary{}, fmt.Errorf("%w: cumulative context exceeds limit", ErrLineageInvalid)
	}
	augmented := summary.Clone()
	augmented.ParentPromptHash = strings.TrimSpace(summary.PromptHash)
	augmented.NormalizedContext = contextText
	augmented.RedactedContext = RedactContext(contextText)
	augmented.PromptHash = hashAuditContext(contextText, augmented.MediaDigests)
	documentHash := sha256.Sum256([]byte(auditinput.ParserVersion + "\nrequest:" + strings.TrimSpace(summary.DocumentHash) + "\nresponse:" + document.Hash))
	augmented.DocumentHash = hex.EncodeToString(documentHash[:])
	if !auditSummaryHasContext(augmented) {
		return AuditSummary{}, fmt.Errorf("%w: cumulative context is empty", ErrLineageInvalid)
	}
	return augmented, nil
}

func auditSummaryHasContext(summary AuditSummary) bool {
	return strings.TrimSpace(summary.RedactedContext) != "" || len(summary.MediaDigests) > 0
}
