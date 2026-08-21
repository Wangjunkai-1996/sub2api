package securityaudit

import (
	"context"
	"errors"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type LegacyModerationAdapter struct {
	service *service.ContentModerationService
}

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
			input.CanonicalText = document.Text
			input.CanonicalClass = string(req.Admission.Class())
		}
	}
	decision, err := a.service.Check(ctx, input)
	if err != nil || decision == nil {
		return nil, err
	}
	return &LegacyDecision{
		Allowed: decision.Allowed, Blocked: decision.Blocked, Flagged: decision.Flagged,
		Message: decision.Message, StatusCode: decision.StatusCode,
		ErrorCode: "content_policy_violation", Action: decision.Action,
	}, nil
}
