package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"
)

var ErrOpenAIReasoningEffortUnsupported = errors.New("reasoning effort unsupported by selected upstream model")

type OpenAIReasoningEffortUnsupportedError struct {
	Requested string
	Effective string
	Model     string
}

func (e *OpenAIReasoningEffortUnsupportedError) Error() string {
	return fmt.Sprintf("reasoning effort %q is not verified as supported by upstream model %q", e.Effective, e.Model)
}

func (e *OpenAIReasoningEffortUnsupportedError) Unwrap() error {
	return ErrOpenAIReasoningEffortUnsupported
}

// ApplyOpenAIManagedReasoningEffortPolicy evaluates mappings against the actual
// selected account model, while returning the original model field so the
// ordinary forwarding chain does not apply account mappings twice.
func ApplyOpenAIManagedReasoningEffortPolicy(ctx context.Context, account *Account, body []byte, maxEffort string, mappings []ReasoningEffortMapping, overLimit string) ([]byte, error) {
	requestedModel := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	actualModel := normalizeOpenAIModelForUpstream(account, canonicalOpenAIAccountSchedulingModel(account, requestedModel))
	scoped, err := sjson.SetBytes(body, "model", actualModel)
	if err != nil {
		return body, err
	}
	effective, changed, err := ApplyOpenAIReasoningEffortPolicy(scoped, maxEffort, mappings, overLimit)
	if err != nil {
		return body, err
	}
	metadata, known := account.GetUpstreamModelMetadata(actualModel)
	levels := normalizeReasoningLevels(metadata.SupportedReasoningLevels)
	for _, path := range []string{"reasoning.effort", "reasoning_effort"} {
		original := gjson.GetBytes(body, path)
		if !original.Exists() || original.Type != gjson.String || strings.TrimSpace(original.String()) == "" {
			continue
		}
		requested := strings.TrimSpace(original.String())
		target := strings.TrimSpace(gjson.GetBytes(effective, path).String())
		supported := false
		for _, level := range levels {
			if target == level {
				supported = true
				break
			}
		}
		needsEvidence := target != requested || requested == "none" || requested == "minimal" || len(levels) > 0 || (known && metadata.Reasoning != nil && !*metadata.Reasoning)
		if needsEvidence && !supported {
			logger.FromContext(ctx).Warn("openai.reasoning_effort_unsupported", zap.String("requested", requested), zap.String("effective", target), zap.String("model", actualModel))
			return body, &OpenAIReasoningEffortUnsupportedError{Requested: requested, Effective: target, Model: actualModel}
		}
		if target != requested {
			logger.FromContext(ctx).Info("openai.reasoning_effort_mapped", zap.String("requested", requested), zap.String("effective", target), zap.String("model", actualModel))
		}
	}
	if !changed {
		return body, nil
	}
	effective, err = sjson.SetBytes(effective, "model", requestedModel)
	if err != nil {
		return body, err
	}
	return effective, nil
}
