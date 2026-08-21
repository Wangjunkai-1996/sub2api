package service

import "context"

type openAIForwardModelContextKey struct{}

type openAIForwardModelResolution uint8

const (
	openAIForwardModelResolutionResponses openAIForwardModelResolution = iota
	openAIForwardModelResolutionDirect
	openAIForwardModelResolutionMessages
)

type openAIForwardModel struct {
	model                  string
	defaultMappedModel     string
	useCompactModelMapping bool
	resolution             openAIForwardModelResolution
}

// WithOpenAIForwardModel records the model present in the forwarded request
// body after channel mapping and whether the legacy /responses/compact-only
// model mapping applies. Native remote compaction v2 keeps this false, so
// channel restriction checks follow the same model chain used by Forward.
func WithOpenAIForwardModel(ctx context.Context, forwardModel string, useCompactModelMapping bool) context.Context {
	return withOpenAIForwardModel(ctx, openAIForwardModel{
		model:                  forwardModel,
		useCompactModelMapping: useCompactModelMapping,
		resolution:             openAIForwardModelResolutionResponses,
	})
}

// WithOpenAIDirectForwardModel records the channel-mapped model used by Chat
// Completions and Responses WebSocket forwarding. Both paths apply the normal
// account model mapping before upstream normalization.
func WithOpenAIDirectForwardModel(ctx context.Context, forwardModel string) context.Context {
	return withOpenAIForwardModel(ctx, openAIForwardModel{
		model:      forwardModel,
		resolution: openAIForwardModelResolutionDirect,
	})
}

// WithOpenAIMessagesForwardModel records the channel-mapped Messages body model
// together with the group dispatch fallback used when no account mapping
// matches.
func WithOpenAIMessagesForwardModel(ctx context.Context, forwardModel, defaultMappedModel string) context.Context {
	return withOpenAIForwardModel(ctx, openAIForwardModel{
		model:              forwardModel,
		defaultMappedModel: defaultMappedModel,
		resolution:         openAIForwardModelResolutionMessages,
	})
}

func withOpenAIForwardModel(ctx context.Context, forwardModel openAIForwardModel) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, openAIForwardModelContextKey{}, forwardModel)
}

func openAIForwardModelFromContext(ctx context.Context) (openAIForwardModel, bool) {
	if ctx == nil {
		return openAIForwardModel{}, false
	}
	forwardModel, ok := ctx.Value(openAIForwardModelContextKey{}).(openAIForwardModel)
	return forwardModel, ok
}

// resolveOpenAISecurityUpstreamModel mirrors the model chain used by the
// corresponding forwarder without parsing or copying the request body.
func resolveOpenAISecurityUpstreamModel(ctx context.Context, account *Account) (string, bool) {
	forwardModel, ok := openAIForwardModelFromContext(ctx)
	if !ok {
		return "", false
	}
	switch forwardModel.resolution {
	case openAIForwardModelResolutionDirect:
		model := resolveOpenAIForwardModel(account, forwardModel.model, "")
		return normalizeOpenAIModelForUpstream(account, model), true
	case openAIForwardModelResolutionMessages:
		model := NormalizeOpenAICompatRequestedModel(forwardModel.model)
		model = resolveOpenAIForwardModel(account, model, forwardModel.defaultMappedModel)
		return normalizeOpenAIModelForUpstream(account, model), true
	default:
		return resolveOpenAIAccountUpstreamModelForRequest(
			account,
			forwardModel.model,
			forwardModel.useCompactModelMapping,
		), true
	}
}
