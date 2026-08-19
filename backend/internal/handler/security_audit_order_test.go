package handler

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type promptAuditOrderCase struct {
	file       string
	function   string
	auditToken string
}

func TestPromptAuditGatePrecedesAccountBillingAndUpstreamSideEffects(t *testing.T) {
	tests := []promptAuditOrderCase{
		{file: "gateway_handler.go", function: "Messages", auditToken: "checkSecurityAudit"},
		{file: "gateway_handler_chat_completions.go", function: "ChatCompletions", auditToken: "checkSecurityAudit"},
		{file: "gateway_handler_responses.go", function: "Responses", auditToken: "checkSecurityAudit"},
		{file: "gemini_v1beta_handler.go", function: "GeminiV1BetaModels", auditToken: "checkSecurityAudit"},
		{file: "grok_media.go", function: "handleGrokMedia", auditToken: "checkSecurityAudit"},
	}
	sideEffectTokens := []string{
		"CheckBillingEligibility(", "SelectAccount", ".Forward", "acquireResponsesUserSlot(",
		"AcquireUserSlot", "TryAcquireUserSlot", "acquireImageGenerationSlot(",
		"h.tasks.Create(", "h.service.Submit(",
	}
	for _, tt := range tests {
		t.Run(tt.file+"/"+tt.function, func(t *testing.T) {
			functionSource := stripGoComments(goFunctionSource(t, tt.file, tt.function))
			auditIndex := strings.Index(functionSource, tt.auditToken)
			require.NotEqual(t, -1, auditIndex, "missing Prompt Audit gate")
			foundSideEffect := false
			for _, sideEffect := range sideEffectTokens {
				index := strings.Index(functionSource, sideEffect)
				if index < 0 {
					continue
				}
				foundSideEffect = true
				require.Lessf(t, auditIndex, index, "%s must run before %s", tt.auditToken, sideEffect)
			}
			require.True(t, foundSideEffect, "coverage case must contain a downstream side effect")
		})
	}
}

func TestOpenAISelectedAccountAuditRunsAfterSlotBeforeForward(t *testing.T) {
	tests := []struct {
		file         string
		function     string
		forwardToken string
	}{
		{file: "openai_gateway_handler.go", function: "Responses", forwardToken: "h.gatewayService.Forward("},
		{file: "openai_gateway_handler.go", function: "Messages", forwardToken: "h.gatewayService.ForwardAsAnthropic("},
		{file: "openai_chat_completions.go", function: "ChatCompletions", forwardToken: "h.gatewayService.ForwardAsChatCompletions("},
	}
	for _, tt := range tests {
		t.Run(tt.file+"/"+tt.function, func(t *testing.T) {
			source := stripGoComments(goFunctionSource(t, tt.file, tt.function))
			selectionIndex := strings.Index(source, "SelectAccountWithSchedulerForCapability(")
			slotIndex := strings.Index(source, "acquireResponsesAccountSlot(")
			slotSuccessIndex := strings.Index(source, "if slotResult != openAISlotAcquireOK")
			refreshedAccountIndex := -1
			if slotSuccessIndex >= 0 {
				if offset := strings.Index(source[slotSuccessIndex:], "account = selection.Account"); offset >= 0 {
					refreshedAccountIndex = slotSuccessIndex + offset
				}
			}
			auditIndex := strings.Index(source, "checkSecurityAuditForSelectedOpenAIProAccount(")
			forwardIndex := strings.Index(source, tt.forwardToken)

			require.NotEqual(t, -1, selectionIndex, "missing final account selection")
			require.NotEqual(t, -1, slotIndex, "missing account concurrency acquisition")
			require.NotEqual(t, -1, slotSuccessIndex, "missing successful account-slot gate")
			require.NotEqual(t, -1, refreshedAccountIndex, "missing final account snapshot refresh")
			require.NotEqual(t, -1, auditIndex, "missing selected-account Prompt Audit gate")
			require.NotEqual(t, -1, forwardIndex, "missing upstream forward")
			require.Less(t, selectionIndex, slotIndex, "account selection must precede slot acquisition")
			require.Less(t, slotIndex, slotSuccessIndex, "slot acquisition must precede its success gate")
			require.Less(t, slotSuccessIndex, refreshedAccountIndex, "final account snapshot must be read only after the account slot is held")
			require.Less(t, refreshedAccountIndex, auditIndex, "selected-account audit must use the final account snapshot")
			require.Less(t, auditIndex, forwardIndex, "selected-account audit must run before the first upstream forward")
		})
	}
}

func TestOtherOpenAIEntryPointsDoNotRunPromptAudit(t *testing.T) {
	tests := []struct {
		file     string
		function string
	}{
		{file: "openai_images.go", function: "Images"},
		{file: "openai_embeddings.go", function: "Embeddings"},
		{file: "openai_alpha_search.go", function: "AlphaSearch"},
		{file: "openai_gateway_handler.go", function: "ResponsesWebSocket"},
		{file: "image_task_handler.go", function: "Submit"},
		{file: "batch_image_handler.go", function: "Submit"},
	}
	for _, tt := range tests {
		t.Run(tt.file+"/"+tt.function, func(t *testing.T) {
			source := stripGoComments(goFunctionSource(t, tt.file, tt.function))
			for _, forbidden := range []string{
				"checkSecurityAudit",
				"checkSecurityAuditForSelectedOpenAIProAccount",
				"ensureSecurityAuditForAccount",
				"newOpenAIAccountAuditState",
			} {
				require.NotContains(t, source, forbidden)
			}
		})
	}
}

func stripGoComments(source string) string {
	source = regexp.MustCompile(`(?s)/\*.*?\*/`).ReplaceAllString(source, "")
	return regexp.MustCompile(`(?m)//.*$`).ReplaceAllString(source, "")
}

func goFunctionSource(t *testing.T, filename, functionName string) string {
	t.Helper()
	raw, err := os.ReadFile(filename)
	require.NoError(t, err)
	files := token.NewFileSet()
	parsed, err := parser.ParseFile(files, filename, raw, 0)
	require.NoError(t, err)
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != functionName || function.Body == nil {
			continue
		}
		start := files.Position(function.Pos()).Offset
		end := files.Position(function.End()).Offset
		require.Greater(t, end, start)
		return string(raw[start:end])
	}
	t.Fatalf("function %s not found in %s", functionName, filename)
	return ""
}
