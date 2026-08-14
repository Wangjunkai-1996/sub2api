package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
	"github.com/Wei-Shaw/sub2api/internal/pkg/apicompat"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const (
	openAIResponsesLineageCaptureContextKey = "sub2api.openai.strict_lineage_capture"
	openAIResponsesLineageCommitContextKey  = "sub2api.openai.strict_lineage_commit"
	// Stored-response lineage keeps a tighter output cap than request auditing.
	// Explicit store=false full-history requests never enable this capture path.
	openAIResponsesLineageOutputMaxBytes = 2 * 1024 * 1024
)

// OpenAIStrictLineageCommitter persists one successful strict-audit turn before
// its success terminal is exposed to the client. turn is 1 for HTTP requests.
type OpenAIStrictLineageCommitter func(turn int, result *OpenAIForwardResult) error

// OpenAIStrictLineageCommitError is a local fail-closed error. Callers must not
// retry the upstream request because the upstream already completed it.
type OpenAIStrictLineageCommitError struct {
	cause error
}

func (e *OpenAIStrictLineageCommitError) Error() string {
	if e == nil || e.cause == nil {
		return "strict audit lineage commit failed"
	}
	return fmt.Sprintf("strict audit lineage commit failed: %v", e.cause)
}

func (e *OpenAIStrictLineageCommitError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// SetOpenAIStrictLineageCommitter installs the request/turn-scoped persistence
// gate. Capture-only unit tests intentionally remain possible without a gate.
func SetOpenAIStrictLineageCommitter(c *gin.Context, committer OpenAIStrictLineageCommitter) {
	if c != nil && committer != nil {
		c.Set(openAIResponsesLineageCommitContextKey, committer)
	}
}

// ClearOpenAIStrictLineage removes request-attempt state before an ineligible
// account (notably APIKey) is forwarded after an audited OAuth attempt failed.
func ClearOpenAIStrictLineage(c *gin.Context) {
	if c == nil {
		return
	}
	c.Set(openAIResponsesLineageCaptureContextKey, false)
	c.Set(openAIResponsesLineageCommitContextKey, nil)
}

func openAIStrictLineageCommitRequired(c *gin.Context) bool {
	if c == nil {
		return false
	}
	value, exists := c.Get(openAIResponsesLineageCommitContextKey)
	if !exists {
		return false
	}
	_, ok := value.(OpenAIStrictLineageCommitter)
	return ok
}

func commitOpenAIStrictLineageBeforeSuccess(c *gin.Context, turn int, result *OpenAIForwardResult) error {
	if c == nil {
		return nil
	}
	value, exists := c.Get(openAIResponsesLineageCommitContextKey)
	if !exists {
		return nil
	}
	if value == nil {
		return nil
	}
	committer, ok := value.(OpenAIStrictLineageCommitter)
	if !ok || committer == nil {
		return &OpenAIStrictLineageCommitError{cause: fmt.Errorf("lineage committer is unavailable")}
	}
	if turn <= 0 {
		turn = 1
	}
	if err := committer(turn, result); err != nil {
		return &OpenAIStrictLineageCommitError{cause: err}
	}
	return nil
}

func commitOpenAIStrictLineageFields(c *gin.Context, turn int, responseID, terminalEvent string, output []byte, complete bool) error {
	if !openAIStrictLineageCommitRequired(c) {
		return nil
	}
	result := &OpenAIForwardResult{
		ResponseID:            strings.TrimSpace(responseID),
		UpstreamTerminalEvent: strings.TrimSpace(terminalEvent),
	}
	result.setOpenAIResponsesLineageOutput(output, complete)
	return commitOpenAIStrictLineageBeforeSuccess(c, turn, result)
}

// EnableOpenAIStrictLineageCapture enables response output capture only after
// strict request admission produced an AuditSummary.
func EnableOpenAIStrictLineageCapture(c *gin.Context) {
	if c != nil {
		c.Set(openAIResponsesLineageCaptureContextKey, true)
	}
}

func openAIResponsesLineageCaptureEnabled(c *gin.Context) bool {
	if c == nil {
		return false
	}
	enabled, exists := c.Get(openAIResponsesLineageCaptureContextKey)
	return exists && enabled == true
}

func extractOpenAIResponsesLineageOutput(payload []byte, expectedResponseID string) (json.RawMessage, bool) {
	if len(payload) == 0 || !gjson.ValidBytes(payload) || auditinput.HasDuplicateJSONFields(payload) {
		return nil, false
	}
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	response := gjson.GetBytes(payload, "response")
	status := ""
	responseID := ""
	output := gjson.Result{}
	switch eventType {
	case "response.completed", "response.done":
		if !response.Exists() || response.Type != gjson.JSON {
			return nil, false
		}
		status = strings.TrimSpace(response.Get("status").String())
		responseID = strings.TrimSpace(response.Get("id").String())
		output = response.Get("output")
	case "", "response":
		// A non-streaming Responses object is the response itself. Event
		// envelopes must use the nested response field above; accepting a
		// completed event with top-level id/output would bless a malformed frame.
		if response.Exists() {
			return nil, false
		}
		status = strings.TrimSpace(gjson.GetBytes(payload, "status").String())
		responseID = strings.TrimSpace(gjson.GetBytes(payload, "id").String())
		output = gjson.GetBytes(payload, "output")
	default:
		return nil, false
	}
	if responseID == "" || (strings.TrimSpace(expectedResponseID) != "" && responseID != strings.TrimSpace(expectedResponseID)) {
		return nil, false
	}
	if status != "completed" && status != "done" {
		return nil, false
	}
	if !output.Exists() || !output.IsArray() {
		return nil, false
	}
	raw := bytes.TrimSpace([]byte(output.Raw))
	if len(raw) == 0 || len(raw) > openAIResponsesLineageOutputMaxBytes || !json.Valid(raw) {
		return nil, false
	}
	return append(json.RawMessage(nil), raw...), true
}

func (r *OpenAIForwardResult) captureOpenAIResponsesLineageOutput(c *gin.Context, payload []byte) {
	if r == nil || !openAIResponsesLineageCaptureEnabled(c) {
		return
	}
	output, complete := extractOpenAIResponsesLineageOutput(payload, r.ResponseID)
	r.responsesLineageOutput = output
	r.responsesLineageOutputComplete = complete
}

func extractSingleOpenAIResponsesSuccessTerminal(body string, expectedResponseID string) ([]byte, bool) {
	var terminal []byte
	count := 0
	forEachOpenAISSEDataPayload(body, func(data []byte) {
		eventType := strings.TrimSpace(gjson.GetBytes(data, "type").String())
		if !isOpenAIWSTerminalEvent(eventType) {
			return
		}
		count++
		if count == 1 && (eventType == "response.completed" || eventType == "response.done") {
			terminal = append([]byte(nil), data...)
		}
	})
	if count != 1 || len(terminal) == 0 {
		return nil, false
	}
	output, complete := extractOpenAIResponsesLineageOutput(terminal, expectedResponseID)
	return output, complete
}

// openAIResponsesLineageAccumulator rebuilds the complete model-visible output
// for transports whose success terminal legitimately contains output: [].
type openAIResponsesLineageAccumulator struct {
	output      *apicompat.BufferedResponseAccumulator
	collector   *responsesStreamOutputCollector
	imageOutput []json.RawMessage
	seenImages  map[string]struct{}
}

func newOpenAIResponsesLineageAccumulator() *openAIResponsesLineageAccumulator {
	return &openAIResponsesLineageAccumulator{
		output:     apicompat.NewBufferedResponseAccumulator(),
		collector:  newResponsesStreamOutputCollector(),
		seenImages: make(map[string]struct{}),
	}
}

func (a *openAIResponsesLineageAccumulator) Observe(payload []byte) {
	if a == nil || len(payload) == 0 {
		return
	}
	a.collector.Observe(payload)
	if imageOutput, ok := extractImageGenerationOutputFromSSEData(payload, a.seenImages); ok {
		a.imageOutput = append(a.imageOutput, imageOutput)
	}
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	if !responsesStreamEventMayContributeToOutput(eventType) {
		return
	}
	var event apicompat.ResponsesStreamEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		a.collector.complete = false
		return
	}
	a.output.ProcessEvent(&event)
}

func (a *openAIResponsesLineageAccumulator) SuccessTerminalOutput(payload []byte, expectedResponseID string) ([]byte, bool) {
	if a == nil {
		return extractOpenAIResponsesLineageOutput(payload, expectedResponseID)
	}
	lineagePayload, complete := normalizeResponsesStreamingLineageTerminalOutput(
		payload,
		a.output,
		a.imageOutput,
		a.collector,
	)
	if !complete || !a.collector.Complete() {
		return nil, false
	}
	return extractOpenAIResponsesLineageOutput(lineagePayload, expectedResponseID)
}

// CaptureOpenAIResponsesLineageOutput is exported for protocol adapters and
// contract tests that already hold the complete terminal response payload.
func (r *OpenAIForwardResult) CaptureOpenAIResponsesLineageOutput(c *gin.Context, payload []byte) {
	r.captureOpenAIResponsesLineageOutput(c, payload)
}

func (r *OpenAIForwardResult) setOpenAIResponsesLineageOutput(output []byte, complete bool) {
	if r == nil {
		return
	}
	r.responsesLineageOutputComplete = complete
	if complete {
		r.responsesLineageOutput = append(json.RawMessage(nil), output...)
	} else {
		r.responsesLineageOutput = nil
	}
}

// OpenAIResponsesLineageOutput returns an immutable copy of the captured
// model-visible output array.
func (r *OpenAIForwardResult) OpenAIResponsesLineageOutput() ([]byte, bool) {
	if r == nil || !r.responsesLineageOutputComplete || len(r.responsesLineageOutput) == 0 {
		return nil, false
	}
	return append([]byte(nil), r.responsesLineageOutput...), true
}
