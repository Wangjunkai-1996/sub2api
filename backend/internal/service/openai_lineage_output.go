package service

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

const (
	openAIResponsesLineageCaptureContextKey = "sub2api.openai.strict_lineage_capture"
	// Response media is not accepted into lineage, so 2 MiB safely covers the
	// 65,536-rune text limit plus JSON escaping and item metadata while bounding
	// per-turn copies under Pro concurrency.
	openAIResponsesLineageOutputMaxBytes = 2 * 1024 * 1024
)

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
