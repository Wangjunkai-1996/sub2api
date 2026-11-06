package service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
)

type OpenAIRecoveryWireFormat uint8

const (
	OpenAIRecoveryResponsesSSE OpenAIRecoveryWireFormat = iota + 1
	OpenAIRecoveryChatSSE
	OpenAIRecoveryResponsesJSON
	OpenAIRecoveryChatJSON
)

const openAIRecoveryMaxPendingBytes = 8 << 20

type openAIRecoveryControl struct {
	Type          string          `json:"type"`
	Kind          string          `json:"kind"`
	LogicalID     string          `json:"logical_id"`
	Seq           int             `json:"seq"`
	Attempt       int             `json:"attempt"`
	Outcome       string          `json:"outcome,omitempty"`
	UsageFormat   string          `json:"usage_format,omitempty"`
	UsageReported *bool           `json:"usage_reported,omitempty"`
	Usage         json.RawMessage `json:"usage,omitempty"`
}

// OpenAIRecoveryStreamWriter owns the public response for all physical attempts
// of one managed request. The ordinary body is forwarded as frames arrive;
// attempt terminal frames remain private until CommitAttempt.
type OpenAIRecoveryStreamWriter struct {
	gin.ResponseWriter
	writeMu               sync.Mutex
	mode                  OpenAIRecoveryWireFormat
	logicalID             string
	publish               func(string) error
	request               []byte
	attempt               int
	abandoned             bool
	seq                   int
	status                int
	started               bool
	visible               bool
	publicTool            bool
	disconnect            bool
	ended                 bool
	protocolErr           error
	created               bool
	finished              bool
	incomplete            bool
	done                  bool
	pending               bytes.Buffer
	pendingScan           int
	terminal              []byte
	terminalEvent         map[string]any
	controls              [][]byte
	usage                 json.RawMessage
	winnerOutput          json.RawMessage
	actualID              string
	outcome               string
	publicSeq             int
	nextIndex             int
	indexes               map[int]int
	itemIDs               map[string]string
	textItems             map[int]*openAIRecoveryTextItem
	display               []map[string]any
	displayIDs            map[string]int
	displaySizes          map[string]int
	displayIndexes        map[string]int
	displayBytes          int
	chatModel             string
	choices               map[int]bool
	roles                 map[int]bool
	expectedChoices       int
	finishedChoices       map[int]bool
	heldItems             map[int]map[string]any
	heldItemIDs           map[string]int
	publicToolItems       map[int]bool
	publicToolInput       map[int]string
	publicToolInputDone   map[int]bool
	publicToolItemDone    map[int]bool
	heldChatTools         map[string]*openAIRecoveryChatTool
	heldBytes             int
	releaseItems          []map[string]any
	structured            bool
	committing            bool
	structuredOutput      bytes.Buffer
	structuredText        map[int][]byte
	structuredAlternative map[int]bool
	reasoningItems        map[string]*openAIRecoveryReasoningItem
}

type openAIRecoveryReasoningItem struct {
	id    string
	index int
	parts map[int]*openAIRecoveryReasoningPart
	done  bool
}

type openAIRecoveryReasoningPart struct {
	text     []byte
	textDone bool
	partDone bool
}

type openAIRecoveryChatTool struct {
	choice    int
	id        string
	name      string
	arguments strings.Builder
}

type openAIRecoveryTextItem struct {
	id       string
	index    int
	item     map[string]any
	parts    map[int]*openAIRecoveryTextPart
	itemDone bool
}

type openAIRecoveryTextPart struct {
	index    int
	text     []byte
	part     map[string]any
	textDone bool
	partDone bool
}

func (item *openAIRecoveryTextItem) content() []any {
	indexes := make([]int, 0, len(item.parts))
	for index := range item.parts {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	content := make([]any, 0, len(indexes))
	for _, index := range indexes {
		state := item.parts[index]
		part := make(map[string]any, len(state.part)+1)
		for key, value := range state.part {
			part[key] = value
		}
		if part["type"] == "refusal" {
			delete(part, "text")
			delete(part, "annotations")
			part["refusal"] = string(state.text)
		} else {
			part["text"] = string(state.text)
		}
		content = append(content, part)
	}
	return content
}

func NewOpenAIRecoveryStreamWriter(base gin.ResponseWriter, mode OpenAIRecoveryWireFormat, logicalID string, requestBody []byte, publish func(string) error) *OpenAIRecoveryStreamWriter {
	var request struct {
		N int `json:"n"`
	}
	_ = json.Unmarshal(requestBody, &request)
	if request.N < 1 {
		request.N = 1
	}
	return &OpenAIRecoveryStreamWriter{ResponseWriter: base, mode: mode, logicalID: logicalID, publish: publish, request: requestBody, publicSeq: -1, displayIDs: make(map[string]int), displaySizes: make(map[string]int), displayIndexes: make(map[string]int), choices: make(map[int]bool), roles: make(map[int]bool), expectedChoices: request.N, structured: OpenAIRecoveryStructuredFormat(requestBody, mode) != ""}
}

func (w *OpenAIRecoveryStreamWriter) BeginAttempt(n int) error {
	if w.ended || w.disconnect {
		return errors.New("OpenAI recovery response cannot begin another attempt")
	}
	if w.publicTool {
		return errors.New("OpenAI recovery cannot retry after a public tool call")
	}
	if n <= w.attempt || n < 1 {
		return errors.New("OpenAI recovery attempt must increase monotonically")
	}
	if w.attempt > 0 && !w.abandoned {
		if err := w.AbandonAttempt(); err != nil {
			w.protocolErr = err
			return err
		}
	}
	w.attempt = n
	w.abandoned = false
	w.pending.Reset()
	w.pendingScan = 0
	w.terminal = nil
	w.terminalEvent = nil
	w.usage = nil
	w.winnerOutput = nil
	w.actualID = ""
	w.outcome = ""
	w.protocolErr = nil
	w.finished = false
	w.incomplete = false
	w.done = false
	w.indexes = make(map[int]int)
	w.itemIDs = make(map[string]string)
	w.textItems = make(map[int]*openAIRecoveryTextItem)
	w.finishedChoices = make(map[int]bool)
	w.heldItems = make(map[int]map[string]any)
	w.heldItemIDs = make(map[string]int)
	w.publicToolItems = make(map[int]bool)
	w.publicToolInput = make(map[int]string)
	w.publicToolInputDone = make(map[int]bool)
	w.publicToolItemDone = make(map[int]bool)
	w.heldChatTools = make(map[string]*openAIRecoveryChatTool)
	w.heldBytes = 0
	w.releaseItems = nil
	w.reasoningItems = make(map[string]*openAIRecoveryReasoningItem)
	w.structuredOutput.Reset()
	w.structuredText = make(map[int][]byte)
	w.structuredAlternative = make(map[int]bool)
	w.visible = false
	w.choices = make(map[int]bool)
	if w.structured {
		w.display = nil
		w.displayIDs = make(map[string]int)
		w.displaySizes = make(map[string]int)
		w.displayIndexes = make(map[string]int)
		w.displayBytes = 0
		w.publicSeq = -1
		w.nextIndex = 0
		w.created = false
		w.roles = make(map[int]bool)
	}
	w.status = 0
	if w.mode == OpenAIRecoveryResponsesSSE || w.mode == OpenAIRecoveryChatSSE {
		w.queueControl("attempt_begin", "")
	}
	return w.protocolErr
}

func (w *OpenAIRecoveryStreamWriter) AbandonAttempt() error {
	if w.attempt == 0 || w.abandoned {
		return nil
	}
	if w.publicTool {
		return errors.New("OpenAI recovery cannot abandon a public tool call")
	}
	w.abandoned = true
	if w.mode == OpenAIRecoveryResponsesSSE || w.mode == OpenAIRecoveryChatSSE {
		w.queueControl("attempt_abandoned", "")
		for _, item := range w.reasoningItems {
			if !item.done {
				if err := w.closeRecoveryReasoning(item, nil); err != nil {
					return err
				}
			}
		}
		if !w.structured {
			indexes := make([]int, 0, len(w.textItems))
			for index := range w.textItems {
				indexes = append(indexes, index)
			}
			sort.Ints(indexes)
			for _, index := range indexes {
				item := w.textItems[index]
				if item.itemDone {
					continue
				}
				partIndexes := make([]int, 0, len(item.parts))
				for partIndex := range item.parts {
					partIndexes = append(partIndexes, partIndex)
				}
				sort.Ints(partIndexes)
				for _, partIndex := range partIndexes {
					part := item.parts[partIndex]
					field, eventName := "text", "response.output_text"
					if part.part["type"] == "refusal" {
						field, eventName = "refusal", "response.refusal"
					}
					part.part[field] = string(part.text)
					if !part.textDone {
						if err := w.writeResponsesEvent(map[string]any{"type": eventName + ".done", "output_index": item.index, "content_index": partIndex, "item_id": item.id, field: string(part.text)}); err != nil {
							return err
						}
						part.textDone = true
					}
					if !part.partDone {
						if err := w.writeResponsesEvent(map[string]any{"type": "response.content_part.done", "output_index": item.index, "content_index": partIndex, "item_id": item.id, "part": part.part}); err != nil {
							return err
						}
						part.partDone = true
					}
				}
				item.item["content"] = item.content()
				item.item["status"] = "completed"
				if err := w.writeResponsesEvent(map[string]any{"type": "response.output_item.done", "output_index": item.index, "item": item.item}); err != nil {
					return err
				}
				item.itemDone = true
				if err := w.rememberDisplayItem(item.item); err != nil {
					return err
				}
			}
		}
		if w.visible && !w.structured {
			if err := w.emitRegenerationMarker(); err != nil {
				return err
			}
		}
	}
	w.pending.Reset()
	w.pendingScan = 0
	w.terminal = nil
	w.usage = nil
	w.actualID = ""
	return nil
}

func (w *OpenAIRecoveryStreamWriter) CommitAttempt(actualResponseID string) error {
	if w.disconnect {
		return http.ErrAbortHandler
	}
	if w.ended {
		return errors.New("OpenAI recovery response already ended")
	}
	if w.protocolErr != nil {
		if strings.Contains(w.protocolErr.Error(), "capacity exceeded") || strings.Contains(w.protocolErr.Error(), "reserved OpenAI recovery control") {
			return w.protocolErr
		}
		return openAIRecoveryInvalidAttempt(w.protocolErr.Error())
	}
	responses := w.mode == OpenAIRecoveryResponsesSSE || w.mode == OpenAIRecoveryResponsesJSON
	if w.mode == OpenAIRecoveryResponsesJSON || w.mode == OpenAIRecoveryChatJSON {
		if err := w.prepareRecoveryJSON(); err != nil {
			return err
		}
	}
	if responses && actualResponseID == "" && w.mode == OpenAIRecoveryResponsesJSON {
		var body struct {
			ID string `json:"id"`
		}
		if json.Unmarshal(w.pending.Bytes(), &body) == nil {
			w.actualID = body.ID
		}
	}
	if actualResponseID == "" {
		actualResponseID = w.actualID
	}
	if responses && actualResponseID == "" {
		return openAIRecoveryInvalidAttempt("OpenAI recovery winner has no actual response ID")
	}
	if responses && w.actualID != "" && actualResponseID != w.actualID {
		return openAIRecoveryInvalidAttempt("OpenAI recovery winner response identity is inconsistent")
	}
	if w.mode == OpenAIRecoveryResponsesSSE || w.mode == OpenAIRecoveryChatSSE {
		if len(w.terminal) == 0 || (w.outcome != "completed" && w.outcome != "incomplete") || (w.mode == OpenAIRecoveryChatSSE && (!w.finished || !w.done || len(w.finishedChoices) != w.expectedChoices)) {
			return openAIRecoveryInvalidAttempt("OpenAI recovery attempt has no complete successful terminal sequence")
		}
		for _, tool := range w.heldChatTools {
			if tool.id == "" || tool.name == "" || !json.Valid([]byte(tool.arguments.String())) {
				return openAIRecoveryInvalidAttempt("OpenAI recovery Chat tool call is incomplete")
			}
			if err := ValidateOpenAIRecoveryToolArguments(w.request, tool.name, tool.arguments.String()); err != nil {
				return openAIRecoveryInvalidAttempt("OpenAI recovery tool arguments failed validation")
			}
			w.structuredAlternative[tool.choice] = true
		}
		validationItems := append([]map[string]any(nil), w.releaseItems...)
		for index := range w.publicToolItems {
			validationItems = append(validationItems, w.heldItems[index])
		}
		for _, item := range validationItems {
			if err := validateRecoveryItem(item); err != nil {
				return openAIRecoveryInvalidAttempt(err.Error())
			}
			if item["type"] == "function_call" {
				name, _ := item["name"].(string)
				arguments, _ := item["arguments"].(string)
				if err := ValidateOpenAIRecoveryToolArguments(w.request, name, arguments); err != nil {
					return openAIRecoveryInvalidAttempt("OpenAI recovery tool arguments failed validation")
				}
			}
			if item["type"] == "function_call" || item["type"] == "custom_tool_call" || item["type"] == "tool_search_call" {
				w.structuredAlternative[0] = true
			}
			if id, _ := item["id"].(string); item["type"] == "reasoning" && w.reasoningItems[id] != nil {
				state := w.reasoningItems[id]
				summary, _ := item["summary"].([]any)
				for index, raw := range summary {
					part, _ := raw.(map[string]any)
					text, _ := part["text"].(string)
					if tracked := state.parts[index]; tracked != nil {
						if !strings.HasPrefix(text, string(tracked.text)) || ((tracked.textDone || tracked.partDone) && text != string(tracked.text)) {
							return openAIRecoveryInvalidAttempt("OpenAI recovery reasoning snapshot differs from streamed summary")
						}
					}
				}
			}
		}
		if len(w.usage) > 0 {
			var usage map[string]any
			if json.Unmarshal(w.usage, &usage) != nil {
				return openAIRecoveryInvalidAttempt("OpenAI recovery usage is invalid")
			}
			fields := []string{"input_tokens", "output_tokens", "total_tokens"}
			if w.mode == OpenAIRecoveryChatSSE {
				fields = []string{"prompt_tokens", "completion_tokens", "total_tokens"}
			}
			for _, field := range fields {
				if _, ok := recoveryInteger(usage[field]); !ok {
					return openAIRecoveryInvalidAttempt("OpenAI recovery usage is incomplete")
				}
			}
		}
		if w.structured {
			units := 1
			if w.mode == OpenAIRecoveryChatSSE {
				units = w.expectedChoices
			}
			for index := 0; index < units; index++ {
				if w.outcome == "completed" && len(w.structuredText[index]) == 0 && !w.structuredAlternative[index] {
					return openAIRecoveryInvalidAttempt("OpenAI recovery structured output is empty")
				}
			}
			for _, text := range w.structuredText {
				if err := ValidateOpenAIRecoveryStructuredOutput(w.request, w.mode, string(text)); err != nil {
					if w.outcome == "incomplete" {
						w.structuredOutput.Reset()
						w.display = nil
						w.publicSeq = -1
						w.created = false
						break
					}
					return openAIRecoveryInvalidAttempt("OpenAI recovery structured output failed validation")
				}
			}
		}
	} else if w.pending.Len() == 0 || w.status >= http.StatusBadRequest {
		return errors.New("OpenAI recovery JSON attempt has no successful body")
	}
	if w.publish != nil {
		if err := w.publish(actualResponseID); err != nil {
			_ = w.FailAttempt("api_error", "alias_publish_failed", "OpenAI recovery continuation is unavailable")
			return err
		}
	}
	if w.mode == OpenAIRecoveryResponsesJSON || w.mode == OpenAIRecoveryChatJSON {
		w.committing = true
		return w.flushJSON()
	}
	w.committing = true
	if err := w.flushControls(); err != nil {
		return err
	}
	if w.structuredOutput.Len() > 0 {
		if _, err := w.writePublic(w.structuredOutput.Bytes()); err != nil {
			return err
		}
		w.structuredOutput.Reset()
	}
	if w.mode == OpenAIRecoveryResponsesSSE && !w.created {
		if err := w.writeResponsesEvent(map[string]any{"type": "response.created", "response": map[string]any{"id": w.logicalID, "object": "response", "status": "in_progress", "output": []any{}}}); err != nil {
			return err
		}
		w.created = true
	}
	for _, item := range w.releaseItems {
		if err := w.releaseResponseItem(item); err != nil {
			return err
		}
	}
	if w.terminalEvent != nil {
		sort.SliceStable(w.display, func(i, j int) bool {
			left, _ := w.display[i]["id"].(string)
			right, _ := w.display[j]["id"].(string)
			return w.displayIndexes[left] < w.displayIndexes[right]
		})
		if response, ok := w.terminalEvent["response"].(map[string]any); ok {
			response["output"] = w.display
		}
		frame, err := w.encodeResponsesEvent(w.terminalEvent)
		if err != nil {
			return err
		}
		w.terminal = frame
	}
	w.queueTerminal(w.outcome)
	if err := w.flushControls(); err != nil {
		return err
	}
	_, err := w.writePublic(w.terminal)
	w.terminal = nil
	w.ended = err == nil
	if err == nil {
		w.ResponseWriter.Flush()
	}
	return err
}

func (w *OpenAIRecoveryStreamWriter) FailAttempt(errType, code, message string) error {
	if w.ended {
		return nil
	}
	if w.disconnect {
		return nil
	}
	w.committing = true
	if !w.started {
		var pendingError struct {
			Error struct {
				Type    string `json:"type"`
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if json.Unmarshal(w.pending.Bytes(), &pendingError) == nil && pendingError.Error.Message != "" {
			errType = pendingError.Error.Type
			code = pendingError.Error.Code
			message = pendingError.Error.Message
		}
	}
	w.pending.Reset()
	w.pendingScan = 0
	w.terminal = nil
	if w.mode == OpenAIRecoveryResponsesJSON || w.mode == OpenAIRecoveryChatJSON || !w.started {
		body, err := json.Marshal(gin.H{"error": gin.H{"type": errType, "code": code, "message": message}})
		if err != nil {
			return err
		}
		w.ResponseWriter.Header().Set("Content-Type", "application/json")
		if w.status < http.StatusBadRequest {
			w.status = http.StatusBadGateway
		}
		_, err = w.writePublic(body)
		w.ended = err == nil
		return err
	}
	w.queueTerminal("failed")
	if err := w.flushControls(); err != nil {
		return err
	}
	body, err := json.Marshal(gin.H{"error": gin.H{"type": errType, "code": code, "message": message}})
	if err != nil {
		return err
	}
	if w.mode == OpenAIRecoveryResponsesSSE {
		w.publicSeq++
		failure := map[string]any{"type": "response.failed", "sequence_number": w.publicSeq, "response": map[string]any{"id": w.logicalID, "object": "response", "status": "failed", "output": w.display, "error": map[string]any{"type": errType, "code": code, "message": message}}}
		body, err = json.Marshal(failure)
		if err != nil {
			return err
		}
		_, err = w.writePublic([]byte("event: response.failed\ndata: " + string(body) + "\n\n"))
	} else {
		_, err = w.writePublic([]byte("event: error\ndata: " + string(body) + "\n\n"))
	}
	w.ended = err == nil
	if err == nil {
		w.ResponseWriter.Flush()
	}
	return err
}

func (w *OpenAIRecoveryStreamWriter) PublicOutputStarted() bool     { return w.started }
func (w *OpenAIRecoveryStreamWriter) PublicToolOutputStarted() bool { return w.publicTool }
func (w *OpenAIRecoveryStreamWriter) ClientDisconnected() bool      { return w.disconnect }
func (w *OpenAIRecoveryStreamWriter) Finalized() bool               { return w.ended }
func (w *OpenAIRecoveryStreamWriter) Written() bool                 { return w.started }

// WinnerOutput contains only the successful physical response output, never the
// public display transcript assembled from abandoned attempts and markers.
func (w *OpenAIRecoveryStreamWriter) WinnerOutput() []byte {
	if w.mode == OpenAIRecoveryResponsesJSON {
		var response map[string]json.RawMessage
		if json.Unmarshal(w.pending.Bytes(), &response) == nil {
			return append([]byte(nil), response["output"]...)
		}
	}
	return append([]byte(nil), w.winnerOutput...)
}

func (w *OpenAIRecoveryStreamWriter) WriteHeader(statusCode int) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	w.status = statusCode
}
func (w *OpenAIRecoveryStreamWriter) WriteHeaderNow() {}

func (w *OpenAIRecoveryStreamWriter) Flush() {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if w.started && !w.disconnect {
		w.ResponseWriter.Flush()
	}
}

func (w *OpenAIRecoveryStreamWriter) WriteString(s string) (int, error) {
	return w.Write([]byte(s))
}

func (w *OpenAIRecoveryStreamWriter) Write(p []byte) (int, error) {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	if w.ended {
		return 0, errors.New("OpenAI recovery response already ended")
	}
	if w.disconnect {
		return 0, http.ErrAbortHandler
	}
	if w.protocolErr != nil {
		return 0, w.protocolErr
	}
	if w.pending.Len()+len(p) > openAIRecoveryMaxPendingBytes {
		return 0, errors.New("OpenAI recovery pending output capacity exceeded")
	}
	if _, err := w.pending.Write(p); err != nil {
		return 0, err
	}
	if w.mode == OpenAIRecoveryResponsesJSON || w.mode == OpenAIRecoveryChatJSON {
		return len(p), nil
	}
	if err := w.processSSE(); err != nil {
		return 0, err
	}
	return len(p), nil
}

func (w *OpenAIRecoveryStreamWriter) processSSE() error {
	for {
		data := w.pending.Bytes()
		unscanned := data[w.pendingScan:]
		end := bytes.Index(unscanned, []byte("\n\n"))
		separatorSize := 2
		if crlfEnd := bytes.Index(unscanned, []byte("\r\n\r\n")); crlfEnd >= 0 && (end < 0 || crlfEnd < end) {
			end = crlfEnd
			separatorSize = 4
		}
		if end < 0 {
			w.pendingScan = max(0, len(data)-3)
			return nil
		}
		end += w.pendingScan
		frame := bytes.ReplaceAll(data[:end+separatorSize], []byte("\r\n"), []byte("\n"))
		w.pending.Next(end + separatorSize)
		w.pendingScan = 0
		if err := w.handleSSEFrame(frame); err != nil {
			w.protocolErr = err
			return err
		}
	}
}

func (w *OpenAIRecoveryStreamWriter) handleSSEFrame(frame []byte) error {
	data, wireType := recoverySSEParts(frame)
	if len(data) == 0 {
		if bytes.HasPrefix(bytes.TrimSpace(frame), []byte(":")) {
			_, err := w.writePublic(frame)
			return err
		}
		return nil
	}
	if bytes.Equal(data, []byte("[DONE]")) {
		if w.mode != OpenAIRecoveryChatSSE {
			return errors.New("unexpected DONE in Responses recovery stream")
		}
		w.done = true
		w.terminal = append(w.terminal, []byte("data: [DONE]\n\n")...)
		return nil
	}
	var event map[string]json.RawMessage
	if err := json.Unmarshal(data, &event); err != nil {
		return err
	}
	var eventType string
	_ = json.Unmarshal(event["type"], &eventType)
	if eventType == "" {
		eventType = wireType
	}
	if eventType == "kkai.recovery.v1" || wireType == "kkai.recovery.v1" {
		return errors.New("upstream emitted reserved OpenAI recovery control")
	}
	if w.mode == OpenAIRecoveryResponsesSSE {
		return w.handleResponsesFrame(data, eventType)
	}
	return w.handleChatFrame(data, eventType)
}

func recoverySSEParts(frame []byte) ([]byte, string) {
	var lines [][]byte
	var eventType string
	for _, line := range bytes.Split(frame, []byte("\n")) {
		if bytes.HasPrefix(line, []byte("data:")) {
			lines = append(lines, bytes.TrimPrefix(bytes.TrimPrefix(line, []byte("data:")), []byte(" ")))
		} else if bytes.HasPrefix(line, []byte("event:")) {
			eventType = strings.TrimSpace(string(line[len("event:"):]))
		}
	}
	return bytes.Join(lines, []byte("\n")), eventType
}

func (w *OpenAIRecoveryStreamWriter) handleChatFrame(data []byte, eventType string) error {
	if eventType == "error" {
		w.outcome = "failed"
		w.terminal = append(w.terminal, []byte("event: error\ndata: "+string(data)+"\n\n")...)
		return nil
	}
	var event map[string]any
	if err := json.Unmarshal(data, &event); err != nil {
		return err
	}
	if model, _ := event["model"].(string); model != "" {
		w.chatModel = model
	}
	event["id"] = w.logicalID
	finish := false
	visible := false
	if choices, ok := event["choices"].([]any); ok {
		for _, rawChoice := range choices {
			choice, ok := rawChoice.(map[string]any)
			if !ok {
				return errors.New("invalid OpenAI recovery Chat choice")
			}
			index, ok := recoveryInteger(choice["index"])
			if !ok || index >= w.expectedChoices {
				return errors.New("invalid OpenAI recovery Chat choice index")
			}
			if choice["finish_reason"] != nil {
				finish = true
				w.finished = true
				w.finishedChoices[index] = true
				if reason, _ := choice["finish_reason"].(string); reason == "length" {
					w.incomplete = true
				}
			}
			if delta, ok := choice["delta"].(map[string]any); ok {
				role, hasRole := delta["role"]
				if hasRole && w.roles[index] {
					delete(delta, "role")
					hasRole = false
				}
				if rawTools, hasTool := delta["tool_calls"]; hasTool {
					tools, ok := rawTools.([]any)
					if !ok {
						return errors.New("invalid OpenAI recovery Chat tools")
					}
					for _, rawTool := range tools {
						tool, ok := rawTool.(map[string]any)
						if !ok {
							return errors.New("invalid OpenAI recovery Chat tool delta")
						}
						toolIndex, ok := recoveryInteger(tool["index"])
						if !ok {
							return errors.New("invalid OpenAI recovery Chat tool index")
						}
						key := strconv.Itoa(index) + ":" + strconv.Itoa(toolIndex)
						state := w.heldChatTools[key]
						if state == nil {
							state = &openAIRecoveryChatTool{choice: index}
							w.heldChatTools[key] = state
						}
						if id, _ := tool["id"].(string); id != "" {
							state.id = id
						}
						if function, ok := tool["function"].(map[string]any); ok {
							if name, _ := function["name"].(string); name != "" {
								state.name += name
							}
							if arguments, _ := function["arguments"].(string); arguments != "" {
								state.arguments.WriteString(arguments)
							}
						}
					}
					toolDelta := map[string]any{"tool_calls": rawTools}
					if hasRole {
						toolDelta["role"] = role
						delete(delta, "role")
						w.roles[index] = true
						hasRole = false
					}
					toolEvent := map[string]any{"id": w.logicalID, "object": event["object"], "model": event["model"], "created": event["created"], "choices": []any{map[string]any{"index": index, "delta": toolDelta, "finish_reason": nil}}}
					encodedTool, err := json.Marshal(toolEvent)
					if err != nil {
						return err
					}
					w.heldBytes += len(encodedTool)
					if w.heldBytes > openAIRecoveryMaxPendingBytes {
						return errors.New("OpenAI recovery tool capacity exceeded")
					}
					w.publicTool = !w.structured
					if err := w.flushControls(); err != nil {
						return err
					}
					if _, err := w.writePublic([]byte("data: " + string(encodedTool) + "\n\n")); err != nil {
						return err
					}
					delete(delta, "tool_calls")
				}
				if rawCall, hasTool := delta["function_call"]; hasTool {
					call, ok := rawCall.(map[string]any)
					if !ok {
						return errors.New("invalid OpenAI recovery Chat function call")
					}
					key := "legacy:" + strconv.Itoa(index)
					state := w.heldChatTools[key]
					if state == nil {
						state = &openAIRecoveryChatTool{choice: index, id: key}
						w.heldChatTools[key] = state
					}
					if name, _ := call["name"].(string); name != "" {
						state.name += name
					}
					if arguments, _ := call["arguments"].(string); arguments != "" {
						state.arguments.WriteString(arguments)
					}
					w.heldBytes += len(data)
					if w.heldBytes > openAIRecoveryMaxPendingBytes {
						return errors.New("OpenAI recovery tool capacity exceeded")
					}
					toolDelta := map[string]any{"function_call": call}
					if hasRole {
						toolDelta["role"] = role
						delete(delta, "role")
						w.roles[index] = true
					}
					toolEvent := map[string]any{"id": w.logicalID, "object": event["object"], "model": event["model"], "created": event["created"], "choices": []any{map[string]any{"index": index, "delta": toolDelta, "finish_reason": nil}}}
					encodedTool, err := json.Marshal(toolEvent)
					if err != nil {
						return err
					}
					w.publicTool = !w.structured
					if err := w.flushControls(); err != nil {
						return err
					}
					if _, err := w.writePublic([]byte("data: " + string(encodedTool) + "\n\n")); err != nil {
						return err
					}
					delete(delta, "function_call")
				}
				if _, hasRole := delta["role"]; hasRole {
					if w.roles[index] {
						delete(delta, "role")
					} else {
						w.roles[index] = true
					}
				}
				if content, _ := delta["content"].(string); content != "" {
					w.choices[index] = true
					visible = true
					w.visible = true
					if w.structured {
						w.structuredText[index] = append(w.structuredText[index], content...)
					}
				}
				if reasoning, _ := delta["reasoning_content"].(string); reasoning != "" {
					w.choices[index] = true
					visible = true
					w.visible = true
				}
				if reasoning, _ := delta["reasoning"].(string); reasoning != "" {
					w.choices[index] = true
					visible = true
					w.visible = true
				}
				if refusal, _ := delta["refusal"].(string); refusal != "" {
					w.structuredAlternative[index] = true
					w.choices[index] = true
					visible = true
					w.visible = true
				}
			}
		}
	}
	if usage, ok := event["usage"].(map[string]any); ok {
		encoded, err := json.Marshal(usage)
		if err != nil {
			return err
		}
		w.usage = encoded
	}
	if finish || event["usage"] != nil {
		tail := make(map[string]any, len(event))
		for key, value := range event {
			tail[key] = value
		}
		tailChoices := make([]any, 0)
		choices, _ := event["choices"].([]any)
		for _, raw := range choices {
			choice := raw.(map[string]any)
			if choice["finish_reason"] != nil {
				tailChoices = append(tailChoices, map[string]any{"index": choice["index"], "delta": map[string]any{}, "finish_reason": choice["finish_reason"]})
				choice["finish_reason"] = nil
			}
		}
		tail["choices"] = tailChoices
		tailBytes, err := json.Marshal(tail)
		if err != nil {
			return err
		}
		w.terminal = append(w.terminal, []byte("data: "+string(tailBytes)+"\n\n")...)
		w.outcome = "completed"
		if w.incomplete {
			w.outcome = "incomplete"
		}
		delete(event, "usage")
		if !visible {
			return nil
		}
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}
	frame := []byte("data: " + string(encoded) + "\n\n")
	if !visible && len(w.terminal) > 0 {
		// Empty provider trailers are not part of the public terminal sequence.
		return nil
	}
	if err := w.flushControls(); err != nil {
		return err
	}
	_, err = w.writePublic(frame)
	return err
}

func recoveryInteger(value any) (int, bool) {
	number, ok := value.(float64)
	return int(number), ok && number >= 0 && number == float64(int(number))
}

func (w *OpenAIRecoveryStreamWriter) handleResponsesFrame(data []byte, eventType string) error {
	var event map[string]any
	if err := json.Unmarshal(data, &event); err != nil {
		return err
	}
	event["type"] = eventType
	if eventType == "error" {
		w.outcome = "failed"
		w.terminal = append(w.terminal[:0], []byte("event: error\ndata: "+string(data)+"\n\n")...)
		return nil
	}
	if response, ok := event["response"].(map[string]any); ok {
		if id, _ := response["id"].(string); id != "" {
			w.actualID = id
		}
		response["id"] = w.logicalID
	}
	if eventType == "response.created" {
		if w.created {
			return nil
		}
		w.created = true
	}
	if !w.created && eventType != "response.failed" && eventType != "response.cancelled" {
		createdResponse := map[string]any{"id": w.logicalID, "object": "response", "status": "in_progress", "output": []any{}}
		if response, ok := event["response"].(map[string]any); ok {
			for _, key := range []string{"model", "created_at", "service_tier"} {
				if value, exists := response[key]; exists {
					createdResponse[key] = value
				}
			}
		}
		if err := w.writeResponsesEvent(map[string]any{"type": "response.created", "response": createdResponse}); err != nil {
			return err
		}
		w.created = true
	}
	if eventType == "response.in_progress" && w.attempt > 1 {
		return nil
	}
	if eventType == "response.completed" || eventType == "response.done" || eventType == "response.incomplete" || eventType == "response.failed" || eventType == "response.cancelled" {
		response, _ := event["response"].(map[string]any)
		w.outcome = "completed"
		if eventType == "response.incomplete" {
			w.outcome = "incomplete"
		} else if eventType == "response.failed" {
			w.outcome = "failed"
		} else if eventType == "response.cancelled" {
			w.outcome = "cancelled"
		}
		if response != nil {
			if status, _ := response["status"].(string); status == "incomplete" || status == "failed" || status == "cancelled" {
				w.outcome = status
			}
			if usage, ok := response["usage"].(map[string]any); ok {
				encoded, err := json.Marshal(usage)
				if err != nil {
					return err
				}
				w.usage = encoded
			}
			if output, _ := response["output"].([]any); len(output) == 0 && (w.outcome == "completed" || w.outcome == "incomplete") {
				response["output"] = w.currentAttemptOutput()
			}
			if output, ok := response["output"].([]any); ok {
				if w.outcome == "completed" || w.outcome == "incomplete" {
					w.winnerOutput, _ = json.Marshal(output)
					if len(w.winnerOutput)+w.displayBytes > openAIRecoveryMaxPendingBytes {
						return errors.New("OpenAI recovery display capacity exceeded")
					}
				}
				for index, rawItem := range output {
					item, ok := rawItem.(map[string]any)
					if !ok {
						return errors.New("invalid managed Responses output item")
					}
					if w.outcome == "completed" || w.outcome == "incomplete" {
						if itemType, _ := item["type"].(string); itemType != "message" {
							if err := validateRecoveryItem(item); err != nil {
								return err
							}
							if w.publicToolItems[index] {
								if err := w.completePublicToolItem(index, item); err != nil {
									return err
								}
								w.heldItems[index] = item
							} else {
								w.releaseItems = append(w.releaseItems, item)
							}
						} else if err := w.normalizeResponseMessage(item, index, true); err != nil {
							return err
						}
					}
				}
			}
			response["output"] = w.display
		}
		frame, err := json.Marshal(event)
		if err != nil {
			return err
		}
		w.terminal = frame
		w.terminalEvent = event
		return nil
	}
	if item, ok := event["item"].(map[string]any); ok {
		if itemType, _ := item["type"].(string); itemType != "message" {
			originalIndex, ok := recoveryInteger(event["output_index"])
			if !ok {
				return errors.New("managed Responses item has no output index")
			}
			if isRecoveryExecutableToolItem(itemType) {
				if eventType == "response.output_item.added" {
					if w.publicToolItems[originalIndex] {
						return errors.New("managed Responses tool item added twice")
					}
					w.publicToolItems[originalIndex] = true
					w.heldItems[originalIndex] = item
					if id, _ := item["id"].(string); id != "" {
						w.heldItemIDs[id] = originalIndex
					}
					index := w.publicOutputIndex(originalIndex)
					w.publicToolInput[originalIndex] = recoveryToolInput(item)
					publicItem := recoveryItemCopy(item)
					w.rewriteResponseItem(publicItem, -1)
					id, _ := publicItem["id"].(string)
					w.displayIndexes[id] = index
					if err := w.rememberDisplayItem(publicItem); err != nil {
						return err
					}
					event["output_index"] = index
					event["item"] = publicItem
					w.publicTool = !w.structured
					return w.writeResponsesEvent(event)
				}
				if eventType == "response.output_item.done" {
					if !w.publicToolItems[originalIndex] {
						if err := w.beginPublicToolItem(originalIndex, item); err != nil {
							return err
						}
					}
					if err := validateRecoveryItem(item); err != nil {
						return err
					}
					w.heldItems[originalIndex] = item
					return w.completePublicToolItem(originalIndex, item)
				}
			}
			w.heldItems[originalIndex] = item
			if id, _ := item["id"].(string); id != "" {
				w.heldItemIDs[id] = originalIndex
			}
			w.heldBytes += len(data)
			if w.heldBytes > openAIRecoveryMaxPendingBytes {
				return errors.New("OpenAI recovery item capacity exceeded")
			}
			return nil
		}
		if eventType == "response.output_item.added" || eventType == "response.output_item.done" {
			originalIndex, ok := recoveryInteger(event["output_index"])
			if !ok {
				return errors.New("managed Responses item has no output index")
			}
			if eventType == "response.output_item.added" {
				if w.textItems[w.publicOutputIndex(originalIndex)] != nil {
					return errors.New("managed Responses message added twice")
				}
				if _, exists := item["content"]; !exists {
					item["content"] = []any{}
				}
			}
			return w.normalizeResponseMessage(item, originalIndex, eventType == "response.output_item.done")
		}
	}
	if originalIndex, ok := recoveryInteger(event["output_index"]); ok && w.heldItems[originalIndex] != nil && !w.publicToolItems[originalIndex] {
		if w.heldItems[originalIndex]["type"] == "reasoning" && strings.HasPrefix(eventType, "response.reasoning_summary_") {
			return w.handleRecoveryReasoning(event, originalIndex)
		}
		w.heldBytes += len(data)
		if w.heldBytes > openAIRecoveryMaxPendingBytes {
			return errors.New("OpenAI recovery item capacity exceeded")
		}
		return nil
	}
	if id, _ := event["item_id"].(string); id != "" {
		if originalIndex, held := w.heldItemIDs[id]; held && !w.publicToolItems[originalIndex] {
			if w.heldItems[originalIndex]["type"] == "reasoning" && strings.HasPrefix(eventType, "response.reasoning_summary_") {
				return w.handleRecoveryReasoning(event, originalIndex)
			}
			return nil
		}
	}
	if strings.HasPrefix(eventType, "response.function_call_arguments.") || strings.HasPrefix(eventType, "response.custom_tool_call_input.") {
		originalIndex, ok := recoveryInteger(event["output_index"])
		if !ok {
			id, _ := event["item_id"].(string)
			originalIndex, ok = w.heldItemIDs[id]
		}
		if !ok || !w.publicToolItems[originalIndex] {
			return errors.New("managed Responses tool input has no public item")
		}
		if strings.HasSuffix(eventType, ".delta") {
			delta, _ := event["delta"].(string)
			if len(w.publicToolInput[originalIndex])+len(delta) > openAIRecoveryMaxPendingBytes {
				return errors.New("OpenAI recovery tool capacity exceeded")
			}
			w.publicToolInput[originalIndex] += delta
		} else if strings.HasSuffix(eventType, ".done") {
			field := "arguments"
			if strings.HasPrefix(eventType, "response.custom_tool_call_input.") {
				field = "input"
			}
			full, _ := event[field].(string)
			if !strings.HasPrefix(full, w.publicToolInput[originalIndex]) {
				return errors.New("OpenAI recovery tool input differs from streamed prefix")
			}
			if suffix := strings.TrimPrefix(full, w.publicToolInput[originalIndex]); suffix != "" {
				index := w.publicOutputIndex(originalIndex)
				id, _ := event["item_id"].(string)
				if err := w.writeResponsesEvent(map[string]any{"type": strings.TrimSuffix(eventType, ".done") + ".delta", "output_index": index, "item_id": w.publicItemID(id), "delta": suffix}); err != nil {
					return err
				}
			}
			w.publicToolInput[originalIndex] = full
			w.publicToolInputDone[originalIndex] = true
		}
	}
	if eventType == "response.output_text.delta" || eventType == "response.output_text.done" || eventType == "response.refusal.delta" || eventType == "response.refusal.done" {
		partType, valueField := "output_text", "text"
		if strings.HasPrefix(eventType, "response.refusal.") {
			partType, valueField = "refusal", "refusal"
		}
		originalIndex, ok := recoveryInteger(event["output_index"])
		if !ok {
			originalIndex = 0
		}
		publicIndex := w.publicOutputIndex(originalIndex)
		id, _ := event["item_id"].(string)
		if id == "" {
			id = "text_" + strconv.Itoa(originalIndex)
		}
		publicID := w.publicItemID(id)
		state := w.textItems[publicIndex]
		if state == nil {
			item := map[string]any{"id": publicID, "type": "message", "role": "assistant", "status": "in_progress", "content": []any{}}
			state = &openAIRecoveryTextItem{id: publicID, index: publicIndex, item: item, parts: make(map[int]*openAIRecoveryTextPart)}
			w.textItems[publicIndex] = state
			if err := w.writeResponsesEvent(map[string]any{"type": "response.output_item.added", "output_index": publicIndex, "item": item}); err != nil {
				return err
			}
		}
		contentIndex, ok := recoveryInteger(event["content_index"])
		if !ok {
			contentIndex = 0
		}
		if state.parts[contentIndex] == nil {
			part := map[string]any{"type": partType, valueField: ""}
			if partType == "output_text" {
				part["annotations"] = []any{}
			}
			state.parts[contentIndex] = &openAIRecoveryTextPart{index: contentIndex, part: part}
			if err := w.writeResponsesEvent(map[string]any{"type": "response.content_part.added", "output_index": publicIndex, "content_index": contentIndex, "item_id": publicID, "part": part}); err != nil {
				return err
			}
		} else if state.parts[contentIndex].part["type"] != partType {
			return errors.New("managed Responses content part changes type")
		}
		event["output_index"] = float64(originalIndex)
		event["item_id"] = id
		if _, exists := event["content_index"]; !exists {
			event["content_index"] = contentIndex
		}
	}
	index := -1
	if originalIndex, ok := recoveryInteger(event["output_index"]); ok {
		index = w.publicOutputIndex(originalIndex)
		event["output_index"] = index
	}
	if item, ok := event["item"].(map[string]any); ok {
		w.rewriteResponseItem(item, -1)
		if id, _ := item["id"].(string); id != "" {
			w.displayIndexes[id] = index
		}
	}
	if oldID, _ := event["item_id"].(string); oldID != "" {
		event["item_id"] = w.publicItemID(oldID)
	}
	if state := w.textItems[index]; state != nil {
		contentIndex, ok := recoveryInteger(event["content_index"])
		if !ok {
			contentIndex = 0
		}
		partState := state.parts[contentIndex]
		switch eventType {
		case "response.content_part.added":
			if partState != nil {
				return errors.New("managed Responses content part added twice")
			}
			part, ok := event["part"].(map[string]any)
			if !ok || (part["type"] != "output_text" && part["type"] != "refusal") {
				return errors.New("managed Responses content part is unsupported")
			}
			field := "text"
			if part["type"] == "refusal" {
				field = "refusal"
			}
			text, ok := part[field].(string)
			if !ok {
				return errors.New("managed Responses content part is incomplete")
			}
			state.parts[contentIndex] = &openAIRecoveryTextPart{index: contentIndex, part: part}
			if text != "" {
				added := make(map[string]any, len(part))
				for key, value := range part {
					added[key] = value
				}
				added[field] = ""
				event["part"] = added
				if err := w.writeResponsesEvent(event); err != nil {
					return err
				}
				return w.extendRecoveryTextPart(state, contentIndex, text)
			}
		case "response.output_text.delta", "response.refusal.delta":
			if partState == nil || partState.textDone || partState.partDone || state.itemDone {
				return errors.New("managed Responses delta follows part completion")
			}
			if delta, _ := event["delta"].(string); delta != "" {
				if len(partState.text)+len(delta)+w.displayBytes > openAIRecoveryMaxPendingBytes {
					return errors.New("OpenAI recovery display capacity exceeded")
				}
				partState.text = append(partState.text, delta...)
				if w.structured && eventType == "response.output_text.delta" {
					w.structuredText[0] = append(w.structuredText[0], delta...)
				}
				if eventType == "response.refusal.delta" {
					w.structuredAlternative[0] = true
				}
				w.visible = true
			}
		case "response.output_text.done", "response.refusal.done":
			if partState == nil || partState.textDone || partState.partDone {
				return errors.New("managed Responses text done is duplicated or misplaced")
			}
			field := "text"
			if eventType == "response.refusal.done" {
				field = "refusal"
			}
			text, ok := event[field].(string)
			if !ok {
				return errors.New("managed Responses text done is incomplete")
			}
			if err := w.extendRecoveryTextPart(state, contentIndex, text); err != nil {
				return err
			}
			partState.textDone = true
		case "response.content_part.done":
			if partState == nil || partState.partDone {
				return errors.New("managed Responses part done is duplicated or misplaced")
			}
			part, ok := event["part"].(map[string]any)
			if !ok || part["type"] != partState.part["type"] {
				return errors.New("managed Responses part done changes type")
			}
			field, eventName := "text", "response.output_text"
			if part["type"] == "refusal" {
				field, eventName = "refusal", "response.refusal"
			}
			text, ok := part[field].(string)
			if !ok {
				return errors.New("managed Responses part done is incomplete")
			}
			if err := w.extendRecoveryTextPart(state, contentIndex, text); err != nil {
				return err
			}
			if !partState.textDone {
				if err := w.writeResponsesEvent(map[string]any{"type": eventName + ".done", "output_index": index, "content_index": contentIndex, "item_id": state.id, field: text}); err != nil {
					return err
				}
				partState.textDone = true
			}
			partState.part = part
			partState.partDone = true
		}
	}
	if eventType == "response.created" {
		if response, ok := event["response"].(map[string]any); ok {
			response["output"] = []any{}
		}
	}
	frame, err := w.encodeResponsesEvent(event)
	if err != nil {
		return err
	}
	if err := w.flushControls(); err != nil {
		return err
	}
	_, err = w.writePublic(frame)
	return err
}

func (w *OpenAIRecoveryStreamWriter) extendRecoveryTextPart(item *openAIRecoveryTextItem, contentIndex int, text string) error {
	part := item.parts[contentIndex]
	if !strings.HasPrefix(text, string(part.text)) || ((part.textDone || part.partDone) && text != string(part.text)) {
		return errors.New("managed Responses text snapshot differs from prefix")
	}
	if len(text)+w.displayBytes > openAIRecoveryMaxPendingBytes {
		return errors.New("OpenAI recovery display capacity exceeded")
	}
	eventName := "response.output_text.delta"
	refusal := part.part["type"] == "refusal"
	if refusal {
		eventName = "response.refusal.delta"
	}
	if suffix := text[len(part.text):]; suffix != "" {
		if err := w.writeResponsesEvent(map[string]any{"type": eventName, "output_index": item.index, "content_index": contentIndex, "item_id": item.id, "delta": suffix}); err != nil {
			return err
		}
		if w.structured && !refusal {
			w.structuredText[0] = append(w.structuredText[0], suffix...)
		}
		w.visible = true
	}
	part.text = []byte(text)
	if refusal && text != "" {
		w.structuredAlternative[0] = true
	}
	return nil
}

func (w *OpenAIRecoveryStreamWriter) currentAttemptOutput() []any {
	indexes := make([]int, 0, len(w.indexes)+len(w.heldItems))
	seen := make(map[int]bool)
	for index := range w.indexes {
		indexes = append(indexes, index)
		seen[index] = true
	}
	for index := range w.heldItems {
		if !seen[index] {
			indexes = append(indexes, index)
		}
	}
	sort.Ints(indexes)
	output := make([]any, 0, len(indexes))
	for _, originalIndex := range indexes {
		if item := w.heldItems[originalIndex]; item != nil {
			output = append(output, item)
			continue
		}
		state := w.textItems[w.indexes[originalIndex]]
		if state == nil {
			continue
		}
		originalID := state.id
		for sourceID, publicID := range w.itemIDs {
			if publicID == state.id {
				originalID = sourceID
				break
			}
		}
		output = append(output, map[string]any{"id": originalID, "type": "message", "role": "assistant", "status": "completed", "content": state.content()})
	}
	return output
}

func validateRecoveryItem(item map[string]any) error {
	typeName, _ := item["type"].(string)
	if id, _ := item["id"].(string); id == "" {
		return errors.New("OpenAI recovery item has no ID")
	}
	if status, _ := item["status"].(string); status != "" && status != "completed" {
		return errors.New("OpenAI recovery item is incomplete")
	}
	switch typeName {
	case "function_call":
		arguments, ok := item["arguments"].(string)
		if !ok || !json.Valid([]byte(arguments)) || item["call_id"] == nil || item["name"] == nil {
			return errors.New("OpenAI recovery function call is incomplete")
		}
	case "custom_tool_call":
		if _, ok := item["input"].(string); !ok || item["call_id"] == nil || item["name"] == nil {
			return errors.New("OpenAI recovery custom tool call is incomplete")
		}
	case "tool_search_call":
		if _, ok := item["arguments"].(map[string]any); !ok || item["call_id"] == nil {
			return errors.New("OpenAI recovery tool search call is incomplete")
		}
	case "reasoning", "compaction", "compaction_summary":
		if encrypted, exists := item["encrypted_content"]; exists {
			if value, ok := encrypted.(string); !ok || value == "" {
				return errors.New("OpenAI recovery opaque item is incomplete")
			}
		}
	default:
		if !isRecoveryExecutableToolItem(typeName) {
			return errors.New("OpenAI recovery item type is not supported")
		}
	}
	return nil
}

func isRecoveryExecutableToolItem(typeName string) bool {
	return strings.HasSuffix(typeName, "_call") && typeName != ""
}

func recoveryItemCopy(item map[string]any) map[string]any {
	copy := make(map[string]any, len(item))
	for key, value := range item {
		copy[key] = value
	}
	return copy
}

func recoveryToolInput(item map[string]any) string {
	field := "arguments"
	if item["type"] == "custom_tool_call" {
		field = "input"
	}
	value, _ := item[field].(string)
	return value
}

func (w *OpenAIRecoveryStreamWriter) beginPublicToolItem(originalIndex int, item map[string]any) error {
	if w.publicToolItems[originalIndex] {
		return nil
	}
	if id, _ := item["id"].(string); id == "" {
		return errors.New("OpenAI recovery tool item has no ID")
	}
	w.publicToolItems[originalIndex] = true
	w.heldItems[originalIndex] = item
	id, _ := item["id"].(string)
	w.heldItemIDs[id] = originalIndex
	index := w.publicOutputIndex(originalIndex)
	w.publicToolInput[originalIndex] = ""
	publicItem := recoveryItemCopy(item)
	w.rewriteResponseItem(publicItem, -1)
	publicItem["status"] = "in_progress"
	if typeName, _ := item["type"].(string); typeName == "function_call" || typeName == "custom_tool_call" {
		field := "arguments"
		if typeName == "custom_tool_call" {
			field = "input"
		}
		publicItem[field] = ""
	}
	w.displayIndexes[publicItem["id"].(string)] = index
	if err := w.rememberDisplayItem(publicItem); err != nil {
		return err
	}
	w.publicTool = !w.structured
	return w.writeResponsesEvent(map[string]any{"type": "response.output_item.added", "output_index": index, "item": publicItem})
}

func (w *OpenAIRecoveryStreamWriter) completePublicToolItem(originalIndex int, item map[string]any) error {
	if !w.publicToolItems[originalIndex] {
		if err := w.beginPublicToolItem(originalIndex, item); err != nil {
			return err
		}
	}
	index := w.publicOutputIndex(originalIndex)
	publicItem := recoveryItemCopy(item)
	w.rewriteResponseItem(publicItem, -1)
	id, _ := publicItem["id"].(string)
	if id == "" {
		return errors.New("OpenAI recovery tool item has no ID")
	}
	if typeName, _ := item["type"].(string); typeName == "function_call" || typeName == "custom_tool_call" {
		field, eventName := "arguments", "response.function_call_arguments"
		if typeName == "custom_tool_call" {
			field, eventName = "input", "response.custom_tool_call_input"
		}
		full, _ := item[field].(string)
		if !strings.HasPrefix(full, w.publicToolInput[originalIndex]) {
			return errors.New("OpenAI recovery tool input differs from streamed prefix")
		}
		if suffix := strings.TrimPrefix(full, w.publicToolInput[originalIndex]); suffix != "" {
			if err := w.writeResponsesEvent(map[string]any{"type": eventName + ".delta", "output_index": index, "item_id": id, "delta": suffix}); err != nil {
				return err
			}
			w.publicToolInput[originalIndex] = full
		}
		if !w.publicToolInputDone[originalIndex] {
			if err := w.writeResponsesEvent(map[string]any{"type": eventName + ".done", "output_index": index, "item_id": id, "call_id": item["call_id"], "name": item["name"], field: full}); err != nil {
				return err
			}
			w.publicToolInputDone[originalIndex] = true
		}
	}
	if !w.publicToolItemDone[originalIndex] {
		if err := w.writeResponsesEvent(map[string]any{"type": "response.output_item.done", "output_index": index, "item": publicItem}); err != nil {
			return err
		}
		w.publicToolItemDone[originalIndex] = true
	}
	return w.rememberDisplayItem(publicItem)
}

func (w *OpenAIRecoveryStreamWriter) handleRecoveryReasoning(event map[string]any, originalIndex int) error {
	source := w.heldItems[originalIndex]
	id, _ := source["id"].(string)
	if id == "" {
		return errors.New("OpenAI recovery reasoning item has no ID")
	}
	state := w.reasoningItems[id]
	if state == nil {
		if _, reused := w.displayIDs[id]; reused {
			return errors.New("OpenAI recovery reasoning ID was reused across attempts")
		}
		index := w.publicOutputIndex(originalIndex)
		state = &openAIRecoveryReasoningItem{id: id, index: index, parts: make(map[int]*openAIRecoveryReasoningPart)}
		w.reasoningItems[id] = state
		w.displayIndexes[id] = index
		if err := w.writeResponsesEvent(map[string]any{"type": "response.output_item.added", "output_index": index, "item": map[string]any{"id": id, "type": "reasoning", "status": "in_progress", "summary": []any{}}}); err != nil {
			return err
		}
	}
	summaryIndex, ok := recoveryInteger(event["summary_index"])
	if !ok {
		summaryIndex = 0
	}
	part := state.parts[summaryIndex]
	if part == nil {
		part = &openAIRecoveryReasoningPart{}
		state.parts[summaryIndex] = part
		if err := w.writeResponsesEvent(map[string]any{"type": "response.reasoning_summary_part.added", "output_index": state.index, "item_id": id, "summary_index": summaryIndex, "part": map[string]any{"type": "summary_text", "text": ""}}); err != nil {
			return err
		}
	}
	eventType, _ := event["type"].(string)
	if eventType == "response.reasoning_summary_part.added" {
		return nil
	}
	event["output_index"] = state.index
	event["item_id"] = id
	event["summary_index"] = summaryIndex
	switch eventType {
	case "response.reasoning_summary_text.delta":
		delta, _ := event["delta"].(string)
		if part.textDone || part.partDone {
			return errors.New("OpenAI recovery reasoning delta follows done")
		}
		if len(part.text)+len(delta)+w.heldBytes > openAIRecoveryMaxPendingBytes {
			return errors.New("OpenAI recovery reasoning capacity exceeded")
		}
		part.text = append(part.text, delta...)
		if delta != "" {
			w.visible = true
		}
	case "response.reasoning_summary_text.done":
		if part.textDone {
			return nil
		}
		text, _ := event["text"].(string)
		if !strings.HasPrefix(text, string(part.text)) {
			return errors.New("OpenAI recovery reasoning snapshot differs from prefix")
		}
		if suffix := text[len(part.text):]; suffix != "" {
			if err := w.writeResponsesEvent(map[string]any{"type": "response.reasoning_summary_text.delta", "output_index": state.index, "item_id": id, "summary_index": summaryIndex, "delta": suffix}); err != nil {
				return err
			}
			w.visible = true
		}
		part.text = []byte(text)
		part.textDone = true
	case "response.reasoning_summary_part.done":
		if part.partDone {
			return nil
		}
		part.partDone = true
	}
	return w.writeResponsesEvent(event)
}

func (w *OpenAIRecoveryStreamWriter) closeRecoveryReasoning(state *openAIRecoveryReasoningItem, winner map[string]any) error {
	if state.done {
		return nil
	}
	if winner != nil {
		if summary, ok := winner["summary"].([]any); ok {
			for index, raw := range summary {
				part, ok := raw.(map[string]any)
				if !ok {
					return errors.New("OpenAI recovery reasoning summary is invalid")
				}
				text, _ := part["text"].(string)
				tracked := state.parts[index]
				if tracked == nil {
					tracked = &openAIRecoveryReasoningPart{}
					state.parts[index] = tracked
					if err := w.writeResponsesEvent(map[string]any{"type": "response.reasoning_summary_part.added", "output_index": state.index, "item_id": state.id, "summary_index": index, "part": map[string]any{"type": "summary_text", "text": ""}}); err != nil {
						return err
					}
				}
				if !strings.HasPrefix(text, string(tracked.text)) {
					return errors.New("OpenAI recovery reasoning summary differs from prefix")
				}
				if suffix := text[len(tracked.text):]; suffix != "" {
					if tracked.textDone || tracked.partDone {
						return errors.New("OpenAI recovery reasoning summary changed after done")
					}
					if err := w.writeResponsesEvent(map[string]any{"type": "response.reasoning_summary_text.delta", "output_index": state.index, "item_id": state.id, "summary_index": index, "delta": suffix}); err != nil {
						return err
					}
				}
				tracked.text = []byte(text)
			}
		}
	}
	indexes := make([]int, 0, len(state.parts))
	for index := range state.parts {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	summary := make([]any, 0, len(indexes))
	for _, index := range indexes {
		part := state.parts[index]
		text := string(part.text)
		if !part.textDone {
			if err := w.writeResponsesEvent(map[string]any{"type": "response.reasoning_summary_text.done", "output_index": state.index, "item_id": state.id, "summary_index": index, "text": text}); err != nil {
				return err
			}
			part.textDone = true
		}
		partValue := map[string]any{"type": "summary_text", "text": text}
		if !part.partDone {
			if err := w.writeResponsesEvent(map[string]any{"type": "response.reasoning_summary_part.done", "output_index": state.index, "item_id": state.id, "summary_index": index, "part": partValue}); err != nil {
				return err
			}
			part.partDone = true
		}
		summary = append(summary, partValue)
	}
	item := winner
	if item == nil {
		item = map[string]any{"id": state.id, "type": "reasoning", "status": "completed", "summary": summary}
	}
	if err := w.writeResponsesEvent(map[string]any{"type": "response.output_item.done", "output_index": state.index, "item": item}); err != nil {
		return err
	}
	state.done = true
	return w.rememberDisplayItem(item)
}

func (w *OpenAIRecoveryStreamWriter) releaseResponseItem(item map[string]any) error {
	typeName, _ := item["type"].(string)
	if id, _ := item["id"].(string); typeName == "reasoning" && w.reasoningItems[id] != nil {
		return w.closeRecoveryReasoning(w.reasoningItems[id], item)
	}
	if typeName != "reasoning" && typeName != "compaction" && typeName != "compaction_summary" {
		w.rewriteResponseItem(item, -1)
	}
	index := w.nextIndex
	w.nextIndex++
	id, _ := item["id"].(string)
	w.displayIndexes[id] = index
	if id == "" {
		return errors.New("OpenAI recovery item has no ID")
	}
	added := make(map[string]any, len(item))
	for key, value := range item {
		added[key] = value
	}
	added["status"] = "in_progress"
	if typeName == "function_call" {
		added["arguments"] = ""
	}
	if typeName == "custom_tool_call" {
		added["input"] = ""
	}
	if err := w.writeResponsesEvent(map[string]any{"type": "response.output_item.added", "output_index": index, "item": added}); err != nil {
		return err
	}
	if typeName == "function_call" || typeName == "custom_tool_call" {
		field := "arguments"
		eventName := "response.function_call_arguments"
		if typeName == "custom_tool_call" {
			field = "input"
			eventName = "response.custom_tool_call_input"
		}
		value, _ := item[field].(string)
		if err := w.writeResponsesEvent(map[string]any{"type": eventName + ".delta", "output_index": index, "item_id": id, "delta": value}); err != nil {
			return err
		}
		if err := w.writeResponsesEvent(map[string]any{"type": eventName + ".done", "output_index": index, "item_id": id, "call_id": item["call_id"], "name": item["name"], field: value}); err != nil {
			return err
		}
	}
	if err := w.writeResponsesEvent(map[string]any{"type": "response.output_item.done", "output_index": index, "item": item}); err != nil {
		return err
	}
	return w.rememberDisplayItem(item)
}

func (w *OpenAIRecoveryStreamWriter) publicOutputIndex(original int) int {
	if index, ok := w.indexes[original]; ok {
		return index
	}
	index := w.nextIndex
	w.nextIndex++
	w.indexes[original] = index
	return index
}

func (w *OpenAIRecoveryStreamWriter) publicItemID(original string) string {
	if id, ok := w.itemIDs[original]; ok {
		return id
	}
	id := fmt.Sprintf("%s_item_%d_%d", w.logicalID, w.attempt, len(w.itemIDs))
	w.itemIDs[original] = id
	return id
}

func (w *OpenAIRecoveryStreamWriter) rewriteResponseItem(item map[string]any, originalIndex int) {
	if id, _ := item["id"].(string); id != "" {
		item["id"] = w.publicItemID(id)
	}
	if originalIndex >= 0 {
		_ = w.publicOutputIndex(originalIndex)
	}
}

func (w *OpenAIRecoveryStreamWriter) rememberDisplayItem(item map[string]any) error {
	id, _ := item["id"].(string)
	if id == "" {
		return errors.New("OpenAI recovery display item has no ID")
	}
	encoded, err := json.Marshal(item)
	if err != nil {
		return err
	}
	nextSize := w.displayBytes - w.displaySizes[id] + len(encoded)
	if nextSize > openAIRecoveryMaxPendingBytes {
		return errors.New("OpenAI recovery display capacity exceeded")
	}
	w.displayBytes = nextSize
	w.displaySizes[id] = len(encoded)
	if position, ok := w.displayIDs[id]; ok {
		w.display[position] = item
		return nil
	}
	w.displayIDs[id] = len(w.display)
	w.display = append(w.display, item)
	return nil
}

func (w *OpenAIRecoveryStreamWriter) normalizeResponseMessage(item map[string]any, originalIndex int, complete bool) error {
	index := w.publicOutputIndex(originalIndex)
	w.rewriteResponseItem(item, -1)
	id, _ := item["id"].(string)
	w.displayIndexes[id] = index
	if itemType, _ := item["type"].(string); itemType != "message" {
		return errors.New("managed Responses terminal output requires a verified message type")
	}
	state := w.textItems[index]
	if state != nil && state.id != id {
		return errors.New("managed Responses terminal item identity changed")
	}
	content, ok := item["content"].([]any)
	if !ok {
		return errors.New("managed Responses terminal message has no content")
	}
	if state != nil {
		if state.itemDone && len(content) != len(state.parts) {
			return errors.New("managed Responses completed message changes content parts")
		}
		for partIndex, existing := range state.parts {
			if partIndex >= len(content) || existing == nil {
				return errors.New("managed Responses terminal omits a streamed content part")
			}
		}
	}
	for partIndex, raw := range content {
		part, ok := raw.(map[string]any)
		if !ok || (part["type"] != "output_text" && part["type"] != "refusal") {
			return errors.New("managed Responses terminal content is not text or refusal")
		}
		field := "text"
		if part["type"] == "refusal" {
			field = "refusal"
		}
		text, ok := part[field].(string)
		if !ok || (complete && field == "refusal" && text == "") {
			return errors.New("managed Responses terminal content is incomplete")
		}
		if state != nil {
			if existing := state.parts[partIndex]; existing != nil {
				if existing.part["type"] != part["type"] || !strings.HasPrefix(text, string(existing.text)) || ((existing.textDone || existing.partDone) && text != string(existing.text)) {
					return errors.New("managed Responses terminal content differs from streamed prefix")
				}
			}
		}
	}
	if state == nil {
		state = &openAIRecoveryTextItem{id: id, index: index, item: item, parts: make(map[int]*openAIRecoveryTextPart)}
		w.textItems[index] = state
		addedItem := map[string]any{"id": id, "type": "message", "role": "assistant", "status": "in_progress", "content": []any{}}
		if err := w.writeResponsesEvent(map[string]any{"type": "response.output_item.added", "output_index": index, "item": addedItem}); err != nil {
			return err
		}
	}
	for partIndex, raw := range content {
		part := raw.(map[string]any)
		field, eventName := "text", "response.output_text"
		if part["type"] == "refusal" {
			field, eventName = "refusal", "response.refusal"
		}
		text := part[field].(string)
		partState := state.parts[partIndex]
		if partState == nil {
			added := map[string]any{"type": part["type"], field: ""}
			if field == "text" {
				added["annotations"] = []any{}
			}
			partState = &openAIRecoveryTextPart{index: partIndex, part: added}
			state.parts[partIndex] = partState
			if err := w.writeResponsesEvent(map[string]any{"type": "response.content_part.added", "output_index": index, "content_index": partIndex, "item_id": id, "part": added}); err != nil {
				return err
			}
		}
		if err := w.extendRecoveryTextPart(state, partIndex, text); err != nil {
			return err
		}
		partState.part = part
		if complete && !partState.textDone {
			if err := w.writeResponsesEvent(map[string]any{"type": eventName + ".done", "output_index": index, "content_index": partIndex, "item_id": id, field: text}); err != nil {
				return err
			}
			partState.textDone = true
		}
		if complete && !partState.partDone {
			if err := w.writeResponsesEvent(map[string]any{"type": "response.content_part.done", "output_index": index, "content_index": partIndex, "item_id": id, "part": part}); err != nil {
				return err
			}
			partState.partDone = true
		}
	}
	state.item = item
	if !complete {
		return nil
	}
	if !state.itemDone {
		if err := w.writeResponsesEvent(map[string]any{"type": "response.output_item.done", "output_index": index, "item": item}); err != nil {
			return err
		}
		state.itemDone = true
	}
	return w.rememberDisplayItem(item)
}

func (w *OpenAIRecoveryStreamWriter) encodeResponsesEvent(event map[string]any) ([]byte, error) {
	w.publicSeq++
	event["sequence_number"] = w.publicSeq
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	if len(data) > openAIRecoveryMaxPendingBytes {
		return nil, errors.New("OpenAI recovery response event capacity exceeded")
	}
	eventType, _ := event["type"].(string)
	return []byte("event: " + eventType + "\ndata: " + string(data) + "\n\n"), nil
}

func (w *OpenAIRecoveryStreamWriter) emitRegenerationMarker() error {
	const marker = "\n\n[Response interrupted. Regenerating...]\n\n"
	if w.mode == OpenAIRecoveryChatSSE {
		if len(w.choices) == 0 {
			return nil
		}
		choices := make([]any, 0, len(w.choices))
		indexes := make([]int, 0, len(w.choices))
		for index := range w.choices {
			indexes = append(indexes, index)
		}
		sort.Ints(indexes)
		for _, index := range indexes {
			choices = append(choices, map[string]any{"index": index, "delta": map[string]any{"content": marker}, "finish_reason": nil})
		}
		data, err := json.Marshal(map[string]any{"id": w.logicalID, "object": "chat.completion.chunk", "model": w.chatModel, "choices": choices})
		if err != nil {
			return err
		}
		w.queueControl("synthetic_begin", "")
		if err := w.flushControls(); err != nil {
			return err
		}
		if _, err := w.writePublic([]byte("data: " + string(data) + "\n\n")); err != nil {
			return err
		}
		w.queueControl("synthetic_end", "")
		return w.flushControls()
	}
	index := w.nextIndex
	w.nextIndex++
	id := w.logicalID + "_marker_" + strconv.Itoa(w.attempt)
	state := &openAIRecoveryTextItem{id: id, index: index, item: map[string]any{"id": id, "type": "message", "role": "assistant", "status": "in_progress", "content": []any{}}, parts: make(map[int]*openAIRecoveryTextPart)}
	w.displayIndexes[id] = index
	w.textItems[index] = state
	if err := w.writeResponsesEvent(map[string]any{"type": "response.output_item.added", "output_index": index, "item": state.item}); err != nil {
		return err
	}
	part := &openAIRecoveryTextPart{index: 0, part: map[string]any{"type": "output_text", "text": "", "annotations": []any{}}}
	state.parts[0] = part
	if err := w.writeResponsesEvent(map[string]any{"type": "response.content_part.added", "output_index": index, "content_index": 0, "item_id": id, "part": part.part}); err != nil {
		return err
	}
	w.queueControl("synthetic_begin", "")
	if err := w.flushControls(); err != nil {
		return err
	}
	if err := w.writeResponsesEvent(map[string]any{"type": "response.output_text.delta", "output_index": index, "content_index": 0, "item_id": id, "delta": marker}); err != nil {
		return err
	}
	w.queueControl("synthetic_end", "")
	if err := w.flushControls(); err != nil {
		return err
	}
	part.text = []byte(marker)
	part.part["text"] = marker
	if err := w.writeResponsesEvent(map[string]any{"type": "response.output_text.done", "output_index": index, "content_index": 0, "item_id": id, "text": marker}); err != nil {
		return err
	}
	part.textDone = true
	if err := w.writeResponsesEvent(map[string]any{"type": "response.content_part.done", "output_index": index, "content_index": 0, "item_id": id, "part": part.part}); err != nil {
		return err
	}
	part.partDone = true
	state.item["content"] = state.content()
	state.item["status"] = "completed"
	if err := w.writeResponsesEvent(map[string]any{"type": "response.output_item.done", "output_index": index, "item": state.item}); err != nil {
		return err
	}
	state.itemDone = true
	if err := w.rememberDisplayItem(state.item); err != nil {
		return err
	}
	return nil
}

func (w *OpenAIRecoveryStreamWriter) writeResponsesEvent(event map[string]any) error {
	frame, err := w.encodeResponsesEvent(event)
	if err != nil {
		return err
	}
	if err := w.flushControls(); err != nil {
		return err
	}
	_, err = w.writePublic(frame)
	return err
}

func (w *OpenAIRecoveryStreamWriter) queueControl(kind, outcome string) {
	if w.seq >= 45 {
		w.protocolErr = errors.New("OpenAI recovery control capacity exceeded")
		return
	}
	w.seq++
	control := openAIRecoveryControl{Type: "kkai.recovery.v1", Kind: kind, LogicalID: w.logicalID, Seq: w.seq, Attempt: w.attempt, Outcome: outcome}
	data, _ := json.Marshal(control)
	w.controls = append(w.controls, []byte("data: "+string(data)+"\n\n"))
}

func (w *OpenAIRecoveryStreamWriter) queueTerminal(outcome string) {
	if w.seq >= 45 {
		w.protocolErr = errors.New("OpenAI recovery control capacity exceeded")
		return
	}
	w.seq++
	reported := len(w.usage) > 0
	format := "responses"
	if w.mode == OpenAIRecoveryChatSSE {
		format = "chat"
	}
	usage := w.usage
	if !reported {
		usage = json.RawMessage("null")
	}
	control := openAIRecoveryControl{Type: "kkai.recovery.v1", Kind: "terminal", LogicalID: w.logicalID, Seq: w.seq, Attempt: w.attempt, Outcome: outcome, UsageFormat: format, UsageReported: &reported, Usage: usage}
	data, _ := json.Marshal(control)
	w.controls = append(w.controls, []byte("data: "+string(data)+"\n\n"))
}

func (w *OpenAIRecoveryStreamWriter) flushControls() error {
	if w.structured && !w.committing {
		return nil
	}
	for _, control := range w.controls {
		if _, err := w.writePublic(control); err != nil {
			return err
		}
	}
	w.controls = nil
	return nil
}

func (w *OpenAIRecoveryStreamWriter) flushJSON() error {
	if w.pending.Len() == 0 {
		return errors.New("OpenAI recovery attempt has no JSON body")
	}
	var body map[string]any
	if err := json.Unmarshal(w.pending.Bytes(), &body); err != nil {
		return err
	}
	if id, _ := body["id"].(string); id != "" {
		w.actualID = id
	}
	body["id"] = w.logicalID
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}
	if w.status == 0 {
		w.status = http.StatusOK
	}
	_, err = w.writePublic(encoded)
	w.pending.Reset()
	w.ended = err == nil
	return err
}

func (w *OpenAIRecoveryStreamWriter) prepareRecoveryJSON() error {
	var body map[string]any
	if json.Unmarshal(w.pending.Bytes(), &body) != nil {
		return openAIRecoveryInvalidAttempt("OpenAI recovery JSON body is invalid")
	}
	if id, _ := body["id"].(string); id != "" {
		w.actualID = id
	}
	if body["error"] != nil {
		return openAIRecoveryInvalidAttempt("OpenAI recovery JSON body contains an error")
	}
	if w.mode == OpenAIRecoveryResponsesJSON {
		if status, _ := body["status"].(string); status != "completed" && status != "incomplete" {
			return openAIRecoveryInvalidAttempt("OpenAI recovery JSON response is not complete")
		}
		output, _ := body["output"].([]any)
		var text strings.Builder
		alternative := false
		for _, raw := range output {
			item, ok := raw.(map[string]any)
			if !ok {
				return openAIRecoveryInvalidAttempt("OpenAI recovery JSON item is invalid")
			}
			if item["type"] != "message" {
				if err := validateRecoveryItem(item); err != nil {
					return openAIRecoveryInvalidAttempt(err.Error())
				}
				if item["type"] == "function_call" {
					name, _ := item["name"].(string)
					arguments, _ := item["arguments"].(string)
					if err := ValidateOpenAIRecoveryToolArguments(w.request, name, arguments); err != nil {
						return openAIRecoveryInvalidAttempt("OpenAI recovery tool arguments failed validation")
					}
				}
				if item["type"] == "function_call" || item["type"] == "custom_tool_call" || item["type"] == "tool_search_call" {
					alternative = true
				}
				continue
			}
			parts, _ := item["content"].([]any)
			for _, rawPart := range parts {
				part, _ := rawPart.(map[string]any)
				if refusal, _ := part["refusal"].(string); part["type"] == "refusal" && refusal != "" {
					alternative = true
				}
				if content, _ := part["text"].(string); content != "" {
					text.WriteString(content)
				}
			}
		}
		if w.structured && (text.Len() > 0 || !alternative) {
			if err := ValidateOpenAIRecoveryStructuredOutput(w.request, w.mode, text.String()); err != nil {
				if body["status"] == "incomplete" {
					body["output"] = []any{}
					encoded, _ := json.Marshal(body)
					w.pending.Reset()
					_, _ = w.pending.Write(encoded)
					return nil
				}
				return openAIRecoveryInvalidAttempt("OpenAI recovery structured output failed validation")
			}
		}
	} else {
		choices, _ := body["choices"].([]any)
		if len(choices) != w.expectedChoices {
			return openAIRecoveryInvalidAttempt("OpenAI recovery JSON choices are incomplete")
		}
		for _, raw := range choices {
			choice, _ := raw.(map[string]any)
			if choice["finish_reason"] == nil {
				return openAIRecoveryInvalidAttempt("OpenAI recovery JSON choice has no finish reason")
			}
			message, _ := choice["message"].(map[string]any)
			if message == nil {
				return openAIRecoveryInvalidAttempt("OpenAI recovery JSON choice has no message")
			}
			refusal, _ := message["refusal"].(string)
			alternative := refusal != ""
			if tools, ok := message["tool_calls"].([]any); ok {
				for _, rawTool := range tools {
					tool, _ := rawTool.(map[string]any)
					function, _ := tool["function"].(map[string]any)
					name, _ := function["name"].(string)
					arguments, _ := function["arguments"].(string)
					if id, _ := tool["id"].(string); id == "" {
						return openAIRecoveryInvalidAttempt("OpenAI recovery Chat tool call has no ID")
					}
					if err := ValidateOpenAIRecoveryToolArguments(w.request, name, arguments); err != nil {
						return openAIRecoveryInvalidAttempt("OpenAI recovery tool arguments failed validation")
					}
					alternative = true
				}
			}
			if content, _ := message["content"].(string); w.structured && (content != "" || !alternative) {
				if err := ValidateOpenAIRecoveryStructuredOutput(w.request, w.mode, content); err != nil {
					if choice["finish_reason"] == "length" {
						message["content"] = nil
						encoded, _ := json.Marshal(body)
						w.pending.Reset()
						_, _ = w.pending.Write(encoded)
						continue
					}
					return openAIRecoveryInvalidAttempt("OpenAI recovery structured output failed validation")
				}
			}
		}
	}
	return nil
}

func (w *OpenAIRecoveryStreamWriter) writePublic(p []byte) (int, error) {
	if w.structured && !w.committing {
		if w.structuredOutput.Len()+len(p) > openAIRecoveryMaxPendingBytes {
			return 0, errors.New("OpenAI recovery structured capacity exceeded")
		}
		return w.structuredOutput.Write(p)
	}
	if !w.started {
		if w.status != 0 {
			w.ResponseWriter.WriteHeader(w.status)
		}
		w.started = true
	}
	n, err := w.ResponseWriter.Write(p)
	if err != nil {
		w.disconnect = true
	}
	return n, err
}

var _ gin.ResponseWriter = (*OpenAIRecoveryStreamWriter)(nil)

func openAIRecoveryInvalidAttempt(message string) *UpstreamFailoverError {
	body, _ := json.Marshal(map[string]any{"error": map[string]any{"type": "upstream_error", "code": "invalid_recovery_attempt", "message": message}})
	return &UpstreamFailoverError{StatusCode: http.StatusBadGateway, ResponseBody: body, RetryableOnSameAccount: true, RequestScopedTransient: true}
}
