package service

import (
	"strings"

	"github.com/tidwall/gjson"
)

// MissingCallIDItemShape contains only an input position and closed protocol categories.
type MissingCallIDItemShape struct {
	Index     int    `json:"index"`
	Type      string `json:"type"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	CallID    string `json:"call_id_shape"`
	Output    string `json:"output_shape"`
}

// MissingCallIDItemShapes runs only for a rejected request. Bound the extra scan
// to the default text request limit and keep at most eight offending items.
func MissingCallIDItemShapes(body []byte) (items []MissingCallIDItemShape, truncated bool) {
	if len(body) > 32<<20 {
		return nil, true
	}
	parseRawJSONView(body).Get("input").ForEach(func(index, item gjson.Result) bool {
		typ := strings.TrimSpace(item.Get("type").String())
		if !isCodexToolCallOutputItemType(typ) {
			return true
		}
		id := item.Get("call_id")
		if strings.TrimSpace(id.String()) != "" {
			return true
		}
		if len(items) == 8 {
			truncated = true
			return false
		}
		items = append(items, MissingCallIDItemShape{
			Index: int(index.Int()), Type: strings.Clone(typ),
			Namespace: diagnosticProtocolValue(item.Get("namespace"), "agent", "codex_app", "codex_tui", "functions"),
			Name: diagnosticProtocolValue(item.Get("name"), "browser_info", "terminal_info", "goal_objective",
				"untrusted_context", "untrusted_content", "ide_context", "proposed_plan", "codex_apps_open_page",
				"create_thread", "send_message_to_thread", "automation_update"),
			CallID: diagnosticJSONShape(id), Output: diagnosticJSONShape(item.Get("output")),
		})
		return true
	})
	return items, truncated
}

func diagnosticProtocolValue(value gjson.Result, known ...string) string {
	if !value.Exists() {
		return "missing"
	}
	for _, token := range known {
		if value.Type == gjson.String && value.Str == token {
			return token
		}
	}
	return "other"
}

func diagnosticJSONShape(value gjson.Result) string {
	switch value.Type {
	case gjson.Null:
		if !value.Exists() {
			return "missing"
		}
		return "null"
	case gjson.String:
		if value.Str == "" {
			return "empty_string"
		}
		if strings.TrimSpace(value.Str) == "" {
			return "blank_string"
		}
		return "string"
	case gjson.Number:
		return "number"
	case gjson.JSON:
		if value.IsArray() {
			return "array"
		}
		return "object"
	default:
		return "boolean"
	}
}
