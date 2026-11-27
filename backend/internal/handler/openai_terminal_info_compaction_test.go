//go:build unit

package handler

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpenAIResponses_TerminalInfoCompactionRoundTrip(t *testing.T) {
	const compactItem = `{"type":"compaction","id":"cmp_test","encrypted_content":"synthetic-state"}`
	const compactStream = `data: {"type":"response.output_item.done","output_index":0,"item":` + compactItem + `}` + "\n\n" +
		`data: {"type":"response.completed","response":{"id":"resp_compact","status":"completed","output":[]}}` + "\n\n"
	const nextStream = `data: {"type":"response.completed","response":{"id":"resp_next","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"continued"}]}]}}` + "\n\n"
	upstream := newAstraProCapturedUpstream(
		preContentSSEResponse(io.NopCloser(strings.NewReader(compactStream))),
		preContentSSEResponse(io.NopCloser(strings.NewReader(nextStream))),
	)
	accounts := []service.Account{{
		ID: 1, Platform: service.PlatformOpenAI, Type: service.AccountTypeAPIKey, Status: service.StatusActive, Schedulable: true,
		Credentials: map[string]any{"api_key": "test", "base_url": "https://api.example.test"},
		Extra:       map[string]any{"openai_responses_supported": true, "openai_passthrough": true},
	}}
	handler := newOpenAIFailoverTestHandlerWithAccounts(t, upstream, accounts)
	router := gin.New()
	groupID := int64(1)
	key := &service.APIKey{ID: 1, GroupID: &groupID, Group: &service.Group{ID: groupID, Platform: service.PlatformOpenAI}, User: &service.User{ID: 1}}
	router.POST("/v1/responses", func(c *gin.Context) {
		c.Set(string(middleware.ContextKeyAPIKey), key)
		c.Set(string(middleware.ContextKeyUser), middleware.AuthSubject{UserID: 1})
	}, handler.Responses)

	post := func(body string) []gjson.Result {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Contains(t, rec.Header().Get("Content-Type"), "text/event-stream")
		var events []gjson.Result
		for line := range strings.Lines(rec.Body.String()) {
			data, ok := strings.CutPrefix(line, "data:")
			data = strings.TrimSpace(data)
			if ok && data != "[DONE]" {
				require.True(t, gjson.Valid(data))
				events = append(events, gjson.Parse(data))
			}
		}
		require.NotEmpty(t, events)
		last := events[len(events)-1]
		require.Equal(t, "response.completed", last.Get("type").String())
		require.Equal(t, "completed", last.Get("response.status").String())
		return events
	}

	events := post(`{"model":"gpt-5.1","stream":true,"input":[
		{"type":"function_call_output","namespace":"agent","name":"terminal_info","output":"synthetic terminal context"},
		{"type":"compaction_trigger"}]}`)
	const endpoint = "https://api.example.test/v1/responses"
	urls, _, bodies := upstream.snapshot()
	require.Equal(t, []string{endpoint}, urls)
	require.True(t, gjson.GetBytes(bodies[0], "stream").Bool())
	require.JSONEq(t, `[
		{"type":"message","role":"user","content":[{"type":"input_text","text":"<external_terminal_info>synthetic terminal context</external_terminal_info>"}]},
		{"type":"compaction_trigger"}]`, gjson.GetBytes(bodies[0], "input").Raw)

	// Codex replays the compaction item received in output_item.done on its next turn.
	var items []gjson.Result
	for _, event := range events {
		if event.Get("type").String() == "response.output_item.done" && event.Get("item.type").String() == "compaction" {
			items = append(items, event.Get("item"))
		}
	}
	require.Len(t, items, 1)
	require.JSONEq(t, compactItem, items[0].Raw)
	nextInput := `[` + items[0].Raw + `,{"role":"user","content":"continue"}]`
	events = post(`{"model":"gpt-5.1","stream":true,"input":` + nextInput + `}`)
	urls, _, bodies = upstream.snapshot()
	require.Equal(t, []string{endpoint, endpoint}, urls)
	require.JSONEq(t, nextInput, gjson.GetBytes(bodies[1], "input").Raw)
	require.Equal(t, "continued", events[len(events)-1].Get("response.output.0.content.0.text").String())
}
