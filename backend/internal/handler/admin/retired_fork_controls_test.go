package admin

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestRetiredGroupSchedulerControlsRejectBeforeMutation(t *testing.T) {
	for _, method := range []string{http.MethodPost, http.MethodPut} {
		for _, field := range []string{`"scheduler_type":null`, `"advanced_scheduler_overrides":{}`} {
			router := newSimpleModeGroupRouter(&stubAdminService{})
			path := "/groups"
			if method == http.MethodPut {
				path += "/1"
			}
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(method, path, bytes.NewBufferString(`{"name":"test","platform":"openai",`+field+`}`))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Contains(t, rec.Body.String(), "retired")
		}
	}
}

func TestRetiredSettingsRejectBeforeMutation(t *testing.T) {
	for _, field := range []string{"openai_window_warmup_enabled", "openai_codex_ticket_v2_enabled", "openai_codex_ticket_harvest_proxy_url"} {
		h := &SettingHandler{}
		router := gin.New()
		router.PUT("/settings", h.UpdateSettings)
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/settings", bytes.NewBufferString(`{"`+field+`":null}`))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "retired")
	}
}
