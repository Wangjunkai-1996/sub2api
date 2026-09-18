package service

import (
	"strconv"

	"github.com/gin-gonic/gin"
)

// The receiving gateway opts in. Queue this with committed output, never flush
// it on its own: an attempt that fails over must not publish its timing.
func openAITTFTComment(c *gin.Context, firstTokenMs *int) string {
	if !openAITTFTCommentRequested(c) || firstTokenMs == nil || *firstTokenMs < 0 {
		return ""
	}
	return ": sub2-ttft-ms=" + strconv.Itoa(*firstTokenMs) + "\n\n"
}

func openAITTFTCommentRequested(c *gin.Context) bool {
	return c != nil && c.Request != nil && c.Request.Header.Get("X-Sub2-TTFT") == "1"
}
