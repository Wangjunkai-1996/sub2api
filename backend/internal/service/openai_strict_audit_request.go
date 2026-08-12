package service

import "github.com/gin-gonic/gin"

const openAIStrictAuditRequestContextKey = "openai_strict_audit_request"

// MarkOpenAIStrictAuditRequest records that the current request passed the
// configured strict-group admission gate. Downstream audit-related transforms
// use this marker so traffic from other groups remains untouched.
func MarkOpenAIStrictAuditRequest(c *gin.Context) {
	if c != nil {
		c.Set(openAIStrictAuditRequestContextKey, true)
	}
}

// IsOpenAIStrictAuditRequest reports whether the current request was admitted
// through the configured strict-group gate.
func IsOpenAIStrictAuditRequest(c *gin.Context) bool {
	if c == nil {
		return false
	}
	value, exists := c.Get(openAIStrictAuditRequestContextKey)
	marked, _ := value.(bool)
	return exists && marked
}
