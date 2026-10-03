package service

import (
	"context"
	"time"
)

// Unrelated scheduler fixtures explicitly model healthy recovery admission.
// The shared cooldown state machine is exercised by the Redis repository tests.
type healthyOpenAI429TestCache struct{}

func (healthyOpenAI429TestCache) AcquireOpenAI429Attempt(context.Context, int64, string, string, time.Duration) (OpenAI429Admission, error) {
	return OpenAI429Admission{Allowed: true, Generation: "healthy"}, nil
}
func (healthyOpenAI429TestCache) FailOpenAI429Attempt(context.Context, int64, string, string, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (healthyOpenAI429TestCache) AcceptOpenAI429Attempt(context.Context, int64, string, string, string, string) (bool, error) {
	return true, nil
}
func (healthyOpenAI429TestCache) RefreshOpenAI429Probe(context.Context, int64, string, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (healthyOpenAI429TestCache) ReleaseOpenAI429Probe(context.Context, int64, string, string, string) (bool, error) {
	return true, nil
}
