package securityaudit

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newStrictAuditLineageTestStore(t *testing.T, ttl time.Duration) (*RedisStrictAuditLineageStore, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	cfg := &config.Config{JWT: config.JWTConfig{Secret: strings.Repeat("s", 32)}}
	cfg.Gateway.OpenAIWS.StickyResponseIDTTLSeconds = int(ttl / time.Second)
	return NewRedisStrictAuditLineageStore(client, cfg), server
}

func allowedStrictAuditSummary(groupID int64) AuditSummary {
	return AuditSummary{
		ParserVersion:      "auditinput/v1",
		ConfigVersion:      7,
		APIKeyID:           91,
		GroupID:            &groupID,
		PreviousResponseID: "resp_parent",
		ParentPromptHash:   strings.Repeat("a", 64),
		PromptHash:         strings.Repeat("b", 64),
		DocumentHash:       strings.Repeat("c", 64),
		NormalizedContext:  "raw bearer secret must never persist",
		RedactedContext:    "redacted context retained in full",
		MediaDigests:       []string{strings.Repeat("d", 64)},
		ContextComplete:    true,
		Verdict:            AuditVerdictAllow,
	}
}

func TestRedisStrictAuditLineageStoreBindsRedactedContextAndLoadsByIdentity(t *testing.T) {
	store, server := newStrictAuditLineageTestStore(t, time.Hour)
	summary := allowedStrictAuditSummary(12)
	require.NoError(t, store.BindAllowedResponse(context.Background(), summary, "resp_child"))

	keys := server.Keys()
	require.Len(t, keys, 1)
	require.True(t, strings.HasPrefix(keys[0], strictAuditLineageKeyPrefix))
	require.NotContains(t, keys[0], "resp_child")
	raw, err := server.Get(keys[0])
	require.NoError(t, err)
	require.NotContains(t, raw, summary.NormalizedContext)
	require.NotContains(t, raw, summary.PreviousResponseID)
	require.Contains(t, raw, summary.RedactedContext)
	var ledger strictAuditLineageLedger
	require.NoError(t, json.Unmarshal([]byte(raw), &ledger))
	require.Equal(t, strictAuditLineageDigest(summary.PreviousResponseID), ledger.ParentResponseHash)
	require.Equal(t, summary.ParentPromptHash, ledger.ParentPromptHash)

	loaded, err := store.Load(context.Background(), LineageLookup{
		GroupID: summary.GroupID, APIKeyID: summary.APIKeyID, PreviousResponseID: "resp_child",
	})
	require.NoError(t, err)
	require.Equal(t, summary.RedactedContext, loaded.NormalizedContext)
	require.Equal(t, summary.RedactedContext, loaded.RedactedContext)
	require.Equal(t, summary.PromptHash, loaded.PromptHash)
	require.Equal(t, summary.MediaDigests, loaded.MediaDigests)
}

func TestRedisStrictAuditLineageStoreRejectsCrossKeyGroupExpiryAndRedisFailure(t *testing.T) {
	store, server := newStrictAuditLineageTestStore(t, time.Hour)
	summary := allowedStrictAuditSummary(12)
	require.NoError(t, store.BindAllowedResponse(context.Background(), summary, "resp_child"))

	otherGroup := int64(13)
	_, err := store.Load(context.Background(), LineageLookup{GroupID: &otherGroup, APIKeyID: summary.APIKeyID, PreviousResponseID: "resp_child"})
	require.ErrorIs(t, err, ErrLineageNotFound)
	_, err = store.Load(context.Background(), LineageLookup{GroupID: summary.GroupID, APIKeyID: summary.APIKeyID + 1, PreviousResponseID: "resp_child"})
	require.ErrorIs(t, err, ErrLineageNotFound)

	server.FastForward(time.Hour + time.Second)
	_, err = store.Load(context.Background(), LineageLookup{GroupID: summary.GroupID, APIKeyID: summary.APIKeyID, PreviousResponseID: "resp_child"})
	require.ErrorIs(t, err, ErrLineageNotFound)

	server.Close()
	_, err = store.Load(context.Background(), LineageLookup{GroupID: summary.GroupID, APIKeyID: summary.APIKeyID, PreviousResponseID: "resp_child"})
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrLineageNotFound)
}

func TestRedisStrictAuditLineageStoreRejectsInvalidAndUnredactedSummaries(t *testing.T) {
	store, _ := newStrictAuditLineageTestStore(t, time.Hour)
	summary := allowedStrictAuditSummary(12)
	summary.RedactedContext = ""
	summary.MediaDigests = nil
	require.ErrorIs(t, store.BindAllowedResponse(context.Background(), summary, "resp_child"), ErrLineageInvalid)

	summary = allowedStrictAuditSummary(12)
	summary.Verdict = "block"
	require.ErrorIs(t, store.BindAllowedResponse(context.Background(), summary, "resp_child"), ErrLineageInvalid)
}

func TestRedisStrictAuditLineageStoreAllowsMediaOnlyContext(t *testing.T) {
	store, _ := newStrictAuditLineageTestStore(t, time.Hour)
	summary := allowedStrictAuditSummary(12)
	summary.NormalizedContext = ""
	summary.RedactedContext = ""

	require.NoError(t, store.BindAllowedResponse(context.Background(), summary, "resp_media_child"))
	loaded, err := store.Load(context.Background(), LineageLookup{
		GroupID: summary.GroupID, APIKeyID: summary.APIKeyID, PreviousResponseID: "resp_media_child",
	})
	require.NoError(t, err)
	require.Empty(t, loaded.RedactedContext)
	require.Equal(t, summary.MediaDigests, loaded.MediaDigests)
}

func TestRedisStrictAuditLineageStoreAllowsLegacyOnlyConfigVersion(t *testing.T) {
	store, _ := newStrictAuditLineageTestStore(t, time.Hour)
	summary := allowedStrictAuditSummary(12)
	summary.ConfigVersion = 0

	require.NoError(t, store.BindAllowedResponse(context.Background(), summary, "resp_legacy_only"))
	loaded, err := store.Load(context.Background(), LineageLookup{
		GroupID: summary.GroupID, APIKeyID: summary.APIKeyID, PreviousResponseID: "resp_legacy_only",
	})
	require.NoError(t, err)
	require.Equal(t, int64(0), loaded.ConfigVersion)

	summary.ConfigVersion = -1
	require.ErrorIs(t, store.BindAllowedResponse(context.Background(), summary, "resp_invalid_version"), ErrLineageInvalid)
}
