package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/auditinput"
	"github.com/Wei-Shaw/sub2api/internal/pkg/pagination"
	"github.com/stretchr/testify/require"
)

type contentModerationTestSettingRepo struct {
	values map[string]string
}

func (r *contentModerationTestSettingRepo) Get(ctx context.Context, key string) (*Setting, error) {
	if value, ok := r.values[key]; ok {
		return &Setting{Key: key, Value: value}, nil
	}
	return nil, ErrSettingNotFound
}

func (r *contentModerationTestSettingRepo) GetValue(ctx context.Context, key string) (string, error) {
	if value, ok := r.values[key]; ok {
		return value, nil
	}
	return "", ErrSettingNotFound
}

func (r *contentModerationTestSettingRepo) Set(ctx context.Context, key, value string) error {
	if r.values == nil {
		r.values = map[string]string{}
	}
	r.values[key] = value
	return nil
}

func (r *contentModerationTestSettingRepo) GetMultiple(ctx context.Context, keys []string) (map[string]string, error) {
	out := map[string]string{}
	for _, key := range keys {
		if value, ok := r.values[key]; ok {
			out[key] = value
		}
	}
	return out, nil
}

func (r *contentModerationTestSettingRepo) SetMultiple(ctx context.Context, settings map[string]string) error {
	if r.values == nil {
		r.values = map[string]string{}
	}
	for key, value := range settings {
		r.values[key] = value
	}
	return nil
}

func (r *contentModerationTestSettingRepo) GetAll(ctx context.Context) (map[string]string, error) {
	out := make(map[string]string, len(r.values))
	for key, value := range r.values {
		out[key] = value
	}
	return out, nil
}

func (r *contentModerationTestSettingRepo) Delete(ctx context.Context, key string) error {
	delete(r.values, key)
	return nil
}

type contentModerationTestRepo struct {
	mu   sync.Mutex
	logs []ContentModerationLog
}

func (r *contentModerationTestRepo) CreateLog(ctx context.Context, log *ContentModerationLog) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if log != nil {
		r.logs = append(r.logs, *log)
	}
	return nil
}

func (r *contentModerationTestRepo) ListLogs(ctx context.Context, filter ContentModerationLogFilter) ([]ContentModerationLog, *pagination.PaginationResult, error) {
	return nil, nil, nil
}

func (r *contentModerationTestRepo) CountFlaggedByUserSince(ctx context.Context, userID int64, since time.Time, excludeCyberPolicy bool) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, log := range r.logs {
		if log.UserID == nil || *log.UserID != userID || !log.Flagged || log.Action == ContentModerationActionHashBlock {
			continue
		}
		if excludeCyberPolicy && log.Action == ContentModerationActionCyberPolicy {
			continue
		}
		if log.CreatedAt.IsZero() || log.CreatedAt.Before(since) {
			continue
		}
		count++
	}
	return count, nil
}

func (r *contentModerationTestRepo) CleanupExpiredLogs(ctx context.Context, hitBefore time.Time, nonHitBefore time.Time) (*ContentModerationCleanupResult, error) {
	return &ContentModerationCleanupResult{}, nil
}

func (r *contentModerationTestRepo) UpdateLogEmailSent(ctx context.Context, id int64, sent bool) error {
	return nil
}

func (r *contentModerationTestRepo) snapshotLogs() []ContentModerationLog {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]ContentModerationLog, len(r.logs))
	copy(out, r.logs)
	return out
}

func requireContentModerationLogCount(t *testing.T, repo *contentModerationTestRepo, want int) []ContentModerationLog {
	t.Helper()
	var logs []ContentModerationLog
	require.Eventually(t, func() bool {
		logs = repo.snapshotLogs()
		return len(logs) == want
	}, time.Second, 10*time.Millisecond)
	return logs
}

func requireRecordedHashCount(t *testing.T, cache *contentModerationTestHashCache, want int) []string {
	t.Helper()
	var hashes []string
	require.Eventually(t, func() bool {
		hashes = cache.snapshotRecorded()
		return len(hashes) == want
	}, time.Second, 10*time.Millisecond)
	return hashes
}

func completeModerationCategoryScores(score float64) map[string]float64 {
	scores := make(map[string]float64, len(contentModerationCategoryOrder))
	for _, category := range contentModerationCategoryOrder {
		scores[category] = score
	}
	return scores
}

func moderationRequestExpectedResultCount(t *testing.T, r *http.Request) int {
	t.Helper()
	var request moderationAPIRequest
	require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
	inputs, ok := request.Input.([]any)
	if !ok || len(inputs) == 0 {
		return 1
	}
	if _, isTextBatch := inputs[0].(string); isTextBatch {
		return len(inputs)
	}
	return 1
}

func writeCompleteModerationResponse(t *testing.T, w http.ResponseWriter, r *http.Request) {
	t.Helper()
	count := moderationRequestExpectedResultCount(t, r)
	results := make([]moderationAPIResult, count)
	for index := range results {
		results[index] = moderationAPIResult{
			Flagged:        false,
			CategoryScores: completeModerationCategoryScores(0.01),
		}
	}
	require.NoError(t, json.NewEncoder(w).Encode(moderationAPIResponse{Results: results}))
}

func strictResponsesBodyWithImages(t *testing.T, imageCount int) []byte {
	t.Helper()
	content := make([]any, 0, imageCount+1)
	content = append(content, map[string]any{
		"type": "input_text",
		"text": strings.Repeat("safe", maxModerationInputRunes/4),
	})
	for index := 0; index < imageCount; index++ {
		content = append(content, map[string]any{
			"type":      "input_image",
			"image_url": "data:image/png;base64," + base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("image-%02d", index))),
		})
	}
	body, err := json.Marshal(map[string]any{
		"input": []any{map[string]any{
			"type":    "message",
			"role":    "user",
			"content": content,
		}},
	})
	require.NoError(t, err)
	return body
}

type contentModerationTestHashCache struct {
	mu            sync.Mutex
	hashes        map[string]struct{}
	recorded      []string
	checked       []string
	deleted       []string
	hasResult     bool
	hasResultUsed bool
}

type contentModerationTestUserRepo struct {
	user    *User
	updated []User
}

func (r *contentModerationTestUserRepo) Create(ctx context.Context, user *User) error {
	panic("unexpected Create call")
}

func (r *contentModerationTestUserRepo) CreateWithEmailAliasGuard(ctx context.Context, user *User) error {
	panic("unexpected CreateWithEmailAliasGuard call")
}

func (r *contentModerationTestUserRepo) GetByID(ctx context.Context, id int64) (*User, error) {
	if r.user == nil {
		return nil, ErrUserNotFound
	}
	clone := *r.user
	return &clone, nil
}

func (r *contentModerationTestUserRepo) GetByEmail(ctx context.Context, email string) (*User, error) {
	panic("unexpected GetByEmail call")
}

func (r *contentModerationTestUserRepo) GetFirstAdmin(ctx context.Context) (*User, error) {
	panic("unexpected GetFirstAdmin call")
}

func (r *contentModerationTestUserRepo) Update(ctx context.Context, user *User, fields UserUpdateFields) error {
	if user == nil {
		return nil
	}
	clone := *user
	r.updated = append(r.updated, clone)
	r.user = &clone
	return nil
}

func (r *contentModerationTestUserRepo) Delete(ctx context.Context, id int64) error {
	panic("unexpected Delete call")
}

func (r *contentModerationTestUserRepo) GetUserAvatar(ctx context.Context, userID int64) (*UserAvatar, error) {
	panic("unexpected GetUserAvatar call")
}

func (r *contentModerationTestUserRepo) UpsertUserAvatar(ctx context.Context, userID int64, input UpsertUserAvatarInput) (*UserAvatar, error) {
	panic("unexpected UpsertUserAvatar call")
}

func (r *contentModerationTestUserRepo) DeleteUserAvatar(ctx context.Context, userID int64) error {
	panic("unexpected DeleteUserAvatar call")
}

func (r *contentModerationTestUserRepo) List(ctx context.Context, params pagination.PaginationParams) ([]User, *pagination.PaginationResult, error) {
	panic("unexpected List call")
}

func (r *contentModerationTestUserRepo) ListWithFilters(ctx context.Context, params pagination.PaginationParams, filters UserListFilters) ([]User, *pagination.PaginationResult, error) {
	panic("unexpected ListWithFilters call")
}

func (r *contentModerationTestUserRepo) GetLatestUsedAtByUserIDs(ctx context.Context, userIDs []int64) (map[int64]*time.Time, error) {
	panic("unexpected GetLatestUsedAtByUserIDs call")
}

func (r *contentModerationTestUserRepo) GetLatestUsedAtByUserID(ctx context.Context, userID int64) (*time.Time, error) {
	panic("unexpected GetLatestUsedAtByUserID call")
}

func (r *contentModerationTestUserRepo) UpdateUserLastActiveAt(ctx context.Context, userID int64, activeAt time.Time) error {
	panic("unexpected UpdateUserLastActiveAt call")
}

func (r *contentModerationTestUserRepo) UpdateBalance(ctx context.Context, id int64, amount float64) error {
	panic("unexpected UpdateBalance call")
}

func (r *contentModerationTestUserRepo) DeductBalance(ctx context.Context, id int64, amount float64) error {
	panic("unexpected DeductBalance call")
}

func (r *contentModerationTestUserRepo) AdjustBalance(ctx context.Context, id int64, delta float64) (BalanceChange, error) {
	panic("unexpected AdjustBalance call")
}

func (r *contentModerationTestUserRepo) SetBalance(ctx context.Context, id int64, value float64) (BalanceChange, error) {
	panic("unexpected SetBalance call")
}

func (r *contentModerationTestUserRepo) UpdateConcurrency(ctx context.Context, id int64, amount int) error {
	panic("unexpected UpdateConcurrency call")
}

func (r *contentModerationTestUserRepo) BatchSetConcurrency(ctx context.Context, userIDs []int64, value int) (int, error) {
	panic("unexpected BatchSetConcurrency call")
}

func (r *contentModerationTestUserRepo) BatchAddConcurrency(ctx context.Context, userIDs []int64, delta int) (int, error) {
	panic("unexpected BatchAddConcurrency call")
}
func (r *contentModerationTestUserRepo) BatchUpdateLimits(ctx context.Context, userIDs []int64, concurrency, rpmLimit *int) (int, error) {
	panic("unexpected BatchUpdateLimits call")
}

func (r *contentModerationTestUserRepo) ExistsByEmail(ctx context.Context, email string) (bool, error) {
	panic("unexpected ExistsByEmail call")
}

func (r *contentModerationTestUserRepo) ExistsByEmailAlias(ctx context.Context, email string) (bool, error) {
	panic("unexpected ExistsByEmailAlias call")
}

func (r *contentModerationTestUserRepo) RemoveGroupFromAllowedGroups(ctx context.Context, groupID int64) (int64, error) {
	panic("unexpected RemoveGroupFromAllowedGroups call")
}

func (r *contentModerationTestUserRepo) AddGroupToAllowedGroups(ctx context.Context, userID int64, groupID int64) error {
	panic("unexpected AddGroupToAllowedGroups call")
}

func (r *contentModerationTestUserRepo) RemoveGroupFromUserAllowedGroups(ctx context.Context, userID int64, groupID int64) error {
	panic("unexpected RemoveGroupFromUserAllowedGroups call")
}

func (r *contentModerationTestUserRepo) ListUserAuthIdentities(ctx context.Context, userID int64) ([]UserAuthIdentityRecord, error) {
	panic("unexpected ListUserAuthIdentities call")
}

func (r *contentModerationTestUserRepo) UnbindUserAuthProvider(ctx context.Context, userID int64, provider string) error {
	panic("unexpected UnbindUserAuthProvider call")
}

func (r *contentModerationTestUserRepo) UpdateTotpSecret(ctx context.Context, userID int64, encryptedSecret *string) error {
	panic("unexpected UpdateTotpSecret call")
}

func (r *contentModerationTestUserRepo) EnableTotp(ctx context.Context, userID int64) error {
	panic("unexpected EnableTotp call")
}

func (r *contentModerationTestUserRepo) DisableTotp(ctx context.Context, userID int64) error {
	panic("unexpected DisableTotp call")
}

func (r *contentModerationTestUserRepo) GetByIDIncludeDeleted(ctx context.Context, id int64) (*User, error) {
	return r.GetByID(ctx, id)
}

type contentModerationTestAuthCacheInvalidator struct {
	userIDs []int64
}

func (i *contentModerationTestAuthCacheInvalidator) InvalidateAuthCacheByKey(ctx context.Context, key string) {
}

func (i *contentModerationTestAuthCacheInvalidator) InvalidateAuthCacheByUserID(ctx context.Context, userID int64) {
	i.userIDs = append(i.userIDs, userID)
}

func (i *contentModerationTestAuthCacheInvalidator) InvalidateAuthCacheByGroupID(ctx context.Context, groupID int64) {
}

func (c *contentModerationTestHashCache) RecordFlaggedInputHash(ctx context.Context, inputHash string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.hashes == nil {
		c.hashes = map[string]struct{}{}
	}
	c.hashes[inputHash] = struct{}{}
	c.recorded = append(c.recorded, inputHash)
	return nil
}

func (c *contentModerationTestHashCache) HasFlaggedInputHash(ctx context.Context, inputHash string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checked = append(c.checked, inputHash)
	if c.hasResultUsed {
		return c.hasResult, nil
	}
	_, ok := c.hashes[inputHash]
	return ok, nil
}

func (c *contentModerationTestHashCache) DeleteFlaggedInputHash(ctx context.Context, inputHash string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleted = append(c.deleted, inputHash)
	if c.hashes == nil {
		return false, nil
	}
	if _, ok := c.hashes[inputHash]; !ok {
		return false, nil
	}
	delete(c.hashes, inputHash)
	return true, nil
}

func (c *contentModerationTestHashCache) ClearFlaggedInputHashes(ctx context.Context) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	deleted := int64(len(c.hashes))
	c.hashes = map[string]struct{}{}
	return deleted, nil
}

func (c *contentModerationTestHashCache) CountFlaggedInputHashes(ctx context.Context) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return int64(len(c.hashes)), nil
}

func (c *contentModerationTestHashCache) snapshotRecorded() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.recorded))
	copy(out, c.recorded)
	return out
}

func (c *contentModerationTestHashCache) snapshotChecked() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.checked))
	copy(out, c.checked)
	return out
}

func (c *contentModerationTestHashCache) hasHash(inputHash string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.hashes[inputHash]
	return ok
}

func (c *contentModerationTestHashCache) snapshotDeleted() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.deleted))
	copy(out, c.deleted)
	return out
}

func TestBuildContentModerationLog_RedactsInputExcerpt(t *testing.T) {
	svc := &ContentModerationService{}
	cfg := defaultContentModerationConfig()
	input := ContentModerationCheckInput{
		RequestID: "req-1",
		Endpoint:  "/v1/chat/completions",
		Provider:  "openai",
	}

	log := svc.buildLog(input, cfg, ContentModerationActionAllow, true, "sexual", 0.8, map[string]float64{"sexual": 0.8}, "hello sk-proj-1234567890abcdef", nil, nil, "")

	require.NotContains(t, log.InputExcerpt, "sk-proj-1234567890abcdef")
	require.Contains(t, log.InputExcerpt, "[已脱敏]")
}

func TestRedactContentModerationSecrets_LongHexAndTokens(t *testing.T) {
	input := "你哈市多大事cf5bbdc4cd508f3aaf0d2070d529d4a4ac29099f8ecc357f696df28e1df91554 token=abc123456789xyz Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.signaturepart https://example.com/private/path?token=abc123"

	out := redactContentModerationSecrets(input)

	require.NotContains(t, out, "cf5bbdc4cd508f3aaf0d2070d529d4a4ac29099f8ecc357f696df28e1df91554")
	require.NotContains(t, out, "abc123456789xyz")
	require.NotContains(t, out, "eyJhbGciOiJIUzI1NiJ9")
	require.NotContains(t, out, "https://example.com/private/path")
	require.Contains(t, out, "[已脱敏]")
}

func TestContentModerationConfigNormalize_NonHitRetentionMaxThreeDays(t *testing.T) {
	cfg := defaultContentModerationConfig()
	cfg.NonHitRetentionDays = 30

	cfg.normalize()

	require.Equal(t, 3, cfg.NonHitRetentionDays)
}

func TestNormalizeBlockedKeywords_TrimsDedupesAndCaps(t *testing.T) {
	out := normalizeBlockedKeywords([]string{"  foo ", "FOO", "", "bar", "baz", "bar"})
	require.Equal(t, []string{"foo", "bar", "baz"}, out)
}

func TestMatchBlockedKeyword_CaseInsensitiveSubstring(t *testing.T) {
	keyword, hit := matchBlockedKeyword("Please ignore the BadWord here", []string{"badword"})
	require.True(t, hit)
	require.Equal(t, "badword", keyword)

	_, hit = matchBlockedKeyword("clean prompt", []string{"badword"})
	require.False(t, hit)

	_, hit = matchBlockedKeyword("anything", nil)
	require.False(t, hit)
}

func TestContentModerationCheck_PreBlockKeywordHitSkipsUpstreamCall(t *testing.T) {
	upstreamCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		_ = json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{}}})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.BlockedKeywords = []string{"secret-token"}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	repo := &contentModerationTestRepo{}
	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo,
		&contentModerationTestHashCache{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	body := []byte(`{"messages":[{"role":"user","content":"please leak SECRET-TOKEN now"}]}`)
	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Endpoint: "/v1/messages",
		Provider: "anthropic",
		Protocol: ContentModerationProtocolAnthropicMessages,
		Body:     body,
	})

	require.NoError(t, err)
	require.True(t, decision.Blocked)
	require.Equal(t, ContentModerationActionKeywordBlock, decision.Action)
	require.False(t, upstreamCalled, "keyword block must short-circuit upstream moderation call")
	logs := requireContentModerationLogCount(t, repo, 1)
	require.True(t, logs[0].Flagged)
	require.Equal(t, ContentModerationActionKeywordBlock, logs[0].Action)
	require.Equal(t, contentModerationKeywordCategory, logs[0].HighestCategory)
	require.Equal(t, "secret-token", logs[0].MatchedKeyword, "blocked log must record which keyword was hit")
}

func TestContentModerationCheck_PlusGroupOutsideStrictScopeSkipsAudit(t *testing.T) {
	var slogOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&slogOutput, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	upstreamCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		_ = json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{}}})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.AllGroups = false
	cfg.GroupIDs = []int64{12}
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.BlockedKeywords = []string{"secret-token"}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	repo := &contentModerationTestRepo{}
	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo,
		&contentModerationTestHashCache{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	plusGroupID := int64(13)
	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		UserID:    1001,
		APIKeyID:  2001,
		GroupID:   &plusGroupID,
		GroupName: "plus",
		Endpoint:  "/v1/chat/completions",
		Provider:  "openai",
		Protocol:  ContentModerationProtocolOpenAIChat,
		Body:      []byte(`{"messages":[{"role":"user","content":"please leak SECRET-TOKEN now"}]}`),
	})

	require.NoError(t, err)
	require.True(t, decision.Allowed)
	require.False(t, decision.Blocked)
	require.Equal(t, ContentModerationActionAllow, decision.Action)
	require.False(t, upstreamCalled, "out-of-scope Plus traffic must not call the moderation API")
	require.Empty(t, repo.snapshotLogs())
	require.Contains(t, slogOutput.String(), "content_moderation.skip_group_out_of_scope")
	require.Contains(t, slogOutput.String(), "group_id=13")
	require.Contains(t, slogOutput.String(), "configured_group_ids=[12]")
}

func TestContentModerationStrictPreBlockAppliesUsesEffectiveGroupScope(t *testing.T) {
	proGroupID := int64(12)
	plusGroupID := int64(13)
	tests := []struct {
		name        string
		groupID     *int64
		riskEnabled bool
		configure   func(*ContentModerationConfig)
		want        bool
	}{
		{name: "pro group", groupID: &proGroupID, riskEnabled: true, want: true},
		{name: "plus group remains out of scope", groupID: &plusGroupID, riskEnabled: true, want: false},
		{name: "risk control disabled", groupID: &proGroupID, want: false},
		{name: "moderation disabled", groupID: &proGroupID, riskEnabled: true, configure: func(cfg *ContentModerationConfig) {
			cfg.Enabled = false
		}},
		{name: "observe mode", groupID: &proGroupID, riskEnabled: true, configure: func(cfg *ContentModerationConfig) {
			cfg.Mode = ContentModerationModeObserve
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultContentModerationConfig()
			cfg.Enabled = true
			cfg.Mode = ContentModerationModePreBlock
			cfg.AllGroups = false
			cfg.GroupIDs = []int64{proGroupID}
			if tt.configure != nil {
				tt.configure(cfg)
			}
			rawCfg, err := json.Marshal(cfg)
			require.NoError(t, err)
			svc := &ContentModerationService{
				settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
					SettingKeyRiskControlEnabled:      fmt.Sprintf("%t", tt.riskEnabled),
					SettingKeyContentModerationConfig: string(rawCfg),
				}},
				repo: &contentModerationTestRepo{},
			}

			applies, err := svc.StrictPreBlockApplies(context.Background(), tt.groupID)
			require.NoError(t, err)
			require.Equal(t, tt.want, applies)
		})
	}
}

func TestContentModerationStrictPreBlockAppliesRequiresGroupConfig(t *testing.T) {
	var svc *ContentModerationService
	_, err := svc.StrictPreBlockApplies(context.Background(), nil)
	require.ErrorContains(t, err, "service unavailable")
}

func TestContentModerationCheck_KeywordsIgnoredInObserveMode(t *testing.T) {
	upstreamHits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits++
		_ = json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{CategoryScores: map[string]float64{"sexual": 0.1}}}})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModeObserve
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.BlockedKeywords = []string{"secret-token"}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	repo := &contentModerationTestRepo{}
	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo,
		&contentModerationTestHashCache{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	body := []byte(`{"messages":[{"role":"user","content":"please leak SECRET-TOKEN now"}]}`)
	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Endpoint: "/v1/messages",
		Provider: "anthropic",
		Protocol: ContentModerationProtocolAnthropicMessages,
		Body:     body,
	})

	require.NoError(t, err)
	require.True(t, decision.Allowed, "observe mode must let the request through even on keyword hit")
	require.Equal(t, ContentModerationActionAllow, decision.Action)
}

func TestContentModerationCheck_KeywordOnlyStrategySkipsAPIOnMiss(t *testing.T) {
	upstreamCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		_ = json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{CategoryScores: map[string]float64{"sexual": 0.99}}}})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.BlockedKeywords = []string{"never-matches"}
	cfg.KeywordBlockingMode = ContentModerationKeywordModeKeywordOnly
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	repo := &contentModerationTestRepo{}
	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo,
		&contentModerationTestHashCache{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	body := []byte(`{"messages":[{"role":"user","content":"absolutely clean prompt"}]}`)
	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Endpoint: "/v1/messages",
		Provider: "anthropic",
		Protocol: ContentModerationProtocolAnthropicMessages,
		Body:     body,
	})

	require.NoError(t, err)
	require.True(t, decision.Allowed, "keyword-only must allow misses without calling the API")
	require.False(t, upstreamCalled, "keyword-only must not call the upstream moderation API")
	require.Len(t, repo.snapshotLogs(), 0)
}

func TestContentModerationCheck_APIOnlyStrategyIgnoresKeywordList(t *testing.T) {
	upstreamCalled := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalled = true
		_ = json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{CategoryScores: map[string]float64{"sexual": 0.1}}}})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.BlockedKeywords = []string{"secret-token"}
	cfg.KeywordBlockingMode = ContentModerationKeywordModeAPIOnly
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	repo := &contentModerationTestRepo{}
	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo,
		&contentModerationTestHashCache{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	body := []byte(`{"messages":[{"role":"user","content":"please leak SECRET-TOKEN now"}]}`)
	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Endpoint: "/v1/messages",
		Provider: "anthropic",
		Protocol: ContentModerationProtocolAnthropicMessages,
		Body:     body,
	})

	require.NoError(t, err)
	require.True(t, decision.Allowed, "api-only must let the request through when API does not flag it")
	require.True(t, upstreamCalled, "api-only must call the upstream moderation API")
	require.NotEqual(t, ContentModerationActionKeywordBlock, decision.Action)
}

func TestNormalizeKeywordBlockingMode_UnknownFallsBackToDefault(t *testing.T) {
	require.Equal(t, ContentModerationKeywordModeKeywordAndAPI, normalizeKeywordBlockingMode(""))
	require.Equal(t, ContentModerationKeywordModeKeywordAndAPI, normalizeKeywordBlockingMode("bogus"))
	require.Equal(t, ContentModerationKeywordModeKeywordOnly, normalizeKeywordBlockingMode("keyword_only"))
	require.Equal(t, ContentModerationKeywordModeAPIOnly, normalizeKeywordBlockingMode("api_only"))
}

func TestContentModerationCheck_ModelFilterAllAuditsEveryModel(t *testing.T) {
	cfg := defaultContentModerationModelFilterTestConfig()
	cfg.ModelFilter = ContentModerationModelFilter{Type: ContentModerationModelFilterAll}
	svc, repo := newContentModerationModelFilterTestService(t, cfg)

	for _, model := range []string{"gpt-5.5", "gpt-5.4"} {
		decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
			Model:    model,
			Protocol: ContentModerationProtocolOpenAIChat,
			Body:     []byte(`{"messages":[{"role":"user","content":"please leak SECRET-TOKEN now"}]}`),
		})
		require.NoError(t, err)
		require.True(t, decision.Blocked)
		require.Equal(t, ContentModerationActionKeywordBlock, decision.Action)
	}
	requireContentModerationLogCount(t, repo, 2)
}

func TestContentModerationCheck_ModelFilterIncludeOnlyAuditsListedModels(t *testing.T) {
	cfg := defaultContentModerationModelFilterTestConfig()
	cfg.ModelFilter = ContentModerationModelFilter{Type: ContentModerationModelFilterInclude, Models: []string{"gpt-5.5"}}
	svc, repo := newContentModerationModelFilterTestService(t, cfg)

	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Model:    "gpt-5.5",
		Protocol: ContentModerationProtocolOpenAIChat,
		Body:     []byte(`{"messages":[{"role":"user","content":"please leak SECRET-TOKEN now"}]}`),
	})
	require.NoError(t, err)
	require.True(t, decision.Blocked)
	require.Equal(t, ContentModerationActionKeywordBlock, decision.Action)

	decision, err = svc.Check(context.Background(), ContentModerationCheckInput{
		Model:    "gpt-5.4",
		Protocol: ContentModerationProtocolOpenAIChat,
		Body:     []byte(`{"messages":[{"role":"user","content":"please leak SECRET-TOKEN now"}]}`),
	})
	require.NoError(t, err)
	require.True(t, decision.Allowed)
	require.False(t, decision.Blocked)
	require.Equal(t, ContentModerationActionAllow, decision.Action)
	logs := requireContentModerationLogCount(t, repo, 1)
	require.Equal(t, "gpt-5.5", logs[0].Model)
}

func TestContentModerationCheck_ModelFilterExcludeSkipsListedModels(t *testing.T) {
	cfg := defaultContentModerationModelFilterTestConfig()
	cfg.ModelFilter = ContentModerationModelFilter{Type: ContentModerationModelFilterExclude, Models: []string{"gpt-5.4"}}
	svc, repo := newContentModerationModelFilterTestService(t, cfg)

	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Model:    "gpt-5.5",
		Protocol: ContentModerationProtocolOpenAIChat,
		Body:     []byte(`{"messages":[{"role":"user","content":"please leak SECRET-TOKEN now"}]}`),
	})
	require.NoError(t, err)
	require.True(t, decision.Blocked)
	require.Equal(t, ContentModerationActionKeywordBlock, decision.Action)

	decision, err = svc.Check(context.Background(), ContentModerationCheckInput{
		Model:    "gpt-5.4",
		Protocol: ContentModerationProtocolOpenAIChat,
		Body:     []byte(`{"messages":[{"role":"user","content":"please leak SECRET-TOKEN now"}]}`),
	})
	require.NoError(t, err)
	require.True(t, decision.Allowed)
	require.False(t, decision.Blocked)
	require.Equal(t, ContentModerationActionAllow, decision.Action)
	logs := requireContentModerationLogCount(t, repo, 1)
	require.Equal(t, "gpt-5.5", logs[0].Model)
}

func TestContentModerationLoadConfig_LegacyConfigDefaultsModelFilterToAll(t *testing.T) {
	raw := `{"enabled":true,"mode":"pre_block","base_url":"https://api.openai.com","model":"omni-moderation-latest","blocked_keywords":["secret-token"]}`
	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyContentModerationConfig: raw,
		}},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	cfg, err := svc.loadConfig(context.Background())

	require.NoError(t, err)
	require.Equal(t, ContentModerationModelFilterAll, cfg.ModelFilter.Type)
	require.Empty(t, cfg.ModelFilter.Models)
	require.True(t, cfg.includesModel("gpt-5.5"))
	require.True(t, cfg.includesModel("gpt-5.4"))
}

func TestContentModerationCheck_ModelFilterUsesRequestedModelNotBodyModel(t *testing.T) {
	cfg := defaultContentModerationModelFilterTestConfig()
	cfg.ModelFilter = ContentModerationModelFilter{Type: ContentModerationModelFilterInclude, Models: []string{"gpt-5.5"}}
	svc, repo := newContentModerationModelFilterTestService(t, cfg)

	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Model:    "gpt-5.5",
		Protocol: ContentModerationProtocolOpenAIChat,
		Body:     []byte(`{"model":"mapped-upstream-model","messages":[{"role":"user","content":"please leak SECRET-TOKEN now"}]}`),
	})

	require.NoError(t, err)
	require.True(t, decision.Blocked)
	require.Equal(t, ContentModerationActionKeywordBlock, decision.Action)
	logs := requireContentModerationLogCount(t, repo, 1)
	require.Equal(t, "gpt-5.5", logs[0].Model)
}

func defaultContentModerationModelFilterTestConfig() *ContentModerationConfig {
	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.BlockedKeywords = []string{"secret-token"}
	return cfg
}

func newContentModerationModelFilterTestService(t *testing.T, cfg *ContentModerationConfig) (*ContentModerationService, *contentModerationTestRepo) {
	t.Helper()
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)
	repo := &contentModerationTestRepo{}
	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo,
		&contentModerationTestHashCache{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	return svc, repo
}

func TestContentModerationUpdateConfig_AppendsAndDeletesAPIKeys(t *testing.T) {
	cfg := defaultContentModerationConfig()
	cfg.APIKeys = []string{"sk-old-a", "sk-old-b"}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	repo := &contentModerationTestSettingRepo{values: map[string]string{
		SettingKeyContentModerationConfig: string(rawCfg),
	}}
	svc := NewContentModerationService(repo, nil, nil, nil, nil, nil, nil, nil)
	deleteHashes := []string{moderationAPIKeyHash("sk-old-a")}
	addKeys := []string{"sk-new-c", "sk-old-b"}

	view, err := svc.UpdateConfig(context.Background(), UpdateContentModerationConfigInput{
		APIKeys:            &addKeys,
		DeleteAPIKeyHashes: &deleteHashes,
	})

	require.NoError(t, err)
	require.Equal(t, 2, view.APIKeyCount)
	require.Equal(t, []string{maskSecretTail("sk-old-b"), maskSecretTail("sk-new-c")}, view.APIKeyMasks)

	var saved ContentModerationConfig
	require.NoError(t, json.Unmarshal([]byte(repo.values[SettingKeyContentModerationConfig]), &saved))
	require.Equal(t, []string{"sk-old-b", "sk-new-c"}, saved.apiKeys())
}

func TestContentModerationUpdateConfig_ReplacesAPIKeysWhenRequested(t *testing.T) {
	cfg := defaultContentModerationConfig()
	cfg.APIKeys = []string{"sk-old-a", "sk-old-b"}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	repo := &contentModerationTestSettingRepo{values: map[string]string{
		SettingKeyContentModerationConfig: string(rawCfg),
	}}
	svc := NewContentModerationService(repo, nil, nil, nil, nil, nil, nil, nil)
	deleteHashes := []string{moderationAPIKeyHash("sk-old-a")}
	replaceKeys := []string{"sk-new-only"}

	view, err := svc.UpdateConfig(context.Background(), UpdateContentModerationConfigInput{
		APIKeys:            &replaceKeys,
		APIKeysMode:        contentModerationAPIKeysModeReplace,
		DeleteAPIKeyHashes: &deleteHashes,
	})

	require.NoError(t, err)
	require.Equal(t, 1, view.APIKeyCount)
	require.Equal(t, []string{maskSecretTail("sk-new-only")}, view.APIKeyMasks)

	var saved ContentModerationConfig
	require.NoError(t, json.Unmarshal([]byte(repo.values[SettingKeyContentModerationConfig]), &saved))
	require.Equal(t, []string{"sk-new-only"}, saved.apiKeys())
}

func TestContentModerationUpdateConfig_SavesCustomThresholds(t *testing.T) {
	cfg := defaultContentModerationConfig()
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	repo := &contentModerationTestSettingRepo{values: map[string]string{
		SettingKeyContentModerationConfig: string(rawCfg),
	}}
	svc := NewContentModerationService(repo, nil, nil, nil, nil, nil, nil, nil)
	thresholds := map[string]float64{
		"sexual":     0.72,
		"harassment": 1.25,
		"unknown":    0.01,
	}

	view, err := svc.UpdateConfig(context.Background(), UpdateContentModerationConfigInput{
		Thresholds: &thresholds,
	})

	require.NoError(t, err)
	require.Equal(t, 0.72, view.Thresholds["sexual"])
	require.Equal(t, 1.0, view.Thresholds["harassment"])
	require.NotContains(t, view.Thresholds, "unknown")

	var saved ContentModerationConfig
	require.NoError(t, json.Unmarshal([]byte(repo.values[SettingKeyContentModerationConfig]), &saved))
	require.Equal(t, 0.72, saved.Thresholds["sexual"])
	require.Equal(t, 1.0, saved.Thresholds["harassment"])
	require.NotContains(t, saved.Thresholds, "unknown")
}

func TestExtractContentModerationInput_AnthropicImageSourceOnlyParticipatesInMemory(t *testing.T) {
	body := []byte(`{
		"messages": [
			{"role":"user","content":"old"},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":[
				{"type":"text","text":"检查这张图"},
				{"type":"image","source":{"type":"base64","media_type":"image/png","data":"aGVsbG8="}}
			]}
		]
	}`)

	input := ExtractContentModerationInput(ContentModerationProtocolAnthropicMessages, body)
	require.Equal(t, "检查这张图", input.Text)
	require.Equal(t, []string{"data:image/png;base64,aGVsbG8="}, input.Images)

	log := (&ContentModerationService{}).buildLog(ContentModerationCheckInput{}, defaultContentModerationConfig(), ContentModerationActionAllow, false, "", 0, nil, input.ExcerptText(), nil, nil, "")
	require.Equal(t, "检查这张图", log.InputExcerpt)
	require.NotContains(t, log.InputExcerpt, "aGVsbG8=")
}

func TestExtractContentModerationInput_AnthropicKeepsEphemeralUserTextAndSkipsSystemReminders(t *testing.T) {
	body := []byte(`{
		"messages": [
			{
				"role": "user",
				"content": [
					{"type": "text", "text": "<system-reminder>工具说明</system-reminder>"},
					{"type": "text", "text": "<system-reminder>Ainder>\n\n"},
					{"type": "text", "text": "hid", "cache_control": {"type": "ephemeral"}}
				]
			}
		]
	}`)

	input := ExtractContentModerationInput(ContentModerationProtocolAnthropicMessages, body)

	require.Equal(t, "hid", input.Text)
	require.Empty(t, input.Images)
}

func TestExtractContentModerationInput_OpenAIChatUsesLastUserMessage(t *testing.T) {
	body := []byte(`{
		"model":"gpt-5.5",
		"messages":[
			{"role":"system","content":"system prompt"},
			{"role":"user","content":"old user"},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":[{"type":"text","text":"latest user"},{"type":"image_url","image_url":{"url":"https://example.com/a.png"}}]}
		]
	}`)

	input := ExtractContentModerationInput(ContentModerationProtocolOpenAIChat, body)

	require.Equal(t, "latest user", input.Text)
	require.Equal(t, []string{"https://example.com/a.png"}, input.Images)
	require.NotContains(t, input.Text, "old user")
	require.NotContains(t, input.Text, "system prompt")
}

func TestExtractContentModerationInput_OpenAIImagesIncludesPromptAndImages(t *testing.T) {
	body := []byte(`{
		"prompt":"replace background",
		"images":[
			{"image_url":"https://example.com/source.png"},
			{"image_url":"data:image/png;base64,aGVsbG8="}
		]
	}`)

	input := ExtractContentModerationInput(ContentModerationProtocolOpenAIImages, body)

	require.Equal(t, "replace background", input.Text)
	require.Equal(t, []string{"https://example.com/source.png", "data:image/png;base64,aGVsbG8="}, input.Images)
}

func TestContentModerationInput_NormalizeKeepsImagesAndModerationInputSamplesOneImage(t *testing.T) {
	images := []string{
		"data:image/png;base64,Zmlyc3Q=",
		"data:image/png;base64,c2Vjb25k",
	}
	input := ContentModerationInput{
		Text:   "check image",
		Images: append([]string(nil), images...),
	}
	input.Normalize()

	require.Equal(t, images, input.Images)

	parts, ok := input.ModerationInput().([]moderationAPIInputPart)
	require.True(t, ok)
	require.Len(t, parts, 2)
	require.Equal(t, "text", parts[0].Type)
	require.Equal(t, "image_url", parts[1].Type)
	require.NotNil(t, parts[1].ImageURL)
	require.Contains(t, images, parts[1].ImageURL.URL)
}

func TestStrictModerationBatchesUsesOneTextInputAndIgnoresImages(t *testing.T) {
	document := &auditinput.Document{
		NormalizedText: "current user text",
		HasImages:      true,
	}

	batches, err := strictModerationBatches(document)

	require.NoError(t, err)
	require.Len(t, batches, 1)
	textInput, ok := batches[0].input.(string)
	require.True(t, ok)
	require.Equal(t, "current user text", textInput)
	require.Equal(t, 1, batches[0].expectedResults)
}

func TestStrictModerationBatchesTruncatesUnnormalizedTextToRuneLimit(t *testing.T) {
	want := strings.Repeat("界", maxStrictModerationTextRunes)
	document := &auditinput.Document{
		NormalizedText: want + "尾",
	}

	batches, err := strictModerationBatches(document)

	require.NoError(t, err)
	require.Len(t, batches, 1)
	textInput, ok := batches[0].input.(string)
	require.True(t, ok)
	require.Equal(t, want, textInput)
	require.Equal(t, maxStrictModerationTextRunes, len([]rune(textInput)))
	require.Equal(t, 1, batches[0].expectedResults)
}

func TestBuildModerationTestInputRejectsMultipleImages(t *testing.T) {
	_, _, err := buildModerationTestInput("check image", []string{
		"data:image/png;base64,Zmlyc3Q=",
		"data:image/png;base64,c2Vjb25k",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "最多上传 1 张测试图片")
}

func TestExtractContentModerationInput_OpenAIResponsesCodexPayloadUsesLastUserMessage(t *testing.T) {
	body := []byte(`{
		"model":"gpt-5.5",
		"instructions":"instructions.....",
		"input":[
			{"type":"message","role":"developer","content":[{"type":"input_text","text":"developer permissions sk-proj-1234567890abcdef"}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"first user prompt"}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"last user prompt"}]}
		],
		"prompt_cache_key":"cache-key"
	}`)

	input := ExtractContentModerationInput(ContentModerationProtocolOpenAIResponses, body)

	require.Equal(t, "last user prompt", input.Text)
	require.Empty(t, input.Images)
	require.NotContains(t, input.Text, "developer permissions")
	require.NotContains(t, input.Text, "first user prompt")
}

func TestExtractStrictCurrentUserText(t *testing.T) {
	longText := strings.Repeat("界", maxStrictModerationTextRunes) + strings.Repeat("尾", 17)
	longBody, err := json.Marshal(map[string]any{
		"input": []any{map[string]any{
			"type": "message", "role": "user",
			"content": []any{map[string]any{"type": "input_text", "text": longText}},
		}},
	})
	require.NoError(t, err)

	tests := []struct {
		name      string
		protocol  string
		body      []byte
		want      string
		forbidden string
	}{
		{
			name:     "chat does not rewind past final tool message",
			protocol: ContentModerationProtocolOpenAIChat,
			body: []byte(`{"messages":[
				{"role":"user","content":"historical user"},
				{"role":"assistant","content":"assistant output"},
				{"role":"user","content":"latest user"},
				{"role":"assistant","content":"later assistant output"},
				{"role":"tool","content":"later tool output"}
			]}`),
			forbidden: "later tool output",
		},
		{
			name:     "responses audits final tool output without rewinding history",
			protocol: ContentModerationProtocolOpenAIResponses,
			body: []byte(`{"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"historical user"}]},
				{"type":"message","role":"user","content":[{"type":"input_text","text":"latest user"}]},
				{"type":"message","role":"assistant","content":[{"type":"output_text","text":"assistant output"}]},
				{"type":"function_call_output","call_id":"call_1","output":"tool output"}
			]}`),
			want:      "tool output",
			forbidden: "historical user",
		},
		{
			name:     "latest image-only user does not fall back to history",
			protocol: ContentModerationProtocolOpenAIResponses,
			body: []byte(`{"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"historical user must not be reused"}]},
				{"type":"message","role":"user","content":[{"type":"input_image","image_url":"opaque-image-secret"}]},
				{"type":"function_call_output","call_id":"call_1","output":"tool output"}
			]}`),
			want:      "tool output",
			forbidden: "historical user must not be reused",
		},
		{
			name:     "flat text and image items are one current user input",
			protocol: ContentModerationProtocolOpenAIResponses,
			body: []byte(`{"input":[
				{"type":"input_text","text":"flat user text"},
				{"type":"input_image","image_url":"opaque-image-secret"}
			]}`),
			want:      "flat user text",
			forbidden: "opaque-image-secret",
		},
		{
			name:     "image values never enter text result",
			protocol: ContentModerationProtocolOpenAIChat,
			body: []byte(`{"messages":[{"role":"user","content":[
				{"type":"text","text":"inspect this"},
				{"type":"image_url","image_url":{"url":"data:image/png;base64,IMAGE_SECRET"}}
			]}]}`),
			want:      "inspect this",
			forbidden: "IMAGE_SECRET",
		},
		{
			name:     "multiple text parts are joined and normalized",
			protocol: ContentModerationProtocolOpenAIResponses,
			body: []byte(`{"input":[{"type":"message","role":"user","content":[
				{"type":"input_text","text":" first\npart "},
				{"type":"input_image","image_url":"opaque-image"},
				{"type":"input_text","text":" second   part "}
			]}]}`),
			want:      "first part second part",
			forbidden: "opaque-image",
		},
		{
			name:      "text is truncated at 12000 runes",
			protocol:  ContentModerationProtocolOpenAIResponses,
			body:      longBody,
			want:      strings.Repeat("界", maxStrictModerationTextRunes),
			forbidden: "尾",
		},
		{
			name:     "trailing compaction remains transparent to current user",
			protocol: ContentModerationProtocolOpenAIResponses,
			body: []byte(`{"input":[
				{"type":"message","role":"user","content":"current user text"},
				{"type":"compaction_trigger"}
			]}`),
			want: "current user text",
		},
		{
			name:     "trailing compaction does not cross tool output",
			protocol: ContentModerationProtocolOpenAIResponses,
			body: []byte(`{"input":[
				{"type":"message","role":"user","content":"historical user text"},
				{"type":"function_call_output","call_id":"call_1","output":"tool output"},
				{"type":"compaction_trigger"}
			]}`),
			want:      "tool output",
			forbidden: "historical user text",
		},
		{
			name:     "forward-sanitized empty image cannot hide current user text",
			protocol: ContentModerationProtocolOpenAIResponses,
			body: []byte(`{"input":[
				{"type":"message","role":"user","content":"current user text"},
				{"type":"input_image","image_url":"data:image/png;base64,   "}
			]}`),
			want: "current user text",
		},
		{
			name:     "forward-sanitized empty image does not cross tool output",
			protocol: ContentModerationProtocolOpenAIResponses,
			body: []byte(`{"input":[
				{"type":"message","role":"user","content":"historical user text"},
				{"type":"function_call_output","call_id":"call_1","output":"done"},
				{"type":"input_image","image_url":"data:image/png;base64,"}
			]}`),
			want:      "done",
			forbidden: "historical user text",
		},
		{
			name:     "strict text keeps system-reminder literals",
			protocol: ContentModerationProtocolOpenAIResponses,
			body: []byte(`{"input":[{"type":"message","role":"user","content":[
				{"type":"input_text","text":"safe prefix"},
				{"type":"input_text","text":"<system-reminder> blocked current text"}
			]}]}`),
			want: "safe prefix <system-reminder> blocked current text",
		},
		{
			name:      "responses null role does not become flat user text",
			protocol:  ContentModerationProtocolOpenAIResponses,
			body:      []byte(`{"input":[{"type":"input_text","role":null,"text":"must not be audited"}]}`),
			forbidden: "must not be audited",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractStrictCurrentUserText(tt.protocol, tt.body)
			require.Equal(t, tt.want, got)
			if tt.forbidden != "" {
				require.NotContains(t, got, tt.forbidden)
			}
		})
	}
}

func TestExtractStrictCurrentUserText_ExplicitResponsesTopLevelPayloadWithHistoricalImage(t *testing.T) {
	tests := []struct {
		name string
		body []byte
		want string
	}{
		{
			name: "message top-level text",
			body: []byte(`{"input":[
				{"type":"message","role":"user","content":[{"type":"input_image","image_url":"opaque-history"}]},
				{"type":"message","role":"user","text":"latest top-level text"}
			]}`),
			want: "latest top-level text",
		},
		{
			name: "empty type top-level refusal",
			body: []byte(`{"input":[
				{"type":"message","role":"user","content":[{"type":"input_image","image_url":"opaque-history"}]},
				{"role":"user","refusal":"latest top-level refusal"}
			]}`),
			want: "latest top-level refusal",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			document := auditinput.ParseForTextAudit(ContentModerationProtocolOpenAIResponses, tt.body)
			require.True(t, document.Complete, "%+v", document.Issues)
			require.False(t, document.HasImages, "historical images are outside the current-user text audit")
			require.Empty(t, document.Media)
			require.Contains(t, document.NormalizedText, tt.want)
			require.Equal(t, tt.want, ExtractStrictCurrentUserText(ContentModerationProtocolOpenAIResponses, tt.body))
		})
	}
}

func TestContentModerationCheck_OpenAIResponsesRecordsNonHitForCodexPayload(t *testing.T) {
	var moderationRequest moderationAPIRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/moderations", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&moderationRequest))
		_ = json.NewEncoder(w).Encode(moderationAPIResponse{
			Results: []moderationAPIResult{{
				CategoryScores: map[string]float64{"sexual": 0.01},
			}},
		})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.RecordNonHits = true
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	repo := &contentModerationTestRepo{}
	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo,
		&contentModerationTestHashCache{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	body := []byte(`{
		"model":"gpt-5.5",
		"input":[
			{"type":"message","role":"developer","content":[{"type":"input_text","text":"developer instructions should not be audited"}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"first user prompt"}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"last user prompt"}]}
		]
	}`)
	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		UserID:   1001,
		Endpoint: "/responses",
		Provider: "openai",
		Model:    "gpt-5.5",
		Protocol: ContentModerationProtocolOpenAIResponses,
		Body:     body,
	})

	require.NoError(t, err)
	require.False(t, decision.Blocked)
	logs := requireContentModerationLogCount(t, repo, 1)
	require.False(t, logs[0].Flagged)
	require.Equal(t, ContentModerationActionAllow, logs[0].Action)
	require.Equal(t, "/responses", logs[0].Endpoint)
	require.Equal(t, "last user prompt", logs[0].InputExcerpt)
	require.Equal(t, "last user prompt", moderationRequest.Input)
}

func TestContentModerationCheck_PreBlockBlocksCodexResponsesLatestUserInput(t *testing.T) {
	var moderationRequest moderationAPIRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/moderations", r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&moderationRequest))
		_ = json.NewEncoder(w).Encode(moderationAPIResponse{
			Results: []moderationAPIResult{{
				CategoryScores: map[string]float64{"sexual": 0.9},
			}},
		})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.BlockStatus = http.StatusUnavailableForLegalReasons
	cfg.BlockMessage = "内容审计测试阻断"
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	repo := &contentModerationTestRepo{}
	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo,
		&contentModerationTestHashCache{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	body := []byte(`{
		"model":"gpt-5.5",
		"instructions":"instructions.....",
		"input":[
			{"type":"message","role":"developer","content":[{"type":"input_text","text":"developer instructions should not be audited"}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"environment context"}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"latest blocked prompt"}]}
		]
	}`)
	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		UserID:   1001,
		Endpoint: "/responses",
		Provider: "openai",
		Model:    "gpt-5.5",
		Protocol: ContentModerationProtocolOpenAIResponses,
		Body:     body,
	})

	require.NoError(t, err)
	require.True(t, decision.Blocked)
	require.Equal(t, ContentModerationActionBlock, decision.Action)
	require.Equal(t, http.StatusUnavailableForLegalReasons, decision.StatusCode)
	require.Equal(t, "内容审计测试阻断", decision.Message)
	logs := requireContentModerationLogCount(t, repo, 1)
	require.True(t, logs[0].Flagged)
	require.Equal(t, ContentModerationActionBlock, logs[0].Action)
	require.Equal(t, ContentModerationModePreBlock, logs[0].Mode)
	require.Equal(t, "latest blocked prompt", logs[0].InputExcerpt)
	require.Equal(t, "latest blocked prompt", moderationRequest.Input)
}

func TestContentModerationCheck_StrictSendsOnlyCurrentUserText(t *testing.T) {
	groupID := int64(12)
	tests := []struct {
		name     string
		protocol string
		body     []byte
		want     string
	}{
		{
			name:     "chat completions",
			protocol: ContentModerationProtocolOpenAIChat,
			body: []byte(`{
				"model":"gpt-5.5",
				"messages":[
					{"role":"system","content":"system secret"},
					{"role":"developer","content":"developer secret"},
					{"role":"user","content":"historical user text"},
					{"role":"assistant","content":"assistant output"},
					{"role":"tool","content":"tool output"},
						{"role":"user","content":[
							{"type":"text","text":"  current   chat user text  ","annotations":[{"label":"annotation must not be audited"}],"logprobs":[{"token":"logprob must not be audited"}]},
						{"type":"image_url","image_url":{"url":"opaque-image-payload"}}
					]}
				],
				"tools":[{"type":"function","function":{"name":"secret_tool","description":"tool schema secret"}}]
			}`),
			want: "current chat user text",
		},
		{
			name:     "responses",
			protocol: ContentModerationProtocolOpenAIResponses,
			body: []byte(`{
				"model":"gpt-5.5",
				"instructions":"system instructions secret",
				"tools":[{"type":"function","name":"secret_tool","description":"tool schema secret"}],
				"input":[
					{"type":"message","role":"developer","content":[{"type":"input_text","text":"developer secret"}]},
					{"type":"message","role":"user","content":[{"type":"input_text","text":"historical user text"}]},
					{"type":"message","role":"assistant","content":[{"type":"output_text","text":"assistant output"}]},
					{"type":"function_call_output","call_id":"call_1","output":"tool output"},
					{"type":"message","role":"user","name":"metadata must not be audited","content":[
						{"type":"input_text","text":"current responses user text","annotations":[{"label":"annotation must not be audited"}],"logprobs":[{"token":"logprob must not be audited"}]},
						{"type":"input_image","image_url":"opaque-image-payload"}
					]}
				]
			}`),
			want: "current responses user text",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			document := auditinput.ParseForTextAudit(tt.protocol, tt.body)
			require.True(t, document.Complete, "%+v", document.Issues)
			require.Equal(t, tt.want, document.NormalizedText)
			require.Equal(t, tt.want, ExtractStrictCurrentUserText(tt.protocol, tt.body))

			var mu sync.Mutex
			requestCount := 0
			var gotInput any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request moderationAPIRequest
				require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
				mu.Lock()
				requestCount++
				gotInput = request.Input
				mu.Unlock()
				require.NoError(t, json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{
					Flagged: false, CategoryScores: completeModerationCategoryScores(0.01),
				}}}))
			}))
			defer server.Close()

			cfg := defaultContentModerationConfig()
			cfg.Enabled = true
			cfg.Mode = ContentModerationModePreBlock
			cfg.AllGroups = false
			cfg.GroupIDs = []int64{groupID}
			cfg.BaseURL = server.URL
			cfg.APIKeys = []string{"sk-test"}
			cfg.RetryCount = 5
			rawCfg, err := json.Marshal(cfg)
			require.NoError(t, err)
			svc := &ContentModerationService{
				settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
					SettingKeyRiskControlEnabled:      "true",
					SettingKeyContentModerationConfig: string(rawCfg),
				}},
				repo:       &contentModerationTestRepo{},
				httpClient: server.Client(),
				asyncQueue: make(chan contentModerationTask, 1),
				keyHealth:  make(map[string]*contentModerationKeyHealth),
			}

			decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
				Strict:       true,
				APIKeyID:     7,
				GroupID:      &groupID,
				Model:        "gpt-5.5",
				Protocol:     tt.protocol,
				Body:         tt.body,
				AuditContext: "system context secret\ndeveloper context secret\nhistorical replay\nassistant replay\ntool output replay",
			})

			require.NoError(t, err)
			require.NotNil(t, decision)
			require.True(t, decision.Allowed)
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, 1, requestCount)
			require.Equal(t, tt.want, gotInput)
		})
	}
}

func TestContentModerationCheck_StrictCompleteNoCurrentUserTextAllowsWithoutRemoteCall(t *testing.T) {
	groupID := int64(12)
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.AllGroups = false
	cfg.GroupIDs = []int64{groupID}
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)
	svc := &ContentModerationService{
		settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo:       &contentModerationTestRepo{},
		httpClient: server.Client(),
		asyncQueue: make(chan contentModerationTask, 1),
		keyHealth:  make(map[string]*contentModerationKeyHealth),
	}

	tests := []struct {
		name     string
		protocol string
		body     string
	}{
		{name: "responses missing input", protocol: ContentModerationProtocolOpenAIResponses, body: `{"model":"gpt-5.5"}`},
		{name: "responses null input", protocol: ContentModerationProtocolOpenAIResponses, body: `{"model":"gpt-5.5","input":null}`},
		{name: "responses empty websocket input", protocol: ContentModerationProtocolOpenAIResponses, body: `{"type":"response.create","model":"gpt-5.5","input":[]}`},
		{name: "responses empty continuation", protocol: ContentModerationProtocolOpenAIResponses, body: `{"previous_response_id":"resp_parent","input":[]}`},
		{name: "responses empty string input", protocol: ContentModerationProtocolOpenAIResponses, body: `{"input":""}`},
		{name: "responses empty input text", protocol: ContentModerationProtocolOpenAIResponses, body: `{"input":[{"type":"input_text","text":""}]}`},
		{name: "responses empty message content", protocol: ContentModerationProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","content":""}]}`},
		{name: "responses name metadata only", protocol: ContentModerationProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","name":"metadata only","content":""}]}`},
		{name: "responses content metadata only", protocol: ContentModerationProtocolOpenAIResponses, body: `{"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"","annotations":[{"label":"metadata only"}],"logprobs":[{"token":"metadata only"}]}]}]}`},
		{name: "responses top-level text metadata only", protocol: ContentModerationProtocolOpenAIResponses, body: `{"input":[{"type":"input_text","text":"","annotations":[{"label":"metadata only"}],"logprobs":[{"token":"metadata only"}]}]}`},
		{name: "chat missing messages", protocol: ContentModerationProtocolOpenAIChat, body: `{"model":"gpt-5.5"}`},
		{name: "chat null messages", protocol: ContentModerationProtocolOpenAIChat, body: `{"model":"gpt-5.5","messages":null}`},
		{name: "chat empty messages", protocol: ContentModerationProtocolOpenAIChat, body: `{"model":"gpt-5.5","messages":[]}`},
		{name: "chat tool output", protocol: ContentModerationProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":"historical user"},{"role":"tool","content":"done"}]}`},
		{name: "chat content metadata only", protocol: ContentModerationProtocolOpenAIChat, body: `{"messages":[{"role":"user","content":[{"type":"text","text":"","annotations":[{"label":"metadata only"}],"logprobs":[{"token":"metadata only"}]}]}]}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			document := auditinput.ParseForTextAudit(test.protocol, []byte(test.body))
			require.True(t, document.Complete, "%+v", document.Issues)
			require.Equal(t, ExtractStrictCurrentUserText(test.protocol, []byte(test.body)), document.NormalizedText)
			require.NotEmpty(t, document.ControlItems)

			decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
				Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-5.5",
				Protocol: test.protocol, Body: []byte(test.body),
			})

			require.NoError(t, err)
			require.NotNil(t, decision)
			require.True(t, decision.Allowed)
			require.Equal(t, ContentModerationActionAllow, decision.Action)
			require.Equal(t, 0, requestCount)
		})
	}
}

func TestContentModerationCheck_StrictResponsesToolOutputCallsModerations(t *testing.T) {
	groupID := int64(12)
	requestCount := 0
	var gotInput any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		var request moderationAPIRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		gotInput = request.Input
		require.NoError(t, json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{
			Flagged: false, CategoryScores: completeModerationCategoryScores(0.01),
		}}}))
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.AllGroups = false
	cfg.GroupIDs = []int64{groupID}
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)
	svc := &ContentModerationService{
		settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled: "true", SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo: &contentModerationTestRepo{}, httpClient: server.Client(),
		asyncQueue: make(chan contentModerationTask, 1), keyHealth: make(map[string]*contentModerationKeyHealth),
	}
	body := []byte(`{"input":[{"type":"message","role":"user","content":"historical user"},{"type":"function_call_output","call_id":"call_1","output":"tool output"}]}`)

	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-5.5",
		Protocol: ContentModerationProtocolOpenAIResponses, Body: body,
	})

	require.NoError(t, err)
	require.True(t, decision.Allowed)
	require.Equal(t, 1, requestCount)
	require.Equal(t, "tool output", gotInput)
}

func TestContentModerationCheck_StrictPenaltySignalsUseOnlyCurrentUserText(t *testing.T) {
	groupID := int64(12)
	requestCount := 0
	var gotInput any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		var request moderationAPIRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		gotInput = request.Input
		require.NoError(t, json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{
			Flagged: false, CategoryScores: completeModerationCategoryScores(0.01),
		}}}))
	}))
	defer server.Close()

	historical := ContentModerationInput{Text: "historical blocked keyword"}
	historical.Normalize()
	currentHashBlocked := ContentModerationInput{Text: "current hash blocked"}
	currentHashBlocked.Normalize()
	hashCache := &contentModerationTestHashCache{hashes: map[string]struct{}{
		historical.Hash():         {},
		currentHashBlocked.Hash(): {},
	}}
	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.AllGroups = false
	cfg.GroupIDs = []int64{groupID}
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.BlockedKeywords = []string{"historical blocked keyword"}
	cfg.PreHashCheckEnabled = true
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)
	svc := &ContentModerationService{
		settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo:       &contentModerationTestRepo{},
		hashCache:  hashCache,
		httpClient: server.Client(),
		asyncQueue: make(chan contentModerationTask, 4),
		keyHealth:  make(map[string]*contentModerationKeyHealth),
	}

	safeCurrent := ContentModerationInput{Text: "safe current text"}
	safeCurrent.Normalize()
	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-5.5",
		Protocol: ContentModerationProtocolOpenAIResponses, Body: []byte(`{"input":"safe current text"}`),
		AuditContext: "historical blocked keyword",
	})

	require.NoError(t, err)
	require.True(t, decision.Allowed)
	require.Equal(t, 1, requestCount)
	require.Equal(t, "safe current text", gotInput)
	require.Equal(t, []string{safeCurrent.Hash()}, hashCache.snapshotChecked())

	keywordDecision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-5.5",
		Protocol: ContentModerationProtocolOpenAIResponses, Body: []byte(`{"input":"historical blocked keyword"}`),
	})
	require.NoError(t, err)
	require.True(t, keywordDecision.Blocked)
	require.Equal(t, ContentModerationActionKeywordBlock, keywordDecision.Action)
	require.Equal(t, 1, requestCount)

	hashDecision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-5.5",
		Protocol: ContentModerationProtocolOpenAIResponses, Body: []byte(`{"input":"current hash blocked"}`),
	})
	require.NoError(t, err)
	require.True(t, hashDecision.Blocked)
	require.Equal(t, ContentModerationActionHashBlock, hashDecision.Action)
	require.Equal(t, 1, requestCount)
}

func TestContentModerationCheck_StrictTruncatesCurrentUserTextToRuneLimit(t *testing.T) {
	groupID := int64(12)
	want := strings.Repeat("界", maxStrictModerationTextRunes)
	body, err := json.Marshal(map[string]any{
		"input": []any{map[string]any{
			"type": "message", "role": "user",
			"content": []any{map[string]any{
				"type": "input_text", "text": want + strings.Repeat("尾", 17),
			}},
		}},
	})
	require.NoError(t, err)

	requestCount := 0
	var gotInput any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		var request moderationAPIRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		gotInput = request.Input
		require.NoError(t, json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{
			Flagged: false, CategoryScores: completeModerationCategoryScores(0.01),
		}}}))
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.AllGroups = false
	cfg.GroupIDs = []int64{groupID}
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.BlockedKeywords = []string{"blocked-after-limit"}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)
	svc := &ContentModerationService{
		settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo:       &contentModerationTestRepo{},
		httpClient: server.Client(),
		asyncQueue: make(chan contentModerationTask, 1),
		keyHealth:  make(map[string]*contentModerationKeyHealth),
	}

	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-5.5",
		Protocol: ContentModerationProtocolOpenAIResponses, Body: body,
	})

	require.NoError(t, err)
	require.NotNil(t, decision)
	require.True(t, decision.Allowed)
	require.Equal(t, 1, requestCount)
	require.Equal(t, want, gotInput)
	require.Equal(t, maxStrictModerationTextRunes, len([]rune(gotInput.(string))))

	blockedBody, err := json.Marshal(map[string]any{
		"input": want + " blocked-after-limit",
	})
	require.NoError(t, err)
	blocked, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-5.5",
		Protocol: ContentModerationProtocolOpenAIResponses, Body: blockedBody,
	})
	require.NoError(t, err)
	require.True(t, blocked.Blocked, "local keyword checks must cover text beyond the remote limit")
	require.Equal(t, ContentModerationActionKeywordBlock, blocked.Action)
	require.Equal(t, 1, requestCount, "local keyword blocks must not consume another Moderations call")
}

func TestContentModerationStatusTracksPreBlockSyncMetrics(t *testing.T) {
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		score := 0.01
		if requestCount == 1 {
			score = 0.9
		}
		time.Sleep(5 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(moderationAPIResponse{
			Results: []moderationAPIResult{{
				CategoryScores: map[string]float64{"sexual": score},
			}},
		})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		&contentModerationTestRepo{},
		&contentModerationTestHashCache{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	for _, prompt := range []string{"blocked prompt", "clean prompt"} {
		_, err := svc.Check(context.Background(), ContentModerationCheckInput{
			UserID:   1001,
			Protocol: ContentModerationProtocolOpenAIChat,
			Body:     []byte(fmt.Sprintf(`{"messages":[{"role":"user","content":%q}]}`, prompt)),
		})
		require.NoError(t, err)
	}

	status, err := svc.GetStatus(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), status.PreBlockChecked)
	require.Equal(t, int64(1), status.PreBlockAllowed)
	require.Equal(t, int64(1), status.PreBlockBlocked)
	require.Equal(t, int64(0), status.PreBlockErrors)
	require.Equal(t, 0, status.PreBlockActive)
	require.GreaterOrEqual(t, status.PreBlockAvgLatencyMS, int64(1))
}

func TestContentModerationStatusTracksPreBlockAPIKeyLoad(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(moderationAPIResponse{
			Results: []moderationAPIResult{{
				CategoryScores: map[string]float64{"sexual": 0.01},
			}},
		})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-one", "sk-two"}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		&contentModerationTestRepo{},
		&contentModerationTestHashCache{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	for idx := 0; idx < 4; idx++ {
		_, err := svc.Check(context.Background(), ContentModerationCheckInput{
			UserID:   1001,
			Protocol: ContentModerationProtocolOpenAIChat,
			Body:     []byte(fmt.Sprintf(`{"messages":[{"role":"user","content":"prompt %d"}]}`, idx)),
		})
		require.NoError(t, err)
	}

	status, err := svc.GetStatus(context.Background())
	require.NoError(t, err)
	require.Len(t, status.PreBlockAPIKeyLoads, 2)
	require.Equal(t, int64(4), status.PreBlockAPIKeyTotalCalls)
	require.Equal(t, int64(2), status.PreBlockAPIKeyAvailableCount)
	require.Equal(t, int64(0), status.PreBlockAPIKeyActive)
	require.Equal(t, int64(0), status.PreBlockAPIKeyLoads[0].Active)
	require.Equal(t, int64(2), status.PreBlockAPIKeyLoads[0].Total)
	require.Equal(t, int64(2), status.PreBlockAPIKeyLoads[0].Success)
	require.Equal(t, int64(0), status.PreBlockAPIKeyLoads[0].Errors)
	require.Equal(t, int64(2), status.PreBlockAPIKeyLoads[1].Total)
	require.Equal(t, int64(2), status.PreBlockAPIKeyLoads[1].Success)
}

func TestContentModerationStatusTracksPreBlockLocalBlocks(t *testing.T) {
	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.KeywordBlockingMode = ContentModerationKeywordModeKeywordOnly
	cfg.BlockedKeywords = []string{"blocked"}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		&contentModerationTestRepo{},
		&contentModerationTestHashCache{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	for _, prompt := range []string{"blocked prompt", "clean prompt"} {
		_, err := svc.Check(context.Background(), ContentModerationCheckInput{
			UserID:   1001,
			Protocol: ContentModerationProtocolOpenAIChat,
			Body:     []byte(fmt.Sprintf(`{"messages":[{"role":"user","content":%q}]}`, prompt)),
		})
		require.NoError(t, err)
	}

	status, err := svc.GetStatus(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), status.PreBlockChecked)
	require.Equal(t, int64(1), status.PreBlockAllowed)
	require.Equal(t, int64(1), status.PreBlockBlocked)
	require.Equal(t, int64(0), status.PreBlockErrors)
}

func TestBuildContentModerationTestAuditResult_UsesConfiguredThresholds(t *testing.T) {
	result := buildContentModerationTestAuditResult(&moderationAPIResult{
		Flagged: true,
		CategoryScores: map[string]float64{
			"harassment": 0.65,
		},
	}, nil)

	require.NotNil(t, result)
	require.False(t, result.Flagged)
	require.Equal(t, "harassment", result.HighestCategory)
	require.Equal(t, 0.65, result.HighestScore)
	require.Equal(t, 0.65, result.CompositeScore)
	require.Equal(t, 0.98, result.Thresholds["harassment"])
}

func TestContentModerationCheck_StrictFindingsLogWithoutUserSideEffects(t *testing.T) {
	const userID int64 = 1001
	groupID := int64(12)

	newService := func(t *testing.T, cfg *ContentModerationConfig, client *http.Client) (*ContentModerationService, *contentModerationTestRepo, *contentModerationTestUserRepo, *contentModerationTestAuthCacheInvalidator) {
		t.Helper()
		rawCfg, err := json.Marshal(cfg)
		require.NoError(t, err)
		repo := &contentModerationTestRepo{}
		userRepo := &contentModerationTestUserRepo{user: &User{ID: userID, Status: StatusActive}}
		invalidator := &contentModerationTestAuthCacheInvalidator{}
		return &ContentModerationService{
			settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
				SettingKeyRiskControlEnabled:      "true",
				SettingKeyContentModerationConfig: string(rawCfg),
			}},
			repo: repo, userRepo: userRepo, authCacheInvalidator: invalidator,
			httpClient: client, asyncQueue: make(chan contentModerationTask, 2),
			keyHealth: make(map[string]*contentModerationKeyHealth),
		}, repo, userRepo, invalidator
	}

	assertBlockedWithoutSideEffects := func(t *testing.T, svc *ContentModerationService, repo *contentModerationTestRepo, userRepo *contentModerationTestUserRepo, invalidator *contentModerationTestAuthCacheInvalidator, body []byte) {
		t.Helper()
		decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
			Strict: true, UserID: userID, UserEmail: "user@example.test", APIKeyID: 7,
			GroupID: &groupID, Model: "gpt-test", Protocol: ContentModerationProtocolOpenAIResponses, Body: body,
		})
		require.NoError(t, err)
		require.True(t, decision.Blocked)
		require.True(t, decision.Flagged)

		var task contentModerationTask
		select {
		case task = <-svc.asyncQueue:
		default:
			require.FailNow(t, "strict finding was not queued for audit logging")
		}
		require.NotNil(t, task.log)
		require.False(t, task.applySideEffects)
		svc.persistContentModerationLog(context.Background(), task.config, task.log, task.inputHash, task.recordHash, task.applySideEffects)

		logs := repo.snapshotLogs()
		require.Len(t, logs, 1)
		require.False(t, logs[0].Flagged, "strict admission findings must never enter later legacy violation counts")
		require.Zero(t, logs[0].ViolationCount)
		require.False(t, logs[0].AutoBanned)
		require.False(t, logs[0].EmailSent)
		require.Empty(t, userRepo.updated)
		require.Empty(t, invalidator.userIDs)
	}

	t.Run("keyword", func(t *testing.T) {
		cfg := defaultContentModerationConfig()
		cfg.Enabled = true
		cfg.APIKeys = []string{"sk-test"}
		cfg.BlockedKeywords = []string{"cyber"}
		cfg.BanThreshold = 1
		svc, repo, userRepo, invalidator := newService(t, cfg, http.DefaultClient)
		assertBlockedWithoutSideEffects(t, svc, repo, userRepo, invalidator, []byte(`{"input":"cyber request"}`))
	})

	t.Run("moderation score threshold", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			scores := completeModerationCategoryScores(0.01)
			scores["violence"] = 0.99
			_ = json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{
				Flagged: false, CategoryScores: scores,
			}}})
		}))
		defer server.Close()

		cfg := defaultContentModerationConfig()
		cfg.Enabled = true
		cfg.BaseURL = server.URL
		cfg.APIKeys = []string{"sk-test"}
		cfg.RetryCount = 0
		cfg.BanThreshold = 1
		svc, repo, userRepo, invalidator := newService(t, cfg, server.Client())
		assertBlockedWithoutSideEffects(t, svc, repo, userRepo, invalidator, []byte(`{"input":"clean request"}`))
	})
}

func TestContentModerationCheck_StrictLowScoreUpstreamFlagIsAllowedAndNotHashed(t *testing.T) {
	groupID := int64(12)
	tests := []struct {
		name     string
		category string
		score    float64
		protocol string
		body     []byte
	}{
		{
			name: "screenshot chat harassment 48.8 percent", category: "harassment", score: 0.488,
			protocol: ContentModerationProtocolOpenAIChat,
			body:     []byte(`{"messages":[{"role":"user","content":"实习生月薪预涨五倍，合同到期妻子追求谈续约"}]}`),
		},
		{
			name: "screenshot responses violence 20.5 percent", category: "violence", score: 0.205,
			protocol: ContentModerationProtocolOpenAIResponses,
			body:     []byte(`{"input":"要求重新生成，歌词不要超过1000字"}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var upstreamCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				upstreamCalls.Add(1)
				scores := completeModerationCategoryScores(0.01)
				scores[tt.category] = tt.score
				require.NoError(t, json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{
					Flagged: true, CategoryScores: scores,
				}}}))
			}))
			defer server.Close()

			cfg := defaultContentModerationConfig()
			cfg.Enabled = true
			cfg.AllGroups = false
			cfg.GroupIDs = []int64{groupID}
			cfg.BaseURL = server.URL
			cfg.APIKeys = []string{"sk-test"}
			cfg.RetryCount = 0
			rawCfg, err := json.Marshal(cfg)
			require.NoError(t, err)

			repo := &contentModerationTestRepo{}
			hashCache := &contentModerationTestHashCache{}
			svc := &ContentModerationService{
				settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
					SettingKeyRiskControlEnabled:      "true",
					SettingKeyContentModerationConfig: string(rawCfg),
				}},
				repo:       repo,
				hashCache:  hashCache,
				httpClient: server.Client(),
				asyncQueue: make(chan contentModerationTask, 1),
				keyHealth:  make(map[string]*contentModerationKeyHealth),
			}

			for attempt := 1; attempt <= 2; attempt++ {
				decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
					Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-test",
					Protocol: tt.protocol, Body: tt.body,
				})

				require.NoError(t, err, "attempt %d", attempt)
				require.NotNil(t, decision, "attempt %d", attempt)
				require.True(t, decision.Allowed, "attempt %d", attempt)
				require.False(t, decision.Blocked, "attempt %d", attempt)
				require.False(t, decision.Flagged, "attempt %d", attempt)
				require.Equal(t, ContentModerationActionAllow, decision.Action, "attempt %d", attempt)
			}

			require.Equal(t, int32(1), upstreamCalls.Load(), "second request should reuse the cached upstream result")
			require.Empty(t, hashCache.snapshotRecorded())
			require.Empty(t, repo.snapshotLogs())
			select {
			case task := <-svc.asyncQueue:
				require.Failf(t, "unexpected moderation task", "task=%+v", task)
			default:
			}
		})
	}
}

func TestContentModerationCheck_StrictValidatesCompleteModerationVerdict(t *testing.T) {
	groupID := int64(12)
	tests := []struct {
		name    string
		mutate  func(map[string]any)
		wantErr bool
	}{
		{
			name: "missing flagged",
			mutate: func(result map[string]any) {
				delete(result, "flagged")
			},
			wantErr: true,
		},
		{
			name: "missing category scores",
			mutate: func(result map[string]any) {
				delete(result, "category_scores")
			},
			wantErr: true,
		},
		{
			name: "incomplete category scores",
			mutate: func(result map[string]any) {
				delete(result["category_scores"].(map[string]float64), "sexual/minors")
			},
			wantErr: true,
		},
		{
			name: "negative category score",
			mutate: func(result map[string]any) {
				result["category_scores"].(map[string]float64)["violence"] = -0.01
			},
			wantErr: true,
		},
		{
			name: "category score above one",
			mutate: func(result map[string]any) {
				result["category_scores"].(map[string]float64)["violence"] = 1.01
			},
			wantErr: true,
		},
		{name: "complete verdict"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := map[string]any{
				"flagged":         false,
				"category_scores": completeModerationCategoryScores(0.01),
			}
			if tt.mutate != nil {
				tt.mutate(result)
			}
			requestCount := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requestCount++
				_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{result}})
			}))
			defer server.Close()

			cfg := defaultContentModerationConfig()
			cfg.Enabled = true
			cfg.AllGroups = false
			cfg.GroupIDs = []int64{groupID}
			cfg.BaseURL = server.URL
			cfg.APIKeys = []string{"sk-test"}
			cfg.RetryCount = 0
			rawCfg, err := json.Marshal(cfg)
			require.NoError(t, err)
			svc := &ContentModerationService{
				settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
					SettingKeyRiskControlEnabled:      "true",
					SettingKeyContentModerationConfig: string(rawCfg),
				}},
				repo:       &contentModerationTestRepo{},
				httpClient: server.Client(),
				asyncQueue: make(chan contentModerationTask, 1),
				keyHealth:  make(map[string]*contentModerationKeyHealth),
			}

			decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
				Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-test",
				Protocol: ContentModerationProtocolOpenAIResponses, Body: []byte(`{"input":"clean request"}`),
			})
			require.Equal(t, 1, requestCount)
			if tt.wantErr {
				require.Error(t, err)
				require.Nil(t, decision)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, decision)
			require.True(t, decision.Allowed)
		})
	}
}

func TestContentModerationCheck_StrictBlocksFlaggedSingleResult(t *testing.T) {
	groupID := int64(12)
	body := []byte(`{"input":"current flagged text"}`)
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		var request moderationAPIRequest
		require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
		require.Equal(t, "current flagged text", request.Input)
		scores := completeModerationCategoryScores(0.01)
		scores["violence"] = 0.99
		require.NoError(t, json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{
			Flagged: true, CategoryScores: scores,
		}}}))
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.AllGroups = false
	cfg.GroupIDs = []int64{groupID}
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.RetryCount = 0
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)
	svc := &ContentModerationService{
		settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo:       &contentModerationTestRepo{},
		httpClient: server.Client(),
		asyncQueue: make(chan contentModerationTask, 1),
		keyHealth:  make(map[string]*contentModerationKeyHealth),
	}

	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-test",
		Protocol: ContentModerationProtocolOpenAIResponses, Body: body,
	})

	require.NoError(t, err)
	require.NotNil(t, decision)
	require.True(t, decision.Blocked)
	require.True(t, decision.Flagged)
	require.Equal(t, "violence", decision.HighestCategory)
	require.Equal(t, 0.99, decision.HighestScore)
	require.Equal(t, 1, requestCount)
}

func TestContentModerationCheck_StrictImageOnlyRequestIsAllowedWithoutModerationCall(t *testing.T) {
	groupID := int64(12)
	content := make([]any, 0, 59)
	for index := 0; index < 59; index++ {
		image := map[string]any{
			"type":      "input_image",
			"image_url": fmt.Sprintf("opaque-image-payload-%02d", index),
		}
		if index == 0 {
			image["id"] = 42
			image["image_url"] = map[string]any{"malformed": true}
			image["future_image_field"] = "ignored"
		}
		content = append(content, image)
	}
	body, err := json.Marshal(map[string]any{
		"input": []any{map[string]any{
			"type": "message", "role": "user", "content": content,
		}},
	})
	require.NoError(t, err)

	var mu sync.Mutex
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.AllGroups = false
	cfg.GroupIDs = []int64{groupID}
	cfg.BaseURL = server.URL
	cfg.APIKeys = nil
	cfg.RetryCount = 0
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)
	svc := &ContentModerationService{
		settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo:       &contentModerationTestRepo{},
		httpClient: server.Client(),
		asyncQueue: make(chan contentModerationTask, 1),
		keyHealth:  make(map[string]*contentModerationKeyHealth),
	}

	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-test",
		Protocol: ContentModerationProtocolOpenAIResponses, Body: body,
	})

	require.NoError(t, err)
	require.NotNil(t, decision)
	require.True(t, decision.Allowed)
	require.False(t, decision.Blocked)
	require.False(t, decision.Flagged)
	require.False(t, decision.Unavailable)
	require.Equal(t, ContentModerationActionAllow, decision.Action)
	require.Equal(t, int64(1), svc.preBlockChecked.Load())
	require.Equal(t, int64(1), svc.preBlockAllowed.Load())
	mu.Lock()
	defer mu.Unlock()
	require.Zero(t, requestCount)
}

func TestContentModerationCheck_StrictControlOnlyRequestAllowsWithoutModerationCall(t *testing.T) {
	groupID := int64(12)
	body := []byte(`{"input":[{"type":"compaction_trigger"}]}`)
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.AllGroups = false
	cfg.GroupIDs = []int64{groupID}
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.RetryCount = 0
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)
	svc := &ContentModerationService{
		settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo:       &contentModerationTestRepo{},
		httpClient: server.Client(),
		asyncQueue: make(chan contentModerationTask, 1),
		keyHealth:  make(map[string]*contentModerationKeyHealth),
	}

	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-test",
		Protocol: ContentModerationProtocolOpenAIResponses, Body: body,
	})

	require.NoError(t, err)
	require.NotNil(t, decision)
	require.True(t, decision.Allowed)
	require.Equal(t, ContentModerationActionAllow, decision.Action)
	require.Zero(t, requestCount)
}

func TestContentModerationCheck_StrictResultCountMismatchFailsClosed(t *testing.T) {
	groupID := int64(12)
	body := []byte(`{"input":"current user text"}`)
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requestCount++
		_ = json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{
			{Flagged: false, CategoryScores: completeModerationCategoryScores(0.01)},
			{Flagged: false, CategoryScores: completeModerationCategoryScores(0.01)},
		}})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.AllGroups = false
	cfg.GroupIDs = []int64{groupID}
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.RetryCount = 0
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)
	svc := &ContentModerationService{
		settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo:       &contentModerationTestRepo{},
		httpClient: server.Client(),
		asyncQueue: make(chan contentModerationTask, 1),
		keyHealth:  make(map[string]*contentModerationKeyHealth),
	}

	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-test",
		Protocol: ContentModerationProtocolOpenAIResponses, Body: body,
	})

	require.Error(t, err)
	require.Nil(t, decision)
	require.Equal(t, 1, requestCount)
}

func TestContentModerationCheck_StrictUsesOneCallAndNeverFailsOver(t *testing.T) {
	groupID := int64(12)
	tests := []struct {
		name      string
		respond   func(*testing.T, http.ResponseWriter, *http.Request)
		wantError bool
	}{
		{
			name: "success",
			respond: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				writeCompleteModerationResponse(t, w, r)
			},
		},
		{
			name: "429 does not select backup key",
			respond: func(_ *testing.T, w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":{"message":"rate limited","type":"rate_limit_error","code":"rate_limit_exceeded"}}`))
			},
			wantError: true,
		},
		{
			name: "malformed verdict does not select backup key",
			respond: func(t *testing.T, w http.ResponseWriter, _ *http.Request) {
				scores := completeModerationCategoryScores(0.01)
				delete(scores, "sexual/minors")
				require.NoError(t, json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{
					Flagged: false, CategoryScores: scores,
				}}}))
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var mu sync.Mutex
			var authorizations []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				authorizations = append(authorizations, r.Header.Get("Authorization"))
				mu.Unlock()
				tt.respond(t, w, r)
			}))
			defer server.Close()

			cfg := defaultContentModerationConfig()
			cfg.Enabled = true
			cfg.Mode = ContentModerationModePreBlock
			cfg.AllGroups = false
			cfg.GroupIDs = []int64{groupID}
			cfg.BaseURL = server.URL
			cfg.APIKeys = []string{"sk-primary", "sk-backup", "sk-third"}
			cfg.RetryCount = 10
			rawCfg, err := json.Marshal(cfg)
			require.NoError(t, err)
			svc := &ContentModerationService{
				settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
					SettingKeyRiskControlEnabled:      "true",
					SettingKeyContentModerationConfig: string(rawCfg),
				}},
				repo:       &contentModerationTestRepo{},
				httpClient: server.Client(),
				asyncQueue: make(chan contentModerationTask, 1),
				keyHealth:  make(map[string]*contentModerationKeyHealth),
			}

			decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
				Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-test",
				Protocol: ContentModerationProtocolOpenAIResponses, Body: []byte(`{"input":"current user text"}`),
			})

			if tt.wantError {
				require.Error(t, err)
				require.Nil(t, decision)
			} else {
				require.NoError(t, err)
				require.NotNil(t, decision)
				require.True(t, decision.Allowed)
			}
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, []string{"Bearer sk-primary"}, authorizations)
		})
	}
}

func TestContentModerationStrictAPICallLimitStopsRequestStorm(t *testing.T) {
	var mu sync.Mutex
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		writeCompleteModerationResponse(t, w, r)
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.RetryCount = 0
	svc := NewContentModerationService(nil, nil, nil, nil, nil, nil, nil, nil)
	batches := make([]strictModerationBatch, maxStrictModerationAPICalls+1)
	for index := range batches {
		batches[index] = strictModerationBatch{input: "safe", expectedResults: 1}
	}

	_, err := svc.callModerationStrictBatches(context.Background(), cfg, batches, newStrictModerationKeyState(cfg))

	require.Error(t, err)
	require.Contains(t, err.Error(), "API call limit")
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, maxStrictModerationAPICalls, requestCount)
}

func TestContentModerationStrict429NeverFailsOverToAnotherKey(t *testing.T) {
	var mu sync.Mutex
	var authorizations []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		authorizations = append(authorizations, r.Header.Get("Authorization"))
		mu.Unlock()
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"rate limited"}}`))
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-primary", "sk-backup"}
	cfg.RetryCount = 1
	svc := NewContentModerationService(nil, nil, nil, nil, nil, nil, nil, nil)
	state := newStrictModerationKeyState(cfg)
	batches := []strictModerationBatch{{input: "current user text", expectedResults: 1}}

	_, err := svc.callModerationStrictBatches(context.Background(), cfg, batches, state)

	require.Error(t, err)
	require.Contains(t, err.Error(), "moderation api status 429")
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"Bearer sk-primary"}, authorizations)
}

func TestContentModerationCheck_Strict59ToolScreenshotsAreNotModeratedUnderConcurrency(t *testing.T) {
	const workers = 16
	groupID := int64(12)
	body := strictResponsesBodyWithImages(t, 59)

	var mu sync.Mutex
	requestCount := 0
	textRequests := 0
	imageInputs := 0
	var handlerErr error
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request moderationAPIRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			mu.Lock()
			if handlerErr == nil {
				handlerErr = err
			}
			mu.Unlock()
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		text, isText := request.Input.(string)
		batchImageInputs := 0
		if inputs, isArray := request.Input.([]any); isArray {
			batchImageInputs = len(inputs)
		}
		mu.Lock()
		requestCount++
		if isText {
			textRequests++
			if got := len([]rune(text)); got != maxStrictModerationTextRunes && handlerErr == nil {
				handlerErr = fmt.Errorf("moderation text rune count = %d, want %d", got, maxStrictModerationTextRunes)
			}
		} else if handlerErr == nil {
			handlerErr = fmt.Errorf("moderation input type = %T, want string", request.Input)
		}
		imageInputs += batchImageInputs
		mu.Unlock()

		_ = json.NewEncoder(w).Encode(moderationAPIResponse{Results: []moderationAPIResult{{
			Flagged: false, CategoryScores: completeModerationCategoryScores(0.01),
		}}})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.AllGroups = false
	cfg.GroupIDs = []int64{groupID}
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.RetryCount = 0
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)
	svc := &ContentModerationService{
		settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo:       &contentModerationTestRepo{},
		httpClient: server.Client(),
		asyncQueue: make(chan contentModerationTask, workers),
		keyHealth:  make(map[string]*contentModerationKeyHealth),
	}

	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for index := 0; index < workers; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decision, checkErr := svc.Check(context.Background(), ContentModerationCheckInput{
				Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-test",
				Protocol: ContentModerationProtocolOpenAIResponses, Body: body,
			})
			if checkErr != nil {
				errCh <- checkErr
				return
			}
			if decision == nil || !decision.Allowed || decision.Unavailable {
				errCh <- errors.New("strict moderation did not allow safe request")
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for checkErr := range errCh {
		require.NoError(t, checkErr)
	}

	mu.Lock()
	defer mu.Unlock()
	require.NoError(t, handlerErr)
	require.Equal(t, 1, requestCount)
	require.Equal(t, 1, textRequests)
	require.Zero(t, imageInputs)
}

func TestContentModerationCheck_Strict429ConcurrencyFailsClosedWithinOneCallPerRequest(t *testing.T) {
	const workers = 32
	groupID := int64(12)
	body := strictResponsesBodyWithImages(t, 59)

	var mu sync.Mutex
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"rate limited"}}`))
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.AllGroups = false
	cfg.GroupIDs = []int64{groupID}
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-primary", "sk-backup"}
	cfg.RetryCount = 1
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)
	svc := &ContentModerationService{
		settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo:       &contentModerationTestRepo{},
		httpClient: server.Client(),
		asyncQueue: make(chan contentModerationTask, workers),
		keyHealth:  make(map[string]*contentModerationKeyHealth),
	}

	resultCh := make(chan error, workers)
	var wg sync.WaitGroup
	for index := 0; index < workers; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decision, checkErr := svc.Check(context.Background(), ContentModerationCheckInput{
				Strict: true, APIKeyID: 7, GroupID: &groupID, Model: "gpt-test",
				Protocol: ContentModerationProtocolOpenAIResponses, Body: body,
			})
			if checkErr == nil || decision != nil {
				resultCh <- errors.New("strict moderation did not fail closed after rate limiting")
				return
			}
			resultCh <- nil
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "strict moderation concurrency test timed out")
	}
	close(resultCh)
	for resultErr := range resultCh {
		require.NoError(t, resultErr)
	}

	mu.Lock()
	defer mu.Unlock()
	require.Greater(t, requestCount, 0)
	require.LessOrEqual(t, requestCount, workers)
}

func TestContentModerationCallModeration_400DoesNotFreezeAPIKey(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"Number of images (5) exceeds maximum of 1","type":"invalid_request_error","param":"input","code":"too_many_images"}}`))
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.RetryCount = 5
	svc := NewContentModerationService(nil, nil, nil, nil, nil, nil, nil, nil)

	_, err := svc.callModeration(context.Background(), cfg, "hello")

	require.Error(t, err)
	require.Equal(t, 1, requestCount)
	status := svc.apiKeyStatusForHash(0, moderationAPIKeyHash("sk-test"), maskSecretTail("sk-test"), true)
	require.Equal(t, "error", status.Status)
	require.Equal(t, http.StatusBadRequest, status.LastHTTPStatus)
	require.Zero(t, status.FailureCount)
	require.Nil(t, status.FrozenUntil)
}

func TestSafeModerationHeaderSanitizesControlCharactersAndTruncates(t *testing.T) {
	require.Equal(t, "req    id", safeModerationHeader(" \r\nreq\r\n\t\x00id\t "))

	got := safeModerationHeader(strings.Repeat("界", 129))
	require.Equal(t, strings.Repeat("界", 128), got)
	require.Equal(t, 128, len([]rune(got)))
}

func TestSafeModerationIdentifierAllowsOnlyDiagnosticCharacters(t *testing.T) {
	require.Equal(t, "Rate_Limit.Error:code/429-", safeModerationIdentifier(" Rate_Limit.Error:code/429-\r\n<>中文 "))
	require.Equal(t, strings.Repeat("a", 64), safeModerationIdentifier(strings.Repeat("a", 65)))
}

func TestParseModerationRetryAfter(t *testing.T) {
	now := time.Date(2026, time.August, 12, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name  string
		value string
		want  time.Duration
	}{
		{name: "seconds", value: "120", want: 2 * time.Minute},
		{name: "HTTP date", value: now.Add(90 * time.Second).Format(http.TimeFormat), want: 90 * time.Second},
		{name: "negative seconds", value: "-1"},
		{name: "integer overflow", value: "999999999999999999999999999999"},
		{name: "seconds clamp to 24 hours", value: "999999999", want: 24 * time.Hour},
		{name: "HTTP date clamp to 24 hours", value: now.Add(7 * 24 * time.Hour).Format(http.TimeFormat), want: 24 * time.Hour},
		{name: "past HTTP date", value: now.Add(-time.Minute).Format(http.TimeFormat)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, parseModerationRetryAfter(tt.value, now))
		})
	}
}

func TestContentModerationCallModeration_429PreservesDiagnosticsWithoutLeakingBody(t *testing.T) {
	const upstreamMessage = "customer prompt must never appear in diagnostics"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		w.Header().Set("x-request-id", "req_moderation_123")
		w.Header().Set("x-ratelimit-limit-requests", "100")
		w.Header().Set("x-ratelimit-remaining-requests", "0")
		w.Header().Set("x-ratelimit-reset-requests", "2m")
		w.Header().Set("x-ratelimit-limit-tokens", "10000")
		w.Header().Set("x-ratelimit-remaining-tokens", "0")
		w.Header().Set("x-ratelimit-reset-tokens", "2m")
		w.Header().Set("x-ratelimit-limit-project-tokens", "50000")
		w.Header().Set("x-ratelimit-remaining-project-tokens", "123")
		w.Header().Set("x-ratelimit-reset-project-tokens", "30s")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = fmt.Fprintf(w, `{"error":{"message":%q,"type":"rate_limit_error","code":"rate_limit_exceeded"}}`, upstreamMessage)
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-diagnostic-test"}
	cfg.RetryCount = 0
	svc := NewContentModerationService(nil, nil, nil, nil, nil, nil, nil, nil)

	_, err := svc.callModeration(context.Background(), cfg, "current user text")

	require.Error(t, err)
	var apiErr *moderationAPIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusTooManyRequests, apiErr.StatusCode)
	require.Equal(t, "rate_limit_error", apiErr.ErrorType)
	require.Equal(t, "rate_limit_exceeded", apiErr.ErrorCode)
	require.Equal(t, moderationAPIKeyHash("sk-diagnostic-test"), apiErr.KeyHash)
	require.Equal(t, "120", apiErr.RetryAfter)
	require.Equal(t, 2*time.Minute, apiErr.retryAfterDuration)
	require.Equal(t, "req_moderation_123", apiErr.RequestID)
	require.Equal(t, "100", apiErr.LimitRequests)
	require.Equal(t, "0", apiErr.RemainingRequests)
	require.Equal(t, "2m", apiErr.ResetRequests)
	require.Equal(t, "10000", apiErr.LimitTokens)
	require.Equal(t, "0", apiErr.RemainingTokens)
	require.Equal(t, "2m", apiErr.ResetTokens)
	require.Equal(t, "50000", apiErr.LimitProjectTokens)
	require.Equal(t, "123", apiErr.RemainingProjectTokens)
	require.Equal(t, "30s", apiErr.ResetProjectTokens)
	require.NotContains(t, err.Error(), upstreamMessage)

	fields := moderationAPIErrorLogFields(err)
	require.Zero(t, len(fields)%2)
	diagnostics := make(map[string]any, len(fields)/2)
	for index := 0; index < len(fields); index += 2 {
		key, ok := fields[index].(string)
		require.True(t, ok)
		diagnostics[key] = fields[index+1]
	}
	require.Equal(t, http.StatusTooManyRequests, diagnostics["moderation_http_status"])
	require.Equal(t, "rate_limit_error", diagnostics["moderation_error_type"])
	require.Equal(t, "rate_limit_exceeded", diagnostics["moderation_error_code"])
	require.Equal(t, "req_moderation_123", diagnostics["openai_request_id"])
	require.Equal(t, "100", diagnostics["ratelimit_limit_requests"])
	require.Equal(t, "0", diagnostics["ratelimit_remaining_requests"])
	require.Equal(t, "10000", diagnostics["ratelimit_limit_tokens"])
	require.Equal(t, "0", diagnostics["ratelimit_remaining_tokens"])
	require.Equal(t, "50000", diagnostics["ratelimit_limit_project_tokens"])
	require.Equal(t, "123", diagnostics["ratelimit_remaining_project_tokens"])
	require.Equal(t, "30s", diagnostics["ratelimit_reset_project_tokens"])
	require.NotContains(t, fmt.Sprint(fields), upstreamMessage)

	status := svc.apiKeyStatusForHash(0, moderationAPIKeyHash("sk-diagnostic-test"), maskSecretTail("sk-diagnostic-test"), true)
	require.Equal(t, "frozen", status.Status)
	require.NotContains(t, status.LastError, upstreamMessage)
	require.NotNil(t, status.FrozenUntil)
	remaining := time.Until(*status.FrozenUntil)
	require.GreaterOrEqual(t, remaining, 115*time.Second)
	require.LessOrEqual(t, remaining, 121*time.Second)
}

func TestContentModerationCallModeration_FreezesByHTTPStatus(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		minFreeze  time.Duration
		maxFreeze  time.Duration
	}{
		{name: "401 freezes ten minutes", statusCode: http.StatusUnauthorized, minFreeze: 9*time.Minute + 55*time.Second, maxFreeze: 10*time.Minute + time.Second},
		{name: "403 freezes ten minutes", statusCode: http.StatusForbidden, minFreeze: 9*time.Minute + 55*time.Second, maxFreeze: 10*time.Minute + time.Second},
		{name: "429 freezes one minute", statusCode: http.StatusTooManyRequests, minFreeze: 55 * time.Second, maxFreeze: time.Minute + time.Second},
		{name: "529 freezes one minute", statusCode: 529, minFreeze: 55 * time.Second, maxFreeze: time.Minute + time.Second},
		{name: "500 freezes ten seconds", statusCode: http.StatusInternalServerError, minFreeze: 5 * time.Second, maxFreeze: 11 * time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)
				_, _ = w.Write([]byte(`{"error":{"message":"upstream error"}}`))
			}))
			defer server.Close()

			cfg := defaultContentModerationConfig()
			cfg.BaseURL = server.URL
			cfg.APIKeys = []string{"sk-test"}
			cfg.RetryCount = 0
			svc := NewContentModerationService(nil, nil, nil, nil, nil, nil, nil, nil)

			_, err := svc.callModeration(context.Background(), cfg, "hello")

			require.Error(t, err)
			status := svc.apiKeyStatusForHash(0, moderationAPIKeyHash("sk-test"), maskSecretTail("sk-test"), true)
			require.Equal(t, "frozen", status.Status)
			require.Equal(t, tt.statusCode, status.LastHTTPStatus)
			require.Equal(t, 1, status.FailureCount)
			require.NotNil(t, status.FrozenUntil)
			remaining := time.Until(*status.FrozenUntil)
			require.GreaterOrEqual(t, remaining, tt.minFreeze)
			require.LessOrEqual(t, remaining, tt.maxFreeze)
		})
	}
}

func TestContentModerationTestAPIKeys_400DoesNotFreezeAPIKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"invalid moderation request"}}`))
	}))
	defer server.Close()

	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{}},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	result, err := svc.TestAPIKeys(context.Background(), TestContentModerationAPIKeysInput{
		APIKeys: []string{"sk-test"},
		BaseURL: server.URL,
		Prompt:  "hello",
	})

	require.NoError(t, err)
	require.Len(t, result.Items, 1)
	require.Equal(t, "error", result.Items[0].Status)
	require.Equal(t, http.StatusBadRequest, result.Items[0].LastHTTPStatus)
	require.Zero(t, result.Items[0].FailureCount)
	require.Nil(t, result.Items[0].FrozenUntil)
}

func TestContentModerationCheck_PreHashUsesRedisHashCache(t *testing.T) {
	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.PreHashCheckEnabled = true
	cfg.APIKeys = []string{"sk-test"}
	cfg.BlockStatus = http.StatusConflict
	cfg.BlockMessage = "命中历史风险输入"
	cfg.AutoBanEnabled = true
	cfg.BanThreshold = 1
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	hashCache := &contentModerationTestHashCache{hashes: map[string]struct{}{}}
	content := ContentModerationInput{Text: "blocked prompt"}
	content.Normalize()
	hashCache.hashes[content.Hash()] = struct{}{}

	repo := &contentModerationTestRepo{}
	userRepo := &contentModerationTestUserRepo{user: &User{ID: 1001, Status: StatusActive}}
	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo,
		hashCache,
		nil,
		userRepo,
		nil,
		nil,
		nil,
	)

	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		UserID:   1001,
		Protocol: ContentModerationProtocolOpenAIChat,
		Body:     []byte(`{"messages":[{"role":"user","content":"blocked prompt"}]}`),
	})
	require.NoError(t, err)
	require.True(t, decision.Blocked)
	require.Equal(t, ContentModerationActionHashBlock, decision.Action)
	require.Equal(t, http.StatusConflict, decision.StatusCode)
	require.Equal(t, content.Hash(), decision.InputHash)
	require.Contains(t, decision.Message, "命中历史风险输入")
	require.Contains(t, decision.Message, content.Hash())
	require.Len(t, hashCache.snapshotChecked(), 1)
	logs := requireContentModerationLogCount(t, repo, 1)
	require.True(t, logs[0].Flagged)
	require.Equal(t, ContentModerationActionHashBlock, logs[0].Action)
	require.Equal(t, 1.0, logs[0].CategoryScores["hash"])
	require.Equal(t, ContentModerationModePreBlock, logs[0].Mode)
	require.Zero(t, logs[0].ViolationCount)
	require.False(t, logs[0].AutoBanned)
	require.Empty(t, userRepo.updated)
}

func TestContentModerationCheck_HashBlockLogsDoNotIncreaseNextViolationCount(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(moderationAPIResponse{
			Results: []moderationAPIResult{{
				CategoryScores: map[string]float64{"sexual": 0.9},
			}},
		})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.AutoBanEnabled = false
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	userID := int64(1001)
	repo := &contentModerationTestRepo{}
	hashLog := &ContentModerationLog{
		UserID:          &userID,
		Action:          ContentModerationActionHashBlock,
		Flagged:         true,
		HighestCategory: "hash",
		HighestScore:    1,
		CreatedAt:       time.Now(),
	}
	require.NoError(t, repo.CreateLog(context.Background(), hashLog))

	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo,
		&contentModerationTestHashCache{},
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		UserID:   userID,
		Protocol: ContentModerationProtocolOpenAIChat,
		Body:     []byte(`{"messages":[{"role":"user","content":"new blocked prompt"}]}`),
	})

	require.NoError(t, err)
	require.True(t, decision.Blocked)
	logs := requireContentModerationLogCount(t, repo, 2)
	require.Equal(t, ContentModerationActionHashBlock, logs[0].Action)
	require.Equal(t, ContentModerationActionBlock, logs[1].Action)
	require.Equal(t, 1, logs[1].ViolationCount)
}

func TestContentModerationAutoBanSkipsAdminAccount(t *testing.T) {
	var slogOutput bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&slogOutput, nil)))
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	cfg := defaultContentModerationConfig()
	cfg.BanThreshold = 2
	cfg.ViolationWindowHours = 24

	userID := int64(1001)
	repo := &contentModerationTestRepo{}
	require.NoError(t, repo.CreateLog(context.Background(), newContentModerationFlaggedLog(userID)))
	userRepo := &contentModerationTestUserRepo{user: &User{ID: userID, Role: RoleAdmin, Status: StatusActive}}
	invalidator := &contentModerationTestAuthCacheInvalidator{}
	svc := NewContentModerationService(nil, repo, nil, nil, userRepo, nil, invalidator, nil)

	svc.persistContentModerationLog(context.Background(), cfg, newContentModerationFlaggedLog(userID), "", false, true)

	logs := requireContentModerationLogCount(t, repo, 2)
	require.Equal(t, 2, logs[1].ViolationCount)
	require.False(t, logs[1].AutoBanned)
	require.Equal(t, StatusActive, userRepo.user.Status)
	require.Empty(t, userRepo.updated)
	require.Empty(t, invalidator.userIDs)
	require.Contains(t, slogOutput.String(), "content_moderation.autoban_skipped_admin")
	require.Contains(t, slogOutput.String(), "user_id=1001")
	require.Contains(t, slogOutput.String(), "role=admin")
	require.Contains(t, slogOutput.String(), "count=2")
	require.Contains(t, slogOutput.String(), "threshold=2")
}

func TestContentModerationAutoBanDisablesRegularUserAtThreshold(t *testing.T) {
	cfg := defaultContentModerationConfig()
	cfg.BanThreshold = 2
	cfg.ViolationWindowHours = 24

	userID := int64(1001)
	repo := &contentModerationTestRepo{}
	require.NoError(t, repo.CreateLog(context.Background(), newContentModerationFlaggedLog(userID)))
	userRepo := &contentModerationTestUserRepo{user: &User{ID: userID, Role: RoleUser, Status: StatusActive}}
	invalidator := &contentModerationTestAuthCacheInvalidator{}
	svc := NewContentModerationService(nil, repo, nil, nil, userRepo, nil, invalidator, nil)

	svc.persistContentModerationLog(context.Background(), cfg, newContentModerationFlaggedLog(userID), "", false, true)

	logs := requireContentModerationLogCount(t, repo, 2)
	require.Equal(t, 2, logs[1].ViolationCount)
	require.True(t, logs[1].AutoBanned)
	require.Len(t, userRepo.updated, 1)
	require.Equal(t, StatusDisabled, userRepo.user.Status)
	require.Equal(t, []int64{userID}, invalidator.userIDs)
}

func TestContentModerationAdminBelowBanThresholdRecordsViolationOnly(t *testing.T) {
	cfg := defaultContentModerationConfig()
	cfg.BanThreshold = 2
	cfg.ViolationWindowHours = 24

	userID := int64(1001)
	repo := &contentModerationTestRepo{}
	userRepo := &contentModerationTestUserRepo{user: &User{ID: userID, Role: RoleAdmin, Status: StatusActive}}
	invalidator := &contentModerationTestAuthCacheInvalidator{}
	svc := NewContentModerationService(nil, repo, nil, nil, userRepo, nil, invalidator, nil)

	svc.persistContentModerationLog(context.Background(), cfg, newContentModerationFlaggedLog(userID), "", false, true)

	logs := requireContentModerationLogCount(t, repo, 1)
	require.Equal(t, 1, logs[0].ViolationCount)
	require.False(t, logs[0].AutoBanned)
	require.Equal(t, StatusActive, userRepo.user.Status)
	require.Empty(t, userRepo.updated)
	require.Empty(t, invalidator.userIDs)
}

func newContentModerationFlaggedLog(userID int64) *ContentModerationLog {
	return &ContentModerationLog{
		UserID:          &userID,
		Action:          ContentModerationActionBlock,
		Flagged:         true,
		HighestCategory: "sexual",
		HighestScore:    0.9,
		CreatedAt:       time.Now(),
	}
}

func TestContentModerationCheck_PreBlockFlaggedWritesRedisHashCache(t *testing.T) {
	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		_ = json.NewEncoder(w).Encode(moderationAPIResponse{
			Results: []moderationAPIResult{{
				CategoryScores: map[string]float64{"sexual": 0.9},
			}},
		})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModePreBlock
	cfg.PreHashCheckEnabled = true
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	cfg.BlockStatus = http.StatusConflict
	cfg.BlockMessage = "命中风险输入"
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	repo := &contentModerationTestRepo{}
	hashCache := &contentModerationTestHashCache{}
	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo,
		hashCache,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	body := []byte(`{"messages":[{"role":"user","content":"repeat blocked prompt"}]}`)
	decision, err := svc.Check(context.Background(), ContentModerationCheckInput{
		Protocol: ContentModerationProtocolOpenAIChat,
		Body:     body,
	})
	require.NoError(t, err)
	require.True(t, decision.Blocked)
	require.Equal(t, ContentModerationActionBlock, decision.Action)
	require.Equal(t, 1, requestCount)
	recorded := requireRecordedHashCount(t, hashCache, 1)
	requireContentModerationLogCount(t, repo, 1)

	decision, err = svc.Check(context.Background(), ContentModerationCheckInput{
		Protocol: ContentModerationProtocolOpenAIChat,
		Body:     body,
	})
	require.NoError(t, err)
	require.True(t, decision.Blocked)
	require.Equal(t, ContentModerationActionHashBlock, decision.Action)
	require.Equal(t, recorded[0], decision.InputHash)
	require.Equal(t, 1, requestCount)
	logs := requireContentModerationLogCount(t, repo, 2)
	require.Equal(t, ContentModerationActionBlock, logs[0].Action)
	require.Equal(t, ContentModerationActionHashBlock, logs[1].Action)
}

func TestContentModerationDeleteFlaggedInputHash_NormalizesAndDeletes(t *testing.T) {
	existingHash := strings.Repeat("a", 64)
	hashCache := &contentModerationTestHashCache{hashes: map[string]struct{}{
		existingHash: {},
	}}
	svc := &ContentModerationService{hashCache: hashCache}

	result, err := svc.DeleteFlaggedInputHash(context.Background(), strings.ToUpper(existingHash))

	require.NoError(t, err)
	require.Equal(t, existingHash, result.InputHash)
	require.True(t, result.Deleted)
	require.False(t, hashCache.hasHash(existingHash))
	require.Equal(t, []string{existingHash}, hashCache.snapshotDeleted())

	result, err = svc.DeleteFlaggedInputHash(context.Background(), existingHash)

	require.NoError(t, err)
	require.Equal(t, existingHash, result.InputHash)
	require.False(t, result.Deleted)
}

func TestContentModerationClearFlaggedInputHashesAndStatusCount(t *testing.T) {
	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	hashCache := &contentModerationTestHashCache{hashes: map[string]struct{}{
		strings.Repeat("a", 64): {},
		strings.Repeat("b", 64): {},
	}}
	svc := &ContentModerationService{
		settingRepo: &contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		hashCache: hashCache,
		keyHealth: make(map[string]*contentModerationKeyHealth),
	}

	status, err := svc.GetStatus(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), status.FlaggedHashCount)

	result, err := svc.ClearFlaggedInputHashes(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), result.Deleted)

	status, err = svc.GetStatus(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(0), status.FlaggedHashCount)
}

func TestContentModerationCheck_AsyncFlaggedWritesRedisHashCache(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(moderationAPIResponse{
			Results: []moderationAPIResult{{
				CategoryScores: map[string]float64{"sexual": 0.9},
			}},
		})
	}))
	defer server.Close()

	cfg := defaultContentModerationConfig()
	cfg.Enabled = true
	cfg.Mode = ContentModerationModeObserve
	cfg.BaseURL = server.URL
	cfg.APIKeys = []string{"sk-test"}
	rawCfg, err := json.Marshal(cfg)
	require.NoError(t, err)

	repo := &contentModerationTestRepo{}
	hashCache := &contentModerationTestHashCache{}
	svc := NewContentModerationService(
		&contentModerationTestSettingRepo{values: map[string]string{
			SettingKeyRiskControlEnabled:      "true",
			SettingKeyContentModerationConfig: string(rawCfg),
		}},
		repo,
		hashCache,
		nil,
		nil,
		nil,
		nil,
		nil,
	)

	decision := svc.checkSync(context.Background(), ContentModerationCheckInput{
		Protocol: ContentModerationProtocolOpenAIChat,
		Body:     []byte(`{"messages":[{"role":"user","content":"bad prompt"}]}`),
	}, cfg, ContentModerationInput{Text: "bad prompt"}, strings.Repeat("b", 64), contentModerationIntPtr(25), false)

	require.False(t, decision.Blocked)
	requireRecordedHashCount(t, hashCache, 1)
	requireContentModerationLogCount(t, repo, 1)
}

func TestBuildContentModerationAccountDisabledEmailBody_ContainsBanDetails(t *testing.T) {
	userID := int64(1001)
	cfg := defaultContentModerationConfig()
	cfg.BanThreshold = 10
	body := buildContentModerationAccountDisabledEmailBody("Sub2API <Admin>", &ContentModerationLog{
		UserID:          &userID,
		UserEmail:       "user@example.com",
		GroupName:       "vip_2",
		HighestCategory: "sexual",
		HighestScore:    0.926,
		ViolationCount:  10,
	}, cfg)

	require.Contains(t, body, "账户已被自动禁用")
	require.Contains(t, body, "封禁详情")
	require.Contains(t, body, "账户当前处于封禁状态，所有 API 请求将被拒绝")
	require.Contains(t, body, "10 次（阈值 10）")
	require.Contains(t, body, "sexual / 0.926")
	require.Contains(t, body, "Sub2API &lt;Admin&gt;")
}

func TestContentModerationUnbanUser_ActivatesUserAndInvalidatesAuthCache(t *testing.T) {
	userRepo := &contentModerationTestUserRepo{user: &User{ID: 1001, Email: "user@example.com", Status: StatusDisabled}}
	invalidator := &contentModerationTestAuthCacheInvalidator{}
	repo := &contentModerationTestRepo{}
	svc := NewContentModerationService(nil, repo, nil, nil, userRepo, nil, invalidator, nil)

	result, err := svc.UnbanUser(context.Background(), 1001)

	require.NoError(t, err)
	require.Equal(t, int64(1001), result.UserID)
	require.Equal(t, StatusActive, result.Status)
	require.Len(t, userRepo.updated, 1)
	require.Equal(t, StatusActive, userRepo.updated[0].Status)
	require.Equal(t, []int64{1001}, invalidator.userIDs)
}

func TestContentModerationUnbanUser_ActiveUserOnlyInvalidatesAuthCache(t *testing.T) {
	userRepo := &contentModerationTestUserRepo{user: &User{ID: 1001, Email: "user@example.com", Status: StatusActive}}
	invalidator := &contentModerationTestAuthCacheInvalidator{}
	repo := &contentModerationTestRepo{}
	svc := NewContentModerationService(nil, repo, nil, nil, userRepo, nil, invalidator, nil)

	result, err := svc.UnbanUser(context.Background(), 1001)

	require.NoError(t, err)
	require.Equal(t, StatusActive, result.Status)
	require.Empty(t, userRepo.updated)
	require.Equal(t, []int64{1001}, invalidator.userIDs)
}

func contentModerationIntPtr(v int) *int {
	return &v
}

func TestContentModerationUpdateConfig_CyberPolicyExcludeFromBanCount(t *testing.T) {
	settingRepo := &contentModerationTestSettingRepo{values: map[string]string{}}
	svc := NewContentModerationService(settingRepo, nil, nil, nil, nil, nil, nil, nil)

	// 默认值必须是 false（计入，保持现状）
	view, err := svc.GetConfig(context.Background())
	require.NoError(t, err)
	require.False(t, view.CyberPolicyExcludeFromBanCount, "默认必须计入封号计数")

	// 指针式部分更新为 true
	exclude := true
	view, err = svc.UpdateConfig(context.Background(), UpdateContentModerationConfigInput{
		CyberPolicyExcludeFromBanCount: &exclude,
	})
	require.NoError(t, err)
	require.True(t, view.CyberPolicyExcludeFromBanCount)

	// 持久化 JSON 含字段
	var saved ContentModerationConfig
	require.NoError(t, json.Unmarshal([]byte(settingRepo.values[SettingKeyContentModerationConfig]), &saved))
	require.True(t, saved.CyberPolicyExcludeFromBanCount)

	// 二次读取（从持久化 JSON 反序列化）roundtrip
	view, err = svc.GetConfig(context.Background())
	require.NoError(t, err)
	require.True(t, view.CyberPolicyExcludeFromBanCount)

	// 不传该字段的更新不得改动它（指针 nil = 保留）
	view, err = svc.UpdateConfig(context.Background(), UpdateContentModerationConfigInput{})
	require.NoError(t, err)
	require.True(t, view.CyberPolicyExcludeFromBanCount)

	// 主动回拨 false 必须生效（防止未来误加 if val 保护逻辑）
	revert := false
	view, err = svc.UpdateConfig(context.Background(), UpdateContentModerationConfigInput{
		CyberPolicyExcludeFromBanCount: &revert,
	})
	require.NoError(t, err)
	require.False(t, view.CyberPolicyExcludeFromBanCount)
}
