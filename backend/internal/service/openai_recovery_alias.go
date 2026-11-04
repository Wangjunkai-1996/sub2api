package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/ctxkey"
)

const (
	openAIRecoveryAliasPrefix      = "resp_lr_"
	openAIRecoveryAliasMaxMetadata = 4 << 10
	openAIRecoveryAliasMaxSnapshot = 8 << 20
)

var (
	ErrOpenAIRecoveryAliasUnavailable = errors.New("OpenAI recovery alias store unavailable")
	ErrOpenAIRecoveryAliasNotFound    = errors.New("OpenAI recovery alias not found")
	ErrOpenAIRecoveryAliasConflict    = errors.New("OpenAI recovery alias state conflict")
	ErrOpenAIRecoveryAliasCapacity    = errors.New("OpenAI recovery alias capacity exceeded")
	ErrOpenAIRecoveryAliasInvalid     = errors.New("invalid OpenAI recovery alias")
)

// OpenAIRecoveryAliasWinner is the actual upstream identity behind a public
// logical response ID. Snapshot is only used for a separately validated
// cross-account replay; same-account continuation does not require it.
type OpenAIRecoveryAliasWinner struct {
	ResponseID      string `json:"response_id"`
	AccountID       int64  `json:"account_id"`
	EgressID        string `json:"egress_id,omitempty"`
	TurnStatePolicy string `json:"turn_state_policy"`
	Snapshot        []byte `json:"-"`
}

type openAIRecoveryAliasRecord struct {
	Version     int                       `json:"version"`
	Revision    int                       `json:"revision"`
	State       string                    `json:"state"`
	LogicalID   string                    `json:"logical_id"`
	Scope       string                    `json:"scope"`
	CreatedAt   int64                     `json:"created_at"`
	ExpiresAt   int64                     `json:"expires_at"`
	HasSnapshot bool                      `json:"has_snapshot"`
	Winner      OpenAIRecoveryAliasWinner `json:"winner"`
}

// OpenAIRecoveryAliasCache is additive to GatewayCache so existing cache
// implementations and tests need no new methods. Production Redis implements
// winner publication and capacity admission atomically.
type OpenAIRecoveryAliasCache interface {
	PublishOpenAIRecoveryAlias(ctx context.Context, scope, alias string, metadata, snapshot []byte, ttl time.Duration) (bool, error)
	GetOpenAIRecoveryAlias(ctx context.Context, scope, alias string) (metadata, snapshot []byte, err error)
	DeleteOpenAIRecoveryAlias(ctx context.Context, scope, alias string) error
}

// OpenAIRecoveryScope is available only after NewAPI attribution verification.
// The Sub2 API key ID is part of the namespace but not a substitute for the
// terminal NewAPI user and token identities.
func OpenAIRecoveryScope(ctx context.Context, apiKeyID int64) (string, bool) {
	if ctx == nil || apiKeyID <= 0 {
		return "", false
	}
	userID, _ := ctx.Value(ctxkey.NewAPIUserID).(int64)
	tokenID, _ := ctx.Value(ctxkey.NewAPITokenID).(int64)
	origin, _ := ctx.Value(ctxkey.NewAPIOrigin).(string)
	origin = strings.TrimSpace(origin)
	if userID <= 0 || tokenID <= 0 || origin == "" {
		return "", false
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%d\x00%d", apiKeyID, origin, userID, tokenID)))
	return hex.EncodeToString(sum[:]), true
}

func NewOpenAIRecoveryAliasID() (string, error) {
	var random [18]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return openAIRecoveryAliasPrefix + hex.EncodeToString(random[:]), nil
}

func IsOpenAIRecoveryAliasID(id string) bool {
	return strings.HasPrefix(id, openAIRecoveryAliasPrefix)
}

func validOpenAIRecoveryAlias(scope, alias string) bool {
	if len(scope) != 64 || len(alias) != len(openAIRecoveryAliasPrefix)+36 || !IsOpenAIRecoveryAliasID(alias) {
		return false
	}
	if _, err := hex.DecodeString(scope); err != nil {
		return false
	}
	_, err := hex.DecodeString(strings.TrimPrefix(alias, openAIRecoveryAliasPrefix))
	return err == nil
}

func (s *OpenAIGatewayService) openAIRecoveryAliasCache() (OpenAIRecoveryAliasCache, error) {
	if s == nil || s.cache == nil {
		return nil, ErrOpenAIRecoveryAliasUnavailable
	}
	cache, ok := s.cache.(OpenAIRecoveryAliasCache)
	if !ok {
		return nil, ErrOpenAIRecoveryAliasUnavailable
	}
	return cache, nil
}

func (s *OpenAIGatewayService) OpenAIRecoveryAliasTTL() time.Duration {
	if s == nil {
		return 0
	}
	return s.openAIWSResponseStickyTTL()
}

func (s *OpenAIGatewayService) PublishOpenAIRecoveryAlias(ctx context.Context, scope, alias string, winner OpenAIRecoveryAliasWinner, ttl time.Duration) error {
	if !validOpenAIRecoveryAlias(scope, alias) || strings.TrimSpace(winner.ResponseID) == "" || IsOpenAIRecoveryAliasID(winner.ResponseID) || winner.AccountID <= 0 || ttl < time.Millisecond || len(winner.Snapshot) > openAIRecoveryAliasMaxSnapshot {
		return ErrOpenAIRecoveryAliasInvalid
	}
	if winner.TurnStatePolicy == "" {
		winner.TurnStatePolicy = "strip_old_echo"
	}
	if winner.TurnStatePolicy != "strip_old_echo" {
		return ErrOpenAIRecoveryAliasInvalid
	}
	cache, err := s.openAIRecoveryAliasCache()
	if err != nil {
		return err
	}
	now := time.Now()
	metadata, err := json.Marshal(openAIRecoveryAliasRecord{
		Version: 1, Revision: 1, State: "published", LogicalID: alias, Scope: scope,
		CreatedAt: now.UnixMilli(), ExpiresAt: now.Add(ttl).UnixMilli(), HasSnapshot: len(winner.Snapshot) != 0, Winner: winner,
	})
	if err != nil || len(metadata) > openAIRecoveryAliasMaxMetadata {
		return ErrOpenAIRecoveryAliasInvalid
	}
	published, err := cache.PublishOpenAIRecoveryAlias(ctx, scope, alias, metadata, winner.Snapshot, ttl)
	if err != nil {
		return err
	}
	if !published {
		return ErrOpenAIRecoveryAliasConflict
	}
	return nil
}

func (s *OpenAIGatewayService) ResolveOpenAIRecoveryAlias(ctx context.Context, scope, alias string) (OpenAIRecoveryAliasWinner, error) {
	if !validOpenAIRecoveryAlias(scope, alias) {
		return OpenAIRecoveryAliasWinner{}, ErrOpenAIRecoveryAliasInvalid
	}
	cache, err := s.openAIRecoveryAliasCache()
	if err != nil {
		return OpenAIRecoveryAliasWinner{}, err
	}
	metadata, snapshot, err := cache.GetOpenAIRecoveryAlias(ctx, scope, alias)
	if err != nil {
		return OpenAIRecoveryAliasWinner{}, err
	}
	if len(metadata) == 0 || string(metadata) == "pending" {
		return OpenAIRecoveryAliasWinner{}, ErrOpenAIRecoveryAliasNotFound
	}
	var record openAIRecoveryAliasRecord
	if len(metadata) > openAIRecoveryAliasMaxMetadata || json.Unmarshal(metadata, &record) != nil || record.Version != 1 || record.Revision != 1 || record.State != "published" || record.LogicalID != alias || record.Scope != scope || record.CreatedAt <= 0 || record.ExpiresAt <= record.CreatedAt || record.Winner.AccountID <= 0 || strings.TrimSpace(record.Winner.ResponseID) == "" || IsOpenAIRecoveryAliasID(record.Winner.ResponseID) || record.Winner.TurnStatePolicy != "strip_old_echo" || len(snapshot) > openAIRecoveryAliasMaxSnapshot || record.HasSnapshot != (len(snapshot) > 0) {
		return OpenAIRecoveryAliasWinner{}, ErrOpenAIRecoveryAliasInvalid
	}
	if time.Now().UnixMilli() >= record.ExpiresAt {
		return OpenAIRecoveryAliasWinner{}, ErrOpenAIRecoveryAliasNotFound
	}
	record.Winner.Snapshot = snapshot
	return record.Winner, nil
}

func (s *OpenAIGatewayService) DeleteOpenAIRecoveryAlias(ctx context.Context, scope, alias string) error {
	if !validOpenAIRecoveryAlias(scope, alias) {
		return ErrOpenAIRecoveryAliasInvalid
	}
	cache, err := s.openAIRecoveryAliasCache()
	if err != nil {
		return err
	}
	return cache.DeleteOpenAIRecoveryAlias(ctx, scope, alias)
}
