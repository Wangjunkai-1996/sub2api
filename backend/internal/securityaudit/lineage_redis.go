package securityaudit

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/redis/go-redis/v9"
)

const (
	strictAuditLineageLedgerVersion = 2
	strictAuditLineageKeyPrefix     = "sub2api:strict_audit_lineage:v2:"
	strictAuditLineageRedisTimeout  = 3 * time.Second
)

type strictAuditLineageLedger struct {
	Version            int      `json:"version"`
	ParserVersion      string   `json:"parser_version"`
	ConfigVersion      int64    `json:"config_version"`
	ParentResponseHash string   `json:"parent_response_hash,omitempty"`
	ParentPromptHash   string   `json:"parent_prompt_hash,omitempty"`
	PromptHash         string   `json:"prompt_hash"`
	DocumentHash       string   `json:"document_hash"`
	Context            string   `json:"context"`
	MediaDigests       []string `json:"media_digests,omitempty"`
	ContextComplete    bool     `json:"context_complete"`
	Verdict            string   `json:"verdict"`
	CreatedAtUnix      int64    `json:"created_at_unix"`
}

// RedisStrictAuditLineageStore keeps strict audit lineage separate from the
// routing sticky cache. Redis is authoritative: there is intentionally no
// process-local fallback for continuation admission.
type RedisStrictAuditLineageStore struct {
	client *redis.Client
	secret []byte
	ttl    time.Duration
	now    func() time.Time
}

func NewRedisStrictAuditLineageStore(client *redis.Client, cfg *config.Config) *RedisStrictAuditLineageStore {
	secret := ""
	ttl := time.Hour
	if cfg != nil {
		secret = strings.TrimSpace(cfg.JWT.Secret)
		if seconds := cfg.Gateway.OpenAIWS.StickyResponseIDTTLSeconds; seconds > 0 {
			ttl = time.Duration(seconds) * time.Second
		}
	}
	return &RedisStrictAuditLineageStore{
		client: client,
		secret: []byte(secret),
		ttl:    ttl,
		now:    time.Now,
	}
}

func (s *RedisStrictAuditLineageStore) Load(ctx context.Context, lookup LineageLookup) (*AuditSummary, error) {
	key, err := s.key(lookup.GroupID, lookup.APIKeyID, lookup.PreviousResponseID)
	if err != nil {
		return nil, err
	}
	readCtx, cancel := strictAuditLineageContext(ctx)
	defer cancel()
	raw, err := s.client.Get(readCtx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrLineageNotFound
		}
		return nil, fmt.Errorf("load strict audit lineage: %w", err)
	}
	var ledger strictAuditLineageLedger
	if err := json.Unmarshal(raw, &ledger); err != nil {
		return nil, fmt.Errorf("decode strict audit lineage: %w", err)
	}
	if !validStrictAuditLineageLedger(ledger) {
		return nil, ErrLineageInvalid
	}
	return &AuditSummary{
		ParserVersion:     ledger.ParserVersion,
		ConfigVersion:     ledger.ConfigVersion,
		APIKeyID:          lookup.APIKeyID,
		GroupID:           cloneInt64Ptr(lookup.GroupID),
		PromptHash:        ledger.PromptHash,
		DocumentHash:      ledger.DocumentHash,
		NormalizedContext: ledger.Context,
		RedactedContext:   ledger.Context,
		MediaDigests:      append([]string(nil), ledger.MediaDigests...),
		ContextComplete:   true,
		Verdict:           AuditVerdictAllow,
	}, nil
}

func (s *RedisStrictAuditLineageStore) BindAllowedResponse(ctx context.Context, summary AuditSummary, responseID string) error {
	key, err := s.key(summary.GroupID, summary.APIKeyID, responseID)
	if err != nil {
		return err
	}
	redactedContext := strings.TrimSpace(summary.RedactedContext)
	if summary.Verdict != AuditVerdictAllow || !summary.ContextComplete || strings.TrimSpace(summary.PromptHash) == "" || !auditSummaryHasContext(summary) {
		return ErrLineageInvalid
	}
	parentResponseHash := ""
	if parent := strings.TrimSpace(summary.PreviousResponseID); parent != "" {
		parentResponseHash = strictAuditLineageDigest(parent)
	}
	ledger := strictAuditLineageLedger{
		Version:            strictAuditLineageLedgerVersion,
		ParserVersion:      strings.TrimSpace(summary.ParserVersion),
		ConfigVersion:      summary.ConfigVersion,
		ParentResponseHash: parentResponseHash,
		ParentPromptHash:   strings.TrimSpace(summary.ParentPromptHash),
		PromptHash:         strings.TrimSpace(summary.PromptHash),
		DocumentHash:       strings.TrimSpace(summary.DocumentHash),
		Context:            redactedContext,
		MediaDigests:       append([]string(nil), summary.MediaDigests...),
		ContextComplete:    true,
		Verdict:            AuditVerdictAllow,
		CreatedAtUnix:      s.now().UTC().Unix(),
	}
	if !validStrictAuditLineageLedger(ledger) {
		return ErrLineageInvalid
	}
	raw, err := json.Marshal(ledger)
	if err != nil {
		return fmt.Errorf("encode strict audit lineage: %w", err)
	}
	writeCtx, cancel := strictAuditLineageContext(ctx)
	defer cancel()
	if err := s.client.Set(writeCtx, key, raw, s.ttl).Err(); err != nil {
		return fmt.Errorf("bind strict audit lineage: %w", err)
	}
	return nil
}

func (s *RedisStrictAuditLineageStore) key(groupID *int64, apiKeyID int64, responseID string) (string, error) {
	if s == nil || s.client == nil || len(s.secret) < 32 || groupID == nil || *groupID <= 0 || apiKeyID <= 0 || strings.TrimSpace(responseID) == "" {
		return "", errors.New("strict audit lineage store unavailable")
	}
	mac := hmac.New(sha256.New, s.secret)
	_, _ = mac.Write([]byte(strconv.FormatInt(*groupID, 10)))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(strconv.FormatInt(apiKeyID, 10)))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(strings.TrimSpace(responseID)))
	return strictAuditLineageKeyPrefix + hex.EncodeToString(mac.Sum(nil)), nil
}

func validStrictAuditLineageLedger(ledger strictAuditLineageLedger) bool {
	return ledger.Version == strictAuditLineageLedgerVersion &&
		strings.TrimSpace(ledger.ParserVersion) != "" &&
		ledger.ConfigVersion > 0 &&
		strings.TrimSpace(ledger.PromptHash) != "" &&
		strings.TrimSpace(ledger.DocumentHash) != "" &&
		(strings.TrimSpace(ledger.Context) != "" || len(ledger.MediaDigests) > 0) &&
		ledger.ContextComplete && ledger.Verdict == AuditVerdictAllow && ledger.CreatedAtUnix > 0
}

func strictAuditLineageDigest(value string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(value)))
	return hex.EncodeToString(sum[:])
}

func strictAuditLineageContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, strictAuditLineageRedisTimeout)
}

var _ LineageStore = (*RedisStrictAuditLineageStore)(nil)
