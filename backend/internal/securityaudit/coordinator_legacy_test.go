package securityaudit

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/Wei-Shaw/sub2api/internal/service"
)

type legacyModerationFake struct {
	mu         sync.Mutex
	texts      []string
	active     atomic.Int32
	maxActive  atomic.Int32
	audited    bool
	blockToken string
	err        error
}

func (f *legacyModerationFake) Check(ctx context.Context, input service.ContentModerationCheckInput) (*service.ContentModerationDecision, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	active := f.active.Add(1)
	for {
		previous := f.maxActive.Load()
		if active <= previous || f.maxActive.CompareAndSwap(previous, active) {
			break
		}
	}
	defer f.active.Add(-1)
	f.mu.Lock()
	f.texts = append(f.texts, input.CanonicalText)
	f.mu.Unlock()
	time.Sleep(time.Millisecond)
	if f.err != nil {
		return nil, f.err
	}
	if f.blockToken != "" && strings.Contains(input.CanonicalText, f.blockToken) {
		return &service.ContentModerationDecision{Blocked: true, Flagged: true, Audited: f.audited, Allowed: false,
			Action: service.ContentModerationActionBlock, StatusCode: 403}, nil
	}
	return &service.ContentModerationDecision{Allowed: true, Audited: f.audited,
		Action: service.ContentModerationActionAllow}, nil
}

func TestSplitCanonicalTextPreservesEveryRune(t *testing.T) {
	const limit = 12000
	text := strings.Repeat("a", limit-1) + "😀" + strings.Repeat("中", limit+3) + "尾部-canary"
	chunks := splitCanonicalText(text, limit)
	if len(chunks) < 2 {
		t.Fatalf("chunks=%d want multiple chunks", len(chunks))
	}
	if strings.Join(chunks, "") != text {
		t.Fatal("chunk join changed canonical text")
	}
	for index, chunk := range chunks {
		if got := utf8.RuneCountInString(chunk); got > limit {
			t.Fatalf("chunk %d has %d runes, limit=%d", index, got, limit)
		}
	}
	if !strings.Contains(chunks[len(chunks)-1], "尾部-canary") {
		t.Fatal("last chunk lost tail content")
	}
}

func TestSplitCanonicalTextHandlesEmptyAndInvalidLimits(t *testing.T) {
	if got := splitCanonicalText("", 12); got != nil {
		t.Fatalf("empty text chunks=%v", got)
	}
	if got := splitCanonicalText("text", 0); got != nil {
		t.Fatalf("zero limit chunks=%v", got)
	}
}

func TestLegacyModerationChunksStopWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	adapter := &LegacyModerationAdapter{service: &service.ContentModerationService{}}
	_, err := adapter.checkCanonicalTextChunks(ctx, service.ContentModerationCheckInput{}, strings.Repeat("x", service.ContentModerationMaxInputRunes()*2))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v, want context canceled", err)
	}
}

func TestLegacyModerationChunksDeduplicateAndBoundConcurrency(t *testing.T) {
	limit := service.ContentModerationMaxInputRunes()
	fake := &legacyModerationFake{audited: true}
	adapter := &LegacyModerationAdapter{service: fake}
	text := strings.Repeat("a", limit) + strings.Repeat("b", limit) + strings.Repeat("a", limit)

	decision, err := adapter.checkCanonicalTextChunks(context.Background(), service.ContentModerationCheckInput{}, text)
	if err != nil {
		t.Fatalf("check chunks: %v", err)
	}
	if decision == nil || !decision.Allowed || !decision.Audited {
		t.Fatalf("decision=%+v", decision)
	}
	fake.mu.Lock()
	callCount := len(fake.texts)
	fake.mu.Unlock()
	if callCount != 2 {
		t.Fatalf("moderation calls=%d want 2 unique chunks", callCount)
	}
	if got := fake.maxActive.Load(); got < 2 || got > canonicalAuditWorkerCount {
		t.Fatalf("max concurrent calls=%d want between 2 and %d", got, canonicalAuditWorkerCount)
	}
}

func TestLegacyModerationAdapterKeepsOverBudgetCanonicalTextAuditable(t *testing.T) {
	body := []byte(`{"model":"gpt-test","messages":[{"role":"user","content":"` +
		strings.Repeat("x", securityadmission.MaxAuditableTextRunes+1) + `"}]}`)
	admission, err := securityadmission.ClassifyWithResourceExpansion(
		string(securityadmission.ProtocolOpenAIChat), body, securityadmission.Options{},
	)
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if admission.Class() != securityadmission.RequestAuditableText ||
		admission.Requirement() != securityadmission.AccountRequirementAny {
		t.Fatalf("admission=%+v", admission)
	}

	adapter := &LegacyModerationAdapter{service: &service.ContentModerationService{}}
	_, err = adapter.Check(context.Background(), Request{
		Protocol: string(securityadmission.ProtocolOpenAIChat), Body: body, Admission: &admission,
	})
	if err == nil {
		t.Fatal("expected the unavailable test service to fail closed, not a budget rejection")
	}
}

func TestLegacyModerationAdapterAuditsEveryCanonicalChunkAndAllows(t *testing.T) {
	limit := service.ContentModerationMaxInputRunes()
	text := strings.Repeat("a", limit) + strings.Repeat("b", limit) +
		strings.Repeat("c", limit) + strings.Repeat("d", limit) +
		strings.Repeat("e", limit) + strings.Repeat("f", securityadmission.MaxAuditableTextRunes+1-5*limit)
	body := []byte(`{"model":"gpt-test","messages":[{"role":"user","content":"` + text + `"}]}`)
	admission, err := securityadmission.ClassifyWithResourceExpansion(
		string(securityadmission.ProtocolOpenAIChat), body, securityadmission.Options{},
	)
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	fake := &legacyModerationFake{audited: true}
	adapter := &LegacyModerationAdapter{service: fake}
	decision, err := adapter.Check(context.Background(), Request{
		Protocol: string(securityadmission.ProtocolOpenAIChat), Body: body, Admission: &admission,
	})
	if err != nil {
		t.Fatalf("legacy check: %v", err)
	}
	if decision == nil || !decision.Allowed || !decision.Audited || decision.Blocked {
		t.Fatalf("decision=%+v", decision)
	}
	fake.mu.Lock()
	callCount := len(fake.texts)
	fake.mu.Unlock()
	if callCount != 6 {
		t.Fatalf("moderation calls=%d want 6 chunks", callCount)
	}
}

func TestLegacyModerationAdapterPropagatesCanonicalChunkBlock(t *testing.T) {
	limit := service.ContentModerationMaxInputRunes()
	text := strings.Repeat("a", limit) + "BLOCK_CANARY" + strings.Repeat("b", limit)
	fake := &legacyModerationFake{audited: true, blockToken: "BLOCK_CANARY"}
	adapter := &LegacyModerationAdapter{service: fake}
	decision, err := adapter.checkCanonicalTextChunks(context.Background(), service.ContentModerationCheckInput{}, text)
	if err != nil {
		t.Fatalf("legacy check: %v", err)
	}
	if decision == nil || !decision.Blocked || decision.Allowed || !decision.Audited {
		t.Fatalf("decision=%+v", decision)
	}
}

func TestLegacyModerationAdapterFailsClosedOnUnavailableOrUnauditedChunk(t *testing.T) {
	limit := service.ContentModerationMaxInputRunes()
	text := strings.Repeat("x", limit+1)
	for _, test := range []struct {
		name    string
		fake    *legacyModerationFake
		wantErr string
	}{
		{name: "unavailable", fake: &legacyModerationFake{err: errors.New("legacy unavailable")}, wantErr: "legacy unavailable"},
		{name: "unaudited", fake: &legacyModerationFake{audited: false}, wantErr: "did not produce an audit proof"},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter := &LegacyModerationAdapter{service: test.fake}
			decision, err := adapter.checkCanonicalTextChunks(context.Background(), service.ContentModerationCheckInput{}, text)
			if decision != nil {
				t.Fatalf("decision=%+v want nil", decision)
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("err=%v want substring %q", err, test.wantErr)
			}
		})
	}
}

func TestMaxAuditableTextRunesBoundsLegacyChunkFanout(t *testing.T) {
	chunks := splitCanonicalText(strings.Repeat("x", securityadmission.MaxAuditableTextRunes), service.ContentModerationMaxInputRunes())
	if len(chunks) != 6 {
		t.Fatalf("chunks=%d want 6", len(chunks))
	}
}
