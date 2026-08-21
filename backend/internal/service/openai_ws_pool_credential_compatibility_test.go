package service

import (
	"context"
	"crypto/sha256"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/securityadmission"
	"github.com/stretchr/testify/require"
)

func TestOpenAIWSConnPool_CredentialProofPreventsCrossCredentialReuse(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	pool := newOpenAIWSConnPool(cfg)
	t.Cleanup(pool.Close)
	dialer := &openAIWSCredentialCompatibilityDialer{}
	pool.setClientDialerForTest(dialer)

	selected := &Account{ID: 7101, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	oldProProof := openAIWSTestCredentialProof(
		selected.ID,
		8101,
		securityadmission.AccountAuditRequired,
		"old-pro-parent-token",
	)
	currentPlusProof := openAIWSTestCredentialProof(
		selected.ID,
		8102,
		securityadmission.AccountAuditExemptVerified,
		"current-plus-parent-token",
	)
	request := func(proof *OpenAICredentialProof) openAIWSAcquireRequest {
		return openAIWSAcquireRequest{
			Account:         selected,
			WSURL:           "wss://example.com/v1/responses",
			CredentialProof: proof,
		}
	}

	oldLease, err := pool.Acquire(context.Background(), request(oldProProof))
	require.NoError(t, err)
	oldConnID := oldLease.ConnID()
	oldLease.Release()

	currentLease, err := pool.Acquire(context.Background(), request(currentPlusProof))
	require.NoError(t, err)
	require.False(t, currentLease.Reused(), "a different parent/class/token proof must dial a new connection")
	require.NotEqual(t, oldConnID, currentLease.ConnID())
	dispatches := 0
	dispatchCtx := WithOpenAIUpstreamDispatchObserver(context.Background(), func() { dispatches++ })
	require.NoError(t, currentLease.PingWithTimeout(time.Second))
	require.Zero(t, dispatches, "pool health checks are not business dispatches")
	require.NoError(t, currentLease.WriteJSONWithContextTimeout(dispatchCtx, map[string]any{"type": "response.create"}, time.Second))
	require.Equal(t, 1, dispatches)
	require.NoError(t, currentLease.WriteJSONWithContextTimeout(dispatchCtx, map[string]any{"type": "response.create"}, time.Second))
	require.Equal(t, 1, dispatches, "request-local retries share the observer once guard")
	currentLease.Release()

	connections := dialer.Connections()
	require.Len(t, connections, 2)
	require.Zero(t, connections[0].WriteCount(), "the seeded Pro/parent-A connection must receive no current request")
	require.Equal(t, 2, connections[1].WriteCount())

	sameProofLease, err := pool.Acquire(context.Background(), request(currentPlusProof))
	require.NoError(t, err)
	require.True(t, sameProofLease.Reused(), "the exact same finalized proof remains reusable")
	require.Equal(t, currentLease.ConnID(), sameProofLease.ConnID())
	sameProofLease.Release()
	require.Len(t, dialer.Connections(), 2)
}

func TestOpenAIWSConnPool_TargetAndProxyPreventCrossRouteReuse(t *testing.T) {
	for _, test := range []struct {
		name        string
		firstURL    string
		secondURL   string
		firstProxy  string
		secondProxy string
	}{
		{
			name: "upstream URL changed", firstURL: "wss://old.example/v1/responses",
			secondURL: "wss://new.example/v1/responses",
		},
		{
			name: "proxy changed", firstURL: "wss://example.com/v1/responses", secondURL: "wss://example.com/v1/responses",
			firstProxy: "http://proxy-old.example:8080", secondProxy: "http://proxy-new.example:8080",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
			cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
			pool := newOpenAIWSConnPool(cfg)
			t.Cleanup(pool.Close)
			dialer := &openAIWSCredentialCompatibilityDialer{}
			pool.setClientDialerForTest(dialer)

			account := &Account{ID: 7107, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
			proof := openAIWSTestCredentialProof(account.ID, account.ID, securityadmission.AccountAuditRequired, "stable-token")
			first, err := pool.Acquire(context.Background(), openAIWSAcquireRequest{
				Account: account, WSURL: test.firstURL, ProxyURL: test.firstProxy, CredentialProof: proof,
			})
			require.NoError(t, err)
			firstID := first.ConnID()
			first.Release()

			second, err := pool.Acquire(context.Background(), openAIWSAcquireRequest{
				Account: account, WSURL: test.secondURL, ProxyURL: test.secondProxy, CredentialProof: proof,
			})
			require.NoError(t, err)
			require.False(t, second.Reused())
			require.NotEqual(t, firstID, second.ConnID())
			second.Release()
			require.Len(t, dialer.Connections(), 2)
		})
	}
}

func TestOpenAIWSConnPool_LegacyAcquireStillReusesUnboundConnections(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	cfg.Gateway.OpenAIWS.MaxIdlePerAccount = 1
	pool := newOpenAIWSConnPool(cfg)
	t.Cleanup(pool.Close)
	dialer := &openAIWSCredentialCompatibilityDialer{}
	pool.setClientDialerForTest(dialer)

	req := openAIWSAcquireRequest{
		Account: &Account{ID: 7102, Platform: PlatformOpenAI, Type: AccountTypeAPIKey},
		WSURL:   "wss://example.com/v1/responses",
	}
	first, err := pool.Acquire(context.Background(), req)
	require.NoError(t, err)
	firstID := first.ConnID()
	first.Release()

	second, err := pool.Acquire(context.Background(), req)
	require.NoError(t, err)
	require.True(t, second.Reused())
	require.Equal(t, firstID, second.ConnID())
	second.Release()
	require.Len(t, dialer.Connections(), 1)
}

func TestOpenAIWSConnPool_AgentIdentityProofDisablesBackgroundPrewarm(t *testing.T) {
	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 2
	cfg.Gateway.OpenAIWS.MinIdlePerAccount = 1
	pool := newOpenAIWSConnPool(cfg)
	t.Cleanup(pool.Close)

	account := &Account{ID: 7104, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	proof := openAIWSTestCredentialProof(account.ID, account.ID, securityadmission.AccountAuditRequired, "")
	proof.authMode = OpenAIAuthModeAgentIdentity
	proof.tokenVersion = 44
	proof.tokenHash = [sha256.Size]byte{}
	proof.hasToken = false

	ap := pool.getOrCreateAccountPool(account.ID)
	ap.mu.Lock()
	ap.lastAcquire = &openAIWSAcquireRequest{
		Account:         account,
		WSURL:           "wss://example.com/v1/responses",
		CredentialProof: proof,
	}
	ap.mu.Unlock()

	pool.ensureTargetIdleAsync(account.ID)

	ap.mu.Lock()
	defer ap.mu.Unlock()
	require.False(t, ap.prewarmActive)
	require.Zero(t, ap.creating, "Agent Identity proof must never schedule context-free prewarm")
}

func TestOpenAIWSConnPool_AgentIdentityTaskRecoveryKeepsStableProof(t *testing.T) {
	_, privateKey := newTestAgentIdentityKey(t)
	parentID := int64(8104)
	selected := &Account{ID: 7105, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ParentAccountID: &parentID}
	owner := &Account{
		ID: parentID, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Credentials: map[string]any{
			"auth_mode":                  OpenAIAuthModeAgentIdentity,
			"agent_runtime_id":           "runtime-stable",
			"agent_private_key":          privateKey,
			"task_id":                    "task-before-recovery",
			"chatgpt_account_id":         "tenant-stable",
			"chatgpt_user_id":            "user-stable",
			"chatgpt_account_is_fedramp": false,
			"plan_type":                  "plus",
		},
	}
	admission := &OpenAIAccountRequirementAdmission{
		Selected: selected, EffectiveCredentialOwner: owner,
		AccountClass: securityadmission.AccountAuditExemptVerified,
	}
	before, err := newOpenAICredentialProof(admission, OpenAIAuthModeAgentIdentity, [sha256.Size]byte{}, false)
	require.NoError(t, err)

	recoveredOwner := *owner
	recoveredOwner.Credentials = shallowCopyMap(owner.Credentials)
	recoveredOwner.Credentials["task_id"] = "task-after-recovery"
	recoveredOwner.Credentials["_token_version"] = int64(99)
	recoveredAdmission := *admission
	recoveredAdmission.EffectiveCredentialOwner = &recoveredOwner
	after, err := newOpenAICredentialProof(&recoveredAdmission, OpenAIAuthModeAgentIdentity, [sha256.Size]byte{}, false)
	require.NoError(t, err)
	require.Equal(t, before, after, "task recovery and an optional version stamp must not split stable identity proof")

	cfg := &config.Config{}
	cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
	pool := newOpenAIWSConnPool(cfg)
	t.Cleanup(pool.Close)
	dialer := &openAIWSCredentialCompatibilityDialer{}
	pool.setClientDialerForTest(dialer)
	request := func(proof *OpenAICredentialProof) openAIWSAcquireRequest {
		return openAIWSAcquireRequest{Account: selected, WSURL: "wss://example.com/v1/responses", CredentialProof: proof}
	}
	first, err := pool.Acquire(context.Background(), request(before))
	require.NoError(t, err)
	firstID := first.ConnID()
	first.Release()
	second, err := pool.Acquire(context.Background(), request(after))
	require.NoError(t, err)
	require.True(t, second.Reused())
	require.Equal(t, firstID, second.ConnID())
	second.Release()
	require.Len(t, dialer.Connections(), 1)
}

func TestOpenAIWSConnPool_AgentIdentityMaterialDriftDoesNotReuseShadowConnection(t *testing.T) {
	_, privateKey := newTestAgentIdentityKey(t)
	parentID := int64(8105)
	selected := &Account{ID: 7106, Platform: PlatformOpenAI, Type: AccountTypeOAuth, ParentAccountID: &parentID}
	owner := &Account{
		ID: parentID, Platform: PlatformOpenAI, Type: AccountTypeOAuth,
		Credentials: map[string]any{
			"auth_mode":                  OpenAIAuthModeAgentIdentity,
			"agent_runtime_id":           "runtime-original",
			"agent_private_key":          privateKey,
			"task_id":                    "task-stable",
			"chatgpt_account_id":         "tenant-original",
			"chatgpt_user_id":            "user-original",
			"chatgpt_account_is_fedramp": false,
			"plan_type":                  "plus",
		},
	}
	baseAdmission := &OpenAIAccountRequirementAdmission{
		Selected: selected, EffectiveCredentialOwner: owner,
		AccountClass: securityadmission.AccountAuditExemptVerified,
	}
	oldProof, err := newOpenAICredentialProof(baseAdmission, OpenAIAuthModeAgentIdentity, [sha256.Size]byte{}, false)
	require.NoError(t, err)

	for _, test := range []struct {
		name  string
		key   string
		value any
	}{
		{name: "runtime", key: "agent_runtime_id", value: "runtime-current"},
		{name: "private key", key: "agent_private_key", value: "private-key-current"},
		{name: "tenant", key: "chatgpt_account_id", value: "tenant-current"},
		{name: "user", key: "chatgpt_user_id", value: "user-current"},
		{name: "fedramp", key: "chatgpt_account_is_fedramp", value: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			driftedOwner := *owner
			driftedOwner.Credentials = shallowCopyMap(owner.Credentials)
			driftedOwner.Credentials[test.key] = test.value
			currentAdmission := *baseAdmission
			currentAdmission.EffectiveCredentialOwner = &driftedOwner
			currentProof, proofErr := newOpenAICredentialProof(&currentAdmission, OpenAIAuthModeAgentIdentity, [sha256.Size]byte{}, false)
			require.NoError(t, proofErr)
			require.NotEqual(t, oldProof, currentProof)

			cfg := &config.Config{}
			cfg.Gateway.OpenAIWS.MaxConnsPerAccount = 1
			pool := newOpenAIWSConnPool(cfg)
			dialer := &openAIWSCredentialCompatibilityDialer{}
			pool.setClientDialerForTest(dialer)
			request := func(proof *OpenAICredentialProof) openAIWSAcquireRequest {
				return openAIWSAcquireRequest{Account: selected, WSURL: "wss://example.com/v1/responses", CredentialProof: proof}
			}
			oldLease, acquireErr := pool.Acquire(context.Background(), request(oldProof))
			require.NoError(t, acquireErr)
			oldLease.Release()
			currentLease, acquireErr := pool.Acquire(context.Background(), request(currentProof))
			require.NoError(t, acquireErr)
			require.False(t, currentLease.Reused())
			require.NoError(t, currentLease.WriteJSONWithContextTimeout(context.Background(), map[string]any{"type": "response.create"}, time.Second))
			currentLease.Release()
			connections := dialer.Connections()
			require.Len(t, connections, 2)
			require.Zero(t, connections[0].WriteCount(), "the old identity connection must receive no current request")
			require.Equal(t, 1, connections[1].WriteCount())
			pool.Close()
		})
	}
}

func TestOpenAIWSFinalizedCredentialProofFromContext_UsesImmutableHashesOnly(t *testing.T) {
	selected := &Account{ID: 7103, Platform: PlatformOpenAI, Type: AccountTypeOAuth}
	parentID := int64(8103)
	selected.Type = AccountTypeOAuth
	selected.ParentAccountID = &parentID
	owner := &Account{
		ID:       parentID,
		Platform: PlatformOpenAI,
		Type:     AccountTypeOAuth,
		Credentials: map[string]any{
			"access_token":   "plus-token-secret",
			"plan_type":      "plus",
			"_token_version": int64(33),
		},
	}
	admission := &OpenAIAccountRequirementAdmission{
		Selected:                 selected,
		EffectiveCredentialOwner: owner,
		AccountClass:             securityadmission.AccountAuditExemptVerified,
	}
	ctx := WithOpenAIAccountTerminalAdmission(context.Background(), admission)
	require.NoError(t, setOpenAIFinalizedCredential(ctx, admission, "plus-token-secret", "oauth"))

	proof, err := openAIWSFinalizedCredentialProofFromContext(ctx, selected)
	require.NoError(t, err)
	require.NotNil(t, proof)
	require.Equal(t, selected.ID, proof.selectedAccountID)
	require.Equal(t, selected.Platform, proof.selectedAccountPlatform)
	require.Equal(t, selected.Type, proof.selectedAccountType)
	require.Equal(t, owner.ID, proof.effectiveOwnerID)
	require.Equal(t, securityadmission.AccountAuditExemptVerified, proof.accountClass)
	require.Equal(t, "oauth", proof.authMode)
	require.Equal(t, int64(33), proof.tokenVersion)
	require.Equal(t, sha256.Sum256([]byte("plus-token-secret")), proof.tokenHash)
	require.True(t, proof.hasToken)

	legacyProof, err := openAIWSFinalizedCredentialProofFromContext(context.Background(), selected)
	require.NoError(t, err)
	require.Nil(t, legacyProof)
}

func openAIWSTestCredentialProof(
	selectedID int64,
	ownerID int64,
	class securityadmission.AccountClass,
	token string,
) *OpenAICredentialProof {
	return &OpenAICredentialProof{
		selectedAccountID:       selectedID,
		selectedAccountPlatform: PlatformOpenAI,
		selectedAccountType:     AccountTypeOAuth,
		effectiveOwnerID:        ownerID,
		effectiveOwnerPlatform:  PlatformOpenAI,
		effectiveOwnerType:      AccountTypeOAuth,
		accountClass:            class,
		authMode:                "oauth",
		tokenVersion:            1,
		tokenHash:               sha256.Sum256([]byte(token)),
		hasToken:                true,
	}
}

type openAIWSCredentialCompatibilityDialer struct {
	mu    sync.Mutex
	conns []*openAIWSCredentialCompatibilityConn
}

func (d *openAIWSCredentialCompatibilityDialer) Dial(
	context.Context,
	string,
	http.Header,
	string,
) (openAIWSClientConn, int, http.Header, error) {
	conn := &openAIWSCredentialCompatibilityConn{}
	d.mu.Lock()
	d.conns = append(d.conns, conn)
	d.mu.Unlock()
	return conn, http.StatusSwitchingProtocols, nil, nil
}

func (d *openAIWSCredentialCompatibilityDialer) Connections() []*openAIWSCredentialCompatibilityConn {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]*openAIWSCredentialCompatibilityConn(nil), d.conns...)
}

type openAIWSCredentialCompatibilityConn struct {
	mu     sync.Mutex
	writes int
	closed bool
}

func (c *openAIWSCredentialCompatibilityConn) WriteJSON(context.Context, any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writes++
	return nil
}

func (c *openAIWSCredentialCompatibilityConn) ReadMessage(context.Context) ([]byte, error) {
	return []byte(`{"type":"response.completed","response":{"id":"resp_credential_proof"}}`), nil
}

func (c *openAIWSCredentialCompatibilityConn) Ping(context.Context) error { return nil }

func (c *openAIWSCredentialCompatibilityConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *openAIWSCredentialCompatibilityConn) WriteCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.writes
}
