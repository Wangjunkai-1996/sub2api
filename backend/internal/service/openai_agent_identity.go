package service

import (
	"context"
	"crypto"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/pkg/httpclient"
	"golang.org/x/crypto/curve25519"
	"golang.org/x/crypto/nacl/box"
)

const (
	OpenAIAuthModeAgentIdentity          = "agentIdentity"
	agentIdentityAuthAPIBaseURL          = "https://auth.openai.com/api/accounts"
	agentIdentityTaskRegistrationTimeout = 30 * time.Second
)

var openAIAgentIdentityAuthAPIBaseURL = agentIdentityAuthAPIBaseURL

var agentIdentityTaskLocks sync.Map // map[int64]*sync.Mutex

type agentIdentityWSConnectionInvalidator interface {
	InvalidateAgentIdentityWSConnections(accountID int64)
}

type agentIdentityKey struct {
	runtimeID  string
	privateKey ed25519.PrivateKey
	taskID     string
}

// agentIdentityCredentialMaterialDigest binds a connection to the stable
// material that determines the effective Agent Identity and ChatGPT tenant.
// task_id is intentionally excluded: it is a recoverable registration under
// the same runtime/signing key, not a change of credential owner. Generated
// assertions are also excluded because their timestamp and signature rotate.
func agentIdentityCredentialMaterialDigest(account *Account) ([sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	if account == nil || !account.IsOpenAIAgentIdentity() {
		return zero, errors.New("agent identity account is required")
	}
	runtimeID := strings.TrimSpace(account.GetCredential("agent_runtime_id"))
	privateKey := strings.TrimSpace(account.GetCredential("agent_private_key"))
	chatGPTAccountID := strings.TrimSpace(account.GetChatGPTAccountID())
	if runtimeID == "" || privateKey == "" || chatGPTAccountID == "" {
		return zero, errors.New("agent identity stable credential material is incomplete")
	}

	digest := sha256.New()
	_, _ = digest.Write([]byte("sub2api/openai-agent-identity-credential/v1"))
	writeField := func(value string) {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = digest.Write(size[:])
		_, _ = digest.Write([]byte(value))
	}
	writeField(runtimeID)
	writeField(privateKey)
	writeField(chatGPTAccountID)
	writeField(strings.TrimSpace(account.GetCredential("chatgpt_user_id")))
	var fedRAMP [1]byte
	if account.IsChatGPTAccountFedRAMP() {
		fedRAMP[0] = 1
	}
	_, _ = digest.Write(fedRAMP[:])

	var result [sha256.Size]byte
	copy(result[:], digest.Sum(nil))
	return result, nil
}

type agentIdentityTaskRegistrationResponse struct {
	TaskID               string `json:"task_id"`
	TaskIDCamel          string `json:"taskId"`
	EncryptedTaskID      string `json:"encrypted_task_id"`
	EncryptedTaskIDCamel string `json:"encryptedTaskId"`
}

type agentIdentityTaskRecoveredError struct{}

func (e *agentIdentityTaskRecoveredError) Error() string {
	return "agent identity task recovered"
}

func (a *Account) IsOpenAIAgentIdentity() bool {
	if a == nil || !a.IsOpenAIOAuth() {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(a.GetCredential(openAIAuthModeCredentialKey)), OpenAIAuthModeAgentIdentity)
}

func agentIdentityPrivateKey(account *Account) (ed25519.PrivateKey, error) {
	if account == nil {
		return nil, errors.New("agent identity account is nil")
	}
	raw := strings.TrimSpace(account.GetCredential("agent_private_key"))
	if raw == "" {
		return nil, errors.New("agent identity private key is missing")
	}
	der, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, errors.New("agent identity private key is not valid base64")
	}
	key, err := x509.ParsePKCS8PrivateKey(der)
	if err != nil {
		return nil, errors.New("agent identity private key is not valid PKCS#8")
	}
	privateKey, ok := key.(ed25519.PrivateKey)
	if !ok || len(privateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("agent identity private key is not Ed25519")
	}
	return privateKey, nil
}

// ValidateOpenAIAgentIdentityPrivateKey validates the stored PKCS#8 Ed25519
// form without returning or logging the key material.
func ValidateOpenAIAgentIdentityPrivateKey(encoded string) error {
	account := &Account{Credentials: map[string]any{"agent_private_key": encoded}}
	_, err := agentIdentityPrivateKey(account)
	return err
}

func agentIdentityKeyFromAccount(account *Account) (agentIdentityKey, error) {
	privateKey, err := agentIdentityPrivateKey(account)
	if err != nil {
		return agentIdentityKey{}, err
	}
	runtimeID := strings.TrimSpace(account.GetCredential("agent_runtime_id"))
	if runtimeID == "" {
		return agentIdentityKey{}, errors.New("agent identity runtime id is missing")
	}
	return agentIdentityKey{
		runtimeID:  runtimeID,
		privateKey: privateKey,
		taskID:     strings.TrimSpace(account.GetCredential("task_id")),
	}, nil
}

func buildAgentAssertion(key agentIdentityKey, now time.Time) (string, error) {
	if key.runtimeID == "" || key.taskID == "" {
		return "", errors.New("agent identity runtime or task id is missing")
	}
	timestamp := now.UTC().Format(time.RFC3339)
	payload := []byte(key.runtimeID + ":" + key.taskID + ":" + timestamp)
	signature, err := key.privateKey.Sign(nil, payload, crypto.Hash(0))
	if err != nil {
		return "", errors.New("failed to sign agent assertion")
	}
	envelope := map[string]string{
		"agent_runtime_id": key.runtimeID,
		"task_id":          key.taskID,
		"timestamp":        timestamp,
		"signature":        base64.StdEncoding.EncodeToString(signature),
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return "", errors.New("failed to serialize agent assertion")
	}
	return "AgentAssertion " + base64.RawURLEncoding.EncodeToString(encoded), nil
}

func signAgentTaskRegistration(key agentIdentityKey, timestamp time.Time) (string, string, error) {
	if key.runtimeID == "" {
		return "", "", errors.New("agent identity runtime id is missing")
	}
	formatted := timestamp.UTC().Format(time.RFC3339)
	signature, err := key.privateKey.Sign(nil, []byte(key.runtimeID+":"+formatted), crypto.Hash(0))
	if err != nil {
		return "", "", errors.New("failed to sign agent task registration")
	}
	return formatted, base64.StdEncoding.EncodeToString(signature), nil
}

func decryptAgentTaskID(key agentIdentityKey, encoded string) (string, error) {
	ciphertext, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil {
		return "", errors.New("encrypted agent task id is not valid base64")
	}
	seed := key.privateKey.Seed()
	digest := sha512.Sum512(seed)
	var curvePrivate [32]byte
	copy(curvePrivate[:], digest[:32])
	curvePrivate[0] &= 248
	curvePrivate[31] &= 127
	curvePrivate[31] |= 64
	curvePublicBytes, err := curve25519.X25519(curvePrivate[:], curve25519.Basepoint)
	if err != nil {
		return "", errors.New("failed to derive agent identity decryption key")
	}
	var curvePublic [32]byte
	copy(curvePublic[:], curvePublicBytes)
	plaintext, ok := box.OpenAnonymous(nil, ciphertext, &curvePublic, &curvePrivate)
	if !ok {
		return "", errors.New("failed to decrypt encrypted agent task id")
	}
	taskID := strings.TrimSpace(string(plaintext))
	if taskID == "" {
		return "", errors.New("decrypted agent task id is empty")
	}
	return taskID, nil
}

func registerAgentIdentityTask(ctx context.Context, account *Account) (string, error) {
	key, err := agentIdentityKeyFromAccount(account)
	if err != nil {
		return "", err
	}
	timestamp, signature, err := signAgentTaskRegistration(key, time.Now())
	if err != nil {
		return "", err
	}
	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	client, err := httpclient.GetClient(httpclient.Options{
		ProxyURL:              proxyURL,
		Timeout:               agentIdentityTaskRegistrationTimeout,
		ResponseHeaderTimeout: 15 * time.Second,
	})
	if err != nil {
		return "", errors.New("invalid proxy configuration for agent task registration")
	}
	body, err := json.Marshal(map[string]string{
		"timestamp": timestamp,
		"signature": signature,
	})
	if err != nil {
		return "", errors.New("failed to serialize agent task registration")
	}
	url := strings.TrimRight(strings.TrimSpace(openAIAgentIdentityAuthAPIBaseURL), "/") + "/v1/agent/" + key.runtimeID + "/task/register"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return "", errors.New("failed to build agent task registration request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", errors.New("agent task registration request failed")
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", fmt.Errorf("agent task registration returned status %d", resp.StatusCode)
	}
	var result agentIdentityTaskRegistrationResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&result); err != nil {
		return "", errors.New("agent task registration response is invalid")
	}
	if taskID := strings.TrimSpace(result.TaskID); taskID != "" {
		return taskID, nil
	}
	if taskID := strings.TrimSpace(result.TaskIDCamel); taskID != "" {
		return taskID, nil
	}
	encrypted := strings.TrimSpace(result.EncryptedTaskID)
	if encrypted == "" {
		encrypted = strings.TrimSpace(result.EncryptedTaskIDCamel)
	}
	if encrypted == "" {
		return "", errors.New("agent task registration response omitted task id")
	}
	return decryptAgentTaskID(key, encrypted)
}

func ensureAgentIdentityTaskForAccount(ctx context.Context, repo AccountRepository, wsInvalidator agentIdentityWSConnectionInvalidator, taskMu *sync.Mutex, account *Account, expectedTaskID string) error {
	if account == nil || !account.IsOpenAIAgentIdentity() {
		return nil
	}
	credAccount := account
	if account.IsShadow() {
		resolved, err := resolveCredentialAccount(ctx, repo, account)
		if err != nil {
			return err
		}
		credAccount = resolved
	}
	if credAccount == nil || !credAccount.IsOpenAIAgentIdentity() {
		return errors.New("agent identity credentials are unavailable")
	}
	currentTaskID := strings.TrimSpace(credAccount.GetCredential("task_id"))
	if currentTaskID != "" && (expectedTaskID == "" || currentTaskID != expectedTaskID) {
		return nil
	}
	if taskMu == nil {
		return errors.New("agent identity task lock is unavailable")
	}
	terminal := OpenAIAccountTerminalAdmissionFromContext(ctx)
	if terminal != nil && repo == nil {
		return NewOpenAISecurityCredentialFailoverError(terminal.Selected, "agent identity credential reload unavailable")
	}
	sharedTaskMu := taskMu
	if credAccount.ID > 0 {
		candidate := &sync.Mutex{}
		actual, _ := agentIdentityTaskLocks.LoadOrStore(credAccount.ID, candidate)
		loadedTaskMu, ok := actual.(*sync.Mutex)
		if !ok {
			return errors.New("agent identity task lock has invalid type")
		}
		sharedTaskMu = loadedTaskMu
	}
	sharedTaskMu.Lock()
	defer sharedTaskMu.Unlock()
	// Re-read inside the shared lock. Different request paths often receive
	// independent repository snapshots; checking only the caller's snapshot
	// would allow sequential duplicate registrations after the first writer
	// has already persisted a new task.
	if repo != nil && credAccount.ID > 0 {
		refreshed, refreshErr := repo.GetByID(ctx, credAccount.ID)
		if terminal != nil {
			if refreshErr != nil || refreshed == nil || refreshed.ID != credAccount.ID || refreshed.IsShadow() ||
				!refreshed.IsOpenAIAgentIdentity() {
				return NewOpenAISecurityCredentialFailoverError(terminal.Selected, "agent identity credential changed before task registration")
			}
			class := ClassifyOpenAIEffectiveCredentialOwner(refreshed)
			if class != terminal.AccountClass || !openAIAccountClassSatisfiesRequirement(class, terminal.Requirement) {
				return NewOpenAISecurityCredentialFailoverError(terminal.Selected, "agent identity account class changed before task registration")
			}
			credAccount = refreshed
			if !account.IsShadow() {
				account.Credentials = shallowCopyMap(credAccount.Credentials)
			}
		} else if refreshErr == nil && refreshed != nil {
			if refreshed.IsShadow() {
				if resolved, resolveErr := resolveCredentialAccount(ctx, repo, refreshed); resolveErr == nil && resolved != nil {
					refreshed = resolved
				}
			}
			if refreshed.IsOpenAIAgentIdentity() {
				credAccount = refreshed
				if !account.IsShadow() {
					account.Credentials = shallowCopyMap(credAccount.Credentials)
				}
			}
		}
	}
	currentTaskID = strings.TrimSpace(credAccount.GetCredential("task_id"))
	if currentTaskID != "" && (expectedTaskID == "" || currentTaskID != expectedTaskID) {
		return nil
	}
	newTaskID, err := registerAgentIdentityTask(ctx, credAccount)
	if err != nil {
		return err
	}
	credentials := make(map[string]any, len(credAccount.Credentials)+1)
	for key, value := range credAccount.Credentials {
		credentials[key] = value
	}
	credentials["task_id"] = newTaskID
	if err := persistAccountCredentials(ctx, repo, credAccount, credentials); err != nil {
		return err
	}
	if !account.IsShadow() && account != credAccount {
		account.Credentials = shallowCopyMap(credAccount.Credentials)
	}
	if wsInvalidator != nil {
		wsInvalidator.InvalidateAgentIdentityWSConnections(credAccount.ID)
		// Pool partitions are keyed by the selected scheduling row. A shadow
		// therefore needs its own partition invalidated in addition to its
		// effective credential owner after task recovery.
		if account.ID > 0 && account.ID != credAccount.ID {
			wsInvalidator.InvalidateAgentIdentityWSConnections(account.ID)
		}
	}
	return nil
}

func (s *OpenAIGatewayService) ensureAgentIdentityTask(ctx context.Context, account *Account, expectedTaskID string) error {
	if s == nil {
		return errors.New("openai gateway service is nil")
	}
	return ensureAgentIdentityTaskForAccount(ctx, s.accountRepo, s, &s.agentIdentityTaskMu, account, expectedTaskID)
}

func isAgentIdentityTaskInvalidHTTPResponse(statusCode int, body []byte) bool {
	if statusCode != http.StatusUnauthorized {
		return false
	}
	lower := strings.ToLower(string(body))
	compact := strings.NewReplacer(" ", "", "\t", "", "\r", "", "\n", "").Replace(lower)
	for _, marker := range []string{
		`"code":"invalid_task_id"`,
		`"code":"task_not_found"`,
		`"code":"task_expired"`,
		`"error":"invalid_task_id"`,
	} {
		if strings.Contains(compact, marker) {
			return true
		}
	}
	for _, marker := range []string{
		"invalid task_id",
		"invalid task id",
		"task_id is invalid",
		"task id is invalid",
		"task not found",
		"task expired",
		"unknown task_id",
		"unknown task id",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

type agentIdentityTaskRecoveryContextKey struct{}

func markAgentIdentityTaskRecoveryTried(ctx context.Context) context.Context {
	return context.WithValue(ctx, agentIdentityTaskRecoveryContextKey{}, true)
}

func agentIdentityTaskRecoveryWasTried(ctx context.Context) bool {
	tried, _ := ctx.Value(agentIdentityTaskRecoveryContextKey{}).(bool)
	return tried
}

func isAgentIdentityTaskInvalidWSDialError(err *openAIWSDialError) bool {
	return err != nil && isAgentIdentityTaskInvalidHTTPResponse(err.StatusCode, err.ResponseBody)
}

func (s *OpenAIGatewayService) buildOpenAIAuthenticationHeaders(ctx context.Context, account *Account, token string) (http.Header, error) {
	if account == nil {
		return nil, errors.New("account is nil")
	}
	if OpenAIAccountTerminalAdmissionFromContext(ctx) != nil {
		credential, err := openAIFinalizedCredentialForAuthentication(ctx, account, token)
		if err != nil {
			return nil, err
		}
		owner := credential.admission.EffectiveCredentialOwner
		var headers http.Header
		if credential.authMode == OpenAIAuthModeAgentIdentity {
			headers, err = buildAgentIdentityAuthenticationHeadersFromSnapshot(owner)
			if err != nil {
				return nil, NewOpenAISecurityCredentialFailoverError(account, "finalized agent identity credential unavailable")
			}
		} else {
			headers = make(http.Header)
			headers.Set("Authorization", "Bearer "+token)
		}
		// Keep ChatGPT tenant metadata on the same immutable owner snapshot as
		// the Authorization value. Downstream header helpers verify/reuse it.
		setOpenAIChatGPTAccountHeaders(headers, owner)
		return headers, nil
	}
	credAccount := account
	if account.IsShadow() {
		resolved, err := resolveCredentialAccount(ctx, s.accountRepo, account)
		if err != nil {
			return nil, err
		}
		credAccount = resolved
	}
	headers := make(http.Header)
	if credAccount != nil && credAccount.IsOpenAIAgentIdentity() {
		agentHeaders, err := buildAgentIdentityAuthenticationHeaders(ctx, s.accountRepo, s, &s.agentIdentityTaskMu, credAccount)
		if err != nil {
			return nil, err
		}
		return agentHeaders, nil
	}
	headers.Set("Authorization", "Bearer "+token)
	return headers, nil
}

func buildAgentIdentityAuthenticationHeaders(ctx context.Context, repo AccountRepository, wsInvalidator agentIdentityWSConnectionInvalidator, taskMu *sync.Mutex, account *Account) (http.Header, error) {
	if account == nil || !account.IsOpenAIAgentIdentity() {
		return nil, errors.New("agent identity account is required")
	}
	if err := ensureAgentIdentityTaskForAccount(ctx, repo, wsInvalidator, taskMu, account, ""); err != nil {
		return nil, err
	}
	return buildAgentIdentityAuthenticationHeadersFromSnapshot(account)
}

func buildAgentIdentityAuthenticationHeadersFromSnapshot(account *Account) (http.Header, error) {
	if account == nil || !account.IsOpenAIAgentIdentity() {
		return nil, errors.New("agent identity account is required")
	}
	key, err := agentIdentityKeyFromAccount(account)
	if err != nil {
		return nil, err
	}
	assertion, err := buildAgentAssertion(key, time.Now())
	if err != nil {
		return nil, err
	}
	headers := make(http.Header)
	headers.Set("Authorization", assertion)
	return headers, nil
}

func (s *OpenAIGatewayService) refreshOpenAIAgentIdentityHeaders(ctx context.Context, account *Account, headers http.Header) (http.Header, error) {
	if account == nil {
		return cloneHeader(headers), nil
	}
	if OpenAIAccountTerminalAdmissionFromContext(ctx) != nil {
		credential, err := openAIFinalizedCredentialFromContext(ctx, account)
		if err != nil {
			return nil, err
		}
		if credential.authMode != OpenAIAuthModeAgentIdentity {
			return cloneHeader(headers), nil
		}
		refreshed := cloneHeader(headers)
		if refreshed == nil {
			refreshed = make(http.Header)
		}
		authHeaders, err := buildAgentIdentityAuthenticationHeadersFromSnapshot(credential.admission.EffectiveCredentialOwner)
		if err != nil {
			return nil, NewOpenAISecurityCredentialFailoverError(account, "finalized agent identity credential unavailable")
		}
		refreshed.Set("Authorization", authHeaders.Get("Authorization"))
		setOpenAIChatGPTAccountHeaders(refreshed, credential.admission.EffectiveCredentialOwner)
		return refreshed, nil
	}
	credAccount := account
	if account.IsShadow() {
		resolved, err := resolveCredentialAccount(ctx, s.accountRepo, account)
		if err != nil {
			return nil, err
		}
		credAccount = resolved
	}
	if !credAccount.IsOpenAIAgentIdentity() {
		return cloneHeader(headers), nil
	}
	refreshed := cloneHeader(headers)
	if refreshed == nil {
		refreshed = make(http.Header)
	}
	authHeaders, err := buildAgentIdentityAuthenticationHeaders(ctx, s.accountRepo, s, &s.agentIdentityTaskMu, credAccount)
	if err != nil {
		return nil, err
	}
	refreshed.Set("Authorization", authHeaders.Get("Authorization"))
	return refreshed, nil
}

func (s *OpenAIGatewayService) recoverAgentIdentityTask(ctx context.Context, account *Account, expectedTaskID string) error {
	if OpenAIAccountTerminalAdmissionFromContext(ctx) != nil {
		credential, err := openAIFinalizedCredentialFromContext(ctx, account)
		if err != nil {
			return err
		}
		if credential.authMode != OpenAIAuthModeAgentIdentity ||
			!credential.admission.EffectiveCredentialOwner.IsOpenAIAgentIdentity() {
			return NewOpenAISecurityCredentialFailoverError(account, "agent identity recovery mode changed")
		}
		selected := credential.admission.Selected
		owner := cloneOpenAICredentialAccountSnapshot(credential.admission.EffectiveCredentialOwner)
		if strings.TrimSpace(expectedTaskID) == "" || selected.IsShadow() {
			expectedTaskID = strings.TrimSpace(owner.GetCredential("task_id"))
		}
		// Invalidate the old dispatch permit before touching credentials. A
		// concurrent header rebuild must fail closed until recovery is freshly
		// admitted and a replacement immutable snapshot is installed.
		clearOpenAIFinalizedCredential(ctx)
		if err := ensureAgentIdentityTaskForAccount(ctx, s.accountRepo, s, &s.agentIdentityTaskMu, owner, expectedTaskID); err != nil {
			return NewOpenAISecurityCredentialFailoverError(selected, "agent identity task recovery unavailable")
		}
		if selected.ID > 0 && selected.ID != owner.ID {
			s.InvalidateAgentIdentityWSConnections(selected.ID)
		}
		fresh, err := s.finalizeOpenAISecurityCredential(ctx, selected, nil, "", OpenAIAuthModeAgentIdentity)
		if err != nil {
			return err
		}
		return setOpenAIFinalizedCredential(ctx, fresh, "", OpenAIAuthModeAgentIdentity)
	}
	if account != nil && account.IsShadow() {
		if resolved, err := resolveCredentialAccount(ctx, s.accountRepo, account); err == nil && resolved != nil && strings.TrimSpace(expectedTaskID) == "" {
			expectedTaskID = strings.TrimSpace(resolved.GetCredential("task_id"))
		}
	}
	return s.ensureAgentIdentityTask(ctx, account, expectedTaskID)
}

func (s *OpenAIGatewayService) isAgentIdentityAccount(ctx context.Context, account *Account) bool {
	if account == nil {
		return false
	}
	if OpenAIAccountTerminalAdmissionFromContext(ctx) != nil {
		credential, err := openAIFinalizedCredentialFromContext(ctx, account)
		return err == nil && credential != nil && credential.authMode == OpenAIAuthModeAgentIdentity &&
			credential.admission.EffectiveCredentialOwner.IsOpenAIAgentIdentity()
	}
	credAccount := account
	if account.IsShadow() {
		resolved, err := resolveCredentialAccount(ctx, s.accountRepo, account)
		if err != nil {
			return false
		}
		credAccount = resolved
	}
	return credAccount != nil && credAccount.IsOpenAIAgentIdentity()
}

// redactAgentIdentitySensitiveBody removes credential values before an
// upstream error can reach logs, ops events, or returned error text. Agent
// Identity responses should not echo these values, but keeping this boundary
// defensive prevents accidental disclosure if an upstream error does.
func redactAgentIdentitySensitiveBodyForAccount(ctx context.Context, repo AccountRepository, account *Account, body []byte) []byte {
	if account == nil || len(body) == 0 {
		return body
	}
	credAccount := account
	if account != nil && account.IsShadow() {
		if resolved, err := resolveCredentialAccount(ctx, repo, account); err == nil && resolved != nil {
			credAccount = resolved
		}
	}
	if credAccount == nil || !credAccount.IsOpenAIAgentIdentity() {
		return body
	}
	redacted := string(body)
	for _, key := range []string{
		"agent_private_key",
		"agent_runtime_id",
		"task_id",
		"access_token",
		"refresh_token",
		"id_token",
		"api_key",
		"session_key",
		"cookie",
	} {
		if value := strings.TrimSpace(credAccount.GetCredential(key)); value != "" {
			redacted = strings.ReplaceAll(redacted, value, "[redacted]")
		}
	}
	const assertionPrefix = "AgentAssertion "
	for offset := 0; offset < len(redacted); {
		relativeStart := strings.Index(redacted[offset:], assertionPrefix)
		if relativeStart < 0 {
			break
		}
		start := offset + relativeStart
		valueStart := start + len(assertionPrefix)
		end := valueStart
		for end < len(redacted) && !strings.ContainsRune(" \t\r\n\"',}", rune(redacted[end])) {
			end++
		}
		redacted = redacted[:valueStart] + "[redacted]" + redacted[end:]
		offset = valueStart + len("[redacted]")
	}
	return []byte(redacted)
}

func (s *OpenAIGatewayService) redactAgentIdentitySensitiveBody(ctx context.Context, account *Account, body []byte) []byte {
	if OpenAIAccountTerminalAdmissionFromContext(ctx) != nil {
		credential, err := openAIFinalizedCredentialFromContext(ctx, account)
		if err != nil || credential == nil || credential.authMode != OpenAIAuthModeAgentIdentity {
			return body
		}
		return redactAgentIdentitySensitiveBodyForAccount(ctx, nil, credential.admission.EffectiveCredentialOwner, body)
	}
	if !s.isAgentIdentityAccount(ctx, account) {
		return body
	}
	return redactAgentIdentitySensitiveBodyForAccount(ctx, s.accountRepo, account, body)
}
