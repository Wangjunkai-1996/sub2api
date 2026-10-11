package service

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestCodexTicketSharedRevocationRejectsOtherInstanceCache(t *testing.T) {
	account := ticketTestAccount(41)
	old := verifiedTestTicket(account, 292)
	account.Extra[openAICodexTicketExtraKey(old.Model)] = old
	repo := &codexTicketRefreshRepo{accounts: []Account{*account}}
	s := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true}, nil)
	s.accountRepo = repo
	other := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true}, nil)
	other.accountRepo = repo
	other.openaiCodexTickets.Store(openAICodexTicketKey(41, old.Model), old)
	// Keep recovery on cooldown so the cross-instance revocation is observable.
	s.openaiCodexAccountJobs = map[int64]*codexAccountTicketJob{41: {revision: old.ConfigRevision, harvestProxyURL: s.openAICodexTicketHarvestProxyURL(), retryAfter: time.Now().Add(time.Minute)}}
	s.invalidateCodexTicketFromResponse(receiptForCodexTicket(old), "model_mismatch")
	require.Nil(t, other.lookupOpenAICodexTicket(account, old.Model))
	require.ErrorIs(t, other.applyOpenAICodexTicket(context.Background(), account, old.Model, http.Header{}), ErrOpenAICodexTicketUnavailable)
}

func TestCodexTicketSharedLeasePreventsConcurrentHarvest(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var probes atomic.Int64
	upstream := &codexTicketFuncUpstream{do: func(req *http.Request) (*http.Response, error) {
		if probes.Add(1) == 1 {
			close(started)
			select {
			case <-release:
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
		return codexTicketResponse(), nil
	}}
	first, repo := ticketJobService(t, upstream)
	second := ticketTestService(t, first.cfg.Gateway.OpenAICodexTicket, upstream)
	second.accountRepo = repo
	t.Cleanup(second.StopOpenAICodexTicketHarvester)
	job := first.startCodexAccountTicketJob(context.Background(), 41, true)
	<-started
	waitCodexTicketJob(t, second.startCodexAccountTicketJob(context.Background(), 41, true))
	require.Equal(t, int64(1), probes.Load())
	close(release)
	waitCodexTicketJob(t, job)
	require.Equal(t, int64(2), probes.Load())
}

func TestCodexTicketV2PreservesLegacyMaterialOnConfigurationChange(t *testing.T) {
	account := ticketTestAccount(41)
	legacyKey := "codex_turn_ticket:gpt-6-astra"
	account.Extra[legacyKey] = map[string]any{"state": "rollback-state"}
	repo := &codexTicketRefreshRepo{accounts: []Account{*account}}
	s := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true}, nil)
	s.accountRepo = repo
	_, err := s.ConfigureCodexAccountTicket(context.Background(), 41, CodexAccountTicketUpdate{Enabled: false})
	require.NoError(t, err)
	live, err := repo.GetByID(context.Background(), 41)
	require.NoError(t, err)
	require.Equal(t, account.Extra[legacyKey], live.Extra[legacyKey])
	require.NotContains(t, RedactOpenAICodexTicketExtra(live.Extra), legacyKey)
	require.NotContains(t, RedactOpenAICodexTicketExtra(live.Extra), codexAccountTicketConfigKey)
}

func TestCodexTicketPoolBindingSurvivesLiveReadAndFencesRouteChanges(t *testing.T) {
	account := openAIEgressHydrationAccount(7, 13, &Proxy{ID: 91, Protocol: "http", Host: "business.example", Port: 8080})
	base := ticketTestAccount(account.ID)
	account.Extra = base.Extra
	account.Credentials = base.Credentials
	ticket := verifiedTestTicket(account, 292)
	ticket.EgressBindingID = codexTicketBusinessBinding(account)
	account.Extra[openAICodexTicketExtraKey(ticket.Model)] = ticket
	repo := &codexTicketRefreshRepo{accounts: []Account{*account}}
	s := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true}, nil)
	s.accountRepo = repo
	binding, err := s.openAICodexTicketRequiredBinding(context.Background(), account, ticket.Model)
	require.NoError(t, err)
	require.Equal(t, "901:41", binding)
	resolved := openAIEgressHydrationResolved(account)
	selected, err := WithResolvedAccountEgress(account, resolved)
	require.NoError(t, err)
	live, err := s.codexTicketLiveAccount(context.Background(), selected)
	require.NoError(t, err)
	require.Same(t, resolved, live.SelectedEgress)
	headers := http.Header{}
	require.NoError(t, s.applyOpenAICodexTicket(context.Background(), selected, ticket.Model, headers))
	require.Equal(t, ticket.State, headers.Get(openAICodexTurnStateHeader))
	require.ErrorIs(t, s.applyOpenAICodexTicket(context.Background(), account, ticket.Model, http.Header{}), ErrOpenAICodexTicketUnavailable)
	repo.accounts[0].EgressRevision++
	require.ErrorIs(t, s.applyOpenAICodexTicket(context.Background(), selected, ticket.Model, http.Header{}), ErrOpenAICodexTicketUnavailable)
}

func TestCodexTicketPoolRolloutOffRejectsEnableAndHarvest(t *testing.T) {
	account := openAIEgressHydrationAccount(7, 13, &Proxy{ID: 91, Protocol: "http", Host: "business.example", Port: 8080})
	base := ticketTestAccount(account.ID)
	account.Extra = base.Extra
	account.Credentials = base.Credentials
	repo := &codexTicketRefreshRepo{accounts: []Account{*account}}
	s := ticketTestService(t, config.OpenAICodexTicketConfig{Enabled: true}, nil)
	s.accountRepo = repo
	status, err := s.GetCodexAccountTicketStatus(context.Background(), account.ID)
	require.NoError(t, err)
	require.False(t, status.FixedProxyConfigured)
	_, err = s.ConfigureCodexAccountTicket(context.Background(), account.ID, CodexAccountTicketUpdate{Enabled: true})
	require.Error(t, err)
	_, err = s.HarvestCodexAccountTicket(context.Background(), account.ID)
	require.Error(t, err)
	_, err = s.ConfigureCodexAccountTicket(context.Background(), account.ID, CodexAccountTicketUpdate{Enabled: false})
	require.NoError(t, err)
}
