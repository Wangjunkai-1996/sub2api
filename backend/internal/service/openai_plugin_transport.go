package service

import (
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/Wei-Shaw/sub2api/internal/pkg/logger"
)

type accountEgressUseBody struct {
	body    io.ReadCloser
	release func()
	once    sync.Once
}

func (b *accountEgressUseBody) Read(p []byte) (int, error) {
	n, err := b.body.Read(p)
	if err != nil {
		b.once.Do(b.release)
	}
	return n, err
}

func (b *accountEgressUseBody) Close() error {
	err := b.body.Close()
	b.once.Do(b.release)
	return err
}

func (s *OpenAIGatewayService) SetPluginManager(manager *PluginManager) {
	s.pluginManager = manager
}

// doOpenAIUpstream 只在 OpenAI OAuth 能力绑定已启用时把真实请求交给插件。
// 插件返回标准 http.Response，响应解析、错误映射、SSE 和计费仍由现有核心链处理。
func (s *OpenAIGatewayService) doOpenAIUpstream(request *http.Request, proxyURL string, account *Account) (response *http.Response, err error) {
	opaqueState := openAIOpaqueRecoveryFromContext(request.Context())
	if opaqueState != nil {
		// A request-local reservation must settle before a second transport can
		// reuse it or clear its route after an explicitly unsent setup failure.
		opaqueState.dispatchMu.Lock()
		defer opaqueState.dispatchMu.Unlock()
	}
	wasOpaquePinned := OpenAIOpaqueRecoveryRoutePinned(request.Context())
	dispatchReserved := false
	var releaseProbeContext func()
	if request != nil {
		request = request.WithContext(ContextWithSelectedAccountEgress(request.Context(), account))
		if HasOpenAIModelDispatchBudget(request.Context()) {
			// A 307/308 must not silently replay a model POST outside the shared budget.
			request = request.WithContext(WithHTTPUpstreamRedirectsDisabled(request.Context()))
		}
		if probe := openAI429AttemptFromContext(request.Context(), account); probe.Probe() {
			upstreamCtx, release := openAI429ProbeContext(request.Context(), account)
			releaseProbeContext = release
			request = request.WithContext(upstreamCtx)
		}
	}
	var releaseEgressUse func()
	if account != nil && account.SelectedEgress != nil && account.SelectedEgress.Lease != nil {
		var err error
		releaseEgressUse, err = account.SelectedEgress.Lease.AcquireUse()
		if err != nil {
			if releaseProbeContext != nil {
				releaseProbeContext()
			}
			return nil, err
		}
	}
	releaseUse := func() {
		if releaseEgressUse != nil {
			releaseEgressUse()
		}
		if releaseProbeContext != nil {
			releaseProbeContext()
		}
	}
	finish := func(response *http.Response, err error) (*http.Response, error) {
		if opaqueState != nil && dispatchReserved {
			var pluginErr *PluginTransportError
			notSent := response == nil && (IsHTTPUpstreamRequestNotSent(err) ||
				(errors.As(err, &pluginErr) && pluginErr != nil && !pluginErr.RequestSent))
			budget := request.Context().Value(openAIModelDispatchBudgetKey{}).(*openAIModelDispatchBudget)
			budget.mu.Lock()
			if notSent {
				budget.attempts--
			} else {
				budget.routeFingerprint = openAIRecoveryRouteFingerprint(request, proxyURL, account)
				recordOpenAIModelDispatch(request.Context(), account.ID, budget, 3)
			}
			budget.mu.Unlock()
			if notSent && !wasOpaquePinned {
				opaqueState.mu.Lock()
				opaqueState.dispatched = false
				opaqueState.route = nil
				opaqueState.wireBody = nil
				opaqueState.wireURL = ""
				opaqueState.fingerprint = nil
				opaqueState.fingerprintReady = false
				opaqueState.mu.Unlock()
			}
		}
		if releaseEgressUse == nil && releaseProbeContext == nil {
			return response, err
		}
		if err != nil || response == nil || response.Body == nil {
			releaseUse()
			return response, err
		}
		response.Body = &accountEgressUseBody{body: response.Body, release: releaseUse}
		return response, nil
	}
	if err := prepareOpenAIOpaqueDispatch(request, proxyURL, account); err != nil {
		return finish(nil, err)
	}
	if err := takeOpenAIModelDispatch(request.Context(), account.ID); err != nil {
		return finish(nil, err)
	}
	dispatchReserved = true
	if IsOpenAIManagedRecovery(request.Context()) && opaqueState == nil {
		if budget, _ := request.Context().Value(openAIModelDispatchBudgetKey{}).(*openAIModelDispatchBudget); budget != nil {
			fingerprint := openAIRecoveryRouteFingerprint(request, proxyURL, account)
			budget.mu.Lock()
			budget.routeFingerprint = fingerprint
			budget.mu.Unlock()
		}
	}
	if state := openAIOpaqueRecoveryFromContext(request.Context()); state != nil {
		state.mu.Lock()
		state.dispatched = true
		state.mu.Unlock()
	}
	if s.pluginManager != nil {
		var handled bool
		response, handled, err = s.pluginManager.RoundTripOpenAIOAuth(request.Context(), request, proxyURL, account)
		if handled {
			return finish(response, err)
		}
	}
	var networkTrace *openAINetworkTrace
	if s.cfg != nil && s.cfg.Gateway.OpenAINetworkTraceEnabled {
		request, networkTrace = startOpenAINetworkTrace(request, account.ID)
	}
	response, err = s.httpUpstream.Do(request, proxyURL, account.ID, account.Concurrency)
	if networkTrace != nil {
		networkTrace.log(logger.L(), response, err)
	}
	return finish(response, err)
}

// doOpenAIAccountTestUpstream 让 OpenAI OAuth 账号测试与真实转发使用同一插件路径。
// API Key 和未命中插件的账号保持各自原有的 HTTPUpstream 行为。
func (s *AccountTestService) doOpenAIAccountTestUpstream(
	request *http.Request,
	proxyURL string,
	account *Account,
	useTLSFallback bool,
) (*http.Response, error) {
	if s.pluginManager != nil {
		response, handled, err := s.pluginManager.RoundTripOpenAIOAuth(request.Context(), request, proxyURL, account)
		if handled {
			return response, err
		}
	}
	if useTLSFallback {
		return s.httpUpstream.DoWithTLS(
			request,
			proxyURL,
			account.ID,
			account.Concurrency,
			s.tlsFPProfileService.ResolveTLSProfile(account),
		)
	}
	return s.httpUpstream.Do(request, proxyURL, account.ID, account.Concurrency)
}
