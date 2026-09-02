import { defineComponent } from 'vue'
import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const {
  createAccountMock,
  probeUpstreamBillingMock,
  importCodexSessionMock,
  createOpenAICodexPATMock,
  getSettingsMock,
  generateAuthUrlMock,
  exchangeCodeMock,
  refreshOpenAITokenMock,
  showErrorMock,
  authIsSimpleMode,
} = vi.hoisted(() => ({
  createAccountMock: vi.fn(),
  probeUpstreamBillingMock: vi.fn(),
  importCodexSessionMock: vi.fn(),
  createOpenAICodexPATMock: vi.fn(),
  getSettingsMock: vi.fn(),
  generateAuthUrlMock: vi.fn(),
  exchangeCodeMock: vi.fn(),
  refreshOpenAITokenMock: vi.fn(),
  showErrorMock: vi.fn(),
  authIsSimpleMode: { value: true },
}))

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({
    showError: showErrorMock,
    showSuccess: vi.fn(),
    showWarning: vi.fn(),
  }),
}))

vi.mock('@/stores/auth', () => ({
  useAuthStore: () => ({
    get isSimpleMode() {
      return authIsSimpleMode.value
    },
  }),
}))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    accounts: {
      create: createAccountMock,
      probeUpstreamBilling: probeUpstreamBillingMock,
      checkMixedChannelRisk: vi.fn().mockResolvedValue({ has_risk: false }),
      importCodexSession: importCodexSessionMock,
      createOpenAICodexPAT: createOpenAICodexPATMock,
      generateAuthUrl: generateAuthUrlMock,
      exchangeCode: exchangeCodeMock,
      refreshOpenAIToken: refreshOpenAITokenMock,
    },
    settings: {
      getWebSearchEmulationConfig: vi.fn().mockResolvedValue({ enabled: false, providers: [] }),
      getSettings: getSettingsMock,
    },
    tlsFingerprintProfiles: {
      list: vi.fn().mockResolvedValue([]),
    },
  },
}))

vi.mock('@/api/admin/accounts', () => ({
  getAntigravityDefaultModelMapping: vi.fn().mockResolvedValue([]),
}))

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({ t: (key: string) => key }),
  }
})

import CreateAccountModal from '../CreateAccountModal.vue'

const BaseDialogStub = defineComponent({
  name: 'BaseDialog',
  props: { show: { type: Boolean, default: false } },
  template: '<div v-if="show"><slot /><slot name="footer" /></div>',
})

const OAuthAuthorizationFlowStub = defineComponent({
  name: 'OAuthAuthorizationFlow',
  props: {
    showManualOption: Boolean,
    showCodexSessionImportOption: Boolean,
    showAgentIdentityOption: Boolean,
    showCodexPatOption: Boolean,
    initialInputMethod: String,
  },
  data: () => ({ inputMethod: 'manual', authCode: '', oauthState: '' }),
  emits: [
    'generate-url',
    'validate-refresh-token',
    'import-codex-session',
    'import-codex-pat',
  ],
  template: `
    <div>
      <button data-testid="generate-auth-url" @click="$emit('generate-url')">generate</button>
      <button data-testid="validate-refresh-token" @click="$emit('validate-refresh-token', 'refresh-token')">refresh</button>
      <button data-testid="import-codex-session" @click="$emit('import-codex-session', 'session-json')">session</button>
      <button data-testid="import-codex-pat" @click="$emit('import-codex-pat', 'pat-token')">pat</button>
    </div>
  `,
})

const GroupSelectorStub = defineComponent({
  name: 'GroupSelector',
  props: {
    modelValue: {
      type: Array,
      default: () => [],
    },
  },
  emits: ['update:modelValue'],
  template: `
    <button
      type="button"
      data-testid="select-pricing-groups"
      @click="$emit('update:modelValue', [1, 2])"
    >
      groups
    </button>
  `,
})

const ModelWhitelistSelectorStub = defineComponent({
  name: 'ModelWhitelistSelector',
  props: {
    modelValue: {
      type: Array,
      default: () => [],
    },
    platform: String,
    syncCredentials: Object,
  },
  emits: ['update:modelValue'],
  template: '<div data-testid="model-whitelist-selector" />',
})

const defaultEgressRoutes = [
  { id: 1, kind: 'proxy', name: 'arbitrary-route-label', state: 'active', eligible: true },
]

function mountModal(groups: any[] = [], extraProps: Record<string, unknown> = {}) {
  return mount(CreateAccountModal, {
    props: {
      show: true,
      proxies: [],
      egressRoutes: defaultEgressRoutes,
      defaultEgressRouteId: 1,
      defaultEgressConcurrency: 3,
      groups,
      ...extraProps
    },
    global: {
      stubs: {
        BaseDialog: BaseDialogStub,
        OAuthAuthorizationFlow: OAuthAuthorizationFlowStub,
        ConfirmDialog: true,
        Select: true,
        Icon: true,
        PlatformIcon: true,
        ProxySelector: true,
        ProxyAdBanner: true,
        GroupSelector: GroupSelectorStub,
        ModelWhitelistSelector: ModelWhitelistSelectorStub,
        QuotaLimitCard: true,
      },
    },
  })
}

async function selectButtonByText(wrapper: ReturnType<typeof mountModal>, text: string) {
  const button = wrapper.findAll('button').find((candidate) => candidate.text().includes(text))
  expect(button).toBeDefined()
  await button?.trigger('click')
  if (text === 'OpenAI') {
    const firstEgressRoute = wrapper.find('input[id^="egress-route-"]')
    if (firstEgressRoute.exists() && !(firstEgressRoute.element as HTMLInputElement).checked) {
      await firstEgressRoute.setValue(true)
    }
  }
}

async function submitApiKeyAccount(
  platform: 'openai' | 'anthropic',
  enableLongContextBilling = false,
  disableUpstreamBillingProbe = false
) {
  const wrapper = mountModal()
  await selectButtonByText(wrapper, platform === 'openai' ? 'OpenAI' : 'admin.accounts.claudeConsole')
  if (platform === 'openai') {
    await selectButtonByText(wrapper, 'API Key')
  }
  await wrapper.get('form#create-account-form input[type="text"]').setValue(`${platform} account`)
  await wrapper.get('form#create-account-form input[type="password"]').setValue('test-api-key')
  if (enableLongContextBilling) {
    await wrapper.get('[data-testid="openai-long-context-billing-toggle"]').trigger('click')
  }
  if (disableUpstreamBillingProbe) {
    await wrapper.get('[data-testid="upstream-billing-auto-probe"]').trigger('click')
  }
  await wrapper.get('form#create-account-form').trigger('submit.prevent')
  await flushPromises()
  return wrapper
}

async function openCodexImportStep(toggleClicks = 0) {
  const wrapper = mountModal()
  await selectButtonByText(wrapper, 'OpenAI')
  for (let click = 0; click < toggleClicks; click += 1) {
    await wrapper.get('[data-testid="openai-long-context-billing-toggle"]').trigger('click')
  }
  await wrapper.get('form#create-account-form input[type="text"]').setValue('Codex import')
  await wrapper.get('form#create-account-form').trigger('submit.prevent')
  return wrapper
}

describe('CreateAccountModal OpenAI long-context billing', () => {
  beforeEach(() => {
    authIsSimpleMode.value = true
    createAccountMock.mockReset().mockResolvedValue({ id: 42, platform: 'openai', type: 'apikey' })
    probeUpstreamBillingMock.mockReset().mockResolvedValue({})
    importCodexSessionMock.mockReset().mockResolvedValue({
      created: 1,
      updated: 0,
      skipped: 0,
      failed: 0,
      errors: [],
      warnings: [],
    })
    createOpenAICodexPATMock.mockReset().mockResolvedValue({})
    getSettingsMock.mockReset().mockResolvedValue({})
    generateAuthUrlMock.mockReset().mockResolvedValue({
      auth_url: 'https://auth.example/authorize?state=oauth-state',
      session_id: 'oauth-session',
    })
    exchangeCodeMock.mockReset().mockResolvedValue({
      access_token: 'access-token',
      refresh_token: 'refresh-token',
      expires_at: 123,
    })
    refreshOpenAITokenMock.mockReset().mockResolvedValue({
      access_token: 'access-token',
      refresh_token: 'refresh-token',
      expires_at: 123,
    })
    showErrorMock.mockReset()
  })

  it('mounts safely while initially closed', () => {
    const wrapper = mountModal([], { show: false })
    wrapper.unmount()
  })

  it('hides only the redundant account toggle when every selected group enables tier pricing', async () => {
    authIsSimpleMode.value = false
    const wrapper = mountModal([
      { id: 1, long_context_pricing_enabled: true },
      { id: 2, long_context_pricing_enabled: true },
    ])

    await selectButtonByText(wrapper, 'OpenAI')
    await wrapper.get('[data-testid="select-pricing-groups"]').trigger('click')

    expect(wrapper.find('[data-testid="openai-long-context-billing-toggle"]').exists()).toBe(false)
    expect(wrapper.find('[data-testid="create-openai-ws-mode"]').exists()).toBe(true)
  })

  it('keeps the account toggle when any selected group disables tier pricing', async () => {
    authIsSimpleMode.value = false
    const wrapper = mountModal([
      { id: 1, long_context_pricing_enabled: true },
      { id: 2, long_context_pricing_enabled: false },
    ])

    await selectButtonByText(wrapper, 'OpenAI')
    await wrapper.get('[data-testid="select-pricing-groups"]').trigger('click')

    expect(wrapper.find('[data-testid="openai-long-context-billing-toggle"]').exists()).toBe(true)
    expect(wrapper.find('[data-testid="create-openai-ws-mode"]').exists()).toBe(true)
  })

  it('sends false explicitly for normal OpenAI account creation by default', async () => {
    await submitApiKeyAccount('openai')

    expect(createAccountMock).toHaveBeenCalledTimes(1)
    expect(createAccountMock.mock.calls[0]?.[0]?.extra?.openai_long_context_billing_enabled).toBe(false)
  })

  // namespace 摊平是仅 OAuth 的兼容开关：API Key 走 chat completions 回退桥时由桥自行摊平
  it('shows the Codex namespace flatten toggle only for OpenAI OAuth accounts', async () => {
    const wrapper = mountModal()
    await selectButtonByText(wrapper, 'OpenAI')

    expect(wrapper.find('[data-testid="create-openai-flatten-namespaces-toggle"]').exists()).toBe(
      true
    )

    await selectButtonByText(wrapper, 'API Key')
    expect(wrapper.find('[data-testid="create-openai-flatten-namespaces-toggle"]').exists()).toBe(
      false
    )
  })

  it('enables upstream billing probes by default for new OpenAI API key accounts', async () => {
    await submitApiKeyAccount('openai')

    expect(createAccountMock.mock.calls[0]?.[0]?.upstream_billing_probe_enabled).toBe(true)
  })

  it('waits for the initial upstream billing probe before refreshing the account list', async () => {
    let resolveProbe: (() => void) | undefined
    probeUpstreamBillingMock.mockImplementationOnce(
      () => new Promise<void>((resolve) => {
        resolveProbe = resolve
      })
    )

    const wrapper = await submitApiKeyAccount('openai')

    expect(probeUpstreamBillingMock).toHaveBeenCalledWith(42)
    expect(wrapper.emitted('created')).toBeUndefined()

    resolveProbe?.()
    await flushPromises()

    expect(wrapper.emitted('created')).toHaveLength(1)
  })

  it('sends an explicit disabled state when the create toggle is turned off', async () => {
    await submitApiKeyAccount('openai', false, true)

    expect(createAccountMock.mock.calls[0]?.[0]?.upstream_billing_probe_enabled).toBe(false)
    expect(probeUpstreamBillingMock).not.toHaveBeenCalled()
  })

  it('submits adaptive Kimi protocol endpoints', async () => {
    const wrapper = mountModal()
    await selectButtonByText(wrapper, 'Kimi')
    await wrapper.get('form#create-account-form input[type="text"]').setValue('Kimi adaptive')
    await wrapper.get('form#create-account-form input[type="password"]').setValue('sk-kimi')

    await wrapper.get('form#create-account-form').trigger('submit.prevent')
    await flushPromises()

    expect(createAccountMock).toHaveBeenCalledTimes(1)
    expect(createAccountMock.mock.calls[0]?.[0]?.credentials).toMatchObject({
      account_mode: 'payg',
      api_protocol: 'adaptive',
      base_url: 'https://api.moonshot.cn/v1',
      api_base_urls: {
        chat_completions: 'https://api.moonshot.cn/v1',
        anthropic: 'https://api.moonshot.cn/anthropic'
      }
    })
  })

  it('uses the edited adaptive Chat endpoint when previewing upstream models', async () => {
    const wrapper = mountModal()
    await selectButtonByText(wrapper, 'Kimi')
    await wrapper
      .get('[data-testid="cn-adaptive-base-url-chat_completions"]')
      .setValue('https://relay.example.com/v1')
    await wrapper.get('form#create-account-form input[type="password"]').setValue('sk-relay')

    expect(wrapper.getComponent(ModelWhitelistSelectorStub).props('syncCredentials')).toMatchObject({
      platform: 'kimi',
      type: 'apikey',
      base_url: 'https://relay.example.com/v1',
      api_key: 'sk-relay'
    })
  })

  it('exposes Agent Identity in the OpenAI authorization methods', async () => {
    const wrapper = mountModal()
    await selectButtonByText(wrapper, 'OpenAI')
    await wrapper.get('form#create-account-form input[type="text"]').setValue('OpenAI account')
    await wrapper.get('form#create-account-form').trigger('submit.prevent')

    const flow = wrapper.getComponent(OAuthAuthorizationFlowStub)
    expect(flow.props('showManualOption')).toBe(true)
    expect(flow.props('showCodexSessionImportOption')).toBe(true)
    expect(flow.props('showAgentIdentityOption')).toBe(true)
    expect(flow.props('showCodexPatOption')).toBe(true)
    expect(flow.props('initialInputMethod')).toBe('manual')
  })

  it.each([
    ['camelCase', { authMode: 'agentIdentity', agentIdentity: { agentRuntimeId: 'runtime' } }],
    ['nested identity without auth_mode', { agent_identity: { agent_runtime_id: 'runtime' } }],
  ])('accepts backend-compatible %s Agent Identity imports', async (_name, content) => {
    const wrapper = await openCodexImportStep()
    const flow = wrapper.getComponent(OAuthAuthorizationFlowStub)
    flow.vm.inputMethod = 'agent_identity'

    flow.vm.$emit('import-codex-session', JSON.stringify(content))
    await flushPromises()

    expect(importCodexSessionMock).toHaveBeenCalledTimes(1)
    expect(importCodexSessionMock.mock.calls[0]?.[0]?.egress_pool).toEqual({
      route_ids: [1],
      primary_route_id: 1,
      concurrency_per_egress: 3
    })
  })

  it('sends true explicitly when OpenAI long-context billing is enabled', async () => {
    await submitApiKeyAccount('openai', true)

    expect(createAccountMock).toHaveBeenCalledTimes(1)
    expect(createAccountMock.mock.calls[0]?.[0]?.extra?.openai_long_context_billing_enabled).toBe(true)
  })

  it('omits the OpenAI setting for non-OpenAI account creation', async () => {
    await submitApiKeyAccount('anthropic')

    expect(createAccountMock).toHaveBeenCalledTimes(1)
    expect(createAccountMock.mock.calls[0]?.[0]?.extra?.openai_long_context_billing_enabled).toBeUndefined()
    // 上游倍率探测已放宽到全部 API-key 平台：非 OpenAI 平台与 OpenAI 一致，默认开启。
    expect(createAccountMock.mock.calls[0]?.[0]?.upstream_billing_probe_enabled).toBe(true)
  })

  it('sends an explicit disabled state when the non-OpenAI create toggle is turned off', async () => {
    await submitApiKeyAccount('anthropic', false, true)

    expect(createAccountMock.mock.calls[0]?.[0]?.upstream_billing_probe_enabled).toBe(false)
  })

  it('antigravity upstream 创建默认携带上游倍率探测开关', async () => {
    // antigravity upstream 走独立创建 helper，
    // 也必须与其余 API-key 平台一样默认开启探测并传递开关。
    const wrapper = mountModal()
    await selectButtonByText(wrapper, 'Antigravity')
    await selectButtonByText(wrapper, 'admin.accounts.types.antigravityApikey')
    await wrapper.get('form#create-account-form input[type="text"]').setValue('antigravity relay')
    const baseInput = wrapper
      .findAll('input')
      .find((candidate) => candidate.attributes('placeholder') === 'https://cloudcode-pa.googleapis.com')
    expect(baseInput).toBeDefined()
    await baseInput?.setValue('https://relay.example')
    await wrapper.get('form#create-account-form input[type="password"]').setValue('sk-upstream')
    await wrapper.get('form#create-account-form').trigger('submit.prevent')
    await flushPromises()

    expect(createAccountMock).toHaveBeenCalledTimes(1)
    const payload = createAccountMock.mock.calls[0]?.[0]
    expect(payload?.platform).toBe('antigravity')
    expect(payload?.type).toBe('apikey')
    expect(payload?.upstream_billing_probe_enabled).toBe(true)
    // 创建成功后前端立即发起一次首探（与其他 apikey 平台一致）。
    expect(probeUpstreamBillingMock).toHaveBeenCalledWith(42)
  })

  it('leaves Codex session import billing ownership to the backend', async () => {
    const wrapper = await openCodexImportStep()
    await wrapper.get('[data-testid="import-codex-session"]').trigger('click')
    await flushPromises()

    expect(importCodexSessionMock).toHaveBeenCalledTimes(1)
    expect(importCodexSessionMock.mock.calls[0]?.[0]?.extra?.openai_long_context_billing_enabled).toBeUndefined()
    expect(importCodexSessionMock.mock.calls[0]?.[0]).toMatchObject({
      egress_mode: 'pool',
      egress_pool: {
        route_ids: [1],
        primary_route_id: 1,
        concurrency_per_egress: 3
      }
    })
  })

  it('leaves Codex PAT import billing ownership to the backend', async () => {
    const wrapper = await openCodexImportStep()
    await wrapper.get('[data-testid="import-codex-pat"]').trigger('click')
    await flushPromises()

    expect(createOpenAICodexPATMock).toHaveBeenCalledTimes(1)
    expect(createOpenAICodexPATMock.mock.calls[0]?.[0]?.extra?.openai_long_context_billing_enabled).toBeUndefined()
    expect(createOpenAICodexPATMock.mock.calls[0]?.[0]).toMatchObject({
      egress_mode: 'pool',
      egress_pool: {
        route_ids: [1],
        primary_route_id: 1,
        concurrency_per_egress: 3
      }
    })
  })

  it('sends explicit true for Codex session import after the toggle is enabled', async () => {
    const wrapper = await openCodexImportStep(1)
    await wrapper.get('[data-testid="import-codex-session"]').trigger('click')
    await flushPromises()

    expect(importCodexSessionMock.mock.calls[0]?.[0]?.extra?.openai_long_context_billing_enabled).toBe(true)
  })

  it('sends explicit false for Codex session import after the toggle is changed back', async () => {
    const wrapper = await openCodexImportStep(2)
    await wrapper.get('[data-testid="import-codex-session"]').trigger('click')
    await flushPromises()

    expect(importCodexSessionMock.mock.calls[0]?.[0]?.extra?.openai_long_context_billing_enabled).toBe(false)
  })

  it('sends explicit true for Codex PAT import after the toggle is enabled', async () => {
    const wrapper = await openCodexImportStep(1)
    await wrapper.get('[data-testid="import-codex-pat"]').trigger('click')
    await flushPromises()

    expect(createOpenAICodexPATMock.mock.calls[0]?.[0]?.extra?.openai_long_context_billing_enabled).toBe(true)
  })

  it('sends explicit false for Codex PAT import after the toggle is changed back', async () => {
    const wrapper = await openCodexImportStep(2)
    await wrapper.get('[data-testid="import-codex-pat"]').trigger('click')
    await flushPromises()

    expect(createOpenAICodexPATMock.mock.calls[0]?.[0]?.extra?.openai_long_context_billing_enabled).toBe(false)
  })

  it('uses the global continuous warmup default for normal OpenAI OAuth creation', async () => {
    getSettingsMock.mockResolvedValue({
      openai_window_warmup_default_policy: 'continuous',
    })
    const wrapper = mountModal()
    await flushPromises()
    await selectButtonByText(wrapper, 'OpenAI')
    await wrapper.get('form#create-account-form input[type="text"]').setValue('OpenAI OAuth')
    await wrapper.get('form#create-account-form').trigger('submit.prevent')
    await wrapper.get('[data-testid="validate-refresh-token"]').trigger('click')
    await flushPromises()

    expect(createAccountMock).toHaveBeenCalledTimes(1)
    expect(createAccountMock.mock.calls[0]?.[0]).toMatchObject({
      platform: 'openai',
      type: 'oauth',
      openai_codex_warmup_policy: 'continuous',
      egress_mode: 'pool',
      egress_pool: {
        route_ids: [1],
        primary_route_id: 1,
        concurrency_per_egress: 3
      }
    })
    expect(refreshOpenAITokenMock).toHaveBeenCalledWith(
      'refresh-token',
      { egress_route_id: 1 },
      '/admin/openai/refresh-token',
      undefined
    )
    expect(createAccountMock.mock.calls[0]?.[0]?.extra).not.toHaveProperty(
      'openai_codex_warmup_policy'
    )
  })

  it('omits the warmup policy when loading global settings fails', async () => {
    getSettingsMock.mockRejectedValue(new Error('settings unavailable'))
    const wrapper = await openCodexImportStep()
    await wrapper.get('[data-testid="import-codex-session"]').trigger('click')
    await flushPromises()

    expect(importCodexSessionMock).toHaveBeenCalledTimes(1)
    expect(importCodexSessionMock.mock.calls[0]?.[0]).not.toHaveProperty(
      'openai_codex_warmup_policy'
    )
  })

  it('sends an explicit off warmup policy selected by the user', async () => {
    getSettingsMock.mockResolvedValue({
      openai_window_warmup_default_policy: 'continuous',
    })
    const wrapper = mountModal()
    await flushPromises()
    await selectButtonByText(wrapper, 'OpenAI')
    await wrapper.get('[data-testid="create-codex-warmup-off"]').trigger('click')
    await wrapper.get('form#create-account-form input[type="text"]').setValue('Codex PAT')
    await wrapper.get('form#create-account-form').trigger('submit.prevent')
    await wrapper.get('[data-testid="import-codex-pat"]').trigger('click')
    await flushPromises()

    expect(createOpenAICodexPATMock).toHaveBeenCalledTimes(1)
    expect(createOpenAICodexPATMock.mock.calls[0]?.[0]?.openai_codex_warmup_policy).toBe('off')
  })

  it('does not let a slow global default overwrite a user selection', async () => {
    let resolveSettings: ((value: Record<string, unknown>) => void) | undefined
    getSettingsMock.mockReturnValue(
      new Promise((resolve) => {
        resolveSettings = resolve
      })
    )
    const wrapper = mountModal()
    await selectButtonByText(wrapper, 'OpenAI')
    await wrapper.get('[data-testid="create-codex-warmup-initial_once"]').trigger('click')

    resolveSettings?.({ openai_window_warmup_default_policy: 'continuous' })
    await flushPromises()

    expect(
      wrapper.get('[data-testid="create-codex-warmup-initial_once"]').attributes('aria-pressed')
    ).toBe('true')
    expect(
      wrapper.get('[data-testid="create-codex-warmup-continuous"]').attributes('aria-pressed')
    ).toBe('false')

    await wrapper.get('form#create-account-form input[type="text"]').setValue('Codex session')
    await wrapper.get('form#create-account-form').trigger('submit.prevent')
    await wrapper.get('[data-testid="import-codex-session"]').trigger('click')
    await flushPromises()

    expect(importCodexSessionMock.mock.calls[0]?.[0]?.openai_codex_warmup_policy).toBe(
      'initial_once'
    )
  })

  it('passes the resolved warmup policy through the authorization-code exchange path', async () => {
    getSettingsMock.mockResolvedValue({
      openai_window_warmup_default_policy: 'initial_once',
    })
    const wrapper = mountModal()
    await flushPromises()
    await selectButtonByText(wrapper, 'OpenAI')
    await wrapper.get('form#create-account-form input[type="text"]').setValue('OAuth exchange')
    await wrapper.get('form#create-account-form').trigger('submit.prevent')

    const flow = wrapper.getComponent(OAuthAuthorizationFlowStub)
    await wrapper.get('[data-testid="generate-auth-url"]').trigger('click')
    await flushPromises()
    flow.vm.authCode = 'authorization-code'
    flow.vm.oauthState = 'oauth-state'
    await wrapper.vm.$nextTick()
    await selectButtonByText(wrapper, 'admin.accounts.oauth.completeAuth')
    await flushPromises()

    expect(exchangeCodeMock).toHaveBeenCalledTimes(1)
    expect(createAccountMock.mock.calls[0]?.[0]?.openai_codex_warmup_policy).toBe('initial_once')
  })

  it('submits a multi-route egress pool with one shared per-egress concurrency', async () => {
    const egressRoutes = [
      { id: 1, kind: 'proxy', name: 'first-route', proxy_id: 1, state: 'active', eligible: true },
      { id: 2, kind: 'proxy', name: 'authoritative-default', proxy_id: 104, state: 'active', eligible: true },
      { id: 3, kind: 'proxy', name: 'unavailable-route', proxy_id: 67, state: 'inactive', eligible: false }
    ]
    const wrapper = mountModal([], {
      egressRoutes,
      defaultEgressRouteId: 2,
      defaultEgressConcurrency: 3
    })
    await selectButtonByText(wrapper, 'OpenAI')
    await wrapper.get('form#create-account-form input[type="text"]').setValue('Pooled account')
    expect((wrapper.get('#egress-route-1').element as HTMLInputElement).checked).toBe(true)
    expect((wrapper.get('#egress-route-2').element as HTMLInputElement).checked).toBe(true)
    expect((wrapper.get('#egress-route-3').element as HTMLInputElement).checked).toBe(false)
    expect((wrapper.get('[data-testid="egress-concurrency-per-route"]').element as HTMLInputElement).value).toBe('3')
    await wrapper.get('[data-testid="egress-concurrency-per-route"]').setValue(4)
    await wrapper.get('form#create-account-form').trigger('submit.prevent')

    const flow = wrapper.getComponent(OAuthAuthorizationFlowStub)
    await wrapper.get('[data-testid="generate-auth-url"]').trigger('click')
    await flushPromises()
    expect(generateAuthUrlMock).toHaveBeenCalledWith('/admin/openai/generate-auth-url', {
      egress_route_id: 2
    })
    flow.vm.authCode = 'authorization-code'
    flow.vm.oauthState = 'oauth-state'
    await wrapper.vm.$nextTick()
    await selectButtonByText(wrapper, 'admin.accounts.oauth.completeAuth')
    await flushPromises()

    expect(exchangeCodeMock).toHaveBeenCalledWith('/admin/openai/exchange-code', expect.objectContaining({
      egress_route_id: 2
    }))

    const payload = createAccountMock.mock.calls[0]?.[0]
    expect(payload).toMatchObject({
      egress_mode: 'pool',
      egress_pool: {
        route_ids: [1, 2],
        primary_route_id: 2,
        concurrency_per_egress: 4
      }
    })
    expect(payload).not.toHaveProperty('proxy_id')
    expect(payload).not.toHaveProperty('concurrency')
  })

  it('uses the selected primary egress route for OAuth authorization', async () => {
    const wrapper = mountModal([], {
      egressRoutes: [
        { id: 2, kind: 'proxy', name: 'RN-104', proxy_id: 104, state: 'active', eligible: true }
      ],
      defaultEgressRouteId: 2
    })
    await selectButtonByText(wrapper, 'OpenAI')
    await wrapper.get('form#create-account-form input[type="text"]').setValue('OAuth account')
    await wrapper.get('#egress-route-2').setValue(true)
    await wrapper.get('form#create-account-form').trigger('submit.prevent')
    await wrapper.get('[data-testid="generate-auth-url"]').trigger('click')
    await flushPromises()

    expect(generateAuthUrlMock).toHaveBeenCalledWith('/admin/openai/generate-auth-url', {
      egress_route_id: 2
    })
  })

  it('does not guess the authentication egress from catalog order', async () => {
    const wrapper = mountModal([], {
      egressRoutes: [
        { id: 9, kind: 'proxy', name: 'first-route', state: 'active', eligible: true },
        { id: 4, kind: 'proxy', name: 'second-route', state: 'active', eligible: true }
      ],
      defaultEgressRouteId: null
    })
    await selectButtonByText(wrapper, 'OpenAI')
    await wrapper.get('form#create-account-form input[type="text"]').setValue('Missing primary')
    await wrapper.get('form#create-account-form').trigger('submit.prevent')

    expect(showErrorMock).toHaveBeenCalledWith('admin.accounts.egressPool.primaryRequired')
    expect(wrapper.findComponent(OAuthAuthorizationFlowStub).exists()).toBe(false)
    expect(generateAuthUrlMock).not.toHaveBeenCalled()
  })

  it('adopts an authoritative primary that arrives after the eligible routes', async () => {
    const wrapper = mountModal([], {
      egressRoutes: [
        { id: 1, kind: 'proxy', name: 'first-route', state: 'active', eligible: true },
        { id: 2, kind: 'proxy', name: 'default-route', state: 'active', eligible: true }
      ],
      defaultEgressRouteId: null
    })
    await selectButtonByText(wrapper, 'OpenAI')
    await wrapper.setProps({ defaultEgressRouteId: 2 })
    await wrapper.get('form#create-account-form input[type="text"]').setValue('Late default')
    await wrapper.get('form#create-account-form').trigger('submit.prevent')
    await wrapper.get('[data-testid="generate-auth-url"]').trigger('click')
    await flushPromises()

    expect(generateAuthUrlMock).toHaveBeenCalledWith('/admin/openai/generate-auth-url', {
      egress_route_id: 2
    })
  })

  it('keeps legacy proxy and concurrency fields for non-OpenAI accounts', async () => {
    const wrapper = await submitApiKeyAccount('anthropic')
    const payload = createAccountMock.mock.calls[0]?.[0]

    expect(wrapper.find('[data-testid="egress-pool-selector"]').exists()).toBe(false)
    expect(payload).toMatchObject({ proxy_id: null, concurrency: 10 })
    expect(payload).not.toHaveProperty('egress_mode')
    expect(payload).not.toHaveProperty('egress_pool')
  })

  it('keeps legacy proxy and concurrency fields for OpenAI API key accounts', async () => {
    const wrapper = await submitApiKeyAccount('openai')
    const payload = createAccountMock.mock.calls[0]?.[0]

    expect(wrapper.find('[data-testid="egress-pool-selector"]').exists()).toBe(false)
    expect(payload).toMatchObject({ proxy_id: null, concurrency: 10 })
    expect(payload).not.toHaveProperty('egress_mode')
    expect(payload).not.toHaveProperty('egress_pool')
  })
})
