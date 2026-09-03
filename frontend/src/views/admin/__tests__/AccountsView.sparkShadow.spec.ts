import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'

import AccountsView from '../AccountsView.vue'
import AccountActionMenu from '@/components/admin/account/AccountActionMenu.vue'
import PlatformTypeBadge from '@/components/common/PlatformTypeBadge.vue'
import ConfirmDialog from '@/components/common/ConfirmDialog.vue'
import HelpTooltip from '@/components/common/HelpTooltip.vue'
import AccountCapacityCell from '@/components/account/AccountCapacityCell.vue'

// 外审 F2:AccountActionMenu emit 'create-spark-shadow',但 AccountsView 此前未监听,
// 导致按钮点击无效。本测试通过真实组件引用 emit 该事件,断言父页面接线调用 API。
const {
  listAccounts,
  listWithEtag,
  getBatchTodayStats,
  getUpstreamBillingProbeSettings,
  getProxyOptions,
  getAssignableEgressCatalog,
  getAccountById,
  verifyEgressRoute,
  getAllGroups,
  duplicateAccount,
  createSparkShadow,
  showSuccess,
  showError
} = vi.hoisted(() => ({
  listAccounts: vi.fn(),
  listWithEtag: vi.fn(),
  getBatchTodayStats: vi.fn(),
  getUpstreamBillingProbeSettings: vi.fn(),
  getProxyOptions: vi.fn(),
  getAssignableEgressCatalog: vi.fn(),
  getAccountById: vi.fn(),
  verifyEgressRoute: vi.fn(),
  getAllGroups: vi.fn(),
  duplicateAccount: vi.fn(),
  createSparkShadow: vi.fn(),
  showSuccess: vi.fn(),
  showError: vi.fn()
}))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    accounts: {
      list: listAccounts,
      listWithEtag,
      getBatchTodayStats,
      getById: getAccountById,
      duplicate: duplicateAccount,
      getUpstreamBillingProbeSettings,
      createSparkShadow,
      delete: vi.fn(),
      batchClearError: vi.fn(),
      batchRefresh: vi.fn(),
      toggleSchedulable: vi.fn()
    },
    proxies: { getOptions: getProxyOptions },
    egressRoutes: {
      getAssignableCatalog: getAssignableEgressCatalog,
      verify: verifyEgressRoute
    },
    groups: { getAll: getAllGroups }
  }
}))

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({ showError, showSuccess, showInfo: vi.fn() })
}))

vi.mock('@/stores/auth', () => ({
  useAuthStore: () => ({ token: 'test-token' })
}))

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({ t: (key: string) => key })
  }
})

const mountView = () =>
  mount(AccountsView, {
    global: {
      stubs: {
        AppLayout: { template: '<div><slot /></div>' },
        TablePageLayout: {
          template: '<div><slot name="filters" /><slot name="table" /><slot name="pagination" /></div>'
        },
        DataTable: true,
        Pagination: true,
        ConfirmDialog: true,
        AccountTableActions: {
          emits: ['create'],
          template: '<div><button data-test="open-create" @click="$emit(\'create\')">create</button><slot name="beforeCreate" /><slot name="after" /></div>'
        },
        AccountTableFilters: { template: '<div></div>' },
        AccountBulkActionsBar: {
          emits: ['edit-selected'],
          template: '<button data-test="open-bulk-edit" @click="$emit(\'edit-selected\')">bulk</button>'
        },
        AccountActionMenu: true,
        ImportDataModal: true,
        ReAuthAccountModal: true,
        AccountTestModal: true,
        AccountStatsModal: true,
        ScheduledTestsPanel: true,
        SyncFromCrsModal: true,
        TempUnschedStatusModal: true,
        ErrorPassthroughRulesModal: true,
        TLSFingerprintProfilesModal: true,
        CreateAccountModal: {
          props: ['show', 'proxies', 'egressRoutes', 'defaultEgressRouteId', 'defaultEgressConcurrency'],
          template: '<div data-test="create-account-modal" :data-show="String(show)" :data-proxy-count="String(proxies?.length || 0)" :data-route-count="String(egressRoutes?.length || 0)" :data-default-route-id="String(defaultEgressRouteId ?? \'\')" :data-default-concurrency="String(defaultEgressConcurrency ?? \'\')" />'
        },
        EditAccountModal: true,
        BulkEditAccountModal: {
          props: ['show', 'proxies', 'egressRoutes'],
          template: '<div data-test="bulk-edit-account-modal" :data-show="String(show)" :data-proxy-count="String(proxies?.length || 0)" :data-route-count="String(egressRoutes?.length || 0)" />'
        },
        PlatformTypeBadge: true,
        AccountCapacityCell: true,
        AccountStatusIndicator: true,
        AccountTodayStatsCell: true,
        AccountGroupsCell: true,
        AccountUsageCell: true,
        Icon: true
      }
    }
  })

describe('admin AccountsView — 外审 F2:spark 影子创建接线', () => {
  beforeEach(() => {
    localStorage.clear()
    for (const fn of [listAccounts, listWithEtag, getBatchTodayStats, getUpstreamBillingProbeSettings, getProxyOptions, getAssignableEgressCatalog, getAccountById, verifyEgressRoute, getAllGroups, duplicateAccount, createSparkShadow, showSuccess, showError]) {
      fn.mockReset()
    }
    listAccounts.mockResolvedValue({ items: [], total: 0, page: 1, page_size: 20, pages: 0 })
    listWithEtag.mockResolvedValue({ notModified: true, etag: null, data: null })
    getBatchTodayStats.mockResolvedValue({ stats: {} })
    getUpstreamBillingProbeSettings.mockResolvedValue({ enabled: true, interval_minutes: 30 })
    getProxyOptions.mockResolvedValue([])
    getAssignableEgressCatalog.mockResolvedValue({ items: [], capabilities: { mutation_enabled: true } })
    getAccountById.mockImplementation(async (id: number) => ({ id }))
    verifyEgressRoute.mockResolvedValue([])
    getAllGroups.mockResolvedValue([])
    duplicateAccount.mockResolvedValue({ id: 998, name: 'parent-acc (Copy)' })
    createSparkShadow.mockResolvedValue({ id: 999, name: 'parent-acc (Spark)' })
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('AccountActionMenu 的 duplicate 事件一键复制账号并刷新列表', async () => {
    const wrapper = mountView()
    await flushPromises()

    wrapper.findComponent(AccountActionMenu).vm.$emit('duplicate', { id: 42, name: 'parent-acc' })
    await flushPromises()

    expect(duplicateAccount).toHaveBeenCalledTimes(1)
    expect(duplicateAccount).toHaveBeenCalledWith(42)
    expect(showSuccess).toHaveBeenCalledWith('admin.accounts.duplicateSuccess')
    expect(listAccounts.mock.calls.length).toBeGreaterThan(1)
    wrapper.unmount()
  })

  it('账号设置只加载脱敏的出口与代理选项', async () => {
    const wrapper = mountView()
    await flushPromises()

    expect(getAssignableEgressCatalog).toHaveBeenCalledTimes(1)
    expect(getProxyOptions).toHaveBeenCalledTimes(1)
    wrapper.unmount()
  })

  it('打开创建弹窗时刷新目录并清除加载失败的旧代理选项', async () => {
    getProxyOptions
      .mockResolvedValueOnce([{ id: 1, name: 'old', display_endpoint: 'old:1', status: 'active', selectable: true }])
      .mockRejectedValueOnce(new Error('proxy catalog unavailable'))
    getAssignableEgressCatalog
      .mockResolvedValueOnce({ items: [], capabilities: { mutation_enabled: true } })
      .mockResolvedValueOnce({
        items: [{ id: 2, kind: 'proxy', name: 'fresh-route', state: 'active', eligible: true }],
        default_route_id: 2,
        default_concurrency: 3,
        capabilities: { mutation_enabled: true }
      })
    vi.spyOn(console, 'error').mockImplementation(() => {})

    const wrapper = mountView()
    await flushPromises()
    await wrapper.get('[data-test="open-create"]').trigger('click')
    await flushPromises()

    expect(getAssignableEgressCatalog).toHaveBeenCalledTimes(2)
    expect(getProxyOptions).toHaveBeenCalledTimes(2)
    const modal = wrapper.get('[data-test="create-account-modal"]')
    expect(modal.attributes('data-show')).toBe('true')
    expect(modal.attributes('data-proxy-count')).toBe('0')
    expect(modal.attributes('data-route-count')).toBe('1')
    expect(modal.attributes('data-default-route-id')).toBe('2')
    expect(modal.attributes('data-default-concurrency')).toBe('3')
    wrapper.unmount()
  })

  it('打开批量编辑弹窗时刷新出口和代理目录', async () => {
    const wrapper = mountView()
    await flushPromises()
    await wrapper.get('[data-test="open-bulk-edit"]').trigger('click')
    await flushPromises()

    expect(getAssignableEgressCatalog).toHaveBeenCalledTimes(2)
    expect(getProxyOptions).toHaveBeenCalledTimes(2)
    expect(wrapper.get('[data-test="bulk-edit-account-modal"]').attributes('data-show')).toBe('true')
    wrapper.unmount()
  })

  it('同一账号复制请求未完成时忽略重复点击', async () => {
    let resolveDuplicate!: (account: { id: number; name: string }) => void
    duplicateAccount.mockImplementationOnce(() => new Promise(resolve => { resolveDuplicate = resolve }))
    const wrapper = mountView()
    await flushPromises()

    const menu = wrapper.findComponent(AccountActionMenu)
    menu.vm.$emit('duplicate', { id: 42, name: 'parent-acc' })
    menu.vm.$emit('duplicate', { id: 42, name: 'parent-acc' })
    await flushPromises()

    expect(duplicateAccount).toHaveBeenCalledTimes(1)
    resolveDuplicate({ id: 998, name: 'parent-acc (Copy)' })
    await flushPromises()
    wrapper.unmount()
  })

  it('复制失败时显示后端错误', async () => {
    const consoleError = vi.spyOn(console, 'error').mockImplementation(() => {})
    duplicateAccount.mockRejectedValueOnce(new Error('duplicate failed'))
    const wrapper = mountView()
    await flushPromises()

    wrapper.findComponent(AccountActionMenu).vm.$emit('duplicate', { id: 42, name: 'parent-acc' })
    await flushPromises()

    expect(showError).toHaveBeenCalledWith('duplicate failed')
    consoleError.mockRestore()
    wrapper.unmount()
  })

  it('AccountActionMenu 的 create-spark-shadow 事件触发 createSparkShadow API + 成功提示', async () => {
    const wrapper = mountView()
    await flushPromises()

    const menu = wrapper.findComponent(AccountActionMenu)
    expect(menu.exists()).toBe(true)

    menu.vm.$emit('create-spark-shadow', { id: 42, name: 'parent-acc' })
    await flushPromises()

    // 不再用原生 confirm,改用应用内 ConfirmDialog:先弹出,点确认才调 API
    const dialog = wrapper.findAllComponents(ConfirmDialog).find(d => d.props('show'))
    expect(dialog).toBeTruthy()
    dialog?.vm.$emit('confirm')
    await flushPromises()

    expect(createSparkShadow).toHaveBeenCalledTimes(1)
    expect(createSparkShadow).toHaveBeenCalledWith(42, { name: 'parent-acc (Spark)' })
    expect(showSuccess).toHaveBeenCalledWith('admin.accounts.createSparkShadowSuccess')
    wrapper.unmount()
  })

  it('用户取消确认时不调用 API', async () => {
    const wrapper = mountView()
    await flushPromises()

    wrapper.findComponent(AccountActionMenu).vm.$emit('create-spark-shadow', { id: 42, name: 'parent-acc' })
    await flushPromises()

    // 弹出 ConfirmDialog 后点取消,不应调用 API
    const dialog = wrapper.findAllComponents(ConfirmDialog).find(d => d.props('show'))
    expect(dialog).toBeTruthy()
    dialog?.vm.$emit('cancel')
    await flushPromises()

    expect(createSparkShadow).not.toHaveBeenCalled()
    wrapper.unmount()
  })
})

// 账号行展示
const mountViewWithRow = () =>
  mount(AccountsView, {
    global: {
      stubs: {
        AppLayout: { template: '<div><slot /></div>' },
        TablePageLayout: {
          template: '<div><slot name="filters" /><slot name="table" /><slot name="pagination" /></div>'
        },
        // 使用能透传 row 数据的自定义 DataTable stub，以便渲染 cell 插槽
        DataTable: {
          props: ['data', 'columns', 'loading'],
          template: `<div>
            <div v-for="(row, idx) in (data || [])" :key="idx">
              <slot name="cell-name" :row="row" :value="row.name" />
              <slot name="cell-platform_type" :row="row" />
              <slot name="cell-capacity" :row="row" />
              <slot name="cell-proxy" :row="row" />
              <slot name="cell-actions" :row="row" />
            </div>
          </div>`
        },
        Pagination: true,
        ConfirmDialog: true,
        AccountTableActions: { template: '<div><slot name="beforeCreate" /><slot name="after" /></div>' },
        AccountTableFilters: { template: '<div></div>' },
        AccountBulkActionsBar: true,
        AccountActionMenu: true,
        ImportDataModal: true,
        ReAuthAccountModal: true,
        AccountTestModal: true,
        AccountStatsModal: true,
        ScheduledTestsPanel: true,
        SyncFromCrsModal: true,
        TempUnschedStatusModal: true,
        ErrorPassthroughRulesModal: true,
        TLSFingerprintProfilesModal: true,
        CreateAccountModal: true,
        EditAccountModal: {
          props: [
            'show',
            'account',
            'proxies',
            'egressRoutes',
            'egressMutationEnabled',
            'egressVerifyingRouteId',
            'egressVerifyErrors'
          ],
          emits: ['verify-egress-route'],
          template: `<div
            data-test="edit-account-modal"
            :data-show="String(show)"
            :data-account-name="account?.name"
            :data-mutation-enabled="String(egressMutationEnabled)"
          >
            <button
              v-for="route in egressRoutes"
              :key="route.id"
              data-test="edit-egress-route"
              @click="$emit('verify-egress-route', route)"
            >{{ route.name }} {{ route.probe_latency_ms }}</button>
            <span v-for="proxy in proxies" :key="proxy.id" data-test="edit-proxy-option">{{ proxy.display_endpoint }}</span>
            <span
              v-for="route in (account?.egress_pool?.routes || [])"
              :key="route.id"
              data-test="edit-embedded-egress-route"
            >{{ route.name }}</span>
          </div>`
        },
        BulkEditAccountModal: true,
        PlatformTypeBadge: true,
        AccountCapacityCell: true,
        AccountStatusIndicator: true,
        AccountTodayStatsCell: true,
        AccountGroupsCell: true,
        AccountUsageCell: true,
        Icon: true
      }
    }
  })

describe('admin AccountsView — 账号行展示', () => {
  beforeEach(() => {
    localStorage.clear()
    for (const fn of [listAccounts, listWithEtag, getBatchTodayStats, getUpstreamBillingProbeSettings, getProxyOptions, getAssignableEgressCatalog, getAccountById, verifyEgressRoute, getAllGroups, duplicateAccount, createSparkShadow, showSuccess, showError]) {
      fn.mockReset()
    }
    listWithEtag.mockResolvedValue({ notModified: true, etag: null, data: null })
    getBatchTodayStats.mockResolvedValue({ stats: {} })
    getUpstreamBillingProbeSettings.mockResolvedValue({ enabled: true, interval_minutes: 30 })
    getProxyOptions.mockResolvedValue([])
    getAssignableEgressCatalog.mockResolvedValue({ items: [], capabilities: { mutation_enabled: true } })
    getAccountById.mockImplementation(async (id: number) => ({ id }))
    verifyEgressRoute.mockResolvedValue([])
    getAllGroups.mockResolvedValue([])
    vi.stubGlobal('confirm', vi.fn(() => true))
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
  })

  it('打开 OpenAI OAuth 编辑弹窗前重新加载可分配出口', async () => {
    const account = {
      id: 11049,
      name: 'openai-oauth',
      platform: 'openai',
      type: 'oauth',
      status: 'active',
      schedulable: true
    }
    const refreshedRoutes = [{
      id: 12,
      kind: 'proxy',
      name: 'racknerd-104-ipv4',
      state: 'active',
      eligible: true,
      observed_ip: '104.223.77.152'
    }]
    const proxyOptions = [{
      id: 104,
      name: 'racknerd-104',
      display_endpoint: 'socks5://104.223.77.152:1080',
      status: 'active',
      selectable: true
    }]
    listAccounts.mockResolvedValue({ items: [account], total: 1, page: 1, page_size: 20, pages: 1 })
    getAssignableEgressCatalog
      .mockResolvedValueOnce({ items: [], capabilities: { mutation_enabled: true } })
      .mockResolvedValueOnce({ items: refreshedRoutes, capabilities: { mutation_enabled: true } })
    getProxyOptions
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce(proxyOptions)
    getAccountById.mockResolvedValue({ ...account, name: 'fresh-openai-oauth' })
    verifyEgressRoute.mockResolvedValue([{ ...refreshedRoutes[0], probe_latency_ms: 61 }])

    const wrapper = mountViewWithRow()
    await flushPromises()

    const editButton = wrapper.findAll('button').find(button => button.text() === 'common.edit')
    expect(editButton).toBeTruthy()
    await editButton!.trigger('click')
    await flushPromises()

    expect(getAssignableEgressCatalog).toHaveBeenCalledTimes(2)
    expect(getAccountById).toHaveBeenCalledWith(11049)
    expect(wrapper.get('[data-test="edit-account-modal"]').attributes('data-show')).toBe('true')
    expect(wrapper.get('[data-test="edit-account-modal"]').attributes('data-account-name')).toBe('fresh-openai-oauth')
    expect(wrapper.get('[data-test="edit-account-modal"]').attributes('data-mutation-enabled')).toBe('true')
    expect(wrapper.get('[data-test="edit-egress-route"]').text()).toContain('racknerd-104-ipv4')
    expect(wrapper.get('[data-test="edit-proxy-option"]').text()).toBe('socks5://104.223.77.152:1080')

    await wrapper.get('[data-test="edit-egress-route"]').trigger('click')
    await flushPromises()
    expect(verifyEgressRoute).toHaveBeenCalledWith([12])
    expect(wrapper.get('[data-test="edit-egress-route"]').text()).toContain('61')
    wrapper.unmount()
  })

  it('目录刷新失败时使用 fresh 详情展示已绑定出口并禁用修改', async () => {
    const rowAccount = {
      id: 11050,
      name: 'stale-row',
      platform: 'openai',
      type: 'oauth',
      status: 'active'
    }
    const embeddedRoute = {
      id: 18,
      kind: 'proxy',
      name: 'bound-route',
      state: 'retired',
      eligible: false
    }
    listAccounts.mockResolvedValue({ items: [rowAccount], total: 1, page: 1, page_size: 20, pages: 1 })
    getAssignableEgressCatalog
      .mockResolvedValueOnce({ items: [], capabilities: { mutation_enabled: true } })
      .mockRejectedValueOnce(new Error('catalog unavailable'))
    getAccountById.mockResolvedValue({
      ...rowAccount,
      name: 'fresh-account',
      egress_mode: 'pool',
      egress_pool: {
        route_ids: [18],
        primary_route_id: 18,
        concurrency_per_egress: 2,
        revision: 4,
        routes: [embeddedRoute]
      }
    })
    vi.spyOn(console, 'error').mockImplementation(() => {})

    const wrapper = mountViewWithRow()
    await flushPromises()
    const editButton = wrapper.findAll('button').find(button => button.text() === 'common.edit')
    await editButton!.trigger('click')
    await flushPromises()

    expect(wrapper.get('[data-test="edit-account-modal"]').attributes('data-show')).toBe('true')
    expect(wrapper.get('[data-test="edit-account-modal"]').attributes('data-account-name')).toBe('fresh-account')
    expect(wrapper.get('[data-test="edit-account-modal"]').attributes('data-mutation-enabled')).toBe('false')
    expect(wrapper.get('[data-test="edit-embedded-egress-route"]').text()).toBe('bound-route')
    wrapper.unmount()
  })

  it('影子行 email 单元格显示 parent_email，PlatformTypeBadge 接收 parent_plan_type/parent_privacy_mode', async () => {
    const shadowAccount = {
      id: 100,
      name: '影子账号',
      platform: 'openai',
      type: 'oauth',
      parent_account_id: 1,
      parent_email: 'parent@example.com',
      parent_plan_type: 'plus',
      parent_privacy_mode: 'false',
      parent_subscription_expires_at: '2027-01-01T00:00:00Z',
      parent_chatgpt_account_id: 'chatgpt-abc123',
    }

    listAccounts.mockResolvedValue({ items: [shadowAccount], total: 1, page: 1, page_size: 20, pages: 1 })

    const wrapper = mountViewWithRow()
    await flushPromises()

    // 1. email 单元格通过 OR 兜底渲染 parent_email
    expect(wrapper.text()).toContain('parent@example.com')

    // 2. PlatformTypeBadge 收到 parent_plan_type 和 parent_privacy_mode
    const badge = wrapper.findComponent(PlatformTypeBadge)
    expect(badge.exists()).toBe(true)
    expect(badge.props('planType')).toBe('plus')
    expect(badge.props('privacyMode')).toBe('false')
    expect(badge.props('subscriptionExpiresAt')).toBe('2027-01-01T00:00:00Z')

    wrapper.unmount()
  })

  it('出口列显示两个节点、折叠剩余数量，并标记继承与降级', async () => {
    listAccounts.mockResolvedValue({
      items: [{
        id: 100,
        name: 'shadow',
        platform: 'openai',
        type: 'oauth',
        parent_account_id: 1,
        egress_mode: 'inherited',
        egress_summary: {
          configured_route_count: 3,
          eligible_route_count: 2,
          degraded_route_count: 1,
          concurrency_per_egress: 4,
          effective_capacity: 8,
          routes: [
            { id: 1, kind: 'direct', name: 'Local', state: 'active', eligible: true },
            { id: 2, kind: 'proxy', name: 'RN-104', state: 'active', eligible: true, observed_ip: '104.223.77.152' },
            { id: 3, kind: 'proxy', name: 'RN-67', state: 'expired', eligible: false }
          ]
        }
      }],
      total: 1,
      page: 1,
      page_size: 20,
      pages: 1
    })

    const wrapper = mountViewWithRow()
    await flushPromises()

    expect(wrapper.text()).toContain('admin.accounts.egressPool.inherited')
    expect(wrapper.text()).toContain('Local')
    expect(wrapper.text()).toContain('RN-104')
    expect(wrapper.text()).not.toContain('RN-67')
    const routeChip = wrapper.findAll('span[title]').find((node) => node.text().includes('RN-104'))
    expect(routeChip?.attributes('title')).toContain('104.***.***.152')
    expect(routeChip?.attributes('title')).not.toContain('104.223.77.152')
    expect(wrapper.text()).toContain('+1')
    expect(wrapper.text()).toContain('admin.accounts.egressPool.degraded')
    wrapper.unmount()
  })

  it('仅将具有安全 base_url 的 API Key 账号名称链接到站点主页', async () => {
    listAccounts.mockResolvedValue({
      items: [
        { id: 101, name: 'relay-account', platform: 'openai', type: 'apikey', credentials: { base_url: 'https://relay.example.com/api/v1/' } },
        { id: 102, name: 'oauth-account', platform: 'openai', type: 'oauth', credentials: { base_url: 'https://oauth.example.com/v1' } },
        { id: 103, name: 'invalid-url', platform: 'openai', type: 'apikey', credentials: { base_url: 'javascript:alert(1)' } },
      ],
      total: 3,
      page: 1,
      page_size: 20,
      pages: 1,
    })

    const wrapper = mountViewWithRow()
    await flushPromises()

    const links = wrapper.findAll('a')
    expect(links).toHaveLength(1)
    const [link] = links
    expect(link.text()).toBe('relay-account')
    expect(link.attributes()).toMatchObject({
      href: 'https://relay.example.com',
      target: '_blank',
      rel: 'noopener noreferrer',
    })
    expect(link.classes()).toEqual(expect.arrayContaining([
      'border-dotted',
      'text-gray-900',
      'dark:text-white',
    ]))
    expect(link.classes()).not.toContain('text-primary-600')
    const tooltip = wrapper.findComponent(HelpTooltip)
    expect(tooltip.props('content')).toBe('https://relay.example.com')
    expect(tooltip.props('widthClass')).toBe('w-max max-w-sm break-all')
    expect(tooltip.classes()).toEqual(expect.arrayContaining(['self-start']))
    expect(wrapper.text()).toContain('oauth-account')
    expect(wrapper.text()).toContain('invalid-url')

    wrapper.unmount()
  })

  it('prefers persisted Grok JWT tier over lagging billing/quota snapshots', async () => {
    const grokAccounts = [
      {
        id: 201,
        name: 'oauth-tier',
        platform: 'grok',
        type: 'oauth',
        credentials: { subscription_tier: 'FREE', plan_type: 'legacy' },
        extra: {
          grok_billing_snapshot: { plan: 'SuperGrok' },
          subscription_tier: 'BASIC',
        },
      },
      {
        id: 202,
        name: 'billing-tier',
        platform: 'grok',
        type: 'oauth',
        credentials: {},
        extra: {
          grok_billing_snapshot: { plan: 'SuperGrok Heavy' },
          subscription_tier: 'BASIC',
        },
      },
      {
        id: 203,
        name: 'quota-tier',
        platform: 'grok',
        type: 'oauth',
        credentials: { subscription_tier: 'FREE' },
        extra: {
          grok_quota_snapshot: { subscription_tier: 'SuperGrok' },
          subscription_tier: 'BASIC',
        },
      },
      {
        id: 204,
        name: 'extra-tier',
        platform: 'grok',
        type: 'oauth',
        credentials: { plan_type: 'SuperGrok' },
        extra: { subscription_tier: 'BASIC' },
      },
      {
        id: 205,
        name: 'legacy-tier',
        platform: 'grok',
        type: 'oauth',
        credentials: { plan_type: 'SuperGrok' },
      },
      {
        id: 206,
        name: 'supergrokpro-responses-quota',
        platform: 'grok',
        type: 'oauth',
        credentials: { subscription_tier: 'SuperGrokPro' },
        extra: {
          grok_billing_snapshot: { plan: 'SuperGrok' },
          grok_usage_snapshot: {
            model: 'grok-4.5',
            last_headers_seen_at: new Date().toISOString(),
            requests: { limit: 8300 },
            tokens: { limit: 53_000_000 },
          },
          grok_quota_snapshot: {
            model: 'grok-4.6',
            last_headers_seen_at: new Date().toISOString(),
            requests: { limit: 8300 },
            tokens: { limit: 53_000_000 },
          },
        },
      },
      {
        id: 207,
        name: 'supergrokpro-other-model-quota',
        platform: 'grok',
        type: 'oauth',
        credentials: { subscription_tier: 'SuperGrokPro' },
        extra: {
          grok_billing_snapshot: { plan: 'SuperGrok' },
          grok_usage_snapshot: {
            model: 'grok-4.6',
            last_headers_seen_at: new Date().toISOString(),
            requests: { limit: 8300 },
            tokens: { limit: 53_000_000 },
          },
        },
      },
      {
        id: 208,
        name: 'usage-over-legacy-quota',
        platform: 'grok',
        type: 'oauth',
        credentials: {},
        extra: {
          grok_usage_snapshot: { subscription_tier: 'SuperGrok' },
          grok_quota_snapshot: { subscription_tier: 'Free' },
        },
      },
      {
        id: 209,
        name: 'legacy-quota-alias',
        platform: 'grok',
        type: 'oauth',
        credentials: {},
        extra: { grok_quota_snapshot: { subscription_tier: 'SuperGrok' } },
      },
    ]

    listAccounts.mockResolvedValue({
      items: grokAccounts,
      total: grokAccounts.length,
      page: 1,
      page_size: 20,
      pages: 1,
    })

    const wrapper = mountViewWithRow()
    await flushPromises()

    const badges = wrapper.findAllComponents(PlatformTypeBadge)
    expect(badges.map((badge) => badge.props('planType'))).toEqual([
      'FREE',
      'SuperGrok Heavy',
      'FREE',
      'BASIC',
      'SuperGrok',
      'SuperGrok Heavy',
      'SuperGrok',
      'SuperGrok',
      'SuperGrok',
    ])

    wrapper.unmount()
  })

  it('skips malformed Grok plan fields and safely uses the next valid fallback', async () => {
    const grokAccounts = [
      {
        id: 210,
        name: 'legacy-fallback',
        platform: 'grok',
        type: 'oauth',
        credentials: {},
        extra: {
          grok_usage_snapshot: { subscription_tier: { name: 'SuperGrok Heavy' } },
          grok_quota_snapshot: { subscription_tier: 'SuperGrok' },
        },
      },
      {
        id: 211,
        name: 'credential-plan-fallback',
        platform: 'grok',
        type: 'oauth',
        credentials: { subscription_tier: 0, plan_type: 'SuperGrok Heavy' },
        extra: {
          grok_billing_snapshot: { plan: {} },
          grok_usage_snapshot: { subscription_tier: 1 },
          grok_quota_snapshot: { subscription_tier: [] },
          subscription_tier: '   ',
        },
      },
      {
        id: 212,
        name: 'no-valid-plan',
        platform: 'grok',
        type: 'oauth',
        credentials: { subscription_tier: {}, plan_type: 2 },
        parent_plan_type: [],
        extra: {
          grok_billing_snapshot: { plan: [] },
          grok_usage_snapshot: { subscription_tier: 1 },
          grok_quota_snapshot: { subscription_tier: {} },
          subscription_tier: null,
        },
      },
    ]

    listAccounts.mockResolvedValue({
      items: grokAccounts,
      total: grokAccounts.length,
      page: 1,
      page_size: 20,
      pages: 1,
    })

    const wrapper = mountViewWithRow()
    await flushPromises()

    expect(wrapper.findAllComponents(PlatformTypeBadge).map((badge) => badge.props('planType'))).toEqual([
      'SuperGrok',
      'SuperGrok Heavy',
      undefined,
    ])
    wrapper.unmount()
  })

  it('replaces a Grok row when auto refresh returns a changed canonical usage snapshot', async () => {
    vi.useFakeTimers()
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    localStorage.setItem('account-auto-refresh', JSON.stringify({ enabled: true, interval_seconds: 5 }))

    const initialAccount = {
      id: 213,
      name: 'refresh-tier',
      platform: 'grok',
      type: 'oauth',
      extra: { grok_usage_snapshot: { subscription_tier: 'Free', status_code: 200 } },
    }
    const refreshedAccount = {
      ...initialAccount,
      extra: { grok_usage_snapshot: { subscription_tier: 'SuperGrok', status_code: 200 } },
    }
    listAccounts.mockResolvedValue({ items: [initialAccount], total: 1, page: 1, page_size: 20, pages: 1 })
    listWithEtag.mockResolvedValueOnce({
      notModified: false,
      etag: 'grok-snapshot-2',
      data: { items: [refreshedAccount], total: 1, page: 1, page_size: 20, pages: 1 },
    })

    const wrapper = mountViewWithRow()
    await flushPromises()
    expect(wrapper.findComponent(PlatformTypeBadge).props('planType')).toBe('Free')

    await vi.advanceTimersByTimeAsync(6000)
    await flushPromises()

    expect(listWithEtag).toHaveBeenCalledTimes(1)
    expect(wrapper.findComponent(PlatformTypeBadge).props('planType')).toBe('SuperGrok')
    wrapper.unmount()
  })

  it('replaces an OpenAI pool row when per-IP load moves but the total stays unchanged', async () => {
    vi.useFakeTimers()
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    localStorage.setItem('account-auto-refresh', JSON.stringify({ enabled: true, interval_seconds: 5 }))

    const initialAccount = {
      id: 214,
      name: 'pool-load-refresh',
      platform: 'openai',
      type: 'oauth',
      current_concurrency: 3,
      egress_summary: {
        configured_route_count: 3,
        eligible_route_count: 3,
        concurrency_per_egress: 3,
        effective_capacity: 9,
        current_concurrency: 3,
        bindings: [
          { route_id: 10, observed_ip: '51.81.109.154', eligible: true, current_concurrency: 1 },
          { route_id: 11, observed_ip: '67.215.237.47', eligible: true, current_concurrency: 2 },
          { route_id: 12, observed_ip: '104.223.77.152', eligible: true, current_concurrency: 0 },
        ],
      },
    }
    const refreshedAccount = {
      ...initialAccount,
      egress_summary: {
        ...initialAccount.egress_summary,
        bindings: [
          { route_id: 10, observed_ip: '51.81.109.154', eligible: true, current_concurrency: 0 },
          { route_id: 11, observed_ip: '67.215.237.47', eligible: true, current_concurrency: 3 },
          { route_id: 12, observed_ip: '104.223.77.152', eligible: true, current_concurrency: 0 },
        ],
      },
    }
    listAccounts.mockResolvedValue({ items: [initialAccount], total: 1, page: 1, page_size: 20, pages: 1 })
    listWithEtag.mockResolvedValueOnce({
      notModified: false,
      etag: 'pool-load-2',
      data: { items: [refreshedAccount], total: 1, page: 1, page_size: 20, pages: 1 },
    })

    const wrapper = mountViewWithRow()
    await flushPromises()
    expect(wrapper.findComponent(AccountCapacityCell).props('account').egress_summary.bindings[0].current_concurrency).toBe(1)

    await vi.advanceTimersByTimeAsync(6000)
    await flushPromises()

    expect(wrapper.findComponent(AccountCapacityCell).props('account').egress_summary.bindings[0].current_concurrency).toBe(0)
    expect(wrapper.findComponent(AccountCapacityCell).props('account').egress_summary.bindings[1].current_concurrency).toBe(3)
    wrapper.unmount()
  })

  it('replaces an OpenAI pool row when binding metadata changes but all loads stay unchanged', async () => {
    vi.useFakeTimers()
    vi.spyOn(document, 'hidden', 'get').mockReturnValue(false)
    localStorage.setItem('account-auto-refresh', JSON.stringify({ enabled: true, interval_seconds: 5 }))

    const initialAccount = {
      id: 215,
      name: 'pool-metadata-refresh',
      platform: 'openai',
      type: 'oauth',
      current_concurrency: 3,
      egress_summary: {
        configured_route_count: 3,
        eligible_route_count: 3,
        concurrency_per_egress: 3,
        effective_capacity: 9,
        current_concurrency: 3,
        bindings: [
          { route_id: 10, name: 'Local', observed_ip: '51.81.109.154', eligible: true, current_concurrency: 1 },
          { route_id: 11, name: 'RN-67', observed_ip: '67.215.237.47', eligible: true, current_concurrency: 2 },
          { route_id: 12, name: 'RN-104', observed_ip: '104.223.77.152', eligible: true, current_concurrency: 0 },
        ],
      },
    }
    const renamedAccount = {
      ...initialAccount,
      egress_summary: {
        ...initialAccount.egress_summary,
        bindings: initialAccount.egress_summary.bindings.map(binding => (
          binding.route_id === 11 ? { ...binding, name: 'RackNerd 67' } : binding
        )),
      },
    }
    const reidentifiedAccount = {
      ...renamedAccount,
      egress_summary: {
        ...renamedAccount.egress_summary,
        bindings: renamedAccount.egress_summary.bindings.map(binding => (
          binding.route_id === 12 ? { ...binding, observed_ip: '104.223.77.153' } : binding
        )),
      },
    }
    listAccounts.mockResolvedValue({ items: [initialAccount], total: 1, page: 1, page_size: 20, pages: 1 })
    listWithEtag
      .mockResolvedValueOnce({
        notModified: false,
        etag: 'pool-metadata-2',
        data: { items: [renamedAccount], total: 1, page: 1, page_size: 20, pages: 1 },
      })
      .mockResolvedValueOnce({
        notModified: false,
        etag: 'pool-metadata-3',
        data: { items: [reidentifiedAccount], total: 1, page: 1, page_size: 20, pages: 1 },
      })

    const wrapper = mountViewWithRow()
    await flushPromises()

    await vi.advanceTimersByTimeAsync(6000)
    await flushPromises()
    let renderedBindings = wrapper.findComponent(AccountCapacityCell).props('account').egress_summary.bindings
    expect(renderedBindings[1].name).toBe('RackNerd 67')
    expect(renderedBindings.map((binding: { current_concurrency: number }) => binding.current_concurrency)).toEqual([1, 2, 0])

    await vi.advanceTimersByTimeAsync(6000)
    await flushPromises()
    renderedBindings = wrapper.findComponent(AccountCapacityCell).props('account').egress_summary.bindings
    expect(renderedBindings[2].observed_ip).toBe('104.223.77.153')
    expect(renderedBindings.map((binding: { current_concurrency: number }) => binding.current_concurrency)).toEqual([1, 2, 0])
    expect(listWithEtag).toHaveBeenCalledTimes(2)
    wrapper.unmount()
  })
})
