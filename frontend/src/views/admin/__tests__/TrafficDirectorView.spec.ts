import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import { createPinia, setActivePinia } from 'pinia'

const { getAllIncludingInactive, get, getStatus, listVersions, preview, publish, rollback, showSuccess } = vi.hoisted(() => ({
  getAllIncludingInactive: vi.fn(),
  get: vi.fn(),
  getStatus: vi.fn(),
  listVersions: vi.fn(),
  preview: vi.fn(),
  publish: vi.fn(),
  rollback: vi.fn(),
  showSuccess: vi.fn()
}))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    groups: { getAllIncludingInactive },
    trafficDirector: { get, getStatus, listVersions, preview, publish, rollback }
  }
}))

vi.mock('@/stores/app', () => ({
  useAppStore: () => ({ showSuccess })
}))

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({
      t: (key: string, params?: Record<string, unknown>) => {
        if (!params) return key
        return `${key}:${JSON.stringify(params)}`
      }
    })
  }
})

import TrafficDirectorView from '../TrafficDirectorView.vue'

const account = (id: number, status = 'active', schedulable = true) => ({ id, name: `account-${id}`, status, schedulable })
const legacyState = () => ({
  group_id: 7,
  group_name: 'OpenAI Stable',
  platform: 'openai',
  head: { group_id: 7, version: 0, mode: 'legacy', spec: null },
  accounts: [account(1)]
})
const shadowState = () => ({
  ...legacyState(),
  head: {
    group_id: 7,
    version: 1,
    mode: 'shadow',
    spec: { schema_version: 1, health_mode: 'off', pools: [{ key: 'stable', weight_bps: 10000, account_ids: [1], min_available: 1 }] }
  }
})
const status = (version: number, mode: 'legacy' | 'shadow') => ({
  group_id: 7,
  group_name: 'OpenAI Stable',
  platform: 'openai',
  head: { group_id: 7, version, mode },
  mode,
  version,
  checksum: 'checksum',
  health_mode: 'off',
  pools: [],
  account_count: 1,
  available_account_count: 1,
  assigned_account_count: mode === 'legacy' ? 0 : 1,
  unassigned_account_ids: mode === 'legacy' ? [1] : []
})
const version = (number: number, mode: 'legacy' | 'shadow') => ({
  group_id: 7,
  version: number,
  mode,
  checksum: 'checksum',
  note: '',
  created_at: '2026-08-20T00:00:00Z'
})

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise
  })
  return { promise, resolve }
}

describe('TrafficDirectorView', () => {
  beforeEach(() => {
    setActivePinia(createPinia())
    getAllIncludingInactive.mockReset().mockResolvedValue([{ id: 7, name: 'OpenAI Stable', platform: 'openai' }])
    get.mockReset().mockResolvedValueOnce(legacyState()).mockResolvedValueOnce(shadowState()).mockResolvedValueOnce(shadowState())
    getStatus.mockReset().mockResolvedValueOnce(status(0, 'legacy')).mockResolvedValueOnce(status(1, 'shadow')).mockResolvedValueOnce(status(1, 'shadow'))
    listVersions.mockReset().mockResolvedValueOnce({ items: [version(0, 'legacy')], total: 1, limit: 100, offset: 0 }).mockResolvedValueOnce({ items: [version(1, 'shadow'), version(0, 'legacy')], total: 2, limit: 100, offset: 0 }).mockResolvedValueOnce({ items: [version(1, 'shadow'), version(0, 'legacy')], total: 2, limit: 100, offset: 0 })
    preview.mockReset().mockResolvedValue({
      group_id: 7,
      expected_version: 0,
      mode: 'shadow',
      normalized_spec: { schema_version: 1, health_mode: 'off', pools: [{ key: 'stable', weight_bps: 10000, account_ids: [1], min_available: 1 }] },
      checksum: 'preview-checksum',
      unassigned_account_ids: [],
      accounts: [account(1)]
    })
    publish.mockReset().mockResolvedValue({ version: version(1, 'shadow'), replayed: false, unassigned_account_ids: [] })
    rollback.mockReset().mockResolvedValue({ version: version(2, 'shadow'), replayed: false, unassigned_account_ids: [] })
    showSuccess.mockReset()
  })

  it('covers preview, shadow publish, and rollback as one operator flow', async () => {
    publish.mockReset()
      .mockRejectedValueOnce({ message: 'network failed' })
      .mockResolvedValueOnce({ version: version(1, 'shadow'), replayed: false, unassigned_account_ids: [] })

    const wrapper = mount(TrafficDirectorView, {
      global: {
        stubs: {
          AppLayout: { template: '<div><slot /></div>' },
          BaseDialog: { props: ['show'], template: '<div v-if="show"><slot /><slot name="footer" /></div>' },
          Select: { template: '<div />' },
          Icon: true,
          LoadingSpinner: true,
          EmptyState: true
        }
      }
    })

    await flushPromises()
    await wrapper.get('[data-testid="traffic-director-mode-shadow"]').trigger('click')
    await wrapper.get('[data-testid="traffic-director-preview"]').trigger('click')
    await flushPromises()
    expect(preview).toHaveBeenCalledWith(7, expect.objectContaining({ expected_version: 0, mode: 'shadow' }))
    expect(wrapper.get('[data-testid="traffic-director-preview-result"]').exists()).toBe(true)

    await wrapper.get('[data-testid="traffic-director-publish"]').trigger('click')
    await flushPromises()
    expect(publish).toHaveBeenCalledWith(7, expect.objectContaining({ expected_version: 0, mode: 'shadow' }), expect.stringContaining('traffic-director-publish-7-'))

    const firstOperationKey = publish.mock.calls[0][2]
    await wrapper.get('[data-testid="traffic-director-publish"]').trigger('click')
    await flushPromises()
    expect(publish).toHaveBeenCalledTimes(2)
    expect(publish.mock.calls[1][2]).toBe(firstOperationKey)
    expect(wrapper.text()).toContain('v1')

    const rollbackButton = wrapper.findAll('button[title="admin.trafficDirector.history.rollback"]').find((button) => button.attributes('disabled') === undefined)
    expect(rollbackButton).toBeDefined()
    await rollbackButton!.trigger('click')
    await flushPromises()
    const dialogButtons = wrapper.findAll('button')
    const confirmRollbackButton = dialogButtons[dialogButtons.length - 1]
    expect(confirmRollbackButton).toBeDefined()
    await confirmRollbackButton.trigger('click')
    await flushPromises()
    expect(rollback).toHaveBeenCalledWith(
      7,
      0,
      expect.objectContaining({ expected_version: 1, confirm_unassigned_accounts: false, note: '' }),
      expect.stringContaining('traffic-director-rollback-0-7-')
    )
  })

  it('keeps an in-flight publish bound to its original group and ignores its stale response after a group switch', async () => {
    const nextGroupState = {
      ...shadowState(),
      group_id: 8,
      group_name: 'OpenAI Canary',
      head: {
        group_id: 8,
        version: 9,
        mode: 'shadow' as const,
        spec: { schema_version: 1 as const, health_mode: 'off' as const, pools: [{ key: 'canary', weight_bps: 10000, account_ids: [8], min_available: 1 }] }
      },
      accounts: [account(8)]
    }
    const nextGroupStatus = {
      ...status(1, 'shadow'),
      group_id: 8,
      group_name: 'OpenAI Canary',
      head: { group_id: 8, version: 9, mode: 'shadow' as const },
      version: 9
    }
    const publishResult = deferred<Awaited<ReturnType<typeof publish>>>()

    getAllIncludingInactive.mockResolvedValue([
      { id: 7, name: 'OpenAI Stable', platform: 'openai' },
      { id: 8, name: 'OpenAI Canary', platform: 'openai' }
    ])
    get.mockReset().mockImplementation((groupId: number) => Promise.resolve(groupId === 8 ? nextGroupState : shadowState()))
    getStatus.mockReset().mockImplementation((groupId: number) => Promise.resolve(groupId === 8 ? nextGroupStatus : status(1, 'shadow')))
    listVersions.mockReset().mockImplementation((groupId: number) => Promise.resolve({
      items: groupId === 8 ? [] : [version(0, 'legacy')],
      total: groupId === 8 ? 0 : 1,
      limit: 100,
      offset: 0
    }))
    preview.mockResolvedValue({
      group_id: 7,
      expected_version: 1,
      mode: 'shadow',
      normalized_spec: shadowState().head.spec,
      checksum: 'preview-checksum-v1',
      unassigned_account_ids: [],
      accounts: [account(1)]
    })
    publish.mockReset().mockReturnValueOnce(publishResult.promise)

    const wrapper = mount(TrafficDirectorView, {
      global: {
        stubs: {
          AppLayout: { template: '<div><slot /></div>' },
          BaseDialog: { props: ['show'], template: '<div v-if="show" data-testid="rollback-dialog"><slot /><slot name="footer" /></div>' },
          Select: {
            name: 'Select',
            props: ['modelValue', 'disabled'],
            emits: ['change', 'update:modelValue'],
            template: '<button type="button" data-testid="group-select" :disabled="disabled" />'
          },
          Icon: true,
          LoadingSpinner: true,
          EmptyState: true
        }
      }
    })

    await flushPromises()
    const rollbackButton = wrapper.findAll('button[title="admin.trafficDirector.history.rollback"]').find((button) => button.attributes('disabled') === undefined)
    expect(rollbackButton).toBeDefined()
    await rollbackButton!.trigger('click')
    expect(wrapper.find('[data-testid="rollback-dialog"]').exists()).toBe(true)

    await wrapper.get('[data-testid="traffic-director-preview"]').trigger('click')
    await flushPromises()
    await wrapper.get('[data-testid="traffic-director-publish"]').trigger('click')
    await Promise.resolve()

    expect(wrapper.get('[data-testid="group-select"]').attributes('disabled')).toBeDefined()
    expect(publish).toHaveBeenCalledWith(
      7,
      expect.objectContaining({ expected_version: 1, mode: 'shadow' }),
      expect.stringContaining('traffic-director-publish-7-')
    )

    wrapper.findComponent({ name: 'Select' }).vm.$emit('change', 8)
    await flushPromises()
    expect(wrapper.find('[data-testid="rollback-dialog"]').exists()).toBe(false)
    expect(wrapper.text()).toContain('v9')

    publishResult.resolve({ version: version(2, 'shadow'), replayed: false, unassigned_account_ids: [] })
    await flushPromises()

    expect(showSuccess).not.toHaveBeenCalled()
    expect(get.mock.calls.filter(([groupId]) => groupId === 8)).toHaveLength(1)
    expect(wrapper.text()).toContain('v9')
  })

  it('allows shadow publication without the enforced-only unassigned confirmation and shows inactive accounts as unavailable', async () => {
    const stateWithUnassignedInactive = {
      ...shadowState(),
      accounts: [account(1), account(2, 'inactive', true)]
    }
    get.mockReset().mockResolvedValue(stateWithUnassignedInactive)
    getStatus.mockReset().mockResolvedValue(status(1, 'shadow'))
    listVersions.mockReset().mockResolvedValue({ items: [version(1, 'shadow'), version(0, 'legacy')], total: 2, limit: 100, offset: 0 })

    const wrapper = mount(TrafficDirectorView, {
      global: {
        stubs: {
          AppLayout: { template: '<div><slot /></div>' },
          BaseDialog: { props: ['show'], template: '<div v-if="show"><slot /><slot name="footer" /></div>' },
          Select: { template: '<div />' },
          Icon: true,
          LoadingSpinner: true,
          EmptyState: true
        }
      }
    })

    await flushPromises()
    const inactiveAccount = wrapper.findAll('label').find((label) => label.text().includes('account-2'))
    expect(inactiveAccount?.text()).toContain('admin.trafficDirector.editor.unavailable')
    expect(wrapper.text()).not.toContain('admin.trafficDirector.editor.confirmUnassigned')
    expect(wrapper.get('[data-testid="traffic-director-publish"]').attributes('disabled')).toBeUndefined()

    await wrapper.get('[data-testid="traffic-director-mode-enforced"]').trigger('click')
    await wrapper.get('[data-testid="traffic-director-confirm-unassigned"]').setValue(true)
    await wrapper.get('#traffic-director-health-mode').setValue('observe')
    expect(wrapper.get('[data-testid="traffic-director-publish"]').attributes('disabled')).toBeDefined()
  })

  it('keeps inactive OpenAI groups available and locks the editor while rolling back to legacy v0 outside history', async () => {
    const rollbackResult = deferred<Awaited<ReturnType<typeof rollback>>>()
    getAllIncludingInactive.mockResolvedValue([
      { id: 7, name: 'OpenAI Stable', platform: 'openai', status: 'inactive' },
      { id: 9, name: 'Anthropic', platform: 'anthropic', status: 'active' }
    ])
    get.mockReset().mockResolvedValue(shadowState())
    getStatus.mockReset().mockResolvedValue(status(1, 'shadow'))
    listVersions.mockReset().mockResolvedValue({ items: [version(1, 'shadow')], total: 101, limit: 100, offset: 0 })
    rollback.mockReset().mockReturnValueOnce(rollbackResult.promise)

    const wrapper = mount(TrafficDirectorView, {
      global: {
        stubs: {
          AppLayout: { template: '<div><slot /></div>' },
          BaseDialog: { props: ['show'], template: '<div v-if="show" data-testid="rollback-dialog"><slot /><slot name="footer" /></div>' },
          Select: {
            name: 'Select',
            props: ['modelValue', 'options', 'disabled'],
            template: '<div />'
          },
          Icon: true,
          LoadingSpinner: true,
          EmptyState: true
        }
      }
    })

    await flushPromises()
    expect(getAllIncludingInactive).toHaveBeenCalledOnce()
    expect(wrapper.findComponent({ name: 'Select' }).props('options')).toEqual([
      { value: 7, label: 'OpenAI Stable (common.inactive)' }
    ])
    expect(wrapper.findAll('button[title="admin.trafficDirector.history.rollback"]')).toHaveLength(1)

    await wrapper.get('[data-testid="traffic-director-rollback-legacy"]').trigger('click')
    expect(wrapper.get('[data-testid="rollback-dialog"]').exists()).toBe(true)
    await wrapper.get('[data-testid="traffic-director-rollback-submit"]').trigger('click')
    await Promise.resolve()

    expect(rollback).toHaveBeenCalledWith(
      7,
      0,
      expect.objectContaining({ expected_version: 1 }),
      expect.stringContaining('traffic-director-rollback-0-7-')
    )
    for (const selector of [
      '[data-testid="traffic-director-mode-shadow"]',
      '#traffic-director-health-mode',
      '[data-testid="traffic-director-add-pool"]',
      '[data-testid="traffic-director-pool-key"]',
      '[data-testid="traffic-director-account"]',
      '#traffic-director-note',
      '[data-testid="traffic-director-preview"]',
      '[data-testid="traffic-director-publish"]',
      '[data-testid="traffic-director-history-refresh"]',
      '[data-testid="traffic-director-rollback-legacy"]',
      '#traffic-director-rollback-note',
      '[data-testid="traffic-director-rollback-confirm-unassigned"]',
      '[data-testid="traffic-director-rollback-cancel"]',
      '[data-testid="traffic-director-rollback-submit"]'
    ]) {
      expect(wrapper.get(selector).attributes('disabled'), selector).toBeDefined()
    }

    rollbackResult.resolve({ version: version(2, 'legacy'), replayed: false, unassigned_account_ids: [] })
    await flushPromises()
    expect(wrapper.find('[data-testid="rollback-dialog"]').exists()).toBe(false)
  })
})
