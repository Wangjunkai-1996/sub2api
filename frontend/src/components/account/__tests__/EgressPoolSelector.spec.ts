import { flushPromises, mount } from '@vue/test-utils'
import { beforeEach, describe, expect, it, vi } from 'vitest'

const { verifyMock } = vi.hoisted(() => ({ verifyMock: vi.fn() }))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    egressRoutes: {
      verify: verifyMock
    }
  }
}))

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({ t: (key: string) => key })
  }
})

import EgressPoolSelector from '../EgressPoolSelector.vue'
import type { AssignableEgressRoute } from '@/types'

const routes: AssignableEgressRoute[] = [
  {
    id: 1,
    kind: 'direct',
    name: 'Local direct',
    state: 'active',
    eligible: true,
    observed_ip: '203.0.113.1',
    probe_latency_ms: 4
  },
  {
    id: 2,
    kind: 'proxy',
    name: 'RackNerd 104',
    proxy_id: 104,
    state: 'active',
    eligible: true,
    observed_ip: '198.51.100.104',
    country_code: 'US',
    probe_latency_ms: 82
  },
  {
    id: 3,
    kind: 'proxy',
    name: 'Expired route',
    proxy_id: 67,
    state: 'expired',
    eligible: false,
    observed_ip: '198.51.100.67'
  },
  {
    id: 4,
    kind: 'proxy',
    name: 'RackNerd 67',
    proxy_id: 68,
    state: 'active',
    eligible: true,
    observed_ip: '198.51.100.68'
  }
]

function mountSelector(selectedRouteIds: number[] = [], primaryRouteId: number | null = null) {
  return mount(EgressPoolSelector, {
    props: {
      routes,
      selectedRouteIds,
      primaryRouteId
    },
    global: { stubs: { Icon: true } }
  })
}

describe('EgressPoolSelector', () => {
  beforeEach(() => {
    verifyMock.mockReset()
  })

  it('shows only explicit proxy routes and hides direct routes even when previously selected', () => {
    const wrapper = mountSelector([1, 2], 1)

    expect(wrapper.find('[data-testid="egress-route-1"]').exists()).toBe(false)
    expect(wrapper.find('[data-testid="verify-egress-1"]').exists()).toBe(false)
    expect(wrapper.get('[data-testid="egress-route-2"]').exists()).toBe(true)
  })

  it('selects multiple routes and assigns the first selected route as primary', async () => {
    const wrapper = mountSelector()

    await wrapper.get('#egress-route-2').setValue(true)
    expect(wrapper.emitted('update:selectedRouteIds')?.[0]).toEqual([[2]])
    expect(wrapper.emitted('update:primaryRouteId')?.[0]).toEqual([2])

    await wrapper.setProps({ selectedRouteIds: [2], primaryRouteId: 2 })
    await wrapper.get('#egress-route-4').setValue(true)
    expect(wrapper.emitted('update:selectedRouteIds')?.[1]).toEqual([[2, 4]])
  })

  it('keeps an invalid selected route removable but prevents selecting an invalid unselected route', async () => {
    const selected = mountSelector([3], 3)
    expect(selected.get('#egress-route-3').attributes('disabled')).toBeUndefined()
    await selected.get('#egress-route-3').setValue(false)
    expect(selected.emitted('update:selectedRouteIds')?.[0]).toEqual([[]])
    expect(selected.emitted('update:primaryRouteId')?.[0]).toEqual([null])

    const unselected = mountSelector()
    expect(unselected.get('#egress-route-3').attributes('disabled')).toBeDefined()
  })

  it('verifies one route through the redacted egress API and emits the refreshed route', async () => {
    const refreshed = { ...routes[1], observed_ip: null, public_ip: '198.51.100.105', probe_success: true, probe_latency_ms: 61 }
    verifyMock.mockResolvedValue([refreshed])
    const wrapper = mountSelector([2], 2)

    await wrapper.get('[data-testid="verify-egress-2"]').trigger('click')
    await flushPromises()

    expect(verifyMock).toHaveBeenCalledWith([2])
    expect(wrapper.emitted('verified')?.[0]).toEqual([refreshed])
    expect(wrapper.text()).toContain('61ms')
    expect(wrapper.text()).toContain('198.51.100.105')
  })

  it('shows the readable probe reason returned by a failed verification', async () => {
    verifyMock.mockResolvedValue([{
      ...routes[1],
      name: 'Route #2',
      proxy_name: 'RackNerd Los Angeles',
      probe_success: false,
      probe_reason_code: 'probe_failed'
    }])
    const wrapper = mountSelector([2], 2)

    await wrapper.get('[data-testid="verify-egress-2"]').trigger('click')
    await flushPromises()

    expect(wrapper.text()).toContain('RackNerd Los Angeles')
    expect(wrapper.text()).toContain('admin.accounts.egressPool.reason.probe_failed')
    expect(wrapper.text()).not.toContain('Route #2')
  })

  it('uses a meaningful proxy fallback when the DTO has no usable route name', () => {
    const wrapper = mount(EgressPoolSelector, {
      props: {
        routes: [{
          id: 77,
          kind: 'proxy',
          name: 'Route #77',
          state: 'inactive',
          eligible: false,
          reason_code: 'proxy_unavailable'
        }],
        selectedRouteIds: [77],
        primaryRouteId: 77
      },
      global: { stubs: { Icon: true } }
    })

    expect(wrapper.text()).toContain('admin.accounts.egressPool.unnamedProxy')
    expect(wrapper.text()).toContain('admin.accounts.egressPool.reason.proxy_unavailable')
    expect(wrapper.text()).not.toContain('Route #77')
  })
})
