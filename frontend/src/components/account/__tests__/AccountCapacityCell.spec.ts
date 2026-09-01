import { mount } from '@vue/test-utils'
import { describe, expect, it, vi } from 'vitest'

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({
      t: (key: string, params?: Record<string, number>) => {
        if (key.endsWith('capacityFormula')) {
          return `${params?.eligible} IP x ${params?.perEgress} = ${params?.capacity}`
        }
        if (key.endsWith('capacityDetail')) {
          return `${params?.configured}/${params?.eligible}/${params?.degraded}`
        }
        return key
      }
    })
  }
})

import AccountCapacityCell from '../AccountCapacityCell.vue'

describe('AccountCapacityCell egress capacity', () => {
  it('uses server-computed effective capacity and shows the shared per-egress formula', () => {
    const wrapper = mount(AccountCapacityCell, {
      props: {
        account: {
          id: 1,
          platform: 'openai',
          type: 'oauth',
          concurrency: 4,
          current_concurrency: 2,
          egress_summary: {
            configured_route_count: 3,
            eligible_route_count: 3,
            degraded_route_count: 0,
            concurrency_per_egress: 4,
            effective_capacity: 12,
            current_concurrency: 5,
            bindings: [
              { route_id: 1, name: 'Local', current_concurrency: 1 },
              { route_id: 2, name: 'RN-104', current_concurrency: 2 },
              { route_id: 3, name: 'RN-67', current_concurrency: 2 }
            ]
          }
        } as any
      }
    })

    expect(wrapper.text()).toContain('5/12')
    expect(wrapper.get('[data-testid="egress-capacity-formula"]').text()).toBe('3 IP x 4 = 12')
    const breakdown = wrapper.get('[data-testid="egress-capacity-breakdown"]').text()
    expect(breakdown).toContain('Local1/4')
    expect(breakdown).toContain('RN-1042/4')
    expect(breakdown).toContain('RN-672/4')
  })

  it('falls back to legacy account concurrency when no egress summary is present', () => {
    const wrapper = mount(AccountCapacityCell, {
      props: {
        account: {
          id: 2,
          platform: 'openai',
          type: 'apikey',
          concurrency: 7,
          current_concurrency: 3
        } as any
      }
    })

    expect(wrapper.text()).toContain('3/7')
    expect(wrapper.find('[data-testid="egress-capacity-formula"]').exists()).toBe(false)
  })
})
