import { describe, expect, it } from 'vitest'

import {
  AccountEgressPolicyError,
  buildCreateAccountEgressPatch,
  buildEditAccountEgressPatch,
  initialAccountEgressDraft
} from '../accountEgressPolicy'
import type { Account, AssignableEgressRoute } from '@/types'

const routes: AssignableEgressRoute[] = [
  { id: 1, kind: 'direct', name: 'local', state: 'active', eligible: true },
  { id: 2, kind: 'proxy', name: 'proxy-a', proxy_id: 20, state: 'active', eligible: true },
  { id: 3, kind: 'proxy', name: 'proxy-b', proxy_id: 30, state: 'active', eligible: true }
]

const account = (overrides: Partial<Account> = {}): Account => ({
  id: 7,
  name: 'openai-oauth',
  platform: 'openai',
  type: 'oauth',
  status: 'active',
  schedulable: true,
  concurrency: 2,
  priority: 1,
  rate_multiplier: 1,
  group_ids: [],
  credentials: {},
  extra: {},
  created_at: '',
  updated_at: '',
  ...overrides
} as Account)

describe('accountEgressPolicy', () => {
  it('normalizes create writes to explicit proxy routes only', () => {
    expect(buildCreateAccountEgressPatch({
      routeIds: [1, 2, 2],
      primaryRouteId: 1,
      concurrencyPerEgress: 0
    }, routes)).toEqual({
      egress_mode: 'pool',
      egress_pool: {
        route_ids: [2],
        primary_route_id: 2,
        concurrency_per_egress: 1
      }
    })
  })

  it('requires a positive revision for a touched edit', () => {
    expect(() => buildEditAccountEgressPatch(account(), true, {
      routeIds: [2],
      primaryRouteId: 2,
      concurrencyPerEgress: 2
    }, routes)).toThrowError(new AccountEgressPolicyError('revision_required'))
  })

  it('builds one versioned edit patch and omits untouched egress', () => {
    const current = account({ egress_revision: 9 })
    expect(buildEditAccountEgressPatch(current, false, {
      routeIds: [2],
      primaryRouteId: 2,
      concurrencyPerEgress: 4
    }, routes)).toEqual({})
    expect(buildEditAccountEgressPatch(current, true, {
      routeIds: [1, 2, 3],
      primaryRouteId: 3,
      concurrencyPerEgress: 4
    }, routes)).toEqual({
      egress_mode: 'pool',
      egress_pool: {
        route_ids: [2, 3],
        primary_route_id: 3,
        concurrency_per_egress: 4,
        revision: 9
      }
    })
  })

  it('maps a legacy proxy binding into the matching redacted route', () => {
    expect(initialAccountEgressDraft(account({ proxy_id: 30, concurrency: 5 }), routes)).toEqual({
      routeIds: [3],
      primaryRouteId: 3,
      concurrencyPerEgress: 5
    })
  })
})
