import { beforeEach, describe, expect, it, vi } from 'vitest'

const { get, post } = vi.hoisted(() => ({
  get: vi.fn(),
  post: vi.fn()
}))

vi.mock('@/api/client', () => ({
  apiClient: { get, post }
}))

import trafficDirectorAPI from '@/api/admin/trafficDirector'

describe('traffic director admin API', () => {
  beforeEach(() => {
    get.mockReset()
    post.mockReset()
    get.mockResolvedValue({ data: {} })
    post.mockResolvedValue({ data: {} })
  })

  it('uses the versioned group endpoints and sends an explicit idempotency key', async () => {
    await trafficDirectorAPI.get(7)
    await trafficDirectorAPI.listVersions(7, { limit: 100 })
    await trafficDirectorAPI.getVersion(7, 3)
    await trafficDirectorAPI.getStatus(7)
    await trafficDirectorAPI.preview(7, {
      expected_version: 0,
      mode: 'shadow',
      spec: { schema_version: 1, health_mode: 'off', pools: [] }
    })
    await trafficDirectorAPI.publish(
      7,
      { expected_version: 0, mode: 'shadow', spec: { schema_version: 1, health_mode: 'off', pools: [] } },
      'td-publish-7'
    )
    await trafficDirectorAPI.rollback(7, 2, { expected_version: 3, note: 'restore' }, 'td-rollback-7')

    expect(get.mock.calls.map(([url]) => url)).toEqual([
      '/admin/groups/7/traffic-director',
      '/admin/groups/7/traffic-director/versions',
      '/admin/groups/7/traffic-director/versions/3',
      '/admin/groups/7/traffic-director/status'
    ])
    expect(get.mock.calls[1][1]).toEqual({ params: { limit: 100 } })
    expect(post.mock.calls[0]).toEqual([
      '/admin/groups/7/traffic-director/preview',
      { expected_version: 0, mode: 'shadow', spec: { schema_version: 1, health_mode: 'off', pools: [] } }
    ])
    expect(post.mock.calls[1][2]).toEqual({ headers: { 'Idempotency-Key': 'td-publish-7' } })
    expect(post.mock.calls[2]).toEqual([
      '/admin/groups/7/traffic-director/rollback/2',
      { expected_version: 3, note: 'restore' },
      { headers: { 'Idempotency-Key': 'td-rollback-7' } }
    ])
  })
})
