import { beforeEach, describe, expect, it, vi } from 'vitest'

const { get, post } = vi.hoisted(() => ({
  get: vi.fn(),
  post: vi.fn()
}))

vi.mock('@/api/client', () => ({
  apiClient: { get, post }
}))

import { confirmIdentity, getAssignable, verify } from '@/api/admin/egressRoutes'

describe('admin egress routes API', () => {
  beforeEach(() => {
    get.mockReset()
    post.mockReset()
  })

  it('loads the redacted assignable route projection', async () => {
    const routes = [{ id: 1, kind: 'direct', name: 'Local', state: 'active', eligible: true }]
    get.mockResolvedValue({ data: { items: routes } })

    await expect(getAssignable()).resolves.toEqual(routes)
    expect(get).toHaveBeenCalledWith('/admin/egress-routes/assignable')
  })

  it('verifies a bounded route id list', async () => {
    post.mockResolvedValue({ data: { routes: [] } })

    await verify([1, 2])

    expect(post).toHaveBeenCalledWith('/admin/egress-routes/verify', { route_ids: [1, 2] })
  })

  it('confirms identity with both route revision and observed IP for CAS safety', async () => {
    post.mockResolvedValue({ data: { id: 2 } })

    await confirmIdentity(2, { route_revision: 9, observed_ip: '198.51.100.2' })

    expect(post).toHaveBeenCalledWith('/admin/egress-routes/2/confirm-identity', {
      route_revision: 9,
      observed_ip: '198.51.100.2'
    })
  })
})
