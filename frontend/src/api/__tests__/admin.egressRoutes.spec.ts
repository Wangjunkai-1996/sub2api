import { beforeEach, describe, expect, it, vi } from 'vitest'

const { get, post } = vi.hoisted(() => ({
  get: vi.fn(),
  post: vi.fn()
}))

vi.mock('@/api/client', () => ({
  apiClient: { get, post }
}))

import { confirmIdentity, getAssignable, getAssignableCatalog, verify } from '@/api/admin/egressRoutes'

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

  it('preserves the authoritative OAuth defaults from the catalog', async () => {
    const routes = [{ id: 7, kind: 'proxy', name: 'renamed-route', state: 'active', eligible: true }]
    get.mockResolvedValue({
      data: {
        items: routes,
        default_route_id: 7,
        default_concurrency: 3,
        capabilities: { mutation_enabled: true }
      }
    })

    await expect(getAssignableCatalog()).resolves.toEqual({
      items: routes,
      generation: undefined,
      default_route_id: 7,
      default_concurrency: 3,
      capabilities: { mutation_enabled: true, reason_code: undefined }
    })
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
