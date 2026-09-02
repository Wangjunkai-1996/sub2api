import { describe, expect, it, vi } from 'vitest'

const { getAssignableCatalogMock, verifyMock } = vi.hoisted(() => ({
  getAssignableCatalogMock: vi.fn(),
  verifyMock: vi.fn()
}))

vi.mock('@/api/admin', () => ({
  adminAPI: {
    egressRoutes: {
      getAssignableCatalog: getAssignableCatalogMock,
      verify: verifyMock
    }
  }
}))

import { mergeAccountEgressRoutes, useAccountEgressCatalog } from '../useAccountEgressCatalog'
import type { AssignableEgressRoute, AssignableEgressRouteCatalog } from '@/types'

const route = (id: number, name = `route-${id}`): AssignableEgressRoute => ({
  id,
  kind: 'proxy',
  name,
  state: 'active',
  eligible: true
})

const deferred = <T>() => {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, resolve, reject }
}

describe('useAccountEgressCatalog', () => {
  it('does not let a stale refresh overwrite the newest catalog', async () => {
    const first = deferred<AssignableEgressRouteCatalog>()
    const second = deferred<AssignableEgressRouteCatalog>()
    getAssignableCatalogMock
      .mockReset()
      .mockReturnValueOnce(first.promise)
      .mockReturnValueOnce(second.promise)
    const catalog = useAccountEgressCatalog()

    const firstRefresh = catalog.refresh()
    const secondRefresh = catalog.refresh()
    second.resolve({
      items: [route(2, 'new')],
      generation: 'new-generation',
      capabilities: { mutation_enabled: true }
    })
    await secondRefresh
    first.resolve({
      items: [route(1, 'stale')],
      generation: 'stale-generation',
      capabilities: { mutation_enabled: true }
    })
    await firstRefresh

    expect(catalog.routes.value.map((item) => item.id)).toEqual([2])
    expect(catalog.generation.value).toBe('new-generation')
    expect(catalog.capabilities.value.mutation_enabled).toBe(true)
  })

  it('replaces a verified route and clears its stale verification error', async () => {
    const original = route(4, 'before')
    const verified = { ...original, name: 'after', probe_latency_ms: 41, probe_success: true }
    getAssignableCatalogMock.mockReset().mockResolvedValue({
      items: [original],
      capabilities: { mutation_enabled: true }
    })
    verifyMock.mockReset().mockResolvedValue([verified])
    const catalog = useAccountEgressCatalog()
    await catalog.refresh()
    catalog.verifyErrors[4] = 'old_failure'

    const verification = catalog.verify(original)
    expect(catalog.verifyingRouteId.value).toBe(4)
    await verification

    expect(verifyMock).toHaveBeenCalledWith([4])
    expect(catalog.verifyingRouteId.value).toBeNull()
    expect(catalog.verifyErrors[4]).toBeUndefined()
    expect(catalog.routes.value).toEqual([verified])
  })

  it('records the current probe reason and disables mutation after a catalog failure', async () => {
    const original = route(6)
    getAssignableCatalogMock.mockReset().mockResolvedValueOnce({
      items: [original],
      capabilities: { mutation_enabled: true }
    })
    verifyMock.mockReset().mockResolvedValue([{ ...original, probe_success: false, probe_reason_code: 'probe_failed' }])
    const catalog = useAccountEgressCatalog()
    await catalog.refresh()
    await catalog.verify(original)
    expect(catalog.verifyErrors[6]).toBe('probe_failed')

    getAssignableCatalogMock.mockResolvedValueOnce({
      items: [{ ...original, probe_success: true }],
      capabilities: { mutation_enabled: true }
    })
    await catalog.refresh()
    expect(catalog.verifyErrors[6]).toBeUndefined()

    getAssignableCatalogMock.mockRejectedValueOnce(new Error('unavailable'))
    await expect(catalog.refresh()).rejects.toThrow('unavailable')
    expect(catalog.routes.value).toEqual([])
    expect(catalog.capabilities.value).toEqual({
      mutation_enabled: false,
      reason_code: 'catalog_unavailable'
    })
  })

  it('lets fresh catalog entries replace embedded copies without losing embedded-only routes', () => {
    expect(mergeAccountEgressRoutes(
      [route(1, 'fresh')],
      [route(1, 'embedded'), route(2, 'retired')]
    )).toEqual([route(1, 'fresh'), route(2, 'retired')])
  })
})
