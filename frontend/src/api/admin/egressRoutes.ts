/**
 * Redacted egress-route APIs used by account assignment UIs.
 */

import { apiClient } from '../client'
import type { AssignableEgressRoute, AssignableEgressRouteCatalog } from '@/types'

type AssignableRoutesResponse =
  | AssignableEgressRoute[]
  | {
      items?: AssignableEgressRoute[]
      routes?: AssignableEgressRoute[]
      generation?: string | number | null
      default_route_id?: number | null
      default_concurrency?: number | null
      capabilities?: { mutation_enabled?: boolean; reason_code?: string | null }
    }

function unwrapRoutes(data: AssignableRoutesResponse): AssignableEgressRoute[] {
  if (Array.isArray(data)) return data
  return data.items ?? data.routes ?? []
}

export async function getAssignable(): Promise<AssignableEgressRoute[]> {
  return (await getAssignableCatalog()).items
}

export async function getAssignableCatalog(): Promise<AssignableEgressRouteCatalog> {
  const { data } = await apiClient.get<AssignableRoutesResponse>('/admin/egress-routes/assignable')
  if (Array.isArray(data)) {
    return { items: data, capabilities: { mutation_enabled: true } }
  }
  return {
    items: unwrapRoutes(data),
    generation: data.generation,
    default_route_id: data.default_route_id,
    default_concurrency: data.default_concurrency,
    capabilities: {
      mutation_enabled: data.capabilities?.mutation_enabled === true,
      reason_code: data.capabilities?.reason_code
    }
  }
}

export async function verify(routeIds: number[]): Promise<AssignableEgressRoute[]> {
  const { data } = await apiClient.post<AssignableRoutesResponse>('/admin/egress-routes/verify', {
    route_ids: routeIds
  })
  return unwrapRoutes(data)
}

export async function confirmIdentity(
  id: number,
  confirmation: { route_revision: number; observed_ip: string }
): Promise<AssignableEgressRoute> {
  const { data } = await apiClient.post<AssignableEgressRoute>(
    `/admin/egress-routes/${id}/confirm-identity`,
    confirmation
  )
  return data
}

export const egressRoutesAPI = {
  getAssignable,
  getAssignableCatalog,
  verify,
  confirmIdentity
}

export default egressRoutesAPI
