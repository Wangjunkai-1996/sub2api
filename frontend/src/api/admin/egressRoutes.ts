/**
 * Redacted egress-route APIs used by account assignment UIs.
 */

import { apiClient } from '../client'
import type { AssignableEgressRoute } from '@/types'

type AssignableRoutesResponse =
  | AssignableEgressRoute[]
  | { items?: AssignableEgressRoute[]; routes?: AssignableEgressRoute[] }

function unwrapRoutes(data: AssignableRoutesResponse): AssignableEgressRoute[] {
  if (Array.isArray(data)) return data
  return data.items ?? data.routes ?? []
}

export async function getAssignable(): Promise<AssignableEgressRoute[]> {
  const { data } = await apiClient.get<AssignableRoutesResponse>('/admin/egress-routes/assignable')
  return unwrapRoutes(data)
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
  verify,
  confirmIdentity
}

export default egressRoutesAPI
