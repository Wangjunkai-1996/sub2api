import type {
  Account,
  AccountEgressPoolWrite,
  AccountEgressPoolOperation,
  AssignableEgressRoute,
  BulkAccountEgressPoolWrite
} from '@/types'

export interface AccountEgressDraft {
  routeIds: number[]
  primaryRouteId: number | null
  concurrencyPerEgress: number
}

export class AccountEgressPolicyError extends Error {
  constructor(readonly code: 'no_selection' | 'revision_required') {
    super(code)
    this.name = 'AccountEgressPolicyError'
  }
}

export const accountSupportsEgressPool = (account: Pick<Account, 'platform' | 'type'>) =>
  account.platform === 'openai' && account.type === 'oauth'

export const accountHasEgressPool = (
  account: Pick<Account, 'platform' | 'type' | 'egress_mode' | 'egress_pool'>
) => accountSupportsEgressPool(account) && account.egress_mode === 'pool' && account.egress_pool != null

const positiveConcurrency = (value: number) => Math.max(1, Number(value) || 1)

export function proxyRouteIDs(routeIDs: number[], routes: AssignableEgressRoute[]): number[] {
  const proxyIDs = new Set(routes.filter((route) => route.kind === 'proxy').map((route) => route.id))
  return Array.from(new Set(routeIDs.filter((routeID) => proxyIDs.has(routeID))))
}

export function initialAccountEgressDraft(
  account: Account,
  routes: AssignableEgressRoute[]
): AccountEgressDraft {
  const pool = accountHasEgressPool(account) ? account.egress_pool : null
  const legacyRoute = !pool && account.proxy_id != null
    ? routes.find((route) => route.kind === 'proxy' && route.proxy_id === account.proxy_id)
    : undefined
  const routeIds = pool?.route_ids?.length
    ? [...pool.route_ids]
    : legacyRoute
      ? [legacyRoute.id]
      : []

  return {
    routeIds,
    primaryRouteId: pool?.primary_route_id ?? legacyRoute?.id ?? null,
    concurrencyPerEgress: positiveConcurrency(
      pool?.concurrency_per_egress
        ?? account.egress_summary?.concurrency_per_egress
        ?? account.concurrency
    )
  }
}

function normalizedPool(
  draft: AccountEgressDraft,
  routes: AssignableEgressRoute[],
  revision?: number
): AccountEgressPoolWrite {
  const routeIds = proxyRouteIDs(draft.routeIds, routes)
  if (routeIds.length === 0) throw new AccountEgressPolicyError('no_selection')
  const primaryRouteId = draft.primaryRouteId != null && routeIds.includes(draft.primaryRouteId)
    ? draft.primaryRouteId
    : routeIds[0]
  return {
    route_ids: routeIds,
    primary_route_id: primaryRouteId,
    concurrency_per_egress: positiveConcurrency(draft.concurrencyPerEgress),
    ...(revision == null ? {} : { revision })
  }
}

export function buildCreateAccountEgressPatch(
  draft: AccountEgressDraft,
  routes: AssignableEgressRoute[]
): { egress_mode: 'pool'; egress_pool: AccountEgressPoolWrite } {
  return { egress_mode: 'pool', egress_pool: normalizedPool(draft, routes) }
}

export function buildEditAccountEgressPatch(
  account: Account,
  touched: boolean,
  draft: AccountEgressDraft,
  routes: AssignableEgressRoute[]
): Record<string, never> | { egress_mode: 'pool'; egress_pool: AccountEgressPoolWrite } {
  if (!touched || !accountSupportsEgressPool(account) || account.parent_account_id != null) return {}
  const revision = account.egress_pool?.revision ?? account.egress_revision
  if (revision == null || revision <= 0) throw new AccountEgressPolicyError('revision_required')
  return { egress_mode: 'pool', egress_pool: normalizedPool(draft, routes, revision) }
}

export function buildBulkAccountEgressPatch(input: {
  operation: AccountEgressPoolOperation
  routeMutation: boolean
  routeIds: number[]
  primaryRouteId: number | null
  concurrencyPerEgress?: number
  routes: AssignableEgressRoute[]
}): { egress_mode: 'pool'; egress_pool: BulkAccountEgressPoolWrite } {
  const routeIds = input.routeMutation ? proxyRouteIDs(input.routeIds, input.routes) : []
  if (input.routeMutation && routeIds.length === 0) throw new AccountEgressPolicyError('no_selection')
  const pool: BulkAccountEgressPoolWrite = {
    operation: input.operation,
    route_ids: routeIds
  }
  if (input.routeMutation && input.operation === 'replace') {
    pool.primary_route_id = input.primaryRouteId != null && routeIds.includes(input.primaryRouteId)
      ? input.primaryRouteId
      : routeIds[0]
  }
  if (input.concurrencyPerEgress != null) {
    pool.concurrency_per_egress = positiveConcurrency(input.concurrencyPerEgress)
  }
  return { egress_mode: 'pool', egress_pool: pool }
}
