import { reactive, ref } from 'vue'
import { adminAPI } from '@/api/admin'
import type {
  AccountEgressCatalogCapabilities,
  AssignableEgressRoute,
  AssignableEgressRouteCatalog
} from '@/types'

const disabledCapabilities = (): AccountEgressCatalogCapabilities => ({
  mutation_enabled: false,
  reason_code: 'catalog_unavailable'
})

export function mergeAccountEgressRoutes(
  catalogRoutes: AssignableEgressRoute[],
  embeddedRoutes: AssignableEgressRoute[] = []
): AssignableEgressRoute[] {
  const routes = new Map<number, AssignableEgressRoute>()
  for (const route of embeddedRoutes) routes.set(route.id, route)
  for (const route of catalogRoutes) routes.set(route.id, route)
  return Array.from(routes.values())
}

export function useAccountEgressCatalog() {
  const routes = ref<AssignableEgressRoute[]>([])
  const capabilities = ref<AccountEgressCatalogCapabilities>(disabledCapabilities())
  const generation = ref<string | number | null>(null)
  const loading = ref(false)
  const verifyingRouteId = ref<number | null>(null)
  const verifyErrors = reactive<Record<number, string>>({})
  let requestGeneration = 0

  const applyCatalog = (catalog: AssignableEgressRouteCatalog, requestID: number) => {
    if (requestID !== requestGeneration) return false
    for (const routeID of Object.keys(verifyErrors)) {
      delete verifyErrors[Number(routeID)]
    }
    routes.value = catalog.items
    capabilities.value = catalog.capabilities
    generation.value = catalog.generation ?? requestID
    return true
  }

  const refresh = async (): Promise<AssignableEgressRouteCatalog> => {
    const requestID = ++requestGeneration
    loading.value = true
    try {
      const catalog = await adminAPI.egressRoutes.getAssignableCatalog()
      applyCatalog(catalog, requestID)
      return catalog
    } catch (error) {
      if (requestID === requestGeneration) {
        routes.value = []
        capabilities.value = disabledCapabilities()
        generation.value = requestID
      }
      throw error
    } finally {
      if (requestID === requestGeneration) loading.value = false
    }
  }

  const verify = async (route: AssignableEgressRoute) => {
    if (verifyingRouteId.value != null) return
    verifyingRouteId.value = route.id
    delete verifyErrors[route.id]
    try {
      const results = await adminAPI.egressRoutes.verify([route.id])
      const verified = results.find((item) => item.id === route.id)
      if (!verified) {
        verifyErrors[route.id] = 'verify_failed'
        return
      }
      routes.value = mergeAccountEgressRoutes([verified], routes.value)
      if (verified.probe_success === false) {
        verifyErrors[route.id] = verified.probe_reason_code || 'verify_failed'
      }
    } catch (error: unknown) {
      const apiError = error as {
        response?: { data?: { probe_reason_code?: string; message?: string; detail?: string } }
        message?: string
      }
      verifyErrors[route.id] = apiError.response?.data?.probe_reason_code
        || apiError.response?.data?.message
        || apiError.response?.data?.detail
        || apiError.message
        || 'verify_failed'
    } finally {
      verifyingRouteId.value = null
    }
  }

  return {
    routes,
    capabilities,
    generation,
    loading,
    verifyingRouteId,
    verifyErrors,
    refresh,
    verify
  }
}
