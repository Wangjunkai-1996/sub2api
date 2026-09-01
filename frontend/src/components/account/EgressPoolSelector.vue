<template>
  <div class="space-y-3" data-testid="egress-pool-selector">
    <div
      v-if="inherited"
      class="flex items-start gap-2 rounded-md border border-sky-200 bg-sky-50 px-3 py-2 text-sm text-sky-800 dark:border-sky-900/60 dark:bg-sky-950/30 dark:text-sky-200"
      data-testid="egress-inherited-notice"
    >
      <Icon name="link" size="sm" class="mt-0.5 shrink-0" />
      <div>
        <p class="font-medium">{{ t('admin.accounts.egressPool.inherited') }}</p>
        <p class="mt-0.5 text-xs opacity-80">{{ t('admin.accounts.egressPool.inheritedHint') }}</p>
      </div>
    </div>

    <div class="overflow-hidden rounded-md border border-gray-200 dark:border-dark-600">
      <div
        v-for="route in visibleRoutes"
        :key="route.id"
        class="border-b border-gray-100 px-3 py-2.5 last:border-b-0 dark:border-dark-700"
        :class="{
          'bg-gray-50/70 dark:bg-dark-800/40': !route.eligible,
          'opacity-70': !route.eligible && !isSelected(route.id)
        }"
        :data-testid="`egress-route-${route.id}`"
      >
        <div class="flex min-w-0 items-start gap-3">
          <input
            :id="`egress-route-${route.id}`"
            type="checkbox"
            class="mt-1 h-4 w-4 shrink-0 rounded border-gray-300 text-primary-600 focus:ring-primary-500"
            :checked="isSelected(route.id)"
            :disabled="disabled || (!route.eligible && !isSelected(route.id))"
            @change="toggleRoute(route.id, ($event.target as HTMLInputElement).checked)"
          />

          <label :for="`egress-route-${route.id}`" class="min-w-0 flex-1" :class="disabled ? 'cursor-default' : 'cursor-pointer'">
            <span class="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
              <span class="truncate text-sm font-medium text-gray-800 dark:text-gray-100">{{ routeLabel(route) }}</span>
              <span class="rounded px-1.5 py-0.5 text-[11px] font-medium" :class="stateClass(route)">
                {{ stateLabel(route.state) }}
              </span>
              <span
                v-if="primaryRouteId === route.id"
                class="inline-flex items-center gap-1 rounded bg-blue-50 px-1.5 py-0.5 text-[11px] font-medium text-blue-700 dark:bg-blue-900/30 dark:text-blue-300"
              >
                <Icon name="key" size="xs" />
                {{ t('admin.accounts.egressPool.primary') }}
              </span>
            </span>
            <span class="mt-1 flex flex-wrap gap-x-3 gap-y-0.5 text-xs text-gray-500 dark:text-gray-400">
              <span>{{ route.kind === 'direct' ? t('admin.accounts.egressPool.direct') : t('admin.accounts.egressPool.proxy') }}</span>
              <span v-if="routeIp(route)" class="font-mono">{{ routeIp(route) }}</span>
              <span v-if="route.country_code || route.country">{{ route.country_code || route.country }}</span>
              <span v-if="route.probe_latency_ms != null">{{ route.probe_latency_ms }}ms</span>
              <span v-if="routeReason(route) && !verifyErrors[route.id]" class="text-amber-700 dark:text-amber-300">{{ routeReason(route) }}</span>
            </span>
          </label>

          <button
            v-if="!disabled"
            type="button"
            class="inline-flex h-7 w-7 shrink-0 items-center justify-center rounded text-gray-500 hover:bg-gray-100 hover:text-primary-600 disabled:cursor-not-allowed disabled:opacity-50 dark:hover:bg-dark-700"
            :title="t('admin.accounts.egressPool.verify')"
            :aria-label="t('admin.accounts.egressPool.verify')"
            :disabled="verifyingRouteId !== null"
            :data-testid="`verify-egress-${route.id}`"
            @click="verifyRoute(route)"
          >
            <Icon name="refresh" size="sm" :class="verifyingRouteId === route.id ? 'animate-spin' : ''" />
          </button>
        </div>

        <label
          v-if="isSelected(route.id) && requirePrimary"
          class="mt-2 ml-7 inline-flex items-center gap-2 text-xs text-gray-600 dark:text-gray-300"
        >
          <input
            type="radio"
            name="egress-primary-route"
            class="h-3.5 w-3.5 border-gray-300 text-primary-600 focus:ring-primary-500"
            :checked="primaryRouteId === route.id"
            :disabled="disabled"
            :aria-label="t('admin.accounts.egressPool.selectPrimary')"
            @change="setPrimary(route.id)"
          />
          {{ t('admin.accounts.egressPool.selectPrimary') }}
        </label>

        <p v-if="verifyErrors[route.id]" class="mt-1 ml-7 text-xs text-red-600 dark:text-red-400">
          {{ verifyErrors[route.id] }}
        </p>
      </div>

      <div v-if="visibleRoutes.length === 0" class="px-3 py-6 text-center text-sm text-gray-500 dark:text-gray-400">
        {{ t('admin.accounts.egressPool.empty') }}
      </div>
    </div>

    <p v-if="selectedRouteIds.length === 0 && !disabled" class="text-xs text-amber-700 dark:text-amber-300">
      {{ t('admin.accounts.egressPool.noSelection') }}
    </p>
    <p v-if="requirePrimary && selectedRouteIds.length > 0" class="text-xs text-gray-500 dark:text-gray-400">
      {{ t('admin.accounts.egressPool.primaryHint') }}
    </p>
  </div>
</template>

<script setup lang="ts">
import { computed, reactive, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { adminAPI } from '@/api/admin'
import Icon from '@/components/icons/Icon.vue'
import type { AssignableEgressRoute, EgressRouteState } from '@/types'

const props = withDefaults(defineProps<{
  routes: AssignableEgressRoute[]
  selectedRoutes?: AssignableEgressRoute[]
  selectedRouteIds: number[]
  primaryRouteId: number | null
  requirePrimary?: boolean
  disabled?: boolean
  inherited?: boolean
}>(), {
  selectedRoutes: () => [],
  requirePrimary: true,
  disabled: false,
  inherited: false
})

const emit = defineEmits<{
  'update:selectedRouteIds': [ids: number[]]
  'update:primaryRouteId': [id: number | null]
  verified: [route: AssignableEgressRoute]
}>()

const { t } = useI18n()
const verifyingRouteId = ref<number | null>(null)
const verifiedRoutes = reactive<Record<number, AssignableEgressRoute>>({})
const verifyErrors = reactive<Record<number, string>>({})

const visibleRoutes = computed(() => {
  const byID = new Map<number, AssignableEgressRoute>()
  for (const route of [...props.routes, ...props.selectedRoutes]) {
    if (route.kind === 'proxy') byID.set(route.id, route)
  }
  for (const [id, route] of Object.entries(verifiedRoutes)) {
    if (route.kind === 'proxy') byID.set(Number(id), route)
  }
  return Array.from(byID.values()).sort((left, right) => {
    return routeLabel(left).localeCompare(routeLabel(right))
  })
})

const isSelected = (id: number) => props.selectedRouteIds.includes(id)

const toggleRoute = (id: number, selected: boolean) => {
  if (props.disabled) return
  const next = selected
    ? Array.from(new Set([...props.selectedRouteIds, id]))
    : props.selectedRouteIds.filter((routeID) => routeID !== id)
  emit('update:selectedRouteIds', next)
  if (selected && props.requirePrimary && props.primaryRouteId == null) {
    emit('update:primaryRouteId', id)
  } else if (!selected && props.primaryRouteId === id) {
    emit('update:primaryRouteId', next[0] ?? null)
  }
}

const setPrimary = (id: number) => {
  if (!props.disabled && isSelected(id)) emit('update:primaryRouteId', id)
}

const legacyRouteNumberPattern = /^route\s*#?\s*\d+$/i

const routeIp = (route: AssignableEgressRoute) => route.observed_ip || route.public_ip || route.ip_address || ''
const routeLabel = (route: AssignableEgressRoute) => {
  if (route.kind === 'direct') return t('admin.accounts.egressPool.direct')
  const names = [route.display_name, route.proxy_name, route.name]
  const name = names.find((value) => {
    const normalized = value?.trim() || ''
    return normalized !== '' && !legacyRouteNumberPattern.test(normalized)
  })?.trim()
  if (name) return name
  return t('admin.accounts.egressPool.unnamedProxy', { id: route.id })
}

const knownReasonCodes = new Set([
  'route_not_found',
  'route_unavailable',
  'probe_failed',
  'invalid_observation',
  'revision_conflict',
  'persistence_failed',
  'request_canceled',
  'pending_verification',
  'route_inactive',
  'route_expired',
  'identity_mismatch',
  'route_retired',
  'identity_unavailable',
  'direct_route_unavailable',
  'proxy_unavailable',
  'proxy_inactive',
  'proxy_expired',
  'route_kind_invalid'
])

const readableReason = (reasonCode?: string | null) => {
  const code = reasonCode?.trim() || ''
  if (!code) return ''
  if (knownReasonCodes.has(code)) return t(`admin.accounts.egressPool.reason.${code}`)
  return t('admin.accounts.egressPool.reason.unknown', { code })
}

const routeReason = (route: AssignableEgressRoute) => readableReason(route.probe_reason_code || route.reason_code)

const stateLabel = (state: EgressRouteState) => t(`admin.accounts.egressPool.state.${state}`)
const stateClass = (route: AssignableEgressRoute) => {
  if (route.eligible && route.state === 'active') {
    return 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'
  }
  if (route.state === 'pending_verification') {
    return 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
  }
  return 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300'
}

const verifyRoute = async (route: AssignableEgressRoute) => {
  if (verifyingRouteId.value !== null) return
  verifyingRouteId.value = route.id
  delete verifyErrors[route.id]
  try {
    const result = await adminAPI.egressRoutes.verify([route.id])
    const verified = result.find((item) => item.id === route.id)
    if (verified) {
      verifiedRoutes[route.id] = verified
      emit('verified', verified)
      const probeReason = readableReason(verified.probe_reason_code)
      if (verified.probe_success === false || probeReason) {
        verifyErrors[route.id] = probeReason || t('admin.accounts.egressPool.verifyFailed')
      } else {
        delete verifyErrors[route.id]
      }
    } else {
      verifyErrors[route.id] = t('admin.accounts.egressPool.verifyFailed')
    }
  } catch (error: unknown) {
    const apiError = error as {
      response?: { data?: { probe_reason_code?: string; message?: string; detail?: string } }
      message?: string
    }
    verifyErrors[route.id] = readableReason(apiError.response?.data?.probe_reason_code)
      || apiError.response?.data?.message
      || apiError.response?.data?.detail
      || apiError.message
      || t('admin.accounts.egressPool.verifyFailed')
  } finally {
    verifyingRouteId.value = null
  }
}
</script>
