<template>
  <AppLayout>
    <div class="space-y-6" data-testid="traffic-director-view">
      <header class="flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 class="text-2xl font-bold text-gray-900 dark:text-white">{{ t('admin.trafficDirector.title') }}</h1>
          <p class="mt-1 max-w-3xl text-sm text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.description') }}</p>
        </div>
        <button type="button" class="btn btn-secondary btn-sm" :disabled="loading || operationInFlight || !selectedGroupId" @click="refresh">
          <Icon name="refresh" size="sm" class="mr-1.5" />
          {{ t('common.refresh') }}
        </button>
      </header>

      <section class="card p-4" aria-labelledby="traffic-director-group-label">
        <div class="flex flex-wrap items-end gap-4">
          <div class="w-full min-w-0 flex-1 sm:max-w-md">
            <label id="traffic-director-group-label" class="input-label" for="traffic-director-group">
              {{ t('admin.trafficDirector.group') }}
            </label>
            <Select
              id="traffic-director-group"
              v-model="selectedGroupId"
              :options="groupOptions"
              :placeholder="t('admin.trafficDirector.selectGroup')"
              :searchable="'auto'"
              :disabled="groupsLoading || operationInFlight"
              :aria-label="t('admin.trafficDirector.group')"
              @change="handleGroupChange"
            />
          </div>
          <p v-if="groupsLoading" class="text-sm text-gray-500 dark:text-gray-400">{{ t('common.loading') }}</p>
          <p v-else-if="groups.length === 0" class="text-sm text-amber-600 dark:text-amber-400">{{ t('admin.trafficDirector.noOpenAIGroups') }}</p>
        </div>
      </section>

      <div v-if="errorMessage" class="rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700 dark:border-red-900/60 dark:bg-red-950/30 dark:text-red-300" role="alert">
        {{ errorMessage }}
      </div>

      <div v-if="loading" class="flex justify-center py-16"><LoadingSpinner size="md" /></div>

      <template v-else-if="state && selectedGroupId">
        <section class="grid grid-cols-2 gap-3 sm:grid-cols-4" :aria-label="t('admin.trafficDirector.status.summaryLabel')">
          <div class="card p-4">
            <p class="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.status.mode') }}</p>
            <p class="mt-1 text-lg font-semibold text-gray-900 dark:text-white" data-testid="traffic-director-mode">{{ modeLabel(state.head.mode) }}</p>
          </div>
          <div class="card p-4">
            <p class="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.status.version') }}</p>
            <p class="mt-1 text-lg font-semibold text-gray-900 dark:text-white">v{{ state.head.version }}</p>
          </div>
          <div class="card p-4">
            <p class="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.status.accounts') }}</p>
            <p class="mt-1 text-lg font-semibold text-gray-900 dark:text-white">{{ status?.assigned_account_count ?? assignedAccountIds.size }} / {{ state.accounts.length }}</p>
          </div>
          <div class="card p-4">
            <p class="text-xs font-medium uppercase tracking-wide text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.status.available') }}</p>
            <p class="mt-1 text-lg font-semibold text-gray-900 dark:text-white">{{ status?.available_account_count ?? schedulableAccountCount }}</p>
          </div>
        </section>

        <section v-if="status" class="card p-4" :aria-label="t('admin.trafficDirector.status.poolLabel')">
          <div class="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm">
            <span class="text-gray-600 dark:text-gray-300">{{ t('admin.trafficDirector.status.health') }}: <strong class="font-medium text-gray-900 dark:text-white">{{ status.health_mode }}</strong></span>
            <span class="text-gray-600 dark:text-gray-300">{{ t('admin.trafficDirector.status.checksum') }}: <code class="break-all font-mono text-xs text-gray-700 dark:text-gray-300">{{ status.checksum }}</code></span>
          </div>
          <div v-if="status.pools.length > 0" class="mt-3 flex flex-wrap gap-2">
            <span v-for="pool in status.pools" :key="pool.key" class="inline-flex items-center gap-1.5 rounded-md bg-gray-100 px-2.5 py-1.5 text-xs text-gray-700 dark:bg-dark-800 dark:text-gray-300">
              <strong class="font-medium">{{ pool.key }}</strong>
              <span>{{ pool.available_count }}/{{ pool.account_count }}</span>
              <span v-if="pool.fallback_pool_key" class="text-gray-500 dark:text-gray-400">→ {{ pool.fallback_pool_key }}</span>
            </span>
          </div>
        </section>

        <section class="card p-4 sm:p-6" aria-labelledby="traffic-director-editor-title">
          <div class="flex flex-wrap items-start justify-between gap-4 border-b border-gray-200 pb-4 dark:border-dark-700">
            <div>
              <h2 id="traffic-director-editor-title" class="text-base font-semibold text-gray-900 dark:text-white">{{ t('admin.trafficDirector.editor.title') }}</h2>
              <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.editor.hint') }}</p>
            </div>
            <div class="grid w-full grid-cols-3 rounded-lg border border-gray-200 p-1 dark:border-dark-600 sm:inline-flex sm:w-auto" role="group" :aria-label="t('admin.trafficDirector.editor.mode')">
              <button
                v-for="option in modeOptions"
                :key="option.value"
                type="button"
                class="min-w-0 rounded-md px-2 py-1.5 text-sm font-medium transition-colors sm:px-3"
                :class="draftMode === option.value ? 'bg-primary-600 text-white' : 'text-gray-600 hover:bg-gray-100 dark:text-gray-300 dark:hover:bg-dark-700'"
                :aria-pressed="draftMode === option.value"
                :data-testid="`traffic-director-mode-${option.value}`"
                :disabled="operationInFlight"
                @click="setMode(option.value)"
              >{{ option.label }}</button>
            </div>
          </div>

          <div v-if="draftMode === 'legacy'" class="mt-5 rounded-lg border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-800 dark:border-blue-900/60 dark:bg-blue-950/30 dark:text-blue-200">
            {{ t('admin.trafficDirector.editor.legacyHint') }}
          </div>

          <template v-else>
            <div class="mt-5 flex flex-wrap items-end gap-4">
              <div class="w-44">
                <label class="input-label" for="traffic-director-health-mode">{{ t('admin.trafficDirector.editor.healthMode') }}</label>
                <select id="traffic-director-health-mode" v-model="draftSpec.health_mode" class="input w-full" :disabled="operationInFlight">
                  <option v-for="option in healthModeOptions" :key="option.value" :value="option.value">{{ option.label }}</option>
                </select>
              </div>
              <p class="text-xs text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.editor.healthHint') }}</p>
            </div>

            <div class="mt-5 space-y-4">
              <div class="flex flex-wrap items-center justify-between gap-3">
                <h3 class="text-sm font-semibold text-gray-800 dark:text-gray-200">{{ t('admin.trafficDirector.editor.pools') }}</h3>
                <button type="button" class="btn btn-secondary btn-sm" :disabled="operationInFlight || draftSpec.pools.length >= 32" data-testid="traffic-director-add-pool" @click="addPool">
                  <Icon name="plus" size="sm" class="mr-1.5" />
                  {{ t('admin.trafficDirector.editor.addPool') }}
                </button>
              </div>

              <div v-if="draftSpec.pools.length === 0" class="rounded-lg border border-dashed border-gray-300 p-6 text-center text-sm text-gray-500 dark:border-dark-600 dark:text-gray-400">
                {{ t('admin.trafficDirector.editor.noPools') }}
              </div>

              <article v-for="(pool, poolIndex) in draftSpec.pools" :key="poolRenderKey(pool)" class="rounded-lg border border-gray-200 p-4 dark:border-dark-700">
                <div class="grid grid-cols-1 gap-3 md:grid-cols-[minmax(0,1fr)_8rem_8rem_minmax(0,1fr)_auto] md:items-end">
                  <div>
                    <label class="input-label" :for="`traffic-director-pool-key-${poolIndex}`">{{ t('admin.trafficDirector.editor.poolKey') }}</label>
                    <input :id="`traffic-director-pool-key-${poolIndex}`" v-model.trim="pool.key" class="input w-full" maxlength="64" :disabled="operationInFlight" data-testid="traffic-director-pool-key" @focus="beginPoolRename(pool)" @change="finishPoolRename(pool)" />
                  </div>
                  <div>
                    <label class="input-label" :for="`traffic-director-pool-weight-${poolIndex}`">{{ t('admin.trafficDirector.editor.weight') }}</label>
                    <input :id="`traffic-director-pool-weight-${poolIndex}`" v-model.number="pool.weight_bps" type="number" min="0" max="10000" step="1" class="input w-full" :disabled="operationInFlight" />
                  </div>
                  <div>
                    <label class="input-label" :for="`traffic-director-pool-min-${poolIndex}`">{{ t('admin.trafficDirector.editor.minAvailable') }}</label>
                    <input :id="`traffic-director-pool-min-${poolIndex}`" v-model.number="pool.min_available" type="number" min="1" :max="Math.max(1, pool.account_ids.length)" step="1" class="input w-full" :disabled="operationInFlight" />
                  </div>
                  <div>
                    <label class="input-label" :for="`traffic-director-pool-fallback-${poolIndex}`">{{ t('admin.trafficDirector.editor.fallback') }}</label>
                    <select :id="`traffic-director-pool-fallback-${poolIndex}`" v-model="pool.fallback_pool_key" class="input w-full" :disabled="operationInFlight">
                      <option value="">{{ t('admin.trafficDirector.editor.noFallback') }}</option>
                      <option v-for="(candidate, candidateIndex) in fallbackOptions(poolIndex)" :key="`${candidate.key}-${candidateIndex}`" :value="candidate.key">{{ candidate.key }}</option>
                    </select>
                  </div>
                  <button v-if="draftSpec.pools.length > 1" type="button" class="btn btn-ghost btn-sm text-red-600 hover:bg-red-50 dark:text-red-400 dark:hover:bg-red-950/30" :title="t('admin.trafficDirector.editor.removePool')" :aria-label="t('admin.trafficDirector.editor.removePool')" :disabled="operationInFlight" @click="removePool(poolIndex)">
                    <Icon name="trash" size="sm" />
                  </button>
                </div>

                <div class="mt-4 border-t border-gray-100 pt-3 dark:border-dark-800">
                  <div class="mb-2 flex flex-wrap items-center justify-between gap-2">
                    <span class="text-xs font-semibold uppercase tracking-wide text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.editor.accounts') }}</span>
                    <span class="text-xs text-gray-500 dark:text-gray-400">{{ pool.account_ids.length }} {{ t('admin.trafficDirector.editor.selected') }}</span>
                  </div>
                  <div class="grid grid-cols-1 gap-2 sm:grid-cols-2 lg:grid-cols-3">
                    <label v-for="account in state.accounts" :key="account.id" class="flex min-w-0 items-center gap-2 rounded-md px-2 py-1.5 text-sm" :class="accountDisabled(account.id, poolIndex) ? 'cursor-not-allowed opacity-45' : 'cursor-pointer hover:bg-gray-50 dark:hover:bg-dark-800'">
                      <input type="checkbox" :checked="pool.account_ids.includes(account.id)" :disabled="accountDisabled(account.id, poolIndex)" data-testid="traffic-director-account" @change="toggleAccount(poolIndex, account.id, ($event.target as HTMLInputElement).checked)" />
                      <span class="min-w-0 truncate text-gray-700 dark:text-gray-300" :title="account.name">{{ account.name }}</span>
                      <span class="ml-auto shrink-0 text-[11px]" :class="isAccountReady(account) ? 'text-emerald-600 dark:text-emerald-400' : 'text-gray-400'">{{ isAccountReady(account) ? t('admin.trafficDirector.editor.ready') : t('admin.trafficDirector.editor.unavailable') }}</span>
                    </label>
                  </div>
                </div>
              </article>
            </div>

            <div class="mt-4 flex flex-wrap items-center gap-3 text-sm" :class="weightTotal === 10000 ? 'text-emerald-600 dark:text-emerald-400' : 'text-amber-600 dark:text-amber-400'">
              <span>{{ t('admin.trafficDirector.editor.weightTotal', { total: weightTotal }) }}</span>
              <span v-if="unassignedAccountIds.length > 0">{{ t('admin.trafficDirector.editor.unassigned', { count: unassignedAccountIds.length }) }}</span>
            </div>
            <div v-if="unassignedAccountIds.length > 0" class="mt-2 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-800 dark:border-amber-900/60 dark:bg-amber-950/30 dark:text-amber-200">
              {{ t('admin.trafficDirector.editor.unassignedHint') }}: {{ unassignedAccountIds.join(', ') }}
            </div>
          </template>

          <div class="mt-6 flex flex-wrap items-end gap-3 border-t border-gray-200 pt-4 dark:border-dark-700">
            <div class="w-full min-w-0 flex-1 sm:max-w-xl">
              <label class="input-label" for="traffic-director-note">{{ t('admin.trafficDirector.editor.note') }}</label>
              <input id="traffic-director-note" v-model.trim="note" class="input w-full" maxlength="2000" :placeholder="t('admin.trafficDirector.editor.notePlaceholder')" :disabled="operationInFlight" />
            </div>
            <label v-if="draftMode === 'enforced' && unassignedAccountIds.length > 0" class="mb-2 inline-flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
              <input v-model="confirmUnassigned" type="checkbox" data-testid="traffic-director-confirm-unassigned" :disabled="operationInFlight" />
              <span>{{ t('admin.trafficDirector.editor.confirmUnassigned') }}</span>
            </label>
            <div class="flex flex-wrap gap-2">
              <button type="button" class="btn btn-secondary" :disabled="!canPreview" data-testid="traffic-director-preview" @click="runPreview">
                <Icon name="search" size="sm" class="mr-1.5" />
                {{ previewing ? t('common.loading') : t('admin.trafficDirector.actions.preview') }}
              </button>
              <button type="button" class="btn btn-primary" :disabled="!canPublish" data-testid="traffic-director-publish" @click="publishPolicy">
                <Icon name="checkCircle" size="sm" class="mr-1.5" />
                {{ publishing ? t('common.loading') : t('admin.trafficDirector.actions.publish') }}
              </button>
            </div>
          </div>
        </section>

        <section v-if="previewResult" class="rounded-lg border border-primary-200 bg-primary-50 p-4 dark:border-primary-900/60 dark:bg-primary-950/20" data-testid="traffic-director-preview-result">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <h2 class="text-sm font-semibold text-primary-900 dark:text-primary-100">{{ t('admin.trafficDirector.preview.title') }}</h2>
            <span class="break-all font-mono text-xs text-primary-700 dark:text-primary-300">{{ previewResult.checksum }}</span>
          </div>
          <p class="mt-2 text-sm text-primary-800 dark:text-primary-200">{{ t('admin.trafficDirector.preview.unassigned', { count: previewResult.unassigned_account_ids.length }) }}</p>
          <pre class="mt-3 max-h-56 overflow-auto rounded bg-white/70 p-3 text-xs text-gray-700 dark:bg-dark-900/60 dark:text-gray-300">{{ JSON.stringify(previewResult.normalized_spec ?? null, null, 2) }}</pre>
        </section>

        <section class="card p-4 sm:p-6" aria-labelledby="traffic-director-history-title">
          <div class="flex flex-wrap items-center justify-between gap-3">
            <div>
              <h2 id="traffic-director-history-title" class="text-base font-semibold text-gray-900 dark:text-white">{{ t('admin.trafficDirector.history.title') }}</h2>
              <p class="mt-1 text-sm text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.history.hint') }}</p>
            </div>
            <div class="flex w-full flex-wrap items-center justify-end gap-2 sm:w-auto">
              <button
                type="button"
                class="btn btn-secondary btn-sm"
                :disabled="operationInFlight || state.head.mode === 'legacy'"
                data-testid="traffic-director-rollback-legacy"
                @click="openLegacyRollback"
              >
                <Icon name="sync" size="sm" class="mr-1.5" />
                {{ t('admin.trafficDirector.history.rollbackLegacy') }}
              </button>
              <button type="button" class="btn btn-ghost btn-sm" :disabled="versionsLoading || operationInFlight" :title="t('admin.trafficDirector.history.refresh')" :aria-label="t('admin.trafficDirector.history.refresh')" data-testid="traffic-director-history-refresh" @click="loadVersions()"><Icon name="refresh" size="sm" /></button>
            </div>
          </div>
          <div class="mt-4 overflow-x-auto">
            <table class="min-w-full text-left text-sm">
              <thead class="border-b border-gray-200 text-xs uppercase tracking-wide text-gray-500 dark:border-dark-700 dark:text-gray-400">
                <tr>
                  <th scope="col" class="px-3 py-2">{{ t('admin.trafficDirector.history.version') }}</th>
                  <th scope="col" class="px-3 py-2">{{ t('admin.trafficDirector.history.mode') }}</th>
                  <th scope="col" class="px-3 py-2">{{ t('admin.trafficDirector.history.createdAt') }}</th>
                  <th scope="col" class="px-3 py-2">{{ t('admin.trafficDirector.history.note') }}</th>
                  <th scope="col" class="px-3 py-2 text-right">{{ t('admin.trafficDirector.history.actions') }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="version in versions" :key="version.version" class="border-b border-gray-100 last:border-0 dark:border-dark-800">
                  <td class="px-3 py-3 font-mono text-gray-800 dark:text-gray-200">v{{ version.version }}</td>
                  <td class="px-3 py-3"><span class="rounded-full bg-gray-100 px-2 py-1 text-xs dark:bg-dark-700">{{ modeLabel(version.mode) }}</span></td>
                  <td class="px-3 py-3 whitespace-nowrap text-gray-500 dark:text-gray-400">{{ formatDate(version.created_at) }}</td>
                  <td class="max-w-xs truncate px-3 py-3 text-gray-600 dark:text-gray-300" :title="version.note">{{ version.note || '-' }}</td>
                  <td class="px-3 py-3 text-right">
                    <button type="button" class="btn btn-ghost btn-sm" :disabled="version.version === state.head.version || operationInFlight" :title="t('admin.trafficDirector.history.rollback')" :aria-label="t('admin.trafficDirector.history.rollback')" @click="openRollback(version)">
                      <Icon name="sync" size="sm" />
                    </button>
                  </td>
                </tr>
                <tr v-if="versions.length === 0"><td colspan="5" class="px-3 py-8 text-center text-sm text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.history.empty') }}</td></tr>
              </tbody>
            </table>
          </div>
          <div v-if="versionsNextOffset < versionsTotal" class="mt-4 flex justify-center">
            <button
              type="button"
              class="btn btn-secondary btn-sm"
              :disabled="versionsLoading || operationInFlight"
              data-testid="traffic-director-history-load-more"
              @click="loadMoreVersions"
            >
              <Icon name="chevronDown" size="sm" class="mr-1.5" />
              {{ versionsLoading
                ? t('common.loading')
                : t('admin.trafficDirector.history.loadMore', { remaining: versionsTotal - versionsNextOffset }) }}
            </button>
          </div>
        </section>
      </template>

      <EmptyState v-else-if="!loading" :title="t('admin.trafficDirector.selectGroup')" :description="t('admin.trafficDirector.selectGroupHint')" />
    </div>

    <BaseDialog
      :show="Boolean(rollbackVersion)"
      :title="t('admin.trafficDirector.history.rollback')"
      width="wide"
      :close-on-escape="!operationInFlight"
      :show-close-button="!operationInFlight"
      @close="closeRollback"
    >
      <p class="text-sm text-gray-600 dark:text-gray-300">{{ t('admin.trafficDirector.history.rollbackConfirm', { version: rollbackVersion?.version ?? 0 }) }}</p>
      <div v-if="rollbackDetailsLoading" class="flex min-h-32 items-center justify-center gap-3 text-sm text-gray-500 dark:text-gray-400" role="status" aria-live="polite" data-testid="traffic-director-rollback-details-loading">
        <LoadingSpinner size="sm" />
        <span>{{ t('admin.trafficDirector.history.loadingDetails') }}</span>
      </div>
      <div v-else-if="rollbackDetailsError" class="mt-4 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700 dark:border-red-900/60 dark:bg-red-950/30 dark:text-red-300" role="alert">
        {{ rollbackDetailsError }}
      </div>
      <div v-else-if="rollbackDetails" class="mt-4 space-y-4" data-testid="traffic-director-rollback-details">
        <dl class="grid grid-cols-1 gap-3 sm:grid-cols-2">
          <div class="min-w-0">
            <dt class="text-xs font-medium uppercase text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.history.targetMode') }}</dt>
            <dd class="mt-1 text-sm font-medium text-gray-900 dark:text-white">{{ modeLabel(rollbackDetails.mode) }}</dd>
          </div>
          <div class="min-w-0">
            <dt class="text-xs font-medium uppercase text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.history.targetChecksum') }}</dt>
            <dd class="mt-1 break-all font-mono text-xs text-gray-700 dark:text-gray-300">{{ rollbackDetails.checksum }}</dd>
          </div>
        </dl>
        <div>
          <p class="text-xs font-medium uppercase text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.history.targetUnassigned') }}</p>
          <p class="mt-1 break-all font-mono text-xs text-gray-700 dark:text-gray-300" data-testid="traffic-director-rollback-unassigned">
            {{ rollbackUnassignedAccountIds.length > 0 ? rollbackUnassignedAccountIds.join(', ') : t('admin.trafficDirector.history.none') }}
          </p>
        </div>
        <div>
          <p class="text-xs font-medium uppercase text-gray-500 dark:text-gray-400">{{ t('admin.trafficDirector.history.targetSpec') }}</p>
          <pre class="mt-2 max-h-72 overflow-auto rounded-md bg-gray-50 p-3 text-xs text-gray-700 dark:bg-dark-800 dark:text-gray-300" data-testid="traffic-director-rollback-spec">{{ JSON.stringify(rollbackDetails.spec ?? null, null, 2) }}</pre>
        </div>
      </div>
      <div class="mt-4">
        <label class="input-label" for="traffic-director-rollback-note">{{ t('admin.trafficDirector.editor.note') }}</label>
        <input id="traffic-director-rollback-note" v-model.trim="rollbackNote" class="input w-full" maxlength="2000" :disabled="operationInFlight || rollbackDetailsLoading || Boolean(rollbackDetailsError)" />
      </div>
      <label v-if="rollbackRequiresUnassignedConfirmation" class="mt-4 flex items-start gap-2 text-sm text-gray-600 dark:text-gray-300">
        <input v-model="rollbackConfirmUnassigned" type="checkbox" class="mt-0.5" :disabled="operationInFlight" data-testid="traffic-director-rollback-confirm-unassigned" />
        <span>{{ t('admin.trafficDirector.history.confirmUnassigned') }}</span>
      </label>
      <template #footer>
        <button type="button" class="btn btn-secondary" :disabled="operationInFlight" data-testid="traffic-director-rollback-cancel" @click="closeRollback">{{ t('common.cancel') }}</button>
        <button type="button" class="btn btn-primary" :disabled="!canSubmitRollback" data-testid="traffic-director-rollback-submit" @click="rollbackPolicy">{{ rollingBack ? t('common.loading') : t('admin.trafficDirector.history.rollback') }}</button>
      </template>
    </BaseDialog>
  </AppLayout>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import AppLayout from '@/components/layout/AppLayout.vue'
import BaseDialog from '@/components/common/BaseDialog.vue'
import EmptyState from '@/components/common/EmptyState.vue'
import Icon from '@/components/icons/Icon.vue'
import LoadingSpinner from '@/components/common/LoadingSpinner.vue'
import Select from '@/components/common/Select.vue'
import { useAppStore } from '@/stores/app'
import { adminAPI } from '@/api/admin'
import { createTrafficDirectorOperationKey } from '@/api/admin/trafficDirector'
import type { AdminGroup } from '@/types'
import type {
  TrafficDirectorAccount,
  TrafficDirectorGroupState,
  TrafficDirectorMode,
  TrafficDirectorPool,
  TrafficDirectorPreview,
  TrafficDirectorSpec,
  TrafficDirectorStatus,
  TrafficDirectorVersion,
  TrafficDirectorVersionSummary
} from '@/api/admin/trafficDirector'
import { extractApiErrorCode, extractI18nErrorMessage } from '@/utils/apiError'

const { t } = useI18n()
const appStore = useAppStore()

const groups = ref<AdminGroup[]>([])
const selectedGroupId = ref<number | null>(null)
const state = ref<TrafficDirectorGroupState | null>(null)
const status = ref<TrafficDirectorStatus | null>(null)
const versions = ref<TrafficDirectorVersionSummary[]>([])
const versionsTotal = ref(0)
const versionsNextOffset = ref(0)
const draftMode = ref<TrafficDirectorMode>('legacy')
const draftSpec = ref<TrafficDirectorSpec>(emptySpec())
const note = ref('')
const confirmUnassigned = ref(false)
const previewResult = ref<TrafficDirectorPreview | null>(null)
const rollbackVersion = ref<TrafficDirectorVersionSummary | null>(null)
const rollbackDetails = ref<TrafficDirectorVersion | null>(null)
const rollbackDetailsLoading = ref(false)
const rollbackDetailsError = ref('')
const rollbackNote = ref('')
const rollbackConfirmUnassigned = ref(false)
const publishOperation = ref<{ key: string; fingerprint: string } | null>(null)
const rollbackOperation = ref<{ key: string; fingerprint: string } | null>(null)
const groupsLoading = ref(false)
const loading = ref(false)
const versionsLoading = ref(false)
const previewing = ref(false)
const publishing = ref(false)
const rollingBack = ref(false)
const errorMessage = ref('')
let groupContextRevision = 0
let versionsRequestRevision = 0
let previewRequestRevision = 0
let publishRequestRevision = 0
let rollbackRequestRevision = 0
let rollbackDetailsRequestRevision = 0
const poolKeyBeforeEdit = new WeakMap<TrafficDirectorPool, string>()
const poolRenderKeys = new WeakMap<TrafficDirectorPool, number>()
let nextPoolRenderKey = 1

const modeOptions = computed(() => [
  { value: 'legacy' as const, label: t('admin.trafficDirector.modes.legacy') },
  { value: 'shadow' as const, label: t('admin.trafficDirector.modes.shadow') },
  { value: 'enforced' as const, label: t('admin.trafficDirector.modes.enforced') }
])
const healthModeOptions = computed(() => [
  { value: 'off' as const, label: t('admin.trafficDirector.healthModes.off') },
  { value: 'observe' as const, label: t('admin.trafficDirector.healthModes.observe') },
  { value: 'enforce' as const, label: t('admin.trafficDirector.healthModes.enforce') }
])
const groupOptions = computed(() => groups.value.map((group) => ({
  value: group.id,
  label: group.status === 'inactive' ? `${group.name} (${t('common.inactive')})` : group.name
})))
const weightTotal = computed(() => draftSpec.value.pools.reduce((sum, pool) => sum + (Number(pool.weight_bps) || 0), 0))
const assignedAccountIds = computed(() => {
  const ids = new Set<number>()
  for (const pool of draftSpec.value.pools) {
    for (const accountId of pool.account_ids) ids.add(accountId)
  }
  return ids
})
const unassignedAccountIds = computed(() => state.value?.accounts.filter((account) => !assignedAccountIds.value.has(account.id)).map((account) => account.id) ?? [])
const schedulableAccountCount = computed(() => state.value?.accounts.filter(isAccountReady).length ?? 0)
const operationInFlight = computed(() => previewing.value || publishing.value || rollingBack.value)
const canPreview = computed(() => {
  if (!state.value || operationInFlight.value) return false
  if (draftMode.value === 'legacy') return true
  return draftSpec.value.pools.length > 0 && weightTotal.value === 10000
})
const canPublish = computed(() => canPreview.value && (draftMode.value !== 'enforced' || unassignedAccountIds.value.length === 0 || confirmUnassigned.value))
const rollbackUnassignedAccountIds = computed(() => {
  const details = rollbackDetails.value
  const currentState = state.value
  if (!details || !currentState || details.mode === 'legacy' || !details.spec) return []
  const assigned = new Set(details.spec.pools.flatMap((pool) => pool.account_ids))
  return currentState.accounts.filter((account) => !assigned.has(account.id)).map((account) => account.id)
})
const rollbackRequiresUnassignedConfirmation = computed(() => rollbackDetails.value?.mode === 'enforced'
  && rollbackUnassignedAccountIds.value.length > 0)
const canSubmitRollback = computed(() => Boolean(rollbackVersion.value)
  && Boolean(rollbackDetails.value)
  && rollbackDetails.value?.group_id === rollbackVersion.value?.group_id
  && rollbackDetails.value?.version === rollbackVersion.value?.version
  && !rollbackDetailsLoading.value
  && !rollbackDetailsError.value
  && !operationInFlight.value
  && (!rollbackRequiresUnassignedConfirmation.value || rollbackConfirmUnassigned.value))

watch([draftMode, draftSpec], () => {
  previewResult.value = null
  confirmUnassigned.value = false
}, { deep: true })

function emptySpec(): TrafficDirectorSpec {
  return { schema_version: 1, health_mode: 'off', pools: [] }
}

function cloneSpec(spec: TrafficDirectorSpec | null | undefined): TrafficDirectorSpec {
  if (!spec) return emptySpec()
  return {
    schema_version: 1,
    health_mode: spec.health_mode,
    pools: spec.pools.map((pool) => ({
      key: pool.key,
      weight_bps: pool.weight_bps,
      account_ids: [...pool.account_ids],
      min_available: pool.min_available,
      fallback_pool_key: pool.fallback_pool_key ?? ''
    }))
  }
}

function defaultSpec(accounts: TrafficDirectorAccount[]): TrafficDirectorSpec {
  return {
    schema_version: 1,
    health_mode: 'off',
    pools: [{ key: 'stable', weight_bps: 10000, account_ids: accounts.map((account) => account.id), min_available: Math.min(1, accounts.length || 1), fallback_pool_key: '' }]
  }
}

function modeLabel(mode: TrafficDirectorMode): string {
  return modeOptions.value.find((option) => option.value === mode)?.label ?? mode
}

function isAccountReady(account: TrafficDirectorAccount): boolean {
  return account.status === 'active' && account.schedulable
}

function formatDate(value: string): string {
  if (!value) return '-'
  const date = new Date(value)
  return Number.isNaN(date.getTime()) ? value : date.toLocaleString()
}

function setMode(mode: TrafficDirectorMode): void {
  if (operationInFlight.value) return
  draftMode.value = mode
  if (mode !== 'legacy' && draftSpec.value.pools.length === 0 && state.value) {
    draftSpec.value = defaultSpec(state.value.accounts)
  }
  previewResult.value = null
}

function handleGroupChange(value: string | number | boolean | null): void {
  if (typeof value === 'number' && Number.isFinite(value)) void loadGroup(value)
  else if (typeof value === 'string' && value.trim() !== '') void loadGroup(Number(value))
}

function fallbackOptions(poolIndex: number): TrafficDirectorPool[] {
  return draftSpec.value.pools.filter((_pool, index) => index !== poolIndex)
}

function poolRenderKey(pool: TrafficDirectorPool): number {
  let key = poolRenderKeys.get(pool)
  if (key === undefined) {
    key = nextPoolRenderKey++
    poolRenderKeys.set(pool, key)
  }
  return key
}

function beginPoolRename(pool: TrafficDirectorPool): void {
  poolKeyBeforeEdit.set(pool, pool.key)
}

function finishPoolRename(pool: TrafficDirectorPool): void {
  if (operationInFlight.value) return
  const previousKey = poolKeyBeforeEdit.get(pool)
  poolKeyBeforeEdit.delete(pool)
  if (!previousKey || previousKey === pool.key) return
  for (const candidate of draftSpec.value.pools) {
    if (candidate.fallback_pool_key === previousKey) candidate.fallback_pool_key = pool.key
  }
}

function addPool(): void {
  if (operationInFlight.value) return
  const index = draftSpec.value.pools.length + 1
  draftSpec.value.pools.push({ key: `pool-${index}`, weight_bps: 0, account_ids: [], min_available: 1, fallback_pool_key: '' })
}

function removePool(index: number): void {
  if (operationInFlight.value) return
  const removed = draftSpec.value.pools[index]?.key
  draftSpec.value.pools.splice(index, 1)
  if (removed) {
    for (const pool of draftSpec.value.pools) {
      if (pool.fallback_pool_key === removed) pool.fallback_pool_key = ''
    }
  }
}

function accountDisabled(accountId: number, poolIndex: number): boolean {
  if (operationInFlight.value) return true
  const pool = draftSpec.value.pools[poolIndex]
  if (pool?.account_ids.includes(accountId)) return false
  return draftSpec.value.pools.some((candidate, index) => index !== poolIndex && candidate.account_ids.includes(accountId))
}

function toggleAccount(poolIndex: number, accountId: number, checked: boolean): void {
  if (operationInFlight.value) return
  const pool = draftSpec.value.pools[poolIndex]
  if (!pool) return
  if (checked && !pool.account_ids.includes(accountId)) pool.account_ids.push(accountId)
  if (!checked) pool.account_ids = pool.account_ids.filter((id) => id !== accountId)
  if (pool.min_available > pool.account_ids.length) pool.min_available = Math.max(1, pool.account_ids.length)
  previewResult.value = null
}

interface GroupOperationContext {
  groupId: number
  expectedVersion: number
  revision: number
}

function captureOperationContext(): GroupOperationContext | null {
  const groupId = selectedGroupId.value
  const currentState = state.value
  if (!groupId || !currentState || currentState.group_id !== groupId) return null
  return {
    groupId,
    expectedVersion: currentState.head.version,
    revision: groupContextRevision
  }
}

function isCurrentGroupContext(groupId: number, revision: number): boolean {
  return selectedGroupId.value === groupId && groupContextRevision === revision
}

function isCurrentOperationContext(context: GroupOperationContext): boolean {
  return isCurrentGroupContext(context.groupId, context.revision)
    && state.value?.group_id === context.groupId
    && state.value.head.version === context.expectedVersion
}

function closeRollback(): void {
  if (operationInFlight.value) return
  resetRollback()
}

function resetRollback(): void {
  ++rollbackDetailsRequestRevision
  rollbackVersion.value = null
  rollbackDetails.value = null
  rollbackDetailsLoading.value = false
  rollbackDetailsError.value = ''
  rollbackNote.value = ''
  rollbackConfirmUnassigned.value = false
}

function trafficDirectorError(error: unknown, fallbackKey: string): string {
  return extractI18nErrorMessage(
    error,
    t,
    'admin.trafficDirector.errors',
    t(fallbackKey)
  )
}

async function loadGroups(): Promise<void> {
  groupsLoading.value = true
  try {
    const allGroups = await adminAPI.groups.getAllIncludingInactive()
    groups.value = allGroups.filter((group) => group.platform === 'openai')
    if (!selectedGroupId.value && groups.value.length > 0) selectedGroupId.value = groups.value[0].id
    if (selectedGroupId.value) await loadGroup(selectedGroupId.value)
  } catch (error) {
    errorMessage.value = trafficDirectorError(error, 'admin.trafficDirector.errors.loadGroups')
  } finally {
    groupsLoading.value = false
  }
}

async function loadGroup(groupId: number | null = selectedGroupId.value): Promise<boolean> {
  if (!groupId) return false
  const normalizedGroupId = Number(groupId)
  const contextRevision = ++groupContextRevision
  ++versionsRequestRevision
  selectedGroupId.value = normalizedGroupId
  loading.value = true
  errorMessage.value = ''
  previewResult.value = null
  resetRollback()
  versions.value = []
  versionsTotal.value = 0
  versionsNextOffset.value = 0
  versionsLoading.value = false
  publishOperation.value = null
  rollbackOperation.value = null
  try {
    const [nextState, nextStatus] = await Promise.all([
      adminAPI.trafficDirector.get(normalizedGroupId),
      adminAPI.trafficDirector.getStatus(normalizedGroupId)
    ])
    if (!isCurrentGroupContext(normalizedGroupId, contextRevision)) return false
    state.value = nextState
    status.value = nextStatus.group_id === nextState.group_id && nextStatus.version === nextState.head.version
      ? nextStatus
      : null
    draftMode.value = nextState.head.mode
    draftSpec.value = cloneSpec(nextState.head.spec)
    if (draftMode.value !== 'legacy' && draftSpec.value.pools.length === 0) draftSpec.value = defaultSpec(nextState.accounts)
    confirmUnassigned.value = false
    note.value = ''
    return await loadVersions(normalizedGroupId, contextRevision)
  } catch (error) {
    if (!isCurrentGroupContext(normalizedGroupId, contextRevision)) return false
    state.value = null
    status.value = null
    errorMessage.value = trafficDirectorError(error, 'admin.trafficDirector.errors.loadGroup')
    return false
  } finally {
    if (isCurrentGroupContext(normalizedGroupId, contextRevision)) loading.value = false
  }
}

async function loadVersions(
  groupId: number | null = selectedGroupId.value,
  contextRevision = groupContextRevision,
  append = false
): Promise<boolean> {
  if (!groupId) return false
  const requestRevision = ++versionsRequestRevision
  versionsLoading.value = true
  try {
    const offset = append ? versionsNextOffset.value : 0
    const result = await adminAPI.trafficDirector.listVersions(groupId, { limit: 100, offset })
    if (!isCurrentGroupContext(groupId, contextRevision) || requestRevision !== versionsRequestRevision) return false
    if (append) {
      const loadedVersions = new Set(versions.value.map((version) => version.version))
      versions.value = [
        ...versions.value,
        ...result.items.filter((version) => !loadedVersions.has(version.version))
      ]
    } else {
      versions.value = result.items
    }
    versionsTotal.value = result.total
    const consumed = result.items.length > 0 ? result.items.length : result.limit
    versionsNextOffset.value = Math.min(result.total, result.offset + consumed)
    return true
  } catch (error) {
    if (!isCurrentGroupContext(groupId, contextRevision) || requestRevision !== versionsRequestRevision) return false
    errorMessage.value = trafficDirectorError(error, 'admin.trafficDirector.errors.loadVersions')
    return false
  } finally {
    if (isCurrentGroupContext(groupId, contextRevision) && requestRevision === versionsRequestRevision) versionsLoading.value = false
  }
}

async function loadMoreVersions(): Promise<void> {
  if (versionsLoading.value || operationInFlight.value || versionsNextOffset.value >= versionsTotal.value) return
  await loadVersions(selectedGroupId.value, groupContextRevision, true)
}

function requestPayload(expectedVersion: number) {
  return {
    expected_version: expectedVersion,
    mode: draftMode.value,
    spec: draftMode.value === 'legacy' ? null : {
      schema_version: 1 as const,
      health_mode: draftSpec.value.health_mode,
      pools: draftSpec.value.pools.map((pool) => ({
        key: pool.key.trim(),
        weight_bps: Number(pool.weight_bps) || 0,
        account_ids: [...pool.account_ids],
        min_available: Number(pool.min_available) || 0,
        ...(pool.fallback_pool_key ? { fallback_pool_key: pool.fallback_pool_key } : {})
      }))
    }
  }
}

function payloadFingerprint(payload: unknown): string {
  return JSON.stringify(payload)
}

async function runPreview(): Promise<void> {
  const context = captureOperationContext()
  if (!context || !canPreview.value) return
  await previewPolicy(context, requestPayload(context.expectedVersion))
}

async function previewPolicy(context: GroupOperationContext, payload: ReturnType<typeof requestPayload>): Promise<TrafficDirectorPreview | null> {
  const requestRevision = ++previewRequestRevision
  previewing.value = true
  errorMessage.value = ''
  try {
    const result = await adminAPI.trafficDirector.preview(context.groupId, payload)
    if (!isCurrentOperationContext(context) || requestRevision !== previewRequestRevision) return null
    previewResult.value = result
    return result
  } catch (error) {
    if (!isCurrentOperationContext(context) || requestRevision !== previewRequestRevision) return null
    errorMessage.value = trafficDirectorError(error, 'admin.trafficDirector.errors.preview')
    return null
  } finally {
    if (requestRevision === previewRequestRevision) previewing.value = false
  }
}

async function publishPolicy(): Promise<void> {
  const context = captureOperationContext()
  if (!context || !canPublish.value) return
  const basePayload = requestPayload(context.expectedVersion)
  const payload = {
    ...basePayload,
    note: note.value,
    confirm_unassigned_accounts: confirmUnassigned.value
  }
  const requestRevision = ++publishRequestRevision
  publishing.value = true
  errorMessage.value = ''
  try {
    const currentPreview = previewResult.value
    if (!currentPreview || currentPreview.group_id !== context.groupId || currentPreview.expected_version !== context.expectedVersion) {
      const result = await previewPolicy(context, basePayload)
      if (!result || !isCurrentOperationContext(context)) return
    }
    const fingerprint = payloadFingerprint({ group_id: context.groupId, ...payload })
    if (!publishOperation.value || publishOperation.value.fingerprint !== fingerprint) {
      publishOperation.value = {
        key: createTrafficDirectorOperationKey(context.groupId, 'publish'),
        fingerprint
      }
    }
    const operationKey = publishOperation.value.key
    await adminAPI.trafficDirector.publish(context.groupId, payload, operationKey)
    if (publishOperation.value?.key === operationKey) publishOperation.value = null
    if (!isCurrentOperationContext(context) || requestRevision !== publishRequestRevision) return
    appStore.showSuccess(t('admin.trafficDirector.messages.published'))
    await loadGroup(context.groupId)
  } catch (error) {
    if (!isCurrentOperationContext(context) || requestRevision !== publishRequestRevision) return
    if (await refreshAfterVersionConflict(error, context)) return
    errorMessage.value = trafficDirectorError(error, 'admin.trafficDirector.errors.publish')
  } finally {
    if (requestRevision === publishRequestRevision) publishing.value = false
  }
}

async function openRollback(version: TrafficDirectorVersionSummary): Promise<void> {
  if (operationInFlight.value) return
  resetRollback()
  rollbackVersion.value = version
  rollbackDetailsLoading.value = true
  const contextRevision = groupContextRevision
  const requestRevision = ++rollbackDetailsRequestRevision
  try {
    const details = await adminAPI.trafficDirector.getVersion(version.group_id, version.version)
    if (!isCurrentGroupContext(version.group_id, contextRevision)
      || requestRevision !== rollbackDetailsRequestRevision
      || rollbackVersion.value?.version !== version.version) return
    rollbackDetails.value = details
  } catch (error) {
    if (!isCurrentGroupContext(version.group_id, contextRevision)
      || requestRevision !== rollbackDetailsRequestRevision
      || rollbackVersion.value?.version !== version.version) return
    rollbackDetailsError.value = trafficDirectorError(error, 'admin.trafficDirector.errors.loadVersion')
  } finally {
    if (requestRevision === rollbackDetailsRequestRevision) rollbackDetailsLoading.value = false
  }
}

function openLegacyRollback(): void {
  const currentState = state.value
  if (!currentState || currentState.head.mode === 'legacy' || operationInFlight.value) return
  void openRollback({
    group_id: currentState.group_id,
    version: 0,
    mode: 'legacy',
    checksum: '',
    note: '',
    created_at: ''
  })
}

async function rollbackPolicy(): Promise<void> {
  if (!canSubmitRollback.value) return
  const context = captureOperationContext()
  const targetVersion = rollbackVersion.value
  if (!context || !targetVersion || targetVersion.group_id !== context.groupId) return
  const payload = {
    expected_version: context.expectedVersion,
    confirm_unassigned_accounts: rollbackConfirmUnassigned.value,
    note: rollbackNote.value
  }
  const requestRevision = ++rollbackRequestRevision
  rollingBack.value = true
  errorMessage.value = ''
  try {
    const fingerprint = payloadFingerprint({ group_id: context.groupId, target: targetVersion.version, ...payload })
    if (!rollbackOperation.value || rollbackOperation.value.fingerprint !== fingerprint) {
      rollbackOperation.value = {
        key: createTrafficDirectorOperationKey(context.groupId, `rollback-${targetVersion.version}`),
        fingerprint
      }
    }
    const operationKey = rollbackOperation.value.key
    await adminAPI.trafficDirector.rollback(context.groupId, targetVersion.version, payload, operationKey)
    if (rollbackOperation.value?.key === operationKey) rollbackOperation.value = null
    if (!isCurrentOperationContext(context) || requestRevision !== rollbackRequestRevision) return
    resetRollback()
    appStore.showSuccess(t('admin.trafficDirector.messages.rolledBack'))
    await loadGroup(context.groupId)
  } catch (error) {
    if (!isCurrentOperationContext(context) || requestRevision !== rollbackRequestRevision) return
    if (await refreshAfterVersionConflict(error, context)) return
    errorMessage.value = trafficDirectorError(error, 'admin.trafficDirector.errors.rollback')
  } finally {
    if (requestRevision === rollbackRequestRevision) rollingBack.value = false
  }
}

async function refreshAfterVersionConflict(error: unknown, context: GroupOperationContext): Promise<boolean> {
  if (extractApiErrorCode(error) !== 'TRAFFIC_DIRECTOR_VERSION_CONFLICT') return false
  const conflictMessage = trafficDirectorError(error, 'admin.trafficDirector.errors.versionConflict')
  publishOperation.value = null
  rollbackOperation.value = null
  const refreshed = await loadGroup(context.groupId)
  if (refreshed && selectedGroupId.value === context.groupId) errorMessage.value = conflictMessage
  return true
}

async function refresh(): Promise<void> {
  if (selectedGroupId.value) await loadGroup(selectedGroupId.value)
  else await loadGroups()
}

onMounted(loadGroups)
</script>
