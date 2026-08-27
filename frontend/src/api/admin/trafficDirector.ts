import { apiClient } from '../client'

export type TrafficDirectorMode = 'legacy' | 'shadow' | 'enforced'
export type TrafficDirectorHealthMode = 'off' | 'observe' | 'enforce'

export interface TrafficDirectorPool {
  key: string
  weight_bps: number
  account_ids: number[]
  min_available: number
  fallback_pool_key?: string
}

export interface TrafficDirectorSpec {
  schema_version: 1
  health_mode: TrafficDirectorHealthMode
  pools: TrafficDirectorPool[]
}

export interface TrafficDirectorHead {
  group_id: number
  version: number
  mode: TrafficDirectorMode
  spec?: TrafficDirectorSpec | null
}

export interface TrafficDirectorAccount {
  id: number
  name: string
  status: string
  schedulable: boolean
}

export interface TrafficDirectorGroupState {
  group_id: number
  group_name: string
  platform: string
  head: TrafficDirectorHead
  accounts: TrafficDirectorAccount[]
}

export interface TrafficDirectorPreview {
  group_id: number
  expected_version: number
  mode: TrafficDirectorMode
  normalized_spec?: TrafficDirectorSpec | null
  checksum: string
  unassigned_account_ids: number[]
  accounts: TrafficDirectorAccount[]
}

export interface TrafficDirectorVersion {
  group_id: number
  version: number
  mode: TrafficDirectorMode
  spec?: TrafficDirectorSpec | null
  checksum: string
  operator_id?: number | null
  note: string
  rollback_from_version?: number | null
  created_at: string
}

export type TrafficDirectorVersionSummary = Omit<TrafficDirectorVersion, 'spec'>

export interface TrafficDirectorVersionList {
  items: TrafficDirectorVersionSummary[]
  total: number
  limit: number
  offset: number
}

export interface TrafficDirectorStatusPool {
  key: string
  weight_bps: number
  account_count: number
  available_count: number
  min_available: number
  fallback_pool_key?: string
}

export interface TrafficDirectorRuntimeMetrics {
  scope: 'process_lifetime'
  routing_decisions: {
    total: number
    legacy_total: number
    shadow_total: number
    enforced_total: number
  }
  pool_routing: {
    exhausted_total: number
    fallback_transitions_total: number
    no_available_pool_total: number
  }
  health: {
    fail_open_total: number
  }
  policy: {
    l1_hit_total: number
    redis_hit_total: number
    db_fallback_total: number
    legacy_fallback_total: number
    unknown_source_total: number
    unavailable_total: number
  }
}

export interface TrafficDirectorStatus {
  group_id: number
  group_name: string
  platform: string
  head: TrafficDirectorHead
  mode: TrafficDirectorMode
  version: number
  checksum: string
  health_mode: TrafficDirectorHealthMode
  pools: TrafficDirectorStatusPool[]
  account_count: number
  available_account_count: number
  assigned_account_count: number
  unassigned_account_ids: number[]
  runtime_metrics: TrafficDirectorRuntimeMetrics
}

export interface TrafficDirectorPublishRequest {
  expected_version: number
  mode: TrafficDirectorMode
  spec?: TrafficDirectorSpec | null
  note?: string
  confirm_unassigned_accounts?: boolean
}

export interface TrafficDirectorPublishResult {
  version: TrafficDirectorVersion
  replayed: boolean
  unassigned_account_ids: number[]
}

export interface TrafficDirectorRollbackRequest {
  expected_version: number
  confirm_unassigned_accounts?: boolean
  note?: string
}

export function createTrafficDirectorOperationKey(groupId: number, action: string): string {
  const requestId = globalThis.crypto?.randomUUID?.() ?? `${Date.now()}-${Math.random().toString(36).slice(2)}`
  return `traffic-director-${action}-${groupId}-${requestId}`
}

function operationHeaders(idempotencyKey: string): { headers: { 'Idempotency-Key': string } } {
  const normalizedKey = idempotencyKey.trim()
  if (!normalizedKey) throw new Error('Traffic Director operations require an idempotency key')
  return { headers: { 'Idempotency-Key': normalizedKey } }
}

export async function get(groupId: number): Promise<TrafficDirectorGroupState> {
  const { data } = await apiClient.get<TrafficDirectorGroupState>(
    `/admin/groups/${groupId}/traffic-director`
  )
  return data
}

export async function listVersions(
  groupId: number,
  options?: { limit?: number; offset?: number }
): Promise<TrafficDirectorVersionList> {
  const { data } = await apiClient.get<TrafficDirectorVersionList>(
    `/admin/groups/${groupId}/traffic-director/versions`,
    { params: options }
  )
  return data
}

export async function getVersion(groupId: number, version: number): Promise<TrafficDirectorVersion> {
  const { data } = await apiClient.get<TrafficDirectorVersion>(
    `/admin/groups/${groupId}/traffic-director/versions/${version}`
  )
  return data
}

export async function preview(
  groupId: number,
  request: Omit<TrafficDirectorPublishRequest, 'note' | 'confirm_unassigned_accounts'>
): Promise<TrafficDirectorPreview> {
  const { data } = await apiClient.post<TrafficDirectorPreview>(
    `/admin/groups/${groupId}/traffic-director/preview`,
    request
  )
  return data
}

export async function publish(
  groupId: number,
  request: TrafficDirectorPublishRequest,
  idempotencyKey: string
): Promise<TrafficDirectorPublishResult> {
  const { data } = await apiClient.post<TrafficDirectorPublishResult>(
    `/admin/groups/${groupId}/traffic-director/publish`,
    request,
    operationHeaders(idempotencyKey)
  )
  return data
}

export async function rollback(
  groupId: number,
  version: number,
  request: TrafficDirectorRollbackRequest,
  idempotencyKey: string
): Promise<TrafficDirectorPublishResult> {
  const { data } = await apiClient.post<TrafficDirectorPublishResult>(
    `/admin/groups/${groupId}/traffic-director/rollback/${version}`,
    request,
    operationHeaders(idempotencyKey)
  )
  return data
}

export async function getStatus(groupId: number): Promise<TrafficDirectorStatus> {
  const { data } = await apiClient.get<TrafficDirectorStatus>(
    `/admin/groups/${groupId}/traffic-director/status`
  )
  return data
}

const trafficDirectorAPI = {
  get,
  listVersions,
  getVersion,
  preview,
  publish,
  rollback,
  getStatus
}

export default trafficDirectorAPI
