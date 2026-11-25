/** 分区回填计划接口：批量安排、合并队列、交集裁决、完成分区接收。 */
import { request } from './client'

export type PlanRequest = {
  request_id?: string | null
  label?: string | null
  start_partition?: string | null
  end_partition?: string | null
  range?: string | null
  mode: string
  priority?: number | null
  partition_modes?: Record<string, string>
  rows?: number | null
}

export type Disposition = {
  partition: string
  resolved: boolean
  winner_request: string | null
  mode: string | null
  mode_label: string | null
  rule: string
  reason: string
  candidates: { request_id: string; mode: string; mode_label: string; priority: number }[]
  source_lookback?: { source: string; request_id: string | null; found: boolean }[]
  status: string
}

export type QueueRequest = {
  request_id: string
  label: string | null
  range: string
  start_partition: string
  end_partition: string
  mode: string
  mode_label: string
  priority: number
  coverage: string[]
  non_intersection_scope: string[]
  intersection_scope: string[]
}

export type MergeQueueItem = {
  queue_id: string
  plan_id: string
  name: string
  table: string
  status: string
  confirmed: boolean
  confirmation: {
    confirmed: boolean
    confirmed_at: string | null
    attempts: number
    token: string | null
  }
  intersection: {
    partitions: string[]
    unresolved_partitions: string[]
    status: string
    dispositions: Disposition[]
    source_lookback_required: boolean
  }
  original_requests: QueueRequest[]
  non_intersection_scope: Record<string, string[]>
  committable_scope: Record<string, string[]>
  intersection_scope: Record<string, string[]>
  pending_partitions: string[]
  received_partitions: string[]
  next_committable: Record<string, string[]>
  next_committable_partitions: string[]
  writebacks: { receipt_id: string | null; source: string; at: string }[]
}

export type PlanView = {
  plan_id: string
  name: string
  table: string
  status: string
  created_at: string
  confirmed_at: string | null
  requests: ({
    request_id: string
    label: string | null
    range: string
    start_partition: string
    end_partition: string
    mode: string
    mode_label: string
    priority: number
    partitions: string[]
    partition_modes: Record<string, string>
  })[]
  coverage: Record<string, string[]>
  non_intersection_scope: Record<string, string[]>
  intersection_scope: Record<string, string[]>
  intersection_partitions: string[]
  pending_partitions: string[]
  received_partitions: string[]
  need_decision_partitions: string[]
  next_committable: Record<string, string[]>
  partitions: {
    partition: string
    table: string
    kind: string
    status: string
    owners: { request_id: string; mode: string; mode_label: string; priority: number; range: string }[]
    adjudication: Disposition | null
    received: { request_id: string; mode: string; mode_label: string; at: string } | null
  }[]
}

export type PlanSummary = {
  plan_id: string
  name: string
  table: string
  status: string
  coverage: Record<string, string[]>
  pending_partitions: string[]
  received_partitions: string[]
  intersection_partitions: string[]
  confirmed: boolean
}

export type PlanDetail = { plan: PlanView; queue: MergeQueueItem }

export type CompletionResult = {
  ok: boolean
  message: string
  plan_id: string
  receipt_id: string | null
  duplicate_receipt: boolean
  applied_count: number
  duplicate_count: number
  rejected_count: number
  results: {
    partition: string
    status: string
    reason?: string
    duplicate?: boolean
    written_to?: string
    mode?: string
    mode_label?: string
    kind?: string
  }[]
  plan: PlanView
  queue: MergeQueueItem
}

async function json<T>(response: Response): Promise<T> {
  const payload = await response.json().catch(() => ({}))
  if (!response.ok) {
    throw new Error((payload as { detail?: string }).detail ?? `接口返回 ${response.status}`)
  }
  return payload as T
}

export async function submitBackfillPlan(
  table: string,
  name: string,
  requests: PlanRequest[],
): Promise<{ ok?: boolean; message: string; plan: PlanView; queue: MergeQueueItem; deduplicated: boolean; duplicate_submissions: number }> {
  return json(
    await request('/api/backfill-plans', {
      method: 'POST',
      body: JSON.stringify({ table, name, requests }),
    }),
  )
}

export async function listBackfillPlans(): Promise<{ items: PlanSummary[]; total: number }> {
  return json(await request('/api/backfill-plans'))
}

export async function loadPlanDetail(planId: string): Promise<PlanDetail> {
  return json(await request(`/api/backfill-plans/${planId}`))
}

export async function loadMergeQueue(): Promise<{ items: MergeQueueItem[]; total: number }> {
  return json(await request('/api/backfill-plans/merge-queue'))
}

export async function confirmPlan(planId: string): Promise<{ ok: boolean; message: string; plan: PlanView; queue: MergeQueueItem }> {
  return json(
    await request(`/api/backfill-plans/${planId}/confirm`, {
      method: 'POST',
      body: JSON.stringify({}),
    }),
  )
}

export async function resolveIntersection(
  planId: string,
  winnerRequest: string,
  partitions: string[],
  remark = '',
): Promise<{ ok: boolean; message: string; plan: PlanView; queue: MergeQueueItem }> {
  return json(
    await request(`/api/backfill-plans/${planId}/intersection`, {
      method: 'POST',
      body: JSON.stringify({ winner_request: winnerRequest, partitions, remark }),
    }),
  )
}

export async function receiveCompletedPartitions(
  planId: string,
  partitions: string[],
  receiptId: string,
): Promise<CompletionResult> {
  return json(
    await request(`/api/backfill-plans/${planId}/completions`, {
      method: 'POST',
      body: JSON.stringify({
        completed_partitions: partitions,
        receipt_id: receiptId,
        source: 'batch_merge',
      }),
    }),
  )
}
