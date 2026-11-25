<template>
  <section class="page">
    <header class="page-head">
      <div>
        <h2>分区回填计划</h2>
        <p class="page-desc">
          把多条历史分区请求放进同一次批量回填安排：保留每个原请求的覆盖要求，
          交集分区交给回填合并队列裁决。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh-plans" type="button" @click="reloadAll">刷新计划与队列</button>
      </div>
    </header>

    <form class="plan-form" data-testid="plan-form" @submit.prevent="submitPlan">
      <div class="plan-form-head">
        <label class="create-field">
          <span>计划名称</span>
          <input v-model="form.name" data-testid="plan-name" placeholder="例如：夜间批量回填" />
        </label>
        <label class="create-field">
          <span>目标湖表</span>
          <input v-model="form.table" data-testid="plan-table" placeholder="例如：dwd_order" required />
        </label>
      </div>

      <table class="data-table request-table">
        <thead>
          <tr>
            <th>原请求</th>
            <th>起始分区</th>
            <th>结束分区</th>
            <th>覆盖模式</th>
            <th>优先级</th>
            <th>分区覆盖模式</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="(item, index) in form.requests" :key="index">
            <td><input v-model="item.label" :data-testid="`req-label-${index}`" placeholder="原请求名称" /></td>
            <td><input v-model="item.start_partition" :data-testid="`req-start-${index}`" placeholder="p1" /></td>
            <td><input v-model="item.end_partition" :data-testid="`req-end-${index}`" placeholder="p3" /></td>
            <td>
              <select v-model="item.mode" :data-testid="`req-mode-${index}`">
                <option value="append">追加</option>
                <option value="replace">替换</option>
              </select>
            </td>
            <td><input v-model="item.priority" :data-testid="`req-priority-${index}`" type="number" min="0" /></td>
            <td>
              <input
                v-model="item.partition_modes_text"
                :data-testid="`req-partition-modes-${index}`"
                placeholder="p2:append, p3:replace"
              />
            </td>
            <td class="row-actions">
              <button class="link" type="button" @click="removeRequest(index)">移除</button>
            </td>
          </tr>
        </tbody>
      </table>

      <div class="page-actions plan-form-actions">
        <button class="btn ghost" data-testid="add-request" type="button" @click="addRequest">再加一个原请求</button>
        <button class="btn primary" data-testid="submit-plan" type="submit" :disabled="submitting">
          {{ submitting ? '提交中…' : '提交批量回填安排' }}
        </button>
      </div>
      <p v-if="message" class="hint-text" data-testid="plan-message">{{ message }}</p>
      <p v-if="errorMessage" class="error-text" data-testid="plan-error">{{ errorMessage }}</p>
    </form>

    <h3 class="section-title">计划执行结果</h3>
    <table class="data-table plan-table">
      <thead>
        <tr>
          <th>计划</th>
          <th>目标湖表</th>
          <th>覆盖范围</th>
          <th>交集分区</th>
          <th>待处理分区</th>
          <th>已接收分区</th>
          <th>状态</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in plans" :key="item.plan_id" :data-testid="`plan-row-${item.plan_id}`">
          <td>{{ item.plan_id }} · {{ item.name }}</td>
          <td>{{ item.table }}</td>
          <td class="scope-cell">
            <span v-for="(partitions, requestId) in item.coverage" :key="requestId" class="scope-chip">
              {{ requestId }}：{{ partitions.join('、') }}
            </span>
          </td>
          <td>{{ item.intersection_partitions.join('、') || '—' }}</td>
          <td data-testid="plan-pending">{{ item.pending_partitions.join('、') || '—' }}</td>
          <td>{{ item.received_partitions.join('、') || '—' }}</td>
          <td>{{ item.status }}{{ item.confirmed ? '（已确认）' : '' }}</td>
          <td class="row-actions">
            <button
              v-if="!item.confirmed"
              class="link"
              :data-testid="`confirm-plan-${item.plan_id}`"
              type="button"
              @click="confirmAndOpen(item.plan_id)"
            >
              确认并查看详情
            </button>
            <button class="link" :data-testid="`open-plan-${item.plan_id}`" type="button" @click="openPlan(item.plan_id)">
              进入回填详情
            </button>
          </td>
        </tr>
        <tr v-if="!plans.length">
          <td colspan="8" class="empty-state">暂无分区回填计划，先提交一次批量回填安排</td>
        </tr>
      </tbody>
    </table>

    <h3 class="section-title">回填合并队列</h3>
    <table class="data-table queue-table">
      <thead>
        <tr>
          <th>队列</th>
          <th>计划</th>
          <th>交集裁决</th>
          <th>原请求</th>
          <th>非交集可提交范围</th>
          <th>下一步可提交</th>
          <th>状态</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in queue" :key="item.queue_id" :data-testid="`queue-row-${item.plan_id}`">
          <td>{{ item.queue_id }}</td>
          <td>{{ item.plan_id }} · {{ item.table }}</td>
          <td class="scope-cell">
            <span v-if="!item.intersection.dispositions.length" class="muted">无交集</span>
            <span
              v-for="disposition in item.intersection.dispositions"
              :key="disposition.partition"
              class="scope-chip"
              :class="{ 'chip-warn': !disposition.resolved }"
              :data-testid="`disposition-${item.plan_id}-${disposition.partition}`"
            >
              {{ disposition.partition }} →
              {{ disposition.resolved ? `${disposition.winner_request}（${disposition.mode_label}）` : '待裁决' }}
            </span>
          </td>
          <td class="scope-cell">
            <span v-for="request in item.original_requests" :key="request.request_id" class="scope-chip">
              {{ request.request_id }} {{ request.range }} / {{ request.mode_label }}
            </span>
          </td>
          <td class="scope-cell">
            <span v-for="(partitions, requestId) in item.committable_scope" :key="requestId" class="scope-chip">
              {{ requestId }}：{{ partitions.join('、') || '—' }}
            </span>
          </td>
          <td>{{ item.next_committable_partitions.join('、') || '—' }}</td>
          <td>{{ item.status }}</td>
          <td class="row-actions">
            <button class="link" :data-testid="`queue-open-${item.plan_id}`" type="button" @click="openPlan(item.plan_id)">
              进入回填详情
            </button>
          </td>
        </tr>
        <tr v-if="!queue.length">
          <td colspan="8" class="empty-state">回填合并队列为空</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ plans.length }} 个分区回填计划</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'
import { useRouter } from 'vue-router'

import {
  confirmPlan,
  listBackfillPlans,
  loadMergeQueue,
  submitBackfillPlan,
} from '@/api/backfill-plan'
import type { MergeQueueItem, PlanRequest, PlanSummary } from '@/api/backfill-plan'

type RequestDraft = {
  label: string
  start_partition: string
  end_partition: string
  mode: string
  priority: string
  partition_modes_text: string
}

const router = useRouter()
const plans = ref<PlanSummary[]>([])
const queue = ref<MergeQueueItem[]>([])
const message = ref('')
const errorMessage = ref('')
const submitting = ref(false)

const form = reactive({
  name: '',
  table: '',
  requests: [] as RequestDraft[],
})

function emptyRequest(start = '', end = ''): RequestDraft {
  return {
    label: '',
    start_partition: start,
    end_partition: end,
    mode: 'append',
    priority: '',
    partition_modes_text: '',
  }
}

function addRequest() {
  form.requests.push(emptyRequest())
}

function removeRequest(index: number) {
  form.requests.splice(index, 1)
}

function parsePartitionModes(text: string): Record<string, string> {
  const result: Record<string, string> = {}
  text
    .split(/[,，;；]/)
    .map((item) => item.trim())
    .filter(Boolean)
    .forEach((item) => {
      const [partition, mode] = item.split(/[:：]/).map((part) => part.trim())
      if (partition && mode) result[partition] = mode
    })
  return result
}

function toPayload(): PlanRequest[] {
  return form.requests.map((item) => ({
    label: item.label || null,
    start_partition: item.start_partition,
    end_partition: item.end_partition || item.start_partition,
    mode: item.mode,
    priority: item.priority === '' ? null : Number(item.priority),
    partition_modes: parsePartitionModes(item.partition_modes_text),
  }))
}

async function submitPlan() {
  submitting.value = true
  message.value = ''
  errorMessage.value = ''
  try {
    const result = await submitBackfillPlan(form.table.trim(), form.name.trim(), toPayload())
    message.value = result.message
    form.requests = []
    await reloadAll()
    if (result.plan?.plan_id) {
      await router.push(`/backfill-plans/${result.plan.plan_id}`)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批量回填安排提交失败'
  } finally {
    submitting.value = false
  }
}

function openPlan(planId: string) {
  void router.push(`/backfill-plans/${planId}`)
}

async function confirmAndOpen(planId: string) {
  errorMessage.value = ''
  try {
    const result = await confirmPlan(planId)
    message.value = result.message
    await reloadAll()
    openPlan(planId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '确认计划失败'
  }
}

async function reloadAll() {
  errorMessage.value = ''
  try {
    const [planPayload, queuePayload] = await Promise.all([listBackfillPlans(), loadMergeQueue()])
    plans.value = planPayload.items
    queue.value = queuePayload.items
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '分区回填计划读取失败'
  }
}

onMounted(() => {
  // 预置演示场景：一次提交 p1 到 p3（追加）与 p2 到 p4（替换）。
  form.requests = [emptyRequest('p1', 'p3'), emptyRequest('p2', 'p4')]
  form.requests[0].label = '历史分区追加'
  form.requests[1].label = '历史分区替换'
  form.requests[1].mode = 'replace'
  void reloadAll()
})
</script>
