<template>
  <section class="page" :data-plan="planId">
    <header class="page-head">
      <div>
        <h2>回填详情 · {{ detail?.plan.name || planId }}</h2>
        <p class="page-desc">
          核对已接收分区、交集处置和下一步可提交范围；批流合并按本计划逐条写入所属原请求。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="back-to-plans" type="button" @click="backToPlans">返回计划列表</button>
        <button class="btn" data-testid="refresh-detail" type="button" @click="reload">刷新</button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="detail-error">{{ errorMessage }}</p>
    <p v-if="message" class="hint-text" data-testid="detail-message">{{ message }}</p>

    <template v-if="detail">
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">计划状态</span>
          <strong class="stat-value">{{ detail.plan.status }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">目标湖表</span>
          <strong class="stat-value">{{ detail.plan.table }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">已接收分区</span>
          <strong class="stat-value" data-testid="received-count">{{ detail.plan.received_partitions.length }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">待处理分区</span>
          <strong class="stat-value" data-testid="pending-count">{{ detail.plan.pending_partitions.length }}</strong>
        </article>
      </div>

      <h3 class="section-title">原请求与覆盖要求</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>原请求</th>
            <th>范围</th>
            <th>覆盖模式</th>
            <th>优先级</th>
            <th>覆盖分区</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="request in detail.plan.requests" :key="request.request_id" :data-testid="`detail-request-${request.request_id}`">
            <td>{{ request.request_id }} · {{ request.label || '未命名' }}</td>
            <td>{{ request.range }}</td>
            <td>{{ request.mode_label }}</td>
            <td>{{ request.priority }}</td>
            <td>{{ request.partitions.join('、') }}</td>
          </tr>
        </tbody>
      </table>

      <h3 class="section-title">交集处置</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>交集分区</th>
            <th>裁决结果</th>
            <th>裁决依据</th>
            <th>双来源回查</th>
            <th>状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="entry in intersectionEntries" :key="entry.partition" :data-testid="`detail-intersection-${entry.partition}`">
            <td>{{ entry.partition }}</td>
            <td>
              {{ entry.adjudication?.resolved
                ? `${entry.adjudication?.winner_request}（${entry.adjudication?.mode_label}）`
                : '待裁决' }}
            </td>
            <td>{{ entry.adjudication?.reason || '—' }}</td>
            <td>
              <span v-for="source in entry.adjudication?.source_lookback || []" :key="source.source" class="scope-chip">
                {{ source.source }}：{{ source.found ? '命中' : '未命中' }}
              </span>
            </td>
            <td>{{ entry.status }}</td>
          </tr>
          <tr v-if="!intersectionEntries.length">
            <td colspan="5" class="empty-state">本计划没有交集分区</td>
          </tr>
        </tbody>
      </table>

      <h3 class="section-title">分区执行与下一步可提交范围</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>分区</th>
            <th>类型</th>
            <th>归属原请求</th>
            <th>模式</th>
            <th>状态</th>
            <th>实际完成</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="entry in detail.plan.partitions" :key="entry.partition" :data-testid="`detail-partition-${entry.partition}`">
            <td>{{ entry.partition }}</td>
            <td>{{ entry.kind }}</td>
            <td>{{ entry.received?.request_id || entry.adjudication?.winner_request || entry.owners[0]?.request_id }}</td>
            <td>{{ entry.received?.mode_label || entry.adjudication?.mode_label || entry.owners[0]?.mode_label }}</td>
            <td>{{ entry.status }}</td>
            <td>{{ entry.received ? `${entry.received.at}（${entry.received.request_id}）` : '—' }}</td>
          </tr>
        </tbody>
      </table>

      <div class="plan-actions-row">
        <button
          class="btn primary"
          data-testid="confirm-plan"
          type="button"
          :disabled="!canConfirm"
          @click="doConfirm"
        >
          确认计划
        </button>
        <template v-if="unresolvedPartitions.length && detail.queue.original_requests.length">
          <select v-model="winnerRequest" data-testid="intersection-winner">
            <option v-for="request in detail.queue.original_requests" :key="request.request_id" :value="request.request_id">
              {{ request.request_id }}（{{ request.mode_label }}）
            </option>
          </select>
          <button class="btn" data-testid="resolve-intersection" type="button" @click="doResolve">
            裁决交集分区
          </button>
        </template>
      </div>

      <form class="receive-form" @submit.prevent="doReceive">
        <label class="create-field">
          <span>批流合并实际完成的分区（逗号分隔）</span>
          <input v-model="completedText" data-testid="completed-partitions" placeholder="p1,p2,p3" />
        </label>
        <button class="btn primary" data-testid="receive-completions" type="submit" :disabled="!detail.queue.confirmed">
          接收完成分区
        </button>
      </form>

      <h3 class="section-title">回填合并队列</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>队列</th>
            <th>状态</th>
            <th>交集裁决</th>
            <th>非交集可提交范围</th>
            <th>下一步可提交</th>
            <th>已回写批次</th>
          </tr>
        </thead>
        <tbody>
          <tr :data-testid="`queue-detail-${detail.queue.plan_id}`">
            <td>{{ detail.queue.queue_id }}</td>
            <td>{{ detail.queue.status }}</td>
            <td>{{ detail.queue.intersection.status }}</td>
            <td class="scope-cell">
              <span v-for="(partitions, requestId) in detail.queue.committable_scope" :key="requestId" class="scope-chip">
                {{ requestId }}：{{ partitions.join('、') || '—' }}
              </span>
            </td>
            <td data-testid="next-committable">{{ detail.queue.next_committable_partitions.join('、') || '—' }}</td>
            <td>{{ detail.queue.writebacks.length }} 批</td>
          </tr>
        </tbody>
      </table>

      <h3 class="section-title">接收结果</h3>
      <table v-if="completion" class="data-table" data-testid="completion-result">
        <thead>
          <tr><th>分区</th><th>结果</th><th>写入原请求</th><th>模式</th><th>说明</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in completion.results" :key="item.partition">
            <td>{{ item.partition }}</td>
            <td>{{ item.status }}</td>
            <td>{{ item.written_to || '—' }}</td>
            <td>{{ item.mode_label || '—' }}</td>
            <td>{{ item.reason || (item.duplicate ? '重复提交，只记一次' : '—') }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="muted">尚未接收完成分区</p>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { useRouter } from 'vue-router'

import {
  confirmPlan,
  loadPlanDetail,
  receiveCompletedPartitions,
  resolveIntersection,
} from '@/api/backfill-plan'
import type { CompletionResult, PlanDetail } from '@/api/backfill-plan'

const props = defineProps<{ planId: string }>()
const router = useRouter()
const detail = ref<PlanDetail | null>(null)
const completion = ref<CompletionResult | null>(null)
const message = ref('')
const errorMessage = ref('')
const completedText = ref('')
const winnerRequest = ref('')

const intersectionEntries = computed(
  () => detail.value?.plan.partitions.filter((entry) => entry.kind === '交集') ?? [],
)
const unresolvedPartitions = computed(() => detail.value?.plan.need_decision_partitions ?? [])
const canConfirm = computed(
  () => detail.value !== null && !detail.value.queue.confirmed && unresolvedPartitions.value.length === 0,
)

async function reload() {
  errorMessage.value = ''
  try {
    detail.value = await loadPlanDetail(props.planId)
    if (!winnerRequest.value) {
      winnerRequest.value = detail.value.queue.original_requests[0]?.request_id ?? ''
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回填详情读取失败'
  }
}

async function doConfirm() {
  message.value = ''
  errorMessage.value = ''
  try {
    const result = await confirmPlan(props.planId)
    message.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '确认失败'
  }
}

async function doResolve() {
  message.value = ''
  errorMessage.value = ''
  try {
    const result = await resolveIntersection(
      props.planId,
      winnerRequest.value,
      unresolvedPartitions.value,
    )
    message.value = result.message
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交集裁决失败'
  }
}

async function doReceive() {
  message.value = ''
  errorMessage.value = ''
  const partitions = completedText.value
    .split(/[,，\s]+/)
    .map((item) => item.trim())
    .filter(Boolean)
  if (!partitions.length) {
    errorMessage.value = '请填写实际完成的分区'
    return
  }
  try {
    const result = await receiveCompletedPartitions(
      props.planId,
      partitions,
      `UI-${props.planId}-${Date.now()}`,
    )
    completion.value = result
    message.value = result.message
    completedText.value = ''
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '接收完成分区失败'
  }
}

function backToPlans() {
  void router.push('/backfill-plans')
}

onMounted(reload)
</script>
