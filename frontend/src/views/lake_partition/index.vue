<template>
  <section class="page" data-module="lake_partition">
    <header class="page-head">
      <div>
        <h2>湖上分区管理</h2>
        <p class="page-desc">维护数据湖表的物理分区、桶化与文件组织规则，为湖上分区提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记分区定义</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出湖上分区清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无湖上分区数据，可先登记分区定义</td>
        </tr>
      </tbody>
    </table>

    <section class="migration-section" data-testid="migration-section">
      <header class="page-head">
        <div>
          <h3>分区迁移计划（小时 → 日）</h3>
          <p class="page-desc">
            支付表分区定义从小时粒度切换到日粒度的存量兼容方案：计划保留旧分区边界与目标分区规格，
            旧小时分区与目标日分区一一对应；中断恢复后只续做未完成批次。
          </p>
        </div>
        <div class="page-actions">
          <select v-model="strategy" class="btn" data-testid="strategy-select" title="旧边界与目标日分区相遇时的冲突处置策略">
            <option value="archive_boundary_first">先归档旧边界</option>
            <option value="switch_metadata_first">先切换目标元数据</option>
          </select>
          <button class="btn primary" data-testid="generate-plan" type="button" @click="generatePlan">生成迁移计划</button>
        </div>
      </header>

      <table class="data-table">
        <thead>
          <tr>
            <th>计划编号</th>
            <th>目标表名</th>
            <th>粒度切换</th>
            <th>版本</th>
            <th>冲突处置策略</th>
            <th>批次进度</th>
            <th>状态</th>
            <th>最近原因</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="plan in plans" :key="plan.plan_id" :data-testid="`plan-${plan.plan_id}`" :data-status="plan.status">
            <td>{{ plan.plan_id }}</td>
            <td>{{ plan.table_name }}</td>
            <td>{{ plan.source_granularity }} → {{ plan.target_granularity }}</td>
            <td>v{{ plan.version }}</td>
            <td>{{ plan.strategy_label }}</td>
            <td>{{ plan.migrated_batches }}/{{ plan.total_batches }}</td>
            <td>{{ plan.status }}</td>
            <td>{{ plan.last_reason ?? '—' }}</td>
            <td class="row-actions">
              <button class="link" type="button" :data-testid="`execute-${plan.plan_id}`" @click="executePlan(plan)">执行下一批次</button>
              <button class="link" type="button" :data-testid="`interrupt-${plan.plan_id}`" @click="interruptPlan(plan)">模拟中断</button>
              <button class="link" type="button" :data-testid="`resume-${plan.plan_id}`" @click="resumePlan(plan)">
                {{ plan.status === '失败' ? '重试' : '恢复迁移' }}
              </button>
              <button class="link" type="button" :data-testid="`detail-${plan.plan_id}`" @click="togglePlanDetail(plan)">映射明细</button>
            </td>
          </tr>
          <tr v-if="!plans.length">
            <td colspan="9" class="empty-state">暂无分区迁移计划记录，可先选择冲突处置策略后生成迁移计划</td>
          </tr>
        </tbody>
      </table>

      <div v-if="planDetail" class="plan-detail" data-testid="plan-detail">
        <h4>
          计划 {{ planDetail.plan_id }} 映射明细：旧小时分区 → 目标日分区一一对应
          （{{ planDetail.source_spec }} → {{ planDetail.target_spec }}）
        </h4>
        <table class="data-table">
          <thead>
            <tr>
              <th>批次号</th>
              <th>目标日分区</th>
              <th>旧小时分区数</th>
              <th>首个旧分区</th>
              <th>末个旧分区</th>
              <th>批次状态</th>
              <th>冲突处置</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="batch in planDetail.batches" :key="batch.batch_no">
              <td>{{ batch.batch_no }}</td>
              <td>{{ batch.target_partition }}</td>
              <td>{{ batch.source_partitions.length }}</td>
              <td>{{ batch.source_partitions[0] }}</td>
              <td>{{ batch.source_partitions[batch.source_partitions.length - 1] }}</td>
              <td>{{ batch.status }}</td>
              <td>{{ batch.resolution ?? '—' }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <p v-if="migrationMessage" class="info-text" data-testid="migration-message">{{ migrationMessage }}</p>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记分区定义</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条湖上分区记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type PlanBatch = {
  batch_no: number
  target_partition: string
  source_partitions: string[]
  status: string
  resolution: string | null
  migrated_at: string | null
}
type Plan = {
  plan_id: string
  table_name: string
  source_granularity: string
  target_granularity: string
  source_spec: string
  target_spec: string
  version: number
  strategy: string
  strategy_label: string
  status: string
  migrated_batches: number
  total_batches: number
  mapping_count: number
  last_reason: string | null
  batches: PlanBatch[]
}

const ENDPOINT = '/api/lake_partition'
const columns = ["分区编号", "目标表名", "分区列名", "分区粒度", "分区转换", "存储位置", "文件格式", "生命周期"]
const actions = ["提交校验", "标记生效", "停用分区"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const metricNames = ["分区定义总数", "待校验定义", "已生效定义"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

const plans = ref<Plan[]>([])
const planDetail = ref<Plan | null>(null)
const strategy = ref('archive_boundary_first')
const migrationMessage = ref('')

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'lake_partition.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) {
      throw new Error(payload.message || '湖上分区动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '湖上分区操作失败'
  }
}

async function loadPlans() {
  try {
    const response = await request(`${ENDPOINT}/migration-plans`)
    if (!response.ok) throw new Error('分区迁移计划列表读取失败')
    const payload = await response.json()
    plans.value = payload.items ?? []
  } catch (error) {
    migrationMessage.value = error instanceof Error ? error.message : '分区迁移计划列表读取失败'
  }
}

async function generatePlan() {
  migrationMessage.value = ''
  const source = rows.value.find((row) => row['分区粒度'] === 'hour')
  if (!source) {
    migrationMessage.value = '没有处于 hour 粒度的分区定义，无法生成小时到日的迁移计划'
    return
  }
  try {
    const response = await request(`${ENDPOINT}/migration-plans`, {
      method: 'POST',
      body: JSON.stringify({ entry_id: source.id, strategy: strategy.value }),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '生成迁移计划失败')
    migrationMessage.value = payload.message
    planDetail.value = null
    await Promise.all([reload(), loadPlans()])
  } catch (error) {
    migrationMessage.value = error instanceof Error ? error.message : '生成迁移计划失败'
  }
}

async function runPlanAction(plan: Plan, action: string, body: Record<string, unknown>) {
  migrationMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/migration-plans/${plan.plan_id}/${action}`, {
      method: 'POST',
      body: JSON.stringify(body),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '迁移计划操作失败')
    migrationMessage.value = payload.message
    await Promise.all([reload(), loadPlans()])
    if (planDetail.value?.plan_id === plan.plan_id) {
      await refreshPlanDetail(plan.plan_id)
    }
  } catch (error) {
    migrationMessage.value = error instanceof Error ? error.message : '迁移计划操作失败'
  }
}

async function executePlan(plan: Plan) {
  await runPlanAction(plan, 'execute', {})
}

async function interruptPlan(plan: Plan) {
  await runPlanAction(plan, 'interrupt', { reason: '模拟中断：迁移窗口人工暂停，待恢复' })
}

async function resumePlan(plan: Plan) {
  await runPlanAction(plan, 'resume', {})
}

async function refreshPlanDetail(planId: string) {
  const response = await request(`${ENDPOINT}/migration-plans/${planId}`)
  if (response.ok) {
    planDetail.value = (await response.json()) as Plan
  }
}

async function togglePlanDetail(plan: Plan) {
  migrationMessage.value = ''
  if (planDetail.value?.plan_id === plan.plan_id) {
    planDetail.value = null
    return
  }
  try {
    await refreshPlanDetail(plan.plan_id)
  } catch (error) {
    migrationMessage.value = error instanceof Error ? error.message : '迁移计划明细读取失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('分区定义列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '湖上分区列表读取失败'
  }
}

onMounted(() => {
  void reload()
  void loadPlans()
})
</script>
