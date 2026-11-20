<template>
  <section class="page" data-module="iceberg_table">
    <header class="page-head">
      <div>
        <h2>Iceberg管理管理</h2>
        <p class="page-desc">维护Iceberg湖表的元数据、快照与文件清理策略，为Iceberg管理提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记表元数据</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出Iceberg管理清单</button>
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
            <button class="link" type="button" :data-testid="`plans-${row.id}`" @click="loadRelatedPlans(row)">迁移计划</button>
            <button class="link" type="button" :data-testid="`verify-${row.id}`" @click="verifyMigration(row)">核对目标元数据</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无Iceberg管理数据，可先登记表元数据</td>
        </tr>
      </tbody>
    </table>

    <section class="migration-section" data-testid="migration-section">
      <header class="page-head">
        <div>
          <h3>关联分区迁移计划{{ relatedTable ? `（${relatedTable}）` : '' }}</h3>
          <p class="page-desc">
            与湖上分区读取同一份分区迁移计划；核对通过后写入支付表目标分区元数据，
            并确认每个支付事件只落入一个目标分区。
          </p>
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
          </tr>
        </thead>
        <tbody>
          <tr v-for="plan in relatedPlans" :key="plan.plan_id" :data-testid="`related-plan-${plan.plan_id}`" :data-status="plan.status">
            <td>{{ plan.plan_id }}</td>
            <td>{{ plan.table_name }}</td>
            <td>{{ plan.source_granularity }} → {{ plan.target_granularity }}</td>
            <td>v{{ plan.version }}</td>
            <td>{{ plan.strategy_label }}</td>
            <td>{{ plan.migrated_batches }}/{{ plan.total_batches }}</td>
            <td>{{ plan.status }}</td>
            <td>{{ plan.last_reason ?? '—' }}</td>
          </tr>
          <tr v-if="!relatedPlans.length">
            <td colspan="8" class="empty-state">暂无关联分区迁移计划记录</td>
          </tr>
        </tbody>
      </table>

      <div v-if="verifyResult" class="verify-result" data-testid="verify-result">
        <h4>目标元数据核对结果</h4>
        <p>
          迁移计划 {{ verifyResult.plan_id }}（v{{ verifyResult.plan_version }}）：
          目标分区规格 {{ verifyResult.target_spec }}，写入后元数据版本 {{ verifyResult.metadata_version }}，
          落区校验{{ verifyResult.assignment_ok ? `通过：${verifyResult.checked_sources} 个支付事件小时分区各自仅落入一个目标分区` : '存在冲突' }}。
        </p>
      </div>

      <p v-if="migrationMessage" class="info-text" data-testid="migration-message">{{ migrationMessage }}</p>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记表元数据</h3>
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
      <span>共 {{ total }} 条Iceberg管理记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>
type Plan = {
  plan_id: string
  table_name: string
  source_granularity: string
  target_granularity: string
  version: number
  strategy_label: string
  status: string
  migrated_batches: number
  total_batches: number
  last_reason: string | null
}
type VerifyResult = {
  plan_id: string
  plan_version: number
  target_spec: string
  metadata_version: string
  assignment_ok: boolean
  checked_sources: number
  verified_at: string
}

const ENDPOINT = '/api/iceberg_table'
const columns = ["表编号", "表名称", "存储格式", "分区策略", "排序字段", "快照过期", "孤立文件清理", "元数据版本"]
const actions = ["提交校验", "确认就绪", "标记下线"]
const statuses = ["草稿", "待校验", "已就绪", "已下线"]
const metricNames = ["表元数据总数", "待校验表", "已就绪表"]

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

const relatedPlans = ref<Plan[]>([])
const relatedTable = ref('')
const relatedEntryId = ref<number | null>(null)
const verifyResult = ref<VerifyResult | null>(null)
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
    link.download = 'iceberg_table.csv'
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
      throw new Error(payload.message || 'Iceberg管理动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'Iceberg管理操作失败'
  }
}

async function loadRelatedPlans(row: Row) {
  migrationMessage.value = ''
  verifyResult.value = null
  try {
    const response = await request(`${ENDPOINT}/${row.id}/migration-plans`)
    if (!response.ok) throw new Error('关联分区迁移计划读取失败')
    const payload = await response.json()
    relatedPlans.value = payload.items ?? []
    relatedTable.value = String(row['表名称'] ?? '')
    relatedEntryId.value = Number(row.id)
  } catch (error) {
    migrationMessage.value = error instanceof Error ? error.message : '关联分区迁移计划读取失败'
  }
}

async function verifyMigration(row: Row) {
  errorMessage.value = ''
  migrationMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/verify-migration`, { method: 'POST', body: JSON.stringify({}) })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '核对目标元数据失败')
    verifyResult.value = (payload.entry?._migration_verify ?? null) as VerifyResult | null
    migrationMessage.value = payload.message
    await reload()
    if (relatedEntryId.value === Number(row.id)) {
      await loadRelatedPlans(row)
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '核对目标元数据失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('表元数据列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'Iceberg管理列表读取失败'
  }
}

onMounted(async () => {
  await reload()
  // 默认展示支付表的关联迁移计划；没有支付表时退到第一条记录
  const paymentRow = rows.value.find((row) => row['表名称'] === 'payments') ?? rows.value[0]
  if (paymentRow) {
    await loadRelatedPlans(paymentRow)
  }
})
</script>
