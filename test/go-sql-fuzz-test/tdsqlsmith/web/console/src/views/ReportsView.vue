<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { RefreshRight } from '@element-plus/icons-vue'
import { ApiClient } from '../api'
import { useAuthStore } from '../stores/auth'
import type { ReportSummary } from '../types'

const router = useRouter()
const auth = useAuthStore()
const api = new ApiClient(() => auth.token)

const loading = ref(false)
const reports = ref<ReportSummary[]>([])
const incidentsGtZero = ref(false)
const pageSize = ref(10)
const page = ref(1)
const sortBy = ref<'generated_desc' | 'incidents_desc' | 'incidents_asc'>('generated_desc')

const pageSizeOptions = [10, 20, 50, 100]

function toTimestamp(s: string): number {
  const ts = Date.parse(s)
  return Number.isNaN(ts) ? 0 : ts
}

function fmtTime(s: string): string {
  const ts = toTimestamp(s)
  if (ts <= 0) {
    return s
  }
  return new Date(ts).toLocaleString()
}

function fmtDuration(ms: number): string {
  const totalMS = Number(ms)
  if (!Number.isFinite(totalMS) || totalMS <= 0) {
    return '0s'
  }
  if (totalMS < 1000) {
    return `${Math.floor(totalMS)}ms`
  }
  let remain = Math.floor(totalMS / 1000)
  const hours = Math.floor(remain / 3600)
  remain %= 3600
  const minutes = Math.floor(remain / 60)
  const seconds = remain % 60
  if (hours > 0) {
    return `${hours}h ${minutes}m ${seconds}s`
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`
  }
  return `${seconds}s`
}

async function loadReports() {
  loading.value = true
  try {
    const out = await api.listReports()
    reports.value = out.items
  } catch (e) {
    ElMessage.error(e instanceof Error ? e.message : String(e))
  } finally {
    loading.value = false
  }
}

const sortedReports = computed(() => {
  const items = [...reports.value]
  switch (sortBy.value) {
    case 'incidents_desc':
      return items.sort((a, b) => {
        if (b.incident_count !== a.incident_count) {
          return b.incident_count - a.incident_count
        }
        return toTimestamp(b.generated_at) - toTimestamp(a.generated_at)
      })
    case 'incidents_asc':
      return items.sort((a, b) => {
        if (a.incident_count !== b.incident_count) {
          return a.incident_count - b.incident_count
        }
        return toTimestamp(b.generated_at) - toTimestamp(a.generated_at)
      })
    default:
      return items.sort((a, b) => toTimestamp(b.generated_at) - toTimestamp(a.generated_at))
  }
})

const filteredReports = computed(() => {
  if (!incidentsGtZero.value) {
    return sortedReports.value
  }
  return sortedReports.value.filter((r) => r.incident_count > 0)
})

const pagedReports = computed(() => {
  const start = (page.value - 1) * pageSize.value
  return filteredReports.value.slice(start, start + pageSize.value)
})

watch([incidentsGtZero, pageSize, sortBy], () => {
  page.value = 1
})

watch(filteredReports, () => {
  const maxPage = Math.max(1, Math.ceil(filteredReports.value.length / pageSize.value))
  if (page.value > maxPage) {
    page.value = maxPage
  }
})

function openDetail(row: ReportSummary) {
  router.push(`/reports/${encodeURIComponent(row.run_id)}`)
}

onMounted(loadReports)
</script>

<template>
  <section class="view-stack">
    <el-card class="view-card" shadow="never">
      <template #header>
        <div class="card-head">
          <div>
            <h2>Crash Reports</h2>
            <p>Track grouped incidents and navigate to details.</p>
          </div>
          <el-button type="primary" :loading="loading" @click="loadReports">
            <el-icon><RefreshRight /></el-icon>
            Refresh
          </el-button>
        </div>
      </template>

      <div class="filter-row">
        <el-checkbox v-model="incidentsGtZero">Incidents &gt; 0</el-checkbox>
        <el-select v-model="sortBy" style="width: 200px">
          <el-option label="Generated ↓" value="generated_desc" />
          <el-option label="Incidents ↓" value="incidents_desc" />
          <el-option label="Incidents ↑" value="incidents_asc" />
        </el-select>
        <el-select v-model="pageSize" style="width: 120px">
          <el-option v-for="n in pageSizeOptions" :key="n" :label="String(n)" :value="n" />
        </el-select>
        <el-tag type="info" effect="plain">Total {{ filteredReports.length }}</el-tag>
      </div>

      <el-table :data="pagedReports" stripe height="560" @row-click="openDetail">
        <el-table-column prop="run_id" label="Run ID" min-width="360">
          <template #default="{ row }">
            <el-link type="primary" :underline="false">{{ row.run_id }}</el-link>
          </template>
        </el-table-column>
        <el-table-column prop="generated_at" label="Generated" min-width="240">
          <template #default="{ row }">
            {{ fmtTime(row.generated_at) }}
          </template>
        </el-table-column>
        <el-table-column prop="started_at" label="Started" min-width="240">
          <template #default="{ row }">
            {{ fmtTime(row.started_at) }}
          </template>
        </el-table-column>
        <el-table-column prop="execution_duration_ms" label="Execution" width="140" align="right">
          <template #default="{ row }">
            {{ fmtDuration(row.execution_duration_ms) }}
          </template>
        </el-table-column>
        <el-table-column prop="completed" label="Completed" width="120" align="center">
          <template #default="{ row }">
            <el-tag :type="row.completed ? 'success' : 'warning'" effect="plain">
              {{ row.completed ? 'Yes' : 'No' }}
            </el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="total_executed" label="Executed" width="120" align="right" />
        <el-table-column prop="incident_count" label="Incidents" width="230" align="right">
          <template #default="{ row }">
            <el-tag :type="row.incident_count > 0 ? 'danger' : 'success'" style="margin-right: 6px">
              {{ row.incident_count }}
            </el-tag>
            <el-tag type="danger" effect="plain" style="margin-right: 4px">taosd {{ row.taosd_incident_count }}</el-tag>
            <el-tag type="warning" effect="plain">tdsqlsmith {{ row.tdsqlsmith_incident_count }}</el-tag>
          </template>
        </el-table-column>
      </el-table>

      <div class="pager-wrap">
        <el-pagination
          v-model:current-page="page"
          :page-size="pageSize"
          :total="filteredReports.length"
          layout="prev, pager, next"
          background
        />
      </div>
    </el-card>
  </section>
</template>
