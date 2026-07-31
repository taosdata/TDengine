<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { ElMessage } from 'element-plus'
import { ArrowLeftBold, Download, RefreshRight } from '@element-plus/icons-vue'
import { ApiClient } from '../api'
import { useAuthStore } from '../stores/auth'
import type { CrashIncident, ReportDetail } from '../types'

const route = useRoute()
const router = useRouter()
const auth = useAuthStore()
const api = new ApiClient(() => auth.token)

const loading = ref(false)
const downloading = ref(false)
const report = ref<ReportDetail | null>(null)
const messageOffset = 72

function toast(type: 'success' | 'warning' | 'error', message: string) {
  ElMessage({
    type,
    message,
    grouping: true,
    showClose: true,
    offset: messageOffset,
  })
}

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

function sortIncidents(items: CrashIncident[] | undefined): CrashIncident[] {
  const list = items ?? []
  return [...list].sort((a, b) => toTimestamp(b.occurred_at) - toTimestamp(a.occurred_at))
}

function fmtSetupSQL(sqls: string[] | undefined): string {
  if (!sqls || sqls.length === 0) {
    return '(empty)'
  }
  return sqls
    .map((item) => {
      const s = String(item ?? '').trim()
      if (!s) {
        return ''
      }
      return s.endsWith(';') ? s : `${s};`
    })
    .filter((s) => s.length > 0)
    .join('\n')
}

function normalizeSQL(sql: string | undefined): string {
  const s = String(sql ?? '').trim()
  if (!s) {
    return ''
  }
  return s.endsWith(';') ? s : `${s};`
}

function incidentSQL(item: CrashIncident): string {
  return normalizeSQL(item.crash_sql || item.sql || item.candidate_sql)
}

function incidentSQLList(items: CrashIncident[]): string[] {
  return items
    .map((item) => incidentSQL(item))
    .filter((s) => s.length > 0)
}

function fmtIncidentSQL(items: CrashIncident[]): string {
  const sqls = incidentSQLList(items)
  if (sqls.length === 0) {
    return '(no SQL recorded in report)'
  }
  return sqls.join('\n\n')
}

function markdownSQLBlock(text: string): string {
  const payload = text.trim()
  if (!payload || payload === '(empty)') {
    return '(empty)'
  }
  return `\`\`\`sql\n${payload}\n\`\`\``
}

function mergedCrashSQLText(items: CrashIncident[]): string {
  const sqls = incidentSQLList(items)
  if (sqls.length === 0) {
    return '(empty)'
  }
  return sqls.join('\n\n')
}

function buildMarkdownReport(detail: ReportDetail): string {
  const setupBlock = markdownSQLBlock(fmtSetupSQL(detail.setup_sql))
  const mergedIncidents = sortIncidents([
    ...(detail.taosd_incidents ?? []),
    ...(detail.tdsqlsmith_incidents ?? []),
  ])
  const crashBlock = markdownSQLBlock(mergedCrashSQLText(mergedIncidents))
  const lines: string[] = [
    '## Initial SQL',
    '',
    setupBlock,
    '',
    '## Crash SQL',
    '',
    crashBlock,
  ]
  return lines.join('\n')
}

const taosdIncidents = computed(() => sortIncidents(report.value?.taosd_incidents))
const tdsqlsmithIncidents = computed(() => sortIncidents(report.value?.tdsqlsmith_incidents))
const totalIncidents = computed(() => taosdIncidents.value.length + tdsqlsmithIncidents.value.length)
const setupSQLText = computed(() => fmtSetupSQL(report.value?.setup_sql))
const taosdIncidentSQLList = computed(() => incidentSQLList(taosdIncidents.value))
const tdsqlsmithIncidentSQLList = computed(() => incidentSQLList(tdsqlsmithIncidents.value))
const taosdIncidentSQL = computed(() => fmtIncidentSQL(taosdIncidents.value))
const tdsqlsmithIncidentSQL = computed(() => fmtIncidentSQL(tdsqlsmithIncidents.value))
const taosdCopyDisabled = computed(() => taosdIncidentSQLList.value.length === 0)
const tdsqlsmithCopyDisabled = computed(() => tdsqlsmithIncidentSQLList.value.length === 0)

async function copyText(text: string, label: string) {
  const payload = text.trim()
  if (!payload || payload === '(empty)' || payload === '(no SQL recorded in report)') {
    toast('warning', `${label}: no SQL to copy`)
    return
  }
  try {
    if (navigator?.clipboard?.writeText) {
      await navigator.clipboard.writeText(payload)
    } else {
      const input = document.createElement('textarea')
      input.value = payload
      input.setAttribute('readonly', 'true')
      input.style.position = 'fixed'
      input.style.left = '-9999px'
      document.body.appendChild(input)
      input.select()
      document.execCommand('copy')
      document.body.removeChild(input)
    }
    toast('success', `${label} copied`)
  } catch (e) {
    toast('error', e instanceof Error ? e.message : `copy failed: ${String(e)}`)
  }
}

async function downloadMarkdown() {
  if (!report.value) {
    toast('warning', 'Report data not loaded')
    return
  }
  if (totalIncidents.value <= 0) {
    toast('warning', 'Only crash reports can be downloaded')
    return
  }
  if (downloading.value) {
    return
  }
  downloading.value = true
  try {
    const content = buildMarkdownReport(report.value)
    const blob = new Blob([content], { type: 'text/markdown;charset=utf-8' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `${report.value.run_id}_crash_report.md`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
    toast('success', 'Markdown report downloaded')
  } catch (e) {
    toast('error', e instanceof Error ? e.message : String(e))
  } finally {
    downloading.value = false
  }
}

async function loadReport() {
  const runId = String(route.params.runId ?? '').trim()
  if (!runId) {
    return
  }
  loading.value = true
  try {
    report.value = await api.getReport(runId)
  } catch (e) {
    toast('error', e instanceof Error ? e.message : String(e))
  } finally {
    loading.value = false
  }
}

watch(() => route.params.runId, loadReport)
onMounted(loadReport)
</script>

<template>
  <section class="view-stack">
    <el-card class="view-card" shadow="never">
      <template #header>
        <div class="card-head">
          <div>
            <h2>Report Detail</h2>
            <p>{{ report?.run_id || String(route.params.runId || '') }}</p>
          </div>
          <div class="head-actions">
            <el-button @click="router.push('/reports')">
              <el-icon><ArrowLeftBold /></el-icon>
              Back
            </el-button>
            <el-button
              type="success"
              :icon="Download"
              :disabled="!report || totalIncidents <= 0"
              :loading="downloading"
              @click="downloadMarkdown"
            >
              Download Markdown
            </el-button>
            <el-button type="primary" :loading="loading" @click="loadReport">
              <el-icon><RefreshRight /></el-icon>
              Refresh
            </el-button>
          </div>
        </div>
      </template>

      <el-descriptions v-if="report" :column="4" border class="meta-grid">
        <el-descriptions-item label="Started">{{ fmtTime(report.started_at) }}</el-descriptions-item>
        <el-descriptions-item label="Generated">{{ fmtTime(report.generated_at) }}</el-descriptions-item>
        <el-descriptions-item label="Execution">{{ fmtDuration(report.execution_duration_ms) }}</el-descriptions-item>
        <el-descriptions-item label="Completed">
          <el-tag :type="report.completed ? 'success' : 'warning'" effect="plain">
            {{ report.completed ? 'Yes' : 'No' }}
          </el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="Incidents">
          <el-tag type="danger">{{ totalIncidents }}</el-tag>
        </el-descriptions-item>
        <el-descriptions-item label="Total Executed">{{ report.total_executed ?? 0 }}</el-descriptions-item>
      </el-descriptions>

      <el-row :gutter="16" class="section-row">
        <el-col :span="24">
          <el-card shadow="never" class="section-card">
            <template #header>
              <div class="section-head">
                <span>Shared Setup SQL</span>
                <div class="section-head-actions">
                  <el-tag type="info" effect="plain">{{ (report?.setup_sql || []).length }}</el-tag>
                  <el-button size="small" text @click="copyText(setupSQLText, 'shared setup SQL')">Copy</el-button>
                </div>
              </div>
            </template>
            <pre class="sql-block">{{ setupSQLText }}</pre>
          </el-card>
        </el-col>

        <el-col :span="24" class="incident-col">
          <el-card shadow="never" class="section-card">
            <template #header>
              <div class="section-head">
                <span>taosd Incidents</span>
                <div class="section-head-actions">
                  <el-tag :type="taosdIncidents.length > 0 ? 'danger' : 'success'">{{ taosdIncidents.length }}</el-tag>
                  <el-tag type="info" effect="plain">SQL {{ taosdIncidentSQLList.length }}/{{ taosdIncidents.length }}</el-tag>
                  <el-button
                    size="small"
                    text
                    :disabled="taosdCopyDisabled"
                    @click="copyText(taosdIncidentSQL, 'taosd incidents SQL')"
                  >
                    Copy
                  </el-button>
                </div>
              </div>
            </template>
            <pre class="sql-block">{{ taosdIncidentSQL }}</pre>
            <p v-if="taosdCopyDisabled && taosdIncidents.length > 0" class="trend-hint">
              This run recorded taosd incidents, but no SQL payload was persisted.
            </p>
          </el-card>
        </el-col>

        <el-col :span="24" class="incident-col">
          <el-card shadow="never" class="section-card">
            <template #header>
              <div class="section-head">
                <span>tdsqlsmith Incidents</span>
                <div class="section-head-actions">
                  <el-tag :type="tdsqlsmithIncidents.length > 0 ? 'danger' : 'success'">{{ tdsqlsmithIncidents.length }}</el-tag>
                  <el-tag type="info" effect="plain">
                    SQL {{ tdsqlsmithIncidentSQLList.length }}/{{ tdsqlsmithIncidents.length }}
                  </el-tag>
                  <el-button
                    size="small"
                    text
                    :disabled="tdsqlsmithCopyDisabled"
                    @click="copyText(tdsqlsmithIncidentSQL, 'tdsqlsmith incidents SQL')"
                  >
                    Copy
                  </el-button>
                </div>
              </div>
            </template>
            <pre class="sql-block">{{ tdsqlsmithIncidentSQL }}</pre>
          </el-card>
        </el-col>
      </el-row>
    </el-card>
  </section>
</template>
