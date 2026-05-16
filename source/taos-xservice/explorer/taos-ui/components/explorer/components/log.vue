<template>
  <div ref="logRef" class="log">
    <div v-if="history.length" ref="content" class="log-content">
      <div v-for="record in history" :key="getRecordKey(record)" class="record-item">
        <div class="first-row">
          <span class="db-and-arrow">{{ record.database }}></span>
          <span
            v-dompurify-html="record.sql"
            style="color: #1652f0; white-space: pre; cursor: pointer"
            @click="addSql(record.sql)"
          ></span>
        </div>
        <div class="second-row">
          [ {{ getRecordDateTime(record) }} ]
          <template v-if="record.type">
            <span v-if="record.rows">{{ record.rows }} rows retrieved</span>
            <span v-else>{{ record.message }}</span>
          </template>
          <template v-else>
            <span style="color: #b22222">{{ t('status.error') }}: {{ record.message }}</span>
          </template>
          <span class="total"> {{ getExecTimeText(record) }} </span>
        </div>
      </div>
    </div>
  </div>
</template>

<script lang="ts" setup>
import {
  addLogEvent,
  changeLogSortEvent,
  formatDurationLabel,
  getLogCreatedAt,
  getLogId,
  parseStoredLogRecords
} from './utils';
import { t } from 'locales';
import { handleDateTime } from 'utils/date';
import { getSqlProvider } from '../model/useExplorer';
import { instance } from 'config';

const logKey = 'explorer_log_' + instance.id;
const logList = ref<Recordable[]>(
  normalizeLogRecords(parseStoredLogRecords(localStorage.getItem(logKey)))
);

const ExplorerLogSortKey = 'explorer_log_sort_' + instance.id;
const logSort = ref(localStorage.getItem(ExplorerLogSortKey) ?? 'desc');

const isDesc = computed(() => logSort.value === 'desc');
const history = computed(() => (isDesc.value ? logList.value.slice().reverse() : logList.value));
const logRef = ref<HTMLElement | null>(null);
const sqlProvider = getSqlProvider();
const addLogUnsubscribe = addLogEvent.on(addLog);
const changeLogSortUnsubscribe = changeLogSortEvent.on(setLogSort);

watch(
  () => history.value,
  () => {
    handleScroll();
  }
);
function setLogSort() {
  logSort.value = logSort.value == 'desc' ? 'asc' : 'desc';
  localStorage.setItem(ExplorerLogSortKey, logSort.value);
}
function addLog(log: Recordable) {
  const list = logList.value;
  list.push(normalizeLogRecord(log, list.length + 1));
  logList.value = list.slice(-100);
  localStorage.setItem(logKey, JSON.stringify(logList.value));
}
function handleScroll() {
  nextTick(() => {
    if (!logRef.value?.scrollHeight) return;
    logRef.value.scrollTop = isDesc.value ? 0 : logRef.value.scrollHeight;
  });
}
function getExecTimeText(record: Recordable) {
  // eslint-disable-next-line prefer-const
  let { executeTime, networkTime, totalTime } = record;
  if (totalTime === undefined || totalTime === null) {
    totalTime = record.time;
  }
  const executeLabel = formatDurationLabel(executeTime);
  const networkLabel = formatDurationLabel(networkTime);
  const totalLabel = formatDurationLabel(totalTime);
  return `(${t('common.execute')}: ${executeLabel}; ${t('common.network')}: ${networkLabel}; ${t('common.total')}: ${totalLabel})`;
}

function getRecordDateTime(record: Recordable): string {
  const createdAt = getLogCreatedAt(record);
  return createdAt === null ? '--' : handleDateTime(createdAt);
}

function getRecordKey(record: Recordable): string {
  const logId = getLogId(record);
  return logId === null ? 'log-unknown' : `log-${logId}`;
}

function normalizeLogRecords(records: Recordable[]): Recordable[] {
  return records.map((record, index) => normalizeLogRecord(record, index + 1));
}

function normalizeLogRecord(record: Recordable, sequence: number): Recordable {
  const logId = getLogId(record) ?? createLegacyLogId(record, sequence);
  return { ...record, logId };
}

function createLegacyLogId(record: Recordable, sequence: number): string {
  const createdAt = getLogCreatedAt(record);
  return createdAt === null ? `legacy-${sequence}` : `legacy-${createdAt}-${sequence}`;
}

function addSql(sql: string) {
  sqlProvider.addSql('\n' + sql);
}
onBeforeUnmount(() => {
  addLogUnsubscribe();
  changeLogSortUnsubscribe();
});
</script>

<style lang="scss" scoped>
.log {
  display: block;
  height: 100%;
  overflow: auto;
  border: 1px solid #dcdfe6;
}

.log-content {
  padding: 20px 30px;
}

.record-item {
  margin-bottom: 13px;
  font-size: 15px;
}

.first-row {
  display: flex;
  flex-direction: row;
  align-items: center;
}

.second-row {
  margin-top: 6px;
  color: #666;
}

.db-and-arrow {
  margin-right: 8px;
  color: #33b169;
}
</style>
