<template>
  <div class="panel">
    <el-tabs v-model="panelActiveTab" type="border-card" size="small">
      <el-tab-pane name="grid">
        <template #label>
          <div class="flex-center">
            <Icon name="table" class="tab-icon"></Icon>
            <span>{{ t('explorer.grid') }}</span>
            <el-tooltip effect="light" :content="t('explorer.cellCopyTip')" placement="bottom">
              <el-icon class="info-icon" :size="12">
                <InfoFilled />
              </el-icon>
            </el-tooltip>
          </div>
        </template>
        <GridView></GridView>
      </el-tab-pane>
      <el-tab-pane name="chart">
        <template #label>
          <div class="flex-center">
            <Icon name="chart" class="tab-icon"></Icon>
            <span>{{ t('common.chart') }}</span>
          </div>
        </template>
        <ChartView ref="chartViewRef"></ChartView>
      </el-tab-pane>
    </el-tabs>
    <div class="panel-right">
      <p class="data-nums">{{ dataSource.length }} rows</p>
      <el-tooltip
        v-if="partActiveTab == 'log'"
        effect="light"
        :content="t('common.' + (isDesc ? 'orderByAscending' : 'orderByDescending'))"
      >
        <el-button class="log-sort-btn" icon="sort" plain size="small" @click="logSortChange"></el-button>
      </el-tooltip>
      <el-tooltip effect="light" :content="t('explorer.exportCurrentData')">
        <el-button :disabled="dataSource.length == 0 || loading" plain size="small" @click="exportAll">
          <Icon name="export" class="export-icon"></Icon>
          {{ t('common.export') }}
        </el-button>
      </el-tooltip>
    </div>
  </div>
</template>

<script lang="ts" setup>
import { t } from 'locales';
import GridView from './grid.vue';
import ChartView from './chart.vue';
import { wsExport, localExport } from 'utils/wsexporter';
import { sqlExecResult, panelActiveTab, changeLogSortEvent, partActiveTab } from './utils';
import { getSqlProvider } from '../model/useExplorer';
import { ElMessageBox, ElMessage } from 'element-plus';
import { instance, project } from 'config';

const { sqlStr } = getSqlProvider();
const ExplorerLogSortKey = 'explorer_log_sort_' + instance.id;
const logSort = ref(localStorage.getItem(ExplorerLogSortKey) ?? 'desc');
const isDesc = computed(() => logSort.value === 'desc');
const loading = ref(false);
const dataSource = computed(() => sqlExecResult.data);

const chartViewRef = ref();

watch(panelActiveTab, (newVal) => {
  if (newVal === 'chart') {
    nextTick(() => {
      chartViewRef.value?.drawChart();
    });
  }
});

function exportAll() {
  const trimmedSql = sqlStr.value.toLowerCase().trim();
  if (
    !trimmedSql.startsWith('select') &&
    !trimmedSql.startsWith('show') &&
    !trimmedSql.startsWith('desc') &&
    !trimmedSql.startsWith('explain')
  ) {
    ElMessage.warning(
      t(
        'explorer.exportError',
        t('status.error', {
          type: 'warning'
        })
      )
    );
    return;
  }

  ElMessageBox.confirm(t('explorer.exportConfirm'), t('common.tips')).then(() => {
    loading.value = true;
    if (project.isCloud) {
      wsExport(instance.gatewayUrl, instance.token, sqlStr.value, true)
        .catch(err => {
          ElMessage.error(err?.message);
        })
        .finally(() => {
          loading.value = false;
        });
    } else {
      try {
        localExport(sqlExecResult);
      } catch (err: any) {
        ElMessage.error(err?.message);
      } finally {
        loading.value = false;
      }
    }
  });
}
function logSortChange() {
  logSort.value = logSort.value == 'desc' ? 'asc' : 'desc';
  changeLogSortEvent.emit();
}
</script>

<style lang="scss" scoped>
.flex-center {
  height: 100%;
}

.panel {
  position: relative;
  height: 100%;
  min-height: 200px;
  background-color: #ffffff;

  /* override el-tabs border-card styles */
  &:deep(.el-tabs--border-card) {
    border: none;
    border-radius: 6px;
    box-shadow: none;
    height: 100%;
    background-color: #f2f3f3;
  }

  &:deep(.el-tabs--border-card > .el-tabs__header) {
    border-radius: 6px 6px 0 0;
    margin-bottom: 0;
    background-color: #f2f3f3;
  }

  /* 第一个 tab 左上角圆角 */
  &:deep(.el-tabs--border-card > .el-tabs__header .el-tabs__item:first-child) {
    border-top-left-radius: 6px !important;
  }
  &:deep(.el-tabs--border-card > .el-tabs__header .el-tabs__item:first-child.is-active) {
    border-top-left-radius: 6px !important;
  }

  &:deep(.el-tabs__content) {
    flex: 1;
    padding: 15px !important;
    overflow: hidden;
    background-color: #ffffff;

    & > .el-tab-pane {
      height: 100%;
    }
  }

  &:deep(.el-tabs) {
    border: none;
  }

  &:deep(.el-tabs--border-card > .el-tabs__header) {
    padding-right: 230px;
  }
}

.tab-icon {
  width: 19px;
  height: 19px;
  margin-right: 5px;
  cursor: pointer;
}

.panel-right {
  position: absolute;
  top: 8px;
  right: 15px;
  display: flex;
  align-items: center;
  color: #333;

  .log-sort-btn {
    padding: 7px;
  }
}

.info-icon {
  margin-left: 2px;
}

.data-nums {
  margin-right: 10px;
  font-size: 14px;
  color: var(--el-text-color-secondary);
}

.export-icon {
  width: 12px;
  height: 12px;
  margin-right: 5px;
}
</style>
