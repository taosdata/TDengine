<template>
  <div class="gird">
    <el-table
      v-load-more.expand.immediate="{
        func: load,
        func1: loadLeft,
        target: '.el-scrollbar__wrap',
        delay: 200,
        distance: 100
      }"
      stripe
      border
      tooltip-effect="light"
      size="small"
      :data="currentTableData"
      height="100%"
      style="border-bottom: none"
      @cell-dblclick="handleCellDblclick"
    >
      <!--数据源-->

      <template v-if="currentHead.length">
        <el-table-column
          v-for="(item, index) in currentHead"
          :key="item.field + index"
          :min-width="item.length + 'px'"
          :show-overflow-tooltip="true"
          :label="item.field"
          :prop="index + ''"
        >
          <template #default="{ row }">
            <el-tooltip :content="t('explorer.cellCopyTip')">
              <span>{{ row[index] }}</span>
            </el-tooltip>
          </template>
        </el-table-column>
      </template>
    </el-table>
    <section v-if="currentHistory && currentTableData.length" class="time-wrapper">
      <div class="time-block">
        <span class="title">{{ t('explorer.execute') }}:</span>
        <span class="value">{{ currentHistory.executTime }} ms</span>
      </div>
      <div class="time-block">
        <span class="title">{{ t('explorer.network') }}:</span>
        <span class="value">{{ currentHistory.networkTime }} ms</span>
      </div>
      <div class="time-block">
        <span class="title">{{ t('common.total') }}:</span>
        <span class="value">{{ currentHistory.totalTime }} ms</span>
      </div>
      <div class="idmptip">
        <router-link to="/idmp">
        <span class="title">{{ t('explorer.idmptip') }}</span>
      </router-link>
      </div>
    </section>
  </div>
</template>
<script lang="ts" setup>
import { copy } from 'utils';
import { t } from 'locales';
import { sqlExecResult, addLogEvent } from './utils';

const head = computed(() => sqlExecResult.head);
const dataSource = computed(() => sqlExecResult.data);
const currentHistory = ref<Recordable | null>(null);
const currentTableData = ref<any[]>([]);
const pageSize = ref(30);
const currentCol = ref(1);
const colSize = ref(20);
const currentPage = ref(1);
const key = ref(0);
const currentHead = ref<any[]>([]);

addLogEvent.on(log => {
  if (log.type == 1) {
    currentHistory.value = log;
  } else {
    currentHistory.value = null;
  }
});

function handleCellDblclick(row: Recordable, column: any) {
  copy(row[column.property]);
}
watch(
  dataSource,
  val => {
    key.value++;
    currentTableData.value = val.slice(0, pageSize.value);
    currentPage.value = 1;
  },
  {
    immediate: true
  }
);
watch(
  head,
  val => {
    currentHead.value = val.slice(0, colSize.value);
    currentCol.value = 1;
  },
  {
    immediate: true
  }
);

function load() {
  if (currentTableData.value.length === dataSource.value.length) return;
  currentPage.value++;
  currentTableData.value.push(
    ...dataSource.value.slice(pageSize.value * (currentPage.value - 1), pageSize.value * currentPage.value)
  );
}
function loadLeft() {
  if (currentHead.value.length === head.value.length) return;
  currentCol.value++;
  currentHead.value.push(...head.value.slice(colSize.value * (currentCol.value - 1), colSize.value * currentCol.value));
}
</script>
<style lang="scss" scoped>
.gird {
  position: relative;
  height: 100%;
  padding-bottom: 30px;

  // overflow: auto;
  overflow: hidden;

  &:deep(.el-table::before) {
    height: 0;
  }

  &:deep(.el-table--mini .el-table__header-wrapper .el-table__cell) {
    cursor: unset;
  }

  .time-wrapper {
    position: absolute;
    right: 0;
    bottom: -3px;
    left: 10px;

    .time-block {
      display: inline-block;
      margin-right: 20px;
      line-height: 20px;

      .title {
        margin-right: 5px;
        font-size: 16px;
        color: #4d6992;
      }

      .value {
        font-size: 14px;
        color: #999;
      }
    }
  }

  .idmptip {
    position: absolute;
    right: 0;
    display: inline-block;

    .title {
      margin-right: 5px;
      font-size: 16px;
      color: #4d6992;
    }

    .title:hover {
      color: #1976d2; /* 悬浮时变为蓝色 */
    }
  }
}
</style>
